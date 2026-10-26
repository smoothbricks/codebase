//! The land target's ref lock (02_workspaces.md "`cowshed land`" step 3): the caller's lock file,
//! which every writer that moves the target's branch outside cowshed holds while it does, taken as
//! the held lock every such writer takes ([`crate::held_lock`]). A land holds it from re-reading
//! the target head through the fast-forward and for nothing else, so a writer queued on it waits
//! for that fence, never for a land's checks, cache initialization, adoption or retirement.

use std::time::Instant;

use crate::api::dto::RefLockPath;
use crate::held_lock::{self, HeldLock, Opened};
use crate::{CowshedError, Result};

/// The held ref lock, recorded as this process's. The git that moves the target's ref under it is
/// handed its descriptor ([`Self::descriptor`]), so the kernel keeps the lock until that git ends;
/// nothing else the land starts inherits it. Dropping it empties the record and releases this
/// process's hold.
pub struct RefLock {
    held: HeldLock,
}

impl RefLock {
    /// Take the ref lock at `path`, creating its file if no writer has yet. An uncontended lock is
    /// taken at once; a held one is waited for on a helper thread, and the wait says every
    /// [`cowshed_os::flock::HOLDER_REPORT`] how long it has waited and who holds the file, as the
    /// kernel shows it then ([`held_lock::holders`]). Nothing bounds the wait: the holder is a writer
    /// moving the same branch, which a land must follow, not overtake.
    pub async fn acquire(path: &RefLockPath) -> Result<Self> {
        let opened = Opened::open(path.as_path()).map_err(|error| failure(path, "open", &error))?;
        let opened = match opened
            .try_hold()
            .map_err(|error| failure(path, "lock", &error))?
        {
            Ok(held) => return Ok(Self { held }),
            Err(opened) => opened,
        };
        let (locked, waiter) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("cowshed-ref-lock".into())
            .spawn(move || {
                // A land dropped while it waited has dropped `waiter`: the failed send returns the
                // held lock inside its error, and dropping that releases the lock just taken.
                let _ = locked.send(opened.hold());
            })
            .map_err(|error| failure(path, "start the waiter for", &error))?;
        tokio::pin!(waiter);
        let started = Instant::now();
        let mut report = tokio::time::interval_at(
            tokio::time::Instant::now(),
            cowshed_os::flock::HOLDER_REPORT,
        );
        loop {
            tokio::select! {
                answer = &mut waiter => {
                    let held = answer
                        .map_err(|_| CowshedError::internal(format!(
                            "the waiter for ref lock {} exited without an answer",
                            path.as_str()
                        )))?
                        .map_err(|error| failure(path, "lock", &error))?;
                    return Ok(Self { held });
                }
                _ = report.tick() => cowshed_os::eprintln!(
                    "cowshed: land has waited {}s for the ref lock {}: {}",
                    started.elapsed().as_secs(),
                    path.as_str(),
                    held_lock::holders(path.as_path()),
                ),
            }
        }
    }

    /// The descriptor that holds the lock, for the fence's git to inherit
    /// ([`held_lock::keep_across_exec`]).
    pub fn descriptor(&self) -> std::os::fd::RawFd {
        self.held.descriptor()
    }
}

fn failure(path: &RefLockPath, operation: &str, error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} the ref lock {}: {error}", path.as_str()),
        "name a ref lock file this user can create and write, the one the target's other branch writers hold",
    )
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::FileBackstop as _;
    use std::fs::{File, TryLockError};

    use super::*;

    fn lock_path(directory: &crate::temp_root::TempRoot) -> RefLockPath {
        RefLockPath::new(directory.join("ref.lock")).unwrap()
    }

    fn try_lock(path: &RefLockPath) -> std::result::Result<(), TryLockError> {
        File::options()
            .write(true)
            .open(path.as_path())
            .unwrap()
            .try_lock()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_takes_a_free_ref_lock_records_itself_and_releases_it_when_dropped() {
        let directory = crate::temp_root::TempRoot::new("ref-lock");
        let path = lock_path(&directory);
        let held = RefLock::acquire(&path).await.unwrap();
        assert!(
            matches!(try_lock(&path), Err(TryLockError::WouldBlock)),
            "a held ref lock admitted a second holder"
        );
        let record: crate::lease_holder::Process =
            serde_json::from_slice(&std::fs::read(path.as_path()).unwrap()).unwrap();
        assert_eq!(record.pid, std::process::id());
        drop(held);
        assert!(
            std::fs::read(path.as_path()).unwrap().is_empty(),
            "a released ref lock kept its holder record"
        );
        try_lock(&path).expect("a dropped ref lock stayed held");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_that_asks_for_a_held_ref_lock_holds_it_once_released() {
        let directory = crate::temp_root::TempRoot::new("ref-lock");
        let path = lock_path(&directory);
        let holder = File::create(path.as_path()).unwrap();
        holder.lock_bounded().unwrap();
        let waiting = tokio::spawn({
            let path = path.clone();
            async move { RefLock::acquire(&path).await }
        });
        drop(holder);
        let held = waiting
            .await
            .unwrap()
            .expect("the waiter never took the released lock");
        assert!(matches!(try_lock(&path), Err(TryLockError::WouldBlock)));
        drop(held);
    }
}
