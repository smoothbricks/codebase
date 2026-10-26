//! Bounded waits for an exclusive `flock(2)` another process holds (specs/cowshed/05_gateway.md,
//! "Deadlines": a short lock is a request).
//!
//! An uncontended lock is taken without blocking. A contended one is waited for on a helper thread
//! that blocks in `flock(2)` and hands the locked descriptor back the moment the holder releases
//! it; nothing polls. At the deadline the wait fails `TIMEOUT`, naming the lock and its holder as
//! the caller's census reads it at that moment. The expiry ends the wait, never the holder. The
//! helper owns the descriptor, so a lock it takes after the caller left is released as soon as it
//! is taken: its send fails and hands the descriptor back, and dropping that closes it.

use std::fmt;
use std::io;
use std::os::fd::{AsFd, AsRawFd as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use crate::deadline::{Deadline, Expired, Progress, clock};

/// The lock a wait was on, and who held it as the caller's census named the holder at expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockHolder {
    pub lock: PathBuf,
    pub holder: String,
}

impl fmt::Display for LockHolder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "lock {} held by {}",
            self.lock.display(),
            self.holder
        )
    }
}

/// How a bounded lock wait failed.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// The holder kept the lock past the bound: code `TIMEOUT`, terminal.
    #[error("{0}")]
    Expired(Box<Expired<LockHolder>>),
    /// The kernel refused the lock, or the waiter thread could not start or answer.
    #[error("{op} on lock {}: {source}", lock.display())]
    Io {
        op: &'static str,
        lock: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl LockError {
    /// The lmao error code this failure is reported under.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Expired(_) => "TIMEOUT",
            Self::Io { .. } => "IO",
        }
    }
}

/// An expiry is `TimedOut` and an I/O failure keeps its kind, each carrying the typed failure, for
/// a caller whose interface is `io::Result`.
impl From<LockError> for io::Error {
    fn from(error: LockError) -> Self {
        let kind = match &error {
            LockError::Expired(_) => io::ErrorKind::TimedOut,
            LockError::Io { source, .. } => source.kind(),
        };
        Self::new(kind, error)
    }
}

/// `flock(2)` `file` with `operation`, again when a signal interrupts it. Private: a blocking
/// operation here would be an unbounded wait; callers take [`try_exclusive`] or [`exclusive`].
// WHY unbounded: a `LOCK_NB` try returns at once; the blocking lock runs on `exclusive`'s helper
// thread, whose answer the caller's deadline bounds.
#[allow(clippy::disallowed_methods)]
fn flock(file: &impl AsFd, operation: libc::c_int) -> io::Result<()> {
    loop {
        // SAFETY: `file` owns the descriptor for the whole call; `flock` does not consume it.
        if unsafe { libc::flock(file.as_fd().as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Take `file`'s exclusive lock if it is free now: `Ok(false)` while another open file description
/// holds it. Never waits.
pub fn try_exclusive(file: &impl AsFd) -> io::Result<bool> {
    match flock(file, libc::LOCK_EX | libc::LOCK_NB) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error),
    }
}
/// Lock `file`, the lock file at `lock`, exclusively within what is left of `deadline`, and hand
/// it back holding the lock. At the bound the wait fails naming `holder(lock)`, read then.
pub fn exclusive<F>(
    file: F,
    lock: &Path,
    deadline: &Deadline,
    holder: impl FnOnce(&Path) -> String,
) -> Result<F, LockError>
where
    F: AsFd + Send + 'static,
{
    let failed = |source| LockError::Io {
        op: deadline.op(),
        lock: lock.to_owned(),
        source,
    };
    let expire = |holder: String| {
        LockError::Expired(Box::new(deadline.expired(
            LockHolder {
                lock: lock.to_owned(),
                holder,
            },
            Progress::Nothing,
            clock(),
        )))
    };
    match try_exclusive(&file) {
        Ok(true) => return Ok(file),
        Ok(false) => {}
        Err(error) => return Err(failed(error)),
    }
    let Some(left) = deadline.remaining(clock()) else {
        return Err(expire(holder(lock)));
    };
    let (locked, waiter) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("cowshed-flock-wait".into())
        .spawn(move || {
            // A caller that timed out has dropped `waiter`: the failed send returns the
            // descriptor inside its error, and dropping that releases the lock just taken.
            let _ = locked.send(flock(&file, libc::LOCK_EX).map(|()| file));
        })
        .map_err(failed)?;
    match waiter.recv_timeout(left) {
        Ok(result) => result.map_err(failed),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            // Leave before the census, which can take a while: a lock granted from here on is
            // released by the failed send, and one sent just now is dropped with the channel.
            drop(waiter);
            Err(expire(holder(lock)))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(failed(io::Error::other(
            "the lock's waiter thread exited without an answer",
        ))),
    }
}

/// How often a waiter queued behind a live holder names it again ([`queued`]).
pub const HOLDER_REPORT: std::time::Duration = std::time::Duration::from_secs(10);

/// Wait for a lock a live holder keeps for as long as its work: queued, never silent. `take`, the
/// kernel's blocking lock, runs on a helper thread while this thread says on stderr, once per
/// `every`, that it still waits for `lock` and who holds it (`holder(lock)`, read anew each
/// time), keeping its place in the kernel's queue. The holder's release, or the kernel's when the
/// holder exits, ends the wait; nothing is signalled and nothing gives up its place.
pub fn queued<T: Send>(
    lock: &Path,
    every: std::time::Duration,
    holder: impl Fn(&Path) -> String,
    take: impl FnOnce() -> io::Result<T> + Send,
) -> io::Result<T> {
    std::thread::scope(|scope| {
        let (granted, grant) = mpsc::channel();
        scope.spawn(move || {
            // This thread waits for the answer below, so the send is never refused.
            let _ = granted.send(take());
        });
        loop {
            match grant.recv_timeout(every) {
                Ok(taken) => return taken,
                Err(mpsc::RecvTimeoutError::Timeout) => crate::eprintln!(
                    "cowshed: still waiting for lock {}, held by {}",
                    lock.display(),
                    holder(lock)
                ),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(io::Error::other(format!(
                        "the waiter for lock {} ended without an answer",
                        lock.display()
                    )));
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::time::Duration;

    use super::*;
    use crate::deadline::{Bound, LatencyClass, start};

    const SHORT: Bound = Bound::new(LatencyClass::Control, Duration::from_millis(50));

    fn lock_file(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cowshed-os-flock-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        File::create(&path).unwrap();
        path
    }

    /// A waiter queued behind a live holder names it at each bound and keeps waiting; once the
    /// holder lets go (here, after its second naming), the waiter takes the lock.
    #[test]
    fn a_queued_waiter_names_its_holder_each_bound_and_takes_the_lock_once_it_is_let_go() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let path = lock_file("queued");
        let holder = File::open(&path).unwrap();
        assert!(
            try_exclusive(&holder).unwrap(),
            "the holder takes the free lock"
        );
        let (release, released) = mpsc::channel::<()>();
        let releaser = std::thread::spawn(move || {
            let _ = released.recv();
            drop(holder);
        });
        let named = AtomicUsize::new(0);
        let waiter = File::open(&path).unwrap();
        queued(
            &path,
            Duration::from_millis(1),
            |_| {
                if named.fetch_add(1, Ordering::SeqCst) == 1 {
                    let _ = release.send(());
                }
                "the test's holder".to_owned()
            },
            || flock(&waiter, libc::LOCK_EX),
        )
        .expect("the waiter takes the lock once the holder lets go");
        assert!(
            named.load(Ordering::SeqCst) >= 2,
            "named at each bound while it waited"
        );
        releaser.join().unwrap();
        let probe = File::open(&path).unwrap();
        assert!(!try_exclusive(&probe).unwrap(), "the waiter holds the lock");
        drop(waiter);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_free_lock_is_taken_without_waiting() {
        let path = lock_file("free");
        let held = exclusive(
            File::open(&path).unwrap(),
            &path,
            &start("free", SHORT),
            |_| unreachable!("a free lock names no holder"),
        )
        .unwrap();
        let probe = File::open(&path).unwrap();
        assert!(!try_exclusive(&probe).unwrap(), "the lock is held");
        drop(held);
        std::fs::remove_file(path).unwrap();
    }

    /// A held lock fails its wait at the bound naming the lock and the holder the census read,
    /// without touching the holder; the lock the helper takes once the holder lets go is released
    /// at once, so the next waiter gets it.
    #[test]
    fn a_held_lock_fails_at_its_bound_naming_the_holder_and_lets_a_late_lock_go() {
        let path = lock_file("held");
        let holder = File::open(&path).unwrap();
        assert!(try_exclusive(&holder).unwrap());

        let error = exclusive(
            File::open(&path).unwrap(),
            &path,
            &start("held", SHORT),
            |lock| format!("the census of {}", lock.display()),
        )
        .expect_err("a held lock is not granted");
        assert_eq!(error.code(), "TIMEOUT");
        let LockError::Expired(expired) = &error else {
            panic!("expected an expiry, got {error}");
        };
        assert_eq!(expired.op, "held");
        assert_eq!(expired.class, LatencyClass::Control);
        assert_eq!(expired.bound, SHORT.within);
        assert!(expired.elapsed >= SHORT.within, "{expired}");
        assert_eq!(expired.last, Progress::Nothing);
        assert_eq!(
            expired.peer,
            LockHolder {
                lock: path.clone(),
                holder: format!("the census of {}", path.display()),
            }
        );
        assert!(
            error.to_string().starts_with("TIMEOUT: held on lock "),
            "{error}"
        );
        assert_eq!(io::Error::from(error).kind(), io::ErrorKind::TimedOut);
        let spent = exclusive(
            File::open(&path).unwrap(),
            &path,
            &start("spent", Bound::new(LatencyClass::Control, Duration::ZERO)),
            |_| "the holder".to_owned(),
        )
        .expect_err("a spent budget is not waited on");
        assert_eq!(spent.code(), "TIMEOUT");

        drop(holder);
        let next = exclusive(
            File::open(&path).unwrap(),
            &path,
            &start(
                "next",
                Bound::new(LatencyClass::Control, crate::test_backstop::NO_PROGRESS),
            ),
            |_| unreachable!("the abandoned waiter released its late lock"),
        )
        .expect("the abandoned waiter's late lock was released");
        drop(next);
        std::fs::remove_file(path).unwrap();
    }
}
