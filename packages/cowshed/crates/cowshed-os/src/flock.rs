//! Bounded waits for an exclusive `flock(2)` another process holds (specs/cowshed/05_gateway.md,
//! "Deadlines": a short lock is a request).
//!
//! An uncontended lock is taken without blocking. A contended one is waited for on a helper thread
//! that blocks in `flock(2)` and hands the locked descriptor back the moment the holder releases
//! it; nothing polls. At the deadline the wait fails `TIMEOUT`, naming the lock and its holder as
//! the caller's census reads it at that moment. The expiry ends the wait, never the holder. The
//! helper owns the descriptor, so a lock it takes after the caller left is released as soon as it
//! is taken: its send fails and hands the descriptor back, and dropping that closes it.
//!
//! A named lease is the inode a name in a held directory names, never the name: every taker
//! checks after locking, and after a refused attempt, that the name still names the inode it
//! locked, by device and inode without following a link, and reopens it when not
//! ([`lock_named`]; specs/cowshed/02_workspaces.md, the project lease, and 11_shell.md
//! "Protocol", the supervisor socket lease). So a holder's removal of the name leaves every
//! taker meeting on the inode at the name.

use std::ffi::CStr;
use std::fmt;
use std::fs::{File, TryLockError};
use std::io;
use std::os::fd::{AsFd, AsRawFd as _, FromRawFd as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use crate::deadline::{Deadline, Expired, Progress, clock};
use crate::fork_lock::Fenced;

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

/// How many times one taking of a named lease reopens it after finding its name removed under
/// the open. Each reopen means a holder removed the inode this open reached, which happens at
/// most once per removal, so a run of them is the directory being tampered with, not a race.
pub const NAMED_LEASE_REOPENS: usize = 16;

/// The mode a lock file is created with: only its owner opens it.
const LOCK_FILE_MODE: libc::c_uint = 0o600;

/// How a named lease is taken.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseMode {
    Shared,
    Exclusive,
}

/// Why a named lease was not taken. `E` is the caller's own failure to open the directory
/// holding it.
#[derive(Debug)]
pub enum NamedLeaseError<E> {
    Directory(E),
    Open(io::Error),
    Lock(io::Error),
    Inspect(io::Error),
    /// Its name was removed under each of [`NAMED_LEASE_REOPENS`] opens in a row.
    Replaced,
}

/// Take the named lease `name` beneath the directory `directory` opens, in `mode`, without
/// waiting; `None` while another holder excludes it. A holder may remove the name after this
/// opened it: after the attempt, locked or refused, the name must still name the inode the
/// attempt was made on, compared by device and inode without following a link, or the lease is
/// reopened beneath a directory opened anew (which reopens a directory the removal took too).
/// The held directory comes back with the lease. `open` is [`open_lock`] outside tests.
pub fn lock_named<E>(
    mut directory: impl FnMut() -> Result<File, E>,
    name: &CStr,
    mode: LeaseMode,
    mut open: impl FnMut(&File, &CStr) -> io::Result<Fenced<File>>,
) -> Result<Option<(File, Fenced<File>)>, NamedLeaseError<E>> {
    for _ in 0..NAMED_LEASE_REOPENS {
        let held = directory().map_err(NamedLeaseError::Directory)?;
        let lease = match open(&held, name) {
            Ok(lease) => lease,
            // A holder removed the directory after `directory` opened it.
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(NamedLeaseError::Open(error)),
        };
        let locked = match mode {
            LeaseMode::Shared => lease.try_lock_shared(),
            LeaseMode::Exclusive => lease.try_lock(),
        };
        let busy = match locked {
            Ok(()) => false,
            Err(TryLockError::WouldBlock) => true,
            Err(TryLockError::Error(error)) => return Err(NamedLeaseError::Lock(error)),
        };
        if names_file(&held, name, &lease).map_err(NamedLeaseError::Inspect)? {
            return Ok(if busy { None } else { Some((held, lease)) });
        }
    }
    Err(NamedLeaseError::Replaced)
}

/// Whether `name` beneath `directory` is `file` itself: the same device and inode, the leaf
/// not followed. An absent `name` is not.
pub fn names_file(directory: &File, name: &CStr, file: &File) -> io::Result<bool> {
    let mut at = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the held directory fd and NUL-terminated leaf outlive the call, which fills `at`
    // on success. NOFOLLOW stats a link at the leaf itself.
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            at.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(error)
        };
    }
    let mut held = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the held fd outlives the call, which fills `held` on success.
    if unsafe { libc::fstat(file.as_raw_fd(), held.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both calls succeeded, so both buffers are initialized.
    let (at, held) = unsafe { (at.assume_init(), held.assume_init()) };
    Ok(at.st_dev == held.st_dev && at.st_ino == held.st_ino)
}

/// Remove the leaf `name` beneath `directory` (`AT_REMOVEDIR` for an empty directory), never
/// following it.
pub fn unlink_at(directory: &File, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    // SAFETY: the held directory fd and NUL-terminated leaf outlive the call.
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), flags) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Open the lock file `name` beneath `directory`, creating it 0600 when absent, fenced from the
/// open on (`fork_lock`). Never one `O_CREAT` open: racing creators on macOS get `ENOENT` from it
/// (cowshed-apfs `store_lock::open_or_create_lock_at`).
pub fn open_lock(directory: &File, name: &CStr) -> io::Result<Fenced<File>> {
    let open = |flags: libc::c_int| {
        // SAFETY: the held directory fd and NUL-terminated leaf outlive the call. NOFOLLOW
        // refuses a link at the leaf; the mode applies only to O_CREAT.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC | flags,
                LOCK_FILE_MODE,
            )
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: a successful openat returned a new owned file descriptor.
            Ok(Fenced::new(unsafe { File::from_raw_fd(fd) }))
        }
    };
    let lock = open(0)
        .or_else(|error| match error.kind() {
            io::ErrorKind::NotFound => open(libc::O_CREAT | libc::O_EXCL),
            _ => Err(error),
        })
        .or_else(|error| match error.kind() {
            io::ErrorKind::AlreadyExists => open(0),
            _ => Err(error),
        })?;
    if !lock.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "lock is not a regular file",
        ));
    }
    Ok(lock)
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
