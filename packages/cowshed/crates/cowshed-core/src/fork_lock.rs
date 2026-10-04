//! The fork lock: no spawn of this process carries a descriptor another of its threads is
//! releasing.
//!
//! Spawning a child copies every descriptor of this process into it, and the child holds those
//! copies until its `exec` closes the close-on-exec ones. A thread that closes a listening socket
//! or a file holding an `flock` lease while another thread is spawning has therefore not released
//! it: the child's copy keeps the socket attached to its file, so a client still connects and then
//! reads end of stream, and keeps the lease held, so a peer's try-lock reports a conflict with an
//! owner that is gone. Go's `syscall.ForkLock` is this lock, for this reason.
//!
//! The contract, process-wide:
//!
//! - Every child is spawned through [`Spawn`], [`Run`] or [`RunAsync`]. They hold the lock
//!   *shared* across the spawn call and nothing else -- never across a wait for the child -- so
//!   spawns proceed together.
//! - Every descriptor whose release a peer observes is held in a [`Fenced`] from the moment it
//!   is opened. Dropping one takes the lock *exclusive* around the close alone: it waits for the
//!   spawns in flight, and for nothing else.
//! - Such a descriptor that is not close-on-exec from the system call that made it -- a socket on
//!   Darwin, which has no `SOCK_CLOEXEC` -- is also created exclusive ([`Fenced::create`]). A
//!   child forked between the `socket` and the `fcntl` that marks it would otherwise keep it past
//!   `exec`, for the child's whole life, where no release can reach it.
//!
//! cowshed-core's `clippy.toml` refuses the unlocked spawns of `std` and `tokio`, so a new call
//! site cannot bypass the lock. The CLI and the gateway spawn through it too. Outside it: the warm
//! shell host (cowshed-shell), a process of its own that releases nothing a peer observes, and a
//! foreign runtime hosting the Node addon, whose own spawns this lock cannot see.
//!
//! The spawn call is the whole window, because it returns only once the child has run `exec` or
//! failed to:
//!
//! - `std::process::Command::spawn` on its fork path (`library/std/src/sys/process/unix/unix.rs`,
//!   `Command::spawn`) reads a close-on-exec status pipe until end of stream, which the child's
//!   `exec` produces by closing it, or until the child's report of a failed `exec`, after which it
//!   has waited for the child. Every `pre_exec` closure runs before that `exec`.
//! - Its `posix_spawn` path is one `posix_spawnp` call: Darwin builds the child's image inside
//!   that system call, and glibc and musl spawn with `CLONE_VFORK`, which suspends the caller until
//!   the child has run `exec`.
//! - `tokio::process::Command::spawn` calls `std::process::Command::spawn` synchronously before it
//!   registers the child with the runtime (tokio 1.53, `src/process/mod.rs`).
//!
//! Neither guard is held across an `.await`. A thread holding the shared guard runs only the spawn
//! call, which drops no [`Fenced`]; a [`Fenced`] never holds another one, so its drop never asks
//! for the exclusive guard it already holds; a [`Fenced::create`] closure makes one descriptor.

use std::fmt;
use std::io;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::os::fd::{AsFd, BorrowedFd};
use std::process::{ExitStatus, Output, Stdio};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Shared by spawns, exclusive to a release. It guards no data: a panic under it leaves nothing
/// inconsistent, so poisoning is ignored.
static FORK_LOCK: RwLock<()> = RwLock::new(());

fn spawning() -> RwLockReadGuard<'static, ()> {
    FORK_LOCK.read().unwrap_or_else(PoisonError::into_inner)
}

fn releasing() -> RwLockWriteGuard<'static, ()> {
    FORK_LOCK.write().unwrap_or_else(PoisonError::into_inner)
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for std::process::Command {}
    impl Sealed for tokio::process::Command {}
}

/// Spawn a child under the fork lock.
pub trait Spawn: sealed::Sealed {
    type Child;

    /// The command's own `spawn`, holding the fork lock shared until the child has run `exec` or
    /// failed to.
    fn spawn_locked(&mut self) -> io::Result<Self::Child>;
}

impl Spawn for std::process::Command {
    type Child = std::process::Child;

    #[expect(
        clippy::disallowed_methods,
        reason = "the one std spawn, under the fork lock"
    )]
    fn spawn_locked(&mut self) -> io::Result<Self::Child> {
        let _spawning = spawning();
        self.spawn()
    }
}

impl Spawn for tokio::process::Command {
    type Child = tokio::process::Child;

    #[expect(
        clippy::disallowed_methods,
        reason = "the one tokio spawn, under the fork lock"
    )]
    fn spawn_locked(&mut self) -> io::Result<Self::Child> {
        let _spawning = spawning();
        self.spawn()
    }
}

/// `std`'s `output` and `status`, spawning under the fork lock and waiting outside it.
pub trait Run: Spawn {
    /// Like `output`: stdin is null and stdout and stderr are collected. Unlike `output`, which
    /// applies those only to streams the command left unset, they replace whatever was set.
    fn output_locked(&mut self) -> io::Result<Output>;

    /// Like `status`: the child inherits every stream the command left unset.
    fn status_locked(&mut self) -> io::Result<ExitStatus>;
}

impl Run for std::process::Command {
    fn output_locked(&mut self) -> io::Result<Output> {
        self.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()?
            .wait_with_output()
    }

    fn status_locked(&mut self) -> io::Result<ExitStatus> {
        self.spawn_locked()?.wait()
    }
}

/// `tokio`'s `output` and `status`, spawning under the fork lock before the returned future
/// first runs, and waiting in it, outside the lock.
pub trait RunAsync: Spawn {
    /// Exactly `output`: stdout and stderr are collected, replacing whatever was set.
    fn output_locked(&mut self) -> impl Future<Output = io::Result<Output>> + Send;

    /// Exactly `status`: the child's piped streams are closed before it is waited for.
    fn status_locked(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send;
}

impl RunAsync for tokio::process::Command {
    fn output_locked(&mut self) -> impl Future<Output = io::Result<Output>> + Send {
        let child = self
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked();
        async move { child?.wait_with_output().await }
    }

    fn status_locked(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send {
        let child = self.spawn_locked();
        async move {
            let mut child = child?;
            // A child blocked on a pipe this process holds would never exit.
            drop(child.stdin.take());
            drop(child.stdout.take());
            drop(child.stderr.take());
            child.wait().await
        }
    }
}

/// A descriptor whose release a peer observes -- a listening socket a client connects to, a file
/// holding an `flock` lease a peer tries -- closed under the exclusive fork lock, so no child
/// still being spawned holds it once the drop returns.
pub struct Fenced<T>(ManuallyDrop<T>);

impl<T: AsFd> Fenced<T> {
    /// A descriptor created close-on-exec in the system call that created it: `open` with
    /// `O_CLOEXEC`, which `std` always passes.
    pub const fn new(descriptor: T) -> Self {
        Self(ManuallyDrop::new(descriptor))
    }

    /// A descriptor `create` makes in two steps, the second marking it close-on-exec: Darwin
    /// has no `SOCK_CLOEXEC`, so `std` and `tokio` create a socket and then set the flag. A child
    /// forked between the two would hold it past its `exec`, for its whole life; creating it
    /// under the exclusive lock leaves no spawn in flight to fork there.
    pub fn create(create: impl FnOnce() -> io::Result<T>) -> io::Result<Self> {
        let _creating = releasing();
        create().map(Self::new)
    }
}

impl<T> Deref for Fenced<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: AsFd> AsFd for Fenced<T> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl<T: fmt::Debug> fmt::Debug for Fenced<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Fenced").field(&*self.0).finish()
    }
}

impl<T> Drop for Fenced<T> {
    fn drop(&mut self) {
        let _releasing = releasing();
        // SAFETY: the descriptor is dropped exactly once, here, and `self` is never used again.
        unsafe { ManuallyDrop::drop(&mut self.0) }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::os::fd::{AsRawFd as _, OwnedFd};
    use std::os::unix::process::CommandExt as _;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::TryLockError;

    use super::*;

    /// A spawn held in flight: its child is parked in `pre_exec`, after the fork and before the
    /// `exec`, holding a copy of every descriptor of this process, until [`InFlight::finish`].
    struct InFlight {
        go: std::io::PipeWriter,
        spawn: std::thread::JoinHandle<io::Result<std::process::Child>>,
    }

    impl InFlight {
        fn start() -> Self {
            let (mut started, started_writer) = std::io::pipe().unwrap();
            let (go_reader, go) = std::io::pipe().unwrap();
            let (started_fd, go_fd) = (started_writer.as_raw_fd(), go_reader.as_raw_fd());
            let go_writer_fd = go.as_raw_fd();
            // `/bin/sh` is the one program path every supported host has: NixOS keeps no
            // `/usr/bin/true`, and its absence there made the spawn itself fail with ENOENT.
            let mut command = Command::new("/bin/sh");
            command.args(["-c", ":"]);
            // SAFETY: the closure runs in the forked child before `exec`, calling only `close`,
            // `write` and `read`, which are async-signal-safe, on descriptors this test keeps open
            // until the spawn has returned. The child closes its own copy of the writer first, so
            // a test that panics before `finish` ends the wait instead of leaving the child parked.
            unsafe {
                command.pre_exec(move || {
                    let mut byte = 0_u8;
                    if libc::close(go_writer_fd) != 0
                        || libc::write(started_fd, (&raw const byte).cast(), 1) != 1
                        || libc::read(go_fd, (&raw mut byte).cast(), 1) != 1
                    {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let spawn = std::thread::spawn(move || {
                let child = command.spawn_locked();
                drop((started_writer, go_reader));
                child
            });
            started.read_exact(&mut [0]).unwrap();
            Self { go, spawn }
        }

        /// Let the parked child run `exec`, and the spawn return.
        fn finish(mut self) {
            self.go.write_all(&[0]).unwrap();
            let mut child = self.spawn.join().unwrap().unwrap();
            assert!(child.wait().unwrap().success());
        }
    }

    fn lease_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "cowshed-fork-lock-{}.lock",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ))
    }

    fn open(path: &Path) -> std::fs::File {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap()
    }

    /// A lease released while a spawn is in flight is free the moment its release returns: the
    /// release waited for the child that copied it to run `exec`.
    #[test]
    fn a_release_waits_for_the_spawn_in_flight() {
        let path = lease_path();
        let lease = Fenced::new(open(&path));
        lease.try_lock().unwrap();
        let in_flight = InFlight::start();
        assert!(
            matches!(FORK_LOCK.try_write(), Err(TryLockError::WouldBlock)),
            "a spawn in flight holds the fork lock"
        );
        let release = std::thread::spawn(move || {
            drop(lease);
            open(&path).try_lock().map(|()| path)
        });
        in_flight.finish();
        let path = release
            .join()
            .unwrap()
            .expect("the released lease is free when its drop returns");
        std::fs::remove_file(path).unwrap();
    }

    /// Names the process [`spawns_do_not_wait_for_each_other`] runs
    /// [`two_spawns_are_in_flight_at_once`] in.
    const ALONE: &str = "COWSHED_TEST_FORK_LOCK_ALONE";

    /// Spawns hold the fork lock shared: two are in flight at once. The pair runs in a process
    /// of its own: a release another test queued meanwhile would hold the second spawn behind
    /// the first, which the test itself keeps in flight, and neither would return.
    #[test]
    fn spawns_do_not_wait_for_each_other() {
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "fork_lock::tests::two_spawns_are_in_flight_at_once",
                "--ignored",
            ])
            .env(ALONE, "1")
            .stdout(Stdio::null())
            .status_locked()
            .unwrap();
        assert!(status.success());
    }

    /// Not a test alone: the process [`spawns_do_not_wait_for_each_other`] starts.
    #[test]
    #[ignore = "helper process of spawns_do_not_wait_for_each_other"]
    fn two_spawns_are_in_flight_at_once() {
        if std::env::var_os(ALONE).is_none() {
            return;
        }
        let first = InFlight::start();
        let second = InFlight::start();
        second.finish();
        first.finish();
    }

    /// The descriptor of a [`Fenced`] is closed while its drop holds the fork lock exclusive.
    #[test]
    fn a_fenced_descriptor_closes_under_the_exclusive_lock() {
        struct Witness {
            descriptor: OwnedFd,
            exclusive: std::sync::mpsc::Sender<bool>,
        }
        impl AsFd for Witness {
            fn as_fd(&self) -> BorrowedFd<'_> {
                self.descriptor.as_fd()
            }
        }
        impl Drop for Witness {
            fn drop(&mut self) {
                let exclusive = matches!(FORK_LOCK.try_read(), Err(TryLockError::WouldBlock));
                self.exclusive.send(exclusive).unwrap();
            }
        }
        let (exclusive, witnessed) = std::sync::mpsc::channel();
        let (reader, _writer) = std::io::pipe().unwrap();
        drop(Fenced::new(Witness {
            descriptor: reader.into(),
            exclusive,
        }));
        assert!(witnessed.recv().unwrap());
    }

    /// [`Fenced::create`] makes its descriptor while it holds the fork lock exclusive.
    #[test]
    fn a_fenced_descriptor_is_created_under_the_exclusive_lock() {
        let mut exclusive = false;
        let created = Fenced::create(|| {
            exclusive = matches!(FORK_LOCK.try_read(), Err(TryLockError::WouldBlock));
            std::io::pipe().map(|(reader, _writer)| reader)
        });
        drop(created.unwrap());
        assert!(exclusive);
    }
}
