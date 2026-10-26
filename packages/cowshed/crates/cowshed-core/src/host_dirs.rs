//! cowshed's own directories in the host user's HOME.
//!
//! The gateway's mirrors live in cowshed's user cache directory (03_caches.md, layer 1), written
//! only by cowshed-gateway; the support directory holds the installed service binaries and the
//! sccache GC root. Neither is ever a sandbox's to write, so no shared cache may reach into one.

use std::path::{Path, PathBuf};

/// `~/Library/Caches/dev.cowshed` on macOS, `$XDG_CACHE_HOME/cowshed` on Linux.
pub fn cache_directory(home: &Path) -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        home.join("Library/Caches/dev.cowshed")
    }
    #[cfg(not(target_os = "macos"))]
    {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| home.join(".cache"))
            .join("cowshed")
    }
}

/// The npm metadata and tarball mirror the gateway serves from.
pub fn gateway_mirror(home: &Path) -> PathBuf {
    cache_directory(home).join("mirror")
}

/// Bare repository mirrors, `<cache directory>/repo-mirrors/<encoded url>`.
pub fn repo_mirrors(home: &Path) -> PathBuf {
    cache_directory(home).join("repo-mirrors")
}

/// `~/Library/Application Support/dev.cowshed`: installed service binaries and GC roots.
pub fn support_directory(home: &Path) -> PathBuf {
    home.join("Library/Application Support/dev.cowshed")
}

/// The install slot's lock in the support directory: an install holds it exclusive from before
/// it reads what is installed until the gateway it restarted serves (05_gateway.md, "The install
/// slot").
pub const INSTALL_LOCK_NAME: &str = "install.lock";

/// What a waiter on the install slot observed of it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotObserved {
    /// No lock file: no install has ever run here, so none runs now.
    Missing,
    /// A shared hold on the slot took at once: no install holds it exclusive.
    SharedTook,
    /// An install holds the slot exclusive now.
    HeldExclusive,
}

/// What a wait for a running install does with what it observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallWait {
    /// Answer at once: no install runs.
    None,
    /// Block until the running install lets go, then answer that one was waited for.
    Running,
}

/// The wait's decision: only a slot an install holds exclusive is waited on.
fn install_wait(slot: SlotObserved) -> InstallWait {
    match slot {
        SlotObserved::Missing | SlotObserved::SharedTook => InstallWait::None,
        SlotObserved::HeldExclusive => InstallWait::Running,
    }
}

/// Wait for the install that holds the slot in `support_directory` now, if one does: `true`
/// when one did and has finished, so the gateway it installed serves; `false` at once when none
/// holds it. The wait holds nothing afterwards, so no install ever waits on its caller
/// (05_gateway.md, "A land step waits for an install in progress"). Blocking: call it off the
/// async workers. An install's own process must not call it, since it would wait on itself.
pub fn wait_for_running_install(support_directory: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let path = support_directory.join(INSTALL_LOCK_NAME);
    let lock = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
    {
        Ok(lock) => Some(lock),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let observed = match &lock {
        None => SlotObserved::Missing,
        Some(lock) => match lock.try_lock_shared() {
            Ok(()) => SlotObserved::SharedTook,
            Err(std::fs::TryLockError::WouldBlock) => SlotObserved::HeldExclusive,
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        },
    };
    match install_wait(observed) {
        InstallWait::None => Ok(false),
        InstallWait::Running => {
            let lock = lock.expect("a held slot is only observed on an open lock");
            cowshed_os::eprintln!(
                "cowshed: an install holds {}; waiting for it to finish",
                path.display()
            );
            // WHY unbounded: a land step waits for the install in progress for as long as it runs, said on
            // stderr first; the operator's interrupt ends it. debt: an install that wedges holding the slot
            // stalls the step. Moves to cowshed_os::flock.
            #[allow(clippy::disallowed_methods)]
            loop {
                match lock.lock_shared() {
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    taken => break taken?,
                }
            }
            Ok(true)
        }
    }
}

/// The host-stable installed binary directory (05_gateway.md, "The installed cowshed").
pub fn installed_binary_directory(home: &Path) -> PathBuf {
    let mut directory = support_directory(home);
    directory.push("bin");
    directory
}

/// Every HOME directory holding cowshed controller state.
pub fn controller_state(home: &Path) -> [PathBuf; 2] {
    [support_directory(home), cache_directory(home)]
}

#[cfg(test)]
mod install_wait_tests {
    use super::*;

    /// The decision itself: a missing or free slot is answered at once; only a slot an
    /// install holds exclusive is waited on.
    #[test]
    fn the_wait_answers_at_once_unless_an_install_holds_the_slot() {
        for (observed, decision) in [
            (SlotObserved::Missing, InstallWait::None),
            (SlotObserved::SharedTook, InstallWait::None),
            (SlotObserved::HeldExclusive, InstallWait::Running),
        ] {
            assert_eq!(install_wait(observed), decision, "{observed:?}");
        }
    }

    /// Names the support directory this test binary waits on when it runs as
    /// [`install_waiter_helper`].
    const WAITER_SUPPORT_ENV: &str = "COWSHED_TEST_INSTALL_WAIT_SUPPORT";

    /// Not a test of its own: the other process of
    /// [`a_wait_ends_with_the_running_install_and_holds_nothing_after`]. Run as this
    /// binary with [`WAITER_SUPPORT_ENV`] set, it waits for that directory's install and
    /// reports the answer on stderr as `waited:<bool>`. Run any other way it does nothing.
    #[test]
    fn install_waiter_helper() {
        use std::io::Write as _;
        let Some(support) = std::env::var_os(WAITER_SUPPORT_ENV) else {
            return;
        };
        let waited = wait_for_running_install(Path::new(&support)).expect("wait for the install");
        std::io::stderr()
            .write_all(format!("waited:{waited}\n").as_bytes())
            .expect("report the wait's answer");
    }

    /// The next line the waiter helper said; its stderr closing first is its death, named
    /// with everything it said.
    fn said_line(
        said: &mut std::io::BufReader<std::process::ChildStderr>,
        transcript: &mut String,
    ) -> String {
        use std::io::BufRead as _;
        let start = transcript.len();
        let read = said
            .read_line(transcript)
            .expect("the waiter helper's stderr");
        assert_ne!(
            read, 0,
            "the waiter helper ended before it answered; it said: {transcript}"
        );
        transcript[start..].trim_end().to_owned()
    }

    /// No install: answered at once. An install holding the slot: a waiter in another
    /// process is held -- proven when production announces the wait, which it does only
    /// after observing the held slot -- and answered once the install lets go. The wait
    /// itself holds nothing, so the next install takes the slot at once.
    #[test]
    fn a_wait_ends_with_the_running_install_and_holds_nothing_after() {
        use crate::temp_root::TempRoot;
        use cowshed_os::fork_lock::Spawn as _;
        use cowshed_os::test_backstop::ChildBackstop as _;
        use std::process::{Command, Stdio};
        let root = TempRoot::new_in(&std::env::temp_dir(), "cs-install-wait");
        assert!(
            !wait_for_running_install(&root).unwrap(),
            "no lock file: no install"
        );
        let lock = root.join(INSTALL_LOCK_NAME);
        let install = std::fs::File::create(&lock).unwrap();
        assert!(
            !wait_for_running_install(&root).unwrap(),
            "a free slot: no install"
        );
        install.try_lock().expect("take the free slot");

        let mut waiter = Command::new(std::env::current_exe().expect("this test binary"))
            .args([
                "--exact",
                "host_dirs::install_wait_tests::install_waiter_helper",
                "--nocapture",
            ])
            .env(WAITER_SUPPORT_ENV, &*root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn_locked()
            .expect("spawn the waiter helper");
        let mut said =
            std::io::BufReader::new(waiter.stderr.take().expect("the waiter helper's stderr"));
        let mut transcript = String::new();
        let waiting_on = lock.display().to_string();
        while !said_line(&mut said, &mut transcript).contains(&waiting_on) {}
        drop(install);
        loop {
            match said_line(&mut said, &mut transcript).as_str() {
                "waited:true" => break,
                "waited:false" => {
                    panic!("the waiter answered at once beside a running install: {transcript}")
                }
                _ => {}
            }
        }
        let status = waiter.wait_bounded().expect("reap the waiter helper");
        assert!(status.success(), "the waiter helper failed: {status}");
        let next = std::fs::File::open(&lock).unwrap();
        next.try_lock()
            .expect("the waiter holds nothing once it returns");
    }
}
