//! cowshed's own directories in the host user's HOME.
//!
//! The gateway's mirrors live in cowshed's user cache directory (03_caches.md, layer 1), written
//! only by cowshed-gateway; the support directory holds the installed service binaries and the
//! mbx GC root. Neither is ever a sandbox's to write, so no shared cache may reach into one.
//!
//! The cache directory and a tool's default config home follow the platform's convention
//! (`linux.rs`, `macos.rs`). The support directory is the same path on both: the installed
//! `cowshed` is found on `PATH` by that one path whatever the host.

use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// `~/Library/Caches/dev.cowshed` on macOS, `$XDG_CACHE_HOME/cowshed` on Linux.
pub use platform::cache_directory;

/// Where a host tool told nothing but its HOME keeps its user settings: `~/Library/Application
/// Support` on macOS, `~/.config` on Linux.
pub use platform::default_config_home;

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

/// Where a file-backed gateway credential store keeps its records, one file per binding, the
/// directory 0700 and each record 0600 (05_gateway.md, "Credentials"). The Linux gateway reads
/// it as its `$CREDENTIALS_DIRECTORY`; macOS keeps the same records in the Keychain and makes
/// no such directory.
pub fn credentials_directory(home: &Path) -> PathBuf {
    support_directory(home).join("credentials")
}

/// The install slot's lock in the support directory: an install holds it exclusive from before
/// it reads what is installed until the gateway it restarted serves (05_gateway.md, "The install
/// slot").
pub const INSTALL_LOCK_NAME: &str = "install.lock";

/// Wait for the install that holds the slot in `support_directory` now, if one does: `true`
/// when one did and has finished, so the gateway it installed serves; `false` at once when none
/// holds it. The wait holds nothing afterwards, so no install ever waits on its caller
/// (05_gateway.md, "A land step waits for an install in progress"). Blocking: call it off the
/// async workers. An install's own process must not call it, since it would wait on itself.
pub fn wait_for_running_install(
    census: &crate::census::CensusScope,
    support_directory: &Path,
) -> std::io::Result<bool> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let path = support_directory.join(INSTALL_LOCK_NAME);
    let lock = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)
    {
        Ok(lock) => lock,
        // No install has ever run here, so none runs now.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match lock.try_lock_shared() {
        Ok(()) => return Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => {}
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    cowshed_os::eprintln!(
        "cowshed: an install holds {}; waiting for it to finish",
        path.display()
    );
    // WHY unbounded: queued, not silent: a land step waits for the install in progress, said on
    // stderr first and its holder named again once per HOLDER_REPORT; the install's release when
    // its gateway serves, or the kernel's at its exit, ends the wait, and the operator may interrupt.
    #[allow(clippy::disallowed_methods)]
    cowshed_os::flock::queued(
        &path,
        cowshed_os::flock::HOLDER_REPORT,
        |lock| crate::held_lock::holders(census, lock),
        || loop {
            match lock.lock_shared() {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                taken => return taken,
            }
        },
    )?;
    Ok(true)
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

    /// No install: answered at once. An install holding the slot: waited for, and answered once
    /// it lets go. The wait itself holds nothing, so the next install takes the slot at once.
    #[test]
    fn a_wait_ends_with_the_running_install_and_holds_nothing_after() {
        let root =
            std::env::temp_dir().join(format!("cowshed-install-wait-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        assert!(
            !wait_for_running_install(&crate::census::this_test(), &root).unwrap(),
            "no lock file: no install"
        );
        let install = std::fs::File::create(root.join(INSTALL_LOCK_NAME)).unwrap();
        assert!(
            !wait_for_running_install(&crate::census::this_test(), &root).unwrap(),
            "a free slot: no install"
        );
        install.try_lock().expect("take the free slot");
        let waiter = std::thread::spawn({
            let root = root.clone();
            move || wait_for_running_install(&crate::census::this_test(), &root)
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            !waiter.is_finished(),
            "the wait lasts as long as the install"
        );
        drop(install);
        assert!(waiter.join().unwrap().unwrap(), "an install was waited for");
        let next = std::fs::File::open(root.join(INSTALL_LOCK_NAME)).unwrap();
        next.try_lock()
            .expect("the waiter holds nothing once it returns");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
