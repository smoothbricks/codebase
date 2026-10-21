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

/// Wait for the install that holds the slot in `support_directory` now, if one does: `true`
/// when one did and has finished, so the gateway it installed serves; `false` at once when none
/// holds it. The wait holds nothing afterwards, so no install ever waits on its caller
/// (05_gateway.md, "A land step waits for an install in progress"). Blocking: call it off the
/// async workers. An install's own process must not call it, since it would wait on itself.
pub fn wait_for_running_install(support_directory: &Path) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd as _;
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
    let flock = |operation| loop {
        // SAFETY: `lock` owns a live descriptor for the call; `flock` does not consume it.
        if unsafe { libc::flock(lock.as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    match flock(libc::LOCK_SH | libc::LOCK_NB) {
        Ok(()) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    eprintln!(
        "cowshed: an install holds {}; waiting for it to finish",
        path.display()
    );
    flock(libc::LOCK_SH)?;
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
        use std::os::fd::AsRawFd as _;
        let root =
            std::env::temp_dir().join(format!("cowshed-install-wait-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        assert!(
            !wait_for_running_install(&root).unwrap(),
            "no lock file: no install"
        );
        let install = std::fs::File::create(root.join(INSTALL_LOCK_NAME)).unwrap();
        assert!(
            !wait_for_running_install(&root).unwrap(),
            "a free slot: no install"
        );
        // SAFETY: a live descriptor.
        assert_eq!(
            unsafe { libc::flock(install.as_raw_fd(), libc::LOCK_EX) },
            0
        );
        let waiter = std::thread::spawn({
            let root = root.clone();
            move || wait_for_running_install(&root)
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            !waiter.is_finished(),
            "the wait lasts as long as the install"
        );
        drop(install);
        assert!(waiter.join().unwrap().unwrap(), "an install was waited for");
        let next = std::fs::File::open(root.join(INSTALL_LOCK_NAME)).unwrap();
        // SAFETY: a live descriptor.
        assert_eq!(
            unsafe { libc::flock(next.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0,
            "the waiter holds nothing once it returns"
        );
        std::fs::remove_dir_all(&root).unwrap();
    }
}
