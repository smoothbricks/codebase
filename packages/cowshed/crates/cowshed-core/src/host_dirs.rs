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

/// Every HOME directory holding cowshed controller state.
pub fn controller_state(home: &Path) -> [PathBuf; 2] {
    [support_directory(home), cache_directory(home)]
}
