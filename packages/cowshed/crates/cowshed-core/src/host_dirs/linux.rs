//! Linux keeps a user's rebuildable caches under the XDG cache home and its tools' settings
//! under the XDG config home.

use std::path::{Path, PathBuf};

pub fn cache_directory(home: &Path) -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(|| home.join(".cache"))
        .join("cowshed")
}

/// The XDG config home of a process whose environment names none: `~/.config`.
pub fn default_config_home(home: &Path) -> PathBuf {
    home.join(".config")
}
