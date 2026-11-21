//! macOS keeps a user's rebuildable caches under `~/Library/Caches` and its tools' settings
//! under `~/Library/Application Support`.

use std::path::{Path, PathBuf};

pub fn cache_directory(home: &Path) -> PathBuf {
    home.join("Library/Caches/dev.cowshed")
}

pub fn default_config_home(home: &Path) -> PathBuf {
    home.join("Library/Application Support")
}
