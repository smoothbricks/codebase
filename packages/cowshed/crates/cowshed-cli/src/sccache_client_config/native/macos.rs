use std::path::{Path, PathBuf};

/// Every directory sccache consults, in the order it consults them.
pub(super) fn config_directories(home: &Path) -> Vec<PathBuf> {
    let bundle = "Mozilla.sccache";
    vec![
        home.join("Library/Application Support").join(bundle),
        home.join("Library/Preferences").join(bundle),
    ]
}
