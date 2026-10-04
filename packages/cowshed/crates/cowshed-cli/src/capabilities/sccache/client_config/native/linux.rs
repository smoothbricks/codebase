use std::path::{Path, PathBuf};

/// `$XDG_CONFIG_HOME/sccache`, falling back to `~/.config/sccache`.
///
/// A relative `XDG_CONFIG_HOME` is ignored rather than joined, exactly as the XDG specification
/// and the `directories` crate treat it.
pub(crate) fn config_directories(home: &Path) -> Vec<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(|| home.join(".config"));
    vec![base.join("sccache")]
}
