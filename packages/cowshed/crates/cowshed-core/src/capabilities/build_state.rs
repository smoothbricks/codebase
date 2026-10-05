//! The build-state contribution contract of spec 16: tools keep their own paths, while one
//! checkout link selects the volume holding them. Neither spelling may escape its root.

use std::path::{Path, PathBuf};

use crate::{CowshedError, Result};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RelPath(PathBuf);

impl RelPath {
    pub fn new(value: &str) -> Result<Self> {
        super::validate_override_directory(value)
            .map(Self)
            .map_err(|reason| {
                CowshedError::integrity(
                    format!("invalid build-state path {value:?}: {reason}"),
                    "repair the capability build-state contribution",
                )
            })
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BuildStatePath {
    pub checkout: RelPath,
    pub volume: RelPath,
}

impl BuildStatePath {
    pub fn new(checkout: &str, volume: &str) -> Result<Self> {
        Ok(Self {
            checkout: RelPath::new(checkout)?,
            volume: RelPath::new(volume)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_state_paths_are_normalized_nonempty_relative_names() {
        for invalid in [
            "",
            "/tmp",
            "../outside",
            "a/../b",
            "./a",
            "a//b",
            "a/./b",
            "a/",
            "a\0b",
        ] {
            assert!(
                BuildStatePath::new(invalid, "target").is_err(),
                "{invalid:?}"
            );
            assert!(
                BuildStatePath::new("target", invalid).is_err(),
                "{invalid:?}"
            );
        }
        let state = BuildStatePath::new("apps/web/.nx/cache", "apps/web/nx/cache").unwrap();
        assert_eq!(state.checkout.as_path(), Path::new("apps/web/.nx/cache"));
        assert_eq!(state.volume.as_path(), Path::new("apps/web/nx/cache"));
    }
}
