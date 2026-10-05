//! The build-state contribution contract of spec 16: tools keep their own paths, while one
//! checkout link selects the volume holding them. Neither spelling may escape its root.
//!
//! Capability detection contributes the paths of the tools it recognizes; a repository declares
//! the rest in `.cowshed.toml` `[build] state` ([`DeclaredState`]): tool state no capability can
//! detect, such as a patch-development checkout of an upstream project and its build tree, or a
//! per-package trace store. Both are the same contract and travel the same way.

use serde::Serialize;
use std::path::{Path, PathBuf};

use crate::{CowshedError, Result};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
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

/// The volume namespace of declared build state: `<checkout path>` lives at
/// `declared/<checkout path>`, apart from every tool's own volume names.
const DECLARED: &str = "declared";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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

    pub fn from_paths(checkout: &Path, volume: &Path) -> Result<Self> {
        fn utf8(path: &Path) -> Result<&str> {
            path.to_str().ok_or_else(|| {
                CowshedError::integrity(
                    format!("build-state path {} is not UTF-8", path.display()),
                    "use a UTF-8 capability directory",
                )
            })
        }
        Self::new(utf8(checkout)?, utf8(volume)?)
    }

    /// The build state a repository declares at the checkout-relative `checkout`
    /// (`.cowshed.toml` `[build] state`), held on the volume at `declared/<checkout>`. Refuses a
    /// path that is not a normalized relative name, and one naming Git's metadata or cowshed's own
    /// checkout namespace: declared state is discarded at its first touch.
    pub fn declared(checkout: &str) -> std::result::Result<Self, &'static str> {
        let path = super::validate_override_directory(checkout)
            .map_err(|_| "must be a nonempty normalized checkout-relative path")?;
        if path.components().any(|part| part.as_os_str() == ".git") {
            return Err("must not name Git metadata");
        }
        if path.starts_with(".cowshed") {
            return Err("must not be inside cowshed's own .cowshed directory");
        }
        Ok(Self {
            volume: RelPath(Path::new(DECLARED).join(&path)),
            checkout: RelPath(path),
        })
    }

    /// Whether this is declared build state ([`Self::declared`]) rather than a capability's.
    pub fn is_declared(&self) -> bool {
        self.volume.as_path().strip_prefix(DECLARED) == Ok(self.checkout.as_path())
    }
}

/// One `.cowshed.toml` `[build] state` entry.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DeclaredState {
    /// A literal checkout path.
    Path(BuildStatePath),
    /// `<selector>/<state>`, e.g. `packages/*/.cache/lmao`. The selector's components hold
    /// `*`, `?` or `[...]` (one directory level each, as Git's glob pathspec reads them) and
    /// select each directory the checkout tracks files under; `state` is the literal path
    /// beneath every selected directory. Expanded at discovery
    /// (`build_state_discovery::expand_declared`), so a newly tracked package joins.
    Pattern { selector: PathBuf, state: PathBuf },
}

impl DeclaredState {
    /// The entry `value` spells: a pattern when any component holds `*`, `?` or `[`.
    pub fn parse(value: &str) -> std::result::Result<Self, &'static str> {
        let glob = |part: &std::ffi::OsStr| {
            part.to_str()
                .is_some_and(|part| part.contains(['*', '?', '[']))
        };
        if !value.contains(['*', '?', '[']) {
            return BuildStatePath::declared(value).map(Self::Path);
        }
        let path = super::validate_override_directory(value)
            .map_err(|_| "must be a nonempty normalized checkout-relative path")?;
        if value.contains("**") {
            return Err("must not use `**`: each pattern component selects one directory level");
        }
        let parts: Vec<&std::ffi::OsStr> = path.components().map(|part| part.as_os_str()).collect();
        let last = parts
            .iter()
            .rposition(|part| glob(part))
            .expect("a pattern holds a glob component");
        if last + 1 == parts.len() {
            return Err(
                "must end in a literal name: the pattern selects directories, the name after it \
                 is the state",
            );
        }
        if parts.iter().any(|part| *part == ".git") {
            return Err("must not name Git metadata");
        }
        if parts[0] == ".cowshed" {
            return Err("must not be inside cowshed's own .cowshed directory");
        }
        Ok(Self::Pattern {
            selector: parts[..=last].iter().collect(),
            state: parts[last + 1..].iter().collect(),
        })
    }

    /// The entry as `.cowshed.toml` spells it.
    pub fn spelling(&self) -> PathBuf {
        match self {
            Self::Path(path) => path.checkout.as_path().to_owned(),
            Self::Pattern { selector, state } => selector.join(state),
        }
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

    #[test]
    fn declared_state_lives_in_its_own_volume_namespace_and_never_names_git_or_cowshed() {
        for invalid in [
            "",
            "/tmp",
            "../outside",
            "a/./b",
            "a/",
            ".git",
            "vendor/.git",
            "vendor/.git/modules",
            ".cowshed",
            ".cowshed/build",
        ] {
            assert!(BuildStatePath::declared(invalid).is_err(), "{invalid:?}");
        }
        let state = BuildStatePath::declared("packages/upstream/.pin").unwrap();
        assert_eq!(
            state.checkout.as_path(),
            Path::new("packages/upstream/.pin")
        );
        assert_eq!(
            state.volume.as_path(),
            Path::new("declared/packages/upstream/.pin")
        );
        assert!(state.is_declared());
        assert!(
            !BuildStatePath::new("target", "target")
                .unwrap()
                .is_declared()
        );
        // A capability directory override can put a tool's state under `declared/`; its volume
        // name is never `declared/` and then its own checkout path.
        assert!(
            !BuildStatePath::new("declared/.codegraph", "declared/codegraph")
                .unwrap()
                .is_declared()
        );
    }

    #[test]
    fn a_pattern_selects_directories_and_names_literal_state_beneath_them() {
        assert_eq!(
            DeclaredState::parse("packages/*/.cache/lmao").unwrap(),
            DeclaredState::Pattern {
                selector: PathBuf::from("packages/*"),
                state: PathBuf::from(".cache/lmao"),
            }
        );
        assert_eq!(
            DeclaredState::parse("apps/web-[ab]/x?/out").unwrap(),
            DeclaredState::Pattern {
                selector: PathBuf::from("apps/web-[ab]/x?"),
                state: PathBuf::from("out"),
            }
        );
        assert_eq!(
            DeclaredState::parse(".cache/lmao").unwrap(),
            DeclaredState::Path(BuildStatePath::declared(".cache/lmao").unwrap())
        );
        for (invalid, reason) in [
            ("packages/*", "must end in a literal name"),
            ("packages/*/.cache/*", "must end in a literal name"),
            ("packages/**/.cache", "must not use `**`"),
            ("*/.git/x", "must not name Git metadata"),
            (".cowshed/*/x", "inside cowshed's own"),
            ("/abs/*/x", "normalized checkout-relative"),
            ("a/../*/x", "normalized checkout-relative"),
        ] {
            let error = DeclaredState::parse(invalid).unwrap_err();
            assert!(error.contains(reason), "{invalid:?}: {error}");
        }
        assert_eq!(
            DeclaredState::parse("packages/*/.cache/lmao")
                .unwrap()
                .spelling(),
            Path::new("packages/*/.cache/lmao")
        );
    }
}
