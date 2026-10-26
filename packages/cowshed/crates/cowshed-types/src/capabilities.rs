//! Capability vocabulary: detector identities and overrides, the build-state path
//! contract of spec 16, and discovery findings. Capability detection contributes the
//! paths of the tools it recognizes; a repository declares the rest in `.cowshed.toml`
//! `[build] state` ([`DeclaredState`]): tool state no capability can detect. Both are
//! the same contract and travel the same way. Detection itself stays with its shells.

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A build-state path that is not a normalized checkout-relative name, or not UTF-8.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum InvalidBuildStatePath {
    #[error("invalid build-state path {value:?}: {reason}")]
    Invalid { value: String, reason: &'static str },
    #[error("build-state path {0} is not UTF-8")]
    NotUtf8(String),
}

/// The real host's conventional installation directories for `home` and `user`, in
/// search order: where core and every detector resolve an individual tool, never a
/// directory that joins PATH whole (15_capabilities.md, "Bootstrap executables").
/// The shell passes the effective user it looked up; this list asks the host nothing.
pub fn host_program_directories(home: &Path, user: Option<&OsStr>) -> Vec<PathBuf> {
    let mut directories = vec![
        home.join(".nix-profile/bin"),
        home.join(".local/state/nix/profile/bin"),
    ];
    if let Some(user) = user {
        directories.push(Path::new("/etc/profiles/per-user").join(user).join("bin"));
        directories.push(
            Path::new("/nix/var/nix/profiles/per-user")
                .join(user)
                .join("profile/bin"),
        );
    }
    directories.extend(
        [
            "/run/current-system/sw/bin",
            "/nix/var/nix/profiles/default/bin",
            "/opt/homebrew/bin",
            "/usr/local/bin",
            "/usr/bin",
            "/bin",
        ]
        .map(PathBuf::from),
    );
    directories
}
/// A detector's identity. It deserializes from its [`Self::name`], the key of a `.cowshed.toml`
/// `[capabilities.<name>]` section.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CapabilityId {
    Direnv,
    Nx,
    Cargo,
    Go,
    Bun,
    Npm,
    Pnpm,
    Uv,
    Zig,
    Gradle,
    Nix,
    Sccache,
    Codegraph,
}

impl CapabilityId {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Direnv => "direnv",
            Self::Nx => "nx",
            Self::Cargo => "cargo",
            Self::Go => "go",
            Self::Bun => "bun",
            Self::Npm => "npm",
            Self::Pnpm => "pnpm",
            Self::Uv => "uv",
            Self::Zig => "zig",
            Self::Gradle => "gradle",
            Self::Nix => "nix",
            Self::Sccache => "sccache",
            Self::Codegraph => "codegraph",
        }
    }
    pub const fn section_name(self) -> &'static str {
        match self {
            Self::Direnv => "capabilities.direnv",
            Self::Nx => "capabilities.nx",
            Self::Cargo => "capabilities.cargo",
            Self::Go => "capabilities.go",
            Self::Bun => "capabilities.bun",
            Self::Npm => "capabilities.npm",
            Self::Pnpm => "capabilities.pnpm",
            Self::Uv => "capabilities.uv",
            Self::Zig => "capabilities.zig",
            Self::Gradle => "capabilities.gradle",
            Self::Nix => "capabilities.nix",
            Self::Sccache => "capabilities.sccache",
            Self::Codegraph => "capabilities.codegraph",
        }
    }

    pub const ALL: [Self; 13] = [
        Self::Direnv,
        Self::Nx,
        Self::Cargo,
        Self::Go,
        Self::Bun,
        Self::Npm,
        Self::Pnpm,
        Self::Uv,
        Self::Zig,
        Self::Gradle,
        Self::Nix,
        Self::Sccache,
        Self::Codegraph,
    ];
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapabilityOverride {
    pub disabled: bool,
    pub directory: Option<PathBuf>,
}
pub fn validate_override_directory(value: &str) -> std::result::Result<PathBuf, &'static str> {
    let path = Path::new(value);
    if value.is_empty()
        || value.as_bytes().contains(&0)
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
        || path.components().collect::<PathBuf>().as_os_str() != path.as_os_str()
    {
        return Err("capability directory must be a nonempty normalized workspace-relative path");
    }
    Ok(path.to_owned())
}
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RelPath(PathBuf);

impl RelPath {
    pub fn new(value: &str) -> std::result::Result<Self, InvalidBuildStatePath> {
        validate_override_directory(value)
            .map(Self)
            .map_err(|reason| InvalidBuildStatePath::Invalid {
                value: value.to_owned(),
                reason,
            })
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.0
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
    pub fn new(checkout: &str, volume: &str) -> std::result::Result<Self, InvalidBuildStatePath> {
        Ok(Self {
            checkout: RelPath::new(checkout)?,
            volume: RelPath::new(volume)?,
        })
    }

    pub fn from_paths(
        checkout: &Path,
        volume: &Path,
    ) -> std::result::Result<Self, InvalidBuildStatePath> {
        fn utf8(path: &Path) -> std::result::Result<&str, InvalidBuildStatePath> {
            path.to_str()
                .ok_or_else(|| InvalidBuildStatePath::NotUtf8(path.display().to_string()))
        }
        Self::new(utf8(checkout)?, utf8(volume)?)
    }

    /// The build state a repository declares at the checkout-relative `checkout`
    /// (`.cowshed.toml` `[build] state`), held on the volume at `declared/<checkout>`. Refuses a
    /// path that is not a normalized relative name, and one naming Git's metadata or cowshed's own
    /// checkout namespace: declared state is discarded at its first touch.
    pub fn declared(checkout: &str) -> std::result::Result<Self, &'static str> {
        let path = validate_override_directory(checkout)
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
        let path = validate_override_directory(value)
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CargoDiscoveryPhase {
    WorkspaceRoot,
    Metadata,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BuildStateFinding {
    CargoUnavailable {
        manifest: PathBuf,
        phase: CargoDiscoveryPhase,
        cause: String,
    },
    /// The job that asks Cargo held no `cargo` on its PATH, so Cargo was asked about none of the
    /// manifests its batch carried. `shell` is the checkout-relative directory of the workspace
    /// shell whose activation put none there, or `None` when no shell selects the directory the
    /// job ran in: cowshed links no toolchain of its own (15_capabilities.md, "Bootstrap
    /// executables").
    NoCargo {
        shell: Option<PathBuf>,
        manifests: Vec<PathBuf>,
    },
}

impl std::fmt::Display for BuildStateFinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CargoUnavailable {
                manifest,
                phase,
                cause,
            } => write!(
                formatter,
                "Cargo could not name the {} of {}, so its target directory is not on the build \
                 volume: {cause}",
                match phase {
                    CargoDiscoveryPhase::WorkspaceRoot => "workspace root",
                    CargoDiscoveryPhase::Metadata => "target directory",
                },
                manifest.display()
            ),
            Self::NoCargo { shell, manifests } => {
                let manifests = manifests
                    .iter()
                    .map(|manifest| manifest.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                match shell {
                    Some(shell) => write!(
                        formatter,
                        "the workspace shell activated from {} put no `cargo` on its jobs' PATH, \
                         and cowshed links none of its own, so Cargo was not asked about \
                         {manifests} and their target directories are not on the build volume: \
                         declare the toolchain in that shell",
                        shell.join(".envrc").display()
                    ),
                    None => write!(
                        formatter,
                        "no workspace shell (`.envrc`) declares a toolchain for {manifests}, and \
                         cowshed links no `cargo` of its own, so Cargo was not asked about them and \
                         their target directories are not on the build volume: declare the \
                         toolchain in the repository's shell"
                    ),
                }
            }
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
