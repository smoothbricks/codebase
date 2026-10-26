//! Host configuration values: the `.cowshed.toml` schema and the stored bounds it
//! carries. Parsing is pure over the text it is given; shells read the file.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use toml::Spanned;

use crate::capabilities::{CapabilityId, CapabilityOverride, DeclaredState};
/// How long a shed must show no use of Nx before its supervisor stops the daemon it keeps: main's
/// `.cowshed.toml` `[nx] daemon_idle`, [`NxDaemonIdle::DEFAULT`] without it (04_sandbox.md, "The
/// Nx daemon stays on"). Never zero: a zero bound would stop a daemon at the probe after the one
/// that found it come up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NxDaemonIdle(Duration);

impl NxDaemonIdle {
    /// Long enough that a gate's runs, an edit and the next run share one warm daemon (a cold one
    /// recomputes the project graph and every plugin worker's state), short enough that a shed
    /// whose agent has finished costs its host five minutes of a daemon, not the rest of the day.
    pub const DEFAULT: Self = Self(Duration::from_secs(5 * 60));

    /// The bound `idle`; `None` for zero.
    pub fn new(idle: Duration) -> Option<Self> {
        (!idle.is_zero()).then_some(Self(idle))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}
pub const VOLUME_MARKER_FILE: &str = ".cowshed-volume.json";
/// Repository-owned settings accepted from `.cowshed.toml`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CowshedConfig {
    substrate: Option<SubstrateConfig>,
    /// `[sandbox] deny`: workspace-relative paths no job may read or write. Trusted only from
    /// main's checkout, which the operator owns; a workspace's copy is the agent's to edit.
    sandbox_deny: Vec<PathBuf>,
    /// `[caches] home`: HOME-relative cache directories the repository's own tooling places,
    /// shared into every sandbox of the project. Trusted only from main's checkout, exactly like
    /// `[sandbox] deny`.
    caches_home: Vec<PathBuf>,
    capabilities: std::collections::BTreeMap<
        crate::capabilities::CapabilityId,
        crate::capabilities::CapabilityOverride,
    >,
    /// `[build] capacity`: the capacity of the build volumes this project creates from nothing
    /// (16_build_volumes.md, "Substrate"); a clone and a seed inherit their source's instead.
    build_capacity: Option<crate::image::ImageCapacity>,
    /// `[build] state`: build state the repository declares for tools no capability detects
    /// (16_build_volumes.md, "Declared build state"), sorted, no spelling inside another.
    build_state: Vec<crate::capabilities::DeclaredState>,
    /// `[nx] daemon_idle`: how long a shed goes unused before its supervisor stops the Nx daemon
    /// it keeps (04_sandbox.md, "The Nx daemon stays on").
    nx_daemon_idle: Option<NxDaemonIdle>,
}

impl CowshedConfig {
    pub fn substrate(&self) -> Option<&SubstrateConfig> {
        self.substrate.as_ref()
    }

    pub fn sandbox_deny(&self) -> &[PathBuf] {
        &self.sandbox_deny
    }

    pub fn caches_home(&self) -> &[PathBuf] {
        &self.caches_home
    }

    pub fn capabilities(
        &self,
    ) -> &std::collections::BTreeMap<
        crate::capabilities::CapabilityId,
        crate::capabilities::CapabilityOverride,
    > {
        &self.capabilities
    }

    /// The capacity a build volume created from nothing gets: `[build] capacity`, else
    /// [`crate::build_volume::DEFAULT_BUILD_VOLUME_CAPACITY`].
    pub fn build_capacity(&self) -> crate::image::ImageCapacity {
        self.build_capacity
            .unwrap_or(crate::build_volume::DEFAULT_BUILD_VOLUME_CAPACITY)
    }

    /// The entries `[build] state` declares; empty without it. Patterns are expanded against the
    /// checkout at discovery.
    pub fn build_state(&self) -> &[crate::capabilities::DeclaredState] {
        &self.build_state
    }

    /// How long a shed goes unused before its supervisor stops its Nx daemon: `[nx] daemon_idle`,
    /// else [`NxDaemonIdle::DEFAULT`].
    pub fn nx_daemon_idle(&self) -> NxDaemonIdle {
        self.nx_daemon_idle.unwrap_or(NxDaemonIdle::DEFAULT)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubstrateConfig {
    pool: String,
}

impl SubstrateConfig {
    pub fn pool(&self) -> &str {
        &self.pool
    }
}

/// `.cowshed.toml` as TOML spells it: every section and key cowshed reads, and nothing else. The
/// TOML parser refuses a duplicated key or table and serde an unknown one, so a setting cannot
/// silently change project detection; the spans locate each value for the validation in
/// [`parse_cowshed_config`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    substrate: Option<SubstrateSection>,
    sandbox: Option<SandboxSection>,
    build: Option<BuildSection>,
    caches: Option<CachesSection>,
    nx: Option<NxSection>,
    #[serde(default)]
    capabilities: BTreeMap<CapabilityId, CapabilitySection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NxSection {
    daemon_idle: Spanned<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubstrateSection {
    kind: ExplicitSubstrate,
    pool: Spanned<String>,
}

/// `[substrate] kind`: only an explicit ZFS pool overrides the filesystem evidence.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExplicitSubstrate {
    Zfs,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxSection {
    deny: Vec<Spanned<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildSection {
    capacity: Option<Spanned<String>>,
    state: Option<Vec<Spanned<String>>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CachesSection {
    home: Vec<Spanned<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilitySection {
    #[serde(default)]
    disabled: bool,
    directory: Option<Spanned<String>>,
}
/// Parse repository-owned storage, sandbox deny, build volume capacity and declared state,
/// repository cache, the Nx daemon idle bound and convention-only capability overrides. Unknown or duplicated settings fail
/// rather than silently changing project detection.
pub fn parse_cowshed_config(input: &str) -> Result<CowshedConfig, ConfigError> {
    let file: ConfigFile = toml::from_str(input).map_err(|error| ConfigError::Malformed {
        line: error.span().map(|span| line_at(input, span.start)),
        message: error.message().to_owned(),
    })?;
    let substrate = match file.substrate {
        Some(SubstrateSection {
            kind: ExplicitSubstrate::Zfs,
            pool,
        }) => {
            let pool = pool.into_inner();
            validate_pool_name(&pool).map_err(ConfigError::InvalidPool)?;
            Some(SubstrateConfig { pool })
        }
        None => None,
    };
    let sandbox_deny = match file.sandbox {
        Some(section) => relative_paths(input, section.deny, "sandbox", "deny", "workspace")?,
        None => Vec::new(),
    };
    let caches_home = match file.caches {
        Some(section) => relative_paths(input, section.home, "caches", "home", "HOME")?,
        None => Vec::new(),
    };
    let (build_capacity, build_state) = match file.build {
        None => (None, Vec::new()),
        Some(BuildSection {
            capacity: None,
            state: None,
        }) => return Err(ConfigError::EmptySection("build")),
        Some(BuildSection { capacity, state }) => (
            capacity
                .map(|capacity| {
                    crate::image::ImageCapacity::parse(capacity.get_ref()).map_err(|reason| {
                        ConfigError::InvalidBuildCapacity {
                            line: line_of(input, &capacity),
                            reason,
                        }
                    })
                })
                .transpose()?,
            match state {
                Some(entries) => build_state(input, entries)?,
                None => Vec::new(),
            },
        ),
    };
    let nx_daemon_idle = file
        .nx
        .map(|section| {
            nx_daemon_idle(section.daemon_idle.get_ref()).ok_or_else(|| {
                ConfigError::InvalidNxDaemonIdle {
                    line: line_of(input, &section.daemon_idle),
                    value: section.daemon_idle.get_ref().clone(),
                }
            })
        })
        .transpose()?;
    let capabilities =
        file.capabilities
            .into_iter()
            .map(|(id, section)| {
                let directory = section
                    .directory
                    .map(|directory| {
                        crate::capabilities::validate_override_directory(directory.get_ref())
                            .map_err(|reason| ConfigError::InvalidCapabilityDirectory {
                                section: id.section_name(),
                                line: line_of(input, &directory),
                                reason,
                            })
                    })
                    .transpose()?;
                Ok((
                    id,
                    CapabilityOverride {
                        disabled: section.disabled,
                        directory,
                    },
                ))
            })
            .collect::<Result<_, ConfigError>>()?;
    Ok(CowshedConfig {
        substrate,
        sandbox_deny,
        caches_home,
        capabilities,
        build_capacity,
        build_state,
        nx_daemon_idle,
    })
}

/// `[nx] daemon_idle`: a whole number of seconds, minutes or hours above zero (`"90s"`, `"5m"`,
/// `"1h"`); `None` for anything else.
fn nx_daemon_idle(text: &str) -> Option<NxDaemonIdle> {
    let (count, seconds) = [('s', 1), ('m', 60), ('h', 60 * 60)]
        .into_iter()
        .find_map(|(unit, seconds)| Some((text.strip_suffix(unit)?, seconds)))?;
    if count.is_empty() || !count.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let seconds = count.parse::<u64>().ok()?.checked_mul(seconds)?;
    NxDaemonIdle::new(std::time::Duration::from_secs(seconds))
}
/// The 1-based line `value` starts on in `input`.
fn line_of<T>(input: &str, value: &Spanned<T>) -> usize {
    line_at(input, value.span().start)
}

/// The 1-based line of byte `offset` in `input`.
fn line_at(input: &str, offset: usize) -> usize {
    input
        .bytes()
        .take(offset)
        .filter(|&byte| byte == b'\n')
        .count()
        + 1
}

/// `[sandbox] deny` and `[caches] home`: paths, each relative to `base` (the workspace, or HOME)
/// and a non-empty path of plain components — no root, no `.` or `..` — sorted and deduplicated.
fn relative_paths(
    input: &str,
    entries: Vec<Spanned<String>>,
    section: &'static str,
    key: &'static str,
    base: &'static str,
) -> Result<Vec<PathBuf>, ConfigError> {
    let mut paths = entries
        .into_iter()
        .map(|entry| {
            let line = line_of(input, &entry);
            let path = entry.into_inner();
            let relative = Path::new(&path);
            if path.is_empty()
                || relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return Err(ConfigError::InvalidRelativePath {
                    section,
                    key,
                    base,
                    line,
                    path,
                });
            }
            Ok(relative.components().collect::<PathBuf>())
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// `[build] state`: checkout-relative paths or patterns, each a [`DeclaredState`], sorted and
/// deduplicated, no spelling inside another: one inside another would link state through state.
/// A pattern's expansion is checked again at discovery, where its matches are known.
fn build_state(
    input: &str,
    entries: Vec<Spanned<String>>,
) -> Result<Vec<DeclaredState>, ConfigError> {
    let mut state = entries
        .into_iter()
        .map(|entry| {
            let line = line_of(input, &entry);
            let path = entry.into_inner();
            DeclaredState::parse(&path)
                .map(|declared| (declared, line))
                .map_err(|reason| ConfigError::InvalidBuildState { line, path, reason })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Sorting by (entry, line) keeps a repeated entry's first line.
    state.sort();
    state.dedup_by(|later, earlier| later.0 == earlier.0);
    let spellings: Vec<(PathBuf, usize)> = state
        .iter()
        .map(|(entry, line)| (entry.spelling(), *line))
        .collect();
    for (index, (outer, _)) in spellings.iter().enumerate() {
        if let Some((inner, line)) = spellings
            .iter()
            .enumerate()
            .find(|(other, (inner, _))| *other != index && inner.starts_with(outer))
            .map(|(_, inner)| inner)
        {
            return Err(ConfigError::OverlappingBuildState {
                line: *line,
                outer: outer.clone(),
                inner: inner.clone(),
            });
        }
    }
    Ok(state.into_iter().map(|(entry, _)| entry).collect())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    /// Not TOML, or TOML that is not a `.cowshed.toml`: a syntax error, a duplicated or unknown
    /// section or key, a missing key, or a value of the wrong type. `message` is the TOML
    /// reader's own, which names what it expected.
    #[error(
        "malformed configuration{}: {message}",
        line.map_or_else(String::new, |line| format!(" at line {line}"))
    )]
    Malformed {
        line: Option<usize>,
        message: String,
    },
    #[error(
        "[{section}] {key} entry {path:?} at line {line} must be a non-empty {base}-relative path without `.` or `..`"
    )]
    InvalidRelativePath {
        section: &'static str,
        key: &'static str,
        base: &'static str,
        line: usize,
        path: String,
    },
    #[error("[{section}] directory at line {line}: {reason}")]
    InvalidCapabilityDirectory {
        section: &'static str,
        line: usize,
        reason: &'static str,
    },
    #[error("[build] capacity at line {line}: {reason}")]
    InvalidBuildCapacity {
        line: usize,
        reason: crate::image::ImageCapacityError,
    },
    #[error("[build] state entry {path:?} at line {line} {reason}")]
    InvalidBuildState {
        line: usize,
        path: String,
        reason: &'static str,
    },
    #[error(
        "[build] state at line {line} declares {} inside {}; declare only the outer path",
        inner.display(),
        outer.display()
    )]
    OverlappingBuildState {
        line: usize,
        outer: PathBuf,
        inner: PathBuf,
    },
    #[error(
        "[nx] daemon_idle {value:?} at line {line} must be a whole number of seconds, minutes or hours above zero, such as \"5m\""
    )]
    InvalidNxDaemonIdle { line: usize, value: String },
    #[error("the [{0}] section sets nothing")]
    EmptySection(&'static str),
    #[error("invalid ZFS pool: {0}")]
    InvalidPool(PoolNameError),
}
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PoolNameError {
    #[error("pool names must be one non-empty component without whitespace or traversal")]
    InvalidPool,
    #[error("invalid containing dataset {0:?}")]
    InvalidDataset(String),
}

pub fn validate_pool_name(pool: &str) -> Result<(), PoolNameError> {
    let mut chars = pool.chars();
    let first = chars.next().ok_or(PoolNameError::InvalidPool)?;
    if first.is_ascii_digit()
        || !matches!(first, 'A'..='Z' | 'a'..='z' | '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        return Err(PoolNameError::InvalidPool);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cowshed_config_parser_accepts_only_complete_known_sections() {
        let config = parse_cowshed_config(
        "# project settings\n[substrate] # deliberate override\nkind = \"zfs\"\npool = \"tank\" # no scan\n",
    )
    .unwrap();
        assert_eq!(config.substrate().unwrap().pool(), "tank");

        for section in ["repository", "devenv"] {
            // Cowshed has no devenv backend: a project activates devenv through its own `.envrc`.
            let error =
                parse_cowshed_config(&format!("[{section}]\nname = \"widget\"\n")).unwrap_err();
            assert!(
                matches!(
                    &error,
                    ConfigError::Malformed { line: Some(1), message }
                        if message.contains(&format!("unknown field `{section}`"))
                ),
                "{error}"
            );
        }
        let invalid = [
            ("[substrate]\npool = \"tank\"\n", "missing field `kind`"),
            ("[substrate]\nkind = \"zfs\"\n", "missing field `pool`"),
            (
                "[substrate]\nkind = \"apfs\"\npool = \"tank\"\n",
                "at line 2: unknown variant `apfs`, expected `zfs`",
            ),
            (
                "[substrate]\nkind = \"zfs\"\npool = \"tank/child\"\n",
                "invalid ZFS pool",
            ),
            (
                "[substrate]\nkind = \"zfs\"\npool = \"tank\"\nextra = \"x\"\n",
                "at line 4: unknown field `extra`",
            ),
            (
                "[substrate]\nkind = \"zfs\"\nkind = \"zfs\"\npool = \"tank\"\n",
                "at line 3: duplicate key",
            ),
            (
                "[substrate]\nkind = \"zfs\"\npool = tank\n",
                "malformed configuration at line 3",
            ),
            (
                "[substrate]\nkind = \"zfs\"\npool = \"tank\"\n[substrate]\nkind = \"zfs\"\npool = \"tank\"\n",
                "at line 4: duplicate key",
            ),
            ("top = \"level\"\n", "at line 1: unknown field `top`"),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
        for source in [
            "[substrate]\nmalformed\n",
            "[substrate]\nkind = \"zfs\npool = \"tank\"\n",
            "[substrate]\nkind = zfs\"\npool = \"tank\"\n",
        ] {
            assert!(
                matches!(
                    parse_cowshed_config(source),
                    Err(ConfigError::Malformed { line: Some(2), .. })
                ),
                "{source:?}"
            );
        }
        // A `#` inside a string is part of it, not a comment.
        for (source, kind) in [
            (
                "[substrate]\nkind = \"z#fs\" # outside comment\npool = \"tank\"\n",
                "z#fs",
            ),
            (
                r##"[substrate]
kind = "z\"#fs" # outside comment
pool = "tank"
"##,
                "z\"#fs",
            ),
        ] {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown variant `{kind}`")),
                "{error}"
            );
        }
        for pool in ["1tank", "-tank"] {
            assert!(matches!(
                parse_cowshed_config(&format!("[substrate]\nkind = \"zfs\"\npool = \"{pool}\"\n")),
                Err(ConfigError::InvalidPool(PoolNameError::InvalidPool))
            ));
        }
    }

    /// `.cowshed.toml` is TOML: an array may span lines, end in a comma and carry comments between
    /// its entries, and a literal string is a string. Each entry error names its own line.
    #[test]
    fn cowshed_config_arrays_follow_the_toml_grammar() {
        use crate::capabilities::{BuildStatePath, DeclaredState};
        let config = parse_cowshed_config(
            r#"[sandbox]
deny = [
  ".runtime", # runtime secrets
  'a/./b',
]

[build]
state = [
  # an upstream checkout and its build tree
  "vendor/upstream",
  "tools/.pin",

]

[caches]
home = [ ".cache/ttsc",
         "Library/Caches/tool", ]
"#,
        )
        .unwrap();
        assert_eq!(
            config.sandbox_deny(),
            [PathBuf::from(".runtime"), PathBuf::from("a/b")]
        );
        assert_eq!(
            config.build_state(),
            [
                DeclaredState::Path(BuildStatePath::declared("tools/.pin").unwrap()),
                DeclaredState::Path(BuildStatePath::declared("vendor/upstream").unwrap()),
            ]
        );
        assert_eq!(
            config.caches_home(),
            [
                PathBuf::from(".cache/ttsc"),
                PathBuf::from("Library/Caches/tool")
            ]
        );

        let invalid = [
            (
                "[build]\nstate = [\n  \"vendor\",\n  \"/abs\",\n]\n",
                "[build] state entry \"/abs\" at line 4 must be a nonempty normalized checkout-relative path",
            ),
            (
                "[build]\nstate = [\n  \"a\",\n  # nested\n  \"a/b\",\n]\n",
                "[build] state at line 5 declares a/b inside a",
            ),
            (
                "[caches]\nhome = [\n  \".cache/ok\",\n  \"../other-user\",\n]\n",
                "[caches] home entry \"../other-user\" at line 4 must be a non-empty HOME-relative path",
            ),
            (
                "[build]\nstate = [\n  \"a\",\n  7,\n]\n",
                "malformed configuration at line 4: invalid type: integer `7`, expected a string",
            ),
            (
                "[build]\nstate = [\n  \"a\"\n  \"b\"\n]\n",
                "malformed configuration at line 4",
            ),
            ("[build]\nstate = [\n  \"a\",\n", "malformed configuration"),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
    }

    /// `land` runs nothing in its target, so `.cowshed.toml` has no `[land]` section: a file that
    /// still declares one is refused, never read as if its step ran.
    #[test]
    fn a_land_section_is_refused() {
        let error = parse_cowshed_config("[land]\nwarm = [\"tooling/warm-main\"]\n").unwrap_err();
        assert!(
            matches!(
                &error,
                ConfigError::Malformed { line: Some(1), message }
                    if message.contains("unknown field `land`")
            ),
            "{error}"
        );
        assert!(
            error.to_string().starts_with(
                "malformed configuration at line 1: unknown field `land`, expected one of"
            ),
            "{error}"
        );
    }

    /// `[sandbox] deny` is an array of workspace-relative paths, normalized, sorted and
    /// deduplicated; anything that could name a path outside the workspace is refused.
    #[test]
    fn sandbox_deny_is_an_array_of_workspace_relative_paths() {
        let config = parse_cowshed_config(
        "# operator-owned denies\n[sandbox]\n# runtime secrets\ndeny = [\".runtime\", \"a/./b\", \".runtime\"] # trailing\n",
    )
    .unwrap();
        assert_eq!(
            config.sandbox_deny(),
            [PathBuf::from(".runtime"), PathBuf::from("a/b")]
        );
        assert!(
            parse_cowshed_config("[substrate]\nkind = \"zfs\"\npool = \"tank\"\n")
                .unwrap()
                .sandbox_deny()
                .is_empty()
        );
        assert!(
            parse_cowshed_config("[sandbox]\ndeny = []\n")
                .unwrap()
                .sandbox_deny()
                .is_empty()
        );

        let invalid = [
            ("[sandbox]\n", "missing field `deny`"),
            (
                "[sandbox]\ndeny = \".runtime\"\n",
                "at line 2: invalid type: string \".runtime\", expected a sequence",
            ),
            ("[sandbox]\ndeny = [1]\n", "expected a string"),
            ("[sandbox]\ndeny = [\"\"]\n", "workspace-relative"),
            ("[sandbox]\ndeny = [\"/etc\"]\n", "workspace-relative"),
            ("[sandbox]\ndeny = [\"../main\"]\n", "workspace-relative"),
            ("[sandbox]\ndeny = [\"a/../../b\"]\n", "workspace-relative"),
            (
                "[sandbox]\ndeny = [\"a\"]\ndeny = [\"b\"]\n",
                "at line 3: duplicate key",
            ),
            ("[sandbox]\nread = [\"a\"]\n", "unknown field `read`"),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
    }

    /// `[build] capacity` sets the capacity of a build volume created from nothing; without it a
    /// project gets 100 GiB. The value is an image capacity, so it refuses what an image refuses.
    #[test]
    fn build_capacity_defaults_to_100_gibibytes_and_is_an_image_capacity() {
        use crate::image::ImageCapacity;
        assert_eq!(
            parse_cowshed_config("").unwrap().build_capacity(),
            ImageCapacity::from_gibibytes(100)
        );
        assert_eq!(
            parse_cowshed_config("[build]\ncapacity = \"250g\" # cap on build-cache growth\n")
                .unwrap()
                .build_capacity(),
            ImageCapacity::from_gibibytes(250)
        );
        let invalid = [
            ("[build]\n", "the [build] section sets nothing"),
            ("[build]\ncapacity = 100\n", "expected a string"),
            (
                "[build]\ncapacity = \"lots\"\n",
                "[build] capacity at line 2",
            ),
            ("[build]\ncapacity = \"1k\"\n", "[build] capacity at line 2"),
            (
                "[build]\ncapacity = \"1g\"\ncapacity = \"2g\"\n",
                "duplicate key",
            ),
            ("[build]\ncapacity = \"1g\"\n[build]\n", "duplicate key"),
            ("[build]\nsize = \"1g\"\n", "unknown field `size`"),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
    }

    /// `[nx] daemon_idle` sets how long a shed goes unused before its supervisor stops the Nx daemon
    /// it keeps; without it a shed gets five minutes. A bound is a whole number of seconds, minutes or
    /// hours above zero.
    #[test]
    fn nx_daemon_idle_defaults_to_five_minutes_and_is_a_positive_whole_duration() {
        use crate::config::NxDaemonIdle;
        use std::time::Duration;
        assert_eq!(
            parse_cowshed_config("").unwrap().nx_daemon_idle(),
            NxDaemonIdle::DEFAULT
        );
        assert_eq!(NxDaemonIdle::DEFAULT.get(), Duration::from_secs(300));
        for (value, seconds) in [("90s", 90), ("10m", 600), ("2h", 7200)] {
            assert_eq!(
                parse_cowshed_config(&format!("[nx]\ndaemon_idle = \"{value}\"\n"))
                    .unwrap()
                    .nx_daemon_idle()
                    .get(),
                Duration::from_secs(seconds),
                "{value}"
            );
        }
        let invalid = [
            ("[nx]\n", "missing field `daemon_idle`"),
            ("[nx]\ndaemon_idle = 300\n", "expected a string"),
            (
                "[nx]\ndaemon_idle = \"0m\"\n",
                "[nx] daemon_idle \"0m\" at line 2",
            ),
            (
                "[nx]\ndaemon_idle = \"5\"\n",
                "[nx] daemon_idle \"5\" at line 2",
            ),
            (
                "[nx]\ndaemon_idle = \"m\"\n",
                "[nx] daemon_idle \"m\" at line 2",
            ),
            (
                "[nx]\ndaemon_idle = \"-5m\"\n",
                "[nx] daemon_idle \"-5m\" at line 2",
            ),
            (
                "[nx]\ndaemon_idle = \"5 m\"\n",
                "[nx] daemon_idle \"5 m\" at line 2",
            ),
            (
                "[nx]\ndaemon_idle = \"5d\"\n",
                "[nx] daemon_idle \"5d\" at line 2",
            ),
            (
                "[nx]\ndaemon_idle = \"99999999999999999999h\"\n",
                "[nx] daemon_idle \"99999999999999999999h\" at line 2",
            ),
            ("[nx]\nidle = \"5m\"\n", "unknown field `idle`"),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
    }

    /// `[caches] home` reuses `[sandbox] deny`'s parsing: an array of HOME-relative paths of
    /// plain components, normalized, sorted and deduplicated; an empty, absolute or escaping entry is
    /// refused, and so is a section without its key.
    #[test]
    fn caches_home_is_an_array_of_home_relative_paths() {
        let config = parse_cowshed_config(
        "[caches]\n# the TypeScript plugin cache\nhome = [\".cache/ttsc\", \"Library/Caches/./tool\", \".cache/ttsc\"]\n",
    )
    .unwrap();
        assert_eq!(
            config.caches_home(),
            [
                PathBuf::from(".cache/ttsc"),
                PathBuf::from("Library/Caches/tool")
            ]
        );
        assert!(config.sandbox_deny().is_empty());
        assert!(
            parse_cowshed_config("[sandbox]\ndeny = [\".runtime\"]\n")
                .unwrap()
                .caches_home()
                .is_empty()
        );
        assert!(
            parse_cowshed_config("[caches]\nhome = []\n")
                .unwrap()
                .caches_home()
                .is_empty()
        );

        let invalid = [
            ("[caches]\n", "missing field `home`"),
            (
                "[caches]\nhome = \".cache/ttsc\"\n",
                "malformed configuration at line 2: invalid type: string \".cache/ttsc\", expected a sequence",
            ),
            ("[caches]\nhome = [7]\n", "expected a string"),
            ("[caches]\nhome = [\"\"]\n", "HOME-relative"),
            ("[caches]\nhome = [\"/etc\"]\n", "HOME-relative"),
            ("[caches]\nhome = [\"..\"]\n", "HOME-relative"),
            ("[caches]\nhome = [\"../other-user\"]\n", "HOME-relative"),
            (
                "[caches]\nhome = [\".cache/../../escape\"]\n",
                "HOME-relative",
            ),
            (
                "[caches]\nhome = [\"a\"]\nhome = [\"b\"]\n",
                "duplicate key",
            ),
            ("[caches]\n[caches]\n", "duplicate key"),
            ("[caches]\nshared = [\"a\"]\n", "unknown field `shared`"),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
    }

    /// `[build] state` declares build state no capability detects: checkout-relative, normalized,
    /// never Git's or cowshed's own, and never one path inside another. The volume keeps each under
    /// its own `declared/` namespace.
    #[test]
    fn build_state_declares_disjoint_checkout_relative_paths() {
        use crate::capabilities::{BuildStatePath, DeclaredState};
        assert_eq!(parse_cowshed_config("").unwrap().build_state(), []);
        let config = parse_cowshed_config(
        "[build]\nstate = [\"vendor/upstream\", \"tools/.pin\", \"packages/*/.cache/lmao\", \"vendor/upstream\"] # rebuildable\n",
    )
    .unwrap();
        assert_eq!(
            config.build_state(),
            [
                DeclaredState::Path(BuildStatePath::declared("tools/.pin").unwrap()),
                DeclaredState::Path(BuildStatePath::declared("vendor/upstream").unwrap()),
                DeclaredState::Pattern {
                    selector: "packages/*".into(),
                    state: ".cache/lmao".into(),
                },
            ]
        );
        assert_eq!(
            BuildStatePath::declared("tools/.pin")
                .unwrap()
                .volume
                .as_path(),
            Path::new("declared/tools/.pin")
        );
        let invalid = [
            ("[build]\nstate = \"a\"\n", "expected a sequence"),
            (
                "[build]\nstate = [\"/abs\"]\n",
                "normalized checkout-relative",
            ),
            (
                "[build]\nstate = [\"a/../b\"]\n",
                "normalized checkout-relative",
            ),
            (
                "[build]\nstate = [\"sub/.git\"]\n",
                "must not name Git metadata",
            ),
            (
                "[build]\nstate = [\".cowshed/x\"]\n",
                "must not be inside cowshed's own",
            ),
            (
                "[build]\nstate = [\"a\", \"a/b\"]\n",
                "declares a/b inside a; declare only the outer path",
            ),
            ("[build]\nstate = []\nstate = []\n", "duplicate key"),
            (
                "[build]\nstate = [\"packages/*\"]\n",
                "must end in a literal name",
            ),
            (
                "[build]\nstate = [\"packages/*/.cache\", \"packages/*/.cache/lmao\"]\n",
                "declares packages/*/.cache/lmao inside packages/*/.cache",
            ),
        ];
        for (source, message) in invalid {
            let error = parse_cowshed_config(source).unwrap_err();
            assert!(error.to_string().contains(message), "{source:?}: {error}");
        }
    }
    #[test]
    fn capability_overrides_are_strict_convention_overrides() {
        use crate::capabilities::CapabilityId;
        for id in CapabilityId::ALL {
            let config = parse_cowshed_config(&format!(
                "[{}]\ndisabled = true\ndirectory = \"tools/project\"\n",
                id.section_name(),
            ))
            .unwrap();
            let override_ = config.capabilities().get(&id).unwrap();
            assert!(override_.disabled);
            assert_eq!(
                override_.directory.as_deref(),
                Some(Path::new("tools/project"))
            );
        }
        let config = parse_cowshed_config("[capabilities.nx]\n").unwrap();
        let override_ = config.capabilities().get(&CapabilityId::Nx).unwrap();
        assert!(!override_.disabled);
        assert!(override_.directory.is_none());
        for source in [
            "[capabilities.missing]\n",
            "[capabilities.nx]\nenabled = true\n",
            "[capabilities.nx]\ndisabled = \"true\"\n",
            "[capabilities.nx]\ndisabled = 1\n",
            "[capabilities.nx]\ndisabled = false\ndisabled = true\n",
            "[capabilities.nx]\ndirectory = \"tools\"\ndirectory = \"other\"\n",
            "[capabilities.nx]\n[capabilities.nx]\n",
            "[capabilities.nx]\ndirectory = \"../outside\"\n",
            "[capabilities.nx]\ndirectory = \"/outside\"\n",
            "[capabilities.nx]\ndirectory = \"\"\n",
            "[capabilities.nx]\ndirectory = \"a//b\"\n",
        ] {
            assert!(parse_cowshed_config(source).is_err(), "{source:?}");
        }
    }
}
