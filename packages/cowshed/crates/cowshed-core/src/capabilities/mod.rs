//! Convention-gated project tooling. The sandbox consumes contributions, never tool names.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use crate::{CowshedError, Result};
use cache::{SharedLayout, SharedToolHome};

mod build_state;
pub use build_state::{BuildStatePath, DeclaredState, RelPath};
mod build_state_discovery;
#[cfg(all(test, target_os = "macos"))]
pub(crate) use build_state_discovery::HostCargo;
pub use build_state_discovery::{
    BuildStateDiscovery, BuildStateFinding, CargoAnswer, CargoDiscoveryPhase, CargoQuery,
    CargoRunner, JobCargo, discover_build_state, may_name_build_state,
    tracked_manifest_fingerprint,
};
pub mod bun;
pub mod cache;
pub mod cargo;
mod codegraph;
mod direnv;
pub mod go;
pub mod gradle;
mod installations;
mod javascript;
pub mod nix;
pub mod npm;
pub mod nx;
pub mod pnpm;
pub mod sccache;
pub mod uv;
pub mod zig;

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

#[derive(Clone, Copy)]
pub struct DetectionContext<'a> {
    pub workspace_root: &'a Path,
    pub project_root: &'a Path,
    pub command_cwd: &'a Path,
    pub home: &'a Path,
    /// `[caches] home` from main's `.cowshed.toml`: HOME-relative cache directories the
    /// repository's own tooling places, shared read-write into every sandbox of the project and
    /// linked from the private HOME. Never a workspace's copy of the file.
    pub repository_caches: &'a [PathBuf],
    pub environment_root: &'a Path,
    pub runtime_dir: &'a Path,
    pub trust_bundle: Option<&'a Path>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnvAction {
    Own(OsString),
    Default(OsString),
    Append(String),
    Unset,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GrantScope {
    Literal,
    Subtree,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GrantAccess {
    Read,
    ReadWrite,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CapabilityGrant {
    pub path: PathBuf,
    pub scope: GrantScope,
    pub access: GrantAccess,
}

/// A host cache directory every sandbox of a detecting project reaches where the host keeps it:
/// created by the supervisor before a child runs, granted as a subtree with `access`, and linked
/// from `private_link` inside the private environment when the tool finds it there rather than
/// through a variable. A cache whose entries a host process executes is shared read-only: a
/// sandbox may use what the host fetched, never plant what the host will run.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SharedCache {
    pub path: PathBuf,
    pub private_link: Option<PathBuf>,
    pub access: GrantAccess,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DaemonIsolation {
    pub directories: Vec<PathBuf>,
    pub discard_at_mint: Vec<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShellActivation {
    pub directory: PathBuf,
    pub script: &'static str,
    pub label: &'static str,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BootstrapProgram {
    pub name: &'static str,
    pub target: PathBuf,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapabilityContribution {
    pub env: BTreeMap<&'static str, EnvAction>,
    pub grants: Vec<CapabilityGrant>,
    pub shared_caches: Vec<SharedCache>,
    /// Checkout-relative tool paths and their destinations relative to the build volume.
    pub build_state: Vec<BuildStatePath>,
    pub daemon_isolation: DaemonIsolation,
    pub unix_sockets: Vec<PathBuf>,
    pub bootstrap_programs: Vec<BootstrapProgram>,
    pub shell: Option<ShellActivation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DetectionScope {
    Project,
    CommandAncestors,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarkerKind {
    File,
    Directory,
}

#[derive(Clone, Copy)]
pub enum ReachedConvention {
    TrackedManifest(&'static str),
    ProjectFile(fn(&Path, &Path) -> Result<bool>),
}

pub struct Detector {
    pub id: CapabilityId,
    pub marker_kind: MarkerKind,
    pub scope: DetectionScope,
    /// All of these convention markers must exist.
    pub all: &'static [&'static str],
    /// At least one of these files must exist; an empty list adds no condition.
    pub any: &'static [&'static str],
    pub contribute: fn(&DetectionContext<'_>) -> Result<CapabilityContribution>,
    /// A convention reached through another project file or a tracked nested manifest.
    pub reached_from: Option<ReachedConvention>,
}

impl Detector {
    pub fn detect(&self, context: &DetectionContext<'_>) -> Result<Option<CapabilityContribution>> {
        self.detect_with(
            context,
            &mut build_state_discovery::TrackedManifests::default(),
        )
    }

    fn detect_with(
        &self,
        context: &DetectionContext<'_>,
        tracked: &mut build_state_discovery::TrackedManifests,
    ) -> Result<Option<CapabilityContribution>> {
        if self.all.is_empty() && self.any.is_empty() {
            return Err(CowshedError::internal(
                "a capability detector must name a convention file",
            ));
        }
        if self.scope == DetectionScope::CommandAncestors
            && context.project_root == context.workspace_root
        {
            for directory in context
                .command_cwd
                .ancestors()
                .take_while(|directory| directory.starts_with(context.workspace_root))
            {
                if self.matches(context.workspace_root, directory, tracked)? {
                    let selected = DetectionContext {
                        project_root: directory,
                        ..*context
                    };
                    return (self.contribute)(&selected).map(Some);
                }
            }
            return Ok(None);
        }
        if self.matches(context.workspace_root, context.project_root, tracked)? {
            (self.contribute)(context).map(Some)
        } else {
            Ok(None)
        }
    }

    fn matches(
        &self,
        workspace: &Path,
        directory: &Path,
        tracked: &mut build_state_discovery::TrackedManifests,
    ) -> Result<bool> {
        let present = |path: &Path| match self.marker_kind {
            MarkerKind::File => convention_file(workspace, path),
            MarkerKind::Directory => convention_directory(workspace, path),
        };
        for file in self.all {
            if !present(&directory.join(file))? {
                return Ok(false);
            }
        }
        if self.any.is_empty() {
            return Ok(true);
        }
        for file in self.any {
            if present(&directory.join(file))? {
                return Ok(true);
            }
        }
        match self.reached_from {
            Some(ReachedConvention::TrackedManifest(name)) => {
                tracked.contains(workspace, directory, name)
            }
            Some(ReachedConvention::ProjectFile(reached)) => reached(workspace, directory),
            None => Ok(false),
        }
    }
}

pub static DETECTORS: [&Detector; 13] = [
    &direnv::DETECTOR,
    &nx::DETECTOR,
    &cargo::DETECTOR,
    &go::DETECTOR,
    &bun::DETECTOR,
    &npm::DETECTOR,
    &pnpm::DETECTOR,
    &uv::DETECTOR,
    &zig::DETECTOR,
    &gradle::DETECTOR,
    &nix::DETECTOR,
    &sccache::DETECTOR,
    &codegraph::DETECTOR,
];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DetectedCapabilities {
    pub active: Vec<CapabilityId>,
    pub contribution: CapabilityContribution,
}

pub fn detect_for_workspace(context: &DetectionContext<'_>) -> Result<DetectedCapabilities> {
    let config = workspace_config(context.workspace_root)?;
    detect(context, config.capabilities())
}

fn workspace_config(workspace_root: &Path) -> Result<crate::storage::bootstrap::CowshedConfig> {
    let path = workspace_root.join(".cowshed.toml");
    if convention_file(workspace_root, &path)? {
        let source = fs::read_to_string(&path).map_err(|error| detection_error(&path, error))?;
        crate::storage::bootstrap::parse_cowshed_config(&source).map_err(|error| {
            CowshedError::usage(
                format!("invalid {}: {error}", path.display()),
                "repair the repository cowshed configuration",
            )
        })
    } else {
        Ok(crate::storage::bootstrap::CowshedConfig::default())
    }
}

pub fn detect(
    context: &DetectionContext<'_>,
    overrides: &BTreeMap<CapabilityId, CapabilityOverride>,
) -> Result<DetectedCapabilities> {
    detect_with(
        context,
        overrides,
        &mut build_state_discovery::TrackedManifests::default(),
    )
}

fn detect_with(
    context: &DetectionContext<'_>,
    overrides: &BTreeMap<CapabilityId, CapabilityOverride>,
    tracked: &mut build_state_discovery::TrackedManifests,
) -> Result<DetectedCapabilities> {
    validate_project_directory(context.workspace_root, context.command_cwd)?;
    let mut result = DetectedCapabilities::default();
    let mut owners = BTreeMap::new();
    for detector in DETECTORS {
        let override_ = overrides.get(&detector.id);
        if override_.is_some_and(|override_| override_.disabled) {
            continue;
        }
        let project = override_
            .and_then(|override_| override_.directory.as_ref())
            .map(|directory| context.workspace_root.join(directory));
        if let Some(project) = &project {
            validate_project_directory(context.workspace_root, project)?;
        }
        let selected = DetectionContext {
            project_root: project.as_deref().unwrap_or(context.workspace_root),
            ..*context
        };
        if let Some(contribution) = detector.detect_with(&selected, tracked)? {
            merge(
                &mut result.contribution,
                &mut owners,
                detector.id,
                contribution,
            )?;
            result.active.push(detector.id);
        }
    }
    installations::contribute_linked_program_reads(
        &mut result.contribution,
        &[
            context.environment_root.join("tools/bin"),
            context.workspace_root.join(".cowshed/bin"),
        ],
    )?;
    // A repository-placed cache is found through `$HOME/<path>`: the private HOME links to the
    // host's own directory, so the host and every sandbox reach the same bytes.
    result
        .contribution
        .shared_caches
        .extend(
            context
                .repository_caches
                .iter()
                .map(|relative| SharedCache {
                    path: context.home.join(relative),
                    private_link: Some(context.environment_root.join("home").join(relative)),
                    access: GrantAccess::ReadWrite,
                }),
        );
    normalize(&mut result.contribution);
    Ok(result)
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum ContributionKey {
    Environment(&'static str),
    Program(&'static str),
    CacheTarget(PathBuf),
    Shell,
}

fn merge_build_state(
    output: &mut Vec<BuildStatePath>,
    incoming: Vec<BuildStatePath>,
) -> Result<()> {
    for path in incoming {
        for prior in output.iter() {
            if prior != &path
                && (prior
                    .checkout
                    .as_path()
                    .starts_with(path.checkout.as_path())
                    || path
                        .checkout
                        .as_path()
                        .starts_with(prior.checkout.as_path())
                    || prior.volume.as_path().starts_with(path.volume.as_path())
                    || path.volume.as_path().starts_with(prior.volume.as_path()))
            {
                return Err(CowshedError::conflict(
                    format!(
                        "overlapping capability build-state paths {} -> {} and {} -> {}",
                        prior.checkout.as_path().display(),
                        prior.volume.as_path().display(),
                        path.checkout.as_path().display(),
                        path.volume.as_path().display(),
                    ),
                    "repair the conflicting capability build-state contributions",
                ));
            }
        }
        output.push(path);
    }
    Ok(())
}

fn merge(
    output: &mut CapabilityContribution,
    owners: &mut BTreeMap<ContributionKey, CapabilityId>,
    id: CapabilityId,
    contribution: CapabilityContribution,
) -> Result<()> {
    for (name, action) in contribution.env {
        if reserved(name) {
            return Err(CowshedError::conflict(
                format!(
                    "{} detector contributes reserved core variable {name}",
                    id.name()
                ),
                "report the invalid capability contribution",
            ));
        }
        if let Some(existing) = output.env.get(name) {
            if existing != &action {
                let previous = owners
                    .get(&ContributionKey::Environment(name))
                    .expect("every contributed variable records its owner");
                return Err(CowshedError::conflict(
                    format!(
                        "{} and {} detectors conflict on {name}",
                        previous.name(),
                        id.name()
                    ),
                    "remove the conflicting capability override or repair the detector",
                ));
            }
        } else {
            owners.insert(ContributionKey::Environment(name), id);
            output.env.insert(name, action);
        }
    }
    for cache in &contribution.shared_caches {
        if let Some(target) = &cache.private_link {
            for prior in &output.shared_caches {
                if prior.private_link.as_ref() == Some(target) && prior.path != cache.path {
                    let previous = owners
                        .get(&ContributionKey::CacheTarget(target.clone()))
                        .expect("every cache target records its owner");
                    return Err(CowshedError::conflict(
                        format!(
                            "{} and {} detectors conflict on cache link {}",
                            previous.name(),
                            id.name(),
                            target.display()
                        ),
                        "repair the conflicting cache contribution",
                    ));
                }
            }
            owners
                .entry(ContributionKey::CacheTarget(target.clone()))
                .or_insert(id);
        }
    }
    if let Some(shell) = contribution.shell {
        if output
            .shell
            .as_ref()
            .is_some_and(|existing| existing != &shell)
        {
            let previous = owners
                .get(&ContributionKey::Shell)
                .expect("every shell records its owner");
            return Err(CowshedError::conflict(
                format!(
                    "{} and {} detectors select different shell activations",
                    previous.name(),
                    id.name()
                ),
                "repair shell capability configuration",
            ));
        }
        owners.entry(ContributionKey::Shell).or_insert(id);
        output.shell = Some(shell);
    }
    output.grants.extend(contribution.grants);
    output.shared_caches.extend(contribution.shared_caches);
    merge_build_state(&mut output.build_state, contribution.build_state)?;
    output
        .daemon_isolation
        .directories
        .extend(contribution.daemon_isolation.directories);
    output
        .daemon_isolation
        .discard_at_mint
        .extend(contribution.daemon_isolation.discard_at_mint);
    output.unix_sockets.extend(contribution.unix_sockets);
    for program in contribution.bootstrap_programs {
        if let Some(previous) = output
            .bootstrap_programs
            .iter()
            .find(|prior| prior.name == program.name)
        {
            if previous.target != program.target {
                let previous = owners
                    .get(&ContributionKey::Program(program.name))
                    .expect("every program records its owner");
                return Err(CowshedError::conflict(
                    format!(
                        "{} and {} detectors conflict on executable {}",
                        previous.name(),
                        id.name(),
                        program.name
                    ),
                    "repair the conflicting tool installation",
                ));
            }
        } else {
            owners.insert(ContributionKey::Program(program.name), id);
            output.bootstrap_programs.push(program);
        }
    }
    Ok(())
}

fn normalize(contribution: &mut CapabilityContribution) {
    contribution.grants.sort();
    contribution.grants.dedup();
    let mut index = 0;
    while index < contribution.grants.len() {
        let grant = &contribution.grants[index];
        let covered = grant.access == GrantAccess::Read
            && contribution.grants.iter().any(|other| {
                other.access == GrantAccess::ReadWrite
                    && ((other.scope == GrantScope::Subtree && grant.path.starts_with(&other.path))
                        || (other.path == grant.path && grant.scope == GrantScope::Literal))
            });
        if covered {
            contribution.grants.remove(index);
        } else {
            index += 1;
        }
    }
    contribution.shared_caches.sort();
    contribution.shared_caches.dedup();
    contribution.build_state.sort();
    contribution.build_state.dedup();
    contribution.daemon_isolation.directories.sort();
    contribution.daemon_isolation.directories.dedup();
    contribution.daemon_isolation.discard_at_mint.sort();
    contribution.daemon_isolation.discard_at_mint.dedup();
    contribution.unix_sockets.sort();
    contribution.unix_sockets.dedup();
    contribution.bootstrap_programs.sort();
    contribution.bootstrap_programs.dedup();
}

fn reserved(name: &str) -> bool {
    matches!(
        name,
        "HOME"
            | "PATH"
            | "TMPDIR"
            | "XDG_CONFIG_HOME"
            | "XDG_CACHE_HOME"
            | "XDG_DATA_HOME"
            | "XDG_STATE_HOME"
            | "XDG_RUNTIME_DIR"
            | "COWSHED_WORKSPACE_TOKEN"
            | "COWSHED_PORT_BASE"
            | "COWSHED_PORT_BLOCK_SIZE"
            | "HTTP_PROXY"
            | "HTTPS_PROXY"
            | "http_proxy"
            | "https_proxy"
            | "NO_PROXY"
            | "no_proxy"
            | "GIT_CONFIG_GLOBAL"
            | "GIT_CONFIG_NOSYSTEM"
            | "GIT_ATTR_NOSYSTEM"
            | "GIT_CONFIG_COUNT"
    ) || name.starts_with("GIT_CONFIG_KEY_")
        || name.starts_with("GIT_CONFIG_VALUE_")
}

pub fn convention_file(workspace: &Path, path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            let resolved = fs::canonicalize(path).map_err(|error| detection_error(path, error))?;
            if !resolved.starts_with(workspace) {
                return Err(CowshedError::sandbox_denied(
                    format!(
                        "capability convention {} escapes workspace {}",
                        path.display(),
                        workspace.display()
                    ),
                    "keep project convention files inside the workspace",
                ));
            }
            let metadata = fs::metadata(&resolved).map_err(|error| detection_error(path, error))?;
            if !metadata.is_file() {
                return Err(CowshedError::integrity(
                    format!("capability convention is not a file: {}", path.display()),
                    "replace the convention path with a regular project file",
                ));
            }
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(detection_error(path, error)),
    }
}

fn convention_directory(workspace: &Path, path: &Path) -> Result<bool> {
    let parent = path.parent().ok_or_else(|| {
        CowshedError::integrity(
            "directory marker has no parent",
            "repair the capability marker",
        )
    })?;
    validate_project_directory(workspace, parent)?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(true),
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let target = fs::read_link(path).map_err(|error| detection_error(path, error))?;
            let relative_parent = parent
                .strip_prefix(workspace)
                .expect("parent was contained");
            let resolved =
                crate::inherited_links::resolve_in_source(workspace, relative_parent, &target);
            if resolved.starts_with(workspace.join(".cowshed/build")) {
                Ok(true)
            } else {
                Err(CowshedError::sandbox_denied(
                    format!(
                        "capability directory marker {} is not linked through .cowshed/build",
                        path.display()
                    ),
                    "keep index state in the checkout's build volume",
                ))
            }
        }
        Ok(_) => Err(CowshedError::integrity(
            format!(
                "capability convention is not a directory: {}",
                path.display()
            ),
            "replace the marker with the indexer's directory",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(detection_error(path, error)),
    }
}

fn validate_project_directory(workspace: &Path, project: &Path) -> Result<()> {
    if project
        .components()
        .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(CowshedError::usage(
            "capability directory is not normalized",
            "use a normalized workspace-relative capability directory",
        ));
    }
    if !project.starts_with(workspace) {
        return Err(CowshedError::usage(
            "capability directory escapes the workspace",
            "use a workspace-relative capability directory",
        ));
    }
    match fs::canonicalize(project) {
        Ok(resolved) if resolved.starts_with(workspace) => Ok(()),
        Ok(_) => Err(CowshedError::sandbox_denied(
            format!(
                "capability directory {} escapes workspace",
                project.display()
            ),
            "keep capability directories inside the workspace",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(detection_error(project, error)),
    }
}

fn detection_error(path: &Path, error: io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!(
            "cannot inspect project capability at {}: {error}",
            path.display()
        ),
        "repair the project convention file and retry",
    )
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

/// Share `tool`'s cache directories where the host keeps them and point a child at that path.
///
/// The cache directories are read-write subtrees the supervisor creates; a split tool's root is a
/// literal read and its root state files read-write literals, so configuration, credentials and
/// binaries beside the caches stay under the HOME read deny. The variable is owned: a caller's own
/// value would name a second cache, or another checkout's.
pub fn add_shared_tool_home(
    contribution: &mut CapabilityContribution,
    home: &Path,
    tool: &'static SharedToolHome,
) {
    let host = tool.host_path(home);
    if let Some(variable) = tool.variable {
        contribution
            .env
            .insert(variable, EnvAction::Own(host.clone().into_os_string()));
    }
    if let SharedLayout::Split { state_files, .. } = tool.layout {
        contribution.grants.push(CapabilityGrant {
            path: host.clone(),
            scope: GrantScope::Literal,
            access: GrantAccess::Read,
        });
        contribution
            .grants
            .extend(state_files.iter().map(|file| CapabilityGrant {
                path: host.join(file),
                scope: GrantScope::Literal,
                access: GrantAccess::ReadWrite,
            }));
    }
    contribution
        .shared_caches
        .extend(
            tool.cache_directories(home)
                .into_iter()
                .map(|path| SharedCache {
                    path,
                    private_link: None,
                    access: GrantAccess::ReadWrite,
                }),
        );
}

/// Find an individual installed program without inheriting the supervisor's PATH.
pub fn bootstrap_program(name: &str, candidates: &[PathBuf]) -> Result<Option<PathBuf>> {
    for directory in candidates {
        let program = directory.join(name);
        let resolved = match fs::canonicalize(&program) {
            Ok(resolved) => resolved,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(detection_error(&program, error)),
        };
        let metadata =
            fs::metadata(&resolved).map_err(|error| detection_error(&resolved, error))?;
        if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
            return Ok(Some(resolved));
        }
    }
    Ok(None)
}

/// The kernel's bound on symbolic links one lookup follows; the next one is `ELOOP`. Linux:
/// `MAXSYMLINKS` in `include/linux/namei.h`, "up to 40 symbolic links" in path_resolution(7).
#[cfg(target_os = "linux")]
const MAX_FOLLOWED_LINKS: usize = 40;
/// The kernel's bound on symbolic links one lookup follows; the next one is `ELOOP`. macOS:
/// XNU's `MAXSYMLINKS` in `bsd/sys/param.h`. `libc` exports this constant for neither kernel.
#[cfg(not(target_os = "linux"))]
const MAX_FOLLOWED_LINKS: usize = 32;

/// What a lookup of `path` visits beneath `home` that its own ancestors do not name: each
/// symbolic link it follows there, and — once it has followed one — where it ends there, the
/// file it resolves to or the first component that does not exist. The walk resolves as the
/// kernel does (a relative target against the link's directory, `..` against the physical
/// parent) and keeps resolving outside HOME, recording nothing there, so a link outside that
/// leads back beneath HOME has its HOME links recorded on re-entry.
///
/// Seatbelt checks every link a lookup follows against the link's own path, so a link beneath
/// HOME the profile does not name refuses the whole lookup with `EPERM`: `~/.nix-profile` alone
/// does not resolve while `~/.local/state/nix/profiles/profile` and its generation link behind
/// it stay denied.
fn home_lookup_trail(home: &Path, path: &Path) -> Result<Vec<PathBuf>> {
    let mut trail = Vec::new();
    let mut resolved = PathBuf::new();
    let mut pending = path.to_path_buf();
    let mut followed = 0;
    loop {
        let mut components = pending.components();
        let Some(component) = components.next() else {
            break;
        };
        let rest = components.as_path().to_path_buf();
        match component {
            Component::RootDir => resolved = PathBuf::from("/"),
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Prefix(_) | Component::CurDir => {}
            Component::Normal(name) => {
                let next = resolved.join(name);
                match fs::symlink_metadata(&next) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        followed += 1;
                        if followed > MAX_FOLLOWED_LINKS {
                            return Err(detection_error(
                                path,
                                io::Error::from_raw_os_error(libc::ELOOP),
                            ));
                        }
                        let target =
                            fs::read_link(&next).map_err(|error| detection_error(&next, error))?;
                        if next.starts_with(home) {
                            trail.push(next);
                        }
                        pending = target.join(rest);
                        continue;
                    }
                    Ok(_) => resolved = next,
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                        ) =>
                    {
                        resolved = next;
                        break;
                    }
                    Err(error) => return Err(detection_error(&next, error)),
                }
            }
        }
        pending = rest;
    }
    if followed > 0 && resolved.starts_with(home) && resolved != home {
        trail.push(resolved);
    }
    Ok(trail)
}

/// Bootstrap `name` from the first of `directories` that installs it.
///
/// The supervisor repeats this search inside its own sandbox, beneath the HOME-wide read deny,
/// so every candidate beneath HOME is granted as the literal program path it probes, with the
/// trail its lookup follows there ([`home_lookup_trail`]): each link, as a literal, and where
/// the lookup ends. The sandboxed search then sees exactly what the host's saw — the program,
/// the links it resolves through, or its absence — and no HOME directory becomes listable.
pub fn add_bootstrap(
    contribution: &mut CapabilityContribution,
    context: &DetectionContext<'_>,
    name: &'static str,
    directories: &[PathBuf],
) -> Result<()> {
    for candidate in directories
        .iter()
        .map(|directory| directory.join(name))
        .filter(|candidate| candidate.starts_with(context.home))
    {
        let trail = home_lookup_trail(context.home, &candidate)?;
        contribution
            .grants
            .extend(
                std::iter::once(candidate)
                    .chain(trail)
                    .map(|path| CapabilityGrant {
                        path,
                        scope: GrantScope::Literal,
                        access: GrantAccess::Read,
                    }),
            );
    }
    if let Some(target) = bootstrap_program(name, directories)? {
        contribution.grants.push(CapabilityGrant {
            path: target.clone(),
            scope: GrantScope::Literal,
            access: GrantAccess::Read,
        });
        installations::contribute_reads(contribution, &target);
        contribution
            .bootstrap_programs
            .push(BootstrapProgram { name, target });
    }
    Ok(())
}

/// Conventional installation directories are searched for individual tools, never inherited whole.
pub fn host_program_directories(context: &DetectionContext<'_>) -> Vec<PathBuf> {
    let mut directories = vec![
        context.home.join(".nix-profile/bin"),
        context.home.join(".local/state/nix/profile/bin"),
    ];
    if let Some(user) = effective_user_name() {
        directories.push(Path::new("/etc/profiles/per-user").join(&user).join("bin"));
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

fn effective_user_name() -> Option<OsString> {
    use std::os::unix::ffi::OsStringExt as _;
    let mut entry = std::mem::MaybeUninit::<libc::passwd>::zeroed();
    let mut buffer = vec![0_u8; 4096];
    let mut found = std::ptr::null_mut();
    // SAFETY: entry and buffer are live writable storage; a successful result points into them.
    let status = unsafe {
        libc::getpwuid_r(
            libc::geteuid(),
            entry.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut found,
        )
    };
    if status != 0 || found.is_null() {
        return None;
    }
    // SAFETY: a successful getpwuid_r returns a NUL-terminated pw_name within buffer.
    let name = unsafe { std::ffi::CStr::from_ptr((*found).pw_name) };
    Some(OsString::from_vec(name.to_bytes().to_vec()))
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    pub struct Fixture {
        pub root: PathBuf,
        pub home: PathBuf,
        pub environment: PathBuf,
        pub runtime: PathBuf,
    }
    impl Fixture {
        pub fn new() -> Self {
            let root = Path::new("/tmp").join(format!("cs-cap-{}", uuid::Uuid::new_v4().simple()));
            fs::create_dir_all(&root).unwrap();
            let root = root.canonicalize().unwrap();
            let home = root.join("host-home");
            let environment = root.join(".cowshed");
            let runtime = environment.join("run");
            fs::create_dir_all(&home).unwrap();
            Self {
                root,
                home,
                environment,
                runtime,
            }
        }
        pub fn context(&self) -> DetectionContext<'_> {
            DetectionContext {
                workspace_root: &self.root,
                project_root: &self.root,
                command_cwd: &self.root,
                home: &self.home,
                repository_caches: &[],
                environment_root: &self.environment,
                runtime_dir: &self.runtime,
                trust_bundle: None,
            }
        }
        pub fn files(&self, files: &[&str]) {
            for file in files {
                let path = self.root.join(file);
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, "").unwrap();
            }
        }
    }
    impl Default for Fixture {
        fn default() -> Self {
            Self::new()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.root) {
                eprintln!(
                    "capability fixture {} was not removed: {error}",
                    self.root.display()
                );
            }
        }
    }
    pub fn assert_switch(detector: &Detector, files: &[&str]) {
        let fixture = Fixture::new();
        assert!(detector.detect(&fixture.context()).unwrap().is_none());
        fixture.files(files);
        assert!(detector.detect(&fixture.context()).unwrap().is_some());
        for file in files {
            fs::remove_file(fixture.root.join(file)).unwrap();
        }
        assert!(detector.detect(&fixture.context()).unwrap().is_none());
    }
}

pub fn mint_daemon_states(workspace: &Path, home: &Path) -> Result<Vec<PathBuf>> {
    let private = workspace.join(".cowshed");
    let runtime = private.join("run");
    let context = DetectionContext {
        workspace_root: workspace,
        project_root: workspace,
        command_cwd: workspace,
        home,
        repository_caches: &[],
        environment_root: &private,
        runtime_dir: &runtime,
        trust_bundle: None,
    };
    Ok(detect_for_workspace(&context)?
        .contribution
        .daemon_isolation
        .discard_at_mint)
}

#[cfg(test)]
mod tests;
