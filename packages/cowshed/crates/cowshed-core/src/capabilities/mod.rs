//! Convention-gated project tooling. The sandbox consumes contributions, never tool names.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use crate::{CowshedError, Result};
use cache::{HostCache, SharedLayout, SharedToolHome};

pub mod cache;
mod installations;
mod javascript;
mod direnv;
pub mod nx;
pub mod cargo;
pub mod go;
mod bun;
mod npm;
mod pnpm;
mod uv;
mod zig;
mod gradle;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|id| id.name() == name)
    }

    pub const ALL: [Self; 12] = [
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
    pub caches_root: &'a Path,
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CacheMount {
    pub source: PathBuf,
    pub private_target: Option<PathBuf>,
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
    pub cache_mounts: Vec<CacheMount>,
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

pub struct Detector {
    pub id: CapabilityId,
    pub scope: DetectionScope,
    /// All of these files must exist.
    pub all: &'static [&'static str],
    /// At least one of these files must exist; an empty list adds no condition.
    pub any: &'static [&'static str],
    pub contribute: fn(&DetectionContext<'_>) -> Result<CapabilityContribution>,
    pub host_cache_homes: &'static [&'static SharedToolHome],
}

impl Detector {
    pub fn detect(&self, context: &DetectionContext<'_>) -> Result<Option<CapabilityContribution>> {
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
                if self.matches(context.workspace_root, directory)? {
                    let selected = DetectionContext {
                        project_root: directory,
                        ..*context
                    };
                    return (self.contribute)(&selected).map(Some);
                }
            }
            return Ok(None);
        }
        if self.matches(context.workspace_root, context.project_root)? {
            (self.contribute)(context).map(Some)
        } else {
            Ok(None)
        }
    }

    fn matches(&self, workspace: &Path, directory: &Path) -> Result<bool> {
        for file in self.all {
            if !convention_file(workspace, &directory.join(file))? {
                return Ok(false);
            }
        }
        if self.any.is_empty() {
            return Ok(true);
        }
        for file in self.any {
            if convention_file(workspace, &directory.join(file))? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub static DETECTORS: [&Detector; 10] = [&direnv::DETECTOR, &nx::DETECTOR, &cargo::DETECTOR, &go::DETECTOR, &bun::DETECTOR, &npm::DETECTOR, &pnpm::DETECTOR, &uv::DETECTOR, &zig::DETECTOR, &gradle::DETECTOR];

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DetectedCapabilities {
    pub active: Vec<CapabilityId>,
    pub contribution: CapabilityContribution,
}

pub fn detect_for_workspace(context: &DetectionContext<'_>) -> Result<DetectedCapabilities> {
    let path = context.workspace_root.join(".cowshed.toml");
    let config = if convention_file(context.workspace_root, &path)? {
        let source = fs::read_to_string(&path).map_err(|error| detection_error(&path, error))?;
        crate::storage::bootstrap::parse_cowshed_config(&source).map_err(|error| {
            CowshedError::usage(
                format!("invalid {}: {error}", path.display()),
                "repair the repository cowshed configuration",
            )
        })?
    } else {
        crate::storage::bootstrap::CowshedConfig::default()
    };
    detect(context, config.capabilities())
}

pub fn detect(
    context: &DetectionContext<'_>,
    overrides: &BTreeMap<CapabilityId, CapabilityOverride>,
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
        if let Some(contribution) = detector.detect(&selected)? {
            merge(
                &mut result.contribution,
                &mut owners,
                detector.id,
                contribution,
            )?;
            result.active.push(detector.id);
        }
    }
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
    for mount in &contribution.cache_mounts {
        if let Some(target) = &mount.private_target {
            for prior in &output.cache_mounts {
                if prior.private_target.as_ref() == Some(target) && prior.source != mount.source {
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
    output.cache_mounts.extend(contribution.cache_mounts);
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
    contribution.cache_mounts.sort();
    contribution.cache_mounts.dedup();
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

/// Reuse the host's exact cache spelling only after its links reach the shared cache volume.
pub fn shared_tool_contribution(
    context: &DetectionContext<'_>,
    tool: &'static SharedToolHome,
) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    let links = tool.links(context.home, context.caches_root);
    let shared = links.iter().all(HostCache::is_shared);
    let host = tool.host_path(context.home);
    if let Some(variable) = tool.variable {
        contribution.env.insert(
            variable,
            if shared {
                EnvAction::Own(host.clone().into_os_string())
            } else {
                EnvAction::Unset
            },
        );
    }
    if context.caches_root.is_dir() {
        contribution
            .cache_mounts
            .extend(links.iter().map(|link| CacheMount {
                source: link.shared.clone(),
                private_target: None,
            }));
    }
    if shared {
        contribution.grants.push(CapabilityGrant {
            path: host.clone(),
            scope: GrantScope::Literal,
            access: GrantAccess::Read,
        });
        if let SharedLayout::Split { links, state_files } = tool.layout {
            contribution
                .grants
                .extend(links.iter().map(|(child, _)| CapabilityGrant {
                    path: host.join(child),
                    scope: GrantScope::Literal,
                    access: GrantAccess::Read,
                }));
            contribution
                .grants
                .extend(state_files.iter().map(|file| CapabilityGrant {
                    path: host.join(file),
                    scope: GrantScope::Literal,
                    access: GrantAccess::ReadWrite,
                }));
        }
    } else if tool.linked_from_checkouts {
        contribution.grants.push(CapabilityGrant {
            path: host,
            scope: GrantScope::Subtree,
            access: GrantAccess::Read,
        });
    }
    Ok(contribution)
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

pub fn add_bootstrap(
    contribution: &mut CapabilityContribution,
    name: &'static str,
    directories: &[PathBuf],
) -> Result<()> {
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
        pub caches: PathBuf,
        pub environment: PathBuf,
        pub runtime: PathBuf,
    }
    impl Fixture {
        pub fn new() -> Self {
            let root = Path::new("/tmp").join(format!("cs-cap-{}", uuid::Uuid::new_v4().simple()));
            fs::create_dir_all(&root).unwrap();
            let root = root.canonicalize().unwrap();
            let home = root.join("host-home");
            let caches = root.join("host-caches");
            let environment = root.join(".cowshed");
            let runtime = environment.join("run");
            for path in [&home, &caches] {
                fs::create_dir_all(path).unwrap();
            }
            Self {
                root,
                home,
                caches,
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
                caches_root: &self.caches,
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
        caches_root: Path::new(crate::storage::bootstrap::CACHES_ROOT),
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
