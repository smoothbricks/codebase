use std::borrow::Cow;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::capabilities::{
    CapabilityGrant, DetectedCapabilities, DetectionContext, GrantAccess, GrantScope,
};
pub use crate::metadata::PortBlock;
use cowshed_gateway_types::STORE_ROOT;

fn cowshed_root() -> &'static Path {
    Path::new(STORE_ROOT)
        .parent()
        .expect("STORE_ROOT is a child of the machine-global cowshed root")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EgressGrant {
    pub host: String,
    pub ports: Vec<u16>,
}

/// Grant snapshot inputs. Egress is enforced by the gateway, not by Seatbelt.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SandboxGrants {
    pub read: Vec<PathBuf>,
    pub write: Vec<PathBuf>,
    /// Workspace-relative paths no job may write.
    pub deny_write: Vec<PathBuf>,
    /// Workspace-relative paths no job may read or write.
    pub deny: Vec<PathBuf>,
    pub egress: Vec<EgressGrant>,
    /// Outbound loopback TCP to every port, not only to the held blocks' literal ports.
    pub loopback_any: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunSandboxMode {
    ReadOnly,
    ReadWrite,
}

/// The authority tier receiving a generated Seatbelt profile.
///
/// An executed child is always a strict, immutable narrowing of the trusted
/// supervisor profile generated from the same configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxProfileRole {
    TrustedSupervisor,
    ExecutedChild,
    /// Controller-invoked Git directory discovery needs only system roots and explicit
    /// readable trees, not the child toolchain's ambient file-read-data permission.
    GitDiscovery,
    /// [`Self::GitDiscovery`] for controller Git that reads files outside its workspace which
    /// the workspace's own jobs cannot: it may execute only Git's own executables, so no program
    /// the repository's config selects runs with that read (02_workspaces.md, "Git-worktree
    /// workspaces").
    GitDiscoveryGitOnly,
}

/// A symlink beside the workspace mount — `<mount_root>/<org>/<project>/<name>` — that an
/// operator planted so a relative dependency (`../<name>`) resolves from every workspace of the
/// project the same way it does beside the canonical checkout.
///
/// The link lives inside the mount-root deny, and Seatbelt must read the link itself before it
/// can follow it to the target. A grant on the target therefore also needs the link readable, or
/// the dependency stays denied through the only spelling the workspace reaches it by.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShedLink {
    pub link: PathBuf,
    /// Absolute, lexically canonical link target.
    pub target: PathBuf,
}

/// The symlinks beside `workspace_mount`, with relative targets resolved against that directory.
///
/// A directory that does not exist yet is an empty shed, not an error: the profile is built for
/// a workspace whose mount may be created after the config is.
pub fn shed_links(workspace_mount: &Path) -> Result<Vec<ShedLink>, SandboxError> {
    let Some(parent) = workspace_mount.parent() else {
        return Ok(Vec::new());
    };
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => {
            return Err(SandboxError::InvalidPath {
                path: parent.to_path_buf(),
                reason: "cannot enumerate the shed directory beside the workspace",
            });
        }
    };
    let mut links = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let link = entry.path();
        let Ok(raw_target) = std::fs::read_link(&link) else {
            continue;
        };
        let joined = if raw_target.is_absolute() {
            raw_target
        } else {
            parent.join(raw_target)
        };
        let Some(target) = canonical_lexical_absolute(&joined) else {
            return Err(SandboxError::InvalidPath {
                path: joined,
                reason: "shed link target traverses above /",
            });
        };
        links.push(ShedLink { link, target });
    }
    links.sort_by(|left, right| left.link.cmp(&right.link));
    Ok(links)
}

/// Resolve `.` and `..` lexically; `None` for a relative path or one that climbs above `/`.
pub fn canonical_lexical_absolute(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::RootDir => normalized.push(Path::new("/")),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            std::path::Component::Normal(component) => normalized.push(component),
            std::path::Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxConfig {
    pub home: PathBuf,
    /// Host-configured root containing every project workspace mount tree.
    pub mount_root: PathBuf,
    pub workspace_mount: PathBuf,
    /// Operator-planted symlinks beside the workspace mount; see [`ShedLink`].
    pub shed_links: Vec<ShedLink>,
    pub exec_temp_dir: PathBuf,
    /// The store the launcher serves: the host's in production, a fixture's own in a test. Launches
    /// keep their profile file in its `run/profiles` ([`Self::profile_directory`]), and host-side
    /// preparation reads only its bindings (04_sandbox.md, "Exec pipeline" step 4; 08_testing.md,
    /// "Fixtures").
    pub store_root: PathBuf,

    /// The workspace's CPU pool directory, where its jobs' jobserver fifos live. The sandbox opens
    /// it for FIFOs alone: no socket or gateway is reached through it. None when the sandbox has
    /// no pool directory.
    pub cpu_pool_directory: Option<PathBuf>,
    /// The block the workspace's environment names (`COWSHED_PORT_BASE`/`_SIZE`) and its
    /// gateway listens on.
    pub port_block: PortBlock,
    /// Blocks the workspace was relocated away from, still reserved to it until they retire:
    /// a background process a finished job left behind may still listen on one, and the
    /// workspace's own children keep reaching it. Outbound loopback TCP is admitted to these as
    /// to `port_block`; nothing else derives from them.
    pub retained_port_blocks: Vec<PortBlock>,
    pub mode: RunSandboxMode,
    pub grants: SandboxGrants,
    /// Canonical, controller-selected sockets only (for example, the Nix daemon).
    pub allowed_unix_sockets: Vec<PathBuf>,
    /// Monotonic effective denies supplied by trusted/operator/repository policy.
    pub additional_denies: Vec<PathBuf>,
    /// The `.git` directory of main's canonical mount, for a git-worktree workspace only.
    ///
    /// This is the mode's stated hole in the isolation: the workspace's repository *is* main's, so
    /// a committing agent reads and writes main's object store and its own
    /// `worktrees/<ws>` administrative directory. It cannot ride the ordinary read/write grants —
    /// those are refused outright when they intersect a protected path, and under the symlink
    /// layout main's mount is inside cowshed's own store while under direct mount it is the
    /// project root that policy denies. It is therefore a distinct, controller-only field, carried
    /// by exactly the workspaces that asked for `--git-worktree` and never implied by the
    /// baseline.
    pub git_worktree_repository: Option<PathBuf>,
    /// The physical build volume selected by the controller at this job's admission.
    ///
    /// Build state stays writable even when source is read-only. Never derive this authority
    /// from a repository-controlled symlink while rendering policy; the controller resolves
    /// the checkout's current owned volume through its validated project layout.
    pub build_volume_mount: Option<PathBuf>,
    /// `[caches] home` from main's `.cowshed.toml`: HOME-relative directories the repository's
    /// own tooling caches into, shared read-write into every sandbox of the project. Never a
    /// workspace's copy of the file.
    pub repository_caches: Vec<PathBuf>,
    /// The host program directories capability detection searches for each bootstrap tool
    /// ([`DetectionContext::program_directories`]): the real host's
    /// ([`crate::capabilities::host_program_directories`]) in production, a fixture's own in a
    /// test.
    pub program_directories: Vec<PathBuf>,
    /// The project capabilities detected in this workspace for this mode
    /// ([`SandboxConfig::configure_capabilities`]): every tool-specific grant, shared cache,
    /// socket and environment entry a child receives comes from here, never from a tool table.
    pub capabilities: DetectedCapabilities,
}

impl SandboxConfig {
    /// Where launches under this sandbox keep their profile file: the `run/profiles` directory of
    /// the store the launcher serves.
    pub fn profile_directory(&self) -> PathBuf {
        crate::exec::profile_directory(&self.store_root)
    }

    /// [`Self::configure_capabilities_for`] a command at the workspace root: the snapshot a
    /// supervisor starts with.
    pub fn configure_capabilities(&mut self) -> crate::Result<()> {
        let root = self.workspace_mount.clone();
        self.configure_capabilities_for(&root)
    }

    /// [`Self::detect_capabilities_for`] a command in `command_cwd`, adopted by this sandbox.
    pub fn configure_capabilities_for(&mut self, command_cwd: &Path) -> crate::Result<()> {
        let capabilities = self.detect_capabilities_for(command_cwd)?;
        self.allowed_unix_sockets = capabilities.contribution.unix_sockets.clone();
        self.capabilities = capabilities;
        Ok(())
    }

    /// This sandbox with `capabilities` in place of its own: every other field as it is, and the
    /// sockets the new contribution admits.
    pub fn with_capabilities(&self, capabilities: DetectedCapabilities) -> Self {
        Self {
            home: self.home.clone(),
            mount_root: self.mount_root.clone(),
            workspace_mount: self.workspace_mount.clone(),
            shed_links: self.shed_links.clone(),
            exec_temp_dir: self.exec_temp_dir.clone(),
            store_root: self.store_root.clone(),
            cpu_pool_directory: self.cpu_pool_directory.clone(),
            port_block: self.port_block,
            retained_port_blocks: self.retained_port_blocks.clone(),
            mode: self.mode,
            grants: self.grants.clone(),
            allowed_unix_sockets: capabilities.contribution.unix_sockets.clone(),
            additional_denies: self.additional_denies.clone(),
            git_worktree_repository: self.git_worktree_repository.clone(),
            build_volume_mount: self.build_volume_mount.clone(),
            repository_caches: self.repository_caches.clone(),
            program_directories: self.program_directories.clone(),
            capabilities,
        }
    }

    /// The workspace's project capabilities for this sandbox's mode, for a command in the
    /// contained `command_cwd`. Project detectors read the project root whatever the cwd; only a
    /// command-scoped convention (a shell hook nearest the cwd) depends on it. The private
    /// environment root and generic runtime remain mode-private; build state and the contributed
    /// Nx daemon/socket leaf belong to the checkout and are shared across job modes.
    pub fn detect_capabilities_for(
        &self,
        command_cwd: &Path,
    ) -> crate::Result<DetectedCapabilities> {
        self.with_detection_context(command_cwd, crate::capabilities::detect_for_workspace)
    }

    /// Run controller discovery against the exact same inputs as capability admission.
    pub fn with_detection_context<T>(
        &self,
        command_cwd: &Path,
        discover: impl FnOnce(&DetectionContext<'_>) -> crate::Result<T>,
    ) -> crate::Result<T> {
        if !command_cwd.starts_with(&self.workspace_mount) {
            return Err(crate::CowshedError::sandbox_denied(
                format!(
                    "command directory {} is outside workspace {}",
                    command_cwd.display(),
                    self.workspace_mount.display()
                ),
                "run the command from inside the workspace",
            ));
        }
        let environment_root = match self.mode {
            RunSandboxMode::ReadOnly => self.exec_temp_dir.clone(),
            RunSandboxMode::ReadWrite => self.workspace_mount.join(".cowshed"),
        };
        let runtime_dir = shared_daemon_runtime_link(self);
        let build_cache = crate::storage::bootstrap::native::provisioned_build_cache();
        let workspace_ca = self
            .workspace_mount
            .join(crate::workspace_credentials::CA_CERTIFICATE_PATH);
        let trust_bundle = environment_root.join(
            crate::workspace_clients::TRUST_BUNDLE_NAME
                .to_str()
                .expect("the bundle name is ASCII"),
        );
        let has_workspace_ca = match std::fs::symlink_metadata(&workspace_ca) {
            Ok(_) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(crate::CowshedError::integrity(
                    format!(
                        "cannot inspect workspace CA {}: {error}",
                        workspace_ca.display()
                    ),
                    "reattach the workspace to mint fresh credentials",
                ));
            }
        };
        let context = DetectionContext {
            workspace_root: &self.workspace_mount,
            project_root: &self.workspace_mount,
            home: &self.home,
            command_cwd,
            repository_caches: &self.repository_caches,
            environment_root: &environment_root,
            runtime_dir: &runtime_dir,
            trust_bundle: has_workspace_ca.then_some(trust_bundle.as_path()),
            program_directories: &self.program_directories,
            build_cache: build_cache.as_deref(),
        };
        discover(&context)
    }
}

/// One workspace as its sandbox sees it: the inputs of [`workspace_sandbox`].
pub struct WorkspaceSandbox<'a> {
    /// The host HOME; the sandbox HOME and the shared tool homes derive from it.
    pub home: &'a Path,
    /// The project's host mount root, which holds every workspace mount.
    pub mount_root: &'a Path,
    /// The project root, which no workspace process may read.
    pub project_root: &'a Path,
    /// Main's canonical mount: the operator's own checkout, which no other workspace may read.
    pub main_mount: &'a Path,
    /// The telemetry root, which no workspace process may read.
    pub telemetry_root: &'a Path,
    /// The workspace's effective grants: its own and the project's.
    pub grants: &'a crate::metadata::GrantSet,
    /// `[sandbox] deny` from main's `.cowshed.toml`: workspace-relative paths the operator's
    /// own checkout declares no job may read or write. Never a workspace's copy of the file.
    pub repository_deny: &'a [PathBuf],
    /// `[caches] home` from main's `.cowshed.toml`, trusted exactly like `repository_deny`.
    pub repository_caches: &'a [PathBuf],
    /// The host's program directories, where detection resolves each bootstrap tool: a production
    /// caller passes [`crate::capabilities::host_program_directories`] of the real host.
    pub program_directories: &'a [PathBuf],
    /// The `.git` of main's mount, for a git-worktree workspace only.
    pub git_worktree_repository: Option<PathBuf>,
    /// The checkout's current owned build-volume mount, resolved by the controller.
    pub build_volume_mount: Option<PathBuf>,
    pub workspace_mount: PathBuf,
    /// The workspace's `TMPDIR`: [`crate::storage::StorageLayout::exec_temp_dir`].
    pub exec_temp_dir: PathBuf,
    /// The store the workspace's supervisor serves, whose `run/profiles` its launches read.
    pub store_root: &'a Path,
    pub workspace_incarnation: &'a crate::metadata::WorkspaceIncarnation,
}

/// The sandbox a workspace's supervisor runs in and hands `plan_exec` for every child it
/// executes — the one builder, so the policy a caller inspects is the policy that runs. A deny,
/// socket or grant field added anywhere else would be a silent sandbox-policy fork.
pub fn workspace_sandbox(workspace: WorkspaceSandbox<'_>) -> crate::Result<SandboxConfig> {
    use crate::CowshedError;
    let WorkspaceSandbox {
        home,
        mount_root,
        project_root,
        main_mount,
        telemetry_root,
        grants,
        repository_deny,
        repository_caches,
        program_directories,
        git_worktree_repository,
        build_volume_mount,
        workspace_mount: mount,
        exec_temp_dir,
        store_root,
        workspace_incarnation,
    } = workspace;
    // Main's checkout lives at the operator's own path, outside the mount root, so the
    // sibling-mount deny never reaches it: it is denied by name, exactly like a sibling. Main
    // itself runs in its own mount and keeps it.
    let mut additional_denies = vec![project_root.to_path_buf(), telemetry_root.to_path_buf()];
    if mount != main_mount {
        additional_denies.push(main_mount.to_path_buf());
    }
    let mut deny: Vec<PathBuf> = grants.deny.iter().chain(repository_deny).cloned().collect();
    deny.sort();
    deny.dedup();
    let mut config = SandboxConfig {
        home: home.to_path_buf(),
        mount_root: mount_root.to_path_buf(),
        exec_temp_dir,
        store_root: store_root.to_path_buf(),

        cpu_pool_directory: Some(crate::runtime::job_cpu::cpu_pool_directory(
            store_root,
            workspace_incarnation,
        )),
        shed_links: shed_links(&mount).map_err(|error| {
            CowshedError::integrity(
                format!("cannot read the shed beside {}: {error}", mount.display()),
                "cowshed doctor --json",
            )
        })?,
        workspace_mount: mount,
        port_block: grants.port_block.ok_or_else(|| {
            CowshedError::integrity("workspace has no port block", "cowshed doctor --json")
        })?,
        retained_port_blocks: grants.retained_port_blocks.clone(),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants {
            read: grants.read.clone(),
            write: grants.write.clone(),
            deny_write: grants.deny_write.clone(),
            deny,
            egress: grants
                .egress
                .iter()
                .map(|rule| EgressGrant {
                    host: rule.host.clone(),
                    ports: rule.ports.clone(),
                })
                .collect(),
            loopback_any: grants.loopback_any,
        },
        allowed_unix_sockets: Vec::new(),
        additional_denies,
        git_worktree_repository,
        build_volume_mount,
        repository_caches: repository_caches.to_vec(),
        program_directories: program_directories.to_vec(),
        capabilities: DetectedCapabilities::default(),
    };
    // The supervisor is the trusted tier of the same workspace; it gets the same capability
    // sockets as the children it launches, or an in-workspace evaluation would depend on which
    // tier ran it.
    config.configure_capabilities()?;
    Ok(config)
}

/// Read-only jobs keep generic process state in their own exec-temp carve-back.
pub fn sandbox_runtime_dir(sandbox: &SandboxConfig) -> PathBuf {
    match sandbox.mode {
        RunSandboxMode::ReadOnly => sandbox.exec_temp_dir.join("run"),
        RunSandboxMode::ReadWrite => shared_daemon_runtime_dir(sandbox),
    }
}

/// Generic runtime aliases stay mode-private. Only a contributed daemon socket leaf is shared.
pub fn sandbox_runtime_link(sandbox: &SandboxConfig) -> PathBuf {
    let link = workspace_runtime_link(&sandbox.workspace_mount);
    match sandbox.mode {
        RunSandboxMode::ReadOnly => {
            let mut name = link.into_os_string();
            name.push("-ro");
            PathBuf::from(name)
        }
        RunSandboxMode::ReadWrite => link,
    }
}

pub(crate) fn shared_daemon_runtime_dir(sandbox: &SandboxConfig) -> PathBuf {
    sandbox.workspace_mount.join(".cowshed/run")
}

pub(crate) fn shared_daemon_runtime_link(sandbox: &SandboxConfig) -> PathBuf {
    workspace_runtime_link(&sandbox.workspace_mount)
}

/// The short link a workspace's jobs and host shells reach its runtime dir through:
/// `/tmp/cs-<12 hex of SHA-256(workspace mount)>`. It is named after its target's workspace,
/// because only one live workspace is mounted at a path: no other workspace, in this store or
/// another, can name the same link and retarget it. A port block is unique only within one
/// store, so a name derived from it is not. The link stays in `/tmp`, under Darwin's 104-byte
/// `sun_path`, for the sockets beneath it. This is the name's one owner.
pub fn workspace_runtime_link(workspace_mount: &Path) -> PathBuf {
    let digest =
        crate::api::dto::Sha256Digest::compute(workspace_mount.as_os_str().as_encoded_bytes());
    PathBuf::from(format!(
        "/tmp/cs-{}",
        crate::api::dto::hex_lower(&digest.as_bytes()[..6])
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxError {
    InvalidPortBlock { base: u16, size: u16 },
    InvalidPath { path: PathBuf, reason: &'static str },
    GrantIntersectsDeny { grant: PathBuf, deny: PathBuf },
    DenyResolution { path: PathBuf, cause: String },
    InstalledCowshed { path: PathBuf, cause: String },
}

impl fmt::Display for SandboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPortBlock { base, size } => write!(
                formatter,
                "invalid macOS port block at {base} with size {size}; a block is a power-of-two number of ports, at least 2, with its base aligned to its size"
            ),
            Self::InvalidPath { path, reason } => {
                write!(
                    formatter,
                    "invalid sandbox path {}: {reason}",
                    path.display()
                )
            }
            Self::GrantIntersectsDeny { grant, deny } => write!(
                formatter,
                "grant {} intersects protected path {}",
                grant.display(),
                deny.display()
            ),
            Self::InstalledCowshed { path, cause } => write!(
                formatter,
                "cannot resolve the installed cowshed {}: {cause}",
                path.display()
            ),
            Self::DenyResolution { path, cause } => write!(
                formatter,
                "cannot resolve workspace deny {}: {cause}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for SandboxError {}

struct ValidatedSandboxPaths<'a> {
    hard_denies: Vec<Cow<'a, Path>>,
    read_grants: Vec<&'a Path>,
    write_grants: Vec<&'a Path>,
    sockets: Vec<&'a Path>,
}

/// Validate every path and grant boundary that profile generation enforces.
///
/// Controllers call this before publishing a grant snapshot, so an invalid grant never becomes
/// durable state that only fails when the next child is launched.
pub fn validate_sandbox_config(config: &SandboxConfig) -> Result<(), SandboxError> {
    validated_sandbox_paths(config).map(|_| ())
}

fn validated_sandbox_paths(
    config: &SandboxConfig,
) -> Result<ValidatedSandboxPaths<'_>, SandboxError> {
    validate_path(&config.home)?;
    validate_path(&config.mount_root)?;
    validate_path(&config.workspace_mount)?;
    validate_path(&config.exec_temp_dir)?;
    for block in held_port_blocks(config) {
        block
            .validate()
            .map_err(|_| SandboxError::InvalidPortBlock {
                base: block.base,
                size: block.size,
            })?;
    }

    if let Some(repository) = &config.git_worktree_repository {
        validate_path(repository)?;
    }
    if let Some(volume) = &config.build_volume_mount {
        validate_path(volume)?;
        let valid = volume
            .strip_prefix(&config.mount_root)
            .is_ok_and(|relative| {
                relative.starts_with(".build")
                    && relative.components().count() == 4
                    && relative
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .is_some_and(|id| {
                            id.len() == 32
                                && id.bytes().all(|byte| {
                                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                                })
                        })
            });
        if !valid {
            return Err(SandboxError::InvalidPath {
                path: volume.clone(),
                reason: "a build grant must name one volume under the host mount root's .build/owner/repo directory",
            });
        }
        if paths_intersect(volume, &config.workspace_mount) {
            return Err(SandboxError::InvalidPath {
                path: volume.clone(),
                reason: "a build volume must be outside the source workspace",
            });
        }
    }
    let hard_denies = hard_denies(&config.home, &config.mount_root, &config.additional_denies)?;
    if let Some(volume) = &config.build_volume_mount
        && let Some(deny) = hard_denies.iter().find(|deny| {
            let boundary = deny.as_ref();
            (boundary != cowshed_root() && boundary != config.mount_root
                || config
                    .additional_denies
                    .iter()
                    .any(|extra| extra == boundary))
                && paths_intersect(volume, boundary)
        })
    {
        return Err(SandboxError::GrantIntersectsDeny {
            grant: volume.clone(),
            deny: deny.as_ref().to_owned(),
        });
    }
    let read_grants = normalized_paths(&config.grants.read)?;
    let write_grants = normalized_paths(&config.grants.write)?;
    let sockets = normalized_paths(&config.allowed_unix_sockets)?;
    for relative in config.grants.deny_write.iter().chain(&config.grants.deny) {
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
            || relative.components().collect::<PathBuf>().as_os_str() != relative.as_os_str()
        {
            return Err(SandboxError::InvalidPath {
                path: relative.clone(),
                reason: "workspace deny must be relative without traversal",
            });
        }
    }
    for link in &config.shed_links {
        validate_path(&link.link)?;
        validate_path(&link.target)?;
    }

    for grant in read_grants.iter().chain(write_grants.iter()) {
        if let Some(deny) = hard_denies
            .iter()
            .find(|deny| paths_intersect(grant, deny.as_ref()))
        {
            return Err(SandboxError::GrantIntersectsDeny {
                grant: (*grant).to_path_buf(),
                deny: deny.as_ref().to_path_buf(),
            });
        }
    }
    let contribution = &config.capabilities.contribution;
    for grant in &contribution.grants {
        validate_path(&grant.path)?;
        // A grant beneath HOME only ever narrows the HOME-wide read deny: HOME itself is never
        // granted, or the deny would be undone wholesale.
        if grant.path == config.home {
            return Err(SandboxError::InvalidPath {
                path: grant.path.clone(),
                reason: "a capability grant beneath HOME must be strictly beneath it",
            });
        }
        // A grant is refused only at or under a hard deny. Beside or above one it stays legal —
        // the literal `~/.cargo` a shared cargo home needs sits beside the denied
        // `~/.cargo/config.toml` — because every hard deny is emitted after every grant.
        if let Some(deny) = hard_denies
            .iter()
            .find(|deny| grant.path.starts_with(deny.as_ref()))
        {
            return Err(SandboxError::GrantIntersectsDeny {
                grant: grant.path.clone(),
                deny: deny.as_ref().to_path_buf(),
            });
        }
    }
    // A shared cache is a read-write subtree of the host HOME: strictly beneath it, and never
    // reaching into a hard deny or cowshed's own controller state in either direction, or the
    // write would cover a credential, another workspace or the gateway's mirrors.
    let controller_state = crate::host_dirs::controller_state(&config.home);
    for cache in &contribution.shared_caches {
        validate_path(&cache.path)?;
        if cache.path == config.home || !cache.path.starts_with(&config.home) {
            return Err(SandboxError::InvalidPath {
                path: cache.path.clone(),
                reason: "a shared cache must lie strictly beneath HOME",
            });
        }
        if let Some(deny) = hard_denies
            .iter()
            .map(AsRef::as_ref)
            .chain(controller_state.iter().map(PathBuf::as_path))
            .find(|deny| paths_intersect(&cache.path, deny))
        {
            return Err(SandboxError::GrantIntersectsDeny {
                grant: cache.path.clone(),
                deny: deny.to_path_buf(),
            });
        }
    }

    Ok(ValidatedSandboxPaths {
        hard_denies,
        read_grants,
        write_grants,
        sockets,
    })
}

/// A rendered profile and the build volume its rules name as [`BUILD_VOLUME_PARAM`].
///
/// The text names the build volume only as `(param "BUILD_VOLUME")`, so a pivot binds a new
/// path to the same text (04_sandbox.md, "Exec pipeline" step 4). An unbound `param` evaluates
/// to `#f`, and `sandbox-exec` refuses the whole profile ("invalid data type of path filter;
/// expected pattern, got boolean", exit 65). The binding therefore travels with the text it
/// completes: [`crate::exec::ProfileLaunch::write`] takes both from this one value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeatbeltProfile {
    text: String,
    build_volume: Option<PathBuf>,
}

impl SeatbeltProfile {
    /// A profile whose text names no parameter: a test's hand-written host profile.
    #[cfg(test)]
    pub(crate) fn unparameterized(text: &str) -> Self {
        Self {
            text: text.to_owned(),
            build_volume: None,
        }
    }

    /// The SBPL program. Launched alone (`sandbox-exec -p`), a profile with a build volume
    /// is refused; launch it through [`crate::exec::ProfileLaunch`].
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The text alone, its binding dropped: a profile with a build volume launches only through
    /// [`crate::exec::ProfileLaunch`].
    pub fn into_text(self) -> String {
        self.text
    }

    /// The path a launch binds to [`BUILD_VOLUME_PARAM`]; `Some` exactly when the text names it.
    pub fn build_volume(&self) -> Option<&Path> {
        self.build_volume.as_deref()
    }
}

/// The one program a workspace command runs outside the sandbox. macOS refuses a sandboxed exec
/// of a set-user-ID program whatever the profile allows, and `/bin/ps` is one, so without this a
/// job cannot list its own processes or read their start environment (04_sandbox.md, "Process
/// inspection"). `ps` only reads the process table and prints it, and shows the environment of
/// this user's processes alone, which every job already reads through `kern.procargs2`.
const PROCESS_STATUS_EXEC: &str = "(allow process-exec (literal \"/bin/ps\") (with no-sandbox))";

/// Where a fixture's short root lies: directly under the world-shared `/tmp`, spelled canonically
/// because Seatbelt matches the resolved path and `/tmp` is a symlink to `/private/tmp`.
const FIXTURE_PARENT: &str = "/private/tmp";

/// What starts a fixture's names beneath [`FIXTURE_PARENT`]: the root `cx<pid>-<2 alphanumerics>`
/// its process creates and removes with everything beneath it, and the `cxsweep.lock` that
/// serializes reclaiming a dead run's roots (04_sandbox.md, "Fixture roots under /tmp"). It holds
/// no regex metacharacter.
const FIXTURE_PREFIX: &str = "cx";

/// The one place a job writes in `/tmp`: a fixture root and what lies beneath it, the sweep lock,
/// and the Unix sockets a daemon binds inside a root. Only these spellings match, so a lookalike
/// name and every other entry of `/tmp` stay denied. A regex is the one SBPL filter that names a
/// pattern, and its engine does not honor a `{n}` interval (measured: `[A-Za-z0-9]{2}` compiles
/// and does not match `ab`), so the two random characters are two classes. The sweep reads
/// `/tmp`'s names with the `file-read-data` the broad allow already grants; no other entry gains
/// metadata.
fn push_fixture_root_rules(profile: &mut String) {
    let root = format!("^{FIXTURE_PARENT}/{FIXTURE_PREFIX}[0-9]+-[A-Za-z0-9][A-Za-z0-9]");
    let lock = format!("{FIXTURE_PARENT}/{FIXTURE_PREFIX}sweep.lock");
    push_line(
        profile,
        &format!("(allow file-read* file-write* (regex #\"{root}(/|$)\") (literal \"{lock}\"))"),
    );
    push_line(
        profile,
        &format!(
            "(allow network-bind network-inbound network-outbound (local unix-socket (path-regex #\"{root}/\")) (remote unix-socket (path-regex #\"{root}/\")))"
        ),
    );
}

/// Generate a complete, deterministic SBPL profile for one authority tier.
///
/// Paths must already be canonical controller data. Child argv, environment,
/// output, and repository-controlled grants are deliberately absent from the
/// role selection and therefore cannot remove the executed-child narrowing.
pub fn seatbelt_profile(
    config: &SandboxConfig,
    role: SandboxProfileRole,
) -> Result<SeatbeltProfile, SandboxError> {
    let ValidatedSandboxPaths {
        hard_denies,
        read_grants,
        write_grants,
        sockets,
    } = validated_sandbox_paths(config)?;

    let cowshed = cowshed_root();
    let mut profile = String::new();

    push_line(&mut profile, "(version 1)");
    push_line(&mut profile, "(deny default)");
    // Hard-link creation is a separate SBPL operation from file-write*.
    // Keep aliases unavailable to both authority tiers.
    push_line(&mut profile, "(deny file-link)");
    if !matches!(
        role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    ) {
        push_line(&mut profile, "(allow file-read-data (subpath \"/\"))");
        // The broad allow is for system paths. Nothing under HOME is readable unless a later
        // rule names it: an explicit grant, the workspace's own mount, the exec temp dir, an
        // allowed socket, or a detected capability's grant. Every one of those follows here,
        // so last-match-wins carves each back.
        push_subpath_rule(&mut profile, "deny file-read*", &config.home)?;
    } else {
        // Git's isolated global configuration is the empty device, not user HOME.
        push_literal_rule(&mut profile, "allow file-read*", Path::new("/dev/null"))?;
    }
    // Every runtime seeds itself from the kernel's entropy before it runs a line; Python reads
    // `/dev/urandom` for it, and without it dies in `_Py_HashRandomization_Init`. Git's own
    // profile may run a standalone repository's merge drivers too, so every role reads both devices.
    for device in ["/dev/random", "/dev/urandom"] {
        push_literal_rule(&mut profile, "allow file-read*", Path::new(device))?;
    }
    // Directory metadata is distinct from file-read-data in Seatbelt. Toolchain
    // launchers (notably /usr/bin/git -> xcrun) must traverse their immutable
    // system roots without gaining metadata access to the user's home.
    // Nix tools load libraries from other immutable store entries even without a Nix
    // capability: dyld needs their metadata as well as the baseline data reads.
    for root in [
        "/Applications",
        "/Library",
        "/System",
        "/bin",
        "/nix/store",
        "/opt",
        "/private/var/select",
        "/sbin",
        "/usr",
        "/var/select",
    ] {
        push_exact_and_subpath_rule(&mut profile, "allow file-read*", Path::new(root))?;
        push_readable_ancestors(&mut profile, Path::new(root))?;
    }
    if role == SandboxProfileRole::GitDiscoveryGitOnly {
        push_line(&mut profile, "(allow process-fork)");
        // The developer-dir `git` the sandbox launches and the helpers it re-executes from its
        // exec path, in both the spelling Git is launched by and the one the link resolves to.
        // Nothing else runs: not a shell, so not any command a config key names.
        let developer = Path::new(GIT_DEVELOPER_DIR);
        // Seatbelt matches the resolved spelling. A developer directory that does not resolve
        // would leave only a rule that never matches, and Git would die with no cause named.
        let resolved = std::fs::canonicalize(developer).map_err(|_| SandboxError::InvalidPath {
            path: developer.to_owned(),
            reason: "the active developer directory does not resolve",
        })?;
        for root in [developer.to_owned(), resolved] {
            push_literal_rule(
                &mut profile,
                "allow process-exec",
                &root.join("usr/bin/git"),
            )?;
            push_subpath_rule(
                &mut profile,
                "allow process-exec",
                &root.join("usr/libexec/git-core"),
            )?;
        }
    } else {
        push_line(&mut profile, "(allow process-exec process-fork)");
        // After the broad allow, which would otherwise match `/bin/ps` last and keep it sandboxed.
        push_line(&mut profile, PROCESS_STATUS_EXEC);
    }
    push_line(&mut profile, "(allow file-map-executable)");
    push_line(&mut profile, "(allow sysctl-read)");
    push_line(&mut profile, "(allow pseudo-tty)");
    push_line(&mut profile, "(allow process-info* (target same-sandbox))");
    push_line(&mut profile, "(allow signal (target same-sandbox))");
    push_line(
        &mut profile,
        "(allow mach-priv-task-port (target same-sandbox))",
    );
    // Filesystem watchers need this service port or they silently receive no
    // events. FSEvents still filters notifications through the file-read
    // policy: the real watcher regression covers an allowed update and a
    // denied descendant. This service adds no filesystem authority.
    push_line(
        &mut profile,
        "(allow mach-lookup (global-name \"com.apple.FSEvents\"))",
    );
    // pwd.h/grp.h use OpenDirectory's read-only libinfo lookup service, as
    // Apple's opendirectory.sb documents. Stock id and ssh-keygen need it to
    // resolve the effective uid; record (.api) and membership services stay denied.
    push_line(
        &mut profile,
        "(allow mach-lookup (global-name \"com.apple.system.opendirectoryd.libinfo\"))",
    );
    // The per-user directory helper (`com.apple.bsd.dirhelper`) stays denied, so `confstr(3)`'s
    // `DARWIN_USER_TEMP_DIR` and `DARWIN_USER_CACHE_DIR` answer EIO and tools fall back to
    // `TMPDIR` or `/tmp`. Answered, they name the host user's own `/var/folders` directories,
    // which no job may write (every host process's temp files live there): `mktemp` under a
    // devenv shell then picked one and failed with EPERM instead of using the job's temp dir.

    for socket in &sockets {
        push_line(
            &mut profile,
            &format!(
                "(allow network-outbound (remote unix-socket (path-literal \"{}\")))",
                sbpl_path(socket)?
            ),
        );
    }
    push_line(
        &mut profile,
        "(allow network-bind network-inbound (local tcp \"localhost:*\"))",
    );
    // Unix sockets the workspace's own processes rendezvous over — devenv's and nx's in the
    // runtime dir, a test's in its temp dir, a tool's under the checkout — are admitted in the
    // workspace's own tree and nowhere else: exec temp, and the whole source mount only for a
    // read-write job. Generic rendezvous state stays private to the job mode; the contributed
    // shared daemon leaf is granted separately below. `path-prefix` is a string prefix, so the
    // trailing slash excludes sibling names.
    let runtime_link = sandbox_runtime_link(config);
    let private_runtime_link =
        PathBuf::from("/private").join(runtime_link.strip_prefix("/").unwrap_or(&runtime_link));
    let own_mount = (config.mode == RunSandboxMode::ReadWrite).then_some(&config.workspace_mount);
    for tree in
        own_mount
            .into_iter()
            .chain([&config.exec_temp_dir, &runtime_link, &private_runtime_link])
    {
        push_line(
            &mut profile,
            &format!(
                "(allow network-bind network-inbound network-outbound (local unix-socket (path-prefix \"{0}/\")) (remote unix-socket (path-prefix \"{0}/\")))",
                sbpl_path(tree)?
            ),
        );
    }
    // Every port of every block the workspace holds, current and retained, one literal each; a
    // loopback-any grant admits every loopback port instead (04_sandbox.md).
    if config.grants.loopback_any {
        push_line(
            &mut profile,
            "(allow network-outbound (remote tcp \"localhost:*\"))",
        );
    } else {
        for block in held_port_blocks(config) {
            for port in block.ports().map_err(|_| SandboxError::InvalidPortBlock {
                base: block.base,
                size: block.size,
            })? {
                push_line(
                    &mut profile,
                    &format!("(allow network-outbound (remote tcp \"localhost:{port}\"))"),
                );
            }
        }
    }

    if !matches!(
        role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    ) {
        let directory = crate::host_dirs::installed_binary_directory(&config.home);
        let stable = directory.join("cowshed");
        let installed = match std::fs::canonicalize(&stable) {
            Ok(path) => Some(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(SandboxError::InstalledCowshed {
                    path: stable.clone(),
                    cause: error.to_string(),
                });
            }
        };
        if let Some(installed) = installed {
            let metadata =
                std::fs::metadata(&installed).map_err(|error| SandboxError::InstalledCowshed {
                    path: installed.clone(),
                    cause: error.to_string(),
                })?;
            if !metadata.is_file() {
                return Err(SandboxError::InvalidPath {
                    path: installed,
                    reason: "the installed cowshed must resolve to one regular executable",
                });
            }
            // Only the installed copy is readable. Lookup of the stable name and its ancestors
            // needs metadata, never directory contents.
            for name in [&stable, &installed] {
                for ancestor in name.ancestors() {
                    push_literal_rule(&mut profile, "allow file-read-metadata", ancestor)?;
                }
            }
            push_literal_rule(&mut profile, "allow file-read*", &installed)?;
            push_literal_rule(&mut profile, "allow process-exec", &installed)?;
        }
    }

    for path in &read_grants {
        push_subpath_rule(&mut profile, "allow file-read*", path)?;
    }
    for path in &write_grants {
        push_subpath_rule(&mut profile, "allow file-read* file-write*", path)?;
    }
    // The machine-global evidence layer and the fixed per-user mount tree are separate protected
    // roots. The mount tree needs its own deny, outside the `/private/cowshed` boundary.
    push_subpath_rule(&mut profile, "deny file-read* file-write*", cowshed)?;
    push_subpath_rule(
        &mut profile,
        "deny file-read* file-write*",
        &config.mount_root,
    )?;
    // `getcwd(2)` and path resolution need read access to every exact ancestor. Literal rules
    // reveal no sibling subtree and are emitted after both broad denies, while the own-workspace
    // subpath grant below carves back only this workspace.
    push_readable_ancestors(&mut profile, &config.workspace_mount)?;
    for path in read_grants.iter().chain(write_grants.iter()) {
        push_readable_ancestors(&mut profile, path)?;
    }
    // A shed link is the spelling a workspace reaches a sibling repository by (`../<name>`), and
    // it sits inside the mount-root deny above. Only a link whose target a grant covers is carved
    // back, as a literal: the link stays unreadable until its target is granted, and reading it
    // reveals nothing a granted subtree does not already show.
    for link in &config.shed_links {
        if read_grants
            .iter()
            .chain(write_grants.iter())
            .any(|grant| paths_intersect(&link.target, grant))
        {
            push_readable_ancestors(&mut profile, &link.link)?;
        }
    }
    // Connecting to an allowed socket resolves its path, so every ancestor must
    // be traversable. Socket directories are not under the immutable roots
    // granted above — the nix daemon socket resolves into `/private/var/run`,
    // while a socket under the store is itself behind the `/private/cowshed` deny — so
    // the literals must land here, after the store-wide deny, or last-match-wins
    // re-denies them and the connect fails on path resolution before the
    // outbound rule is ever consulted.
    for socket in &sockets {
        push_readable_ancestors(&mut profile, socket)?;
    }
    // The workspace's CPU pool directory, `<store>/run/cpu/<incarnation>` (05_gateway.md
    // "Parallelism inside a job"), holds its jobs' jobserver fifos (05_gateway.md "Cargo reads
    // its holder's jobserver"). It sits behind the `/private/cowshed` deny, so its rules land
    // here for the reason the socket ancestors above do: emitted before that deny, last-match-wins
    // re-denied the jobserver FIFO's open with EPERM, and Cargo ran without its jobserver. The
    // profile is rendered per workspace, not per job, so no job's fifo can be a literal here: the
    // scope is this workspace's own pool directory, and inside it only FIFOs open. Its ancestors
    // and entries resolve by metadata, as the installed cowshed's do: neither the directory nor
    // `run/cpu`, where sibling workspaces' directories sit, is listable.
    if let Some(directory) = &config.cpu_pool_directory {
        for ancestor in directory.ancestors().skip(1) {
            push_literal_rule(&mut profile, "allow file-read-metadata", ancestor)?;
        }
        let directory = sbpl_path(directory)?;
        push_line(
            &mut profile,
            &format!("(allow file-read-metadata (subpath \"{directory}\"))"),
        );
        // Its jobserver fifo's tokens are read and written back (05_gateway.md "Cargo reads its
        // holder's jobserver"), and nothing else there is.
        push_line(
            &mut profile,
            &format!(
                "(allow file-read-data file-write-data (require-all (subpath \"{directory}\") (vnode-type FIFO)))"
            ),
        );
    }
    // Every tool-specific authority is a detected capability's contribution (15_capabilities.md):
    // its shared caches with their access where the host keeps them, their ancestors beneath HOME
    // metadata only, and its exact grants. Configuration, credentials and binaries a tool keeps
    // beside them stay hard denies, which follow every grant below.
    let contribution = &config.capabilities.contribution;
    for cache in &contribution.shared_caches {
        push_capability_grant(
            &mut profile,
            &config.home,
            &CapabilityGrant {
                path: cache.path.clone(),
                scope: GrantScope::Subtree,
                access: cache.access,
            },
        )?;
    }
    for grant in &contribution.grants {
        push_capability_grant(&mut profile, &config.home, grant)?;
    }
    // Build state is not source. Grant only the controller-selected physical volume, after
    // the store and mount-tree denies; sibling volumes and images remain inaccessible. The volume
    // is the profile's one parameter (`BUILD_VOLUME_PARAM`), so a build-volume pivot binds a new
    // path to the same profile rather than rendering another.
    if let Some(volume) = &config.build_volume_mount {
        if let Some(parent) = volume.parent() {
            push_readable_ancestors(&mut profile, parent)?;
        }
        push_volume_exact_and_subpath_rule(
            &mut profile,
            "allow file-read* file-write*",
            volume,
            volume,
        )?;
    }
    push_subpath_rule(&mut profile, "allow file-read*", &config.workspace_mount)?;
    if config.mode == RunSandboxMode::ReadWrite {
        push_subpath_rule(&mut profile, "allow file-write*", &config.workspace_mount)?;
    }
    // Last-match-wins: a workspace-relative deny must follow every mount and grant allow.
    // Deny rename/unlink on ancestors too, or renaming `.git` would move the protected
    // children out of the denied spelling before rewriting them.
    for (relatives, operations) in [
        (&config.grants.deny_write, "deny file-write*"),
        (&config.grants.deny, "deny file-read* file-write*"),
    ] {
        for relative in relatives {
            let mut parent = config.workspace_mount.clone();
            for component in relative.components() {
                parent.push(component);
                push_literal_rule(&mut profile, "deny file-write-unlink", &parent)?;
            }
            push_exact_and_subpath_rule(
                &mut profile,
                operations,
                &config.workspace_mount.join(relative),
            )?;
            if let Some(volume) = &config.build_volume_mount {
                let resolved = resolve_deny(&config.workspace_mount.join(relative))?;
                if let Ok(relative) = resolved.strip_prefix(volume) {
                    let mut parent = volume.clone();
                    for component in relative.components() {
                        parent.push(component);
                        push_line(
                            &mut profile,
                            &format!(
                                "(deny file-write-unlink (literal {}))",
                                volume_expression(volume, &parent)?
                            ),
                        );
                    }
                    push_volume_exact_and_subpath_rule(
                        &mut profile,
                        operations,
                        volume,
                        &resolved,
                    )?;
                }
            }
        }
    }
    let workspace_metadata = config.workspace_mount.join(".cowshed");
    let job_artifacts = workspace_metadata.join("job");

    // SBPL is last-match-wins. Policy denies (project roots the host configured) come first,
    // then the two carve-backs they would otherwise close, then the immutable secret denies as
    // the profile's last word on the shared tree: no grant may follow a secret's deny.
    let (policy, secrets): (Vec<_>, Vec<_>) = hard_denies
        .into_iter()
        .filter(|path| path.as_ref() != cowshed && path.as_ref() != config.mount_root.as_path())
        .partition(|path| {
            config
                .additional_denies
                .iter()
                .any(|extra| extra == path.as_ref())
        });
    for deny in policy {
        push_exact_and_subpath_rule(&mut profile, "deny file-read* file-write*", deny.as_ref())?;
    }

    // After every deny that would otherwise close it: the mount-root deny covers
    // main under the symlink layout, and policy denies the project root under direct mount. The
    // carve-back is narrowed to `.git`, never main's working tree, which stays as unreachable as
    // any sibling workspace.
    if let Some(repository) = &config.git_worktree_repository {
        push_readable_ancestors(&mut profile, repository)?;
        push_exact_and_subpath_rule(&mut profile, "allow file-read* file-write*", repository)?;
    }

    // The exec temp dir is exported as TMPDIR and lives in the project's store directory, so its
    // grant must follow every deny that covers it - the store-wide one and the project root
    // above - or last-match-wins re-denies it: every `mktemp` in a child then fails on a path
    // the child never chose. Read and write both - a child reads back what it wrote there, and
    // metadata on the directory is what a `realpath(os.tmpdir())` needs.
    push_subpath_rule(&mut profile, "allow file-read*", &config.exec_temp_dir)?;
    push_line(
        &mut profile,
        &format!(
            "(allow file-write* (subpath \"{}\") (literal \"/dev/null\"))",
            sbpl_path(&config.exec_temp_dir)?
        ),
    );
    // A child writes the null device through a descriptor its parent opened write-only (a
    // shell's `>/dev/null`, a detached job's stdio), and runtimes fstat their stdio before
    // running a line: Node aborts in its process setup, with no message, when that fstat
    // fails. fstat on a write-only descriptor is file-read-metadata, which neither the
    // write grant above nor file-read-data on `/` includes.
    push_literal_rule(
        &mut profile,
        "allow file-read-metadata",
        Path::new("/dev/null"),
    )?;
    // A child opens its stdio by name too (usdt's proc macro runs `dtrace -h -s /dev/stdin -o
    // /dev/stdout`). The names are links into /dev/fd, and Seatbelt matches the resolved
    // spelling: a rule on a link name never admits an open, only its lookup, which is
    // file-read-metadata. The open lands on /dev/fd/N, which duplicates a descriptor this
    // process already holds and refuses an access mode wider than that descriptor's, so the
    // subtree reaches nothing the child could not already read or write.
    for link in ["/dev/stdin", "/dev/stdout", "/dev/stderr"] {
        push_literal_rule(&mut profile, "allow file-read-metadata", Path::new(link))?;
    }
    push_subpath_rule(
        &mut profile,
        "allow file-read* file-write*",
        Path::new("/dev/fd"),
    )?;
    push_readable_ancestors(&mut profile, &config.exec_temp_dir)?;
    // A contributed daemon leaf (Nx's run/nx) is shared across modes, never the surrounding
    // generic runtime directory. The rest of a read-only job's rendezvous state stays private.
    let shared_alias = shared_daemon_runtime_link(config);
    let shared_runtime = shared_daemon_runtime_dir(config);
    for alias in &config
        .capabilities
        .contribution
        .daemon_isolation
        .directories
    {
        let Ok(relative) = alias.strip_prefix(&shared_alias) else {
            continue;
        };
        validate_path(alias)?;
        if relative.as_os_str().is_empty() {
            return Err(SandboxError::InvalidPath {
                path: alias.clone(),
                reason: "a shared daemon grant must name a leaf below its runtime root",
            });
        }
        let directory = shared_runtime.join(relative);
        push_readable_ancestors(&mut profile, &directory)?;
        push_subpath_rule(&mut profile, "allow file-read* file-write*", &directory)?;
        let canonical_alias = Path::new("/private").join(alias.strip_prefix("/").unwrap_or(alias));
        for socket_tree in [&directory, alias, &canonical_alias] {
            push_line(
                &mut profile,
                &format!(
                    "(allow network-bind network-inbound network-outbound (local unix-socket (path-prefix \"{0}/\")) (remote unix-socket (path-prefix \"{0}/\")))",
                    sbpl_path(socket_tree)?
                ),
            );
        }
        for name in [
            &shared_alias,
            &Path::new("/private").join(shared_alias.strip_prefix("/").unwrap_or(&shared_alias)),
        ] {
            push_literal_rule(&mut profile, "allow file-read-metadata", name)?;
        }
    }
    // Generic short runtime links resolve to the mode's own runtime directory. Only metadata
    // on their exact names is allowed, never metadata for the rest of /tmp.
    let runtime_name = runtime_link
        .file_name()
        .expect("runtime link has a file name");
    for tmp in ["/tmp", "/private/tmp"] {
        push_literal_rule(&mut profile, "allow file-read-metadata", Path::new(tmp))?;
        push_literal_rule(
            &mut profile,
            "allow file-read-metadata",
            &Path::new(tmp).join(runtime_name),
        )?;
    }

    // The one world-shared `/tmp` area a job writes: a fixture's short root and its sweep lock
    // (04_sandbox.md, "Fixture roots under /tmp"). Controller Git keeps its narrower write
    // boundary.
    if !matches!(
        role,
        SandboxProfileRole::GitDiscovery | SandboxProfileRole::GitDiscoveryGitOnly
    ) {
        push_fixture_root_rules(&mut profile);
    }

    for deny in secrets {
        push_exact_and_subpath_rule(&mut profile, "deny file-read* file-write*", deny.as_ref())?;
    }

    for protected in [
        crate::storage::WORKSPACE_MARKER_PATH,
        crate::workspace_credentials::CA_CERTIFICATE_PATH,
        crate::workspace_credentials::WORKSPACE_TOKEN_PATH,
        crate::workspace_environment::WORKSPACE_ENVIRONMENT_PATH,
        crate::workspace_git_fetch::WORKSPACE_GIT_FETCH_CONFIG_PATH,
        crate::git::WORKSPACE_GIT_IDENTITY_CONFIG_PATH,
    ] {
        push_literal_rule(
            &mut profile,
            "deny file-write*",
            &config.workspace_mount.join(protected),
        )?;
    }
    // The staged exec host every warm shell runs as. A job that could replace it would run its
    // own program as every later command's parent, able to forge their wait status.
    push_exact_and_subpath_rule(
        &mut profile,
        "deny file-write*",
        &config
            .workspace_mount
            .join(crate::runtime::shell_host::SHELL_HOST_DIRECTORY),
    )?;

    match role {
        SandboxProfileRole::TrustedSupervisor => {
            // The trusted writer's reserved authority is the final narrow
            // carve-back, including when the repository itself is read-only.
            push_exact_and_subpath_rule(&mut profile, "allow file-write*", &job_artifacts)?;
        }
        SandboxProfileRole::ExecutedChild
        | SandboxProfileRole::GitDiscovery
        | SandboxProfileRole::GitDiscoveryGitOnly => {
            // These terminal rules are emitted after every configurable or broad
            // allow. Denying create/unlink at the metadata directory itself
            // prevents replacing or renaming that ancestor without blocking
            // writes to unrelated metadata children.
            // file-write* covers create, data write/truncate, rename, unlink, and
            // symlink creation. Hard links are separately denied for both tiers.
            push_literal_rule(
                &mut profile,
                "deny file-write-create file-write-unlink",
                &workspace_metadata,
            )?;
            push_exact_and_subpath_rule(&mut profile, "deny file-write*", &job_artifacts)?;
            let tool_root = match config.mode {
                RunSandboxMode::ReadOnly => config.exec_temp_dir.join("tools"),
                RunSandboxMode::ReadWrite => workspace_metadata.join("tools"),
            };
            push_exact_and_subpath_rule(&mut profile, "deny file-write*", &tool_root)?;
        }
    }
    Ok(SeatbeltProfile {
        text: profile,
        build_volume: config.build_volume_mount.clone(),
    })
}

/// The current block, then every retained one.
fn held_port_blocks(config: &SandboxConfig) -> impl Iterator<Item = PortBlock> + '_ {
    std::iter::once(config.port_block).chain(config.retained_port_blocks.iter().copied())
}

fn hard_denies<'a>(
    home: &Path,
    mount_root: &'a Path,
    additional: &'a [PathBuf],
) -> Result<Vec<Cow<'a, Path>>, SandboxError> {
    let mut denies = vec![
        Cow::Borrowed(cowshed_root()),
        Cow::Borrowed(mount_root),
        Cow::Owned(home.join(".ssh")),
        Cow::Owned(home.join(".gnupg")),
        Cow::Owned(home.join(".aws")),
        Cow::Owned(home.join(".config/gh")),
        Cow::Owned(home.join(".netrc")),
        Cow::Owned(home.join(".npmrc")),
        Cow::Owned(home.join(".pypirc")),
        Cow::Owned(home.join(".cargo/config.toml")),
        Cow::Owned(home.join(".cargo/config")),
        Cow::Owned(home.join(".cargo/credentials.toml")),
        Cow::Owned(home.join(".cargo/credentials")),
        Cow::Owned(home.join(".gradle/gradle.properties")),
        Cow::Owned(home.join("Library/Keychains")),
    ];
    denies.extend(additional.iter().map(|path| Cow::Borrowed(path.as_path())));
    for path in &denies {
        validate_path(path)?;
    }
    denies.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
    denies.dedup_by(|left, right| left.as_ref() == right.as_ref());
    Ok(denies)
}

fn normalized_paths(paths: &[PathBuf]) -> Result<Vec<&Path>, SandboxError> {
    let mut paths: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
    for path in &paths {
        validate_path(path)?;
    }
    paths.sort_unstable();
    paths.dedup();
    Ok(paths)
}

/// Resolve a deny even when its leaf has not been created. This adds no authority: only
/// deny rules beneath an already controller-selected physical build volume use the result.
fn resolve_deny(path: &Path) -> Result<PathBuf, SandboxError> {
    let mut existing = path;
    let mut suffix = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut resolved) => {
                for part in suffix.into_iter().rev() {
                    resolved.push(part);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(name) = existing.file_name() else {
                    return Err(SandboxError::DenyResolution {
                        path: path.to_owned(),
                        cause: error.to_string(),
                    });
                };
                suffix.push(name);
                existing = existing
                    .parent()
                    .expect("an absolute named path has a parent");
            }
            Err(error) => {
                return Err(SandboxError::DenyResolution {
                    path: path.to_owned(),
                    cause: error.to_string(),
                });
            }
        }
    }
}

fn validate_path(path: &Path) -> Result<(), SandboxError> {
    if !path.is_absolute() {
        return Err(SandboxError::InvalidPath {
            path: path.to_path_buf(),
            reason: "path is not absolute",
        });
    }
    if !crate::repository::is_lexically_canonical(path) {
        return Err(SandboxError::InvalidPath {
            path: path.to_path_buf(),
            reason: "path is not canonical",
        });
    }
    if path.as_os_str().to_string_lossy().contains('\0') {
        return Err(SandboxError::InvalidPath {
            path: path.to_path_buf(),
            reason: "path contains NUL",
        });
    }
    Ok(())
}

fn paths_intersect(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn sbpl_path(path: &Path) -> Result<String, SandboxError> {
    validate_path(path)?;
    Ok(path
        .as_os_str()
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\""))
}

/// One capability grant. Every grant makes its path and ancestors resolvable; a read-write
/// literal is a file the tool itself writes, and a subtree extends the grant to everything
/// beneath its root. An ancestor beneath HOME gets metadata only: enough to resolve and
/// `realpath` through it, never to list `~` or `~/Library/Application Support`, which the
/// HOME-wide read deny keeps closed.
fn push_capability_grant(
    profile: &mut String,
    home: &Path,
    grant: &CapabilityGrant,
) -> Result<(), SandboxError> {
    for ancestor in grant.path.ancestors().skip(1) {
        let operation = if ancestor.starts_with(home) {
            "allow file-read-metadata"
        } else {
            "allow file-read*"
        };
        push_literal_rule(profile, operation, ancestor)?;
    }
    let operation = match grant.access {
        GrantAccess::ReadMetadata => "allow file-read-metadata",
        GrantAccess::Read => "allow file-read*",
        GrantAccess::ReadWrite => "allow file-read* file-write*",
    };
    match grant.scope {
        GrantScope::Literal => push_literal_rule(profile, operation, &grant.path),
        GrantScope::Subtree => push_subpath_rule(profile, operation, &grant.path),
    }
}

fn push_readable_ancestors(profile: &mut String, path: &Path) -> Result<(), SandboxError> {
    for ancestor in path.ancestors() {
        push_literal_rule(profile, "allow file-read*", ancestor)?;
    }
    Ok(())
}

/// Where controller Git runs from: the active developer directory's `usr/bin/git` and its
/// `usr/libexec/git-core` exec path.
pub(crate) const GIT_DEVELOPER_DIR: &str = "/var/select/developer_dir";

fn push_subpath_rule(
    profile: &mut String,
    operation: &str,
    path: &Path,
) -> Result<(), SandboxError> {
    push_line(
        profile,
        &format!("({operation} (subpath \"{}\"))", sbpl_path(path)?),
    );
    Ok(())
}

fn push_literal_rule(
    profile: &mut String,
    operation: &str,
    path: &Path,
) -> Result<(), SandboxError> {
    push_line(
        profile,
        &format!("({operation} (literal \"{}\"))", sbpl_path(path)?),
    );
    Ok(())
}

fn push_exact_and_subpath_rule(
    profile: &mut String,
    operation: &str,
    path: &Path,
) -> Result<(), SandboxError> {
    let path = sbpl_path(path)?;
    push_line(
        profile,
        &format!("({operation} (literal \"{path}\") (subpath \"{path}\"))"),
    );
    Ok(())
}

/// The Seatbelt parameter a profile names its build volume by; `sandbox-exec -D` binds it at
/// launch (04_sandbox.md, "Exec pipeline" step 4).
pub const BUILD_VOLUME_PARAM: &str = "BUILD_VOLUME";

/// `path`, inside the build volume `volume`, as an SBPL string expression over the
/// [`BUILD_VOLUME_PARAM`] parameter.
fn volume_expression(volume: &Path, path: &Path) -> Result<String, SandboxError> {
    sbpl_path(path)?;
    let parameter = format!("(param \"{BUILD_VOLUME_PARAM}\")");
    let relative = path
        .strip_prefix(volume)
        .map_err(|_| SandboxError::InvalidPath {
            path: path.to_owned(),
            reason: "a build volume rule must name a path inside the build volume",
        })?;
    if relative.as_os_str().is_empty() {
        return Ok(parameter);
    }
    let relative = relative
        .as_os_str()
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    Ok(format!("(string-append {parameter} \"/{relative}\")"))
}

fn push_volume_exact_and_subpath_rule(
    profile: &mut String,
    operation: &str,
    volume: &Path,
    path: &Path,
) -> Result<(), SandboxError> {
    let path = volume_expression(volume, path)?;
    push_line(
        profile,
        &format!("({operation} (literal {path}) (subpath {path}))"),
    );
    Ok(())
}

fn push_line(profile: &mut String, line: &str) {
    profile.push_str(line);
    profile.push('\n');
    // An explicit file-read-data rule takes precedence over file-read*, even
    // when the wildcard comes later. Give every read rule the same specificity
    // so ordered denies and carve-backs govern actual reads as well as metadata.
    let Some((operations, filters)) = line.split_once(" (") else {
        return;
    };
    let Some((effect, names)) = operations.split_once(' ') else {
        return;
    };
    if names.split_whitespace().any(|name| name == "file-read*") {
        profile.push_str(effect);
        profile.push_str(" file-read-data (");
        profile.push_str(filters);
        profile.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use cowshed_os::fork_lock::Spawn as _;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
    use std::fs;
    #[cfg(target_os = "macos")]
    use std::io::Write as _;
    #[cfg(target_os = "macos")]
    use std::process::Stdio;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_SANDBOX_DIR: AtomicU64 = AtomicU64::new(0);

    fn config(mode: RunSandboxMode) -> SandboxConfig {
        SandboxConfig {
            home: PathBuf::from("/Users/tester"),
            mount_root: PathBuf::from("/Users/tester/.cowshed/mnt"),
            workspace_mount: PathBuf::from(
                "/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount",
            ),
            shed_links: Vec::new(),
            exec_temp_dir: PathBuf::from("/private/tmp/cowshed-raven"),
            store_root: PathBuf::from("/private/tmp/cowshed-raven-store"),
            port_block: PortBlock::new(40_960, 16).unwrap(),
            retained_port_blocks: Vec::new(),
            mode,
            grants: SandboxGrants {
                read: vec![PathBuf::from("/opt/shared"), PathBuf::from("/opt/shared")],
                write: vec![PathBuf::from("/opt/output")],
                deny_write: Vec::new(),
                deny: Vec::new(),
                egress: vec![EgressGrant {
                    host: "example.com".into(),
                    ports: vec![443],
                }],
                loopback_any: false,
            },
            allowed_unix_sockets: vec![PathBuf::from("/var/run/nix/daemon-socket/socket")],
            cpu_pool_directory: None,
            additional_denies: vec![],
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            program_directories: Vec::new(),
            capabilities: DetectedCapabilities::default(),
        }
    }

    #[test]
    fn build_volume_grants_require_one_external_mount_and_preserve_explicit_denies() {
        let mut sandbox = config(RunSandboxMode::ReadOnly);
        let volume = sandbox
            .mount_root
            .join(".build/example-org/example-app/0123456789abcdef0123456789abcdef");
        sandbox.build_volume_mount = Some(volume.clone());
        validate_sandbox_config(&sandbox).unwrap();
        let contribution = sandbox.capabilities.clone();
        assert_eq!(
            sandbox.with_capabilities(contribution).build_volume_mount,
            Some(volume.clone())
        );
        for bad in [
            PathBuf::from("relative"),
            sandbox.mount_root.clone(),
            volume.parent().unwrap().to_owned(),
            sandbox
                .mount_root
                .join(".build/example-org/example-app/not-a-volume"),
            sandbox.workspace_mount.clone(),
        ] {
            sandbox.build_volume_mount = Some(bad);
            assert!(validate_sandbox_config(&sandbox).is_err());
        }
        sandbox.build_volume_mount = Some(volume.clone());
        sandbox
            .additional_denies
            .push(volume.parent().unwrap().to_owned());
        assert!(matches!(
            validate_sandbox_config(&sandbox),
            Err(SandboxError::GrantIntersectsDeny { .. })
        ));
        sandbox.additional_denies.clear();
        sandbox.workspace_mount = volume.parent().unwrap().to_owned();
        assert!(matches!(
            validate_sandbox_config(&sandbox),
            Err(SandboxError::InvalidPath { .. })
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_volume_is_writable_with_read_only_source_and_a_pivot_changes_only_new_job_authority() {
        let fixture = crate::capabilities::test_support::Fixture::new();
        let mut sandbox = config(RunSandboxMode::ReadOnly);
        sandbox.home = fixture.home.clone();
        sandbox.mount_root = fixture.root.join("mounts");
        sandbox.workspace_mount = sandbox.mount_root.join("workspace");
        sandbox.exec_temp_dir = fixture.root.join("exec-temp");
        sandbox.store_root = fixture.root.join("store");
        sandbox.grants = SandboxGrants::default();
        sandbox.allowed_unix_sockets.clear();
        let volumes = sandbox.mount_root.join(".build/example-org/example-app");
        let first = volumes.join("0123456789abcdef0123456789abcdef");
        let second = volumes.join("abcdef0123456789abcdef0123456789");
        fs::create_dir_all(&sandbox.workspace_mount).unwrap();
        fs::create_dir_all(&sandbox.exec_temp_dir).unwrap();
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        let paths = vec![
            crate::capabilities::BuildStatePath::new("target", "target").unwrap(),
            crate::capabilities::BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
        ];
        crate::build_volume::link::point(&sandbox.workspace_mount, &first).unwrap();
        crate::build_volume::link::link_paths(&sandbox.workspace_mount, &first, &paths).unwrap();
        crate::build_volume::link::link_paths(&sandbox.workspace_mount, &second, &paths).unwrap();
        sandbox.build_volume_mount = Some(first.clone());
        sandbox
            .grants
            .deny_write
            .push(PathBuf::from("target/blocked"));
        // A launch as every launcher makes one: the profile's kept file, bound to the volume.
        let launch = |sandbox: &SandboxConfig| {
            let profile = seatbelt_profile(sandbox, SandboxProfileRole::ExecutedChild).unwrap();
            (
                profile.text().to_owned(),
                crate::exec::ProfileLaunch::write(&sandbox.profile_directory(), &profile).unwrap(),
            )
        };
        let (old_text, old) = launch(&sandbox);
        let touch = |profile: &crate::exec::ProfileLaunch, path: &Path| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(profile.args())
                .arg("/usr/bin/touch")
                .arg(path)
                .output_bounded()
                .unwrap()
        };
        for path in ["target/cargo-output", ".nx/cache/nx-output"] {
            let output = touch(&old, &sandbox.workspace_mount.join(path));
            assert!(output.status.success(), "{path}: {output:?}");
        }
        assert!(
            !touch(&old, &sandbox.workspace_mount.join("source.rs"))
                .status
                .success()
        );
        assert!(
            !touch(&old, &sandbox.workspace_mount.join("target/blocked"))
                .status
                .success(),
            "build authority cannot override a workspace-relative deny"
        );
        assert!(
            !touch(&old, &second.join("target/sibling-output"))
                .status
                .success()
        );
        crate::build_volume::link::point(&sandbox.workspace_mount, &second).unwrap();
        assert!(
            !touch(&old, &sandbox.workspace_mount.join("target/after-pivot"))
                .status
                .success(),
            "an old job never acquires the new volume"
        );
        assert!(
            touch(&old, &first.join("target/still-owned"))
                .status
                .success()
        );
        sandbox.build_volume_mount = Some(second.clone());
        let (new_text, new) = launch(&sandbox);
        assert_eq!(
            new_text, old_text,
            "a pivot binds the new volume to the same profile file"
        );
        assert_eq!(new.file(), old.file());
        assert!(
            touch(&new, &sandbox.workspace_mount.join("target/after-pivot"))
                .status
                .success()
        );
        assert!(
            !touch(&new, &first.join("target/old-volume"))
                .status
                .success()
        );
        assert!(
            !touch(&new, &sandbox.workspace_mount.join("source.rs"))
                .status
                .success()
        );
        sandbox.mode = RunSandboxMode::ReadWrite;
        let (_, writable_source) = launch(&sandbox);
        assert!(
            touch(&writable_source, &sandbox.workspace_mount.join("source.rs"))
                .status
                .success()
        );
        assert!(
            touch(
                &writable_source,
                &sandbox.workspace_mount.join(".nx/cache/rw-output")
            )
            .status
            .success()
        );
        assert!(
            !touch(&writable_source, &first.join("target/sibling-output"))
                .status
                .success()
        );
    }

    /// A profile names its build volume only as `(param "BUILD_VOLUME")`. Unbound, that is
    /// `#f`, and `sandbox-exec` refuses the profile with exit 65 ("invalid data type of path
    /// filter; expected pattern, got boolean"): a launch of the bare text, with no `-D`, broke
    /// `host_controller_build_state_answers_with_only_the_installed_executable_grant`. The
    /// rendered value names the binding exactly when its text needs one, and its launch passes it.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_rendered_profile_carries_the_build_volume_its_text_names_and_launches_with_it() {
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        let unbound = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        assert_eq!(unbound.build_volume(), None);
        assert!(
            !unbound.text().contains("(param "),
            "no volume, no parameter"
        );

        let volume = sandbox
            .mount_root
            .join(".build/acme/widget/0123456789abcdef0123456789abcdef");
        sandbox.build_volume_mount = Some(volume.clone());
        let profile = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        assert!(profile.text().contains(&format!(
            "(literal (param \"{BUILD_VOLUME_PARAM}\")) (subpath (param \"{BUILD_VOLUME_PARAM}\"))"
        )));
        assert_eq!(profile.build_volume(), Some(volume.as_path()));
        let launch =
            crate::exec::ProfileLaunch::write(&sandbox.profile_directory(), &profile).unwrap();
        let ran = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(launch.args())
            .arg("/usr/bin/true")
            .output_bounded()
            .unwrap();
        assert!(
            ran.status.success(),
            "{}: {}",
            ran.status,
            String::from_utf8_lossy(&ran.stderr)
        );
    }

    /// A profile past the 128 KiB a command argument may hold -- many read grants make one --
    /// launches with no argument over that bound, from its file, and `sandbox-exec -f` enforces
    /// it: a granted tree reads and stays unwritable, and the workspace writes. Passed as
    /// `sandbox-exec -p`, the profile was one argument a job's process observation read back and
    /// its record could not hold.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_profile_past_the_argument_bound_launches_from_its_file_and_is_enforced() {
        use crate::api::dto::MAX_COMMAND_ARG_BYTES;
        use crate::exec::{SandboxExecRequest, SpawnRunner as _, SystemSpawnRunner, plan_exec};

        let fixture = crate::capabilities::test_support::Fixture::new();
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        sandbox.home = fixture.home.clone();
        sandbox.mount_root = fixture.root.join("mounts");
        sandbox.workspace_mount = sandbox.mount_root.join("workspace");
        sandbox.exec_temp_dir = fixture.root.join("exec-temp");
        sandbox.store_root = fixture.root.join("store");
        sandbox.grants = SandboxGrants::default();
        sandbox.allowed_unix_sockets.clear();
        fs::create_dir_all(&sandbox.workspace_mount).unwrap();
        fs::create_dir_all(&sandbox.exec_temp_dir).unwrap();
        // Seatbelt bounds a compiled profile's string data at 64 KiB, not its text: deep grants
        // that share their ancestors repeat those ancestors' rules in the text and their strings
        // once in the data, so the text passes 128 KiB and still compiles.
        let mut grants = fixture.root.join("grants");
        for level in 0..24 {
            grants.push(format!("shared-ancestor-{level:02}"));
        }
        let count = 120;
        for index in 0..count {
            let tree = grants.join(format!("{index:03}"));
            fs::create_dir_all(&tree).unwrap();
            sandbox.grants.read.push(tree);
        }
        let readable = sandbox.grants.read[count - 1].join("file");
        fs::write(&readable, b"granted\n").unwrap();
        let profile = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(
            profile.len() > MAX_COMMAND_ARG_BYTES,
            "the fixture needs a profile past one argument's bound, not {} bytes",
            profile.len()
        );

        let written = sandbox.workspace_mount.join("written");
        let script = format!(
            "/bin/cat '{}' && ! /usr/bin/touch '{}' 2>/dev/null && /usr/bin/touch '{}'",
            readable.display(),
            sandbox.grants.read[0].join("forbidden").display(),
            written.display()
        );
        let plan = plan_exec(
            SandboxExecRequest {
                argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
                cwd: sandbox.workspace_mount.clone(),
            },
            &sandbox,
        )
        .unwrap();
        assert_eq!(&plan.args[0], "-f");
        assert_eq!(&plan.args[2], "--");
        assert!(
            plan.args
                .iter()
                .all(|argument| argument.len() <= MAX_COMMAND_ARG_BYTES),
            "no launch argument may exceed one argument's bound"
        );
        assert_eq!(fs::read_to_string(&plan.args[1]).unwrap(), profile);
        let status = SystemSpawnRunner.run(&plan).unwrap();
        assert!(status.success(), "the sandboxed script failed: {status:?}");
        assert!(written.is_file(), "the workspace is writable");
        assert!(
            !sandbox.grants.read[0].join("forbidden").exists(),
            "a read grant stays unwritable"
        );
    }

    /// Bun's global registration is only a lookup hop. The provider still needs its own grant,
    /// and traversing that hop grants neither registry contents, listings nor provider writes.
    #[cfg(target_os = "macos")]
    #[test]
    fn bun_registered_provider_links_require_target_grants_and_only_metadata_at_the_registry() {
        let fixture = crate::capabilities::test_support::Fixture::new();
        fixture.files(&["workspace/package.json", "workspace/bun.lock"]);
        let mut sandbox = config(RunSandboxMode::ReadWrite);
        sandbox.home = fixture.home.clone();
        sandbox.mount_root = fixture.root.join("mounts");
        sandbox.workspace_mount = fixture.root.join("workspace");
        sandbox.exec_temp_dir = fixture.root.join("exec-temp");
        sandbox.store_root = fixture.root.join("store");
        sandbox.grants = SandboxGrants::default();
        sandbox.allowed_unix_sockets.clear();
        fs::create_dir_all(&sandbox.exec_temp_dir).unwrap();
        fs::write(sandbox.workspace_mount.join("package.json"), b"{}\n").unwrap();
        let registry = fixture.home.join(".bun/install/global/node_modules");
        let provider = fixture.home.join("providers/granted");
        let ungranted = fixture.home.join("providers/ungranted");
        let modules = sandbox.workspace_mount.join("node_modules");
        for directory in [&registry, &provider, &ungranted, &modules] {
            fs::create_dir_all(directory).unwrap();
        }
        for directory in [&provider, &ungranted, &registry] {
            fs::write(directory.join("package.json"), b"provider\n").unwrap();
        }
        for (name, target) in [("granted", &provider), ("ungranted", &ungranted)] {
            std::os::unix::fs::symlink(target, registry.join(name)).unwrap();
            std::os::unix::fs::symlink(registry.join(name), modules.join(name)).unwrap();
        }
        sandbox.capabilities =
            crate::capabilities::detect_for_workspace(&crate::capabilities::DetectionContext {
                workspace_root: &sandbox.workspace_mount,
                project_root: &sandbox.workspace_mount,
                command_cwd: &sandbox.workspace_mount,
                ..fixture.context()
            })
            .unwrap();
        let paths = serde_json::to_string(&[
            provider.join("package.json"),
            modules.join("granted/package.json"),
            modules.join("ungranted/package.json"),
            registry.join("package.json"),
            registry.clone(),
        ])
        .unwrap();
        for granted in [false, true] {
            if granted {
                sandbox.grants.read.push(provider.clone());
            }
            let profile = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
            let launch =
                crate::exec::ProfileLaunch::write(&sandbox.profile_directory(), &profile).unwrap();
            let script = format!(
                r#"
const fs = require('node:fs');
const assert = require('node:assert/strict');
const [canonical, linked, ungranted, payload, registry] = {paths};
for (const path of [canonical, linked]) {{
  if ({granted}) assert.equal(fs.readFileSync(path, 'utf8'), 'provider\n');
  else assert.throws(() => fs.readFileSync(path), {{code: 'EPERM'}});
}}
for (const path of [ungranted, payload]) assert.throws(() => fs.readFileSync(path), {{code: 'EPERM'}});
assert.throws(() => fs.readdirSync(registry), {{code: 'EPERM'}});
assert.throws(() => fs.writeFileSync(linked, 'changed'), {{code: 'EPERM'}});
"#
            );
            let output = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(launch.args())
                .arg("/usr/bin/env")
                .args(["node", "-e", &script])
                .current_dir(&sandbox.workspace_mount)
                .output_bounded()
                .unwrap();
            assert!(
                output.status.success(),
                "granted={granted}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert_eq!(
            fs::read(provider.join("package.json")).unwrap(),
            b"provider\n"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_source_read_only_job_writes_only_its_shared_nx_leaf_not_other_checkout_runtime() {
        let fixture = crate::capabilities::test_support::Fixture::new();
        let mut sandbox = config(RunSandboxMode::ReadOnly);
        sandbox.home = fixture.home.clone();
        sandbox.mount_root = fixture.root.join("mounts");
        sandbox.workspace_mount = sandbox.mount_root.join("workspace");
        sandbox.exec_temp_dir = fixture.root.join("exec-temp");
        sandbox.store_root = fixture.root.join("store");
        sandbox.grants = SandboxGrants::default();
        sandbox.allowed_unix_sockets.clear();
        let runtime = shared_daemon_runtime_dir(&sandbox);
        for directory in [
            runtime.join("nx"),
            runtime.join("other"),
            sandbox_runtime_dir(&sandbox),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        sandbox
            .capabilities
            .contribution
            .daemon_isolation
            .directories = vec![shared_daemon_runtime_link(&sandbox).join("nx")];
        let profile = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let touch = |path: &Path| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/touch"])
                .arg(path)
                .output_bounded()
                .unwrap()
        };
        for path in [
            runtime.join("nx/state"),
            sandbox_runtime_dir(&sandbox).join("private-state"),
        ] {
            let output = touch(&path);
            assert!(
                output.status.success(),
                "{}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(path.exists());
        }
        for path in [
            runtime.join("other/state"),
            sandbox.workspace_mount.join("source"),
        ] {
            let output = touch(&path);
            assert!(
                !output.status.success(),
                "{} must remain read-only",
                path.display()
            );
            assert!(!path.exists());
        }
        assert!(
            !profile.contains(&format!(
                "(allow file-read* file-write* (subpath \"{}\"))",
                runtime.display()
            )),
            "no grant opens the generic checkout runtime"
        );
    }

    #[test]
    fn sandbox_errors_report_the_rejected_values() {
        let invalid_port = SandboxError::InvalidPortBlock {
            base: 65_520,
            size: 8,
        };
        assert_eq!(
            invalid_port.to_string(),
            "invalid macOS port block at 65520 with size 8; a block is a power-of-two number of ports, at least 2, with its base aligned to its size"
        );

        let invalid_path = SandboxError::InvalidPath {
            path: PathBuf::from("relative/path"),
            reason: "path is not absolute",
        };
        assert_eq!(
            invalid_path.to_string(),
            "invalid sandbox path relative/path: path is not absolute"
        );

        let intersecting_grant = SandboxError::GrantIntersectsDeny {
            grant: PathBuf::from("/Users/tester/.ssh/id_ed25519"),
            deny: PathBuf::from("/Users/tester/.ssh"),
        };
        assert_eq!(
            intersecting_grant.to_string(),
            "grant /Users/tester/.ssh/id_ed25519 intersects protected path /Users/tester/.ssh"
        );
    }

    #[test]
    fn every_profile_path_must_be_absolute_and_canonical() {
        let mut relative = config(RunSandboxMode::ReadOnly);
        relative.home = PathBuf::from("Users/tester");
        assert_eq!(
            seatbelt_profile(&relative, SandboxProfileRole::ExecutedChild),
            Err(SandboxError::InvalidPath {
                path: PathBuf::from("Users/tester"),
                reason: "path is not absolute",
            })
        );

        let mut traversing = config(RunSandboxMode::ReadOnly);
        traversing
            .grants
            .write
            .push(PathBuf::from("/opt/output/../private"));
        assert_eq!(
            seatbelt_profile(&traversing, SandboxProfileRole::ExecutedChild),
            Err(SandboxError::InvalidPath {
                path: PathBuf::from("/opt/output/../private"),
                reason: "path is not canonical",
            })
        );

        let mut nul = config(RunSandboxMode::ReadOnly);
        nul.allowed_unix_sockets = vec![PathBuf::from("/var/run/socket\0suffix")];
        assert_eq!(
            seatbelt_profile(&nul, SandboxProfileRole::ExecutedChild),
            Err(SandboxError::InvalidPath {
                path: PathBuf::from("/var/run/socket\0suffix"),
                reason: "path contains NUL",
            })
        );
    }

    #[test]
    fn additional_denies_are_validated_before_becoming_authoritative() {
        let mut invalid = config(RunSandboxMode::ReadOnly);
        invalid.additional_denies = vec![PathBuf::from("relative/deny")];
        assert_eq!(
            seatbelt_profile(&invalid, SandboxProfileRole::ExecutedChild),
            Err(SandboxError::InvalidPath {
                path: PathBuf::from("relative/deny"),
                reason: "path is not absolute",
            })
        );
    }

    #[test]
    fn custom_mount_root_denies_siblings_and_carves_back_only_own_workspace() {
        let mut custom = config(RunSandboxMode::ReadWrite);
        custom.mount_root = PathBuf::from("/Users/tester/Dev/.cowshed-mounts");
        custom.workspace_mount =
            PathBuf::from("/Users/tester/Dev/.cowshed-mounts/acme/widget/raven");
        let profile = seatbelt_profile(&custom, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();

        let root_deny = profile
            .find("(deny file-read* file-write* (subpath \"/Users/tester/Dev/.cowshed-mounts\"))")
            .expect("configured mount-root deny");
        let global_deny = profile
            .find("(deny file-read* file-write* (subpath \"/private/cowshed\"))")
            .expect("machine-global cowshed deny");
        let root_traversal = profile
            .find("(allow file-read* (literal \"/Users/tester/Dev/.cowshed-mounts\"))")
            .expect("mount-root traversal carve-back");
        let own_read = profile
            .find("(allow file-read* (subpath \"/Users/tester/Dev/.cowshed-mounts/acme/widget/raven\"))")
            .expect("own workspace read carve-back");
        let own_write = profile
            .find("(allow file-write* (subpath \"/Users/tester/Dev/.cowshed-mounts/acme/widget/raven\"))")
            .expect("own workspace write carve-back");
        assert!(global_deny < root_traversal);
        assert!(root_deny < root_traversal);
        assert!(root_deny < own_read);
        assert!(root_deny < own_write);
        assert!(
            !profile.contains("(deny file-read* file-write* (subpath \"/Users/tester/.cowshed\"))")
        );
        assert!(!profile.contains(
            "(allow file-read* (subpath \"/Users/tester/Dev/.cowshed-mounts/acme/widget/swift\"))"
        ));
    }

    /// Reads under HOME deny by default: the deny follows the broad system read and precedes
    /// every carve-back — an explicit grant, the workspace's own mount, a detected capability's
    /// grant — so last-match-wins admits each of those and nothing else. The capability grants
    /// here have the detectors' shapes: Bun's still-unshared install cache, a bootstrap probe
    /// through the Nix profile link, and an installed tool's link in cowshed's support directory.
    #[test]
    fn home_reads_deny_by_default_and_every_carve_back_follows_the_deny() {
        let home = Path::new("/Users/tester");
        let installed = crate::capabilities::mbx::host_link(home);
        let mut config = with_contribution(
            vec![
                grant(
                    "/Users/tester/.bun/install/cache",
                    GrantScope::Subtree,
                    GrantAccess::Read,
                ),
                grant(
                    "/Users/tester/.nix-profile/bin/direnv",
                    GrantScope::Literal,
                    GrantAccess::Read,
                ),
                CapabilityGrant {
                    path: installed.clone(),
                    scope: GrantScope::Literal,
                    access: GrantAccess::Read,
                },
            ],
            &[],
        );
        config.grants.read = vec![PathBuf::from("/Users/tester/Dev/sibling")];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let broad = profile
            .find("(allow file-read-data (subpath \"/\"))")
            .unwrap();
        let home_deny = profile
            .find("(deny file-read* (subpath \"/Users/tester\"))\n(deny file-read-data (subpath \"/Users/tester\"))\n")
            .expect("HOME read deny, data reads included");
        assert!(broad < home_deny);
        for carve_back in [
            "(allow file-read* (subpath \"/Users/tester/Dev/sibling\"))",
            "(allow file-read* (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount\"))",
            "(allow file-read* (subpath \"/Users/tester/.bun/install/cache\"))",
            "(allow file-read-metadata (literal \"/Users/tester/.nix-profile\"))",
            "(allow file-read* (literal \"/Users/tester/.nix-profile/bin/direnv\"))",
            "(allow file-read* (literal \"/Users/tester/Library/Application Support/dev.cowshed/bin/mbx\"))",
        ] {
            let at = profile
                .find(carve_back)
                .unwrap_or_else(|| panic!("missing {carve_back}"));
            assert!(home_deny < at, "{carve_back} must follow the HOME deny");
        }
        // A capability grant's ancestors beneath HOME resolve but do not list.
        for ancestor in [
            "/Users/tester/Library/Application Support",
            "/Users/tester/.nix-profile",
            "/Users/tester/.bun",
            "/Users/tester/.bun/install",
        ] {
            assert!(profile.contains(&format!(
                "(allow file-read-metadata (literal \"{ancestor}\"))"
            )));
            assert!(
                !profile.contains(&format!("(allow file-read* (literal \"{ancestor}\"))")),
                "{ancestor} must not be listable"
            );
        }
        // Controller-only Git discovery never had the broad read, so it needs no HOME deny.
        let discovery = seatbelt_profile(&config, SandboxProfileRole::GitDiscovery)
            .unwrap()
            .into_text();
        assert!(!discovery.contains("(allow file-read-data (subpath \"/\"))"));
    }

    /// A capability grant only ever narrows the HOME-wide deny: HOME itself is refused, and so is
    /// a grant inside a protected path. One that encloses a secret is harmless — the secret
    /// denies are the profile's last word — and one outside HOME (the Nix store) is the
    /// detector's to make.
    #[test]
    fn home_reads_stay_strictly_beneath_home_and_outside_protected_paths() {
        for (read, expected) in [
            (
                grant("/Users/tester", GrantScope::Subtree, GrantAccess::Read),
                Err(SandboxError::InvalidPath {
                    path: PathBuf::from("/Users/tester"),
                    reason: "a capability grant beneath HOME must be strictly beneath it",
                }),
            ),
            (
                grant("/Users/tester", GrantScope::Literal, GrantAccess::Read),
                Err(SandboxError::InvalidPath {
                    path: PathBuf::from("/Users/tester"),
                    reason: "a capability grant beneath HOME must be strictly beneath it",
                }),
            ),
            (
                grant(
                    "/Users/tester/.ssh/keys",
                    GrantScope::Subtree,
                    GrantAccess::Read,
                ),
                Err(SandboxError::GrantIntersectsDeny {
                    grant: PathBuf::from("/Users/tester/.ssh/keys"),
                    deny: PathBuf::from("/Users/tester/.ssh"),
                }),
            ),
            (
                grant(
                    "/Users/tester/.cowshed/mnt/acme",
                    GrantScope::Subtree,
                    GrantAccess::Read,
                ),
                Err(SandboxError::GrantIntersectsDeny {
                    grant: PathBuf::from("/Users/tester/.cowshed/mnt/acme"),
                    deny: PathBuf::from("/Users/tester/.cowshed/mnt"),
                }),
            ),
            // Enclosing a secret is harmless: the secret denies are the profile's last word.
            (
                grant(
                    "/Users/tester/.cargo",
                    GrantScope::Subtree,
                    GrantAccess::Read,
                ),
                Ok(()),
            ),
            (
                grant("/nix/store", GrantScope::Subtree, GrantAccess::Read),
                Ok(()),
            ),
        ] {
            let config = with_contribution(vec![read], &[]);
            assert_eq!(validate_sandbox_config(&config), expected);
        }
    }

    /// Main's checkout sits at the operator's own path, outside the mount root, so the sibling
    /// deny never reached it. The production builder denies it by name to every other
    /// workspace, in the same shape as a sibling's deny, and never to main itself.
    #[test]
    fn the_workspace_builder_denies_main_to_every_workspace_but_main() {
        let home = Path::new("/Users/tester");
        let mount_root = Path::new("/Users/tester/.cowshed/mnt");
        let main = Path::new("/Users/tester/Dev/widget");
        let grants = crate::metadata::GrantSet {
            port_block: Some(PortBlock::new(40_960, 16).unwrap()),
            ..crate::metadata::GrantSet::default()
        };
        let build = |mount: &Path| {
            workspace_sandbox(WorkspaceSandbox {
                home,
                mount_root,
                project_root: Path::new("/private/cowshed/store/projects/acme"),
                main_mount: main,
                telemetry_root: Path::new("/private/cowshed/store/telemetry"),
                grants: &grants,
                repository_deny: &[],
                repository_caches: &[],
                program_directories: &[],
                git_worktree_repository: None,
                build_volume_mount: None,
                workspace_mount: mount.to_path_buf(),
                exec_temp_dir: PathBuf::from("/private/tmp/cowshed-raven"),
                store_root: Path::new("/private/tmp/cowshed-raven-store"),
                workspace_incarnation: &crate::metadata::WorkspaceIncarnation::new(
                    "0123456789abcdef0123456789abcdef",
                )
                .unwrap(),
            })
            .unwrap()
        };
        let main_deny = "(deny file-read* file-write* (literal \"/Users/tester/Dev/widget\") (subpath \"/Users/tester/Dev/widget\"))";

        let raven = build(&mount_root.join("acme/widget/raven"));
        assert!(raven.additional_denies.contains(&main.to_path_buf()));
        let profile = seatbelt_profile(&raven, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let deny = profile.find(main_deny).expect("main denied like a sibling");
        let own = profile
            .find("(allow file-read* (subpath \"/Users/tester/.cowshed/mnt/acme/widget/raven\"))")
            .unwrap();
        assert!(own < deny, "no workspace carve-back may follow main's deny");
        // An explicit grant into main is refused rather than silently shadowed.
        let mut granted = raven.clone();
        granted.grants.read.push(main.join("docs"));
        assert_eq!(
            validate_sandbox_config(&granted),
            Err(SandboxError::GrantIntersectsDeny {
                grant: main.join("docs"),
                deny: main.to_path_buf(),
            })
        );

        let itself = build(main);
        assert!(!itself.additional_denies.contains(&main.to_path_buf()));
        let profile = seatbelt_profile(&itself, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(!profile.contains(main_deny));
        assert!(profile.contains("(allow file-read* (subpath \"/Users/tester/Dev/widget\"))"));
    }

    /// A workspace-relative deny closes reads and writes beneath a path of the job's own mount:
    /// the read+write deny (with its file-read-data twin) and the ancestor unlink denies follow
    /// every mount allow, the same shape a write-only deny has.
    #[test]
    fn a_workspace_relative_deny_closes_reads_and_writes_after_every_mount_allow() {
        let mount = "/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount";
        let mut config = config(RunSandboxMode::ReadWrite);
        config.grants.deny = vec![PathBuf::from(".runtime/secrets")];
        config.grants.deny_write = vec![PathBuf::from(".git/hooks")];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let target = format!("{mount}/.runtime/secrets");
        let deny = profile
            .find(&format!(
                "(deny file-read* file-write* (literal \"{target}\") (subpath \"{target}\"))\n(deny file-read-data (literal \"{target}\") (subpath \"{target}\"))\n"
            ))
            .expect("read+write deny with its data twin");
        for ancestor in [".runtime", ".runtime/secrets"] {
            let unlink = profile
                .find(&format!(
                    "(deny file-write-unlink (literal \"{mount}/{ancestor}\"))"
                ))
                .unwrap_or_else(|| panic!("{ancestor} cannot be renamed away"));
            assert!(unlink < deny);
        }
        for allow in [
            format!("(allow file-read* (subpath \"{mount}\"))"),
            format!("(allow file-write* (subpath \"{mount}\"))"),
        ] {
            assert!(profile.find(&allow).unwrap() < deny, "{allow} must precede");
        }
        // The write-only deny keeps its own, narrower shape.
        assert!(profile.contains(&format!(
            "(deny file-write* (literal \"{mount}/.git/hooks\") (subpath \"{mount}/.git/hooks\"))"
        )));
        assert!(!profile.contains(&format!(
            "(deny file-read* file-write* (literal \"{mount}/.git/hooks\")"
        )));

        for escaping in ["../main", "/etc", ""] {
            config.grants.deny = vec![PathBuf::from(escaping)];
            assert_eq!(
                validate_sandbox_config(&config),
                Err(SandboxError::InvalidPath {
                    path: PathBuf::from(escaping),
                    reason: "workspace deny must be relative without traversal",
                })
            );
        }
    }

    /// Main's `[sandbox] deny` joins the workspace's own granted denies, as one sorted set.
    #[test]
    fn the_workspace_builder_unions_granted_and_repository_denies() {
        let grants = crate::metadata::GrantSet {
            port_block: Some(PortBlock::new(40_960, 16).unwrap()),
            deny: vec![PathBuf::from("b"), PathBuf::from("a")],
            ..crate::metadata::GrantSet::default()
        };
        let config = workspace_sandbox(WorkspaceSandbox {
            home: Path::new("/Users/tester"),
            mount_root: Path::new("/Users/tester/.cowshed/mnt"),
            project_root: Path::new("/private/cowshed/store/projects/acme"),
            main_mount: Path::new("/Users/tester/Dev/widget"),
            telemetry_root: Path::new("/private/cowshed/store/telemetry"),
            grants: &grants,
            repository_deny: &[PathBuf::from("c"), PathBuf::from("a")],
            repository_caches: &[],
            program_directories: &[],
            git_worktree_repository: None,
            build_volume_mount: None,
            workspace_mount: PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/raven"),
            exec_temp_dir: PathBuf::from("/private/tmp/cowshed-raven"),
            store_root: Path::new("/private/tmp/cowshed-raven-store"),
            workspace_incarnation: &crate::metadata::WorkspaceIncarnation::new(
                "0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
        })
        .unwrap();
        assert_eq!(
            config.grants.deny,
            [PathBuf::from("a"), PathBuf::from("b"), PathBuf::from("c")]
        );
    }

    /// The builder resolves a bootstrap tool only in the program directories it is given
    /// (15_capabilities.md, "Detection inputs"): a bash planted in the one given directory is the
    /// bash the workspace links, and with none given no bash is linked at all, though a macOS host
    /// keeps its own in `/bin`. Every grant detection takes lies inside the fixture, so the build
    /// resolved nothing in the host's own program directories.
    #[test]
    fn the_workspace_builder_resolves_programs_only_in_the_given_directories() {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "cowshed-program-directories",
        );
        let home = root.join("home");
        let mount_root = root.join("mnt");
        let mount = mount_root.join("acme/widget/raven");
        let bin = root.join("bin");
        for directory in [&home, &mount, &bin] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let bash = bin.join("bash");
        std::fs::write(&bash, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&bash, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        let grants = crate::metadata::GrantSet {
            port_block: Some(PortBlock::new(40_960, 16).unwrap()),
            ..crate::metadata::GrantSet::default()
        };
        let incarnation =
            crate::metadata::WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef").unwrap();
        let build = |program_directories: &[PathBuf]| {
            workspace_sandbox(WorkspaceSandbox {
                home: &home,
                mount_root: &mount_root,
                project_root: &root.join("project"),
                main_mount: &root.join("main"),
                telemetry_root: &root.join("telemetry"),
                grants: &grants,
                repository_deny: &[],
                repository_caches: &[],
                program_directories,
                git_worktree_repository: None,
                build_volume_mount: None,
                workspace_mount: mount.clone(),
                exec_temp_dir: root.join("tmp"),
                store_root: &root.join("store"),
                workspace_incarnation: &incarnation,
            })
            .unwrap()
        };
        let assert_inside = |config: &SandboxConfig| {
            for grant in &config.capabilities.contribution.grants {
                assert!(
                    grant.path.starts_with(&*root),
                    "{} lies outside the fixture",
                    grant.path.display()
                );
            }
        };

        let given = build(std::slice::from_ref(&bin));
        assert_eq!(
            given.capabilities.contribution.bootstrap_programs,
            [crate::capabilities::BootstrapProgram {
                name: "bash",
                target: bash,
            }]
        );
        assert_inside(&given);

        let none = build(&[]);
        assert!(
            none.capabilities.contribution.bootstrap_programs.is_empty(),
            "{:?}",
            none.capabilities.contribution.bootstrap_programs
        );
        assert_inside(&none);
    }

    #[test]
    fn shed_link_is_readable_exactly_when_its_target_is_granted() {
        let mut custom = config(RunSandboxMode::ReadWrite);
        custom.mount_root = PathBuf::from("/Users/tester/Dev/.cowshed");
        custom.workspace_mount = PathBuf::from("/Users/tester/Dev/.cowshed/acme/widget/raven");
        custom.grants.read = vec![PathBuf::from("/Users/tester/Dev/sibling")];
        custom.grants.write = vec![PathBuf::from("/opt/output/nested")];
        custom.shed_links = vec![
            ShedLink {
                link: PathBuf::from("/Users/tester/Dev/.cowshed/acme/widget/sibling"),
                target: PathBuf::from("/Users/tester/Dev/sibling"),
            },
            ShedLink {
                link: PathBuf::from("/Users/tester/Dev/.cowshed/acme/widget/output"),
                target: PathBuf::from("/opt/output"),
            },
            ShedLink {
                link: PathBuf::from("/Users/tester/Dev/.cowshed/acme/widget/secrets"),
                target: PathBuf::from("/Users/tester/Dev/elsewhere"),
            },
        ];
        let profile = seatbelt_profile(&custom, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();

        let root_deny = profile
            .find("(deny file-read* file-write* (subpath \"/Users/tester/Dev/.cowshed\"))")
            .expect("mount-root deny");
        let granted_link = profile
            .find("(allow file-read* (literal \"/Users/tester/Dev/.cowshed/acme/widget/sibling\"))")
            .expect("link to a read-granted target is readable");
        let containing_link = profile
            .find("(allow file-read* (literal \"/Users/tester/Dev/.cowshed/acme/widget/output\"))")
            .expect("link whose target contains a write grant is readable");
        assert!(root_deny < granted_link);
        assert!(root_deny < containing_link);
        assert!(profile.contains("(allow file-read* (subpath \"/Users/tester/Dev/sibling\"))"));
        assert!(!profile.contains("/Users/tester/Dev/.cowshed/acme/widget/secrets"));
        assert!(!profile.contains(
            "(allow file-read* (subpath \"/Users/tester/Dev/.cowshed/acme/widget/sibling\"))"
        ));
    }

    #[test]
    fn shed_links_resolve_relative_targets_and_skip_plain_entries() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-shed-links-{}-{}",
            std::process::id(),
            NEXT_SANDBOX_DIR.fetch_add(1, Ordering::SeqCst)
        ));
        let shed = root.join("acme/widget");
        fs::create_dir_all(shed.join("raven")).unwrap();
        fs::create_dir_all(root.join("sibling")).unwrap();
        std::os::unix::fs::symlink("../../sibling", shed.join("sibling")).unwrap();
        std::os::unix::fs::symlink("/opt/absolute", shed.join("absolute")).unwrap();

        let links = shed_links(&shed.join("raven")).unwrap();
        assert_eq!(
            links,
            vec![
                ShedLink {
                    link: shed.join("absolute"),
                    target: PathBuf::from("/opt/absolute"),
                },
                ShedLink {
                    link: shed.join("sibling"),
                    target: root.join("sibling"),
                },
            ]
        );
        assert_eq!(shed_links(&root.join("missing/raven")).unwrap(), Vec::new());
        fs::remove_dir_all(&root).unwrap();
    }

    /// A live workspace's 16-port block and a new workspace's block each get one literal rule
    /// per port of their own recorded size.
    #[test]
    fn profile_is_deterministic_and_has_one_literal_rule_per_block_port() {
        for size in [16, crate::metadata::NEW_PORT_BLOCK_SIZE] {
            let mut config = config(RunSandboxMode::ReadWrite);
            config.port_block = PortBlock::new(40_960, size).unwrap();
            let first = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
                .unwrap()
                .into_text();
            let second = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
                .unwrap()
                .into_text();
            assert_eq!(first, second);
            assert_eq!(
                first
                    .lines()
                    .filter(|line| line.contains("remote tcp \"localhost:"))
                    .count(),
                usize::from(size)
            );
            let last = 40_960 + (size - 1);
            for port in 40_960..=last {
                assert!(first.contains(&format!("remote tcp \"localhost:{port}\"")));
            }
            assert!(!first.contains(&format!("localhost:{}", last + 1)));
            assert!(!first.contains(&format!("localhost:40960-{last}")));
            assert!(!first.contains("example.com"));
        }
    }

    /// A relocated workspace keeps reaching the blocks it still holds, and only those: every
    /// port of the current and each retained block has its literal rule, a neighbouring block's
    /// ports have none, and an invalid retained block refuses the profile.
    #[test]
    fn retained_port_blocks_admit_their_own_ports_and_no_neighbour() {
        let mut config = config(RunSandboxMode::ReadWrite);
        config.port_block = PortBlock::new(41_088, 128).unwrap();
        config.retained_port_blocks = vec![
            PortBlock::new(40_960, 64).unwrap(),
            PortBlock::new(41_024, 16).unwrap(),
        ];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let admitted: Vec<u16> = profile
            .lines()
            .filter_map(|line| {
                line.strip_prefix("(allow network-outbound (remote tcp \"localhost:")?
                    .strip_suffix("\"))")?
                    .parse()
                    .ok()
            })
            .collect();
        let held: Vec<u16> = (41_088..=41_215)
            .chain(40_960..=41_023)
            .chain(41_024..=41_039)
            .collect();
        assert_eq!(admitted, held);
        for neighbour in [40_959, 41_040, 41_087, 41_216] {
            assert!(!profile.contains(&format!("localhost:{neighbour}\"")));
        }
        assert!(
            profile.contains("(allow network-bind network-inbound (local tcp \"localhost:*\"))")
        );

        config.retained_port_blocks.push(PortBlock {
            base: 41_041,
            size: 16,
        });
        assert_eq!(
            seatbelt_profile(&config, SandboxProfileRole::ExecutedChild),
            Err(SandboxError::InvalidPortBlock {
                base: 41_041,
                size: 16
            })
        );
    }

    /// A loopback-any grant admits outbound loopback TCP to every port with one wildcard rule in
    /// place of the held blocks' literal ports, and leaves bind and inbound as they were.
    #[test]
    fn loopback_any_admits_every_loopback_port_in_place_of_the_block_literals() {
        let mut config = config(RunSandboxMode::ReadWrite);
        config.retained_port_blocks = vec![PortBlock::new(41_024, 16).unwrap()];
        config.grants.loopback_any = true;
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let outbound: Vec<&str> = profile
            .lines()
            .filter(|line| line.contains("(remote tcp "))
            .collect();
        assert_eq!(
            outbound,
            ["(allow network-outbound (remote tcp \"localhost:*\"))"]
        );
        assert!(
            profile.contains("(allow network-bind network-inbound (local tcp \"localhost:*\"))")
        );

        config.grants.loopback_any = false;
        let blocks = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(!blocks.contains("(remote tcp \"localhost:*\")"));
        assert!(blocks.contains("(allow network-outbound (remote tcp \"localhost:41024\"))"));
    }

    /// The git-worktree hole, stated as what the profile actually says: narrowed to `.git`, and
    /// last, because the store-wide deny and the project-root deny both cover the path it opens.
    #[test]
    fn git_worktree_repository_carve_back_is_narrow_and_outlives_every_deny() {
        let mut linked = config(RunSandboxMode::ReadWrite);
        let main_mount = PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/main");
        linked.additional_denies = vec![main_mount.clone()];
        linked.git_worktree_repository = Some(main_mount.join(".git"));
        let profile = seatbelt_profile(&linked, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();

        let carve_back = profile
            .find("(allow file-read* file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/main/.git\") (subpath \"/Users/tester/.cowshed/mnt/acme/widget/main/.git\"))")
            .expect("git-worktree repository carve-back");
        let store_deny = profile
            .find("(deny file-read* file-write* (subpath \"/private/cowshed\"))")
            .unwrap();
        let policy_deny = profile
            .rfind("(deny file-read* file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/main\") (subpath \"/Users/tester/.cowshed/mnt/acme/widget/main\"))")
            .expect("policy deny on main's mount");
        // SBPL is last-match-wins, so ordering is the whole enforcement.
        assert!(store_deny < carve_back);
        assert!(policy_deny < carve_back);
        // Main's working tree stays as unreachable as any other workspace's.
        assert!(!profile.contains(
            "(allow file-read* file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/main\") (subpath"
        ));

        // A workspace that did not ask for the mode never gets the hole.
        let standalone = seatbelt_profile(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap()
        .into_text();
        assert!(!standalone.contains("/Users/tester/.cowshed/mnt/acme/widget/main/.git"));
    }

    /// A host service socket under the store-wide deny — a capability's daemon socket, for one —
    /// stays connectable: SBPL is last-match-wins, so the ancestor literals that make the
    /// connect's path resolution work are emitted after the store deny.
    #[test]
    fn an_admitted_socket_under_the_store_deny_stays_connectable() {
        let mut with_socket = config(RunSandboxMode::ReadWrite);
        let socket = PathBuf::from("/private/cowshed/store/daemon.sock");
        with_socket.allowed_unix_sockets.push(socket.clone());
        let profile = seatbelt_profile(&with_socket, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(profile.contains(
            "(allow network-outbound (remote unix-socket (path-literal \"/private/cowshed/store/daemon.sock\")))"
        ));
        let store_deny = profile
            .find("(deny file-read* file-write* (subpath \"/private/cowshed\"))")
            .unwrap();
        let socket_literal = profile
            .rfind("(allow file-read* (literal \"/private/cowshed/store/daemon.sock\"))")
            .expect("socket path literal");
        assert!(store_deny < socket_literal);
    }

    fn grant(path: &str, scope: GrantScope, access: GrantAccess) -> CapabilityGrant {
        CapabilityGrant {
            path: PathBuf::from(path),
            scope,
            access,
        }
    }

    fn with_contribution(grants: Vec<CapabilityGrant>, shared: &[&str]) -> SandboxConfig {
        let mut config = config(RunSandboxMode::ReadWrite);
        config.capabilities.contribution.grants = grants;
        config.capabilities.contribution.shared_caches = shared
            .iter()
            .map(|path| crate::capabilities::SharedCache {
                path: PathBuf::from(path),
                private_link: None,
                access: crate::capabilities::GrantAccess::ReadWrite,
            })
            .collect();
        config
    }

    /// A plain repository detects no capability, and its profile names no tool: no shared cache
    /// is writable and nothing in a host tool home is readable by grant.
    #[test]
    fn without_capabilities_no_tool_cache_or_tool_home_is_granted() {
        let profile = seatbelt_profile(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap()
        .into_text();
        let allows: Vec<&str> = profile
            .lines()
            .filter(|line| line.starts_with("(allow"))
            .collect();
        for home in [".cargo", ".bun", ".cache", ".rustup", ".gradle", "go"] {
            let named = format!("\"/Users/tester/{home}");
            assert!(
                !allows.iter().any(|line| line.contains(&named)),
                "{home} is granted without a capability"
            );
        }
    }

    /// A capability's shared caches are read-write subtrees with metadata-only ancestors beneath
    /// HOME, its grants are emitted exactly as contributed — a literal as a literal with its
    /// ancestors, a subtree as a subtree — and every secret deny still follows them.
    #[test]
    fn a_contribution_is_emitted_exactly_and_the_secret_denies_follow_it() {
        let config = with_contribution(
            vec![
                grant(
                    "/Users/tester/.cargo",
                    GrantScope::Literal,
                    GrantAccess::Read,
                ),
                grant(
                    "/Users/tester/.cargo/.package-cache",
                    GrantScope::Literal,
                    GrantAccess::ReadWrite,
                ),
                grant(
                    "/Users/tester/.cargo/bin",
                    GrantScope::Subtree,
                    GrantAccess::Read,
                ),
            ],
            &["/Users/tester/.cargo/registry"],
        );
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(profile.contains(
            "(allow file-read* file-write* (subpath \"/Users/tester/.cargo/registry\"))"
        ));
        assert!(profile.contains("(allow file-read-metadata (literal \"/Users/tester/.cargo\"))"));
        assert!(!profile.contains("(subpath \"/Users/tester/.cargo/git\")"));
        for literal in ["/Users/tester", "/Users/tester/.cargo"] {
            assert!(profile.contains(&format!("(allow file-read* (literal \"{literal}\"))")));
        }
        assert!(profile.contains(
            "(allow file-read* file-write* (literal \"/Users/tester/.cargo/.package-cache\"))"
        ));
        assert!(profile.contains("(allow file-read* (subpath \"/Users/tester/.cargo/bin\"))"));
        assert!(!profile.contains("(subpath \"/Users/tester/.cargo\")"));
        let last_grant = profile
            .rfind("(literal \"/Users/tester/.cargo/.package-cache\")")
            .unwrap();
        for denied in ["config.toml", "config", "credentials.toml", "credentials"] {
            let path = format!("/Users/tester/.cargo/{denied}");
            let deny = profile
                .rfind(&format!(
                    "(deny file-read* file-write* (literal \"{path}\") (subpath \"{path}\"))"
                ))
                .unwrap_or_else(|| panic!("{denied} must stay denied"));
            assert!(last_grant < deny, "{denied} deny must outlive the grants");
        }
    }

    /// A grant collides only with a deny at or above it: a tool home beside or above its denied
    /// configuration stays legal (the denies follow every grant), and a grant on or under a
    /// denied path is refused.
    #[test]
    fn capability_grants_never_reach_a_hard_deny() {
        for allowed in [
            grant(
                "/Users/tester/.cargo",
                GrantScope::Literal,
                GrantAccess::Read,
            ),
            grant(
                "/Users/tester/.cargo",
                GrantScope::Subtree,
                GrantAccess::Read,
            ),
            grant(
                "/Users/tester/.cargo/bin",
                GrantScope::Subtree,
                GrantAccess::Read,
            ),
        ] {
            let config = with_contribution(vec![allowed.clone()], &[]);
            validate_sandbox_config(&config).unwrap_or_else(|error| panic!("{allowed:?}: {error}"));
        }
        for (refused, deny) in [
            (
                grant(
                    "/Users/tester/.cargo/credentials.toml",
                    GrantScope::Literal,
                    GrantAccess::Read,
                ),
                "/Users/tester/.cargo/credentials.toml",
            ),
            (
                grant(
                    "/Users/tester/.ssh/id_ed25519",
                    GrantScope::Literal,
                    GrantAccess::Read,
                ),
                "/Users/tester/.ssh",
            ),
        ] {
            let config = with_contribution(vec![refused.clone()], &[]);
            assert_eq!(
                validate_sandbox_config(&config),
                Err(SandboxError::GrantIntersectsDeny {
                    grant: refused.path.clone(),
                    deny: PathBuf::from(deny),
                })
            );
        }
    }

    /// A shared cache is a subtree strictly beneath HOME that reaches into no hard deny and no
    /// cowshed controller state in either direction: a credential, another workspace, or the
    /// gateway's mirrors never become writable through one.
    #[test]
    fn shared_caches_stay_beneath_home_and_clear_every_protected_path() {
        for allowed in [
            "/Users/tester/.cargo/registry",
            "/Users/tester/.cache/ttsc",
            "/Users/tester/go/pkg/mod",
        ] {
            let config = with_contribution(Vec::new(), &[allowed]);
            validate_sandbox_config(&config).unwrap_or_else(|error| panic!("{allowed}: {error}"));
        }
        for outside in ["/Users/tester", "/private/tmp/cache"] {
            let config = with_contribution(Vec::new(), &[outside]);
            assert!(
                matches!(
                    validate_sandbox_config(&config),
                    Err(SandboxError::InvalidPath { ref path, .. }) if path == Path::new(outside)
                ),
                "{outside} must be refused"
            );
        }
        for (refused, deny) in [
            ("/Users/tester/.cargo", "/Users/tester/.cargo/config"),
            ("/Users/tester/.ssh/cache", "/Users/tester/.ssh"),
            ("/Users/tester/.cowshed", "/Users/tester/.cowshed/mnt"),
            ("/Users/tester/Library", "/Users/tester/Library/Keychains"),
            // macOS's layout: elsewhere cowshed's cache is `$XDG_CACHE_HOME/cowshed` and
            // `~/Library/Caches` is nothing of ours.
            #[cfg(target_os = "macos")]
            (
                "/Users/tester/Library/Caches",
                "/Users/tester/Library/Caches/dev.cowshed",
            ),
            (
                "/Users/tester/Library/Application Support/dev.cowshed/nix",
                "/Users/tester/Library/Application Support/dev.cowshed",
            ),
        ] {
            let config = with_contribution(Vec::new(), &[refused]);
            assert_eq!(
                validate_sandbox_config(&config),
                Err(SandboxError::GrantIntersectsDeny {
                    grant: PathBuf::from(refused),
                    deny: PathBuf::from(deny),
                }),
                "{refused}"
            );
        }
    }

    /// Cargo/libgit2 canonicalizes a newly fetched git cache path, so a shared cache's ancestors
    /// beneath HOME resolve after the HOME read deny without an unrelated socket grant: metadata
    /// only, never a listing of the tool's home.
    #[test]
    fn shared_cache_ancestors_resolve_without_reading_their_home() {
        let config = with_contribution(Vec::new(), &["/Users/tester/.cargo/git"]);
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let deny = profile
            .find("(deny file-read* (subpath \"/Users/tester\"))")
            .expect("HOME read deny");
        let metadata = profile
            .rfind("(allow file-read-metadata (literal \"/Users/tester/.cargo\"))")
            .expect("a tool must canonicalize its cache path without an unrelated socket grant");
        assert!(
            metadata > deny,
            "cache ancestor metadata follows the HOME read deny"
        );
        assert!(!profile.contains("(allow file-read* (literal \"/Users/tester/.cargo\"))"));
        assert!(
            profile
                .contains("(allow file-read* file-write* (subpath \"/Users/tester/.cargo/git\"))")
        );
    }

    #[test]
    fn read_only_removes_only_workspace_write_carve_back() {
        let read_write = seatbelt_profile(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap()
        .into_text();
        let read_only = seatbelt_profile(
            &config(RunSandboxMode::ReadOnly),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap()
        .into_text();
        let workspace_write = "(allow file-write* (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount\"))";
        assert!(read_write.contains(workspace_write));
        assert!(!read_only.contains(workspace_write));
        assert!(read_only.contains("(allow file-read* (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount\"))"));
    }

    #[test]
    fn profile_allows_system_tool_metadata_and_exact_workspace_ancestors() {
        let profile = seatbelt_profile(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap()
        .into_text();
        assert!(profile.contains(
            "(allow file-read* (literal \"/Applications\") (subpath \"/Applications\"))"
        ));
        assert!(profile.contains("(allow file-read* (literal \"/usr\") (subpath \"/usr\"))"));
        assert!(!profile.contains("(allow file-read* (literal \"/Users\") (subpath \"/Users\"))"));

        let store_deny = profile
            .find("(deny file-read* file-write* (subpath \"/private/cowshed\"))")
            .unwrap();
        let mount_parent = profile
            .find(
                "(allow file-read* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven\"))",
            )
            .unwrap();
        let secret_deny = profile.rfind("/Users/tester/.ssh").unwrap();
        assert!(store_deny < mount_parent);
        assert!(mount_parent < secret_deny);
    }

    #[test]
    fn nix_store_reads_are_baseline_authority_without_nix_state_or_writes() {
        for mode in [RunSandboxMode::ReadOnly, RunSandboxMode::ReadWrite] {
            for role in [
                SandboxProfileRole::TrustedSupervisor,
                SandboxProfileRole::ExecutedChild,
                SandboxProfileRole::GitDiscovery,
            ] {
                let profile = seatbelt_profile(&config(mode), role).unwrap().into_text();
                for operation in ["file-read*", "file-read-data"] {
                    assert!(profile.contains(&format!(
                        "(allow {operation} (literal \"/nix/store\") (subpath \"/nix/store\"))"
                    )));
                }
                assert!(profile.contains("(allow file-read* (literal \"/nix\"))"));
                for root in ["/nix", "/nix/var"] {
                    assert!(
                        !profile.contains(&format!("(subpath \"{root}\")")),
                        "{mode:?} {role:?} must not admit Nix state as a baseline subtree"
                    );
                }
                assert!(
                    profile
                        .lines()
                        .filter(|line| line.contains("file-write"))
                        .all(|line| !line.contains("\"/nix")),
                    "{mode:?} {role:?} must not grant Nix writes"
                );
            }
        }
    }

    #[test]
    fn secret_denies_follow_grants_and_carve_backs() {
        let profile = seatbelt_profile(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap()
        .into_text();
        let grant = profile.find("/opt/shared").unwrap();
        let carve_back = profile.rfind("allow file-write*").unwrap();
        let secret = profile.rfind("/Users/tester/.ssh").unwrap();
        assert!(grant < secret);
        assert!(carve_back < secret);
    }

    // The Seatbelt rule-ordering check and its parsers are macOS-only: Seatbelt is macOS's
    // sandbox (Linux runs unsandboxed, 04_sandbox.md), and rendering the git roles resolves the
    // host's active developer directory, which only macOS has.
    #[cfg(target_os = "macos")]
    /// One rendered rule: whether it allows or denies, the file operations it names, and the
    /// `(kind "path")` filters it holds.
    struct FileRule<'a> {
        allow: bool,
        operations: Vec<&'a str>,
        filters: Vec<(&'a str, &'a str)>,
    }

    #[cfg(target_os = "macos")]
    fn file_rule(line: &str) -> Option<FileRule<'_>> {
        let (allow, rest) = line
            .strip_prefix("(allow ")
            .map(|rest| (true, rest))
            .or_else(|| line.strip_prefix("(deny ").map(|rest| (false, rest)))?;
        let operations = rest
            .split_whitespace()
            .take_while(|token| !token.starts_with('('))
            .filter(|operation| operation.starts_with("file-"))
            .collect::<Vec<_>>();
        let mut filters = Vec::new();
        for kind in ["subpath", "literal", "path-prefix"] {
            let opener = format!("({kind} \"");
            let mut text = rest;
            while let Some(start) = text.find(&opener) {
                let path = &text[start + opener.len()..];
                let end = path.find('"').expect("a quoted filter path closes");
                filters.push((kind, &path[..end]));
                text = &path[end..];
            }
        }
        Some(FileRule {
            allow,
            operations,
            filters,
        })
    }

    /// `file-read*` covers `file-read-data`; each operation covers itself.
    #[cfg(target_os = "macos")]
    fn operations_overlap(allow: &[&str], deny: &[&str]) -> bool {
        let covers = |wide: &str, narrow: &str| {
            wide == narrow
                || wide
                    .strip_suffix('*')
                    .is_some_and(|stem| narrow.starts_with(stem))
        };
        allow
            .iter()
            .any(|a| deny.iter().any(|d| covers(a, d) || covers(d, a)))
    }

    /// Whether every path `allow` matches lies within what `deny` matches: an allow beside or
    /// above a deny is the broad rule the deny narrows, one at or beneath it is undone by it.
    #[cfg(target_os = "macos")]
    fn filter_within((allow_kind, allow): (&str, &str), (deny_kind, deny): (&str, &str)) -> bool {
        match deny_kind {
            "subpath" => allow == deny || allow.starts_with(&format!("{deny}/")),
            "path-prefix" => allow.starts_with(deny),
            _ => allow == deny && allow_kind != "path-prefix",
        }
    }

    /// SBPL is last-match-wins, so an allow at or beneath a later deny of an overlapping file
    /// operation is dead: the deny re-denies it. The CPU pool directory's jobserver fifo rule sat
    /// before the `/private/cowshed` deny and Cargo inside every sandbox ran without its
    /// jobserver. Rendered with every carve-back under a protected root that a workspace has —
    /// its pool directory, a store socket, a store temp dir — for every role and mode, no such
    /// allow precedes any deny.
    #[cfg(target_os = "macos")]
    #[test]
    fn no_allow_precedes_a_deny_that_covers_it() {
        let pool = crate::runtime::job_cpu::cpu_pool_directory(
            Path::new("/private/cowshed/store"),
            &crate::metadata::WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .unwrap(),
        );
        let fifos = format!(
            "(allow file-read-data file-write-data (require-all (subpath \"{}\") (vnode-type FIFO)))",
            pool.display()
        );
        for mode in [RunSandboxMode::ReadOnly, RunSandboxMode::ReadWrite] {
            let mut config = config(mode);
            config.cpu_pool_directory = Some(pool.clone());
            config
                .allowed_unix_sockets
                .push(PathBuf::from("/private/cowshed/store/daemon.sock"));
            config.exec_temp_dir = PathBuf::from("/private/cowshed/store/acme/widget/tmp/raven");
            for role in [
                SandboxProfileRole::TrustedSupervisor,
                SandboxProfileRole::ExecutedChild,
                SandboxProfileRole::GitDiscovery,
                SandboxProfileRole::GitDiscoveryGitOnly,
            ] {
                let profile = seatbelt_profile(&config, role).unwrap().into_text();
                let rules = profile.lines().filter_map(file_rule).collect::<Vec<_>>();
                assert!(
                    rules.iter().any(|rule| !rule.allow
                        && rule.filters.contains(&("subpath", "/private/cowshed"))),
                    "{mode:?} {role:?} denies the protected store root"
                );
                let mut dead = Vec::new();
                for (index, deny) in rules.iter().enumerate().filter(|(_, rule)| !rule.allow) {
                    for allow in rules[..index].iter().filter(|rule| rule.allow) {
                        if operations_overlap(&allow.operations, &deny.operations)
                            && allow.filters.iter().any(|allowed| {
                                deny.filters
                                    .iter()
                                    .any(|denied| filter_within(*allowed, *denied))
                            })
                        {
                            dead.push(format!(
                                "{:?} {:?} before {:?}",
                                allow.operations, allow.filters, deny.filters
                            ));
                        }
                    }
                }
                assert!(dead.is_empty(), "{mode:?} {role:?}: {dead:#?}");
                assert!(
                    profile.contains(&fifos),
                    "{mode:?} {role:?} opens the pool's fifos"
                );
            }
        }
    }

    #[test]
    fn controller_validation_rejects_secret_and_workspace_grants() {
        for grant in [
            "/Users/tester",
            "/Users/tester/.ssh/id_ed25519",
            "/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/data",
        ] {
            let mut config = config(RunSandboxMode::ReadWrite);
            config.grants.read = vec![PathBuf::from(grant)];
            assert!(matches!(
                validate_sandbox_config(&config),
                Err(SandboxError::GrantIntersectsDeny { .. })
            ));
            assert!(matches!(
                seatbelt_profile(&config, SandboxProfileRole::ExecutedChild),
                Err(SandboxError::GrantIntersectsDeny { .. })
            ));
        }
    }

    #[test]
    fn executed_child_is_a_terminal_narrowing_of_the_supervisor() {
        let config = config(RunSandboxMode::ReadOnly);
        let supervisor = seatbelt_profile(&config, SandboxProfileRole::TrustedSupervisor)
            .unwrap()
            .into_text();
        let child = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let protected_allow = "(allow file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\") (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\"))";
        let ancestor_deny = "(deny file-write-create file-write-unlink (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed\"))";
        let protected_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\") (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\"))";
        let token_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/token\"))";
        let environment_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/env\"))";

        assert_eq!(supervisor.lines().last(), Some(protected_allow));
        assert!(!supervisor.contains(ancestor_deny));
        assert!(!supervisor.contains(protected_deny));
        let tool_deny = "(deny file-write* (literal \"/private/tmp/cowshed-raven/tools\") (subpath \"/private/tmp/cowshed-raven/tools\"))";
        assert_eq!(child.lines().last(), Some(tool_deny));
        assert!(child.rfind("(allow ").unwrap() < child.find(ancestor_deny).unwrap());
        assert!(supervisor.contains(token_deny));
        assert!(child.contains(token_deny));
        assert!(supervisor.contains(environment_deny));
        assert!(child.contains(environment_deny));
        assert!(child.find("allow file-write*").unwrap() < child.find(token_deny).unwrap());

        let common_supervisor = supervisor
            .strip_suffix(&format!("{protected_allow}\n"))
            .unwrap();
        let child_suffix = format!("{ancestor_deny}\n{protected_deny}\n{tool_deny}\n");
        let common_child = child.strip_suffix(&child_suffix).unwrap();
        assert_eq!(common_child, common_supervisor);
    }

    #[test]
    fn protected_artifacts_cannot_be_regranted_or_aliased() {
        let mut config = config(RunSandboxMode::ReadWrite);
        let protected_stream = config.workspace_mount.join(".cowshed/job/7/out");
        config.grants.write.push(protected_stream.clone());

        assert!(matches!(
            seatbelt_profile(&config, SandboxProfileRole::ExecutedChild),
            Err(SandboxError::GrantIntersectsDeny { grant, .. })
                if grant == protected_stream
        ));

        config.grants.write.pop();
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(profile.lines().any(|line| line == "(deny file-link)"));
    }

    #[test]
    fn port_block_is_exact_and_cannot_overflow() {
        assert!(PortBlock::new(40_960, 15).is_err());
        assert!(PortBlock::new(u16::MAX - 14, 16).is_err());
        for size in [16, crate::metadata::NEW_PORT_BLOCK_SIZE] {
            assert_eq!(
                PortBlock::new(40_960, size)
                    .unwrap()
                    .ports()
                    .unwrap()
                    .count(),
                usize::from(size)
            );
        }
    }

    /// Stores allocate port blocks independently, so two live workspaces can share a base; the
    /// link each one's jobs reach their runtime through is named after its own mount, so it
    /// differs between them and never moves for one of them.
    #[test]
    fn a_runtime_link_is_named_after_its_workspace_mount_not_its_port_block() {
        let raven = config(RunSandboxMode::ReadWrite);
        let mut crow = config(RunSandboxMode::ReadWrite);
        crow.workspace_mount = PathBuf::from("/Users/other/.cowshed/mnt/acme/widget/crow");
        assert_eq!(raven.port_block, crow.port_block);
        assert_ne!(sandbox_runtime_link(&raven), sandbox_runtime_link(&crow));
        assert_ne!(
            shared_daemon_runtime_link(&raven),
            shared_daemon_runtime_link(&crow)
        );

        let mut moved = config(RunSandboxMode::ReadWrite);
        moved.port_block = PortBlock::new(49_152, 64).unwrap();
        let link = sandbox_runtime_link(&raven);
        assert_eq!(sandbox_runtime_link(&moved), link);
        assert_eq!(shared_daemon_runtime_link(&raven), link);
        assert_eq!(workspace_runtime_link(&raven.workspace_mount), link);
        let name = link.to_str().unwrap().strip_prefix("/tmp/cs-").unwrap();
        assert!(
            name.len() == 12 && crate::api::dto::is_lowercase_hex(name),
            "{name}"
        );

        let read_only = config(RunSandboxMode::ReadOnly);
        assert_eq!(
            sandbox_runtime_link(&read_only),
            PathBuf::from(format!("{}-ro", link.display()))
        );
    }

    /// Controller Git that holds reads beyond its workspace executes only Git: the profile forks
    /// and executes the developer directory's `git` and `git-core`, and nothing broader.
    #[cfg(target_os = "macos")]
    #[test]
    fn git_only_profile_executes_only_git() {
        let config = config(RunSandboxMode::ReadWrite);
        let profile = seatbelt_profile(&config, SandboxProfileRole::GitDiscoveryGitOnly)
            .unwrap()
            .into_text();
        assert!(!profile.contains("(allow process-exec process-fork)"));
        assert!(profile.contains("(allow process-fork)"));
        let execs = profile
            .lines()
            .filter(|line| line.contains("process-exec"))
            .collect::<Vec<_>>();
        assert!(!execs.is_empty());
        assert!(
            execs.iter().all(|line| line.ends_with("/usr/bin/git\"))")
                || line.ends_with("/usr/libexec/git-core\"))")),
            "{execs:?}"
        );
        let broad = seatbelt_profile(&config, SandboxProfileRole::GitDiscovery)
            .unwrap()
            .into_text();
        assert!(broad.contains("(allow process-exec process-fork)"));
    }

    /// Every role, Git's included, reads the entropy devices; none reaches the per-user directory
    /// helper, whose answers name host directories no job may write
    /// (`host_controller_seatbelt_gives_every_role_entropy` runs both).
    #[test]
    fn every_role_has_entropy_and_no_user_directory_helper() {
        let config = config(RunSandboxMode::ReadWrite);
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
            #[cfg(target_os = "macos")]
            SandboxProfileRole::GitDiscoveryGitOnly,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            for rule in [
                "(allow file-read* (literal \"/dev/random\"))",
                "(allow file-read* (literal \"/dev/urandom\"))",
            ] {
                assert!(profile.contains(rule), "{role:?} lacks {rule}");
            }
            assert!(!profile.contains("com.apple.bsd.dirhelper"), "{role:?}");
        }
    }

    /// Stdio names are links into /dev/fd and Seatbelt matches the resolved spelling: every role
    /// resolves the links by metadata and opens through /dev/fd, and names no link in any other
    /// rule, where it could never match an open (`host_controller_a_child_opens_its_stdio_by_name`
    /// runs it).
    #[test]
    fn every_role_opens_stdio_through_the_descriptor_directory() {
        let config = config(RunSandboxMode::ReadWrite);
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
            #[cfg(target_os = "macos")]
            SandboxProfileRole::GitDiscoveryGitOnly,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            for link in ["/dev/stdin", "/dev/stdout", "/dev/stderr"] {
                let lookup = format!("(allow file-read-metadata (literal \"{link}\"))");
                assert!(profile.contains(&lookup), "{role:?} lacks {lookup}");
                assert_eq!(
                    profile.matches(&format!("(literal \"{link}\")")).count(),
                    1,
                    "{role:?} names {link} beyond its lookup"
                );
            }
            assert!(
                profile.contains("(allow file-read* file-write* (subpath \"/dev/fd\"))"),
                "{role:?} lacks the descriptor directory"
            );
        }
    }

    /// Every role that runs workspace commands executes `/bin/ps` outside the sandbox, after the
    /// broad exec allow, and no other program leaves it. Git's executes-only-Git role keeps it
    /// out with everything but Git (`git_only_profile_executes_only_git`).
    #[test]
    fn only_process_status_leaves_the_sandbox() {
        let config = config(RunSandboxMode::ReadWrite);
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            let lines = profile.lines().collect::<Vec<_>>();
            let escapes = lines
                .iter()
                .enumerate()
                .filter(|(_, line)| line.contains("no-sandbox"))
                .map(|(index, line)| (index, *line))
                .collect::<Vec<_>>();
            let broad = lines
                .iter()
                .position(|line| *line == "(allow process-exec process-fork)")
                .unwrap_or_else(|| panic!("{role:?} executes workspace programs"));
            assert!(
                matches!(escapes.as_slice(), [(index, PROCESS_STATUS_EXEC)] if broad < *index),
                "{role:?}: {escapes:?}"
            );
        }
    }

    /// An admitted socket reaches the profile as an outbound rule with its directory traversable,
    /// or connecting fails on path resolution before the outbound rule is consulted.
    #[test]
    fn an_admitted_socket_is_connectable_and_its_directory_traversable() {
        let socket = PathBuf::from("/private/var/run/daemon-socket/socket");
        let mut config = config(RunSandboxMode::ReadWrite);
        config.allowed_unix_sockets = vec![socket.clone()];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        assert!(profile.contains(
            "(allow network-outbound (remote unix-socket (path-literal \"/private/var/run/daemon-socket/socket\")))"
        ));
        assert!(
            profile.contains("(allow file-read* (literal \"/private/var/run/daemon-socket\"))"),
            "the socket's own directory must be traversable"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_seatbelt_resolves_unix_identity_without_directory_record_authority() {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root_alias = std::env::temp_dir().join(format!(
            "cowshed-identity-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root_alias).unwrap();
        let root = fs::canonicalize(&root_alias).unwrap();
        let mut config = config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.mount_root = root.join("mounts");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.allowed_unix_sockets.clear();
        for path in [&config.home, &config.workspace_mount, &config.exec_temp_dir] {
            fs::create_dir_all(path).unwrap();
        }
        let host_name = std::process::Command::new("/usr/bin/id")
            .arg("-un")
            .output_bounded()
            .unwrap();
        assert!(host_name.status.success(), "{host_name:?}");
        let name = std::str::from_utf8(&host_name.stdout).unwrap().trim();
        let record = format!("/Users/{name}");
        let host_record = std::process::Command::new("/usr/bin/dscl")
            .args([".", "-read", &record, "UniqueID"])
            .output_bounded()
            .unwrap();
        assert!(host_record.status.success(), "{host_record:?}");
        let key = config.workspace_mount.join("identity-key");
        let generated = std::process::Command::new("/usr/bin/ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&key)
            .output_bounded()
            .unwrap();
        assert!(generated.status.success(), "{generated:?}");
        let public_key = key.with_extension("pub");
        let host_fingerprint = std::process::Command::new("/usr/bin/ssh-keygen")
            .arg("-lf")
            .arg(&public_key)
            .output_bounded()
            .unwrap();
        assert!(host_fingerprint.status.success(), "{host_fingerprint:?}");
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            let identity = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/id", "-un"])
                .output_bounded()
                .unwrap();
            assert!(identity.status.success(), "{role:?}: {identity:?}");
            assert_eq!(identity.stdout, host_name.stdout, "{role:?}");
            let fingerprint = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/ssh-keygen", "-lf"])
                .arg(&public_key)
                .output_bounded()
                .unwrap();
            assert!(fingerprint.status.success(), "{role:?}: {fingerprint:?}");
            assert_eq!(fingerprint.stdout, host_fingerprint.stdout, "{role:?}");
            let denied_record = std::process::Command::new("/usr/bin/sandbox-exec")
                .args([
                    "-p",
                    &profile,
                    "--",
                    "/usr/bin/dscl",
                    ".",
                    "-read",
                    &record,
                    "UniqueID",
                ])
                .output_bounded()
                .unwrap();
            assert!(
                !denied_record.status.success(),
                "{role:?} must not acquire directory-record authority: {denied_record:?}"
            );
        }
        fs::remove_dir_all(&root).unwrap();
    }

    /// Every runtime seeds itself from the kernel's entropy before it runs a line. Git runs
    /// merge drivers under the controller's Git profile during `cowshed rebase`, and a Python
    /// driver there died in `_Py_HashRandomization_Init` ("failed to get random numbers"). Every
    /// role reads the entropy devices. The per-user directory helper stays denied: answered, it
    /// names `/var/folders` directories no job may write, and `mktemp` picked them over the
    /// job's own `TMPDIR`.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_seatbelt_gives_every_role_entropy() {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root_alias = std::env::temp_dir().join(format!(
            "cowshed-entropy-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root_alias).unwrap();
        let root = fs::canonicalize(&root_alias).unwrap();
        let mut config = config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.mount_root = root.join("mounts");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.allowed_unix_sockets.clear();
        for path in [&config.home, &config.workspace_mount, &config.exec_temp_dir] {
            fs::create_dir_all(path).unwrap();
        }
        // The interpreter itself, not the `/usr/bin/python3` shim: the shim's own lookups are
        // not what this proves.
        let found = std::process::Command::new("/usr/bin/xcrun")
            .args(["--find", "python3"])
            .output_bounded()
            .unwrap();
        assert!(found.status.success(), "{found:?}");
        let python = String::from_utf8(found.stdout).unwrap().trim().to_owned();
        let run = |profile: &str, argv: &[&str]| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--"])
                .args(argv)
                .current_dir(&config.workspace_mount)
                .output_bounded()
                .unwrap()
        };
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            let seeded = run(
                &profile,
                &[&python, "-c", "import os; print(len(os.urandom(8)))"],
            );
            assert!(
                seeded.status.success() && seeded.stdout == b"8\n",
                "{role:?} entropy: {}",
                String::from_utf8_lossy(&seeded.stderr)
            );
            // The user directories stay unnamed, so a temp file lands where `TMPDIR` says: the
            // job's own temp dir, never the host user's `/var/folders`.
            let named = run(&profile, &["/usr/bin/getconf", "DARWIN_USER_TEMP_DIR"]);
            assert!(!named.status.success(), "{role:?}: {named:?}");
            let made = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/mktemp"])
                .env("TMPDIR", &config.exec_temp_dir)
                .current_dir(&config.workspace_mount)
                .output_bounded()
                .unwrap();
            assert!(
                made.status.success()
                    && Path::new(String::from_utf8_lossy(&made.stdout).trim())
                        .starts_with(&config.exec_temp_dir),
                "{role:?} mktemp: {made:?}"
            );
        }
        fs::remove_dir_all(&root).unwrap();
    }

    /// A job inspects its own processes with `ps`, their start environment included. macOS
    /// refuses a sandboxed exec of a set-user-ID program whatever the profile allows, and
    /// `/bin/ps` is one, so every role runs that one program outside the sandbox; `top`, also
    /// set-user-ID, stays refused.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_seatbelt_runs_process_status_and_no_other_set_user_id_program() {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root_alias = std::env::temp_dir().join(format!(
            "cowshed-process-status-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root_alias).unwrap();
        let root = fs::canonicalize(&root_alias).unwrap();
        let mut config = config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.mount_root = root.join("mounts");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.allowed_unix_sockets.clear();
        for path in [&config.home, &config.workspace_mount, &config.exec_temp_dir] {
            fs::create_dir_all(path).unwrap();
        }
        let marker = format!(
            "COWSHED_PROCESS_STATUS_PROBE={}-{sequence}",
            std::process::id()
        );
        let (name, value) = marker.split_once('=').unwrap();
        let run = |profile: &str, argv: &[&str]| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--"])
                .args(argv)
                .env(name, value)
                .current_dir(&config.workspace_mount)
                .output_bounded()
                .unwrap()
        };
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            // The shell execs `ps` on its own pid: the kernel shows a platform program's start
            // environment to that program alone, and `/bin/sh` and `/bin/ps` are both platform
            // programs. `ps -E` follows the command line with that environment.
            let listed = run(
                &profile,
                &["/bin/sh", "-c", "exec /bin/ps -E -ww -o command= -p $$"],
            );
            let line = String::from_utf8_lossy(&listed.stdout);
            assert!(
                listed.status.success() && line.split_whitespace().any(|word| word == marker),
                "{role:?} ps: {listed:?}"
            );
            let other = run(&profile, &["/usr/bin/top", "-l", "1", "-n", "0"]);
            assert!(
                !other.status.success()
                    && String::from_utf8_lossy(&other.stderr).contains("Operation not permitted"),
                "{role:?} top: {other:?}"
            );
        }
        fs::remove_dir_all(&root).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_seatbelt_enforces_supervisor_and_child_artifact_authority() {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root_alias = std::env::temp_dir().join(format!(
            "cowshed-sandbox-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root_alias).unwrap();
        let root = fs::canonicalize(&root_alias).unwrap();

        let mut config = config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.allowed_unix_sockets.clear();
        let protected = config.workspace_mount.join(".cowshed/job");
        fs::create_dir_all(&config.home).unwrap();
        fs::create_dir_all(&config.exec_temp_dir).unwrap();
        fs::create_dir_all(&protected).unwrap();
        let git_config = crate::workspace_git_fetch::git_fetch_config_path(&config.workspace_mount);
        fs::write(&git_config, b"controller mapping\n").unwrap();
        let git_alias = config.workspace_mount.join("git-config-alias");
        std::os::unix::fs::symlink(&git_config, &git_alias).unwrap();

        let supervisor = seatbelt_profile(&config, SandboxProfileRole::TrustedSupervisor)
            .unwrap()
            .into_text();
        let child = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        let canonical_stream = protected.join("out");
        let child_stream = protected.join("child");
        let workspace_file = config.workspace_mount.join("ordinary");
        let hardlink = config.workspace_mount.join("alias");

        let supervisor_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &supervisor, "--", "/usr/bin/touch"])
            .arg(&canonical_stream)
            .stderr(Stdio::null())
            .status_bounded()
            .unwrap();
        let child_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/bin/touch"])
            .arg(&child_stream)
            .stderr(Stdio::null())
            .status_bounded()
            .unwrap();
        let ordinary_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/bin/touch"])
            .arg(&workspace_file)
            .stderr(Stdio::null())
            .status_bounded()
            .unwrap();
        let hardlink_attempt = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &supervisor, "--", "/bin/ln"])
            .args([&canonical_stream, &hardlink])
            .stderr(Stdio::null())
            .status_bounded()
            .unwrap();

        for path in [&git_config, &git_alias] {
            let write = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &child, "--", "/usr/bin/tee"])
                .arg(path)
                .stdin(Stdio::null())
                .output_bounded()
                .unwrap();
            assert!(
                !write.status.success(),
                "child must not truncate {}",
                path.display()
            );
        }
        let replace_parent = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/bin/mv"])
            .arg(config.workspace_mount.join(".cowshed"))
            .arg(config.workspace_mount.join("moved-metadata"))
            .output_bounded()
            .unwrap();
        assert!(
            !replace_parent.status.success(),
            "child must not replace the protected mapping's ancestor"
        );
        assert_eq!(fs::read(&git_config).unwrap(), b"controller mapping\n");

        fs::remove_dir_all(&root).unwrap();
        assert!(supervisor_write.success());
        assert!(!child_write.success());
        assert!(ordinary_write.success());
        assert!(!hardlink_attempt.success());
    }

    /// An executed child's profile over a fresh canonical root holding its HOME, mount and exec
    /// temp dir. The caller removes the root.
    #[cfg(target_os = "macos")]
    fn host_child_profile(label: &str) -> (PathBuf, SandboxConfig, String) {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root_alias = std::env::temp_dir().join(format!(
            "cowshed-{label}-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root_alias).unwrap();
        let root = fs::canonicalize(&root_alias).unwrap();
        let mut config = config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.allowed_unix_sockets.clear();
        for directory in [&config.home, &config.workspace_mount, &config.exec_temp_dir] {
            fs::create_dir_all(directory).unwrap();
        }
        let child = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild)
            .unwrap()
            .into_text();
        (root, config, child)
    }

    /// A child opens its stdio by name, as usdt's proc macro runs `dtrace -h -s /dev/stdin -o
    /// /dev/stdout`. The names are links into /dev/fd and Seatbelt matches the resolved
    /// spelling, so a profile that names only the links refuses every such open.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_child_opens_its_stdio_by_name() {
        let (root, config, child) = host_child_profile("stdio-names");

        let mut run = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/bin/sh", "-c"])
            .arg("/bin/cat /dev/stdin > /dev/stdout && echo named > /dev/stderr")
            .current_dir(&config.workspace_mount)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .unwrap();
        run.stdin
            .take()
            .unwrap()
            .write_all(b"through the links\n")
            .unwrap();
        let output = run.wait_with_output_bounded().unwrap();
        // usdt's proc macro, verbatim: the provider arrives on stdin, the header leaves on stdout.
        let mut dtrace = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/sbin/dtrace"])
            .args(["-h", "-s", "/dev/stdin", "-o", "/dev/stdout"])
            .current_dir(&config.workspace_mount)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .unwrap();
        dtrace
            .stdin
            .take()
            .unwrap()
            .write_all(b"provider cowshed_probe { probe ping(int); };\n")
            .unwrap();
        let header = dtrace.wait_with_output_bounded().unwrap();

        fs::remove_dir_all(&root).unwrap();
        assert!(
            output.status.success(),
            "stdio by name: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"through the links\n");
        assert_eq!(output.stderr, b"named\n");
        assert!(
            header.status.success(),
            "dtrace -h through stdio names: {}",
            String::from_utf8_lossy(&header.stderr)
        );
        assert!(
            String::from_utf8_lossy(&header.stdout).contains("COWSHED_PROBE_PING"),
            "{}",
            String::from_utf8_lossy(&header.stdout)
        );
    }

    /// A job writes `/tmp` nowhere but a fixture's short root, the sweep lock beside the roots and
    /// the Unix sockets beneath a root: both roles that run workspace commands carry exactly those
    /// two rules in either mode, controller Git's roles carry neither, and no write rule names
    /// `/tmp` itself. The rules follow the store deny and precede the secret denies, so
    /// last-match-wins leaves them as emitted.
    #[test]
    fn the_fixture_namespace_is_the_only_tmp_write_and_only_workspace_commands_carry_it() {
        let files = "(allow file-read* file-write* (regex #\"^/private/tmp/cx[0-9]+-[A-Za-z0-9][A-Za-z0-9](/|$)\") (literal \"/private/tmp/cxsweep.lock\"))";
        let sockets = "(allow network-bind network-inbound network-outbound (local unix-socket (path-regex #\"^/private/tmp/cx[0-9]+-[A-Za-z0-9][A-Za-z0-9]/\")) (remote unix-socket (path-regex #\"^/private/tmp/cx[0-9]+-[A-Za-z0-9][A-Za-z0-9]/\")))";
        let store_deny = "(deny file-read* file-write* (subpath \"/private/cowshed\"))";
        for mode in [RunSandboxMode::ReadOnly, RunSandboxMode::ReadWrite] {
            for role in [
                SandboxProfileRole::TrustedSupervisor,
                SandboxProfileRole::ExecutedChild,
                SandboxProfileRole::GitDiscovery,
                #[cfg(target_os = "macos")]
                SandboxProfileRole::GitDiscoveryGitOnly,
            ] {
                let profile = seatbelt_profile(&config(mode), role).unwrap().into_text();
                let carries = matches!(
                    role,
                    SandboxProfileRole::TrustedSupervisor | SandboxProfileRole::ExecutedChild
                );
                for rule in [files, sockets] {
                    assert_eq!(
                        profile.lines().filter(|line| *line == rule).count(),
                        usize::from(carries),
                        "{mode:?} {role:?}: {rule}"
                    );
                }
                for line in profile.lines().filter(|line| line.contains("file-write")) {
                    for blanket in [
                        "(literal \"/tmp\")",
                        "(subpath \"/tmp\")",
                        "(literal \"/private/tmp\")",
                        "(subpath \"/private/tmp\")",
                    ] {
                        assert!(
                            !line.contains(blanket),
                            "{mode:?} {role:?} writes all of /tmp: {line}"
                        );
                    }
                }
                if carries {
                    let rule = profile.find(files).unwrap();
                    let after_store = profile.find(store_deny).unwrap();
                    let before_secrets = profile.rfind("/Users/tester/.ssh").unwrap();
                    assert!(
                        after_store < rule && rule < before_secrets,
                        "{mode:?} {role:?} must follow the store deny and precede the secret denies"
                    );
                }
            }
        }
    }

    /// What a probe made in the world-shared `/tmp`, removed however the test ends, a failed
    /// assertion included: a leaked root is one more entry every fixture sweep has to read.
    #[cfg(target_os = "macos")]
    struct ProbeEntries(Vec<PathBuf>);

    #[cfg(target_os = "macos")]
    impl Drop for ProbeEntries {
        fn drop(&mut self) {
            for path in &self.0 {
                let removed = match fs::symlink_metadata(path) {
                    Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
                    Ok(_) => fs::remove_file(path),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => Err(error),
                };
                match removed {
                    Ok(()) => {}
                    // The unwinding test already reports its own failure; a second panic would
                    // abort it.
                    Err(error) if std::thread::panicking() => {
                        cowshed_os::eprintln!("remove probe entry {}: {error}", path.display());
                    }
                    Err(error) => panic!("remove probe entry {}: {error}", path.display()),
                }
            }
        }
    }

    /// `argv` under `profile`, started in `workspace`.
    #[cfg(target_os = "macos")]
    fn under_profile(profile: &str, workspace: &Path, argv: &[&str]) -> std::process::Output {
        std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", profile, "--"])
            .args(argv)
            .current_dir(workspace)
            .output_bounded()
            .unwrap()
    }

    /// What a daemon and its client do with a Unix socket: bind one at the path given, connect to
    /// it and say so. Perl's `IO::Socket::UNIX` is a system tool with no shim in front of it.
    #[cfg(target_os = "macos")]
    const SOCKET_SCRIPT: &str = "use IO::Socket::UNIX; use Socket; my $path = shift; unlink $path; \
        my $server = IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $path, Listen => 1) \
        or die \"bind $path: $!\\n\"; \
        my $client = IO::Socket::UNIX->new(Type => SOCK_STREAM, Peer => $path) \
        or die \"connect $path: $!\\n\"; print \"ok\\n\";";

    /// The listing a fixture sweep reads to find roots: whether `/tmp` names the entry given.
    #[cfg(target_os = "macos")]
    const LISTING_SCRIPT: &str = "opendir(my $tmp, '/tmp') or die \"opendir /tmp: $!\\n\"; \
        my $name = shift; \
        print((grep { $_ eq $name } readdir($tmp)) ? \"found\\n\" : \"missing\\n\");";

    /// What `profile` admits in `/tmp` and what it refuses there, each proved by a command the
    /// kernel runs under it in `workspace`. Every refusal is the profile's own (`EPERM`) and leaves
    /// nothing behind.
    #[cfg(target_os = "macos")]
    fn assert_fixture_namespace(profile: &str, workspace: &Path, entries: &mut ProbeEntries) {
        let pid = std::process::id();
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let tag = &tag[..2];
        let at = |name: String| format!("/tmp/{name}");
        let name = format!("{FIXTURE_PREFIX}{pid}-{tag}");
        let root = at(name.clone());
        let lock = at(format!("{FIXTURE_PREFIX}sweep.lock"));
        let stale_lock = format!("{lock}2");
        // Each of these differs from a root's spelling by one character, or sits beside it.
        let lookalikes = [
            format!("{FIXTURE_PREFIX}{pid}-{tag}x"),
            format!("{FIXTURE_PREFIX}{pid}-{}", &tag[..1]),
            format!("{FIXTURE_PREFIX}-{tag}"),
            format!("{FIXTURE_PREFIX}{pid}{tag}"),
            format!("x{FIXTURE_PREFIX}{pid}-{tag}"),
            format!("{FIXTURE_PREFIX}{pid}-{tag}.old"),
            format!("{FIXTURE_PREFIX}{pid}-{}_", &tag[..1]),
            format!("{FIXTURE_PREFIX}sweep"),
            format!("{FIXTURE_PREFIX}sweep.lock.bak"),
            format!("outside-{pid}-{tag}"),
        ]
        .map(at);
        // Lookalike directories that already exist, so a write inside one is refused for its name
        // and not for want of a directory.
        let premade = [
            format!("{FIXTURE_PREFIX}{pid}-{tag}y"),
            format!("{FIXTURE_PREFIX}{pid}-{tag}-evil"),
        ]
        .map(at);
        let other_parent = format!("/private/var/tmp/{name}");
        let canonical_outside = format!("/private/tmp/outside-canonical-{pid}-{tag}");
        let dotdot_target = at(format!("outside-dotdot-{pid}-{tag}"));
        let pivot_target = at(format!("pivot-{pid}-{tag}"));
        let moved = at(format!("moved-{pid}-{tag}"));
        for path in [
            &root,
            &stale_lock,
            &other_parent,
            &canonical_outside,
            &dotdot_target,
            &pivot_target,
            &moved,
        ]
        .into_iter()
        .chain(&lookalikes)
        .chain(&premade)
        {
            entries.0.push(PathBuf::from(path));
        }
        for directory in &premade {
            fs::create_dir(directory).unwrap();
        }

        let allowed = |argv: &[&str]| {
            let output = under_profile(profile, workspace, argv);
            assert!(
                output.status.success(),
                "{argv:?} must be admitted: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        let refused = |argv: &[&str]| {
            let output = under_profile(profile, workspace, argv);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                !output.status.success() && stderr.contains("Operation not permitted"),
                "{argv:?} must be refused by the profile: {stderr}"
            );
        };
        let absent = |path: &str| {
            let found = fs::symlink_metadata(path);
            assert!(
                matches!(&found, Err(error) if error.kind() == std::io::ErrorKind::NotFound),
                "{path} must not exist: {found:?}"
            );
        };

        // A root as a fixture builds it: through `/tmp`, a short alias beside real leaves, then
        // written, renamed, listed and bound through the alias.
        allowed(&["/bin/mkdir", &root]);
        allowed(&["/bin/mkdir", "-p", &format!("{root}/.cowshed/run/nx")]);
        allowed(&[
            "/bin/ln",
            "-s",
            &format!("{root}/.cowshed/run"),
            &format!("{root}/r"),
        ]);
        assert_eq!(
            fs::read_link(format!("{root}/r")).unwrap(),
            PathBuf::from(format!("{root}/.cowshed/run"))
        );
        let state = format!("{root}/r/nx/state");
        allowed(&["/bin/sh", "-c", "printf hello > \"$1\"", "sh", &state]);
        assert_eq!(
            fs::read(format!("{root}/.cowshed/run/nx/state")).unwrap(),
            b"hello"
        );
        assert_eq!(allowed(&["/bin/cat", &state]), "hello");
        allowed(&["/bin/mv", &state, &format!("{root}/moved")]);
        assert_eq!(fs::read(format!("{root}/moved")).unwrap(), b"hello");
        absent(&format!("{root}/.cowshed/run/nx/state"));
        assert_eq!(
            allowed(&[
                "/usr/bin/perl",
                "-e",
                SOCKET_SCRIPT,
                &format!("{root}/r/nx/d.sock")
            ]),
            "ok\n"
        );
        assert_eq!(
            allowed(&["/usr/bin/perl", "-e", LISTING_SCRIPT, &name]),
            "found\n"
        );
        allowed(&["/bin/sh", "-c", ": >> \"$1\"", "sh", &lock]);
        allowed(&["/bin/mkdir", &format!("/private{root}/canonical")]);

        // Nothing else in `/tmp` is writable, and no other spelling is a root or the lock.
        for lookalike in &lookalikes {
            refused(&["/bin/mkdir", lookalike]);
            absent(lookalike);
        }
        refused(&["/bin/sh", "-c", ": >> \"$1\"", "sh", &stale_lock]);
        absent(&stale_lock);
        refused(&["/usr/bin/touch", &canonical_outside]);
        absent(&canonical_outside);
        refused(&["/usr/bin/touch", &other_parent]);
        absent(&other_parent);
        for directory in &premade {
            let inside = format!("{directory}/file");
            refused(&["/usr/bin/touch", &inside]);
            absent(&inside);
            let socket = format!("{directory}/s.sock");
            refused(&["/usr/bin/perl", "-e", SOCKET_SCRIPT, &socket]);
            absent(&socket);
        }

        // Nothing inside a root reaches outside it: `..` and a symlink resolve to the path they
        // name, which decides; a root cannot be renamed out; hard links stay denied.
        refused(&[
            "/bin/mkdir",
            &format!("{root}/../outside-dotdot-{pid}-{tag}"),
        ]);
        absent(&dotdot_target);
        allowed(&["/bin/ln", "-s", "/tmp", &format!("{root}/up")]);
        refused(&["/usr/bin/touch", &format!("{root}/up/pivot-{pid}-{tag}")]);
        absent(&pivot_target);
        refused(&["/bin/mv", &root, &moved]);
        absent(&moved);
        assert!(
            fs::symlink_metadata(&root).is_ok(),
            "{root} stays where it is"
        );
        allowed(&[
            "/bin/sh",
            "-c",
            "printf x > \"$1\"",
            "sh",
            &format!("{root}/linked"),
        ]);
        refused(&[
            "/bin/ln",
            &format!("{root}/linked"),
            &format!("{root}/hard"),
        ]);
        absent(&format!("{root}/hard"));

        // What a sweep does to a dead run's root, by the one operation that does it.
        allowed(&["/bin/rm", "-rf", &root]);
        absent(&root);
    }

    /// The kernel enforces the fixture namespace the profile states (04_sandbox.md, "Fixture roots
    /// under /tmp"): the supervisor's and the child's profiles admit a pid-named root with its
    /// alias, files and sockets, and the sweep lock; they refuse every lookalike, a write beside a
    /// root and an escape from one; and controller Git's profile refuses even a well-formed root.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_seatbelt_admits_only_pid_named_fixture_roots_and_the_sweep_lock_in_tmp() {
        let (root, mut config, _) = host_child_profile("fixture-root");
        let mut entries = ProbeEntries(vec![root]);
        for (mode, role) in [
            (RunSandboxMode::ReadWrite, SandboxProfileRole::ExecutedChild),
            (
                RunSandboxMode::ReadWrite,
                SandboxProfileRole::TrustedSupervisor,
            ),
            (RunSandboxMode::ReadOnly, SandboxProfileRole::ExecutedChild),
        ] {
            config.mode = mode;
            let profile = seatbelt_profile(&config, role).unwrap().into_text();
            assert_fixture_namespace(&profile, &config.workspace_mount, &mut entries);
        }

        let git = seatbelt_profile(&config, SandboxProfileRole::GitDiscovery)
            .unwrap()
            .into_text();
        let well_formed = format!("/tmp/{FIXTURE_PREFIX}{}-gt", std::process::id());
        entries.0.push(PathBuf::from(&well_formed));
        let made = under_profile(&git, &config.workspace_mount, &["/bin/mkdir", &well_formed]);
        assert!(
            !made.status.success()
                && String::from_utf8_lossy(&made.stderr).contains("Operation not permitted"),
            "controller Git must not make a fixture root: {made:?}"
        );
        let found = fs::symlink_metadata(&well_formed);
        assert!(
            matches!(&found, Err(error) if error.kind() == std::io::ErrorKind::NotFound),
            "{well_formed} must not exist: {found:?}"
        );
    }

    /// A child whose stdout is the null device can fstat it. Runtimes classify their stdio
    /// before running a line — Node aborts in its process setup, with no message, when fstat on
    /// fd 0-2 fails with anything but EBADF — so a denied fstat makes every `>/dev/null` and
    /// every detached job's null stdout kill the runtime before it starts.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_child_can_fstat_the_null_device_its_stdio_points_at() {
        let (root, config, child) = host_child_profile("null-device");

        // `Stdio::null()` opens the device write-only for stdout, as a shell's `>/dev/null`
        // does; BSD stat with no operand fstats its stdin, here that same descriptor.
        let fstat = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/bin/sh", "-c"])
            .arg("exec /usr/bin/stat -f %HT <&1")
            .current_dir(&config.workspace_mount)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn_locked()
            .and_then(std::process::Child::wait_with_output_bounded)
            .unwrap();

        fs::remove_dir_all(&root).unwrap();
        assert!(
            fstat.status.success(),
            "fstat on a write-only /dev/null stdout: {}",
            String::from_utf8_lossy(&fstat.stderr)
        );
    }
}
