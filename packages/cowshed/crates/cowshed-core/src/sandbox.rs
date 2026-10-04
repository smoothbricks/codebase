use std::borrow::Cow;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::capabilities::{
    CapabilityGrant, DetectedCapabilities, DetectionContext, GrantAccess, GrantScope,
};
pub use crate::metadata::PortBlock;
use crate::storage::bootstrap::STORE_ROOT;

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
    /// The project capabilities detected in this workspace for this mode
    /// ([`SandboxConfig::configure_capabilities`]): every tool-specific grant, shared cache,
    /// socket and environment entry a child receives comes from here, never from a tool table.
    pub capabilities: DetectedCapabilities,
}

impl SandboxConfig {
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
            port_block: self.port_block,
            retained_port_blocks: self.retained_port_blocks.clone(),
            mode: self.mode,
            grants: self.grants.clone(),
            allowed_unix_sockets: capabilities.contribution.unix_sockets.clone(),
            additional_denies: self.additional_denies.clone(),
            git_worktree_repository: self.git_worktree_repository.clone(),
            build_volume_mount: self.build_volume_mount.clone(),
            repository_caches: self.repository_caches.clone(),
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
    /// The `.git` of main's mount, for a git-worktree workspace only.
    pub git_worktree_repository: Option<PathBuf>,
    /// The checkout's current owned build-volume mount, resolved by the controller.
    pub build_volume_mount: Option<PathBuf>,
    pub workspace_mount: PathBuf,
    /// The workspace's `TMPDIR`: [`crate::storage::StorageLayout::exec_temp_dir`].
    pub exec_temp_dir: PathBuf,
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
        git_worktree_repository,
        build_volume_mount,
        workspace_mount: mount,
        exec_temp_dir,
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
        },
        allowed_unix_sockets: Vec::new(),
        additional_denies,
        git_worktree_repository,
        build_volume_mount,
        repository_caches: repository_caches.to_vec(),
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
    let suffix = match sandbox.mode {
        RunSandboxMode::ReadOnly => "-ro",
        RunSandboxMode::ReadWrite => "",
    };
    PathBuf::from(format!("/tmp/cs-{}{suffix}", sandbox.port_block.base()))
}

pub(crate) fn shared_daemon_runtime_dir(sandbox: &SandboxConfig) -> PathBuf {
    sandbox.workspace_mount.join(".cowshed/run")
}

pub(crate) fn shared_daemon_runtime_link(sandbox: &SandboxConfig) -> PathBuf {
    PathBuf::from(format!("/tmp/cs-{}", sandbox.port_block.base()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxError {
    InvalidPortBlock { base: u16, size: u16 },
    InvalidPath { path: PathBuf, reason: &'static str },
    GrantIntersectsDeny { grant: PathBuf, deny: PathBuf },
    DenyResolution { path: PathBuf, cause: String },
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

/// Generate a complete, deterministic SBPL profile for one authority tier.
///
/// Paths must already be canonical controller data. Child argv, environment,
/// output, and repository-controlled grants are deliberately absent from the
/// role selection and therefore cannot remove the executed-child narrowing.
pub fn seatbelt_profile(
    config: &SandboxConfig,
    role: SandboxProfileRole,
) -> Result<String, SandboxError> {
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
    if role != SandboxProfileRole::GitDiscovery {
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
    // Directory metadata is distinct from file-read-data in Seatbelt. Toolchain
    // launchers (notably /usr/bin/git -> xcrun) must traverse their immutable
    // system roots without gaining metadata access to the user's home.
    for root in [
        "/Applications",
        "/Library",
        "/System",
        "/bin",
        "/opt",
        "/private/var/select",
        "/sbin",
        "/usr",
        "/var/select",
    ] {
        push_exact_and_subpath_rule(&mut profile, "allow file-read*", Path::new(root))?;
        push_readable_ancestors(&mut profile, Path::new(root))?;
    }
    push_line(&mut profile, "(allow process-exec process-fork)");
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
    // Every port of every block the workspace holds, current and retained, one literal each.
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

    for path in &read_grants {
        push_subpath_rule(&mut profile, "allow file-read*", path)?;
    }
    for path in &write_grants {
        push_subpath_rule(&mut profile, "allow file-read* file-write*", path)?;
    }
    // The machine-global evidence layer and the host-configured mount tree are separate protected
    // roots. The latter may live anywhere on Data, so it must keep its own deny rather than relying
    // on the fixed `/private/cowshed` boundary.
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
    // while the sccache socket is itself behind the `/private/cowshed` deny — so
    // the literals must land here, after the store-wide deny, or last-match-wins
    // re-denies them and the connect fails on path resolution before the
    // outbound rule is ever consulted.
    for socket in &sockets {
        push_readable_ancestors(&mut profile, socket)?;
    }
    // Every tool-specific authority is a detected capability's contribution (15_capabilities.md):
    // its shared caches read-write where the host keeps them, their ancestors beneath HOME
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
                access: GrantAccess::ReadWrite,
            },
        )?;
    }
    for grant in &contribution.grants {
        push_capability_grant(&mut profile, &config.home, grant)?;
    }
    // Build state is not source. Grant only the controller-selected physical volume, after
    // the store and mount-tree denies; sibling volumes and images remain inaccessible.
    if let Some(volume) = &config.build_volume_mount {
        push_readable_ancestors(&mut profile, volume)?;
        push_exact_and_subpath_rule(&mut profile, "allow file-read* file-write*", volume)?;
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
                        push_literal_rule(&mut profile, "deny file-write-unlink", &parent)?;
                    }
                    push_exact_and_subpath_rule(&mut profile, operations, &resolved)?;
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

    // After every deny that would otherwise close it: the configured mount-root deny covers
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
            "(allow file-write* (subpath \"{}\") (literal \"/dev/null\") (literal \"/dev/stdout\") (literal \"/dev/stderr\"))",
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
    // on their exact names is allowed, never a /tmp listing.
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
        SandboxProfileRole::ExecutedChild | SandboxProfileRole::GitDiscovery => {
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
        }
    }
    Ok(profile)
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
    use crate::fork_lock::Run as _;
    #[cfg(target_os = "macos")]
    use crate::fork_lock::Spawn as _;
    use std::fs;
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
            },
            allowed_unix_sockets: vec![PathBuf::from("/var/run/nix/daemon-socket/socket")],
            additional_denies: vec![],
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
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
        let old = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        let touch = |profile: &str, path: &Path| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--", "/usr/bin/touch"])
                .arg(path)
                .output_locked()
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
        let new = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
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
        let writable_source =
            seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
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

    #[cfg(target_os = "macos")]
    #[test]
    fn a_source_read_only_job_writes_only_its_shared_nx_leaf_not_other_checkout_runtime() {
        let fixture = crate::capabilities::test_support::Fixture::new();
        let mut sandbox = config(RunSandboxMode::ReadOnly);
        sandbox.home = fixture.home.clone();
        sandbox.mount_root = fixture.root.join("mounts");
        sandbox.workspace_mount = sandbox.mount_root.join("workspace");
        sandbox.exec_temp_dir = fixture.root.join("exec-temp");
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
        let profile = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        let touch = |path: &Path| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/touch"])
                .arg(path)
                .output_locked()
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
        let profile = seatbelt_profile(&custom, SandboxProfileRole::ExecutedChild).unwrap();

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
    /// through the Nix profile link, and the sccache GC root.
    #[test]
    fn home_reads_deny_by_default_and_every_carve_back_follows_the_deny() {
        let home = Path::new("/Users/tester");
        let sccache_root = crate::capabilities::sccache::gc_root(home);
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
                    path: sccache_root.clone(),
                    scope: GrantScope::Literal,
                    access: GrantAccess::Read,
                },
            ],
            &[],
        );
        config.grants.read = vec![PathBuf::from("/Users/tester/Dev/sibling")];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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
            "(allow file-read* (literal \"/Users/tester/Library/Application Support/dev.cowshed/nix/sccache\"))",
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
        let discovery = seatbelt_profile(&config, SandboxProfileRole::GitDiscovery).unwrap();
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
                git_worktree_repository: None,
                build_volume_mount: None,
                workspace_mount: mount.to_path_buf(),
                exec_temp_dir: PathBuf::from("/private/tmp/cowshed-raven"),
            })
            .unwrap()
        };
        let main_deny = "(deny file-read* file-write* (literal \"/Users/tester/Dev/widget\") (subpath \"/Users/tester/Dev/widget\"))";

        let raven = build(&mount_root.join("acme/widget/raven"));
        assert!(raven.additional_denies.contains(&main.to_path_buf()));
        let profile = seatbelt_profile(&raven, SandboxProfileRole::ExecutedChild).unwrap();
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
        let profile = seatbelt_profile(&itself, SandboxProfileRole::ExecutedChild).unwrap();
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
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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
            git_worktree_repository: None,
            build_volume_mount: None,
            workspace_mount: PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/raven"),
            exec_temp_dir: PathBuf::from("/private/tmp/cowshed-raven"),
        })
        .unwrap();
        assert_eq!(
            config.grants.deny,
            [PathBuf::from("a"), PathBuf::from("b"), PathBuf::from("c")]
        );
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
        let profile = seatbelt_profile(&custom, SandboxProfileRole::ExecutedChild).unwrap();

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
            let first = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
            let second = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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

    /// The git-worktree hole, stated as what the profile actually says: narrowed to `.git`, and
    /// last, because the store-wide deny and the project-root deny both cover the path it opens.
    #[test]
    fn git_worktree_repository_carve_back_is_narrow_and_outlives_every_deny() {
        let mut linked = config(RunSandboxMode::ReadWrite);
        let main_mount = PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/main");
        linked.additional_denies = vec![main_mount.clone()];
        linked.git_worktree_repository = Some(main_mount.join(".git"));
        let profile = seatbelt_profile(&linked, SandboxProfileRole::ExecutedChild).unwrap();

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
        .unwrap();
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
        let profile = seatbelt_profile(&with_socket, SandboxProfileRole::ExecutedChild).unwrap();
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
        .unwrap();
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
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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
        .unwrap();
        let read_only = seatbelt_profile(
            &config(RunSandboxMode::ReadOnly),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap();
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
        .unwrap();
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
    fn secret_denies_follow_grants_and_carve_backs() {
        let profile = seatbelt_profile(
            &config(RunSandboxMode::ReadWrite),
            SandboxProfileRole::ExecutedChild,
        )
        .unwrap();
        let grant = profile.find("/opt/shared").unwrap();
        let carve_back = profile.rfind("allow file-write*").unwrap();
        let secret = profile.rfind("/Users/tester/.ssh").unwrap();
        assert!(grant < secret);
        assert!(carve_back < secret);
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
        let supervisor = seatbelt_profile(&config, SandboxProfileRole::TrustedSupervisor).unwrap();
        let child = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
        let protected_allow = "(allow file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\") (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\"))";
        let ancestor_deny = "(deny file-write-create file-write-unlink (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed\"))";
        let protected_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\") (subpath \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/job\"))";
        let token_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/token\"))";
        let environment_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/mnt/acme/widget/workspaces/raven/mount/.cowshed/env\"))";

        assert_eq!(supervisor.lines().last(), Some(protected_allow));
        assert!(!supervisor.contains(ancestor_deny));
        assert!(!supervisor.contains(protected_deny));
        assert_eq!(child.lines().last(), Some(protected_deny));
        assert!(child.rfind("(allow ").unwrap() < child.find(ancestor_deny).unwrap());
        assert!(supervisor.contains(token_deny));
        assert!(child.contains(token_deny));
        assert!(supervisor.contains(environment_deny));
        assert!(child.contains(environment_deny));
        assert!(child.find("allow file-write*").unwrap() < child.find(token_deny).unwrap());

        let common_supervisor = supervisor
            .strip_suffix(&format!("{protected_allow}\n"))
            .unwrap();
        let child_suffix = format!("{ancestor_deny}\n{protected_deny}\n");
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
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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

    /// An admitted socket reaches the profile as an outbound rule with its directory traversable,
    /// or connecting fails on path resolution before the outbound rule is consulted.
    #[test]
    fn an_admitted_socket_is_connectable_and_its_directory_traversable() {
        let socket = PathBuf::from("/private/var/run/daemon-socket/socket");
        let mut config = config(RunSandboxMode::ReadWrite);
        config.allowed_unix_sockets = vec![socket.clone()];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
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
            .output_locked()
            .unwrap();
        assert!(host_name.status.success(), "{host_name:?}");
        let name = std::str::from_utf8(&host_name.stdout).unwrap().trim();
        let record = format!("/Users/{name}");
        let host_record = std::process::Command::new("/usr/bin/dscl")
            .args([".", "-read", &record, "UniqueID"])
            .output_locked()
            .unwrap();
        assert!(host_record.status.success(), "{host_record:?}");
        let key = config.workspace_mount.join("identity-key");
        let generated = std::process::Command::new("/usr/bin/ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(&key)
            .output_locked()
            .unwrap();
        assert!(generated.status.success(), "{generated:?}");
        let public_key = key.with_extension("pub");
        let host_fingerprint = std::process::Command::new("/usr/bin/ssh-keygen")
            .arg("-lf")
            .arg(&public_key)
            .output_locked()
            .unwrap();
        assert!(host_fingerprint.status.success(), "{host_fingerprint:?}");
        for role in [
            SandboxProfileRole::TrustedSupervisor,
            SandboxProfileRole::ExecutedChild,
            SandboxProfileRole::GitDiscovery,
        ] {
            let profile = seatbelt_profile(&config, role).unwrap();
            let identity = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/id", "-un"])
                .output_locked()
                .unwrap();
            assert!(identity.status.success(), "{role:?}: {identity:?}");
            assert_eq!(identity.stdout, host_name.stdout, "{role:?}");
            let fingerprint = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/usr/bin/ssh-keygen", "-lf"])
                .arg(&public_key)
                .output_locked()
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
                .output_locked()
                .unwrap();
            assert!(
                !denied_record.status.success(),
                "{role:?} must not acquire directory-record authority: {denied_record:?}"
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

        let supervisor = seatbelt_profile(&config, SandboxProfileRole::TrustedSupervisor).unwrap();
        let child = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
        let canonical_stream = protected.join("out");
        let child_stream = protected.join("child");
        let workspace_file = config.workspace_mount.join("ordinary");
        let hardlink = config.workspace_mount.join("alias");

        let supervisor_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &supervisor, "--", "/usr/bin/touch"])
            .arg(&canonical_stream)
            .stderr(Stdio::null())
            .status_locked()
            .unwrap();
        let child_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/bin/touch"])
            .arg(&child_stream)
            .stderr(Stdio::null())
            .status_locked()
            .unwrap();
        let ordinary_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/bin/touch"])
            .arg(&workspace_file)
            .stderr(Stdio::null())
            .status_locked()
            .unwrap();
        let hardlink_attempt = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &supervisor, "--", "/bin/ln"])
            .args([&canonical_stream, &hardlink])
            .stderr(Stdio::null())
            .status_locked()
            .unwrap();

        for path in [&git_config, &git_alias] {
            let write = std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &child, "--", "/usr/bin/tee"])
                .arg(path)
                .stdin(Stdio::null())
                .output_locked()
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
            .output_locked()
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

    /// A child whose stdout is the null device can fstat it. Runtimes classify their stdio
    /// before running a line — Node aborts in its process setup, with no message, when fstat on
    /// fd 0-2 fails with anything but EBADF — so a denied fstat makes every `>/dev/null` and
    /// every detached job's null stdout kill the runtime before it starts.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_a_child_can_fstat_the_null_device_its_stdio_points_at() {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root_alias = std::env::temp_dir().join(format!(
            "cowshed-null-device-test-{}-{sequence}",
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
        let child = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();

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
            .and_then(std::process::Child::wait_with_output)
            .unwrap();

        fs::remove_dir_all(&root).unwrap();
        assert!(
            fstat.status.success(),
            "fstat on a write-only /dev/null stdout: {}",
            String::from_utf8_lossy(&fstat.stderr)
        );
    }
}
