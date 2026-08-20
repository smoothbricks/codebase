use std::borrow::Cow;
use std::fmt;
use std::path::{Component, Path, PathBuf};

pub use crate::metadata::PortBlock;

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
}

/// The canonical entry point to a multi-user Nix installation's daemon socket.
pub const NIX_DAEMON_SOCKET: &str = "/nix/var/nix/daemon-socket/socket";

/// The nix daemon socket to admit for this host, if it has one.
///
/// Multi-user Nix is a requirement: a sandboxed client never writes `/nix/store` itself, it asks
/// the daemon, which runs as root *outside* the sandbox. Admitting the socket therefore grants the
/// ability to ask, not the ability to write — the store is already world-readable through the broad
/// `file-read-data` allow, and binary-cache substitution is already an accepted trusted-mediator
/// channel. Without this, in-workspace `nix` and `devenv` evaluation cannot reach the daemon at all.
///
/// The canonical entry is conventionally a symlink — on macOS it points into `/var/run` — and
/// Seatbelt matches `path-literal` against the resolved path, so the link is followed here rather
/// than admitted as written. Resolving doubles as the check: what gets admitted is whatever the
/// canonical entry actually reaches, and it has to be a socket, so nothing is admitted by being
/// named. A host with no daemon yields no grant and the profile is byte-identical to one built
/// before this existed.
pub fn nix_daemon_socket() -> Option<PathBuf> {
    nix_daemon_socket_at(Path::new(NIX_DAEMON_SOCKET))
}

fn nix_daemon_socket_at(entry: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::FileTypeExt as _;

    let resolved = std::fs::canonicalize(entry).ok()?;
    let metadata = std::fs::symlink_metadata(&resolved).ok()?;
    metadata.file_type().is_socket().then_some(resolved)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxConfig {
    pub home: PathBuf,
    pub workspace_mount: PathBuf,
    pub exec_temp_dir: PathBuf,
    pub port_block: PortBlock,
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxError {
    InvalidPortBlock { base: u16, size: u16 },
    InvalidPath { path: PathBuf, reason: &'static str },
    GrantIntersectsDeny { grant: PathBuf, deny: PathBuf },
}

impl fmt::Display for SandboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPortBlock { base, size } => write!(
                formatter,
                "invalid macOS port block at {base} with size {size}; exactly 16 ports are required"
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
        }
    }
}

impl std::error::Error for SandboxError {}

/// Generate a complete, deterministic SBPL profile for one authority tier.
///
/// Paths must already be canonical controller data. Child argv, environment,
/// output, and repository-controlled grants are deliberately absent from the
/// role selection and therefore cannot remove the executed-child narrowing.
pub fn seatbelt_profile(
    config: &SandboxConfig,
    role: SandboxProfileRole,
) -> Result<String, SandboxError> {
    validate_path(&config.home)?;
    validate_path(&config.workspace_mount)?;
    validate_path(&config.exec_temp_dir)?;
    config
        .port_block
        .validate()
        .map_err(|_| SandboxError::InvalidPortBlock {
            base: config.port_block.base,
            size: config.port_block.size,
        })?;

    if let Some(repository) = &config.git_worktree_repository {
        validate_path(repository)?;
    }
    let hard_denies = hard_denies(&config.home, &config.additional_denies)?;
    let read_grants = normalized_paths(&config.grants.read)?;
    let write_grants = normalized_paths(&config.grants.write)?;
    let sockets = normalized_paths(&config.allowed_unix_sockets)?;

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

    let home = &config.home;
    let cowshed = home.join(".cowshed");
    let caches = cowshed.join("caches");
    let mut profile = String::new();

    push_line(&mut profile, "(version 1)");
    push_line(&mut profile, "(deny default)");
    // Hard-link creation is a separate SBPL operation from file-write*.
    // Keep aliases unavailable to both authority tiers.
    push_line(&mut profile, "(deny file-link)");
    push_line(&mut profile, "(allow file-read-data (subpath \"/\"))");
    // Directory metadata is distinct from file-read-data in Seatbelt. Toolchain
    // launchers (notably /usr/bin/git -> xcrun) must traverse their immutable
    // system roots without gaining metadata access to the user's home.
    for root in [
        "/Applications",
        "/Library",
        "/System",
        "/bin",
        "/opt",
        "/nix",
        // Nix per-user profiles: where nix-darwin and NixOS put a user's installed tools, reached
        // through a stable store-backed symlink. `sandbox_path` admits these to `PATH`, and without
        // the matching read grant every tool on them is unrunnable — `file-read-data` on `/` is not
        // enough, because resolving a path for exec needs `file-read*` metadata on its roots.
        //
        // Both spellings are listed for the same reason `/var/select` and `/private/var/select`
        // both are: Seatbelt matches the *resolved* path, `/etc` is a symlink to `/private/etc`,
        // and a rule naming only the pretty form silently never matches.
        "/etc/profiles",
        "/etc/static/profiles",
        "/private/etc/profiles",
        "/private/etc/static/profiles",
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

    for socket in sockets {
        push_line(
            &mut profile,
            &format!(
                "(allow network-outbound (remote unix-socket (path-literal \"{}\")))",
                sbpl_path(socket)?
            ),
        );
        // Connecting resolves the path, so every ancestor must be traversable. The socket's own
        // directory is not under any of the immutable roots granted above — on macOS the daemon
        // socket resolves into `/private/var/run` — and without these literals the connect fails on
        // path resolution before the outbound rule is ever consulted.
        push_readable_ancestors(&mut profile, socket)?;
    }
    push_line(
        &mut profile,
        "(allow network-bind network-inbound (local tcp \"localhost:*\"))",
    );
    for port in config
        .port_block
        .ports()
        .map_err(|_| SandboxError::InvalidPortBlock {
            base: config.port_block.base,
            size: config.port_block.size,
        })?
    {
        push_line(
            &mut profile,
            &format!("(allow network-outbound (remote tcp \"localhost:{port}\"))"),
        );
    }

    for path in &read_grants {
        push_subpath_rule(&mut profile, "allow file-read*", path)?;
    }
    for path in &write_grants {
        push_subpath_rule(&mut profile, "allow file-read* file-write*", path)?;
    }
    push_line(
        &mut profile,
        &format!(
            "(allow file-write* (subpath \"{}\") (literal \"/dev/null\") (literal \"/dev/stdout\") (literal \"/dev/stderr\"))",
            sbpl_path(&config.exec_temp_dir)?
        ),
    );

    // The store-wide deny intentionally precedes only narrow controller-owned carve-backs.
    push_subpath_rule(&mut profile, "deny file-read* file-write*", &cowshed)?;
    // `getcwd(2)` and path resolution need read access to every exact ancestor.
    // Literal rules reveal no sibling subtree and are emitted after the store-wide
    // deny so an own workspace nested under ~/.cowshed remains reachable.
    push_readable_ancestors(&mut profile, &config.workspace_mount)?;
    push_readable_ancestors(&mut profile, &config.exec_temp_dir)?;
    for path in read_grants.iter().chain(write_grants.iter()) {
        push_readable_ancestors(&mut profile, path)?;
    }
    push_subpath_rule(&mut profile, "allow file-read*", &caches)?;
    for suffix in [
        "cargo/registry",
        "cargo/git",
        "sccache",
        "zig",
        "gradle/caches",
        "go/mod",
        "go/build",
        "nix/cache",
        "nix/state",
    ] {
        push_subpath_rule(
            &mut profile,
            "allow file-read* file-write*",
            &caches.join(suffix),
        )?;
    }
    push_subpath_rule(&mut profile, "allow file-read*", &config.workspace_mount)?;
    if config.mode == RunSandboxMode::ReadWrite {
        push_subpath_rule(&mut profile, "allow file-write*", &config.workspace_mount)?;
    }
    let workspace_metadata = config.workspace_mount.join(".cowshed");
    let job_artifacts = workspace_metadata.join("job");

    // SBPL is last-match-wins: immutable secrets and policy denies close the shared profile.
    for deny in hard_denies
        .into_iter()
        .filter(|path| path.as_ref() != cowshed.as_path())
    {
        push_exact_and_subpath_rule(&mut profile, "deny file-read* file-write*", deny.as_ref())?;
    }

    // After every deny that would otherwise close it: the store-wide `~/.cowshed` deny covers
    // main's mount under the symlink layout, and policy denies the project root under direct
    // mount. SBPL is last-match-wins, so the carve-back has to be stated last to be real — and it
    // is narrowed to `.git`, never main's working tree, which stays as unreachable as any other
    // workspace's.
    if let Some(repository) = &config.git_worktree_repository {
        push_readable_ancestors(&mut profile, repository)?;
        push_exact_and_subpath_rule(&mut profile, "allow file-read* file-write*", repository)?;
    }

    for protected in [
        crate::storage::WORKSPACE_MARKER_PATH,
        crate::workspace_credentials::CA_CERTIFICATE_PATH,
        crate::workspace_credentials::WORKSPACE_TOKEN_PATH,
    ] {
        push_literal_rule(
            &mut profile,
            "deny file-write*",
            &config.workspace_mount.join(protected),
        )?;
    }

    match role {
        SandboxProfileRole::TrustedSupervisor => {
            // The trusted writer's reserved authority is the final narrow
            // carve-back, including when the repository itself is read-only.
            push_exact_and_subpath_rule(&mut profile, "allow file-write*", &job_artifacts)?;
        }
        SandboxProfileRole::ExecutedChild => {
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

fn hard_denies<'a>(
    home: &Path,
    additional: &'a [PathBuf],
) -> Result<Vec<Cow<'a, Path>>, SandboxError> {
    let mut denies = vec![
        Cow::Owned(home.join(".cowshed")),
        Cow::Owned(home.join(".ssh")),
        Cow::Owned(home.join(".gnupg")),
        Cow::Owned(home.join(".aws")),
        Cow::Owned(home.join(".config/gh")),
        Cow::Owned(home.join(".netrc")),
        Cow::Owned(home.join(".npmrc")),
        Cow::Owned(home.join(".pypirc")),
        Cow::Owned(home.join(".cargo/config.toml")),
        Cow::Owned(home.join(".cargo/credentials.toml")),
        Cow::Owned(home.join(".cargo/bin")),
        Cow::Owned(home.join(".gradle/gradle.properties")),
        Cow::Owned(home.join("go")),
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

fn validate_path(path: &Path) -> Result<(), SandboxError> {
    if !path.is_absolute() {
        return Err(SandboxError::InvalidPath {
            path: path.to_path_buf(),
            reason: "path is not absolute",
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use std::fs;
    #[cfg(target_os = "macos")]
    use std::process::Stdio;
    #[cfg(target_os = "macos")]
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(target_os = "macos")]
    static NEXT_SANDBOX_DIR: AtomicU64 = AtomicU64::new(0);

    fn config(mode: RunSandboxMode) -> SandboxConfig {
        SandboxConfig {
            home: PathBuf::from("/Users/tester"),
            workspace_mount: PathBuf::from(
                "/Users/tester/.cowshed/acme/widget/workspaces/raven/mount",
            ),
            exec_temp_dir: PathBuf::from("/private/tmp/cowshed-raven"),
            port_block: PortBlock::new(40_960, 16).unwrap(),
            mode,
            grants: SandboxGrants {
                read: vec![PathBuf::from("/opt/shared"), PathBuf::from("/opt/shared")],
                write: vec![PathBuf::from("/opt/output")],
                egress: vec![EgressGrant {
                    host: "example.com".into(),
                    ports: vec![443],
                }],
            },
            allowed_unix_sockets: vec![PathBuf::from("/var/run/nix/daemon-socket/socket")],
            additional_denies: vec![],
            git_worktree_repository: None,
        }
    }

    #[test]
    fn sandbox_errors_report_the_rejected_values() {
        let invalid_port = SandboxError::InvalidPortBlock {
            base: 65_520,
            size: 8,
        };
        assert_eq!(
            invalid_port.to_string(),
            "invalid macOS port block at 65520 with size 8; exactly 16 ports are required"
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
    fn profile_is_deterministic_and_has_exactly_sixteen_literal_ports() {
        let config = config(RunSandboxMode::ReadWrite);
        let first = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
        let second = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first
                .lines()
                .filter(|line| line.contains("remote tcp \"localhost:"))
                .count(),
            16
        );
        for port in 40_960..=40_975 {
            assert!(first.contains(&format!("remote tcp \"localhost:{port}\"")));
        }
        assert!(!first.contains("localhost:40960-40975"));
        assert!(!first.contains("example.com"));
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
            .find("(deny file-read* file-write* (subpath \"/Users/tester/.cowshed\"))")
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
        let workspace_write = "(allow file-write* (subpath \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount\"))";
        assert!(read_write.contains(workspace_write));
        assert!(!read_only.contains(workspace_write));
        assert!(read_only.contains("(allow file-read* (subpath \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount\"))"));
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
            .find("(deny file-read* file-write* (subpath \"/Users/tester/.cowshed\"))")
            .unwrap();
        let mount_parent = profile
            .find(
                "(allow file-read* (literal \"/Users/tester/.cowshed/acme/widget/workspaces/raven\"))",
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
    fn ancestor_and_descendant_secret_grants_are_rejected() {
        for grant in ["/Users/tester", "/Users/tester/.ssh/id_ed25519"] {
            let mut config = config(RunSandboxMode::ReadWrite);
            config.grants.read = vec![PathBuf::from(grant)];
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
        let protected_allow = "(allow file-write* (literal \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount/.cowshed/job\") (subpath \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount/.cowshed/job\"))";
        let ancestor_deny = "(deny file-write-create file-write-unlink (literal \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount/.cowshed\"))";
        let protected_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount/.cowshed/job\") (subpath \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount/.cowshed/job\"))";
        let token_deny = "(deny file-write* (literal \"/Users/tester/.cowshed/acme/widget/workspaces/raven/mount/.cowshed/token\"))";

        assert_eq!(supervisor.lines().last(), Some(protected_allow));
        assert!(!supervisor.contains(ancestor_deny));
        assert!(!supervisor.contains(protected_deny));
        assert_eq!(child.lines().last(), Some(protected_deny));
        assert!(child.rfind("(allow ").unwrap() < child.find(ancestor_deny).unwrap());
        assert!(supervisor.contains(token_deny));
        assert!(child.contains(token_deny));
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
        assert_eq!(
            PortBlock::new(40_960, 16).unwrap().ports().unwrap().count(),
            16
        );
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn the_daemon_socket_is_admitted_only_by_resolving_to_a_real_socket() {
        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root = fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "cowshed-socket-test-{}-{sequence}",
                std::process::id()
            ));
        fs::create_dir_all(&root).unwrap();

        // A path that is not a socket is not admitted by being named, and neither is a missing one.
        let regular = root.join("not-a-socket");
        fs::write(&regular, b"").unwrap();
        assert_eq!(nix_daemon_socket_at(&regular), None);
        assert_eq!(nix_daemon_socket_at(&root.join("absent")), None);

        // A symlink to a real socket is admitted as its resolved target, because that is what
        // Seatbelt's `path-literal` matches against.
        let listener = root.join("real.socket");
        let _server = std::os::unix::net::UnixListener::bind(&listener).unwrap();
        let link = root.join("link-to-socket");
        std::os::unix::fs::symlink(&listener, &link).unwrap();
        assert_eq!(nix_daemon_socket_at(&link), Some(listener.clone()));

        // The admitted path reaches the profile with its ancestors traversable, or connecting fails
        // on path resolution before the outbound rule is consulted.
        let mut config = config(RunSandboxMode::ReadWrite);
        config.allowed_unix_sockets = vec![listener.clone()];
        let profile = seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap();
        assert!(profile.contains(&format!(
            "(allow network-outbound (remote unix-socket (path-literal \"{}\")))",
            listener.display()
        )));
        assert!(
            profile.contains(&format!(
                "(allow file-read* (literal \"{}\"))",
                root.display()
            )),
            "the socket's own directory must be traversable"
        );

        fs::remove_dir_all(&root).unwrap();
    }

    /// The boundary, exercised live: a sandboxed process must be able to reach the real Nix daemon
    /// when the socket is admitted, and must not when it is not. Without this grant no in-workspace
    /// `nix` or `devenv` evaluation can build or substitute anything.
    #[cfg(target_os = "macos")]
    #[test]
    fn seatbelt_admits_the_nix_daemon_socket_only_when_it_is_granted() {
        let Some(socket) = nix_daemon_socket() else {
            // No multi-user Nix on this host: there is no boundary to exercise.
            return;
        };
        let Ok(nix) = fs::canonicalize("/nix/var/nix/profiles/default/bin/nix") else {
            return;
        };

        let sequence = NEXT_SANDBOX_DIR.fetch_add(1, Ordering::Relaxed);
        let root = fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!(
                "cowshed-nix-daemon-{}-{sequence}",
                std::process::id()
            ));
        let mut config = config(RunSandboxMode::ReadWrite);
        config.home = root.join("home");
        config.workspace_mount = root.join("workspace");
        config.exec_temp_dir = root.join("tmp");
        config.grants = SandboxGrants::default();
        // The private HOME the supervisor exports lives inside the mount, and nix refuses to run
        // without a readable one. Using the real home here would test a shape production never has.
        let private_home = config.workspace_mount.join(".cowshed/home");
        for directory in [&config.home, &private_home, &config.exec_temp_dir] {
            fs::create_dir_all(directory).unwrap();
        }

        // `nix store info` does nothing but open the daemon connection and speak the handshake,
        // which is exactly the authority under test and nothing else.
        let run = |profile: &str| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", profile, "--"])
                .arg(&nix)
                .args(["store", "info", "--store", "daemon"])
                .env("HOME", &private_home)
                .env("NIX_REMOTE", "daemon")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
        };

        config.allowed_unix_sockets = vec![socket];
        let granted = run(&seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap());
        config.allowed_unix_sockets.clear();
        let denied = run(&seatbelt_profile(&config, SandboxProfileRole::ExecutedChild).unwrap());

        fs::remove_dir_all(&root).ok();
        assert!(
            granted.success(),
            "an admitted daemon socket must be reachable from inside the sandbox"
        );
        assert!(
            !denied.success(),
            "without the grant the daemon must be unreachable, or the grant proves nothing"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn seatbelt_enforces_supervisor_and_child_artifact_authority() {
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
            .status()
            .unwrap();
        let child_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/bin/touch"])
            .arg(&child_stream)
            .stderr(Stdio::null())
            .status()
            .unwrap();
        let ordinary_write = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &child, "--", "/usr/bin/touch"])
            .arg(&workspace_file)
            .stderr(Stdio::null())
            .status()
            .unwrap();
        let hardlink_attempt = std::process::Command::new("/usr/bin/sandbox-exec")
            .args(["-p", &supervisor, "--", "/bin/ln"])
            .args([&canonical_stream, &hardlink])
            .stderr(Stdio::null())
            .status()
            .unwrap();

        fs::remove_dir_all(&root).unwrap();
        assert!(supervisor_write.success());
        assert!(!child_write.success());
        assert!(ordinary_write.success());
        assert!(!hardlink_attempt.success());
    }
}
