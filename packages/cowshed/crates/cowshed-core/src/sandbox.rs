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
