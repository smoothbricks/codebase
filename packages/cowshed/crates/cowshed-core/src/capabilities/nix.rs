use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CacheMount, CapabilityContribution, CapabilityGrant, CapabilityId, DetectionContext, Detector,
    EnvAction, GrantAccess, GrantScope, add_bootstrap, host_program_directories,
};
use crate::{CowshedError, Result};
use std::os::unix::fs::FileTypeExt as _;
use std::path::{Path, PathBuf};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Nix,
    scope: super::DetectionScope::Project,
    all: &[],
    any: &["flake.nix", "devenv.nix"],
    contribute,
    host_cache_homes: &[&CACHE_HOME, &STATE_HOME],
};
pub static CACHE_HOME: SharedToolHome = SharedToolHome {
    variable: None,
    home: ".cache/nix",
    layout: SharedLayout::Whole("nix/cache"),
    linked_from_checkouts: false,
};
pub static STATE_HOME: SharedToolHome = SharedToolHome {
    variable: None,
    home: ".local/state/nix",
    layout: SharedLayout::Whole("nix/state"),
    linked_from_checkouts: false,
};
pub const DAEMON_SOCKET: &str = "/nix/var/nix/daemon-socket/socket";

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    for path in ["/nix", "/etc/nix", "/private/etc/nix"] {
        contribution.grants.push(CapabilityGrant {
            path: path.into(),
            scope: GrantScope::Subtree,
            access: GrantAccess::Read,
        });
    }
    if context.caches_root.is_dir() {
        for directory in ["cache", "state"] {
            contribution.cache_mounts.push(CacheMount {
                source: context.caches_root.join("nix").join(directory),
                private_target: Some(context.environment_root.join(directory).join("nix")),
            });
        }
    }
    if let Some(socket) = daemon_socket_at(Path::new(DAEMON_SOCKET))? {
        contribution.unix_sockets.push(socket);
    }
    if let Some(bundle) = context.trust_bundle {
        contribution.env.insert(
            "NIX_SSL_CERT_FILE",
            EnvAction::Default(bundle.as_os_str().to_owned()),
        );
        contribution.env.insert(
            "NIX_CONFIG",
            EnvAction::Append(format!("ssl-cert-file = {}", bundle.display())),
        );
    }
    let directories = host_program_directories(context);
    for name in [
        "nix",
        "nix-store",
        "nix-shell",
        "nix-instantiate",
        "nix-env",
        "devenv",
    ] {
        add_bootstrap(&mut contribution, context, name, &directories)?;
    }
    Ok(contribution)
}

pub fn daemon_socket_at(entry: &Path) -> Result<Option<PathBuf>> {
    let resolved = match std::fs::canonicalize(entry) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect Nix daemon socket {}: {error}",
                    entry.display()
                ),
                "repair the Nix daemon socket or disable the Nix capability",
            ));
        }
    };
    let metadata = std::fs::symlink_metadata(&resolved).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot inspect Nix daemon socket {}: {error}",
                resolved.display()
            ),
            "repair the Nix daemon socket or disable the Nix capability",
        )
    })?;
    Ok(metadata.file_type().is_socket().then_some(resolved))
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{Fixture, assert_switch};
    use super::*;
    #[test]
    fn a_flake_enables_nix() {
        assert_switch(&DETECTOR, &["flake.nix"]);
    }
    #[test]
    fn devenv_enables_nix() {
        assert_switch(&DETECTOR, &["devenv.nix"]);
    }
    #[test]
    fn the_daemon_entry_must_resolve_to_a_socket() {
        let fixture = Fixture::new();
        let entry = fixture.root.join("socket");
        assert!(daemon_socket_at(&entry).unwrap().is_none());
        std::fs::write(&entry, b"not a socket").unwrap();
        assert!(daemon_socket_at(&entry).unwrap().is_none());
        std::fs::remove_file(&entry).unwrap();
        let socket = fixture.root.join("real-socket");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        std::os::unix::fs::symlink(&socket, &entry).unwrap();
        assert_eq!(
            daemon_socket_at(&entry).unwrap(),
            Some(std::fs::canonicalize(socket).unwrap())
        );
    }

    /// A sandbox for the fixture workspace, carrying a Nix project's contribution detected against
    /// the real caches root, as a supervisor detects it — cache mounts are only ever beneath it.
    /// The mount root is a sibling directory, so its deny never covers the fixture's host home.
    fn nix_sandbox(fixture: &Fixture) -> crate::sandbox::SandboxConfig {
        fixture.files(&["flake.nix"]);
        let context = DetectionContext {
            caches_root: Path::new(crate::storage::bootstrap::CACHES_ROOT),
            ..fixture.context()
        };
        let contribution = DETECTOR.detect(&context).unwrap().expect("nix detected");
        crate::sandbox::SandboxConfig {
            home: fixture.home.clone(),
            mount_root: fixture.root.join("mounts"),
            workspace_mount: fixture.root.clone(),
            shed_links: Vec::new(),
            exec_temp_dir: fixture.root.join(".cowshed-tmp"),
            port_block: crate::metadata::PortBlock::new(40_960, 16).unwrap(),
            retained_port_blocks: Vec::new(),
            mode: crate::sandbox::RunSandboxMode::ReadWrite,
            grants: crate::sandbox::SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            git_worktree_repository: None,
            capabilities: super::super::DetectedCapabilities {
                active: vec![CapabilityId::Nix],
                contribution,
            },
        }
    }

    /// Nix stats its configuration files before reading them, optional ones included, so a Nix
    /// project reads `/etc/nix` (both spellings: Seatbelt matches the resolved `/private/etc`)
    /// and traverses `/etc` without reading the rest of it or writing anything there.
    #[test]
    fn a_nix_project_reads_its_config_tree_and_nothing_else_in_etc() {
        let fixture = Fixture::new();
        let config = nix_sandbox(&fixture);
        for role in [
            crate::sandbox::SandboxProfileRole::TrustedSupervisor,
            crate::sandbox::SandboxProfileRole::ExecutedChild,
        ] {
            let profile = crate::sandbox::seatbelt_profile(&config, role).unwrap();
            for root in ["/etc/nix", "/private/etc/nix"] {
                let rule = format!("(allow file-read* (subpath \"{root}\"))");
                assert!(profile.contains(&rule), "{role:?} lacks {rule}");
            }
            for ancestor in ["/etc", "/private/etc"] {
                assert!(profile.contains(&format!("(allow file-read* (literal \"{ancestor}\"))")));
                assert!(!profile.contains(&format!("(subpath \"{ancestor}\")")));
            }
            assert!(
                profile
                    .lines()
                    .filter(|line| line.contains("file-write"))
                    .all(|line| !line.contains("/etc")),
                "{role:?} must not grant writes under /etc"
            );
        }
    }

    /// The same contract measured by the kernel: a missing Nix config file is ENOENT, never the
    /// EPERM that Determinate Nix treats as fatal, the real config reads unchanged and refuses
    /// writes, and metadata elsewhere in `/etc` stays denied.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_nix_config_metadata_is_readable_and_nothing_else_in_etc() {
        use crate::fork_lock::Run as _;

        let nix_conf = Path::new("/etc/nix/nix.conf");
        assert!(
            nix_conf.is_file(),
            "the host must have {}",
            nix_conf.display()
        );
        let host_config = std::fs::read(nix_conf).unwrap();
        let fixture = Fixture::new();
        let config = nix_sandbox(&fixture);
        std::fs::create_dir_all(&config.exec_temp_dir).unwrap();
        let absent = PathBuf::from(format!("/etc/nix/cowshed-absent-{}", std::process::id()));
        let profile = crate::sandbox::seatbelt_profile(
            &config,
            crate::sandbox::SandboxProfileRole::ExecutedChild,
        )
        .unwrap();
        let run = |script: &str, path: &Path| {
            std::process::Command::new("/usr/bin/sandbox-exec")
                .args(["-p", &profile, "--", "/bin/sh", "-c", script, "sh"])
                .arg(path)
                .current_dir(&config.workspace_mount)
                .stdin(std::process::Stdio::null())
                .output_locked()
                .unwrap()
        };
        let stderr =
            |output: &std::process::Output| String::from_utf8_lossy(&output.stderr).into_owned();
        let read = run("/bin/cat \"$1\"", nix_conf);
        assert!(
            read.status.success() && read.stdout == host_config,
            "{}",
            stderr(&read)
        );
        let missing = run("/usr/bin/stat \"$1\"", &absent);
        assert!(
            stderr(&missing).contains("No such file or directory"),
            "{}",
            stderr(&missing)
        );
        let unrelated = run("/usr/bin/stat \"$1\"", Path::new("/etc/hosts"));
        assert!(
            stderr(&unrelated).contains("Operation not permitted"),
            "{}",
            stderr(&unrelated)
        );
        assert!(
            !run(": >> \"$1\"", nix_conf).status.success(),
            "nix.conf must not open for writing"
        );
        assert_eq!(std::fs::read(nix_conf).unwrap(), host_config);
    }
}
