use std::path::{Path, PathBuf};
use std::os::unix::fs::FileTypeExt as _;
use crate::{CowshedError, Result};
use super::{CapabilityContribution, CapabilityGrant, CapabilityId, CacheMount, DetectionContext, Detector, EnvAction, GrantAccess, GrantScope, add_bootstrap, host_program_directories};
use super::cache::{SharedLayout, SharedToolHome};

pub const DETECTOR: Detector = Detector { id: CapabilityId::Nix, scope: super::DetectionScope::Project, all: &[], any: &["flake.nix", "devenv.nix"], contribute, host_cache_homes: &[&CACHE_HOME, &STATE_HOME] };
pub static CACHE_HOME: SharedToolHome = SharedToolHome { variable: None, home: ".cache/nix", layout: SharedLayout::Whole("nix/cache"), linked_from_checkouts: false };
pub static STATE_HOME: SharedToolHome = SharedToolHome { variable: None, home: ".local/state/nix", layout: SharedLayout::Whole("nix/state"), linked_from_checkouts: false };
pub const DAEMON_SOCKET: &str = "/nix/var/nix/daemon-socket/socket";

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    for path in ["/nix", "/etc/nix", "/private/etc/nix"] {
        contribution.grants.push(CapabilityGrant { path: path.into(), scope: GrantScope::Subtree, access: GrantAccess::Read });
    }
    if context.caches_root.is_dir() {
        for directory in ["cache", "state"] {
            contribution.cache_mounts.push(CacheMount {
                source: context.caches_root.join("nix").join(directory),
                private_target: Some(context.environment_root.join(directory).join("nix")),
            });
        }
    }
    if let Some(socket) = daemon_socket_at(Path::new(DAEMON_SOCKET))? { contribution.unix_sockets.push(socket); }
    if let Some(bundle) = context.trust_bundle {
        contribution.env.insert("NIX_SSL_CERT_FILE", EnvAction::Default(bundle.as_os_str().to_owned()));
        contribution.env.insert("NIX_CONFIG", EnvAction::Append(format!("ssl-cert-file = {}", bundle.display())));
    }
    let directories = host_program_directories(context);
    for name in ["nix", "nix-store", "nix-shell", "nix-instantiate", "nix-env", "devenv"] { add_bootstrap(&mut contribution, name, &directories)?; }
    Ok(contribution)
}

pub fn daemon_socket_at(entry: &Path) -> Result<Option<PathBuf>> {
    let resolved = match std::fs::canonicalize(entry) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(CowshedError::environment_missing(format!("cannot inspect Nix daemon socket {}: {error}", entry.display()), "repair the Nix daemon socket or disable the Nix capability")),
    };
    let metadata = std::fs::symlink_metadata(&resolved).map_err(|error| CowshedError::environment_missing(format!("cannot inspect Nix daemon socket {}: {error}", resolved.display()), "repair the Nix daemon socket or disable the Nix capability"))?;
    Ok(metadata.file_type().is_socket().then_some(resolved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_support::{Fixture, assert_switch};
    #[test] fn a_flake_enables_nix() { assert_switch(&DETECTOR, &["flake.nix"]); }
    #[test] fn devenv_enables_nix() { assert_switch(&DETECTOR, &["devenv.nix"]); }
    #[test] fn the_daemon_entry_must_resolve_to_a_socket() {
        let fixture = Fixture::new(); let entry = fixture.root.join("socket");
        assert!(daemon_socket_at(&entry).unwrap().is_none());
        std::fs::write(&entry, b"not a socket").unwrap();
        assert!(daemon_socket_at(&entry).unwrap().is_none()); std::fs::remove_file(&entry).unwrap();
        let socket = fixture.root.join("real-socket");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        std::os::unix::fs::symlink(&socket, &entry).unwrap();
        assert_eq!(daemon_socket_at(&entry).unwrap(), Some(std::fs::canonicalize(socket).unwrap()));
    }

}
