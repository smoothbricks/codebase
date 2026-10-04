use std::path::{Path, PathBuf};
use crate::{CowshedError, Result};
use crate::storage::bootstrap::{CACHES_ROOT, STORE_ROOT};
use super::{CapabilityContribution, CapabilityId, CapabilityGrant, DetectionContext, Detector, EnvAction, GrantAccess, GrantScope};

pub const DETECTOR: Detector = Detector { id: CapabilityId::Sccache, scope: super::DetectionScope::Project, all: &["Cargo.toml"], any: &[], contribute, host_cache_homes: &[] };

pub fn server_socket() -> PathBuf { Path::new(STORE_ROOT).join("sccache.sock") }
pub fn cache_directory() -> PathBuf { Path::new(CACHES_ROOT).join("sccache") }
pub fn gc_root(home: &Path) -> PathBuf { home.join("Library/Application Support/dev.cowshed/nix/sccache") }

pub fn client(home: &Path) -> Result<Option<PathBuf>> {
    let root = gc_root(home);
    let store = match std::fs::read_link(&root) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(CowshedError::environment_missing(format!("cannot inspect compiler-cache client {}: {error}", root.display()), "repair the installed compiler-cache root or disable its capability")),
    };
    if !store.is_absolute() { return Err(CowshedError::integrity(format!("compiler-cache root {} is not absolute", root.display()), "repair the compiler-cache installation")); }
    let program = store.join("bin/sccache");
    match std::fs::metadata(&program) {
        Ok(metadata) if metadata.is_file() => Ok(Some(program)),
        Ok(_) => Err(CowshedError::integrity(format!("compiler-cache client {} is not a file", program.display()), "repair the compiler-cache installation")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::environment_missing(format!("cannot inspect compiler-cache client {}: {error}", program.display()), "repair the compiler-cache installation")),
    }
}

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    if let Some(client) = client(context.home)? {
        contribution.env.insert("RUSTC_WRAPPER", EnvAction::Own(client.clone().into_os_string()));
        contribution.env.insert("SCCACHE_BASEDIR_CWD", EnvAction::Own("1".into()));
        contribution.env.insert("SCCACHE_SERVER_UDS", EnvAction::Own(server_socket().into_os_string()));
        contribution.env.insert("SCCACHE_DIR", EnvAction::Own(cache_directory().into_os_string()));
        contribution.unix_sockets.push(server_socket());
        contribution.grants.push(CapabilityGrant { path: client, scope: GrantScope::Literal, access: GrantAccess::Read });
    } else {
        contribution.env.insert("RUSTC_WRAPPER", EnvAction::Unset);
        contribution.env.insert("SCCACHE_BASEDIR_CWD", EnvAction::Unset);
    }
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_support::Fixture;
    #[test] fn an_installed_client_needs_the_cargo_convention() {
        let fixture = Fixture::new();
        let store = fixture.root.join("installed"); std::fs::create_dir_all(store.join("bin")).unwrap();
        std::fs::write(store.join("bin/sccache"), b"client").unwrap();
        let root = gc_root(&fixture.home); std::fs::create_dir_all(root.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(store, root).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
        fixture.files(&["Cargo.toml"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().unwrap().env.contains_key("RUSTC_WRAPPER"));
        std::fs::remove_file(fixture.root.join("Cargo.toml")).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
    #[test] fn cargo_without_an_installed_client_gets_no_socket_or_wrapper() {
        let fixture = Fixture::new(); fixture.files(&["Cargo.toml"]);
        let contribution = DETECTOR.detect(&fixture.context()).unwrap().unwrap();
        assert!(contribution.unix_sockets.is_empty());
        assert_eq!(contribution.env.get("RUSTC_WRAPPER"), Some(&EnvAction::Unset));
        assert_eq!(contribution.env.get("SCCACHE_BASEDIR_CWD"), Some(&EnvAction::Unset));
    }
}
