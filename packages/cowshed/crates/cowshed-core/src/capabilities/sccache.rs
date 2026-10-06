use super::{
    CapabilityContribution, CapabilityGrant, CapabilityId, DetectionContext, Detector, EnvAction,
    GrantAccess, GrantScope,
};
use crate::storage::bootstrap::STORE_ROOT;
use crate::{CowshedError, Result};
use std::path::{Path, PathBuf};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Sccache,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::Project,
    all: &[],
    any: &["Cargo.toml"],
    contribute,
    reached_from: Some(super::ReachedConvention::TrackedManifest("Cargo.toml")),
};

pub fn server_socket() -> PathBuf {
    Path::new(STORE_ROOT).join("sccache.sock")
}
/// sccache's own default disk cache, which the host-owned daemon serves and no sandbox is granted.
pub fn cache_directory(home: &Path) -> PathBuf {
    #[cfg(target_os = "macos")]
    let default = "Library/Caches/Mozilla.sccache";
    #[cfg(not(target_os = "macos"))]
    let default = ".cache/sccache";
    home.join(default)
}
pub fn gc_root(home: &Path) -> PathBuf {
    home.join("Library/Application Support/dev.cowshed/nix/sccache")
}

pub fn client(home: &Path) -> Result<Option<PathBuf>> {
    let root = gc_root(home);
    let store = match std::fs::read_link(&root) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect compiler-cache client {}: {error}",
                    root.display()
                ),
                "repair the installed compiler-cache root or disable its capability",
            ));
        }
    };
    if !store.is_absolute() {
        return Err(CowshedError::integrity(
            format!("compiler-cache root {} is not absolute", root.display()),
            "repair the compiler-cache installation",
        ));
    }
    let program = store.join("bin/sccache");
    match std::fs::metadata(&program) {
        Ok(metadata) if metadata.is_file() => Ok(Some(program)),
        Ok(_) => Err(CowshedError::integrity(
            format!("compiler-cache client {} is not a file", program.display()),
            "repair the compiler-cache installation",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::environment_missing(
            format!(
                "cannot inspect compiler-cache client {}: {error}",
                program.display()
            ),
            "repair the compiler-cache installation",
        )),
    }
}

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    // The sandboxed supervisor reads the GC root beneath the HOME-wide read deny to name the
    // client, so the root itself is granted whether or not the host installed one: a link into
    // the store, it admits no HOME bytes, and its ancestors stay metadata-only.
    contribution.grants.push(CapabilityGrant {
        path: gc_root(context.home),
        scope: GrantScope::Literal,
        access: GrantAccess::Read,
    });
    if let Some(client) = client(context.home)? {
        contribution.env.insert(
            "RUSTC_WRAPPER",
            EnvAction::Own(client.clone().into_os_string()),
        );
        contribution
            .env
            .insert("SCCACHE_BASEDIR_CWD", EnvAction::Own("1".into()));
        // cargo runs a registry crate's rustc from its package directory, so the cwd base does
        // not cover the workspace: the OUT_DIR a build script fills sits below it, and a crate
        // that includes from OUT_DIR (serde does) would key, and record, this workspace's path,
        // as would every crate built on it. The workspace root as the request base strips and
        // remaps it as the cwd is; at a workspace member's cwd it keys as the cwd alone.
        contribution.env.insert(
            "SCCACHE_BASEDIR",
            EnvAction::Own(context.workspace_root.as_os_str().to_owned()),
        );
        contribution.env.insert(
            "SCCACHE_SERVER_UDS",
            EnvAction::Own(server_socket().into_os_string()),
        );
        contribution.env.insert(
            "SCCACHE_DIR",
            EnvAction::Own(cache_directory(context.home).into_os_string()),
        );
        contribution.unix_sockets.push(server_socket());
        contribution.grants.push(CapabilityGrant {
            path: client,
            scope: GrantScope::Literal,
            access: GrantAccess::Read,
        });
    } else {
        contribution.env.insert("RUSTC_WRAPPER", EnvAction::Unset);
        contribution
            .env
            .insert("SCCACHE_BASEDIR_CWD", EnvAction::Unset);
        contribution.env.insert("SCCACHE_BASEDIR", EnvAction::Unset);
    }
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::Fixture;
    use super::*;
    #[test]
    fn an_installed_client_needs_the_cargo_convention() {
        let fixture = Fixture::new();
        let store = fixture.root.join("installed");
        std::fs::create_dir_all(store.join("bin")).unwrap();
        std::fs::write(store.join("bin/sccache"), b"client").unwrap();
        let root = gc_root(&fixture.home);
        std::fs::create_dir_all(root.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(store, root).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
        fixture.files(&["Cargo.toml"]);
        let contribution = DETECTOR.detect(&fixture.context()).unwrap().unwrap();
        assert!(contribution.env.contains_key("RUSTC_WRAPPER"));
        assert_eq!(
            contribution.env.get("SCCACHE_BASEDIR"),
            Some(&EnvAction::Own(fixture.root.clone().into_os_string()))
        );
        std::fs::remove_file(fixture.root.join("Cargo.toml")).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
    #[test]
    fn cargo_without_an_installed_client_gets_no_socket_or_wrapper() {
        let fixture = Fixture::new();
        fixture.files(&["Cargo.toml"]);
        let contribution = DETECTOR.detect(&fixture.context()).unwrap().unwrap();
        assert!(contribution.unix_sockets.is_empty());
        assert_eq!(
            contribution.env.get("RUSTC_WRAPPER"),
            Some(&EnvAction::Unset)
        );
        for name in ["SCCACHE_BASEDIR_CWD", "SCCACHE_BASEDIR"] {
            assert_eq!(contribution.env.get(name), Some(&EnvAction::Unset));
        }
    }
}
