//! Linux serves each session on a Unix socket in the store's data socket root.
//!
//! No Linux workspace holds a port block. Its jobs run in a private network namespace, where a
//! connector on `127.0.0.1:7644` relays every connection to the workspace's own socket here,
//! `<store>/run/gateway/<workspace incarnation>.sock` (04_sandbox.md, "Linux enforcement"). The
//! socket a connection arrived on is the workspace it speaks for, so the root is held to one
//! owner at mode 0700 and a session to a socket directly inside it: the gateway binds no socket
//! a controller could point anywhere else.

use std::io;
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use cowshed_gateway_types::{
    ConfigError, WorkspaceEndpoint, WorkspaceSession, linux_data_socket_root, validate_identifier,
};

use crate::config::GatewayConfig;

/// A Linux gateway serves its store's workspaces in the store's `run/gateway`.
pub fn data_socket_root(store: &Path) -> Option<PathBuf> {
    Some(linux_data_socket_root(store))
}

/// Make the configured data socket root when it is absent, for its owner alone, with the
/// store's `run` directory above it as that directory's other users make it. A root that is
/// already there is left exactly as found: [`validate_config`] judges it.
pub(crate) fn prepare(config: &GatewayConfig) -> io::Result<()> {
    let Some(root) = config.data_socket_root.as_deref() else {
        return Ok(());
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(root)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "cannot create the workspace data socket root {}: {error}",
                    root.display()
                ),
            )
        })
}

pub(crate) fn validate_config(config: &GatewayConfig) -> Result<(), ConfigError> {
    let root = config
        .data_socket_root
        .as_deref()
        .ok_or(ConfigError::MissingDataSocketRoot)?;
    root_refusal(root, observe(root), effective_uid())
}

pub(crate) fn validate_session(
    config: &GatewayConfig,
    session: &WorkspaceSession,
) -> Result<(), ConfigError> {
    let root = config
        .data_socket_root
        .as_deref()
        .ok_or(ConfigError::MissingDataSocketRoot)?;
    socket_refusal(root, config.control_socket.as_deref(), &session.endpoint)
}

/// What the data socket root's own entry is, read without following a link.
#[derive(Clone, Copy, Debug)]
enum RootEntry {
    /// Nothing readable is there.
    Unreadable,
    Entry {
        /// A directory itself, never a link to one.
        directory: bool,
        uid: u32,
        mode: u32,
    },
}

fn observe(root: &Path) -> RootEntry {
    match std::fs::symlink_metadata(root) {
        Ok(metadata) => RootEntry::Entry {
            directory: metadata.is_dir(),
            uid: metadata.uid(),
            mode: metadata.permissions().mode(),
        },
        Err(_) => RootEntry::Unreadable,
    }
}

/// The root is an absolute path to a real directory of `uid`'s that no other user enters:
/// whoever can create an entry there can answer as any workspace.
fn root_refusal(root: &Path, entry: RootEntry, uid: u32) -> Result<(), ConfigError> {
    if !root.is_absolute() {
        return Err(ConfigError::RelativeSocketPath);
    }
    match entry {
        RootEntry::Entry {
            directory: true,
            uid: owner,
            mode,
        } if owner == uid && mode & 0o077 == 0 => Ok(()),
        RootEntry::Entry { .. } | RootEntry::Unreadable => Err(ConfigError::InsecureDataSocketRoot),
    }
}

/// A session's endpoint is a Unix socket named `<identifier>.sock` directly inside `root`, and
/// never the gateway's own control socket: binding removes a stale socket at the path first.
fn socket_refusal(
    root: &Path,
    control_socket: Option<&Path>,
    endpoint: &WorkspaceEndpoint,
) -> Result<(), ConfigError> {
    let WorkspaceEndpoint::Unix(path) = endpoint else {
        return Err(ConfigError::ExpectedUnixEndpoint);
    };
    if path.parent() != Some(root) || control_socket == Some(path.as_path()) {
        return Err(ConfigError::EndpointOutsideDataSocketRoot);
    }
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or(ConfigError::InvalidDataSocketName)?;
    if path.extension().and_then(|extension| extension.to_str()) != Some("sock")
        || validate_identifier("workspace socket", stem).is_err()
    {
        return Err(ConfigError::InvalidDataSocketName);
    }
    Ok(())
}

fn effective_uid() -> u32 {
    // SAFETY: `geteuid` reads the calling process's credentials and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;

    const UID: u32 = 1000;

    fn rendered(result: Result<(), ConfigError>) -> Result<(), String> {
        result.map_err(|error| error.to_string())
    }

    fn refused(error: ConfigError) -> Result<(), String> {
        Err(error.to_string())
    }

    #[test]
    fn the_root_is_a_real_directory_of_the_gateways_user_alone() {
        let entry = |directory, uid, mode| RootEntry::Entry {
            directory,
            uid,
            mode,
        };
        let root = Path::new("/private/cowshed/store/run/gateway");
        for (case, root, observed, expected) in [
            ("owned 0700", root, entry(true, UID, 0o40700), Ok(())),
            ("owned 0500", root, entry(true, UID, 0o40500), Ok(())),
            (
                "group may enter",
                root,
                entry(true, UID, 0o40750),
                refused(ConfigError::InsecureDataSocketRoot),
            ),
            (
                "others may list",
                root,
                entry(true, UID, 0o40704),
                refused(ConfigError::InsecureDataSocketRoot),
            ),
            (
                "another user's",
                root,
                entry(true, UID + 1, 0o40700),
                refused(ConfigError::InsecureDataSocketRoot),
            ),
            (
                "a link or a file",
                root,
                entry(false, UID, 0o120700),
                refused(ConfigError::InsecureDataSocketRoot),
            ),
            (
                "absent",
                root,
                RootEntry::Unreadable,
                refused(ConfigError::InsecureDataSocketRoot),
            ),
            (
                "relative",
                Path::new("run/gateway"),
                entry(true, UID, 0o40700),
                refused(ConfigError::RelativeSocketPath),
            ),
        ] {
            assert_eq!(
                rendered(root_refusal(root, observed, UID)),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn a_session_socket_is_an_identifier_directly_inside_the_root() {
        let root = Path::new("/store/run/gateway");
        let control = Path::new("/store/run/gateway/gateway.sock");
        let unix = |path: &str| WorkspaceEndpoint::Unix(PathBuf::from(path));
        for (case, endpoint, expected) in [
            (
                "an incarnation's socket",
                WorkspaceEndpoint::linux_data_socket(
                    Path::new("/store"),
                    "00112233445566778899aabbccddeeff",
                ),
                Ok(()),
            ),
            (
                "a TCP endpoint",
                WorkspaceEndpoint::Tcp {
                    address: SocketAddr::from((Ipv4Addr::LOCALHOST, 7_644)),
                    block_size: 64,
                },
                refused(ConfigError::ExpectedUnixEndpoint),
            ),
            (
                "another directory",
                unix("/store/run/0011.sock"),
                refused(ConfigError::EndpointOutsideDataSocketRoot),
            ),
            (
                "a directory below the root",
                unix("/store/run/gateway/nested/0011.sock"),
                refused(ConfigError::EndpointOutsideDataSocketRoot),
            ),
            (
                "the control socket",
                unix("/store/run/gateway/gateway.sock"),
                refused(ConfigError::EndpointOutsideDataSocketRoot),
            ),
            (
                "no .sock suffix",
                unix("/store/run/gateway/0011"),
                refused(ConfigError::InvalidDataSocketName),
            ),
            (
                "not an identifier",
                unix("/store/run/gateway/00 11.sock"),
                refused(ConfigError::InvalidDataSocketName),
            ),
        ] {
            assert_eq!(
                rendered(socket_refusal(root, Some(control), &endpoint)),
                expected,
                "{case}"
            );
        }
        // With no control socket configured, the same name is an ordinary session socket.
        assert_eq!(
            rendered(socket_refusal(
                root,
                None,
                &unix("/store/run/gateway/gateway.sock")
            )),
            Ok(())
        );
    }

    /// A start makes the root, and the store's `run` above it, for the gateway's user alone, and
    /// leaves a root that is already there as it found it: one another user may enter is refused,
    /// never repaired.
    #[test]
    fn a_start_makes_an_absent_root_private_and_never_repairs_a_present_one() {
        let store = std::env::temp_dir().join(format!(
            "cowshed-gateway-data-root-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&store).expect("scratch store");
        let config = GatewayConfig {
            data_socket_root: data_socket_root(&store),
            ..GatewayConfig::default()
        };
        let root = store.join("run/gateway");
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::InsecureDataSocketRoot)
        ));
        prepare(&config).expect("make the root");
        let mode = |path: &Path| {
            std::fs::symlink_metadata(path)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o7777
        };
        assert_eq!(mode(&root), 0o700);
        assert_eq!(mode(&store.join("run")), 0o700);
        validate_config(&config).expect("the root a start made");

        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).expect("widen");
        prepare(&config).expect("a present root is left as found");
        assert_eq!(mode(&root), 0o755);
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::InsecureDataSocketRoot)
        ));

        // A gateway configured with no root has none to make, and says which setting is missing.
        let unconfigured = GatewayConfig::default();
        prepare(&unconfigured).expect("nothing to make");
        assert!(matches!(
            validate_config(&unconfigured),
            Err(ConfigError::MissingDataSocketRoot)
        ));
        std::fs::remove_dir_all(&store).expect("remove scratch store");
    }
}
