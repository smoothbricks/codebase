//! The workspace trust bundle, written into the workspace's private environment (`.cowshed/`,
//! never a tracked file).
//!
//! Every sandboxed process reaches the network through the workspace gateway's proxy endpoint
//! (the `HTTP_PROXY`/`HTTPS_PROXY` variables), where a granted host's TLS is terminated with a
//! leaf the workspace CA signed, or tunnelled opaquely. TLS clients that read one CA file get a
//! combined trust bundle: the platform's roots (for opaque tunnels, which present the real
//! upstream certificate) followed by the workspace CA (for intercepted hosts). Git and OpenSSL
//! are pointed at it by the core; each detected tool's own variable comes from its capability
//! (15_capabilities.md).
//!
//! Written when the workspace is minted and rewritten before every exec. Writes are idempotent:
//! an unchanged file is left as it is.

use std::ffi::CStr;
use std::io;

use crate::fsio::AnchoredDirectory;

/// The combined trust bundle, at the root of the private environment.
pub const TRUST_BUNDLE_NAME: &CStr = c"ca-bundle.pem";
/// git's CA file for HTTPS (`http.sslCAInfo`).
pub const GIT_CA_ENV: &str = "GIT_SSL_CAINFO";
/// The CA file OpenSSL verifies against.
pub const SSL_CERT_ENV: &str = "SSL_CERT_FILE";

/// The platform root bundle the combined bundle starts from: what an opaque-tunnelled client
/// would verify the real upstream certificate against.
#[cfg(target_os = "macos")]
pub const SYSTEM_TRUST_BUNDLE: &str = "/etc/ssl/cert.pem";
#[cfg(not(target_os = "macos"))]
pub const SYSTEM_TRUST_BUNDLE: &str = "/etc/ssl/certs/ca-certificates.crt";

/// The platform roots followed by the workspace CA.
pub fn trust_bundle(system: &[u8], workspace_ca: &[u8]) -> Vec<u8> {
    let mut bundle = Vec::with_capacity(system.len() + workspace_ca.len() + 1);
    bundle.extend_from_slice(system);
    if !bundle.is_empty() && !bundle.ends_with(b"\n") {
        bundle.push(b'\n');
    }
    bundle.extend_from_slice(workspace_ca);
    bundle
}

/// Everything the wiring is derived from.
pub struct ClientWiring<'a> {
    /// The workspace CA certificate; no bundle is published without one.
    pub workspace_ca: Option<&'a [u8]>,
    pub system_bundle: &'a [u8],
}

/// Publish the wiring into the private environment rooted at `root` (held open, so a child that
/// renamed a directory cannot redirect the write).
pub(crate) fn publish_client_wiring(
    root: &AnchoredDirectory,
    wiring: &ClientWiring<'_>,
) -> io::Result<()> {
    // An earlier wiring published bun's global bunfig here: the loopback mirror registry and the
    // workspace token. Bun reads `$XDG_CONFIG_HOME/.bunfig.toml`, so a stale copy would keep
    // sending every install to the retired route and leave the token on disk. Removed as the
    // entry itself, never a link target; bun resolves its registry from the repository's own
    // configuration and reaches it through the proxy variables.
    root.child(c"config")?.remove_file(c".bunfig.toml")?;
    // The netrc an earlier wiring published for Go carried the workspace token, and no Go client
    // ever sent it; nothing else reads it but clients that would hand it to any loopback server.
    root.child(c"home")?.remove_file(c".netrc")?;
    match wiring.workspace_ca {
        Some(workspace_ca) => root.publish_file(
            TRUST_BUNDLE_NAME,
            &trust_bundle(wiring.system_bundle, workspace_ca),
        ),
        None => Ok(()),
    }
}

/// The platform root bundle's bytes.
pub fn system_trust_bundle() -> io::Result<Vec<u8>> {
    std::fs::read(SYSTEM_TRUST_BUNDLE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_trust_bundle_keeps_the_platform_roots_and_adds_the_workspace_ca() {
        assert_eq!(trust_bundle(b"ROOTS", b"CA\n"), b"ROOTS\nCA\n");
        assert_eq!(trust_bundle(b"ROOTS\n", b"CA\n"), b"ROOTS\nCA\n");
        assert_eq!(trust_bundle(b"", b"CA\n"), b"CA\n");
    }

    #[test]
    fn publication_writes_the_bundle_private_and_rewrites_it_only_on_change() {
        let root = std::env::temp_dir().join(format!("client-wiring-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let anchored = AnchoredDirectory::create(&root).unwrap();
        let wiring = ClientWiring {
            workspace_ca: Some(b"CA\n"),
            system_bundle: b"ROOTS\n",
        };
        publish_client_wiring(&anchored, &wiring).unwrap();
        let read = |path: &str| std::fs::read_to_string(root.join(path)).unwrap();
        assert!(!root.join("home/.netrc").exists());
        assert!(!root.join("config/.bunfig.toml").exists());
        assert!(
            !root.join("cache/go").exists(),
            "no Go policy file is published"
        );
        assert_eq!(read("ca-bundle.pem"), "ROOTS\nCA\n");
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(root.join("ca-bundle.pem"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // Unchanged wiring leaves the file itself in place; a rotated CA rewrites it.
        let inode = |path: &str| {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(root.join(path)).unwrap().ino()
        };
        let before = inode("ca-bundle.pem");
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert_eq!(inode("ca-bundle.pem"), before);
        publish_client_wiring(
            &anchored,
            &ClientWiring {
                workspace_ca: Some(b"ROTATED\n"),
                ..wiring
            },
        )
        .unwrap();
        assert_eq!(read("ca-bundle.pem"), "ROOTS\nROTATED\n");

        // A child that planted a link where a file belongs cannot redirect the host's write.
        let outside = root.join("outside");
        std::fs::write(&outside, b"untouched").unwrap();
        std::fs::remove_file(root.join("ca-bundle.pem")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("ca-bundle.pem")).unwrap();
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
        assert!(
            !std::fs::symlink_metadata(root.join("ca-bundle.pem"))
                .unwrap()
                .file_type()
                .is_symlink()
        );

        // A netrc a previous wiring left, or a link planted in its place, is removed — the link
        // itself, never what it points at.
        std::os::unix::fs::symlink(&outside, root.join("home/.netrc")).unwrap();
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert!(std::fs::symlink_metadata(root.join("home/.netrc")).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// The bunfig an earlier wiring published carried the workspace token and the loopback mirror
    /// registry, and bun reads it from `XDG_CONFIG_HOME`: a stale copy would keep sending every
    /// install to the retired route. Publication removes it — or a link planted in its place — as
    /// the entry itself, never what the link points at, and nothing in the private environment
    /// keeps the token.
    #[test]
    fn publication_removes_the_retired_bunfig_and_leaves_no_token_on_disk() {
        fn leaked(directory: &std::path::Path, needle: &str) -> bool {
            std::fs::read_dir(directory).unwrap().any(|entry| {
                let path = entry.unwrap().path();
                let metadata = std::fs::symlink_metadata(&path).unwrap();
                if metadata.is_dir() {
                    leaked(&path, needle)
                } else if metadata.is_file() {
                    String::from_utf8_lossy(&std::fs::read(&path).unwrap()).contains(needle)
                } else {
                    false
                }
            })
        }

        let root = std::env::temp_dir().join(format!("client-wiring-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("config")).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let anchored = AnchoredDirectory::create(&root).unwrap();
        let wiring = ClientWiring {
            workspace_ca: Some(b"CA\n"),
            system_bundle: b"ROOTS\n",
        };
        let bunfig = root.join("config/.bunfig.toml");
        std::fs::write(
            &bunfig,
            "[install]\nregistry = { url = \"http://127.0.0.1:40960/npm/\", token = \"stale-token\" }\n",
        )
        .unwrap();
        assert!(leaked(&root, "stale-token"));
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert!(std::fs::symlink_metadata(&bunfig).is_err());
        assert!(!leaked(&root, "stale-token"));

        let outside = root.join("outside");
        std::fs::write(&outside, b"untouched").unwrap();
        std::os::unix::fs::symlink(&outside, &bunfig).unwrap();
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert!(std::fs::symlink_metadata(&bunfig).is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
