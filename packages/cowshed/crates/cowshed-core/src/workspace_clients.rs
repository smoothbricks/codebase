//! Client wiring for a workspace's package managers and TLS clients, written into the
//! workspace's private environment (`.cowshed/{home,config,cache}`, never a tracked file).
//!
//! Every sandboxed process reaches the network through the workspace gateway: bun through its npm
//! mirror route (`<GATEWAY_HTTP>/npm`), everything else through the proxy, where a granted host's
//! TLS is terminated with a leaf the workspace CA signed. Two things make that work for tools
//! cowshed does not configure by argument:
//!
//! - **The mirror client** — bun — finds its registry and the workspace token in its global
//!   bunfig in the private environment, and sends the token in its own `Authorization` header,
//!   never in a URL. Go is not a mirror client: `cmd/go` sends credentials only over HTTPS, so
//!   its `GOENV` file points it at the public module proxy, reached through an opaque tunnel.
//! - **TLS clients** that read one CA file — git, cargo, nix, uv and OpenSSL — get a combined
//!   trust bundle: the platform's roots (for opaque tunnels, which present the real upstream
//!   certificate) followed by the workspace CA (for intercepted hosts).
//!
//! Written when the workspace is minted and rewritten before every exec, so an endpoint that
//! moved or a token that rotated is never served stale. Writes are idempotent: an unchanged
//! file is left as it is.

use std::ffi::CStr;
use std::io;
use std::path::Path;

use crate::fsio::AnchoredDirectory;
use crate::metadata::{Platform, PortBlock};

/// The combined trust bundle, at the root of the private environment.
pub const TRUST_BUNDLE_NAME: &CStr = c"ca-bundle.pem";
/// git's CA file for HTTPS (`http.sslCAInfo`).
pub const GIT_CA_ENV: &str = "GIT_SSL_CAINFO";
/// cargo's documented environment spelling of `http.cainfo`.
pub const CARGO_CA_ENV: &str = "CARGO_HTTP_CAINFO";
/// nix's CA file when no `ssl-cert-file` is configured.
pub const NIX_CA_ENV: &str = "NIX_SSL_CERT_FILE";
/// Configuration nix applies after every `nix.conf`: a host whose `nix.conf` names its own
/// `ssl-cert-file` (measured on Determinate Nix 2.35: `/etc/nix/macos-keychain.crt`) outranks
/// `NIX_SSL_CERT_FILE`, and only `NIX_CONFIG` outranks that file.
pub const NIX_CONFIG_ENV: &str = "NIX_CONFIG";
/// The CA file OpenSSL, and uv in system-certificate mode, verify against.
pub const SSL_CERT_ENV: &str = "SSL_CERT_FILE";
/// uv verifies against its bundled roots unless told to use the platform's, which it then reads
/// from `SSL_CERT_FILE`.
pub const UV_SYSTEM_CERTS_ENV: &str = "UV_SYSTEM_CERTS";

/// The platform root bundle the combined bundle starts from: what an opaque-tunnelled client
/// would verify the real upstream certificate against.
#[cfg(target_os = "macos")]
pub const SYSTEM_TRUST_BUNDLE: &str = "/etc/ssl/cert.pem";
#[cfg(not(target_os = "macos"))]
pub const SYSTEM_TRUST_BUNDLE: &str = "/etc/ssl/certs/ca-certificates.crt";

/// The workspace's gateway base URL: its port-block base on macOS, the namespace-local
/// connector on Linux (05_gateway.md).
pub fn gateway_http(platform: Platform, port_block: Option<PortBlock>) -> Option<String> {
    match (platform, port_block) {
        (Platform::Macos, Some(block)) => Some(format!("http://127.0.0.1:{}", block.base())),
        (Platform::Linux, None) => Some("http://127.0.0.1:7644".to_owned()),
        _ => None,
    }
}

/// bun's global config: the npm mirror as the default registry, authenticated with the token.
///
/// Measured on bun 1.4.2: bun reads `$XDG_CONFIG_HOME/.bunfig.toml` when `XDG_CONFIG_HOME` is set
/// (and `$HOME/.bunfig.toml` only when it is not), and reads it alongside a repository's own
/// `bunfig.toml`; the token is sent as `Authorization: Bearer`.
pub fn bunfig(gateway_http: &str, token: &str) -> String {
    format!("[install]\nregistry = {{ url = \"{gateway_http}/npm/\", token = \"{token}\" }}\n")
}

/// Go's env file (`go env -w` format), reached through `GOENV` (03_caches.md).
///
/// The public module proxy, not the gateway's loopback mirror: `cmd/go` attaches credentials —
/// netrc, `GOAUTH`, URL userinfo — only to HTTPS URLs, so no Go client can present the workspace
/// token to a plain-HTTP route. It names no endpoint and carries no token, so a host process that
/// loads the workspace's `.envrc` reads the same file and fetches directly.
pub fn go_env(environment: &Path) -> String {
    let go = environment.join("cache/go");
    format!(
        "GOPROXY=https://proxy.golang.org\n\
         GOSUMDB=sum.golang.org\n\
         GOMODCACHE={caches}/go/mod\n\
         GOCACHE={caches}/go/build\n\
         GOPATH={path}\n\
         GOBIN={bin}\n\
         GOTOOLCHAIN=local\n",
        caches = crate::storage::bootstrap::CACHES_ROOT,
        path = go.join("path").display(),
        bin = go.join("bin").display(),
    )
}

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
    pub gateway_http: &'a str,
    pub token: &'a str,
    /// The workspace CA certificate; no bundle is published without one.
    pub workspace_ca: Option<&'a [u8]>,
    pub system_bundle: &'a [u8],
    /// The private environment root as the sandboxed child sees it.
    pub environment: &'a Path,
}

/// Publish the wiring into the private environment rooted at `root` (held open, so a child that
/// renamed a directory cannot redirect the write).
pub(crate) fn publish_client_wiring(
    root: &AnchoredDirectory,
    wiring: &ClientWiring<'_>,
) -> io::Result<()> {
    root.child(c"config")?.publish_file(
        c".bunfig.toml",
        bunfig(wiring.gateway_http, wiring.token).as_bytes(),
    )?;
    root.child(c"cache")?
        .child(c"go")?
        .publish_file(c"env", go_env(wiring.environment).as_bytes())?;
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
    fn bun_carries_the_token_outside_any_url_and_go_names_the_public_proxy() {
        let gateway = "http://127.0.0.1:40960";
        assert_eq!(
            bunfig(gateway, "tok"),
            "[install]\nregistry = { url = \"http://127.0.0.1:40960/npm/\", token = \"tok\" }\n"
        );
        let go = go_env(Path::new("/w/.cowshed"));
        assert!(go.contains("GOPROXY=https://proxy.golang.org\n"));
        assert!(!go.contains("direct"));
        assert!(!go.contains("127.0.0.1"));
        assert!(go.contains("GOMODCACHE=/private/cowshed/caches/go/mod\n"));
        assert!(go.contains("GOPATH=/w/.cowshed/cache/go/path\n"));
        assert!(go.contains("GOBIN=/w/.cowshed/cache/go/bin\n"));
        assert!(go.contains("GOTOOLCHAIN=local\n"));
        assert_eq!(
            gateway_http(Platform::Macos, Some(PortBlock::new(40_960, 16).unwrap())).as_deref(),
            Some(gateway)
        );
        assert_eq!(
            gateway_http(Platform::Linux, None).as_deref(),
            Some("http://127.0.0.1:7644")
        );
        assert_eq!(gateway_http(Platform::Macos, None), None);
    }

    #[test]
    fn the_trust_bundle_keeps_the_platform_roots_and_adds_the_workspace_ca() {
        assert_eq!(trust_bundle(b"ROOTS", b"CA\n"), b"ROOTS\nCA\n");
        assert_eq!(trust_bundle(b"ROOTS\n", b"CA\n"), b"ROOTS\nCA\n");
        assert_eq!(trust_bundle(b"", b"CA\n"), b"CA\n");
    }

    #[test]
    fn publication_writes_each_file_where_its_tool_reads_it_and_rewrites_only_on_change() {
        let root = std::env::temp_dir().join(format!("client-wiring-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(&root).unwrap();
        let anchored = AnchoredDirectory::create(&root).unwrap();
        let wiring = ClientWiring {
            gateway_http: "http://127.0.0.1:40960",
            token: "tok",
            workspace_ca: Some(b"CA\n"),
            system_bundle: b"ROOTS\n",
            environment: &root,
        };
        publish_client_wiring(&anchored, &wiring).unwrap();
        let read = |path: &str| std::fs::read_to_string(root.join(path)).unwrap();
        assert!(read("config/.bunfig.toml").contains("/npm/"));
        assert!(read("cache/go/env").contains("GOPROXY=https://proxy.golang.org\n"));
        assert!(!root.join("home/.netrc").exists());
        assert_eq!(read("ca-bundle.pem"), "ROOTS\nCA\n");
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(root.join("config/.bunfig.toml"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // Unchanged wiring leaves the file itself in place; a rotated token rewrites it.
        let inode = |path: &str| {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(root.join(path)).unwrap().ino()
        };
        let before = inode("config/.bunfig.toml");
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert_eq!(inode("config/.bunfig.toml"), before);
        publish_client_wiring(
            &anchored,
            &ClientWiring {
                token: "rotated",
                ..wiring
            },
        )
        .unwrap();
        assert!(read("config/.bunfig.toml").contains("token = \"rotated\""));

        // A child that planted a link where a file belongs cannot redirect the host's write.
        let outside = root.join("outside");
        std::fs::write(&outside, b"untouched").unwrap();
        std::fs::remove_file(root.join("config/.bunfig.toml")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("config/.bunfig.toml")).unwrap();
        publish_client_wiring(&anchored, &wiring).unwrap();
        assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
        assert!(
            !std::fs::symlink_metadata(root.join("config/.bunfig.toml"))
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
}
