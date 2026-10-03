use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use thiserror::Error;
use zeroize::Zeroizing;

use crate::metadata::{MetadataError, Platform, PortBlock, write_atomic_bytes};

pub const WORKSPACE_ENVIRONMENT_PATH: &str = ".cowshed/env";
pub const WORKSPACE_TOKEN_ENV: &str = "COWSHED_WORKSPACE_TOKEN";
pub const PORT_BASE_ENV: &str = "COWSHED_PORT_BASE";
/// The workspace's current port block size: `base+1 … base+size-1` are its service ports.
/// Capacity grants can grow it, so tools read the admitted size rather than assuming one.
pub const PORT_BLOCK_SIZE_ENV: &str = "COWSHED_PORT_BLOCK_SIZE";
pub const GO_ENV: &str = "GOENV";
/// Node's and Bun's additive TLS trust anchor. The workspace CA is wired here and nowhere else:
/// `SSL_CERT_FILE` belongs to nix and devenv, which supply the toolchain's own bundle.
pub const NODE_CA_ENV: &str = "NODE_EXTRA_CA_CERTS";
const GO_ENV_RELATIVE_PATH: &str = ".cowshed/cache/go/env";

#[derive(Debug, Error)]
pub enum WorkspaceEnvironmentError {
    #[error("workspace mount must be an absolute UTF-8 path: {0}")]
    InvalidMount(PathBuf),
    #[error("workspace environment has invalid {platform:?} port wiring")]
    InvalidPortWiring {
        platform: Platform,
        port_block: Option<PortBlock>,
    },
    #[error(transparent)]
    Publication(#[from] MetadataError),
}

/// Atomically publish the source-able, workspace-local build environment inside an image. Its
/// one caller is `workspace_credentials::publish_workspace_environment`, which reads the token
/// the image publishes.
pub(crate) fn write_workspace_environment(
    image_root: &Path,
    workspace_mount: &Path,
    token: &Zeroizing<String>,
    platform: Platform,
    port_block: Option<PortBlock>,
) -> Result<(), WorkspaceEnvironmentError> {
    if !workspace_mount.is_absolute() {
        return Err(WorkspaceEnvironmentError::InvalidMount(
            workspace_mount.to_owned(),
        ));
    }
    match (platform, port_block) {
        (Platform::Macos, Some(block)) => {
            block
                .validate()
                .map_err(|_| WorkspaceEnvironmentError::InvalidPortWiring {
                    platform,
                    port_block,
                })?
        }
        (Platform::Linux, None) => {}
        _ => {
            return Err(WorkspaceEnvironmentError::InvalidPortWiring {
                platform,
                port_block,
            });
        }
    }

    let go_env = workspace_mount.join(GO_ENV_RELATIVE_PATH);
    let go_env = go_env
        .to_str()
        .ok_or_else(|| WorkspaceEnvironmentError::InvalidMount(workspace_mount.to_owned()))?;
    let go_env_word = shell_word(go_env);
    // Token alphabet is unpadded base64url (`A-Za-z0-9_-`), already shell-safe.
    // `shell_word` would `to_owned()`/`format!` it into a plain String that is
    // never zeroized; push the borrowed token into a Zeroizing buffer instead.
    // Capacity is sized so the buffer cannot reallocate after the token is copied: the fixed
    // text is 46 bytes around the two words plus at most 68 for the two port lines.
    let mut contents = Zeroizing::new(String::with_capacity(128 + go_env_word.len() + token.len()));
    contents.push_str("export ");
    contents.push_str(GO_ENV);
    contents.push('=');
    contents.push_str(&go_env_word);
    contents.push_str("\nexport ");
    contents.push_str(WORKSPACE_TOKEN_ENV);
    contents.push('=');
    contents.push_str(token);
    contents.push('\n');
    if let Some(block) = port_block {
        writeln!(
            &mut *contents,
            "export {PORT_BASE_ENV}={}\nexport {PORT_BLOCK_SIZE_ENV}={}",
            block.base(),
            block.size()
        )
        .expect("writing to a String cannot fail");
    }

    write_atomic_bytes(
        &image_root.join(WORKSPACE_ENVIRONMENT_PATH),
        contents.as_bytes(),
    )?;
    Ok(())
}

fn shell_word(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-'
                )
        })
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_words_quote_mounts_with_spaces_and_single_quotes() {
        assert_eq!(shell_word("/tmp/a b's/env"), "'/tmp/a b'\"'\"'s/env'");
    }

    #[test]
    fn workspace_env_var_names_are_the_sandbox_contract() {
        assert_eq!(GO_ENV, "GOENV");
        assert_eq!(WORKSPACE_TOKEN_ENV, "COWSHED_WORKSPACE_TOKEN");
        assert_eq!(PORT_BASE_ENV, "COWSHED_PORT_BASE");
        assert_eq!(PORT_BLOCK_SIZE_ENV, "COWSHED_PORT_BLOCK_SIZE");
    }
}
