use std::fmt::Write as _;
use std::path::Path;

use thiserror::Error;
use zeroize::Zeroizing;

use crate::metadata::{MetadataError, Platform, PortBlock, write_atomic_bytes};

pub const WORKSPACE_ENVIRONMENT_PATH: &str = ".cowshed/env";
pub const WORKSPACE_TOKEN_ENV: &str = "COWSHED_WORKSPACE_TOKEN";
pub const PORT_BASE_ENV: &str = "COWSHED_PORT_BASE";
/// The workspace's current port block size: `base+1 … base+size-1` are its service ports.
/// Capacity grants can grow it, so tools read the admitted size rather than assuming one.
pub const PORT_BLOCK_SIZE_ENV: &str = "COWSHED_PORT_BLOCK_SIZE";

#[derive(Debug, Error)]
pub enum WorkspaceEnvironmentError {
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
    token: &Zeroizing<String>,
    platform: Platform,
    port_block: Option<PortBlock>,
) -> Result<(), WorkspaceEnvironmentError> {
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
    // Token alphabet is unpadded base64url (`A-Za-z0-9_-`), already shell-safe; it is pushed
    // into a Zeroizing buffer and never through an intermediate String. Capacity is sized so the
    // buffer cannot reallocate after the token is copied: 32 fixed bytes around the token plus
    // at most 68 for the two port lines.
    let mut contents = Zeroizing::new(String::with_capacity(128 + token.len()));
    contents.push_str("export ");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_env_var_names_are_the_sandbox_contract() {
        assert_eq!(WORKSPACE_TOKEN_ENV, "COWSHED_WORKSPACE_TOKEN");
        assert_eq!(PORT_BASE_ENV, "COWSHED_PORT_BASE");
        assert_eq!(PORT_BLOCK_SIZE_ENV, "COWSHED_PORT_BLOCK_SIZE");
    }
}
