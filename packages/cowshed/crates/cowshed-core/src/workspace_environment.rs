use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::metadata::{MetadataError, Platform, PortBlock, write_atomic_bytes};

pub const WORKSPACE_ENVIRONMENT_PATH: &str = ".cowshed/env";
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

/// Atomically publish the source-able, workspace-local build environment inside an image.
pub fn write_workspace_environment(
    image_root: &Path,
    workspace_mount: &Path,
    token: &str,
    platform: Platform,
    port_block: Option<PortBlock>,
) -> Result<(), WorkspaceEnvironmentError> {
    if !workspace_mount.is_absolute() {
        return Err(WorkspaceEnvironmentError::InvalidMount(
            workspace_mount.to_owned(),
        ));
    }
    match (platform, port_block) {
        (Platform::Macos, Some(block)) => block.validate().map_err(|_| {
            WorkspaceEnvironmentError::InvalidPortWiring {
                platform,
                port_block,
            }
        })?,
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
    let mut contents = format!(
        "export GOENV={}\nexport COWSHED_WORKSPACE_TOKEN={}\n",
        shell_word(go_env),
        shell_word(token),
    );
    if let Some(block) = port_block {
        contents.push_str(&format!("export COWSHED_PORT_BASE={}\n", block.base()));
    }

    write_atomic_bytes(&image_root.join(WORKSPACE_ENVIRONMENT_PATH), contents.as_bytes())?;
    Ok(())
}

fn shell_word(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'_' | b'@' | b'%' | b'+' | b'=' | b':' | b',' | b'.' | b'/' | b'-')
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
}
