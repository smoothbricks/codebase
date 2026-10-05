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

/// Agent harnesses export `CI`; only a real runner may change local Cargo unit identities.
pub(crate) const DEV_CI_POLICY: &str =
    "if [ \"${GITHUB_ACTIONS:-}\" != true ]; then unset CI; fi\n";

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
    // buffer cannot reallocate: 32 fixed bytes around the token, at most 68 for the port lines,
    // plus the fixed development CI policy.
    let mut contents = Zeroizing::new(String::with_capacity(
        128 + token.len() + DEV_CI_POLICY.len(),
    ));
    contents.push_str(DEV_CI_POLICY);
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

    #[test]
    fn published_environment_strips_harness_ci_but_preserves_real_ci_and_compiler_identity() {
        use crate::fork_lock::Run;

        let root = std::env::temp_dir().join(format!("cowshed-env-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".cowshed")).unwrap();
        write_workspace_environment(
            &root,
            &Zeroizing::new("token".to_owned()),
            Platform::Linux,
            None,
        )
        .unwrap();
        for marker in [None, Some(""), Some("false"), Some("true")] {
            let mut command = std::process::Command::new("/bin/sh");
            command
                .args([
                    "-c",
                    ". \"$1\"; printf '%s\\n' \"${CI-unset}\" \"$CC_aarch64_apple_darwin\" \"$CXX_aarch64_apple_darwin\" \"$AR_aarch64_apple_darwin\"",
                    "cowshed-env",
                ])
                .arg(root.join(WORKSPACE_ENVIRONMENT_PATH))
                .env_clear()
                .env("CI", "true")
                .env("CC_aarch64_apple_darwin", "/usr/bin/clang")
                .env("CXX_aarch64_apple_darwin", "/usr/bin/clang++")
                .env("AR_aarch64_apple_darwin", "/usr/bin/ar");
            if let Some(marker) = marker {
                command.env("GITHUB_ACTIONS", marker);
            }
            let output = command.output_locked().unwrap();
            assert!(output.status.success());
            let ci = if marker == Some("true") {
                "true"
            } else {
                "unset"
            };
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("{ci}\n/usr/bin/clang\n/usr/bin/clang++\n/usr/bin/ar\n"),
                "{marker:?}",
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
