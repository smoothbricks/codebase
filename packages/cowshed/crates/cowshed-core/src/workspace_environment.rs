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
/// The sandbox's `TMPDIR` ([`crate::storage::StorageLayout::exec_temp_dir`]), exported to host
/// shells too, so a host gate and a land gate of one workspace share one scratch directory and
/// neither shares the machine's: Bun reads every ancestor directory of its working directory
/// whole, and a user temp directory every concurrent gate fills holds each start for seconds.
pub const TEMP_DIR_ENV: &str = "TMPDIR";
/// The short link to the workspace's runtime dir, [`crate::sandbox::workspace_runtime_link`],
/// exported so a host shell names the link its jobs use without re-deriving it: the Nx socket
/// dir host shells and jobs share is `nx` below it, by that literal path.
pub const RUNTIME_LINK_ENV: &str = "COWSHED_RUNTIME_LINK";

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
    #[error(
        "workspace temp directory {0:?} cannot be exported: it must be an absolute UTF-8 path without a quote or newline"
    )]
    UnexportableTempDir(std::path::PathBuf),
    #[error(transparent)]
    Publication(#[from] MetadataError),
}

/// Where `.cowshed/env` is published, and what is known about the workspace there.
#[derive(Clone, Copy, Debug)]
pub enum EnvironmentMount<'a> {
    /// A mint's mount, possibly a staging path: no sandbox serves it yet, so the file names
    /// neither a TMPDIR nor a runtime link. The supervisor start that serves it adds both.
    Minted(&'a Path),
    /// The workspace mount its supervisor serves, with that sandbox's `TMPDIR`. The runtime
    /// link is named after this mount.
    Served {
        workspace_mount: &'a Path,
        temp_dir: &'a Path,
    },
}

impl<'a> EnvironmentMount<'a> {
    pub fn root(self) -> &'a Path {
        match self {
            Self::Minted(root)
            | Self::Served {
                workspace_mount: root,
                ..
            } => root,
        }
    }
}

/// Atomically publish the source-able, workspace-local build environment inside an image. Its
/// one caller is `workspace_credentials::publish_workspace_environment`, which reads the token
/// the image publishes. A served mount also exports its `TMPDIR`, single-quoted, and its
/// runtime link.
pub(crate) fn write_workspace_environment(
    mount: EnvironmentMount<'_>,
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
    let served = match mount {
        EnvironmentMount::Minted(_) => None,
        EnvironmentMount::Served {
            workspace_mount,
            temp_dir,
        } => {
            let temp_dir = temp_dir
                .to_str()
                .filter(|text| temp_dir.is_absolute() && !text.contains(['\'', '\n']))
                .ok_or_else(|| {
                    WorkspaceEnvironmentError::UnexportableTempDir(temp_dir.to_path_buf())
                })?;
            Some((
                temp_dir,
                crate::sandbox::workspace_runtime_link(workspace_mount),
            ))
        }
    };
    // Token alphabet is unpadded base64url (`A-Za-z0-9_-`), already shell-safe; it is pushed
    // into a Zeroizing buffer and never through an intermediate String. Capacity is sized so the
    // buffer cannot reallocate: 32 fixed bytes around the token, at most 68 for the port lines,
    // 18 around the temp directory, 49 for the runtime link line, plus the fixed development CI
    // policy.
    let mut contents = Zeroizing::new(String::with_capacity(
        192 + token.len()
            + DEV_CI_POLICY.len()
            + served.as_ref().map_or(0, |(temp_dir, _)| temp_dir.len()),
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
    if let Some((temp_dir, runtime_link)) = served {
        // The link is `/tmp/cs-` and hex digits: shell-safe, so it is written unquoted.
        writeln!(
            &mut *contents,
            "export {TEMP_DIR_ENV}='{temp_dir}'\nexport {RUNTIME_LINK_ENV}={}",
            runtime_link.display()
        )
        .expect("writing to a String cannot fail");
    }

    write_atomic_bytes(
        &mount.root().join(WORKSPACE_ENVIRONMENT_PATH),
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
        assert_eq!(TEMP_DIR_ENV, "TMPDIR");
        assert_eq!(RUNTIME_LINK_ENV, "COWSHED_RUNTIME_LINK");
    }

    #[test]
    fn a_temp_dir_that_would_not_survive_single_quotes_is_refused() {
        for refused in [
            "relative/tmp",
            "/private/tmp/it's",
            "/private/tmp/two\nlines",
        ] {
            assert!(matches!(
                write_workspace_environment(
                    EnvironmentMount::Served {
                        workspace_mount: Path::new("/nonexistent-image"),
                        temp_dir: Path::new(refused),
                    },
                    &Zeroizing::new("token".to_owned()),
                    Platform::Linux,
                    None,
                ),
                Err(WorkspaceEnvironmentError::UnexportableTempDir(path)) if path == Path::new(refused)
            ));
        }
    }

    #[test]
    fn published_environment_strips_harness_ci_but_preserves_real_ci_and_compiler_identity() {
        use crate::fork_lock::Run;

        /// Removes the fixture root however the test ends, a failed assertion included.
        struct Root(std::path::PathBuf);
        impl Drop for Root {
            fn drop(&mut self) {
                if let Err(error) = std::fs::remove_dir_all(&self.0) {
                    eprintln!("remove {}: {error}", self.0.display());
                }
            }
        }
        let root = Root(std::env::temp_dir().join(format!("cowshed-env-{}", uuid::Uuid::new_v4())));
        let root = &root.0;
        std::fs::create_dir_all(root.join(".cowshed")).unwrap();
        let temp_dir = root.join("exec temp");
        write_workspace_environment(
            EnvironmentMount::Served {
                workspace_mount: root,
                temp_dir: &temp_dir,
            },
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
                    ". \"$1\"; printf '%s\\n' \"${CI-unset}\" \"$CC_aarch64_apple_darwin\" \"$CXX_aarch64_apple_darwin\" \"$AR_aarch64_apple_darwin\" \"$TMPDIR\" \"$COWSHED_RUNTIME_LINK\"",
                    "cowshed-env",
                ])
                .arg(root.join(WORKSPACE_ENVIRONMENT_PATH))
                .env_clear()
                .env("CI", "true")
                .env("CC_aarch64_apple_darwin", "/usr/bin/clang")
                .env("CXX_aarch64_apple_darwin", "/usr/bin/clang++")
                .env("AR_aarch64_apple_darwin", "/usr/bin/ar")
                .env("TMPDIR", "/var/folders/machine/T");
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
                format!(
                    "{ci}\n/usr/bin/clang\n/usr/bin/clang++\n/usr/bin/ar\n{}\n{}\n",
                    temp_dir.display(),
                    crate::sandbox::workspace_runtime_link(root).display()
                ),
                "{marker:?}",
            );
        }
    }
}
