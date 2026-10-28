use std::fmt::Write as _;
use std::path::Path;

use crate::capabilities::{CapabilityContribution, EnvAction};

use thiserror::Error;
use zeroize::Zeroizing;

use crate::metadata::{MetadataError, Platform, PortBlock, write_atomic_bytes};

pub const WORKSPACE_ENVIRONMENT_PATH: &str = ".cowshed/env";

/// The variable `.cowshed/env` restores `NX_PARALLEL` from after a devenv shell sets its own: a
/// job's Nx fan-out, its jobserver count (05_gateway.md "Parallelism inside a job").
pub const NX_PARALLEL_RESTORE_ENV: &str = "COWSHED_NX_PARALLEL";
pub const WORKSPACE_TOKEN_ENV: &str = "COWSHED_WORKSPACE_TOKEN";
pub const PORT_BASE_ENV: &str = "COWSHED_PORT_BASE";
/// The workspace's current port block size: `base+1 … base+size-1` are its service ports.
/// Capacity grants can grow it, so tools read the admitted size rather than assuming one.
pub const PORT_BLOCK_SIZE_ENV: &str = "COWSHED_PORT_BLOCK_SIZE";
pub const NX_REMOTE_SERVER_ENV: &str = "NX_SELF_HOSTED_REMOTE_CACHE_SERVER";
/// A shed reads its project's Nx cache and never stores to it; only a land publishes
/// (05_gateway.md "`/nx/v1/cache/{hash}`").
pub const NX_REMOTE_READ_ONLY_ENV: &str = "NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY";
/// The sandbox's `TMPDIR` ([`crate::storage::StorageLayout::exec_temp_dir`]), exported to host
/// shells too, so a host gate and a land gate of one workspace share one scratch directory and
/// neither shares the machine's: Bun reads every ancestor directory of its working directory
/// whole, and a user temp directory every concurrent gate fills holds each start for seconds.
pub const TEMP_DIR_ENV: &str = "TMPDIR";
/// Node and compatible runtimes keep module compilation in the workspace's private cache,
/// never the runtime's default loose directory under TMPDIR.
pub const NODE_COMPILE_CACHE_ENV: &str = "NODE_COMPILE_CACHE";
pub(crate) const NODE_COMPILE_CACHE_DIRECTORY: &str = "node-compile-cache";
/// The short link to the workspace's runtime dir, [`crate::sandbox::workspace_runtime_link`],
/// exported so a host shell names the link its jobs use without re-deriving it: the Nx socket
/// dir host shells and jobs share is `nx` below it, by that literal path.
pub const RUNTIME_LINK_ENV: &str = "COWSHED_RUNTIME_LINK";
/// The build-cache volume's root on a host that has it provisioned (01a_apfs.md, "Dedicated
/// build-cache volume"): the one spelling every consumer reads, never a path it hardcodes.
pub const BUILD_CACHE_ENV: &str = "COWSHED_BUILD_CACHE";

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
    #[error("workspace capability variable {0} cannot be exported as UTF-8 shell text")]
    UnexportableCapability(&'static str),
    #[error(
        "workspace Node compile cache {0:?} cannot be exported as an absolute UTF-8 shell path"
    )]
    UnexportableNodeCompileCache(std::path::PathBuf),
    #[error("installed cowshed directory {0:?} cannot be exported as UTF-8 shell text")]
    UnexportableInstalledDirectory(std::path::PathBuf),
    #[error("build-cache root {0:?} cannot be exported as an absolute UTF-8 shell path")]
    UnexportableBuildCache(std::path::PathBuf),
    #[error(transparent)]
    Publication(#[from] MetadataError),
}

/// Where `.cowshed/env` is published, and what is known about the workspace there.
#[derive(Clone, Copy, Debug)]
pub enum EnvironmentMount<'a> {
    /// A mint's mount, possibly a staging path: no sandbox serves it yet, so the file names
    /// neither a TMPDIR, a runtime link nor a compile cache. The supervisor start adds them.
    Minted(&'a Path),
    /// The workspace mount its supervisor serves, with that sandbox's `TMPDIR`. The runtime
    /// link is named after this mount.
    Served {
        workspace_mount: &'a Path,
        /// The controller-selected host identity, before the job HOME is rebound.
        host_home: &'a Path,
        temp_dir: &'a Path,
        capability_env: &'a CapabilityContribution,
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
/// one caller is `workspace_credentials::publish_workspace_environment`. A served mount also exports
/// its `TMPDIR`, single-quoted, and its runtime link, and binds module compilation to the
/// receiving workspace's private cache. `build_cache` is the provisioned build-cache volume's
/// root, exported as [`BUILD_CACHE_ENV`]; a host without one gets no line.
pub(crate) fn write_workspace_environment(
    mount: EnvironmentMount<'_>,
    platform: Platform,
    port_block: Option<PortBlock>,
    build_cache: Option<&Path>,
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
            ..
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
    let node_compile_cache = served.as_ref().map(|_| {
        mount
            .root()
            .join(".cowshed/cache")
            .join(NODE_COMPILE_CACHE_DIRECTORY)
    });
    let node_compile_cache_text = node_compile_cache
        .as_ref()
        .map(|path| {
            path.to_str()
                .filter(|text| path.is_absolute() && !text.contains(['\0', '\n', '\r']))
                .ok_or_else(|| {
                    WorkspaceEnvironmentError::UnexportableNodeCompileCache(path.clone())
                })
        })
        .transpose()?;
    let installed_directory = match mount {
        EnvironmentMount::Served { host_home, .. } => {
            Some(crate::host_dirs::installed_binary_directory(host_home))
        }
        EnvironmentMount::Minted(_) => None,
    };
    let installed_directory_text = installed_directory
        .as_ref()
        .map(|path| {
            path.to_str()
                .filter(|text| !text.contains(['\0', '\n', '\r']))
                .ok_or_else(|| {
                    WorkspaceEnvironmentError::UnexportableInstalledDirectory(path.clone())
                })
        })
        .transpose()?;
    // Keep the capability values in one Zeroizing buffer. Reserve their shell-escaped bound
    // too so publication does not reallocate and copy them.
    let capability_bytes = match mount {
        EnvironmentMount::Served { capability_env, .. } => capability_env
            .env
            .iter()
            .filter(|(name, _)| host_capability_names().any(|host| host == **name))
            .map(|(name, action)| {
                name.len()
                    + 96
                    + match action {
                        EnvAction::Own(value) | EnvAction::Default(value) => value.len() * 4,
                        EnvAction::Append(_) | EnvAction::Unset => 0,
                    }
            })
            .sum::<usize>(),
        EnvironmentMount::Minted(_) => 0,
    };
    let mut contents = Zeroizing::new(String::with_capacity(
        384 + DEV_CI_POLICY.len()
            + build_cache.map_or(0, |path| path.as_os_str().len() * 4 + 64)
            + capability_bytes
            + installed_directory_text.map_or(0, |path| path.len() * 4 + 96)
            + node_compile_cache_text.map_or(0, |path| path.len() * 4 + 256)
            + served
                .as_ref()
                .map_or(0, |(temp_dir, _)| temp_dir.len() * 5),
    ));
    let build_cache_text = build_cache
        .map(|path| {
            path.to_str()
                .filter(|text| path.is_absolute() && !text.contains(['\0', '\n', '\r']))
                .ok_or_else(|| WorkspaceEnvironmentError::UnexportableBuildCache(path.to_owned()))
        })
        .transpose()?;
    contents.push_str(DEV_CI_POLICY);
    contents.push_str(&nx_parallel_restore());
    if let Some(root) = build_cache_text {
        write!(&mut *contents, "export {BUILD_CACHE_ENV}=")
            .expect("writing to a String cannot fail");
        write_shell_quoted(&mut contents, root);
        contents.push('\n');
    }
    // No bare `COWSHED_WORKSPACE_TOKEN`: every shell that sources this file would hand it to every
    // child. `.cowshed/workspace.json` is what marks a workspace.
    if let Some(block) = port_block {
        writeln!(
            &mut *contents,
            "export {PORT_BASE_ENV}={}\nexport {PORT_BLOCK_SIZE_ENV}={}",
            block.base(),
            block.size()
        )
        .expect("writing to a String cannot fail");
    }
    if mount.root().join("nx.json").is_file() {
        let port = port_block.map_or(7_644, |block| block.base());
        writeln!(
            &mut *contents,
            "export {NX_REMOTE_SERVER_ENV}=http://127.0.0.1:{port}/nx\nexport {NX_REMOTE_READ_ONLY_ENV}=true"
        )
        .expect("writing to a String cannot fail");
    }
    if let (Some((temp_dir, runtime_link)), Some(node_compile_cache)) =
        (served, node_compile_cache_text)
    {
        // The link is `/tmp/cs-` and hex digits: shell-safe, so it is written unquoted.
        writeln!(
            &mut *contents,
            "export {TEMP_DIR_ENV}='{temp_dir}'\nexport {RUNTIME_LINK_ENV}={}",
            runtime_link.display()
        )
        .expect("writing to a String cannot fail");
        // Host shells use this workspace's cache, not the caller's XDG cache. An activated
        // read-only job keeps its already-authorized exec-temp cache instead of writing source.
        contents.push_str("if [ \"${XDG_CACHE_HOME-}\" = ");
        write_shell_quoted(&mut contents, temp_dir);
        contents.push_str("/cache ]; then\n");
        writeln!(
            &mut *contents,
            "export {NODE_COMPILE_CACHE_ENV}=\"${{XDG_CACHE_HOME}}/{NODE_COMPILE_CACHE_DIRECTORY}\"\nelse"
        )
        .expect("writing to a String cannot fail");
        write!(&mut *contents, "export {NODE_COMPILE_CACHE_ENV}=")
            .expect("writing to a String cannot fail");
        write_shell_quoted(&mut contents, node_compile_cache);
        contents.push_str("\nfi\n");
    }
    if let Some(directory) = installed_directory_text {
        contents.push_str("export PATH=");
        write_shell_quoted(&mut contents, directory);
        contents.push_str("\"${PATH:+:}${PATH-}\"\n");
    } else {
        // A newly minted image has no served runtime identity yet. Do not
        // let a host shell borrow an inherited sibling's served marker.
        writeln!(
            &mut *contents,
            "unset {RUNTIME_LINK_ENV} {NODE_COMPILE_CACHE_ENV}"
        )
        .expect("writing to a String cannot fail");
    }
    if let EnvironmentMount::Served { capability_env, .. } = mount {
        for name in host_capability_names() {
            let Some(action) = capability_env.env.get(name) else {
                continue;
            };
            match action {
                EnvAction::Unset => {
                    writeln!(&mut *contents, "unset {name}")
                        .expect("writing to a String cannot fail");
                }
                EnvAction::Own(value) | EnvAction::Default(value) => {
                    let value = value
                        .to_str()
                        .filter(|text| !text.contains(['\0', '\n', '\r']))
                        .ok_or(WorkspaceEnvironmentError::UnexportableCapability(name))?;
                    if matches!(action, EnvAction::Default(_)) {
                        writeln!(&mut *contents, "if [ \"${{{name}+x}}\" != x ]; then")
                            .expect("writing to a String cannot fail");
                    }
                    write!(&mut *contents, "export {name}=")
                        .expect("writing to a String cannot fail");
                    write_shell_quoted(&mut contents, value);
                    contents.push('\n');
                    if matches!(action, EnvAction::Default(_)) {
                        contents.push_str("fi\n");
                    }
                }
                EnvAction::Append(_) => {
                    return Err(WorkspaceEnvironmentError::UnexportableCapability(name));
                }
            }
        }
    }

    write_atomic_bytes(
        &mount.root().join(WORKSPACE_ENVIRONMENT_PATH),
        contents.as_bytes(),
    )?;
    Ok(())
}

/// The line that puts a job's Nx fan-out back after a devenv shell sourced before `.cowshed/env`
/// set its own `NX_PARALLEL` (05_gateway.md "Parallelism inside a job").
fn nx_parallel_restore() -> String {
    format!(
        "if [ -n \"${{{NX_PARALLEL_RESTORE_ENV}-}}\" ]; then export NX_PARALLEL=\"${NX_PARALLEL_RESTORE_ENV}\"; fi\n"
    )
}

/// The capability-contributed variables a host shell sources from `.cowshed/env` as its jobs get them: the
/// compiler-cache client's, a Cargo project's recording of unit use, and Nx's cache size.
fn host_capability_names() -> impl Iterator<Item = &'static str> {
    crate::capabilities::sccache::HOST_ENVIRONMENT
        .into_iter()
        .chain(crate::capabilities::cargo::HOST_ENVIRONMENT)
        .chain(crate::capabilities::mbx::HOST_ENVIRONMENT)
        .chain(["NX_MAX_CACHE_SIZE"])
}
fn write_shell_quoted(contents: &mut String, value: &str) {
    contents.push('\'');
    for character in value.chars() {
        if character == '\'' {
            contents.push_str("'\\''");
        } else {
            contents.push(character);
        }
    }
    contents.push('\'');
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::test_backstop::CommandBackstop as _;

    /// A host with a provisioned build-cache volume publishes its root, the one spelling every
    /// consumer reads; a host without one publishes no line (02_workspaces.md).
    #[test]
    fn the_build_cache_root_is_exported_only_when_provisioned() {
        let root =
            std::env::temp_dir().join(format!("cowshed-build-cache-env-{}", uuid::Uuid::new_v4()));
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).unwrap();
        let build_cache = root.join("shared/build-cache");
        for (provisioned, expected) in [
            (
                Some(build_cache.as_path()),
                build_cache.display().to_string(),
            ),
            (None, "unset".to_owned()),
        ] {
            write_workspace_environment(
                EnvironmentMount::Minted(&mount),
                Platform::Linux,
                None,
                provisioned,
            )
            .unwrap();
            let output = std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    ". \"$1\"; printf '%s' \"${COWSHED_BUILD_CACHE-unset}\"",
                    "build-cache",
                ])
                .arg(mount.join(WORKSPACE_ENVIRONMENT_PATH))
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .output_bounded()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn workspace_env_var_names_are_the_sandbox_contract() {
        assert_eq!(WORKSPACE_TOKEN_ENV, "COWSHED_WORKSPACE_TOKEN");
        assert_eq!(NODE_COMPILE_CACHE_ENV, "NODE_COMPILE_CACHE");
        assert_eq!(PORT_BASE_ENV, "COWSHED_PORT_BASE");
        assert_eq!(PORT_BLOCK_SIZE_ENV, "COWSHED_PORT_BLOCK_SIZE");
        assert_eq!(TEMP_DIR_ENV, "TMPDIR");
        assert_eq!(RUNTIME_LINK_ENV, "COWSHED_RUNTIME_LINK");
        assert_eq!(
            crate::capabilities::cargo::MTIME_ON_USE_ENV,
            "CARGO_UNSTABLE_MTIME_ON_USE"
        );
    }

    /// A granted job's `NX_PARALLEL` survives the devenv shell sourced before `.cowshed/env`; a
    /// shell no grant marked keeps its own.
    #[test]
    fn the_grants_nx_fan_out_is_restored_over_a_devenv_shells() {
        let run = |granted: Option<&str>| {
            let mut shell = std::process::Command::new("/bin/sh");
            shell
                .arg("-c")
                .arg(format!(
                    "{}printf %s \"$NX_PARALLEL\"",
                    nx_parallel_restore()
                ))
                .env("NX_PARALLEL", "100%")
                .env_remove(NX_PARALLEL_RESTORE_ENV);
            if let Some(granted) = granted {
                shell.env(NX_PARALLEL_RESTORE_ENV, granted);
            }
            let output = cowshed_os::test_backstop::CommandBackstop::output_bounded(&mut shell)
                .expect("run sh");
            String::from_utf8(output.stdout).expect("UTF-8")
        };
        assert_eq!(run(Some("2")), "2");
        assert_eq!(run(None), "100%");
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
                                                            host_home: Path::new("/Users/tester"),
                                    temp_dir: Path::new(refused),
                                    capability_env: &CapabilityContribution::default(),
                                },
                                Platform::Linux,
            None,
            None,
                            ),
                            Err(WorkspaceEnvironmentError::UnexportableTempDir(path)) if path == Path::new(refused)
                        ));
        }
    }

    #[test]
    fn published_environment_strips_harness_ci_but_preserves_real_ci_and_compiler_identity() {
        /// Removes the fixture root however the test ends, a failed assertion included.
        struct Root(std::path::PathBuf);
        impl Drop for Root {
            fn drop(&mut self) {
                if let Err(error) = std::fs::remove_dir_all(&self.0) {
                    cowshed_os::eprintln!("remove {}: {error}", self.0.display());
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
                host_home: root,
                capability_env: &CapabilityContribution::default(),
            },
            Platform::Linux,
            None,
            None,
        )
        .unwrap();
        let published = std::fs::read_to_string(root.join(WORKSPACE_ENVIRONMENT_PATH)).unwrap();
        assert!(!published.contains(NX_REMOTE_SERVER_ENV));
        assert!(!published.contains(NX_REMOTE_READ_ONLY_ENV));
        assert!(!published.contains("NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN"));
        std::fs::write(root.join("nx.json"), "{}").unwrap();
        write_workspace_environment(
            EnvironmentMount::Served {
                workspace_mount: root,
                temp_dir: &temp_dir,
                host_home: root,
                capability_env: &CapabilityContribution::default(),
            },
            Platform::Linux,
            None,
            None,
        )
        .unwrap();
        let published = std::fs::read_to_string(root.join(WORKSPACE_ENVIRONMENT_PATH)).unwrap();
        assert!(
            published
                .contains("export NX_SELF_HOSTED_REMOTE_CACHE_SERVER=http://127.0.0.1:7644/nx\n")
        );
        assert!(published.contains("export NX_SELF_HOSTED_REMOTE_CACHE_READ_ONLY=true\n"));
        assert!(!published.contains("NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN"));
        assert!(!published.contains("--exit-on-stdin-eof"));
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
            let output = command.output_bounded().unwrap();
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

    /// A served environment of a Cargo project makes a host shell's Cargo record each unit use,
    /// whatever the shell exported before it sourced the file; a plain repository's environment
    /// leaves the variable alone.
    #[test]
    fn a_cargo_projects_published_environment_records_each_unit_use() {
        let root =
            std::env::temp_dir().join(format!("cowshed-mtime-on-use-{}", uuid::Uuid::new_v4()));
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).unwrap();
        let mut cargo = CapabilityContribution::default();
        cargo.env.insert(
            crate::capabilities::cargo::MTIME_ON_USE_ENV,
            EnvAction::Own(std::ffi::OsString::from("true")),
        );
        let plain = CapabilityContribution::default();
        for (label, contribution, expected) in [
            ("a Cargo project", &cargo, "true\n"),
            ("a plain repository", &plain, "false\n"),
        ] {
            write_workspace_environment(
                EnvironmentMount::Served {
                    workspace_mount: &mount,
                    host_home: &root,
                    temp_dir: &root,
                    capability_env: contribution,
                },
                Platform::Linux,
                None,
                None,
            )
            .unwrap();
            let output = std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    ". \"$1\"; printf '%s\\n' \"${CARGO_UNSTABLE_MTIME_ON_USE-unset}\"",
                    "cowshed-env",
                ])
                .arg(mount.join(WORKSPACE_ENVIRONMENT_PATH))
                .env_clear()
                .env(crate::capabilities::cargo::MTIME_ON_USE_ENV, "false")
                .output_bounded()
                .unwrap();
            assert!(output.status.success(), "{label}: {output:?}");
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                expected,
                "{label}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn served_cargo_path_uses_host_identity_after_home_is_rebound() {
        let root = std::env::temp_dir().join(format!("cowshed-host-path-{}", uuid::Uuid::new_v4()));
        let mount = root.join("workspace");
        let home = root.join("host's home $literal");
        let private_home = mount.join(".cowshed/home");
        std::fs::create_dir_all(mount.join(".cowshed")).unwrap();
        write_workspace_environment(
            EnvironmentMount::Served {
                workspace_mount: &mount,
                host_home: &home,
                temp_dir: &root,
                capability_env: &CapabilityContribution::default(),
            },
            Platform::Linux,
            None,
            None,
        )
        .unwrap();
        let output = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                ". \"$1\"; printf '%s\\0' \"$HOME\" \"${PATH%%:*}\"",
                "served-path",
            ])
            .arg(mount.join(WORKSPACE_ENVIRONMENT_PATH))
            .env_clear()
            .env("HOME", &private_home)
            .env("PATH", "/toolchain/bin:/usr/bin:/bin")
            .output_bounded()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            output.stdout,
            format!(
                "{}\0{}\0",
                private_home.display(),
                crate::host_dirs::installed_binary_directory(&home).display()
            )
            .as_bytes()
        );
        write_workspace_environment(
            EnvironmentMount::Minted(&mount),
            Platform::Linux,
            None,
            None,
        )
        .unwrap();
        let output = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                ". \"$1\"; printf '%s' \"${COWSHED_RUNTIME_LINK-unset}\"",
                "minted-path",
            ])
            .arg(mount.join(WORKSPACE_ENVIRONMENT_PATH))
            .env("COWSHED_RUNTIME_LINK", "/tmp/stale-sibling")
            .output_bounded()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert_eq!(output.stdout, b"unset");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn host_capability_values_are_shell_quoted_and_defaults_preserve_overrides() {
        let root =
            std::env::temp_dir().join(format!("cowshed-env-values-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join(".cowshed")).unwrap();
        let mut contribution = CapabilityContribution::default();
        contribution.env.insert(
            "RUSTC_WRAPPER",
            EnvAction::Own("/store/it's a client".into()),
        );
        contribution.env.insert(
            "NX_MAX_CACHE_SIZE",
            EnvAction::Default("10737418240".into()),
        );
        write_workspace_environment(
            EnvironmentMount::Served {
                workspace_mount: &root,
                host_home: &root,
                temp_dir: &root,
                capability_env: &contribution,
            },
            Platform::Linux,
            None,
            None,
        )
        .unwrap();
        for override_value in [None, Some("20GB")] {
            let mut command = std::process::Command::new("/bin/sh");
            command
                .env_clear()
                .args([
                    "-c",
                    ". \"$1\"; printf '%s\\n' \"$RUSTC_WRAPPER\" \"$NX_MAX_CACHE_SIZE\"",
                    "host",
                ])
                .arg(root.join(WORKSPACE_ENVIRONMENT_PATH));
            if let Some(value) = override_value {
                command.env("NX_MAX_CACHE_SIZE", value);
            }
            let output = command.output_bounded().unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!(
                    "/store/it's a client\n{}\n",
                    override_value.unwrap_or("10737418240")
                )
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Node and Bun compile modules into the receiving workspace's private cache when a host
    /// shell sources its published environment: enableCompileCache() with no argument — the
    /// call whose TMPDIR default stranded a retired workspace's loose node-compile-cache —
    /// resolves through the published NODE_COMPILE_CACHE, so the bytecode lands under
    /// .cowshed/cache, never under the served TMPDIR or the caller's own cache. A read-only
    /// job's activated shell (its XDG_CACHE_HOME is already the exec-temp cache) keeps that
    /// authorized cache instead of writing the image, and a minted file enables no inherited
    /// cache at all. Foreign files in every cache the run could have touched survive.
    #[test]
    fn host_shells_compile_into_the_receiving_workspaces_private_cache() {
        /// Removes the fixture root however the test ends, a failed assertion included.
        struct Root(std::path::PathBuf);
        impl Drop for Root {
            fn drop(&mut self) {
                if let Err(error) = std::fs::remove_dir_all(&self.0) {
                    cowshed_os::eprintln!("remove {}: {error}", self.0.display());
                }
            }
        }

        fn files_under(root: &Path) -> Vec<std::path::PathBuf> {
            let mut files = Vec::new();
            let mut pending = vec![root.to_path_buf()];
            while let Some(directory) = pending.pop() {
                let entries = match std::fs::read_dir(&directory) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => panic!("read {}: {error}", directory.display()),
                };
                for entry in entries {
                    let entry = entry.unwrap_or_else(|error| panic!("read entry: {error}"));
                    if entry.file_type().expect("entry type").is_dir() {
                        pending.push(entry.path());
                    } else {
                        files.push(entry.path());
                    }
                }
            }
            files.sort();
            files
        }

        fn host_program(name: &str) -> std::path::PathBuf {
            let path = std::env::var_os("PATH").expect("the gate PATH names the host toolchain");
            std::env::split_paths(&path)
                .map(|directory| directory.join(name))
                .find(|candidate| candidate.is_file())
                .unwrap_or_else(|| panic!("host {name} is required for this proof"))
        }

        // The producer's actual call, then a real module load whose bytecode the runtime
        // flushes into the resolved cache before it exits.
        const COMPILE: &str = r#"const m = require("node:module");
    m.enableCompileCache();
    const dir = m.getCompileCacheDir();
    if (!dir) { console.error("compile cache is not enabled"); process.exit(3); }
    console.log(dir);
    import("./compile-cache-lib.mjs").then((lib) => {
      if (lib.answer() !== 42) { process.exit(4); }
      if (m.flushCompileCache) { m.flushCompileCache(); }
    });
    "#;
        const DISABLED: &str = r#"const m = require("node:module");
    if (m.getCompileCacheDir() !== undefined) { console.error(m.getCompileCacheDir()); process.exit(5); }
    "#;

        let root = Root(
            std::env::temp_dir().join(format!("cowshed-compile-cache-{}", uuid::Uuid::new_v4())),
        );
        let root = &root.0;
        let mount = root.join("workspace");
        std::fs::create_dir_all(mount.join(".cowshed")).unwrap();
        // A space in the served temp dir, so the published quoting is what the shell runs.
        let temp_dir = root.join("exec temp");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let caller_cache = root.join("caller cache");
        std::fs::create_dir_all(&caller_cache).unwrap();
        let caller_sentinel = caller_cache.join("sentinel");
        std::fs::write(&caller_sentinel, b"the caller's own").unwrap();
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(
            work.join("compile-cache-lib.mjs"),
            "export function answer() { return 42; }\n",
        )
        .unwrap();
        let compile = work.join("compile.cjs");
        std::fs::write(&compile, COMPILE).unwrap();
        let disabled = work.join("disabled.cjs");
        std::fs::write(&disabled, DISABLED).unwrap();
        let runtimes = [host_program("node"), host_program("bun")];

        write_workspace_environment(
            EnvironmentMount::Served {
                workspace_mount: &mount,
                host_home: root,
                temp_dir: &temp_dir,
                capability_env: &CapabilityContribution::default(),
            },
            Platform::Linux,
            None,
            None,
        )
        .unwrap();

        // Source the published file in a real shell, as a host activation does, with the
        // caller's own cache and machine temp already in the environment to be overridden.
        let run = |xdg_cache_home: &Path, program: &Path, entry: &Path| {
            let mut command = std::process::Command::new("sh");
            command
                .env_clear()
                .env("PATH", std::env::var_os("PATH").expect("gate PATH"))
                .args(["-c", ". \"$1\"; exec \"$2\" \"$3\"", "host-shell"])
                .arg(mount.join(WORKSPACE_ENVIRONMENT_PATH))
                .arg(program)
                .arg(entry)
                .current_dir(&work)
                .env("HOME", root)
                .env("TMPDIR", root.join("machine tmp"))
                .env("XDG_CACHE_HOME", xdg_cache_home)
                .env(NODE_COMPILE_CACHE_ENV, &caller_cache);
            let output = command.output_bounded().unwrap();
            assert!(
                output.status.success(),
                "{} {}: {output:?}",
                program.display(),
                entry.display()
            );
            output
        };
        let compiled_into = |output: &std::process::Output, expected: &Path| {
            let stdout = std::str::from_utf8(&output.stdout).unwrap();
            let reported = std::path::PathBuf::from(stdout.lines().next().expect("cache dir"));
            assert!(
                reported.starts_with(expected),
                "compiled into {}, not {}",
                reported.display(),
                expected.display()
            );
            assert!(
                files_under(&reported)
                    .iter()
                    .any(|file| std::fs::metadata(file).expect("cache file").len() > 0),
                "no bytecode under {}",
                reported.display()
            );
        };

        // An ordinary host shell: the bytecode is the receiving workspace's, under its
        // private cache, for both runtimes.
        let image_cache = mount
            .join(".cowshed/cache")
            .join(NODE_COMPILE_CACHE_DIRECTORY);
        for program in &runtimes {
            let output = run(&caller_cache, program, &compile);
            compiled_into(&output, &image_cache);
        }
        let image_files = files_under(&image_cache);
        assert!(!image_files.is_empty());
        assert!(
            !temp_dir.join(NODE_COMPILE_CACHE_DIRECTORY).exists(),
            "a loose compile cache under the served TMPDIR"
        );
        assert!(
            !root
                .join("machine tmp")
                .join(NODE_COMPILE_CACHE_DIRECTORY)
                .exists(),
            "a loose compile cache under the caller's TMPDIR"
        );

        // A read-only job's activated shell keeps its already-authorized exec-temp cache
        // and writes nothing more into the image.
        let exec_cache = temp_dir.join("cache");
        let exec_compile_cache = exec_cache.join(NODE_COMPILE_CACHE_DIRECTORY);
        for program in &runtimes {
            let output = run(&exec_cache, program, &compile);
            compiled_into(&output, &exec_compile_cache);
        }
        assert_eq!(
            files_under(&image_cache),
            image_files,
            "a read-only shell wrote the image's cache"
        );
        assert!(
            !temp_dir.join(NODE_COMPILE_CACHE_DIRECTORY).exists(),
            "a loose compile cache beside the read-only cache"
        );

        // The caller's cache is untouched, file for file and byte for byte.
        assert_eq!(
            files_under(&caller_cache),
            std::slice::from_ref(&caller_sentinel)
        );
        assert_eq!(
            std::fs::read(&caller_sentinel).unwrap(),
            b"the caller's own"
        );

        // A minted image publishes no cache identity: an inherited sibling value is unset,
        // so neither runtime starts with a compile cache enabled into it.
        write_workspace_environment(
            EnvironmentMount::Minted(&mount),
            Platform::Linux,
            None,
            None,
        )
        .unwrap();
        for program in &runtimes {
            run(&caller_cache, program, &disabled);
        }
        assert_eq!(files_under(&caller_cache), [caller_sentinel]);
    }
}
