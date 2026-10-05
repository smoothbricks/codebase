//! Discover per-tree build state at mint and when the tracked Cargo input contents change.
//! A repository may contain several independent Cargo workspaces. Cargo, not a second TOML
//! workspace resolver, decides membership and the configured target directory of each one.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use super::{BuildStatePath, CapabilityId, DetectionContext, merge_build_state};
use crate::fork_lock::Run;
use crate::{CowshedError, Result};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CargoDiscoveryPhase {
    WorkspaceRoot,
    Metadata,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BuildStateFinding {
    CargoUnavailable {
        manifest: PathBuf,
        phase: CargoDiscoveryPhase,
        cause: String,
    },
}

impl std::fmt::Display for BuildStateFinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CargoUnavailable {
                manifest,
                phase,
                cause,
            } => write!(
                formatter,
                "Cargo could not name the {} of {}, so its target directory is not on the build \
                 volume: {cause}",
                match phase {
                    CargoDiscoveryPhase::WorkspaceRoot => "workspace root",
                    CargoDiscoveryPhase::Metadata => "target directory",
                },
                manifest.display()
            ),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildStateDiscovery {
    pub paths: Vec<BuildStatePath>,
    pub findings: Vec<BuildStateFinding>,
}

/// Track the files Cargo reads, including unstaged edits, but not caller or untracked inputs.
/// One index query supplies the path set; BLAKE3 hashes their working-tree bytes. Source-image
/// clones inherit the matching fingerprint and links; capability config/markers invalidate it.
pub fn tracked_manifest_fingerprint(context: &DetectionContext<'_>) -> Result<String> {
    let mut digest = blake3::Hasher::new();
    for name in tracked_build_inputs(context.workspace_root)?
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let path = context
            .workspace_root
            .join(std::ffi::OsStr::from_bytes(name));
        digest.update(name);
        digest.update(&[0]);
        if super::convention_file(context.workspace_root, &path)? {
            let bytes = fs::read(&path).map_err(|error| super::detection_error(&path, error))?;
            digest.update(&[1]);
            digest.update(blake3::hash(&bytes).as_bytes());
        } else {
            digest.update(&[0]);
        }
    }
    let config = context.workspace_root.join(".cowshed.toml");
    match fs::read(&config) {
        Ok(bytes) => {
            digest.update(&bytes);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(super::detection_error(&config, error)),
    }
    let settings = super::workspace_config(context)?;
    for detector in [&super::nx::DETECTOR, &super::codegraph::DETECTOR] {
        let setting = settings.capabilities().get(&detector.id);
        if setting.is_some_and(|setting| setting.disabled) {
            digest.update(&[0]);
            continue;
        }
        let project = setting
            .and_then(|setting| setting.directory.as_ref())
            .map(|directory| context.workspace_root.join(directory))
            .unwrap_or_else(|| context.workspace_root.to_owned());
        super::validate_project_directory(context.workspace_root, &project)?;
        digest.update(&[u8::from(
            detector.matches(context.workspace_root, &project)?,
        )]);
    }
    Ok(digest.finalize().to_hex().to_string())
}

pub fn discover_build_state(
    context: &DetectionContext<'_>,
    environment: &BTreeMap<OsString, OsString>,
) -> Result<BuildStateDiscovery> {
    let detected = super::detect_for_workspace(context)?;
    let mut result = BuildStateDiscovery {
        paths: detected.contribution.build_state,
        findings: Vec::new(),
    };
    let config = super::workspace_config(context)?;
    let cargo_override = config.capabilities().get(&CapabilityId::Cargo);
    if cargo_override.is_some_and(|setting| setting.disabled) {
        return Ok(result);
    }
    let selected = cargo_override
        .and_then(|setting| setting.directory.as_ref())
        .map(|directory| context.workspace_root.join(directory))
        .unwrap_or_else(|| context.workspace_root.to_owned());
    super::validate_project_directory(context.workspace_root, &selected)?;
    let mut roots = BTreeSet::new();
    for manifest in tracked_manifests(context.workspace_root, &selected)? {
        let absolute = context.workspace_root.join(&manifest);
        let root = cargo_output(
            context.workspace_root,
            &selected,
            &absolute,
            environment,
            &["locate-project", "--workspace", "--message-format", "plain"],
        );
        match root {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(root) => {
                    let root = PathBuf::from(root.trim_end_matches(['\n', '\r']));
                    if root.is_absolute() && root.starts_with(context.workspace_root) {
                        roots.insert(root);
                    } else {
                        result.findings.push(BuildStateFinding::CargoUnavailable {
                            manifest,
                            phase: CargoDiscoveryPhase::WorkspaceRoot,
                            cause: format!(
                                "Cargo workspace root is outside the checkout: {}",
                                root.display()
                            ),
                        });
                    }
                }
                Err(error) => result.findings.push(BuildStateFinding::CargoUnavailable {
                    manifest,
                    phase: CargoDiscoveryPhase::WorkspaceRoot,
                    cause: error.to_string(),
                }),
            },
            Err(cause) => result.findings.push(BuildStateFinding::CargoUnavailable {
                manifest,
                phase: CargoDiscoveryPhase::WorkspaceRoot,
                cause,
            }),
        }
    }
    for manifest in roots {
        let relative_manifest = manifest
            .strip_prefix(context.workspace_root)
            .expect("workspace roots were checked above")
            .to_owned();
        let metadata = cargo_output(
            context.workspace_root,
            manifest
                .parent()
                .expect("Cargo returned an absolute manifest"),
            &manifest,
            environment,
            &[
                "metadata",
                "--no-deps",
                "--offline",
                "--format-version",
                "1",
            ],
        )
        .and_then(|bytes| {
            serde_json::from_slice::<CargoMetadata>(&bytes).map_err(|error| error.to_string())
        });
        let metadata = match metadata {
            Ok(metadata) => metadata,
            Err(cause) => {
                result.findings.push(BuildStateFinding::CargoUnavailable {
                    manifest: relative_manifest,
                    phase: CargoDiscoveryPhase::Metadata,
                    cause,
                });
                continue;
            }
        };
        // The tool's logical target path is inside the checkout. After provisioning its fixed
        // link deliberately resolves outside the source volume, through .cowshed/build.
        let Ok(relative) = metadata
            .target_directory
            .strip_prefix(context.workspace_root)
        else {
            continue;
        };
        match BuildStatePath::from_paths(relative, relative) {
            Ok(path) => merge_build_state(&mut result.paths, vec![path])?,
            Err(error) => result.findings.push(BuildStateFinding::CargoUnavailable {
                manifest: relative_manifest,
                phase: CargoDiscoveryPhase::Metadata,
                cause: error.to_string(),
            }),
        }
    }
    result.paths.sort();
    result.paths.dedup();
    Ok(result)
}

#[derive(Deserialize)]
struct CargoMetadata {
    target_directory: PathBuf,
}

fn cargo_output(
    workspace: &Path,
    directory: &Path,
    manifest: &Path,
    environment: &BTreeMap<OsString, OsString>,
    args: &[&str],
) -> std::result::Result<Vec<u8>, String> {
    super::validate_project_directory(workspace, manifest.parent().unwrap_or(directory))
        .map_err(|error| error.to_string())?;
    let output = Command::new("cargo")
        .env_clear()
        .envs(environment)
        .current_dir(directory)
        .args(args)
        .arg("--manifest-path")
        .arg(manifest)
        .output_locked()
        .map_err(|error| format!("cannot execute Cargo: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "Cargo {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end()
        ))
    }
}

pub(super) fn tracked_cargo_convention(workspace: &Path, directory: &Path) -> Result<bool> {
    Ok(!tracked_manifests(workspace, directory)?.is_empty())
}

fn tracked_build_inputs(workspace: &Path) -> Result<Vec<u8>> {
    match fs::symlink_metadata(workspace.join(".git")) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(super::detection_error(&workspace.join(".git"), error)),
    }
    let output = crate::git::git_command_at(workspace)
        .args([
            "ls-files",
            "-z",
            "--",
            ":(glob)**/Cargo.toml",
            ":(glob)**/.cargo/config",
            ":(glob)**/.cargo/config.toml",
        ])
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error))?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "cannot fingerprint tracked Cargo inputs: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            "repair the checkout's Git index and retry",
        ));
    }
    Ok(output.stdout)
}

fn tracked_manifests(workspace: &Path, directory: &Path) -> Result<Vec<PathBuf>> {
    super::validate_project_directory(workspace, directory)?;
    match fs::symlink_metadata(workspace.join(".git")) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(super::detection_error(&workspace.join(".git"), error)),
    }
    let output = crate::git::git_command_at(workspace)
        .args([
            "ls-files",
            "-z",
            "--",
            ":(glob)**/Cargo.toml",
            ":(exclude,glob)**/vendor/**",
        ])
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error))?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "cannot discover tracked Cargo manifests: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            "repair the checkout's Git index and retry",
        ));
    }
    let prefix = directory
        .strip_prefix(workspace)
        .expect("directory was contained");
    let mut paths = Vec::new();
    for name in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let path = Path::new(std::ffi::OsStr::from_bytes(name));
        if path.file_name() == Some(std::ffi::OsStr::new("Cargo.toml")) && path.starts_with(prefix)
        {
            paths.push(path.to_owned());
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::super::test_support::Fixture;
    use super::*;

    fn write(fixture: &Fixture, relative: &str, content: &str) {
        let path = fixture.root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn git(fixture: &Fixture, args: &[&str]) {
        let output = crate::git::git_command_at(&fixture.root)
            .args(args)
            .output_locked()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn package(fixture: &Fixture, directory: &str, name: &str) {
        write(
            fixture,
            &format!("{directory}/Cargo.toml"),
            &format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n"),
        );
        write(
            fixture,
            &format!("{directory}/src/lib.rs"),
            "pub fn value() -> u8 { 1 }\n",
        );
    }

    async fn cargo_paths(fixture: &Fixture) -> BuildStateDiscovery {
        use crate::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants};
        write(fixture, ".cowshed/token", &"A".repeat(43));
        let mut sandbox = SandboxConfig {
            home: PathBuf::from(std::env::var_os("HOME").expect("host home")),
            mount_root: fixture.root.join("other-mounts"),
            workspace_mount: fixture.root.clone(),
            exec_temp_dir: fixture.root.join("exec-temp"),
            port_block: crate::metadata::PortBlock::new(49_184, 16).unwrap(),
            retained_port_blocks: Vec::new(),
            mode: RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            build_volume_mount: None,
            capabilities: Default::default(),
        };
        sandbox.configure_capabilities().unwrap();
        let caller = BTreeMap::from([("CARGO_TARGET_DIR".to_owned(), "/tmp/x".to_owned())]);
        let environment = crate::runtime::supervisor::job_environment(&sandbox, &caller)
            .await
            .unwrap();
        if sandbox.capabilities.active.contains(&CapabilityId::Cargo) {
            assert!(
                !environment.contains_key(std::ffi::OsStr::new("CARGO_TARGET_DIR")),
                "a caller target override never reaches discovery or a Cargo job"
            );
        }
        discover_build_state(&fixture.context(), &environment).unwrap()
    }

    #[tokio::test]
    async fn tracked_independent_workspaces_contribute_once_and_honor_their_own_target_config() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        write(
            &fixture,
            "rust/Cargo.toml",
            "[workspace]\nmembers = [\"member\"]\nresolver = \"2\"\n",
        );
        package(&fixture, "rust/member", "member");
        package(&fixture, "tooling/helper", "helper");
        package(&fixture, "vendor/ignored", "ignored_vendor");
        package(&fixture, "untracked", "ignored_untracked");
        write(
            &fixture,
            "tooling/helper/.cargo/config.toml",
            "[build]\ntarget-dir = \"build-target\"\n",
        );
        git(
            &fixture,
            &[
                "add",
                "rust/Cargo.toml",
                "rust/member/Cargo.toml",
                "tooling/helper/Cargo.toml",
                "tooling/helper/.cargo/config.toml",
                "vendor/ignored/Cargo.toml",
            ],
        );
        let state = cargo_paths(&fixture).await;
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert_eq!(
            state.paths,
            [
                BuildStatePath::new("rust/target", "rust/target").unwrap(),
                BuildStatePath::new("tooling/helper/build-target", "tooling/helper/build-target")
                    .unwrap(),
            ],
        );
        let capabilities = super::super::detect_for_workspace(&fixture.context()).unwrap();
        assert!(capabilities.active.contains(&CapabilityId::Cargo));
        assert!(
            !fixture.root.join("target").exists(),
            "discovery never builds"
        );
    }

    #[tokio::test]
    async fn directory_and_disabled_overrides_narrow_tracked_cargo_discovery() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        package(&fixture, "first", "first");
        package(&fixture, "second", "second");
        git(&fixture, &["add", "first/Cargo.toml", "second/Cargo.toml"]);
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.cargo]\ndirectory = \"second\"\n",
        );
        let state = cargo_paths(&fixture).await;
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert_eq!(
            state.paths,
            [BuildStatePath::new("second/target", "second/target").unwrap()]
        );
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.cargo]\ndisabled = true\n",
        );
        assert_eq!(cargo_paths(&fixture).await, BuildStateDiscovery::default());
    }

    #[tokio::test]
    async fn offline_metadata_failure_is_a_typed_finding_and_does_not_hide_other_workspaces() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        package(&fixture, "healthy", "healthy");
        package(&fixture, "offline", "offline");
        write(
            &fixture,
            "offline/Cargo.toml",
            "[package]\nname = \"offline\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
             [workspace]\nmembers = [\"missing-member\"]\n",
        );
        git(
            &fixture,
            &["add", "healthy/Cargo.toml", "offline/Cargo.toml"],
        );
        let state = cargo_paths(&fixture).await;
        assert_eq!(
            state.paths,
            [BuildStatePath::new("healthy/target", "healthy/target").unwrap()]
        );
        assert!(
            matches!(
                state.findings.as_slice(),
                [BuildStateFinding::CargoUnavailable { manifest, phase: CargoDiscoveryPhase::Metadata, cause }]
                    if manifest == Path::new("offline/Cargo.toml") && cause.contains("missing-member")
            ),
            "{:?}",
            state.findings
        );
    }

    #[test]
    fn fingerprint_tracks_worktree_bytes_of_all_tracked_cargo_inputs_not_untracked_files() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        package(&fixture, "rust", "rust");
        write(&fixture, ".cargo/config", "[build]\n");
        write(&fixture, "rust/.cargo/config.toml", "[build]\n");
        git(
            &fixture,
            &[
                "add",
                "rust/Cargo.toml",
                ".cargo/config",
                "rust/.cargo/config.toml",
            ],
        );
        let mut fingerprint = tracked_manifest_fingerprint(&fixture.context()).unwrap();
        write(&fixture, "untracked/Cargo.toml", "not a manifest");
        write(&fixture, "untracked/.cargo/config.toml", "not a config");
        assert_eq!(
            fingerprint,
            tracked_manifest_fingerprint(&fixture.context()).unwrap()
        );
        for path in [
            "rust/Cargo.toml",
            ".cargo/config",
            "rust/.cargo/config.toml",
        ] {
            let at = fixture.root.join(path);
            let previous = fs::read_to_string(&at).unwrap();
            fs::write(&at, format!("{previous}\n# new tracked content\n")).unwrap();
            let next = tracked_manifest_fingerprint(&fixture.context()).unwrap();
            assert_ne!(
                fingerprint, next,
                "{path} worktree bytes must invalidate discovery"
            );
            git(&fixture, &["add", path]);
            assert_eq!(
                next,
                tracked_manifest_fingerprint(&fixture.context()).unwrap(),
                "staging unchanged worktree content does not change the fingerprint"
            );
            fingerprint = next;
        }
        fs::create_dir(fixture.root.join(".codegraph")).unwrap();
        assert_ne!(
            fingerprint,
            tracked_manifest_fingerprint(&fixture.context()).unwrap(),
            "an indexer marker adds its build state"
        );
    }

    #[tokio::test]
    async fn an_unstaged_tracked_target_config_edit_refreshes_where_the_next_job_writes() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        package(&fixture, "rust", "rust");
        write(
            &fixture,
            "rust/.cargo/config.toml",
            "[build]\ntarget-dir = \"first-target\"\n",
        );
        git(
            &fixture,
            &["add", "rust/Cargo.toml", "rust/.cargo/config.toml"],
        );
        let before = tracked_manifest_fingerprint(&fixture.context()).unwrap();
        assert_eq!(
            cargo_paths(&fixture).await.paths,
            [BuildStatePath::new("rust/first-target", "rust/first-target").unwrap()]
        );
        write(
            &fixture,
            "rust/.cargo/config.toml",
            "[build]\ntarget-dir = \"second-target\"\n",
        );
        assert_ne!(
            before,
            tracked_manifest_fingerprint(&fixture.context()).unwrap()
        );
        assert_eq!(
            cargo_paths(&fixture).await.paths,
            [BuildStatePath::new("rust/second-target", "rust/second-target").unwrap()]
        );
    }
}
