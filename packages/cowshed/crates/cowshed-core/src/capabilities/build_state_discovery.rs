//! Discover per-tree build state at mint and when the tracked Cargo input contents change.
//! A repository may contain several independent Cargo workspaces. Cargo, not a second TOML
//! workspace resolver, decides membership and the configured target directory of each one.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

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

/// How discovery asks its tools. A recorded fingerprint certifies what discovery found for those
/// inputs, so it must also name the discovery that found it: a cowshed that asks differently
/// would otherwise keep an answer an earlier one got wrong, findings included, until a manifest
/// happens to change. Bump it whenever discovery's questions or the environment they run in
/// change. 2: Cargo is asked as a job, after the workspace shell's activation. 3: installed
/// JavaScript packages contribute their `node_modules/.cache`.
const DISCOVERY_REVISION: &[u8] = b"cowshed build-state discovery 3";

/// The manifest whose directory, once installed, holds a JavaScript package's tool caches.
const PACKAGE_MANIFEST: &str = "package.json";

/// Track the files Cargo reads, including unstaged edits, but not caller or untracked inputs.
/// One index query supplies the path set; BLAKE3 hashes their working-tree bytes. A tracked
/// `package.json` contributes whether its package is installed, not its bytes: only that decides
/// its build state, and a dependency edit must not rediscover Cargo. Source-image clones inherit
/// the matching fingerprint and links; capability config/markers and a new
/// [`DISCOVERY_REVISION`] invalidate it.
pub fn tracked_manifest_fingerprint(context: &DetectionContext<'_>) -> Result<String> {
    let mut digest = blake3::Hasher::new();
    digest.update(DISCOVERY_REVISION);
    digest.update(&[0]);
    for name in tracked_build_inputs(context.workspace_root)?
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let relative = Path::new(std::ffi::OsStr::from_bytes(name));
        let path = context.workspace_root.join(relative);
        digest.update(name);
        digest.update(&[0]);
        if relative.file_name() == Some(std::ffi::OsStr::new(PACKAGE_MANIFEST)) {
            digest.update(&[u8::from(installed_package(context.workspace_root, &path)?)]);
        } else if super::convention_file(context.workspace_root, &path)? {
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
    for detector in [
        &super::nx::DETECTOR,
        &super::bun::DETECTOR,
        &super::npm::DETECTOR,
        &super::pnpm::DETECTOR,
        &super::codegraph::DETECTOR,
    ] {
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
        digest.update(&[u8::from(detector.matches(
            context.workspace_root,
            &project,
            &mut TrackedManifests::default(),
        )?)]);
    }
    Ok(digest.finalize().to_hex().to_string())
}

/// Cargo's answer to one query discovery asks of it: its stdout, or why it gave none.
pub type CargoAnswer = std::result::Result<Vec<u8>, String>;

/// One question discovery asks Cargo: `cargo <args> --manifest-path <manifest>` in `directory`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CargoQuery {
    pub directory: PathBuf,
    pub manifest: PathBuf,
}

/// Where discovery's Cargo runs. Answers every query of one batch, in order. `Err` is a failure
/// to run anything at all, which leaves the whole discovery unanswered.
pub trait CargoRunner {
    fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<Vec<CargoAnswer>>;
}

/// Discovery's Cargo, run the way the workspace's jobs run it
/// ([`crate::runtime::supervisor::run_as_job`]): inside the sandbox, after the workspace shell's
/// activation, so a checkout whose toolchain comes from its dev environment is asked with that
/// toolchain and not whatever the bootstrap PATH holds. One job per workspace shell that a
/// batch's directories select answers the whole batch, so a batch pays one activation per
/// shell rather than one per manifest.
///
/// Runs off the async runtime (the blocking pool): each job is awaited on `runtime`.
pub struct JobCargo<'a> {
    sandbox: &'a crate::sandbox::SandboxConfig,
    runtime: tokio::runtime::Handle,
}

impl<'a> JobCargo<'a> {
    pub fn new(
        sandbox: &'a crate::sandbox::SandboxConfig,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self { sandbox, runtime }
    }
}

impl CargoRunner for JobCargo<'_> {
    fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<Vec<CargoAnswer>> {
        let mut selected: BTreeMap<&Path, Option<PathBuf>> = BTreeMap::new();
        let mut shells: BTreeMap<Option<PathBuf>, Vec<usize>> = BTreeMap::new();
        for (index, query) in queries.iter().enumerate() {
            let shell = match selected.get(query.directory.as_path()) {
                Some(shell) => shell.clone(),
                None => {
                    let shell = self
                        .sandbox
                        .detect_capabilities_for(&query.directory)?
                        .contribution
                        .shell
                        .map(|shell| shell.directory);
                    selected.insert(&query.directory, shell.clone());
                    shell
                }
            };
            shells.entry(shell).or_default().push(index);
        }
        let mut answers: Vec<Option<CargoAnswer>> = vec![None; queries.len()];
        for indices in shells.into_values() {
            let batch: Vec<&CargoQuery> = indices.iter().map(|index| &queries[*index]).collect();
            let directory = &batch[0].directory;
            let job = crate::runtime::supervisor::run_as_job(
                self.sandbox,
                directory,
                driver_argv(args, &batch),
            );
            let output = self.runtime.block_on(async {
                tokio::pin!(job);
                let started = std::time::Instant::now();
                let mut report = tokio::time::interval_at(
                    tokio::time::Instant::now() + WAIT_REPORT,
                    WAIT_REPORT,
                );
                loop {
                    tokio::select! {
                        output = &mut job => break output,
                        _ = report.tick() => eprintln!(
                            "cowshed: build-state discovery has waited {}s on `cargo {}` in {} \
                             ({} manifests, run as a job after the workspace shell's \
                             activation); {}",
                            started.elapsed().as_secs(),
                            args.first().copied().unwrap_or_default(),
                            directory.display(),
                            batch.len(),
                            cargo_lock_holders(&self.sandbox.home),
                        ),
                    }
                }
            })?;
            for (index, answer) in indices
                .into_iter()
                .zip(driver_answers(&output, batch.len()))
            {
                answers[index] = Some(answer);
            }
        }
        Ok(answers
            .into_iter()
            .map(|answer| answer.expect("every query belongs to exactly one shell's batch"))
            .collect())
    }
}

/// How often a discovery job that has not answered says what it waits on.
const WAIT_REPORT: std::time::Duration = std::time::Duration::from_secs(10);

/// Who holds the host Cargo home's own locks open. Cargo blocks on its package-cache lock
/// while another cargo resolves or builds, and discovery's Cargo shares that `$CARGO_HOME`
/// ([`super::cargo::HOME`]), so a concurrent build is the usual reason a discovery job waits.
fn cargo_lock_holders(home: &Path) -> String {
    #[cfg(target_os = "macos")]
    {
        let cargo_home = home.join(".cargo");
        let mut held = Vec::new();
        for file in super::cargo::STATE_FILES {
            let lock = cargo_home.join(file);
            match crate::build_volume::nx::holders(&lock) {
                Ok(holders) => held.extend(
                    holders
                        .into_iter()
                        .map(|holder| format!("{} holds {}", holder, lock.display())),
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => held.push(format!("{} cannot be inspected: {error}", lock.display())),
            }
        }
        if held.is_empty() {
            format!(
                "no process holds Cargo's locks under {}, so the wait is the shell's activation \
                 or Cargo itself",
                cargo_home.display()
            )
        } else {
            format!(
                "Cargo waits for its package-cache lock: {}",
                held.join("; ")
            )
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        format!(
            "a concurrent cargo holding the package-cache lock under {} is the usual reason",
            home.join(".cargo").display()
        )
    }
}

/// Runs each query's Cargo in its own directory and frames each answer for
/// [`driver_answers`]: exit status, stdout, stderr, each NUL-terminated. Cargo prints no NUL.
const DRIVER: &str = r#"set -u
count=$1
shift
query=("${@:1:count}")
shift "$count"
stderr=$(/usr/bin/mktemp) || exit 70
trap '/bin/rm -f -- "$stderr"' EXIT
while (($# >= 2)); do
  directory=$1
  manifest=$2
  shift 2
  stdout=$({ cd -- "$directory" && exec cargo "${query[@]}" --manifest-path "$manifest"; } 2>"$stderr")
  status=$?
  printf '%s\0%s\0' "$status" "$stdout"
  /bin/cat -- "$stderr"
  printf '\0'
done
"#;

fn driver_argv(args: &[&str], queries: &[&CargoQuery]) -> Vec<OsString> {
    let mut argv: Vec<OsString> = vec![
        "/bin/bash".into(),
        "-c".into(),
        DRIVER.into(),
        "cowshed-cargo-discovery".into(),
        args.len().to_string().into(),
    ];
    argv.extend(args.iter().map(OsString::from));
    for query in queries {
        argv.push(query.directory.clone().into_os_string());
        argv.push(query.manifest.clone().into_os_string());
    }
    argv
}

/// The driver's `count` answers, in query order. A query the driver never answered -- its
/// shell's activation failed, or the job died -- carries the job's own exit and stderr, which
/// is where the activation said what went wrong.
fn driver_answers(output: &std::process::Output, count: usize) -> Vec<CargoAnswer> {
    let mut fields = output.stdout.split(|byte| *byte == 0);
    let mut answers: Vec<CargoAnswer> = std::iter::from_fn(|| {
        let status = fields.next()?;
        let stdout = fields.next()?;
        let stderr = fields.next()?;
        Some(
            match std::str::from_utf8(status)
                .ok()
                .and_then(|status| status.parse::<i32>().ok())
            {
                Some(0) => Ok(stdout.to_vec()),
                Some(status) => Err(format!(
                    "Cargo exit status {status}: {}",
                    String::from_utf8_lossy(stderr).trim_end()
                )),
                None => Err(format!(
                    "the job running Cargo framed an exit status as {:?}",
                    String::from_utf8_lossy(status)
                )),
            },
        )
    })
    .take(count)
    .collect();
    while answers.len() < count {
        answers.push(Err(format!(
            "the job running Cargo ended ({}) before Cargo answered: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end()
        )));
    }
    answers
}

/// Discovery's Cargo run straight on the host, in the test process's environment: the driver
/// and its framing without a sandbox, for tests about what discovery does with the answers.
#[cfg(all(test, target_os = "macos"))]
pub(crate) struct HostCargo;

#[cfg(all(test, target_os = "macos"))]
impl CargoRunner for HostCargo {
    fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<Vec<CargoAnswer>> {
        let Some(first) = queries.first() else {
            return Ok(Vec::new());
        };
        let argv = driver_argv(args, &queries.iter().collect::<Vec<_>>());
        let output = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .current_dir(&first.directory)
            // Where a job writes depends on the tracked project files, never a caller's shell.
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("CARGO_BUILD_TARGET_DIR")
            .output_locked()
            .map_err(|error| CowshedError::internal(format!("cannot run bash: {error}")))?;
        Ok(driver_answers(&output, queries.len()))
    }
}

pub fn discover_build_state(
    context: &DetectionContext<'_>,
    cargo: &mut dyn CargoRunner,
) -> Result<BuildStateDiscovery> {
    let config = super::workspace_config(context)?;
    let mut tracked = TrackedManifests::default();
    let detected = super::detect_with(context, config.capabilities(), &mut tracked)?;
    let mut result = BuildStateDiscovery {
        paths: detected.contribution.build_state,
        findings: Vec::new(),
    };
    let caches = javascript_tool_caches(
        context,
        config.capabilities(),
        &detected.active,
        &mut tracked,
    )?;
    merge_build_state(&mut result.paths, caches)?;
    result.paths.sort();
    let cargo_override = config.capabilities().get(&CapabilityId::Cargo);
    if cargo_override.is_some_and(|setting| setting.disabled) {
        return Ok(result);
    }
    let selected = cargo_override
        .and_then(|setting| setting.directory.as_ref())
        .map(|directory| context.workspace_root.join(directory))
        .unwrap_or_else(|| context.workspace_root.to_owned());
    super::validate_project_directory(context.workspace_root, &selected)?;
    let mut manifests = Vec::new();
    let mut queries = Vec::new();
    for manifest in tracked_manifests(context.workspace_root, &selected, &mut tracked)? {
        let absolute = context.workspace_root.join(&manifest);
        if let Err(error) = super::validate_project_directory(
            context.workspace_root,
            absolute.parent().unwrap_or(&selected),
        ) {
            result.findings.push(BuildStateFinding::CargoUnavailable {
                manifest,
                phase: CargoDiscoveryPhase::WorkspaceRoot,
                cause: error.to_string(),
            });
            continue;
        }
        manifests.push(manifest);
        queries.push(CargoQuery {
            directory: selected.clone(),
            manifest: absolute,
        });
    }
    let mut roots = BTreeSet::new();
    let located = cargo.run(
        &["locate-project", "--workspace", "--message-format", "plain"],
        &queries,
    )?;
    for (manifest, root) in manifests.into_iter().zip(located) {
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
    let mut queries = Vec::with_capacity(roots.len());
    for manifest in roots {
        let directory = manifest
            .parent()
            .expect("Cargo returned an absolute manifest")
            .to_owned();
        if let Err(error) = super::validate_project_directory(context.workspace_root, &directory) {
            result.findings.push(BuildStateFinding::CargoUnavailable {
                manifest: manifest
                    .strip_prefix(context.workspace_root)
                    .expect("workspace roots were checked above")
                    .to_owned(),
                phase: CargoDiscoveryPhase::Metadata,
                cause: error.to_string(),
            });
            continue;
        }
        queries.push(CargoQuery {
            directory,
            manifest,
        });
    }
    let metadata = cargo.run(
        &[
            "metadata",
            "--no-deps",
            "--offline",
            "--format-version",
            "1",
        ],
        &queries,
    )?;
    for (query, metadata) in queries.iter().zip(metadata) {
        let relative_manifest = query
            .manifest
            .strip_prefix(context.workspace_root)
            .expect("workspace roots were checked above")
            .to_owned();
        let metadata = metadata.and_then(|bytes| {
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

/// The checkout-relative directory a JavaScript package's tools keep their per-tree state in:
/// `node_modules/.cache/<tool>` of the package they run in (the find-cache-dir convention babel,
/// webpack, ava and stryker follow, and lmao's trace sink).
const TOOL_CACHE: &str = "node_modules/.cache";

/// The JavaScript tool caches of every package a detected JavaScript package manager installed:
/// a tracked `package.json` beside a real `node_modules` directory. Each contributes its own
/// `node_modules/.cache` to the build volume. That directory is rewritten on every run, rebuilt
/// when missing and reused only by the tree that wrote it, which is build state
/// (16_build_volumes.md); left on the source volume, every run's writes fragment an image that
/// clones share. A tracked `package.json` the package manager never installed, such as a
/// fixture's, contributes nothing, and none under `node_modules` is a package of the project.
fn javascript_tool_caches(
    context: &DetectionContext<'_>,
    overrides: &BTreeMap<CapabilityId, super::CapabilityOverride>,
    active: &[CapabilityId],
    tracked: &mut TrackedManifests,
) -> Result<Vec<BuildStatePath>> {
    let mut caches = Vec::new();
    for id in [CapabilityId::Bun, CapabilityId::Npm, CapabilityId::Pnpm] {
        if !active.contains(&id) {
            continue;
        }
        let selected = overrides
            .get(&id)
            .and_then(|setting| setting.directory.as_ref())
            .map(|directory| context.workspace_root.join(directory))
            .unwrap_or_else(|| context.workspace_root.to_owned());
        super::validate_project_directory(context.workspace_root, &selected)?;
        let prefix = selected
            .strip_prefix(context.workspace_root)
            .expect("directory was contained");
        for name in tracked
            .inputs(context.workspace_root)?
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
        {
            let manifest = Path::new(std::ffi::OsStr::from_bytes(name));
            if !tracked_manifest(manifest, prefix, PACKAGE_MANIFEST)
                || !installed_package(
                    context.workspace_root,
                    &context.workspace_root.join(manifest),
                )?
            {
                continue;
            }
            let cache = manifest
                .parent()
                .expect("a manifest path names a file")
                .join(TOOL_CACHE);
            caches.push(BuildStatePath::from_paths(&cache, &cache)?);
        }
    }
    caches.sort();
    caches.dedup();
    Ok(caches)
}

/// Whether the package whose tracked manifest is `manifest` is installed: the manifest is a file
/// in the checkout and a real `node_modules` directory sits beside it.
fn installed_package(workspace: &Path, manifest: &Path) -> Result<bool> {
    if !super::convention_file(workspace, manifest)? {
        return Ok(false);
    }
    let modules = manifest
        .parent()
        .expect("a manifest path names a file")
        .join("node_modules");
    match fs::symlink_metadata(&modules) {
        Ok(metadata) => Ok(metadata.is_dir()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(super::detection_error(&modules, error)),
    }
}

#[derive(Default)]
pub(super) struct TrackedManifests {
    inputs: Option<Vec<u8>>,
}

impl TrackedManifests {
    fn inputs(&mut self, workspace: &Path) -> Result<&[u8]> {
        if self.inputs.is_none() {
            self.inputs = Some(tracked_build_inputs(workspace)?);
        }
        Ok(self.inputs.as_deref().expect("snapshot was loaded"))
    }

    pub(super) fn contains(
        &mut self,
        workspace: &Path,
        directory: &Path,
        manifest: &str,
    ) -> Result<bool> {
        super::validate_project_directory(workspace, directory)?;
        let prefix = directory
            .strip_prefix(workspace)
            .expect("directory was contained");
        for name in self
            .inputs(workspace)?
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
        {
            let path = Path::new(std::ffi::OsStr::from_bytes(name));
            if tracked_manifest(path, prefix, manifest)
                && super::convention_file(workspace, &workspace.join(path))?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn tracked_manifest(path: &Path, prefix: &Path, manifest: &str) -> bool {
    let excluded = match manifest {
        "Cargo.toml" => Some("vendor"),
        PACKAGE_MANIFEST => Some("node_modules"),
        _ => None,
    };
    path.file_name() == Some(std::ffi::OsStr::new(manifest))
        && path.starts_with(prefix)
        && excluded.is_none_or(|excluded| {
            !path
                .components()
                .any(|component| component.as_os_str() == excluded)
        })
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
            ":(glob)**/.cargo/config*",
            ":(glob)**/go.mod",
            ":(glob)**/package.json",
        ])
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error))?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "cannot enumerate tracked tool manifests: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            "repair the checkout's Git index and retry",
        ));
    }
    Ok(output.stdout)
}

fn tracked_manifests(
    workspace: &Path,
    directory: &Path,
    tracked: &mut TrackedManifests,
) -> Result<Vec<PathBuf>> {
    super::validate_project_directory(workspace, directory)?;
    let inputs = tracked.inputs(workspace)?;
    let prefix = directory
        .strip_prefix(workspace)
        .expect("directory was contained");
    let mut paths = Vec::new();
    for name in inputs
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let path = Path::new(std::ffi::OsStr::from_bytes(name));
        if tracked_manifest(path, prefix, "Cargo.toml") {
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

    #[test]
    fn cargo_go_and_sccache_share_one_lazy_tracked_manifest_snapshot() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        fixture.files(&[
            "packages/rust/Cargo.toml",
            "packages/rust/.cargo/config.toml",
            "packages/go/go.mod",
        ]);
        git(&fixture, &["add", "packages"]);
        let mut tracked = TrackedManifests::default();
        assert!(tracked.inputs.is_none(), "the index query is lazy");
        assert!(
            super::super::cargo::DETECTOR
                .detect_with(&fixture.context(), &mut tracked)
                .unwrap()
                .is_some()
        );
        assert!(tracked.inputs.is_some());
        // A second query now sees an empty index. Go and sccache must instead use the
        // exact snapshot Cargo loaded, including Go's nested module.
        fs::remove_file(fixture.root.join(".git/index")).unwrap();
        let detected =
            super::super::detect_with(&fixture.context(), &BTreeMap::new(), &mut tracked).unwrap();
        assert_eq!(
            detected.active,
            [CapabilityId::Cargo, CapabilityId::Go, CapabilityId::Sccache]
        );
        for detector in [
            &super::super::go::DETECTOR,
            &super::super::sccache::DETECTOR,
        ] {
            assert!(
                detector
                    .detect_with(&fixture.context(), &mut tracked)
                    .unwrap()
                    .is_some(),
                "{} re-queried the index rather than sharing the snapshot",
                detector.id.name(),
            );
            assert!(detector.detect(&fixture.context()).unwrap().is_none());
        }
    }

    #[test]
    fn nested_go_detection_tracks_the_index_and_directory_overrides() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        fixture.files(&["packages/go/go.mod", "other/untracked/go.mod"]);
        let detected = || super::super::detect_for_workspace(&fixture.context()).unwrap();
        assert!(!detected().active.contains(&CapabilityId::Go));
        git(&fixture, &["add", "packages/go/go.mod"]);
        assert!(detected().active.contains(&CapabilityId::Go));
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.go]\ndirectory = \"other\"\n",
        );
        assert!(!detected().active.contains(&CapabilityId::Go));
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.go]\ndirectory = \"packages\"\n",
        );
        assert!(detected().active.contains(&CapabilityId::Go));
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.go]\ndisabled = true\n",
        );
        assert!(!detected().active.contains(&CapabilityId::Go));
        fs::remove_file(fixture.root.join(".cowshed.toml")).unwrap();
        fs::remove_file(fixture.root.join("packages/go/go.mod")).unwrap();
        assert!(!detected().active.contains(&CapabilityId::Go));
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

    fn cargo_paths(fixture: &Fixture) -> BuildStateDiscovery {
        discover_build_state(&fixture.context(), &mut HostCargo).unwrap()
    }

    #[test]
    fn tracked_independent_workspaces_contribute_once_and_honor_their_own_target_config() {
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
        let state = cargo_paths(&fixture);
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

    #[test]
    fn directory_and_disabled_overrides_narrow_tracked_cargo_discovery() {
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
        let state = cargo_paths(&fixture);
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
        assert_eq!(cargo_paths(&fixture), BuildStateDiscovery::default());
    }

    #[test]
    fn offline_metadata_failure_is_a_typed_finding_and_does_not_hide_other_workspaces() {
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
        let state = cargo_paths(&fixture);
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
    fn a_query_the_job_never_answered_carries_the_activation_s_own_account() {
        use std::os::unix::process::ExitStatusExt as _;
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: b"0\0/checkout/Cargo.toml\n\0\x00101\0\0error: no workspace\n\0".to_vec(),
            stderr: b"direnv: error .envrc failed\n".to_vec(),
        };
        assert_eq!(
            driver_answers(&output, 3),
            [
                Ok(b"/checkout/Cargo.toml\n".to_vec()),
                Err("Cargo exit status 101: error: no workspace".to_owned()),
                Err(
                    "the job running Cargo ended (exit status: 1) before Cargo answered: \
                     direnv: error .envrc failed"
                        .to_owned()
                ),
            ]
        );
    }

    #[test]
    fn fingerprint_tracks_worktree_bytes_of_all_tracked_tool_inputs_not_untracked_files() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        package(&fixture, "rust", "rust");
        write(&fixture, ".cargo/config", "[build]\n");
        write(&fixture, "rust/.cargo/config.toml", "[build]\n");
        write(&fixture, "packages/go/go.mod", "module example.com/probe\n");
        git(
            &fixture,
            &[
                "add",
                "rust/Cargo.toml",
                ".cargo/config",
                "rust/.cargo/config.toml",
                "packages/go/go.mod",
            ],
        );
        let mut fingerprint = tracked_manifest_fingerprint(&fixture.context()).unwrap();
        write(&fixture, "untracked/Cargo.toml", "not a manifest");
        write(&fixture, "untracked/.cargo/config.toml", "not a config");
        write(&fixture, "untracked/go.mod", "not a module");
        assert_eq!(
            fingerprint,
            tracked_manifest_fingerprint(&fixture.context()).unwrap()
        );
        for path in [
            "rust/Cargo.toml",
            ".cargo/config",
            "rust/.cargo/config.toml",
            "packages/go/go.mod",
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

    #[test]
    fn an_unstaged_tracked_target_config_edit_refreshes_where_the_next_job_writes() {
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
            cargo_paths(&fixture).paths,
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
            cargo_paths(&fixture).paths,
            [BuildStatePath::new("rust/second-target", "rust/second-target").unwrap()]
        );
    }

    /// A Bun workspace: the root and `packages/app` are installed, `packages/fixture` is a
    /// tracked manifest nothing installed, and `node_modules/vendored` is a tracked dependency.
    fn javascript_workspace(fixture: &Fixture) {
        git(fixture, &["init", "--quiet"]);
        for manifest in [
            "package.json",
            "packages/app/package.json",
            "packages/fixture/package.json",
            "node_modules/vendored/package.json",
        ] {
            write(fixture, manifest, "{}\n");
        }
        write(fixture, "bun.lock", "{}\n");
        fs::create_dir_all(fixture.root.join("packages/app/node_modules")).unwrap();
        git(fixture, &["add", "-f", "."]);
    }

    #[test]
    fn installed_javascript_packages_contribute_their_tool_caches_and_nothing_else_does() {
        let fixture = Fixture::new();
        javascript_workspace(&fixture);
        let state = cargo_paths(&fixture);
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert_eq!(
            state.paths,
            [
                BuildStatePath::new("node_modules/.cache", "node_modules/.cache").unwrap(),
                BuildStatePath::new(
                    "packages/app/node_modules/.cache",
                    "packages/app/node_modules/.cache"
                )
                .unwrap(),
            ]
        );
        assert!(
            !fixture.root.join("node_modules/.cache").exists()
                && !fixture.root.join("packages/fixture/node_modules").exists(),
            "discovery creates nothing"
        );

        // An override narrows the packages to its directory; disabling the package manager, or
        // removing its convention, contributes none.
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.bun]\ndirectory = \"packages/app\"\n",
        );
        write(&fixture, "packages/app/bun.lock", "{}\n");
        assert_eq!(
            cargo_paths(&fixture).paths,
            [BuildStatePath::new(
                "packages/app/node_modules/.cache",
                "packages/app/node_modules/.cache"
            )
            .unwrap()]
        );
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.bun]\ndisabled = true\n",
        );
        assert_eq!(cargo_paths(&fixture), BuildStateDiscovery::default());
        fs::remove_file(fixture.root.join(".cowshed.toml")).unwrap();
        fs::remove_file(fixture.root.join("bun.lock")).unwrap();
        assert_eq!(cargo_paths(&fixture), BuildStateDiscovery::default());
    }

    #[test]
    fn installing_a_package_refreshes_discovery_but_editing_its_manifest_does_not() {
        let fixture = Fixture::new();
        javascript_workspace(&fixture);
        let before = tracked_manifest_fingerprint(&fixture.context()).unwrap();
        write(
            &fixture,
            "packages/app/package.json",
            "{\"dependencies\":{}}\n",
        );
        assert_eq!(
            before,
            tracked_manifest_fingerprint(&fixture.context()).unwrap(),
            "a dependency edit leaves where tools write unchanged"
        );
        fs::create_dir(fixture.root.join("packages/fixture/node_modules")).unwrap();
        assert_ne!(
            before,
            tracked_manifest_fingerprint(&fixture.context()).unwrap(),
            "a newly installed package adds its tool cache"
        );
        fs::remove_dir(fixture.root.join("packages/fixture/node_modules")).unwrap();
        assert_eq!(
            before,
            tracked_manifest_fingerprint(&fixture.context()).unwrap()
        );
        fs::remove_file(fixture.root.join("bun.lock")).unwrap();
        assert_ne!(
            before,
            tracked_manifest_fingerprint(&fixture.context()).unwrap(),
            "the package manager's convention decides whether any cache is build state"
        );
    }
}
