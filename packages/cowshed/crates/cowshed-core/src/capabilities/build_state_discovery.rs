//! Discover per-tree build state at mint and when the tracked Cargo input contents change.
//! A repository may contain several independent Cargo workspaces. Cargo, not a second TOML
//! workspace resolver, decides membership and the configured target directory of each one.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use serde::{Deserialize, Serialize};

use super::{BuildStatePath, CapabilityId, DeclaredState, DetectionContext, merge_build_state};
use crate::input_memo::InputMemo;
use crate::runtime::supervisor::ObservedRun;
use crate::workspace_environment::{NODE_COMPILE_CACHE_CHECKOUT, NODE_COMPILE_CACHE_DIRECTORY};
use crate::{CowshedError, Result};
use cowshed_os::fork_lock::Run;

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
    /// The job that asks Cargo held no `cargo` on its PATH, so Cargo was asked about none of the
    /// manifests its batch carried. `shell` is the checkout-relative directory of the workspace
    /// shell whose activation put none there, or `None` when no shell selects the directory the
    /// job ran in: cowshed links no toolchain of its own (15_capabilities.md, "Bootstrap
    /// executables").
    NoCargo {
        shell: Option<PathBuf>,
        manifests: Vec<PathBuf>,
    },
    /// The job's `cargo` is mbx's Cargo shim, which runs the Cargo mbx resolves (14_nix.md,
    /// "mbx"), and `cargo -V` through it failed before the batch's first query, so Cargo was asked
    /// about none of the manifests its batch carried. `cause` is the probe's exit status and mbx's
    /// own account; `shell` is as for [`Self::NoCargo`].
    CargoRouteFailed {
        shell: Option<PathBuf>,
        manifests: Vec<PathBuf>,
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
            Self::NoCargo { shell, manifests } => {
                let manifests = manifest_list(manifests);
                match shell {
                    Some(shell) => write!(
                        formatter,
                        "the workspace shell activated from {} put no `cargo` on its jobs' PATH, \
                         and cowshed links none of its own, so Cargo was not asked about \
                         {manifests} and their target directories are not on the build volume: \
                         declare the toolchain in that shell",
                        shell.join(".envrc").display()
                    ),
                    None => write!(
                        formatter,
                        "no workspace shell (`.envrc`) declares a toolchain for {manifests}, and \
                         cowshed links no `cargo` of its own, so Cargo was not asked about them and \
                         their target directories are not on the build volume: declare the \
                         toolchain in the repository's shell"
                    ),
                }
            }
            Self::CargoRouteFailed {
                shell,
                manifests,
                cause,
            } => {
                let manifests = manifest_list(manifests);
                match shell {
                    Some(shell) => write!(
                        formatter,
                        "the jobs of the workspace shell activated from {} reach Cargo through \
                         mbx's Cargo shim, and `cargo -V` through it ended with {cause}, so Cargo \
                         was not asked about {manifests} and their target directories are not on \
                         the build volume",
                        shell.join(".envrc").display()
                    ),
                    None => write!(
                        formatter,
                        "no workspace shell (`.envrc`) selects the directory of {manifests}; its \
                         jobs reach Cargo through mbx's Cargo shim, and `cargo -V` through it \
                         ended with {cause}, so Cargo was not asked about them and their target \
                         directories are not on the build volume"
                    ),
                }
            }
        }
    }
}

/// The manifests a batch-wide finding names, as one comma-separated list.
fn manifest_list(manifests: &[PathBuf]) -> String {
    manifests
        .iter()
        .map(|manifest| manifest.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildStateDiscovery {
    pub paths: Vec<BuildStatePath>,
    pub findings: Vec<BuildStateFinding>,
    /// The realpath of the mbx reached through the canonical Cargo route, recorded with target ownership.
    pub mbx: Option<PathBuf>,
    /// The Cargo target directories mbx owns now, which the build volume holds no longer.
    pub released: Vec<BuildStatePath>,
}

impl BuildStateDiscovery {
    /// Record why Cargo gave no answer about `manifest`. A batch whose job held no `cargo`, or
    /// whose `cargo` is mbx's Cargo shim and ran no Cargo, is one finding naming its shell, which
    /// every manifest of that batch joins.
    fn unanswered(&mut self, manifest: PathBuf, phase: CargoDiscoveryPhase, why: CargoUnanswered) {
        match why {
            CargoUnanswered::Failed(cause) => {
                self.findings.push(BuildStateFinding::CargoUnavailable {
                    manifest,
                    phase,
                    cause,
                });
            }
            CargoUnanswered::NoCargo { shell } => {
                let batch = self.findings.iter_mut().find_map(|finding| match finding {
                    BuildStateFinding::NoCargo {
                        shell: held,
                        manifests,
                    } if *held == shell => Some(manifests),
                    _ => None,
                });
                match batch {
                    Some(manifests) => manifests.push(manifest),
                    None => self.findings.push(BuildStateFinding::NoCargo {
                        shell,
                        manifests: vec![manifest],
                    }),
                }
            }
            CargoUnanswered::CargoRouteFailed { shell, cause } => {
                let batch = self.findings.iter_mut().find_map(|finding| match finding {
                    BuildStateFinding::CargoRouteFailed {
                        shell: held,
                        manifests,
                        cause: said,
                    } if *held == shell && *said == cause => Some(manifests),
                    _ => None,
                });
                match batch {
                    Some(manifests) => manifests.push(manifest),
                    None => self.findings.push(BuildStateFinding::CargoRouteFailed {
                        shell,
                        manifests: vec![manifest],
                        cause,
                    }),
                }
            }
        }
    }
}

/// How discovery asks its tools. A recorded fingerprint certifies what discovery found for those
/// inputs, so it must also name the discovery that found it: a cowshed that asks differently
/// would otherwise keep an answer an earlier one got wrong, findings included, until a manifest
/// happens to change. Bump it whenever discovery's questions or the environment they run in
/// change. 2: Cargo is asked as a job, after the workspace shell's activation. 3: installed
/// JavaScript packages contribute their `node_modules/.cache`. 5: Codegraph indexes are build-volume state.
/// 6: a job that holds no `cargo` is one finding per shell, not one Cargo failure per manifest.
/// 7: the job resolves `mbx`, and the canonical activation's input identity keys the answer.
/// 8: installed host mbx supplies the Cargo route without a project opt-in.
/// 9: capabilities mark seed-retained subtrees and clone-invalid process files.
/// 10: a detected JavaScript package manager links the module compile cache onto the build volume.
/// 11: under mbx's Cargo shim the job asks `cargo -V` once; a failure is one finding per shell.
/// 12: the driver reads a cargo's header with bash 3.2's `read`, so the system bash names mbx.
const DISCOVERY_REVISION: &[u8] = b"cowshed build-state discovery 12";

/// The manifest whose directory, once installed, holds a JavaScript package's tool caches.
const PACKAGE_MANIFEST: &str = "package.json";

/// Track the files Cargo reads, including unstaged edits, but not caller or untracked inputs.
/// One index query supplies the path set, shared with the detectors' tracked-manifest
/// conventions; BLAKE3 hashes their working-tree bytes. A tracked `package.json` contributes
/// whether its package is installed, not its bytes: only that decides its build state, and a
/// dependency edit must not rediscover Cargo. Source-image clones inherit the matching
/// fingerprint and links; capability config/markers, the expansion of the declared build state
/// (a newly tracked package joins a pattern) and a new [`DISCOVERY_REVISION`] invalidate it.
///
/// A function of the checkout at `workspace` alone: the controller's refresh and a resident
/// `exec` (06_cli.md, "Resident workspaces") compute the same value for the same checkout.
pub fn tracked_manifest_fingerprint(workspace: &Path) -> Result<String> {
    let mut digest = blake3::Hasher::new();
    digest.update(DISCOVERY_REVISION);
    digest.update(&[0]);
    // The canonical activation's input identity: a changed devenv pin or resolved mbx build
    // cannot keep an earlier capability answer (15_capabilities.md, "mbx detection").
    digest.update(
        crate::runtime::shell_inherit::recorded_key(workspace)
            .unwrap_or_default()
            .as_bytes(),
    );
    digest.update(&[0]);
    let mut tracked = TrackedManifests::default();
    for name in tracked
        .inputs(workspace)?
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let relative = Path::new(std::ffi::OsStr::from_bytes(name));
        let path = workspace.join(relative);
        digest.update(name);
        digest.update(&[0]);
        if relative.file_name() == Some(std::ffi::OsStr::new(PACKAGE_MANIFEST)) {
            digest.update(&[u8::from(installed_package(workspace, &path)?)]);
        } else if super::convention_file(workspace, &path)? {
            let bytes = fs::read(&path).map_err(|error| super::detection_error(&path, error))?;
            digest.update(&[1]);
            digest.update(blake3::hash(&bytes).as_bytes());
        } else {
            digest.update(&[0]);
        }
    }
    let config = workspace.join(".cowshed.toml");
    match fs::read(&config) {
        Ok(bytes) => {
            digest.update(&bytes);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(super::detection_error(&config, error)),
    }
    let settings = super::workspace_config(workspace)?;
    for state in expand_declared(workspace, settings.build_state())? {
        digest.update(state.checkout.as_path().as_os_str().as_bytes());
        digest.update(&[0]);
    }
    for detector in [
        &super::nx::DETECTOR,
        &super::codegraph::DETECTOR,
        &super::bun::DETECTOR,
        &super::npm::DETECTOR,
        &super::pnpm::DETECTOR,
    ] {
        let setting = settings.capabilities().get(&detector.id);
        if setting.is_some_and(|setting| setting.disabled) {
            digest.update(&[0]);
            continue;
        }
        let project = setting
            .and_then(|setting| setting.directory.as_ref())
            .map(|directory| workspace.join(directory))
            .unwrap_or_else(|| workspace.to_owned());
        super::validate_project_directory(workspace, &project)?;
        digest.update(&[u8::from(detector.matches(
            workspace,
            &project,
            &mut tracked,
        )?)]);
    }
    Ok(digest.finalize().to_hex().to_string())
}

/// Whether discovery at `workspace` can name any build state: it names only a tracked Cargo
/// manifest's targets, a tracked package manifest's installed tool caches, a declared `[build]
/// state`, or what an enabled Nx or Codegraph detector contributes. The other tracked build
/// inputs (`.cargo/config*`, `go.mod`, whose caches are the host's) name none. A checkout with
/// none of them has its first touch make no volume, so `adopt` mints none beside main's image
/// for it.
pub fn may_name_build_state(workspace: &Path) -> Result<bool> {
    let mut tracked = TrackedManifests::default();
    if tracked
        .inputs(workspace)?
        .split(|byte| *byte == 0)
        .map(|name| Path::new(std::ffi::OsStr::from_bytes(name)))
        .any(|path| {
            tracked_manifest(path, Path::new(""), "Cargo.toml")
                || tracked_manifest(path, Path::new(""), PACKAGE_MANIFEST)
        })
    {
        return Ok(true);
    }
    let settings = super::workspace_config(workspace)?;
    if !settings.build_state().is_empty() {
        return Ok(true);
    }
    for detector in [&super::nx::DETECTOR, &super::codegraph::DETECTOR] {
        let setting = settings.capabilities().get(&detector.id);
        if setting.is_some_and(|setting| setting.disabled) {
            continue;
        }
        let project = setting
            .and_then(|setting| setting.directory.as_ref())
            .map(|directory| workspace.join(directory))
            .unwrap_or_else(|| workspace.to_owned());
        super::validate_project_directory(workspace, &project)?;
        if detector.matches(workspace, &project, &mut tracked)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Cargo's answer to one query discovery asks of it: its stdout, or why it gave none.
pub type CargoAnswer = std::result::Result<Vec<u8>, CargoUnanswered>;

/// One batch's answers, in query order, and the `mbx` its job resolved on its PATH.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CargoAnswers {
    pub answers: Vec<CargoAnswer>,
    pub mbx: Option<PathBuf>,
}

/// Why discovery has no answer from Cargo to one query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CargoUnanswered {
    /// Cargo, or the job running it, ended without one; the cause is their own account.
    Failed(String),
    /// The job of the query's batch held no `cargo` on its PATH, so Cargo never ran. `shell` is
    /// the checkout-relative directory of the workspace shell activated before the driver, or
    /// `None` when no shell selects the batch's directory: cowshed links no `cargo` of its own
    /// (15_capabilities.md, "Bootstrap executables").
    NoCargo { shell: Option<PathBuf> },
    /// The job's `cargo` is mbx's Cargo shim, and `cargo -V` through it failed, so Cargo never
    /// ran: `cause` is the probe's exit status and mbx's own account (14_nix.md, "mbx"). `shell`
    /// is as for [`Self::NoCargo`].
    CargoRouteFailed {
        shell: Option<PathBuf>,
        cause: String,
    },
}

/// One question discovery asks Cargo: `cargo <args> --manifest-path <manifest>` in `directory`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CargoQuery {
    pub directory: PathBuf,
    pub manifest: PathBuf,
}

/// Where discovery's Cargo runs. Answers every query of one batch, in order, with the `mbx` the
/// job resolved. `Err` is a failure to run anything at all, which leaves the whole discovery
/// unanswered.
pub trait CargoRunner {
    fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<CargoAnswers>;
}

/// Discovery's Cargo runs in the workspace's canonical supervisor and activated shell.
/// It shares cold activation and the published export with ordinary jobs and the spare,
/// using the workspace's toolchain rather than the bootstrap PATH. One job per selected
/// workspace shell answers the whole batch.
///
/// Runs off the async runtime (the blocking pool): each job is awaited on runtime.
/// Cancellation ends its held canonical job; the tool's silence bound starts only once
/// the command starts, never while it waits for shared shell activation.
pub struct JobCargo<'a> {
    sandbox: &'a crate::sandbox::SandboxConfig,
    supervisor: &'a crate::runtime::supervisor::WorkspaceSupervisorHandle,
    socket: &'a Path,
    build_volume: Option<PathBuf>,
    runtime: tokio::runtime::Handle,
    cancelled: tokio::sync::oneshot::Receiver<()>,
}

impl<'a> JobCargo<'a> {
    pub fn new(
        sandbox: &'a crate::sandbox::SandboxConfig,
        supervisor: &'a crate::runtime::supervisor::WorkspaceSupervisorHandle,
        socket: &'a Path,
        build_volume: Option<PathBuf>,
        runtime: tokio::runtime::Handle,
        cancelled: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        Self {
            sandbox,
            supervisor,
            socket,
            build_volume,
            runtime,
            cancelled,
        }
    }
}

impl CargoRunner for JobCargo<'_> {
    fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<CargoAnswers> {
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
        let mut mbx = None;
        for (shell, indices) in shells {
            // Detection selects a shell only among the directories of the workspace it inspects.
            let shell = shell.map(|shell| {
                shell
                    .strip_prefix(&self.sandbox.workspace_mount)
                    .expect("a workspace shell lies inside its workspace")
                    .to_owned()
            });
            let batch: Vec<&CargoQuery> = indices.iter().map(|index| &queries[*index]).collect();
            let directory = &batch[0].directory;
            let (job_cancel, job_cancelled) = tokio::sync::oneshot::channel::<()>();
            let mut job_cancel = Some(job_cancel);
            let observed = crate::runtime::supervisor::JobRunState::default();
            let request = crate::runtime::supervisor::project_tool_request(
                &self.sandbox.workspace_mount,
                directory,
                driver_argv(args, &batch),
            )?;
            let job = crate::runtime::supervisor::run_in_supervisor(
                self.supervisor,
                self.socket,
                self.build_volume.clone(),
                request,
                crate::runtime::supervisor::RunControl {
                    silence: cowshed_os::deadline::HEARTBEAT_SILENCE,
                    // Discovery's Cargo waits behind no holder: measured, neither
                    // `locate-project` nor `metadata --no-deps --offline` waits on Cargo's
                    // package-cache lock, held or churned, so no holder extends its silence.
                    queued: Box::new(|| None),
                    cancelled: job_cancelled,
                    observed: &observed,
                },
            );
            let runtime = self.runtime.clone();
            let output = runtime.block_on(async {
                tokio::pin!(job);
                let started = std::time::Instant::now();
                let mut report = tokio::time::interval_at(
                    tokio::time::Instant::now() + WAIT_REPORT,
                    WAIT_REPORT,
                );
                let mut caller_gone = false;
                let output = loop {
                    tokio::select! {
                        output = &mut job => break output,
                        _ = report.tick() => cowshed_os::eprintln!(
                            "{}",
                            wait_report(
                                started.elapsed().as_secs(),
                                args.first().copied().unwrap_or_default(),
                                directory,
                                batch.len(),
                                observed.snapshot(),
                            )
                        ),
                        _ = &mut self.cancelled, if !caller_gone => {
                            caller_gone = true;
                            drop(job_cancel.take());
                        }
                    }
                };
                if caller_gone {
                    // The job ended with its caller or raced it to an answer: the
                    // batch's answers belong to nobody now, and the spent receiver
                    // must never be polled by a later batch.
                    return Err(CowshedError::conflict(
                        "build state discovery was cancelled: its caller is gone",
                        "retry",
                    ));
                }
                output
            })?;
            let answered = driver_answers(&output, batch.len(), shell.as_deref());
            mbx = mbx.or(answered.mbx);
            for (index, answer) in indices.into_iter().zip(answered.answers) {
                answers[index] = Some(answer);
            }
        }
        Ok(CargoAnswers {
            answers: answers
                .into_iter()
                .map(|answer| answer.expect("every query belongs to exactly one shell's batch"))
                .collect(),
            mbx,
        })
    }
}

/// How often a discovery job that has not answered says what phase it is in.
const WAIT_REPORT: std::time::Duration = std::time::Duration::from_secs(10);

/// What a discovery job that has not answered says about its wait: how long, which Cargo
/// question, and the phase the command itself was observed in -- not started yet,
/// or running as a named process in a named kernel state. Nothing else: a
/// cause inferred from other processes (who happened to hold some file at the first
/// report) is a guess, and a printed guess is read as fact.
fn wait_report(
    waited_secs: u64,
    arg: &str,
    directory: &Path,
    manifests: usize,
    observed: ObservedRun,
) -> String {
    let phase = match observed {
        ObservedRun::Preparing => {
            "the command has not started; preparing its sandbox environment or waiting for its workspace shell".to_owned()
        }
        ObservedRun::Running {
            pid,
            state,
            command,
        } => match command {
            Some(command) => format!("its process {pid} is {state}: {command}"),
            None => format!("its process {pid} is {state}"),
        },
    };
    format!(
        "cowshed: build-state discovery has waited {waited_secs}s on `cargo {arg}` in {} \
         ({manifests} manifests, run as a job after the workspace shell's activation); {phase}",
        directory.display()
    )
}

/// Bound child processes independently of the number of tracked manifests. The existing
/// manifest fingerprint caches discovery across refreshes; a cold batch pays one shell
/// activation and at most this many Cargo queries at once.
const CARGO_QUERY_PARALLELISM: usize = 4;

/// What the driver runs besides `bash` and the repository's own `cargo`. Core links each onto the
/// bootstrap PATH the way it links `bash`, so a job whose shell supplies none of them, on a host
/// whose fixed system directories hold none (NixOS), still runs the driver.
pub(super) const DRIVER_PROGRAMS: [&str; 3] = ["cat", "mktemp", "rm"];

/// Runs each query's Cargo in its own directory and frames each answer for
/// [`driver_answers`]: exit status, stdout, stderr, each NUL-terminated. Cargo prints no NUL.
/// Each worker owns its files; the parent drains them in query order after waiting, so
/// interleaved completions cannot associate an answer or failure with another manifest.
///
/// `bash`, which runs it, and the `mktemp`, `rm` and `cat` it runs besides `cargo` are each the
/// first executable of their name on the PATH the job holds, never a fixed `/bin` or `/usr/bin`
/// path: NixOS keeps only `/bin/sh` and `/usr/bin/env`, so a job whose shell supplies none of
/// them reaches `bash` and [`DRIVER_PROGRAMS`] through core's bootstrap links
/// (15_capabilities.md, "Detection inputs"). A job whose PATH holds no `cargo` prints
/// [`NO_CARGO`] alone, once for its whole batch, and asks nothing. A job whose `cargo` is mbx's
/// Cargo shim (the canonical route, 11_shell.md, "Canonical mbx environment") asks it
/// `cargo -V` once before its first query, since the shim runs the Cargo mbx resolves
/// (14_nix.md, "mbx"); when that fails it prints [`ROUTE_FAILED`], the probe's exit status and
/// its stderr, each NUL-terminated, and asks nothing. Otherwise the job prints first the `mbx`
/// its PATH resolves when its `cargo` is the shim, or nothing, NUL-terminated: the shim is a
/// short script that sets `MBX_CARGO_SHIM_MODE`, so its first bytes say so, and a plain Cargo's
/// never do. Its `bash` may be macOS's `/bin/bash`, bash 3.2, which core links where no profile
/// directory holds a newer one (15_capabilities.md, "Bootstrap executables"), so it uses nothing
/// newer: its bounded header read stops at the first NUL or 4096 characters (`read -d '' -n`),
/// never bash 4's `read -N`, which bash 3.2 refuses.
const DRIVER: &str = r#"set -u
parallel=$1
count=$2
shift 2
query=("${@:1:count}")
shift "$count"
command -v cargo >/dev/null || { printf 'no-cargo\0'; exit 127; }
cargo=$(command -v cargo)
head=
IFS= read -r -d '' -n 4096 head <"$cargo" 2>/dev/null || :
case $head in
*MBX_CARGO_SHIM_MODE*)
  probe=$(cargo -V 2>&1 >/dev/null)
  status=$?
  if ((status != 0)); then
    printf 'route-failed\0%s\0%s\0' "$status" "$probe"
    exit "$status"
  fi
  printf '%s\0' "$(command -v mbx)"
  ;;
*) printf '\0' ;;
esac
work=$(mktemp -d) || exit 70
trap 'rm -rf -- "$work"' EXIT
while (($# >= 2)); do
  pids=()
  for ((slot=0; slot<parallel && $#>=2; slot++)); do
    directory=$1
    manifest=$2
    shift 2
    (
      stdout=$({ cd -- "$directory" && exec cargo "${query[@]}" --manifest-path "$manifest"; } 2>"$work/$slot.stderr")
      status=$?
      printf '%s\0%s\0' "$status" "$stdout"
      cat -- "$work/$slot.stderr"
      printf '\0'
    ) >"$work/$slot.answer" &
    pids+=("$!")
  done
  for ((slot=0; slot<${#pids[@]}; slot++)); do
    wait "${pids[$slot]}" || exit $?
    cat -- "$work/$slot.answer" || exit 70
  done
done
"#;

/// All the driver prints when its job's PATH holds no `cargo`: no shell selects the batch's
/// directory, or the one that does activated none, and core links none (15_capabilities.md,
/// "Bootstrap executables"). Never the start of an answer, whose first field is a decimal status.
const NO_CARGO: &[u8] = b"no-cargo\0";

/// What the driver prints first when its `cargo` is mbx's Cargo shim and `cargo -V` through it
/// failed, followed by that probe's exit status and stderr, each NUL-terminated, and nothing
/// else. Never the start of an answer, whose first field is an absolute `mbx` path or empty.
const ROUTE_FAILED: &[u8] = b"route-failed\0";

fn driver_argv(args: &[&str], queries: &[&CargoQuery]) -> Vec<OsString> {
    let mut argv: Vec<OsString> = vec![
        // Found on the job's PATH, as cargo is: NixOS has no bash in its fixed system directories.
        "bash".into(),
        "-c".into(),
        DRIVER.into(),
        "cowshed-cargo-discovery".into(),
        CARGO_QUERY_PARALLELISM.to_string().into(),
        args.len().to_string().into(),
    ];
    argv.extend(args.iter().map(OsString::from));
    for query in queries {
        argv.push(query.directory.clone().into_os_string());
        argv.push(query.manifest.clone().into_os_string());
    }
    argv
}

/// The driver's `count` answers, in query order, and the `mbx` its job resolved. A query the
/// driver never answered -- its shell's activation failed, or the job died -- carries the job's
/// own exit and stderr, which is where the activation said what went wrong. A driver that found no
/// `cargo` on its job's PATH, or whose mbx Cargo shim failed `cargo -V`, answers every query with
/// that, naming `shell`, the batch's checkout-relative workspace shell directory, if any.
fn driver_answers(
    output: &std::process::Output,
    count: usize,
    shell: Option<&Path>,
) -> CargoAnswers {
    if output.stdout == NO_CARGO {
        return CargoAnswers {
            answers: vec![
                Err(CargoUnanswered::NoCargo {
                    shell: shell.map(Path::to_owned),
                });
                count
            ],
            mbx: None,
        };
    }
    if let Some(frame) = output.stdout.strip_prefix(ROUTE_FAILED) {
        let mut fields = frame.split(|byte| *byte == 0);
        let status = fields
            .next()
            .and_then(|status| std::str::from_utf8(status).ok())
            .and_then(|status| status.parse::<i32>().ok());
        let cause = match (status, fields.next()) {
            (Some(status), Some(stderr)) => format!(
                "exit status {status}: {}",
                String::from_utf8_lossy(stderr).trim_end()
            ),
            _ => format!(
                "the job running Cargo framed a failed route as {:?}",
                String::from_utf8_lossy(frame)
            ),
        };
        return CargoAnswers {
            answers: vec![
                Err(CargoUnanswered::CargoRouteFailed {
                    shell: shell.map(Path::to_owned),
                    cause,
                });
                count
            ],
            mbx: None,
        };
    }
    let mut fields = output.stdout.split(|byte| *byte == 0);
    let mbx = fields
        .next()
        .filter(|path| !path.is_empty())
        .map(|path| PathBuf::from(std::ffi::OsStr::from_bytes(path)));
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
                Some(status) => Err(CargoUnanswered::Failed(format!(
                    "Cargo exit status {status}: {}",
                    String::from_utf8_lossy(stderr).trim_end()
                ))),
                None => Err(CargoUnanswered::Failed(format!(
                    "the job running Cargo framed an exit status as {:?}",
                    String::from_utf8_lossy(status)
                ))),
            },
        )
    })
    .take(count)
    .collect();
    while answers.len() < count {
        answers.push(Err(CargoUnanswered::Failed(format!(
            "the job running Cargo ended ({}) before Cargo answered: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end()
        ))));
    }
    CargoAnswers { answers, mbx }
}

/// Discovery's Cargo run straight on the host, in the test process's environment: the driver
/// and its framing without a sandbox, for tests about what discovery does with the answers. The
/// test process's own PATH is no project's canonical environment, so the job reports the `mbx`
/// a test names, none by default, never whatever the host shell happens to hold.
#[cfg(all(test, target_os = "macos"))]
#[derive(Default)]
pub(crate) struct HostCargo {
    pub(crate) mbx: Option<PathBuf>,
}

#[cfg(all(test, target_os = "macos"))]
impl CargoRunner for HostCargo {
    fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<CargoAnswers> {
        use cowshed_os::test_backstop::CommandBackstop as _;
        let Some(first) = queries.first() else {
            return Ok(CargoAnswers::default());
        };
        let argv = driver_argv(args, &queries.iter().collect::<Vec<_>>());
        let output = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .current_dir(&first.directory)
            // Where a job writes depends on the tracked project files, never a caller's shell.
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("CARGO_BUILD_TARGET_DIR")
            .output_bounded()
            .map_err(|error| CowshedError::internal(format!("cannot run bash: {error}")))?;
        let mut answered = driver_answers(&output, queries.len(), None);
        answered.mbx = self.mbx.clone();
        Ok(answered)
    }
}

/// The build state of the checkout: what capability detection contributes, each Cargo target
/// directory inside it, and what `.cowshed.toml` `[build] state` declares.
pub fn discover_build_state(
    context: &DetectionContext<'_>,
    cargo: &mut dyn CargoRunner,
) -> Result<BuildStateDiscovery> {
    let config = super::workspace_config(context.workspace_root)?;
    let mut tracked = TrackedManifests::default();
    let detected = super::detect_with(context, config.capabilities(), &mut tracked)?;
    let mut result = BuildStateDiscovery {
        paths: detected.contribution.build_state,
        ..BuildStateDiscovery::default()
    };
    let caches = javascript_tool_caches(
        context,
        config.capabilities(),
        &detected.active,
        &mut tracked,
    )?;
    merge_build_state(&mut result.paths, caches)?;
    discover_cargo_targets(context, &config, &mut tracked, cargo, &mut result)?;
    let declared = expand_declared(context.workspace_root, config.build_state())?;
    declare(context, &declared, &mut result.paths)?;
    result.paths.sort();
    result.paths.dedup();
    Ok(result)
}

/// Every checkout path the `[build] state` entries name, sorted: a literal as spelled, and a
/// pattern once per directory its selector matches among those the checkout tracks files under
/// (one `git ls-files` per pattern, with Git's own glob reading), each with the pattern's
/// literal state beneath it. A literal and a pattern's match naming one path are one declared
/// path, kept when either entry is. No two may overlap: a pattern's match inside a literal path,
/// or the other way round, would link state through state.
pub(super) fn expand_declared(
    workspace: &Path,
    entries: &[DeclaredState],
) -> Result<Vec<BuildStatePath>> {
    let mut expanded = BTreeMap::new();
    for entry in entries {
        let (selector, state, keep) = match entry {
            DeclaredState::Path(path) => {
                insert_declared(&mut expanded, path.clone());
                continue;
            }
            DeclaredState::Pattern {
                selector,
                state,
                keep,
            } => (selector, state, *keep),
        };
        if fs::symlink_metadata(workspace.join(".git")).is_err() {
            continue;
        }
        let depth = selector.components().count();
        let mut pathspec = OsString::from(":(glob)");
        pathspec.push(selector.join("**"));
        let listed = ls_files(workspace, &[pathspec.as_os_str()], |output| {
            CowshedError::environment_missing(
                format!(
                    "cannot expand [build] state {}: {}",
                    entry.spelling().display(),
                    crate::git::git_ended(output)
                ),
                "repair the checkout's Git index and retry",
            )
        })?;
        let mut selected = BTreeSet::new();
        for name in listed.split(|byte| *byte == 0) {
            let tracked = Path::new(std::ffi::OsStr::from_bytes(name));
            if tracked.components().count() > depth {
                selected.insert(tracked.components().take(depth).collect::<PathBuf>());
            }
        }
        for directory in selected {
            let path = directory.join(state);
            let declared = path
                .to_str()
                .ok_or("is not UTF-8")
                .and_then(BuildStatePath::declared)
                .map(|path| if keep { path.kept() } else { path })
                .map_err(|reason| {
                    CowshedError::usage(
                        format!(
                            ".cowshed.toml [build] state {} selects {}, which {reason}",
                            entry.spelling().display(),
                            path.display()
                        ),
                        "narrow the pattern so it selects only checkout directories",
                    )
                })?;
            insert_declared(&mut expanded, declared);
        }
    }
    let expanded: Vec<BuildStatePath> = expanded.into_values().collect();
    for (index, outer) in expanded.iter().enumerate() {
        if let Some(inner) = expanded[index + 1..].iter().find(|inner| {
            inner
                .checkout
                .as_path()
                .starts_with(outer.checkout.as_path())
        }) {
            return Err(CowshedError::usage(
                format!(
                    ".cowshed.toml [build] state declares {} inside {}",
                    inner.checkout.as_path().display(),
                    outer.checkout.as_path().display()
                ),
                "declare only the outer path, or narrow the pattern that selects the inner one",
            ));
        }
    }
    Ok(expanded)
}

/// Add `path` to the expansion: a literal and a pattern's match naming one checkout path are one
/// declared path, kept when either entry is.
fn insert_declared(expanded: &mut BTreeMap<PathBuf, BuildStatePath>, path: BuildStatePath) {
    let keep = path.keep;
    expanded
        .entry(path.checkout.as_path().to_owned())
        .and_modify(|held| held.keep |= keep)
        .or_insert(path);
}

/// Join `declared` to the `discovered` paths. Declared state leaves its path at its first touch,
/// discarded as a cache or, where `[build] keep` names it, kept intact, and is rebuilt on the
/// volume, never copied (16_build_volumes.md, "Declared build state"), so a declared path may not
/// hold tracked source (the guard migration applies), may
/// not reach outside the checkout through a symlinked parent, and may not overlap a tool's state,
/// which its capability already links.
fn declare(
    context: &DetectionContext<'_>,
    declared: &[BuildStatePath],
    discovered: &mut Vec<BuildStatePath>,
) -> Result<()> {
    let root = context.workspace_root;
    for state in declared {
        let path = state.checkout.as_path();
        super::validate_project_directory(root, &root.join(path.parent().unwrap_or(path)))
            .map_err(|error| {
                CowshedError::usage(
                    format!(
                        ".cowshed.toml [build] state declares {}, which is not inside the \
                         checkout: {error}",
                        path.display()
                    ),
                    "declare a path whose parent directories are real directories of the checkout",
                )
            })?;
        if let Some(tool) = discovered.iter().find(|tool| {
            tool.checkout.as_path().starts_with(path)
                || path.starts_with(tool.checkout.as_path())
                || tool.volume.as_path().starts_with(state.volume.as_path())
                || state.volume.as_path().starts_with(tool.volume.as_path())
        }) {
            return Err(CowshedError::conflict(
                format!(
                    ".cowshed.toml [build] state declares {}, which overlaps {}, the {:?} build \
                     state capability detection already keeps on the build volume",
                    path.display(),
                    tool.checkout.as_path().display(),
                    crate::build_volume::BuildStateTool::of(tool),
                ),
                format!(
                    "remove {} from [build] state: declare only tool state no capability detects",
                    path.display()
                ),
            ));
        }
    }
    let declared_refs: Vec<&BuildStatePath> = declared.iter().collect();
    if let Some(refusal) = crate::build_volume::tracked_source(root, &declared_refs)? {
        let mut files = String::new();
        for file in &refusal.tracked_files {
            files.push(' ');
            files.push_str(&file.to_string_lossy());
        }
        return Err(CowshedError::usage(
            format!(
                ".cowshed.toml [build] state declares {}, which holds tracked source:{files}",
                refusal.path.display()
            ),
            format!(
                "declared build state leaves its path at its first touch and is rebuilt, never \
                 copied, on the volume, so it holds only untracked files: remove {} from [build] \
                 state, or untrack it (`git rm -r --cached`) and ignore it",
                refusal.path.display()
            ),
        ));
    }
    merge_build_state(discovered, declared.to_vec())
}

/// Each in-checkout Cargo target directory of the tracked Cargo workspaces, asked of `cargo`,
/// and a finding for each workspace it could not answer for.
fn discover_cargo_targets(
    context: &DetectionContext<'_>,
    config: &crate::storage::bootstrap::CowshedConfig,
    tracked: &mut TrackedManifests,
    cargo: &mut dyn CargoRunner,
    result: &mut BuildStateDiscovery,
) -> Result<()> {
    let cargo_override = config.capabilities().get(&CapabilityId::Cargo);
    if cargo_override.is_some_and(|setting| setting.disabled) {
        return Ok(());
    }
    let selected = cargo_override
        .and_then(|setting| setting.directory.as_ref())
        .map(|directory| context.workspace_root.join(directory))
        .unwrap_or_else(|| context.workspace_root.to_owned());
    super::validate_project_directory(context.workspace_root, &selected)?;
    let mut manifests = Vec::new();
    let mut queries = Vec::new();
    for manifest in tracked_manifests(context.workspace_root, &selected, tracked)? {
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
    // Record the canonical installed mbx route alongside the target ownership discovered here.
    result.mbx = located
        .mbx
        .as_deref()
        .and_then(|path| fs::canonicalize(path).ok());
    for (manifest, root) in manifests.into_iter().zip(located.answers) {
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
            Err(unanswered) => {
                result.unanswered(manifest, CargoDiscoveryPhase::WorkspaceRoot, unanswered)
            }
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
    for (query, metadata) in queries.iter().zip(metadata.answers) {
        let relative_manifest = query
            .manifest
            .strip_prefix(context.workspace_root)
            .expect("workspace roots were checked above")
            .to_owned();
        let metadata = metadata.and_then(|bytes| {
            serde_json::from_slice::<CargoMetadata>(&bytes)
                .map_err(|error| CargoUnanswered::Failed(error.to_string()))
        });
        let metadata = match metadata {
            Ok(metadata) => metadata,
            Err(unanswered) => {
                result.unanswered(relative_manifest, CargoDiscoveryPhase::Metadata, unanswered);
                continue;
            }
        };
        // mbx manages `<workspace>/target` alone and leaves a configured one unmanaged, so in an
        // mbx project that build would land in the checkout once the volume released it
        // (16_build_volumes.md, "One Cargo environment on every path").
        if result.mbx.is_some() {
            let own = metadata.workspace_root.join("target");
            let configured = Some(&metadata.target_directory)
                .filter(|directory| **directory != own)
                .map(|directory| ("build.target-dir", directory))
                .or_else(|| {
                    metadata
                        .build_directory
                        .as_ref()
                        .filter(|directory| **directory != metadata.target_directory)
                        .map(|directory| ("build.build-dir", directory))
                });
            if let Some((key, directory)) = configured {
                return Err(crate::CowshedError::usage(
                    format!(
                        "mbx-target-dir: {} builds into {} ({key}), which mbx leaves unmanaged, so \
                         its build would land in the checkout",
                        relative_manifest.display(),
                        directory.display()
                    ),
                    format!(
                        "remove {key} from the tracked Cargo configuration, or disable the mbx \
                         capability"
                    ),
                ));
            }
        }
        // The tool's logical target path is inside the checkout. After provisioning its fixed
        // link deliberately resolves outside the source volume, through .cowshed/build.
        let Ok(relative) = metadata
            .target_directory
            .strip_prefix(context.workspace_root)
        else {
            continue;
        };
        match BuildStatePath::from_paths(relative, relative) {
            // mbx owns the target directory of a project whose environment resolves it
            // (16_build_volumes.md, "One link per checkout"): the volume releases it.
            Ok(path) if result.mbx.is_some() => result.released.push(path),
            Ok(path) => merge_build_state(&mut result.paths, vec![path])?,
            Err(error) => result.findings.push(BuildStateFinding::CargoUnavailable {
                manifest: relative_manifest,
                phase: CargoDiscoveryPhase::Metadata,
                cause: error.to_string(),
            }),
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct CargoMetadata {
    target_directory: PathBuf,
    workspace_root: PathBuf,
    /// Cargo's `build.build-dir`, where it reports one: the target directory when unset.
    #[serde(default)]
    build_directory: Option<PathBuf>,
}

/// The JavaScript tool caches of every package a detected JavaScript package manager installed:
/// a tracked `package.json` beside a real `node_modules` directory. Each contributes its own
/// `node_modules/.cache` to the build volume. That directory is rewritten on every run, rebuilt
/// when missing and reused only by the tree that wrote it, which is build state
/// (16_build_volumes.md); left on the source volume, every run's writes fragment an image that
/// clones share. A tracked `package.json` the package manager never installed, such as a
/// fixture's, contributes nothing, and none under `node_modules` is a package of the project.
/// Any of them active also contributes the checkout's module compile cache,
/// [`NODE_COMPILE_CACHE_CHECKOUT`] at the volume's [`NODE_COMPILE_CACHE_DIRECTORY`]: Node and Bun
/// rewrite it as modules change and rebuild it when it is missing, and on the source image every
/// fork cloned main's whole cache.
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
        // Every active manager names the one compile cache; the dedup below keeps one.
        caches.push(BuildStatePath::new(
            NODE_COMPILE_CACHE_CHECKOUT,
            NODE_COMPILE_CACHE_DIRECTORY,
        )?);
        let selected = overrides
            .get(&id)
            .and_then(|setting| setting.directory.as_ref())
            .map(|directory| context.workspace_root.join(directory))
            .unwrap_or_else(|| context.workspace_root.to_owned());
        super::validate_project_directory(context.workspace_root, &selected)?;
        let prefix = selected
            .strip_prefix(context.workspace_root)
            .expect("directory was contained");
        visit_installed_javascript_trees(context.workspace_root, prefix, tracked, |tree| {
            let cache = tree.join(".cache");
            caches.push(BuildStatePath::from_paths(&cache, &cache)?);
            Ok(())
        })?;
    }
    caches.sort();
    caches.dedup();
    Ok(caches)
}

/// The package trees already selected by JavaScript build-state discovery, never dependency stores.
pub(crate) fn installed_javascript_trees(
    workspace: &Path,
    visit: impl FnMut(&Path) -> Result<()>,
) -> Result<()> {
    visit_installed_javascript_trees(
        workspace,
        Path::new(""),
        &mut TrackedManifests::default(),
        visit,
    )
}

fn visit_installed_javascript_trees(
    workspace: &Path,
    prefix: &Path,
    tracked: &mut TrackedManifests,
    mut visit: impl FnMut(&Path) -> Result<()>,
) -> Result<()> {
    for name in tracked
        .inputs(workspace)?
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let manifest = Path::new(std::ffi::OsStr::from_bytes(name));
        if tracked_manifest(manifest, prefix, PACKAGE_MANIFEST)
            && installed_package(workspace, &workspace.join(manifest))?
        {
            let tree = manifest
                .parent()
                .expect("a manifest names a file")
                .join("node_modules");
            visit(&tree)?;
        }
    }
    Ok(())
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
    inputs: Option<Arc<[u8]>>,
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

fn tracked_build_inputs(workspace: &Path) -> Result<Arc<[u8]>> {
    match fs::symlink_metadata(workspace.join(".git")) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Arc::from(&[][..])),
        Err(error) => return Err(super::detection_error(&workspace.join(".git"), error)),
    }
    let pathspecs = [
        ":(glob)**/Cargo.toml",
        ":(glob)**/.cargo/config*",
        ":(glob)**/go.mod",
        ":(glob)**/package.json",
    ]
    .map(OsStr::new);
    ls_files(workspace, &pathspecs, |output| {
        CowshedError::environment_missing(
            format!(
                "cannot enumerate tracked tool manifests: {}",
                crate::git::git_ended(output)
            ),
            "repair the checkout's Git index and retry",
        )
    })
}

/// The tracked paths of the checkout at `workspace` that `pathspecs` select, NUL-separated, as
/// `git ls-files -z` lists them; `refused` names a listing Git refused. The listing reads the
/// index, and the local config for how to read it, and nothing else, so its answer is kept while
/// both keep their identity ([`InputMemo`]): every exec admission asks it twice and a resident
/// exec once more (15_capabilities.md, "Detection inputs").
fn ls_files(
    workspace: &Path,
    pathspecs: &[&OsStr],
    refused: impl FnOnce(&std::process::Output) -> CowshedError,
) -> Result<Arc<[u8]>> {
    // A workspace and the pathspecs listed in it.
    type Listing = (PathBuf, Vec<OsString>);
    static LISTINGS: LazyLock<InputMemo<Listing, Arc<[u8]>>> = LazyLock::new(InputMemo::new);
    let list = || {
        // WHY unbounded: a git of this host's own repository with hooks, fsmonitor, pager,
        // editors and prompts off (`git_command_at`): it waits on no other process.
        #[allow(clippy::disallowed_methods)]
        let output = crate::git::git_command_at(workspace)
            .args(["ls-files", "-z", "--"])
            .args(pathspecs)
            .output_locked()
            .map_err(|error| crate::git::git_spawn_error(&error))?;
        if !output.status.success() {
            return Err(refused(&output));
        }
        Ok(Arc::from(output.stdout))
    };
    match kept_listing_inputs(workspace) {
        Some(inputs) => {
            let key = (
                workspace.to_owned(),
                pathspecs.iter().map(|spec| spec.to_os_string()).collect(),
            );
            LISTINGS.read(&key, &inputs, list)
        }
        None => list(),
    }
}

/// What a listing at `workspace` reads, when that is the checkout's own `.git/index` and its
/// own configuration ([`local_config_inputs`]): the index first. `.git` is a directory, and no
/// variable of this process points Git at another repository, index or tree. A linked worktree's
/// `.git` file, or a redirected Git, is listed afresh every time.
pub(crate) fn kept_listing_inputs(workspace: &Path) -> Option<Vec<PathBuf>> {
    const REDIRECTS: [&str; 4] = [
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
    ];
    if REDIRECTS
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        return None;
    }
    let git = workspace.join(".git");
    if !fs::symlink_metadata(&git).ok()?.is_dir() {
        return None;
    }
    let mut inputs = vec![git.join("index")];
    inputs.extend(local_config_inputs(&git)?);
    Some(inputs)
}

/// The files Git's configuration lookup reads from the repository whose own directory is `git`,
/// when that is its `config` and the files it includes unconditionally, then the `commondir`
/// pointer whose creation would move the lookup elsewhere (11_shell.md, "Kept readings"). A
/// missing included file is an input too: Git skips it, and its creation moves it. `None`, read
/// again every time, when a `commondir` puts the repository's configuration and objects
/// elsewhere, `extensions.worktreeConfig` adds a per-worktree file, an `includeIf` makes the
/// lookup depend on the checkout's state, an included file includes further or cannot be read,
/// or an include names a path through a home directory.
pub(crate) fn local_config_inputs(git: &Path) -> Option<Vec<PathBuf>> {
    let commondir = git.join("commondir");
    if commondir.symlink_metadata().is_ok() {
        return None;
    }
    let config = git.join("config");
    let includes = config_includes(&fs::read(&config).ok()?)?;
    let mut inputs = vec![config];
    for include in includes {
        // A home or install prefix Git expands is no path this reads.
        if include.starts_with('~') || include.starts_with("%(") {
            return None;
        }
        // Relative to the directory of the file that includes it; an absolute path stands.
        let included = git.join(include);
        match fs::read(&included) {
            Ok(bytes) if config_includes(&bytes)?.is_empty() => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => return None,
        }
        inputs.push(included);
    }
    inputs.push(commondir);
    Some(inputs)
}

/// The `path` of every `[include]` section of the Git configuration `text`, as written. `None`
/// when this does not read the text as Git does: not UTF-8, a continued line, a variable on a
/// section's header line, a quoted or escaped include path, an `includeIf`, or
/// `extensions.worktreeConfig`. A leading byte-order mark is skipped, as Git skips it.
fn config_includes(text: &[u8]) -> Option<Vec<String>> {
    let text = std::str::from_utf8(text).ok()?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut section = String::new();
    let mut paths = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.ends_with('\\') {
            return None;
        }
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if let Some(header) = line.strip_prefix('[') {
            let (name, rest) = header.split_once(']')?;
            let rest = rest.trim();
            if !(rest.is_empty() || rest.starts_with(['#', ';'])) {
                return None;
            }
            section = name.trim().to_ascii_lowercase();
            if section.starts_with("includeif") {
                return None;
            }
            continue;
        }
        let (key, value) = line.split_once('=').unwrap_or((line, ""));
        let key = key.trim().to_ascii_lowercase();
        let value = value.split(['#', ';']).next().unwrap_or("").trim();
        if section == "extensions" && key == "worktreeconfig" {
            return None;
        }
        if section == "include" && key == "path" {
            if value.is_empty() || value.contains(['"', '\\']) {
                return None;
            }
            paths.push(value.to_owned());
        }
    }
    Some(paths)
}

/// The directory of every tracked package manifest of the checkout at `workspace` outside any
/// `node_modules`: where an install makes or removes a package tree.
pub(crate) fn tracked_package_directories(workspace: &Path) -> Result<Vec<PathBuf>> {
    let mut tracked = TrackedManifests::default();
    Ok(tracked
        .inputs(workspace)?
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| Path::new(std::ffi::OsStr::from_bytes(name)))
        .filter(|manifest| tracked_manifest(manifest, Path::new(""), PACKAGE_MANIFEST))
        .map(|manifest| workspace.join(manifest.parent().expect("a manifest names a file")))
        .collect())
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

/// The driver run for real on every platform: the NixOS runner reaches these proofs, and the
/// macOS-only module below does not.
#[cfg(test)]
mod driver_tests {
    use std::os::unix::fs::PermissionsExt;

    use super::super::test_support::Fixture;
    use super::*;
    use cowshed_os::test_backstop::CommandBackstop as _;

    /// An executable file of the fixture, `relative` to its root.
    pub(super) fn executable(fixture: &Fixture, relative: &str, content: &str) {
        let path = fixture.root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, content).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The host's own PATH: where a fixture program finds the real program it stands in front of.
    fn host_path() -> OsString {
        std::env::var_os("PATH").expect("the test process has a PATH")
    }

    /// The driver as a job runs it: `bash` is looked up on a PATH that leads with the fixture's
    /// `bin` and keeps the host's own behind it, so the programs the fixture does not provide are
    /// the host's real ones (08_testing.md, "Fixtures").
    #[cfg(target_os = "macos")]
    pub(super) fn run_driver(
        fixture: &Fixture,
        args: &[&str],
        queries: &[CargoQuery],
    ) -> std::process::Output {
        let path = std::env::join_paths(
            std::iter::once(fixture.root.join("bin")).chain(std::env::split_paths(&host_path())),
        )
        .unwrap();
        run_driver_on(fixture, args, queries, path)
    }

    /// The driver with `bash` looked up on exactly `path`, as a command a test may add to.
    /// `HOST_PATH` is the host's PATH, for a fixture program that hands over to the host's own;
    /// `PROBE_ROOT` is where fixture programs leave their notes.
    pub(super) fn driver_command(
        fixture: &Fixture,
        args: &[&str],
        queries: &[CargoQuery],
        path: OsString,
    ) -> std::process::Command {
        let argv = driver_argv(args, &queries.iter().collect::<Vec<_>>());
        let mut command = std::process::Command::new(&argv[0]);
        command
            .args(&argv[1..])
            .env("PATH", path)
            .env("HOST_PATH", host_path())
            .env("PROBE_ROOT", &fixture.root)
            // A job's environment holds neither a caller target directory (15_capabilities.md,
            // "Detection inputs") nor the `CARGO` the test's own Cargo exports: a shell that
            // names its Cargo that way is a test's to say.
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("CARGO_BUILD_TARGET_DIR")
            .env_remove("CARGO");
        command
    }

    /// [`driver_command`] run to its end.
    pub(super) fn run_driver_on(
        fixture: &Fixture,
        args: &[&str],
        queries: &[CargoQuery],
        path: OsString,
    ) -> std::process::Output {
        driver_command(fixture, args, queries, path)
            .output_bounded()
            .unwrap()
    }

    /// Core's bootstrap links in the fixture's `bin`: `bash` and [`DRIVER_PROGRAMS`], each noting
    /// that it ran and handing over to the host's own. No `cargo`: cowshed links none
    /// (15_capabilities.md, "Bootstrap executables").
    pub(super) fn bootstrap_links(fixture: &Fixture) {
        for name in ["bash"].into_iter().chain(DRIVER_PROGRAMS) {
            executable(
                fixture,
                &format!("bin/{name}"),
                &format!(
                    "#!/bin/sh\n\
                     echo {name} >>\"$PROBE_ROOT/ran\"\n\
                     real=$(PATH=$HOST_PATH command -v {name}) || exit 127\n\
                     exec \"$real\" \"$@\"\n"
                ),
            );
        }
    }

    /// mbx is the project's only where its shell routes `cargo` through mbx: the Cargo shim
    /// `mbx setup` produces (11_shell.md, "Canonical mbx environment"). An `mbx` that is merely on
    /// PATH beside a plain `cargo` names nothing, or plain Cargo would write a real `target/`
    /// into the source tree once the build volume released it.
    #[test]
    fn the_job_names_mbx_only_when_its_cargo_is_mbx_s_shim() {
        let queries = [CargoQuery {
            directory: PathBuf::from("/"),
            manifest: "Cargo.toml".into(),
        }];
        for (cargo, named) in [
            ("#!/bin/sh\necho answered\n", false),
            (
                "#!/bin/sh\nMBX_CARGO_SHIM_MODE=1\nexport MBX_CARGO_SHIM_MODE\necho answered\n",
                true,
            ),
        ] {
            let fixture = Fixture::new();
            bootstrap_links(&fixture);
            executable(&fixture, "bin/cargo", cargo);
            executable(&fixture, "bin/mbx", "#!/bin/sh\nexit 0\n");
            let only_the_fixture = fixture.root.join("bin").into_os_string();
            let output = run_driver_on(&fixture, &["locate-project"], &queries, only_the_fixture);
            assert!(output.status.success(), "{output:?}");
            let answered = driver_answers(&output, 1, None);
            assert_eq!(answered.answers, [Ok(b"answered".to_vec())]);
            assert_eq!(
                answered.mbx,
                named.then(|| fixture.root.join("bin/mbx")),
                "{cargo}"
            );
        }
    }

    /// The driver under the oldest `bash` core may link. On a macOS host whose Nix and Homebrew
    /// profiles hold none, core's bootstrap `bash` is the system's `/bin/bash`, bash 3.2
    /// (15_capabilities.md, "Bootstrap executables"), and a shell-less checkout's job runs the
    /// driver with it. The driver still recognises mbx's Cargo shim there: a shim that runs Cargo
    /// names mbx, and one that runs none is the batch's one route failure.
    #[cfg(target_os = "macos")]
    #[test]
    fn the_driver_recognises_mbx_s_shim_under_the_system_bash() {
        let version = std::process::Command::new("/bin/bash")
            .args(["-c", "printf %s \"${BASH_VERSINFO[0]}\""])
            .output_bounded()
            .unwrap();
        assert_eq!(
            version.stdout, b"3",
            "/bin/bash is not bash 3, so this test no longer runs the driver under bash 3.2"
        );
        let queries = [CargoQuery {
            directory: PathBuf::from("/"),
            manifest: "Cargo.toml".into(),
        }];
        for runs_cargo in [true, false] {
            let fixture = Fixture::new();
            bootstrap_links(&fixture);
            executable(&fixture, "bin/bash", "#!/bin/sh\nexec /bin/bash \"$@\"\n");
            executable(
                &fixture,
                "bin/cargo",
                &format!(
                    "#!/bin/sh\nMBX_CARGO_SHIM_MODE=1\nexport MBX_CARGO_SHIM_MODE\n{}\n",
                    if runs_cargo {
                        "echo answered"
                    } else {
                        "echo 'mbx[error]: no Cargo' >&2\nexit 1"
                    }
                ),
            );
            executable(&fixture, "bin/mbx", "#!/bin/sh\nexit 0\n");
            let only_the_fixture = fixture.root.join("bin").into_os_string();
            let output = run_driver_on(&fixture, &["locate-project"], &queries, only_the_fixture);
            let expected = if runs_cargo {
                CargoAnswers {
                    answers: vec![Ok(b"answered".to_vec())],
                    mbx: Some(fixture.root.join("bin/mbx")),
                }
            } else {
                CargoAnswers {
                    answers: vec![Err(CargoUnanswered::CargoRouteFailed {
                        shell: None,
                        cause: "exit status 1: mbx[error]: no Cargo".to_owned(),
                    })],
                    mbx: None,
                }
            };
            assert_eq!(driver_answers(&output, 1, None), expected, "{output:?}");
        }
    }

    /// A job whose PATH holds core's bootstrap links and the repository's `cargo` has no more than
    /// `bash`, `cargo` and [`DRIVER_PROGRAMS`]: the driver finds each there, never at a fixed path
    /// (NixOS keeps only `/bin/sh` and `/usr/bin/env`), and needs nothing else. The PATH here is the
    /// fixture's `bin` alone, where each program notes that it ran and hands over to the host's own:
    /// a program reached any other way leaves no note, and one the driver newly needs is not found.
    #[test]
    fn cargo_driver_runs_on_a_path_holding_only_its_programs() {
        let fixture = Fixture::new();
        fs::write(fixture.root.join("ran"), "").unwrap();
        bootstrap_links(&fixture);
        executable(
            &fixture,
            "bin/cargo",
            "#!/bin/sh\necho cargo >>\"$PROBE_ROOT/ran\"\necho answered\n",
        );
        let queries = [CargoQuery {
            directory: fixture.root.clone(),
            manifest: "Cargo.toml".into(),
        }];
        let only_the_fixture = fixture.root.join("bin").into_os_string();
        let output = run_driver_on(&fixture, &["locate-project"], &queries, only_the_fixture);
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            driver_answers(&output, 1, None).answers,
            [Ok(b"answered".to_vec())]
        );
        let notes = fs::read_to_string(fixture.root.join("ran")).unwrap();
        let mut ran: Vec<&str> = notes.lines().collect();
        ran.sort_unstable();
        ran.dedup();
        let mut wanted: Vec<&str> = ["bash", "cargo"]
            .into_iter()
            .chain(DRIVER_PROGRAMS)
            .collect();
        wanted.sort_unstable();
        assert_eq!(ran, wanted);
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::super::test_support::Fixture;
    use super::driver_tests::{
        bootstrap_links, driver_command, executable, run_driver, run_driver_on,
    };
    use super::*;
    use cowshed_os::test_backstop::CommandBackstop as _;

    fn write(fixture: &Fixture, relative: &str, content: &str) {
        let path = fixture.root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    /// The report of a discovery job that has not answered names what the job itself was
    /// observed doing -- preparing, or running as a named process in a named state -- and
    /// never a cause inferred from other processes: a guessed lock holder printed as fact
    /// once sent four readers to a lock the job never waited on.
    #[test]
    fn a_wait_report_names_only_the_observed_phase() {
        let preparing = wait_report(
            42,
            "locate-project",
            Path::new("/checkout"),
            3,
            ObservedRun::Preparing,
        );
        assert!(
            preparing.contains("the command has not started"),
            "{preparing}"
        );
        let running = wait_report(
            42,
            "metadata",
            Path::new("/checkout"),
            1,
            ObservedRun::Running {
                pid: 4242,
                state: "sleeping".to_owned(),
                command: Some("direnv export bash".to_owned()),
            },
        );
        assert!(running.contains("4242"), "{running}");
        assert!(running.contains("sleeping"), "{running}");
        assert!(running.contains("direnv export bash"), "{running}");
        for report in [&preparing, &running] {
            assert!(
                !report.contains("package-cache") && !report.contains("lock"),
                "a report must not name a cause it did not observe: {report}"
            );
        }
    }

    /// A supervisor keeps the tracked listing while the index keeps its identity, so a manifest
    /// staged after a kept listing is listed and moves the fingerprint: a resident exec is never
    /// admitted onto build state a refresh would change (06_cli.md, "Resident workspaces").
    #[test]
    fn a_manifest_staged_after_a_kept_listing_is_listed() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-kept-listing-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let git = |args: &[&str]| {
            let output = crate::git::git_command_at(&root)
                .args(args)
                .output_bounded()
                .unwrap();
            assert!(output.status.success(), "git {args:?}: {output:?}");
        };
        let write = |relative: &str, content: &str| {
            let path = root.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        };
        git(&["init", "--quiet"]);
        write("Cargo.toml", "[workspace]\n");
        git(&["add", "Cargo.toml"]);
        for input in ["index", "config"] {
            crate::input_memo::settle(&root.join(".git").join(input));
        }
        let listed = || TrackedManifests::default().inputs(&root).unwrap().to_vec();
        let before = tracked_manifest_fingerprint(&root).unwrap();
        assert_eq!(listed(), b"Cargo.toml\0");
        assert_eq!(listed(), b"Cargo.toml\0", "the kept listing");
        write("member/Cargo.toml", "[package]\nname = \"member\"\n");
        git(&["add", "member/Cargo.toml"]);
        assert_eq!(listed(), b"Cargo.toml\0member/Cargo.toml\0");
        assert_ne!(tracked_manifest_fingerprint(&root).unwrap(), before);
        fs::remove_dir_all(&root).unwrap();
    }
    fn git(fixture: &Fixture, args: &[&str]) {
        let output = crate::git::git_command_at(&fixture.root)
            .args(args)
            .output_bounded()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn cargo_and_go_share_one_lazy_tracked_manifest_snapshot() {
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
        // A second query now sees an empty index. Go must instead use the
        // exact snapshot Cargo loaded, including Go's nested module.
        fs::remove_file(fixture.root.join(".git/index")).unwrap();
        let detected =
            super::super::detect_with(&fixture.context(), &BTreeMap::new(), &mut tracked).unwrap();
        assert_eq!(detected.active, [CapabilityId::Cargo, CapabilityId::Go]);
        let go = &super::super::go::DETECTOR;
        assert!(
            go.detect_with(&fixture.context(), &mut tracked)
                .unwrap()
                .is_some(),
            "go re-queried the index rather than sharing the snapshot",
        );
        assert!(go.detect(&fixture.context()).unwrap().is_none());
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
        discover_build_state(&fixture.context(), &mut HostCargo::default()).unwrap()
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

        // Once the canonical environment resolves mbx, mbx owns every Cargo target: the volume
        // releases them, and the realpath of that mbx is the answer. A tracked target directory
        // mbx would leave unmanaged is refused, naming its manifest and the remedy
        // (16_build_volumes.md, "One Cargo environment on every path").
        let pin = fixture.root.join("pin/bin/mbx");
        fs::create_dir_all(pin.parent().unwrap()).unwrap();
        fs::write(&pin, b"#!/bin/sh\n").unwrap();
        let mut resolving = HostCargo {
            mbx: Some(pin.clone()),
        };
        let refused = discover_build_state(&fixture.context(), &mut resolving)
            .expect_err("a tracked target-dir under mbx is refused");
        assert!(
            refused.message.starts_with("mbx-target-dir:")
                && refused.message.contains("tooling/helper/Cargo.toml")
                && refused.message.contains("build-target"),
            "{refused:?}"
        );
        git(
            &fixture,
            &[
                "rm",
                "--cached",
                "--quiet",
                "tooling/helper/.cargo/config.toml",
            ],
        );
        fs::remove_file(fixture.root.join("tooling/helper/.cargo/config.toml")).unwrap();
        let state = discover_build_state(&fixture.context(), &mut resolving).unwrap();
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert!(state.paths.is_empty(), "{:?}", state.paths);
        assert_eq!(
            state.released,
            [
                BuildStatePath::new("rust/target", "rust/target").unwrap(),
                BuildStatePath::new("tooling/helper/target", "tooling/helper/target").unwrap(),
            ],
        );
        assert_eq!(state.mbx, Some(fs::canonicalize(&pin).unwrap()));
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

    /// Discovery's Cargo asked the way a job of a checkout without a shell asks it: on a PATH
    /// holding core's bootstrap links alone ([`bootstrap_links`]).
    struct ShelllessCargo<'a>(&'a Fixture);

    impl CargoRunner for ShelllessCargo<'_> {
        fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<CargoAnswers> {
            let bootstrap = self.0.root.join("bin").into_os_string();
            let output = run_driver_on(self.0, args, queries, bootstrap);
            Ok(driver_answers(&output, queries.len(), None))
        }
    }

    /// A Cargo workspace of two manifests that pins its toolchain (`rust-toolchain.toml`), with
    /// core's bootstrap links and no shell to supply that toolchain.
    fn toolchain_pinned_workspace(fixture: &Fixture) {
        git(fixture, &["init", "--quiet"]);
        write(
            fixture,
            "Cargo.toml",
            "[workspace]\nmembers = [\"probe\"]\nresolver = \"2\"\n",
        );
        package(fixture, "probe", "probe");
        write(
            fixture,
            "rust-toolchain.toml",
            "[toolchain]\nchannel = \"stable\"\n",
        );
        git(
            fixture,
            &[
                "add",
                "Cargo.toml",
                "probe/Cargo.toml",
                "rust-toolchain.toml",
            ],
        );
        bootstrap_links(fixture);
    }

    /// A checkout that pins its toolchain (`rust-toolchain.toml`) but has no shell to supply it:
    /// its job's PATH holds no `cargo`. That is one finding naming the missing shell and every
    /// manifest it skips, never one Cargo failure per manifest, and discovery goes on.
    #[test]
    fn a_job_holding_no_cargo_is_one_finding_naming_the_missing_shell() {
        let fixture = Fixture::new();
        toolchain_pinned_workspace(&fixture);
        let state =
            discover_build_state(&fixture.context(), &mut ShelllessCargo(&fixture)).unwrap();
        assert!(state.paths.is_empty(), "{:?}", state.paths);
        assert_eq!(
            state.findings,
            [BuildStateFinding::NoCargo {
                shell: None,
                manifests: vec!["Cargo.toml".into(), "probe/Cargo.toml".into()],
            }]
        );
    }

    /// Discovery's Cargo asked the way a job on an mbx host asks it: its PATH leads with the
    /// installed route ([`mbx_route`]), then core's bootstrap links, then `behind`, what the
    /// workspace shell adds to PATH; `cargo` is the `CARGO` the shell exports, if any
    /// (11_shell.md, "Canonical mbx environment").
    struct RoutedCargo<'a> {
        fixture: &'a Fixture,
        behind: Vec<PathBuf>,
        cargo: Option<PathBuf>,
    }

    impl CargoRunner for RoutedCargo<'_> {
        fn run(&mut self, args: &[&str], queries: &[CargoQuery]) -> Result<CargoAnswers> {
            let path = std::env::join_paths(
                [
                    self.fixture.root.join("route"),
                    self.fixture.root.join("bin"),
                ]
                .into_iter()
                .chain(self.behind.iter().cloned()),
            )
            .expect("fixture directories join into a PATH");
            let mut command = driver_command(self.fixture, args, queries, path);
            if let Some(cargo) = &self.cargo {
                command.env("CARGO", cargo);
            }
            let output = command.output_bounded().unwrap();
            Ok(driver_answers(&output, queries.len(), None))
        }
    }

    /// The route `cowshed setup` provisions, in the fixture's `route`: an `mbx`, and mbx's Cargo
    /// shim, standing in for mbx's own resolution of the Cargo it runs (14_nix.md, "mbx"): `CARGO`
    /// when the job exports it, else the first `cargo` on PATH outside the shim's own directory,
    /// and with neither the failure mbx reports.
    fn mbx_route(fixture: &Fixture) {
        executable(
            fixture,
            "route/cargo",
            r#"#!/bin/sh
MBX_CARGO_SHIM_MODE=1
export MBX_CARGO_SHIM_MODE
if [ -n "${CARGO-}" ]; then
  exec "$CARGO" "$@"
fi
rest=$PATH:
while [ -n "$rest" ]; do
  directory=${rest%%:*}
  rest=${rest#*:}
  [ "$directory" = "${0%/*}" ] && continue
  [ -x "$directory/cargo" ] && exec "$directory/cargo" "$@"
done
echo "mbx[error]: could not find the real Cargo after excluding $0" >&2
exit 1
"#,
        );
        executable(fixture, "route/mbx", "#!/bin/sh\nexit 0\n");
    }

    /// On an mbx host the route leads every Cargo job's PATH with mbx's Cargo shim, which runs
    /// the Cargo mbx resolves (14_nix.md, "mbx"). A checkout whose shell supplies it none is one
    /// finding carrying mbx's own account, asked once by `cargo -V`, never one mbx failure per
    /// manifest (15_capabilities.md, "Detection inputs").
    #[test]
    fn a_route_that_runs_no_cargo_is_one_finding_carrying_mbx_s_account() {
        let fixture = Fixture::new();
        toolchain_pinned_workspace(&fixture);
        mbx_route(&fixture);
        let mut cargo = RoutedCargo {
            fixture: &fixture,
            behind: Vec::new(),
            cargo: None,
        };
        let state = discover_build_state(&fixture.context(), &mut cargo).unwrap();
        assert_eq!(
            state.findings,
            [BuildStateFinding::CargoRouteFailed {
                shell: None,
                manifests: vec!["Cargo.toml".into(), "probe/Cargo.toml".into()],
                cause: format!(
                    "exit status 1: mbx[error]: could not find the real Cargo after excluding {}",
                    fixture.root.join("route/cargo").display()
                ),
            }]
        );
        assert!(
            state.paths.is_empty() && state.released.is_empty(),
            "{state:?}"
        );
        assert_eq!(state.mbx, None);
    }

    /// The shell's `cargo` behind the route is the toolchain the shim runs: discovery names the
    /// workspace root through the route, records the mbx it reaches, and releases the target mbx
    /// owns.
    #[test]
    fn a_shell_s_cargo_behind_the_mbx_route_names_its_workspace_root() {
        let fixture = Fixture::new();
        toolchain_pinned_workspace(&fixture);
        mbx_route(&fixture);
        let toolchain = fixture.root.join("toolchain");
        fs::create_dir(&toolchain).unwrap();
        std::os::unix::fs::symlink(real_cargo(), toolchain.join("cargo")).unwrap();
        let mut cargo = RoutedCargo {
            fixture: &fixture,
            behind: vec![toolchain],
            cargo: None,
        };
        let state = discover_build_state(&fixture.context(), &mut cargo).unwrap();
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert!(state.paths.is_empty(), "{:?}", state.paths);
        assert_eq!(
            state.released,
            [BuildStatePath::new("target", "target").unwrap()]
        );
        assert_eq!(
            state.mbx,
            Some(fs::canonicalize(fixture.root.join("route/mbx")).unwrap())
        );
    }

    /// The Cargo driving this test, canonical: never a `cargo` found on PATH (08_testing.md).
    fn real_cargo() -> PathBuf {
        fs::canonicalize(
            std::env::var_os("CARGO").expect("Cargo publishes its real compiler driver"),
        )
        .expect("real Cargo driver")
    }

    /// A shell may name its Cargo only through `CARGO`, which mbx's shim runs before any `cargo`
    /// on PATH (14_nix.md, "mbx"): such a job holds a toolchain, and discovery names its
    /// workspace root through the route.
    #[test]
    fn a_shell_that_exports_only_cargo_is_asked_through_the_mbx_route() {
        let fixture = Fixture::new();
        toolchain_pinned_workspace(&fixture);
        mbx_route(&fixture);
        let mut cargo = RoutedCargo {
            fixture: &fixture,
            behind: Vec::new(),
            cargo: Some(real_cargo()),
        };
        let state = discover_build_state(&fixture.context(), &mut cargo).unwrap();
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert_eq!(
            state.released,
            [BuildStatePath::new("target", "target").unwrap()]
        );
        assert_eq!(
            state.mbx,
            Some(fs::canonicalize(fixture.root.join("route/mbx")).unwrap())
        );
    }

    #[test]
    fn cargo_driver_bounds_parallelism_and_keeps_answers_in_manifest_order() {
        let fixture = Fixture::new();
        let directory = fixture.root.join("directory with spaces");
        fs::create_dir(&directory).unwrap();
        executable(
            &fixture,
            "bin/cargo",
            r#"#!/usr/bin/env bash
set -eu
index=$3
first=$((index / 4 * 4))
end=$((first + 4))
((end <= 9)) || end=9
# A new wave must not start before every member of the previous wave ended.
for ((previous=0; previous<first; previous++)); do
  [[ -f "$PROBE_ROOT/done-$previous" ]] || { echo "unbounded query $index" >&2; exit 90; }
done
: >"$PROBE_ROOT/started-$index"
# Four-way rendezvous: a serial or under-filled wave fails rather than hanging.
started=$SECONDS
for ((peer=first; peer<end; peer++)); do
  while [[ ! -f "$PROBE_ROOT/started-$peer" ]]; do
    ((SECONDS - started < 5)) || { echo "query $index did not overlap $peer" >&2; exit 91; }
    sleep 0.01
  done
done
# Force a later query to finish first, exercising ordered output framing.
if ((index == first && end - first > 1)); then
  while [[ ! -f "$PROBE_ROOT/done-$((end - 1))" ]]; do
    ((SECONDS - started < 5)) || { echo "last query did not finish" >&2; exit 92; }
    sleep 0.01
  done
fi
: >"$PROBE_ROOT/done-$index"
if ((index == 2)); then
  echo "refused manifest $index" >&2
  exit 101
fi
printf '%s:%s\n' "$index" "$PWD"
"#,
        );
        let queries: Vec<CargoQuery> = (0..9)
            .map(|index| CargoQuery {
                directory: directory.clone(),
                manifest: index.to_string().into(),
            })
            .collect();
        let output = run_driver(&fixture, &["locate-project"], &queries);
        assert!(output.status.success(), "{output:?}");
        let expected: Vec<CargoAnswer> = (0..9)
            .map(|index| {
                if index == 2 {
                    Err(CargoUnanswered::Failed(
                        "Cargo exit status 101: refused manifest 2".to_owned(),
                    ))
                } else {
                    Ok(format!("{index}:{}", directory.display()).into_bytes())
                }
            })
            .collect();
        assert_eq!(
            driver_answers(&output, queries.len(), None).answers,
            expected
        );
        for index in 0..9 {
            assert!(fixture.root.join(format!("done-{index}")).is_file());
        }
    }

    /// The job names the `mbx` its PATH resolves before its first answer, and an empty field when
    /// it resolves none: the canonical environment's mbx, never the controller's.
    #[test]
    fn the_job_names_the_mbx_its_path_resolves_before_its_answers() {
        use std::os::unix::process::ExitStatusExt as _;
        for (stdout, mbx) in [
            (
                b"/pin/bin/mbx\x000\0answer\0\0".to_vec(),
                Some(PathBuf::from("/pin/bin/mbx")),
            ),
            (b"\x000\0answer\0\0".to_vec(), None),
        ] {
            let output = std::process::Output {
                status: std::process::ExitStatus::from_raw(0),
                stdout,
                stderr: Vec::new(),
            };
            assert_eq!(
                driver_answers(&output, 1, None),
                CargoAnswers {
                    answers: vec![Ok(b"answer".to_vec())],
                    mbx
                }
            );
        }
    }

    #[test]
    fn a_query_the_job_never_answered_carries_the_activation_s_own_account() {
        use std::os::unix::process::ExitStatusExt as _;
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: b"\x000\0/checkout/Cargo.toml\n\0\x00101\0\0error: no workspace\n\0".to_vec(),
            stderr: b"direnv: error .envrc failed\n".to_vec(),
        };
        assert_eq!(
            driver_answers(&output, 3, None).answers,
            [
                Ok(b"/checkout/Cargo.toml\n".to_vec()),
                Err(CargoUnanswered::Failed(
                    "Cargo exit status 101: error: no workspace".to_owned()
                )),
                Err(CargoUnanswered::Failed(
                    "the job running Cargo ended (exit status: 1) before Cargo answered: \
                     direnv: error .envrc failed"
                        .to_owned()
                )),
            ]
        );
    }

    /// A driver whose job found no `cargo` answers every query of its batch with the shell that
    /// supplied none, and discovery folds that batch into one finding, however many manifests it
    /// carried.
    #[test]
    fn a_batch_without_cargo_is_one_finding_naming_its_shell() {
        use std::os::unix::process::ExitStatusExt as _;
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(127 << 8),
            stdout: NO_CARGO.to_vec(),
            stderr: Vec::new(),
        };
        let shell = Path::new("tooling/shell");
        let mut discovery = BuildStateDiscovery::default();
        for (manifest, answer) in ["a/Cargo.toml", "b/Cargo.toml"]
            .into_iter()
            .zip(driver_answers(&output, 2, Some(shell)).answers)
        {
            discovery.unanswered(
                manifest.into(),
                CargoDiscoveryPhase::WorkspaceRoot,
                answer.unwrap_err(),
            );
        }
        assert_eq!(
            discovery.findings,
            [BuildStateFinding::NoCargo {
                shell: Some(shell.to_owned()),
                manifests: vec!["a/Cargo.toml".into(), "b/Cargo.toml".into()],
            }]
        );
    }

    #[test]
    fn an_owned_index_names_build_state_and_relinking_keeps_its_fingerprint() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        assert!(!may_name_build_state(&fixture.root).unwrap());
        let absent = tracked_manifest_fingerprint(&fixture.root).unwrap();
        fs::create_dir(fixture.root.join(".codegraph")).unwrap();
        assert!(may_name_build_state(&fixture.root).unwrap());
        let present = tracked_manifest_fingerprint(&fixture.root).unwrap();
        assert_ne!(absent, present);
        fs::remove_dir(fixture.root.join(".codegraph")).unwrap();
        std::os::unix::fs::symlink(".cowshed/build/codegraph", fixture.root.join(".codegraph"))
            .unwrap();
        assert_eq!(
            present,
            tracked_manifest_fingerprint(&fixture.root).unwrap()
        );
        assert_eq!(
            cargo_paths(&fixture).paths,
            [BuildStatePath::new(".codegraph", "codegraph")
                .unwrap()
                .retain()
                .discard_on_clone("writer.pid")
                .unwrap()]
        );
        write(
            &fixture,
            ".cowshed.toml",
            "[capabilities.codegraph]\ndisabled = true\n",
        );
        assert!(!may_name_build_state(&fixture.root).unwrap());
        assert!(cargo_paths(&fixture).paths.is_empty());
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
        let mut fingerprint = tracked_manifest_fingerprint(&fixture.root).unwrap();
        write(&fixture, "untracked/Cargo.toml", "not a manifest");
        write(&fixture, "untracked/.cargo/config.toml", "not a config");
        write(&fixture, "untracked/go.mod", "not a module");
        assert_eq!(
            fingerprint,
            tracked_manifest_fingerprint(&fixture.root).unwrap()
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
            let next = tracked_manifest_fingerprint(&fixture.root).unwrap();
            assert_ne!(
                fingerprint, next,
                "{path} worktree bytes must invalidate discovery"
            );
            git(&fixture, &["add", path]);
            assert_eq!(
                next,
                tracked_manifest_fingerprint(&fixture.root).unwrap(),
                "staging unchanged worktree content does not change the fingerprint"
            );
            fingerprint = next;
        }
        fs::create_dir(fixture.root.join(".codegraph")).unwrap();
        assert_ne!(
            fingerprint,
            tracked_manifest_fingerprint(&fixture.root).unwrap(),
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
        let before = tracked_manifest_fingerprint(&fixture.root).unwrap();
        assert_eq!(
            cargo_paths(&fixture).paths,
            [BuildStatePath::new("rust/first-target", "rust/first-target").unwrap()]
        );
        write(
            &fixture,
            "rust/.cargo/config.toml",
            "[build]\ntarget-dir = \"second-target\"\n",
        );
        assert_ne!(before, tracked_manifest_fingerprint(&fixture.root).unwrap());
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
                BuildStatePath::new(".cowshed/cache/node-compile-cache", "node-compile-cache")
                    .unwrap(),
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
                && !fixture.root.join("packages/fixture/node_modules").exists()
                && !fixture.root.join(".cowshed").exists(),
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
            [
                BuildStatePath::new(".cowshed/cache/node-compile-cache", "node-compile-cache")
                    .unwrap(),
                BuildStatePath::new(
                    "packages/app/node_modules/.cache",
                    "packages/app/node_modules/.cache"
                )
                .unwrap(),
            ]
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
        let before = tracked_manifest_fingerprint(&fixture.root).unwrap();
        write(
            &fixture,
            "packages/app/package.json",
            "{\"dependencies\":{}}\n",
        );
        assert_eq!(
            before,
            tracked_manifest_fingerprint(&fixture.root).unwrap(),
            "a dependency edit leaves where tools write unchanged"
        );
        fs::create_dir(fixture.root.join("packages/fixture/node_modules")).unwrap();
        assert_ne!(
            before,
            tracked_manifest_fingerprint(&fixture.root).unwrap(),
            "a newly installed package adds its tool cache"
        );
        fs::remove_dir(fixture.root.join("packages/fixture/node_modules")).unwrap();
        assert_eq!(before, tracked_manifest_fingerprint(&fixture.root).unwrap());
        fs::remove_file(fixture.root.join("bun.lock")).unwrap();
        assert_ne!(
            before,
            tracked_manifest_fingerprint(&fixture.root).unwrap(),
            "the package manager's convention decides whether any cache is build state"
        );
    }

    /// A patch-development checkout of an upstream project and its build tree: a nested Git
    /// checkout the repository ignores, which no capability can recognize as build state.
    fn upstream_checkout(fixture: &Fixture) {
        write(fixture, ".gitignore", "/vendor/upstream/\n");
        git(fixture, &["add", ".gitignore"]);
        write(fixture, "vendor/upstream/build/build.ninja", "rule cc\n");
        let output = crate::git::git_command_at(&fixture.root.join("vendor/upstream"))
            .args(["init", "--quiet"])
            .output_bounded()
            .unwrap();
        assert!(output.status.success());
    }

    #[test]
    fn declared_build_state_joins_discovery_and_its_list_moves_the_fingerprint() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        upstream_checkout(&fixture);
        fs::create_dir(fixture.root.join(".codegraph")).unwrap();
        let undeclared = tracked_manifest_fingerprint(&fixture.root).unwrap();
        assert_eq!(
            cargo_paths(&fixture).paths,
            [BuildStatePath::new(".codegraph", "codegraph")
                .unwrap()
                .retain()
                .discard_on_clone("writer.pid")
                .unwrap()]
        );
        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\"vendor/upstream\"]\n",
        );
        let declared = tracked_manifest_fingerprint(&fixture.root).unwrap();
        assert_ne!(undeclared, declared, "a new declaration is rediscovered");
        let state = cargo_paths(&fixture);
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert_eq!(
            state.paths,
            [
                BuildStatePath::new(".codegraph", "codegraph")
                    .unwrap()
                    .retain()
                    .discard_on_clone("writer.pid")
                    .unwrap(),
                BuildStatePath::declared("vendor/upstream").unwrap()
            ]
        );
        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\"vendor/upstream\", \"tools/.pin\"]\n",
        );
        assert_ne!(
            declared,
            tracked_manifest_fingerprint(&fixture.root).unwrap(),
            "a changed declaration is rediscovered"
        );
        assert_eq!(
            cargo_paths(&fixture).paths,
            [
                BuildStatePath::new(".codegraph", "codegraph")
                    .unwrap()
                    .retain()
                    .discard_on_clone("writer.pid")
                    .unwrap(),
                BuildStatePath::declared("tools/.pin").unwrap(),
                BuildStatePath::declared("vendor/upstream").unwrap(),
            ]
        );
    }

    #[test]
    fn declared_state_holding_source_or_a_tool_s_state_or_escaping_is_refused_with_its_remedy() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        write(&fixture, "vendor/upstream/fix.patch", "tracked source\n");
        git(&fixture, &["add", "vendor/upstream/fix.patch"]);
        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\"vendor/upstream\"]\n",
        );
        let refused = |fixture: &Fixture| {
            discover_build_state(&fixture.context(), &mut HostCargo::default()).unwrap_err()
        };
        let tracked = refused(&fixture);
        assert_eq!(
            tracked.message,
            ".cowshed.toml [build] state declares vendor/upstream, which holds tracked source: \
             vendor/upstream/fix.patch"
        );
        assert!(
            tracked.hint.contains("rebuilt, never copied")
                && tracked
                    .hint
                    .contains("remove vendor/upstream from [build] state"),
            "{tracked:?}"
        );
        assert_eq!(
            fs::read_to_string(fixture.root.join("vendor/upstream/fix.patch")).unwrap(),
            "tracked source\n",
            "a refused declaration deletes nothing"
        );

        write(&fixture, "nx.json", "{}");
        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\".nx/cache/db\"]\n",
        );
        let overlap = refused(&fixture);
        assert_eq!(overlap.code, crate::ErrorCode::Conflict);
        assert_eq!(
            overlap.message,
            ".cowshed.toml [build] state declares .nx/cache/db, which overlaps .nx/cache, the \
             Nx build state capability detection already keeps on the build volume"
        );
        assert!(
            overlap
                .hint
                .contains("remove .nx/cache/db from [build] state"),
            "{overlap:?}"
        );

        std::os::unix::fs::symlink("/tmp", fixture.root.join("escape")).unwrap();
        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\"escape/pin\"]\n",
        );
        let escape = refused(&fixture);
        assert!(
            escape.message.starts_with(
                ".cowshed.toml [build] state declares escape/pin, which is not inside the checkout"
            ),
            "{escape:?}"
        );
    }

    /// A pattern selects the directories the checkout tracks files under, never an untracked
    /// one, and a package that becomes tracked joins it through the fingerprint. A literal
    /// inside one of its matches is refused by name.
    #[test]
    fn a_declared_pattern_expands_over_tracked_packages_and_their_arrival_rediscovers() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        write(&fixture, "packages/a/package.json", "{}\n");
        write(&fixture, "packages/b/src/index.ts", "\n");
        write(&fixture, "packages/README.md", "\n");
        git(&fixture, &["add", "packages"]);
        write(&fixture, "packages/untracked/package.json", "{}\n");
        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\"packages/*/.cache/lmao\", \".cache/lmao\"]\n",
        );
        let expected = |names: &[&str]| {
            let mut paths = vec![BuildStatePath::declared(".cache/lmao").unwrap()];
            paths.extend(names.iter().map(|name| {
                BuildStatePath::declared(&format!("packages/{name}/.cache/lmao")).unwrap()
            }));
            paths
        };
        let state = cargo_paths(&fixture);
        assert!(state.findings.is_empty(), "{:?}", state.findings);
        assert_eq!(state.paths, expected(&["a", "b"]));
        assert!(
            !fixture.root.join("packages/a/.cache").exists(),
            "discovery makes nothing"
        );
        let before = tracked_manifest_fingerprint(&fixture.root).unwrap();
        git(&fixture, &["add", "packages/untracked/package.json"]);
        assert_ne!(
            before,
            tracked_manifest_fingerprint(&fixture.root).unwrap(),
            "a newly tracked package moves the fingerprint"
        );
        assert_eq!(
            cargo_paths(&fixture).paths,
            expected(&["a", "b", "untracked"])
        );

        write(
            &fixture,
            ".cowshed.toml",
            "[build]\nstate = [\"packages/*/.cache/lmao\", \"packages/a/.cache\"]\n",
        );
        let overlap =
            discover_build_state(&fixture.context(), &mut HostCargo::default()).unwrap_err();
        assert_eq!(
            overlap.message,
            ".cowshed.toml [build] state declares packages/a/.cache/lmao inside packages/a/.cache"
        );
    }

    /// A pattern `[build] keep` names expands to kept paths, one per match, and the entries it
    /// does not name stay caches; a literal and a pattern's match naming one path are one declared
    /// path, kept when either entry is.
    #[test]
    fn a_kept_pattern_expands_to_kept_paths() {
        let fixture = Fixture::new();
        git(&fixture, &["init", "--quiet"]);
        write(&fixture, "packages/a/package.json", "{}\n");
        write(&fixture, "packages/b/package.json", "{}\n");
        git(&fixture, &["add", "packages"]);
        for (keep, b_kept) in [("packages/*/.pin", true), ("packages/a/.pin", false)] {
            write(
                &fixture,
                ".cowshed.toml",
                &format!(
                    "[build]\nstate = [\"packages/*/.pin\", \"packages/a/.pin\", \".cache/lmao\"]\nkeep = [\"{keep}\"]\n"
                ),
            );
            let state = cargo_paths(&fixture);
            assert!(state.findings.is_empty(), "{keep}: {:?}", state.findings);
            let b = BuildStatePath::declared("packages/b/.pin").unwrap();
            assert_eq!(
                state.paths,
                [
                    BuildStatePath::declared(".cache/lmao").unwrap(),
                    BuildStatePath::declared("packages/a/.pin").unwrap().kept(),
                    if b_kept { b.kept() } else { b },
                ],
                "{keep}"
            );
        }
    }
}
