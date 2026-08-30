//! Host-owned sccache server managed as a LaunchAgent.
//!
//! Mirrors the gateway service verbs: `start` installs (or repairs) the
//! `dev.cowshed.sccache` plist and activates it, `status` reports launchd and
//! socket health, `stop` deactivates and removes the plist. The agent runs the
//! sccache binary itself as a foreground unix-socket server *outside* every
//! workspace sandbox — launchd, not any workspace, owns the process, so it
//! enforces no workspace's Seatbelt boundary and can serve them all
//! (cowshed_core::sandbox::sccache_server_socket documents the client side).

use crate::args::SccacheCommand;
use crate::gateway_service::{
    activate_launch_agent, canonical_home, effective_uid, inspect_install_state,
    install_host_stable_executable, launchd_error, output_error, remove_launch_agent,
};
use crate::launchd::{
    ExistingPlist, HostStableExecutable, InstallState, LaunchAgentSpec, LaunchdExecutor,
    LaunchdServiceStatus, NativeFilesystem, NativeLaunchctlCommand, SCCACHE_BINARY_NAME,
    kickstart_hint, plan_install,
};
use crate::output::Output;
use cowshed_core::api::{EmptyResult, SccacheStats, SccacheStatus};
use cowshed_core::metadata::ImageCapacity;
use cowshed_core::sandbox::{sccache_cache_directory, sccache_server_socket};
use cowshed_core::storage::bootstrap::{STORE_ROOT, ValidatedHostStorage};
use cowshed_core::{CowshedError, NativeGatewayInventory, Result, validate_existing_host_storage};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const START_DEADLINE: Duration = Duration::from_secs(10);
const START_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Where the absolute path of the `sccache` this host installed is written down.
///
/// Beside the host-stable binaries, on the volume that also carries the plists: whatever launchd
/// can read the agent definition from, it can read this from too.
const SCCACHE_SOURCE_RECORD: &str = "sccache-source";

/// No host gets a smaller cache than this, whatever the store currently holds.
///
/// One debug graph of a workspace-shaped Rust project measures around 18 GB on this hardware, and
/// the point of the cache is to hold several generations of one, so sccache's 10 GiB default
/// evicts a single project's entries faster than a second tenant can reuse them.
const MINIMUM_CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(40);

pub async fn dispatch<W, E>(
    action: SccacheCommand,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    W: Write + Send,
    E: Write + Send,
{
    match action {
        SccacheCommand::Start { capacity } => {
            let capacity = capacity
                .as_deref()
                .map(|value| {
                    value
                        .to_str()
                        .ok_or_else(|| {
                            CowshedError::usage(
                                "--capacity must be valid UTF-8",
                                "use a capacity such as 40g, 120g, or 1t",
                            )
                        })
                        .and_then(|text| {
                            ImageCapacity::parse(text).map_err(|error| {
                                CowshedError::usage(
                                    error.to_string(),
                                    "use a capacity such as 40g, 120g, or 1t",
                                )
                            })
                        })
                })
                .transpose()?;
            let status = start_service(capacity).await?;
            emit_sccache_status(output, json, status)?;
        }
        SccacheCommand::Stop => {
            stop_service()?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            } else {
                output
                    .guidance("sccache daemon is stopped")
                    .map_err(output_error)?;
            }
        }
        SccacheCommand::Status => {
            let status = service_status().await?;
            emit_sccache_status(output, json, status)?;
        }
    }
    Ok(0)
}

/// Install (or repair) the agent and wait for its socket.
///
/// The cap is the caller's when given and derived from the store otherwise, and it is written into
/// the plist: sccache reads `SCCACHE_CACHE_SIZE` once, at server start, so the number has to be in
/// launchd's environment rather than any client's.
pub async fn start_service(capacity: Option<ImageCapacity>) -> Result<SccacheStatus> {
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let cache_directory = sccache_cache_directory();
    fs::create_dir_all(&cache_directory).map_err(|error| {
        CowshedError::internal(format!(
            "could not create {}: {error}",
            cache_directory.display()
        ))
    })?;
    let capacity = match capacity {
        Some(capacity) => capacity,
        None => derived_capacity(&storage).await?,
    };
    let socket = sccache_server_socket();
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let source = resolve_sccache_source(&home)?;
    let executable =
        install_host_stable_executable(&mut executor, &home, SCCACHE_BINARY_NAME, &source)?;
    record_sccache_source(&executable, &source);
    let spec = LaunchAgentSpec::sccache(
        &executable,
        &socket,
        &cache_directory,
        capacity,
        storage.store(),
    )
    .map_err(launchd_error)?;
    if let Some(parent) = spec.standard_error_path().parent() {
        fs::create_dir_all(parent).map_err(|error| {
            CowshedError::internal(format!("could not create {}: {error}", parent.display()))
        })?;
    }
    let observed = inspect_install_state(&spec)?;
    let plan = plan_install(
        &spec,
        InstallState {
            launch_agents_directory_mode: observed.directory_mode,
            plist: observed.plist.as_ref().map(|plist| ExistingPlist {
                bytes: &plist.bytes,
                mode: plist.mode,
            }),
        },
    );
    let uid = effective_uid();
    let written = executor.execute_install(&plan).map_err(launchd_error)?;
    activate_launch_agent(&mut executor, uid, &spec, written)?;

    let deadline = tokio::time::Instant::now() + START_DEADLINE;
    loop {
        if socket_answers(&socket).await {
            return Ok(SccacheStatus {
                installed: true,
                running: true,
                stats: read_stats(&home, &socket).await,
                socket,
            });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CowshedError::environment_missing(
                "sccache daemon did not answer on its socket before the startup deadline",
                kickstart_hint(uid, spec.label()),
            ));
        }
        tokio::time::sleep(START_POLL_INTERVAL).await;
    }
}

/// The cache cap for a host that did not name one.
///
/// Derived from the size the store already carries: the sum of every adopted project's main image
/// as it is *allocated* on disk, floored at [`MINIMUM_CAPACITY`] and rounded up to a whole
/// gibibyte. That sum is the closest cheap proxy for "how much compiled output this host produces"
/// — a main image holds a checkout and, on a host that builds in it, that checkout's object graph —
/// and the cache has to hold the compressed artifacts of several generations of it.
///
/// Allocated rather than logical bytes: these images are sparse and provisioned far beyond their
/// contents (100 GiB by default), so `len()` would derive a cap from the provisioning, not the data.
///
/// Shared with `setup`, which writes the same number into sccache's own config file: a client that
/// finds no daemon starts a server of its own over the same directory, and two servers deriving
/// two different caps would make the shared store's eviction bound depend on which one started.
pub(crate) async fn derived_capacity(storage: &ValidatedHostStorage) -> Result<ImageCapacity> {
    use cowshed_core::metadata::ImageFormat;
    use cowshed_core::storage::StorageLayout;
    use std::os::unix::fs::MetadataExt as _;

    let projects = NativeGatewayInventory::new(storage.clone())
        .adopted_projects()
        .await
        .map_err(|error| {
            CowshedError::internal(format!("could not enumerate adopted projects: {error}"))
        })?;
    let mut bytes = 0_u64;
    for project in projects {
        let layout = StorageLayout::new(storage.store(), &project.repo_id).map_err(|error| {
            CowshedError::internal(format!(
                "could not resolve the layout of {}: {error}",
                project.repo_id
            ))
        })?;
        for format in [ImageFormat::Asif, ImageFormat::Sparse] {
            let Ok(image) = layout.main_image(format) else {
                continue;
            };
            if let Ok(metadata) = fs::metadata(image.image()) {
                bytes = bytes.saturating_add(metadata.blocks().saturating_mul(512));
            }
        }
    }
    let gibibytes = bytes.div_ceil(ImageCapacity::GIBIBYTE);
    Ok(ImageCapacity::from_gibibytes(
        gibibytes.max(MINIMUM_CAPACITY.bytes() / ImageCapacity::GIBIBYTE),
    ))
}

fn stop_service() -> Result<()> {
    let home = canonical_home()?;
    remove_launch_agent(&control_spec(&home)?)?;
    remove_stale_socket(&sccache_server_socket())?;
    Ok(())
}

/// The socket file is the daemon's artifact and cowshed owns its lifecycle: a booted-out server
/// never unlinks it, and a stale socket only confuses inspection (a fresh server unlinks-then-
/// rebinds anyway). Removes exactly a socket; anything else at the path is not ours to delete.
pub(crate) fn remove_stale_socket(socket: &Path) -> Result<()> {
    let Ok(metadata) = fs::symlink_metadata(socket) else {
        return Ok(());
    };
    use std::os::unix::fs::FileTypeExt as _;
    if metadata.file_type().is_socket() {
        fs::remove_file(socket).map_err(|error| {
            CowshedError::internal(format!(
                "could not remove stale sccache socket {}: {error}",
                socket.display()
            ))
        })?;
    }
    Ok(())
}

pub(crate) async fn service_status() -> Result<SccacheStatus> {
    let home = canonical_home()?;
    let socket = sccache_server_socket();
    let spec = control_spec(&home)?;
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let installed = match executor
        .execute_status(&crate::launchd::ControlPlan::print(effective_uid(), &spec))
        .map_err(launchd_error)?
    {
        LaunchdServiceStatus::Loaded { .. } => true,
        LaunchdServiceStatus::NotLoaded { .. } => false,
    };
    let running = socket_answers(&socket).await;
    let stats = if running {
        read_stats(&home, &socket).await
    } else {
        None
    };
    Ok(SccacheStatus {
        installed,
        running,
        stats,
        socket,
    })
}

/// The daemon's own `--show-stats`, as the only authority on what it is doing.
///
/// Reported per language, because the operational question a slot host asks is never "how many
/// hits" but "are the *Rust* units hitting" — cross-workspace C/C++ reuse works without slots and
/// would otherwise mask a Rust hit rate of zero. `base_directories` rides along for the same
/// reason: it is the only way to see whether the server actually took `SCCACHE_BASEDIRS`.
///
/// A stats read that fails is absence, not an error: the verb's job is to report health, and a
/// daemon that answers its socket but not a stats request is still installed and running. The
/// client binary is resolved the same way the install resolves it, so `doctor` reports stats under
/// launchd — where there is no `PATH` — instead of reporting a running daemon with no statistics.
async fn read_stats(home: &Path, socket: &Path) -> Option<SccacheStats> {
    #[derive(serde::Deserialize)]
    struct Counted {
        counts: std::collections::BTreeMap<String, u64>,
    }
    #[derive(serde::Deserialize)]
    struct Counters {
        compile_requests: u64,
        requests_executed: u64,
        cache_hits: Counted,
        cache_misses: Counted,
    }
    #[derive(serde::Deserialize)]
    struct Report {
        max_cache_size: Option<u64>,
        #[serde(default)]
        basedirs: Vec<PathBuf>,
        stats: Counters,
    }

    let output = tokio::process::Command::new(resolve_sccache_source(home).ok()?)
        .args(["--show-stats", "--stats-format", "json"])
        .env("SCCACHE_SERVER_UDS", socket)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let report: Report = serde_json::from_slice(&output.stdout).ok()?;
    Some(SccacheStats {
        max_cache_size: report.max_cache_size.unwrap_or_default(),
        base_directories: report.basedirs,
        compile_requests: report.stats.compile_requests,
        requests_executed: report.stats.requests_executed,
        hits: report.stats.cache_hits.counts,
        misses: report.stats.cache_misses.counts,
    })
}

/// A spec good for launchctl control targets (label and plist path).
///
/// Deterministic, and deliberately not a `PATH` lookup: stop and status must reach the agent
/// `start` installed even after sccache has left `PATH` — a devenv update or removal must not
/// strand it — and `start` writes the plist against this same host-stable path.
pub(crate) fn control_spec(home: &Path) -> Result<LaunchAgentSpec> {
    let executable = HostStableExecutable::new(home, SCCACHE_BINARY_NAME).map_err(launchd_error)?;
    LaunchAgentSpec::sccache(
        &executable,
        &sccache_server_socket(),
        &sccache_cache_directory(),
        MINIMUM_CAPACITY,
        Path::new(STORE_ROOT),
    )
    .map_err(launchd_error)
}

/// The sccache agent, its installed binary, and its socket — everything `setup --uninstall`
/// removes for this service. Returned together because the caller has to deactivate the agent
/// before deleting the binary it runs.
pub(crate) fn sccache_launch_agent(
    home: &Path,
) -> Result<(HostStableExecutable, LaunchAgentSpec, PathBuf)> {
    let executable = HostStableExecutable::new(home, SCCACHE_BINARY_NAME).map_err(launchd_error)?;
    let spec = control_spec(home)?;
    Ok((executable, spec, sccache_server_socket()))
}

/// The record naming the `sccache` this host installed.
fn sccache_source_record(home: &Path) -> Result<PathBuf> {
    Ok(HostStableExecutable::new(home, SCCACHE_BINARY_NAME)
        .map_err(launchd_error)?
        .support_directory()
        .join(SCCACHE_SOURCE_RECORD))
}

/// The `sccache` binary to install, preferring the absolute path this host already recorded.
///
/// launchd hands a LaunchAgent none of the user's `PATH`, so the gateway's startup heal — itself a
/// LaunchAgent — can never find a devenv- or nix-provided `sccache` by name. It only ever resolved
/// because `sccache start` had been run from a user shell, which is why a host with sccache
/// installed all along reported "sccache is not on PATH" on every single boot. The absolute path is
/// therefore recorded when an install succeeds and read back here; `PATH` remains the fallback for
/// the first install on a host that has no record yet.
fn resolve_sccache_source(home: &Path) -> Result<PathBuf> {
    resolve_sccache_source_in(home, std::env::var_os("PATH").as_deref())
}

/// `search_path` is passed rather than read so the resolution order is testable without mutating
/// the process environment, and so the launchd case — no `PATH` at all — is reachable as `None`.
fn resolve_sccache_source_in(home: &Path, search_path: Option<&OsStr>) -> Result<PathBuf> {
    let recorded = recorded_sccache_source(home)?;
    // A record naming a binary that has gone is not an error: a devenv or nix upgrade moves the
    // path, and PATH is where the replacement is found.
    if let Some(path) = recorded.as_deref().filter(|path| path.is_file()) {
        return canonical_sccache(path);
    }
    search_sccache_on_path(search_path).map_err(|error| match recorded {
        // Name the stale record, or the only symptom is PATH being blamed on a host that has had
        // sccache installed the whole time.
        Some(stale) => CowshedError::environment_missing(
            format!(
                "sccache is not on PATH, and the recorded install path {} no longer exists",
                stale.display()
            ),
            error.hint,
        ),
        None => error,
    })
}

/// The recorded path, or `None` when this host has no usable record.
///
/// A record that cannot be read, or does not name an absolute path, is reported and then ignored:
/// the `PATH` fallback still has a chance of working, and a startup heal must not die on a note.
fn recorded_sccache_source(home: &Path) -> Result<Option<PathBuf>> {
    let record = sccache_source_record(home)?;
    let recorded = match fs::read_to_string(&record) {
        Ok(recorded) => recorded,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            eprintln!("cowshed: could not read {}: {error}", record.display());
            return Ok(None);
        }
    };
    let recorded = Path::new(recorded.trim());
    if !recorded.is_absolute() {
        eprintln!(
            "cowshed: ignoring {}: {} is not an absolute path",
            record.display(),
            recorded.display()
        );
        return Ok(None);
    }
    Ok(Some(recorded.to_path_buf()))
}

/// Write the source down so the next heal needs no `PATH`.
///
/// Best effort: an install that succeeded is not undone because a note could not be written. The
/// failure is still said out loud, because what it costs is a PATH complaint on every boot.
fn record_sccache_source(executable: &HostStableExecutable, source: &Path) {
    let record = executable.support_directory().join(SCCACHE_SOURCE_RECORD);
    if let Err(error) = fs::write(&record, format!("{}\n", source.display())) {
        eprintln!(
            "cowshed: could not record the sccache path in {}: {error}",
            record.display()
        );
    }
}

fn search_sccache_on_path(search_path: Option<&OsStr>) -> Result<PathBuf> {
    let search_path = search_path.ok_or_else(|| {
        CowshedError::environment_missing(
            "PATH is not set",
            "run from a shell with sccache on PATH",
        )
    })?;
    for directory in std::env::split_paths(search_path) {
        let candidate = directory.join(SCCACHE_BINARY_NAME);
        if candidate.is_file() {
            return canonical_sccache(&candidate);
        }
    }
    Err(CowshedError::environment_missing(
        "sccache is not on PATH",
        "install sccache (devenv/nix) and run cowshed sccache start from that shell",
    ))
}

fn canonical_sccache(path: &Path) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|error| {
        CowshedError::environment_missing(
            format!("could not resolve {}: {error}", path.display()),
            format!(
                "reinstall sccache with devenv or nix so {} is a real executable, then retry cowshed sccache start",
                path.display()
            ),
        )
    })
}

async fn socket_answers(socket: &Path) -> bool {
    tokio::net::UnixStream::connect(socket).await.is_ok()
}

pub fn emit_sccache_status<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    status: SccacheStatus,
) -> Result<()> {
    if json {
        output.success(status).map_err(output_error)?;
    } else if status.running {
        output
            .guidance("sccache daemon is healthy")
            .map_err(output_error)?;
    } else if status.installed {
        output
            .guidance("sccache daemon is installed but not answering on its socket")
            .map_err(output_error)?;
    } else {
        output
            .guidance("sccache daemon is stopped")
            .map_err(output_error)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    /// A canonical absolute home, as `HostStableExecutable` requires.
    fn scratch_home(label: &str) -> PathBuf {
        let home = std::env::temp_dir().join(format!(
            "cowshed-cli-sccache-{label}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(home.join("Library/Application Support/dev.cowshed/bin"))
            .expect("scratch support directory");
        home.canonicalize().expect("canonical scratch home")
    }

    /// An executable file standing in for a devenv- or nix-provided sccache.
    fn fake_sccache(directory: &Path) -> PathBuf {
        fs::create_dir_all(directory).expect("candidate directory");
        let binary = directory.join(SCCACHE_BINARY_NAME);
        fs::write(&binary, b"#!/bin/sh\nexit 0\n").expect("candidate binary");
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).expect("candidate mode");
        binary
    }

    fn installed(home: &Path) -> HostStableExecutable {
        HostStableExecutable::new(home, SCCACHE_BINARY_NAME).expect("host-stable sccache")
    }

    /// The defect: launchd carries no PATH, so the gateway's startup heal reported "sccache is not
    /// on PATH" on every boot of a host that had sccache all along. With the install-time path
    /// recorded, resolution succeeds with no PATH whatsoever.
    #[test]
    fn the_recorded_path_resolves_sccache_without_any_path() {
        let home = scratch_home("recorded");
        let binary = fake_sccache(&home.join("nix/bin"));
        record_sccache_source(&installed(&home), &binary);

        assert_eq!(
            resolve_sccache_source_in(&home, None).expect("recorded sccache resolves"),
            binary.canonicalize().expect("canonical candidate")
        );
    }

    /// The record wins over PATH, so the heal installs the sccache this host was set up with
    /// rather than whatever a shell happened to expose.
    #[test]
    fn the_record_is_preferred_over_a_path_candidate() {
        let home = scratch_home("preferred");
        let recorded = fake_sccache(&home.join("recorded/bin"));
        let on_path = fake_sccache(&home.join("shell/bin"));
        record_sccache_source(&installed(&home), &recorded);

        let resolved = resolve_sccache_source_in(
            &home,
            Some(OsStr::new(
                on_path
                    .parent()
                    .expect("path directory")
                    .to_str()
                    .expect("utf-8"),
            )),
        )
        .expect("recorded sccache resolves");

        assert_eq!(
            resolved,
            recorded.canonicalize().expect("canonical recorded")
        );
        assert_ne!(resolved, on_path);
    }

    /// First install on a host with no record: PATH is still the way sccache is found, and the
    /// path it found is written down so the next heal does not need a shell.
    #[test]
    fn a_host_without_a_record_falls_back_to_path_and_then_records_it() {
        let home = scratch_home("fallback");
        let binary = fake_sccache(&home.join("shell/bin"));
        let directory = binary.parent().expect("path directory");

        let resolved =
            resolve_sccache_source_in(&home, Some(OsStr::new(directory.to_str().expect("utf-8"))))
                .expect("path sccache resolves");
        assert_eq!(
            resolved,
            binary.canonicalize().expect("canonical candidate")
        );

        record_sccache_source(&installed(&home), &resolved);
        assert_eq!(
            resolve_sccache_source_in(&home, None).expect("recorded sccache resolves"),
            resolved
        );
    }

    /// A devenv or nix upgrade moves the binary. That is not a broken host: the stale record is
    /// ignored and PATH re-resolves it.
    #[test]
    fn a_stale_record_falls_back_to_path() {
        let home = scratch_home("stale");
        record_sccache_source(&installed(&home), &home.join("gone/bin/sccache"));
        let binary = fake_sccache(&home.join("shell/bin"));

        let resolved = resolve_sccache_source_in(
            &home,
            Some(OsStr::new(
                binary
                    .parent()
                    .expect("path directory")
                    .to_str()
                    .expect("utf-8"),
            )),
        )
        .expect("path sccache resolves");

        assert_eq!(
            resolved,
            binary.canonicalize().expect("canonical candidate")
        );
    }

    /// When both the record and PATH come up empty, the error names the stale record: blaming PATH
    /// alone sends the operator looking for a shell problem on a host that had sccache installed.
    #[test]
    fn a_stale_record_with_no_path_candidate_names_the_record() {
        let home = scratch_home("stale-empty");
        let missing = home.join("gone/bin/sccache");
        record_sccache_source(&installed(&home), &missing);

        let error =
            resolve_sccache_source_in(&home, None).expect_err("no sccache anywhere is an error");

        assert_eq!(error.code.as_str(), "environment-missing");
        assert!(
            error.message.contains(&missing.display().to_string()),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("no longer exists"),
            "{}",
            error.message
        );
    }

    /// A record cowshed cannot make sense of must not strand the fallback: the heal reports it and
    /// carries on with PATH.
    #[test]
    fn a_malformed_record_is_ignored_rather_than_fatal() {
        let home = scratch_home("malformed");
        let record = sccache_source_record(&home).expect("record path");
        fs::write(&record, b"sccache\n").expect("relative record");
        let binary = fake_sccache(&home.join("shell/bin"));

        let resolved = resolve_sccache_source_in(
            &home,
            Some(OsStr::new(
                binary
                    .parent()
                    .expect("path directory")
                    .to_str()
                    .expect("utf-8"),
            )),
        )
        .expect("path sccache resolves");

        assert_eq!(
            resolved,
            binary.canonicalize().expect("canonical candidate")
        );
        assert_eq!(recorded_sccache_source(&home).expect("record read"), None);
    }

    /// The record lives beside the host-stable binaries, which is what makes it readable by a
    /// LaunchAgent: the same volume carries the plist launchd already read.
    #[test]
    fn the_record_sits_beside_the_host_stable_binaries() {
        let home = scratch_home("location");

        assert_eq!(
            sccache_source_record(&home).expect("record path"),
            home.join("Library/Application Support/dev.cowshed/sccache-source")
        );
    }
}
