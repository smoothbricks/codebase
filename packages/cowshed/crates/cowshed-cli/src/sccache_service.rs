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
    activate_launch_agent, canonical_home, deactivate_launch_agent, effective_uid,
    inspect_install_state, install_host_stable_executable, launchd_error, output_error,
};
use crate::launchd::{
    ExistingPlist, HostStableExecutable, InstallState, LaunchAgentSpec, LaunchdExecutor,
    LaunchdServiceStatus, NativeFilesystem, NativeLaunchctlCommand, SCCACHE_BINARY_NAME,
    plan_install, plan_remove,
};
use crate::output::Output;
use cowshed_core::api::{EmptyResult, SccacheStats, SccacheStatus};
use cowshed_core::metadata::ImageCapacity;
use cowshed_core::sandbox::{sccache_cache_directory, sccache_server_socket};
use cowshed_core::storage::bootstrap::ValidatedHostStorage;
use cowshed_core::{CowshedError, NativeGatewayInventory, Result, validate_existing_host_storage};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const START_DEADLINE: Duration = Duration::from_secs(10);
const START_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    let cache_directory = sccache_cache_directory(&home);
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
    let socket = sccache_server_socket(&home);
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let executable = install_host_stable_executable(
        &mut executor,
        &home,
        SCCACHE_BINARY_NAME,
        &resolve_sccache_executable()?,
    )?;
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
    executor.execute_install(&plan).map_err(launchd_error)?;
    activate_launch_agent(&mut executor, uid, &spec)?;

    let deadline = tokio::time::Instant::now() + START_DEADLINE;
    loop {
        if socket_answers(&socket).await {
            return Ok(SccacheStatus {
                installed: true,
                running: true,
                stats: read_stats(&socket).await,
                socket,
            });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CowshedError::environment_missing(
                "sccache daemon did not answer on its socket before the startup deadline",
                format!("launchctl kickstart -k gui/{uid}/{}", spec.label()),
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
async fn derived_capacity(storage: &ValidatedHostStorage) -> Result<ImageCapacity> {
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
    let spec = control_spec(&home)?;
    let uid = effective_uid();
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    deactivate_launch_agent(&mut executor, uid, &spec)?;
    let installed = fs::symlink_metadata(spec.plist_path()).is_ok();
    executor
        .execute_install(&plan_remove(&spec, installed))
        .map_err(launchd_error)?;
    // The socket file is the daemon's artifact and cowshed owns its lifecycle:
    // a booted-out server never unlinks it, and a stale socket only confuses
    // inspection (a fresh server unlinks-then-rebinds anyway). Remove exactly
    // a socket; anything else at the path is not ours to delete.
    let socket = sccache_server_socket(&home);
    if let Ok(metadata) = fs::symlink_metadata(&socket) {
        use std::os::unix::fs::FileTypeExt as _;
        if metadata.file_type().is_socket() {
            fs::remove_file(&socket).map_err(|error| {
                CowshedError::internal(format!(
                    "could not remove stale sccache socket {}: {error}",
                    socket.display()
                ))
            })?;
        }
    }
    Ok(())
}

async fn service_status() -> Result<SccacheStatus> {
    let home = canonical_home()?;
    let socket = sccache_server_socket(&home);
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
        read_stats(&socket).await
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
/// daemon that answers its socket but not a stats request is still installed and running.
async fn read_stats(socket: &Path) -> Option<SccacheStats> {
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

    let output = tokio::process::Command::new(resolve_sccache_executable().ok()?)
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
fn control_spec(home: &Path) -> Result<LaunchAgentSpec> {
    let socket = sccache_server_socket(home);
    let cache_directory = sccache_cache_directory(home);
    let executable = HostStableExecutable::new(home, SCCACHE_BINARY_NAME).map_err(launchd_error)?;
    LaunchAgentSpec::sccache(
        &executable,
        &socket,
        &cache_directory,
        MINIMUM_CAPACITY,
        &home.join(".cowshed"),
    )
    .map_err(launchd_error)
}

fn resolve_sccache_executable() -> Result<PathBuf> {
    let path = std::env::var_os("PATH").ok_or_else(|| {
        CowshedError::environment_missing(
            "PATH is not set",
            "run from a shell with sccache on PATH",
        )
    })?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join("sccache");
        if candidate.is_file() {
            return fs::canonicalize(&candidate).map_err(|error| {
                CowshedError::environment_missing(
                    format!("could not resolve {}: {error}", candidate.display()),
                    "repair the sccache installation and retry",
                )
            });
        }
    }
    Err(CowshedError::environment_missing(
        "sccache is not on PATH",
        "install sccache (devenv/nix) and run cowshed sccache start from that shell",
    ))
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
