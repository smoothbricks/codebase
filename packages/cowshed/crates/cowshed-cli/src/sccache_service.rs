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
    inspect_install_state, launchd_error, output_error,
};
use crate::launchd::{
    ExistingPlist, InstallState, LaunchAgentSpec, LaunchdExecutor, LaunchdServiceStatus,
    NativeFilesystem, NativeLaunchctlCommand, plan_install, plan_remove,
};
use crate::output::Output;
use cowshed_core::api::{EmptyResult, SccacheStatus};
use cowshed_core::sandbox::sccache_server_socket;
use cowshed_core::{CowshedError, Result, validate_existing_host_storage};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

const START_DEADLINE: Duration = Duration::from_secs(10);
const START_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
        SccacheCommand::Start => {
            let status = start_service().await?;
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

async fn start_service() -> Result<SccacheStatus> {
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let cache_directory = storage.caches().join("sccache");
    fs::create_dir_all(&cache_directory).map_err(|error| {
        CowshedError::internal(format!(
            "could not create {}: {error}",
            cache_directory.display()
        ))
    })?;
    let socket = sccache_server_socket(&home);
    let executable = resolve_sccache_executable()?;
    let spec = LaunchAgentSpec::sccache(&home, &executable, &socket, &cache_directory)
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
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    executor.execute_install(&plan).map_err(launchd_error)?;
    activate_launch_agent(&mut executor, uid, &spec)?;

    let deadline = tokio::time::Instant::now() + START_DEADLINE;
    loop {
        if socket_answers(&socket).await {
            return Ok(SccacheStatus {
                installed: true,
                running: true,
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
    Ok(SccacheStatus {
        installed,
        running,
        socket,
    })
}

/// A spec good for launchctl control targets (label and plist path).
///
/// Stop and status must work after the sccache binary has left `PATH` — a
/// devenv update or removal must not strand the agent — so the executable
/// recorded here falls back to the cowshed binary. Only `start` writes a
/// plist, and `start` always resolves the real sccache executable.
fn control_spec(home: &Path) -> Result<LaunchAgentSpec> {
    let socket = sccache_server_socket(home);
    let cache_directory = home.join(".cowshed/caches/sccache");
    let executable = resolve_sccache_executable().or_else(|_| {
        fs::canonicalize(std::env::current_exe().map_err(|error| {
            CowshedError::internal(format!(
                "could not identify the cowshed executable: {error}"
            ))
        })?)
        .map_err(|error| {
            CowshedError::internal(format!("could not resolve the cowshed executable: {error}"))
        })
    })?;
    LaunchAgentSpec::sccache(home, &executable, &socket, &cache_directory).map_err(launchd_error)
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
