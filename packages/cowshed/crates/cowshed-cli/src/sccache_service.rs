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
    activate_launch_agent, canonical_home, effective_uid, inspect_install_state, launchd_error,
    output_error, remove_launch_agent,
};
use crate::launchd::{
    ExistingPlist, InstallState, LaunchAgentSpec, LaunchAgentTarget, LaunchdExecutor,
    LaunchdServiceStatus, NativeFilesystem, NativeLaunchctlCommand, SCCACHE_LABEL,
    StoreBackedProgram, kickstart_hint, plan_install,
};
use crate::output::Output;
use crate::sccache_nix;
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
///
/// The program comes from the nix GC root `cowshed setup --sccache` registered, never from `PATH`.
/// launchd hands a LaunchAgent none of the user's `PATH`, so a name would only ever have resolved
/// because a person happened to run this from a shell that had sccache in it — which is how a host
/// with sccache installed all along reported "sccache is not on PATH" on every boot, and how a
/// devenv upgrade could silently substitute a different build under a running server.
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
    let program = installed_program(&home)?;
    let spec = LaunchAgentSpec::sccache(
        &program,
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
    // `plan_install` compares the desired plist bytes against the installed ones, and the program
    // path is in those bytes. Because the plist names the resolved store path, a rebuild that moved
    // sccache is a changed plist, which forces the bootout-then-bootstrap below: the running server
    // cannot outlive the build it was started from.
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
    activate_launch_agent(&mut executor, uid, spec.target(), written)?;

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
    remove_launch_agent(&control_target(&home)?)?;
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
    let target = control_target(&home)?;
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let installed = match executor
        .execute_status(&crate::launchd::ControlPlan::print(effective_uid(), &target))
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

    let output = tokio::process::Command::new(installed_program(home).ok()?.program())
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

/// The launchctl control target for this agent: its label, and the plist that defines it.
///
/// A target rather than a whole definition, because `stop` and `status` must reach the agent
/// `start` installed even when the program it names is gone — a store path whose GC root was
/// deleted and then collected is exactly that state, and it is one `doctor` has to be able to
/// report rather than one that makes the control verbs unconstructible.
pub(crate) fn control_target(home: &Path) -> Result<LaunchAgentTarget> {
    LaunchAgentTarget::new(home, SCCACHE_LABEL).map_err(launchd_error)
}

/// The sccache agent's control target, its nix GC root, and its socket — everything
/// `setup --uninstall` removes for this service.
///
/// Returned together because the order matters: the agent is deactivated before the root is
/// released. Removing the root first would leave a `KeepAlive` agent respawning a store path the
/// next collection is entitled to delete.
pub(crate) fn sccache_launch_agent(home: &Path) -> Result<(LaunchAgentTarget, PathBuf, PathBuf)> {
    Ok((
        control_target(home)?,
        sccache_nix::gc_root(home),
        sccache_server_socket(),
    ))
}

/// The sccache this host installed, named by its own nix GC root.
///
/// One lookup, one authority: the root is a symlink into the store, so "which sccache is installed"
/// and "which sccache is pinned" cannot give two answers. There is deliberately no `PATH` fallback
/// and no recorded-path file — a name resolved per-process is how an unpatched binary from some
/// other profile ended up serving a patched client, and a recorded path is a mutable pointer where
/// a store path is an identity.
pub(crate) fn installed_program(home: &Path) -> Result<StoreBackedProgram> {
    sccache_nix::rooted_program(home, &sccache_nix::gc_root(home))
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
    use cowshed_core::storage::bootstrap::STORE_ROOT;
    use std::os::unix::fs::PermissionsExt as _;

    /// A canonical absolute home, as every host-path type requires.
    fn scratch_home(label: &str) -> PathBuf {
        let home = std::env::temp_dir().join(format!(
            "cowshed-cli-sccache-{label}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&home);
        fs::create_dir_all(home.join("Library/Application Support/dev.cowshed"))
            .expect("scratch support directory");
        home.canonicalize().expect("canonical scratch home")
    }

    /// A store path shaped like nix's output, with the GC root symlink pointing at it.
    fn rooted_store_path(home: &Path) -> PathBuf {
        let store = home.join("nix-store").join("abc123-sccache-0.17.0-cowshed");
        let binary = store.join("bin").join("sccache");
        fs::create_dir_all(binary.parent().expect("bin")).expect("store bin");
        fs::write(&binary, b"#!/bin/sh\nexit 0\n").expect("store binary");
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o555)).expect("store mode");
        let root = sccache_nix::gc_root(home);
        fs::create_dir_all(root.parent().expect("root parent")).expect("root parent");
        std::os::unix::fs::symlink(&store, &root).expect("gc root symlink");
        store
    }

    /// The whole resolution: one symlink, and the program is *inside* the store path it pins. Not
    /// a `PATH` search and not a recorded pointer — those are how an unpatched binary from another
    /// profile ended up serving a patched client.
    #[test]
    fn the_installed_program_is_the_binary_inside_the_pinned_store_path() {
        let home = scratch_home("rooted");
        let store = rooted_store_path(&home);

        let program = installed_program(&home).expect("the rooted sccache resolves");
        assert_eq!(program.program(), store.join("bin").join("sccache"));
        assert_eq!(program.gc_root(), sccache_nix::gc_root(&home));
        assert!(
            program.program().starts_with(&store),
            "the program must live inside the store path the root pins; got {}",
            program.program().display()
        );

        // The plist launchd reads names that store path, not the out-link: a rebuild is therefore a
        // changed plist rather than a silently substituted program.
        let spec = LaunchAgentSpec::sccache(
            &program,
            Path::new("/private/cowshed/store/sccache.sock"),
            Path::new("/private/cowshed/caches/sccache"),
            MINIMUM_CAPACITY,
            Path::new(STORE_ROOT),
        )
        .expect("valid spec");
        assert_eq!(spec.program_arguments().next(), program.program().to_str());
        assert_ne!(
            spec.program_arguments().next(),
            program.gc_root().to_str(),
            "naming the out-link would let a later build repoint it under a loaded agent"
        );

        let _ = fs::remove_dir_all(&home);
    }

    /// sccache is opt-in, so "no root" is the common state and it must say what to run — never
    /// blame `PATH`, which no longer has anything to do with it.
    #[test]
    fn a_host_with_no_gc_root_is_told_to_opt_in() {
        let home = scratch_home("unrooted");

        let error = installed_program(&home).expect_err("no root means no installed sccache");
        assert_eq!(error.code.as_str(), "environment-missing");
        assert!(
            error
                .message
                .contains(&sccache_nix::gc_root(&home).display().to_string()),
            "the error must name the root it looked for; got {}",
            error.message
        );
        assert_eq!(error.hint, "cowshed setup --sccache");

        let _ = fs::remove_dir_all(&home);
    }

    /// The state that used to be unconstructible: `stop` and `status` must still reach the agent
    /// when the program it names is gone, because that is exactly the state worth reporting.
    #[test]
    fn the_control_target_needs_no_program_at_all() {
        let home = scratch_home("control");

        let target = control_target(&home).expect("a control target needs only a home and a label");
        assert_eq!(target.label(), SCCACHE_LABEL);
        assert_eq!(
            target.plist_path(),
            home.join("Library/LaunchAgents/dev.cowshed.sccache.plist")
        );

        // And teardown names the root rather than a copied binary: releasing the root is what lets
        // the store path be collected.
        let (agent, root, socket) = sccache_launch_agent(&home).expect("teardown artifacts");
        assert_eq!(agent.label(), SCCACHE_LABEL);
        assert_eq!(root, sccache_nix::gc_root(&home));
        assert_eq!(socket, sccache_server_socket());

        let _ = fs::remove_dir_all(&home);
    }
}
