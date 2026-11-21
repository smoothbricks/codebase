//! Native host storage bootstrap.
//!
//! Shared types and fail-closed policy live in [`shared`]. Each platform's adapter is its own
//! file behind one set of entrypoints, so host I/O is not sprinkled with `cfg` tags: `macos`
//! provisions APFS volumes under one Authorization Services session, `linux` a directory store
//! or the datasets of an existing ZFS root.

mod shared;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub use shared::{
    CachesVolume, FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan,
    HostSetupReport, HostUninstallPlan, NativeBootstrapError, NativeBootstrapMode,
    SystemBootstrapHost, UninstallFstabOutcome, UninstallReport, UninstallServiceOutcome,
    VolumeOutcome, VolumeState, execute_native_bootstrap_plan,
};

pub use platform::{
    bootstrap_system_storage, execute_host_setup, execute_host_uninstall,
    execute_mount_root_provisioning, plan_host_setup, plan_host_uninstall, provisioned_build_cache,
    validate_existing_host_storage,
};
