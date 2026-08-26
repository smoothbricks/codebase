//! Native host storage bootstrap.
//!
//! Shared types and fail-closed policy live in [`shared`]. Platform adapters live in
//! [`macos`] and [`linux`] so host I/O is not sprinkled with `cfg` tags.

mod shared;

#[cfg(not(target_os = "macos"))]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

pub use shared::{
    FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan, HostSetupReport,
    HostUninstallPlan, NativeBootstrapError, NativeBootstrapMode, SystemBootstrapHost,
    UninstallFstabOutcome, UninstallReport, UninstallServiceOutcome, VolumeOutcome, VolumeState,
    execute_native_bootstrap_plan,
};

#[cfg(target_os = "macos")]
pub use macos::{
    bootstrap_system_storage, execute_host_setup, execute_host_uninstall, plan_host_setup,
    plan_host_uninstall, validate_existing_host_storage,
};

#[cfg(not(target_os = "macos"))]
pub use linux::{
    bootstrap_system_storage, execute_host_setup, execute_host_uninstall, plan_host_setup,
    plan_host_uninstall, validate_existing_host_storage,
};
