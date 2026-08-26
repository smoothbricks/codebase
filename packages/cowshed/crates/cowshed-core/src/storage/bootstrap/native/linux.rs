//! Linux has no APFS host adapter yet. Public entrypoints fail closed with
//! `UnsupportedPlatform` instead of compiling macOS diskutil and Authorization Services.

use std::path::Path;

use super::super::{
    ApfsVolumeProvision, BootstrapHost, BootstrapPlan, HostCommand, HostCommandOutput, HostError,
    MountpointState, ValidatedHostStorage,
};
use super::shared::{
    HostSetupPlan, HostSetupReport, HostUninstallPlan, NativeBootstrapError, NativeBootstrapMode,
    SystemBootstrapHost, UninstallReport, existing_host_storage_error, platform_host_error,
    setup_execution_error,
};
use crate::storage::fstab::FstabPin;

impl BootstrapHost for SystemBootstrapHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        Err(platform_host_error("ZFS bootstrap delegation"))
    }

    fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
        Err(platform_host_error("mountpoint inspection"))
    }

    fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
        Err(platform_host_error("directory creation"))
    }

    fn reclaim_mountpoint(&self, _path: &Path) -> Result<(), HostError> {
        Err(platform_host_error("mountpoint reclaim"))
    }

    fn run_command(&self, _command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        Err(platform_host_error("host command execution"))
    }

    fn provision_apfs_volumes(
        &self,
        _container: &str,
        _volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        Err(platform_host_error("APFS volume creation authorization"))
    }

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        Err(platform_host_error("atomic marker write"))
    }

    fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
        Err(platform_host_error("fstab installation"))
    }
}

pub async fn bootstrap_system_storage(
    _project_root: &Path,
    _home: &Path,
    _mode: NativeBootstrapMode,
) -> Result<BootstrapPlan, NativeBootstrapError> {
    Err(NativeBootstrapError::UnsupportedPlatform(
        std::env::consts::OS,
    ))
}

pub async fn validate_existing_host_storage(_home: &Path) -> crate::Result<ValidatedHostStorage> {
    Err(existing_host_storage_error(
        NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
    ))
}

pub async fn plan_host_setup(_home: &Path) -> crate::Result<HostSetupPlan> {
    Err(existing_host_storage_error(
        NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
    ))
}

pub async fn execute_host_setup(_home: &Path) -> crate::Result<HostSetupReport> {
    Err(setup_execution_error(
        NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        "cowshed setup",
    ))
}

pub async fn plan_host_uninstall(_home: &Path) -> crate::Result<HostUninstallPlan> {
    Err(existing_host_storage_error(
        NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
    ))
}

pub async fn execute_host_uninstall(_home: &Path) -> crate::Result<UninstallReport> {
    Err(setup_execution_error(
        NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        "cowshed setup --uninstall",
    ))
}
