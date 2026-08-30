use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::super::{
    ApfsVolumeProvision, BlockingLane, BootstrapExecutionError, BootstrapHost, BootstrapPlan,
    DISKUTIL, HostCommand, HostCommandFailure, HostError, HostOperation, PlanError, SelectionError,
    VolumeRole, execute_bootstrap,
};
use crate::error::CowshedError;

/// Whether native bootstrap may apply its mutating host plan.
///
/// `ExistingOnly` is the safe capability for ordinary commands and background services. It may
/// gather evidence, reclaim a launchd StandardErrorPath stub, and remount already-created cowshed
/// volumes. It cannot create volumes, write markers, or open an authorization prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBootstrapMode {
    Provision,
    ExistingOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostSetupPlan {
    pub actions: Vec<HostAction>,
    /// The planner's own per-volume classification at planning time. Carried on the plan so a
    /// consumer (the doctor) renders the state the planner observed instead of
    /// reverse-engineering it from the action list — an action-derived guess collapses states
    /// the planner distinguishes (a volume in a foreign container plans the same `MountExisting`
    /// as a merely detached one).
    pub volumes: Vec<VolumeOutcome>,
    pub requires_authorization: bool,
    pub non_destructive: bool,
}

impl HostSetupPlan {
    pub fn new(
        actions: Vec<HostAction>,
        volumes: Vec<VolumeOutcome>,
        requires_authorization: bool,
    ) -> Self {
        let non_destructive = actions.iter().all(HostAction::is_non_destructive);
        Self {
            actions,
            volumes,
            requires_authorization,
            non_destructive,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum HostAction {
    CreateVolume {
        name: String,
        container: String,
        mount_at: PathBuf,
    },
    MountExisting {
        name: String,
        uuid: String,
        size_bytes: u64,
        mount_at: PathBuf,
    },
    RepairMounted {
        name: String,
        uuid: String,
        size_bytes: u64,
        mounted_at: PathBuf,
        mount_at: PathBuf,
    },
    EncryptVolume {
        name: String,
        uuid: String,
        size_bytes: u64,
    },
    PinFstab {
        uuid: String,
        mount_at: PathBuf,
    },
    ReclaimStubs {
        paths: Vec<PathBuf>,
    },
    InstallMountService {
        label: String,
    },
}

impl HostAction {
    fn is_non_destructive(&self) -> bool {
        !matches!(self, Self::CreateVolume { .. })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostSetupReport {
    pub action_outcomes: Vec<HostActionOutcome>,
    pub volumes: Vec<VolumeOutcome>,
    pub fstab: FstabOutcome,
    pub authorized: bool,
}

impl HostSetupReport {
    pub fn failure(&self) -> Option<&CowshedError> {
        self.action_outcomes.iter().find_map(|outcome| {
            let HostActionResult::Failed { error } = &outcome.outcome else {
                return None;
            };
            Some(error)
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostActionOutcome {
    pub action: HostAction,
    pub outcome: HostActionResult,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum HostActionResult {
    Done,
    Failed { error: CowshedError },
    Skipped,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeOutcome {
    pub name: String,
    pub role: VolumeRole,
    pub state_before: VolumeState,
    pub action: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum VolumeState {
    Absent,
    MountedValid,
    MountedIncomplete,
    Detached,
    MisMounted {
        mounted_at: PathBuf,
    },
    FoundElsewhere {
        container: String,
        device: String,
        mounted_at: Option<PathBuf>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FstabOutcome {
    Pinned,
    AlreadyCurrent,
    Skipped(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostUninstallPlan {
    pub pins_to_remove: Vec<String>,
    pub requires_authorization: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UninstallReport {
    pub fstab: UninstallFstabOutcome,
    pub services: Vec<UninstallServiceOutcome>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UninstallServiceOutcome {
    pub what: String,
    pub outcome: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UninstallFstabOutcome {
    Removed,
    AlreadyClean,
}

/// Stateless production host adapter. Platform modules supply the `BootstrapHost` impl.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemBootstrapHost;

pub(crate) fn existing_host_storage_error(error: NativeBootstrapError) -> CowshedError {
    CowshedError::environment_missing(error.to_string(), "cowshed setup")
}

pub(crate) fn setup_execution_error(
    error: NativeBootstrapError,
    hint: &'static str,
) -> CowshedError {
    let authorization_denied = match &error {
        NativeBootstrapError::Host(host) => host.is_authorization_denied(),
        NativeBootstrapError::Execution(BootstrapExecutionError::Host(host)) => {
            host.is_authorization_denied()
        }
        _ => false,
    };
    if authorization_denied {
        CowshedError::sandbox_denied("authorization was declined; nothing was changed", hint)
    } else {
        existing_host_storage_error(error)
    }
}

/// Apply a previously planned native bootstrap with an explicit provisioning capability.
///
/// This boundary is public so alternate foreground/background hosts can share the same fail-closed
/// policy. `ExistingOnly` rejects the complete plan before dispatch when setup is required.
pub async fn execute_native_bootstrap_plan<H, L>(
    plan: &BootstrapPlan,
    mode: NativeBootstrapMode,
    host: Arc<H>,
    lane: &L,
) -> Result<(), NativeBootstrapError>
where
    H: BootstrapHost + 'static,
    L: BlockingLane,
{
    if mode == NativeBootstrapMode::ExistingOnly {
        let actions = mutating_setup_actions(plan);
        if !actions.is_empty() {
            return Err(NativeBootstrapError::StorageSetupRequired {
                actions,
                hint: "cowshed setup",
            });
        }
    }
    execute_bootstrap(plan, host, lane)
        .await
        .map_err(NativeBootstrapError::Execution)
}

/// One classification of a host operation. ExistingOnly and read-only validation
/// keep distinct policies; they both match this enum so a new `HostOperation`
/// variant cannot compile in one table and vanish from the other.
enum HostOperationClass<'a> {
    GuardMountpoint,
    VerifyZfsDelegation {
        required_root: &'a str,
    },
    EnsureDirectory(&'a Path),
    ReclaimMountpoint(&'a Path),
    MountApfsVolume {
        mountpoint: &'a Path,
    },
    RunCommand {
        command: &'a HostCommand,
        unprivileged_remount: bool,
    },
    ProvisionApfsVolumes {
        volumes: &'a [ApfsVolumeProvision],
    },
    WriteMarkerAtomic {
        path: &'a Path,
    },
    PinVolumesInFstab,
    ReportVolumeIssue {
        detail: &'a str,
    },
}

fn classify_operation(operation: &HostOperation) -> HostOperationClass<'_> {
    match operation {
        HostOperation::GuardMountpoint { .. } => HostOperationClass::GuardMountpoint,
        HostOperation::VerifyZfsDelegation { required_root, .. } => {
            HostOperationClass::VerifyZfsDelegation { required_root }
        }
        HostOperation::EnsureDirectory(path) => HostOperationClass::EnsureDirectory(path),
        HostOperation::ReclaimMountpoint(path) => HostOperationClass::ReclaimMountpoint(path),
        HostOperation::MountApfsVolume { mountpoint, .. } => {
            HostOperationClass::MountApfsVolume { mountpoint }
        }
        HostOperation::RunCommand(command) => HostOperationClass::RunCommand {
            command,
            unprivileged_remount: is_unprivileged_apfs_remount(command),
        },
        HostOperation::ProvisionApfsVolumes { volumes, .. } => {
            HostOperationClass::ProvisionApfsVolumes { volumes }
        }
        HostOperation::WriteMarkerAtomic { path, .. } => {
            HostOperationClass::WriteMarkerAtomic { path }
        }
        HostOperation::PinVolumesInFstab { .. } => HostOperationClass::PinVolumesInFstab,
        HostOperation::ReportVolumeIssue { detail, .. } => {
            HostOperationClass::ReportVolumeIssue { detail }
        }
    }
}

fn provision_volumes_action(volumes: &[ApfsVolumeProvision]) -> String {
    format!(
        "create APFS volumes {}",
        volumes
            .iter()
            .map(ApfsVolumeProvision::name)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn write_marker_action(path: &Path) -> String {
    format!("write volume marker {}", path.display())
}

const PIN_FSTAB_ACTION: &str = "pin cowshed APFS volumes in /etc/fstab";

pub(crate) fn mutating_setup_actions(plan: &BootstrapPlan) -> Vec<String> {
    plan.operations()
        .iter()
        .filter_map(|operation| match classify_operation(operation) {
            HostOperationClass::GuardMountpoint
            | HostOperationClass::VerifyZfsDelegation { .. }
            | HostOperationClass::EnsureDirectory(_)
            | HostOperationClass::ReclaimMountpoint(_)
            | HostOperationClass::MountApfsVolume { .. }
            | HostOperationClass::ReportVolumeIssue { .. }
            | HostOperationClass::RunCommand {
                unprivileged_remount: true,
                ..
            } => None,
            HostOperationClass::RunCommand {
                unprivileged_remount: false,
                ..
            } => Some("run privileged host command".to_owned()),
            HostOperationClass::ProvisionApfsVolumes { volumes } => {
                Some(provision_volumes_action(volumes))
            }
            HostOperationClass::WriteMarkerAtomic { path } => Some(write_marker_action(path)),
            HostOperationClass::PinVolumesInFstab => Some(PIN_FSTAB_ACTION.to_owned()),
        })
        .collect()
}

pub(crate) fn read_only_validation_actions(plan: &BootstrapPlan) -> Vec<String> {
    plan.operations()
        .iter()
        .filter_map(|operation| match classify_operation(operation) {
            HostOperationClass::GuardMountpoint => None,
            HostOperationClass::VerifyZfsDelegation { required_root } => {
                Some(format!("verify delegated ZFS root {required_root}"))
            }
            HostOperationClass::EnsureDirectory(path) => {
                Some(format!("create mountpoint {}", path.display()))
            }
            HostOperationClass::ReclaimMountpoint(path) => {
                Some(format!("reclaim mountpoint {}", path.display()))
            }
            HostOperationClass::MountApfsVolume { mountpoint } => {
                Some(format!("mount APFS volume at {}", mountpoint.display()))
            }
            HostOperationClass::RunCommand { command, .. } => Some(format!(
                "run {} {}",
                command.program(),
                command.args().join(" ")
            )),
            HostOperationClass::ProvisionApfsVolumes { volumes } => {
                Some(provision_volumes_action(volumes))
            }
            HostOperationClass::WriteMarkerAtomic { path } => Some(write_marker_action(path)),
            HostOperationClass::PinVolumesInFstab => Some(PIN_FSTAB_ACTION.to_owned()),
            HostOperationClass::ReportVolumeIssue { detail } => Some(detail.to_owned()),
        })
        .collect()
}

fn is_unprivileged_apfs_remount(command: &HostCommand) -> bool {
    if command.program() != DISKUTIL {
        return false;
    }
    let args = command.args();
    args.first().is_some_and(|argument| argument == "unmount")
        || args.starts_with(&[
            "mount".to_owned(),
            "-nobrowse".to_owned(),
            "-mountPoint".to_owned(),
        ])
}

#[derive(Debug, Error)]
pub enum NativeBootstrapError {
    #[error("native storage bootstrap is unsupported on {0}")]
    UnsupportedPlatform(&'static str),
    #[error("path must be absolute and normalized: {0:?}")]
    NonCanonicalPath(PathBuf),
    #[error("cannot inspect filesystem for {path:?}: {source}")]
    StatFs { path: PathBuf, source: io::Error },
    #[error("project root {path:?} is on unsupported filesystem {fs_type:?}")]
    UnsupportedFilesystem { path: PathBuf, fs_type: String },
    #[error("APFS kernel mount source is not an exact /dev/disk identifier: {0:?}")]
    InvalidMountSource(PathBuf),
    #[error("native bootstrap host operation failed: {0}")]
    Host(#[from] HostError),
    #[error("{0}")]
    CommandFailed(HostCommandFailure),
    #[error("diskutil APFS inventory is malformed: {0}")]
    MalformedPlist(String),
    #[error("kernel device {device:?} belongs to no APFS container in diskutil evidence")]
    ContainerNotFound { device: String },
    #[error("kernel device {device:?} ambiguously belongs to {matches} APFS containers")]
    AmbiguousContainer { device: String, matches: usize },
    #[error("APFS container {container:?} has {matches} volumes named {name:?}")]
    AmbiguousVolume {
        container: String,
        name: &'static str,
        matches: usize,
    },
    #[error(
        "FileVault volume {name:?} ({uuid}) has no usable System.keychain password; refusing to replace its unlock credential"
    )]
    MissingVolumeKeychain { name: &'static str, uuid: String },
    #[error("APFS volume {identifier:?} has invalid mountpoint evidence {mountpoint:?}")]
    InvalidVolumeMountpoint {
        identifier: String,
        mountpoint: Option<PathBuf>,
    },
    #[error(
        "mountpoint {path:?} contains data but is not the exact expected APFS mount; run cowshed setup"
    )]
    MaskedMountpoint { path: PathBuf },
    #[error("mountpoint {path:?} conflicts with diskutil evidence for {identifier:?}")]
    MountEvidenceMismatch { path: PathBuf, identifier: String },
    #[error("mounted APFS marker at {path:?} is invalid: {message}")]
    InvalidMountedMarker { path: PathBuf, message: String },
    #[error("cowshed storage setup is required ({actions:?}); {hint}")]
    StorageSetupRequired {
        actions: Vec<String>,
        hint: &'static str,
    },
    #[error("native bootstrap evidence blocking lane closed without a result")]
    EvidenceLaneClosed,
    #[error(transparent)]
    Selection(#[from] SelectionError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Execution(#[from] BootstrapExecutionError),
}

pub(crate) fn platform_host_error(operation: &str) -> HostError {
    HostError::new(format!(
        "{operation} is unsupported on {}",
        std::env::consts::OS
    ))
}
