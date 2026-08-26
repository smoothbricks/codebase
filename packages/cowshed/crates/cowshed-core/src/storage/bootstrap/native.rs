#[cfg(unix)]
use std::ffi::{CStr, CString};
#[cfg(target_os = "macos")]
use std::ffi::{OsString, c_void};
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::fs::File;
use std::io;
#[cfg(target_os = "macos")]
use std::io::Read;
#[cfg(unix)]
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use crate::error::CowshedError;
use plist::{Dictionary, Value};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "macos")]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot;
#[cfg(unix)]
use uuid::Uuid;

use super::{
    APFS_CACHES_VOLUME, APFS_STORE_VOLUME, ApfsVolumeProvision, BlockingLane, BootstrapEvidence,
    BootstrapExecutionError, BootstrapHost, BootstrapPlan, DISKUTIL, ExistingStorage, HostCommand,
    HostCommandFailure, HostCommandOutput, HostError, HostOperation, MountpointState, PlanError,
    SelectionError, StatFsEvidence, SubstrateKind, TokioBlockingLane, ValidatedHostStorage,
    VolumeRole, execute_bootstrap, plan_bootstrap, require_mounted_marker, select_substrate,
};
use crate::storage::fstab::{FstabPin, build_fstab};
#[cfg(target_os = "macos")]
use super::{ApfsProvisionKind, VOLUME_MARKER_FILE, VolumeMarker};

#[cfg(unix)]
const MARKER_MODE: libc::mode_t = 0o600;
#[cfg(target_os = "macos")]
const CHOWN: &str = "/usr/sbin/chown";
#[cfg(target_os = "macos")]
const AUTHORIZED_OUTPUT_LIMIT: usize = 1024 * 1024;
#[cfg(target_os = "macos")]
const FSTAB: &str = "/etc/fstab";
#[cfg(target_os = "macos")]
const INSTALL: &str = "/usr/bin/install";

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
    pub actions: Vec<String>,
    pub requires_authorization: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HostSetupReport {
    pub volumes: Vec<VolumeOutcome>,
    pub fstab: FstabOutcome,
    pub authorized: bool,
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
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UninstallFstabOutcome {
    Removed,
    AlreadyClean,
}

/// Stateless production host adapter for trusted storage bootstrap operations.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemBootstrapHost;

impl BootstrapHost for SystemBootstrapHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        Err(platform_host_error("ZFS bootstrap delegation"))
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        inspect_system_mountpoint(path)
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        ensure_supported_host()?;
        require_host_canonical(path)?;
        fs::create_dir_all(path).map_err(|source| host_io_error("create directory", path, source))
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        reclaim_system_mountpoint(path)
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        ensure_supported_host()?;
        run_command_with(command, |program, args| {
            Command::new(program).args(args).output()
        })
    }

    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        ensure_supported_host()?;
        #[cfg(target_os = "macos")]
        {
            let uid = unsafe { libc::getuid() };
            let gid = unsafe { libc::getgid() };
            provision_apfs_volumes_with(
                container,
                volumes,
                uid,
                gid,
                MacAuthorizationSession::acquire,
                &SystemApfsProvisionIo,
            )
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (container, volumes);
            Err(platform_host_error("APFS provisioning authorization"))
        }
    }

    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        write_marker_atomic(path, contents)
    }
    fn pin_volumes_in_fstab(&self, pins: &[FstabPin]) -> Result<(), HostError> {
        ensure_supported_host()?;
        #[cfg(target_os = "macos")]
        {
            let mut session = MacAuthorizationSession::acquire()?;
            pin_volumes_in_fstab_with(&mut session, pins).map(|_| ())
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = pins;
            Err(platform_host_error("fstab installation"))
        }
    }
}

#[cfg(target_os = "macos")]
struct AuthorizedBootstrapHost {
    session: Mutex<MacAuthorizationSession>,
}

#[cfg(target_os = "macos")]
impl AuthorizedBootstrapHost {
    fn new(session: MacAuthorizationSession) -> Self {
        Self {
            session: Mutex::new(session),
        }
    }

    fn session(&self) -> Result<std::sync::MutexGuard<'_, MacAuthorizationSession>, HostError> {
        self.session
            .lock()
            .map_err(|_| HostError::new("authorization session lock is poisoned"))
    }
}

#[cfg(target_os = "macos")]
impl BootstrapHost for AuthorizedBootstrapHost {
    fn verify_zfs_delegation(&self, pool: &str, required_root: &str) -> Result<(), HostError> {
        SystemBootstrapHost.verify_zfs_delegation(pool, required_root)
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        SystemBootstrapHost.inspect_mountpoint(path)
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        SystemBootstrapHost.create_dir_all(path)
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        SystemBootstrapHost.reclaim_mountpoint(path)
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        if command.program() == DISKUTIL {
            return self.session()?.execute(command);
        }
        SystemBootstrapHost.run_command(command)
    }

    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        provision_apfs_volumes_in_session(
            container,
            volumes,
            uid,
            gid,
            &mut *self.session()?,
            &SystemApfsProvisionIo,
        )
    }

    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        SystemBootstrapHost.write_file_atomic(path, contents)
    }

    fn pin_volumes_in_fstab(&self, pins: &[FstabPin]) -> Result<(), HostError> {
        pin_volumes_in_fstab_with(&mut *self.session()?, pins).map(|_| ())
    }
}


/// Gather authoritative APFS evidence, plan bootstrap purely, then execute it according to the
/// explicit capability mode.
///
/// The returned plan is the exact plan that completed successfully. In `ExistingOnly`, a plan
/// containing any mutating operation is rejected before the execution lane is dispatched.
pub async fn bootstrap_system_storage(
    project_root: &Path,
    home: &Path,
    mode: NativeBootstrapMode,
) -> Result<BootstrapPlan, NativeBootstrapError> {
    if !cfg!(target_os = "macos") {
        return Err(NativeBootstrapError::UnsupportedPlatform(
            std::env::consts::OS,
        ));
    }

    let host = Arc::new(SystemBootstrapHost);
    let lane = TokioBlockingLane;
    let project_root = project_root.to_owned();
    let home = home.to_owned();
    let gather_home = home.clone();
    let gather_host = Arc::clone(&host);
    let (sender, receiver) = oneshot::channel();
    lane.dispatch(Box::new(move || {
        let mut source = SystemEvidenceSource {
            host: gather_host.as_ref(),
        };
        let result = plan_native_bootstrap(&mut source, &project_root, &gather_home);
        sender.send(result).map_err(|_| {
            BootstrapExecutionError::BlockingLane(
                "native bootstrap evidence receiver closed".to_owned(),
            )
        })
    }))
    .await
    .map_err(NativeBootstrapError::Execution)?;

    let plan = receiver
        .await
        .map_err(|_| NativeBootstrapError::EvidenceLaneClosed)??;
    execute_native_bootstrap_plan(&plan, mode, host, &lane).await?;
    Ok(plan)
}
/// Validate the invoking user's pre-existing, machine-global host storage.
///
/// The home directory is the sole filesystem-selection anchor. This boundary gathers the same
/// exact APFS inventory, ownership, mount-flag, and marker evidence as bootstrap. Existing-only
/// execution may reclaim a launchd stub and remount already-created volumes; volume creation and
/// authorization remain provision-only.
pub async fn validate_existing_host_storage(home: &Path) -> crate::Result<ValidatedHostStorage> {
    if !cfg!(target_os = "macos") {
        return Err(existing_host_storage_error(
            NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        ));
    }

    let host = Arc::new(SystemBootstrapHost);
    let lane = TokioBlockingLane;
    let home = home.to_owned();
    let gather_home = home.clone();
    let gather_host = Arc::clone(&host);
    let (sender, receiver) = oneshot::channel();
    lane.dispatch(Box::new(move || {
        let mut source = SystemEvidenceSource {
            host: gather_host.as_ref(),
        };
        let result = plan_existing_host_storage(&mut source, &gather_home);
        sender.send(result).map_err(|_| {
            BootstrapExecutionError::BlockingLane(
                "existing host-storage evidence receiver closed".to_owned(),
            )
        })
    }))
    .await
    .map_err(NativeBootstrapError::Execution)
    .map_err(existing_host_storage_error)?;

    let plan = receiver
        .await
        .map_err(|_| NativeBootstrapError::EvidenceLaneClosed)
        .and_then(|result| result)
        .map_err(existing_host_storage_error)?;
    validate_existing_plan(&plan, host, &lane).await
}

async fn validate_existing_plan<H, L>(
    plan: &BootstrapPlan,
    host: Arc<H>,
    lane: &L,
) -> crate::Result<ValidatedHostStorage>
where
    H: BootstrapHost + 'static,
    L: BlockingLane,
{
    execute_native_bootstrap_plan(plan, NativeBootstrapMode::ExistingOnly, host, lane)
        .await
        .map_err(existing_host_storage_error)?;
    Ok(ValidatedHostStorage::new(plan.roots().clone()))
}

#[derive(Clone, Debug)]
enum PlannedFstab {
    AlreadyCurrent,
    NeedsPin(Vec<FstabPin>),
    Deferred(String),
}

#[derive(Clone, Debug)]
struct SetupSnapshot {
    plan: BootstrapPlan,
    volumes: Vec<VolumeOutcome>,
    fstab: PlannedFstab,
}

fn prepare_setup_snapshot(
    source: &mut impl EvidenceSource,
    home: &Path,
    existing_fstab: &str,
) -> Result<SetupSnapshot, NativeBootstrapError> {
    let gathered = gather_existing_apfs_evidence(source, home)?;
    let selected = select_substrate(gathered.statfs, None)?;
    let mut plan = plan_bootstrap(selected, home, gathered.bootstrap)?;
    let pins = gathered
        .volumes
        .iter()
        .filter_map(|volume| match &volume.storage {
            ExistingStorage::Absent | ExistingStorage::FoundElsewhere { .. } => None,
            _ => volume.volume_uuid.as_ref().map(|volume_uuid| FstabPin {
                volume_uuid: volume_uuid.clone(),
                mountpoint: volume.mountpoint.clone(),
                label: volume.name.to_owned(),
            }),
        })
        .collect::<Vec<_>>();
    let fstab = if pins.len() == gathered.volumes.len() {
        let desired = build_fstab(existing_fstab, &pins)
            .map_err(|error| NativeBootstrapError::Host(HostError::new(error.to_string())))?;
        if desired.as_bytes() == existing_fstab.as_bytes() {
            PlannedFstab::AlreadyCurrent
        } else {
            plan.push_operation(HostOperation::PinVolumesInFstab { pins: pins.clone() });
            PlannedFstab::NeedsPin(pins)
        }
    } else {
        let reason = if gathered
            .volumes
            .iter()
            .any(|volume| matches!(volume.storage, ExistingStorage::FoundElsewhere { .. }))
        {
            "one or more cowshed volumes were found outside the home APFS container".to_owned()
        } else {
            "volume UUIDs will be available after provisioning".to_owned()
        };
        PlannedFstab::Deferred(reason)
    };
    let volumes = gathered
        .volumes
        .iter()
        .map(|volume| VolumeOutcome {
            name: volume.name.to_owned(),
            role: volume.role,
            state_before: volume_state(&volume.storage),
            action: volume_action(&volume.storage).to_owned(),
        })
        .collect();
    Ok(SetupSnapshot {
        plan,
        volumes,
        fstab,
    })
}

fn volume_state(storage: &ExistingStorage) -> VolumeState {
    match storage {
        ExistingStorage::Absent => VolumeState::Absent,
        ExistingStorage::MountedValid { .. } => VolumeState::MountedValid,
        ExistingStorage::MountedIncomplete { .. } => VolumeState::MountedIncomplete,
        ExistingStorage::ExistingUnmounted { .. } | ExistingStorage::DetachedIncomplete { .. } => {
            VolumeState::Detached
        }
        ExistingStorage::MisMountedIncomplete {
            current_mountpoint,
            ..
        } => VolumeState::MisMounted {
            mounted_at: current_mountpoint.clone(),
        },
        ExistingStorage::FoundElsewhere {
            container,
            device,
            mounted_at,
        } => VolumeState::FoundElsewhere {
            container: container.clone(),
            device: device.clone(),
            mounted_at: mounted_at.clone(),
        },
    }
}

fn volume_action(storage: &ExistingStorage) -> &'static str {
    match storage {
        ExistingStorage::Absent => "provisioned",
        ExistingStorage::MountedValid { .. } => "already-current",
        ExistingStorage::MountedIncomplete { .. } => "repaired",
        ExistingStorage::ExistingUnmounted { .. } | ExistingStorage::DetachedIncomplete { .. } => {
            "mounted"
        }
        ExistingStorage::MisMountedIncomplete { .. } => "remounted",
        ExistingStorage::FoundElsewhere { .. } => "reported",
    }
}

fn host_setup_actions(snapshot: &SetupSnapshot) -> Vec<String> {
    let mut actions = snapshot
        .plan
        .operations()
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::VerifyZfsDelegation { .. } | HostOperation::GuardMountpoint { .. } => {
                None
            }
            HostOperation::EnsureDirectory(path) => {
                Some(format!("create mountpoint {}", path.display()))
            }
            HostOperation::ReclaimMountpoint(path) => {
                Some(format!("reclaim mountpoint {}", path.display()))
            }
            HostOperation::MountApfsVolume { mountpoint, .. } => {
                Some(format!("mount APFS volume at {}", mountpoint.display()))
            }
            HostOperation::ProvisionApfsVolumes { volumes, .. } => Some(format!(
                "provision APFS volumes {}",
                volumes
                    .iter()
                    .map(ApfsVolumeProvision::name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            HostOperation::RunCommand(command) => Some(format!(
                "run {} {}",
                command.program(),
                command.args().join(" ")
            )),
            HostOperation::WriteMarkerAtomic { path, .. } => {
                Some(format!("write volume marker {}", path.display()))
            }
            HostOperation::PinVolumesInFstab { .. } => {
                Some("pin cowshed APFS volumes in /etc/fstab".to_owned())
            }
            HostOperation::ReportVolumeIssue { detail, .. } => Some(detail.clone()),
        })
        .collect::<Vec<_>>();
    if matches!(snapshot.fstab, PlannedFstab::Deferred(_))
        && snapshot
            .plan
            .operations()
            .iter()
            .any(|operation| matches!(operation, HostOperation::ProvisionApfsVolumes { .. }))
    {
        actions.push("pin cowshed APFS volumes in /etc/fstab after provisioning".to_owned());
    }
    actions
}

fn setup_requires_authorization(plan: &BootstrapPlan) -> bool {
    plan.operations().iter().any(|operation| match operation {
        HostOperation::ProvisionApfsVolumes { .. }
        | HostOperation::PinVolumesInFstab { .. }
        | HostOperation::MountApfsVolume { .. } => true,
        HostOperation::RunCommand(command) => {
            command.program() == DISKUTIL
                && command
                    .args()
                    .first()
                    .is_some_and(|argument| argument == "mount" || argument == "unmount")
        }
        _ => false,
    })
}

#[cfg(target_os = "macos")]
async fn gather_setup_snapshot(
    home: &Path,
    host: Arc<dyn BootstrapHost>,
) -> Result<SetupSnapshot, NativeBootstrapError> {
    let home = home.to_owned();
    tokio::task::spawn_blocking(move || {
        let existing_fstab = read_fstab_text().map_err(NativeBootstrapError::Host)?;
        let mut source = SystemEvidenceSource {
            host: host.as_ref(),
        };
        prepare_setup_snapshot(&mut source, &home, &existing_fstab)
    })
    .await
    .map_err(|error| {
        NativeBootstrapError::Execution(BootstrapExecutionError::BlockingLane(error.to_string()))
    })?
}

pub async fn plan_host_setup(home: &Path) -> crate::Result<HostSetupPlan> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = home;
        return Err(existing_host_storage_error(
            NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        ));
    }
    #[cfg(target_os = "macos")]
    {
        let snapshot = gather_setup_snapshot(home, Arc::new(SystemBootstrapHost))
            .await
            .map_err(existing_host_storage_error)?;
        Ok(HostSetupPlan {
            actions: host_setup_actions(&snapshot),
            requires_authorization: setup_requires_authorization(&snapshot.plan),
        })
    }
}

pub async fn execute_host_setup(home: &Path) -> crate::Result<HostSetupReport> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = home;
        return Err(existing_host_storage_error(
            NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        ));
    }
    #[cfg(target_os = "macos")]
    {
        let initial = gather_setup_snapshot(home, Arc::new(SystemBootstrapHost))
            .await
            .map_err(existing_host_storage_error)?;
        let authorized = setup_requires_authorization(&initial.plan);
        let host: Arc<dyn BootstrapHost> = if authorized {
            let session = tokio::task::spawn_blocking(MacAuthorizationSession::acquire)
                .await
                .map_err(|error| {
                    existing_host_storage_error(NativeBootstrapError::Execution(
                        BootstrapExecutionError::BlockingLane(error.to_string()),
                    ))
                })?
                .map_err(NativeBootstrapError::Host)
                .map_err(existing_host_storage_error)?;
            Arc::new(AuthorizedBootstrapHost::new(session))
        } else {
            Arc::new(SystemBootstrapHost)
        };
        execute_bootstrap(&initial.plan, Arc::clone(&host), &TokioBlockingLane)
            .await
            .map_err(NativeBootstrapError::Execution)
            .map_err(existing_host_storage_error)?;

        let pinned_initially = matches!(initial.fstab, PlannedFstab::NeedsPin(_));
        let needs_post_evidence = matches!(initial.fstab, PlannedFstab::Deferred(_))
            || initial.plan.operations().iter().any(|operation| {
                matches!(
                    operation,
                    HostOperation::ProvisionApfsVolumes { .. }
                        | HostOperation::MountApfsVolume { .. }
                        | HostOperation::ReclaimMountpoint(_)
                ) || matches!(
                    operation,
                    HostOperation::RunCommand(command) if command.program() == DISKUTIL
                )
            });
        let fstab = if needs_post_evidence {
            let post = gather_setup_snapshot(home, Arc::clone(&host))
                .await
                .map_err(existing_host_storage_error)?;
            if post.plan.operations().iter().any(|operation| {
                matches!(operation, HostOperation::ProvisionApfsVolumes { .. })
            }) {
                return Err(existing_host_storage_error(NativeBootstrapError::Host(
                    HostError::new(
                        "post-setup evidence still proposes APFS provisioning; refusing a second create",
                    ),
                )));
            }
            match post.fstab {
                PlannedFstab::AlreadyCurrent if pinned_initially => FstabOutcome::Pinned,
                PlannedFstab::AlreadyCurrent => FstabOutcome::AlreadyCurrent,
                PlannedFstab::NeedsPin(pins) => {
                    let pin_host = Arc::clone(&host);
                    tokio::task::spawn_blocking(move || pin_host.pin_volumes_in_fstab(&pins))
                        .await
                        .map_err(|error| {
                            existing_host_storage_error(NativeBootstrapError::Execution(
                                BootstrapExecutionError::BlockingLane(error.to_string()),
                            ))
                        })?
                        .map_err(NativeBootstrapError::Host)
                        .map_err(existing_host_storage_error)?;
                    FstabOutcome::Pinned
                }
                PlannedFstab::Deferred(reason) => FstabOutcome::Skipped(reason),
            }
        } else {
            match initial.fstab {
                PlannedFstab::AlreadyCurrent => FstabOutcome::AlreadyCurrent,
                PlannedFstab::NeedsPin(_) => FstabOutcome::Pinned,
                PlannedFstab::Deferred(reason) => FstabOutcome::Skipped(reason),
            }
        };
        Ok(HostSetupReport {
            volumes: initial.volumes,
            fstab,
            authorized,
        })
    }
}

#[cfg(target_os = "macos")]
fn host_uninstall_plan(home: &Path) -> Result<HostUninstallPlan, NativeBootstrapError> {
    let existing = read_fstab_text().map_err(NativeBootstrapError::Host)?;
    host_uninstall_plan_from_text(home, &existing)
}

fn host_uninstall_plan_from_text(
    home: &Path,
    existing: &str,
) -> Result<HostUninstallPlan, NativeBootstrapError> {
    require_canonical(home)?;
    let pins_to_remove = existing
        .lines()
        .filter_map(|line| {
            line.split_once("# cowshed created volume labelled")
                .map(|(_, label)| label.trim().to_owned())
        })
        .collect::<Vec<_>>();
    Ok(HostUninstallPlan {
        requires_authorization: !pins_to_remove.is_empty(),
        pins_to_remove,
    })
}

pub async fn plan_host_uninstall(home: &Path) -> crate::Result<HostUninstallPlan> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = home;
        return Err(existing_host_storage_error(
            NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        ));
    }
    #[cfg(target_os = "macos")]
    {
        let home = home.to_owned();
        tokio::task::spawn_blocking(move || host_uninstall_plan(&home))
            .await
            .map_err(|error| {
                existing_host_storage_error(NativeBootstrapError::Execution(
                    BootstrapExecutionError::BlockingLane(error.to_string()),
                ))
            })?
            .map_err(existing_host_storage_error)
    }
}

pub async fn execute_host_uninstall(home: &Path) -> crate::Result<UninstallReport> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = home;
        return Err(existing_host_storage_error(
            NativeBootstrapError::UnsupportedPlatform(std::env::consts::OS),
        ));
    }
    #[cfg(target_os = "macos")]
    {
        let plan = plan_host_uninstall(home).await?;
        if !plan.requires_authorization {
            return Ok(UninstallReport {
                fstab: UninstallFstabOutcome::AlreadyClean,
            });
        }
        tokio::task::spawn_blocking(move || {
            let mut session = MacAuthorizationSession::acquire()?;
            pin_volumes_in_fstab_with(&mut session, &[]).map(|_| ())
        })
        .await
        .map_err(|error| {
            existing_host_storage_error(NativeBootstrapError::Execution(
                BootstrapExecutionError::BlockingLane(error.to_string()),
            ))
        })?
        .map_err(NativeBootstrapError::Host)
        .map_err(existing_host_storage_error)?;
        Ok(UninstallReport {
            fstab: UninstallFstabOutcome::Removed,
        })
    }
}

fn existing_host_storage_error(error: NativeBootstrapError) -> CowshedError {
    CowshedError::environment_missing(error.to_string(), "cowshed setup")
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

fn mutating_setup_actions(plan: &BootstrapPlan) -> Vec<String> {
    plan.operations()
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::VerifyZfsDelegation { .. }
            | HostOperation::GuardMountpoint { .. }
            | HostOperation::EnsureDirectory(_)
            | HostOperation::ReclaimMountpoint(_)
            | HostOperation::MountApfsVolume { .. } => None,
            HostOperation::RunCommand(command) => {
                remount_setup_action(command).map(|action| action.to_owned())
            }
            HostOperation::ProvisionApfsVolumes { volumes, .. } => Some(format!(
                "provision APFS volumes {}",
                volumes
                    .iter()
                    .map(ApfsVolumeProvision::name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            HostOperation::WriteMarkerAtomic { path, .. } => {
                Some(format!("write volume marker {}", path.display()))
            }
            HostOperation::PinVolumesInFstab { .. } => {
                Some("pin cowshed APFS volumes in /etc/fstab".to_owned())
            }
            HostOperation::ReportVolumeIssue { detail, .. } => Some(detail.clone()),
        })
        .collect()
}

fn remount_setup_action(command: &HostCommand) -> Option<&'static str> {
    if is_unprivileged_apfs_remount(command) {
        return None;
    }
    Some("run privileged host command")
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct StatFsSnapshot {
    fs_type: String,
    mount_source: PathBuf,
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    mountpoint: PathBuf,
    nobrowse: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MountedVolumeEvidence {
    exact_identifier: String,
    mountpoint: PathBuf,
    nobrowse: bool,
    uid: u32,
    gid: u32,
}

trait EvidenceSource {
    fn statfs(&mut self, path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError>;
    fn run_command(
        &mut self,
        command: &HostCommand,
    ) -> Result<HostCommandOutput, NativeBootstrapError>;
    fn inspect_mountpoint(&mut self, path: &Path) -> Result<MountpointState, NativeBootstrapError>;
    fn mounted_volume(
        &mut self,
        path: &Path,
    ) -> Result<MountedVolumeEvidence, NativeBootstrapError>;
    /// Where the named volume is currently mounted, per per-volume diskutil evidence.
    ///
    /// `None` means detached. This exists because `diskutil apfs list -plist` stopped
    /// reporting `MountPoint` keys on recent macOS releases, so container-inventory
    /// evidence alone can no longer distinguish "detached" from "mounted somewhere else".
    fn volume_mountpoint(
        &mut self,
        identifier: &str,
    ) -> Result<Option<PathBuf>, NativeBootstrapError>;
    fn invoking_identity(&mut self) -> (u32, u32);
}

struct SystemEvidenceSource<'a> {
    host: &'a dyn BootstrapHost,
}

impl EvidenceSource for SystemEvidenceSource<'_> {
    fn statfs(&mut self, path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError> {
        system_statfs(path)
    }

    fn run_command(
        &mut self,
        command: &HostCommand,
    ) -> Result<HostCommandOutput, NativeBootstrapError> {
        self.host.run_command(command).map_err(Into::into)
    }

    fn inspect_mountpoint(&mut self, path: &Path) -> Result<MountpointState, NativeBootstrapError> {
        self.host.inspect_mountpoint(path).map_err(Into::into)
    }

    fn mounted_volume(
        &mut self,
        path: &Path,
    ) -> Result<MountedVolumeEvidence, NativeBootstrapError> {
        let snapshot = system_statfs(path)?;
        let metadata =
            fs::symlink_metadata(path).map_err(|source| NativeBootstrapError::StatFs {
                path: path.to_owned(),
                source,
            })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(NativeBootstrapError::MountEvidenceMismatch {
                path: path.to_owned(),
                identifier: "mountpoint is not a no-follow directory".to_owned(),
            });
        }
        Ok(MountedVolumeEvidence {
            exact_identifier: exact_device_identifier(&snapshot.mount_source)?,
            mountpoint: snapshot.mountpoint,

            nobrowse: snapshot.nobrowse,
            uid: metadata.uid(),
            gid: metadata.gid(),
        })
    }

    fn volume_mountpoint(
        &mut self,
        identifier: &str,
    ) -> Result<Option<PathBuf>, NativeBootstrapError> {
        let command = HostCommand::new(DISKUTIL, ["info", "-plist", identifier]);
        let output = self.host.run_command(&command).map_err(NativeBootstrapError::Host)?;
        if !output.succeeded() {
            return Err(NativeBootstrapError::CommandFailed(
                HostCommandFailure::new(command, output),
            ));
        }
        let value = plist::Value::from_reader(std::io::Cursor::new(&output.stdout))
            .map_err(|error| NativeBootstrapError::MalformedPlist(error.to_string()))?;
        let root = dictionary(&value, "diskutil info root")?;
        Ok(match root.get("MountPoint") {
            Some(plist::Value::String(mountpoint)) if !mountpoint.is_empty() => {
                Some(PathBuf::from(mountpoint))
            }
            _ => None,
        })
    }
    fn invoking_identity(&mut self) -> (u32, u32) {
        (unsafe { libc::getuid() }, unsafe { libc::getgid() })
    }
}

struct GatheredEvidence {
    statfs: StatFsEvidence,
    bootstrap: BootstrapEvidence,
    volumes: Vec<ClassifiedVolume>,
}

#[derive(Clone, Debug)]
struct ClassifiedVolume {
    name: &'static str,
    role: VolumeRole,
    mountpoint: PathBuf,
    storage: ExistingStorage,
    volume_uuid: Option<String>,
}

fn plan_native_bootstrap(
    source: &mut impl EvidenceSource,
    project_root: &Path,
    home: &Path,
) -> Result<BootstrapPlan, NativeBootstrapError> {
    let gathered = gather_apfs_evidence(source, project_root, home)?;
    let selected = select_substrate(gathered.statfs, None)?;
    plan_bootstrap(selected, home, gathered.bootstrap).map_err(Into::into)
}

fn plan_existing_host_storage(
    source: &mut impl EvidenceSource,
    home: &Path,
) -> Result<BootstrapPlan, NativeBootstrapError> {
    let gathered = gather_existing_apfs_evidence(source, home)?;
    let selected = select_substrate(gathered.statfs, None)?;
    plan_bootstrap(selected, home, gathered.bootstrap).map_err(Into::into)
}

fn gather_apfs_evidence(
    source: &mut impl EvidenceSource,
    project_root: &Path,
    home: &Path,
) -> Result<GatheredEvidence, NativeBootstrapError> {
    require_canonical(project_root)?;
    gather_existing_apfs_evidence(source, home)
}

fn gather_existing_apfs_evidence(
    source: &mut impl EvidenceSource,
    home: &Path,
) -> Result<GatheredEvidence, NativeBootstrapError> {
    require_canonical(home)?;
    let snapshot = source.statfs(home)?;
    if snapshot.fs_type != "apfs" {
        return Err(NativeBootstrapError::UnsupportedFilesystem {
            path: home.to_owned(),
            fs_type: snapshot.fs_type,
        });
    }
    let mount_device = exact_device_identifier(&snapshot.mount_source)?;
    // Ask for the one container that can hold the home volume rather than every container on
    // the host: each mounted workspace image is its own APFS container, so the unscoped listing
    // grows with the number of warm workspaces and costs seconds per CLI invocation on a busy
    // host. An APFS volume's BSD name is `<container>s<n>`, so the container is known before
    // asking; `containing_container` still verifies that the answer really lists the device.
    let container_reference = container_reference_of(&mount_device);
    let command = HostCommand::new(
        DISKUTIL,
        ["apfs", "list", "-plist", container_reference.as_str()],
    );
    let inventory = run_apfs_inventory_command(source, command)?;
    let container = inventory.containing_container(&mount_device)?;
    let roots = super::CanonicalRoots::for_home(home)?;
    let mut store = classify_volume(
        source,
        container,
        APFS_STORE_VOLUME,
        roots.store(),
        VolumeRole::Store,
    )?;
    let mut caches = classify_volume(
        source,
        container,
        APFS_CACHES_VOLUME,
        roots.caches(),
        VolumeRole::Caches,
    )?;
    if matches!(store, ExistingStorage::Absent) || matches!(caches, ExistingStorage::Absent) {
        let global = run_apfs_inventory_command(
            source,
            HostCommand::new(DISKUTIL, ["apfs", "list", "-plist"]),
        )?;
        guard_absent_volume_globally(source, &global, APFS_STORE_VOLUME, &mut store)?;
        guard_absent_volume_globally(source, &global, APFS_CACHES_VOLUME, &mut caches)?;
    }
    let classified = [
        (APFS_STORE_VOLUME, VolumeRole::Store, roots.store(), &store),
        (
            APFS_CACHES_VOLUME,
            VolumeRole::Caches,
            roots.caches(),
            &caches,
        ),
    ]
    .into_iter()
    .map(|(name, role, mountpoint, storage)| ClassifiedVolume {
        name,
        role,
        mountpoint: mountpoint.to_owned(),
        storage: storage.clone(),
        volume_uuid: container
            .volumes
            .iter()
            .find(|volume| volume.name == name)
            .map(|volume| volume.volume_uuid.clone()),
    })
    .collect();
    Ok(GatheredEvidence {
        statfs: StatFsEvidence::Apfs {
            mount_source: snapshot.mount_source,
            container: Some(container.reference.clone()),
        },
        bootstrap: BootstrapEvidence::Apfs { store, caches },
        volumes: classified,
    })
}

fn run_apfs_inventory_command(
    source: &mut impl EvidenceSource,
    command: HostCommand,
) -> Result<ApfsInventory, NativeBootstrapError> {
    let output = source.run_command(&command)?;
    if !output.succeeded() {
        return Err(NativeBootstrapError::CommandFailed(
            HostCommandFailure::new(command, output),
        ));
    }
    parse_apfs_inventory(&output.stdout)
}

fn guard_absent_volume_globally(
    source: &mut impl EvidenceSource,
    inventory: &ApfsInventory,
    name: &'static str,
    state: &mut ExistingStorage,
) -> Result<(), NativeBootstrapError> {
    if !matches!(state, ExistingStorage::Absent) {
        return Ok(());
    }
    let matches = inventory
        .containers
        .iter()
        .flat_map(|container| {
            container
                .volumes
                .iter()
                .filter(move |volume| volume.name == name)
                .map(move |volume| (container, volume))
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(()),
        [(container, volume)] => {
            let mounted_at = match &volume.mountpoint {
                Some(path) => Some(path.clone()),
                None => source.volume_mountpoint(&volume.identifier)?,
            };
            *state = ExistingStorage::FoundElsewhere {
                container: container.reference.clone(),
                device: volume.identifier.clone(),
                mounted_at,
            };
            Ok(())
        }
        _ => Err(NativeBootstrapError::AmbiguousVolume {
            container: "all APFS containers".to_owned(),
            name,
            matches: matches.len(),
        }),
    }
}

fn require_canonical(path: &Path) -> Result<(), NativeBootstrapError> {
    if path.is_absolute()
        && !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        Ok(())
    } else {
        Err(NativeBootstrapError::NonCanonicalPath(path.to_owned()))
    }
}

fn exact_device_identifier(path: &Path) -> Result<String, NativeBootstrapError> {
    let bytes = path.as_os_str().as_encoded_bytes();
    let Some(identifier) = bytes.strip_prefix(b"/dev/") else {
        return Err(NativeBootstrapError::InvalidMountSource(path.to_owned()));
    };
    if valid_volume_identifier(identifier) {
        Ok(String::from_utf8(identifier.to_vec()).expect("validated ASCII identifier"))
    } else {
        Err(NativeBootstrapError::InvalidMountSource(path.to_owned()))
    }
}

/// The synthesized APFS container (`diskN`) that a validated volume identifier (`diskNs…`) lives
/// in.
fn container_reference_of(volume_identifier: &str) -> String {
    let digits = volume_identifier["disk".len()..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    volume_identifier[.."disk".len() + digits].to_owned()
}

fn valid_container_identifier(value: &[u8]) -> bool {
    value
        .strip_prefix(b"disk")
        .is_some_and(|digits| !digits.is_empty() && digits.iter().all(u8::is_ascii_digit))
}

fn valid_volume_identifier(value: &[u8]) -> bool {
    let Some(rest) = value.strip_prefix(b"disk") else {
        return false;
    };
    let Some(separator) = rest.iter().position(|byte| *byte == b's') else {
        return false;
    };
    let (disk, slice_with_separator) = rest.split_at(separator);
    let slice = &slice_with_separator[1..];
    !disk.is_empty()
        && disk.iter().all(u8::is_ascii_digit)
        && !slice.is_empty()
        && slice.iter().all(u8::is_ascii_digit)
}

#[derive(Clone, Debug)]
struct ApfsInventory {
    containers: Vec<ApfsContainer>,
}

#[derive(Clone, Debug)]
struct ApfsContainer {
    reference: String,
    volumes: Vec<ApfsVolume>,
}

#[derive(Clone, Debug)]
struct ApfsVolume {
    name: String,
    identifier: String,
    mountpoint: Option<PathBuf>,
    volume_uuid: String,
}

impl ApfsInventory {
    fn containing_container(&self, device: &str) -> Result<&ApfsContainer, NativeBootstrapError> {
        let matches: Vec<_> = self
            .containers
            .iter()
            .filter(|container| {
                container
                    .volumes
                    .iter()
                    .any(|volume| volume.identifier == device)
            })
            .collect();
        match matches.as_slice() {
            [container] => Ok(container),
            [] => Err(NativeBootstrapError::ContainerNotFound {
                device: device.to_owned(),
            }),
            _ => Err(NativeBootstrapError::AmbiguousContainer {
                device: device.to_owned(),
                matches: matches.len(),
            }),
        }
    }
}

fn parse_apfs_inventory(bytes: &[u8]) -> Result<ApfsInventory, NativeBootstrapError> {
    let value = Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| NativeBootstrapError::MalformedPlist(error.to_string()))?;
    let root = dictionary(&value, "root")?;
    let containers = root
        .get("Containers")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed("missing Containers array"))?;
    if containers.is_empty() {
        return Err(malformed("Containers array is empty"));
    }
    let containers = containers
        .iter()
        .enumerate()
        .map(|(index, value)| parse_container(value, index))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ApfsInventory { containers })
}

fn parse_container(value: &Value, index: usize) -> Result<ApfsContainer, NativeBootstrapError> {
    let container = dictionary(value, &format!("Containers[{index}]"))?;
    let reference = required_string(container, "ContainerReference", "container")?;
    if !valid_container_identifier(reference.as_bytes()) {
        return Err(malformed(format!(
            "invalid ContainerReference {reference:?}"
        )));
    }
    let volumes = container
        .get("Volumes")
        .and_then(Value::as_array)
        .ok_or_else(|| malformed(format!("container {reference:?} has no Volumes array")))?;
    let volumes = volumes
        .iter()
        .enumerate()
        .map(|(volume_index, volume)| parse_volume(volume, &reference, volume_index))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ApfsContainer { reference, volumes })
}

fn parse_volume(
    value: &Value,
    container: &str,
    index: usize,
) -> Result<ApfsVolume, NativeBootstrapError> {
    let volume = dictionary(value, &format!("{container}.Volumes[{index}]"))?;
    let name = required_string(volume, "Name", "volume")?;
    let identifier = required_string(volume, "DeviceIdentifier", "volume")?;
    let volume_uuid = required_string(volume, "APFSVolumeUUID", "volume")?;
    if !valid_volume_identifier(identifier.as_bytes())
        || !identifier.strip_prefix(container).is_some_and(|slice| {
            slice.strip_prefix('s').is_some_and(|digits| {
                !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
            })
        })
    {
        return Err(malformed(format!(
            "volume DeviceIdentifier {identifier:?} is not in container {container:?}"
        )));
    }
    let mountpoint = match volume.get("MountPoint") {
        None => None,
        Some(Value::String(value)) if !value.is_empty() => Some(PathBuf::from(value)),
        Some(_) => {
            return Err(malformed(format!(
                "volume {identifier:?} has invalid MountPoint"
            )));
        }
    };
    Ok(ApfsVolume {
        name,
        identifier,
        volume_uuid,
        mountpoint,
    })
}

fn dictionary<'a>(value: &'a Value, context: &str) -> Result<&'a Dictionary, NativeBootstrapError> {
    value
        .as_dictionary()
        .ok_or_else(|| malformed(format!("{context} is not a dictionary")))
}

fn required_string(
    dictionary: &Dictionary,
    key: &str,
    context: &str,
) -> Result<String, NativeBootstrapError> {
    dictionary
        .get(key)
        .and_then(Value::as_string)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| malformed(format!("{context} has no nonempty {key} string")))
}

fn malformed(message: impl Into<String>) -> NativeBootstrapError {
    NativeBootstrapError::MalformedPlist(message.into())
}

fn classify_volume(
    source: &mut impl EvidenceSource,
    container: &ApfsContainer,
    name: &'static str,
    expected_mountpoint: &Path,
    role: VolumeRole,
) -> Result<ExistingStorage, NativeBootstrapError> {
    let matches: Vec<_> = container
        .volumes
        .iter()
        .filter(|volume| volume.name == name)
        .collect();
    if matches.len() > 1 {
        return Err(NativeBootstrapError::AmbiguousVolume {
            container: container.reference.clone(),
            name,
            matches: matches.len(),
        });
    }
    let state = source.inspect_mountpoint(expected_mountpoint)?;
    let Some(volume) = matches.first() else {
        return match state {
            MountpointState::Missing
            | MountpointState::EmptyDirectory
            | MountpointState::ReclaimableStub => Ok(ExistingStorage::Absent),
            MountpointState::NonEmptyDirectoryWithoutMount => {
                Err(NativeBootstrapError::MaskedMountpoint {
                    path: expected_mountpoint.to_owned(),
                })
            }
            MountpointState::Mounted { .. } => Err(NativeBootstrapError::MountEvidenceMismatch {
                path: expected_mountpoint.to_owned(),
                identifier: name.to_owned(),
            }),
        };
    };
    match state {
        MountpointState::Mounted { marker } => {
            let mounted = source.mounted_volume(expected_mountpoint)?;
            if mounted.exact_identifier != volume.identifier
                || mounted.mountpoint != expected_mountpoint
            {
                return Err(NativeBootstrapError::MountEvidenceMismatch {
                    path: expected_mountpoint.to_owned(),
                    identifier: mounted.exact_identifier,
                });
            }
            let marker_missing = marker.is_none();
            if let Some(marker) = marker {
                require_mounted_marker(Some(&marker), role, SubstrateKind::Apfs).map_err(
                    |error| NativeBootstrapError::InvalidMountedMarker {
                        path: expected_mountpoint.to_owned(),
                        message: error.to_string(),
                    },
                )?;
            }
            if !mounted.nobrowse {
                return Ok(ExistingStorage::mis_mounted_incomplete(
                    &volume.identifier,
                    expected_mountpoint,
                ));
            }
            let (uid, gid) = source.invoking_identity();
            if marker_missing || mounted.uid != uid || mounted.gid != gid {
                return Ok(ExistingStorage::mounted_incomplete(&volume.identifier));
            }
            Ok(ExistingStorage::mounted_valid(&volume.identifier))
        }
        MountpointState::Missing
        | MountpointState::EmptyDirectory
        | MountpointState::ReclaimableStub => {
            // Container-inventory mountpoint evidence is unreliable: `diskutil apfs list -plist`
            // stopped emitting `MountPoint` keys on recent macOS releases, so a volume that is
            // mounted somewhere else (macOS auto-mounts every container volume under /Volumes at
            // boot) arrives here with `mountpoint: None`. Asking per-volume evidence before
            // concluding "detached" is what keeps that state classifiable as a mis-mount and
            // therefore repairable with the unprivileged remount plan below, instead of
            // degrading into a privileged re-provision demand.
            let current = match &volume.mountpoint {
                Some(mountpoint) => Some(mountpoint.clone()),
                None => source.volume_mountpoint(&volume.identifier)?,
            };
            match current {
                None => Ok(ExistingStorage::detached_incomplete(&volume.identifier)),
                Some(current) => {
                    require_canonical(&current)?;
                    let mounted = source.mounted_volume(&current)?;
                    if mounted.exact_identifier != volume.identifier
                        || mounted.mountpoint != current
                    {
                        return Err(NativeBootstrapError::MountEvidenceMismatch {
                            path: current.clone(),
                            identifier: mounted.exact_identifier,
                        });
                    }
                    Ok(ExistingStorage::mis_mounted_incomplete(
                        &volume.identifier,
                        current,
                    ))
                }
            }
        }
        MountpointState::NonEmptyDirectoryWithoutMount => {
            Err(NativeBootstrapError::MaskedMountpoint {
                path: expected_mountpoint.to_owned(),
            })
        }
    }
}

fn run_command_with(
    command: &HostCommand,
    spawn: impl FnOnce(&Path, &[String]) -> io::Result<Output>,
) -> Result<HostCommandOutput, HostError> {
    let program = Path::new(command.program());
    if !program.is_absolute() {
        return Err(HostError::new(format!(
            "refusing non-absolute command program {:?}",
            command.program()
        )));
    }
    let output = spawn(program, command.args()).map_err(|source| {
        HostError::new(format!(
            "cannot execute {:?} with argv {:?}: {source}",
            command.program(),
            command.args()
        ))
    })?;
    Ok(output.into())
}

#[cfg(target_os = "macos")]
trait PrivilegedCommandSession {
    fn execute(&mut self, command: &HostCommand) -> Result<HostCommandOutput, HostError>;
}

#[cfg(target_os = "macos")]
trait ApfsProvisionIo {
    fn prepare_mountpoint(&self, path: &Path) -> Result<(), HostError>;
    fn attest_mounted(
        &self,
        path: &Path,
        exact_identifier: &str,
        require_nobrowse: bool,
    ) -> Result<(), HostError>;
    fn attest_owner(&self, path: &Path, uid: u32, gid: u32) -> Result<(), HostError>;
    fn write_marker(&self, path: &Path, contents: &[u8]) -> Result<(), HostError>;
}

#[cfg(target_os = "macos")]
struct SystemApfsProvisionIo;

#[cfg(target_os = "macos")]
impl ApfsProvisionIo for SystemApfsProvisionIo {
    fn prepare_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        match inspect_system_mountpoint(path)? {
            MountpointState::Missing => fs::create_dir_all(path)
                .map_err(|source| host_io_error("create APFS mountpoint", path, source)),
            MountpointState::EmptyDirectory => Ok(()),
            MountpointState::ReclaimableStub => reclaim_system_mountpoint(path),
            MountpointState::NonEmptyDirectoryWithoutMount | MountpointState::Mounted { .. } => {
                Err(HostError::new(format!(
                    "refusing to provision over non-empty or mounted path {path:?}"
                )))
            }
        }
    }

    fn attest_mounted(
        &self,
        path: &Path,
        exact_identifier: &str,
        require_nobrowse: bool,
    ) -> Result<(), HostError> {
        let metadata = fs::symlink_metadata(path)
            .map_err(|source| host_io_error("inspect mounted APFS path", path, source))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(HostError::new(format!(
                "mounted APFS path is not a no-follow directory: {path:?}"
            )));
        }
        let snapshot = system_statfs(path).map_err(|error| HostError::new(error.to_string()))?;
        let actual = exact_device_identifier(&snapshot.mount_source)
            .map_err(|error| HostError::new(error.to_string()))?;
        if snapshot.mountpoint != path
            || actual != exact_identifier
            || (require_nobrowse && !snapshot.nobrowse)
        {
            return Err(HostError::new(format!(
                "mounted APFS identity at {path:?} is {actual:?} from {:?} with nobrowse={}, expected {exact_identifier:?}",
                snapshot.mountpoint, snapshot.nobrowse
            )));
        }
        Ok(())
    }

    fn attest_owner(&self, path: &Path, uid: u32, gid: u32) -> Result<(), HostError> {
        let metadata = fs::symlink_metadata(path)
            .map_err(|source| host_io_error("inspect APFS volume root ownership", path, source))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.uid() != uid
            || metadata.gid() != gid
        {
            return Err(HostError::new(format!(
                "APFS volume root {path:?} ownership is {}:{}, expected {uid}:{gid}",
                metadata.uid(),
                metadata.gid()
            )));
        }
        Ok(())
    }

    fn write_marker(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        write_marker_atomic(path, contents)
    }
}

/// Return a freshly attested volume to the detached state.
///
/// `-nomount` is passed on creation, but some macOS releases mount the new
/// volume at `/Volumes/<name>` anyway. Mounting it a second time at the private
/// mountpoint would leave the browsable default mount in place, so the default
/// mount is dropped first and provisioning proceeds identically on either host.
#[cfg(target_os = "macos")]
fn detach_auto_mounted_volume<S>(
    session: &mut S,
    state: super::CreatedMountState,
    exact_identifier: &str,
) -> Result<(), HostError>
where
    S: PrivilegedCommandSession,
{
    if state == super::CreatedMountState::Unmounted {
        return Ok(());
    }
    let unmount = HostCommand::new(DISKUTIL, ["unmount", exact_identifier]);
    run_privileged_command(session, &unmount)?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn provision_apfs_volumes_with<S>(
    container: &str,
    volumes: &[ApfsVolumeProvision],
    uid: u32,
    gid: u32,
    acquire: impl FnOnce() -> Result<S, HostError>,
    io: &impl ApfsProvisionIo,
) -> Result<(), HostError>
where
    S: PrivilegedCommandSession,
{
    let mut session = acquire()?;
    provision_apfs_volumes_in_session(container, volumes, uid, gid, &mut session, io)
}

#[cfg(target_os = "macos")]
fn provision_apfs_volumes_in_session<S>(
    container: &str,
    volumes: &[ApfsVolumeProvision],
    uid: u32,
    gid: u32,
    session: &mut S,
    io: &impl ApfsProvisionIo,
) -> Result<(), HostError>
where
    S: PrivilegedCommandSession,
{
    validate_provision_batch(container, volumes)?;
    let create_names = volumes
        .iter()
        .filter(|volume| matches!(volume.kind(), ApfsProvisionKind::Create))
        .map(ApfsVolumeProvision::name)
        .collect::<Vec<_>>();
    if !create_names.is_empty() {
        let inventory_command = HostCommand::new(DISKUTIL, ["apfs", "list", "-plist"]);
        let output = run_privileged_command(session, &inventory_command)?;
        let inventory = parse_apfs_inventory(&output.stdout)
            .map_err(|error| HostError::new(error.to_string()))?;
        if let Some((container, volume)) = inventory.containers.iter().find_map(|container| {
            container
                .volumes
                .iter()
                .find(|volume| create_names.iter().any(|name| *name == volume.name))
                .map(|volume| (container, volume))
        }) {
            return Err(HostError::new(format!(
                "refusing to create APFS volume {:?}: it already exists as {} in container {} at {:?}; run cowshed setup after inspecting that volume",
                volume.name, volume.identifier, container.reference, volume.mountpoint
            )));
        }
    }
    for volume in volumes {
        let exact_identifier = match volume.kind() {
            ApfsProvisionKind::Create => {
                io.prepare_mountpoint(volume.mountpoint())?;
                let create = HostCommand::new(
                    DISKUTIL,
                    [
                        "apfs",
                        "addVolume",
                        container,
                        "APFS",
                        volume.name(),
                        "-nomount",
                    ],
                );
                let output = run_privileged_command(session, &create)?;
                let exact_identifier = super::parse_created_apfs_identifier(&output.stdout)
                    .map_err(|error| HostError::new(error.to_string()))?;
                let info =
                    HostCommand::new(DISKUTIL, ["info", "-plist", exact_identifier.as_str()]);
                let output = run_privileged_command(session, &info)?;
                let mount_state = super::attest_created_apfs_info(
                    &output.stdout,
                    &exact_identifier,
                    container,
                    volume.name(),
                )
                .map_err(|error| HostError::new(error.to_string()))?;
                detach_auto_mounted_volume(session, mount_state, &exact_identifier)?;
                let mountpoint = path_argument(volume.mountpoint())?;
                let mount = HostCommand::new(
                    DISKUTIL,
                    [
                        "mount".to_owned(),
                        "-nobrowse".to_owned(),
                        "-mountPoint".to_owned(),
                        mountpoint,
                        exact_identifier.clone(),
                    ],
                );
                run_privileged_command(session, &mount)?;
                exact_identifier
            }
            ApfsProvisionKind::RepairMounted { exact_identifier } => exact_identifier.clone(),
            ApfsProvisionKind::RecoverDetached { exact_identifier } => {
                io.prepare_mountpoint(volume.mountpoint())?;
                let info =
                    HostCommand::new(DISKUTIL, ["info", "-plist", exact_identifier.as_str()]);
                let output = run_privileged_command(session, &info)?;
                let mount_state = super::attest_created_apfs_info(
                    &output.stdout,
                    exact_identifier,
                    container,
                    volume.name(),
                )
                .map_err(|error| HostError::new(error.to_string()))?;
                detach_auto_mounted_volume(session, mount_state, exact_identifier)?;
                let mountpoint = path_argument(volume.mountpoint())?;
                let mount = HostCommand::new(
                    DISKUTIL,
                    [
                        "mount".to_owned(),
                        "-nobrowse".to_owned(),
                        "-mountPoint".to_owned(),
                        mountpoint,
                        exact_identifier.clone(),
                    ],
                );
                run_privileged_command(session, &mount)?;
                exact_identifier.clone()
            }
            ApfsProvisionKind::RepairMisMounted {
                exact_identifier,
                current_mountpoint,
            } => {
                io.attest_mounted(current_mountpoint, exact_identifier, false)?;
                let info =
                    HostCommand::new(DISKUTIL, ["info", "-plist", exact_identifier.as_str()]);
                let output = run_privileged_command(session, &info)?;
                attest_mounted_apfs_info(
                    &output.stdout,
                    exact_identifier,
                    container,
                    volume.name(),
                    current_mountpoint,
                )?;
                let unmount = HostCommand::new(DISKUTIL, ["unmount", exact_identifier.as_str()]);
                run_privileged_command(session, &unmount)?;
                io.prepare_mountpoint(volume.mountpoint())?;
                let mountpoint = path_argument(volume.mountpoint())?;
                let mount = HostCommand::new(
                    DISKUTIL,
                    [
                        "mount".to_owned(),
                        "-nobrowse".to_owned(),
                        "-mountPoint".to_owned(),
                        mountpoint,
                        exact_identifier.clone(),
                    ],
                );
                run_privileged_command(session, &mount)?;
                exact_identifier.clone()
            }
        };

        io.attest_mounted(volume.mountpoint(), &exact_identifier, true)?;
        let info = HostCommand::new(DISKUTIL, ["info", "-plist", exact_identifier.as_str()]);
        let output = run_privileged_command(session, &info)?;
        attest_mounted_apfs_info(
            &output.stdout,
            &exact_identifier,
            container,
            volume.name(),
            volume.mountpoint(),
        )?;

        let owner = format!("{uid}:{gid}");
        let mountpoint = path_argument(volume.mountpoint())?;
        let chown = HostCommand::new(CHOWN, [owner, mountpoint]);
        run_privileged_command(session, &chown)?;
        io.attest_owner(volume.mountpoint(), uid, gid)?;

        let marker = VolumeMarker::new(volume.role(), SubstrateKind::Apfs)
            .to_json()
            .map_err(|error| HostError::new(error.to_string()))?;
        io.write_marker(
            &volume.mountpoint().join(VOLUME_MARKER_FILE),
            marker.as_slice(),
        )?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn validate_provision_batch(
    container: &str,
    volumes: &[ApfsVolumeProvision],
) -> Result<(), HostError> {
    if !valid_container_identifier(container.as_bytes()) {
        return Err(HostError::new(format!(
            "invalid APFS container identifier {container:?}"
        )));
    }
    if volumes.is_empty() || volumes.len() > 2 {
        return Err(HostError::new(format!(
            "APFS provisioning batch contains {} volumes",
            volumes.len()
        )));
    }
    for (index, volume) in volumes.iter().enumerate() {
        require_host_canonical(volume.mountpoint())?;
        let expected_role = match volume.name() {
            APFS_STORE_VOLUME => VolumeRole::Store,
            APFS_CACHES_VOLUME => VolumeRole::Caches,
            name => {
                return Err(HostError::new(format!(
                    "refusing unexpected APFS volume name {name:?}"
                )));
            }
        };
        if volume.role() != expected_role
            || volumes[..index]
                .iter()
                .any(|prior| prior.name() == volume.name())
        {
            return Err(HostError::new(format!(
                "invalid or duplicate APFS provisioning role for {:?}",
                volume.name()
            )));
        }
        let exact_identifier = match volume.kind() {
            ApfsProvisionKind::Create => continue,
            ApfsProvisionKind::RepairMounted { exact_identifier }
            | ApfsProvisionKind::RecoverDetached { exact_identifier }
            | ApfsProvisionKind::RepairMisMounted {
                exact_identifier, ..
            } => exact_identifier,
        };
        if !valid_volume_identifier(exact_identifier.as_bytes()) {
            return Err(HostError::new(format!(
                "invalid APFS recovery identifier {exact_identifier:?}"
            )));
        }
        if let ApfsProvisionKind::RepairMisMounted {
            current_mountpoint, ..
        } = volume.kind()
        {
            require_host_canonical(current_mountpoint)?;
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn path_argument(path: &Path) -> Result<String, HostError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| HostError::new(format!("path is not UTF-8: {path:?}")))
}

#[cfg(target_os = "macos")]
fn run_privileged_command(
    session: &mut impl PrivilegedCommandSession,
    command: &HostCommand,
) -> Result<HostCommandOutput, HostError> {
    let output = session.execute(command)?;
    if !output.succeeded() {
        return Err(HostError::new(
            HostCommandFailure::new(command.clone(), output).to_string(),
        ));
    }
    Ok(output)
}

#[cfg(target_os = "macos")]
fn read_fstab_text() -> Result<String, HostError> {
    match fs::read(FSTAB) {
        Ok(bytes) => String::from_utf8(bytes)
            .map_err(|_| HostError::new(format!("{FSTAB} is not valid UTF-8"))),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(source) => Err(host_io_error("read fstab", Path::new(FSTAB), source)),
    }
}

#[cfg(target_os = "macos")]
fn desired_fstab(existing: &str, pins: &[FstabPin]) -> Result<String, HostError> {
    build_fstab(existing, pins).map_err(|error| HostError::new(error.to_string()))
}

#[cfg(target_os = "macos")]
fn pin_volumes_in_fstab_with(
    session: &mut impl PrivilegedCommandSession,
    pins: &[FstabPin],
) -> Result<bool, HostError> {
    let existing = read_fstab_text()?;
    let desired = desired_fstab(&existing, pins)?;
    if desired.as_bytes() == existing.as_bytes() {
        return Ok(false);
    }

    let temporary_path =
        PathBuf::from(format!("/private/tmp/cowshed-fstab-{}", Uuid::new_v4()));
    let result = (|| {
        let mut temporary = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary_path)
            .map_err(|source| host_io_error("create temporary fstab", &temporary_path, source))?;
        temporary
            .write_all(desired.as_bytes())
            .map_err(|source| host_io_error("write temporary fstab", &temporary_path, source))?;
        temporary
            .sync_all()
            .map_err(|source| host_io_error("sync temporary fstab", &temporary_path, source))?;
        drop(temporary);

        let temporary = path_argument(&temporary_path)?;
        let install = HostCommand::new(INSTALL, ["-m", "644", temporary.as_str(), FSTAB]);
        run_privileged_command(session, &install)?;
        Ok(true)
    })();
    match fs::remove_file(&temporary_path) {
        Ok(()) => result,
        Err(source) if result.is_ok() => Err(host_io_error(
            "remove temporary fstab",
            &temporary_path,
            source,
        )),
        Err(_) => result,
    }
}

#[cfg(target_os = "macos")]
fn attest_mounted_apfs_info(
    bytes: &[u8],
    expected_identifier: &str,
    expected_container: &str,
    expected_name: &str,
    expected_mountpoint: &Path,
) -> Result<(), HostError> {
    let value = Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| HostError::new(format!("invalid diskutil info plist: {error}")))?;
    let dictionary = value
        .as_dictionary()
        .ok_or_else(|| HostError::new("diskutil info plist root is not a dictionary"))?;
    let expected_mountpoint = path_argument(expected_mountpoint)?;
    for (key, expected) in [
        ("DeviceIdentifier", expected_identifier),
        ("APFSContainerReference", expected_container),
        ("VolumeName", expected_name),
        ("FilesystemType", "apfs"),
        ("MountPoint", expected_mountpoint.as_str()),
    ] {
        let actual = dictionary.get(key).and_then(Value::as_string);
        if actual != Some(expected) {
            return Err(HostError::new(format!(
                "mounted APFS info {key} is {actual:?}, expected {expected:?}"
            )));
        }
    }
    if !matches!(dictionary.get("APFSSnapshot"), Some(Value::Boolean(false))) {
        return Err(HostError::new(
            "mounted APFS object is not an ordinary volume",
        ));
    }
    for role_key in ["APFSVolumeRole", "APFSVolumeRoles", "Roles"] {
        match dictionary.get(role_key) {
            None => {}
            Some(Value::Array(values)) if values.is_empty() => {}
            Some(Value::String(value)) if value.is_empty() => {}
            Some(_) => {
                return Err(HostError::new(format!(
                    "mounted APFS volume unexpectedly has {role_key} metadata"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
struct MacAuthorizationSession {
    reference: AuthorizationRef,
}

#[cfg(target_os = "macos")]
unsafe impl Send for MacAuthorizationSession {}

#[cfg(target_os = "macos")]
impl MacAuthorizationSession {
    fn acquire() -> Result<Self, HostError> {
        let mut reference = std::ptr::null();
        let status =
            unsafe { AuthorizationCreate(std::ptr::null(), std::ptr::null(), 0, &mut reference) };
        authorization_status("create authorization session", status)?;
        if reference.is_null() {
            return Err(HostError::new(
                "AuthorizationCreate succeeded without an authorization reference",
            ));
        }
        let session = Self { reference };
        let mut item = AuthorizationItem {
            name: AUTHORIZATION_RIGHT_EXECUTE.as_ptr().cast(),
            value_length: 0,
            value: std::ptr::null_mut(),
            flags: 0,
        };
        let rights = AuthorizationRights {
            count: 1,
            items: &mut item,
        };
        let flags = AUTHORIZATION_FLAG_INTERACTION_ALLOWED
            | AUTHORIZATION_FLAG_EXTEND_RIGHTS
            | AUTHORIZATION_FLAG_PREAUTHORIZE;
        let status = unsafe {
            AuthorizationCopyRights(
                session.reference,
                &rights,
                std::ptr::null(),
                flags,
                std::ptr::null_mut(),
            )
        };
        authorization_status("preauthorize privileged execution", status)?;
        Ok(session)
    }
}

#[cfg(target_os = "macos")]
impl PrivilegedCommandSession for MacAuthorizationSession {
    fn execute(&mut self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        if !Path::new(command.program()).is_absolute() {
            return Err(HostError::new(format!(
                "refusing non-absolute authorized program {:?}",
                command.program()
            )));
        }
        let program = CString::new(command.program()).map_err(|_| {
            HostError::new(format!(
                "authorized program contains NUL: {:?}",
                command.program()
            ))
        })?;
        let arguments: Vec<CString> = command
            .args()
            .iter()
            .map(|argument| {
                CString::new(argument.as_bytes()).map_err(|_| {
                    HostError::new(format!("authorized argument contains NUL: {argument:?}"))
                })
            })
            .collect::<Result<_, _>>()?;
        let mut argument_pointers: Vec<*mut libc::c_char> = arguments
            .iter()
            .map(|argument| argument.as_ptr().cast_mut())
            .chain(std::iter::once(std::ptr::null_mut()))
            .collect();
        let mut pipe = std::ptr::null_mut();
        let status = unsafe {
            AuthorizationExecuteWithPrivileges(
                self.reference,
                program.as_ptr(),
                0,
                argument_pointers.as_mut_ptr(),
                &mut pipe,
            )
        };
        authorization_status("execute privileged command", status)?;
        if pipe.is_null() {
            return Err(HostError::new(
                "privileged command returned no communications pipe",
            ));
        }
        let stdout = read_authorized_output(pipe)?;
        Ok(HostCommandOutput::success(stdout))
    }
}

#[cfg(target_os = "macos")]
impl Drop for MacAuthorizationSession {
    fn drop(&mut self) {
        unsafe {
            AuthorizationFree(self.reference, AUTHORIZATION_FLAG_DESTROY_RIGHTS);
        }
    }
}

#[cfg(target_os = "macos")]
fn read_authorized_output(pipe: *mut libc::FILE) -> Result<Vec<u8>, HostError> {
    struct Pipe(*mut libc::FILE);
    impl Drop for Pipe {
        fn drop(&mut self) {
            unsafe {
                libc::fclose(self.0);
            }
        }
    }

    let pipe = Pipe(pipe);
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = unsafe { libc::fread(buffer.as_mut_ptr().cast(), 1, buffer.len(), pipe.0) };
        if read > 0 {
            if output.len().saturating_add(read) > AUTHORIZED_OUTPUT_LIMIT {
                return Err(HostError::new(format!(
                    "privileged command output exceeded {AUTHORIZED_OUTPUT_LIMIT} bytes"
                )));
            }
            output.extend_from_slice(&buffer[..read]);
        }
        if read < buffer.len() {
            if unsafe { libc::ferror(pipe.0) } != 0 {
                return Err(HostError::new(format!(
                    "cannot read privileged command output: {}",
                    io::Error::last_os_error()
                )));
            }
            break;
        }
    }
    Ok(output)
}

#[cfg(target_os = "macos")]
fn authorization_status(operation: &str, status: i32) -> Result<(), HostError> {
    if status == 0 {
        Ok(())
    } else {
        Err(HostError::new(format!(
            "{operation} failed with Authorization Services status {status}"
        )))
    }
}

#[cfg(target_os = "macos")]
type AuthorizationRef = *const c_void;

#[cfg(target_os = "macos")]
#[repr(C)]
struct AuthorizationItem {
    name: *const libc::c_char,
    value_length: usize,
    value: *mut c_void,
    flags: u32,
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct AuthorizationRights {
    count: u32,
    items: *mut AuthorizationItem,
}

#[cfg(target_os = "macos")]
const AUTHORIZATION_RIGHT_EXECUTE: &[u8] = b"system.privilege.admin\0";
#[cfg(target_os = "macos")]
const AUTHORIZATION_FLAG_INTERACTION_ALLOWED: u32 = 1 << 0;
#[cfg(target_os = "macos")]
const AUTHORIZATION_FLAG_EXTEND_RIGHTS: u32 = 1 << 1;
#[cfg(target_os = "macos")]
const AUTHORIZATION_FLAG_DESTROY_RIGHTS: u32 = 1 << 3;
#[cfg(target_os = "macos")]
const AUTHORIZATION_FLAG_PREAUTHORIZE: u32 = 1 << 4;

#[cfg(target_os = "macos")]
#[link(name = "Security", kind = "framework")]
unsafe extern "C" {
    fn AuthorizationCreate(
        rights: *const AuthorizationRights,
        environment: *const AuthorizationRights,
        flags: u32,
        authorization: *mut AuthorizationRef,
    ) -> i32;
    fn AuthorizationCopyRights(
        authorization: AuthorizationRef,
        rights: *const AuthorizationRights,
        environment: *const AuthorizationRights,
        flags: u32,
        authorized_rights: *mut *mut AuthorizationRights,
    ) -> i32;
    fn AuthorizationExecuteWithPrivileges(
        authorization: AuthorizationRef,
        path_to_tool: *const libc::c_char,
        options: u32,
        arguments: *mut *mut libc::c_char,
        communications_pipe: *mut *mut libc::FILE,
    ) -> i32;
    fn AuthorizationFree(authorization: AuthorizationRef, flags: u32) -> i32;
}

fn ensure_supported_host() -> Result<(), HostError> {
    if cfg!(target_os = "macos") {
        Ok(())
    } else {
        Err(platform_host_error("native bootstrap"))
    }
}

fn platform_host_error(operation: &str) -> HostError {
    HostError::new(format!(
        "{operation} is unsupported on {}",
        std::env::consts::OS
    ))
}

#[cfg(target_os = "macos")]
fn system_statfs(path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError> {
    require_canonical(path)?;
    let path_c =
        CString::new(path.as_os_str().as_bytes()).map_err(|_| NativeBootstrapError::StatFs {
            path: path.to_owned(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"),
        })?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: `path_c` is NUL-terminated and `stats` points to writable storage.
    if unsafe { libc::statfs(path_c.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(NativeBootstrapError::StatFs {
            path: path.to_owned(),
            source: io::Error::last_os_error(),
        });
    }
    // SAFETY: successful statfs initialized the output structure.
    let stats = unsafe { stats.assume_init() };
    Ok(StatFsSnapshot {
        fs_type: c_char_field(&stats.f_fstypename, path)?,
        mount_source: PathBuf::from(OsString::from_vec(c_char_field_bytes(
            &stats.f_mntfromname,
            path,
        )?)),
        mountpoint: PathBuf::from(OsString::from_vec(c_char_field_bytes(
            &stats.f_mntonname,
            path,
        )?)),
        nobrowse: stats.f_flags & libc::MNT_DONTBROWSE as u32 != 0,
    })
}

#[cfg(not(target_os = "macos"))]
fn system_statfs(_path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError> {
    Err(NativeBootstrapError::UnsupportedPlatform(
        std::env::consts::OS,
    ))
}

#[cfg(target_os = "macos")]
fn c_char_field(field: &[libc::c_char], path: &Path) -> Result<String, NativeBootstrapError> {
    String::from_utf8(c_char_field_bytes(field, path)?).map_err(|source| {
        NativeBootstrapError::StatFs {
            path: path.to_owned(),
            source: io::Error::new(io::ErrorKind::InvalidData, source),
        }
    })
}

#[cfg(target_os = "macos")]
fn c_char_field_bytes(
    field: &[libc::c_char],
    path: &Path,
) -> Result<Vec<u8>, NativeBootstrapError> {
    let nul =
        field
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| NativeBootstrapError::StatFs {
                path: path.to_owned(),
                source: io::Error::new(io::ErrorKind::InvalidData, "unterminated statfs field"),
            })?;
    Ok(field[..nul].iter().map(|byte| *byte as u8).collect())
}

#[cfg(target_os = "macos")]
fn inspect_system_mountpoint(path: &Path) -> Result<MountpointState, HostError> {
    require_host_canonical(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(MountpointState::Missing);
        }
        Err(source) => return Err(host_io_error("inspect mountpoint", path, source)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(HostError::new(format!(
            "mountpoint is not a no-follow directory: {path:?}"
        )));
    }
    let snapshot = system_statfs(path).map_err(|error| HostError::new(error.to_string()))?;
    if snapshot.mountpoint == path {
        return read_marker_no_follow(path).map(|marker| MountpointState::Mounted { marker });
    }
    if is_reclaimable_launchd_stub(path)? {
        return Ok(MountpointState::ReclaimableStub);
    }
    let mut entries = fs::read_dir(path)
        .map_err(|source| host_io_error("read mountpoint directory", path, source))?;
    if entries
        .next()
        .transpose()
        .map_err(|source| host_io_error("read mountpoint directory entry", path, source))?
        .is_some()
    {
        Ok(MountpointState::NonEmptyDirectoryWithoutMount)
    } else {
        Ok(MountpointState::EmptyDirectory)
    }
}

#[cfg(target_os = "macos")]
fn is_reclaimable_launchd_stub(path: &Path) -> Result<bool, HostError> {
    let mut saw_telemetry = false;
    for entry in fs::read_dir(path)
        .map_err(|source| host_io_error("read mountpoint directory", path, source))?
    {
        let entry = entry
            .map_err(|source| host_io_error("read mountpoint directory entry", path, source))?;
        if entry.file_name() == ".DS_Store" {
            continue;
        }
        if entry.file_name() != "telemetry" {
            return Ok(false);
        }
        let file_type = entry
            .file_type()
            .map_err(|source| host_io_error("inspect mountpoint entry", &entry.path(), source))?;
        if !file_type.is_dir() {
            return Ok(false);
        }
        if !is_reclaimable_telemetry_dir(&entry.path())? {
            return Ok(false);
        }
        saw_telemetry = true;
    }
    Ok(saw_telemetry)
}

#[cfg(target_os = "macos")]
fn is_reclaimable_telemetry_dir(path: &Path) -> Result<bool, HostError> {
    for entry in
        fs::read_dir(path).map_err(|source| host_io_error("read telemetry stub", path, source))?
    {
        let entry =
            entry.map_err(|source| host_io_error("read telemetry stub entry", path, source))?;
        let name = entry.file_name();
        if name != "daemon-stderr.log" && name != "sccache-stderr.log" {
            return Ok(false);
        }
        let file_type = entry.file_type().map_err(|source| {
            host_io_error("inspect telemetry stub entry", &entry.path(), source)
        })?;
        if !file_type.is_file() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(target_os = "macos")]
fn reclaim_system_mountpoint(path: &Path) -> Result<(), HostError> {
    require_host_canonical(path)?;
    match inspect_system_mountpoint(path)? {
        MountpointState::Missing | MountpointState::EmptyDirectory => Ok(()),
        MountpointState::ReclaimableStub => {
            fs::remove_dir_all(path)
                .map_err(|source| host_io_error("reclaim launchd mount stub", path, source))?;
            fs::create_dir_all(path)
                .map_err(|source| host_io_error("recreate APFS mountpoint", path, source))
        }
        MountpointState::NonEmptyDirectoryWithoutMount | MountpointState::Mounted { .. } => {
            Err(HostError::new(format!(
                "refusing to reclaim non-empty or mounted path {path:?}"
            )))
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn reclaim_system_mountpoint(_path: &Path) -> Result<(), HostError> {
    Err(platform_host_error("mountpoint reclaim"))
}

#[cfg(not(target_os = "macos"))]
fn inspect_system_mountpoint(_path: &Path) -> Result<MountpointState, HostError> {
    Err(platform_host_error("mountpoint inspection"))
}

#[cfg(target_os = "macos")]
fn read_marker_no_follow(root: &Path) -> Result<Option<Vec<u8>>, HostError> {
    let marker = root.join(VOLUME_MARKER_FILE);
    let marker_c = CString::new(marker.as_os_str().as_bytes())
        .map_err(|_| HostError::new(format!("marker path contains NUL: {marker:?}")))?;
    let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: `marker_c` is a valid NUL-terminated path and flags request a read-only fd.
    let fd = unsafe { libc::open(marker_c.as_ptr(), flags) };
    if fd == -1 {
        let source = io::Error::last_os_error();
        if source.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(host_io_error(
            "open marker without following",
            &marker,
            source,
        ));
    }
    // SAFETY: `fd` is newly owned by this function after successful open.
    let mut file = unsafe { File::from_raw_fd(fd) };
    let metadata = file
        .metadata()
        .map_err(|source| host_io_error("inspect marker", &marker, source))?;
    if !metadata.is_file() {
        return Err(HostError::new(format!(
            "marker is not a regular file: {marker:?}"
        )));
    }
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .map_err(|source| host_io_error("read marker", &marker, source))?;
    Ok(Some(contents))
}

#[cfg(unix)]
fn write_marker_atomic(path: &Path, contents: &[u8]) -> Result<(), HostError> {
    ensure_supported_host()?;
    require_host_canonical(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| HostError::new(format!("marker has no parent: {path:?}")))?;
    let name = path
        .file_name()
        .ok_or_else(|| HostError::new(format!("marker has no filename: {path:?}")))?;
    let parent_c = CString::new(parent.as_os_str().as_bytes())
        .map_err(|_| HostError::new(format!("marker parent contains NUL: {parent:?}")))?;
    let name_c = CString::new(name.as_bytes())
        .map_err(|_| HostError::new(format!("marker filename contains NUL: {path:?}")))?;
    let parent_flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: `parent_c` is NUL-terminated and open returns a new owned descriptor.
    let parent_fd = unsafe { libc::open(parent_c.as_ptr(), parent_flags) };
    if parent_fd == -1 {
        return Err(host_io_error(
            "open marker parent without following",
            parent,
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `parent_fd` is newly owned after successful open.
    let parent_file = unsafe { File::from_raw_fd(parent_fd) };
    reject_non_regular_destination(parent_file.as_raw_fd(), &name_c, path)?;

    let temporary_name = format!(".{}.tmp.{}", name.to_string_lossy(), Uuid::new_v4());
    let temporary_c = CString::new(temporary_name.as_bytes())
        .expect("UUID temporary marker filename contains no NUL");
    let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: parent fd is an open directory and temporary_c is one NUL-terminated component.
    let temporary_fd = unsafe {
        libc::openat(
            parent_file.as_raw_fd(),
            temporary_c.as_ptr(),
            flags,
            MARKER_MODE as libc::c_uint,
        )
    };
    if temporary_fd == -1 {
        return Err(host_io_error(
            "create temporary marker",
            path,
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `temporary_fd` is newly owned after successful openat.
    let mut temporary = unsafe { File::from_raw_fd(temporary_fd) };
    let result = (|| {
        // SAFETY: temporary is an open file descriptor owned by this function.
        if unsafe { libc::fchmod(temporary.as_raw_fd(), MARKER_MODE) } != 0 {
            return Err(host_io_error(
                "set temporary marker mode",
                path,
                io::Error::last_os_error(),
            ));
        }
        temporary
            .write_all(contents)
            .map_err(|source| host_io_error("write temporary marker", path, source))?;
        temporary
            .sync_all()
            .map_err(|source| host_io_error("sync temporary marker", path, source))?;
        drop(temporary);
        // SAFETY: both names are NUL-terminated entries relative to the same open directory.
        if unsafe {
            libc::renameat(
                parent_file.as_raw_fd(),
                temporary_c.as_ptr(),
                parent_file.as_raw_fd(),
                name_c.as_ptr(),
            )
        } != 0
        {
            return Err(host_io_error(
                "publish marker atomically",
                path,
                io::Error::last_os_error(),
            ));
        }
        parent_file
            .sync_all()
            .map_err(|source| host_io_error("sync marker parent", parent, source))
    })();
    if result.is_err() {
        // SAFETY: unlinkat removes only the no-follow temporary directory entry.
        unsafe {
            libc::unlinkat(parent_file.as_raw_fd(), temporary_c.as_ptr(), 0);
        }
    }
    result
}

#[cfg(not(unix))]
fn write_marker_atomic(_path: &Path, _contents: &[u8]) -> Result<(), HostError> {
    Err(platform_host_error("atomic marker write"))
}

#[cfg(unix)]
fn reject_non_regular_destination(
    parent_fd: libc::c_int,
    name: &CStr,
    path: &Path,
) -> Result<(), HostError> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::zeroed();
    // SAFETY: parent fd and name identify an entry; metadata points to writable storage.
    let result = unsafe {
        libc::fstatat(
            parent_fd,
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == -1 {
        let source = io::Error::last_os_error();
        if source.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(host_io_error("inspect marker destination", path, source));
    }
    // SAFETY: successful fstatat initialized metadata.
    let metadata = unsafe { metadata.assume_init() };
    if metadata.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(HostError::new(format!(
            "refusing non-regular marker destination: {path:?}"
        )));
    }
    Ok(())
}

fn require_host_canonical(path: &Path) -> Result<(), HostError> {
    if path.is_absolute()
        && !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        Ok(())
    } else {
        Err(HostError::new(format!(
            "path must be absolute and normalized: {path:?}"
        )))
    }
}

fn host_io_error(operation: &str, path: &Path, source: io::Error) -> HostError {
    HostError::new(format!("cannot {operation} {path:?}: {source}"))
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::collections::VecDeque;
    #[cfg(target_os = "macos")]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    #[cfg(target_os = "macos")]
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    #[cfg(target_os = "macos")]
    use crate::storage::bootstrap::ApfsProvisionKind;
    use crate::storage::bootstrap::VolumeMarker;

    struct FakeEvidenceSource {
        statfs: StatFsSnapshot,
        statfs_overrides: BTreeMap<PathBuf, StatFsSnapshot>,
        statfs_paths: Vec<PathBuf>,
        command_output: HostCommandOutput,
        command_outputs: VecDeque<HostCommandOutput>,
        mountpoints: BTreeMap<PathBuf, MountpointState>,
        mounted_volumes: BTreeMap<PathBuf, MountedVolumeEvidence>,
        // Per-identifier answers for `volume_mountpoint`; an absent identifier means detached,
        // mirroring what `diskutil info -plist` reports with an empty MountPoint.
        volume_mountpoints: BTreeMap<String, Option<PathBuf>>,
        commands: Vec<HostCommand>,
        invoking_identity: (u32, u32),
    }

    impl EvidenceSource for FakeEvidenceSource {
        fn statfs(&mut self, path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError> {
            self.statfs_paths.push(path.to_owned());
            Ok(self
                .statfs_overrides
                .get(path)
                .cloned()
                .unwrap_or_else(|| self.statfs.clone()))
        }

        fn run_command(
            &mut self,
            command: &HostCommand,
        ) -> Result<HostCommandOutput, NativeBootstrapError> {
            self.commands.push(command.clone());
            Ok(self
                .command_outputs
                .pop_front()
                .unwrap_or_else(|| self.command_output.clone()))
        }

        fn inspect_mountpoint(
            &mut self,
            path: &Path,
        ) -> Result<MountpointState, NativeBootstrapError> {
            self.mountpoints.remove(path).ok_or_else(|| {
                NativeBootstrapError::MountEvidenceMismatch {
                    path: path.to_owned(),
                    identifier: "missing test evidence".to_owned(),
                }
            })
        }

        fn mounted_volume(
            &mut self,
            path: &Path,
        ) -> Result<MountedVolumeEvidence, NativeBootstrapError> {
            self.mounted_volumes.get(path).cloned().ok_or_else(|| {
                NativeBootstrapError::MountEvidenceMismatch {
                    path: path.to_owned(),
                    identifier: "missing mounted volume test evidence".to_owned(),
                }
            })
        }

        fn volume_mountpoint(
            &mut self,
            identifier: &str,
        ) -> Result<Option<PathBuf>, NativeBootstrapError> {
            Ok(self.volume_mountpoints.get(identifier).cloned().flatten())
        }

        fn invoking_identity(&mut self) -> (u32, u32) {
            self.invoking_identity
        }
    }

    fn plist(containers: &str) -> Vec<u8> {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<plist version=\"1.0\"><dict><key>Containers</key><array>{containers}</array></dict></plist>"
        )
        .into_bytes()
    }

    fn container(reference: &str, volumes: &str) -> String {
        format!(
            "<dict><key>ContainerReference</key><string>{reference}</string><key>Volumes</key><array>{volumes}</array></dict>"
        )
    }

    fn volume(name: &str, identifier: &str, mountpoint: Option<&str>) -> String {
        let mountpoint = mountpoint
            .map(|path| format!("<key>MountPoint</key><string>{path}</string>"))
            .unwrap_or_default();
        format!(
            "<dict><key>Name</key><string>{name}</string><key>DeviceIdentifier</key><string>{identifier}</string><key>APFSVolumeUUID</key><string>{identifier}-UUID</string>{mountpoint}</dict>"
        )
    }

    fn source(inventory: Vec<u8>) -> FakeEvidenceSource {
        FakeEvidenceSource {
            statfs: StatFsSnapshot {
                fs_type: "apfs".to_owned(),
                mount_source: PathBuf::from("/dev/disk3s5"),
                mountpoint: PathBuf::from("/System/Volumes/Data"),
                nobrowse: true,
            },
            statfs_overrides: BTreeMap::new(),
            statfs_paths: Vec::new(),
            command_output: HostCommandOutput::success(inventory),
            command_outputs: VecDeque::new(),
            mountpoints: BTreeMap::from([
                (
                    PathBuf::from("/Users/alice/.cowshed"),
                    MountpointState::EmptyDirectory,
                ),
                (
                    PathBuf::from("/Users/alice/.cowshed/caches"),
                    MountpointState::Missing,
                ),
            ]),
            mounted_volumes: BTreeMap::from([
                (
                    PathBuf::from("/Users/alice/.cowshed"),
                    MountedVolumeEvidence {
                        exact_identifier: "disk3s8".to_owned(),
                        mountpoint: PathBuf::from("/Users/alice/.cowshed"),
                        nobrowse: true,
                        uid: 501,
                        gid: 20,
                    },
                ),
                (
                    PathBuf::from("/Users/alice/.cowshed/caches"),
                    MountedVolumeEvidence {
                        exact_identifier: "disk3s9".to_owned(),
                        mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
                        nobrowse: true,
                        uid: 501,
                        gid: 20,
                    },
                ),
            ]),
            volume_mountpoints: BTreeMap::new(),
            invoking_identity: (501, 20),
            commands: Vec::new(),
        }
    }

    fn healthy_existing_source() -> FakeEvidenceSource {
        let volumes = volume("Data", "disk3s5", Some("/System/Volumes/Data"))
            + &volume(APFS_STORE_VOLUME, "disk3s8", Some("/Users/alice/.cowshed"))
            + &volume(
                APFS_CACHES_VOLUME,
                "disk3s9",
                Some("/Users/alice/.cowshed/caches"),
            );
        let mut source = source(plist(&container("disk3", &volumes)));
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
                        .to_json()
                        .expect("store marker"),
                ),
            },
        );
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed/caches"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Apfs)
                        .to_json()
                        .expect("caches marker"),
                ),
            },
        );
        source
    }

    fn source_with_caches_inventory_mountpoint_omitted(
        volume_mountpoint: Option<&str>,
    ) -> FakeEvidenceSource {
        let volumes = volume("Data", "disk3s5", Some("/System/Volumes/Data"))
            + &volume(APFS_STORE_VOLUME, "disk3s8", Some("/Users/alice/.cowshed"))
            + &volume(APFS_CACHES_VOLUME, "disk3s9", None);
        let mut source = healthy_existing_source();
        source.command_output = HostCommandOutput::success(plist(&container("disk3", &volumes)));
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed/caches"),
            MountpointState::Missing,
        );
        source
            .mounted_volumes
            .remove(Path::new("/Users/alice/.cowshed/caches"));
        source.mounted_volumes.insert(
            PathBuf::from("/Volumes/cowshed.caches"),
            MountedVolumeEvidence {
                exact_identifier: "disk3s9".to_owned(),
                mountpoint: PathBuf::from("/Volumes/cowshed.caches"),
                nobrowse: false,
                uid: 501,
                gid: 20,
            },
        );
        if let Some(mountpoint) = volume_mountpoint {
            source.volume_mountpoints.insert(
                "disk3s9".to_owned(),
                Some(PathBuf::from(mountpoint)),
            );
        }
        source
    }

    #[derive(Default)]
    struct ReadOnlyValidationHost {
        inspections: AtomicUsize,
        mutation_calls: AtomicUsize,
        authorization_calls: AtomicUsize,
    }

    impl BootstrapHost for ReadOnlyValidationHost {
        fn verify_zfs_delegation(
            &self,
            _pool: &str,
            _required_root: &str,
        ) -> Result<(), HostError> {
            Err(HostError::new("unexpected ZFS validation"))
        }

        fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
            self.inspections.fetch_add(1, Ordering::SeqCst);
            let role = if path == Path::new("/Users/alice/.cowshed") {
                VolumeRole::Store
            } else if path == Path::new("/Users/alice/.cowshed/caches") {
                VolumeRole::Caches
            } else {
                return Err(HostError::new(format!(
                    "unexpected mountpoint inspection: {}",
                    path.display()
                )));
            };
            Ok(MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(role, SubstrateKind::Apfs)
                        .to_json()
                        .expect("validation marker"),
                ),
            })
        }

        fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
            self.mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn reclaim_mountpoint(&self, _path: &Path) -> Result<(), HostError> {
            self.mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn run_command(&self, _command: &HostCommand) -> Result<HostCommandOutput, HostError> {
            self.mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(HostCommandOutput::default())
        }

        fn provision_apfs_volumes(
            &self,
            _container: &str,
            _volumes: &[ApfsVolumeProvision],
        ) -> Result<(), HostError> {
            self.mutation_calls.fetch_add(1, Ordering::SeqCst);
            self.authorization_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
            self.mutation_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
            self.mutation_calls.fetch_add(1, Ordering::SeqCst);
            self.authorization_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct ValidationLane {
        dispatches: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlockingLane for ValidationLane {
        async fn dispatch(
            &self,
            job: super::super::BlockingJob,
        ) -> Result<(), BootstrapExecutionError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            job()
        }
    }

    #[cfg(target_os = "macos")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum ProvisionEvent {
        Acquire,
        Command { program: String, args: Vec<String> },
        Prepare(PathBuf),
        AttestMounted(PathBuf, String, bool),
        AttestOwner(PathBuf, u32, u32),
        Marker(PathBuf, Vec<u8>),
        Free,
    }

    #[cfg(target_os = "macos")]
    struct FakePrivilegedSession {
        events: Rc<RefCell<Vec<ProvisionEvent>>>,
        outputs: VecDeque<Result<HostCommandOutput, HostError>>,
    }

    #[cfg(target_os = "macos")]
    impl PrivilegedCommandSession for FakePrivilegedSession {
        fn execute(&mut self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
            self.events.borrow_mut().push(ProvisionEvent::Command {
                program: command.program().to_owned(),
                args: command.args().to_vec(),
            });
            self.outputs
                .pop_front()
                .unwrap_or_else(|| Err(HostError::new("missing fake privileged output")))
        }
    }

    #[cfg(target_os = "macos")]
    impl Drop for FakePrivilegedSession {
        fn drop(&mut self) {
            self.events.borrow_mut().push(ProvisionEvent::Free);
        }
    }

    #[cfg(target_os = "macos")]
    struct FakeProvisionIo {
        events: Rc<RefCell<Vec<ProvisionEvent>>>,
    }

    #[cfg(target_os = "macos")]
    impl ApfsProvisionIo for FakeProvisionIo {
        fn prepare_mountpoint(&self, path: &Path) -> Result<(), HostError> {
            self.events
                .borrow_mut()
                .push(ProvisionEvent::Prepare(path.to_owned()));
            Ok(())
        }

        fn attest_mounted(
            &self,
            path: &Path,
            exact_identifier: &str,
            require_nobrowse: bool,
        ) -> Result<(), HostError> {
            self.events.borrow_mut().push(ProvisionEvent::AttestMounted(
                path.to_owned(),
                exact_identifier.to_owned(),
                require_nobrowse,
            ));
            Ok(())
        }

        fn attest_owner(&self, path: &Path, uid: u32, gid: u32) -> Result<(), HostError> {
            self.events
                .borrow_mut()
                .push(ProvisionEvent::AttestOwner(path.to_owned(), uid, gid));
            Ok(())
        }

        fn write_marker(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
            self.events
                .borrow_mut()
                .push(ProvisionEvent::Marker(path.to_owned(), contents.to_vec()));
            Ok(())
        }
    }

    #[cfg(target_os = "macos")]
    fn provision_info(identifier: &str, container: &str, name: &str, mountpoint: &str) -> Vec<u8> {
        format!(
            "<?xml version=\"1.0\"?><plist version=\"1.0\"><dict>\
             <key>DeviceIdentifier</key><string>{identifier}</string>\
             <key>APFSContainerReference</key><string>{container}</string>\
             <key>VolumeName</key><string>{name}</string>\
             <key>FilesystemType</key><string>apfs</string>\
             <key>MountPoint</key><string>{mountpoint}</string>\
             <key>APFSSnapshot</key><false/>\
             </dict></plist>"
        )
        .into_bytes()
    }

    #[test]
    fn exact_container_is_selected_and_diskutil_argv_is_fixed() {
        let unrelated = container("disk2", &volume("Data", "disk2s1", None));
        let containing = container("disk3", &volume("Data", "disk3s5", Some("/")));
        let mut source = source(plist(&(unrelated + &containing)));
        let gathered = gather_apfs_evidence(
            &mut source,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.statfs,
            StatFsEvidence::Apfs { container: Some(ref value), .. } if value == "disk3"
        ));
        assert_eq!(source.commands.len(), 2);
        assert_eq!(source.commands[0].program(), "/usr/sbin/diskutil");
        assert_eq!(
            source.commands[0].args(),
            ["apfs", "list", "-plist", "disk3"]
        );
        assert_eq!(source.commands[1].args(), ["apfs", "list", "-plist"]);
    }

    #[test]
    fn home_anchor_selects_host_container_when_project_is_on_another_filesystem() {
        let data = volume("Data", "disk3s5", Some("/System/Volumes/Data"));
        let cowshed = volume(APFS_STORE_VOLUME, "disk3s8", None)
            + &volume(APFS_CACHES_VOLUME, "disk3s9", None);
        let project_container = container(
            "disk13",
            &volume("Workspace", "disk13s1", Some("/workspace")),
        );
        let mut source = source(plist(
            &(project_container + &container("disk3", &(data + &cowshed))),
        ));
        source.statfs_overrides.insert(
            PathBuf::from("/workspace/project"),
            StatFsSnapshot {
                fs_type: "asif".to_owned(),
                mount_source: PathBuf::from("/dev/disk13s1"),
                mountpoint: PathBuf::from("/workspace"),
                nobrowse: true,
            },
        );
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
                        .to_json()
                        .unwrap(),
                ),
            },
        );
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed/caches"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Apfs)
                        .to_json()
                        .unwrap(),
                ),
            },
        );

        let plan = plan_native_bootstrap(
            &mut source,
            Path::new("/workspace/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();

        assert_eq!(source.statfs_paths, [PathBuf::from("/Users/alice")]);
        assert!(matches!(
            plan.substrate(),
            crate::storage::bootstrap::SelectedSubstrate::Apfs { container, .. } if container == "disk3"
        ));
        assert_eq!(plan.operations().len(), 2);
        assert!(
            plan.operations()
                .iter()
                .all(|operation| matches!(operation, HostOperation::GuardMountpoint { .. }))
        );
    }

    #[test]
    fn duplicate_container_membership_and_malformed_plist_fail_closed() {
        let duplicated = plist(
            &(container("disk3", &volume("Data", "disk3s5", None))
                + &container("disk3", &volume("Data", "disk3s5", None))),
        );
        let mut duplicate_source = source(duplicated);
        assert!(matches!(
            gather_apfs_evidence(
                &mut duplicate_source,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice")
            ),
            Err(NativeBootstrapError::AmbiguousContainer { matches: 2, .. })
        ));

        for malformed_bytes in [
            b"not a plist".to_vec(),
            plist("<dict><key>ContainerReference</key><string>disk3</string></dict>"),
            plist(&container(
                "disk3",
                "<dict><key>Name</key><string>Data</string></dict>",
            )),
        ] {
            let mut malformed_source = source(malformed_bytes);
            assert!(matches!(
                gather_apfs_evidence(
                    &mut malformed_source,
                    Path::new("/Users/alice/project"),
                    Path::new("/Users/alice")
                ),
                Err(NativeBootstrapError::MalformedPlist(_))
            ));
        }
    }

    #[test]
    fn kernel_mount_identity_and_marker_override_omitted_inventory_mountpoint() {
        let marker = VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
            .to_json()
            .unwrap();
        let volumes = volume(APFS_STORE_VOLUME, "disk3s8", None)
            + &volume("Data", "disk3s5", Some("/System/Volumes/Data"));
        let inventory = plist(&container("disk3", &volumes));
        let mut valid = source(inventory.clone());
        valid.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        let gathered = gather_apfs_evidence(
            &mut valid,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs { store: ExistingStorage::MountedValid { ref exact_identifier }, .. }
                if exact_identifier == "disk3s8"
        ));

        let mut wrong_owner = source(inventory.clone());
        wrong_owner.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        wrong_owner
            .mounted_volumes
            .get_mut(Path::new("/Users/alice/.cowshed"))
            .unwrap()
            .uid = 0;
        let gathered = gather_apfs_evidence(
            &mut wrong_owner,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::MountedIncomplete { ref exact_identifier },
                ..
            } if exact_identifier == "disk3s8"
        ));

        let mut wrong_group = source(inventory.clone());
        wrong_group.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        wrong_group
            .mounted_volumes
            .get_mut(Path::new("/Users/alice/.cowshed"))
            .unwrap()
            .gid = 0;
        let gathered = gather_apfs_evidence(
            &mut wrong_group,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::MountedIncomplete { ref exact_identifier },
                ..
            } if exact_identifier == "disk3s8"
        ));

        let mut wrong_flags = source(inventory.clone());
        wrong_flags.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        wrong_flags
            .mounted_volumes
            .get_mut(Path::new("/Users/alice/.cowshed"))
            .unwrap()
            .nobrowse = false;
        let gathered = gather_apfs_evidence(
            &mut wrong_flags,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::MisMountedIncomplete {
                    ref exact_identifier,
                    ref current_mountpoint,
                },
                ..
            } if exact_identifier == "disk3s8"
                && current_mountpoint == Path::new("/Users/alice/.cowshed")
        ));

        let mut incomplete = source(inventory.clone());
        incomplete.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted { marker: None },
        );
        let gathered = gather_apfs_evidence(
            &mut incomplete,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::MountedIncomplete { ref exact_identifier },
                ..
            } if exact_identifier == "disk3s8"
        ));

        let mut detached = source(inventory.clone());
        let gathered = gather_apfs_evidence(
            &mut detached,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::DetachedIncomplete { ref exact_identifier },
                ..
            } if exact_identifier == "disk3s8"
        ));

        let mut invalid = source(inventory);
        invalid.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted {
                marker: Some(b"{}".to_vec()),
            },
        );
        assert!(matches!(
            gather_apfs_evidence(
                &mut invalid,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice")
            ),
            Err(NativeBootstrapError::InvalidMountedMarker { .. })
        ));

        let duplicates = volume(APFS_STORE_VOLUME, "disk3s8", None)
            + &volume(APFS_STORE_VOLUME, "disk3s9", None)
            + &volume("Data", "disk3s5", None);
        let mut duplicate = source(plist(&container("disk3", &duplicates)));
        assert!(matches!(
            gather_apfs_evidence(
                &mut duplicate,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice")
            ),
            Err(NativeBootstrapError::AmbiguousVolume { .. })
        ));
    }

    #[test]
    fn masked_nonempty_mountpoint_is_refused() {
        let inventory = plist(&container(
            "disk3",
            &volume("Data", "disk3s5", Some("/System/Volumes/Data")),
        ));
        let mut source = source(inventory);
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::NonEmptyDirectoryWithoutMount,
        );
        assert!(matches!(
            gather_apfs_evidence(
                &mut source,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice")
            ),
            Err(NativeBootstrapError::MaskedMountpoint { .. })
        ));
    }

    #[test]
    fn automounted_exact_volume_plans_unprivileged_remount() {
        let volumes = volume(APFS_STORE_VOLUME, "disk3s8", Some("/Volumes/cowshed.store"))
            + &volume(
                APFS_CACHES_VOLUME,
                "disk3s9",
                Some("/Volumes/cowshed.caches"),
            )
            + &volume("Data", "disk3s5", Some("/System/Volumes/Data"));
        let mut source = source(plist(&container("disk3", &volumes)));
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::ReclaimableStub,
        );
        source.mounted_volumes.insert(
            PathBuf::from("/Volumes/cowshed.store"),
            MountedVolumeEvidence {
                exact_identifier: "disk3s8".to_owned(),
                mountpoint: PathBuf::from("/Volumes/cowshed.store"),
                nobrowse: false,
                uid: 0,
                gid: 0,
            },
        );
        source.mounted_volumes.insert(
            PathBuf::from("/Volumes/cowshed.caches"),
            MountedVolumeEvidence {
                exact_identifier: "disk3s9".to_owned(),
                mountpoint: PathBuf::from("/Volumes/cowshed.caches"),
                nobrowse: false,
                uid: 0,
                gid: 0,
            },
        );

        let plan = plan_native_bootstrap(
            &mut source,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        let operations = plan.operations();
        assert!(
            operations.iter().any(|operation| matches!(
                operation,
                HostOperation::ReclaimMountpoint(path)
                    if path == Path::new("/Users/alice/.cowshed")
            )),
            "{operations:?}"
        );
        assert!(
            operations.iter().any(|operation| matches!(
                operation,
                HostOperation::RunCommand(command)
                    if command.args() == ["unmount", "force", "disk3s8"]
            )),
            "{operations:?}"
        );
        assert!(
            operations.iter().any(|operation| matches!(
                operation,
                HostOperation::RunCommand(command)
                    if command.args() == ["unmount", "force", "disk3s9"]
            )),
            "{operations:?}"
        );
        assert!(
            !operations
                .iter()
                .any(|operation| matches!(operation, HostOperation::ProvisionApfsVolumes { .. }))
        );
        assert_eq!(source.commands.len(), 1);
        assert_eq!(
            source.commands[0].args(),
            ["apfs", "list", "-plist", "disk3"]
        );
    }

    #[test]
    fn reclaimable_launchd_stub_classifies_as_mis_mounted_when_volume_exists() {
        let volumes = volume(APFS_STORE_VOLUME, "disk3s8", Some("/Volumes/cowshed.store"))
            + &volume("Data", "disk3s5", Some("/System/Volumes/Data"));
        let mut source = source(plist(&container("disk3", &volumes)));
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::ReclaimableStub,
        );
        source.mounted_volumes.insert(
            PathBuf::from("/Volumes/cowshed.store"),
            MountedVolumeEvidence {
                exact_identifier: "disk3s8".to_owned(),
                mountpoint: PathBuf::from("/Volumes/cowshed.store"),
                nobrowse: false,
                uid: 0,
                gid: 0,
            },
        );

        let gathered = gather_apfs_evidence(
            &mut source,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::MisMountedIncomplete {
                    ref exact_identifier,
                    ref current_mountpoint,
                },
                ..
            } if exact_identifier == "disk3s8"
                && current_mountpoint == Path::new("/Volumes/cowshed.store")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn command_runner_uses_argv_and_preserves_failure_status_and_streams() {
        let seen = RefCell::new(VecDeque::new());
        let command = HostCommand::new(
            "/usr/sbin/diskutil",
            ["apfs", "list", "-plist", "literal;not-shell"],
        );
        let output = run_command_with(&command, |program, args| {
            seen.borrow_mut()
                .push_back((program.to_owned(), args.to_vec()));
            Ok(Output {
                status: std::process::ExitStatus::from_raw(1 << 8),
                stdout: b"ignored".to_vec(),
                stderr: b"diskutil exact failure\n".to_vec(),
            })
        })
        .unwrap();
        assert!(!output.succeeded());
        assert_eq!(output.stderr, b"diskutil exact failure\n");
        assert_eq!(
            HostCommandFailure::new(command, output).to_string(),
            "command failed: executable \"/usr/sbin/diskutil\", argv [\"apfs\", \"list\", \"-plist\", \"literal;not-shell\"], exit status 1; stdout: ignored; stderr: diskutil exact failure"
        );
        assert_eq!(
            seen.into_inner().pop_front().unwrap(),
            (
                PathBuf::from("/usr/sbin/diskutil"),
                vec![
                    "apfs".to_owned(),
                    "list".to_owned(),
                    "-plist".to_owned(),
                    "literal;not-shell".to_owned(),
                ]
            )
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn one_authorization_session_wraps_fixed_argv_for_all_new_volumes() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(plist(&container(
                "disk3",
                &volume("Data", "disk3s5", Some("/System/Volumes/Data")),
            )))),
            Ok(HostCommandOutput::success(
                b"Created new APFS Volume disk3s8\n".to_vec(),
            )),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s8",
                "disk3",
                APFS_STORE_VOLUME,
                "",
            ))),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s8",
                "disk3",
                APFS_STORE_VOLUME,
                "/Users/alice/.cowshed",
            ))),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(
                b"Created new APFS Volume disk3s9\n".to_vec(),
            )),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "",
            ))),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Users/alice/.cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volumes = [
            ApfsVolumeProvision {
                name: APFS_STORE_VOLUME,
                mountpoint: PathBuf::from("/Users/alice/.cowshed"),
                role: VolumeRole::Store,
                kind: ApfsProvisionKind::Create,
            },
            ApfsVolumeProvision {
                name: APFS_CACHES_VOLUME,
                mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
                role: VolumeRole::Caches,
                kind: ApfsProvisionKind::Create,
            },
        ];
        let acquire_events = Rc::clone(&events);
        provision_apfs_volumes_with(
            "disk3",
            &volumes,
            501,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs,
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .unwrap();

        let events = events.borrow();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ProvisionEvent::Acquire))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ProvisionEvent::Free))
                .count(),
            1
        );
        assert!(matches!(events.first(), Some(ProvisionEvent::Acquire)));
        assert!(matches!(events.last(), Some(ProvisionEvent::Free)));
        let commands: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { program, args } => Some((program.as_str(), args)),
                _ => None,
            })
            .collect();
        assert_eq!(commands.len(), 11);
        assert!(
            commands
                .iter()
                .all(|(program, _)| { *program == DISKUTIL || *program == CHOWN })
        );
        assert_eq!(
            commands
                .iter()
                .filter(|(program, _)| *program == CHOWN)
                .collect::<Vec<_>>(),
            [
                &(
                    CHOWN,
                    &vec!["501:20".to_owned(), "/Users/alice/.cowshed".to_owned()]
                ),
                &(
                    CHOWN,
                    &vec![
                        "501:20".to_owned(),
                        "/Users/alice/.cowshed/caches".to_owned()
                    ]
                ),
            ]
        );
        for root in [
            Path::new("/Users/alice/.cowshed"),
            Path::new("/Users/alice/.cowshed/caches"),
        ] {
            let chown = events
                .iter()
                .position(|event| matches!(
                    event,
                    ProvisionEvent::Command { program, args }
                        if program == CHOWN && args.get(1).is_some_and(|path| Path::new(path) == root)
                ))
                .unwrap();
            let owner = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        ProvisionEvent::AttestOwner(path, 501, 20) if path == root
                    )
                })
                .unwrap();
            let marker = events
                .iter()
                .position(|event| {
                    matches!(
                        event,
                        ProvisionEvent::Marker(path, _)
                            if path == &root.join(VOLUME_MARKER_FILE)
                    )
                })
                .unwrap();
            assert!(chown < owner && owner < marker);
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn execution_time_global_inventory_refuses_duplicate_create() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let global = plist(
            &(container("disk3", &volume("Data", "disk3s5", None))
                + &container(
                    "disk7",
                    &volume(
                        APFS_STORE_VOLUME,
                        "disk7s2",
                        Some("/Volumes/cowshed.store"),
                    ),
                )),
        );
        let volume = ApfsVolumeProvision {
            name: APFS_STORE_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed"),
            role: VolumeRole::Store,
            kind: ApfsProvisionKind::Create,
        };
        let acquire_events = Rc::clone(&events);
        let error = provision_apfs_volumes_with(
            "disk3",
            &[volume],
            501,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs: VecDeque::from([Ok(HostCommandOutput::success(global))]),
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .expect_err("an existing reserved-name volume must prevent addVolume");
        assert!(error.to_string().contains("already exists as disk7s2"));
        let events = events.borrow();
        let commands = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { args, .. } => Some(args.as_slice()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(commands, [vec!["apfs", "list", "-plist"].as_slice()]);
        assert!(!events.iter().any(|event| matches!(
            event,
            ProvisionEvent::Prepare(_) | ProvisionEvent::Marker(_, _)
        )));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn markerless_exact_mount_is_repaired_without_create_or_mount() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Users/alice/.cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
            role: VolumeRole::Caches,
            kind: ApfsProvisionKind::RepairMounted {
                exact_identifier: "disk3s9".to_owned(),
            },
        };
        let acquire_events = Rc::clone(&events);
        provision_apfs_volumes_with(
            "disk3",
            &[volume],
            502,
            80,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs,
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .unwrap();

        let events = events.borrow();
        let commands: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { program, args } => Some((program, args)),
                _ => None,
            })
            .collect();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].0, DISKUTIL);
        assert_eq!(commands[0].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[1].0, CHOWN);
        assert_eq!(
            commands[1].1.as_slice(),
            ["502:80", "/Users/alice/.cowshed/caches"]
        );
        let mounted = events
            .iter()
            .position(|event| matches!(event, ProvisionEvent::AttestMounted(_, identifier, true) if identifier == "disk3s9"))
            .unwrap();
        let marker = events
            .iter()
            .position(|event| matches!(event, ProvisionEvent::Marker(_, _)))
            .unwrap();
        assert!(mounted < marker);
        assert!(matches!(events.first(), Some(ProvisionEvent::Acquire)));
        assert!(matches!(events.last(), Some(ProvisionEvent::Free)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn detached_exact_volume_is_reattested_and_recovered_without_recreation() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "",
            ))),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Users/alice/.cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
            role: VolumeRole::Caches,
            kind: ApfsProvisionKind::RecoverDetached {
                exact_identifier: "disk3s9".to_owned(),
            },
        };
        let acquire_events = Rc::clone(&events);
        provision_apfs_volumes_with(
            "disk3",
            &[volume],
            503,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs,
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .unwrap();

        let events = events.borrow();
        let commands: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { program, args } => Some((program, args)),
                _ => None,
            })
            .collect();
        assert_eq!(commands.len(), 4);
        assert_eq!(commands[0].0, DISKUTIL);
        assert_eq!(commands[0].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[1].0, DISKUTIL);
        assert_eq!(
            commands[1].1.as_slice(),
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/Users/alice/.cowshed/caches",
                "disk3s9",
            ]
        );
        assert_eq!(commands[2].0, DISKUTIL);
        assert_eq!(commands[2].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[3].0, CHOWN);
        assert_eq!(
            commands[3].1.as_slice(),
            ["503:20", "/Users/alice/.cowshed/caches"]
        );
        assert!(
            !commands.iter().any(|(_, args)| {
                args.starts_with(&["apfs".to_owned(), "addVolume".to_owned()])
            })
        );
        let mount = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ProvisionEvent::Command { args, .. }
                        if args.first().is_some_and(|arg| arg == "mount")
                )
            })
            .unwrap();
        let attested = events
            .iter()
            .position(|event| matches!(event, ProvisionEvent::AttestMounted(_, _, true)))
            .unwrap();
        let marker = events
            .iter()
            .position(|event| matches!(event, ProvisionEvent::Marker(_, _)))
            .unwrap();
        assert!(mount < attested && attested < marker);
        assert!(matches!(events.first(), Some(ProvisionEvent::Acquire)));
        assert!(matches!(events.last(), Some(ProvisionEvent::Free)));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn mismounted_volume_is_unmounted_and_repaired_inside_one_session() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Volumes/cowshed-wrong",
            ))),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Users/alice/.cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
            role: VolumeRole::Caches,
            kind: ApfsProvisionKind::RepairMisMounted {
                exact_identifier: "disk3s9".to_owned(),
                current_mountpoint: PathBuf::from("/Volumes/cowshed-wrong"),
            },
        };
        let acquire_events = Rc::clone(&events);
        provision_apfs_volumes_with(
            "disk3",
            &[volume],
            504,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs,
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .unwrap();

        let events = events.borrow();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ProvisionEvent::Acquire))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ProvisionEvent::Free))
                .count(),
            1
        );
        let commands: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { program, args } => Some((program, args)),
                _ => None,
            })
            .collect();
        assert_eq!(commands.len(), 5);
        assert_eq!(commands[0].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[1].1.as_slice(), ["unmount", "disk3s9"]);
        assert_eq!(
            commands[2].1.as_slice(),
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/Users/alice/.cowshed/caches",
                "disk3s9",
            ]
        );
        assert_eq!(commands[3].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[4].0, CHOWN);
        assert_eq!(
            commands[4].1.as_slice(),
            ["504:20", "/Users/alice/.cowshed/caches"]
        );
        let pre_attestation = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ProvisionEvent::AttestMounted(path, identifier, false)
                        if path == Path::new("/Volumes/cowshed-wrong")
                            && identifier == "disk3s9"
                )
            })
            .unwrap();
        let unmount = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ProvisionEvent::Command { args, .. }
                        if args.first().is_some_and(|arg| arg == "unmount")
                )
            })
            .unwrap();
        let canonical_attestation = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    ProvisionEvent::AttestMounted(path, identifier, true)
                        if path == Path::new("/Users/alice/.cowshed/caches")
                            && identifier == "disk3s9"
                )
            })
            .unwrap();
        let marker = events
            .iter()
            .position(|event| matches!(event, ProvisionEvent::Marker(_, _)))
            .unwrap();
        assert!(pre_attestation < unmount && unmount < canonical_attestation);
        assert!(canonical_attestation < marker);
        assert!(matches!(events.first(), Some(ProvisionEvent::Acquire)));
        assert!(matches!(events.last(), Some(ProvisionEvent::Free)));
    }

    /// The macOS 26 arm: `diskutil apfs addVolume -nomount` returns a volume
    /// already mounted at `/Volumes/<name>`. The plist shape here is the one
    /// this host's `diskutil info -plist cowshed.store` actually reports.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_auto_mounted_new_volume_is_detached_before_its_private_mount() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(plist(&container(
                "disk3",
                &volume("Data", "disk3s5", Some("/System/Volumes/Data")),
            )))),
            Ok(HostCommandOutput::success(
                b"Created new APFS Volume disk3s8\n".to_vec(),
            )),
            // The system mounted it at the default location despite -nomount.
            Ok(HostCommandOutput::success(provision_info(
                "disk3s8",
                "disk3",
                APFS_STORE_VOLUME,
                "/Volumes/cowshed.store",
            ))),
            // unmount
            Ok(HostCommandOutput::default()),
            // mount at the private mountpoint
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s8",
                "disk3",
                APFS_STORE_VOLUME,
                "/Users/alice/.cowshed",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volumes = [ApfsVolumeProvision {
            name: APFS_STORE_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed"),
            role: VolumeRole::Store,
            kind: ApfsProvisionKind::Create,
        }];
        let acquire_events = Rc::clone(&events);
        provision_apfs_volumes_with(
            "disk3",
            &volumes,
            501,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs,
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .unwrap();

        let events = events.borrow();
        let commands: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { args, .. } => Some(args.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            commands[0].as_slice(),
            ["apfs", "list", "-plist"],
            "global inventory is rechecked inside the authorization session"
        );
        assert_eq!(
            commands[1].as_slice(),
            [
                "apfs",
                "addVolume",
                "disk3",
                "APFS",
                APFS_STORE_VOLUME,
                "-nomount"
            ],
            "-nomount is still requested; the unmount is a fallback, not a replacement"
        );
        assert_eq!(commands[2].as_slice(), ["info", "-plist", "disk3s8"]);
        assert_eq!(
            commands[3].as_slice(),
            ["unmount", "disk3s8"],
            "the default mount is dropped before the private mount"
        );
        assert_eq!(
            commands[4].as_slice(),
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/Users/alice/.cowshed",
                "disk3s8",
            ]
        );
    }

    /// A volume recorded as detached can be auto-mounted by the system between
    /// discovery and the recovery attestation. That is the shape most likely to
    /// strand an existing installation, so it converges the same way.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_detached_volume_found_auto_mounted_is_detached_before_recovery() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Volumes/cowshed.caches",
            ))),
            // unmount, then mount at the private mountpoint
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::default()),
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/Users/alice/.cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volumes = [ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
            role: VolumeRole::Caches,
            kind: ApfsProvisionKind::RecoverDetached {
                exact_identifier: "disk3s9".to_owned(),
            },
        }];
        let acquire_events = Rc::clone(&events);
        provision_apfs_volumes_with(
            "disk3",
            &volumes,
            502,
            80,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs,
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&events),
            },
        )
        .unwrap();

        let events = events.borrow();
        let diskutil: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { program, args } if program == DISKUTIL => {
                    Some(args.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(diskutil[0].as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(diskutil[1].as_slice(), ["unmount", "disk3s9"]);
        assert_eq!(
            diskutil[2].as_slice(),
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/Users/alice/.cowshed/caches",
                "disk3s9",
            ]
        );
        assert!(
            !diskutil
                .iter()
                .any(|args| args.first().is_some_and(|arg| arg == "apfs")),
            "recovery must never recreate the volume"
        );
    }

    /// The pre-26 arm and the fail-closed boundary.
    #[cfg(target_os = "macos")]
    #[test]
    fn created_volume_attestation_admits_only_detached_or_the_default_mount() {
        let attest = |mountpoint: &str| {
            super::super::attest_created_apfs_info(
                &provision_info("disk3s8", "disk3", APFS_STORE_VOLUME, mountpoint),
                "disk3s8",
                "disk3",
                APFS_STORE_VOLUME,
            )
        };

        assert_eq!(
            attest("").unwrap(),
            super::super::CreatedMountState::Unmounted
        );
        assert_eq!(
            attest("/Volumes/cowshed.store").unwrap(),
            super::super::CreatedMountState::AutoMounted
        );

        // A volume mounted anywhere else is not the pristine object being
        // vouched for, and must not be silently unmounted and repurposed.
        let foreign = attest("/Users/alice/somewhere").unwrap_err();
        assert!(
            foreign.to_string().contains("unexpected location"),
            "{foreign}"
        );
        // The default mount of a *different* volume name is equally foreign.
        let mismatched = attest("/Volumes/cowshed.caches").unwrap_err();
        assert!(
            mismatched.to_string().contains("unexpected location"),
            "{mismatched}"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn authorization_denial_and_child_failure_propagate_without_marker_publication() {
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/Users/alice/.cowshed/caches"),
            role: VolumeRole::Caches,
            kind: ApfsProvisionKind::Create,
        };
        let denied_events = Rc::new(RefCell::new(Vec::new()));
        let acquire_events = Rc::clone(&denied_events);
        let denied = provision_apfs_volumes_with(
            "disk3",
            std::slice::from_ref(&volume),
            501,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Err::<FakePrivilegedSession, _>(HostError::new(
                    "Authorization Services status -60005",
                ))
            },
            &FakeProvisionIo {
                events: Rc::clone(&denied_events),
            },
        )
        .unwrap_err();
        assert!(denied.to_string().contains("-60005"));
        assert_eq!(*denied_events.borrow(), [ProvisionEvent::Acquire]);

        let failed_events = Rc::new(RefCell::new(Vec::new()));
        let acquire_events = Rc::clone(&failed_events);
        let failed = provision_apfs_volumes_with(
            "disk3",
            &[volume],
            501,
            20,
            move || {
                acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                Ok(FakePrivilegedSession {
                    events: Rc::clone(&acquire_events),
                    outputs: VecDeque::from([
                        Ok(HostCommandOutput::success(plist(&container("disk3", "")))),
                        Ok(HostCommandOutput::failure(1, "child failed\n")),
                    ]),
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&failed_events),
            },
        )
        .unwrap_err();
        assert!(failed.to_string().contains("child failed"));
        let events = failed_events.borrow();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ProvisionEvent::Free))
                .count(),
            1
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProvisionEvent::Marker(_, _)))
        );
    }

    #[test]
    fn container_reference_is_the_synthesized_disk_of_the_volume_identifier() {
        assert_eq!(container_reference_of("disk3s5"), "disk3");
        assert_eq!(container_reference_of("disk13s1"), "disk13");
        // A sealed system snapshot mounts as `<volume>s<snapshot>`; the container is unchanged.
        assert_eq!(container_reference_of("disk3s1s1"), "disk3");
    }

    #[test]
    fn volume_mounted_outside_inventory_is_remountable_without_setup() {
        let mut evidence_source =
            source_with_caches_inventory_mountpoint_omitted(Some("/Volumes/cowshed.caches"));
        let gathered =
            gather_existing_apfs_evidence(&mut evidence_source, Path::new("/Users/alice"))
                .expect("mis-mounted caches evidence");
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                caches: ExistingStorage::MisMountedIncomplete {
                    ref exact_identifier,
                    ref current_mountpoint,
                },
                ..
            } if exact_identifier == "disk3s9"
                && current_mountpoint == Path::new("/Volumes/cowshed.caches")
        ));

        let mut plan_source =
            source_with_caches_inventory_mountpoint_omitted(Some("/Volumes/cowshed.caches"));
        let plan = plan_existing_host_storage(&mut plan_source, Path::new("/Users/alice"))
            .expect("mis-mounted caches repair plan");
        assert!(mutating_setup_actions(&plan).is_empty());
    }

    #[test]
    fn volume_absent_from_inventory_and_mountpoint_info_requires_provisioning() {
        let mut evidence_source = source_with_caches_inventory_mountpoint_omitted(None);
        let gathered =
            gather_existing_apfs_evidence(&mut evidence_source, Path::new("/Users/alice"))
                .expect("detached caches evidence");
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                caches: ExistingStorage::DetachedIncomplete {
                    ref exact_identifier,
                },
                ..
            } if exact_identifier == "disk3s9"
        ));

        let mut plan_source = source_with_caches_inventory_mountpoint_omitted(None);
        let plan = plan_existing_host_storage(&mut plan_source, Path::new("/Users/alice"))
            .expect("detached caches repair plan");
        assert!(plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::ProvisionApfsVolumes { volumes, .. }
                if volumes.iter().any(|volume| {
                    volume.name() == APFS_CACHES_VOLUME
                        && matches!(
                            volume.kind(),
                            ApfsProvisionKind::RecoverDetached { exact_identifier }
                                if exact_identifier == "disk3s9"
                        )
                })
        )));
        assert!(!mutating_setup_actions(&plan).is_empty());
    }

    #[test]
    fn global_inventory_prevents_create_when_reserved_volume_is_in_another_container() {
        let scoped = plist(&container(
            "disk3",
            &volume("Data", "disk3s5", Some("/System/Volumes/Data")),
        ));
        let global = plist(
            &(container(
                "disk3",
                &volume("Data", "disk3s5", Some("/System/Volumes/Data")),
            ) + &container(
                "disk7",
                &volume(
                    APFS_STORE_VOLUME,
                    "disk7s2",
                    Some("/Volumes/cowshed.store"),
                ),
            )),
        );
        let mut source = source(scoped.clone());
        source.command_outputs = VecDeque::from([
            HostCommandOutput::success(scoped),
            HostCommandOutput::success(global),
        ]);

        let plan = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
            .expect("cross-container volume must be reportable");
        assert!(plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::ReportVolumeIssue { name, detail }
                if *name == APFS_STORE_VOLUME
                    && detail.contains("disk7s2")
                    && detail.contains("disk7")
        )));
        assert!(!plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::ProvisionApfsVolumes { volumes, .. }
                if volumes.iter().any(|volume| {
                    volume.name() == APFS_STORE_VOLUME
                        && matches!(volume.kind(), ApfsProvisionKind::Create)
                })
        )));
        assert!(plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::ProvisionApfsVolumes { volumes, .. }
                if volumes.iter().any(|volume| {
                    volume.name() == APFS_CACHES_VOLUME
                        && matches!(volume.kind(), ApfsProvisionKind::Create)
                })
        )));
        assert_eq!(source.commands[1].args(), ["apfs", "list", "-plist"]);
    }

    #[tokio::test]
    async fn valid_manually_created_volumes_are_pinned_without_reprovisioning() {
        let mut source = healthy_existing_source();
        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect("healthy manually-created volumes plan");
        assert!(!snapshot.plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::ProvisionApfsVolumes { .. }
        )));
        assert!(snapshot.plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::PinVolumesInFstab { pins }
                if pins.len() == 2
                    && pins.iter().any(|pin| pin.volume_uuid == "disk3s8-UUID")
                    && pins.iter().any(|pin| pin.volume_uuid == "disk3s9-UUID")
        )));
        assert!(setup_requires_authorization(&snapshot.plan));

        let read_only_host = Arc::new(ReadOnlyValidationHost::default());
        let error = execute_native_bootstrap_plan(
            &snapshot.plan,
            NativeBootstrapMode::ExistingOnly,
            Arc::clone(&read_only_host),
            &ValidationLane::default(),
        )
        .await
        .expect_err("existing-only lane must reject fstab installation");
        assert!(matches!(
            error,
            NativeBootstrapError::StorageSetupRequired { .. }
        ));
        assert_eq!(read_only_host.mutation_calls.load(Ordering::SeqCst), 0);

        let host = Arc::new(ReadOnlyValidationHost::default());
        execute_native_bootstrap_plan(
            &snapshot.plan,
            NativeBootstrapMode::Provision,
            Arc::clone(&host),

            &ValidationLane::default(),
        )
        .await
        .expect("provision lane applies fstab pin");
        assert_eq!(host.authorization_calls.load(Ordering::SeqCst), 1);
        assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn uninstall_plan_removes_only_tagged_pins_and_is_idempotent_when_clean() {
        let tagged = "UUID=STORE /private/cowshed/store apfs rw # cowshed created volume labelled cowshed.store\n\
LABEL=nix /nix apfs rw\n\
UUID=CACHES /private/cowshed/caches apfs rw # cowshed created volume labelled cowshed.caches\n";
        let plan = host_uninstall_plan_from_text(Path::new("/Users/alice"), tagged)
            .expect("tagged uninstall plan");
        assert_eq!(
            plan,
            HostUninstallPlan {
                pins_to_remove: vec![
                    "cowshed.store".to_owned(),
                    "cowshed.caches".to_owned()
                ],
                requires_authorization: true,
            }
        );

        let clean = host_uninstall_plan_from_text(
            Path::new("/Users/alice"),
            "LABEL=nix /nix apfs rw\n",
        )
        .expect("clean uninstall plan");
        assert_eq!(
            clean,
            HostUninstallPlan {
                pins_to_remove: Vec::new(),
                requires_authorization: false,
            }
        );
    }

    #[test]
    fn healthy_host_with_current_fstab_has_zero_mutation_plan() {
        let mut first_source = healthy_existing_source();
        let first = prepare_setup_snapshot(&mut first_source, Path::new("/Users/alice"), "")
            .expect("pin plan");
        let PlannedFstab::NeedsPin(pins) = first.fstab else {
            panic!("empty fstab must need pins");
        };
        let current = build_fstab("", &pins).expect("desired fstab");

        let mut source = healthy_existing_source();
        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), &current)
            .expect("healthy setup plan");
        assert_eq!(host_setup_actions(&snapshot), Vec::<String>::new());
        assert!(!setup_requires_authorization(&snapshot.plan));
        assert!(matches!(snapshot.fstab, PlannedFstab::AlreadyCurrent));
    }

    #[tokio::test]
    async fn existing_host_storage_returns_roots_and_queries_only_home_without_mutation() {
        let mut source = healthy_existing_source();
        let plan = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
            .expect("healthy plan");

        assert_eq!(source.statfs_paths, [PathBuf::from("/Users/alice")]);
        assert_eq!(source.commands.len(), 1);
        assert_eq!(source.commands[0].program(), DISKUTIL);
        assert_eq!(
            source.commands[0].args(),
            ["apfs", "list", "-plist", "disk3"]
        );
        assert!(
            plan.operations()
                .iter()
                .all(|operation| matches!(operation, HostOperation::GuardMountpoint { .. }))
        );

        let host = Arc::new(ReadOnlyValidationHost::default());
        let lane = ValidationLane::default();
        let validated = validate_existing_plan(&plan, Arc::clone(&host), &lane)
            .await
            .expect("existing storage validates");

        assert_eq!(validated.home(), Path::new("/Users/alice"));
        assert_eq!(validated.store(), Path::new("/Users/alice/.cowshed"));
        assert_eq!(
            validated.caches(),
            Path::new("/Users/alice/.cowshed/caches")
        );
        assert_eq!(
            validated.telemetry(),
            Path::new("/Users/alice/.cowshed/telemetry")
        );
        assert_eq!(host.inspections.load(Ordering::SeqCst), 2);
        assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.authorization_calls.load(Ordering::SeqCst), 0);
        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn missing_marker_is_setup_required_before_executor_dispatch() {
        let mut source = healthy_existing_source();
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed"),
            MountpointState::Mounted { marker: None },
        );
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed/caches"),
            MountpointState::Mounted { marker: None },
        );
        let plan = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
            .expect("missing marker produces a repair plan");
        assert!(
            plan.operations()
                .iter()
                .any(|operation| matches!(operation, HostOperation::ProvisionApfsVolumes { .. }))
        );

        let host = Arc::new(ReadOnlyValidationHost::default());
        let lane = ValidationLane::default();
        let error = validate_existing_plan(&plan, Arc::clone(&host), &lane)
            .await
            .expect_err("existing-only validation must reject marker repair");

        assert_eq!(error.code, crate::error::ErrorCode::EnvironmentMissing);
        assert_eq!(error.hint, "cowshed setup");
        assert!(error.message.contains("storage setup is required"));
        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(host.inspections.load(Ordering::SeqCst), 0);
        assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.authorization_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn wrong_marker_is_an_operational_setup_error() {
        let mut source = healthy_existing_source();
        source.mountpoints.insert(
            PathBuf::from("/Users/alice/.cowshed/caches"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
                        .to_json()
                        .expect("wrong marker"),
                ),
            },
        );

        let native = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
            .expect_err("wrong role must fail closed");
        assert!(matches!(
            native,
            NativeBootstrapError::InvalidMountedMarker { .. }
        ));
        let error = existing_host_storage_error(native);
        assert_eq!(error.code, crate::error::ErrorCode::EnvironmentMissing);
        assert_eq!(error.hint, "cowshed setup");
    }

    #[tokio::test]
    async fn wrong_owner_and_flags_require_setup_without_mutating_dispatch() {
        for unsafe_evidence in ["owner", "flags"] {
            let mut source = healthy_existing_source();
            for path in [
                Path::new("/Users/alice/.cowshed"),
                Path::new("/Users/alice/.cowshed/caches"),
            ] {
                let mounted = source
                    .mounted_volumes
                    .get_mut(path)
                    .expect("mounted volume evidence");
                match unsafe_evidence {
                    "owner" => mounted.uid = 0,
                    "flags" => mounted.nobrowse = false,
                    _ => unreachable!(),
                }
            }

            let plan = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
                .expect("unsafe evidence produces a repair plan");
            let host = Arc::new(ReadOnlyValidationHost::default());
            let lane = ValidationLane::default();
            let error = validate_existing_plan(&plan, Arc::clone(&host), &lane)
                .await
                .expect_err("existing-only validation rejects unsafe roots");

            assert_eq!(error.code, crate::error::ErrorCode::EnvironmentMissing);
            assert_eq!(error.hint, "cowshed setup");
            assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
            assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 0);
            assert_eq!(host.authorization_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn absent_storage_setup_plan_is_rejected_without_authorization() {
        let inventory = plist(&container(
            "disk3",
            &volume("Data", "disk3s5", Some("/System/Volumes/Data")),
        ));
        let mut source = source(inventory);
        let plan = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
            .expect("absence produces a setup plan");
        let host = Arc::new(ReadOnlyValidationHost::default());
        let lane = ValidationLane::default();

        let error = validate_existing_plan(&plan, Arc::clone(&host), &lane)
            .await
            .expect_err("existing-only validation rejects provisioning");

        assert_eq!(error.code, crate::error::ErrorCode::EnvironmentMissing);
        assert_eq!(error.hint, "cowshed setup");
        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.authorization_calls.load(Ordering::SeqCst), 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn atomic_marker_is_mode_0600_and_refuses_symlink_destination() {
        use std::os::unix::fs::symlink;

        let directory =
            std::env::temp_dir().join(format!("cowshed-bootstrap-native-test-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let marker = directory.join(VOLUME_MARKER_FILE);
        write_marker_atomic(&marker, b"first\n").unwrap();
        assert_eq!(fs::read(&marker).unwrap(), b"first\n");
        assert_eq!(
            fs::metadata(&marker).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let target = directory.join("target");
        fs::write(&target, b"unchanged").unwrap();
        fs::remove_file(&marker).unwrap();
        symlink(&target, &marker).unwrap();
        assert!(write_marker_atomic(&marker, b"attack").is_err());
        assert_eq!(fs::read(&target).unwrap(), b"unchanged");
        fs::remove_dir_all(&directory).unwrap();
    }
}
