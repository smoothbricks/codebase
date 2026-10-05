//! macOS host adapter: kernel APFS inventory, Authorization Services, diskutil, fstab.
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use plist::Value;
use tokio::sync::oneshot;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::super::{
    APFS_CACHES_VOLUME, APFS_STORE_VOLUME, ApfsProvisionKind, ApfsVolumeProvision, BlockingLane,
    BootstrapEvidence, BootstrapExecutionError, BootstrapHost, BootstrapPlan, CanonicalRoots,
    CreatedMountState, DISKUTIL, ExistingStorage, HostCommand, HostCommandFailure,
    HostCommandOutput, HostError, HostOperation, MOUNT_SERVICE_PLIST, MountpointState,
    StatFsEvidence, SubstrateKind, TokioBlockingLane, VOLUME_MARKER_FILE, ValidatedHostStorage,
    VolumeMarker, VolumeRole, attest_created_apfs_info, execute_bootstrap_operation,
    parse_created_apfs_identifier, plan_bootstrap, require_mounted_marker, select_substrate,
};
use super::shared::{
    FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan, HostSetupReport,
    HostUninstallPlan, NativeBootstrapError, NativeBootstrapMode, PIN_FSTAB_ACTION,
    SystemBootstrapHost, UninstallFstabOutcome, UninstallReport, UninstallServiceOutcome,
    VolumeOutcome, VolumeState, execute_native_bootstrap_plan, existing_host_storage_error,
    platform_host_error, provision_volumes_action, setup_execution_error, write_marker_action,
};
use crate::apfs::{RegisteredApfsContainer, registered_apfs_containers};
use crate::error::CowshedError;
use crate::fork_lock::{Run as _, Spawn as _};
use crate::storage::apfs::native::{
    KernelMountSnapshot, KernelMountSource, SystemKernelMountSource,
};
use crate::storage::fstab::{COWSHED_FSTAB_TAG, FstabPin, build_fstab};

const DISKUTIL_PROBE_DEADLINE: Duration = Duration::from_secs(5);
const DISKUTIL_MOUNT_DEADLINE: Duration = Duration::from_secs(30);
const KILLALL: &str = "/usr/bin/killall";
const MKDIR: &str = "/bin/mkdir";
const RM: &str = "/bin/rm";
const RMDIR: &str = "/bin/rmdir";
const CHOWN: &str = "/usr/sbin/chown";
const AUTHORIZED_OUTPUT_LIMIT: usize = 1024 * 1024;
const PRIVILEGED_SHELL: &str = "/bin/sh";
/// POSIX `sh -c` body. `$0` is the absolute tool, `$@` its argv.
///
/// `AuthorizationExecuteWithPrivileges` reports that the tool launched, not
/// waitpid. The wrapper appends the child exit status onto the communications
/// pipe so callers' `succeeded()` checks observe the real outcome.
const PRIVILEGED_STATUS_WRAPPER: &str =
    r#""$0" "$@"; status=$?; printf '\n--cowshed-aewp-exit:%d--\n' "$status"; exit "$status""#;
const FSTAB: &str = "/etc/fstab";
const INSTALL: &str = "/usr/bin/install";
const LAUNCHCTL: &str = "/bin/launchctl";
const SECURITY: &str = "/usr/bin/security";
const SYSTEM_KEYCHAIN: &str = "/Library/Keychains/System.keychain";
const APFS_USER_AGENT: &str = "/System/Library/CoreServices/APFSUserAgent";
const CS_USER_AGENT: &str = "/System/Library/CoreServices/CSUserAgent";
const VOLUME_KEYCHAIN_LABELS: [&str; 2] = [APFS_STORE_VOLUME, APFS_CACHES_VOLUME];
const MOUNT_SERVICE_LABEL: &str = "dev.cowshed.storage";
const MOUNT_SERVICE_TARGET: &str = "system/dev.cowshed.storage";
const MOUNT_SERVICE_DIRECTORY: &str = "/Library/Application Support/dev.cowshed";
const MOUNT_SERVICE_SCRIPT: &str = "/Library/Application Support/dev.cowshed/mount-volumes.sh";
const AUTHORIZATION_DENIED: i32 = -60005;
const AUTHORIZATION_CANCELED: i32 = -60006;

fn is_diskutil_mount(command: &HostCommand) -> bool {
    command.program() == DISKUTIL
        && command
            .args()
            .first()
            .is_some_and(|argument| argument == "mount")
}

fn arbitration_probe(device: &str) -> Result<(), HostError> {
    let probe = HostCommand::new(DISKUTIL, ["info", device]);
    spawn_with_deadline(Path::new(DISKUTIL), probe.args(), DISKUTIL_PROBE_DEADLINE).map(|_| ())
}

impl BootstrapHost for SystemBootstrapHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        Err(platform_host_error("ZFS bootstrap delegation"))
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        inspect_system_mountpoint(path)
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        ensure_supported_host()?;
        if is_diskutil_mount(command) {
            let device = command.args().last().expect("mount argv carries a device");
            arbitration_probe(device).map_err(|_| {
                HostError::new(
                    "disk arbitration daemon did not answer within 5s; it is wedged and needs a restart (cowshed setup performs this)",
                )
            })?;
            return spawn_with_deadline(
                Path::new(command.program()),
                command.args(),
                DISKUTIL_MOUNT_DEADLINE,
            )
            .map(HostCommandOutput::from);
        }
        run_command_with(command, |program, args| {
            Command::new(program).args(args).output_locked()
        })
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        ensure_supported_host()?;
        require_host_canonical(path)?;
        fs::create_dir_all(path).map_err(|source| host_io_error("create directory", path, source))
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        reclaim_system_mountpoint(path)
    }

    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        ensure_supported_host()?;
        {
            let uid = unsafe { libc::getuid() };
            let gid = unsafe { libc::getgid() };
            let mut session = MacAuthorizationSession::acquire()?;
            for volume in volumes {
                create_authorized_directory(&mut session, volume.mountpoint())?;
            }
            provision_apfs_volumes_in_session(
                container,
                volumes,
                uid,
                gid,
                &mut session,
                &SystemApfsProvisionIo,
            )
        }
    }

    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        write_marker_atomic(path, contents)
    }
    fn pin_volumes_in_fstab(&self, pins: &[FstabPin]) -> Result<(), HostError> {
        ensure_supported_host()?;
        {
            let mut session = MacAuthorizationSession::acquire()?;
            pin_volumes_in_fstab_with(&mut session, pins).map(|_| ())
        }
    }
}

struct AuthorizedBootstrapHost {
    // Sequential reuse of one Authorization Services session across &self host
    // methods. The blocking lane never overlaps calls; this is not a lock for
    // concurrency.
    session: Mutex<MacAuthorizationSession>,
}

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

impl BootstrapHost for AuthorizedBootstrapHost {
    fn verify_zfs_delegation(&self, pool: &str, required_root: &str) -> Result<(), HostError> {
        SystemBootstrapHost.verify_zfs_delegation(pool, required_root)
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        SystemBootstrapHost.inspect_mountpoint(path)
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        create_authorized_directory(&mut *self.session()?, path)
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        reclaim_authorized_mountpoint(&mut *self.session()?, path)
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        if is_diskutil_mount(command) {
            let device = command.args().last().expect("mount argv carries a device");
            if arbitration_probe(device).is_err() {
                // The daemon is wedged. Restart it through the already-open authorization
                // session (killall needs root; launchd respawns it on demand), wait out the
                // respawn, and prove responsiveness before re-attempting the mount.
                let restart = HostCommand::new(KILLALL, ["diskarbitrationd"]);
                self.session()?.execute(&restart)?;
                std::thread::sleep(Duration::from_secs(3));
                arbitration_probe(device).map_err(|_| {
                    HostError::new("disk arbitration daemon remained unresponsive after a restart")
                })?;
            }
        }
        if [DISKUTIL, INSTALL, LAUNCHCTL, RM, SECURITY].contains(&command.program()) {
            return self.session()?.execute(command);
        }
        SystemBootstrapHost.run_command(command)
    }

    fn run_command_with_input(
        &self,
        command: &HostCommand,
        input: &[u8],
    ) -> Result<HostCommandOutput, HostError> {
        if command.program() != DISKUTIL {
            return Err(HostError::new(format!(
                "refusing privileged standard input for {:?}",
                command.program()
            )));
        }
        self.session()?.execute_with_input(command, input)
    }

    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let mut session = self.session()?;
        for volume in volumes {
            create_authorized_directory(&mut *session, volume.mountpoint())?;
        }
        provision_apfs_volumes_in_session(
            container,
            volumes,
            uid,
            gid,
            &mut *session,
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
/// exact APFS inventory, ownership, mount-flag, and marker evidence as setup, but never executes
/// the resulting plan. A detached or mis-mounted volume is reported as setup-required so callers
/// such as gateways and cache services cannot mutate host storage during startup validation.
pub async fn validate_existing_host_storage(home: &Path) -> crate::Result<ValidatedHostStorage> {
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

/// Everything a read-only validation would have had to do, named for the
/// operator. Only `GuardMountpoint` is silent: guarding a mountpoint asserts an
/// invariant rather than requesting work.
///
/// This lives in the macOS adapter because only macOS plans host storage; the
/// Linux entrypoints fail closed before a `BootstrapPlan` exists. Its
/// counterpart is `shared::mutating_setup_actions`, which keeps the inverse
/// policy and matches `HostOperation` exhaustively too, so a new variant cannot
/// compile in one table and vanish from the other.
fn read_only_validation_actions(plan: &BootstrapPlan) -> Vec<String> {
    plan.operations()
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::GuardMountpoint { .. } => None,
            HostOperation::VerifyZfsDelegation { required_root, .. } => {
                Some(format!("verify delegated ZFS root {required_root}"))
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
            HostOperation::RunCommand(command) => Some(format!(
                "run {} {}",
                command.program(),
                command.args().join(" ")
            )),
            HostOperation::ProvisionApfsVolumes { volumes, .. } => {
                Some(provision_volumes_action(volumes))
            }
            HostOperation::WriteMarkerAtomic { path, .. } => Some(write_marker_action(path)),
            HostOperation::PinVolumesInFstab { .. } => Some(PIN_FSTAB_ACTION.to_owned()),
            HostOperation::ReportVolumeIssue { detail, .. } => Some(detail.to_owned()),
        })
        .collect()
}

async fn validate_existing_plan<H, L>(
    plan: &BootstrapPlan,
    _host: Arc<H>,
    _lane: &L,
) -> crate::Result<ValidatedHostStorage>
where
    H: BootstrapHost + 'static,
    L: BlockingLane,
{
    let actions = read_only_validation_actions(plan);
    if !actions.is_empty() {
        return Err(existing_host_storage_error(
            NativeBootstrapError::StorageSetupRequired {
                actions,
                hint: "cowshed setup",
            },
        ));
    }
    Ok(ValidatedHostStorage::new(
        plan.home().to_owned(),
        plan.roots().clone(),
    ))
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
    actions: Vec<HostAction>,
    classified: Vec<ClassifiedVolume>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MountServiceFiles {
    script: Vec<u8>,
    plist: Vec<u8>,
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn desired_mount_service(pins: &[FstabPin]) -> Result<MountServiceFiles, NativeBootstrapError> {
    let mut mounts = String::new();
    for pin in pins {
        Uuid::parse_str(&pin.volume_uuid).map_err(|_| {
            NativeBootstrapError::MalformedApfsInventory(format!(
                "{} has invalid APFS volume UUID {:?}",
                pin.label, pin.volume_uuid
            ))
        })?;
        let mountpoint = pin.mountpoint.to_str().ok_or_else(|| {
            NativeBootstrapError::Host(HostError::new(format!(
                "mountpoint is not UTF-8: {:?}",
                pin.mountpoint
            )))
        })?;
        mounts.push_str(&format!(
            "mount_volume {} {} {}\n",
            shell_quote(&pin.label),
            shell_quote(&pin.volume_uuid),
            shell_quote(mountpoint)
        ));
    }
    let script = format!(
        r#"#!/bin/sh
set -eu
umask 022

mount_volume() {{
    label=$1
    uuid=$2
    mountpoint=$3
    info=$(/usr/sbin/diskutil info -plist "$uuid")
    current=$(printf '%s' "$info" | /usr/bin/plutil -extract MountPoint raw -o - - 2>/dev/null || :)

    if [ "$current" = "$mountpoint" ]; then
        return 0
    fi
    if [ -n "$current" ]; then
        echo "cowshed storage: $uuid is mounted at $current, expected $mountpoint" >&2
        return 1
    fi
    if [ -L "$mountpoint" ]; then
        echo "cowshed storage: refusing symlink mountpoint $mountpoint" >&2
        return 1
    fi
    if [ ! -d "$mountpoint" ]; then
        /bin/mkdir -p "$mountpoint"
    fi
    if [ -n "$(/bin/ls -A "$mountpoint")" ]; then
        echo "cowshed storage: refusing nonempty mountpoint $mountpoint" >&2
        return 1
    fi

    {SECURITY} find-generic-password -a "$label" -s "$label" -w {SYSTEM_KEYCHAIN} \
        | /usr/sbin/diskutil apfs unlockVolume "$uuid" -nomount -stdinpassphrase
    /usr/sbin/diskutil mount -nobrowse -mountPoint "$mountpoint" "$uuid"
    info=$(/usr/sbin/diskutil info -plist "$uuid")
    current=$(printf '%s' "$info" | /usr/bin/plutil -extract MountPoint raw -o - -)
    if [ "$current" != "$mountpoint" ]; then
        echo "cowshed storage: $uuid mounted at $current, expected $mountpoint" >&2
        return 1
    fi
}}

{mounts}"#
    );
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{MOUNT_SERVICE_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/sh</string>
    <string>{MOUNT_SERVICE_SCRIPT}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
"#
    );
    Ok(MountServiceFiles {
        script: script.into_bytes(),
        plist: plist.into_bytes(),
    })
}

fn mount_service_files(
    snapshot: &SetupSnapshot,
) -> Result<Option<MountServiceFiles>, NativeBootstrapError> {
    let pins = snapshot
        .classified
        .iter()
        .map(|volume| {
            volume.volume_uuid.as_ref().map(|uuid| FstabPin {
                volume_uuid: uuid.clone(),
                mountpoint: volume.mountpoint.clone(),
                label: volume.name.to_owned(),
            })
        })
        .collect::<Option<Vec<_>>>();
    pins.as_deref().map(desired_mount_service).transpose()
}

fn mount_service_loaded() -> bool {
    Command::new(LAUNCHCTL)
        .args(["print", MOUNT_SERVICE_TARGET])
        .output_locked()
        .is_ok_and(|output| output.status.success())
}

fn root_owned_path_has_mode(path: &Path, directory: bool, mode: u32) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| {
        let kind_matches = if directory {
            metadata.file_type().is_dir()
        } else {
            metadata.file_type().is_file()
        };
        kind_matches
            && metadata.uid() == 0
            && metadata.gid() == 0
            && metadata.mode() & 0o7777 == mode
    })
}

fn mount_service_contents_are_current(
    files: &MountServiceFiles,
    script: Option<&[u8]>,
    plist: Option<&[u8]>,
    loaded: bool,
) -> bool {
    script == Some(files.script.as_slice()) && plist == Some(files.plist.as_slice()) && loaded
}

fn mount_service_is_current(files: &MountServiceFiles) -> bool {
    let script = fs::read(MOUNT_SERVICE_SCRIPT).ok();
    let plist = fs::read(MOUNT_SERVICE_PLIST).ok();
    root_owned_path_has_mode(Path::new(MOUNT_SERVICE_DIRECTORY), true, 0o755)
        && root_owned_path_has_mode(Path::new(MOUNT_SERVICE_SCRIPT), false, 0o755)
        && root_owned_path_has_mode(Path::new(MOUNT_SERVICE_PLIST), false, 0o644)
        && mount_service_contents_are_current(
            files,
            script.as_deref(),
            plist.as_deref(),
            mount_service_loaded(),
        )
}

fn add_mount_service_action(snapshot: &mut SetupSnapshot) -> Result<(), NativeBootstrapError> {
    if let Some(files) = mount_service_files(snapshot)?
        && !mount_service_is_current(&files)
    {
        snapshot.actions.push(HostAction::InstallMountService {
            label: MOUNT_SERVICE_LABEL.to_owned(),
        });
    }
    Ok(())
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
            ExistingStorage::Absent => None,
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
            "volume UUIDs will be available after creation".to_owned()
        };
        PlannedFstab::Deferred(reason)
    };
    let actions = build_host_actions(&gathered.volumes, &fstab)?;
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
        actions,
        classified: gathered.volumes,
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
            current_mountpoint, ..
        } => VolumeState::MisMounted {
            mounted_at: current_mountpoint.clone(),
        },
        ExistingStorage::FoundElsewhere {
            container,
            device,
            mounted_at,
            ..
        } => VolumeState::FoundElsewhere {
            container: container.clone(),
            device: device.clone(),
            mounted_at: mounted_at.clone(),
        },
    }
}

fn volume_action(storage: &ExistingStorage) -> &'static str {
    match storage {
        ExistingStorage::Absent => "created",
        ExistingStorage::MountedValid { .. } => "already-current",
        ExistingStorage::MountedIncomplete { .. } => "repaired",
        ExistingStorage::ExistingUnmounted { .. } | ExistingStorage::DetachedIncomplete { .. } => {
            "mounted"
        }
        ExistingStorage::MisMountedIncomplete { .. } => "remounted",
        ExistingStorage::FoundElsewhere {
            mounted_at: Some(_),
            ..
        } => "remounted",
        ExistingStorage::FoundElsewhere {
            mounted_at: None, ..
        } => "mounted",
    }
}
fn build_host_actions(
    volumes: &[ClassifiedVolume],
    fstab: &PlannedFstab,
) -> Result<Vec<HostAction>, NativeBootstrapError> {
    let mut reclaimable_stubs = Vec::new();
    for path in volumes
        .iter()
        .flat_map(|volume| volume.reclaimable_stubs.iter())
    {
        if !reclaimable_stubs.contains(path) {
            reclaimable_stubs.push(path.clone());
        }
    }
    let mut actions = Vec::new();
    if !reclaimable_stubs.is_empty() {
        actions.push(HostAction::ReclaimStubs {
            paths: reclaimable_stubs,
        });
    }
    let mut encrypt_actions = Vec::new();
    for volume in volumes {
        let existing_identity = || {
            let uuid = volume.volume_uuid.clone().ok_or_else(|| {
                NativeBootstrapError::MalformedApfsInventory(format!(
                    "{} has no APFS volume UUID",
                    volume.name
                ))
            })?;
            let size_bytes = volume.size_bytes.ok_or_else(|| {
                NativeBootstrapError::MalformedApfsInventory(format!(
                    "{} has no APFS container capacity",
                    volume.name
                ))
            })?;
            Ok::<_, NativeBootstrapError>((uuid, size_bytes))
        };
        match &volume.storage {
            ExistingStorage::Absent => actions.push(HostAction::CreateVolume {
                name: volume.name.to_owned(),
                container: volume.container.clone(),
                mount_at: volume.mountpoint.clone(),
            }),
            ExistingStorage::MountedValid { .. } => {}
            ExistingStorage::MountedIncomplete { .. } => {
                let (uuid, size_bytes) = existing_identity()?;
                actions.push(HostAction::RepairMounted {
                    name: volume.name.to_owned(),
                    uuid,
                    size_bytes,
                    mounted_at: volume.mountpoint.clone(),
                    mount_at: volume.mountpoint.clone(),
                });
            }
            ExistingStorage::ExistingUnmounted { .. }
            | ExistingStorage::DetachedIncomplete { .. } => {
                let (uuid, size_bytes) = existing_identity()?;
                actions.push(HostAction::MountExisting {
                    name: volume.name.to_owned(),
                    uuid,
                    size_bytes,
                    mount_at: volume.mountpoint.clone(),
                });
            }
            ExistingStorage::MisMountedIncomplete {
                current_mountpoint, ..
            } => {
                let (uuid, size_bytes) = existing_identity()?;
                actions.push(HostAction::RepairMounted {
                    name: volume.name.to_owned(),
                    uuid,
                    size_bytes,
                    mounted_at: current_mountpoint.clone(),
                    mount_at: volume.mountpoint.clone(),
                });
            }
            ExistingStorage::FoundElsewhere { mounted_at, .. } => {
                let (uuid, size_bytes) = existing_identity()?;
                if let Some(mounted_at) = mounted_at {
                    actions.push(HostAction::RepairMounted {
                        name: volume.name.to_owned(),
                        uuid,
                        size_bytes,
                        mounted_at: mounted_at.clone(),
                        mount_at: volume.mountpoint.clone(),
                    });
                } else {
                    actions.push(HostAction::MountExisting {
                        name: volume.name.to_owned(),
                        uuid,
                        size_bytes,
                        mount_at: volume.mountpoint.clone(),
                    });
                }
            }
        }
        if volume.file_vault == Some(false) {
            let (uuid, size_bytes) = existing_identity()?;
            encrypt_actions.push(HostAction::EncryptVolume {
                name: volume.name.to_owned(),
                uuid,
                size_bytes,
            });
        }
    }
    actions.extend(encrypt_actions);
    if let PlannedFstab::NeedsPin(pins) = fstab {
        actions.extend(pins.iter().map(|pin| HostAction::PinFstab {
            uuid: pin.volume_uuid.clone(),
            mount_at: pin.mountpoint.clone(),
        }));
    }
    Ok(actions)
}

fn host_setup_actions(snapshot: &SetupSnapshot) -> Vec<HostAction> {
    snapshot.actions.clone()
}

fn setup_requires_authorization(snapshot: &SetupSnapshot) -> bool {
    snapshot.actions.iter().any(|action| {
        matches!(
            action,
            HostAction::EncryptVolume { .. } | HostAction::InstallMountService { .. }
        )
    }) || snapshot
        .plan
        .operations()
        .iter()
        .any(|operation| match operation {
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

fn action_volume_name(action: &HostAction) -> Option<&str> {
    match action {
        HostAction::CreateVolume { name, .. }
        | HostAction::MountExisting { name, .. }
        | HostAction::RepairMounted { name, .. }
        | HostAction::EncryptVolume { name, .. } => Some(name),
        HostAction::PinFstab { .. }
        | HostAction::ReclaimStubs { .. }
        | HostAction::InstallMountService { .. } => None,
    }
}

fn storage_device(storage: &ExistingStorage) -> Option<&str> {
    match storage {
        ExistingStorage::Absent => None,
        ExistingStorage::MountedValid { exact_identifier }
        | ExistingStorage::MountedIncomplete { exact_identifier }
        | ExistingStorage::DetachedIncomplete { exact_identifier }
        | ExistingStorage::MisMountedIncomplete {
            exact_identifier, ..
        }
        | ExistingStorage::ExistingUnmounted {
            exact_identifier, ..
        } => Some(exact_identifier),
        ExistingStorage::FoundElsewhere { device, .. } => Some(device),
    }
}

fn operations_for_volume(plan: &BootstrapPlan, volume: &ClassifiedVolume) -> Vec<HostOperation> {
    for operation in plan.operations() {
        if let HostOperation::ProvisionApfsVolumes { container, volumes } = operation
            && let Some(provision) = volumes
                .iter()
                .find(|provision| provision.name() == volume.name)
        {
            return vec![HostOperation::ProvisionApfsVolumes {
                container: container.clone(),
                volumes: vec![provision.clone()],
            }];
        }
    }
    let device = storage_device(&volume.storage);
    plan.operations()
        .iter()
        .filter(|operation| match operation {
            HostOperation::GuardMountpoint { path, role, .. } => {
                path == &volume.mountpoint && *role == volume.role
            }
            HostOperation::EnsureDirectory(path) | HostOperation::ReclaimMountpoint(path) => {
                path == &volume.mountpoint
            }
            HostOperation::MountApfsVolume { mountpoint, .. } => mountpoint == &volume.mountpoint,
            HostOperation::RunCommand(command) => device
                .is_some_and(|device| command.args().iter().any(|argument| argument == device)),
            HostOperation::ReportVolumeIssue { name, .. } => *name == volume.name,
            HostOperation::VerifyZfsDelegation { .. }
            | HostOperation::ProvisionApfsVolumes { .. }
            | HostOperation::WriteMarkerAtomic { .. }
            | HostOperation::PinVolumesInFstab { .. } => false,
        })
        .cloned()
        .collect()
}

async fn execute_setup_operations(
    operations: &[HostOperation],
    host: Arc<dyn BootstrapHost>,
) -> Result<(), CowshedError> {
    for operation in operations {
        execute_bootstrap_operation(operation, Arc::clone(&host), &TokioBlockingLane)
            .await
            .map_err(NativeBootstrapError::Execution)
            .map_err(|error| setup_execution_error(error, "cowshed setup"))?;
    }
    Ok(())
}

fn required_host_command(host: &dyn BootstrapHost, command: HostCommand) -> Result<(), HostError> {
    let output = host.run_command(&command)?;
    if output.succeeded() {
        return Ok(());
    }
    Err(HostError::new(
        HostCommandFailure::new(command, output).to_string(),
    ))
}
fn random_volume_password() -> Result<Zeroizing<String>, HostError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| HostError::new(format!("cannot generate FileVault password: {error}")))?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut password = String::with_capacity(32);
    for byte in random {
        password.push(HEX[usize::from(byte >> 4)] as char);
        password.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    Ok(Zeroizing::new(password))
}

fn lookup_volume_password(
    host: &dyn BootstrapHost,
    name: &str,
) -> Result<Option<Zeroizing<String>>, HostError> {
    let command = HostCommand::new(
        SECURITY,
        [
            "find-generic-password",
            "-a",
            name,
            "-s",
            name,
            "-w",
            SYSTEM_KEYCHAIN,
        ],
    );
    let output = host.run_command(&command)?;
    if !output.succeeded() {
        return Ok(None);
    }
    let mut password = String::from_utf8(output.stdout).map_err(|_| {
        HostError::new(format!(
            "System.keychain password for {name} is not valid UTF-8"
        ))
    })?;
    if password.ends_with('\n') {
        password.pop();
        if password.ends_with('\r') {
            password.pop();
        }
    }
    if password.is_empty() {
        // AuthorizationExecuteWithPrivileges reports success and empty stdout when
        // `security` exits 44 (item not found); stderr is not on the communications pipe.
        return Ok(None);
    }
    Ok(Some(Zeroizing::new(password)))
}

fn encrypt_volume_with(host: &dyn BootstrapHost, name: &str, uuid: &str) -> Result<(), HostError> {
    if !matches!(name, APFS_STORE_VOLUME | APFS_CACHES_VOLUME) {
        return Err(HostError::new(format!(
            "refusing to encrypt unexpected APFS volume {name:?}"
        )));
    }
    Uuid::parse_str(uuid)
        .map_err(|_| HostError::new(format!("invalid APFS volume UUID {uuid:?}")))?;
    // Persist the passphrase before encryptVolume: a crash mid-encrypt must still have a
    // keychain item. A leftover item from that crash is reused, never replaced.
    let password = match lookup_volume_password(host, name)? {
        Some(existing) => existing,
        None => {
            let password = random_volume_password()?;
            required_host_command(
                host,
                HostCommand::new(
                    SECURITY,
                    [
                        "add-generic-password",
                        "-a",
                        name,
                        "-s",
                        name,
                        "-l",
                        format!("{name} encryption password").as_str(),
                        "-D",
                        "Encrypted volume password",
                        "-w",
                        password.as_str(),
                        "-T",
                        SECURITY,
                        "-T",
                        APFS_USER_AGENT,
                        "-T",
                        CS_USER_AGENT,
                        SYSTEM_KEYCHAIN,
                    ],
                ),
            )?;
            password
        }
    };
    let command = HostCommand::new(
        DISKUTIL,
        [
            "apfs",
            "encryptVolume",
            uuid,
            "-user",
            "disk",
            "-stdinpassphrase",
        ],
    );
    let mut input = Zeroizing::new(Vec::with_capacity(password.len() + 1));
    input.extend_from_slice(password.as_bytes());
    input.push(b'\n');
    let output = host.run_command_with_input(&command, &input)?;
    if output.succeeded() {
        Ok(())
    } else {
        Err(HostError::new(
            HostCommandFailure::new(command, output).to_string(),
        ))
    }
}

async fn encrypt_volume(
    host: Arc<dyn BootstrapHost>,
    name: String,
    uuid: String,
) -> Result<(), CowshedError> {
    tokio::task::spawn_blocking(move || encrypt_volume_with(host.as_ref(), &name, &uuid))
        .await
        .map_err(|error| {
            setup_execution_error(
                NativeBootstrapError::Execution(BootstrapExecutionError::BlockingLane(
                    error.to_string(),
                )),
                "cowshed setup",
            )
        })?
        .map_err(NativeBootstrapError::Host)
        .map_err(|error| setup_execution_error(error, "cowshed setup"))
}

fn write_mount_service_temporary(contents: &[u8], kind: &str) -> Result<PathBuf, HostError> {
    let path = PathBuf::from(format!(
        "/private/tmp/cowshed-mount-service-{}-{kind}",
        Uuid::new_v4()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(|source| host_io_error("create temporary mount service file", &path, source))?;
    file.write_all(contents)
        .map_err(|source| host_io_error("write temporary mount service file", &path, source))?;
    file.sync_all()
        .map_err(|source| host_io_error("sync temporary mount service file", &path, source))?;
    Ok(path)
}

fn install_mount_service_with(
    host: &dyn BootstrapHost,
    files: &MountServiceFiles,
) -> Result<(), HostError> {
    let script_temporary = write_mount_service_temporary(&files.script, "script")?;
    let plist_temporary = match write_mount_service_temporary(&files.plist, "plist") {
        Ok(path) => path,
        Err(error) => {
            let _ = fs::remove_file(&script_temporary);
            return Err(error);
        }
    };
    let result = (|| {
        // A missing job is the normal first-install state. Every other command is required.
        host.run_command(&HostCommand::new(
            LAUNCHCTL,
            ["bootout", MOUNT_SERVICE_TARGET],
        ))?;
        required_host_command(
            host,
            HostCommand::new(
                INSTALL,
                [
                    "-d",
                    "-o",
                    "root",
                    "-g",
                    "wheel",
                    "-m",
                    "755",
                    MOUNT_SERVICE_DIRECTORY,
                ],
            ),
        )?;
        let script_temporary = path_argument(&script_temporary)?;
        required_host_command(
            host,
            HostCommand::new(
                INSTALL,
                [
                    "-o",
                    "root",
                    "-g",
                    "wheel",
                    "-m",
                    "755",
                    script_temporary.as_str(),
                    MOUNT_SERVICE_SCRIPT,
                ],
            ),
        )?;
        let plist_temporary = path_argument(&plist_temporary)?;
        required_host_command(
            host,
            HostCommand::new(
                INSTALL,
                [
                    "-o",
                    "root",
                    "-g",
                    "wheel",
                    "-m",
                    "644",
                    plist_temporary.as_str(),
                    MOUNT_SERVICE_PLIST,
                ],
            ),
        )?;
        required_host_command(
            host,
            HostCommand::new(LAUNCHCTL, ["enable", MOUNT_SERVICE_TARGET]),
        )?;
        required_host_command(
            host,
            HostCommand::new(LAUNCHCTL, ["bootstrap", "system", MOUNT_SERVICE_PLIST]),
        )?;
        required_host_command(
            host,
            HostCommand::new(LAUNCHCTL, ["kickstart", "-k", MOUNT_SERVICE_TARGET]),
        )
    })();
    let _ = fs::remove_file(script_temporary);
    let _ = fs::remove_file(plist_temporary);
    result
}

async fn install_mount_service(
    snapshot: &SetupSnapshot,
    host: Arc<dyn BootstrapHost>,
) -> Result<(), CowshedError> {
    let files = mount_service_files(snapshot)
        .map_err(|error| setup_execution_error(error, "cowshed setup"))?
        .ok_or_else(|| {
            CowshedError::internal("mount service installation requires both APFS volume UUIDs")
        })?;
    tokio::task::spawn_blocking(move || install_mount_service_with(host.as_ref(), &files))
        .await
        .map_err(|error| {
            setup_execution_error(
                NativeBootstrapError::Execution(BootstrapExecutionError::BlockingLane(
                    error.to_string(),
                )),
                "cowshed setup",
            )
        })?
        .map_err(NativeBootstrapError::Host)
        .map_err(|error| setup_execution_error(error, "cowshed setup"))
}

async fn execute_snapshot_actions(
    snapshot: &SetupSnapshot,
    host: Arc<dyn BootstrapHost>,
) -> (Vec<HostActionOutcome>, Option<CowshedError>) {
    let mut outcomes = snapshot
        .actions
        .iter()
        .cloned()
        .map(|action| HostActionOutcome {
            action,
            outcome: HostActionResult::Skipped,
        })
        .collect::<Vec<_>>();
    let mut index = 0;
    while index < outcomes.len() {
        let action = outcomes[index].action.clone();
        let result = match &action {
            HostAction::ReclaimStubs { .. } => {
                let operations = snapshot
                    .classified
                    .iter()
                    .filter(|volume| !volume.reclaimable_stubs.is_empty())
                    .map(|volume| HostOperation::ReclaimMountpoint(volume.mountpoint.clone()))
                    .collect::<Vec<_>>();
                execute_setup_operations(&operations, Arc::clone(&host)).await
            }
            HostAction::InstallMountService { .. } => {
                install_mount_service(snapshot, Arc::clone(&host)).await
            }
            HostAction::EncryptVolume { name, uuid, .. } => {
                encrypt_volume(Arc::clone(&host), name.clone(), uuid.clone()).await
            }
            HostAction::PinFstab { .. } => {
                let pin_indices = (index..outcomes.len())
                    .filter(|candidate| {
                        matches!(outcomes[*candidate].action, HostAction::PinFstab { .. })
                    })
                    .collect::<Vec<_>>();
                let operation =
                    snapshot.plan.operations().iter().find(|operation| {
                        matches!(operation, HostOperation::PinVolumesInFstab { .. })
                    });
                let result = match operation {
                    Some(operation) => {
                        execute_setup_operations(std::slice::from_ref(operation), Arc::clone(&host))
                            .await
                    }
                    None => Err(CowshedError::internal(
                        "setup pin action has no fstab operation",
                    )),
                };
                match result {
                    Ok(()) => {
                        for pin_index in &pin_indices {
                            outcomes[*pin_index].outcome = HostActionResult::Done;
                        }
                        index = pin_indices.last().map_or(index + 1, |last| last + 1);
                        continue;
                    }
                    Err(error) => {
                        for pin_index in pin_indices {
                            outcomes[pin_index].outcome = HostActionResult::Failed {
                                error: error.clone(),
                            };
                        }
                        return (outcomes, Some(error));
                    }
                }
            }
            _ => {
                let Some(name) = action_volume_name(&action) else {
                    unreachable!("non-volume actions were handled above")
                };
                let operations = snapshot
                    .classified
                    .iter()
                    .find(|volume| volume.name == name)
                    .map(|volume| operations_for_volume(&snapshot.plan, volume))
                    .unwrap_or_default();
                if operations.is_empty() {
                    Err(CowshedError::internal(format!(
                        "setup action for {name} has no host operations"
                    )))
                } else {
                    execute_setup_operations(&operations, Arc::clone(&host)).await
                }
            }
        };
        match result {
            Ok(()) => outcomes[index].outcome = HostActionResult::Done,
            Err(error) => {
                outcomes[index].outcome = HostActionResult::Failed {
                    error: error.clone(),
                };
                return (outcomes, Some(error));
            }
        }
        index += 1;
    }
    (outcomes, None)
}

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
        let mut snapshot = prepare_setup_snapshot(&mut source, &home, &existing_fstab)?;
        add_mount_service_action(&mut snapshot)?;
        Ok(snapshot)
    })
    .await
    .map_err(|error| {
        NativeBootstrapError::Execution(BootstrapExecutionError::BlockingLane(error.to_string()))
    })?
}

pub async fn plan_host_setup(home: &Path) -> crate::Result<HostSetupPlan> {
    {
        let snapshot = gather_setup_snapshot(home, Arc::new(SystemBootstrapHost))
            .await
            .map_err(existing_host_storage_error)?;
        Ok(HostSetupPlan::new(
            host_setup_actions(&snapshot),
            snapshot.volumes.clone(),
            setup_requires_authorization(&snapshot),
        ))
    }
}

pub async fn execute_host_setup(home: &Path) -> crate::Result<HostSetupReport> {
    {
        let initial = gather_setup_snapshot(home, Arc::new(SystemBootstrapHost))
            .await
            .map_err(existing_host_storage_error)?;
        let authorized = setup_requires_authorization(&initial);
        let host: Arc<dyn BootstrapHost> = if authorized {
            let session = tokio::task::spawn_blocking(MacAuthorizationSession::acquire)
                .await
                .map_err(|error| {
                    existing_host_storage_error(NativeBootstrapError::Execution(
                        BootstrapExecutionError::BlockingLane(error.to_string()),
                    ))
                })?
                .map_err(NativeBootstrapError::Host)
                .map_err(|error| setup_execution_error(error, "cowshed setup"))?;
            Arc::new(AuthorizedBootstrapHost::new(session))
        } else {
            Arc::new(SystemBootstrapHost)
        };
        let (mut action_outcomes, failure) =
            execute_snapshot_actions(&initial, Arc::clone(&host)).await;
        if let Some(error) = failure {
            return Ok(HostSetupReport {
                action_outcomes,
                volumes: initial.volumes,
                fstab: FstabOutcome::Skipped(error.message.clone()),
                authorized,
            });
        }

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
            let post = match gather_setup_snapshot(home, Arc::clone(&host)).await {
                Ok(post) => post,
                Err(error) => {
                    let error = setup_execution_error(error, "cowshed setup");
                    if let Some(last) = action_outcomes.last_mut() {
                        last.outcome = HostActionResult::Failed {
                            error: error.clone(),
                        };
                    }
                    return Ok(HostSetupReport {
                        action_outcomes,
                        volumes: initial.volumes,
                        fstab: FstabOutcome::Skipped(error.message.clone()),
                        authorized,
                    });
                }
            };
            if post
                .plan
                .operations()
                .iter()
                .any(|operation| matches!(operation, HostOperation::ProvisionApfsVolumes { .. }))
            {
                let error = CowshedError::internal(
                    "post-setup evidence still proposes APFS volume creation; refusing a second create",
                );
                if let Some(last) = action_outcomes.last_mut() {
                    last.outcome = HostActionResult::Failed {
                        error: error.clone(),
                    };
                }
                return Ok(HostSetupReport {
                    action_outcomes,
                    volumes: initial.volumes,
                    fstab: FstabOutcome::Skipped(error.message.clone()),
                    authorized,
                });
            }
            let mut post_actions = post.clone();
            post_actions.actions.retain(|action| {
                matches!(
                    action,
                    HostAction::EncryptVolume { .. }
                        | HostAction::PinFstab { .. }
                        | HostAction::InstallMountService { .. }
                )
            });
            let (post_outcomes, post_failure) =
                execute_snapshot_actions(&post_actions, Arc::clone(&host)).await;
            action_outcomes.extend(post_outcomes);
            if let Some(error) = post_failure {
                return Ok(HostSetupReport {
                    action_outcomes,
                    volumes: initial.volumes,
                    fstab: FstabOutcome::Skipped(error.message.clone()),
                    authorized,
                });
            }
            match post.fstab {
                PlannedFstab::AlreadyCurrent if pinned_initially => FstabOutcome::Pinned,
                PlannedFstab::AlreadyCurrent => FstabOutcome::AlreadyCurrent,
                PlannedFstab::NeedsPin(_) => FstabOutcome::Pinned,
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
            action_outcomes,
            volumes: initial.volumes,
            fstab,
            authorized,
        })
    }
}

fn mount_service_artifacts_present() -> bool {
    fs::symlink_metadata(MOUNT_SERVICE_SCRIPT).is_ok()
        || fs::symlink_metadata(MOUNT_SERVICE_PLIST).is_ok()
        || mount_service_loaded()
}
fn volume_keychain_item_present(label: &str) -> Result<bool, HostError> {
    let command = HostCommand::new(
        SECURITY,
        [
            "find-generic-password",
            "-a",
            label,
            "-s",
            label,
            SYSTEM_KEYCHAIN,
        ],
    );
    run_command_with(&command, |program, args| {
        Command::new(program).args(args).output_locked()
    })
    .map(|output| output.succeeded())
}

fn host_uninstall_plan(home: &Path) -> Result<HostUninstallPlan, NativeBootstrapError> {
    let existing = read_fstab_text().map_err(NativeBootstrapError::Host)?;
    let mut plan = host_uninstall_plan_from_text(home, &existing)?;
    let has_keychain_items = VOLUME_KEYCHAIN_LABELS
        .iter()
        .try_fold(false, |found, label| {
            volume_keychain_item_present(label).map(|present| found || present)
        })
        .map_err(NativeBootstrapError::Host)?;
    plan.requires_authorization |= mount_service_artifacts_present() || has_keychain_items;
    Ok(plan)
}

fn host_uninstall_plan_from_text(
    home: &Path,
    existing: &str,
) -> Result<HostUninstallPlan, NativeBootstrapError> {
    require_canonical(home)?;
    let pins_to_remove = existing
        .lines()
        .filter_map(|line| {
            line.split_once(COWSHED_FSTAB_TAG)
                .map(|(_, label)| label.trim().to_owned())
        })
        .collect::<Vec<_>>();
    Ok(HostUninstallPlan {
        requires_authorization: !pins_to_remove.is_empty(),
        pins_to_remove,
    })
}

pub async fn plan_host_uninstall(home: &Path) -> crate::Result<HostUninstallPlan> {
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
    {
        let plan = plan_host_uninstall(home).await?;
        if !plan.requires_authorization {
            return Ok(UninstallReport {
                fstab: UninstallFstabOutcome::AlreadyClean,
                services: Vec::new(),
            });
        }
        let remove_service = mount_service_artifacts_present();
        let remove_fstab = !plan.pins_to_remove.is_empty();
        let removed_keychain_labels = tokio::task::spawn_blocking(move || {
            let mut keychain_labels = Vec::new();
            for label in VOLUME_KEYCHAIN_LABELS {
                if volume_keychain_item_present(label)? {
                    keychain_labels.push(label);
                }
            }
            let mut session = MacAuthorizationSession::acquire()?;
            if remove_service {
                // A job which is already absent is still successfully uninstalled.
                session.execute(&HostCommand::new(
                    LAUNCHCTL,
                    ["bootout", MOUNT_SERVICE_TARGET],
                ))?;
                run_privileged_command(
                    &mut session,
                    &HostCommand::new(RM, ["-f", MOUNT_SERVICE_PLIST, MOUNT_SERVICE_SCRIPT]),
                )?;
                // Keep the directory when another root-owned artifact shares it.
                let _ = session.execute(&HostCommand::new(RMDIR, [MOUNT_SERVICE_DIRECTORY]));
            }
            for label in &keychain_labels {
                run_privileged_command(
                    &mut session,
                    &HostCommand::new(
                        SECURITY,
                        [
                            "delete-generic-password",
                            "-a",
                            *label,
                            "-s",
                            *label,
                            SYSTEM_KEYCHAIN,
                        ],
                    ),
                )?;
            }
            if remove_fstab {
                pin_volumes_in_fstab_with(&mut session, &[])?;
            }
            Ok::<_, HostError>(keychain_labels)
        })
        .await
        .map_err(|error| {
            existing_host_storage_error(NativeBootstrapError::Execution(
                BootstrapExecutionError::BlockingLane(error.to_string()),
            ))
        })?
        .map_err(NativeBootstrapError::Host)
        .map_err(|error| setup_execution_error(error, "cowshed setup --uninstall"))?;
        let mut services = Vec::new();
        if remove_service {
            services.push(UninstallServiceOutcome {
                what: format!("{MOUNT_SERVICE_LABEL} system LaunchDaemon"),
                outcome: "removed".to_owned(),
            });
        }
        services.extend(
            removed_keychain_labels
                .into_iter()
                .map(|label| UninstallServiceOutcome {
                    what: format!("{label} System.keychain item"),
                    outcome: "removed".to_owned(),
                }),
        );
        Ok(UninstallReport {
            fstab: if remove_fstab {
                UninstallFstabOutcome::Removed
            } else {
                UninstallFstabOutcome::AlreadyClean
            },
            services,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StatFsSnapshot {
    fs_type: String,
    mount_source: PathBuf,
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
    /// One authoritative kernel snapshot of every APFS container, with each volume's kernel mount.
    ///
    /// Required of every source: no listing decision may come from a CLI projection, so tests
    /// hand the planner typed registry and mount-table records instead of command output.
    fn apfs_inventory(&mut self) -> Result<ApfsInventory, NativeBootstrapError>;
    fn inspect_mountpoint(&mut self, path: &Path) -> Result<MountpointState, NativeBootstrapError>;
    fn mounted_volume(
        &mut self,
        path: &Path,
    ) -> Result<MountedVolumeEvidence, NativeBootstrapError>;
    /// FileVault state of one reserved volume already selected from the kernel inventory.
    ///
    /// No kernel property carries FileVault, so this is per-volume crypto metadata for that
    /// exact record — never a listing and never a global query.
    fn volume_file_vault(
        &mut self,
        container: &str,
        volume: &ApfsVolume,
    ) -> Result<bool, NativeBootstrapError>;
    fn keychain_item_usable(&mut self, label: &'static str) -> Result<bool, NativeBootstrapError>;
    fn invoking_identity(&mut self) -> (u32, u32);
}

struct SystemEvidenceSource<'a> {
    host: &'a dyn BootstrapHost,
}

impl EvidenceSource for SystemEvidenceSource<'_> {
    fn statfs(&mut self, path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError> {
        system_statfs(path)
    }

    fn apfs_inventory(&mut self) -> Result<ApfsInventory, NativeBootstrapError> {
        system_apfs_inventory()
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

    fn volume_file_vault(
        &mut self,
        container: &str,
        volume: &ApfsVolume,
    ) -> Result<bool, NativeBootstrapError> {
        let command = HostCommand::new(DISKUTIL, ["info", "-plist", volume.identifier.as_str()]);
        let output = self.host.run_command(&command)?;
        if !output.succeeded() {
            return Err(NativeBootstrapError::CommandFailed(
                HostCommandFailure::new(command, output),
            ));
        }
        attest_volume_file_vault(&output.stdout, container, volume)
    }

    fn keychain_item_usable(&mut self, label: &'static str) -> Result<bool, NativeBootstrapError> {
        let command = HostCommand::new(
            SECURITY,
            [
                "find-generic-password",
                "-a",
                label,
                "-s",
                label,
                "-w",
                SYSTEM_KEYCHAIN,
            ],
        );
        self.host
            .run_command(&command)
            .map(|output| output.succeeded())
            .map_err(NativeBootstrapError::Host)
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
    container: String,
    mountpoint: PathBuf,
    storage: ExistingStorage,
    volume_uuid: Option<String>,
    size_bytes: Option<u64>,
    reclaimable_stubs: Vec<PathBuf>,
    file_vault: Option<bool>,
}

#[derive(Clone, Debug)]
struct ClassifiedStorage {
    storage: ExistingStorage,
    reclaimable_stubs: Vec<PathBuf>,
    file_vault: Option<bool>,
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
    // One kernel snapshot answers both the home-container classification and the global
    // reserved-name guard, so the two decisions can never observe different listings.
    let inventory = source.apfs_inventory()?;
    let container = inventory.containing_container(&mount_device)?;
    let roots = CanonicalRoots::global();
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
    guard_absent_volume_globally(source, &inventory, APFS_STORE_VOLUME, &mut store)?;
    guard_absent_volume_globally(source, &inventory, APFS_CACHES_VOLUME, &mut caches)?;

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
    .map(|(name, role, mountpoint, classified)| {
        let (container_name, volume_uuid, size_bytes) = match &classified.storage {
            ExistingStorage::FoundElsewhere {
                container,
                volume_uuid,
                size_bytes,
                ..
            } => (
                container.clone(),
                Some(volume_uuid.clone()),
                Some(*size_bytes),
            ),
            _ => (
                container.reference.clone(),
                container
                    .volumes
                    .iter()
                    .find(|volume| volume.name == name)
                    .map(|volume| volume.volume_uuid.clone()),
                Some(container.capacity_bytes),
            ),
        };
        ClassifiedVolume {
            name,
            role,
            container: container_name,
            mountpoint: mountpoint.to_owned(),
            storage: classified.storage.clone(),
            volume_uuid,
            size_bytes,
            reclaimable_stubs: classified.reclaimable_stubs.clone(),
            file_vault: classified.file_vault,
        }
    })
    .collect::<Vec<_>>();
    for volume in &classified {
        if volume.file_vault == Some(true) && !source.keychain_item_usable(volume.name)? {
            let uuid = volume.volume_uuid.clone().ok_or_else(|| {
                NativeBootstrapError::MalformedApfsInventory(format!(
                    "{} has FileVault enabled but no APFS volume UUID",
                    volume.name
                ))
            })?;
            return Err(NativeBootstrapError::MissingVolumeKeychain {
                name: volume.name,
                uuid,
            });
        }
    }
    Ok(GatheredEvidence {
        statfs: StatFsEvidence::Apfs {
            mount_source: snapshot.mount_source,
            container: Some(container.reference.clone()),
        },
        bootstrap: BootstrapEvidence::Apfs {
            store: store.storage,
            caches: caches.storage,
        },
        volumes: classified,
    })
}

fn guard_absent_volume_globally(
    source: &mut impl EvidenceSource,
    inventory: &ApfsInventory,
    name: &'static str,
    classified: &mut ClassifiedStorage,
) -> Result<(), NativeBootstrapError> {
    let state = &mut classified.storage;
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
            let mounted_at = volume.mountpoint.clone();
            *state = ExistingStorage::FoundElsewhere {
                container: container.reference.clone(),
                device: volume.identifier.clone(),
                volume_uuid: volume.volume_uuid.clone(),
                size_bytes: container.capacity_bytes,
                mounted_at,
            };
            classified.file_vault = Some(source.volume_file_vault(&container.reference, volume)?);
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
    if crate::repository::is_lexically_canonical(path) {
        Ok(())
    } else {
        Err(NativeBootstrapError::NonCanonicalPath(path.to_owned()))
    }
}

fn exact_device_identifier(path: &Path) -> Result<String, NativeBootstrapError> {
    let invalid = || NativeBootstrapError::InvalidMountSource(path.to_owned());
    let identifier = path
        .to_str()
        .and_then(|value| value.strip_prefix("/dev/"))
        .ok_or_else(invalid)?;
    if valid_volume_identifier(identifier.as_bytes()) {
        Ok(identifier.to_owned())
    } else {
        Err(invalid())
    }
}

fn valid_container_identifier(value: &[u8]) -> bool {
    str::from_utf8(value)
        .ok()
        .and_then(crate::device::identifier_depth)
        == Some(0)
}

/// A synthesized APFS volume always publishes as exactly `diskNsM`; anything deeper is a snapshot
/// and anything shallower a container, and neither is a mount source this bootstrap accepts.
fn valid_volume_identifier(value: &[u8]) -> bool {
    str::from_utf8(value)
        .ok()
        .and_then(crate::device::identifier_depth)
        == Some(1)
}

/// Every APFS container the kernel registry published in one snapshot.
///
/// Only [`ApfsInventory::from_kernel`] builds one, so container references and volume
/// identifiers are globally unique, every volume lies inside its container, and each volume
/// carries at most one kernel mountpoint.
#[derive(Clone, Debug)]
struct ApfsInventory {
    containers: Vec<ApfsContainer>,
}

#[derive(Clone, Debug)]
struct ApfsContainer {
    reference: String,
    capacity_bytes: u64,
    volumes: Vec<ApfsVolume>,
}

/// Kernel identity of one volume. FileVault is deliberately absent: the registry's `Encrypted`
/// property is per-volume encryption (the Data volume is encrypted without FileVault), so the
/// FileVault answer comes from [`EvidenceSource::volume_file_vault`] for reserved volumes only.
#[derive(Clone, Debug)]
struct ApfsVolume {
    name: String,
    identifier: String,
    /// The kernel mount-table mountpoint of this exact device; `None` means detached.
    mountpoint: Option<PathBuf>,
    volume_uuid: String,
}

/// IORegistry names the containers and volumes, the kernel mount table names where each volume
/// is mounted. Neither read queues behind Disk Arbitration, and no CLI listing is consulted.
fn system_apfs_inventory() -> Result<ApfsInventory, NativeBootstrapError> {
    let registered = registered_apfs_containers()
        .map_err(|source| NativeBootstrapError::ApfsRegistry { source })?;
    let mounts = SystemKernelMountSource
        .mounts()
        .map_err(|source| NativeBootstrapError::KernelMountTable { source })?;
    ApfsInventory::from_kernel(registered, &mounts)
}

impl ApfsInventory {
    /// Validate one registry snapshot and attach each volume's kernel mount. Every rule fails
    /// closed: an empty or inconsistent snapshot can never establish that a volume is absent.
    fn from_kernel(
        registered: Vec<RegisteredApfsContainer>,
        mounts: &[KernelMountSnapshot],
    ) -> Result<Self, NativeBootstrapError> {
        if registered.is_empty() {
            return Err(NativeBootstrapError::EmptyApfsInventory);
        }
        let mut mounted: BTreeMap<&str, Vec<PathBuf>> = BTreeMap::new();
        for mount in mounts {
            if let Some(device) = mount.source_device.strip_prefix("/dev/") {
                mounted
                    .entry(device)
                    .or_default()
                    .push(mount.mount_point.clone());
            }
        }
        let mut references = BTreeSet::new();
        let mut identifiers = BTreeSet::new();
        let mut containers = Vec::with_capacity(registered.len());
        for container in registered {
            if !valid_container_identifier(container.reference.as_bytes()) {
                return Err(malformed(format!(
                    "invalid APFS container reference {:?}",
                    container.reference
                )));
            }
            if !references.insert(container.reference.clone()) {
                return Err(NativeBootstrapError::DuplicateApfsContainer(
                    container.reference,
                ));
            }
            if container.capacity_bytes == 0 {
                return Err(malformed(format!(
                    "APFS container {:?} has no capacity",
                    container.reference
                )));
            }
            let mut volumes = Vec::with_capacity(container.volumes.len());
            for volume in container.volumes {
                if !valid_volume_identifier(volume.identifier.as_bytes())
                    || crate::device::container_of(&volume.identifier)
                        != Some(container.reference.as_str())
                {
                    return Err(malformed(format!(
                        "volume {:?} is not in container {:?}",
                        volume.identifier, container.reference
                    )));
                }
                if volume.name.is_empty() {
                    return Err(malformed(format!(
                        "volume {:?} has no name",
                        volume.identifier
                    )));
                }
                if volume.volume_uuid.len() != 36
                    || !Uuid::try_parse(&volume.volume_uuid).is_ok_and(|uuid| !uuid.is_nil())
                {
                    return Err(malformed(format!(
                        "volume {:?} has invalid volume UUID {:?}",
                        volume.identifier, volume.volume_uuid
                    )));
                }
                if !identifiers.insert(volume.identifier.clone()) {
                    return Err(NativeBootstrapError::DuplicateApfsVolume(volume.identifier));
                }
                let mountpoint = match mounted.remove(volume.identifier.as_str()) {
                    None => None,
                    Some(mountpoints) => match <[PathBuf; 1]>::try_from(mountpoints) {
                        Ok([mountpoint]) => Some(mountpoint),
                        Err(mountpoints) => {
                            return Err(NativeBootstrapError::AmbiguousVolumeMount {
                                identifier: volume.identifier,
                                mountpoints,
                            });
                        }
                    },
                };
                volumes.push(ApfsVolume {
                    name: volume.name,
                    identifier: volume.identifier,
                    mountpoint,
                    volume_uuid: volume.volume_uuid,
                });
            }
            containers.push(ApfsContainer {
                reference: container.reference,
                capacity_bytes: container.capacity_bytes,
                volumes,
            });
        }
        Ok(Self { containers })
    }

    /// Identifiers are unique across the validated snapshot, so at most one container matches.
    fn containing_container(&self, device: &str) -> Result<&ApfsContainer, NativeBootstrapError> {
        self.containers
            .iter()
            .find(|container| {
                container
                    .volumes
                    .iter()
                    .any(|volume| volume.identifier == device)
            })
            .ok_or_else(|| NativeBootstrapError::ContainerNotFound {
                device: device.to_owned(),
            })
    }
}

fn malformed(message: impl Into<String>) -> NativeBootstrapError {
    NativeBootstrapError::MalformedApfsInventory(message.into())
}

/// Read FileVault from one volume's `diskutil info -plist` answer. The answer counts only when
/// it names the exact kernel record — device, volume UUID, container and name — and states
/// `FileVault` explicitly; an empty, foreign, or silent answer is unknown and fails closed.
fn attest_volume_file_vault(
    bytes: &[u8],
    container: &str,
    volume: &ApfsVolume,
) -> Result<bool, NativeBootstrapError> {
    let refuse = |reason: String| NativeBootstrapError::VolumeFileVaultEvidence {
        identifier: volume.identifier.clone(),
        reason,
    };
    let value = Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| refuse(format!("unreadable plist: {error}")))?;
    let info = value
        .as_dictionary()
        .ok_or_else(|| refuse("plist root is not a dictionary".to_owned()))?;
    for (key, expected) in [
        ("DeviceIdentifier", volume.identifier.as_str()),
        ("APFSContainerReference", container),
        ("VolumeName", volume.name.as_str()),
    ] {
        let actual = info.get(key).and_then(Value::as_string);
        if actual != Some(expected) {
            return Err(refuse(format!(
                "{key} is {actual:?}, expected {expected:?}"
            )));
        }
    }
    let uuid = info.get("VolumeUUID").and_then(Value::as_string);
    if !uuid.is_some_and(|uuid| uuid.eq_ignore_ascii_case(&volume.volume_uuid)) {
        return Err(refuse(format!(
            "VolumeUUID is {uuid:?}, expected {:?}",
            volume.volume_uuid
        )));
    }
    match info.get("FileVault") {
        Some(Value::Boolean(enabled)) => Ok(*enabled),
        other => Err(refuse(format!(
            "FileVault is {other:?}, expected a boolean"
        ))),
    }
}

fn classify_volume(
    source: &mut impl EvidenceSource,
    container: &ApfsContainer,
    name: &'static str,
    expected_mountpoint: &Path,
    role: VolumeRole,
) -> Result<ClassifiedStorage, NativeBootstrapError> {
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
    let (state, reclaimable_stubs) = match source.inspect_mountpoint(expected_mountpoint)? {
        MountpointState::ReclaimableStub { paths } => (MountpointState::EmptyDirectory, paths),
        state => (state, Vec::new()),
    };
    let storage = if let Some(volume) = matches.first() {
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
                    ExistingStorage::mis_mounted_incomplete(&volume.identifier, expected_mountpoint)
                } else {
                    let (uid, gid) = source.invoking_identity();
                    if marker_missing || mounted.uid != uid || mounted.gid != gid {
                        ExistingStorage::mounted_incomplete(&volume.identifier)
                    } else {
                        ExistingStorage::mounted_valid(&volume.identifier)
                    }
                }
            }
            MountpointState::Missing | MountpointState::EmptyDirectory => {
                // The kernel mount table distinguishes detached from mounted elsewhere; the
                // statfs of that other mountpoint must then re-attest the same exact device.
                match &volume.mountpoint {
                    None => ExistingStorage::detached_incomplete(&volume.identifier),
                    Some(current) => {
                        require_canonical(current)?;
                        let mounted = source.mounted_volume(current)?;
                        if mounted.exact_identifier != volume.identifier
                            || mounted.mountpoint != *current
                        {
                            return Err(NativeBootstrapError::MountEvidenceMismatch {
                                path: current.clone(),
                                identifier: mounted.exact_identifier,
                            });
                        }
                        ExistingStorage::mis_mounted_incomplete(&volume.identifier, current)
                    }
                }
            }
            MountpointState::ReclaimableStub { .. } => {
                unreachable!("reclaimable evidence is normalized above")
            }
            MountpointState::NonEmptyDirectoryWithoutMount => {
                return Err(NativeBootstrapError::MaskedMountpoint {
                    path: expected_mountpoint.to_owned(),
                });
            }
        }
    } else {
        match state {
            MountpointState::Missing | MountpointState::EmptyDirectory => ExistingStorage::Absent,
            MountpointState::ReclaimableStub { .. } => {
                unreachable!("reclaimable evidence is normalized above")
            }
            MountpointState::NonEmptyDirectoryWithoutMount => {
                return Err(NativeBootstrapError::MaskedMountpoint {
                    path: expected_mountpoint.to_owned(),
                });
            }
            // The exact reserved volume may live in another APFS container. Preserve the
            // mountpoint observation until the mandatory global scan resolves its identity.
            MountpointState::Mounted { .. } => ExistingStorage::Absent,
        }
    };
    let file_vault = match matches.first() {
        Some(volume) => Some(source.volume_file_vault(&container.reference, volume)?),
        None => None,
    };
    Ok(ClassifiedStorage {
        storage,
        reclaimable_stubs,
        file_vault,
    })
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

trait PrivilegedCommandSession {
    fn execute(&mut self, command: &HostCommand) -> Result<HostCommandOutput, HostError>;
    fn execute_with_input(
        &mut self,
        command: &HostCommand,
        _input: &[u8],
    ) -> Result<HostCommandOutput, HostError> {
        Err(HostError::new(format!(
            "privileged session cannot provide standard input to {:?}",
            command.program()
        )))
    }
}

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
    /// The global kernel APFS inventory the create recheck refuses reserved names against.
    fn apfs_inventory(&self) -> Result<ApfsInventory, NativeBootstrapError>;
}

struct SystemApfsProvisionIo;

impl ApfsProvisionIo for SystemApfsProvisionIo {
    fn prepare_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        match inspect_system_mountpoint(path)? {
            MountpointState::Missing => fs::create_dir_all(path)
                .map_err(|source| host_io_error("create APFS mountpoint", path, source)),
            MountpointState::EmptyDirectory => Ok(()),
            MountpointState::ReclaimableStub { .. } => reclaim_system_mountpoint(path),
            MountpointState::NonEmptyDirectoryWithoutMount | MountpointState::Mounted { .. } => {
                Err(HostError::new(format!(
                    "refusing to create a volume over non-empty or mounted path {path:?}"
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

    fn apfs_inventory(&self) -> Result<ApfsInventory, NativeBootstrapError> {
        system_apfs_inventory()
    }
}

/// Return a freshly attested volume to the detached state.
///
/// `-nomount` is passed on creation, but some macOS releases mount the new
/// volume at `/Volumes/<name>` anyway. Mounting it a second time at the private
/// mountpoint would leave the browsable default mount in place, so the default
/// mount is dropped first and provisioning proceeds identically on either host.
fn detach_auto_mounted_volume<S>(
    session: &mut S,
    state: CreatedMountState,
    exact_identifier: &str,
) -> Result<(), HostError>
where
    S: PrivilegedCommandSession,
{
    if state == CreatedMountState::Unmounted {
        return Ok(());
    }
    let unmount = HostCommand::new(DISKUTIL, ["unmount", exact_identifier]);
    run_privileged_command(session, &unmount)?;
    Ok(())
}

#[cfg(test)]
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
        // Recheck inside the authorization session, immediately before any create, against a
        // fresh kernel snapshot: planning-time absence may have gone stale while the user
        // answered the prompt, and an unreadable or empty snapshot refuses the create.
        let inventory = io
            .apfs_inventory()
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
                let exact_identifier = parse_created_apfs_identifier(&output.stdout)
                    .map_err(|error| HostError::new(error.to_string()))?;
                let info =
                    HostCommand::new(DISKUTIL, ["info", "-plist", exact_identifier.as_str()]);
                let output = run_privileged_command(session, &info)?;
                let mount_state = attest_created_apfs_info(
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
                let mount_state = attest_created_apfs_info(
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
            "APFS creation batch contains {} volumes",
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
                "invalid or duplicate APFS creation role for {:?}",
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

fn path_argument(path: &Path) -> Result<String, HostError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| HostError::new(format!("path is not UTF-8: {path:?}")))
}

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

fn read_fstab_text() -> Result<String, HostError> {
    match fs::read(FSTAB) {
        Ok(bytes) => String::from_utf8(bytes)
            .map_err(|_| HostError::new(format!("{FSTAB} is not valid UTF-8"))),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(source) => Err(host_io_error("read fstab", Path::new(FSTAB), source)),
    }
}

fn desired_fstab(existing: &str, pins: &[FstabPin]) -> Result<String, HostError> {
    build_fstab(existing, pins).map_err(|error| HostError::new(error.to_string()))
}

fn pin_volumes_in_fstab_with(
    session: &mut impl PrivilegedCommandSession,
    pins: &[FstabPin],
) -> Result<bool, HostError> {
    let existing = read_fstab_text()?;
    let desired = desired_fstab(&existing, pins)?;
    if desired.as_bytes() == existing.as_bytes() {
        return Ok(false);
    }

    let temporary_path = PathBuf::from(format!("/private/tmp/cowshed-fstab-{}", Uuid::new_v4()));
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

struct MacAuthorizationSession {
    reference: AuthorizationRef,
}

unsafe impl Send for MacAuthorizationSession {}

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
    fn execute_authorized(
        &mut self,
        command: &HostCommand,
        input: Option<&[u8]>,
    ) -> Result<HostCommandOutput, HostError> {
        if !Path::new(command.program()).is_absolute() {
            return Err(HostError::new(format!(
                "refusing non-absolute authorized program {:?}",
                command.program()
            )));
        }
        // AEWP's OSStatus is "tool launched", not the child wait status. Run a
        // fixed /bin/sh wrapper so the pipe carries `{stdout, wait_status}` and
        // a failed security/install/launchctl cannot surface as Exit(0).
        let program = CString::new(PRIVILEGED_SHELL)
            .map_err(|_| HostError::new("privileged shell path contains NUL"))?;
        let mut arguments = Vec::with_capacity(command.args().len() + 3);
        arguments.push(
            CString::new("-c")
                .map_err(|_| HostError::new("authorized argument contains NUL: \"-c\""))?,
        );
        arguments.push(
            CString::new(PRIVILEGED_STATUS_WRAPPER)
                .map_err(|_| HostError::new("privileged status wrapper contains NUL"))?,
        );
        arguments.push(CString::new(command.program()).map_err(|_| {
            HostError::new(format!(
                "authorized program contains NUL: {:?}",
                command.program()
            ))
        })?);
        for argument in command.args() {
            arguments.push(CString::new(argument.as_bytes()).map_err(|_| {
                HostError::new(format!("authorized argument contains NUL: {argument:?}"))
            })?);
        }
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
        if let Some(input) = input {
            let written = unsafe { libc::fwrite(input.as_ptr().cast(), 1, input.len(), pipe) };
            if written != input.len() || unsafe { libc::fflush(pipe) } != 0 {
                let source = io::Error::last_os_error();
                unsafe {
                    libc::fclose(pipe);
                }
                return Err(HostError::new(format!(
                    "cannot write privileged command input: {source}"
                )));
            }
        }
        let stdout = read_authorized_output(pipe)?;
        authorized_command_output(stdout)
    }
}

impl PrivilegedCommandSession for MacAuthorizationSession {
    fn execute(&mut self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        self.execute_authorized(command, None)
    }

    fn execute_with_input(
        &mut self,
        command: &HostCommand,
        input: &[u8],
    ) -> Result<HostCommandOutput, HostError> {
        self.execute_authorized(command, Some(input))
    }
}

impl Drop for MacAuthorizationSession {
    fn drop(&mut self) {
        unsafe {
            AuthorizationFree(self.reference, AUTHORIZATION_FLAG_DESTROY_RIGHTS);
        }
    }
}

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

fn authorized_command_output(mut output: Vec<u8>) -> Result<HostCommandOutput, HostError> {
    let code = strip_authorized_exit(&mut output)?;
    if code == 0 {
        Ok(HostCommandOutput::success(output))
    } else {
        Ok(HostCommandOutput::failure_with_streams(
            crate::process::ProcessStatus::Exit(code),
            output,
            Vec::new(),
        ))
    }
}

fn strip_authorized_exit(output: &mut Vec<u8>) -> Result<i32, HostError> {
    const PREFIX: &[u8] = b"\n--cowshed-aewp-exit:";
    const SUFFIX: &[u8] = b"--\n";
    let missing = || HostError::new("privileged command finished without reporting an exit status");
    if !output.ends_with(SUFFIX) {
        return Err(missing());
    }
    let end = output.len() - SUFFIX.len();
    let body = &output[..end];
    let Some(prefix_at) = body
        .windows(PREFIX.len())
        .rposition(|window| window == PREFIX)
    else {
        return Err(missing());
    };
    let code_bytes = &body[prefix_at + PREFIX.len()..];
    if code_bytes.is_empty() || !code_bytes.iter().all(u8::is_ascii_digit) {
        return Err(missing());
    }
    let code = std::str::from_utf8(code_bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(missing)?;
    output.truncate(prefix_at);
    Ok(code)
}

fn authorization_status(operation: &str, status: i32) -> Result<(), HostError> {
    if status == 0 {
        Ok(())
    } else if matches!(status, AUTHORIZATION_DENIED | AUTHORIZATION_CANCELED) {
        Err(HostError::authorization_denied(format!(
            "{operation} was declined"
        )))
    } else {
        Err(HostError::new(format!(
            "{operation} failed with Authorization Services status {status}"
        )))
    }
}

type AuthorizationRef = *const c_void;

#[repr(C)]
struct AuthorizationItem {
    name: *const libc::c_char,
    value_length: usize,
    value: *mut c_void,
    flags: u32,
}

#[repr(C)]
struct AuthorizationRights {
    count: u32,
    items: *mut AuthorizationItem,
}

const AUTHORIZATION_RIGHT_EXECUTE: &[u8] = b"system.privilege.admin\0";
const AUTHORIZATION_FLAG_INTERACTION_ALLOWED: u32 = 1 << 0;
const AUTHORIZATION_FLAG_EXTEND_RIGHTS: u32 = 1 << 1;
const AUTHORIZATION_FLAG_DESTROY_RIGHTS: u32 = 1 << 3;
const AUTHORIZATION_FLAG_PREAUTHORIZE: u32 = 1 << 4;

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
    Ok(())
}

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

fn c_char_field(field: &[libc::c_char], path: &Path) -> Result<String, NativeBootstrapError> {
    String::from_utf8(c_char_field_bytes(field, path)?).map_err(|source| {
        NativeBootstrapError::StatFs {
            path: path.to_owned(),
            source: io::Error::new(io::ErrorKind::InvalidData, source),
        }
    })
}

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
    if let Some(paths) = reclaimable_stub_paths(path)? {
        return Ok(MountpointState::ReclaimableStub { paths });
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

/// Inventory a masked mountpoint's contents when every entry is cowshed-authored,
/// disposable residue; `None` means something real lives there and the mountpoint
/// stays fail-closed masked.
///
/// Beyond empty scaffolding, two writers race the store mount and plant regenerable state on the
/// bare Data volume: the `dev.cowshed.sccache` LaunchAgent binds its socket and creates its cache
/// directory before the volumes are remounted, and the gateway's project heal creates `mnt/`
/// mountpoint parents. Treating those as a mask wedges the host permanently — the gateway
/// crash-loops on exit 5 and no verb can repair it — so an idle daemon socket, the sccache compile
/// cache (disposable by contract) and directory-only `mnt/` scaffolding are reclaimable. One
/// foreign entry anywhere keeps the masked verdict.
fn reclaimable_stub_paths(path: &Path) -> Result<Option<Vec<PathBuf>>, HostError> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|source| host_io_error("read mountpoint directory", path, source))?
    {
        let entry = entry
            .map_err(|source| host_io_error("read mountpoint directory entry", path, source))?;
        let entry_path = entry.path();
        let name = entry.file_name();
        let file_type = entry
            .file_type()
            .map_err(|source| host_io_error("inspect mountpoint entry", &entry_path, source))?;
        let safe = if name == ".DS_Store" {
            file_type.is_file()
        } else if name == "sccache.sock" {
            // The daemon rebinds on start; an existing socket file is never load-bearing.
            std::os::unix::fs::FileTypeExt::is_socket(&file_type)
        } else if name == "sccache" && file_type.is_dir() {
            // The shared compile cache: disposable by contract, whatever it holds.
            true
        } else if name == "mnt" && file_type.is_dir() {
            // Workspace mountpoint scaffolding: directories all the way down, nothing else.
            is_directory_only_tree(&entry_path)?
        } else if file_type.is_dir() {
            is_empty_directory(&entry_path)?
        } else {
            false
        };
        if !safe {
            return Ok(None);
        }
        paths.push(entry_path);
    }
    if paths.is_empty() {
        Ok(None)
    } else {
        paths.sort();
        Ok(Some(paths))
    }
}

fn is_empty_directory(path: &Path) -> Result<bool, HostError> {
    Ok(fs::read_dir(path)
        .map_err(|source| host_io_error("read empty mountpoint stub", path, source))?
        .next()
        .transpose()
        .map_err(|source| host_io_error("read empty mountpoint stub entry", path, source))?
        .is_none())
}

/// Whether `path` contains only directories, recursively — mountpoint scaffolding, no data.
fn is_directory_only_tree(path: &Path) -> Result<bool, HostError> {
    for entry in fs::read_dir(path)
        .map_err(|source| host_io_error("read scaffold directory", path, source))?
    {
        let entry =
            entry.map_err(|source| host_io_error("read scaffold directory entry", path, source))?;
        let file_type = entry
            .file_type()
            .map_err(|source| host_io_error("inspect scaffold entry", &entry.path(), source))?;
        if !file_type.is_dir() || !is_directory_only_tree(&entry.path())? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn reclaim_system_mountpoint(path: &Path) -> Result<(), HostError> {
    require_host_canonical(path)?;
    match inspect_system_mountpoint(path)? {
        MountpointState::Missing => fs::create_dir_all(path)
            .map_err(|source| host_io_error("create APFS mountpoint", path, source)),
        MountpointState::EmptyDirectory => Ok(()),
        MountpointState::ReclaimableStub { .. } => {
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

fn create_authorized_directory(
    session: &mut impl PrivilegedCommandSession,
    path: &Path,
) -> Result<(), HostError> {
    require_host_canonical(path)?;
    let path_argument = path_argument(path)?;
    run_privileged_command(
        session,
        &HostCommand::new(MKDIR, ["-p", path_argument.as_str()]),
    )?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| host_io_error("inspect authorized directory", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(HostError::new(format!(
            "authorized mountpoint is not a no-follow directory: {path:?}"
        )));
    }
    Ok(())
}

fn reclaim_authorized_mountpoint(
    session: &mut impl PrivilegedCommandSession,
    path: &Path,
) -> Result<(), HostError> {
    require_host_canonical(path)?;
    match inspect_system_mountpoint(path)? {
        MountpointState::Missing => create_authorized_directory(session, path),
        MountpointState::EmptyDirectory => Ok(()),
        MountpointState::ReclaimableStub { .. } => {
            let path_argument = path_argument(path)?;
            run_privileged_command(
                session,
                &HostCommand::new(RM, ["-rf", path_argument.as_str()]),
            )?;
            if fs::symlink_metadata(path).is_ok() {
                return Err(HostError::new(format!(
                    "authorized reclaim left the mountpoint in place: {path:?}"
                )));
            }
            create_authorized_directory(session, path)
        }
        MountpointState::NonEmptyDirectoryWithoutMount | MountpointState::Mounted { .. } => {
            Err(HostError::new(format!(
                "refusing to reclaim non-empty or mounted path {path:?}"
            )))
        }
    }
}

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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::ExitStatusExt;
    use std::process::Stdio;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    use super::super::shared::mutating_setup_actions;
    use super::*;
    use crate::apfs::RegisteredApfsVolume;
    use crate::storage::bootstrap::{BlockingJob, CACHES_ROOT, STORE_ROOT};
    use uuid::Uuid;

    #[test]
    fn real_apfs_inventory_survives_concurrent_image_teardown() {
        use crate::apfs::{ApfsBackend, DetachIntent, MacOsApfsBackend, SystemCommandRunner};

        let root =
            crate::scratch_apfs::ScratchRoot::new("inventory-teardown").expect("scratch root");
        let backend = MacOsApfsBackend::new(SystemCommandRunner);
        let image = root.path().join("transient.asif");
        crate::blank_image::blank_image(&image);

        // Exercise the production read-only planner, not installation validation: a hosted
        // runner need not have cowshed.store/cowshed.caches. The actual home device and its
        // container must remain the selection anchor while an unrelated image disappears.
        let home = std::env::home_dir().expect("home directory");
        let snapshot = system_statfs(&home).expect("home filesystem");
        assert_eq!(snapshot.fs_type, "apfs");
        let device = exact_device_identifier(&snapshot.mount_source).expect("home device");
        let expected = select_substrate(
            StatFsEvidence::Apfs {
                mount_source: snapshot.mount_source,
                container: Some(
                    crate::device::container_of(&device)
                        .expect("home container")
                        .to_owned(),
                ),
            },
            None,
        )
        .expect("home APFS selection");
        let mut source = SystemEvidenceSource {
            host: &SystemBootstrapHost,
        };
        let attachment = backend.attach_verified(&image).expect("attach real image");
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_start = std::sync::Arc::clone(&start);
        let detach = std::thread::spawn(move || {
            let backend = MacOsApfsBackend::new(SystemCommandRunner);
            worker_start.wait();
            let started = std::time::Instant::now();
            let result = backend.detach(&attachment, DetachIntent::Release);
            let finished = std::time::Instant::now();
            result.expect("detach the fixture's image");
            (started, finished)
        });
        start.wait();
        let planner_started = std::time::Instant::now();
        let plan = plan_existing_host_storage(&mut source, &home);
        let planner_finished = std::time::Instant::now();
        let (detach_started, detach_finished) = detach.join().expect("detach worker");
        let overlap = planner_finished
            .min(detach_finished)
            .saturating_duration_since(planner_started.max(detach_started));
        assert!(
            !overlap.is_zero(),
            "the production planner and real image teardown must execute concurrently"
        );
        eprintln!(
            "inventory teardown race: planner={:?} detach={:?} overlap={overlap:?}",
            planner_finished.duration_since(planner_started),
            detach_finished.duration_since(detach_started),
        );
        let plan = plan.expect("home inventory must survive unrelated APFS teardown");
        assert_eq!(plan.substrate(), &expected);
    }

    /// Read-only host evidence that panics on any diskutil listing. The captured failure was
    /// `diskutil apfs list -plist` exiting 0 with an empty root dictionary; the only diskutil
    /// call planning may make is one `info -plist` FileVault attestation per reserved volume,
    /// which is recorded and delegated to the real tool.
    #[derive(Default)]
    struct ListingForbiddenHost {
        file_vault_queries: Mutex<Vec<String>>,
    }

    impl BootstrapHost for ListingForbiddenHost {
        fn verify_zfs_delegation(
            &self,
            _pool: &str,
            _required_root: &str,
        ) -> Result<(), HostError> {
            unreachable!("APFS planning has no ZFS evidence")
        }

        fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
            SystemBootstrapHost.inspect_mountpoint(path)
        }

        fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
            unreachable!("read-only planning created {path:?}")
        }

        fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
            unreachable!("read-only planning reclaimed {path:?}")
        }

        fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
            match (command.program(), command.args()) {
                (DISKUTIL, [info, plist, identifier])
                    if info == "info"
                        && plist == "-plist"
                        && valid_volume_identifier(identifier.as_bytes()) =>
                {
                    self.file_vault_queries
                        .lock()
                        .expect("query log")
                        .push(identifier.clone());
                    SystemBootstrapHost.run_command(command)
                }
                (DISKUTIL, args) => panic!("APFS planning invoked a diskutil listing: {args:?}"),
                (SECURITY, _) => Ok(HostCommandOutput::success(Vec::new())),
                (program, args) => panic!("unexpected planning command {program} {args:?}"),
            }
        }

        fn provision_apfs_volumes(
            &self,
            container: &str,
            _volumes: &[ApfsVolumeProvision],
        ) -> Result<(), HostError> {
            unreachable!("read-only planning provisioned in {container:?}")
        }

        fn write_file_atomic(&self, path: &Path, _contents: &[u8]) -> Result<(), HostError> {
            unreachable!("read-only planning wrote {path:?}")
        }

        fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
            unreachable!("read-only planning pinned fstab")
        }
    }

    #[test]
    fn production_planner_reads_kernel_authority_without_any_diskutil_listing() {
        let home = std::env::home_dir().expect("home directory");
        let snapshot = system_statfs(&home).expect("home filesystem");
        assert_eq!(snapshot.fs_type, "apfs");
        let device = exact_device_identifier(&snapshot.mount_source).expect("home device");

        let inventory = system_apfs_inventory().expect("kernel APFS inventory");
        let container = inventory
            .containing_container(&device)
            .expect("home device is registered in exactly one container");
        assert_eq!(
            Some(container.reference.as_str()),
            crate::device::container_of(&device)
        );
        let home_volume = container
            .volumes
            .iter()
            .find(|volume| volume.identifier == device)
            .expect("home volume record");
        assert_eq!(
            home_volume.mountpoint.as_deref(),
            Some(snapshot.mountpoint.as_path()),
            "the kernel mount table and statfs must name the same home mount"
        );

        let host = ListingForbiddenHost::default();
        let plan = plan_existing_host_storage(&mut SystemEvidenceSource { host: &host }, &home)
            .expect("production planner over the kernel snapshot");
        assert!(matches!(
            plan.substrate(),
            crate::storage::bootstrap::SelectedSubstrate::Apfs { container: selected, .. }
                if *selected == container.reference
        ));
        let reserved = inventory
            .containers
            .iter()
            .flat_map(|container| &container.volumes)
            .filter(|volume| volume.name == APFS_STORE_VOLUME || volume.name == APFS_CACHES_VOLUME)
            .map(|volume| volume.identifier.clone())
            .collect::<BTreeSet<_>>();
        let queried = host.file_vault_queries.into_inner().expect("query log");
        assert!(
            queried
                .iter()
                .all(|identifier| reserved.contains(identifier)),
            "FileVault is attested only for reserved volumes: {queried:?} vs {reserved:?}"
        );
    }

    fn mount_service_pins() -> Vec<FstabPin> {
        vec![
            FstabPin {
                volume_uuid: "FEC35F46-22C8-40BC-943A-ADC4BD39CAE5".to_owned(),
                mountpoint: PathBuf::from(STORE_ROOT),
                label: APFS_STORE_VOLUME.to_owned(),
            },
            FstabPin {
                volume_uuid: "D4B312DB-9378-4EC5-9B0B-8F244F1B38FA".to_owned(),
                mountpoint: PathBuf::from(CACHES_ROOT),
                label: APFS_CACHES_VOLUME.to_owned(),
            },
        ]
    }

    #[test]
    fn boot_mount_service_is_a_root_owned_shell_contract_not_a_cowshed_command() {
        let files = desired_mount_service(&mount_service_pins()).unwrap();
        let script = String::from_utf8(files.script.clone()).unwrap();
        let plist = String::from_utf8(files.plist.clone()).unwrap();

        assert!(script.starts_with("#!/bin/sh\nset -eu\n"));
        assert!(script.contains(
            "mount_volume 'cowshed.store' 'FEC35F46-22C8-40BC-943A-ADC4BD39CAE5' '/private/cowshed/store'"
        ));
        assert!(script.contains(
            "mount_volume 'cowshed.caches' 'D4B312DB-9378-4EC5-9B0B-8F244F1B38FA' '/private/cowshed/caches'"
        ));
        assert!(script.contains(
            "/usr/bin/security find-generic-password -a \"$label\" -s \"$label\" -w /Library/Keychains/System.keychain"
        ));
        assert!(
            script.contains(
                "/usr/sbin/diskutil apfs unlockVolume \"$uuid\" -nomount -stdinpassphrase"
            )
        );
        assert!(script.contains("/usr/sbin/diskutil mount -nobrowse -mountPoint"));
        assert!(!script.contains("cowshed setup"));
        assert!(plist.contains("<string>dev.cowshed.storage</string>"));
        assert!(plist.contains("<string>/bin/sh</string>"));
        assert!(plist.contains(MOUNT_SERVICE_SCRIPT));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<key>SuccessfulExit</key>"));
        Value::from_reader_xml(files.plist.as_slice()).expect("valid launchd plist");
        let mut shell = Command::new("/bin/sh")
            .arg("-n")
            .stdin(Stdio::piped())
            .spawn_locked()
            .expect("spawn system shell parser");
        shell
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(&files.script)
            .expect("write generated script");
        assert!(shell.wait().expect("wait for shell parser").success());
    }

    #[test]
    fn boot_mount_service_install_is_current_only_for_exact_loaded_contents() {
        let files = desired_mount_service(&mount_service_pins()).unwrap();
        assert!(mount_service_contents_are_current(
            &files,
            Some(&files.script),
            Some(&files.plist),
            true,
        ));
        assert!(!mount_service_contents_are_current(
            &files,
            Some(b"stale"),
            Some(&files.plist),
            true,
        ));
        assert!(!mount_service_contents_are_current(
            &files,
            Some(&files.script),
            Some(&files.plist),
            false,
        ));
    }

    struct MountServiceInstallHost {
        commands: mpsc::Sender<HostCommand>,
    }

    impl BootstrapHost for MountServiceInstallHost {
        fn verify_zfs_delegation(
            &self,
            _pool: &str,
            _required_root: &str,
        ) -> Result<(), HostError> {
            unreachable!("mount service install has no ZFS operation")
        }

        fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
            unreachable!("mount service install has no mountpoint inspection")
        }

        fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
            unreachable!("mount service install uses privileged install")
        }

        fn reclaim_mountpoint(&self, _path: &Path) -> Result<(), HostError> {
            unreachable!("mount service install reclaims nothing")
        }

        fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
            self.commands
                .send(command.clone())
                .expect("install command receiver remains alive");
            Ok(HostCommandOutput::default())
        }

        fn provision_apfs_volumes(
            &self,
            _container: &str,
            _volumes: &[ApfsVolumeProvision],
        ) -> Result<(), HostError> {
            unreachable!("mount service install provisions no volume")
        }

        fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
            unreachable!("mount service install writes through privileged install")
        }

        fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
            unreachable!("mount service install does not pin fstab")
        }
    }

    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_boot_mount_service_install_uses_fixed_root_owned_artifacts_and_system_launchd()
     {
        let (commands, received) = mpsc::channel();
        let host = MountServiceInstallHost { commands };
        let files = desired_mount_service(&mount_service_pins()).unwrap();
        install_mount_service_with(&host, &files).unwrap();
        let commands = received.try_iter().collect::<Vec<_>>();

        assert_eq!(
            commands
                .iter()
                .map(HostCommand::program)
                .collect::<Vec<_>>(),
            [
                LAUNCHCTL, INSTALL, INSTALL, INSTALL, LAUNCHCTL, LAUNCHCTL, LAUNCHCTL
            ]
        );
        assert_eq!(
            commands[0].args(),
            ["bootout", "system/dev.cowshed.storage"]
        );
        assert_eq!(
            commands[1].args(),
            [
                "-d",
                "-o",
                "root",
                "-g",
                "wheel",
                "-m",
                "755",
                MOUNT_SERVICE_DIRECTORY,
            ]
        );
        assert_eq!(
            commands[2].args().last().map(String::as_str),
            Some(MOUNT_SERVICE_SCRIPT)
        );
        assert_eq!(
            commands[3].args().last().map(String::as_str),
            Some(MOUNT_SERVICE_PLIST)
        );
        assert_eq!(commands[4].args(), ["enable", "system/dev.cowshed.storage"]);
        assert_eq!(
            commands[5].args(),
            ["bootstrap", "system", MOUNT_SERVICE_PLIST]
        );
        assert_eq!(
            commands[6].args(),
            ["kickstart", "-k", "system/dev.cowshed.storage"]
        );
    }

    #[test]
    fn boot_mount_service_rejects_non_uuid_identifiers_before_shell_rendering() {
        let mut pins = mount_service_pins();
        pins[0].volume_uuid = "disk3s8; touch /tmp/pwned".to_owned();
        let error = desired_mount_service(&pins).unwrap_err();
        assert!(matches!(
            error,
            NativeBootstrapError::MalformedApfsInventory(_)
        ));
    }

    /// Typed kernel evidence: IORegistry container records plus the kernel mount table, and
    /// the per-volume FileVault answer a reserved record's attestation would return.
    #[derive(Clone, Debug)]
    struct KernelFixture {
        containers: Vec<RegisteredApfsContainer>,
        mounts: Vec<KernelMountSnapshot>,
        file_vault: BTreeMap<String, bool>,
    }

    impl KernelFixture {
        fn inventory(&self) -> Result<ApfsInventory, NativeBootstrapError> {
            ApfsInventory::from_kernel(self.containers.clone(), &self.mounts)
        }
    }

    struct VolumeFixture {
        record: RegisteredApfsVolume,
        mountpoint: Option<PathBuf>,
        file_vault: bool,
    }

    /// A canonical uppercase volume UUID unique to `identifier`.
    fn volume_uuid(identifier: &str) -> String {
        let mut bytes = [0; 16];
        bytes[..identifier.len()].copy_from_slice(identifier.as_bytes());
        Uuid::from_bytes(bytes)
            .hyphenated()
            .to_string()
            .to_uppercase()
    }

    fn container(
        reference: &str,
        volumes: impl IntoIterator<Item = VolumeFixture>,
    ) -> KernelFixture {
        let mut records = Vec::new();
        let mut mounts = Vec::new();
        let mut file_vault = BTreeMap::new();
        for VolumeFixture {
            record,
            mountpoint,
            file_vault: enabled,
        } in volumes
        {
            if let Some(mountpoint) = mountpoint {
                mounts.push(KernelMountSnapshot::new(
                    0,
                    mountpoint,
                    format!("/dev/{}", record.identifier),
                    true,
                    true,
                ));
            }
            file_vault.insert(record.identifier.clone(), enabled);
            records.push(record);
        }
        KernelFixture {
            containers: vec![RegisteredApfsContainer {
                reference: reference.to_owned(),
                capacity_bytes: 1_000_000_000_000,
                volumes: records,
            }],
            mounts,
            file_vault,
        }
    }

    /// One kernel snapshot spanning several containers.
    fn kernel(parts: impl IntoIterator<Item = KernelFixture>) -> KernelFixture {
        let mut snapshot = KernelFixture {
            containers: Vec::new(),
            mounts: Vec::new(),
            file_vault: BTreeMap::new(),
        };
        for part in parts {
            snapshot.containers.extend(part.containers);
            snapshot.mounts.extend(part.mounts);
            snapshot.file_vault.extend(part.file_vault);
        }
        snapshot
    }

    fn volume(name: &str, identifier: &str, mountpoint: Option<&str>) -> VolumeFixture {
        volume_with_filevault(name, identifier, mountpoint, true)
    }

    fn volume_with_filevault(
        name: &str,
        identifier: &str,
        mountpoint: Option<&str>,
        file_vault: bool,
    ) -> VolumeFixture {
        VolumeFixture {
            record: RegisteredApfsVolume {
                name: name.to_owned(),
                identifier: identifier.to_owned(),
                volume_uuid: volume_uuid(identifier),
            },
            mountpoint: mountpoint.map(PathBuf::from),
            file_vault,
        }
    }

    struct FakeEvidenceSource {
        statfs: StatFsSnapshot,
        statfs_overrides: BTreeMap<PathBuf, StatFsSnapshot>,
        statfs_paths: Vec<PathBuf>,
        /// `Err` is an unreadable registry; the error kind is what IOKit would surface.
        kernel: Result<KernelFixture, io::ErrorKind>,
        inventory_reads: usize,
        file_vault_queries: Vec<String>,
        mountpoints: BTreeMap<PathBuf, MountpointState>,
        mounted_volumes: BTreeMap<PathBuf, MountedVolumeEvidence>,
        keychain_items: BTreeMap<&'static str, bool>,
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

        fn apfs_inventory(&mut self) -> Result<ApfsInventory, NativeBootstrapError> {
            self.inventory_reads += 1;
            match &self.kernel {
                Ok(kernel) => kernel.inventory(),
                Err(kind) => Err(NativeBootstrapError::ApfsRegistry {
                    source: io::Error::from(*kind),
                }),
            }
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

        fn volume_file_vault(
            &mut self,
            _container: &str,
            volume: &ApfsVolume,
        ) -> Result<bool, NativeBootstrapError> {
            self.file_vault_queries.push(volume.identifier.clone());
            self.kernel
                .as_ref()
                .ok()
                .and_then(|kernel| kernel.file_vault.get(&volume.identifier).copied())
                .ok_or_else(|| NativeBootstrapError::VolumeFileVaultEvidence {
                    identifier: volume.identifier.clone(),
                    reason: "no fixture answer".to_owned(),
                })
        }

        fn keychain_item_usable(
            &mut self,
            label: &'static str,
        ) -> Result<bool, NativeBootstrapError> {
            Ok(self.keychain_items.get(label).copied().unwrap_or(false))
        }

        fn invoking_identity(&mut self) -> (u32, u32) {
            self.invoking_identity
        }
    }

    fn source(kernel: KernelFixture) -> FakeEvidenceSource {
        FakeEvidenceSource {
            statfs: StatFsSnapshot {
                fs_type: "apfs".to_owned(),
                mount_source: PathBuf::from("/dev/disk3s5"),
                mountpoint: PathBuf::from("/System/Volumes/Data"),
                nobrowse: true,
            },
            statfs_overrides: BTreeMap::new(),
            statfs_paths: Vec::new(),
            kernel: Ok(kernel),
            inventory_reads: 0,
            file_vault_queries: Vec::new(),
            mountpoints: BTreeMap::from([
                (
                    PathBuf::from("/private/cowshed/store"),
                    MountpointState::EmptyDirectory,
                ),
                (
                    PathBuf::from("/private/cowshed/caches"),
                    MountpointState::Missing,
                ),
            ]),
            mounted_volumes: BTreeMap::from([
                (
                    PathBuf::from("/private/cowshed/store"),
                    MountedVolumeEvidence {
                        exact_identifier: "disk3s8".to_owned(),
                        mountpoint: PathBuf::from("/private/cowshed/store"),
                        nobrowse: true,
                        uid: 501,
                        gid: 20,
                    },
                ),
                (
                    PathBuf::from("/private/cowshed/caches"),
                    MountedVolumeEvidence {
                        exact_identifier: "disk3s9".to_owned(),
                        mountpoint: PathBuf::from("/private/cowshed/caches"),
                        nobrowse: true,
                        uid: 501,
                        gid: 20,
                    },
                ),
            ]),
            keychain_items: BTreeMap::from([(APFS_STORE_VOLUME, true), (APFS_CACHES_VOLUME, true)]),
            invoking_identity: (501, 20),
        }
    }

    fn healthy_existing_source() -> FakeEvidenceSource {
        let mut source = source(container(
            "disk3",
            [
                volume("Data", "disk3s5", Some("/System/Volumes/Data")),
                volume(APFS_STORE_VOLUME, "disk3s8", Some("/private/cowshed/store")),
                volume(
                    APFS_CACHES_VOLUME,
                    "disk3s9",
                    Some("/private/cowshed/caches"),
                ),
            ],
        ));
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
                        .to_json()
                        .expect("store marker"),
                ),
            },
        );
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/caches"),
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

    /// The canonical caches mountpoint is vacant; the kernel mount table alone says whether
    /// the caches volume is detached (`None`) or mounted somewhere else.
    fn source_with_caches_kernel_mount(kernel_mount: Option<&str>) -> FakeEvidenceSource {
        let mut source = healthy_existing_source();
        source.kernel = Ok(container(
            "disk3",
            [
                volume("Data", "disk3s5", Some("/System/Volumes/Data")),
                volume(APFS_STORE_VOLUME, "disk3s8", Some("/private/cowshed/store")),
                volume(APFS_CACHES_VOLUME, "disk3s9", kernel_mount),
            ],
        ));
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/caches"),
            MountpointState::Missing,
        );
        source
            .mounted_volumes
            .remove(Path::new("/private/cowshed/caches"));
        if let Some(mountpoint) = kernel_mount {
            let mountpoint = PathBuf::from(mountpoint);
            source.mounted_volumes.insert(
                mountpoint.clone(),
                MountedVolumeEvidence {
                    exact_identifier: "disk3s9".to_owned(),
                    mountpoint,
                    nobrowse: false,
                    uid: 501,
                    gid: 20,
                },
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
            let role = if path == Path::new("/private/cowshed/store") {
                VolumeRole::Store
            } else if path == Path::new("/private/cowshed/caches") {
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

    struct PartialProgressHost {
        fail_volume: &'static str,
    }

    impl BootstrapHost for PartialProgressHost {
        fn verify_zfs_delegation(
            &self,
            _pool: &str,
            _required_root: &str,
        ) -> Result<(), HostError> {
            Err(HostError::new("unexpected ZFS operation"))
        }

        fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
            Err(HostError::new("unexpected mountpoint inspection"))
        }

        fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
            Err(HostError::new("unexpected directory creation"))
        }

        fn reclaim_mountpoint(&self, _path: &Path) -> Result<(), HostError> {
            Err(HostError::new("unexpected reclaim"))
        }

        fn run_command(&self, _command: &HostCommand) -> Result<HostCommandOutput, HostError> {
            Err(HostError::new("unexpected command"))
        }

        fn provision_apfs_volumes(
            &self,
            _container: &str,
            volumes: &[ApfsVolumeProvision],
        ) -> Result<(), HostError> {
            let [volume] = volumes else {
                return Err(HostError::new("setup action was not split by volume"));
            };
            if volume.name() == self.fail_volume {
                return Err(HostError::new(format!("{} action failed", volume.name())));
            }
            Ok(())
        }

        fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
            Err(HostError::new("unexpected marker write"))
        }

        fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
            Err(HostError::new("pin must be skipped after volume failure"))
        }
    }

    #[derive(Default)]
    struct ValidationLane {
        dispatches: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlockingLane for ValidationLane {
        async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            job()
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum ProvisionEvent {
        Acquire,
        Inventory,
        Command { program: String, args: Vec<String> },
        Prepare(PathBuf),
        AttestMounted(PathBuf, String, bool),
        AttestOwner(PathBuf, u32, u32),
        Marker(PathBuf, Vec<u8>),
        Free,
    }

    struct FakePrivilegedSession {
        events: Rc<RefCell<Vec<ProvisionEvent>>>,
        outputs: VecDeque<Result<HostCommandOutput, HostError>>,
    }

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

    impl Drop for FakePrivilegedSession {
        fn drop(&mut self) {
            self.events.borrow_mut().push(ProvisionEvent::Free);
        }
    }

    /// What the create recheck's kernel read observes inside the authorization session.
    enum KernelRead {
        Snapshot(KernelFixture),
        Unreadable(io::ErrorKind),
        /// Batches without a create must never read the inventory.
        Forbidden,
    }

    struct FakeProvisionIo {
        events: Rc<RefCell<Vec<ProvisionEvent>>>,
        kernel: KernelRead,
    }

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

        fn apfs_inventory(&self) -> Result<ApfsInventory, NativeBootstrapError> {
            self.events.borrow_mut().push(ProvisionEvent::Inventory);
            match &self.kernel {
                KernelRead::Snapshot(kernel) => kernel.inventory(),
                KernelRead::Unreadable(kind) => Err(NativeBootstrapError::ApfsRegistry {
                    source: io::Error::from(*kind),
                }),
                KernelRead::Forbidden => {
                    panic!("a batch without a create read the kernel APFS inventory")
                }
            }
        }
    }

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
    fn exact_container_is_selected_from_one_kernel_snapshot() {
        let mut source = source(kernel([
            container("disk2", [volume("Data", "disk2s1", None)]),
            container("disk3", [volume("Data", "disk3s5", Some("/"))]),
        ]));
        let gathered = gather_apfs_evidence(
            &mut source,
            Path::new("/Users/alice/project"),
            Path::new("/Users/alice"),
        )
        .unwrap();
        assert!(matches!(
            &gathered.statfs,
            StatFsEvidence::Apfs { container: Some(value), .. } if value == "disk3"
        ));
        assert_eq!(
            source.inventory_reads, 1,
            "home selection and the global guard share one snapshot"
        );
    }

    #[test]
    fn home_anchor_selects_host_container_when_project_is_on_another_filesystem() {
        let mut source = source(kernel([
            container(
                "disk13",
                [volume("Workspace", "disk13s1", Some("/workspace"))],
            ),
            container(
                "disk3",
                [
                    volume("Data", "disk3s5", Some("/System/Volumes/Data")),
                    volume(APFS_STORE_VOLUME, "disk3s8", Some("/private/cowshed/store")),
                    volume(
                        APFS_CACHES_VOLUME,
                        "disk3s9",
                        Some("/private/cowshed/caches"),
                    ),
                ],
            ),
        ]));
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
            PathBuf::from("/private/cowshed/store"),
            MountpointState::Mounted {
                marker: Some(
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
                        .to_json()
                        .unwrap(),
                ),
            },
        );
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/caches"),
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
    fn duplicate_ambiguous_and_malformed_kernel_records_fail_closed() {
        let gather = |kernel: KernelFixture| {
            let mut evidence = source(kernel);
            let result = gather_apfs_evidence(
                &mut evidence,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice"),
            )
            .map(|_| ());
            assert_eq!(
                evidence.inventory_reads, 1,
                "a refused snapshot is never reread"
            );
            result
        };
        let home = || {
            container(
                "disk3",
                [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
            )
        };

        let unmounted_home = || container("disk3", [volume("Data", "disk3s5", None)]);
        assert!(matches!(
            gather(kernel([unmounted_home(), unmounted_home()])),
            Err(NativeBootstrapError::DuplicateApfsContainer(reference)) if reference == "disk3"
        ));
        assert!(matches!(
            gather(container(
                "disk3",
                [
                    volume("Data", "disk3s5", None),
                    volume("Shadow", "disk3s5", None),
                ],
            )),
            Err(NativeBootstrapError::DuplicateApfsVolume(identifier)) if identifier == "disk3s5"
        ));
        let mut mounted_twice = home();
        mounted_twice.mounts.push(KernelMountSnapshot::new(
            1,
            "/Volumes/Data",
            "/dev/disk3s5",
            true,
            true,
        ));
        assert!(matches!(
            gather(mounted_twice),
            Err(NativeBootstrapError::AmbiguousVolumeMount { identifier, mountpoints })
                if identifier == "disk3s5" && mountpoints.len() == 2
        ));

        let mut invalid_reference = home();
        invalid_reference.containers[0].reference = "not-a-device".to_owned();
        let mut no_capacity = home();
        no_capacity.containers[0].capacity_bytes = 0;
        let mut foreign_volume = home();
        foreign_volume.containers[0].volumes[0].identifier = "disk4s5".to_owned();
        let mut snapshot_depth = home();
        snapshot_depth.containers[0].volumes[0].identifier = "disk3s5s1".to_owned();
        let mut unnamed = home();
        unnamed.containers[0].volumes[0].name = String::new();
        let mut invalid_uuid = home();
        invalid_uuid.containers[0].volumes[0].volume_uuid = "disk3s5-UUID".to_owned();
        let mut braced_uuid = home();
        braced_uuid.containers[0].volumes[0].volume_uuid =
            format!("{{{}}}", volume_uuid("disk3s5"));
        let mut nil_uuid = home();
        nil_uuid.containers[0].volumes[0].volume_uuid =
            "00000000-0000-0000-0000-000000000000".to_owned();
        for malformed in [
            invalid_reference,
            no_capacity,
            foreign_volume,
            snapshot_depth,
            unnamed,
            invalid_uuid,
            braced_uuid,
            nil_uuid,
        ] {
            assert!(matches!(
                gather(malformed),
                Err(NativeBootstrapError::MalformedApfsInventory(_))
            ));
        }
    }

    /// The captured failure: an image teardown made the listing read back an empty root. The
    /// kernel registry is the only listing now; an empty or unreadable snapshot is refused by
    /// every planner after exactly one read, never repaired into "no reserved volumes exist".
    #[test]
    fn empty_or_unreadable_kernel_snapshot_never_establishes_absence() {
        // No mountpoint evidence is staged: classifying any volume before the snapshot is
        // refused would surface as `MountEvidenceMismatch`, not as the inventory error.
        let empty = || {
            let mut evidence = source(kernel([]));
            evidence.mountpoints.clear();
            evidence
        };
        let unreadable = || {
            let mut evidence = empty();
            evidence.kernel = Err(io::ErrorKind::InvalidData);
            evidence
        };
        let mut evidence = empty();
        assert!(matches!(
            plan_existing_host_storage(&mut evidence, Path::new("/Users/alice")),
            Err(NativeBootstrapError::EmptyApfsInventory)
        ));
        assert_eq!(evidence.inventory_reads, 1);
        let mut evidence = unreadable();
        assert!(matches!(
            plan_existing_host_storage(&mut evidence, Path::new("/Users/alice")),
            Err(NativeBootstrapError::ApfsRegistry { source })
                if source.kind() == io::ErrorKind::InvalidData
        ));
        assert_eq!(evidence.inventory_reads, 1);
        for mut evidence in [empty(), unreadable()] {
            assert!(matches!(
                prepare_setup_snapshot(&mut evidence, Path::new("/Users/alice"), ""),
                Err(NativeBootstrapError::EmptyApfsInventory
                    | NativeBootstrapError::ApfsRegistry { .. })
            ));
            assert_eq!(evidence.inventory_reads, 1);
        }
        for mut evidence in [empty(), unreadable()] {
            assert!(matches!(
                plan_native_bootstrap(
                    &mut evidence,
                    Path::new("/Users/alice/project"),
                    Path::new("/Users/alice"),
                ),
                Err(NativeBootstrapError::EmptyApfsInventory
                    | NativeBootstrapError::ApfsRegistry { .. })
            ));
        }
    }

    #[test]
    fn statfs_identity_marker_and_kernel_mount_classify_the_store() {
        let marker = VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs)
            .to_json()
            .unwrap();
        let store_mounted_at = |mountpoint: Option<&str>| {
            container(
                "disk3",
                [
                    volume(APFS_STORE_VOLUME, "disk3s8", mountpoint),
                    volume("Data", "disk3s5", Some("/System/Volumes/Data")),
                ],
            )
        };
        let inventory = store_mounted_at(Some("/private/cowshed/store"));
        let mut valid = source(inventory.clone());
        valid.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
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
            PathBuf::from("/private/cowshed/store"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        wrong_owner
            .mounted_volumes
            .get_mut(Path::new("/private/cowshed/store"))
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
            PathBuf::from("/private/cowshed/store"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        wrong_group
            .mounted_volumes
            .get_mut(Path::new("/private/cowshed/store"))
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
            PathBuf::from("/private/cowshed/store"),
            MountpointState::Mounted {
                marker: Some(marker.clone()),
            },
        );
        wrong_flags
            .mounted_volumes
            .get_mut(Path::new("/private/cowshed/store"))
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
                && current_mountpoint == Path::new("/private/cowshed/store")
        ));

        let mut incomplete = source(inventory.clone());
        incomplete.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
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

        let mut detached = source(store_mounted_at(None));
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
            PathBuf::from("/private/cowshed/store"),
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

        // The kernel mounts the store elsewhere, but statfs at that path names another device.
        let mut mismatched = source(store_mounted_at(Some("/Volumes/cowshed.store")));
        mismatched.mounted_volumes.insert(
            PathBuf::from("/Volumes/cowshed.store"),
            MountedVolumeEvidence {
                exact_identifier: "disk3s9".to_owned(),
                mountpoint: PathBuf::from("/Volumes/cowshed.store"),
                nobrowse: false,
                uid: 0,
                gid: 0,
            },
        );
        assert!(matches!(
            gather_apfs_evidence(
                &mut mismatched,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice")
            ),
            Err(NativeBootstrapError::MountEvidenceMismatch { path, identifier })
                if path == Path::new("/Volumes/cowshed.store") && identifier == "disk3s9"
        ));

        let mut duplicate = source(container(
            "disk3",
            [
                volume(APFS_STORE_VOLUME, "disk3s8", None),
                volume(APFS_STORE_VOLUME, "disk3s9", None),
                volume("Data", "disk3s5", None),
            ],
        ));
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
        let inventory = container(
            "disk3",
            [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
        );
        let mut source = source(inventory);
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
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
        let mut source = source(container(
            "disk3",
            [
                volume(APFS_STORE_VOLUME, "disk3s8", Some("/Volumes/cowshed.store")),
                volume(
                    APFS_CACHES_VOLUME,
                    "disk3s9",
                    Some("/Volumes/cowshed.caches"),
                ),
                volume("Data", "disk3s5", Some("/System/Volumes/Data")),
            ],
        ));
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
            MountpointState::ReclaimableStub {
                paths: vec![PathBuf::from("/private/cowshed/store/sccache.sock")],
            },
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
                    if path == Path::new("/private/cowshed/store")
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
        assert_eq!(source.inventory_reads, 1);
    }

    #[test]
    fn reclaimable_launchd_stub_classifies_as_mis_mounted_when_volume_exists() {
        let mut source = source(container(
            "disk3",
            [
                volume(APFS_STORE_VOLUME, "disk3s8", Some("/Volumes/cowshed.store")),
                volume("Data", "disk3s5", Some("/System/Volumes/Data")),
            ],
        ));
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
            MountpointState::ReclaimableStub {
                paths: vec![PathBuf::from("/private/cowshed/store/sccache.sock")],
            },
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

    #[test]
    fn safe_masking_topology_is_an_enumerated_reclaim_plan_not_a_fatal_error() {
        let mut source = source(container(
            "disk3",
            [
                volume("Data", "disk3s5", Some("/System/Volumes/Data")),
                volume(APFS_STORE_VOLUME, "disk3s8", None),
                volume(APFS_CACHES_VOLUME, "disk3s9", None),
            ],
        ));
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
            MountpointState::ReclaimableStub {
                paths: vec![
                    PathBuf::from("/private/cowshed/caches"),
                    PathBuf::from("/private/cowshed/store/sccache.sock"),
                ],
            },
        );

        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect("safe stubs must become a repair plan");
        assert_eq!(
            snapshot.actions.first(),
            Some(&HostAction::ReclaimStubs {
                paths: vec![
                    PathBuf::from("/private/cowshed/caches"),
                    PathBuf::from("/private/cowshed/store/sccache.sock"),
                ],
            })
        );
        assert_eq!(
            snapshot
                .actions
                .iter()
                .filter(|action| matches!(action, HostAction::MountExisting { .. }))
                .count(),
            2
        );
        assert!(
            !snapshot
                .actions
                .iter()
                .any(|action| matches!(action, HostAction::CreateVolume { .. }))
        );
        assert!(
            HostSetupPlan::new(snapshot.actions, snapshot.volumes, true).non_destructive,
            "mounting existing volumes and reclaiming known stubs is non-destructive"
        );
    }

    #[test]
    fn command_runner_uses_argv_and_preserves_failure_status_and_streams() {
        let seen = RefCell::new(VecDeque::new());
        let command = HostCommand::new(
            "/usr/sbin/diskutil",
            ["info", "-plist", "literal;not-shell"],
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
            "command failed: executable \"/usr/sbin/diskutil\", argv [\"info\", \"-plist\", \"literal;not-shell\"], exit status 1; stdout: ignored; stderr: diskutil exact failure"
        );
        assert_eq!(
            seen.into_inner().pop_front().unwrap(),
            (
                PathBuf::from("/usr/sbin/diskutil"),
                vec![
                    "info".to_owned(),
                    "-plist".to_owned(),
                    "literal;not-shell".to_owned(),
                ]
            )
        );
    }

    #[test]
    fn one_authorization_session_wraps_fixed_argv_for_all_new_volumes() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
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
                "/private/cowshed/store",
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
                "/private/cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volumes = [
            ApfsVolumeProvision {
                name: APFS_STORE_VOLUME,
                mountpoint: PathBuf::from("/private/cowshed/store"),
                role: VolumeRole::Store,
                kind: ApfsProvisionKind::Create,
            },
            ApfsVolumeProvision {
                name: APFS_CACHES_VOLUME,
                mountpoint: PathBuf::from("/private/cowshed/caches"),
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
                kernel: KernelRead::Snapshot(container(
                    "disk3",
                    [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
                )),
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
        assert_eq!(commands.len(), 10);
        assert!(
            matches!(
                events.as_slice(),
                [
                    ProvisionEvent::Acquire,
                    ProvisionEvent::Inventory,
                    ProvisionEvent::Prepare(_),
                    ..
                ]
            ),
            "the kernel recheck runs inside the session, before any create: {events:?}"
        );
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
                    &vec!["501:20".to_owned(), "/private/cowshed/store".to_owned()]
                ),
                &(
                    CHOWN,
                    &vec!["501:20".to_owned(), "/private/cowshed/caches".to_owned()]
                ),
            ]
        );
        for root in [
            Path::new("/private/cowshed/store"),
            Path::new("/private/cowshed/caches"),
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

    #[test]
    fn execution_time_kernel_recheck_refuses_duplicate_create() {
        let store = || ApfsVolumeProvision {
            name: APFS_STORE_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/store"),
            role: VolumeRole::Store,
            kind: ApfsProvisionKind::Create,
        };
        let provision = |kernel: KernelRead| {
            let events = Rc::new(RefCell::new(Vec::new()));
            let acquire_events = Rc::clone(&events);
            let result = provision_apfs_volumes_with(
                "disk3",
                &[store()],
                501,
                20,
                move || {
                    acquire_events.borrow_mut().push(ProvisionEvent::Acquire);
                    Ok(FakePrivilegedSession {
                        events: Rc::clone(&acquire_events),
                        outputs: VecDeque::new(),
                    })
                },
                &FakeProvisionIo {
                    events: Rc::clone(&events),
                    kernel,
                },
            );
            let events = events.borrow().clone();
            (result, events)
        };
        let refused_before_any_command = |events: &[ProvisionEvent]| {
            events
                == [
                    ProvisionEvent::Acquire,
                    ProvisionEvent::Inventory,
                    ProvisionEvent::Free,
                ]
        };

        let (result, events) = provision(KernelRead::Snapshot(kernel([
            container("disk3", [volume("Data", "disk3s5", None)]),
            container(
                "disk7",
                [volume(
                    APFS_STORE_VOLUME,
                    "disk7s2",
                    Some("/Volumes/cowshed.store"),
                )],
            ),
        ])));
        let error = result.expect_err("an existing reserved-name volume must prevent addVolume");
        assert!(error.to_string().contains("already exists as disk7s2"));
        assert!(refused_before_any_command(&events), "{events:?}");

        let (result, events) = provision(KernelRead::Snapshot(kernel([])));
        assert!(
            result
                .expect_err("an empty snapshot must not authorize a create")
                .to_string()
                .contains("no containers")
        );
        assert!(refused_before_any_command(&events), "{events:?}");

        let (result, events) = provision(KernelRead::Unreadable(io::ErrorKind::InvalidData));
        assert!(
            result
                .expect_err("an unreadable registry must not authorize a create")
                .to_string()
                .contains("kernel APFS registry")
        );
        assert!(refused_before_any_command(&events), "{events:?}");
    }

    #[test]
    fn markerless_exact_mount_is_repaired_without_create_or_mount() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
            Ok(HostCommandOutput::success(provision_info(
                "disk3s9",
                "disk3",
                APFS_CACHES_VOLUME,
                "/private/cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/caches"),
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
                kernel: KernelRead::Forbidden,
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
            ["502:80", "/private/cowshed/caches"]
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
                "/private/cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/caches"),
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
                kernel: KernelRead::Forbidden,
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
                "/private/cowshed/caches",
                "disk3s9",
            ]
        );
        assert_eq!(commands[2].0, DISKUTIL);
        assert_eq!(commands[2].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[3].0, CHOWN);
        assert_eq!(
            commands[3].1.as_slice(),
            ["503:20", "/private/cowshed/caches"]
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
                "/private/cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/caches"),
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
                kernel: KernelRead::Forbidden,
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
                "/private/cowshed/caches",
                "disk3s9",
            ]
        );
        assert_eq!(commands[3].1.as_slice(), ["info", "-plist", "disk3s9"]);
        assert_eq!(commands[4].0, CHOWN);
        assert_eq!(
            commands[4].1.as_slice(),
            ["504:20", "/private/cowshed/caches"]
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
                        if path == Path::new("/private/cowshed/caches")
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
    #[test]
    fn an_auto_mounted_new_volume_is_detached_before_its_private_mount() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let outputs = VecDeque::from([
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
                "/private/cowshed/store",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volumes = [ApfsVolumeProvision {
            name: APFS_STORE_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/store"),
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
                kernel: KernelRead::Snapshot(container(
                    "disk3",
                    [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
                )),
            },
        )
        .unwrap();

        let events = events.borrow();
        assert!(
            matches!(
                events.as_slice(),
                [ProvisionEvent::Acquire, ProvisionEvent::Inventory, ..]
            ),
            "the kernel inventory is rechecked inside the authorization session"
        );
        let commands: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ProvisionEvent::Command { args, .. } => Some(args.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            commands[0].as_slice(),
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
        assert_eq!(commands[1].as_slice(), ["info", "-plist", "disk3s8"]);
        assert_eq!(
            commands[2].as_slice(),
            ["unmount", "disk3s8"],
            "the default mount is dropped before the private mount"
        );
        assert_eq!(
            commands[3].as_slice(),
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/private/cowshed/store",
                "disk3s8",
            ]
        );
    }

    /// A volume recorded as detached can be auto-mounted by the system between
    /// discovery and the recovery attestation. That is the shape most likely to
    /// strand an existing installation, so it converges the same way.
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
                "/private/cowshed/caches",
            ))),
            Ok(HostCommandOutput::default()),
        ]);
        let volumes = [ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/caches"),
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
                kernel: KernelRead::Forbidden,
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
                "/private/cowshed/caches",
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
    #[test]
    fn created_volume_attestation_admits_only_detached_or_the_default_mount() {
        let attest = |mountpoint: &str| {
            super::attest_created_apfs_info(
                &provision_info("disk3s8", "disk3", APFS_STORE_VOLUME, mountpoint),
                "disk3s8",
                "disk3",
                APFS_STORE_VOLUME,
            )
        };

        assert_eq!(attest("").unwrap(), super::CreatedMountState::Unmounted);
        assert_eq!(
            attest("/Volumes/cowshed.store").unwrap(),
            super::CreatedMountState::AutoMounted
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

    #[test]
    fn authorization_denial_and_child_failure_propagate_without_marker_publication() {
        let volume = ApfsVolumeProvision {
            name: APFS_CACHES_VOLUME,
            mountpoint: PathBuf::from("/private/cowshed/caches"),
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
                kernel: KernelRead::Forbidden,
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
                    outputs: VecDeque::from([Ok(HostCommandOutput::failure(1, "child failed\n"))]),
                })
            },
            &FakeProvisionIo {
                events: Rc::clone(&failed_events),
                kernel: KernelRead::Snapshot(container("disk3", [])),
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
    fn declined_authorization_is_typed_policy_denial_with_nothing_changed() {
        for status in [AUTHORIZATION_DENIED, AUTHORIZATION_CANCELED] {
            let host = authorization_status("preauthorize privileged execution", status)
                .expect_err("decline must be typed");
            assert!(host.is_authorization_denied());
            let error = setup_execution_error(NativeBootstrapError::Host(host), "cowshed setup");
            assert_eq!(error.code, crate::error::ErrorCode::SandboxDenied);
            assert_eq!(error.exit_code(), 6);
            assert_eq!(
                error.message,
                "authorization was declined; nothing was changed"
            );
            assert_eq!(error.hint, "cowshed setup");
        }
    }

    #[test]
    fn volume_container_is_the_synthesized_disk_of_the_volume_identifier() {
        use crate::device::container_of;
        assert_eq!(container_of("disk3s5"), Some("disk3"));
        assert_eq!(container_of("disk13s1"), Some("disk13"));
        // A sealed system snapshot mounts as `<volume>s<snapshot>`; the container is unchanged,
        // which is why kernel volume records must also be exactly volume depth.
        assert_eq!(container_of("disk3s1s1"), Some("disk3"));
        assert!(!valid_volume_identifier(b"disk3s1s1"));
        // Input that never named a device is refused, not sliced.
        assert_eq!(container_of("not-a-disk"), None);
        assert_eq!(container_of("disk01s1"), None);
    }

    #[tokio::test]
    async fn validator_reports_mis_mounted_volume_without_executing_remount() {
        let mut evidence_source = source_with_caches_kernel_mount(Some("/Volumes/cowshed.caches"));
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

        let mut plan_source = source_with_caches_kernel_mount(Some("/Volumes/cowshed.caches"));
        let plan = plan_existing_host_storage(&mut plan_source, Path::new("/Users/alice"))
            .expect("mis-mounted caches repair plan");
        assert!(mutating_setup_actions(&plan).is_empty());
        let host = Arc::new(ReadOnlyValidationHost::default());
        let lane = ValidationLane::default();
        let error = validate_existing_plan(&plan, Arc::clone(&host), &lane)
            .await
            .expect_err("validation must not heal a mis-mounted volume");
        assert_eq!(error.hint, "cowshed setup");
        assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn kernel_unmounted_volume_requires_recovery() {
        let mut evidence_source = source_with_caches_kernel_mount(None);
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

        let mut plan_source = source_with_caches_kernel_mount(None);
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
    fn detached_reserved_volume_in_another_container_is_found_elsewhere() {
        let mut evidence = source(kernel([
            container("disk3", [volume("Data", "disk3s5", None)]),
            container(
                "disk7",
                [volume_with_filevault(
                    APFS_STORE_VOLUME,
                    "disk7s2",
                    None,
                    false,
                )],
            ),
        ]));
        let gathered = gather_existing_apfs_evidence(&mut evidence, Path::new("/Users/alice"))
            .expect("a foreign reserved volume is classified, never created over");
        assert!(matches!(
            gathered.bootstrap,
            BootstrapEvidence::Apfs {
                store: ExistingStorage::FoundElsewhere { container, device, .. },
                caches: ExistingStorage::Absent,
            } if container == "disk7" && device == "disk7s2"
        ));
    }

    #[test]
    fn global_inventory_prevents_create_when_reserved_volume_is_in_another_container() {
        let mut source = source(kernel([
            container(
                "disk3",
                [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
            ),
            container(
                "disk7",
                [volume(
                    APFS_STORE_VOLUME,
                    "disk7s2",
                    Some("/Volumes/cowshed.store"),
                )],
            ),
        ]));

        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect("cross-container volume must be repairable");
        let plan = &snapshot.plan;
        assert!(plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::RunCommand(command)
                if command.program() == DISKUTIL
                    && command.args() == ["unmount", "force", "disk7s2"]
        )));
        assert!(snapshot.actions.iter().any(|action| matches!(
            action,
            HostAction::RepairMounted {
                name,
                uuid,
                size_bytes: 1_000_000_000_000,
                mounted_at,
                mount_at,
            } if name == APFS_STORE_VOLUME
                && uuid == &volume_uuid("disk7s2")
                && mounted_at == Path::new("/Volumes/cowshed.store")
                && mount_at == Path::new("/private/cowshed/store")
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
        assert!(snapshot.actions.iter().any(|action| matches!(
            action,
            HostAction::CreateVolume { name, container, mount_at }
                if name == APFS_CACHES_VOLUME
                    && container == "disk3"
                    && mount_at == Path::new("/private/cowshed/caches")
        )));
        let public_plan = HostSetupPlan::new(
            snapshot.actions.clone(),
            snapshot.volumes.clone(),
            setup_requires_authorization(&snapshot),
        );
        assert!(!public_plan.non_destructive);
        assert_eq!(source.inventory_reads, 1);
    }

    #[tokio::test]
    async fn valid_manually_created_volumes_are_pinned_without_reprovisioning() {
        let mut source = healthy_existing_source();
        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect("healthy manually-created volumes plan");
        assert!(
            !snapshot
                .plan
                .operations()
                .iter()
                .any(|operation| matches!(operation, HostOperation::ProvisionApfsVolumes { .. }))
        );
        assert!(snapshot.plan.operations().iter().any(|operation| matches!(
            operation,
            HostOperation::PinVolumesInFstab { pins }
                if pins.len() == 2
                    && pins.iter().any(|pin| pin.volume_uuid == volume_uuid("disk3s8"))
                    && pins.iter().any(|pin| pin.volume_uuid == volume_uuid("disk3s9"))
        )));
        assert!(setup_requires_authorization(&snapshot));
        assert_eq!(
            snapshot.actions,
            vec![
                HostAction::PinFstab {
                    uuid: volume_uuid("disk3s8"),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                },
                HostAction::PinFstab {
                    uuid: volume_uuid("disk3s9"),
                    mount_at: PathBuf::from("/private/cowshed/caches"),
                },
            ]
        );
        assert!(
            HostSetupPlan::new(snapshot.actions.clone(), snapshot.volumes.clone(), true)
                .non_destructive,
            "pinning existing volumes is non-destructive"
        );

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
        let action_host: Arc<dyn BootstrapHost> = Arc::new(ReadOnlyValidationHost::default());
        let (outcomes, failure) = execute_snapshot_actions(&snapshot, action_host).await;
        assert!(failure.is_none());
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome.outcome, HostActionResult::Done))
        );
    }

    #[tokio::test]
    async fn mid_sequence_failure_reports_done_failed_and_skipped_actions() {
        let mut source = healthy_existing_source();
        for path in [
            PathBuf::from("/private/cowshed/store"),
            PathBuf::from("/private/cowshed/caches"),
        ] {
            source
                .mountpoints
                .insert(path, MountpointState::Mounted { marker: None });
        }
        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect("two-volume repair snapshot");
        assert_eq!(snapshot.actions.len(), 4);
        let host: Arc<dyn BootstrapHost> = Arc::new(PartialProgressHost {
            fail_volume: APFS_CACHES_VOLUME,
        });

        let (outcomes, failure) = execute_snapshot_actions(&snapshot, Arc::clone(&host)).await;
        let failure = failure.expect("second volume fails");
        assert!(failure.message.contains("cowshed.caches action failed"));
        assert!(matches!(outcomes[0].outcome, HostActionResult::Done));
        assert!(matches!(
            &outcomes[1].outcome,
            HostActionResult::Failed { error }
                if error.message.contains("cowshed.caches action failed")
        ));
        assert!(matches!(outcomes[2].outcome, HostActionResult::Skipped));
        assert!(matches!(outcomes[3].outcome, HostActionResult::Skipped));
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
                pins_to_remove: vec!["cowshed.store".to_owned(), "cowshed.caches".to_owned()],
                requires_authorization: true,
            }
        );

        let clean =
            host_uninstall_plan_from_text(Path::new("/Users/alice"), "LABEL=nix /nix apfs rw\n")
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
    fn setup_and_uninstall_report_json_is_frozen_camel_case() {
        let setup = crate::api::JsonEnvelope::success(HostSetupReport {
            action_outcomes: Vec::new(),
            volumes: vec![
                VolumeOutcome {
                    name: APFS_STORE_VOLUME.to_owned(),
                    role: VolumeRole::Store,
                    state_before: VolumeState::FoundElsewhere {
                        container: "disk4".to_owned(),
                        device: "disk4s7".to_owned(),
                        mounted_at: None,
                    },
                    action: "reported".to_owned(),
                },
                VolumeOutcome {
                    name: APFS_CACHES_VOLUME.to_owned(),
                    role: VolumeRole::Caches,
                    state_before: VolumeState::MisMounted {
                        mounted_at: PathBuf::from("/Volumes/cowshed.caches"),
                    },
                    action: "remounted".to_owned(),
                },
            ],
            fstab: FstabOutcome::Skipped("volume elsewhere".to_owned()),
            authorized: false,
        });
        assert_eq!(
            serde_json::to_string(&setup).expect("setup envelope"),
            "{\"ok\":true,\"result\":{\"actionOutcomes\":[],\"volumes\":[{\"name\":\"cowshed.store\",\"role\":\"store\",\"stateBefore\":{\"foundElsewhere\":{\"container\":\"disk4\",\"device\":\"disk4s7\",\"mountedAt\":null}},\"action\":\"reported\"},{\"name\":\"cowshed.caches\",\"role\":\"caches\",\"stateBefore\":{\"misMounted\":{\"mountedAt\":\"/Volumes/cowshed.caches\"}},\"action\":\"remounted\"}],\"fstab\":{\"skipped\":\"volume elsewhere\"},\"authorized\":false}}"
        );

        let action = HostAction::RepairMounted {
            name: APFS_STORE_VOLUME.to_owned(),
            uuid: "STORE-UUID".to_owned(),
            size_bytes: 1_000_000_000_000,
            mounted_at: PathBuf::from("/Volumes/cowshed.store"),
            mount_at: PathBuf::from("/private/cowshed/store"),
        };
        assert_eq!(
            serde_json::to_string(&action).expect("host action"),
            "{\"repairMounted\":{\"name\":\"cowshed.store\",\"uuid\":\"STORE-UUID\",\"sizeBytes\":1000000000000,\"mountedAt\":\"/Volumes/cowshed.store\",\"mountAt\":\"/private/cowshed/store\"}}"
        );

        let failed = HostActionOutcome {
            action: HostAction::PinFstab {
                uuid: "STORE-UUID".to_owned(),
                mount_at: PathBuf::from("/private/cowshed/store"),
            },
            outcome: HostActionResult::Failed {
                error: CowshedError::internal("install failed"),
            },
        };
        assert_eq!(
            serde_json::to_string(&failed).expect("failed outcome"),
            "{\"action\":{\"pinFstab\":{\"uuid\":\"STORE-UUID\",\"mountAt\":\"/private/cowshed/store\"}},\"outcome\":{\"failed\":{\"error\":{\"code\":\"internal\",\"message\":\"install failed\",\"hint\":\"cowshed doctor --json\"}}}}"
        );

        let uninstall = crate::api::JsonEnvelope::success(UninstallReport {
            fstab: UninstallFstabOutcome::Removed,
            services: Vec::new(),
        });
        assert_eq!(
            serde_json::to_string(&uninstall).expect("uninstall envelope"),
            "{\"ok\":true,\"result\":{\"fstab\":\"removed\",\"services\":[]}}"
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
        assert_eq!(host_setup_actions(&snapshot), Vec::<HostAction>::new());
        assert!(!setup_requires_authorization(&snapshot));
        assert!(matches!(snapshot.fstab, PlannedFstab::AlreadyCurrent));
    }

    #[tokio::test]
    async fn existing_host_storage_returns_roots_and_queries_only_home_without_mutation() {
        let mut source = healthy_existing_source();
        let plan = plan_existing_host_storage(&mut source, Path::new("/Users/alice"))
            .expect("healthy plan");

        assert_eq!(source.statfs_paths, [PathBuf::from("/Users/alice")]);
        assert_eq!(source.inventory_reads, 1);
        assert_eq!(source.file_vault_queries, ["disk3s8", "disk3s9"]);
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
        assert_eq!(validated.store(), Path::new("/private/cowshed/store"));
        assert_eq!(validated.caches(), Path::new("/private/cowshed/caches"));
        assert_eq!(
            validated.telemetry(),
            Path::new("/private/cowshed/store/telemetry")
        );
        assert_eq!(host.inspections.load(Ordering::SeqCst), 0);
        assert_eq!(host.mutation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(host.authorization_calls.load(Ordering::SeqCst), 0);
        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_marker_is_setup_required_before_executor_dispatch() {
        let mut source = healthy_existing_source();
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/store"),
            MountpointState::Mounted { marker: None },
        );
        source.mountpoints.insert(
            PathBuf::from("/private/cowshed/caches"),
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
            PathBuf::from("/private/cowshed/caches"),
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
                Path::new("/private/cowshed/store"),
                Path::new("/private/cowshed/caches"),
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
        let inventory = container(
            "disk3",
            [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
        );
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

    #[test]
    fn reclaimable_stub_inventory_enumerates_only_finder_metadata_and_empty_directories() {
        let directory =
            std::env::temp_dir().join(format!("cowshed-reclaimable-test-{}", Uuid::new_v4()));
        let empty = directory.join("empty");
        fs::create_dir_all(&empty).unwrap();
        fs::write(directory.join(".DS_Store"), b"metadata").unwrap();

        let paths = reclaimable_stub_paths(&directory)
            .expect("inspect safe stubs")
            .expect("safe stubs");
        assert_eq!(paths, vec![directory.join(".DS_Store"), empty]);

        fs::write(directory.join("user-data"), b"precious").unwrap();
        assert_eq!(
            reclaimable_stub_paths(&directory).expect("inspect unsafe data"),
            None
        );
        fs::remove_dir_all(&directory).unwrap();
    }

    /// The exact wedge the residue arms exist for: the sccache agent and the
    /// gateway heal plant socket/cache/mnt state on the bare Data volume before
    /// the store volume is remounted, and that residue must inventory as
    /// reclaimable — not read as a masked mountpoint that fail-closes every
    /// later validation.
    #[test]
    fn cowshed_runtime_residue_is_reclaimable_but_foreign_data_masks() {
        // Socket fixtures need the admitted short runtime spelling under cowshed;
        // outside a workspace, /tmp keeps the name below macOS's SUN_LEN.
        let root = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(format!("cowshed-residue-{}", Uuid::new_v4().simple()));
        fs::create_dir(&root).unwrap();

        // Daemon socket, workspace mountpoint scaffolding, compile cache.
        std::os::unix::net::UnixListener::bind(root.join("sccache.sock")).unwrap();
        fs::create_dir_all(root.join("mnt/acme/widget/slot@1")).unwrap();
        fs::create_dir_all(root.join("sccache/0")).unwrap();
        fs::write(root.join("sccache/0/entry.bin"), b"cache").unwrap();
        let paths = reclaimable_stub_paths(&root)
            .expect("inspect residue")
            .expect("residue is reclaimable");
        assert_eq!(
            paths,
            vec![
                root.join("mnt"),
                root.join("sccache"),
                root.join("sccache.sock"),
            ]
        );

        // A directory the whitelist does not name is scaffolding only while empty.
        fs::create_dir(root.join("telemetry")).unwrap();
        fs::write(root.join("telemetry/daemon-stderr.log"), b"").unwrap();
        assert_eq!(reclaimable_stub_paths(&root).expect("inspect data"), None);
        fs::remove_dir_all(root.join("telemetry")).unwrap();

        // A regular file inside mnt/ is data, not scaffolding: fail closed.
        fs::write(root.join("mnt/acme/widget/notes.txt"), b"mine").unwrap();
        assert_eq!(reclaimable_stub_paths(&root).expect("inspect data"), None);
        fs::remove_file(root.join("mnt/acme/widget/notes.txt")).unwrap();

        // Any foreign entry at the root keeps the masked verdict.
        fs::write(root.join("keep.txt"), b"user data").unwrap();
        assert_eq!(reclaimable_stub_paths(&root).expect("inspect data"), None);
        fs::remove_dir_all(&root).unwrap();
    }
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

    #[test]
    fn unencrypted_existing_volumes_mount_before_encrypting_in_place() {
        // FileVault comes only from the per-volume attestation; a false answer plans
        // encryption after the existing volumes are mounted.
        let mut source = healthy_existing_source();
        source.kernel = Ok(container(
            "disk3",
            [
                volume("Data", "disk3s5", Some("/System/Volumes/Data")),
                volume_with_filevault(APFS_STORE_VOLUME, "disk3s8", None, false),
                volume_with_filevault(APFS_CACHES_VOLUME, "disk3s9", None, false),
            ],
        ));
        for path in [
            Path::new("/private/cowshed/store"),
            Path::new("/private/cowshed/caches"),
        ] {
            source
                .mountpoints
                .insert(path.to_owned(), MountpointState::Missing);
            source.mounted_volumes.remove(path);
        }

        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "").unwrap();
        assert!(matches!(
            &snapshot.actions[0],
            HostAction::MountExisting { name, .. } if name == APFS_STORE_VOLUME
        ));
        assert!(matches!(
            &snapshot.actions[1],
            HostAction::MountExisting { name, .. } if name == APFS_CACHES_VOLUME
        ));
        assert!(matches!(
            &snapshot.actions[2],
            HostAction::EncryptVolume { name, .. } if name == APFS_STORE_VOLUME
        ));
        assert!(matches!(
            &snapshot.actions[3],
            HostAction::EncryptVolume { name, .. } if name == APFS_CACHES_VOLUME
        ));
    }

    #[test]
    fn filevault_volumes_with_usable_keychain_items_do_not_plan_encryption() {
        let mut source = healthy_existing_source();
        let snapshot = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "").unwrap();
        assert!(
            !snapshot
                .actions
                .iter()
                .any(|action| matches!(action, HostAction::EncryptVolume { .. }))
        );
    }

    #[test]
    fn filevault_volume_without_usable_keychain_item_fails_closed() {
        let mut source = healthy_existing_source();
        source.keychain_items.insert(APFS_STORE_VOLUME, false);
        let error = prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect_err("FileVault without its unlock credential must fail");
        assert!(matches!(
            error,
            NativeBootstrapError::MissingVolumeKeychain {
                name: APFS_STORE_VOLUME,
                uuid,
            } if uuid == volume_uuid("disk3s8")
        ));
    }

    #[test]
    fn filevault_is_attested_only_for_reserved_kernel_records() {
        let mut source = healthy_existing_source();
        prepare_setup_snapshot(&mut source, Path::new("/Users/alice"), "")
            .expect("healthy setup plan");
        assert_eq!(source.file_vault_queries, ["disk3s8", "disk3s9"]);

        let mut silent = healthy_existing_source();
        if let Ok(kernel) = &mut silent.kernel {
            kernel.file_vault.remove("disk3s9");
        }
        assert!(matches!(
            prepare_setup_snapshot(&mut silent, Path::new("/Users/alice"), ""),
            Err(NativeBootstrapError::VolumeFileVaultEvidence { identifier, .. })
                if identifier == "disk3s9"
        ));
    }

    #[test]
    fn foreign_filevault_volume_keeps_kernel_uuid_mount_and_keychain_guard() {
        let mut missing_keychain = source_with_foreign_store();
        missing_keychain
            .keychain_items
            .insert(APFS_STORE_VOLUME, false);
        assert!(matches!(
            gather_existing_apfs_evidence(&mut missing_keychain, Path::new("/Users/alice")),
            Err(NativeBootstrapError::MissingVolumeKeychain {
                name: APFS_STORE_VOLUME,
                uuid,
            }) if uuid == volume_uuid("disk7s2")
        ));
        assert_eq!(missing_keychain.file_vault_queries, ["disk7s2"]);

        let mut source = source_with_foreign_store();
        let gathered = gather_existing_apfs_evidence(&mut source, Path::new("/Users/alice"))
            .expect("foreign FileVault store with its keychain item");
        let store = &gathered.volumes[0];
        assert_eq!(store.file_vault, Some(true));
        assert_eq!(store.volume_uuid, Some(volume_uuid("disk7s2")));
        assert!(matches!(
            &store.storage,
            ExistingStorage::FoundElsewhere { container, device, mounted_at, .. }
                if container == "disk7"
                    && device == "disk7s2"
                    && mounted_at.as_deref() == Some(Path::new("/Volumes/cowshed.store"))
        ));
    }

    fn source_with_foreign_store() -> FakeEvidenceSource {
        source(kernel([
            container(
                "disk3",
                [volume("Data", "disk3s5", Some("/System/Volumes/Data"))],
            ),
            container(
                "disk7",
                [volume(
                    APFS_STORE_VOLUME,
                    "disk7s2",
                    Some("/Volumes/cowshed.store"),
                )],
            ),
        ]))
    }

    fn volume_info(entries: &[(&str, Value)]) -> Vec<u8> {
        let mut info = plist::Dictionary::new();
        for (key, value) in entries {
            info.insert((*key).to_owned(), value.clone());
        }
        let mut bytes = Vec::new();
        Value::Dictionary(info)
            .to_writer_xml(&mut bytes)
            .expect("encode volume info");
        bytes
    }

    #[test]
    fn filevault_attestation_requires_the_exact_kernel_record_and_an_explicit_answer() {
        let store = ApfsVolume {
            name: APFS_STORE_VOLUME.to_owned(),
            identifier: "disk3s8".to_owned(),
            mountpoint: Some(PathBuf::from("/private/cowshed/store")),
            volume_uuid: volume_uuid("disk3s8"),
        };
        let exact = |file_vault: Option<Value>| {
            let mut entries = vec![
                ("DeviceIdentifier", Value::from("disk3s8")),
                ("VolumeUUID", Value::from(volume_uuid("disk3s8"))),
                ("APFSContainerReference", Value::from("disk3")),
                ("VolumeName", Value::from(APFS_STORE_VOLUME)),
                // Per-volume encryption is reported independently and must not stand in.
                ("EncryptionThisVolumeProper", Value::Boolean(true)),
            ];
            entries.extend(file_vault.map(|value| ("FileVault", value)));
            entries
        };
        let attest = |entries: &[(&str, Value)]| {
            attest_volume_file_vault(&volume_info(entries), "disk3", &store)
        };
        assert!(matches!(
            attest(&exact(Some(Value::Boolean(true)))),
            Ok(true)
        ));
        assert!(matches!(
            attest(&exact(Some(Value::Boolean(false)))),
            Ok(false)
        ));

        let refused = |result: Result<bool, NativeBootstrapError>| {
            matches!(
                result,
                Err(NativeBootstrapError::VolumeFileVaultEvidence { identifier, .. })
                    if identifier == "disk3s8"
            )
        };
        assert!(refused(attest_volume_file_vault(
            b"not a plist",
            "disk3",
            &store
        )));
        assert!(refused(attest(&[])), "an empty root is unknown evidence");
        assert!(
            refused(attest(&exact(None))),
            "missing FileVault is not false"
        );
        assert!(refused(attest(&exact(Some(Value::from("true"))))));
        for (key, foreign) in [
            ("DeviceIdentifier", Value::from("disk3s9")),
            ("VolumeUUID", Value::from(volume_uuid("disk3s9"))),
            ("APFSContainerReference", Value::from("disk7")),
            ("VolumeName", Value::from(APFS_CACHES_VOLUME)),
        ] {
            let mut entries = exact(Some(Value::Boolean(true)));
            for entry in &mut entries {
                if entry.0 == key {
                    entry.1 = foreign.clone();
                }
            }
            assert!(refused(attest(&entries)), "foreign {key} must be refused");
        }
    }

    struct EncryptCommandHost {
        commands: mpsc::Sender<(HostCommand, Option<Vec<u8>>)>,
        existing_password: Option<&'static str>,
        persist_output: Option<HostCommandOutput>,
    }

    impl BootstrapHost for EncryptCommandHost {
        fn verify_zfs_delegation(
            &self,
            _pool: &str,
            _required_root: &str,
        ) -> Result<(), HostError> {
            unreachable!("FileVault encryption has no ZFS operation")
        }

        fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
            unreachable!("volume was mounted and attested before encryption")
        }

        fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
            unreachable!("volume was mounted before encryption")
        }

        fn reclaim_mountpoint(&self, _path: &Path) -> Result<(), HostError> {
            unreachable!("FileVault encryption reclaims nothing")
        }

        fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
            self.commands
                .send((command.clone(), None))
                .expect("encryption command receiver remains alive");
            if command
                .args()
                .first()
                .is_some_and(|argument| argument == "find-generic-password")
            {
                return Ok(match self.existing_password {
                    Some(password) => HostCommandOutput::success(format!("{password}\n")),
                    None => HostCommandOutput::failure(
                        44,
                        "The specified item could not be found in the keychain.",
                    ),
                });
            }
            if command
                .args()
                .first()
                .is_some_and(|argument| argument == "add-generic-password")
                && let Some(output) = &self.persist_output
            {
                return Ok(output.clone());
            }
            Ok(HostCommandOutput::default())
        }

        fn run_command_with_input(
            &self,
            command: &HostCommand,
            input: &[u8],
        ) -> Result<HostCommandOutput, HostError> {
            self.commands
                .send((command.clone(), Some(input.to_vec())))
                .expect("encryption command receiver remains alive");
            Ok(HostCommandOutput::default())
        }

        fn provision_apfs_volumes(
            &self,
            _container: &str,
            _volumes: &[ApfsVolumeProvision],
        ) -> Result<(), HostError> {
            unreachable!("encryption does not provision a volume")
        }

        fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
            unreachable!("encryption writes no marker")
        }

        fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
            unreachable!("encryption does not pin fstab")
        }
    }

    #[test]
    fn encryption_stores_one_random_password_and_feeds_it_to_diskutil() {
        let (commands, received) = mpsc::channel();
        let host = EncryptCommandHost {
            commands,
            existing_password: None,
            persist_output: None,
        };
        let uuid = "FEC35F46-22C8-40BC-943A-ADC4BD39CAE5";
        encrypt_volume_with(&host, APFS_STORE_VOLUME, uuid).unwrap();
        let commands = received.try_iter().collect::<Vec<_>>();
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].0.program(), SECURITY);
        assert_eq!(commands[0].0.args()[0], "find-generic-password");
        assert_eq!(commands[1].0.program(), SECURITY);
        assert_eq!(commands[1].0.args()[0], "add-generic-password");
        assert!(commands[1].0.args().windows(2).any(|pair| {
            pair == ["-a", APFS_STORE_VOLUME] || pair == ["-s", APFS_STORE_VOLUME]
        }));
        for trusted in [SECURITY, APFS_USER_AGENT, CS_USER_AGENT] {
            assert!(
                commands[1]
                    .0
                    .args()
                    .windows(2)
                    .any(|pair| { pair[0] == "-T" && pair[1] == trusted })
            );
        }
        assert_eq!(
            commands[1].0.args().last().map(String::as_str),
            Some(SYSTEM_KEYCHAIN)
        );
        let password_index = commands[1]
            .0
            .args()
            .iter()
            .position(|argument| argument == "-w")
            .expect("security password argument")
            + 1;
        let password = &commands[1].0.args()[password_index];
        assert_eq!(password.len(), 32);
        assert!(password.bytes().all(|byte| byte.is_ascii_hexdigit()));

        assert_eq!(commands[2].0.program(), DISKUTIL);
        assert_eq!(
            commands[2].0.args(),
            [
                "apfs",
                "encryptVolume",
                uuid,
                "-user",
                "disk",
                "-stdinpassphrase",
            ]
        );
        let mut expected_input = password.as_bytes().to_vec();
        expected_input.push(b'\n');
        assert_eq!(commands[2].1.as_deref(), Some(expected_input.as_slice()));
    }

    #[test]
    fn encryption_reuses_an_existing_keychain_password() {
        let (commands, received) = mpsc::channel();
        let host = EncryptCommandHost {
            commands,
            existing_password: Some("already-stored-pass"),
            persist_output: None,
        };
        let uuid = "FEC35F46-22C8-40BC-943A-ADC4BD39CAE5";
        encrypt_volume_with(&host, APFS_STORE_VOLUME, uuid).unwrap();
        let commands = received.try_iter().collect::<Vec<_>>();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].0.args()[0], "find-generic-password");
        assert!(
            !commands.iter().any(|(command, _)| command
                .args()
                .first()
                .is_some_and(|argument| argument == "add-generic-password")),
            "a stored passphrase must not be replaced"
        );
        assert_eq!(commands[1].0.args()[0], "apfs");
        assert_eq!(
            commands[1].1.as_deref(),
            Some(b"already-stored-pass\n".as_slice())
        );
    }

    #[test]
    fn encryption_treats_empty_privileged_lookup_as_missing() {
        let (commands, received) = mpsc::channel();
        let host = EncryptCommandHost {
            commands,
            existing_password: Some(""),
            persist_output: None,
        };
        encrypt_volume_with(
            &host,
            APFS_STORE_VOLUME,
            "FEC35F46-22C8-40BC-943A-ADC4BD39CAE5",
        )
        .unwrap();
        let commands = received.try_iter().collect::<Vec<_>>();
        assert_eq!(commands[1].0.args()[0], "add-generic-password");
        assert_eq!(commands[2].0.args()[0], "apfs");
    }

    #[test]
    fn privileged_runner_reports_child_exit_instead_of_fabricating_success() {
        let output =
            authorized_command_output(b"secret\n--cowshed-aewp-exit:44--\n".to_vec()).unwrap();
        assert!(!output.succeeded());
        assert_eq!(output.status, crate::process::ProcessStatus::Exit(44));
        assert_eq!(output.stdout, b"secret");
    }

    #[test]
    fn privileged_runner_strips_zero_exit_trailer_from_stdout() {
        let output =
            authorized_command_output(b"plist-bytes\n--cowshed-aewp-exit:0--\n".to_vec()).unwrap();
        assert!(output.succeeded());
        assert_eq!(output.stdout, b"plist-bytes");
    }

    #[test]
    fn privileged_runner_without_exit_trailer_is_an_error() {
        assert!(authorized_command_output(b"launched but no status".to_vec()).is_err());
    }

    #[test]
    fn encryption_stops_when_keychain_persist_fails() {
        let (commands, received) = mpsc::channel();
        let host = EncryptCommandHost {
            commands,
            existing_password: None,
            persist_output: Some(HostCommandOutput::failure(
                1,
                "could not add the generic password",
            )),
        };
        encrypt_volume_with(
            &host,
            APFS_STORE_VOLUME,
            "FEC35F46-22C8-40BC-943A-ADC4BD39CAE5",
        )
        .expect_err("failed add-generic-password must not reach encryptVolume");
        let commands = received.try_iter().collect::<Vec<_>>();
        assert!(commands.iter().any(|(command, _)| {
            command
                .args()
                .first()
                .is_some_and(|argument| argument == "add-generic-password")
        }));
        assert!(
            !commands
                .iter()
                .any(|(command, _)| command.program() == DISKUTIL),
            "encryptVolume must not run without a persisted keychain item"
        );
    }

    fn deadline_spawn(
        args: &[&str],
        deadline: Duration,
    ) -> Result<std::process::Output, HostError> {
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        spawn_with_deadline(Path::new("/bin/sleep"), &args, deadline)
    }

    #[test]
    fn deadline_spawn_completes_when_the_child_answers() {
        let output = deadline_spawn(&["0.05"], Duration::from_secs(10)).expect("fast child");
        assert!(output.status.success());
    }

    #[test]
    fn deadline_spawn_reports_unresponsiveness_and_kills_the_child() {
        let started = std::time::Instant::now();
        let error = deadline_spawn(&["30"], Duration::from_millis(250)).expect_err("deadline");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(error.to_string().contains("unresponsive"));
    }
}

fn require_host_canonical(path: &Path) -> Result<(), HostError> {
    if crate::repository::is_lexically_canonical(path) {
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

mod unix {
    use super::*;
    use std::ffi::{CStr, CString};
    use std::fs::File;
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::{AsRawFd, FromRawFd};
    use std::process::{Command, Output, Stdio};
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    const MARKER_MODE: libc::mode_t = 0o600;

    pub(super) fn spawn_with_deadline(
        program: &Path,
        args: &[String],
        deadline: Duration,
    ) -> Result<Output, HostError> {
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .map_err(|source| host_io_error("execute", program, source))?;
        let started = Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(_)) => {
                    return child
                        .wait_with_output()
                        .map_err(|source| host_io_error("collect output from", program, source));
                }
                Ok(None) => {
                    if started.elapsed() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(HostError::new(format!(
                            "{program:?} produced no result within {deadline:?}; the disk arbitration daemon is unresponsive"
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(source) => return Err(host_io_error("wait for", program, source)),
            }
        }
    }

    pub(super) fn write_marker_atomic(path: &Path, contents: &[u8]) -> Result<(), HostError> {
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
        let flags =
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
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
}

use unix::{spawn_with_deadline, write_marker_atomic};
