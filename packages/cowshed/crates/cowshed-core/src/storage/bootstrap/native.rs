#[cfg(target_os = "macos")]
use std::ffi::OsString;
#[cfg(unix)]
use std::ffi::{CStr, CString};
use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::io;
#[cfg(target_os = "macos")]
use std::io::Read;
#[cfg(unix)]
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use plist::{Dictionary, Value};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};
use thiserror::Error;
use tokio::sync::oneshot;
#[cfg(unix)]
use uuid::Uuid;

#[cfg(target_os = "macos")]
use super::VOLUME_MARKER_FILE;
use super::{
    APFS_CACHES_VOLUME, APFS_STORE_VOLUME, BlockingLane, BootstrapEvidence,
    BootstrapExecutionError, BootstrapHost, BootstrapPlan, DISKUTIL, ExistingStorage, HostCommand,
    HostCommandOutput, HostError, HostOperation, MountpointState, PlanError, SelectionError,
    StatFsEvidence, SubstrateKind, TokioBlockingLane, VolumeRole, execute_bootstrap,
    plan_bootstrap, require_mounted_marker, select_substrate,
};

#[cfg(unix)]
const MARKER_MODE: libc::mode_t = 0o600;

/// Whether native bootstrap may apply its mutating host plan.
///
/// `ExistingOnly` is the safe capability for ordinary commands and background services. It may
/// gather read-only evidence and validate mounted volume markers, but it cannot create directories
/// or volumes, mount volumes, run mutating commands, or write markers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBootstrapMode {
    Provision,
    ExistingOnly,
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

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        ensure_supported_host()?;
        run_command_with(command, |program, args| {
            Command::new(program).args(args).output()
        })
    }

    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        write_marker_atomic(path, contents)
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
                hint: "next: cowshed adopt",
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
            HostOperation::VerifyZfsDelegation { .. } | HostOperation::GuardMountpoint { .. } => {
                None
            }
            HostOperation::EnsureDirectory(path) => {
                Some(format!("create directory {}", path.display()))
            }
            HostOperation::CreateApfsVolume { name, .. } => {
                Some(format!("create APFS volume {name}"))
            }
            HostOperation::MountApfsVolume { mountpoint, .. } => {
                Some(format!("mount APFS volume at {}", mountpoint.display()))
            }
            HostOperation::RunCommand(command) => Some(format!(
                "run {} {}",
                command.program(),
                command.args().join(" ")
            )),
            HostOperation::WriteMarkerAtomic { path, .. } => {
                Some(format!("write volume marker {}", path.display()))
            }
        })
        .collect()
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
    #[error("command {command:?} failed: {stderr}")]
    CommandFailed {
        command: HostCommand,
        stderr: String,
    },
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
    #[error("APFS volume {identifier:?} is not mounted, so its marker cannot be authenticated")]
    UnauthenticatedUnmountedVolume { identifier: String },
    #[error("mountpoint {path:?} contains data but is not the exact expected APFS mount")]
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
}

trait EvidenceSource {
    fn statfs(&mut self, path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError>;
    fn run_command(
        &mut self,
        command: &HostCommand,
    ) -> Result<HostCommandOutput, NativeBootstrapError>;
    fn inspect_mountpoint(&mut self, path: &Path) -> Result<MountpointState, NativeBootstrapError>;
    fn mounted_identifier(&mut self, path: &Path) -> Result<String, NativeBootstrapError>;
}

struct SystemEvidenceSource<'a> {
    host: &'a SystemBootstrapHost,
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

    fn mounted_identifier(&mut self, path: &Path) -> Result<String, NativeBootstrapError> {
        let snapshot = system_statfs(path)?;
        if snapshot.mountpoint != path {
            return Err(NativeBootstrapError::MountEvidenceMismatch {
                path: path.to_owned(),
                identifier: snapshot.mount_source.display().to_string(),
            });
        }
        exact_device_identifier(&snapshot.mount_source)
    }
}

struct GatheredEvidence {
    statfs: StatFsEvidence,
    bootstrap: BootstrapEvidence,
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

fn gather_apfs_evidence(
    source: &mut impl EvidenceSource,
    project_root: &Path,
    home: &Path,
) -> Result<GatheredEvidence, NativeBootstrapError> {
    require_canonical(project_root)?;
    require_canonical(home)?;
    let snapshot = source.statfs(project_root)?;
    if snapshot.fs_type != "apfs" {
        return Err(NativeBootstrapError::UnsupportedFilesystem {
            path: project_root.to_owned(),
            fs_type: snapshot.fs_type,
        });
    }
    let mount_device = exact_device_identifier(&snapshot.mount_source)?;
    let command = HostCommand::new(DISKUTIL, ["apfs", "list", "-plist"]);
    let output = source.run_command(&command)?;
    if !output.success {
        return Err(NativeBootstrapError::CommandFailed {
            command,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    let inventory = parse_apfs_inventory(&output.stdout)?;
    let container = inventory.containing_container(&mount_device)?;
    let roots = super::CanonicalRoots::for_home(home)?;
    let store = classify_volume(
        source,
        container,
        APFS_STORE_VOLUME,
        roots.store(),
        VolumeRole::Store,
    )?;
    let caches = classify_volume(
        source,
        container,
        APFS_CACHES_VOLUME,
        roots.caches(),
        VolumeRole::Caches,
    )?;
    Ok(GatheredEvidence {
        statfs: StatFsEvidence::Apfs {
            mount_source: snapshot.mount_source,
            container: Some(container.reference.clone()),
        },
        bootstrap: BootstrapEvidence::Apfs { store, caches },
    })
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
            MountpointState::Missing | MountpointState::EmptyDirectory => {
                Ok(ExistingStorage::Absent)
            }
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
            let mounted_identifier = source.mounted_identifier(expected_mountpoint)?;
            if mounted_identifier != volume.identifier {
                return Err(NativeBootstrapError::MountEvidenceMismatch {
                    path: expected_mountpoint.to_owned(),
                    identifier: mounted_identifier,
                });
            }
            require_mounted_marker(marker.as_deref(), role, SubstrateKind::Apfs).map_err(
                |error| NativeBootstrapError::InvalidMountedMarker {
                    path: expected_mountpoint.to_owned(),
                    message: error.to_string(),
                },
            )?;
            Ok(ExistingStorage::mounted_valid(&volume.identifier))
        }
        MountpointState::Missing | MountpointState::EmptyDirectory => match &volume.mountpoint {
            None => Err(NativeBootstrapError::UnauthenticatedUnmountedVolume {
                identifier: volume.identifier.clone(),
            }),
            Some(mountpoint) => Err(NativeBootstrapError::InvalidVolumeMountpoint {
                identifier: volume.identifier.clone(),
                mountpoint: Some(mountpoint.clone()),
            }),
        },
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
    Ok(HostCommandOutput {
        success: output.status.success(),
        stdout: output.stdout,
        stderr: output.stderr,
    })
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

    use super::*;
    use crate::storage::bootstrap::VolumeMarker;

    struct FakeEvidenceSource {
        statfs: StatFsSnapshot,
        command_output: HostCommandOutput,
        mountpoints: BTreeMap<PathBuf, MountpointState>,
        mounted_identifiers: BTreeMap<PathBuf, String>,
        commands: Vec<HostCommand>,
    }

    impl EvidenceSource for FakeEvidenceSource {
        fn statfs(&mut self, _path: &Path) -> Result<StatFsSnapshot, NativeBootstrapError> {
            Ok(self.statfs.clone())
        }

        fn run_command(
            &mut self,
            command: &HostCommand,
        ) -> Result<HostCommandOutput, NativeBootstrapError> {
            self.commands.push(command.clone());
            Ok(self.command_output.clone())
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

        fn mounted_identifier(&mut self, path: &Path) -> Result<String, NativeBootstrapError> {
            self.mounted_identifiers.get(path).cloned().ok_or_else(|| {
                NativeBootstrapError::MountEvidenceMismatch {
                    path: path.to_owned(),
                    identifier: "missing mounted identifier test evidence".to_owned(),
                }
            })
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
            "<dict><key>Name</key><string>{name}</string><key>DeviceIdentifier</key><string>{identifier}</string>{mountpoint}</dict>"
        )
    }

    fn source(inventory: Vec<u8>) -> FakeEvidenceSource {
        FakeEvidenceSource {
            statfs: StatFsSnapshot {
                fs_type: "apfs".to_owned(),
                mount_source: PathBuf::from("/dev/disk3s5"),
                mountpoint: PathBuf::from("/System/Volumes/Data"),
            },
            command_output: HostCommandOutput {
                success: true,
                stdout: inventory,
                stderr: Vec::new(),
            },
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
            mounted_identifiers: BTreeMap::from([
                (PathBuf::from("/Users/alice/.cowshed"), "disk3s8".to_owned()),
                (
                    PathBuf::from("/Users/alice/.cowshed/caches"),
                    "disk3s9".to_owned(),
                ),
            ]),
            commands: Vec::new(),
        }
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
        assert_eq!(source.commands.len(), 1);
        assert_eq!(source.commands[0].program(), "/usr/sbin/diskutil");
        assert_eq!(source.commands[0].args(), ["apfs", "list", "-plist"]);
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
                marker: Some(marker),
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
    fn existing_only_planning_refuses_mismounted_volume_before_mutation_dispatch() {
        let volumes = volume(APFS_STORE_VOLUME, "disk3s8", Some("/Volumes/cowshed-wrong"))
            + &volume("Data", "disk3s5", Some("/System/Volumes/Data"));
        let mut source = source(plist(&container("disk3", &volumes)));

        assert!(matches!(
            plan_native_bootstrap(
                &mut source,
                Path::new("/Users/alice/project"),
                Path::new("/Users/alice")
            ),
            Err(NativeBootstrapError::InvalidVolumeMountpoint {
                identifier,
                mountpoint: Some(path),
            }) if identifier == "disk3s8" && path == Path::new("/Volumes/cowshed-wrong")
        ));
        assert_eq!(source.commands.len(), 1);
        assert_eq!(source.commands[0].args(), ["apfs", "list", "-plist"]);
    }

    #[cfg(unix)]
    #[test]
    fn command_runner_uses_argv_and_preserves_stderr() {
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
        assert!(!output.success);
        assert_eq!(output.stderr, b"diskutil exact failure\n");
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
