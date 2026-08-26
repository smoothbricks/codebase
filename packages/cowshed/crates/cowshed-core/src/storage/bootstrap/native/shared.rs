use std::io;
use std::path::{Component, Path, PathBuf};
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
    pub requires_authorization: bool,
    pub non_destructive: bool,
}

impl HostSetupPlan {
    pub(crate) fn new(actions: Vec<HostAction>, requires_authorization: bool) -> Self {
        let non_destructive = actions.iter().all(HostAction::is_non_destructive);
        Self {
            actions,
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
    PinFstab {
        uuid: String,
        mount_at: PathBuf,
    },
    ReclaimStubs {
        paths: Vec<PathBuf>,
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

pub(crate) fn read_only_validation_actions(plan: &BootstrapPlan) -> Vec<String> {
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
            HostOperation::ProvisionApfsVolumes { volumes, .. } => Some(format!(
                "create APFS volumes {}",
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

pub(crate) fn mutating_setup_actions(plan: &BootstrapPlan) -> Vec<String> {
    plan.operations()
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::VerifyZfsDelegation { .. }
            | HostOperation::GuardMountpoint { .. }
            | HostOperation::EnsureDirectory(_)
            | HostOperation::ReclaimMountpoint(_)
            | HostOperation::MountApfsVolume { .. }
            | HostOperation::ReportVolumeIssue { .. } => None,
            HostOperation::RunCommand(command) => {
                remount_setup_action(command).map(|action| action.to_owned())
            }
            HostOperation::ProvisionApfsVolumes { volumes, .. } => Some(format!(
                "create APFS volumes {}",
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

pub(crate) fn require_host_canonical(path: &Path) -> Result<(), HostError> {
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

pub(crate) fn host_io_error(operation: &str, path: &Path, source: io::Error) -> HostError {
    HostError::new(format!("cannot {operation} {path:?}: {source}"))
}

pub(crate) fn platform_host_error(operation: &str) -> HostError {
    HostError::new(format!(
        "{operation} is unsupported on {}",
        std::env::consts::OS
    ))
}

#[cfg(unix)]
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

    pub(crate) fn spawn_with_deadline(
        program: &Path,
        args: &[String],
        deadline: Duration,
    ) -> Result<Output, HostError> {
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
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

    pub(crate) fn write_marker_atomic(path: &Path, contents: &[u8]) -> Result<(), HostError> {
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

#[cfg(unix)]
pub(crate) use unix::{spawn_with_deadline, write_marker_atomic};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Duration;

    #[cfg(unix)]
    fn deadline_spawn(
        args: &[&str],
        deadline: Duration,
    ) -> Result<std::process::Output, HostError> {
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        spawn_with_deadline(Path::new("/bin/sleep"), &args, deadline)
    }

    #[cfg(unix)]
    #[test]
    fn deadline_spawn_completes_when_the_child_answers() {
        let output = deadline_spawn(&["0.05"], Duration::from_secs(10)).expect("fast child");
        assert!(output.status.success());
    }

    #[cfg(unix)]
    #[test]
    fn deadline_spawn_reports_unresponsiveness_and_kills_the_child() {
        let started = std::time::Instant::now();
        let error = deadline_spawn(&["30"], Duration::from_millis(250)).expect_err("deadline");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(error.to_string().contains("unresponsive"));
    }
}
