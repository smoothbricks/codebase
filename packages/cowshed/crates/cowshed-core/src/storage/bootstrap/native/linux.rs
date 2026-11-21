//! Linux host adapter: a directory store on whatever filesystem holds the store path, or the
//! datasets of an existing ZFS root (09_substrates.md, "Linux host paths").
//!
//! Evidence is the kernel's and the filesystem's own: `/proc/self/mountinfo`, a no-follow `lstat`
//! of each fixed host path with its marker, and `zfs get` of exact dataset names. Nothing lists a
//! pool. Every decision is a pure function of that evidence ([`gather`], [`prepare_setup`],
//! [`owned_directory_step`]); the shell around them reads the host and runs the planned
//! operations. Two things need root, and each has one door: a directory under `/private` is made
//! by `sudo` ([`SudoSession`]), and a dataset is changed by `cowshed-helper`
//! ([`helper_invocation`]). Everything else runs as the invoking user.

use std::collections::BTreeMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::io::{self, Read};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use super::super::{
    ApfsVolumeProvision, BUILD_CACHE_MOUNTPOINT, BootstrapEvidence, BootstrapExecutionError,
    BootstrapHost, BootstrapPlan, CanonicalRoots, DelegatedZfsDataset, DirectoryOwner,
    ExistingDirectory, ExistingMarkerEvidence, ExistingStorage, HostCommand, HostCommandOutput,
    HostError, HostOperation, MarkerError, MountpointState, RETIRED_CACHES_MOUNTPOINT,
    SelectedSubstrate, StatFsEvidence, SubstrateConfig, SubstrateKind, TokioBlockingLane,
    VOLUME_MARKER_FILE, ValidatedHostStorage, VolumeMarker, VolumeRole, ZFS, ZFS_BUILD_CACHE_CHILD,
    ZFS_HELPER, ZFS_OWNER_PROPERTY, ZFS_PROJECTS_CHILD, ZFS_RETIRED_CACHES_CHILD,
    ZFS_ROLE_PROPERTY, ZFS_STORE_CHILD, ZFS_VERSION_PROPERTY, execute_bootstrap_operation,
    parse_cowshed_config, plan_bootstrap, plan_directory, plan_zfs_build_cache,
    plan_zfs_retire_caches, require_mounted_marker, select_substrate, zfs_child, zfs_root,
};
use super::shared::{
    CachesVolume, FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan,
    HostSetupReport, HostUninstallPlan, NativeBootstrapError, NativeBootstrapMode,
    SystemBootstrapHost, UninstallFstabOutcome, UninstallReport, VolumeOutcome, VolumeState,
    execute_native_bootstrap_plan, existing_host_storage_error, platform_host_error,
    provision_volumes_action, setup_execution_error, write_marker_action,
};
use crate::error::CowshedError;
use crate::storage::fstab::FstabPin;
use crate::storage::host_config::{HOST_CONFIG_FILE, HostConfig, canonical_mount_root};
use cowshed_os::deadline::{self, Bound, LatencyClass};
use cowshed_os::fork_lock::Run as _;
use cowshed_os::fsio::AnchoredDirectory;

const MOUNTINFO: &str = "/proc/self/mountinfo";
const SUDO: &str = "sudo";
const COWSHED_CONFIG_FILE: &str = ".cowshed.toml";
const ZFS_FILESYSTEM: &str = "zfs";
/// The per-user mount root and its parent are the user's alone.
const MOUNT_ROOT_MODE: u32 = 0o700;
/// A marker is a few dozen bytes; anything past this is not one.
const MARKER_LIMIT: u64 = 64 * 1024;
/// What `zfs` says of a name no dataset has, in the `C` locale every read runs in.
const ZFS_NO_DATASET: &str = "dataset does not exist";
const SETUP: &str = "cowshed setup";
/// One `zfs get` of one dataset, which the pool answers from its in-core state: it runs on every
/// command's storage validation, so it is bounded like any other host tool call.
const ZFS_GET: Bound = Bound::new(LatencyClass::Ffi, std::time::Duration::from_secs(10));

// ---------------------------------------------------------------------------------------------
// Kernel mount table
// ---------------------------------------------------------------------------------------------

/// One line of `/proc/self/mountinfo`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Mount {
    id: u64,
    /// The path within the filesystem that is mounted: `/` for a whole filesystem.
    root: String,
    mountpoint: PathBuf,
    fs_type: String,
    source: String,
}

impl Mount {
    /// Whether this is the dataset `source` mounted whole, never a bind of a directory in it.
    fn is_dataset(&self) -> bool {
        self.fs_type == ZFS_FILESYSTEM && self.root == "/"
    }
}

/// `mountinfo` writes space, tab, newline and backslash as `\ooo`.
fn unescape(field: &str) -> Vec<u8> {
    let bytes = field.as_bytes();
    let mut plain = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let octal = bytes
            .get(index + 1..index + 4)
            .filter(|_| bytes[index] == b'\\')
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        match octal {
            Some(byte) => {
                plain.push(byte);
                index += 4;
            }
            None => {
                plain.push(bytes[index]);
                index += 1;
            }
        }
    }
    plain
}

/// Every mount of `table`, in the kernel's order: a later line is mounted over an earlier one at
/// the same point. A line that does not parse fails the whole read: a table with a hole in it
/// cannot establish that nothing is mounted somewhere.
fn parse_mountinfo(table: &str) -> Result<Vec<Mount>, String> {
    table
        .lines()
        .map(|line| {
            let malformed = || format!("malformed mountinfo line {line:?}");
            let (mount, filesystem) = line.split_once(" - ").ok_or_else(malformed)?;
            let mut mount = mount.split(' ');
            let id = mount
                .next()
                .and_then(|id| id.parse::<u64>().ok())
                .ok_or_else(malformed)?;
            let (root, mountpoint) = (
                mount.nth(2).ok_or_else(malformed)?,
                mount.next().ok_or_else(malformed)?,
            );
            let mut filesystem = filesystem.split(' ');
            let (fs_type, source) = (
                filesystem.next().ok_or_else(malformed)?,
                filesystem.next().ok_or_else(malformed)?,
            );
            Ok(Mount {
                id,
                root: String::from_utf8_lossy(&unescape(root)).into_owned(),
                mountpoint: PathBuf::from(OsString::from_vec(unescape(mountpoint))),
                fs_type: fs_type.to_owned(),
                source: String::from_utf8_lossy(&unescape(source)).into_owned(),
            })
        })
        .collect()
}

/// The mount `path` lies in: the deepest mountpoint that contains it, the latest of equals.
/// Lexical, so it answers for a path that does not exist yet.
fn mount_of<'a>(mounts: &'a [Mount], path: &Path) -> Option<&'a Mount> {
    mounts
        .iter()
        .filter(|mount| path.starts_with(&mount.mountpoint))
        .max_by_key(|mount| mount.mountpoint.components().count())
}

/// The mount whose mountpoint is exactly `path`, the latest of equals.
fn mount_at<'a>(mounts: &'a [Mount], path: &Path) -> Option<&'a Mount> {
    mounts.iter().rev().find(|mount| mount.mountpoint == path)
}

// ---------------------------------------------------------------------------------------------
// Host paths
// ---------------------------------------------------------------------------------------------

/// What one host path holds, read without following a link.
#[derive(Clone, Debug, Eq, PartialEq)]
enum PathEntry {
    Missing,
    Directory(DirectoryEntry),
    /// Anything else, in words.
    Other(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectoryEntry {
    uid: u32,
    /// Permission bits, with setuid, setgid and sticky.
    mode: u32,
    /// `false` also for a directory this user cannot list: what cannot be seen is not empty.
    empty: bool,
    /// The bytes of its volume marker, when it holds a regular one.
    marker: Option<Vec<u8>>,
}

/// The owner and mode one directory is provisioned with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Owned {
    uid: u32,
    gid: u32,
    mode: u32,
}

/// What brings one directory to its owner and mode.
#[derive(Clone, Debug, Eq, PartialEq)]
enum DirectoryStep {
    Current,
    /// Make it. `privileged` unless the invoking user makes it for themselves in a directory of
    /// their own.
    Create {
        privileged: bool,
    },
    /// It is the right owner's with another mode.
    Repair {
        privileged: bool,
    },
    Refuse(String),
}

/// Decide what one planned directory needs, from what is at its path and at its parent's.
///
/// A root ancestor (`want.uid` 0) is taken as found when it is root's and nobody else may write
/// it: its exact mode is the administrator's. Anything else there is refused, since whoever can
/// write an ancestor can replace the store below it. The owner's own directory is never taken
/// over from another user, and a root-owned one is not adopted either: an empty root-owned
/// directory at a host path is also what an unmounted dataset's mountpoint looks like.
fn owned_directory_step(
    entry: &PathEntry,
    parent: &PathEntry,
    want: Owned,
    invoker: u32,
) -> DirectoryStep {
    let directory = match entry {
        PathEntry::Missing => {
            let own_parent =
                matches!(parent, PathEntry::Directory(parent) if parent.uid == invoker);
            return DirectoryStep::Create {
                privileged: !(want.uid == invoker && own_parent),
            };
        }
        PathEntry::Other(kind) => return DirectoryStep::Refuse(format!("it is {kind}")),
        PathEntry::Directory(directory) => directory,
    };
    if want.uid == 0 {
        return if directory.uid == 0 && directory.mode & 0o022 == 0 {
            DirectoryStep::Current
        } else {
            DirectoryStep::Refuse(format!(
                "it belongs to uid {} with mode {:04o}; every directory above cowshed's storage is root's and writable by root alone",
                directory.uid, directory.mode
            ))
        };
    }
    if directory.uid == want.uid {
        return if directory.mode == want.mode {
            DirectoryStep::Current
        } else {
            DirectoryStep::Repair {
                privileged: want.uid != invoker,
            }
        };
    }
    if directory.uid == 0 && directory.empty {
        return DirectoryStep::Refuse(
            "it is root's and empty: the mountpoint of a dataset that is not mounted, or what an interrupted setup left; mount the dataset, or remove the directory, then rerun cowshed setup".to_owned(),
        );
    }
    DirectoryStep::Refuse(format!(
        "it belongs to uid {}, not to uid {}",
        directory.uid, want.uid
    ))
}

/// What a run of planned directories needs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DirectoryChanges {
    /// Some directory is created or repaired.
    any: bool,
    /// Some step needs root.
    privileged: bool,
}

/// Walk the owned directories `operations` plan, in order, against what `observe` finds now. A
/// directory an earlier operation will have made counts as its parent's state, so a chain under
/// a missing ancestor is judged as it will be when each step runs.
fn directory_changes(
    operations: &[HostOperation],
    invoker: u32,
    mut observe: impl FnMut(&Path) -> Result<PathEntry, NativeBootstrapError>,
) -> Result<DirectoryChanges, NativeBootstrapError> {
    let mut planned: Vec<(&Path, u32)> = Vec::new();
    let mut changes = DirectoryChanges::default();
    for operation in operations {
        let HostOperation::EnsureOwnedDirectory {
            path,
            uid,
            gid,
            mode,
        } = operation
        else {
            continue;
        };
        let entry = observe(path)?;
        let parent = match path.parent() {
            None => PathEntry::Missing,
            Some(parent) => match planned.iter().find(|(made, _)| *made == parent) {
                Some((_, owner)) => PathEntry::Directory(DirectoryEntry {
                    uid: *owner,
                    mode: *mode,
                    empty: true,
                    marker: None,
                }),
                None => observe(parent)?,
            },
        };
        let want = Owned {
            uid: *uid,
            gid: *gid,
            mode: *mode,
        };
        match owned_directory_step(&entry, &parent, want, invoker) {
            DirectoryStep::Current => {}
            DirectoryStep::Create { privileged } => {
                changes.any = true;
                changes.privileged |= privileged;
                planned.push((path.as_path(), *uid));
            }
            DirectoryStep::Repair { privileged } => {
                changes.any = true;
                changes.privileged |= privileged;
            }
            DirectoryStep::Refuse(reason) => {
                return Err(NativeBootstrapError::ForeignHostPath {
                    path: path.clone(),
                    reason,
                });
            }
        }
    }
    Ok(changes)
}

/// The state of one fixed path of a directory store, or why it cannot be one.
fn classify_directory(
    path: &Path,
    role: VolumeRole,
    entry: &PathEntry,
    owner: DirectoryOwner,
) -> Result<ExistingDirectory, NativeBootstrapError> {
    let foreign = |reason: String| NativeBootstrapError::ForeignHostPath {
        path: path.to_owned(),
        reason,
    };
    let directory = match entry {
        PathEntry::Missing => return Ok(ExistingDirectory::Unpublished),
        PathEntry::Other(kind) => return Err(foreign(format!("it is {kind}"))),
        PathEntry::Directory(directory) => directory,
    };
    match &directory.marker {
        Some(marker) if directory.uid == owner.uid => {
            require_mounted_marker(Some(marker), role, SubstrateKind::Directory)
                .map(|_| ExistingDirectory::Published)
                .map_err(|error| foreign(format!("its marker is not this store's: {error}")))
        }
        Some(_) => Err(foreign(format!(
            "it carries a cowshed marker but belongs to uid {}, not to uid {}",
            directory.uid, owner.uid
        ))),
        // Whose it is, and whether it may be created over, is the provisioning's own decision
        // ([`owned_directory_step`]).
        None if directory.empty => Ok(ExistingDirectory::Unpublished),
        None => Err(foreign(
            "it holds entries but no cowshed marker; a store published over it would claim them"
                .to_owned(),
        )),
    }
}

/// A directory store moves a workspace between the store and its mountpoint by rename, and a
/// rename never crosses a mount, not even between two mounts of one filesystem.
fn require_one_mount(
    mounts: &[Mount],
    store: &Path,
    mount_root: &Path,
) -> Result<(), NativeBootstrapError> {
    match (mount_of(mounts, store), mount_of(mounts, mount_root)) {
        (Some(of_store), Some(of_root)) if of_store.id != of_root.id => {
            Err(NativeBootstrapError::SplitDirectoryStore {
                store: store.to_owned(),
                mount_root: mount_root.to_owned(),
                store_mount: of_store.mountpoint.clone(),
                mount_root_mount: of_root.mountpoint.clone(),
            })
        }
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------------------------
// ZFS datasets
// ---------------------------------------------------------------------------------------------

/// One dataset as `zfs get` reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Dataset {
    guid: String,
    used: u64,
    /// `cowshed:owner`, when it is set on the dataset itself: an inherited value authorizes
    /// nothing (09_substrates.md, "Privileged helper").
    owner: Option<String>,
    /// What its `cowshed:role` and `cowshed:version` properties say.
    marker: ExistingMarkerEvidence,
}

fn dataset_properties() -> String {
    format!("guid,used,{ZFS_OWNER_PROPERTY},{ZFS_ROLE_PROPERTY},{ZFS_VERSION_PROPERTY}")
}

/// Read a dataset from the stdout of `zfs get -H -p -o property,value,source <properties>`.
fn parse_dataset(stdout: &str) -> Result<Dataset, String> {
    let mut properties: BTreeMap<&str, (&str, &str)> = BTreeMap::new();
    for line in stdout.lines() {
        let mut fields = line.split('\t');
        let (Some(property), Some(value), Some(source)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return Err(format!("malformed property line {line:?}"));
        };
        properties.insert(property, (value, source));
    }
    let value = |property: &str| {
        properties
            .get(property)
            .map(|(value, _)| *value)
            .ok_or_else(|| format!("zfs reported no {property}"))
    };
    let unset = |property: &str| value(property).map(|value| (value != "-").then_some(value));
    let used = value("used")?;
    let marker = match (unset(ZFS_ROLE_PROPERTY)?, unset(ZFS_VERSION_PROPERTY)?) {
        (None, None) => ExistingMarkerEvidence::Missing,
        (Some(role), Some(version)) => {
            let json = serde_json::json!({
                "version": version.parse::<u32>().ok(),
                "role": role,
                "substrate": "zfs",
            });
            match VolumeMarker::from_json(json.to_string().as_bytes()) {
                Ok(marker) => ExistingMarkerEvidence::Valid(marker),
                Err(MarkerError::UnsupportedVersion(version)) => {
                    ExistingMarkerEvidence::UnsupportedVersion(version)
                }
                Err(MarkerError::Encode(_) | MarkerError::Decode(_)) => {
                    ExistingMarkerEvidence::Invalid
                }
            }
        }
        _ => ExistingMarkerEvidence::Invalid,
    };
    Ok(Dataset {
        guid: value("guid")?.to_owned(),
        used: used
            .parse()
            .map_err(|_| format!("zfs reported used={used:?}"))?,
        owner: properties
            .get(ZFS_OWNER_PROPERTY)
            .filter(|(value, source)| *value != "-" && *source == "local")
            .map(|(value, _)| (*value).to_owned()),
        marker,
    })
}

/// The state of one mounted dataset of the fixed hierarchy. The kernel's mount table says where
/// it is mounted; `entry` is what its mountpoint holds.
fn classify_dataset(
    name: &str,
    mountpoint: &Path,
    role: VolumeRole,
    dataset: Option<&Dataset>,
    mounts: &[Mount],
    entry: &PathEntry,
) -> Result<ExistingStorage, NativeBootstrapError> {
    let foreign = |path: &Path, reason: String| NativeBootstrapError::ForeignHostPath {
        path: path.to_owned(),
        reason,
    };
    let here = mount_at(mounts, mountpoint);
    // What a dataset mounted at this point would mask.
    let free = |unmounted: ExistingStorage| match entry {
        _ if here.is_some() => Err(foreign(
            mountpoint,
            format!(
                "{} is mounted there, not {name}",
                here.map_or("another filesystem", |mount| mount.source.as_str())
            ),
        )),
        PathEntry::Missing => Ok(unmounted),
        PathEntry::Directory(directory) if directory.empty => Ok(unmounted),
        PathEntry::Directory(_) => Err(foreign(
            mountpoint,
            format!("it holds entries that {name} mounted there would mask"),
        )),
        PathEntry::Other(kind) => Err(foreign(mountpoint, format!("it is {kind}"))),
    };
    let Some(dataset) = dataset else {
        return free(ExistingStorage::Absent);
    };
    if let Some(mount) = here.filter(|mount| mount.is_dataset() && mount.source == name) {
        let PathEntry::Directory(directory) = entry else {
            return Err(foreign(
                mountpoint,
                format!(
                    "the kernel mounts {} there, but it is not a directory",
                    mount.source
                ),
            ));
        };
        return match &directory.marker {
            Some(marker) => require_mounted_marker(Some(marker), role, SubstrateKind::Zfs)
                .map(|_| ExistingStorage::mounted_valid(name))
                .map_err(|error| {
                    foreign(
                        mountpoint,
                        format!("its marker is not this store's: {error}"),
                    )
                }),
            None if directory.empty => Ok(ExistingStorage::mounted_incomplete(name)),
            None => Err(foreign(
                mountpoint,
                format!("{name} is mounted there holding entries but no cowshed marker"),
            )),
        };
    }
    if let Some(elsewhere) = mounts
        .iter()
        .find(|mount| mount.is_dataset() && mount.source == name)
    {
        return Err(foreign(
            &elsewhere.mountpoint,
            format!(
                "{name} is mounted there, not at {}; unmount it, then rerun cowshed setup",
                mountpoint.display()
            ),
        ));
    }
    free(ExistingStorage::ExistingUnmounted {
        exact_identifier: name.to_owned(),
        marker: dataset.marker.clone(),
    })
}

/// The nearest of `dataset` and its ancestors whose `cowshed` child names `uid` as its owner.
/// `owner_of` reads that one property of that one name.
fn serving_root_parent(
    dataset: &str,
    uid: u32,
    mut owner_of: impl FnMut(&str) -> Result<Option<String>, NativeBootstrapError>,
) -> Result<Option<String>, NativeBootstrapError> {
    let uid = uid.to_string();
    let mut candidate = Some(dataset);
    while let Some(parent) = candidate {
        if owner_of(&zfs_root(parent))?.as_deref() == Some(uid.as_str()) {
            return Ok(Some(parent.to_owned()));
        }
        candidate = parent.rsplit_once('/').map(|(above, _)| above);
    }
    Ok(None)
}

// ---------------------------------------------------------------------------------------------
// Evidence and selection
// ---------------------------------------------------------------------------------------------

/// Everything the planners read of the host. The system source asks the kernel and `zfs`; a test
/// hands the planner typed records.
trait EvidenceSource {
    fn mounts(&mut self) -> Result<Vec<Mount>, NativeBootstrapError>;
    fn entry(&mut self, path: &Path) -> Result<PathEntry, NativeBootstrapError>;
    /// The dataset of exactly this name, `None` when there is none.
    fn dataset(&mut self, name: &str) -> Result<Option<Dataset>, NativeBootstrapError>;
    /// Whether `store` has no host configuration yet. A configuration that records another
    /// mount root than `home`'s refuses here, before setup publishes anything.
    fn host_config_missing(
        &mut self,
        store: &Path,
        home: &Path,
    ) -> Result<bool, NativeBootstrapError>;
    /// Whether the mounted retired caches dataset holds nothing but its marker.
    fn holds_only_marker(&mut self, mountpoint: &Path) -> Result<bool, NativeBootstrapError>;
    fn owner(&mut self) -> DirectoryOwner;
}

/// Which of cowshed's storage an evidence read classifies. Every command reads the store; only
/// setup and doctor read the build cache, as on macOS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scope {
    HostStorage,
    Setup,
}

/// The store this host already has, as its own path shows it.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ExistingStore {
    None,
    Directory { fs_type: String },
    Zfs { parent: String, dataset: String },
}

/// A store that is already published decides the substrate: a dataset named `<parent>/cowshed/
/// store` mounted at the store path, or a directory there carrying a directory-store marker.
fn existing_store(mounts: &[Mount], store: &Path, entry: &PathEntry) -> ExistingStore {
    // `<parent>/cowshed/store`, less its parent.
    let suffix = zfs_child("", ZFS_STORE_CHILD);
    if let Some(mount) = mount_at(mounts, store).filter(|mount| mount.is_dataset())
        && let Some(parent) = mount.source.strip_suffix(suffix.as_str())
    {
        return ExistingStore::Zfs {
            parent: parent.to_owned(),
            dataset: mount.source.clone(),
        };
    }
    let directory_store = matches!(
        entry,
        PathEntry::Directory(DirectoryEntry { marker: Some(marker), .. })
            if VolumeMarker::from_json(marker)
                .is_ok_and(|marker| marker.substrate() == SubstrateKind::Directory)
    );
    if directory_store {
        return ExistingStore::Directory {
            fs_type: mount_of(mounts, store)
                .map_or_else(|| "unknown".to_owned(), |mount| mount.fs_type.clone()),
        };
    }
    ExistingStore::None
}

/// The selection anchor an existing store gives, `None` when the host has no store yet and the
/// anchor path decides. A declaration that names another store than the one the host has is
/// refused: a host has one.
fn existing_anchor(
    existing: &ExistingStore,
    store: &Path,
    configured: Option<&SubstrateConfig>,
) -> Result<Option<StatFsEvidence>, NativeBootstrapError> {
    match existing {
        ExistingStore::None => Ok(None),
        ExistingStore::Directory { fs_type } => match configured {
            Some(configured) => Err(NativeBootstrapError::SubstrateConflict {
                existing: format!("the directory store at {}", store.display()),
                configured: configured.parent().to_owned(),
            }),
            None => Ok(Some(StatFsEvidence::Other {
                fs_type: fs_type.clone(),
            })),
        },
        ExistingStore::Zfs { parent, dataset } => {
            if let Some(configured) = configured.filter(|configured| configured.parent() != parent)
            {
                return Err(NativeBootstrapError::SubstrateConflict {
                    existing: format!("the ZFS dataset {dataset}"),
                    configured: configured.parent().to_owned(),
                });
            }
            DelegatedZfsDataset::new(dataset.clone(), Some(parent.clone()))
                .map(|dataset| {
                    Some(StatFsEvidence::Zfs {
                        containing_dataset: Some(dataset),
                    })
                })
                .map_err(|error| NativeBootstrapError::ZfsRead {
                    dataset: dataset.clone(),
                    message: error.to_string(),
                })
        }
    }
}

/// The filesystem evidence of `anchor` itself: its mount, and for a ZFS one the root that serves
/// the caller from its dataset chain.
fn anchor_statfs(
    source: &mut impl EvidenceSource,
    mounts: &[Mount],
    anchor: &Path,
    owner: DirectoryOwner,
) -> Result<StatFsEvidence, NativeBootstrapError> {
    let Some(mount) = mount_of(mounts, anchor) else {
        return Err(NativeBootstrapError::StatFs {
            path: anchor.to_owned(),
            source: io::Error::new(io::ErrorKind::NotFound, "no mount contains this path"),
        });
    };
    if mount.fs_type != ZFS_FILESYSTEM {
        return Ok(StatFsEvidence::Other {
            fs_type: mount.fs_type.clone(),
        });
    }
    let root_parent = serving_root_parent(&mount.source, owner.uid, |root| {
        Ok(source.dataset(root)?.and_then(|dataset| dataset.owner))
    })?;
    // A dataset name the helper's grammar refuses can hold no root the helper would serve.
    Ok(StatFsEvidence::Zfs {
        containing_dataset: DelegatedZfsDataset::new(mount.source.clone(), root_parent).ok(),
    })
}

/// One of the host's storage objects, classified, with everything that brings it up.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Volume {
    name: String,
    role: VolumeRole,
    /// What it is made in: the root dataset, or the directory above a directory store's path.
    container: String,
    mountpoint: PathBuf,
    state: VolumeState,
    /// A dataset's guid and the bytes it uses.
    identity: Option<(String, u64)>,
    operations: Vec<HostOperation>,
}

impl Volume {
    fn outcome(&self) -> VolumeOutcome {
        VolumeOutcome {
            name: self.name.clone(),
            role: self.role,
            state_before: self.state.clone(),
            action: match self.state {
                VolumeState::Absent => "created",
                VolumeState::MountedValid => "already-current",
                VolumeState::MountedIncomplete => "repaired",
                VolumeState::Detached => "mounted",
                VolumeState::MisMounted { .. } | VolumeState::FoundElsewhere { .. } => "remounted",
            }
            .to_owned(),
        }
    }

    /// The action that announces this volume's operations, none when it is current.
    fn action(&self) -> Option<HostAction> {
        let (uuid, size_bytes) = self.identity.clone().unwrap_or_default();
        match self.state {
            VolumeState::MountedValid => None,
            VolumeState::Absent => Some(HostAction::CreateVolume {
                name: self.name.clone(),
                container: self.container.clone(),
                mount_at: self.mountpoint.clone(),
            }),
            VolumeState::Detached => Some(HostAction::MountExisting {
                name: self.name.clone(),
                uuid,
                size_bytes,
                mount_at: self.mountpoint.clone(),
            }),
            VolumeState::MountedIncomplete
            | VolumeState::MisMounted { .. }
            | VolumeState::FoundElsewhere { .. } => Some(HostAction::RepairMounted {
                name: self.name.clone(),
                uuid,
                size_bytes,
                mounted_at: self.mountpoint.clone(),
                mount_at: self.mountpoint.clone(),
            }),
        }
    }
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

/// The operations of `plan` that belong to one object: those naming its dataset or its
/// mountpoint, behind the read that attests the root. Exhaustive, so an operation a planner adds
/// is placed here or does not compile.
fn operations_for(
    operations: &[HostOperation],
    dataset: Option<&str>,
    mountpoint: Option<&Path>,
) -> Vec<HostOperation> {
    operations
        .iter()
        .filter(|operation| match operation {
            HostOperation::VerifyZfsDelegation { .. } => true,
            HostOperation::GuardMountpoint { path, .. } => Some(path.as_path()) == mountpoint,
            HostOperation::WriteMarkerAtomic { path, .. } => {
                mountpoint.is_some() && path.parent() == mountpoint
            }
            HostOperation::EnsureOwnedDirectory { path, .. } => {
                mountpoint.is_some_and(|mountpoint| mountpoint.starts_with(path))
            }
            // `cowshed-helper <root> <verb> <dataset> …`.
            HostOperation::RunCommand(command) => {
                dataset.is_some() && command.args().get(2).map(String::as_str) == dataset
            }
            HostOperation::EnsureDirectory(_)
            | HostOperation::ReclaimMountpoint(_)
            | HostOperation::MountApfsVolume { .. }
            | HostOperation::ProvisionApfsVolumes { .. }
            | HostOperation::PinVolumesInFstab { .. }
            | HostOperation::ReportVolumeIssue { .. } => false,
        })
        .cloned()
        .collect()
}

/// The host as one evidence read found it, and the plan that follows.
#[derive(Clone, Debug)]
struct Gathered {
    plan: BootstrapPlan,
    owner: DirectoryOwner,
    /// The store first, then a retired caches dataset the host still has, then (for setup) the
    /// build cache.
    volumes: Vec<Volume>,
    /// The `projects` container's name and what creates it, when a ZFS root lacks it.
    projects: Option<(String, Vec<HostOperation>)>,
}

fn require_canonical(path: &Path) -> Result<(), NativeBootstrapError> {
    if crate::repository::is_lexically_canonical(path) {
        Ok(())
    } else {
        Err(NativeBootstrapError::NonCanonicalPath(path.to_owned()))
    }
}

/// What [`read_mounted_dataset`] reads of one dataset: its name, its state, and the guid and
/// used bytes of one that exists.
type MountedDataset = (String, ExistingStorage, Option<(String, u64)>);

/// One mounted dataset of the fixed hierarchy as the host shows it.
fn read_mounted_dataset(
    source: &mut impl EvidenceSource,
    mounts: &[Mount],
    parent: &str,
    child: &str,
    mountpoint: &Path,
    role: VolumeRole,
) -> Result<MountedDataset, NativeBootstrapError> {
    let name = zfs_child(parent, child);
    let entry = source.entry(mountpoint)?;
    let dataset = source.dataset(&name)?;
    let storage = classify_dataset(&name, mountpoint, role, dataset.as_ref(), mounts, &entry)?;
    Ok((
        name,
        storage,
        dataset.map(|dataset| (dataset.guid, dataset.used)),
    ))
}

/// Read the host, select its substrate and plan its store. `anchor` is the path whose
/// filesystem selects a substrate on a host that has no store yet: the project for an opening,
/// the home for setup. Reads only.
fn gather(
    source: &mut impl EvidenceSource,
    scope: Scope,
    anchor: &Path,
    home: &Path,
    configured: Option<&SubstrateConfig>,
) -> Result<Gathered, NativeBootstrapError> {
    require_canonical(anchor)?;
    require_canonical(home)?;
    let owner = source.owner();
    let mounts = source.mounts()?;
    let roots = CanonicalRoots::global();
    let store_path = roots.store().to_owned();
    let build_cache_path = Path::new(BUILD_CACHE_MOUNTPOINT);
    let store_entry = source.entry(&store_path)?;
    let existing = existing_store(&mounts, &store_path, &store_entry);
    let statfs = match existing_anchor(&existing, &store_path, configured)? {
        Some(statfs) => statfs,
        None => anchor_statfs(source, &mounts, anchor, owner)?,
    };
    let selected = select_substrate(statfs, configured)?;
    match &selected {
        SelectedSubstrate::Directory { .. } => {
            require_one_mount(&mounts, &store_path, &canonical_mount_root(home))?;
            let directory = |path: &Path,
                             role: VolumeRole,
                             state: &ExistingDirectory,
                             operations: Vec<HostOperation>| Volume {
                name: path.display().to_string(),
                role,
                container: path
                    .parent()
                    .map_or_else(String::new, |parent| parent.display().to_string()),
                mountpoint: path.to_owned(),
                state: match state {
                    ExistingDirectory::Unpublished => VolumeState::Absent,
                    ExistingDirectory::Published => VolumeState::MountedValid,
                },
                identity: None,
                operations,
            };
            let store = classify_directory(&store_path, VolumeRole::Store, &store_entry, owner)?;
            let plan = plan_bootstrap(
                selected.clone(),
                home,
                BootstrapEvidence::Directory {
                    store: store.clone(),
                    owner,
                },
            )?;
            let mut volumes = vec![directory(
                &store_path,
                VolumeRole::Store,
                &store,
                plan.operations().to_vec(),
            )];
            if scope == Scope::Setup {
                let entry = source.entry(build_cache_path)?;
                let state =
                    classify_directory(build_cache_path, VolumeRole::BuildCache, &entry, owner)?;
                volumes.push(directory(
                    build_cache_path,
                    VolumeRole::BuildCache,
                    &state,
                    plan_directory(
                        build_cache_path,
                        VolumeRole::BuildCache,
                        state.clone(),
                        owner,
                    ),
                ));
            }
            Ok(Gathered {
                plan,
                owner,
                volumes,
                projects: None,
            })
        }
        SelectedSubstrate::Zfs { parent, .. } => {
            let root = zfs_root(parent);
            let caches_path = Path::new(RETIRED_CACHES_MOUNTPOINT);
            let root_state = match source.dataset(&root)? {
                Some(_) => ExistingStorage::mounted_valid(root.clone()),
                None => ExistingStorage::Absent,
            };
            let (store_name, store, store_identity) = read_mounted_dataset(
                source,
                &mounts,
                parent,
                ZFS_STORE_CHILD,
                &store_path,
                VolumeRole::Store,
            )?;
            let (caches_name, caches, caches_identity) = read_mounted_dataset(
                source,
                &mounts,
                parent,
                ZFS_RETIRED_CACHES_CHILD,
                caches_path,
                VolumeRole::Caches,
            )?;
            let projects_name = zfs_child(parent, ZFS_PROJECTS_CHILD);
            let projects = match source.dataset(&projects_name)? {
                Some(dataset) => ExistingStorage::ExistingUnmounted {
                    exact_identifier: projects_name.clone(),
                    marker: dataset.marker,
                },
                None => ExistingStorage::Absent,
            };
            let build_cache = match scope {
                Scope::Setup => Some(read_mounted_dataset(
                    source,
                    &mounts,
                    parent,
                    ZFS_BUILD_CACHE_CHILD,
                    build_cache_path,
                    VolumeRole::BuildCache,
                )?),
                Scope::HostStorage => None,
            };
            let plan = plan_bootstrap(
                selected.clone(),
                home,
                BootstrapEvidence::Zfs {
                    root: root_state,
                    store: store.clone(),
                    caches: caches.clone(),
                    projects: Box::new(projects.clone()),
                },
            )?;
            let volume = |name: String,
                          role: VolumeRole,
                          mountpoint: &Path,
                          storage: &ExistingStorage,
                          identity: Option<(String, u64)>,
                          operations: Vec<HostOperation>| Volume {
                name,
                role,
                container: root.clone(),
                mountpoint: mountpoint.to_owned(),
                state: volume_state(storage),
                identity,
                operations,
            };
            let mut volumes = vec![volume(
                store_name.clone(),
                VolumeRole::Store,
                &store_path,
                &store,
                store_identity,
                operations_for(plan.operations(), Some(&store_name), Some(&store_path)),
            )];
            if !matches!(caches, ExistingStorage::Absent) {
                volumes.push(volume(
                    caches_name.clone(),
                    VolumeRole::Caches,
                    caches_path,
                    &caches,
                    caches_identity,
                    operations_for(plan.operations(), Some(&caches_name), Some(caches_path)),
                ));
            }
            if let Some((name, storage, identity)) = build_cache {
                let operations = plan_zfs_build_cache(parent, build_cache_path, &storage)?;
                volumes.push(volume(
                    name,
                    VolumeRole::BuildCache,
                    build_cache_path,
                    &storage,
                    identity,
                    operations,
                ));
            }
            let projects = matches!(projects, ExistingStorage::Absent).then(|| {
                let operations = operations_for(plan.operations(), Some(&projects_name), None);
                (projects_name, operations)
            });
            Ok(Gathered {
                plan,
                owner,
                volumes,
                projects,
            })
        }
        SelectedSubstrate::Apfs { .. } => Err(NativeBootstrapError::UnsupportedFilesystem {
            path: anchor.to_owned(),
            fs_type: "apfs".to_owned(),
        }),
    }
}

/// Everything a read-only validation would have had to do, named for the operator. A guard and
/// the root's attestation are reads the validation itself performs, so they ask for no setup.
/// Exhaustive, like `shared::mutating_setup_actions`, which keeps the inverse policy.
fn required_setup_actions(plan: &BootstrapPlan) -> Vec<String> {
    plan.operations()
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::GuardMountpoint { .. } | HostOperation::VerifyZfsDelegation { .. } => {
                None
            }
            HostOperation::EnsureDirectory(path) => {
                Some(format!("create directory {}", path.display()))
            }
            HostOperation::EnsureOwnedDirectory { path, .. } => {
                Some(format!("provision owned directory {}", path.display()))
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
            HostOperation::PinVolumesInFstab { .. } => Some("pin volumes in /etc/fstab".to_owned()),
            HostOperation::ReportVolumeIssue { detail, .. } => Some(detail.clone()),
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------------------------

/// What follows a step's operations.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Then {
    Nothing,
    /// Record the host configuration of a store that has none, once its mount root exists.
    PublishHostConfig,
}

/// One announced action and exactly the operations that carry it out.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Step {
    action: HostAction,
    operations: Vec<HostOperation>,
    then: Then,
}

#[derive(Clone, Debug)]
struct Setup {
    home: PathBuf,
    store: PathBuf,
    steps: Vec<Step>,
    volumes: Vec<VolumeOutcome>,
    requires_authorization: bool,
    /// Why this host pins nothing in `/etc/fstab`.
    boot_mounts: &'static str,
}

/// The per-user mount root and the directory it lies in, both the user's alone.
fn mount_root_operations(home: &Path, owner: DirectoryOwner) -> Vec<HostOperation> {
    let root = canonical_mount_root(home);
    root.parent()
        .into_iter()
        .chain(std::iter::once(root.as_path()))
        .map(|path| HostOperation::EnsureOwnedDirectory {
            path: path.to_owned(),
            uid: owner.uid,
            gid: owner.gid,
            mode: MOUNT_ROOT_MODE,
        })
        .collect()
}

/// Plan setup from one evidence read: an action for each object that is not current, the
/// `projects` container of a ZFS root that lacks it, and the per-user mount root. Reads only.
fn prepare_setup(
    source: &mut impl EvidenceSource,
    home: &Path,
    configured: Option<&SubstrateConfig>,
    caches_volume: CachesVolume,
) -> Result<Setup, NativeBootstrapError> {
    let mut gathered = gather(source, Scope::Setup, home, home, configured)?;
    let store = gathered.plan.roots().store().to_owned();
    let mut steps = Vec::new();
    let boot_mounts = match gathered.plan.substrate() {
        SelectedSubstrate::Zfs { parent, .. } => {
            if caches_volume == CachesVolume::Retire
                && let Some(index) = gathered
                    .volumes
                    .iter()
                    .position(|volume| volume.role == VolumeRole::Caches)
            {
                let caches = gathered.volumes.remove(index);
                steps.push(retire_caches(source, parent, &caches)?);
            }
            "ZFS mounts this root's datasets when their pool is imported"
        }
        SelectedSubstrate::Directory { .. } | SelectedSubstrate::Apfs { .. } => {
            "a directory store has no volume to mount at boot"
        }
    };
    let volume_step = |volume: &Volume| {
        volume.action().map(|action| Step {
            action,
            operations: volume.operations.clone(),
            then: Then::Nothing,
        })
    };
    let (build_cache, first): (Vec<&Volume>, Vec<&Volume>) = gathered
        .volumes
        .iter()
        .partition(|volume| volume.role == VolumeRole::BuildCache);
    steps.extend(first.into_iter().filter_map(volume_step));
    if let Some((name, operations)) = &gathered.projects {
        steps.push(Step {
            action: HostAction::CreateVolume {
                name: name.clone(),
                container: gathered
                    .volumes
                    .first()
                    .map(|store| store.container.clone())
                    .unwrap_or_default(),
                // Never mounted: the datasets below it mount under the mount root and at each
                // main's checkout.
                mount_at: PathBuf::from("none"),
            },
            operations: operations.clone(),
            then: Then::Nothing,
        });
    }
    steps.extend(build_cache.into_iter().filter_map(volume_step));

    let owner = gathered.owner;
    let mount_root = mount_root_operations(home, owner);
    let config_missing = source.host_config_missing(&store, home)?;
    let mount_root_changes = directory_changes(&mount_root, owner.uid, |path| source.entry(path))?;
    if mount_root_changes.any || config_missing {
        steps.push(Step {
            action: HostAction::EnsureMountRoot {
                path: canonical_mount_root(home),
                uid: owner.uid,
                gid: owner.gid,
            },
            operations: mount_root,
            then: if config_missing {
                Then::PublishHostConfig
            } else {
                Then::Nothing
            },
        });
    }
    let planned: Vec<HostOperation> = steps
        .iter()
        .flat_map(|step| step.operations.iter().cloned())
        .collect();
    let requires_authorization =
        directory_changes(&planned, owner.uid, |path| source.entry(path))?.privileged;
    Ok(Setup {
        home: home.to_owned(),
        store,
        steps,
        volumes: gathered.volumes.iter().map(Volume::outcome).collect(),
        requires_authorization,
        boot_mounts,
    })
}

/// The deletion of the retired caches dataset, refused unless it is mounted where it belongs
/// holding nothing but its marker: only then can setup show that nothing on it is lost.
fn retire_caches(
    source: &mut impl EvidenceSource,
    parent: &str,
    caches: &Volume,
) -> Result<Step, NativeBootstrapError> {
    if caches.state != VolumeState::MountedValid {
        return Err(NativeBootstrapError::CachesVolumeNotRetirable(format!(
            "{} is not mounted at {}; run cowshed setup to mount it and move what it holds first",
            caches.name,
            caches.mountpoint.display()
        )));
    }
    if !source.holds_only_marker(&caches.mountpoint)? {
        return Err(NativeBootstrapError::CachesVolumeNotRetirable(format!(
            "{} still holds more than its marker; run cowshed setup to move what it holds first",
            caches.mountpoint.display()
        )));
    }
    Ok(Step {
        action: HostAction::DeleteVolume {
            name: caches.name.clone(),
            uuid: caches
                .identity
                .as_ref()
                .map(|(guid, _)| guid.clone())
                .unwrap_or_default(),
            mounted_at: caches.mountpoint.clone(),
        },
        operations: plan_zfs_retire_caches(parent),
        then: Then::Nothing,
    })
}

// ---------------------------------------------------------------------------------------------
// The host
// ---------------------------------------------------------------------------------------------

fn host_io_error(operation: &str, path: &Path, source: io::Error) -> HostError {
    HostError::new(format!("cannot {operation} {path:?}: {source}"))
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

fn invoking_identity() -> DirectoryOwner {
    // SAFETY: both calls only read this process's credentials.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    DirectoryOwner { uid, gid }
}

fn read_marker(directory: &Path) -> io::Result<Option<Vec<u8>>> {
    let marker = directory.join(VOLUME_MARKER_FILE);
    let file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&marker)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        // A link at the marker's name: not a marker.
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => return Ok(None),
        Err(error) => return Err(error),
    };
    if !file.metadata()?.is_file() {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    file.take(MARKER_LIMIT).read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

fn observe_path(path: &Path) -> io::Result<PathEntry> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(PathEntry::Missing),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_symlink() {
        return Ok(PathEntry::Other("a symbolic link"));
    }
    if !metadata.is_dir() {
        return Ok(PathEntry::Other("not a directory"));
    }
    let listed = fs::read_dir(path).and_then(|mut entries| entries.next().transpose());
    let (empty, marker) = match listed {
        Ok(first) => (first.is_none(), read_marker(path)?),
        // Another user's private directory: what cannot be listed is not empty.
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => (false, None),
        Err(error) => return Err(error),
    };
    Ok(PathEntry::Directory(DirectoryEntry {
        uid: metadata.uid(),
        mode: metadata.permissions().mode() & 0o7777,
        empty,
        marker,
    }))
}

fn read_mounts() -> Result<Vec<Mount>, NativeBootstrapError> {
    let unreadable = |source| NativeBootstrapError::StatFs {
        path: PathBuf::from(MOUNTINFO),
        source,
    };
    let table = fs::read_to_string(MOUNTINFO).map_err(unreadable)?;
    parse_mountinfo(&table)
        .map_err(|message| unreadable(io::Error::new(io::ErrorKind::InvalidData, message)))
}

fn read_dataset(name: &str) -> Result<Option<Dataset>, NativeBootstrapError> {
    let unreadable = |message: String| NativeBootstrapError::ZfsRead {
        dataset: name.to_owned(),
        message,
    };
    let output = deadline::output_within(
        ZFS,
        Command::new(ZFS)
            .args(["get", "-H", "-p", "-o", "property,value,source"])
            .arg(dataset_properties())
            .arg(name)
            .env("LC_ALL", "C"),
        &deadline::start("zfs get", ZFS_GET),
    )
    .map_err(|error| unreadable(error.to_string()))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return if stderr.contains(ZFS_NO_DATASET) {
            Ok(None)
        } else {
            Err(unreadable(format!(
                "{ZFS} get ended {}: {}",
                output.status,
                stderr.trim()
            )))
        };
    }
    parse_dataset(&String::from_utf8_lossy(&output.stdout))
        .map(Some)
        .map_err(unreadable)
}

struct SystemEvidence;

impl EvidenceSource for SystemEvidence {
    fn mounts(&mut self) -> Result<Vec<Mount>, NativeBootstrapError> {
        read_mounts()
    }

    fn entry(&mut self, path: &Path) -> Result<PathEntry, NativeBootstrapError> {
        observe_path(path).map_err(|source| NativeBootstrapError::StatFs {
            path: path.to_owned(),
            source,
        })
    }

    fn dataset(&mut self, name: &str) -> Result<Option<Dataset>, NativeBootstrapError> {
        read_dataset(name)
    }

    fn host_config_missing(
        &mut self,
        store: &Path,
        home: &Path,
    ) -> Result<bool, NativeBootstrapError> {
        let path = store.join(HOST_CONFIG_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => HostConfig::load(store, home)
                .map(|_| false)
                .map_err(|error| NativeBootstrapError::Host(HostError::new(error.to_string()))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(NativeBootstrapError::Host(host_io_error(
                "inspect host configuration",
                &path,
                error,
            ))),
        }
    }

    fn holds_only_marker(&mut self, mountpoint: &Path) -> Result<bool, NativeBootstrapError> {
        crate::caches_retirement::holds_only_marker(mountpoint).map_err(|source| {
            NativeBootstrapError::StatFs {
                path: mountpoint.to_owned(),
                source,
            }
        })
    }

    fn owner(&mut self) -> DirectoryOwner {
        invoking_identity()
    }
}

/// The helper setup installed as root's ([`cowshed_helper::host::SYSTEM_DIRECTORY`]), else the
/// one beside the running executable, else the first on `path`.
fn locate_helper(
    beside: Option<&Path>,
    path: &OsStr,
    is_file: impl Fn(&Path) -> bool,
) -> Option<PathBuf> {
    std::iter::once(PathBuf::from(cowshed_helper::host::SYSTEM_DIRECTORY))
        .chain(beside.map(Path::to_path_buf))
        .chain(std::env::split_paths(path))
        .filter(|directory| directory.is_absolute())
        .map(|directory| directory.join(ZFS_HELPER))
        .find(|candidate| is_file(candidate))
}

/// How the helper at `program` is run as root (09_substrates.md, "Privileged helper"): directly
/// when it is installed setuid root or the caller is root already, else through `sudo -n`, which
/// a sudoers entry naming it admits without a prompt.
fn helper_invocation(
    runs_as_root: bool,
    program: &Path,
    arguments: &[String],
) -> (OsString, Vec<OsString>) {
    let arguments = arguments.iter().map(OsString::from);
    if runs_as_root {
        (program.as_os_str().to_owned(), arguments.collect())
    } else {
        (
            OsString::from(SUDO),
            [
                OsString::from("-n"),
                OsString::from("--"),
                program.as_os_str().to_owned(),
            ]
            .into_iter()
            .chain(arguments)
            .collect(),
        )
    }
}

fn helper_command(arguments: &[String]) -> Result<Command, HostError> {
    let executable = std::env::current_exe()
        .and_then(fs::canonicalize)
        .map_err(|error| HostError::new(format!("cannot locate this executable: {error}")))?;
    let path = std::env::var_os("PATH").unwrap_or_default();
    let program = locate_helper(executable.parent(), &path, |candidate| candidate.is_file())
        .ok_or_else(|| {
            HostError::new(format!(
                "{ZFS_HELPER} is neither beside {} nor on PATH; every ZFS mutation goes through it",
                executable.display()
            ))
        })?;
    let metadata = fs::metadata(&program)
        .map_err(|error| host_io_error("inspect the ZFS helper", &program, error))?;
    let setuid_root = metadata.uid() == 0 && metadata.permissions().mode() & 0o4000 != 0;
    let (program, arguments) = helper_invocation(
        setuid_root || invoking_identity().uid == 0,
        &program,
        arguments,
    );
    let mut command = Command::new(program);
    command.args(arguments);
    Ok(command)
}

// WHY unbounded: one helper run is one `zfs` mutation for `cowshed setup`, an operator's verb;
// the operator's interrupt ends it, and a mutation cut short would leave less than `zfs` itself
// guarantees.
#[allow(clippy::disallowed_methods)]
fn run_host_command(command: &HostCommand) -> Result<HostCommandOutput, HostError> {
    let mut spawned = match command.program() {
        ZFS_HELPER => helper_command(command.args())?,
        ZFS => {
            let mut zfs = Command::new(ZFS);
            zfs.args(command.args()).env("LC_ALL", "C");
            zfs
        }
        other => {
            return Err(HostError::new(format!(
                "refusing host command {other:?}: storage setup runs {ZFS} and {ZFS_HELPER} only"
            )));
        }
    };
    spawned.output_locked().map(Into::into).map_err(|source| {
        HostError::new(format!(
            "cannot execute {:?} with {} arguments: {source}",
            command.program(),
            command.args().len()
        ))
    })
}

fn verify_root(parent: &str, required_root: &str) -> Result<(), HostError> {
    if required_root != zfs_root(parent) {
        return Err(HostError::new(format!(
            "{required_root} is not the cowshed root of {parent}"
        )));
    }
    let uid = invoking_identity().uid;
    let dataset = read_dataset(required_root)
        .map_err(|error| HostError::new(error.to_string()))?
        .ok_or_else(|| {
            HostError::new(format!(
                "ZFS root {required_root} does not exist; whoever administers {parent} creates it: zfs create -o mountpoint=none -o {ZFS_OWNER_PROPERTY}={uid} {required_root}"
            ))
        })?;
    if dataset.owner.as_deref() == Some(uid.to_string().as_str()) {
        Ok(())
    } else {
        Err(HostError::new(format!(
            "ZFS root {required_root} does not name uid {uid} as its owner ({ZFS_OWNER_PROPERTY} is {}); whoever administers {parent} sets it: zfs set {ZFS_OWNER_PROPERTY}={uid} {required_root}",
            dataset.owner.as_deref().unwrap_or("not set on it")
        )))
    }
}

/// What a guard reads at `path`. A directory that carries a marker is published whatever backs
/// it: the marker's substrate says which, and the guard compares it.
fn inspect_mountpoint(path: &Path) -> Result<MountpointState, HostError> {
    require_host_canonical(path)?;
    let entry = observe_path(path).map_err(|source| host_io_error("inspect", path, source))?;
    let directory = match entry {
        PathEntry::Missing => return Ok(MountpointState::Missing),
        PathEntry::Other(kind) => {
            return Err(HostError::new(format!("mountpoint {path:?} is {kind}")));
        }
        PathEntry::Directory(directory) => directory,
    };
    if directory.marker.is_some() {
        return Ok(MountpointState::Mounted {
            marker: directory.marker,
        });
    }
    if directory.empty {
        return Ok(MountpointState::EmptyDirectory);
    }
    let mounts = read_mounts().map_err(|error| HostError::new(error.to_string()))?;
    Ok(match mount_at(&mounts, path) {
        Some(_) => MountpointState::Mounted { marker: None },
        None => MountpointState::NonEmptyDirectoryWithoutMount,
    })
}

fn write_file_atomic(path: &Path, contents: &[u8]) -> Result<(), HostError> {
    require_host_canonical(path)?;
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(HostError::new(format!("{path:?} names no file")));
    };
    let name = CString::new(name.as_bytes())
        .map_err(|_| HostError::new(format!("file name contains NUL: {path:?}")))?;
    AnchoredDirectory::open(parent)
        .and_then(|directory| directory.publish_file(&name, contents))
        .map_err(|source| host_io_error("publish", path, source))
}

fn observe_for(path: &Path) -> Result<(PathEntry, PathEntry), HostError> {
    let observe =
        |path: &Path| observe_path(path).map_err(|source| host_io_error("inspect", path, source));
    let parent = match path.parent() {
        Some(parent) => observe(parent)?,
        None => PathEntry::Missing,
    };
    Ok((observe(path)?, parent))
}

/// Bring one directory to `want` as the invoking user, or say that it needs root.
fn ensure_owned_directory_unprivileged(path: &Path, want: Owned) -> Result<(), HostError> {
    require_host_canonical(path)?;
    let (entry, parent) = observe_for(path)?;
    let needs_root = || {
        HostError::new(format!(
            "{} needs administrator authorization to be provisioned; run {SETUP}",
            path.display()
        ))
    };
    let chmod = || {
        fs::set_permissions(path, fs::Permissions::from_mode(want.mode))
            .map_err(|source| host_io_error("set the mode of", path, source))
    };
    match owned_directory_step(&entry, &parent, want, invoking_identity().uid) {
        DirectoryStep::Current => Ok(()),
        DirectoryStep::Create { privileged: false } => {
            fs::DirBuilder::new()
                .mode(want.mode)
                .create(path)
                .map_err(|source| host_io_error("create directory", path, source))?;
            // The creation mode is masked by the umask.
            chmod()
        }
        DirectoryStep::Repair { privileged: false } => chmod(),
        DirectoryStep::Create { privileged: true } | DirectoryStep::Repair { privileged: true } => {
            Err(needs_root())
        }
        DirectoryStep::Refuse(reason) => Err(HostError::new(format!(
            "host path {path:?} cannot hold cowshed storage: {reason}"
        ))),
    }
}

impl BootstrapHost for SystemBootstrapHost {
    fn verify_zfs_delegation(&self, parent: &str, required_root: &str) -> Result<(), HostError> {
        verify_root(parent, required_root)
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        inspect_mountpoint(path)
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        require_host_canonical(path)?;
        fs::create_dir_all(path).map_err(|source| host_io_error("create directory", path, source))
    }

    fn ensure_owned_directory(
        &self,
        path: &Path,
        uid: u32,
        gid: u32,
        mode: u32,
    ) -> Result<(), HostError> {
        ensure_owned_directory_unprivileged(path, Owned { uid, gid, mode })
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        // Nothing on Linux leaves a stub at a mountpoint: only an absent or empty one is free.
        match inspect_mountpoint(path)? {
            MountpointState::Missing | MountpointState::EmptyDirectory => Ok(()),
            MountpointState::ReclaimableStub { .. }
            | MountpointState::NonEmptyDirectoryWithoutMount
            | MountpointState::Mounted { .. } => Err(HostError::new(format!(
                "refusing to reclaim non-empty or mounted path {path:?}"
            ))),
        }
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        run_host_command(command)
    }

    fn provision_apfs_volumes(
        &self,
        _container: &str,
        _volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        Err(platform_host_error("APFS volume provisioning"))
    }

    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        write_file_atomic(path, contents)
    }

    fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
        Err(platform_host_error("pinning APFS volumes in /etc/fstab"))
    }
}

/// Proof that `sudo` authorized the invoking user for this setup: what Authorization Services is
/// to the macOS adapter. `sudo` asks on the terminal when its policy wants a password, and
/// remembers the answer for the commands that follow.
struct SudoSession(());

impl SudoSession {
    // WHY unbounded: `sudo -v` waits for the operator's password at the terminal.
    #[allow(clippy::disallowed_methods)]
    fn acquire() -> Result<Self, HostError> {
        let output = Command::new(SUDO)
            .arg("-v")
            .output_locked()
            .map_err(|source| HostError::new(format!("cannot run {SUDO}: {source}")))?;
        if output.status.success() {
            Ok(Self(()))
        } else {
            Err(HostError::authorization_denied(format!(
                "{SUDO} did not authorize this user: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }

    /// Run one fixed tool as root, by its name on root's own `PATH`.
    // WHY unbounded: `mkdir`, `install` and `chmod` of one directory for `cowshed setup`; the
    // operator's interrupt ends it.
    #[allow(clippy::disallowed_methods)]
    fn run(&self, tool: &str, arguments: &[String]) -> Result<(), HostError> {
        let output = Command::new(SUDO)
            .arg("--")
            .arg(tool)
            .args(arguments)
            .output_locked()
            .map_err(|source| HostError::new(format!("cannot run {SUDO} {tool}: {source}")))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(HostError::new(format!(
                "{SUDO} {tool} {} ended {}: {}",
                arguments.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }
}

/// The arguments of the root tool that carries out one privileged directory step: `install -d`
/// makes a directory with its owner and mode in one run, so no root-owned directory is left
/// between two.
fn privileged_directory_command(
    step: &DirectoryStep,
    path: &str,
    want: Owned,
) -> Option<(&'static str, Vec<String>)> {
    let mode = format!("{:04o}", want.mode);
    match step {
        DirectoryStep::Create { privileged: true } => Some((
            "install",
            vec![
                "-d".to_owned(),
                "-m".to_owned(),
                mode,
                "-o".to_owned(),
                want.uid.to_string(),
                "-g".to_owned(),
                want.gid.to_string(),
                "--".to_owned(),
                path.to_owned(),
            ],
        )),
        DirectoryStep::Repair { privileged: true } => {
            Some(("chmod", vec![mode, "--".to_owned(), path.to_owned()]))
        }
        DirectoryStep::Current
        | DirectoryStep::Create { privileged: false }
        | DirectoryStep::Repair { privileged: false }
        | DirectoryStep::Refuse(_) => None,
    }
}

struct AuthorizedBootstrapHost {
    session: SudoSession,
}

impl BootstrapHost for AuthorizedBootstrapHost {
    fn verify_zfs_delegation(&self, parent: &str, required_root: &str) -> Result<(), HostError> {
        SystemBootstrapHost.verify_zfs_delegation(parent, required_root)
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        SystemBootstrapHost.inspect_mountpoint(path)
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        SystemBootstrapHost.create_dir_all(path)
    }

    fn ensure_owned_directory(
        &self,
        path: &Path,
        uid: u32,
        gid: u32,
        mode: u32,
    ) -> Result<(), HostError> {
        require_host_canonical(path)?;
        let want = Owned { uid, gid, mode };
        let invoker = invoking_identity().uid;
        let (entry, parent) = observe_for(path)?;
        let step = owned_directory_step(&entry, &parent, want, invoker);
        let text = path
            .to_str()
            .ok_or_else(|| HostError::new(format!("host path is not UTF-8: {path:?}")))?;
        let Some((tool, arguments)) = privileged_directory_command(&step, text, want) else {
            return ensure_owned_directory_unprivileged(path, want);
        };
        self.session.run(tool, &arguments)?;
        let (entry, parent) = observe_for(path)?;
        match owned_directory_step(&entry, &parent, want, invoker) {
            DirectoryStep::Current => Ok(()),
            other => Err(HostError::new(format!(
                "{} failed its ownership and mode attestation after {SUDO} {tool}: {other:?}",
                path.display()
            ))),
        }
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        SystemBootstrapHost.reclaim_mountpoint(path)
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        SystemBootstrapHost.run_command(command)
    }

    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        SystemBootstrapHost.provision_apfs_volumes(container, volumes)
    }

    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError> {
        SystemBootstrapHost.write_file_atomic(path, contents)
    }

    fn pin_volumes_in_fstab(&self, pins: &[FstabPin]) -> Result<(), HostError> {
        SystemBootstrapHost.pin_volumes_in_fstab(pins)
    }
}

// ---------------------------------------------------------------------------------------------
// Entrypoints
// ---------------------------------------------------------------------------------------------

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, NativeBootstrapError> + Send + 'static,
) -> Result<T, NativeBootstrapError> {
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        NativeBootstrapError::Execution(BootstrapExecutionError::BlockingLane(error.to_string()))
    })?
}

/// The `[substrate]` the `.cowshed.toml` in `directory` declares, `None` when there is no file
/// or it declares none.
fn declared_substrate(directory: &Path) -> Result<Option<SubstrateConfig>, NativeBootstrapError> {
    let path = directory.join(COWSHED_CONFIG_FILE);
    let declaration = |message: String| NativeBootstrapError::SubstrateDeclaration {
        path: path.clone(),
        message,
    };
    match fs::read_to_string(&path) {
        Ok(input) => parse_cowshed_config(&input)
            .map(|config| config.substrate().cloned())
            .map_err(|error| declaration(error.to_string())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(declaration(error.to_string())),
    }
}

/// The declaration setup runs under: that of the nearest `.cowshed.toml` at or above the
/// directory it was started in. Setup takes no project, so the checkout it is run from is the
/// one place a host with no store yet can name a ZFS root from.
fn declared_substrate_above(start: &Path) -> Result<Option<SubstrateConfig>, NativeBootstrapError> {
    for directory in start.ancestors() {
        if fs::symlink_metadata(directory.join(COWSHED_CONFIG_FILE)).is_ok() {
            return declared_substrate(directory);
        }
    }
    Ok(None)
}

fn working_directory_declaration() -> Result<Option<SubstrateConfig>, NativeBootstrapError> {
    let start = std::env::current_dir().map_err(|source| NativeBootstrapError::StatFs {
        path: PathBuf::from("."),
        source,
    })?;
    declared_substrate_above(&start)
}

/// Gather the host's evidence for the checkout at `project_root`, plan its store purely, then
/// execute the plan according to the explicit capability mode.
///
/// The returned plan is the exact plan that completed successfully. In `ExistingOnly`, a plan
/// containing any mutating operation is rejected before the execution lane is dispatched.
pub async fn bootstrap_system_storage(
    project_root: &Path,
    home: &Path,
    mode: NativeBootstrapMode,
) -> Result<BootstrapPlan, NativeBootstrapError> {
    let (project_root, home) = (project_root.to_owned(), home.to_owned());
    let plan = blocking(move || {
        let configured = declared_substrate(&project_root)?;
        gather(
            &mut SystemEvidence,
            Scope::HostStorage,
            &project_root,
            &home,
            configured.as_ref(),
        )
        .map(|gathered| gathered.plan)
    })
    .await?;
    execute_native_bootstrap_plan(
        &plan,
        mode,
        Arc::new(SystemBootstrapHost),
        &TokioBlockingLane,
    )
    .await?;
    Ok(plan)
}

/// The build cache's root when this host has it provisioned: its path carrying its build-cache
/// marker, and for a ZFS one a dataset mounted there. Anything less proves nothing, and the
/// workspace environment then names no root. Reads one directory and, for ZFS, the mount table;
/// it runs no command.
pub fn provisioned_build_cache() -> Option<PathBuf> {
    let path = Path::new(BUILD_CACHE_MOUNTPOINT);
    let PathEntry::Directory(directory) = observe_path(path).ok()? else {
        return None;
    };
    let marker = VolumeMarker::from_json(directory.marker.as_deref()?).ok()?;
    if marker.role() != VolumeRole::BuildCache || !marker.is_current() {
        return None;
    }
    let provisioned = match marker.substrate() {
        SubstrateKind::Directory => directory.uid == invoking_identity().uid,
        SubstrateKind::Zfs => mount_at(&read_mounts().ok()?, path).is_some_and(Mount::is_dataset),
        SubstrateKind::Apfs => false,
    };
    provisioned.then(|| path.to_owned())
}

/// Validate the invoking user's pre-existing, machine-global host storage.
///
/// The store's own path decides what it is; `home` anchors the selection only on a host that has
/// none. This boundary gathers the same evidence as setup and executes only its reads: the
/// guards, and for ZFS the root's attestation. Anything else a plan holds is reported as
/// setup-required, so a gateway or a command never changes host storage while validating it.
pub async fn validate_existing_host_storage(home: &Path) -> crate::Result<ValidatedHostStorage> {
    let gather_home = home.to_owned();
    let plan = blocking(move || {
        gather(
            &mut SystemEvidence,
            Scope::HostStorage,
            &gather_home,
            &gather_home,
            None,
        )
        .map(|gathered| gathered.plan)
    })
    .await
    .map_err(existing_host_storage_error)?;
    let actions = required_setup_actions(&plan);
    if !actions.is_empty() {
        return Err(existing_host_storage_error(
            NativeBootstrapError::StorageSetupRequired {
                actions,
                hint: SETUP,
            },
        ));
    }
    execute_native_bootstrap_plan(
        &plan,
        NativeBootstrapMode::ExistingOnly,
        Arc::new(SystemBootstrapHost),
        &TokioBlockingLane,
    )
    .await
    .map_err(existing_host_storage_error)?;
    Ok(ValidatedHostStorage::new(
        plan.home().to_owned(),
        crate::capabilities::host_program_directories(plan.home()),
        plan.roots().clone(),
    ))
}

async fn gather_setup(
    home: &Path,
    caches_volume: CachesVolume,
) -> Result<Setup, NativeBootstrapError> {
    let home = home.to_owned();
    blocking(move || {
        let configured = working_directory_declaration()?;
        prepare_setup(
            &mut SystemEvidence,
            &home,
            configured.as_ref(),
            caches_volume,
        )
    })
    .await
}

pub async fn plan_host_setup(
    home: &Path,
    caches_volume: CachesVolume,
) -> crate::Result<HostSetupPlan> {
    let setup = gather_setup(home, caches_volume)
        .await
        .map_err(existing_host_storage_error)?;
    Ok(HostSetupPlan::new(
        setup.steps.iter().map(|step| step.action.clone()).collect(),
        setup.volumes,
        setup.requires_authorization,
    ))
}

/// Publish the host configuration of a store that has none, for the mount root just attested.
fn publish_host_config(store: &Path, home: &Path) -> crate::Result<()> {
    let missing = |error: String| CowshedError::environment_missing(error, SETUP);
    let config = HostConfig::load(store, home).map_err(|error| missing(error.to_string()))?;
    if config.mount_root() != canonical_mount_root(home) {
        return Err(CowshedError::conflict(
            "host mount root changed during setup",
            SETUP,
        ));
    }
    let path = store.join(HOST_CONFIG_FILE);
    match fs::symlink_metadata(&path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => config
            .save(store)
            .map_err(|error| missing(error.to_string())),
        Err(error) => Err(missing(
            host_io_error("inspect host configuration", &path, error).to_string(),
        )),
    }
}

async fn execute_step(
    setup: &Setup,
    step: &Step,
    host: Arc<dyn BootstrapHost>,
) -> crate::Result<()> {
    for operation in &step.operations {
        execute_bootstrap_operation(operation, Arc::clone(&host), &TokioBlockingLane)
            .await
            .map_err(NativeBootstrapError::Execution)
            .map_err(|error| setup_execution_error(error, SETUP))?;
    }
    match step.then {
        Then::Nothing => Ok(()),
        Then::PublishHostConfig => {
            let (store, home) = (setup.store.clone(), setup.home.clone());
            tokio::task::spawn_blocking(move || publish_host_config(&store, &home))
                .await
                .map_err(|error| {
                    CowshedError::internal(format!("host configuration lane failed: {error}"))
                })?
        }
    }
}

pub async fn execute_host_setup(
    home: &Path,
    caches_volume: CachesVolume,
) -> crate::Result<HostSetupReport> {
    let setup = gather_setup(home, caches_volume)
        .await
        .map_err(existing_host_storage_error)?;
    let authorized = setup.requires_authorization;
    let host: Arc<dyn BootstrapHost> = if authorized {
        let session = blocking(|| SudoSession::acquire().map_err(NativeBootstrapError::Host))
            .await
            .map_err(|error| setup_execution_error(error, SETUP))?;
        Arc::new(AuthorizedBootstrapHost { session })
    } else {
        Arc::new(SystemBootstrapHost)
    };
    let mut action_outcomes: Vec<HostActionOutcome> = setup
        .steps
        .iter()
        .map(|step| HostActionOutcome {
            action: step.action.clone(),
            outcome: HostActionResult::Skipped,
        })
        .collect();
    let mut fstab = FstabOutcome::Skipped(setup.boot_mounts.to_owned());
    for (step, outcome) in setup.steps.iter().zip(&mut action_outcomes) {
        match execute_step(&setup, step, Arc::clone(&host)).await {
            Ok(()) => outcome.outcome = HostActionResult::Done,
            Err(error) => {
                fstab = FstabOutcome::Skipped(error.message.clone());
                outcome.outcome = HostActionResult::Failed { error };
                break;
            }
        }
    }
    Ok(HostSetupReport {
        action_outcomes,
        volumes: setup.volumes,
        fstab,
        authorized,
    })
}

/// Setup pins nothing in `/etc/fstab` and installs no system service on Linux, so teardown of
/// the host's storage presence has nothing to remove: the store, like a macOS volume, stays.
pub async fn plan_host_uninstall(home: &Path) -> crate::Result<HostUninstallPlan> {
    require_canonical(home).map_err(existing_host_storage_error)?;
    Ok(HostUninstallPlan {
        pins_to_remove: Vec::new(),
        requires_authorization: false,
    })
}

pub async fn execute_host_uninstall(home: &Path) -> crate::Result<UninstallReport> {
    plan_host_uninstall(home).await?;
    Ok(UninstallReport {
        fstab: UninstallFstabOutcome::AlreadyClean,
        services: Vec::new(),
    })
}

/// Provision only the fixed per-user mount root; an alternative path must be a user-owned alias.
pub async fn execute_mount_root_provisioning(root: &Path) -> crate::Result<()> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|home| crate::repository::is_lexically_canonical(home))
        .ok_or_else(|| {
            CowshedError::environment_missing(
                "HOME is missing or is not absolute",
                "run cowshed with a canonical HOME",
            )
        })?;
    let required = canonical_mount_root(&home);
    if root != required {
        return Err(CowshedError::usage(
            format!(
                "workspace mount root is fixed at {}; create a symlink at the preferred path pointing to it, never the reverse",
                required.display()
            ),
            SETUP,
        ));
    }
    let host: Arc<dyn BootstrapHost> = Arc::new(SystemBootstrapHost);
    for operation in mount_root_operations(&home, invoking_identity()) {
        execute_bootstrap_operation(&operation, Arc::clone(&host), &TokioBlockingLane)
            .await
            .map_err(NativeBootstrapError::Execution)
            .map_err(|error| setup_execution_error(error, SETUP))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
