//! Root-held mountpoints: a bare, unmounted mountpoint whose only entry is a root-owned
//! `.fseventsd` event log (macOS's `fseventsd` leaves one at a volume's root, and it stays behind
//! in the mountpoint directory when the volume unmounts first). No unprivileged cleanup removes
//! it, so retirement holds the directory by descriptor, proves what it is, and renames it aside
//! for a privileged cleanup instead of failing on it.
//!
//! Every check is fd-relative POSIX (`AnchoredDirectory`), so the same code answers on every
//! platform; where no daemon leaves such a log, none is ever found.

use std::ffi::CString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use cowshed_apfs::CommandRunner;
use cowshed_os::fsio::AnchoredDirectory;

use super::{
    ApfsStorageError, MacOsApfsExecutionHost, RootHeldInspection, RootHeldMountpoint, io_error,
};

/// A bare mountpoint held with its parent, without following a mutable path component.
pub(super) struct RootHeldDirectory {
    path: PathBuf,
    parent: AnchoredDirectory,
    directory: AnchoredDirectory,
    name: CString,
    identity: (libc::dev_t, libc::ino_t),
}

impl RootHeldDirectory {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn into_path(self) -> PathBuf {
        self.path
    }
}

fn root_owned_event_log(metadata: &libc::stat) -> bool {
    metadata.st_uid == 0
        && metadata.st_mode & libc::S_IFMT == libc::S_IFDIR
        && metadata.st_mode & 0o777 == 0o700
}

/// Whether the entry at `path`, `relative` to the mountpoint a stray walk started from, is the
/// root-owned event log `fseventsd` keeps at a volume's root.
pub(super) fn root_held_event_log(
    path: &Path,
    relative: &Path,
    file_type: fs::FileType,
) -> Result<bool, ApfsStorageError> {
    if relative == Path::new(".fseventsd") && file_type.is_dir() {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| io_error("inspect root-held event-log candidate", path, error))?;
        if metadata.uid() == 0 && metadata.mode() & 0o777 == 0o700 {
            return Ok(true);
        }
    }
    Ok(false)
}

impl<R: CommandRunner> MacOsApfsExecutionHost<R> {
    /// The root-held mountpoint at `path`, held, or `None` when `path` is not one. An
    /// [`RootHeldInspection::OnlyLog`] inspection also requires the event log to be its only
    /// entry.
    pub(super) fn inspect_root_held_mountpoint(
        &self,
        path: &Path,
        inspection: RootHeldInspection,
    ) -> Result<Option<RootHeldDirectory>, ApfsStorageError> {
        let parent_path = path.parent().ok_or(ApfsStorageError::InvalidPlan(
            "root-held mountpoint has no parent",
        ))?;
        let name = CString::new(
            path.file_name()
                .ok_or(ApfsStorageError::InvalidPlan(
                    "root-held mountpoint has no name",
                ))?
                .as_bytes(),
        )
        .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let parent = match AnchoredDirectory::open(parent_path) {
            Ok(parent) => parent,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io_error(
                    "hold root-held mountpoint parent",
                    parent_path,
                    error,
                ));
            }
        };
        let directory = match parent.existing_child(&name) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error("hold root-held mountpoint", path, error)),
        };
        if !directory
            .stat_child(c".fseventsd")
            .map_err(|error| io_error("inspect held event-log metadata", path, error))?
            .as_ref()
            .is_some_and(root_owned_event_log)
            || (matches!(inspection, RootHeldInspection::OnlyLog)
                && !directory
                    .contains_only(c".fseventsd")
                    .map_err(|error| io_error("inspect held mountpoint entries", path, error))?)
        {
            return Ok(None);
        }
        let metadata = directory
            .metadata()
            .map_err(|error| io_error("inspect held mountpoint", path, error))?;
        let parent_metadata = parent
            .metadata()
            .map_err(|error| io_error("inspect held mountpoint parent", parent_path, error))?;
        if metadata.dev() != parent_metadata.dev()
            || self
                .mount_source
                .mounts()?
                .iter()
                .any(|mount| mount.mount_point.starts_with(path))
        {
            return Err(ApfsStorageError::Host(format!(
                "root-held mountpoint still carries a mounted filesystem: {}",
                path.display()
            )));
        }
        let entry = parent
            .stat_child(&name)
            .map_err(|error| io_error("inspect held mountpoint name", path, error))?
            .ok_or(ApfsStorageError::GcPlanStale)?;
        if entry.st_ino != metadata.ino() || entry.st_mode & libc::S_IFMT != libc::S_IFDIR {
            return Err(ApfsStorageError::GcPlanStale);
        }
        Ok(Some(RootHeldDirectory {
            path: path.to_owned(),
            parent,
            directory,
            name,
            identity: (entry.st_dev, entry.st_ino),
        }))
    }

    pub(super) fn retain_root_held_mountpoint(
        &self,
        held: RootHeldDirectory,
    ) -> Result<RootHeldMountpoint, ApfsStorageError> {
        // Cleanup may have removed other artifacts since the directory was first held. Look at
        // this directory, and the name that still refers to it, before changing the namespace.
        let current = held.parent.stat_child(&held.name).map_err(|error| {
            io_error("inspect root-held name before retention", &held.path, error)
        })?;
        if current.as_ref().map(|entry| (entry.st_dev, entry.st_ino)) != Some(held.identity)
            || !held
                .directory
                .contains_only(c".fseventsd")
                .map_err(|error| {
                    io_error(
                        "inspect root-held entries before retention",
                        &held.path,
                        error,
                    )
                })?
            || !held
                .directory
                .stat_child(c".fseventsd")
                .map_err(|error| {
                    io_error("inspect root-held log before retention", &held.path, error)
                })?
                .as_ref()
                .is_some_and(root_owned_event_log)
        {
            return Err(ApfsStorageError::GcPlanStale);
        }
        if self
            .mount_source
            .mounts()?
            .iter()
            .any(|mount| mount.mount_point.starts_with(&held.path))
        {
            return Err(ApfsStorageError::Host(format!(
                "root-held mountpoint became mounted before retention: {}",
                held.path.display()
            )));
        }
        let destination_name = format!(
            "{}{}",
            crate::storage::recovery::ROOT_HELD_MOUNT_PREFIX,
            uuid::Uuid::new_v4().simple()
        );
        let destination = CString::new(destination_name.as_bytes())
            .expect("a fixed prefix and UUID contain no NUL");
        held.parent
            .rename_child_noreplace(&held.name, &destination)
            .map_err(|error| {
                io_error(
                    "retain root-held mountpoint without replacement",
                    &held.path,
                    error,
                )
            })?;
        let path = held.path.with_file_name(destination_name);
        let retained = RootHeldMountpoint::Retained(path);
        let (_, cleanup) = retained.cleanup_guidance();
        cowshed_os::eprintln!(
            "cowshed: retained root-owned event-log debris from {} at {}; privileged cleanup: {}",
            held.path.display(),
            retained.mount_point().display(),
            cleanup
        );
        Ok(retained)
    }
}
