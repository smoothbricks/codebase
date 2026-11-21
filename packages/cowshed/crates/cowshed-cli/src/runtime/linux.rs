//! What the CLI says and reads of a Linux host: a store of ZFS datasets under one root dataset,
//! or of plain directories, that setup neither encrypts nor mounts through a service. Linux has
//! no IOKit registry, no disk-image helper and no disk tool, so doctor reports no device class
//! and `disk-command` refuses.

use std::path::Path;

use cowshed_core::api::{Finding, FindingSeverity, WorkspaceInfo};
use cowshed_core::{CowshedError, Result};

/// The parties doctor's host-storage probe waits on: planning host setup reads the kernel's
/// mount table and asks `zfs` for each dataset of a ZFS store by its exact name.
pub(super) const HOST_STORAGE_PARTIES: &str =
    "the kernel mount table (/proc/self/mountinfo) and, for a ZFS store, zfs get of its datasets";

/// How the planner writes the mountpoint of a dataset that is never mounted, the `projects`
/// container (`storage/bootstrap/native/linux.rs`, `prepare_setup`): ZFS's own word for it.
const NEVER_MOUNTED: &str = "none";

/// `disk-command` runs `diskutil` or `hdiutil`. Linux has neither, and nothing for one to act
/// on: a workspace is a ZFS dataset or a directory, never an attached disk image. The verb
/// refuses before it takes the disk lease or spawns anything.
pub(super) fn disk_tools() -> Result<()> {
    Err(CowshedError::environment_missing(
        "disk-command runs diskutil or hdiutil, which only macOS has: a Linux store keeps its workspaces as ZFS datasets or directories and attaches no disk image",
        "cowshed doctor",
    ))
}

/// What an adoption created: main's payload in the store, a ZFS dataset whose refquota is the
/// capacity, or a plain directory, which grows within its filesystem and takes no cap. Which of
/// the two the store is, is not read here, so the line says what the capacity means for each.
pub(super) fn created_main(info: &WorkspaceInfo, capacity: &str) -> String {
    format!(
        "created {} for {} (a ZFS store caps it at {capacity}; a directory store sets no cap)",
        info.workspace, info.repo_id,
    )
}

/// What a planned storage object is, read off its name as the planner gives it
/// (`storage/bootstrap/native/linux.rs`, `gather`): an object of a directory store is named by
/// its own absolute path, one of a ZFS root by its dataset name, which never begins with `/`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Object {
    Dataset,
    Directory,
}

fn object(name: &str) -> Object {
    if Path::new(name).is_absolute() {
        Object::Directory
    } else {
        Object::Dataset
    }
}

/// What creating one storage object will do. `container` is the root dataset a dataset is made
/// in, or the directory above a directory store's path.
pub(super) fn create_volume_evidence(name: &str, container: &str, mount_at: &Path) -> String {
    match object(name) {
        Object::Directory => {
            format!("create directory {name} under {container} and publish its marker")
        }
        Object::Dataset if mount_at == Path::new(NEVER_MOUNTED) => {
            format!("create ZFS dataset {name} in root dataset {container}, never mounted")
        }
        Object::Dataset => format!(
            "create ZFS dataset {name} in root dataset {container} and mount it at {}",
            mount_at.display()
        ),
    }
}

/// The Linux planner encrypts nothing: a dataset's encryption is its pool administrator's and a
/// directory store has none. An encryption that is planned all the same is said as what the
/// action is, with no mechanism claimed for it.
pub(super) fn encrypt_volume_evidence(name: &str, uuid: &str, size_bytes: u64) -> String {
    format!("encrypt existing {name} ({uuid}, {size_bytes} bytes) in place")
}

/// The Linux planner installs no mount service: ZFS mounts a root's datasets when their pool is
/// imported, and a directory store has nothing to mount at boot. One that is planned all the
/// same is said as what the action is.
pub(super) fn mount_service_evidence(label: &str) -> String {
    format!("install system mount service {label}")
}

/// A storage object that is current: a dataset mounted where it belongs, or a directory, which
/// is not mounted anywhere, carrying its marker.
pub(super) fn current_volume(name: &str, expected: &Path) -> String {
    match object(name) {
        Object::Dataset => format!(
            "{name}: present, mounted at {}, marker valid",
            expected.display()
        ),
        Object::Directory => format!("{name}: present, a directory carrying its marker"),
    }
}

/// A storage object setup has yet to create: a dataset its root dataset lacks, or a directory
/// store's path that has no entry or only the owner's empty directory without a marker.
pub(super) fn absent_volume(name: &str, container: Option<&str>, expected: &Path) -> String {
    match (object(name), container) {
        (Object::Directory, _) => {
            format!("{name}: not published: no directory there, or an empty one without its marker")
        }
        (Object::Dataset, Some(container)) => format!(
            "{name}: absent from ZFS root dataset {container}; expected mount {}; marker absent",
            expected.display()
        ),
        (Object::Dataset, None) => format!(
            "{name}: absent; expected mount {}; marker absent",
            expected.display()
        ),
    }
}

/// Doctor's finding for a planned encryption, which the Linux planner never plans
/// ([`encrypt_volume_evidence`]).
pub(super) fn encryption_finding(name: &str, uuid: &str, size_bytes: u64) -> Finding {
    Finding {
        code: "host-volume-encryption".into(),
        severity: FindingSeverity::Error,
        message: format!("{name}: encryption is not configured ({uuid}, {size_bytes} bytes)"),
        hint: "cowshed setup".into(),
        path: None,
    }
}

/// Doctor's finding for a planned mount service, which the Linux planner never plans
/// ([`mount_service_evidence`]). No definition file is named: Linux setup writes none.
pub(super) fn mount_service_finding(label: &str) -> Finding {
    Finding {
        code: "host-mount-service".into(),
        severity: FindingSeverity::Error,
        message: format!("system mount service {label} needs repair"),
        hint: "cowshed setup".into(),
        path: None,
    }
}

/// The kernel device classes doctor reports: none. The disk-image helpers, the attach budget,
/// the IOMedia match-notification cap and the departed volume references are all read from
/// macOS's IOKit registry, and Linux has no registry, no helper process per image and no image
/// to attach.
pub(super) fn device_findings() -> Vec<Finding> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "tank/cowshed";
    const STORE: &str = "/private/cowshed/store";

    #[test]
    fn a_dataset_is_named_with_its_root_dataset_and_mountpoint() {
        let dataset = "tank/cowshed/store";
        assert_eq!(
            create_volume_evidence(dataset, ROOT, Path::new(STORE)),
            "create ZFS dataset tank/cowshed/store in root dataset tank/cowshed and mount it at /private/cowshed/store"
        );
        assert_eq!(
            create_volume_evidence("tank/cowshed/projects", ROOT, Path::new(NEVER_MOUNTED)),
            "create ZFS dataset tank/cowshed/projects in root dataset tank/cowshed, never mounted"
        );
        assert_eq!(
            current_volume(dataset, Path::new(STORE)),
            "tank/cowshed/store: present, mounted at /private/cowshed/store, marker valid"
        );
        assert_eq!(
            absent_volume(dataset, Some(ROOT), Path::new(STORE)),
            "tank/cowshed/store: absent from ZFS root dataset tank/cowshed; expected mount /private/cowshed/store; marker absent"
        );
    }

    /// A directory store's object is its own path: nothing is mounted, so nothing says it is.
    #[test]
    fn a_directory_is_never_said_to_be_mounted() {
        let said = [
            create_volume_evidence(STORE, "/private/cowshed", Path::new(STORE)),
            current_volume(STORE, Path::new(STORE)),
            absent_volume(STORE, Some("/private/cowshed"), Path::new(STORE)),
        ];
        for line in &said {
            assert!(line.contains(STORE), "{line}");
            assert!(!line.contains("mount"), "{line}");
            assert!(!line.contains("ZFS"), "{line}");
        }
        assert_eq!(
            said[0],
            "create directory /private/cowshed/store under /private/cowshed and publish its marker"
        );
    }

    #[test]
    fn doctor_reports_no_device_class_and_disk_command_refuses() {
        assert!(device_findings().is_empty());
        let refused = disk_tools().expect_err("Linux has no disk tool");
        assert_eq!(refused.code, cowshed_core::ErrorCode::EnvironmentMissing);
        assert!(refused.message.contains("only macOS"), "{refused:?}");
    }
}
