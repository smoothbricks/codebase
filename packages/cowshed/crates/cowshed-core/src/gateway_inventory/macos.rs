//! macOS's reading of one project's native store: APFS volumes, whose mounts the kernel mount
//! table reports. One reading of that table serves the whole pass, so the ambiguity check and
//! the mount facts it guards judge the same mounts.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use cowshed_apfs::SystemCommandRunner;
use cowshed_apfs::mounts::KernelMountSnapshot;

use super::{GatewayInventoryError, ProjectInventoryFacts, expected_mount_paths};
use crate::repository::RepoId;
use crate::storage::StorageLayout;
use crate::storage::apfs::native::{
    KernelMountSource, MacOsApfsExecutionHost, SystemKernelMountSource,
};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig};

#[derive(Clone)]
struct CapturedKernelMountSource {
    mounts: Vec<KernelMountSnapshot>,
}

impl KernelMountSource for CapturedKernelMountSource {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
        Ok(self.mounts.clone())
    }
}

/// The storage and mount facts of `repo`, whose checkout `config` names, from one reading of the
/// kernel mount table.
pub(super) fn project_facts(
    config: &ApfsSubstrateConfig,
    layout: &StorageLayout,
    repo: &RepoId,
) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
    let captured = SystemKernelMountSource.mounts()?;
    let host = MacOsApfsExecutionHost::with_mount_source(
        SystemCommandRunner::new(config.namespace),
        config.clone(),
        CapturedKernelMountSource {
            mounts: captured.clone(),
        },
    )?;
    let storage_facts = host.list(repo)?;
    let mount_paths = expected_mount_paths(config, layout, &storage_facts)?;
    reject_ambiguous_native_mounts(&captured, &mount_paths)?;
    let mounts = host.mounts(repo)?;
    Ok(ProjectInventoryFacts {
        storage: storage_facts,
        mounts,
        mount_paths,
    })
}

fn reject_ambiguous_native_mounts(
    mounts: &[KernelMountSnapshot],
    expected: &BTreeMap<String, PathBuf>,
) -> Result<(), GatewayInventoryError> {
    let expected_paths = expected.values().collect::<BTreeSet<_>>();
    let mut sources_at_canonical_path = BTreeSet::new();
    for mount in mounts {
        if expected_paths.contains(&mount.mount_point) {
            if !sources_at_canonical_path.insert(mount.source_device.as_str()) {
                return Err(GatewayInventoryError::AmbiguousMount(
                    mount.mount_point.display().to_string(),
                ));
            }
            let source_count = mounts
                .iter()
                .filter(|candidate| candidate.source_device == mount.source_device)
                .count();
            if source_count != 1 {
                return Err(GatewayInventoryError::AmbiguousMount(
                    mount.source_device.clone(),
                ));
            }
        }
    }
    for path in expected_paths {
        if mounts
            .iter()
            .filter(|mount| &mount.mount_point == path)
            .count()
            > 1
        {
            return Err(GatewayInventoryError::AmbiguousMount(
                path.display().to_string(),
            ));
        }
    }
    Ok(())
}
