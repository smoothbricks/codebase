//! A checkout's Nx state, deleted where its build links point (16_build_volumes.md, "Clearing a
//! checkout's Nx state"). `nx reset` removes `.nx/cache` and `.nx/workspace-data` themselves,
//! which in a cowshed checkout are the build links, and leaves every entry on the volume. This
//! deletes what the links name, under the exclusions reseed takes: the workspace's image lock, the
//! volume's exclusive job-admission claim, and the checkout's Nx closed.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use super::reseed::named;
use super::{BuildVolumes, RuntimeStorageHost, io, storage};
use crate::api::dto::NxStateClearResult;
use crate::build_volume::{BuildVolumeState, nx};
use crate::metadata::WorkspaceName;
use crate::storage::apfs::native::BuildVolumeClaim;
use crate::{CowshedError, Result};
use cowshed_apfs::store_lock::LockMode;

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    /// Delete every entry directly in each Nx cache and workspace-data directory the build volume
    /// `checkout` links holds. Refused before anything is deleted while another lifecycle
    /// operation holds the workspace's image lock at `image_lock`, a job or land holds the
    /// volume, or a process other than the checkout's Nx daemon has a task database open or is
    /// opening one. The daemon is stopped as `nx daemon --stop` stops it; no job is stopped and
    /// nothing is waited for. Nx's open locks stay held, and stay on disk, until the deletion
    /// ends, so no Nx process opens a task database under it.
    pub async fn clear_nx_state(
        &self,
        workspace: WorkspaceName,
        checkout: PathBuf,
        image_lock: PathBuf,
    ) -> Result<NxStateClearResult> {
        self.blocking(move |host, layout| {
            let refused = |what: String| {
                CowshedError::conflict(
                    format!("cannot clear {workspace}'s Nx state: {what}; nothing was cleared"),
                    format!("let it finish, then run `cowshed cache clear {workspace}` again"),
                )
            };
            let Some(_locked) = host
                .lock_images(std::slice::from_ref(&image_lock), LockMode::Try)
                .map_err(storage)?
            else {
                let holders = nx::holders(&image_lock)
                    .map_err(|error| io("name image lock holders", &image_lock, &error))?;
                return Err(refused(format!(
                    "a lifecycle operation holds its image lock {}: {}",
                    image_lock.display(),
                    named(&holders)
                )));
            };
            let volume = layout.linked(&checkout)?.ok_or_else(|| {
                CowshedError::not_found(
                    format!("workspace {workspace} links no build volume, so cowshed holds no Nx state for it"),
                    "cowshed doctor --json",
                )
            })?;
            let _claim = match host.claim_build_volume(layout, &volume).map_err(storage)? {
                BuildVolumeClaim::Claimed(claim) => claim,
                BuildVolumeClaim::Held { open } => {
                    let hold = layout.hold_path(&volume);
                    let holders = nx::holders(&hold)
                        .map_err(|error| io("name build hold holders", &hold, &error))?;
                    return Err(refused(format!(
                        "a job or land holds build volume {volume}: hold {} is held by {}; volume open by {}",
                        hold.display(),
                        named(&holders),
                        named(&open)
                    )));
                }
            };
            let mount = host.mount_build_volume(layout, &volume).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            let closing = |error: std::io::Error| io("close the checkout's Nx state", &mount, &error);
            if let Err(busy) = nx::close(&mount, &state).map_err(closing)? {
                return Err(refused(format!("Nx is running in it: {busy}")));
            }
            let open_locks: BTreeSet<PathBuf> = nx::task_databases(&mount, &state)
                .map_err(closing)?
                .into_iter()
                .map(|database| database.with_extension("lock"))
                .collect();
            let _opens = match nx::hold_opens(&mount, &state, &mount).map_err(closing)? {
                Ok(opens) => opens,
                Err(opening) => {
                    return Err(refused(format!(
                        "Nx is opening {}: {}",
                        opening.database.display(),
                        named(&opening.holders)
                    )));
                }
            };
            if let Err(busy) = nx::held(&mount, &state).map_err(closing)? {
                return Err(refused(format!("Nx is running in it: {busy}")));
            }
            let mut cache_entries = 0;
            for cache in state.nx_cache() {
                cache_entries += empty(&mount.join(cache), &open_locks)?;
            }
            let mut workspace_data_entries = 0;
            for data in state.nx_workspace_data() {
                workspace_data_entries += empty(&mount.join(data), &open_locks)?;
            }
            Ok(NxStateClearResult {
                workspace,
                volume: volume.to_string(),
                cache_entries,
                workspace_data_entries,
            })
        })
        .await
    }
}

/// Delete every entry directly in `directory` but `keep`, each with whatever it contains, and
/// count them. The directory itself stays: it is what the checkout's build link names. One that
/// does not exist holds nothing.
fn empty(directory: &Path, keep: &BTreeSet<PathBuf>) -> Result<u64> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(io("list Nx state", directory, &error)),
    };
    let mut deleted = 0;
    for entry in entries {
        let entry = entry.map_err(|error| io("list Nx state", directory, &error))?;
        let path = entry.path();
        if keep.contains(&path) {
            continue;
        }
        let kind = entry
            .file_type()
            .map_err(|error| io("inspect Nx state", &path, &error))?;
        if kind.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        }
        .map_err(|error| io("delete Nx state", &path, &error))?;
        deleted += 1;
    }
    Ok(deleted)
}
