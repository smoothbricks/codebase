//! Explicit workspace cache replacement, not a target seed refresh (16_build_volumes.md,
//! "Workspace reseed"). Preview reads only; execution rechecks the exact identities under the
//! existing lifecycle/image locks and the old volume's exclusive job-admission claim.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{BuildVolumes, Owner, RuntimeStorageHost, io, storage};
use crate::api::dto::ReseedResult;
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, BuildVolumeState, link,
    nx,
};
use crate::storage::ImagePaths;
use crate::storage::apfs::native::BuildVolumeClaim;
use crate::storage::recovery::{LIFECYCLE_INTENTS_FILE, LifecycleIntentJournal};
use crate::{CowshedError, Result};
use cowshed_apfs::store_lock::LockMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReseedWorkspace {
    pub owner: Owner,
    pub checkout: PathBuf,
    pub image: ImagePaths,
    pub intent_lease: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ImageObservation {
    device: u64,
    inode: u64,
    modified: SystemTime,
    bytes: u64,
}

/// An immutable observation, never authority to delete an image after its identity changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReseedPlan {
    workspace: ReseedWorkspace,
    from: ReseedWorkspace,
    source_identity: (u64, u64),
    target_identity: (u64, u64),
    previous: BuildVolumeId,
    previous_record: BuildVolumeRecord,
    previous_image: ImageObservation,
    seed: BuildVolumeId,
    seed_record: BuildVolumeRecord,
    seed_image: ImageObservation,
}

impl ReseedPlan {
    pub fn preview(&self) -> ReseedResult {
        ReseedResult {
            workspace: self.workspace.owner.name.clone(),
            from: self.from.owner.name.clone(),
            previous_volume: self.previous.to_string(),
            seed: self.seed.to_string(),
            replacement_volume: None,
            previous_bytes: self.previous_image.bytes,
            replacement_bytes: self.seed_image.bytes,
            freed_bytes: 0,
            dry_run: true,
        }
    }
}

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    pub async fn preview_reseed(
        &self,
        workspace: ReseedWorkspace,
        from: ReseedWorkspace,
    ) -> Result<ReseedPlan> {
        self.blocking(move |_, layout| observe(layout, workspace, from))
            .await
    }

    pub async fn execute_reseed(&self, plan: ReseedPlan) -> Result<ReseedResult> {
        self.blocking(move |host, layout| {
            let mut locks = vec![
                plan.workspace.image.lock().to_owned(),
                plan.from.image.lock().to_owned(),
                plan.workspace.intent_lease.clone(),
                plan.from.intent_lease.clone(),
            ];
            locks.sort();
            locks.dedup();
            let Some(_locked) = host.lock_images(&locks, LockMode::Try).map_err(storage)? else {
                let mut holders = Vec::new();
                for lock in &locks {
                    let open = nx::holders(lock).map_err(|error| io("name reseed lock holders", lock, &error))?;
                    if !open.is_empty() {
                        holders.push(format!("{} held by {}", lock.display(), named(&open)));
                    }
                }
                return Err(CowshedError::conflict(
                    format!("cannot reseed {}: lifecycle/image lock {}", plan.workspace.owner.name,
                        if holders.is_empty() { "held by a process this user cannot see".to_owned() } else { holders.join("; ") }),
                    "let the named lifecycle operation finish, then retry the reseed",
                ));
            };
            require_current(layout, &plan)?;
            let claim = match host.claim_build_volume(layout, &plan.previous).map_err(storage)? {
                BuildVolumeClaim::Claimed(claim) => claim,
                BuildVolumeClaim::Held { open } => {
                    let hold = layout.hold_path(&plan.previous);
                    let holders = nx::holders(&hold).map_err(|error| io("name build hold holders", &hold, &error))?;
                    return Err(CowshedError::conflict(
                        format!("cannot reseed {}: build volume {} hold {} is held by {}; volume open by {}",
                            plan.workspace.owner.name, plan.previous, hold.display(), named(&holders), named(&open)),
                        "let the named job or land finish, then retry; no jobs were stopped",
                    ));
                }
            };
            require_current(layout, &plan)?;
            let old_mount = host.mount_build_volume(layout, &plan.previous).map_err(storage)?;
            let state = BuildVolumeState::read(&old_mount)?;
            if let Err(busy) = nx::close(&old_mount, &state)
                .map_err(|error| io("close the reseeded volume's Nx state", &old_mount, &error))?
            {
                return Err(CowshedError::conflict(
                    format!("cannot reseed {}: build volume {} is held: {busy}", plan.workspace.owner.name, plan.previous),
                    "stop the named holder, then retry; no jobs were stopped",
                ));
            }
            let replacement = BuildVolumeId::mint();
            let creating = layout.hold_new(&replacement)
                .map_err(|error| io("hold the reseed replacement", &layout.hold_path(&replacement), &error))?;
            let prepared: Result<(u64, u64)> = (|| {
                host.clone_build_volume(layout, &plan.seed, &replacement, &BuildVolumeRecord::new(
                    plan.seed_record.tree.clone(),
                    BuildVolumeRole::Linked { checkout: plan.workspace.owner.name.clone() },
                )).map_err(storage)?;
                let mount = host.mount_build_volume(layout, &replacement).map_err(storage)?;
                let state = BuildVolumeState::read(&mount)?;
                nx::discard_daemon_records(&mount, &state)
                    .map_err(|error| io("discard the seed's Nx daemon record", &mount, &error))?;
                host.detach_claimed_build_volume(layout, &claim).map_err(|error| {
                    if matches!(&error, crate::storage::apfs::ApfsStorageError::BuildVolumeHeld { .. }) {
                        CowshedError::conflict(
                            format!("cannot reseed {}: old build volume {} refused its non-forced seal: {error}", plan.workspace.owner.name, plan.previous),
                            "stop the named holder, then retry; the old build link is unchanged",
                        )
                    } else {
                        storage(error)
                    }
                })?;
                // The seal may flush pending image blocks: execution accounts the allocation
                // actually deleted, not the earlier preview's sample.
                let previous_bytes = allocated_image_bytes(&layout.image(&plan.previous))?;
                let replacement_bytes = allocated_image_bytes(&layout.image(&replacement))?;
                link::point(&plan.workspace.checkout, &mount)?;
                Ok((previous_bytes, replacement_bytes))
            })();
            let (previous_bytes, replacement_bytes) = match prepared {
                Ok(bytes) => bytes,
                Err(error) => {
                    // Nothing published: the original link still names the original image.
                    if let Err(restore) = host.mount_build_volume(layout, &plan.previous) {
                        eprintln!("cowshed: restoring {}'s original build volume {} after reseed refusal failed: {restore}", plan.workspace.owner.name, plan.previous);
                    }
                    drop(creating);
                    match host.release_build_volume(layout, &replacement) {
                        Ok(release) => super::say_release(&replacement, "unpublished reseed replacement", &release),
                        Err(cleanup) => eprintln!("cowshed: releasing unpublished reseed replacement {replacement} failed: {cleanup}"),
                    }
                    return Err(error);
                }
            };
            // The replacement record preceded the atomic link, so an interruption on either
            // side leaves one owned link and at most an unreachable image for ordinary GC.
            layout.write_record(&plan.previous, &BuildVolumeRecord {
                role: BuildVolumeRole::Unlinked,
                ..plan.previous_record.clone()
            })?;
            host.release_claimed(layout, claim).map_err(storage)?;
            drop(creating);
            Ok(ReseedResult {
                replacement_volume: Some(replacement.to_string()),
                previous_bytes,
                replacement_bytes,
                freed_bytes: previous_bytes,
                dry_run: false,
                ..plan.preview()
            })
        }).await
    }
}

fn require_current(layout: &BuildVolumeLayout, plan: &ReseedPlan) -> Result<()> {
    let current = observe(layout, plan.workspace.clone(), plan.from.clone())?;
    if &current != plan {
        return Err(CowshedError::conflict(
            format!(
                "reseed plan for {} from {} is stale: the workspace, build link, image allocation/identity or latest seed changed",
                plan.workspace.owner.name, plan.from.owner.name
            ),
            "run cowshed reseed again to preview the current images; nothing was replaced",
        ));
    }
    Ok(())
}

fn observe(
    layout: &BuildVolumeLayout,
    workspace: ReseedWorkspace,
    from: ReseedWorkspace,
) -> Result<ReseedPlan> {
    let journal_path = layout.project_root().join(LIFECYCLE_INTENTS_FILE);
    let journal = LifecycleIntentJournal::load(&journal_path)?;
    for name in journal.forming() {
        if name == &workspace.owner.name || name == &from.owner.name {
            let record = journal
                .get(name)
                .expect("forming entries are journal entries");
            return Err(CowshedError::conflict(
                format!(
                    "cannot reseed {} from {}: unfinished lifecycle journal entry for {name} at {}: {:?}",
                    workspace.owner.name,
                    from.owner.name,
                    journal_path.display(),
                    record.operation
                ),
                "finish the named create/fork/adopt before reseeding; no lifecycle work was replayed",
            ));
        }
    }
    let previous = layout.linked(&workspace.checkout)?.ok_or_else(|| {
        CowshedError::not_found(
            format!("workspace {} links no build volume", workspace.owner.name),
            "there is no accumulated build volume to reseed",
        )
    })?;
    let previous_record = layout.read_record(&previous)?;
    if !matches!(&previous_record.role, BuildVolumeRole::Linked { checkout } if checkout == &workspace.owner.name)
    {
        return Err(CowshedError::integrity(
            format!(
                "workspace {} does not own linked build volume {previous}",
                workspace.owner.name
            ),
            "cowshed doctor --json",
        ));
    }
    let (seed, seed_record) = layout.seed_of(&from.owner.name, &from.owner.incarnation)?
        .ok_or_else(|| CowshedError::not_found(
            format!("target {} at incarnation {} has no current seed to reseed {} from", from.owner.name, from.owner.incarnation.as_str(), workspace.owner.name),
            "a normal fork or land creates the target's seed; reseed never refreshes or creates a seed",
        ))?;
    Ok(ReseedPlan {
        source_identity: identity(workspace.image.image())?,
        target_identity: identity(from.image.image())?,
        previous_image: image_observation(&layout.image(&previous))?,
        seed_image: image_observation(&layout.image(&seed))?,
        workspace,
        from,
        previous,
        previous_record,
        seed,
        seed_record,
    })
}

fn identity(path: &Path) -> Result<(u64, u64)> {
    let metadata =
        fs::metadata(path).map_err(|error| io("read workspace image identity", path, &error))?;
    Ok((metadata.dev(), metadata.ino()))
}

fn image_observation(path: &Path) -> Result<ImageObservation> {
    let metadata =
        fs::metadata(path).map_err(|error| io("observe a reseed image", path, &error))?;
    Ok(ImageObservation {
        device: metadata.dev(),
        inode: metadata.ino(),
        modified: metadata
            .modified()
            .map_err(|error| io("read reseed image clock", path, &error))?,
        bytes: allocated_image_bytes(path)?,
    })
}

/// The same allocation basis as build-volume GC; shared APFS extents are not unique bytes.
fn allocated_image_bytes(path: &Path) -> Result<u64> {
    let metadata =
        fs::metadata(path).map_err(|error| io("measure reseed image allocation", path, &error))?;
    if metadata.is_dir() {
        crate::storage::directory::allocated_tree_bytes(path)
            .map_err(|error| io("measure reseed volume allocation", path, &error))
    } else {
        Ok(metadata.blocks().saturating_mul(512))
    }
}

fn named(holders: &[cowshed_os::process::Holder]) -> String {
    if holders.is_empty() {
        "no visible process (the kernel or another user may hold it)".to_owned()
    } else {
        holders
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}
