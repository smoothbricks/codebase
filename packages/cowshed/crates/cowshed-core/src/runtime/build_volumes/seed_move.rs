//! Move a workspace onto its own compacted seed without losing newer writes
//! (16_build_volumes.md, "Seed compaction"). Execution rechecks the observed identities under
//! its lifecycle/image locks and the old volume's exclusive job-admission claim.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{BuildVolumes, Owner, RuntimeStorageHost, io, storage};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, BuildVolumeState,
    Registrant, Registration, link, nx,
};
use crate::storage::ImagePaths;
use crate::storage::apfs::native::BuildVolumeClaim;
use crate::storage::recovery::{LIFECYCLE_INTENTS_FILE, LifecycleIntentJournal};
use crate::{CowshedError, Result};
use cowshed_apfs::store_lock::LockMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SeedMoveWorkspace {
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
}

/// An immutable observation, never authority to delete an image after its identity changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SeedMovePlan {
    workspace: SeedMoveWorkspace,
    source_identity: (u64, u64),
    previous: BuildVolumeId,
    previous_record: BuildVolumeRecord,
    previous_image: ImageObservation,
    seed: BuildVolumeId,
    seed_record: BuildVolumeRecord,
    seed_image: ImageObservation,
}

impl SeedMovePlan {
    /// The seed the workspace is given a clone of.
    pub fn seed(&self) -> &BuildVolumeId {
        &self.seed
    }
}

/// The live volume and its guaranteed replacement after a successful compaction move.
#[derive(Debug)]
pub(crate) struct MovedVolume {
    pub previous_volume: String,
    pub replacement_volume: String,
}

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    pub async fn plan_move_onto_seed(&self, workspace: SeedMoveWorkspace) -> Result<SeedMovePlan> {
        self.blocking(move |_, layout| observe(layout, workspace))
            .await
    }

    /// Seed compaction's lineage (16_build_volumes.md, "Seed compaction"): move a workspace
    /// onto its own seed only when its live volume holds no write the seed lacks.
    pub async fn move_onto_seed(&self, plan: SeedMovePlan) -> Result<MovedVolume> {
        self.move_onto_seed_with(plan, || {}).await
    }

    /// [`Self::move_onto_seed`], running `sealing` once the replacement is cloned and mounted
    /// and before the old volume is sealed: where a host write races the move.
    #[cfg(test)]
    pub async fn move_onto_seed_racing(
        &self,
        plan: SeedMovePlan,
        sealing: impl FnOnce() + Send + 'static,
    ) -> Result<MovedVolume> {
        self.move_onto_seed_with(plan, sealing).await
    }

    async fn move_onto_seed_with(
        &self,
        plan: SeedMovePlan,
        sealing: impl FnOnce() + Send + 'static,
    ) -> Result<MovedVolume> {
        self.blocking(move |host, layout| {
            let locks = [
                plan.workspace.image.lock().to_owned(),
                plan.workspace.intent_lease.clone(),
            ];
            let holding = crate::api::held_lock::LockHolding::of(
                crate::api::held_lock::LockOperation::MoveOntoSeed,
                &plan.workspace.owner.name,
            );
            let Some(_locked) = host
                .lock_images(&locks, LockMode::Try, &holding)
                .map_err(storage)?
            else {
                let mut holders = Vec::new();
                for lock in &locks {
                    let open = nx::holders(host.census(), lock).map_err(|error| io("name seed move lock holders", lock, &error))?;
                    if !open.is_empty() {
                        holders.push(format!("{} held by {}", lock.display(), named(&open)));
                    }
                }
                return Err(CowshedError::conflict(
                    format!("cannot move {} onto its seed: lifecycle/image lock {}", plan.workspace.owner.name,
                        if holders.is_empty() { "held by a process this user cannot see".to_owned() } else { holders.join("; ") }),
                    "let the named lifecycle operation finish, then retry the seed move",
                ));
            };
            require_current(layout, &plan)?;
            let claim = match host.claim_build_volume(layout, &plan.previous).map_err(storage)? {
                BuildVolumeClaim::Claimed(claim) => claim,
                BuildVolumeClaim::Held { open } => {
                    let hold = layout.hold_path(&plan.previous);
                    let holders = nx::holders(host.census(), &hold).map_err(|error| io("name build hold holders", &hold, &error))?;
                    return Err(CowshedError::conflict(
                        format!("cannot move {} onto its seed: build volume {} hold {} is held by {}; volume open by {}",
                            plan.workspace.owner.name, plan.previous, hold.display(), named(&holders), named(&open)),
                        "let the named job or land finish, then retry; no jobs were stopped",
                    ));
                }
            };
            require_current(layout, &plan)?;
            let (old_mount, _) = host.mount_build_volume(layout, &plan.previous).map_err(storage)?;
            let state = BuildVolumeState::read(&old_mount)?;
            if let Err(busy) = nx::close(host.census(), &old_mount, &state)
                .map_err(|error| io("close the moving volume's Nx state", &old_mount, &error))?
            {
                return Err(CowshedError::conflict(
                    format!("cannot move {} onto its seed: build volume {} is held: {busy}", plan.workspace.owner.name, plan.previous),
                    "stop the named holder, then retry; no jobs were stopped",
                ));
            }
            require_no_newer_write(host, layout, &plan)?;
            let replacement = BuildVolumeId::mint();
            let creating = layout.hold_new(&replacement)
                .map_err(|error| io("hold the seed move replacement", &layout.hold_path(&replacement), &error))?;
            let prepared: Result<()> = (|| {
                host.clone_build_volume(layout, &plan.seed, &replacement, &Registration::new(
                    BuildVolumeRecord::new(
                        plan.seed_record.tree.clone(),
                        BuildVolumeRole::Linked { checkout: plan.workspace.owner.name.clone() },
                    ),
                    Registrant::Workspace { incarnation: plan.workspace.owner.incarnation.clone() },
                )).map_err(storage)?;
                let (mount, _) = host.mount_build_volume(layout, &replacement).map_err(storage)?;
                let state = BuildVolumeState::read(&mount)?;
                nx::discard_daemon_records(&mount, &state)
                    .map_err(|error| io("discard the seed's Nx daemon record", &mount, &error))?;
                sealing();
                require_no_newer_write(host, layout, &plan)?;
                host.detach_claimed_build_volume(layout, &claim).map_err(|error| {
                    if matches!(&error, crate::storage::apfs::ApfsStorageError::BuildVolumeHeld { .. }) {
                        CowshedError::conflict(
                            format!("cannot move {} onto its seed: old build volume {} refused its non-forced seal: {error}", plan.workspace.owner.name, plan.previous),
                            "stop the named holder, then retry; the old build link is unchanged",
                        )
                    } else {
                        storage(error)
                    }
                })?;
                link::point(&plan.workspace.checkout, &mount)?;
                Ok(())
            })();
            match prepared {
                Ok(()) => {}
                Err(error) => {
                    // Nothing published: the original link still names the original image.
                    if let Err(restore) = host.mount_build_volume(layout, &plan.previous) {
                        cowshed_os::eprintln!("cowshed: restoring {}'s original build volume {} after seed move refusal failed: {restore}", plan.workspace.owner.name, plan.previous);
                    }
                    drop(creating);
                    match host.release_build_volume(layout, &replacement) {
                        Ok(release) => super::say_release(&replacement, "unpublished seed move replacement", &release),
                        Err(cleanup) => cowshed_os::eprintln!("cowshed: releasing unpublished seed move replacement {replacement} failed: {cleanup}"),
                    }
                    return Err(error);
                }
            };
            // The replacement record preceded the atomic link, so an interruption on either
            // side leaves one owned link and the other volume still recorded as the workspace's.
            // The previous volume stays the workspace's, at its incarnation, until this release
            // deletes it: a release that fails is the move's error, and the record still names
            // its owner.
            layout.write_record(&plan.previous, &BuildVolumeRecord {
                role: BuildVolumeRole::Superseded {
                    owner: plan.workspace.owner.name.clone(),
                    incarnation: plan.workspace.owner.incarnation.clone(),
                },
                ..plan.previous_record.clone()
            })?;
            host.release_claimed(layout, claim)
                .map_err(storage)?;
            drop(creating);
            Ok(MovedVolume {
                previous_volume: plan.previous.to_string(),
                replacement_volume: replacement.to_string(),
            })
        }).await
    }
}

fn require_current(layout: &BuildVolumeLayout, plan: &SeedMovePlan) -> Result<()> {
    let current = observe(layout, plan.workspace.clone())?;
    if &current != plan {
        return Err(CowshedError::conflict(
            format!(
                "seed move plan for {} is stale: the workspace, build link, image identity or latest seed changed",
                plan.workspace.owner.name
            ),
            "retry compaction against the current images; nothing was replaced",
        ));
    }
    Ok(())
}

/// Refuse a write after the seed's mark before cloning and again immediately before sealing.
/// The exclusive claim fences jobs and Nx restarts, but not a host writer during the clone;
/// checking after the seal would mistake the unmount's own writes for newer build state.
/// (16_build_volumes.md, "Seed compaction").
fn require_no_newer_write(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    plan: &SeedMovePlan,
) -> Result<()> {
    let live = host
        .build_volume_written(layout, &plan.previous)
        .map_err(storage)?;
    let mark = host
        .build_volume_written(layout, &plan.seed)
        .map_err(storage)?;
    match live.duration_since(mark) {
        Ok(behind) if !behind.is_zero() => Err(CowshedError::conflict(
            format!(
                "cannot move {} onto seed {}: its build volume {} was written {} ms after the seed's mark, and the move would discard that write",
                plan.workspace.owner.name,
                plan.seed,
                plan.previous,
                behind.as_millis()
            ),
            "the next fork's reseed refreezes the seed from that volume; nothing was replaced",
        )),
        _ => Ok(()),
    }
}

fn observe(layout: &BuildVolumeLayout, workspace: SeedMoveWorkspace) -> Result<SeedMovePlan> {
    let journal_path = layout.project_root().join(LIFECYCLE_INTENTS_FILE);
    let journal = LifecycleIntentJournal::load(&journal_path)?;
    for name in journal.forming() {
        if name == &workspace.owner.name {
            let record = journal
                .get(name)
                .expect("forming entries are journal entries");
            return Err(CowshedError::conflict(
                format!(
                    "cannot move {} onto its seed: unfinished lifecycle journal entry for {name} at {}: {:?}",
                    workspace.owner.name,
                    journal_path.display(),
                    record.operation
                ),
                "finish the named create/fork/adopt before moving onto the seed; no lifecycle work was replayed",
            ));
        }
    }
    let previous = layout.linked(&workspace.checkout)?.ok_or_else(|| {
        CowshedError::not_found(
            format!("workspace {} links no build volume", workspace.owner.name),
            "there is no live build volume to move onto the seed",
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
    let (seed, seed_record) = layout
        .seed_of(&workspace.owner.name, &workspace.owner.incarnation)?
        .ok_or_else(|| {
            CowshedError::not_found(
                format!(
                    "workspace {} at incarnation {} has no current seed",
                    workspace.owner.name,
                    workspace.owner.incarnation.as_str()
                ),
                "a normal fork or land creates the seed; compaction never creates a fallback",
            )
        })?;
    Ok(SeedMovePlan {
        source_identity: identity(workspace.image.image())?,
        previous_image: image_observation(&layout.image(&previous))?,
        seed_image: image_observation(&layout.image(&seed))?,
        workspace,
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
        fs::metadata(path).map_err(|error| io("observe a seed move image", path, &error))?;
    Ok(ImageObservation {
        device: metadata.dev(),
        inode: metadata.ino(),
        modified: metadata
            .modified()
            .map_err(|error| io("read seed move image clock", path, &error))?,
    })
}

pub(super) fn named(holders: &[cowshed_os::process::Holder]) -> String {
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
