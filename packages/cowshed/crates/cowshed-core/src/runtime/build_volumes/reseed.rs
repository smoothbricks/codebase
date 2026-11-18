//! A workspace's live build volume replaced by what a fork of its target is given now
//! (16_build_volumes.md, "Reseed"): Fork steps 3 and 4 make the new volume, seed compaction's
//! move swaps it in, and the old one goes through the ordinary release.
//!
//! Not the fork's reseed (Fork step 2), which refreezes a target's seed from the target's live
//! volume: this one leaves every seed of the target as it is and replaces a workspace's live
//! volume. The target's seed is cloned as it stands, so nothing of the target is stopped.

use std::path::{Path, PathBuf};

use super::seed_move::{lock_workspace, named};
use super::{
    BuildVolumes, CapturedFork, Destination, Harmonized, Owner, PreparedFork, ReplacedWorkspace,
    RuntimeStorageHost, SourceDaemon, SourcePaths, allocated_image_bytes, capture_live,
    capture_seed, claim_superseded, head_tree, io, ready, retire_seeds, say_release, seeds_of,
    source_mtimes, storage,
};
use crate::api::held_lock::{LockHolding, LockOperation};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, BuildVolumeState,
    Registrant, cargo, link, nx,
};
use crate::storage::apfs::ApfsStorageError;
use crate::storage::apfs::native::{BuildVolumeClaim, BuildVolumeRelease};
use crate::{CowshedError, Result};
use cowshed_apfs::store_lock::LockMode;

/// The timing scope of a reseed's steps: not `reseed`, which names the fork's seed refresh.
const SCOPE: &str = "workspace-reseed";

/// What a reseed of `workspace` reads: its target (the workspace it was forked from and lands
/// into), where the target's checkout and image lock are, and the workspace's paths the mtime pass
/// compares against the target's checkout.
#[derive(Debug)]
pub(crate) struct ReseedPlan {
    pub workspace: ReplacedWorkspace,
    pub target: Owner,
    pub target_checkout: PathBuf,
    pub target_lock: PathBuf,
    pub paths: SourcePaths,
}

/// What a reseed did: the volume the new one is a clone of (the target's latest seed, or an mbx
/// target's live volume), the old volume and the bytes its image had allocated, the new volume,
/// and, without mbx, what the mtime pass changed.
#[derive(Debug)]
pub(crate) struct Reseeded {
    pub cloned_from: BuildVolumeId,
    pub previous: BuildVolumeId,
    pub previous_bytes: u64,
    pub volume: BuildVolumeId,
    pub source_mtimes: Option<Harmonized>,
}

/// Why main is never reseeded, and what to run instead (16_build_volumes.md, "Reseed").
pub(crate) fn main_reseed_refusal() -> CowshedError {
    CowshedError::usage(
        "main is not reseeded: it is every workspace's root target, forked from nothing, and only \
         seed compaction moves main's live volume onto its own compacted seed",
        "cowshed defrag main rewrites main's images contiguously",
    )
}

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    /// Replace the build volume `plan`'s workspace links with what a fork of its target is given
    /// now (16_build_volumes.md, "Reseed"), leaving its source image, branch, uncommitted files and
    /// grants as they are. Refused, naming the holder and changing nothing, while another lifecycle
    /// operation holds the workspace's image lock or intent lease, a job or land holds its volume,
    /// a process other than its own Nx daemon holds a task database, or a Cargo build holds a
    /// profile lock; the daemon is stopped, as `cache clear` stops it, and starts again on its
    /// next request. Main has no target and is refused by name.
    pub async fn reseed_workspace(&self, plan: ReseedPlan) -> Result<Reseeded> {
        self.blocking(move |host, layout| reseed(host, layout, plan))
            .await
    }
}

fn reseed<H: RuntimeStorageHost>(
    host: &H,
    layout: &BuildVolumeLayout,
    plan: ReseedPlan,
) -> Result<Reseeded> {
    let ReseedPlan {
        workspace,
        target,
        target_checkout,
        target_lock,
        paths,
    } = plan;
    let name = workspace.owner.name.clone();
    if name.is_main() {
        return Err(main_reseed_refusal());
    }
    if target.name == name {
        return Err(CowshedError::usage(
            format!("{name} cannot be reseeded from its own seed"),
            "a workspace is reseeded from the target it was forked from",
        ));
    }
    let refused = |what: String| {
        CowshedError::conflict(
            format!("cannot reseed {name}: {what}; nothing was replaced"),
            format!("let it finish, then run `cowshed reseed {name}` again"),
        )
    };
    let census = host.census();
    let _locked = match lock_workspace(host, &workspace, LockOperation::Reseed)? {
        Ok(locked) => locked,
        Err(held) => {
            return Err(refused(format!(
                "another lifecycle operation holds its lifecycle/image lock {held}"
            )));
        }
    };
    let previous = layout.linked(&workspace.checkout)?.ok_or_else(|| {
        CowshedError::not_found(
            format!("workspace {name} links no build volume, so it has none to reseed"),
            "cowshed doctor --json reports its build link",
        )
    })?;
    let previous_record = layout.read_record(&previous)?;
    if !matches!(&previous_record.role, BuildVolumeRole::Linked { checkout } if *checkout == name) {
        return Err(CowshedError::integrity(
            format!("workspace {name} does not own its linked build volume {previous}"),
            "cowshed doctor --json",
        ));
    }
    let claim = match host
        .claim_build_volume(layout, &previous)
        .map_err(storage)?
    {
        BuildVolumeClaim::Claimed(claim) => claim,
        BuildVolumeClaim::Held { open } => {
            let hold = layout.hold_path(&previous);
            let holders = nx::holders(census, &hold)
                .map_err(|error| io("name build hold holders", &hold, &error))?;
            return Err(refused(format!(
                "a job or land holds build volume {previous}: hold {} is held by {}; volume open by {}",
                hold.display(),
                named(&holders),
                named(&open)
            )));
        }
    };
    let (old_mount, _) = host
        .mount_build_volume(layout, &previous)
        .map_err(storage)?;
    let state = BuildVolumeState::read(&old_mount)?;
    if let Err(busy) = nx::close(census, &old_mount, &state)
        .map_err(|error| io("close the workspace's Nx state", &old_mount, &error))?
    {
        return Err(refused(format!(
            "Nx is running in build volume {previous}: {busy}"
        )));
    }
    // Cargo holds a profile's lock shared for its whole build: a running build is refused by name
    // here, before any clone, rather than by the seal. The locks are files of the old volume, so
    // they are let go before it is sealed.
    match cargo::hold(&old_mount, &state)
        .map_err(|error| io("take the workspace's Cargo build locks", &old_mount, &error))?
    {
        Ok(builds) => drop(builds),
        Err(lock) => {
            let holders = nx::holders(census, &lock)
                .map_err(|error| io("list the Cargo build's processes", &lock, &error))?;
            return Err(refused(format!(
                "a Cargo build holds {}: {}",
                lock.display(),
                named(&holders)
            )));
        }
    }
    let image = layout.image(&previous);
    let previous_bytes = allocated_image_bytes(&image)
        .map_err(|error| io("measure the old build image", &image, &error))?;
    let previous_seeds = seeds_of(layout, &workspace.owner)?;
    let captured = {
        let _span = cowshed_os::timing::span(SCOPE, "capture");
        capture_target(
            host,
            layout,
            &target,
            &target_checkout,
            &target_lock,
            &workspace,
        )?
    };
    // A seed's capture gives the workspace a seed of its own; an mbx target's live capture does not.
    let warm = captured.own_seed;
    let cloned_from = captured.from.clone();
    let prepared = {
        let _span = cowshed_os::timing::span(SCOPE, "ready");
        ready(
            host,
            layout,
            captured,
            workspace.owner.clone(),
            Registrant::Workspace {
                incarnation: workspace.owner.incarnation.clone(),
            },
        )?
    };
    // Collection releases a published volume nothing links: a job's hold keeps it off the new
    // volume until the checkout's link names it.
    let held = layout.admit(&prepared.live).map_err(|error| {
        io(
            "hold the reseeded build volume",
            &layout.hold_path(&prepared.live),
            &error,
        )
    })?;
    let swapped = (|| -> Result<Option<Harmonized>> {
        // Without mbx the new volume's Cargo units were built from the target's bytes and are
        // judged by mtime: the workspace's sources take the target's view of time before the link
        // names the volume (16_build_volumes.md, "Reseed", "Source and build state").
        let source_mtimes = if warm {
            let _span = cowshed_os::timing::span(SCOPE, "source-mtimes");
            Some(source_mtimes::harmonize(
                &target_checkout,
                &workspace.checkout,
                &paths.all_compared(),
            )?)
        } else {
            None
        };
        {
            let _span = cowshed_os::timing::span(SCOPE, "seal");
            host.detach_claimed_build_volume(layout, &claim)
                .map_err(|error| match error {
                    ApfsStorageError::BuildVolumeHeld { .. } => refused(format!(
                        "its build volume {previous} refused its non-forced seal: {error}"
                    )),
                    error => storage(error),
                })?;
        }
        link::point(&workspace.checkout, &prepared.mount)?;
        Ok(source_mtimes)
    })();
    let source_mtimes = match swapped {
        Ok(source_mtimes) => source_mtimes,
        Err(error) => {
            // Nothing is linked to the new volume: the checkout still names the old one.
            if let Err(restore) = host.mount_build_volume(layout, &previous) {
                cowshed_os::eprintln!(
                    "cowshed: remounting {name}'s build volume {previous} after its refused reseed failed: {restore}"
                );
            }
            drop(held);
            abandon(host, layout, &prepared);
            return Err(error);
        }
    };
    drop(held);
    // The old volume stays the workspace's, at its incarnation, until this release deletes it: a
    // release that fails is the reseed's error, and the record still names its owner.
    layout.write_record(
        &previous,
        &BuildVolumeRecord {
            role: BuildVolumeRole::Superseded {
                owner: name.clone(),
                incarnation: workspace.owner.incarnation.clone(),
            },
            ..previous_record
        },
    )?;
    {
        let _span = cowshed_os::timing::span(SCOPE, "release");
        let refused = host.release_claimed(layout, claim).map_err(storage)?;
        say_release(
            &previous,
            &format!("{name}'s previous"),
            &BuildVolumeRelease::Deleted { refused },
        );
    }
    // The workspace's own seed is its new volume's clone; the older ones are retired under its
    // image lock, which every fork of it holds while it clones its seed. Its own forks keep their
    // volumes, clones independent of any seed.
    if warm {
        let superseded = claim_superseded(host, layout, &workspace.owner, previous_seeds)?;
        retire_seeds(host, layout, superseded)?;
    }
    Ok(Reseeded {
        cloned_from,
        previous,
        previous_bytes,
        volume: prepared.live,
        source_mtimes,
    })
}

/// Fork step 3 for a reseed, under the target's image lock, waited for and said on stderr, so no
/// land or seed refresh of the target retires the seed being cloned: the target's latest seed, or
/// an mbx target's live volume under its jobs' shared hold, with Nx guards that leave the target's
/// daemon running (a running daemon is a holder, so the workspace starts without the target's Nx
/// state). Nothing holds a target's image lock and then waits on a workspace's lock or lease, so
/// the wait, under the workspace's own lock and claim, cannot deadlock.
fn capture_target<H: RuntimeStorageHost>(
    host: &H,
    layout: &BuildVolumeLayout,
    target: &Owner,
    target_checkout: &Path,
    target_lock: &Path,
    workspace: &ReplacedWorkspace,
) -> Result<CapturedFork> {
    let holding = LockHolding::of(LockOperation::Reseed, &workspace.owner.name);
    let locks = [target_lock.to_owned()];
    let _locked = match host
        .lock_images(&locks, LockMode::Try, &holding)
        .map_err(storage)?
    {
        Some(locked) => locked,
        None => {
            cowshed_os::eprintln!(
                "cowshed: another cowshed operation on {} holds its image lock (a fork, land or seed refresh of it); the reseed of {} clones from it once that ends",
                target.name,
                workspace.owner.name
            );
            host.lock_images(&locks, LockMode::Wait, &holding)
                .map_err(storage)?
                .ok_or_else(|| {
                    CowshedError::internal("a blocking image lock answered that it was held")
                })?
        }
    };
    let unlinked = || {
        CowshedError::not_found(
            format!(
                "{} links no published build volume, so it has nothing to reseed {} from",
                target.name, workspace.owner.name
            ),
            "cowshed doctor --json reports its build link",
        )
    };
    let live = layout.linked(target_checkout)?.ok_or_else(unlinked)?;
    if !layout.read_record(&live)?.is_published() {
        return Err(unlinked());
    }
    let mbx = BuildVolumeState::read(&layout.mount(&live))?.mbx.is_some();
    let destination = Destination {
        name: workspace.owner.name.clone(),
        registrant: Registrant::Workspace {
            incarnation: workspace.owner.incarnation.clone(),
        },
        placed: head_tree(&workspace.checkout).ok(),
    };
    if mbx {
        return capture_live(
            host,
            layout,
            target.clone(),
            live,
            destination,
            SourceDaemon::Keep,
        );
    }
    capture_seed(host, layout, target.clone(), destination, None)?.ok_or_else(|| {
        CowshedError::not_found(
            format!(
                "{} has no seed to reseed {} from",
                target.name, workspace.owner.name
            ),
            format!("a fork or land of {} makes its seed", target.name),
        )
    })
}

/// Release what [`ready`] made for a reseed whose swap was refused: no checkout links it and
/// nothing ran in it. The refusal is the reseed's error, so a release that fails too is said on
/// stderr and left to `gc`.
fn abandon(host: &impl RuntimeStorageHost, layout: &BuildVolumeLayout, prepared: &PreparedFork) {
    for (id, whose) in std::iter::once((&prepared.live, "a refused reseed")).chain(
        prepared
            .own_seed
            .iter()
            .map(|seed| (seed, "the seed of a refused reseed")),
    ) {
        match host.release_build_volume(layout, id) {
            Ok(release) => say_release(id, whose, &release),
            Err(error) => cowshed_os::eprintln!(
                "cowshed: build volume {id} of {whose} stays: {error}; `cowshed gc` retries it"
            ),
        }
    }
}
