//! A workspace's live build volume replaced by what a fork of its target is given now
//! (16_build_volumes.md, "Reseed"): Fork steps 3 and 4 make the new volume, seed compaction's
//! move swaps it in, and the old one goes through the ordinary release.

use std::path::PathBuf;

use super::{BuildVolumes, Harmonized, Owner, ReplacedWorkspace, RuntimeStorageHost, SourcePaths};
use crate::build_volume::BuildVolumeId;
use crate::{CowshedError, Result};

/// What a reseed of `workspace` reads: its target, where the target's checkout and image lock are,
/// and the workspace's paths the mtime pass compares against the target's checkout.
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
    pub async fn reseed_workspace(&self, plan: ReseedPlan) -> Result<Reseeded> {
        Err(CowshedError::internal(format!(
            "reseeding {} is not implemented",
            plan.workspace.owner.name
        )))
    }
}
