//! The volume this host's substrate stats, an APFS volume fenced by its mount's filesystem id,
//! and the stat that reads it.

use std::path::Path;

use super::VolumeAtSpawn;

pub use crate::storage::apfs::native::{
    ApfsVolume as HostVolume, ApfsVolumeStat as HostVolumeStat,
};

/// The volume mounted at `mount` as a job's spawn finds it: its identity and used bytes, or why
/// it has neither.
pub fn at_spawn(mount: &Path) -> VolumeAtSpawn<HostVolume> {
    VolumeAtSpawn::observed(HostVolume::capture(mount))
}
