//! A job's volume usage (07_api.md "Job monitoring"): each owned volume's used bytes, read at
//! spawn and again at every sample through its substrate's volume stat, never a directory scan
//! or a container's free space. The baseline keeps the volume's identity and its spawn reading;
//! every sample compares the volume's reading now against that same baseline.
//!
//! A volume that cannot be read at spawn or at a sample is that volume's typed unavailability in
//! the sample, never a zero delta and never the failure of the whole sample.

use crate::api::resources::{
    JobVolumeUsage, ResourceUnitError, VolumeUnavailable, VolumeUsage, VolumeUsedBytesDelta,
};
use crate::error::CowshedError;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum VolumeStatError {
    #[error("volume used-byte stat failed: {0}")]
    Read(#[from] CowshedError),
    #[error(transparent)]
    Unit(#[from] ResourceUnitError),
}

/// A substrate's volume stat: the bytes one owned volume itself uses. Neither this boundary nor
/// its caller substitutes a path walk, a shared store's free space or a fabricated value.
pub trait VolumeStat {
    type Volume;

    fn used_bytes(&self, volume: &Self::Volume) -> Result<u64, VolumeStatError>;
}

/// One volume as its spawn found it: the volume and its used bytes then, or why neither is
/// known.
#[derive(Clone, Debug, PartialEq)]
pub enum VolumeAtSpawn<V> {
    Read { volume: V, used: u64 },
    Unavailable(VolumeUnavailable),
}

impl<V> VolumeAtSpawn<V> {
    /// The baseline a spawn stat answered: its volume and used bytes, or why it has none.
    pub fn observed(stat: Result<(V, u64), VolumeStatError>) -> Self {
        match stat {
            Ok((volume, used)) => Self::Read { volume, used },
            Err(error) => Self::Unavailable(failed(&error)),
        }
    }

    /// The volume's usage at a sample whose stat of it answered `current`: the signed change
    /// from the spawn's reading, never clamped. A failed read, or a baseline that never was,
    /// stays the volume's typed unavailability.
    pub fn against(&self, current: impl FnOnce(&V) -> Result<u64, VolumeStatError>) -> VolumeUsage {
        match self {
            Self::Unavailable(reason) => VolumeUsage::Unavailable {
                reason: reason.clone(),
            },
            Self::Read { volume, used } => match current(volume)
                .and_then(|now| Ok(VolumeUsedBytesDelta::between(*used, now)?))
            {
                Ok(delta_bytes) => VolumeUsage::Read { delta_bytes },
                Err(error) => VolumeUsage::Unavailable {
                    reason: failed(&error),
                },
            },
        }
    }
}

fn failed(error: &VolumeStatError) -> VolumeUnavailable {
    VolumeUnavailable::Failed {
        message: error.to_string(),
    }
}

/// A job's volumes as its spawn found them: its workspace volume, and its build volume when the
/// job runs with one.
#[derive(Clone, Debug, PartialEq)]
pub struct VolumeBaseline<V> {
    pub workspace: VolumeAtSpawn<V>,
    pub build: Option<VolumeAtSpawn<V>>,
}

impl<V> VolumeBaseline<V> {
    /// Each volume's usage at a sample, read through `reader` now.
    pub fn sample(&self, reader: &impl VolumeStat<Volume = V>) -> JobVolumeUsage {
        self.against(|volume| reader.used_bytes(volume))
    }

    /// Each volume's usage at a sample whose stat of a volume answers `current`.
    fn against(&self, current: impl Fn(&V) -> Result<u64, VolumeStatError>) -> JobVolumeUsage {
        JobVolumeUsage {
            workspace: self.workspace.against(&current),
            build: self.build.as_ref().map(|build| build.against(&current)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;

    /// The real pure transform over typed snapshots: no substrate reads anything here. A volume
    /// is named by the string a test gives it.
    fn at_spawn(used: u64) -> VolumeAtSpawn<&'static str> {
        VolumeAtSpawn::observed(Ok(("volume", used)))
    }

    fn now(used: u64) -> impl FnOnce(&&'static str) -> Result<u64, VolumeStatError> {
        move |_| Ok(used)
    }

    fn read(delta: i64) -> VolumeUsage {
        VolumeUsage::Read {
            delta_bytes: VolumeUsedBytesDelta::new(delta).expect("exact"),
        }
    }

    fn job_volumes(workspace: u64, build: u64) -> VolumeBaseline<&'static str> {
        VolumeBaseline {
            workspace: VolumeAtSpawn::observed(Ok(("workspace", workspace))),
            build: Some(VolumeAtSpawn::observed(Ok(("build", build)))),
        }
    }

    /// Each volume's reading now, by the name its baseline holds.
    fn reading(
        workspace: u64,
        build: u64,
    ) -> impl Fn(&&'static str) -> Result<u64, VolumeStatError> {
        move |volume| match *volume {
            "workspace" => Ok(workspace),
            "build" => Ok(build),
            other => unreachable!("no volume {other} was captured"),
        }
    }

    #[test]
    fn deltas_are_signed_against_the_spawn_baseline_and_never_clamped() {
        let baseline = job_volumes(100, 50);
        assert_eq!(
            baseline.against(reading(40, 80)),
            JobVolumeUsage {
                workspace: read(-60),
                build: Some(read(30)),
            }
        );
        assert_eq!(
            baseline.against(reading(150, 30)),
            JobVolumeUsage {
                workspace: read(50),
                build: Some(read(-20)),
            },
            "every sample compares against the same spawn reading"
        );
    }

    #[test]
    fn one_volume_that_cannot_be_read_leaves_the_other_read() {
        let failure: VolumeStatError =
            CowshedError::environment_missing("the build volume left", "cowshed doctor --json")
                .into();
        assert_eq!(
            job_volumes(100, 50).against(|volume| match *volume {
                "workspace" => Ok(40),
                _ => Err(failure.clone()),
            }),
            JobVolumeUsage {
                workspace: read(-60),
                build: Some(VolumeUsage::Unavailable {
                    reason: VolumeUnavailable::Failed {
                        message: failure.to_string(),
                    },
                }),
            }
        );
    }

    #[test]
    fn a_failed_read_stays_its_typed_error_never_a_zero_delta() {
        let failure: VolumeStatError = CowshedError::environment_missing(
            "volume usage read failed: /mnt/lane no longer holds its volume",
            "cowshed doctor --json",
        )
        .into();
        let expected = VolumeUsage::Unavailable {
            reason: VolumeUnavailable::Failed {
                message: failure.to_string(),
            },
        };
        assert!(matches!(
            &failure,
            VolumeStatError::Read(source) if source.code == ErrorCode::EnvironmentMissing
        ));
        let spawn_failure = failure.clone();
        assert_eq!(
            VolumeAtSpawn::<&str>::observed(Err(spawn_failure)).against(|_| {
                unreachable!("a volume with no spawn reading is never read again")
            }),
            expected,
            "a spawn stat that failed"
        );
        assert_eq!(
            at_spawn(100).against(move |_| Err(failure)),
            expected,
            "a sample stat that failed"
        );
    }

    #[test]
    fn a_delta_no_projection_holds_exactly_is_unavailable_not_clamped() {
        let VolumeUsage::Unavailable {
            reason: VolumeUnavailable::Failed { message },
        } = at_spawn(0).against(now(u64::MAX))
        else {
            panic!("an inexact delta is no reading");
        };
        assert!(message.contains("volumeUsedBytesDelta"), "{message}");
    }

    #[test]
    fn an_unavailable_spawn_volume_keeps_its_reason() {
        for reason in [
            VolumeUnavailable::Unconfigured,
            VolumeUnavailable::UnsupportedPlatform,
        ] {
            assert_eq!(
                VolumeAtSpawn::<&str>::Unavailable(reason.clone())
                    .against(|_| unreachable!("nothing to read")),
                VolumeUsage::Unavailable { reason }
            );
        }
    }

    #[test]
    fn no_build_volume_stays_absent() {
        let baseline = VolumeBaseline {
            workspace: at_spawn(100),
            build: None,
        };
        assert_eq!(
            baseline.against(|_| Ok(40)),
            JobVolumeUsage {
                workspace: read(-60),
                build: None,
            }
        );
    }
}
