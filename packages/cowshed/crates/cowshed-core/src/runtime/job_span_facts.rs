//! The host and volume facts a job span's rows carry (13_telemetry.md, "Process-tree spans").
//!
//! The job span, not each process span, carries the host's one-minute load and online cores at
//! its start and end, and the signed used-bytes change of the job's workspace and build volumes
//! at its end. The end row's facts are a projection of the job's terminal resource sample
//! ([`JobResourceSample`]) and state what it states, no more: an absent sample, a volume the job
//! ran without, and a volume whose usage the sample could not read are each a null cell, never a
//! zero. The start row states the host as the supervisor read it at admission, when no sample
//! exists yet. Process I/O and volume allocation stay different facts, and no process span
//! carries any of these columns.
//!
//! Host load converts once, to the nearest milliload, through [`HostColumns::convert`]: a
//! non-finite or negative load, a load past the `u32` milliload range and a core count past the
//! `u16` range are typed [`HostColumnError`]s and never truncate.

use crate::api::resources::{HostLoadSample, JobResourceSample, VolumeUsage, VolumeUsedBytesDelta};

/// The largest milliload a `u32` column holds, exactly representable as an `f64`.
const MAX_MILLILOAD: f64 = 4_294_967_295.0;

/// A host reading no column can state without changing it.
#[derive(Clone, Copy, Debug, PartialEq, thiserror::Error)]
pub enum HostColumnError {
    #[error("host load1 {value} is not finite")]
    LoadNotFinite { value: f64 },
    #[error("host load1 {value} is negative")]
    LoadNegative { value: f64 },
    #[error("host load1 {value} exceeds the u32 milliload range")]
    LoadOverflow { value: f64 },
    #[error("host core count {value} exceeds the u16 range")]
    CoresOverflow { value: u64 },
}

/// A host reading as its two job-span columns: `load1_milli` and `cores`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostColumns {
    pub load1_milli: u32,
    pub cores: u16,
}

impl HostColumns {
    /// `load1` rounded once to the nearest milliload (an exact half rounds away from zero), and
    /// `cores` as the `u16` the column holds.
    pub fn convert(load1: f64, cores: u64) -> Result<Self, HostColumnError> {
        if !load1.is_finite() {
            return Err(HostColumnError::LoadNotFinite { value: load1 });
        }
        if load1 < 0.0 {
            return Err(HostColumnError::LoadNegative { value: load1 });
        }
        // A finite load whose thousandths pass the finite range is infinite here, which the
        // bound below refuses all the same.
        let milli = (load1 * 1000.0).round();
        if milli > MAX_MILLILOAD {
            return Err(HostColumnError::LoadOverflow { value: load1 });
        }
        Ok(Self {
            // `milli` is whole and within `0..=u32::MAX`, so this cast is exact.
            load1_milli: milli as u32,
            cores: u16::try_from(cores)
                .map_err(|_| HostColumnError::CoresOverflow { value: cores })?,
        })
    }

    /// A job's host sample as its columns. The sample's own types already refuse a non-finite or
    /// negative load and a core count past `u16`, so only the milliload range can refuse it.
    pub fn of(sample: HostLoadSample) -> Result<Self, HostColumnError> {
        Self::convert(sample.load1.get(), u64::from(sample.cores.get()))
    }
}

/// What one boundary row of a job span states of the host and the volumes: each fact, or its
/// absence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpanFacts {
    pub host: Option<HostLoadSample>,
    pub workspace_delta: Option<VolumeUsedBytesDelta>,
    pub build_delta: Option<VolumeUsedBytesDelta>,
}

impl SpanFacts {
    /// The start row's facts: the host as admission read it. No volume has changed yet, so none
    /// states a delta.
    pub fn start(host: Option<HostLoadSample>) -> Self {
        Self {
            host,
            ..Self::default()
        }
    }

    /// The end row's facts from the job's terminal `sample`: its host, and each volume's change
    /// since admission's baseline. A job with no sample, a volume the job ran without, and a
    /// volume whose usage the sample could not read state no fact.
    pub fn end(sample: Option<&JobResourceSample>) -> Self {
        let Some(sample) = sample else {
            return Self::default();
        };
        Self {
            host: Some(sample.host),
            workspace_delta: delta(Some(&sample.volumes.workspace)),
            build_delta: delta(sample.volumes.build.as_ref()),
        }
    }
}

fn delta(usage: Option<&VolumeUsage>) -> Option<VolumeUsedBytesDelta> {
    match usage {
        Some(VolumeUsage::Read { delta_bytes }) => Some(*delta_bytes),
        Some(VolumeUsage::Unavailable { .. }) | None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::resources::VolumeUnavailable;

    #[test]
    fn load_converts_once_to_the_nearest_milliload() {
        let milli = |load1| HostColumns::convert(load1, 8).map(|columns| columns.load1_milli);
        assert_eq!(milli(0.0), Ok(0));
        assert_eq!(milli(-0.0), Ok(0));
        assert_eq!(milli(0.0004), Ok(0));
        assert_eq!(milli(0.0006), Ok(1));
        assert_eq!(milli(1.2344), Ok(1234));
        assert_eq!(milli(1.2346), Ok(1235));
        assert_eq!(milli(2.5), Ok(2500));
        assert_eq!(milli(350.0), Ok(350_000));
        assert_eq!(milli(4_294_967.295), Ok(u32::MAX));
    }

    #[test]
    fn a_load_that_is_not_a_milliload_is_a_typed_error_not_a_truncation() {
        for not_finite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                matches!(
                    HostColumns::convert(not_finite, 8),
                    Err(HostColumnError::LoadNotFinite { .. })
                ),
                "{not_finite}"
            );
        }
        assert_eq!(
            HostColumns::convert(-0.001, 8),
            Err(HostColumnError::LoadNegative { value: -0.001 })
        );
        // One milliload past the range, and a product that passes the finite range itself.
        for overflowing in [4_294_967.296, 1.0e12, f64::MAX] {
            assert_eq!(
                HostColumns::convert(overflowing, 8),
                Err(HostColumnError::LoadOverflow { value: overflowing })
            );
        }
    }

    #[test]
    fn a_core_count_past_u16_is_a_typed_error() {
        assert_eq!(
            HostColumns::convert(1.0, 65_535).map(|columns| columns.cores),
            Ok(u16::MAX)
        );
        assert_eq!(
            HostColumns::convert(1.0, 65_536),
            Err(HostColumnError::CoresOverflow { value: 65_536 })
        );
        assert_eq!(
            HostColumns::convert(1.0, u64::MAX),
            Err(HostColumnError::CoresOverflow { value: u64::MAX })
        );
    }

    #[test]
    fn a_host_sample_states_its_own_load_and_cores() {
        let sample = HostLoadSample::new(12.3456, 10).expect("a host sample");
        assert_eq!(
            HostColumns::of(sample),
            Ok(HostColumns {
                load1_milli: 12_346,
                cores: 10
            })
        );
    }

    #[test]
    fn the_start_states_only_the_host_and_an_absent_sample_states_nothing() {
        let host = HostLoadSample::new(0.5, 4).expect("a host sample");
        assert_eq!(
            SpanFacts::start(Some(host)),
            SpanFacts {
                host: Some(host),
                workspace_delta: None,
                build_delta: None
            }
        );
        assert_eq!(SpanFacts::start(None), SpanFacts::default());
        assert_eq!(SpanFacts::end(None), SpanFacts::default());
    }

    #[test]
    fn a_volume_states_its_delta_only_when_it_was_read() {
        let read = |bytes| VolumeUsage::Read {
            delta_bytes: VolumeUsedBytesDelta::new(bytes).expect("a delta"),
        };
        let unavailable = VolumeUsage::Unavailable {
            reason: VolumeUnavailable::Unconfigured,
        };
        assert_eq!(delta(None), None);
        assert_eq!(delta(Some(&unavailable)), None);
        assert_eq!(
            delta(Some(&read(-4096))).map(VolumeUsedBytesDelta::get),
            Some(-4096)
        );
        // A volume that really did not change states zero, which is not absence.
        assert_eq!(
            delta(Some(&read(0))).map(VolumeUsedBytesDelta::get),
            Some(0)
        );
    }
}
