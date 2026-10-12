//! Job-span host and volume columns (13_telemetry.md, "Process-tree spans").
//!
//! These columns belong only on the job span. A process span that carries one fails
//! [`crate::runtime::process_spans::read_process_segment`].

use arrow_schema::{DataType, Field};

use crate::api::resources::{
    ChargedMemoryUsage, HostLoad1, HostLoadSample, JobVolumeUsage, ResourceUnitError, VolumeUsage,
};
use crate::host_load::HostLoadError;
use crate::runtime::volume_usage::{HostVolume, HostVolumeStat, VolumeBaseline};

/// Nearest milliload. Non-finite, negative, and overflowing loads are refused, never truncated.
pub fn milliload(load: HostLoad1) -> Result<u32, ResourceUnitError> {
    let scaled = load.get() * 1000.0;
    if !scaled.is_finite() || scaled < 0.0 || scaled > f64::from(u32::MAX) {
        return Err(ResourceUnitError::InvalidHostLoad { value: load.get() });
    }
    // The range check makes this an integer in `u32`. Parsing that decimal is the
    // checked conversion; a float-to-int cast is not.
    format!("{:.0}", scaled.round())
        .parse()
        .map_err(|_| ResourceUnitError::InvalidHostLoad { value: load.get() })
}

/// Signed workspace and build deltas for one job-span boundary. An absent or unreadable build
/// volume is null, never zero.
pub fn volume_deltas(volumes: &JobVolumeUsage) -> (Option<i64>, Option<i64>) {
    (
        delta(&volumes.workspace),
        volumes.build.as_ref().and_then(delta),
    )
}

fn delta(usage: &VolumeUsage) -> Option<i64> {
    match usage {
        VolumeUsage::Read { delta_bytes } => Some(delta_bytes.get()),
        VolumeUsage::Unavailable { .. } => None,
    }
}

/// The job-only columns. Process spans do not declare these names.
pub fn job_boundary_fields() -> Vec<Field> {
    vec![
        Field::new("disk_ws_delta_bytes", DataType::Int64, true),
        Field::new("disk_build_delta_bytes", DataType::Int64, true),
        Field::new("load1_milli", DataType::UInt32, true),
        Field::new("cores", DataType::UInt16, true),
    ]
}

/// Host, volume, and charged-memory facts for one job-span start or end.
/// Absent build volume and absent charged memory stay null. This does not
/// name a leaf or compute unattributed usage.
#[derive(Debug)]
pub struct JobSpanBoundary {
    pub host: HostLoadSample,
    pub volumes: JobVolumeUsage,
    pub charged: Option<ChargedMemoryUsage>,
}

/// The host's current load and a sample of the baseline admission already took.
///
/// The actor passes `job.volumes`. This does not admit again, so an end row's
/// delta is against that baseline, not against the terminal instant. An absent
/// build on the baseline stays absent. Charged memory is not read here.
pub fn sample_job_boundary(
    volumes: &VolumeBaseline<HostVolume>,
) -> Result<JobSpanBoundary, HostLoadError> {
    let host = crate::host_load::read_host_load()?;
    Ok(JobSpanBoundary {
        host,
        volumes: volumes.sample(&HostVolumeStat),
        charged: None,
    })
}

/// Charged-memory columns for the job span only. Absent when the accounting source has no
/// charged-memory counter, never written into an RSS column.
pub fn charged_memory_fields() -> Vec<Field> {
    vec![
        Field::new("charged_memory_current_bytes", DataType::UInt64, true),
        Field::new("charged_memory_peak_bytes", DataType::UInt64, true),
    ]
}

pub fn charged_memory_values(
    charged: Option<crate::api::resources::ChargedMemoryUsage>,
) -> (Option<u64>, Option<u64>) {
    match charged {
        Some(charged) => (
            Some(charged.current_bytes.get()),
            Some(charged.peak_bytes.get()),
        ),
        None => (None, None),
    }
}

/// Host facts at a job-span boundary. Cores come from the checked sample, so a zero core count
/// cannot be represented.
pub fn boundary_host(host: HostLoadSample) -> Result<(u32, u16), ResourceUnitError> {
    Ok((milliload(host.load1)?, host.cores.get()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::resources::{HostCores, VolumeUnavailable, VolumeUsedBytesDelta};
    use crate::runtime::process_spans::process_custom_fields;

    #[test]
    fn milliload_rounds_to_nearest_and_refuses_overflow() {
        let load = |value| HostLoad1::new(value).unwrap();
        assert_eq!(milliload(load(1.2344)).unwrap(), 1234);
        assert_eq!(milliload(load(1.2345)).unwrap(), 1235);
        assert!(milliload(load(f64::from(u32::MAX) + 1.0)).is_err());
    }

    #[test]
    fn an_absent_build_volume_is_null_not_zero() {
        let volumes = JobVolumeUsage {
            workspace: VolumeUsage::Read {
                delta_bytes: VolumeUsedBytesDelta::new(-4096).unwrap(),
            },
            build: None,
        };
        assert_eq!(volume_deltas(&volumes), (Some(-4096), None));
        let unread = JobVolumeUsage {
            workspace: VolumeUsage::Unavailable {
                reason: VolumeUnavailable::Unconfigured,
            },
            build: Some(VolumeUsage::Unavailable {
                reason: VolumeUnavailable::UnsupportedPlatform,
            }),
        };
        assert_eq!(volume_deltas(&unread), (None, None));
        let host = HostLoadSample::new(0.5, 8).unwrap();
        assert_eq!(boundary_host(host).unwrap(), (500, 8));
        assert!(HostCores::new(0).is_err());
    }

    #[test]
    fn job_columns_are_not_process_columns() {
        let process: Vec<_> = process_custom_fields()
            .into_iter()
            .map(|field| field.name().to_owned())
            .collect();
        for field in job_boundary_fields() {
            assert!(
                !process.iter().any(|name| name == field.name()),
                "{}",
                field.name()
            );
        }
        assert_eq!(process.len(), 14);
    }

    #[test]
    fn a_boundary_samples_the_admitted_baseline_not_a_fresh_one() {
        use std::path::PathBuf;

        use crate::runtime::volume_usage::{VolumeAtSpawn, VolumeMountpoint};

        let mut candidates = vec![
            PathBuf::from("/System/Volumes/Data"),
            PathBuf::from("/System/Volumes/Preboot"),
            PathBuf::from("/dev"),
        ];
        if let Ok(entries) = std::fs::read_dir("/Volumes") {
            candidates.extend(entries.flatten().map(|entry| entry.path()));
        }
        let (workspace, admitted) = candidates
            .into_iter()
            .find_map(|path| {
                let mount = VolumeMountpoint::new(path).ok()?;
                let admitted = VolumeBaseline::admitted(Some(&mount), None);
                matches!(admitted.workspace, VolumeAtSpawn::Read { .. })
                    .then_some((mount, admitted))
            })
            .expect("this host has no volume the production capture can read");
        let VolumeAtSpawn::Read { volume, used } = &admitted.workspace else {
            unreachable!("the finder kept only a read baseline");
        };
        let stale = VolumeBaseline {
            workspace: VolumeAtSpawn::Read {
                volume: volume.clone(),
                used: used.saturating_add(1 << 40),
            },
            build: None,
        };
        let boundary = sample_job_boundary(&stale).expect("the host answers");
        assert!(boundary.charged.is_none());
        assert!(boundary.volumes.build.is_none());
        assert_eq!(boundary.volumes, stale.sample(&HostVolumeStat));
        let fresh = VolumeBaseline::admitted(Some(&workspace), None).sample(&HostVolumeStat);
        let (
            VolumeUsage::Read { delta_bytes },
            VolumeUsage::Read {
                delta_bytes: fresh_delta,
            },
        ) = (&boundary.volumes.workspace, &fresh.workspace)
        else {
            panic!("both probes of a readable volume must be deltas: {boundary:?} {fresh:?}");
        };
        assert_ne!(
            delta_bytes.get(),
            fresh_delta.get(),
            "a new admission would not keep the passed reading {used}"
        );
    }
}
