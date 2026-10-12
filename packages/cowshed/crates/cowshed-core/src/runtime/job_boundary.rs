//! Job-span host and volume columns (13_telemetry.md, "Process-tree spans").
//!
//! These columns belong only on the job span. A process span that carries one fails
//! [`crate::runtime::process_spans::read_process_segment`].

use std::path::Path;

use arrow_schema::{DataType, Field};

use crate::api::resources::{
    ChargedMemoryUsage, HostLoad1, HostLoadSample, JobVolumeUsage, ResourceUnitError, VolumeUsage,
};
use crate::host_load::HostLoadError;
use crate::runtime::volume_usage::{HostVolumeStat, VolumeBaseline, VolumeMountpoint};

/// Nearest milliload. Non-finite, negative, and overflowing loads are refused, never truncated.
pub fn milliload(load: HostLoad1) -> Result<u32, ResourceUnitError> {
    let scaled = load.get() * 1000.0;
    if !scaled.is_finite() || scaled < 0.0 || scaled > f64::from(u32::MAX) {
        return Err(ResourceUnitError::InvalidHostLoad { value: load.get() });
    }
    Ok(scaled.round() as u32)
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
pub struct JobSpanBoundary {
    pub host: HostLoadSample,
    pub volumes: JobVolumeUsage,
    pub charged: Option<ChargedMemoryUsage>,
}

/// The host's current load and the admitted volume probe.
///
/// `workspace` is required. `build` is probed only when the admission named
/// one; an absent build mount is `None`, never a zero and never an invented
/// path. A stat that fails stays [`VolumeUsage::Unavailable`]. Charged memory
/// is not read here.
pub fn sample_job_boundary(
    workspace: &VolumeMountpoint,
    build: Option<&Path>,
) -> Result<JobSpanBoundary, HostLoadError> {
    let host = crate::host_load::read_host_load()?;
    let volumes = VolumeBaseline::admitted(Some(workspace), build).sample(&HostVolumeStat);
    Ok(JobSpanBoundary {
        host,
        volumes,
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
    fn a_native_boundary_reads_the_host_and_leaves_an_absent_build_null() {
        use std::path::Path;

        use crate::runtime::volume_usage::VolumeMountpoint;

        let workspace = VolumeMountpoint::new("/".into()).expect("root is a mount");
        let absent = sample_job_boundary(&workspace, None).expect("the host answers");
        assert!(absent.volumes.build.is_none(), "{:?}", absent.volumes.build);
        assert!(absent.charged.is_none());
        let host = crate::host_load::read_host_load().expect("a second host read");
        assert_eq!(absent.host.cores, host.cores);
        let (workspace_delta, build_delta) = volume_deltas(&absent.volumes);
        assert!(build_delta.is_none());
        match &absent.volumes.workspace {
            VolumeUsage::Read { delta_bytes } => {
                assert_eq!(workspace_delta, Some(delta_bytes.get()));
            }
            VolumeUsage::Unavailable { .. } => assert!(workspace_delta.is_none()),
        }

        let present =
            sample_job_boundary(&workspace, Some(Path::new("/"))).expect("the host answers");
        let build = present
            .volumes
            .build
            .as_ref()
            .expect("the asked volume was probed");
        let (_, build_delta) = volume_deltas(&present.volumes);
        match build {
            VolumeUsage::Read { delta_bytes } => assert_eq!(build_delta, Some(delta_bytes.get())),
            VolumeUsage::Unavailable { .. } => assert!(build_delta.is_none()),
        }
    }
}
