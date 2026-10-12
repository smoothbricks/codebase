//! Job-span host and volume columns (13_telemetry.md, "Process-tree spans").
//!
//! These columns belong only on the job span. A process span that carries one fails
//! [`crate::runtime::process_spans::read_process_segment`].

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field};
use thiserror::Error;

use crate::api::resources::{
    ChargedMemoryUsage, HostLoad1, HostLoadSample, JobVolumeUsage, ResourceUnitError, VolumeUsage,
};
use crate::host_load::HostLoadError;
use crate::runtime::volume_usage::{HostVolume, HostVolumeStat, VolumeBaseline};
use crate::storage::trace_segment::TraceSegmentError;

/// Nearest milliload. Non-finite, negative, and overflowing loads are refused, never truncated.
pub fn milliload(load: HostLoad1) -> Result<u32, ResourceUnitError> {
    let scaled = load.get() * 1000.0;
    if !scaled.is_finite() || scaled < 0.0 || scaled > f64::from(u32::MAX) {
        return Err(ResourceUnitError::InvalidHostLoad { value: load.get() });
    }
    let rounded = scaled.round();
    if rounded < 1.0 {
        return Ok(0);
    }
    let bits = rounded.to_bits();
    let exponent = (bits >> 52) & 0x7ff;
    // A rounded integer in 1..=u32::MAX has biased exponent 1023..=1054,
    // so the shift below is 21..=52 and cannot wrap.
    if !(1023..=1054).contains(&exponent) {
        return Err(ResourceUnitError::InvalidHostLoad { value: load.get() });
    }
    let significand = (bits & ((1u64 << 52) - 1)) | (1u64 << 52);
    let shift = u32::try_from(1075 - exponent)
        .map_err(|_| ResourceUnitError::InvalidHostLoad { value: load.get() })?;
    u32::try_from(significand >> shift)
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

/// The one declaration of the six job-only observation columns.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(
    any(),
    cowshed_api(
        arrow_columns = "job_boundary",
        fields = "job_boundary_fields",
        error = "BoundaryColumnError"
    )
)]
pub struct BoundaryColumns {
    #[cfg_attr(any(), cowshed_api(arrow = "int64"))]
    pub disk_ws_delta_bytes: Option<i64>,
    #[cfg_attr(any(), cowshed_api(arrow = "int64"))]
    pub disk_build_delta_bytes: Option<i64>,
    #[cfg_attr(any(), cowshed_api(arrow = "uint32"))]
    pub load1_milli: Option<u32>,
    #[cfg_attr(any(), cowshed_api(arrow = "uint16"))]
    pub cores: Option<u16>,
    #[cfg_attr(any(), cowshed_api(arrow = "uint64"))]
    pub charged_memory_current_bytes: Option<u64>,
    #[cfg_attr(any(), cowshed_api(arrow = "uint64"))]
    pub charged_memory_peak_bytes: Option<u64>,
}

impl BoundaryColumns {
    pub(crate) fn from_observation(
        host: HostLoadSample,
        volumes: &JobVolumeUsage,
        charged: Option<ChargedMemoryUsage>,
    ) -> Result<Self, ResourceUnitError> {
        let (load1_milli, cores) = boundary_host(host)?;
        let (disk_ws_delta_bytes, disk_build_delta_bytes) = volume_deltas(volumes);
        let (charged_memory_current_bytes, charged_memory_peak_bytes) =
            charged_memory_values(charged);
        Ok(Self {
            disk_ws_delta_bytes,
            disk_build_delta_bytes,
            load1_milli: Some(load1_milli),
            cores: Some(cores),
            charged_memory_current_bytes,
            charged_memory_peak_bytes,
        })
    }
}

#[derive(Debug, Error)]
pub enum BoundaryColumnError {
    #[error("job boundary does not have the declared fixed schema")]
    Schema,
}

include!("job_boundary_columns.generated.rs");

pub(crate) fn boundary_columns(
    columns: BoundaryColumns,
) -> Result<impl Iterator<Item = (Field, ArrayRef)>, TraceSegmentError> {
    Ok(job_boundary_fields()
        .into_iter()
        .zip(encode_job_boundary_columns(&columns)?))
}

/// Read the exact boundary suffix of one row; the caller owns the preceding columns.
pub fn read_boundary_columns(
    batch: &RecordBatch,
    start: usize,
) -> Result<BoundaryColumns, BoundaryColumnError> {
    let expected = job_boundary_fields();
    let schema = batch.schema();
    if batch.num_rows() != 1
        || start.checked_add(JOB_BOUNDARY_COLUMN_COUNT) != Some(schema.fields().len())
        || !schema
            .fields()
            .iter()
            .skip(start)
            .map(AsRef::as_ref)
            .eq(expected.iter())
    {
        return Err(BoundaryColumnError::Schema);
    }
    decode_job_boundary_columns(batch, start)
}

fn int64(value: Option<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vec![value]))
}

fn uint16(value: Option<u16>) -> ArrayRef {
    Arc::new(UInt16Array::from(vec![value]))
}

fn uint32(value: Option<u32>) -> ArrayRef {
    Arc::new(UInt32Array::from(vec![value]))
}

fn uint64(value: Option<u64>) -> ArrayRef {
    Arc::new(UInt64Array::from(vec![value]))
}

fn scalar<T: ArrowPrimitiveType>(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<T::Native>, BoundaryColumnError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| BoundaryColumnError::Schema)?;
    if index < start {
        return Err(BoundaryColumnError::Schema);
    }
    let values = batch
        .column(index)
        .as_primitive_opt::<T>()
        .ok_or(BoundaryColumnError::Schema)?;
    Ok(values.is_valid(0).then(|| values.value(0)))
}

fn optional_i64(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<i64>, BoundaryColumnError> {
    scalar::<Int64Type>(batch, start, name)
}

fn optional_u16(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<u16>, BoundaryColumnError> {
    scalar::<UInt16Type>(batch, start, name)
}

fn optional_u32(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<u32>, BoundaryColumnError> {
    scalar::<UInt32Type>(batch, start, name)
}

fn optional_u64(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<u64>, BoundaryColumnError> {
    scalar::<UInt64Type>(batch, start, name)
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
    fn generated_boundary_columns_round_trip_signed_bytes_and_absence() {
        let volumes = JobVolumeUsage {
            workspace: VolumeUsage::Read {
                delta_bytes: VolumeUsedBytesDelta::new(-4096).unwrap(),
            },
            build: None,
        };
        let columns = BoundaryColumns::from_observation(
            HostLoadSample::new(1.2345, u16::MAX).unwrap(),
            &volumes,
            None,
        )
        .unwrap();
        assert_eq!(columns.disk_ws_delta_bytes, Some(-4096));
        assert_eq!(columns.disk_build_delta_bytes, None);
        assert_eq!(columns.load1_milli, Some(1235));
        assert_eq!(columns.cores, Some(u16::MAX));
        assert_eq!(columns.charged_memory_current_bytes, None);
        assert_eq!(columns.charged_memory_peak_bytes, None);
        for columns in [columns, BoundaryColumns::default()] {
            let batch = RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(job_boundary_fields())),
                encode_job_boundary_columns(&columns).unwrap(),
            )
            .unwrap();
            assert_eq!(batch.num_columns(), 6);
            assert_eq!(read_boundary_columns(&batch, 0).unwrap(), columns);
        }
    }

    #[test]
    fn generated_boundary_schema_rejects_a_widened_core_or_extra_column() {
        let columns = BoundaryColumns {
            cores: Some(8),
            charged_memory_current_bytes: Some(4096),
            charged_memory_peak_bytes: Some(8192),
            ..BoundaryColumns::default()
        };
        let mut fields = job_boundary_fields();
        let mut arrays = encode_job_boundary_columns(&columns).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(fields.clone())),
            arrays.clone(),
        )
        .unwrap();
        assert_eq!(read_boundary_columns(&batch, 0).unwrap(), columns);
        let cores = fields
            .iter()
            .position(|field| field.name() == "cores")
            .unwrap();
        fields[cores] = Field::new("cores", DataType::UInt32, true);
        arrays[cores] = uint32(Some(8));
        let widened =
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap();
        assert!(matches!(
            read_boundary_columns(&widened, 0),
            Err(BoundaryColumnError::Schema)
        ));

        let mut fields = job_boundary_fields();
        let mut arrays = encode_job_boundary_columns(&columns).unwrap();
        fields.push(Field::new("process_specific_usage", DataType::UInt64, true));
        arrays.push(uint64(Some(4096)));
        let extra =
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap();
        assert!(matches!(
            read_boundary_columns(&extra, 0),
            Err(BoundaryColumnError::Schema)
        ));
    }

    #[test]
    fn milliload_rounds_to_nearest_and_refuses_overflow() {
        let load = |value| HostLoad1::new(value).unwrap();
        assert_eq!(milliload(load(0.0)).unwrap(), 0);
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

    #[cfg(target_os = "macos")]
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
