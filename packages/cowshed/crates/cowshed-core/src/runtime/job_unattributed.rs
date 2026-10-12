//! Signed observation reconciliation on the job span (13_telemetry.md, "Process-tree spans").
//! The one annotated record owns the generated fixed columns and enum vocabularies. It never
//! grows the process schema, embeds a process tree, or fills an unavailable I/O comparison with zero.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt8Type, UInt32Type};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field};
use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::JobInfo;
use crate::api::process::{ProcessCoverage, ProcessCoverageGap};
use crate::api::resources::JobAccounting;
use crate::runtime::job_spans::{JOB_SPAN_ID, job_span_thread};
use crate::runtime::process_spans::enumeration;
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError, dict_type,
    seal_segment, system_fields, trace_batch,
};

pub const JOB_UNATTRIBUTED: &str = "job.unattributed";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(any(), cowshed_api(arrow_columns = "unattributed"))]
pub struct SparseReconciliation {
    #[cfg_attr(any(), cowshed_api(arrow = "int64"))]
    pub unattributed_cpu_us: Option<i64>,
    #[cfg_attr(any(), cowshed_api(arrow = "int64"))]
    pub unattributed_io_read_bytes: Option<i64>,
    #[cfg_attr(any(), cowshed_api(arrow = "int64"))]
    pub unattributed_io_write_bytes: Option<i64>,
    #[cfg_attr(
        any(),
        cowshed_api(arrow = "dict8", values = "unused|macosRusageChildren|linuxCgroupV2")
    )]
    pub source: Option<u8>,
    #[cfg_attr(
        any(),
        cowshed_api(
            arrow = "dict8",
            values = "unused|complete|eventsLost|unobservedBirth|unobservedExit|uncountedFork|unreadImage|unreadFinalUsage"
        )
    )]
    pub coverage: Option<u8>,
}

include!("job_unattributed_columns.generated.rs");

#[derive(Debug, Error)]
pub enum UnattributedSpanError {
    #[error("job reconciliation time is before the Unix epoch")]
    BeforeEpoch,
    #[error("job reconciliation time exceeds i64 nanoseconds")]
    BeyondNanoseconds,
    #[error(
        "job reconciliation segment does not have the declared fixed schema and enum dictionaries"
    )]
    Schema,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
}

pub struct UnattributedSpanRow {
    at: SystemTime,
    trace_id: String,
    span: SpanAddress,
    columns: SparseReconciliation,
}

impl UnattributedSpanRow {
    /// A discrepancy or observation gap, on the existing job span. A complete observation that
    /// agrees with every available independent counter creates no row.
    pub fn project(info: &JobInfo, accounting: JobAccounting) -> Option<Self> {
        let columns = project_accounting(accounting)?;
        Some(Self {
            at: SystemTime::now(),
            trace_id: info.trace.trace_id.as_str().to_owned(),
            span: SpanAddress {
                thread_id: job_span_thread(&info.repo_id, &info.workspace_incarnation, info.job_id),
                span_id: JOB_SPAN_ID,
            },
            columns,
        })
    }
}

fn project_accounting(accounting: JobAccounting) -> Option<SparseReconciliation> {
    let reconciliation = accounting.reconciliation();
    let cpu = reconciliation.cpu_us.get();
    let read = reconciliation.io_read_bytes.map(|delta| delta.get());
    let write = reconciliation.io_write_bytes.map(|delta| delta.get());
    if cpu == 0
        && read.is_none_or(|value| value == 0)
        && write.is_none_or(|value| value == 0)
        && reconciliation.coverage == ProcessCoverage::Complete
    {
        return None;
    }
    let source = match accounting {
        JobAccounting::MacOsRusageChildren { .. } => "macosRusageChildren",
        JobAccounting::LinuxCgroupV2 { .. } => "linuxCgroupV2",
    };
    let coverage = match reconciliation.coverage {
        ProcessCoverage::Complete => "complete",
        ProcessCoverage::Gap { reason } => match reason {
            ProcessCoverageGap::EventsLost => "eventsLost",
            ProcessCoverageGap::UnobservedBirth { .. } => "unobservedBirth",
            ProcessCoverageGap::UnobservedExit { .. } => "unobservedExit",
            ProcessCoverageGap::UncountedFork { .. } => "uncountedFork",
            ProcessCoverageGap::UnreadImage { .. } => "unreadImage",
            ProcessCoverageGap::UnreadFinalUsage { .. } => "unreadFinalUsage",
        },
    };
    Some(SparseReconciliation {
        unattributed_cpu_us: Some(cpu),
        unattributed_io_read_bytes: read,
        unattributed_io_write_bytes: write,
        source: Some(enum_key(&SOURCE_NAMES, source)),
        coverage: Some(enum_key(&COVERAGE_NAMES, coverage)),
    })
}

fn enum_key(names: &[&str], name: &str) -> u8 {
    let key = names
        .iter()
        .position(|declared| *declared == name)
        .expect("every projected variant is in the source-owned vocabulary");
    u8::try_from(key).expect("the declared vocabulary fits its dictionary key")
}

pub fn seal_unattributed_row(
    root: &Path,
    writer: Uuid,
    order: u64,
    row: &UnattributedSpanRow,
) -> Result<PathBuf, UnattributedSpanError> {
    let since_epoch = row
        .at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| UnattributedSpanError::BeforeEpoch)?;
    let timestamp_ns = i64::try_from(since_epoch.as_nanos())
        .map_err(|_| UnattributedSpanError::BeyondNanoseconds)?;
    let mut system = SystemColumns::with_capacity(1);
    system.push(SystemRow {
        timestamp_ns,
        trace_id: &row.trace_id,
        span: row.span,
        parent: None,
        entry_type: EntryType::Trace,
        message: Some(JOB_UNATTRIBUTED),
    })?;
    let fields = unattributed_fields();
    let arrays = encode_unattributed_columns(&row.columns)?;
    debug_assert_eq!(fields.len(), UNATTRIBUTED_COLUMN_COUNT);
    debug_assert_eq!(arrays.len(), fields.len());
    let batch = trace_batch(system, fields.into_iter().zip(arrays).collect())?;
    Ok(seal_segment(
        root,
        TelemetryDate::from_unix_seconds(since_epoch.as_secs()),
        &format!("unattributed-{order:020}-{writer}"),
        &batch,
    )?)
}

fn int64(value: Option<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vec![value]))
}

fn column<'a>(
    batch: &'a RecordBatch,
    start: usize,
    name: &str,
) -> Result<&'a ArrayRef, UnattributedSpanError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| UnattributedSpanError::Schema)?;
    if index < start {
        return Err(UnattributedSpanError::Schema);
    }
    Ok(batch.column(index))
}

fn optional_i64(
    batch: &RecordBatch,
    start: usize,
    name: &str,
) -> Result<Option<i64>, UnattributedSpanError> {
    let column = column(batch, start, name)?;
    let values = column
        .as_primitive_opt::<Int64Type>()
        .ok_or(UnattributedSpanError::Schema)?;
    Ok(values.is_valid(0).then(|| values.value(0)))
}

fn optional_key(
    batch: &RecordBatch,
    start: usize,
    name: &str,
    names: &[&str],
) -> Result<Option<u8>, UnattributedSpanError> {
    let column = column(batch, start, name)?;
    let dictionary = column
        .as_dictionary_opt::<UInt8Type>()
        .ok_or(UnattributedSpanError::Schema)?;
    let values = dictionary
        .values()
        .as_string_opt::<i32>()
        .ok_or(UnattributedSpanError::Schema)?;
    if values.len() != names.len()
        || values
            .iter()
            .zip(names)
            .any(|(actual, declared)| actual != Some(*declared))
    {
        return Err(UnattributedSpanError::Schema);
    }
    if dictionary.is_null(0) {
        return Ok(None);
    }
    let key = dictionary.keys().value(0);
    if usize::from(key) >= names.len() {
        return Err(UnattributedSpanError::Schema);
    }
    Ok(Some(key))
}

/// The system identity and the same generated record the writer encodes: no mirror of its fields.
#[derive(Debug, Eq, PartialEq)]
pub struct DecodedUnattributedRow {
    pub thread_id: u64,
    pub span_id: u32,
    pub columns: SparseReconciliation,
}

pub fn read_unattributed_segment(
    path: &Path,
) -> Result<DecodedUnattributedRow, UnattributedSpanError> {
    let file = std::fs::File::open(path).map_err(|source| TraceSegmentError::Io {
        step: crate::storage::trace_segment::SealStep::CreatingSegment,
        source,
    })?;
    let mut reader = StreamReader::try_new(file, None).map_err(TraceSegmentError::from)?;
    let batch = reader
        .next()
        .ok_or(UnattributedSpanError::Schema)?
        .map_err(TraceSegmentError::from)?;
    if reader.next().is_some() || batch.num_rows() != 1 {
        return Err(UnattributedSpanError::Schema);
    }
    let mut expected = system_fields();
    let start = expected.len();
    expected.extend(unattributed_fields());
    if batch
        .schema()
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .ne(expected.iter())
    {
        return Err(UnattributedSpanError::Schema);
    }
    let thread = batch
        .column_by_name("thread_id")
        .ok_or(UnattributedSpanError::Schema)?;
    let span = batch
        .column_by_name("span_id")
        .ok_or(UnattributedSpanError::Schema)?;
    Ok(DecodedUnattributedRow {
        thread_id: thread
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0),
        span_id: span.as_primitive::<UInt32Type>().value(0),
        columns: decode_unattributed_columns(&batch, start)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::resources::{
        ChargedMemoryBytes, ChargedMemoryUsage, CpuMicros, CpuMicrosDelta, CpuTotals,
        StorageIoBytes, StorageIoBytesDelta, StorageIoTotals, UsageReconciliation,
    };

    fn accounting(cpu: i64, io: Option<i64>, coverage: ProcessCoverage) -> JobAccounting {
        let totals = CpuTotals {
            user_us: CpuMicros::new(1000).unwrap(),
            sys_us: CpuMicros::new(0).unwrap(),
        };
        let reconciliation = UsageReconciliation {
            cpu_us: CpuMicrosDelta::new(cpu).unwrap(),
            io_read_bytes: io.map(|value| StorageIoBytesDelta::new(value).unwrap()),
            io_write_bytes: None,
            coverage,
        };
        match io {
            None => JobAccounting::MacOsRusageChildren {
                cpu: totals,
                io: None,
                reconciliation,
            },
            Some(_) => JobAccounting::LinuxCgroupV2 {
                cpu: totals,
                io: StorageIoTotals {
                    read_bytes: StorageIoBytes::new(1).unwrap(),
                    write_bytes: StorageIoBytes::new(0).unwrap(),
                },
                charged_memory: ChargedMemoryUsage {
                    current_bytes: ChargedMemoryBytes::new(0).unwrap(),
                    peak_bytes: ChargedMemoryBytes::new(0).unwrap(),
                },
                reconciliation,
            },
        }
    }

    #[test]
    fn signed_differences_and_unavailable_io_keep_their_exact_units() {
        for delta in [-150, 1, 150] {
            let columns =
                project_accounting(accounting(delta, None, ProcessCoverage::Complete)).unwrap();
            assert_eq!(columns.unattributed_cpu_us, Some(delta));
            assert_eq!(columns.unattributed_io_read_bytes, None);
            assert_eq!(columns.unattributed_io_write_bytes, None);
            assert_eq!(
                SOURCE_NAMES[usize::from(columns.source.unwrap())],
                "macosRusageChildren"
            );
            assert_eq!(
                COVERAGE_NAMES[usize::from(columns.coverage.unwrap())],
                "complete"
            );
        }
        assert!(project_accounting(accounting(0, None, ProcessCoverage::Complete)).is_none());
        let columns =
            project_accounting(accounting(0, Some(-1), ProcessCoverage::Complete)).unwrap();
        assert_eq!(columns.unattributed_io_read_bytes, Some(-1));
    }

    #[test]
    fn every_gap_is_explicit_even_when_the_counters_agree() {
        for reason in [
            ProcessCoverageGap::EventsLost,
            ProcessCoverageGap::UnobservedBirth { pid: 2 },
            ProcessCoverageGap::UnobservedExit { pid: 2 },
            ProcessCoverageGap::UncountedFork { pid: 2 },
            ProcessCoverageGap::UnreadImage { pid: 2 },
            ProcessCoverageGap::UnreadFinalUsage { pid: 2 },
        ] {
            let columns =
                project_accounting(accounting(0, None, ProcessCoverage::Gap { reason })).unwrap();
            assert_eq!(columns.unattributed_cpu_us, Some(0));
            assert_ne!(
                columns.coverage,
                Some(enum_key(&COVERAGE_NAMES, "complete"))
            );
        }
        let process = crate::runtime::process_spans::process_custom_fields();
        assert_eq!(process.len(), 14);
        for field in unattributed_fields() {
            assert!(!process.iter().any(|process| process.name() == field.name()));
        }
    }

    #[test]
    fn the_shared_sealer_keeps_signed_precision_source_and_unavailable_io() {
        let root = crate::temp_root::TempRoot::new("cowshed-unattributed");
        let cases = [
            accounting(
                150,
                None,
                ProcessCoverage::Gap {
                    reason: ProcessCoverageGap::EventsLost,
                },
            ),
            accounting(-1, None, ProcessCoverage::Complete),
            accounting(-150, Some(-1), ProcessCoverage::Complete),
        ];
        for (index, accounting) in cases.into_iter().enumerate() {
            let columns = project_accounting(accounting).unwrap();
            let row = UnattributedSpanRow {
                at: UNIX_EPOCH + std::time::Duration::from_secs(1),
                trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_owned(),
                span: SpanAddress {
                    thread_id: 9,
                    span_id: JOB_SPAN_ID,
                },
                columns,
            };
            let path =
                seal_unattributed_row(&root, Uuid::nil(), u64::try_from(index).unwrap() + 1, &row)
                    .unwrap();
            let decoded = read_unattributed_segment(&path).unwrap();
            assert_eq!(decoded.thread_id, 9);
            assert_eq!(decoded.span_id, JOB_SPAN_ID);
            assert_eq!(decoded.columns, columns);
        }
    }
}
