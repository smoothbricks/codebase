//! The job span's unattributed-usage row (13_telemetry.md). One fixed row kind. Missing
//! comparisons stay absent. The fourteen process columns do not grow.

use arrow_schema::{DataType, Field};

use crate::api::process::{ProcessCoverage, UsageReconciliation};
use crate::runtime::process_spans::process_custom_fields;

/// The row template. Not a process span and not a JSON payload.
pub const JOB_UNATTRIBUTED: &str = "job.unattributed";

/// Fixed columns of a `job.unattributed` row. Coverage is a fixed enum name, not a new column
/// per gap reason.
pub fn unattributed_fields() -> Vec<Field> {
    vec![
        Field::new("unattributed_cpu_us", DataType::Int64, true),
        Field::new("unattributed_io_read_bytes", DataType::Int64, true),
        Field::new("unattributed_io_write_bytes", DataType::Int64, true),
        Field::new("coverage", DataType::Utf8, true),
    ]
}

/// The signed differences and coverage a job span records. Absent I/O stays absent.
pub fn unattributed_values(
    reconciliation: &UsageReconciliation,
) -> (i64, Option<i64>, Option<i64>, &'static str) {
    (
        reconciliation.cpu_us.get(),
        reconciliation.io_read_bytes.map(|delta| delta.get()),
        reconciliation.io_write_bytes.map(|delta| delta.get()),
        match &reconciliation.coverage {
            ProcessCoverage::Complete => "complete",
            ProcessCoverage::Gap { reason } => match reason {
                crate::api::process::ProcessCoverageGap::EventsLost => "eventsLost",
                crate::api::process::ProcessCoverageGap::UnobservedBirth { .. } => {
                    "unobservedBirth"
                }
                crate::api::process::ProcessCoverageGap::UnobservedExit { .. } => "unobservedExit",
                crate::api::process::ProcessCoverageGap::UncountedFork { .. } => "uncountedFork",
                crate::api::process::ProcessCoverageGap::UnreadImage { .. } => "unreadImage",
                crate::api::process::ProcessCoverageGap::UnreadFinalUsage { .. } => {
                    "unreadFinalUsage"
                }
            },
        },
    )
}
mod tests {
    use super::*;
    use crate::api::process::{
        CpuMicrosDelta, ProcessCoverage, ProcessCoverageGap, StorageIoBytesDelta,
    };

    #[test]
    fn a_missing_io_comparison_stays_absent_and_the_process_schema_does_not_grow() {
        let values = unattributed_values(&UsageReconciliation {
            cpu_us: CpuMicrosDelta::new(-150),
            io_read_bytes: None,
            io_write_bytes: Some(StorageIoBytesDelta::new(8)),
            coverage: ProcessCoverage::Gap {
                reason: ProcessCoverageGap::EventsLost,
            },
        });
        assert_eq!(values, (-150, None, Some(8), "eventsLost"));
        let process: Vec<_> = process_custom_fields()
            .into_iter()
            .map(|field| field.name().to_owned())
            .collect();
        assert_eq!(process.len(), 14);
        for field in unattributed_fields() {
            assert!(!process.iter().any(|name| name == field.name()));
        }
    }
}
