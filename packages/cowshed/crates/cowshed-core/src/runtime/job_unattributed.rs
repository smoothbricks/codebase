//! A job's unattributed usage, as its span states it (13_telemetry.md, "Process-tree spans";
//! 07_api.md, "Complete job accounting and observation reconciliation").
//!
//! A job's totals come from a source that never asks which processes were observed, so the two
//! can disagree. The terminal sample keeps the disagreement as a [`UsageReconciliation`]: signed
//! differences in named units, each present only where the source and the observed processes both
//! had a count to compare, and the coverage the observed processes had of the job.
//! [`UnattributedUsage`] is that reconciliation whole, beside the source that counted the totals;
//! the job's span carries it as one row of kind [`JOB_UNATTRIBUTED_MESSAGE`].
//!
//! The projection keeps every distinction the reconciliation states:
//!
//! - A difference stays signed. Observation can see work its source has not counted yet, and the
//!   two are read at different instants, so a negative difference is a fact, never an error to
//!   clamp.
//! - A comparison nothing could make stays absent, never zero.
//! - A coverage gap is stated even when every difference is zero, with the pid its evidence names.
//!   Only a complete tree that agrees with the totals in every comparison the source could make
//!   states nothing.
//! - The row names no leaf. Which process led the job is the sample's own
//!   [`JobLeafAttribution`](crate::api::process::JobLeafAttribution), judged by the supervisor's
//!   threshold; nothing infers one from what was missed.

use crate::api::process::{ProcessCoverage, ProcessCoverageGap};
use crate::api::resources::{JobAccounting, UsageReconciliation};
use crate::storage::trace_columns::trace_enum;

/// The message of the unattributed-usage row: with its entry type, the row's fixed kind.
pub const JOB_UNATTRIBUTED_MESSAGE: &str = "job.unattributed";

trace_enum! {
    /// The independent source that counted a job's totals: which [`JobAccounting`] variant held
    /// them, named as its wire kind.
    pub enum AccountingSource {
        LinuxCgroupV2 = "linuxCgroupV2",
        MacOsRusageChildren = "macOsRusageChildren",
    }
}

impl AccountingSource {
    /// The source that counted `accounting`'s totals.
    pub const fn of(accounting: &JobAccounting) -> Self {
        match accounting {
            JobAccounting::LinuxCgroupV2 { .. } => Self::LinuxCgroupV2,
            JobAccounting::MacOsRusageChildren { .. } => Self::MacOsRusageChildren,
        }
    }
}

trace_enum! {
    /// What a [`ProcessCoverage`] says, less the pid a gap's evidence names: one value for the
    /// complete tree and one for each way a tree was found incomplete, named as its wire kind.
    pub enum CoverageKind {
        Complete = "complete",
        EventsLost = "eventsLost",
        UnobservedBirth = "unobservedBirth",
        UnobservedExit = "unobservedExit",
        UncountedFork = "uncountedFork",
        UnreadImage = "unreadImage",
        UnreadFinalUsage = "unreadFinalUsage",
    }
}

impl CoverageKind {
    /// `coverage`'s kind, and the pid its evidence names: the observed process a gap is about.
    pub const fn of(coverage: ProcessCoverage) -> (Self, Option<u32>) {
        match coverage {
            ProcessCoverage::Complete => (Self::Complete, None),
            ProcessCoverage::Gap { reason } => match reason {
                ProcessCoverageGap::EventsLost => (Self::EventsLost, None),
                ProcessCoverageGap::UnobservedBirth { pid } => (Self::UnobservedBirth, Some(pid)),
                ProcessCoverageGap::UnobservedExit { pid } => (Self::UnobservedExit, Some(pid)),
                ProcessCoverageGap::UncountedFork { pid } => (Self::UncountedFork, Some(pid)),
                ProcessCoverageGap::UnreadImage { pid } => (Self::UnreadImage, Some(pid)),
                ProcessCoverageGap::UnreadFinalUsage { pid } => (Self::UnreadFinalUsage, Some(pid)),
            },
        }
    }
}

/// A job's terminal reconciliation, stated as unattributed: the source that counted its totals,
/// and what those totals found of the observed processes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnattributedUsage {
    pub source: AccountingSource,
    pub reconciliation: UsageReconciliation,
}

impl UnattributedUsage {
    /// What `accounting` states as unattributed, or `None` when it states nothing: the observed
    /// processes covered the job completely and agreed with its totals in every comparison the
    /// source could make. Any other reconciliation is stated whole, whatever the sign or size of
    /// its differences: a gap with every difference zero is still a gap.
    pub fn of(accounting: &JobAccounting) -> Option<Self> {
        let reconciliation = *accounting.reconciliation();
        let agrees = reconciliation.coverage == ProcessCoverage::Complete
            && reconciliation.cpu_us.get() == 0
            && reconciliation
                .io_read_bytes
                .is_none_or(|delta| delta.get() == 0)
            && reconciliation
                .io_write_bytes
                .is_none_or(|delta| delta.get() == 0);
        (!agrees).then(|| Self {
            source: AccountingSource::of(accounting),
            reconciliation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::resources::{
        ChargedMemoryBytes, ChargedMemoryUsage, CpuMicros, CpuMicrosDelta, CpuTotals,
        StorageIoBytes, StorageIoBytesDelta, StorageIoTotals,
    };
    use crate::storage::trace_columns::TraceEnum as _;

    const GAPS: [ProcessCoverageGap; 6] = [
        ProcessCoverageGap::EventsLost,
        ProcessCoverageGap::UnobservedBirth { pid: 11 },
        ProcessCoverageGap::UnobservedExit { pid: 12 },
        ProcessCoverageGap::UncountedFork { pid: 13 },
        ProcessCoverageGap::UnreadImage { pid: 14 },
        ProcessCoverageGap::UnreadFinalUsage { pid: 15 },
    ];

    fn totals() -> CpuTotals {
        CpuTotals {
            user_us: CpuMicros::new(1_250_000).unwrap(),
            sys_us: CpuMicros::new(80_000).unwrap(),
        }
    }

    /// A reconciliation of `cpu_us`, and of `io` as `(read, write)` where the source counts bytes.
    fn reconciliation(
        cpu_us: i64,
        io: Option<(i64, i64)>,
        coverage: ProcessCoverage,
    ) -> UsageReconciliation {
        UsageReconciliation {
            cpu_us: CpuMicrosDelta::new(cpu_us).unwrap(),
            io_read_bytes: io.map(|(read, _)| StorageIoBytesDelta::new(read).unwrap()),
            io_write_bytes: io.map(|(_, write)| StorageIoBytesDelta::new(write).unwrap()),
            coverage,
        }
    }

    fn rusage(reconciliation: UsageReconciliation) -> JobAccounting {
        JobAccounting::MacOsRusageChildren {
            cpu: totals(),
            io: None,
            reconciliation,
        }
    }

    fn cgroup(reconciliation: UsageReconciliation) -> JobAccounting {
        JobAccounting::LinuxCgroupV2 {
            cpu: totals(),
            io: StorageIoTotals {
                read_bytes: StorageIoBytes::new(8192).unwrap(),
                write_bytes: StorageIoBytes::new(4096).unwrap(),
            },
            charged_memory: ChargedMemoryUsage {
                current_bytes: ChargedMemoryBytes::new(1 << 20).unwrap(),
                peak_bytes: ChargedMemoryBytes::new(2 << 20).unwrap(),
            },
            reconciliation,
        }
    }

    #[test]
    fn a_complete_tree_that_agrees_in_every_comparison_states_nothing() {
        let agreed = |io| reconciliation(0, io, ProcessCoverage::Complete);
        assert_eq!(UnattributedUsage::of(&rusage(agreed(None))), None);
        assert_eq!(UnattributedUsage::of(&cgroup(agreed(Some((0, 0))))), None);
    }

    #[test]
    fn any_difference_is_stated_whole_and_signed() {
        for sign in [1, -1] {
            for (cpu_us, io) in [
                (sign, Some((0, 0))),
                (0, Some((sign, 0))),
                (0, Some((0, sign))),
                (sign * 1_500_000, Some((sign * 4096, sign * 2048))),
            ] {
                let observed = reconciliation(cpu_us, io, ProcessCoverage::Complete);
                assert_eq!(
                    UnattributedUsage::of(&cgroup(observed)),
                    Some(UnattributedUsage {
                        source: AccountingSource::LinuxCgroupV2,
                        reconciliation: observed,
                    }),
                    "{cpu_us} us, {io:?} bytes"
                );
            }
        }
    }

    #[test]
    fn a_gap_is_stated_though_every_difference_is_zero() {
        for reason in GAPS {
            let coverage = ProcessCoverage::Gap { reason };
            for accounting in [
                rusage(reconciliation(0, None, coverage)),
                cgroup(reconciliation(0, Some((0, 0)), coverage)),
            ] {
                let usage = UnattributedUsage::of(&accounting).expect("a gap is stated");
                assert_eq!(usage.reconciliation.coverage, coverage);
                assert_eq!(usage.reconciliation.cpu_us.get(), 0);
            }
        }
    }

    #[test]
    fn an_absent_comparison_stays_absent_beside_a_present_zero() {
        let gap = ProcessCoverage::Gap {
            reason: ProcessCoverageGap::EventsLost,
        };
        let absent = UnattributedUsage::of(&rusage(reconciliation(0, None, gap))).unwrap();
        let zero = UnattributedUsage::of(&cgroup(reconciliation(0, Some((0, 0)), gap))).unwrap();
        assert_eq!(absent.reconciliation.io_read_bytes, None);
        assert_eq!(absent.reconciliation.io_write_bytes, None);
        let zero_bytes = Some(StorageIoBytesDelta::new(0).unwrap());
        assert_eq!(zero.reconciliation.io_read_bytes, zero_bytes);
        assert_eq!(zero.reconciliation.io_write_bytes, zero_bytes);
    }

    #[test]
    fn the_source_is_the_accounting_variant_that_counted_the_totals() {
        let observed = reconciliation(7, None, ProcessCoverage::Complete);
        assert_eq!(
            AccountingSource::of(&rusage(observed)),
            AccountingSource::MacOsRusageChildren
        );
        assert_eq!(
            AccountingSource::of(&cgroup(observed)),
            AccountingSource::LinuxCgroupV2
        );
    }

    #[test]
    fn a_coverage_splits_into_its_kind_and_the_pid_its_evidence_names() {
        assert_eq!(
            CoverageKind::of(ProcessCoverage::Complete),
            (CoverageKind::Complete, None)
        );
        let kinds = GAPS.map(|reason| CoverageKind::of(ProcessCoverage::Gap { reason }));
        assert_eq!(
            kinds,
            [
                (CoverageKind::EventsLost, None),
                (CoverageKind::UnobservedBirth, Some(11)),
                (CoverageKind::UnobservedExit, Some(12)),
                (CoverageKind::UncountedFork, Some(13)),
                (CoverageKind::UnreadImage, Some(14)),
                (CoverageKind::UnreadFinalUsage, Some(15)),
            ]
        );
    }

    /// The trace's enums are the wire's kinds, so one vocabulary names a source and a coverage in
    /// the API records and in the span.
    #[test]
    fn the_trace_names_are_the_wire_kinds() {
        let kind = |value: serde_json::Value| value["kind"].as_str().unwrap().to_owned();
        let observed = reconciliation(0, None, ProcessCoverage::Complete);
        for accounting in [rusage(observed), cgroup(observed)] {
            assert_eq!(
                AccountingSource::of(&accounting).name(),
                kind(serde_json::to_value(accounting).unwrap())
            );
        }
        assert_eq!(
            CoverageKind::Complete.name(),
            kind(serde_json::to_value(ProcessCoverage::Complete).unwrap())
        );
        for reason in GAPS {
            let coverage = ProcessCoverage::Gap { reason };
            let wire = serde_json::to_value(coverage).unwrap();
            assert_eq!(
                CoverageKind::of(coverage).0.name(),
                kind(wire["reason"].clone())
            );
        }
    }
}
