//! One pure comparison of retained process-own usage with an independent job total.
//! A coverage gap never erases that total, clamps a signed difference, or invents an unseen
//! CPU winner (07_api.md, "Process-tree observations").

use crate::api::process::{
    CpuCoveragePermille, CpuMicrosDelta, JobLeafAttribution, JobProcessLeaf, JobProcessTree,
    ProcessStorageIo, StorageIoBytesDelta, UsageReconciliation,
};
use crate::api::resources::{
    CpuMicros, CpuTotals, ResourceUnitError, StorageIoBytes, StorageIoTotals,
};

/// Reconcile `tree` with `independent` CPU and optional storage-I/O totals. `required` is the
/// fraction, in thousandths, that observed own CPU must cover before a leaf is named. The same
/// inputs, including that fraction, replay to the same result.
pub fn reconcile(
    tree: &JobProcessTree,
    independent: CpuTotals,
    io: Option<StorageIoTotals>,
    required: CpuCoveragePermille,
) -> Result<(JobLeafAttribution, UsageReconciliation), ResourceUnitError> {
    let mut observed_cpu = CpuMicros::new(0)?;
    let mut observed_read = StorageIoBytes::new(0)?;
    let mut observed_written = StorageIoBytes::new(0)?;
    let mut io_complete = io.is_some();
    let mut winner: Option<(&crate::api::process::JobProcessSample, CpuMicros)> = None;
    for process in &tree.processes {
        let Some(usage) = process.usage else {
            io_complete = false;
            continue;
        };
        let cpu = usage.cpu_user_us.checked_add(usage.cpu_sys_us)?;
        observed_cpu = observed_cpu.checked_add(cpu)?;
        match usage.io {
            ProcessStorageIo::Read {
                read_bytes,
                write_bytes,
            } => {
                observed_read =
                    StorageIoBytes::new(observed_read.get().checked_add(read_bytes.get()).ok_or(
                        ResourceUnitError::Inexact {
                            unit: "observedIoRead",
                            value: u128::from(observed_read.get()) + u128::from(read_bytes.get()),
                        },
                    )?)?;
                observed_written = StorageIoBytes::new(
                    observed_written
                        .get()
                        .checked_add(write_bytes.get())
                        .ok_or(ResourceUnitError::Inexact {
                            unit: "observedIoWritten",
                            value: u128::from(observed_written.get())
                                + u128::from(write_bytes.get()),
                        })?,
                )?;
            }
            ProcessStorageIo::Unavailable { .. } => io_complete = false,
        }
        let wins = match &winner {
            None => true,
            Some((prior, prior_cpu)) => {
                cpu > *prior_cpu || (cpu == *prior_cpu && process.pid < prior.pid)
            }
        };
        if wins {
            winner = Some((process, cpu));
        }
    }
    let accounted = independent.user_us.checked_add(independent.sys_us)?;
    let cpu_us = signed_delta(observed_cpu.get(), accounted.get(), "cpuUs")?;
    let (io_read_bytes, io_write_bytes) = match io.filter(|_| io_complete) {
        Some(totals) => (
            Some(StorageIoBytesDelta::new(signed_delta(
                observed_read.get(),
                totals.read_bytes.get(),
                "ioReadBytes",
            )?)),
            Some(StorageIoBytesDelta::new(signed_delta(
                observed_written.get(),
                totals.write_bytes.get(),
                "ioWriteBytes",
            )?)),
        ),
        None => (None, None),
    };
    let reconciliation = UsageReconciliation {
        cpu_us: CpuMicrosDelta::new(cpu_us),
        io_read_bytes,
        io_write_bytes,
        coverage: tree.coverage.clone(),
    };
    let leaf = if accounted.get() == 0 {
        JobLeafAttribution::NoAccountedCpu
    } else {
        let fraction = u128::from(observed_cpu.get()) * 1000 / u128::from(accounted.get());
        let fraction = u16::try_from(fraction).map_err(|_| ResourceUnitError::Inexact {
            unit: "observedCpuPermille",
            value: fraction,
        })?;
        let fraction = CpuCoveragePermille::new(fraction);
        match winner {
            Some((process, _)) if fraction >= required && observed_cpu.get() > 0 => {
                JobLeafAttribution::Attributed {
                    leaf: JobProcessLeaf {
                        pid: process.pid,
                        program: process.program.clone(),
                        argv: process.argv.clone(),
                        truncated: process.truncated,
                    },
                    observed_cpu_permille: fraction,
                }
            }
            _ => JobLeafAttribution::Unattributed {
                observed_cpu_permille: fraction,
                required_permille: required,
            },
        }
    };
    Ok((leaf, reconciliation))
}

fn signed_delta(
    observed: u64,
    accounted: u64,
    unit: &'static str,
) -> Result<i64, ResourceUnitError> {
    let delta = i128::from(observed) - i128::from(accounted);
    i64::try_from(delta).map_err(|_| ResourceUnitError::InexactSigned { unit, value: delta })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::{CommandArg, JobId, UtcTimestamp};
    use crate::api::process::{
        JobProcessSample, LEAF_CPU_PERMILLE, ProcessCoverage, ProcessCoverageGap, ProcessStorageIo,
        ProcessUsage,
    };
    use crate::api::resources::{ResidentBytes, StorageIoBytes};

    fn tree(cpu: u64, coverage: ProcessCoverage) -> JobProcessTree {
        JobProcessTree {
            job_id: JobId::new(1).unwrap(),
            sampled_at: UtcTimestamp::new("2026-10-12T00:00:00Z").unwrap(),
            processes: vec![process(40, "/bin/winner", cpu)],
            coverage,
        }
    }

    fn process(pid: u32, program: &str, cpu: u64) -> JobProcessSample {
        JobProcessSample {
            pid,
            ppid: 1,
            program: program.to_owned(),
            argv: vec![CommandArg::new(program)],
            truncated: false,
            born_at: UtcTimestamp::new("2026-10-12T00:00:00Z").unwrap(),
            usage: Some(ProcessUsage {
                cpu_user_us: CpuMicros::new(cpu).unwrap(),
                cpu_sys_us: CpuMicros::new(0).unwrap(),
                busy: false,
                rss_bytes: ResidentBytes::new(1).unwrap(),
                rss_peak_bytes: ResidentBytes::new(1).unwrap(),
                io: ProcessStorageIo::Read {
                    read_bytes: StorageIoBytes::new(0).unwrap(),
                    write_bytes: StorageIoBytes::new(0).unwrap(),
                },
            }),
            blocked_on: None,
            exit: None,
        }
    }

    fn totals(cpu: u64) -> CpuTotals {
        CpuTotals {
            user_us: CpuMicros::new(cpu).unwrap(),
            sys_us: CpuMicros::new(0).unwrap(),
        }
    }

    #[test]
    fn coverage_crossing_the_configured_fraction_names_the_observed_winner() {
        let required = LEAF_CPU_PERMILLE;
        assert_eq!(required.get(), 900);
        for (observed, expect_named) in [(899, false), (900, true), (950, true)] {
            let gap = ProcessCoverage::Gap {
                reason: ProcessCoverageGap::EventsLost,
            };
            let (leaf, reconciliation) =
                reconcile(&tree(observed, gap.clone()), totals(1000), None, required).unwrap();
            let again =
                reconcile(&tree(observed, gap.clone()), totals(1000), None, required).unwrap();
            assert_eq!((&leaf, &reconciliation), (&again.0, &again.1));
            assert_eq!(reconciliation.coverage, gap);
            assert_eq!(
                reconciliation.cpu_us.get(),
                i64::try_from(observed).unwrap() - 1000
            );
            assert!(
                reconciliation.cpu_us.get() < 0,
                "the shortfall is not clamped away"
            );
            assert!(reconciliation.io_read_bytes.is_none());
            match (expect_named, leaf) {
                (
                    false,
                    JobLeafAttribution::Unattributed {
                        observed_cpu_permille,
                        required_permille,
                    },
                ) => {
                    assert_eq!(observed_cpu_permille.get(), observed as u16);
                    assert_eq!(required_permille, required);
                }
                (
                    true,
                    JobLeafAttribution::Attributed {
                        leaf,
                        observed_cpu_permille,
                    },
                ) => {
                    assert_eq!(leaf.pid, 40);
                    assert_eq!(observed_cpu_permille.get(), observed as u16);
                }
                (_, other) => panic!("observed {observed}: {other:?}"),
            }
        }
    }

    #[test]
    fn zero_accounted_cpu_names_no_leaf_and_keeps_a_signed_excess() {
        let (leaf, reconciliation) = reconcile(
            &tree(100, ProcessCoverage::Complete),
            totals(0),
            None,
            LEAF_CPU_PERMILLE,
        )
        .unwrap();
        assert_eq!(leaf, JobLeafAttribution::NoAccountedCpu);
        assert_eq!(reconciliation.cpu_us.get(), 100);
        let (none, _) = reconcile(
            &tree(0, ProcessCoverage::Complete),
            totals(0),
            None,
            LEAF_CPU_PERMILLE,
        )
        .unwrap();
        assert_eq!(none, JobLeafAttribution::NoAccountedCpu);
    }

    #[test]
    fn a_lower_pid_wins_a_tie_and_an_unseen_process_is_not_invented() {
        let mut observed = tree(
            500,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid: 1 },
            },
        );
        observed.processes.push(process(30, "/bin/other", 500));
        let (leaf, _) = reconcile(&observed, totals(2000), None, LEAF_CPU_PERMILLE).unwrap();
        match leaf {
            JobLeafAttribution::Unattributed {
                observed_cpu_permille,
                ..
            } => {
                assert_eq!(observed_cpu_permille.get(), 500);
            }
            other => panic!("below threshold must not name a seen stand-in: {other:?}"),
        }
        let (leaf, _) = reconcile(&observed, totals(1000), None, LEAF_CPU_PERMILLE).unwrap();
        match leaf {
            JobLeafAttribution::Attributed { leaf, .. } => assert_eq!(leaf.pid, 30),
            other => panic!("{other:?}"),
        }
    }
}
