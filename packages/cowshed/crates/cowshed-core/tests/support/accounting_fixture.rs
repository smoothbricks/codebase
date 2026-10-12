//! The terminal resource sample the span tests hand the production span writer: what a job's
//! accounting source and its processes' resident memory state once the job has ended
//! (13_telemetry.md, "Process-tree spans"). Shared by the test binaries that write job spans
//! (`#[path]`-included, not a crate).

use cowshed_core::api::{
    ChargedMemoryBytes, ChargedMemoryUsage, CpuMicros, CpuMicrosDelta, CpuTotals, HostLoadSample,
    JobAccounting, JobId, JobLeafAttribution, JobResourceSample, JobStreamWatermark,
    JobVolumeUsage, OneCoreCpuPercent, ProcessCoverage, ProcessCoverageGap, ResidentBytes,
    StorageIoBytes, StorageIoBytesDelta, StorageIoTotals, StreamBytes, StreamLines,
    UsageReconciliation, UtcTimestamp, VolumeUnavailable, VolumeUsage, WallMicros,
};

/// The terminal sample of job `job_id`: its processes held `resident` bytes at it and
/// `resident_peak` at most, and its accounting source is a cgroup charged `charged` -- its
/// `(current, peak)` bytes -- or, for `None`, the macOS rusage source, which has no
/// charged-memory counter. Every other reading is quiet.
pub(crate) fn terminal_sample(
    job_id: JobId,
    charged: Option<(u64, u64)>,
    resident: u64,
    resident_peak: u64,
) -> JobResourceSample {
    let wall = WallMicros::new(1_000_000).expect("a wall time");
    let zero = CpuMicros::new(0).expect("a CPU time");
    let cpu = CpuTotals {
        user_us: zero,
        sys_us: zero,
    };
    let quiet = JobStreamWatermark {
        bytes: StreamBytes::new(0).expect("bytes"),
        lines: StreamLines::new(0).expect("lines"),
    };
    let host = HostLoadSample::new(1.25, 8).expect("a host sample");
    let accounting = match charged {
        Some((current, peak)) => JobAccounting::LinuxCgroupV2 {
            cpu,
            io: StorageIoTotals {
                read_bytes: StorageIoBytes::new(0).expect("bytes"),
                write_bytes: StorageIoBytes::new(0).expect("bytes"),
            },
            charged_memory: ChargedMemoryUsage {
                current_bytes: ChargedMemoryBytes::new(current).expect("charged bytes"),
                peak_bytes: ChargedMemoryBytes::new(peak).expect("charged bytes"),
            },
            reconciliation: UsageReconciliation {
                cpu_us: CpuMicrosDelta::new(0).expect("a CPU delta"),
                io_read_bytes: Some(StorageIoBytesDelta::new(0).expect("a byte delta")),
                io_write_bytes: Some(StorageIoBytesDelta::new(0).expect("a byte delta")),
                coverage: ProcessCoverage::Complete,
            },
        },
        None => JobAccounting::MacOsRusageChildren {
            cpu,
            io: None,
            reconciliation: UsageReconciliation {
                cpu_us: CpuMicrosDelta::new(0).expect("a CPU delta"),
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: ProcessCoverage::Gap {
                    reason: ProcessCoverageGap::UncountedFork { pid: 100 },
                },
            },
        },
    };
    JobResourceSample {
        job_id,
        sampled_at: UtcTimestamp::new("2026-10-07T00:00:01Z").expect("a timestamp"),
        wall_ms: wall.millis(),
        wall_us: wall,
        leader_pid: 100,
        members: Vec::new(),
        leaf: JobLeafAttribution::NoAccountedCpu,
        cpu_user_ms: zero.millis(),
        cpu_sys_ms: zero.millis(),
        cpu_pct: OneCoreCpuPercent::ZERO,
        host_start: host,
        host,
        rss_bytes: ResidentBytes::new(resident).expect("resident bytes"),
        rss_peak_bytes: ResidentBytes::new(resident_peak).expect("resident bytes"),
        accounting,
        volumes: JobVolumeUsage {
            workspace: VolumeUsage::Unavailable {
                reason: VolumeUnavailable::Unconfigured,
            },
            build: None,
        },
        stdout: quiet,
        stderr: quiet,
    }
}
