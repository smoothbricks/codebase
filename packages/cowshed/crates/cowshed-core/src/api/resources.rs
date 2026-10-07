//! The named units of job and per-process resource readings (07_api.md "Job monitoring").
//!
//! Each unit has a private field and a checked constructor; its wire projection is the bare
//! number. Its scalar declaration gives generated validators the same safe-integer boundary
//! as its Rust constructor, so no projection admits a value another projection refuses.

use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::dto::{JobId, MAX_JOB_ID, UtcTimestamp};

/// The largest integer Rust, JSON and a JavaScript `number` all hold exactly.
pub const MAX_EXACT_INTEGER: u64 = MAX_JOB_ID;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ResourceUnitError {
    #[error(
        "{unit} {value} exceeds {MAX_EXACT_INTEGER}, the largest value every projection holds exactly"
    )]
    Inexact { unit: &'static str, value: u128 },
    #[error("host load1 must be finite and non-negative, got {value}")]
    InvalidHostLoad { value: f64 },
    #[error("host core count must be positive")]
    ZeroHostCores,
}

/// CPU time in microseconds, cumulative from the start of whatever it counts: one process's own
/// user or system time, or a job's total from its accounting source.
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct CpuMicros(u64);

impl CpuMicros {
    pub fn new(value: u64) -> Result<Self, ResourceUnitError> {
        exact("cpuUs", u128::from(value)).map(Self)
    }

    /// A count of `ticks`, each `numerator / denominator` nanoseconds long, in whole
    /// microseconds: the one conversion a kernel's tick counter takes.
    pub fn of_ticks(
        ticks: u64,
        numerator: u32,
        denominator: NonZeroU32,
    ) -> Result<Self, ResourceUnitError> {
        let nanoseconds = u128::from(ticks) * u128::from(numerator);
        exact(
            "cpuUs",
            nanoseconds / (u128::from(denominator.get()) * 1_000),
        )
        .map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The CPU time `self` and `other` count together.
    pub fn checked_add(self, other: Self) -> Result<Self, ResourceUnitError> {
        exact("cpuUs", u128::from(self.0) + u128::from(other.0)).map(Self)
    }
}

impl TryFrom<u64> for CpuMicros {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CpuMicros> for u64 {
    fn from(value: CpuMicros) -> Self {
        value.0
    }
}

/// Bytes of memory resident in RAM: what processes hold now, never memory charged to them
/// elsewhere (a cgroup's `memory.current` counts page cache and kernel charges too).
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct ResidentBytes(u64);

impl ResidentBytes {
    pub const ZERO: Self = Self(0);

    pub fn new(value: u64) -> Result<Self, ResourceUnitError> {
        exact("rssBytes", u128::from(value)).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// What `self` and `other` hold together.
    pub fn checked_add(self, other: Self) -> Result<Self, ResourceUnitError> {
        exact("rssBytes", u128::from(self.0) + u128::from(other.0)).map(Self)
    }
}

impl TryFrom<u64> for ResidentBytes {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ResidentBytes> for u64 {
    fn from(value: ResidentBytes) -> Self {
        value.0
    }
}

/// Bytes a kernel counted as moved to or from storage: never logical reads its cache served,
/// volume-allocation deltas, or operation counts converted into bytes.
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct StorageIoBytes(u64);

impl StorageIoBytes {
    pub fn new(value: u64) -> Result<Self, ResourceUnitError> {
        exact("ioBytes", u128::from(value)).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for StorageIoBytes {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<StorageIoBytes> for u64 {
    fn from(value: StorageIoBytes) -> Self {
        value.0
    }
}

/// User and system CPU time, each cumulative from the start of whatever it counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CpuTotals {
    pub user_us: CpuMicros,
    pub sys_us: CpuMicros,
}

impl CpuTotals {
    pub const ZERO: Self = Self {
        user_us: CpuMicros(0),
        sys_us: CpuMicros(0),
    };

    /// What `self` and `other` count together, each part summed on its own.
    pub fn checked_add(self, other: Self) -> Result<Self, ResourceUnitError> {
        Ok(Self {
            user_us: self.user_us.checked_add(other.user_us)?,
            sys_us: self.sys_us.checked_add(other.sys_us)?,
        })
    }
}

/// Bytes a job's accounting source counted as read from and written to storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StorageIoTotals {
    pub read_bytes: StorageIoBytes,
    pub write_bytes: StorageIoBytes,
}

/// A job's complete totals and the independent source that counted them: never a sum of the
/// processes a sampler happened to see, which misses every descendant born and reaped between
/// two samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum JobAccounting {
    /// macOS `proc_pid_rusage` of each process that led the job, read while its parent held it
    /// unreaped: its own CPU (`ri_user_time`/`ri_system_time`) plus that of every child it
    /// reaped (`ri_child_user_time`/`ri_child_system_time`, which carry their own reaped
    /// children's in turn). A cold host's activation and the command each count once, the
    /// activation up to its end. A descendant still running, or one orphaned and reaped by
    /// another process, is not in it yet.
    MacOsRusageChildren {
        cpu: CpuTotals,
        /// Always absent: the children accumulators carry no disk I/O bytes, so the source has
        /// no job total. Never zero in its place, and never `ri_child_pageins` operations
        /// converted into bytes.
        io: Option<StorageIoTotals>,
    },
}

/// Elapsed wall time since the job's first owned process spawned, in microseconds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
pub struct WallMicros(u64);

impl WallMicros {
    pub fn new(value: u64) -> Result<Self, ResourceUnitError> {
        exact("wallUs", u128::from(value)).map(Self)
    }

    pub fn of(elapsed: Duration) -> Result<Self, ResourceUnitError> {
        exact("wallUs", elapsed.as_micros()).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The whole milliseconds this duration holds: its display projection.
    pub const fn millis(self) -> WallMillis {
        WallMillis(self.0 / 1_000)
    }
}

impl TryFrom<u64> for WallMicros {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<WallMicros> for u64 {
    fn from(value: WallMicros) -> Self {
        value.0
    }
}

/// Elapsed wall time in whole milliseconds; only [`WallMicros::millis`] makes one.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
pub struct WallMillis(u64);

impl WallMillis {
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for WallMillis {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        exact("wallMs", u128::from(value)).map(Self)
    }
}

impl From<WallMillis> for u64 {
    fn from(value: WallMillis) -> Self {
        value.0
    }
}

fn exact(unit: &'static str, value: u128) -> Result<u64, ResourceUnitError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or(ResourceUnitError::Inexact { unit, value })
}

/// The host's one-minute run-queue load, never a missing observation filled with zero. Finite:
/// the wire bound is the largest finite f64, so a projection refuses infinity as the constructor
/// does, and NaN fails the lower bound.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Minimum<0> & tags.Maximum<1.7976931348623157e308>")
)]
pub struct HostLoad1(f64);

impl HostLoad1 {
    pub fn new(value: f64) -> Result<Self, ResourceUnitError> {
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(ResourceUnitError::InvalidHostLoad { value })
        }
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

// NaN is the only non-reflexive f64 value; the constructor and decoder both refuse it.
impl Eq for HostLoad1 {}

impl TryFrom<f64> for HostLoad1 {
    type Error = ResourceUnitError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<HostLoad1> for f64 {
    fn from(value: HostLoad1) -> Self {
        value.get()
    }
}

/// Online host cores, independent of the current process's affinity mask.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<\"uint32\"> & tags.Minimum<1> & tags.Maximum<65535>")
)]
pub struct HostCores(NonZeroU16);

impl HostCores {
    pub fn new(value: u16) -> Result<Self, ResourceUnitError> {
        NonZeroU16::new(value)
            .map(Self)
            .ok_or(ResourceUnitError::ZeroHostCores)
    }

    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostLoadSample {
    pub load1: HostLoad1,
    pub cores: HostCores,
}

impl HostLoadSample {
    pub fn new(load1: f64, cores: u16) -> Result<Self, ResourceUnitError> {
        Ok(Self {
            load1: HostLoad1::new(load1)?,
            cores: HostCores::new(cores)?,
        })
    }
}

/// Bytes of one output stream the supervisor admitted: also the stream's next read cursor.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
pub struct StreamBytes(u64);

impl StreamBytes {
    pub fn new(value: u64) -> Result<Self, ResourceUnitError> {
        exact("bytes", u128::from(value)).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for StreamBytes {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<StreamBytes> for u64 {
    fn from(value: StreamBytes) -> Self {
        value.0
    }
}

/// Lines in one output stream's admitted bytes: one per `\n`, and one more for a trailing line
/// no `\n` ended.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(
    any(),
    cowshed_api(scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>")
)]
pub struct StreamLines(u64);

impl StreamLines {
    pub fn new(value: u64) -> Result<Self, ResourceUnitError> {
        exact("lines", u128::from(value)).map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for StreamLines {
    type Error = ResourceUnitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<StreamLines> for u64 {
    fn from(value: StreamLines) -> Self {
        value.0
    }
}

/// How far one output stream reached by the sample boundary, counted as the supervisor admitted
/// each chunk of it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobStreamWatermark {
    pub bytes: StreamBytes,
    pub lines: StreamLines,
}

impl JobStreamWatermark {
    /// Whether admitted bytes could hold these lines: every line holds at least one byte, and
    /// any byte is on a line.
    pub fn possible(&self) -> bool {
        self.lines.get() <= self.bytes.get() && (self.bytes.get() == 0) == (self.lines.get() == 0)
    }
}

/// What a job's processes cost, observed at `sampled_at`. A sample exists only once the job owns
/// a process: its shell activation on a cold host, otherwise its command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobResourceSample {
    /// The job sampled, retained so a standalone progress event or receipt keeps its identity.
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    /// `wall_us` in whole milliseconds.
    pub wall_ms: WallMillis,
    /// Elapsed since the first owned process spawned.
    pub wall_us: WallMicros,
    /// The job's observed leader: the activation's while a cold host activates, then the
    /// command's. Retained after it exits.
    pub leader_pid: u32,
    /// The pid of every running process in the job's group whose resident memory was read at
    /// this boundary, the leader's among them while it runs: the complete membership, never a
    /// truncated one. Empty once nothing of the group runs.
    pub members: Vec<u32>,
    /// Captured once with the first owned process, even when that process is activation.
    pub host_start: HostLoadSample,
    /// The host's load and online cores at this sample boundary.
    pub host: HostLoadSample,
    /// What the `members` held resident together, each read at this boundary.
    pub rss_bytes: ResidentBytes,
    /// The largest `rssBytes` this job has been sampled at, this sample's included: a group's
    /// peak, never the sum of its processes' separate peaks.
    pub rss_peak_bytes: ResidentBytes,
    /// The job's complete totals from its platform's independent source, as of this boundary:
    /// absent only where no complete source exists yet (Linux, until its cgroup v2 totals).
    pub accounting: Option<JobAccounting>,
    pub stdout: JobStreamWatermark,
    pub stderr: JobStreamWatermark,
}

impl JobResourceSample {
    /// Whether the sample's fields agree with each other: `wallMs` projects `wallUs`, no peak
    /// lies below the sample it includes, and each stream's bytes could hold its lines.
    pub fn consistent(&self) -> bool {
        self.wall_ms == self.wall_us.millis()
            && self.rss_bytes <= self.rss_peak_bytes
            && self.stdout.possible()
            && self.stderr.possible()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    fn length(denominator: u32) -> NonZeroU32 {
        NonZeroU32::new(denominator).expect("a tick length's denominator")
    }

    #[test]
    fn ticks_convert_once_through_their_rational_length() {
        // Apple silicon's timebase: 125/3 ns per tick. 6_000_569 ticks are 250_023_708 ns.
        assert_eq!(
            CpuMicros::of_ticks(6_000_569, 125, length(3)).map(CpuMicros::get),
            Ok(250_023)
        );
        // Linux `USER_HZ`: one tick is 10 ms.
        assert_eq!(
            CpuMicros::of_ticks(7, 10_000_000, length(1)).map(CpuMicros::get),
            Ok(70_000)
        );
        assert!(CpuMicros::of_ticks(u64::MAX, u32::MAX, length(1)).is_err());
        assert!(CpuMicros::new(MAX_EXACT_INTEGER + 1).is_err());
    }

    #[test]
    fn resource_units_share_the_safe_integer_boundary() {
        for value in [0, MAX_EXACT_INTEGER] {
            assert_eq!(CpuMicros::new(value).map(CpuMicros::get), Ok(value));
            assert_eq!(ResidentBytes::new(value).map(ResidentBytes::get), Ok(value));
            assert_eq!(
                StorageIoBytes::new(value).map(StorageIoBytes::get),
                Ok(value)
            );
        }
        for value in [MAX_EXACT_INTEGER + 1, u64::MAX] {
            assert!(CpuMicros::new(value).is_err());
            assert!(ResidentBytes::new(value).is_err());
            assert!(StorageIoBytes::new(value).is_err());
            let json = value.to_string();
            assert!(serde_json::from_str::<CpuMicros>(&json).is_err());
            assert!(serde_json::from_str::<ResidentBytes>(&json).is_err());
            assert!(serde_json::from_str::<StorageIoBytes>(&json).is_err());
        }
    }

    fn timestamp() -> UtcTimestamp {
        UtcTimestamp::new("2026-10-07T12:00:00Z").expect("timestamp")
    }

    fn host() -> HostLoadSample {
        HostLoadSample::new(1.25, 8).expect("host snapshot")
    }

    fn bytes(value: u64) -> ResidentBytes {
        ResidentBytes::new(value).expect("exact")
    }

    fn stream(bytes: u64, lines: u64) -> JobStreamWatermark {
        JobStreamWatermark {
            bytes: StreamBytes::new(bytes).expect("exact"),
            lines: StreamLines::new(lines).expect("exact"),
        }
    }

    fn sample(wall: WallMicros, stdout: JobStreamWatermark) -> JobResourceSample {
        JobResourceSample {
            job_id: JobId::new(3).expect("job"),
            sampled_at: timestamp(),
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: 99,
            members: vec![99, 100],
            host_start: host(),
            host: host(),
            rss_bytes: bytes(0),
            rss_peak_bytes: bytes(0),
            accounting: None,
            stdout,
            stderr: stream(0, 0),
        }
    }

    #[test]
    fn wall_milliseconds_project_the_microseconds() {
        let wall = WallMicros::of(Duration::from_micros(12_345_999)).expect("exact");
        let sample = JobResourceSample {
            job_id: JobId::new(7).expect("job"),
            sampled_at: timestamp(),
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: 41,
            members: vec![41],
            host_start: host(),
            host: host(),
            rss_bytes: bytes(4096),
            rss_peak_bytes: bytes(8192),
            accounting: None,
            stdout: stream(0, 0),
            stderr: stream(0, 0),
        };
        assert_eq!(
            (sample.wall_us.get(), sample.wall_ms.get()),
            (12_345_999, 12_345)
        );
        assert!(sample.consistent());
    }

    #[test]
    fn a_peak_below_its_own_sample_is_inconsistent() {
        let wall = WallMicros::new(1).expect("exact");
        let sample = JobResourceSample {
            job_id: JobId::new(7).expect("job"),
            sampled_at: timestamp(),
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: 41,
            members: vec![41],
            host_start: host(),
            host: host(),
            rss_bytes: bytes(8192),
            rss_peak_bytes: bytes(4096),
            accounting: None,
            stdout: stream(0, 0),
            stderr: stream(0, 0),
        };
        assert!(!sample.consistent());
    }

    #[test]
    fn resident_bytes_no_projection_holds_exactly_are_an_error() {
        let inexact = Err(ResourceUnitError::Inexact {
            unit: "rssBytes",
            value: u128::from(MAX_EXACT_INTEGER) + 1,
        });
        assert_eq!(ResidentBytes::new(MAX_EXACT_INTEGER + 1), inexact);
        assert_eq!(bytes(MAX_EXACT_INTEGER).checked_add(bytes(1)), inexact);
        assert_eq!(
            bytes(MAX_EXACT_INTEGER - 1).checked_add(bytes(1)),
            Ok(bytes(MAX_EXACT_INTEGER))
        );
    }

    #[test]
    fn a_duration_no_projection_holds_exactly_is_an_error() {
        let past = Duration::from_micros(MAX_EXACT_INTEGER) + Duration::from_micros(1);
        assert_eq!(
            WallMicros::of(past),
            Err(ResourceUnitError::Inexact {
                unit: "wallUs",
                value: u128::from(MAX_EXACT_INTEGER) + 1,
            })
        );
        assert!(WallMicros::of(Duration::MAX).is_err());
        for refused in [
            StreamBytes::new(MAX_EXACT_INTEGER + 1).map(StreamBytes::get),
            StreamLines::new(MAX_EXACT_INTEGER + 1).map(StreamLines::get),
        ] {
            assert!(refused.is_err(), "{refused:?}");
        }
    }

    #[test]
    fn a_streams_lines_must_fit_its_bytes() {
        let wall = WallMicros::new(1).expect("exact");
        for (bytes, lines) in [(0, 0), (1, 1), (5, 3), (5, 5)] {
            assert!(
                sample(wall, stream(bytes, lines)).consistent(),
                "{bytes} bytes hold {lines} lines"
            );
        }
        for (bytes, lines) in [(0, 1), (1, 0), (3, 4)] {
            assert!(
                !sample(wall, stream(bytes, lines)).consistent(),
                "{bytes} bytes cannot hold {lines} lines"
            );
        }
    }

    #[test]
    fn the_wire_is_camel_case_numbers_and_refuses_an_inexact_unit() {
        let wall = WallMicros::new(1_500).expect("exact");
        let sample = JobResourceSample {
            job_id: JobId::new(3).expect("job"),
            sampled_at: timestamp(),
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: 99,
            members: vec![99, 100],
            host_start: host(),
            host: host(),
            rss_bytes: bytes(1 << 20),
            rss_peak_bytes: bytes(3 << 20),
            accounting: Some(JobAccounting::MacOsRusageChildren {
                cpu: CpuTotals {
                    user_us: CpuMicros::new(1_250_000).expect("exact"),
                    sys_us: CpuMicros::new(80_000).expect("exact"),
                },
                io: None,
            }),
            stdout: stream(5, 3),
            stderr: stream(0, 0),
        };
        let json = serde_json::to_value(&sample).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "jobId": 3,
                "sampledAt": "2026-10-07T12:00:00Z",
                "wallMs": 1,
                "wallUs": 1_500,
                "leaderPid": 99,
                "members": [99, 100],
                "hostStart": { "load1": 1.25, "cores": 8 },
                "host": { "load1": 1.25, "cores": 8 },
                "rssBytes": 1_048_576,
                "rssPeakBytes": 3_145_728,
                "accounting": {
                    "kind": "macOsRusageChildren",
                    "cpu": {"userUs": 1_250_000, "sysUs": 80_000},
                    "io": null,
                },
                "stdout": {"bytes": 5, "lines": 3},
                "stderr": {"bytes": 0, "lines": 0},
            })
        );
        assert_eq!(
            serde_json::from_value::<JobResourceSample>(json.clone()).expect("round trip"),
            sample
        );
        for (pointer, inexact) in [
            ("/wallUs", serde_json::json!(MAX_EXACT_INTEGER + 1)),
            ("/rssBytes", serde_json::json!(MAX_EXACT_INTEGER + 1)),
            ("/rssPeakBytes", serde_json::json!(MAX_EXACT_INTEGER + 1)),
            (
                "/accounting/cpu/userUs",
                serde_json::json!(MAX_EXACT_INTEGER + 1),
            ),
            ("/stdout/bytes", serde_json::json!(MAX_EXACT_INTEGER + 1)),
            ("/stderr/lines", serde_json::json!(MAX_EXACT_INTEGER + 1)),
        ] {
            let mut refused = json.clone();
            *refused.pointer_mut(pointer).expect("field") = inexact;
            assert!(
                serde_json::from_value::<JobResourceSample>(refused).is_err(),
                "{pointer} is refused"
            );
        }
        for (pointer, field, value, why) in [
            (
                "/stdout",
                "chars",
                serde_json::json!(5),
                "a watermark has only bytes and lines",
            ),
            (
                "/accounting",
                "pageins",
                serde_json::json!(5),
                "no block-operation count stands in for bytes",
            ),
            (
                "/accounting/cpu",
                "childUs",
                serde_json::json!(5),
                "CPU totals are user and system",
            ),
        ] {
            let mut unknown = json.clone();
            unknown.pointer_mut(pointer).expect("object")[field] = value;
            assert!(
                serde_json::from_value::<JobResourceSample>(unknown).is_err(),
                "{why}"
            );
        }
        let mut sourceless = json;
        sourceless["accounting"]["kind"] = serde_json::json!("liveMembers");
        assert!(
            serde_json::from_value::<JobResourceSample>(sourceless).is_err(),
            "totals name a declared source"
        );
    }

    #[test]
    fn cpu_totals_add_part_by_part_within_the_exact_boundary() {
        let cpu = |user, sys| CpuTotals {
            user_us: CpuMicros::new(user).expect("exact"),
            sys_us: CpuMicros::new(sys).expect("exact"),
        };
        assert_eq!(cpu(300, 20).checked_add(cpu(700, 5)), Ok(cpu(1_000, 25)));
        assert_eq!(CpuTotals::ZERO.checked_add(cpu(1, 2)), Ok(cpu(1, 2)));
        assert_eq!(
            cpu(MAX_EXACT_INTEGER, 0).checked_add(cpu(1, 0)),
            Err(ResourceUnitError::Inexact {
                unit: "cpuUs",
                value: u128::from(MAX_EXACT_INTEGER) + 1,
            })
        );
    }
}

#[cfg(test)]
mod host_tests {
    use super::*;

    #[test]
    fn host_sample_wire_is_checked_numbers() {
        let sample = HostLoadSample::new(12.5, 8).expect("host snapshot");
        let json = serde_json::json!({ "load1": 12.5, "cores": 8 });
        assert_eq!(serde_json::to_value(sample).expect("serialize"), json);
        assert_eq!(
            serde_json::from_value::<HostLoadSample>(json).expect("decode"),
            sample
        );
        for invalid in [
            serde_json::json!({ "load1": -0.1, "cores": 8 }),
            serde_json::json!({ "load1": 1.0, "cores": 0 }),
            serde_json::json!({ "load1": 1.0, "cores": 65_536 }),
            serde_json::json!({ "load1": 1.0, "cores": 1.5 }),
        ] {
            assert!(serde_json::from_value::<HostLoadSample>(invalid).is_err());
        }
    }

    #[test]
    fn host_load_refuses_nonfinite_and_negative_values() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            assert!(HostLoad1::new(value).is_err());
        }
        assert_eq!(HostLoad1::new(0.0).expect("idle").get(), 0.0);
        assert_eq!(HostCores::new(u16::MAX).expect("maximum").get(), u16::MAX);
        assert!(HostCores::new(0).is_err());
    }
}
