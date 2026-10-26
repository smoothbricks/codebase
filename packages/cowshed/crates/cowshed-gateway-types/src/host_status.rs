//! What the host's kernel reports about load, memory pressure and swap, as plain data
//! (05_gateway.md "Host scheduling", "Status").
//!
//! The kernel schedules CPU, memory and I/O; cowshed reports what the kernel measured, never a
//! count of its own. Each reading is the kernel's answer or the reason the read failed: a failed
//! read is never a zero, because zero load, no swap and normal pressure are real answers.
//!
//! The memory-pressure value is the one the supervisor's background-start wait reads, so the
//! status and the wait name the same number.

use serde::{Deserialize, Serialize};

/// The largest integer Rust, JSON and a JavaScript `number` all hold exactly.
const MAX_EXACT_BYTES: u64 = (1 << 53) - 1;

/// A value a kernel reading cannot hold.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum HostValueError {
    #[error("a load average must be finite and non-negative, got {value}")]
    InvalidLoad { value: f64 },
    #[error("a pressure share is at most 10000 hundredths of a percent, got {value}")]
    PercentOutOfRange { value: u16 },
    #[error(
        "{value} bytes exceeds {MAX_EXACT_BYTES}, the largest value every projection holds exactly"
    )]
    InexactBytes { value: u64 },
    #[error("swap in use {used_bytes} bytes exceeds the {total_bytes} bytes configured")]
    SwapUsedExceedsTotal { used_bytes: u64, total_bytes: u64 },
}

/// One `getloadavg(3)` average: the run-queue length over its window. Finite and non-negative.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "get",
        scalar = "number & tags.Minimum<0> & tags.Maximum<1.7976931348623157e308>"
    )
)]
pub struct LoadAverage(f64);

impl LoadAverage {
    pub fn new(value: f64) -> Result<Self, HostValueError> {
        if value.is_finite() && value >= 0.0 {
            Ok(Self(value))
        } else {
            Err(HostValueError::InvalidLoad { value })
        }
    }

    pub const fn get(self) -> f64 {
        self.0
    }
}

// NaN is the only non-reflexive f64 value; the constructor and decoder both refuse it.
impl Eq for LoadAverage {}

impl TryFrom<f64> for LoadAverage {
    type Error = HostValueError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LoadAverage> for f64 {
    fn from(value: LoadAverage) -> Self {
        value.get()
    }
}

/// The host's load over the last 1, 5 and 15 minutes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadAverages {
    pub one: LoadAverage,
    pub five: LoadAverage,
    pub fifteen: LoadAverage,
}

/// A share in hundredths of a percent, as `/proc/pressure/*` prints it (`avg10=12.34` is 1234).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "get",
        scalar = "number & tags.Type<\"uint32\"> & tags.Maximum<10000>"
    )
)]
pub struct CentiPercent(u16);

impl CentiPercent {
    pub fn new(value: u16) -> Result<Self, HostValueError> {
        if value <= 10_000 {
            Ok(Self(value))
        } else {
            Err(HostValueError::PercentOutOfRange { value })
        }
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

impl TryFrom<u16> for CentiPercent {
    type Error = HostValueError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CentiPercent> for u16 {
    fn from(value: CentiPercent) -> Self {
        value.get()
    }
}

impl std::fmt::Display for CentiPercent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{:02}%", self.0 / 100, self.0 % 100)
    }
}

/// macOS `kern.memorystatus_vm_pressure_level`. The sysctl answers the dispatch level, not the
/// kernel's internal one: Normal 1, Warning 2 (the kernel's Warning and Urgent both), Critical 4
/// (`sysctl_memorystatus_vm_pressure_level` in XNU `bsd/kern/kern_memorystatus_notify.c`).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DarwinPressureLevel {
    Normal,
    Warning,
    Critical,
}

/// The host's memory pressure, as its kernel states it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MemoryPressure {
    /// macOS: the memorystatus pressure level.
    Darwin { level: DarwinPressureLevel },
    /// Linux: `/proc/pressure/memory` `some avg10`, the share of the last ten seconds in which at
    /// least one task stalled on memory.
    Psi { some_avg10: CentiPercent },
}

/// The one rendering the status and the supervisor's memory wait both print.
impl std::fmt::Display for MemoryPressure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Darwin { level } => {
                let level = match level {
                    DarwinPressureLevel::Normal => "normal",
                    DarwinPressureLevel::Warning => "warning",
                    DarwinPressureLevel::Critical => "critical",
                };
                write!(formatter, "level={level}")
            }
            Self::Psi { some_avg10 } => write!(formatter, "some-avg10={some_avg10}"),
        }
    }
}

/// A byte count of swap, exact in every projection.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "get",
        scalar = "number & tags.Type<'uint64'> & tags.Maximum<9007199254740991>"
    )
)]
pub struct SwapBytes(u64);

impl SwapBytes {
    pub fn new(value: u64) -> Result<Self, HostValueError> {
        if value <= MAX_EXACT_BYTES {
            Ok(Self(value))
        } else {
            Err(HostValueError::InexactBytes { value })
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for SwapBytes {
    type Error = HostValueError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SwapBytes> for u64 {
    fn from(value: SwapBytes) -> Self {
        value.get()
    }
}

/// Swap in use and swap configured: macOS `vm.swapusage`, Linux `/proc/meminfo`. Never more in
/// use than configured: a reading that says so is malformed, not clamped.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "SwapUsageWire", into = "SwapUsageWire")]
pub struct SwapUsage {
    used_bytes: SwapBytes,
    total_bytes: SwapBytes,
}

impl SwapUsage {
    pub fn new(used_bytes: u64, total_bytes: u64) -> Result<Self, HostValueError> {
        Self::try_from(SwapUsageWire {
            used_bytes: SwapBytes::new(used_bytes)?,
            total_bytes: SwapBytes::new(total_bytes)?,
        })
    }

    pub const fn used_bytes(self) -> SwapBytes {
        self.used_bytes
    }

    pub const fn total_bytes(self) -> SwapBytes {
        self.total_bytes
    }
}

/// [`SwapUsage`] as it travels: a decoded pair passes the same check a reader's does.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwapUsageWire {
    pub used_bytes: SwapBytes,
    pub total_bytes: SwapBytes,
}

impl TryFrom<SwapUsageWire> for SwapUsage {
    type Error = HostValueError;

    fn try_from(wire: SwapUsageWire) -> Result<Self, Self::Error> {
        if wire.used_bytes > wire.total_bytes {
            return Err(HostValueError::SwapUsedExceedsTotal {
                used_bytes: wire.used_bytes.get(),
                total_bytes: wire.total_bytes.get(),
            });
        }
        Ok(Self {
            used_bytes: wire.used_bytes,
            total_bytes: wire.total_bytes,
        })
    }
}

impl From<SwapUsage> for SwapUsageWire {
    fn from(usage: SwapUsage) -> Self {
        Self {
            used_bytes: usage.used_bytes,
            total_bytes: usage.total_bytes,
        }
    }
}

/// The host's load, or why the kernel did not report it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum LoadReading {
    Read { load: LoadAverages },
    Unread { error: String },
}

/// The host's memory pressure, or why it could not be read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MemoryPressureReading {
    Read { pressure: MemoryPressure },
    Unread { error: String },
}

/// The host's swap, or why it could not be read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SwapReading {
    Read { swap: SwapUsage },
    Unread { error: String },
}

/// The kernel readings `gateway status` reports; each answers for itself, so one failed read
/// never hides the others.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostReadings {
    pub load: LoadReading,
    pub memory_pressure: MemoryPressureReading,
    pub swap: SwapReading,
}
