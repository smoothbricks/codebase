//! The named units of job and per-process resource readings (07_api.md "Job monitoring").
//!
//! Each unit has a private field and a checked constructor; its wire projection is the bare
//! number. Its scalar declaration gives generated validators the same safe-integer boundary
//! as its Rust constructor, so no projection admits a value another projection refuses.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use super::dto::MAX_JOB_ID;

/// The largest integer Rust, JSON and a JavaScript `number` all hold exactly.
pub const MAX_EXACT_INTEGER: u64 = MAX_JOB_ID;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResourceUnitError {
    #[error(
        "{unit} {value} exceeds {MAX_EXACT_INTEGER}, the largest value every projection holds exactly"
    )]
    Inexact { unit: &'static str, value: u128 },
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

fn exact(unit: &'static str, value: u128) -> Result<u64, ResourceUnitError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or(ResourceUnitError::Inexact { unit, value })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::{CpuMicros, MAX_EXACT_INTEGER, ResidentBytes, StorageIoBytes};

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
}
