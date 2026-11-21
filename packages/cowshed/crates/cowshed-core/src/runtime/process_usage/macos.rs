//! A life's own counters as XNU keeps them: `proc_pid_rusage(RUSAGE_INFO_V4)`, its CPU times in
//! Mach ticks, fenced to the life by its start time and, on a first read, its unique id.

use std::io;
use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::time::Instant;

use super::{OwnCounters, SampleError, Sampled, StartStamp, unit};
use crate::api::process::ProcessStorageIo;
use crate::api::resources::{CpuMicros, ResidentBytes, ResourceUnitError, StorageIoBytes};
use crate::runtime::process_tree::ProcessIdentity;

/// Read `process`'s own counters at the supervisor's sample tick `at`; the caller folds them.
/// XNU fences the read by the life's start, so the pidfd a Linux observer holds has no twin here
/// and `_held` is unread.
///
/// A first read names the pid alone. It is of `process` if the pid names `process`'s unique id
/// after the read: the observer saw that life before, and a life holds its pid until it is
/// reaped, so it held it during the read too. That read anchors the life's start; every later
/// read carries its own start, so it is fenced by the read itself, an exited life included.
pub fn sample(
    process: ProcessIdentity,
    _held: Option<BorrowedFd<'_>>,
    start: Option<StartStamp>,
    at: Instant,
) -> Result<Sampled, SampleError> {
    let failed = |source| SampleError::Read {
        pid: process.pid,
        source,
    };
    let pid =
        libc::pid_t::try_from(process.pid).map_err(|error| failed(io::Error::other(error)))?;
    let Some(counters) = own_counters(pid).map_err(failed)? else {
        return Ok(Sampled::Gone);
    };
    match start {
        Some(start) if counters.started != start => return Ok(Sampled::Gone),
        Some(_) => {}
        None => match crate::runtime::job_groups::process_record(pid).map_err(failed)? {
            Some(record) if record.unique_id == process.birth.0 => {}
            Some(_) => return Ok(Sampled::Gone),
            // Exited, reaped or not: no unique id can be asked of it now.
            None => return Ok(Sampled::Unproven),
        },
    }
    Ok(Sampled::Read(counters.reading(at)))
}

/// `proc_pid_rusage(RUSAGE_INFO_V4)`: the process's own CPU (not `ri_child_*`, the children it
/// reaped), resident size and disk I/O bytes (`ri_diskio_*`: the I/O it issued to disk, which
/// its cache hits issue none of), which still answers for an exited process its parent has not
/// reaped. `None` once it is reaped. Its times are Mach ticks (xnu `task_power_info_locked`
/// stores `rm_time_mach` unconverted; measured 125/3 ns per tick on Apple silicon), converted
/// once by the timebase. An exited process (`ri_proc_exit_abstime` set) answers with the size
/// it last held; it holds none.
pub(super) fn own_counters(pid: libc::pid_t) -> io::Result<Option<OwnCounters>> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
    // SAFETY: `info` is writable storage of the size the V4 flavor writes.
    let result =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V4, info.as_mut_ptr().cast()) };
    if result != 0 {
        let error = io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: a successful call filled the V4 record.
    let info = unsafe { info.assume_init() };
    let tick = MachTick::read().map_err(io::Error::other)?;
    let cpu = |ticks| tick.micros(ticks).map_err(unit);
    let exited = info.ri_proc_exit_abstime != 0;
    Ok(Some(OwnCounters {
        started: StartStamp(info.ri_proc_start_abstime),
        cpu_user: cpu(info.ri_user_time)?,
        cpu_sys: cpu(info.ri_system_time)?,
        resident: if exited {
            ResidentBytes::ZERO
        } else {
            ResidentBytes::new(info.ri_resident_size).map_err(unit)?
        },
        io: ProcessStorageIo::Read {
            read_bytes: StorageIoBytes::new(info.ri_diskio_bytesread).map_err(unit)?,
            write_bytes: StorageIoBytes::new(info.ri_diskio_byteswritten).map_err(unit)?,
        },
        exited,
    }))
}

/// The current process's observed disk-write counter, not an image or job total.
/// Shares the native sampler with process spans; no children or later async writes.
pub(crate) fn own_process_write_bytes() -> io::Result<u64> {
    // SAFETY: getpid has no pointer arguments or preconditions.
    let counters = own_counters(unsafe { libc::getpid() })?
        .ok_or_else(|| io::Error::other("current process has no rusage record"))?;
    match counters.io {
        ProcessStorageIo::Read { write_bytes, .. } => Ok(write_bytes.get()),
        ProcessStorageIo::Unavailable { reason } => Err(io::Error::other(format!(
            "process I/O unavailable: {reason:?}"
        ))),
    }
}

/// `struct mach_timebase_info` (`<mach/mach_time.h>`): one tick is `numer / denom` ns. The
/// `libc` crate's declaration is deprecated in favor of another crate; this is all of it.
#[repr(C)]
struct MachTimebase {
    numer: u32,
    denom: u32,
}

unsafe extern "C" {
    fn mach_timebase_info(info: *mut MachTimebase) -> libc::c_int;
}

/// Why the length of a Mach tick could not be read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TimebaseError {
    #[error("mach_timebase_info failed with kern_return_t {0}")]
    Failed(libc::c_int),
    #[error("mach_timebase_info reported a zero denominator")]
    ZeroDenominator,
}

/// The length of one Mach tick: `numerator / denominator` nanoseconds (125/3 on Apple silicon,
/// 1/1 on Intel). Kernel CPU times kept in ticks are converted through it once, never read as
/// nanoseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MachTick {
    pub numerator: u32,
    pub denominator: NonZeroU32,
}

impl MachTick {
    pub fn read() -> Result<Self, TimebaseError> {
        let mut timebase = MachTimebase { numer: 0, denom: 0 };
        // SAFETY: one writable timebase record.
        let result = unsafe { mach_timebase_info(&mut timebase) };
        if result != libc::KERN_SUCCESS {
            return Err(TimebaseError::Failed(result));
        }
        Ok(Self {
            numerator: timebase.numer,
            denominator: NonZeroU32::new(timebase.denom).ok_or(TimebaseError::ZeroDenominator)?,
        })
    }

    /// `ticks` of this length, in whole microseconds.
    pub fn micros(self, ticks: u64) -> Result<CpuMicros, ResourceUnitError> {
        CpuMicros::of_ticks(ticks, self.numerator, self.denominator)
    }
}
