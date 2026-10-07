//! What each observed process of a job has cost itself (07_api.md, "Process-tree observations"):
//! its own CPU, never that of the children it waited for, whether it was busy over the
//! supervisor's last sample window, and the memory it holds resident now and at most.
//!
//! The fold is pure: the supervisor's sampler is its only writer, and it applies each reading of
//! a life it read from the kernel. Reading the folded usage changes nothing, so a second reader
//! can neither reset the window nor see a different answer from the first. The kernel reads are
//! the thin shell below the fold; each is fenced to the one life it names.
//!
//! A life's usage is final only when it was read after the life exited (an exited, unreaped
//! process still answers). An exit with no such read keeps the last usage observed and makes
//! the usage coverage a gap ([`ProcessCoverageGap::UnreadFinalUsage`]). A life whose counters
//! were never read -- it was reaped before the sampler reached it -- has no usage, not zeroes:
//! the job's accounting source still counts what it cost.

use std::collections::HashMap;
use std::io;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use crate::api::process::ProcessIoUnavailable;
use crate::api::process::{BUSY_CPU_PERMILLE, ProcessCoverageGap, ProcessStorageIo, ProcessUsage};
use crate::api::resources::{CpuMicros, ResidentBytes, ResourceUnitError, StorageIoBytes};
use crate::runtime::process_tree::ProcessIdentity;

/// When a process started, on the kernel clock the counters were read with: macOS
/// `ri_proc_start_abstime`, Linux `starttime` clock ticks after boot. On macOS it tells the lives
/// of one pid apart, so a reading that carries a life's first start is of that life. A Linux
/// pid reused within one clock tick repeats it, so there it is only a consistency check: the
/// pidfd held for the life is the fence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StartStamp(pub u64);

/// One read of a life's own counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UsageReading {
    pub started: StartStamp,
    /// When the sampler read it.
    pub at: Instant,
    pub cpu_user: CpuMicros,
    pub cpu_sys: CpuMicros,
    /// What it holds resident now: nothing once it has exited.
    pub resident: ResidentBytes,
    pub io: ProcessStorageIo,
    /// Whether the kernel had already seen the life exit when it was read.
    pub exited: bool,
}

impl UsageReading {
    fn cpu_total(&self) -> u64 {
        // Each part is at most `MAX_EXACT_INTEGER`, so their sum holds in a u64.
        self.cpu_user.get() + self.cpu_sys.get()
    }
}

/// What the sampler tells the fold.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UsageObservation {
    Read {
        process: ProcessIdentity,
        reading: UsageReading,
    },
    /// The life exited: its last reading is final if it was read after the exit.
    Exited { process: ProcessIdentity },
}

/// A reading that contradicts what the fold holds: the fence or the sampler is wrong.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum UsageFoldError {
    #[error("a reading of process {pid} is of a life that started at {read:?}, not {first:?}")]
    OtherLife {
        pid: u32,
        first: StartStamp,
        read: StartStamp,
    },
    #[error("process {pid}'s {counter} fell from {before} to {after}")]
    Regressed {
        pid: u32,
        counter: &'static str,
        before: u64,
        after: u64,
    },
    #[error("process {pid} was read at an instant before its previous reading")]
    Backwards { pid: u32 },
    #[error("process {pid} was read or ended again after its exit")]
    AfterExit { pid: u32 },
    #[error("process {pid} was read running after a read that found it exited")]
    Revived { pid: u32 },
}

#[derive(Debug)]
struct Counted {
    latest: UsageReading,
    /// The reading the current window opened at: the last one a positive span after its
    /// predecessor.
    window: UsageReading,
    busy: bool,
    /// The most it was read holding.
    rss_peak: ResidentBytes,
}

#[derive(Debug, Default)]
struct Life {
    counted: Option<Counted>,
    exited: bool,
}

#[derive(Debug, Default)]
pub struct ProcessUsageFold {
    lives: HashMap<ProcessIdentity, Life>,
    /// The first exit whose usage was not read after it.
    gap: Option<ProcessCoverageGap>,
}

impl ProcessUsageFold {
    pub fn apply(&mut self, observation: UsageObservation) -> Result<(), UsageFoldError> {
        match observation {
            UsageObservation::Read { process, reading } => {
                let life = self.lives.entry(process).or_default();
                if life.exited {
                    return Err(UsageFoldError::AfterExit { pid: process.pid });
                }
                // A refused reading leaves the life as it was.
                let counted = match &life.counted {
                    None => Counted {
                        latest: reading,
                        window: reading,
                        // No span has passed in which it could have been busy.
                        busy: false,
                        rss_peak: reading.resident,
                    },
                    Some(counted) => counted.read(process.pid, reading)?,
                };
                life.counted = Some(counted);
                Ok(())
            }
            UsageObservation::Exited { process } => {
                let life = self.lives.entry(process).or_default();
                if life.exited {
                    return Err(UsageFoldError::AfterExit { pid: process.pid });
                }
                life.exited = true;
                let read_after_exit = life
                    .counted
                    .as_ref()
                    .is_some_and(|counted| counted.latest.exited);
                if !read_after_exit && self.gap.is_none() {
                    self.gap = Some(ProcessCoverageGap::UnreadFinalUsage { pid: process.pid });
                }
                Ok(())
            }
        }
    }

    /// `process`'s usage as last read; `None` for a life never read.
    pub fn usage(&self, process: ProcessIdentity) -> Option<ProcessUsage> {
        let counted = self.lives.get(&process)?.counted.as_ref()?;
        Some(ProcessUsage {
            cpu_user_us: counted.latest.cpu_user,
            cpu_sys_us: counted.latest.cpu_sys,
            busy: counted.busy,
            rss_bytes: counted.latest.resident,
            rss_peak_bytes: counted.rss_peak,
            io: counted.latest.io,
        })
    }

    /// The start a reading of `process` must carry: that of its first reading.
    pub fn start(&self, process: ProcessIdentity) -> Option<StartStamp> {
        let counted = self.lives.get(&process)?.counted.as_ref()?;
        Some(counted.latest.started)
    }

    pub fn exited(&self, process: ProcessIdentity) -> bool {
        self.lives.get(&process).is_some_and(|life| life.exited)
    }

    /// The first exit whose final usage was not read; `None` while every exit's was.
    pub fn gap(&self) -> Option<ProcessCoverageGap> {
        self.gap
    }
}

impl Counted {
    fn read(&self, pid: u32, reading: UsageReading) -> Result<Self, UsageFoldError> {
        let latest = self.latest;
        if latest.exited && !reading.exited {
            return Err(UsageFoldError::Revived { pid });
        }
        if reading.started != latest.started {
            return Err(UsageFoldError::OtherLife {
                pid,
                first: latest.started,
                read: reading.started,
            });
        }
        if reading.at < latest.at {
            return Err(UsageFoldError::Backwards { pid });
        }
        // Storage counters are compared only between two reads that both had them.
        let (read, written) = match (latest.io, reading.io) {
            (
                ProcessStorageIo::Read {
                    read_bytes: read_before,
                    write_bytes: written_before,
                },
                ProcessStorageIo::Read {
                    read_bytes,
                    write_bytes,
                },
            ) => (
                (read_before.get(), read_bytes.get()),
                (written_before.get(), write_bytes.get()),
            ),
            _ => ((0, 0), (0, 0)),
        };
        for (counter, before, after) in [
            (
                "user CPU microseconds",
                latest.cpu_user.get(),
                reading.cpu_user.get(),
            ),
            (
                "system CPU microseconds",
                latest.cpu_sys.get(),
                reading.cpu_sys.get(),
            ),
            ("storage read bytes", read.0, read.1),
            ("storage write bytes", written.0, written.1),
        ] {
            if after < before {
                return Err(UsageFoldError::Regressed {
                    pid,
                    counter,
                    before,
                    after,
                });
            }
        }
        let rss_peak = self.rss_peak.max(reading.resident);
        let span = reading.at.duration_since(self.window.at);
        if span.is_zero() {
            // No time passed in which to judge: the window stays open, the judgment stands.
            return Ok(Self {
                latest: reading,
                window: self.window,
                busy: self.busy,
                rss_peak,
            });
        }
        Ok(Self {
            latest: reading,
            window: reading,
            busy: busy(reading.cpu_total() - self.window.cpu_total(), span),
            rss_peak,
        })
    }
}

/// Whether `cpu_us` of own CPU over `span` is at least [`BUSY_CPU_PERMILLE`] of one core,
/// compared in exact nanoseconds: a span shorter than a microsecond is not rounded to none.
fn busy(cpu_us: u64, span: Duration) -> bool {
    u128::from(cpu_us) * 1_000_000 >= u128::from(BUSY_CPU_PERMILLE) * span.as_nanos()
}

// ---------------------------------------------------------------------------------------------
// The kernel reads.

/// Why a life's counters could not be read.
#[derive(Debug, thiserror::Error)]
pub enum SampleError {
    #[error("reading the own counters of process {pid} failed: {source}")]
    Read { pid: u32, source: io::Error },
    #[error(transparent)]
    Fold(#[from] UsageFoldError),
}

/// What sampling one life found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sampled {
    Read,
    /// Its pid no longer names it: it was reaped, and perhaps the pid given to another.
    Gone,
    /// A first read that nothing proves is of this life (macOS: it exited before its counters
    /// were first read, so its unique id can no longer be asked). Its usage stays absent.
    Unproven,
}

/// Read `process`'s own counters at the supervisor's sample tick `at` and fold them. `held` is
/// the pidfd the observer opened for this very life.
#[cfg(target_os = "linux")]
pub fn sample(
    fold: &mut ProcessUsageFold,
    process: ProcessIdentity,
    held: std::os::fd::BorrowedFd<'_>,
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
    // The pidfd names the life it was opened for alone. Unreaped after the read, that life
    // held the pid throughout, so the counters read were its own -- exited or not.
    if !unreaped(held).map_err(failed)? {
        return Ok(Sampled::Gone);
    }
    fold_counters(fold, process, counters, at)
}

/// Read `process`'s own counters at the supervisor's sample tick `at` and fold them.
///
/// A first read names the pid alone. It is of `process` if the pid names `process`'s unique id
/// after the read: the observer saw that life before, and a life holds its pid until it is
/// reaped, so it held it during the read too. That read anchors the life's start; every later
/// read carries its own start, so it is fenced by the read itself, an exited life included.
#[cfg(target_os = "macos")]
pub fn sample(
    fold: &mut ProcessUsageFold,
    process: ProcessIdentity,
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
    match fold.start(process) {
        Some(start) if counters.started != start => return Ok(Sampled::Gone),
        Some(_) => {}
        None => match crate::runtime::job_groups::process_record(pid).map_err(failed)? {
            Some(record) if record.unique_id == process.birth.0 => {}
            Some(_) => return Ok(Sampled::Gone),
            // Exited, reaped or not: no unique id can be asked of it now.
            None => return Ok(Sampled::Unproven),
        },
    }
    fold_counters(fold, process, counters, at)
}

fn fold_counters(
    fold: &mut ProcessUsageFold,
    process: ProcessIdentity,
    counters: OwnCounters,
    at: Instant,
) -> Result<Sampled, SampleError> {
    fold.apply(UsageObservation::Read {
        process,
        reading: UsageReading {
            started: counters.started,
            at,
            cpu_user: counters.cpu_user,
            cpu_sys: counters.cpu_sys,
            resident: counters.resident,
            io: counters.io,
            exited: counters.exited,
        },
    })?;
    Ok(Sampled::Read)
}

/// Whether the process a pidfd names has not been reaped: signal 0 reaches an exited, unreaped
/// process and fails with `ESRCH` only once it is reaped.
#[cfg(target_os = "linux")]
fn unreaped(held: std::os::fd::BorrowedFd<'_>) -> io::Result<bool> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: pidfd_send_signal reads a live descriptor and takes no info pointer.
    let sent = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            held.as_raw_fd(),
            0,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if sent == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error)
}

/// A process's own counters, read in one call, named by pid alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OwnCounters {
    started: StartStamp,
    cpu_user: CpuMicros,
    cpu_sys: CpuMicros,
    /// Zero for an exited process, whatever size the kernel last recorded for it: it holds
    /// nothing now, on either platform.
    resident: ResidentBytes,
    io: ProcessStorageIo,
    exited: bool,
}

fn unit(error: ResourceUnitError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// `proc_pid_rusage(RUSAGE_INFO_V4)`: the process's own CPU (not `ri_child_*`, the children it
/// reaped), resident size and disk I/O bytes (`ri_diskio_*`: the I/O it issued to disk, which
/// its cache hits issue none of), which still answers for an exited process its parent has not
/// reaped. `None` once it is reaped. Its times are Mach ticks (xnu `task_power_info_locked`
/// stores `rm_time_mach` unconverted; measured 125/3 ns per tick on Apple silicon), converted
/// once by the timebase. An exited process (`ri_proc_exit_abstime` set) answers with the size
/// it last held; it holds none.
#[cfg(target_os = "macos")]
fn own_counters(pid: libc::pid_t) -> io::Result<Option<OwnCounters>> {
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
    let (numerator, denominator) = mach_tick()?;
    let cpu = |ticks| CpuMicros::of_ticks(ticks, numerator, denominator).map_err(unit);
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

/// `struct mach_timebase_info` (`<mach/mach_time.h>`): one tick is `numer / denom` ns. The
/// `libc` crate's declaration is deprecated in favor of another crate; this is all of it.
#[cfg(target_os = "macos")]
#[repr(C)]
struct MachTimebase {
    numer: u32,
    denom: u32,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn mach_timebase_info(info: *mut MachTimebase) -> libc::c_int;
}

/// The length of one Mach tick: `numerator / denominator` nanoseconds.
#[cfg(target_os = "macos")]
fn mach_tick() -> io::Result<(u32, NonZeroU32)> {
    let mut timebase = MachTimebase { numer: 0, denom: 0 };
    // SAFETY: one writable timebase record.
    let result = unsafe { mach_timebase_info(&mut timebase) };
    if result != libc::KERN_SUCCESS {
        return Err(io::Error::other(format!(
            "mach_timebase_info failed with kern_return_t {result}"
        )));
    }
    let denominator = NonZeroU32::new(timebase.denom).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "mach_timebase_info reported a zero denominator",
        )
    })?;
    Ok((timebase.numer, denominator))
}

/// `/proc/<pid>/stat`'s `utime` and `stime` (fields 14 and 15: the process's own, not the
/// `cutime`/`cstime` of children it waited for), `starttime` (field 22) and `rss` pages
/// (field 24), read at once, then its storage I/O ([`storage_io`]). An exited process keeps
/// both until it is reaped; `None` once it is. An exited one (state `Z` or `X`) holds no memory.
#[cfg(target_os = "linux")]
fn own_counters(pid: libc::pid_t) -> io::Result<Option<OwnCounters>> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            // An unmounted procfs proves nothing about `pid`.
            std::fs::metadata("/proc/self/stat")?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let invalid = |what: &str| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid}'s stat {stat:?} {what}"),
        )
    };
    // The command name is parenthesized and may hold anything; field 3 follows the last ')'.
    // Walk only the required fields, once, without allocating a vector of every field.
    let mut fields = stat
        .rfind(')')
        .map(|closing| stat[closing + 1..].split_whitespace())
        .ok_or_else(|| invalid("has no command delimiter"))?;
    let field = |value: Option<&str>| -> io::Result<u64> {
        value
            .ok_or_else(|| invalid("is too short"))?
            .parse()
            .map_err(|_| invalid("holds a field that is no count"))
    };
    let state = fields.next().ok_or_else(|| invalid("has no state"))?;
    let user_ticks = field(fields.nth(10))?; // Field 14, after the consumed state (field 3).
    let sys_ticks = field(fields.next())?; // Field 15.
    let started = field(fields.nth(6))?; // Field 22, after field 15.
    let exited = matches!(state, "Z" | "X");
    let resident_pages = if exited {
        0
    } else {
        field(fields.nth(1))? // Field 24, after field 22.
    };
    // SAFETY: sysconf takes a plain integer and touches no memory of ours.
    let hertz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let hertz = u32::try_from(hertz)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| io::Error::other(format!("the clock tick rate {hertz} is no rate")))?;
    // SAFETY: sysconf takes a plain integer and touches no memory of ours.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page = u64::try_from(page)
        .map_err(|_| io::Error::other(format!("the page size {page} is no size")))?;
    let cpu = |ticks| CpuMicros::of_ticks(ticks, 1_000_000_000, hertz).map_err(unit);
    let resident = if exited {
        ResidentBytes::ZERO
    } else {
        let bytes = resident_pages
            .checked_mul(page)
            .ok_or_else(|| invalid("holds more resident bytes than a u64 counts"))?;
        ResidentBytes::new(bytes).map_err(unit)?
    };
    let Some(io) = storage_io(pid)? else {
        return Ok(None);
    };
    Ok(Some(OwnCounters {
        started: StartStamp(started),
        cpu_user: cpu(user_ticks)?,
        cpu_sys: cpu(sys_ticks)?,
        resident,
        io,
        exited,
    }))
}

/// `/proc/<pid>/io`'s `read_bytes` (reads the process caused to be fetched from storage; its
/// page-cache hits count none) and `write_bytes` (pages it dirtied for storage, counted when
/// dirtied, not at writeback). A refused read is [`ProcessIoUnavailable::NotPermitted`]; a
/// missing file, of a process whose stat was just read, is a kernel without per-task I/O
/// accounting -- unless the process was reaped meanwhile, which the caller's fence finds.
/// `None` when the kernel says the process is gone (`ESRCH`).
#[cfg(target_os = "linux")]
fn storage_io(pid: libc::pid_t) -> io::Result<Option<ProcessStorageIo>> {
    let text = match std::fs::read_to_string(format!("/proc/{pid}/io")) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            return Ok(Some(ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotPermitted,
            }));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Some(ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotAccounted,
            }));
        }
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
        Err(error) => return Err(error),
    };
    let counter = |name: &str| -> io::Result<StorageIoBytes> {
        let value = text
            .lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("process {pid}'s io {text:?} has no {name}"),
                )
            })?;
        let value = value.trim().parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid}'s io {name} {value:?} is no count"),
            )
        })?;
        StorageIoBytes::new(value).map_err(unit)
    };
    Ok(Some(ProcessStorageIo::Read {
        read_bytes: counter("read_bytes")?,
        write_bytes: counter("write_bytes")?,
    }))
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use super::{
        OwnCounters, ProcessUsageFold, Sampled, StartStamp, UsageFoldError, UsageObservation,
        UsageReading, own_counters, sample,
    };
    use crate::api::process::{
        ProcessCoverageGap, ProcessIoUnavailable, ProcessStorageIo, ProcessUsage,
    };
    use crate::api::resources::{CpuMicros, ResidentBytes, StorageIoBytes};
    use crate::fork_lock::Spawn as _;
    use crate::runtime::process_tree::{BirthToken, ProcessIdentity};

    const ROLE: &str = "COWSHED_PROCESS_USAGE_ROLE";
    const ROLE_TEST: &str = "runtime::process_usage::tests::process_usage_role";
    const MARK: &str = "process-usage-fixture";
    /// The CPU the burner spends on itself before it reports.
    const BURN_US: u64 = 1_000_000;

    fn identity(pid: u32) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(pid.into()),
        }
    }

    fn cpu(micros: u64) -> CpuMicros {
        CpuMicros::new(micros).expect("a CPU time")
    }

    fn resident(bytes: u64) -> ResidentBytes {
        ResidentBytes::new(bytes).expect("a resident size")
    }

    fn moved(read: u64, written: u64) -> ProcessStorageIo {
        ProcessStorageIo::Read {
            read_bytes: StorageIoBytes::new(read).expect("bytes"),
            write_bytes: StorageIoBytes::new(written).expect("bytes"),
        }
    }

    fn reading(at: Instant, user_us: u64, sys_us: u64) -> UsageReading {
        UsageReading {
            started: StartStamp(7),
            at,
            cpu_user: cpu(user_us),
            cpu_sys: cpu(sys_us),
            resident: resident(1 << 20),
            io: moved(0, 0),
            exited: false,
        }
    }

    fn holding(bytes: u64, reading: UsageReading) -> UsageReading {
        UsageReading {
            resident: resident(bytes),
            ..reading
        }
    }

    fn read(process: ProcessIdentity, reading: UsageReading) -> UsageObservation {
        UsageObservation::Read { process, reading }
    }

    fn busy(fold: &ProcessUsageFold, process: ProcessIdentity) -> bool {
        fold.usage(process).expect("a read life").busy
    }

    #[test]
    fn busy_is_the_share_of_one_window_and_flips_back_to_idle() {
        let process = identity(40);
        let start = Instant::now();
        let mut fold = ProcessUsageFold::default();
        fold.apply(read(process, reading(start, 0, 0))).unwrap();
        assert!(!busy(&fold, process), "no window has passed yet");

        // 600 ms of a 1 s window.
        let second = start + Duration::from_secs(1);
        fold.apply(read(process, reading(second, 500_000, 100_000)))
            .unwrap();
        assert!(busy(&fold, process));

        // 9 ms in the next second is under 1 % of one core: idle, with no counter unchanged
        // needed to say so.
        let third = second + Duration::from_secs(1);
        fold.apply(read(process, reading(third, 509_000, 100_000)))
            .unwrap();
        assert!(!busy(&fold, process));
        // Exactly 1 %: busy.
        let fourth = third + Duration::from_secs(1);
        fold.apply(read(process, reading(fourth, 519_000, 100_000)))
            .unwrap();
        assert!(busy(&fold, process));
        assert_eq!(
            fold.usage(process),
            Some(ProcessUsage {
                cpu_user_us: cpu(519_000),
                cpu_sys_us: cpu(100_000),
                busy: true,
                rss_bytes: resident(1 << 20),
                rss_peak_bytes: resident(1 << 20),
                io: moved(0, 0),
            })
        );
    }

    #[test]
    fn storage_counters_never_fall_and_an_unavailable_source_stays_said() {
        let process = identity(47);
        let start = Instant::now();
        let second = |n| start + Duration::from_secs(n);
        let mut fold = ProcessUsageFold::default();
        let doing = |io, reading| UsageReading { io, ..reading };
        fold.apply(read(
            process,
            doing(moved(4096, 8192), reading(start, 0, 0)),
        ))
        .unwrap();
        assert_eq!(
            fold.apply(read(
                process,
                doing(moved(4095, 8192), reading(second(1), 0, 0))
            )),
            Err(UsageFoldError::Regressed {
                pid: 47,
                counter: "storage read bytes",
                before: 4096,
                after: 4095,
            })
        );
        // An exec into a set-id image can refuse the counters: said so, never zeroes.
        let refused = ProcessStorageIo::Unavailable {
            reason: ProcessIoUnavailable::NotPermitted,
        };
        fold.apply(read(process, doing(refused, reading(second(2), 0, 0))))
            .unwrap();
        assert_eq!(fold.usage(process).map(|usage| usage.io), Some(refused));
    }

    #[test]
    fn current_resident_memory_falls_and_the_peak_it_was_read_at_stays() {
        let process = identity(46);
        let start = Instant::now();
        let second = |n| start + Duration::from_secs(n);
        let mut fold = ProcessUsageFold::default();
        let rss = |fold: &ProcessUsageFold| {
            let usage = fold.usage(process).expect("a read life");
            (usage.rss_bytes.get(), usage.rss_peak_bytes.get())
        };
        fold.apply(read(process, holding(4 << 20, reading(start, 0, 0))))
            .unwrap();
        assert_eq!(rss(&fold), (4 << 20, 4 << 20));
        fold.apply(read(process, holding(96 << 20, reading(second(1), 0, 0))))
            .unwrap();
        assert_eq!(rss(&fold), (96 << 20, 96 << 20));
        fold.apply(read(process, holding(8 << 20, reading(second(2), 0, 0))))
            .unwrap();
        assert_eq!(rss(&fold), (8 << 20, 96 << 20), "released, the peak stays");
        // A zero-length window still takes the reading's size.
        fold.apply(read(process, holding(128 << 20, reading(second(2), 0, 0))))
            .unwrap();
        assert_eq!(rss(&fold), (128 << 20, 128 << 20));
        fold.apply(read(process, holding(0, reading(second(3), 0, 0))))
            .unwrap();
        fold.apply(UsageObservation::Exited { process }).unwrap();
        assert_eq!(rss(&fold), (0, 128 << 20), "exited, it holds nothing");
    }

    #[test]
    fn a_zero_length_window_judges_nothing_and_divides_by_nothing() {
        let process = identity(41);
        let start = Instant::now();
        let mut fold = ProcessUsageFold::default();
        fold.apply(read(process, reading(start, 0, 0))).unwrap();
        // A second reading at the first one's instant, CPU already advanced.
        fold.apply(read(process, reading(start, 30_000, 0)))
            .unwrap();
        assert!(!busy(&fold, process));
        assert_eq!(
            fold.usage(process).map(|usage| usage.cpu_user_us),
            Some(cpu(30_000)),
            "the counters are still the latest"
        );
        // The window stayed open at `start`: 40 ms over 1 s is busy.
        let later = start + Duration::from_secs(1);
        fold.apply(read(process, reading(later, 40_000, 0)))
            .unwrap();
        assert!(busy(&fold, process));
        // Another zero-length reading keeps that judgment.
        fold.apply(read(process, reading(later, 40_000, 0)))
            .unwrap();
        assert!(busy(&fold, process));
    }

    #[test]
    fn a_sub_microsecond_window_without_cpu_is_idle() {
        let start = Instant::now();
        for nanos in [1, 999] {
            let process = identity(49);
            let mut fold = ProcessUsageFold::default();
            fold.apply(read(process, reading(start, 0, 0))).unwrap();
            fold.apply(read(
                process,
                reading(start + Duration::from_nanos(nanos), 0, 0),
            ))
            .unwrap();
            assert!(!busy(&fold, process), "{nanos} ns without CPU is idle");
            // One microsecond of CPU in under one is far over 1 % of a core.
            fold.apply(read(
                process,
                reading(start + Duration::from_nanos(nanos * 2), 1, 0),
            ))
            .unwrap();
            assert!(busy(&fold, process), "1 us over {nanos} ns is busy");
        }
    }

    #[test]
    fn reading_the_usage_never_moves_the_window() {
        let process = identity(42);
        let start = Instant::now();
        let mut fold = ProcessUsageFold::default();
        fold.apply(read(process, reading(start, 0, 0))).unwrap();
        fold.apply(read(
            process,
            reading(start + Duration::from_secs(1), 200_000, 0),
        ))
        .unwrap();
        let first = fold.usage(process);
        // However many readers ask, they get the same answer from the same window.
        for _ in 0..3 {
            assert_eq!(fold.usage(process), first);
        }
        assert!(busy(&fold, process));
    }

    #[test]
    fn an_exited_life_keeps_its_last_counters_and_takes_no_more() {
        let process = identity(43);
        let start = Instant::now();
        let mut fold = ProcessUsageFold::default();
        fold.apply(read(process, reading(start, 70_000, 5_000)))
            .unwrap();
        fold.apply(UsageObservation::Exited { process }).unwrap();
        let usage = fold.usage(process).expect("final usage");
        assert_eq!(
            (usage.cpu_user_us, usage.cpu_sys_us),
            (cpu(70_000), cpu(5_000))
        );
        assert_eq!(
            fold.apply(read(
                process,
                reading(start + Duration::from_secs(1), 80_000, 5_000)
            )),
            Err(UsageFoldError::AfterExit { pid: 43 })
        );
        assert_eq!(
            fold.apply(UsageObservation::Exited { process }),
            Err(UsageFoldError::AfterExit { pid: 43 })
        );
        assert_eq!(
            fold.usage(process),
            Some(usage),
            "unchanged by the refusals"
        );

        // It was last read running: its usage is the last observed, not known final.
        assert_eq!(
            fold.gap(),
            Some(ProcessCoverageGap::UnreadFinalUsage { pid: 43 })
        );

        // A life reaped before it was ever read has no usage at all, not zeroes; the first
        // gap stays the one named.
        let unread = identity(44);
        fold.apply(UsageObservation::Exited { process: unread })
            .unwrap();
        assert_eq!(fold.usage(unread), None);
        assert!(fold.exited(unread));
        assert_eq!(
            fold.gap(),
            Some(ProcessCoverageGap::UnreadFinalUsage { pid: 43 })
        );
    }

    #[test]
    fn usage_read_after_the_exit_is_final_and_no_read_runs_it_again() {
        let process = identity(48);
        let start = Instant::now();
        let mut fold = ProcessUsageFold::default();
        fold.apply(read(process, reading(start, 1_000, 0))).unwrap();
        let at_exit = UsageReading {
            exited: true,
            ..holding(0, reading(start + Duration::from_secs(1), 2_000, 0))
        };
        fold.apply(read(process, at_exit)).unwrap();
        assert_eq!(
            fold.apply(read(
                process,
                reading(start + Duration::from_secs(2), 2_000, 0)
            )),
            Err(UsageFoldError::Revived { pid: 48 })
        );
        fold.apply(UsageObservation::Exited { process }).unwrap();
        assert_eq!(fold.gap(), None, "its final usage was read");
        assert_eq!(
            fold.usage(process).map(|usage| usage.cpu_user_us),
            Some(cpu(2_000))
        );
    }

    #[test]
    fn a_reading_of_another_life_or_running_backwards_is_refused() {
        let process = identity(45);
        let start = Instant::now();
        let later = start + Duration::from_secs(1);
        let mut fold = ProcessUsageFold::default();
        fold.apply(read(process, reading(later, 10, 10))).unwrap();
        let other = UsageReading {
            started: StartStamp(8),
            ..reading(later, 20, 20)
        };
        assert_eq!(
            fold.apply(read(process, other)),
            Err(UsageFoldError::OtherLife {
                pid: 45,
                first: StartStamp(7),
                read: StartStamp(8),
            })
        );
        assert_eq!(
            fold.apply(read(process, reading(later, 9, 10))),
            Err(UsageFoldError::Regressed {
                pid: 45,
                counter: "user CPU microseconds",
                before: 10,
                after: 9,
            })
        );
        assert_eq!(
            fold.apply(read(process, reading(start, 10, 10))),
            Err(UsageFoldError::Backwards { pid: 45 })
        );
    }

    // -----------------------------------------------------------------------------------------
    // A native parent that waits for a child burning a CPU second.

    /// The fixture roles, run only when a test re-executes this binary with [`ROLE`] set.
    #[test]
    fn process_usage_role() {
        let Some(role) = std::env::var_os(ROLE) else {
            return;
        };
        let mut out = std::io::stdout();
        match role.to_str() {
            Some("parent") => {
                let mut burner = Command::new(std::env::current_exe().expect("test binary"))
                    .args(["--exact", ROLE_TEST, "--nocapture"])
                    .env(ROLE, "burner")
                    .stdin(Stdio::inherit())
                    .stdout(Stdio::inherit())
                    .spawn_locked()
                    .expect("spawn the burner");
                let status = burner.wait().expect("reap the burner");
                assert!(status.success(), "burner: {status}");
                let children = rusage_us(libc::RUSAGE_CHILDREN);
                writeln!(out, "{MARK} reaped {children}").expect("report");
                out.flush().expect("report");
                // Held until the test closes stdin.
                while hold_byte() {}
            }
            Some("burner") => {
                writeln!(out, "{MARK} burning {}", std::process::id()).expect("report");
                out.flush().expect("report");
                let mut spin = 0_u64;
                let burned = loop {
                    let burned = rusage_us(libc::RUSAGE_SELF);
                    if burned >= BURN_US {
                        break burned;
                    }
                    for _ in 0..10_000 {
                        spin = std::hint::black_box(spin.wrapping_mul(31).wrapping_add(7));
                    }
                };
                writeln!(out, "{MARK} burned {burned}").expect("report");
                out.flush().expect("report");
                // Exits once the test writes one byte.
                assert!(hold_byte(), "the release byte");
            }
            Some(role) if role.starts_with("allocate ") => {
                let mebibytes: usize = role["allocate ".len()..].parse().expect("MiB");
                let length = mebibytes << 20;
                // SAFETY: a fresh private anonymous mapping no other code knows of.
                let region = unsafe {
                    libc::mmap(
                        std::ptr::null_mut(),
                        length,
                        libc::PROT_READ | libc::PROT_WRITE,
                        libc::MAP_PRIVATE | libc::MAP_ANON,
                        -1,
                        0,
                    )
                };
                assert_ne!(
                    region,
                    libc::MAP_FAILED,
                    "mmap: {}",
                    std::io::Error::last_os_error()
                );
                // SAFETY: sysconf takes a plain integer and touches no memory of ours.
                let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
                    .expect("a page size");
                for offset in (0..length).step_by(page) {
                    // SAFETY: inside the writable mapping made above.
                    unsafe { region.cast::<u8>().add(offset).write_volatile(1) };
                }
                writeln!(out, "{MARK} allocated {}", std::process::id()).expect("report");
                out.flush().expect("report");
                assert!(hold_byte(), "the release byte");
                // SAFETY: the whole mapping made above, unmapped once.
                assert_eq!(unsafe { libc::munmap(region, length) }, 0, "munmap");
                writeln!(out, "{MARK} released").expect("report");
                out.flush().expect("report");
                assert!(hold_byte(), "the exit byte");
            }
            Some(role) if role.starts_with("io ") => {
                let mut words = role["io ".len()..].splitn(3, ' ');
                let mut mebibytes =
                    || -> usize { words.next().expect("a size").parse().expect("MiB") };
                let (write, read) = (mebibytes(), mebibytes());
                let path = words.next().expect("a path").to_owned();
                writeln!(out, "{MARK} ready").expect("report");
                out.flush().expect("report");
                assert!(hold_byte(), "the start byte");
                uncached_io(&path, write, read);
                writeln!(out, "{MARK} done").expect("report");
                out.flush().expect("report");
                assert!(hold_byte(), "the exit byte");
            }
            other => panic!("unknown role {other:?}"),
        }
    }

    /// Write `write` MiB to a new file at `path`, flushed to storage, then read back its last
    /// `read` MiB, every transfer past the cache: macOS `F_NOCACHE`, Linux `O_DIRECT`, each
    /// from a page-aligned buffer.
    ///
    /// The read skips the file's head because a filesystem may cache it despite `O_DIRECT`:
    /// OpenZFS writes the first block of a file whose block size is still growing through its
    /// ARC (`zfs_write`'s `o_direct_defer`), and `fsync` commits the intent log without evicting
    /// that block, so reading it back is a cache hit that counts no storage bytes (measured on
    /// the Linux CI's ZFS: the first 1 MiB read of a fresh file counted 128 KiB less than every
    /// later one). Blocks written directly are not cached, and reading them is a storage read.
    fn uncached_io(path: &str, write: usize, read: usize) {
        use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};

        assert!(read <= write, "reads back only what it wrote");

        const CHUNK: usize = 1 << 20;
        let open = |flags: libc::c_int| -> OwnedFd {
            #[cfg(target_os = "linux")]
            let flags = flags | libc::O_DIRECT;
            let name = std::ffi::CString::new(path).expect("a path");
            // SAFETY: a NUL-terminated path and plain flags.
            let fd = unsafe { libc::open(name.as_ptr(), flags | libc::O_CLOEXEC, 0o600) };
            assert!(fd >= 0, "open {path}: {}", std::io::Error::last_os_error());
            // SAFETY: a new descriptor this role owns.
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            #[cfg(target_os = "macos")]
            {
                // SAFETY: fcntl on a live descriptor with an integer argument.
                let set = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_NOCACHE, 1) };
                assert_eq!(set, 0, "F_NOCACHE: {}", std::io::Error::last_os_error());
            }
            fd
        };
        // SAFETY: a fresh private anonymous mapping, page-aligned by construction.
        let buffer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                CHUNK,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        assert_ne!(buffer, libc::MAP_FAILED, "mmap");
        // SAFETY: the whole writable mapping made above.
        unsafe { std::ptr::write_bytes(buffer.cast::<u8>(), 0x5a, CHUNK) };

        let file = open(libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC);
        for _ in 0..write {
            // SAFETY: CHUNK readable bytes of the mapping.
            let written = unsafe { libc::write(file.as_raw_fd(), buffer, CHUNK) };
            assert_eq!(
                usize::try_from(written).ok(),
                Some(CHUNK),
                "write: {}",
                std::io::Error::last_os_error()
            );
        }
        // SAFETY: fsync on a live descriptor.
        assert_eq!(unsafe { libc::fsync(file.as_raw_fd()) }, 0, "fsync");
        drop(file);

        let file = open(libc::O_RDONLY);
        for chunk in write - read..write {
            let offset = libc::off_t::try_from(chunk * CHUNK).expect("an offset");
            // SAFETY: CHUNK writable bytes of the mapping.
            let got = unsafe { libc::pread(file.as_raw_fd(), buffer, CHUNK, offset) };
            assert_eq!(
                usize::try_from(got).ok(),
                Some(CHUNK),
                "read: {}",
                std::io::Error::last_os_error()
            );
        }
        drop(file);
        // SAFETY: the whole mapping made above, unmapped once.
        assert_eq!(unsafe { libc::munmap(buffer, CHUNK) }, 0, "munmap");
    }

    /// The user plus system CPU `getrusage` reports, in microseconds: the oracle, read by a
    /// different call than the reader under test.
    fn rusage_us(who: libc::c_int) -> u64 {
        // SAFETY: an all-zero rusage is a valid value of the plain C struct.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: one writable rusage record.
        assert_eq!(unsafe { libc::getrusage(who, &mut usage) }, 0, "getrusage");
        let micros = |time: libc::timeval| {
            u64::try_from(time.tv_sec).expect("seconds") * 1_000_000
                + u64::try_from(time.tv_usec).expect("microseconds")
        };
        micros(usage.ru_utime) + micros(usage.ru_stime)
    }

    /// One byte from stdin; `false` at its end.
    fn hold_byte() -> bool {
        let mut byte = 0_u8;
        loop {
            // SAFETY: one byte into a stack variable.
            let read = unsafe { libc::read(0, std::ptr::from_mut(&mut byte).cast(), 1) };
            match read {
                1 => return true,
                0 => return false,
                _ if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted => {}
                _ => panic!("read stdin: {}", std::io::Error::last_os_error()),
            }
        }
    }

    /// A life as the platform's observer holds it: its identity, and on Linux the pidfd opened
    /// for it.
    struct Observed {
        identity: ProcessIdentity,
        #[cfg(target_os = "linux")]
        pidfd: std::os::fd::OwnedFd,
    }

    fn observed(pid: u32) -> Observed {
        let pid_t = libc::pid_t::try_from(pid).expect("pid");
        #[cfg(target_os = "macos")]
        let birth = crate::runtime::job_groups::process_record(pid_t)
            .expect("record")
            .expect("a running process")
            .unique_id;
        #[cfg(target_os = "linux")]
        let birth = own_counters(pid_t)
            .expect("stat")
            .expect("a running process")
            .started
            .0;
        Observed {
            identity: ProcessIdentity {
                pid,
                birth: BirthToken(birth),
            },
            #[cfg(target_os = "linux")]
            pidfd: {
                use std::os::fd::FromRawFd as _;
                // SAFETY: pidfd_open takes plain integers; the descriptor is owned from here on.
                let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid_t, 0) };
                let opened = i32::try_from(opened).expect("a descriptor");
                assert!(
                    opened >= 0,
                    "pidfd_open: {}",
                    std::io::Error::last_os_error()
                );
                // SAFETY: a new descriptor this test owns.
                unsafe { std::os::fd::OwnedFd::from_raw_fd(opened) }
            },
        }
    }

    /// One supervisor tick's read of `life`.
    fn tick(fold: &mut ProcessUsageFold, life: &Observed) -> Sampled {
        #[cfg(target_os = "linux")]
        let sampled = {
            use std::os::fd::AsFd as _;
            sample(fold, life.identity, life.pidfd.as_fd(), Instant::now())
        };
        #[cfg(target_os = "macos")]
        let sampled = sample(fold, life.identity, Instant::now());
        sampled.expect("a sample")
    }

    fn total(usage: ProcessUsage) -> u64 {
        usage.cpu_user_us.get() + usage.cpu_sys_us.get()
    }

    fn next_report(
        lines: &mut impl Iterator<Item = std::io::Result<String>>,
        what: &str,
    ) -> Vec<String> {
        for line in lines.by_ref() {
            let line = line.expect("fixture stdout");
            let mut words = line.split_whitespace();
            if words.next() == Some(MARK) && words.next() == Some(what) {
                return words.map(str::to_owned).collect();
            }
        }
        panic!("the fixture ended before reporting {what}");
    }

    /// The child's own counters account for the CPU second it burned, read through the fenced
    /// reader and kept after it exits; its parent, which only waited and reaped it, is not
    /// charged that second, though the kernel's reaped-children total for it holds it.
    #[test]
    fn a_waiting_parent_is_not_charged_the_cpu_its_child_burned() {
        let mut parent = Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", ROLE_TEST, "--nocapture"])
            .env(ROLE, "parent")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn_locked()
            .expect("spawn the parent");
        let mut release = parent.stdin.take().expect("parent stdin");
        let mut lines = BufReader::new(parent.stdout.take().expect("parent stdout")).lines();
        let parent_life = observed(parent.id());
        let mut fold = ProcessUsageFold::default();
        assert_eq!(tick(&mut fold, &parent_life), Sampled::Read);

        let burner_pid: u32 = next_report(&mut lines, "burning")[0].parse().expect("pid");
        let burner = observed(burner_pid);
        assert_eq!(tick(&mut fold, &burner), Sampled::Read);
        let burned: u64 = next_report(&mut lines, "burned")[0]
            .parse()
            .expect("burned");
        assert_eq!(tick(&mut fold, &burner), Sampled::Read);
        let usage = fold.usage(burner.identity).expect("the burner's usage");
        assert!(usage.busy, "it burned throughout the window: {usage:?}");
        // Linux counts whole 10 ms ticks; the burner did almost nothing after measuring itself.
        let own = total(usage);
        assert!(
            own + 20_000 >= burned && own <= burned + 200_000,
            "the burner's own CPU {own} us is the {burned} us it measured"
        );

        release.write_all(b"x").expect("release the burner");
        let reaped: u64 = next_report(&mut lines, "reaped")[0]
            .parse()
            .expect("children");
        fold.apply(UsageObservation::Exited {
            process: burner.identity,
        })
        .expect("the burner's exit");
        assert_eq!(tick(&mut fold, &burner), Sampled::Gone);
        assert_eq!(
            fold.usage(burner.identity),
            Some(usage),
            "its last observed counters are kept"
        );
        assert_eq!(
            fold.gap(),
            Some(ProcessCoverageGap::UnreadFinalUsage { pid: burner_pid }),
            "reaped before a read after its exit, its usage is not known final"
        );

        assert_eq!(tick(&mut fold, &parent_life), Sampled::Read);
        let parent_own = total(
            fold.usage(parent_life.identity)
                .expect("the parent's usage"),
        );
        assert!(
            reaped >= burned,
            "the kernel charged the parent's children {reaped} us"
        );
        assert!(
            parent_own < 250_000 && parent_own * 4 < reaped,
            "the parent only waited: its own {parent_own} us holds none of the child's {reaped} us"
        );

        drop(release);
        let status = parent.wait().expect("reap the parent");
        assert!(status.success(), "parent: {status}");
    }

    /// A `sh` that exits once it reads a line.
    fn held_shell() -> (std::process::Child, std::process::ChildStdin) {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "read line"])
            .stdin(Stdio::piped())
            .spawn_locked()
            .expect("spawn");
        let stdin = child.stdin.take().expect("stdin");
        (child, stdin)
    }

    /// Release a held shell and wait for its exit without reaping it.
    fn exit_unreaped(child: &std::process::Child, mut stdin: std::process::ChildStdin) {
        stdin.write_all(b"go\n").expect("release");
        drop(stdin);
        let id = libc::id_t::try_from(child.id()).expect("pid");
        // SAFETY: an all-zero siginfo is a valid value of the plain C struct.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: waiting for our own child without reaping it.
        let waited =
            unsafe { libc::waitid(libc::P_PID, id, &mut info, libc::WEXITED | libc::WNOWAIT) };
        assert_eq!(waited, 0, "waitid: {}", std::io::Error::last_os_error());
    }

    /// An anchored life is read to its end: exited and unreaped, its final counters are still
    /// its own; once reaped, the pid names nothing of it.
    #[test]
    fn an_exited_unreaped_life_is_read_to_its_end_and_gone_once_reaped() {
        let (mut child, stdin) = held_shell();
        let life = observed(child.id());
        let mut fold = ProcessUsageFold::default();
        assert_eq!(tick(&mut fold, &life), Sampled::Read);
        exit_unreaped(&child, stdin);
        assert_eq!(
            tick(&mut fold, &life),
            Sampled::Read,
            "a zombie's last counters"
        );
        fold.apply(UsageObservation::Exited {
            process: life.identity,
        })
        .expect("its exit");
        assert_eq!(fold.gap(), None, "its final counters were read");
        child.wait().expect("reap");
        assert_eq!(tick(&mut fold, &life), Sampled::Gone);
    }

    /// A life first read after it exited: on Linux the pidfd still proves the read its own; on
    /// macOS nothing does, so its usage stays absent.
    #[test]
    fn a_life_first_read_after_its_exit_is_read_only_where_a_handle_proves_it() {
        let (mut child, stdin) = held_shell();
        let life = observed(child.id());
        exit_unreaped(&child, stdin);
        let mut fold = ProcessUsageFold::default();
        #[cfg(target_os = "linux")]
        assert_eq!(tick(&mut fold, &life), Sampled::Read);
        #[cfg(target_os = "macos")]
        {
            assert_eq!(tick(&mut fold, &life), Sampled::Unproven);
            assert_eq!(fold.usage(life.identity), None);
        }
        child.wait().expect("reap");
    }

    /// A life reaped before its first read has no usage: never zeroes.
    #[test]
    fn a_life_reaped_before_its_first_read_has_no_usage() {
        let (mut child, stdin) = held_shell();
        let life = observed(child.id());
        exit_unreaped(&child, stdin);
        child.wait().expect("reap");
        let pid = libc::pid_t::try_from(life.identity.pid).expect("pid");
        assert_eq!(
            own_counters(pid).expect("a reaped pid's read"),
            None::<OwnCounters>
        );
        let mut fold = ProcessUsageFold::default();
        assert_eq!(tick(&mut fold, &life), Sampled::Gone);
        fold.apply(UsageObservation::Exited {
            process: life.identity,
        })
        .expect("its exit");
        assert_eq!(fold.usage(life.identity), None);
        assert_eq!(
            fold.gap(),
            Some(ProcessCoverageGap::UnreadFinalUsage {
                pid: life.identity.pid
            })
        );
    }

    /// A re-executed fixture role, held by its stdin and reporting on its stdout.
    struct Fixture {
        child: std::process::Child,
        release: std::process::ChildStdin,
        lines: std::io::Lines<BufReader<std::process::ChildStdout>>,
        life: Observed,
    }

    impl Fixture {
        /// Start `role` and wait for its `ready` report.
        fn start(role: &str, ready: &str) -> Self {
            let mut child = Command::new(std::env::current_exe().expect("test binary"))
                .args(["--exact", ROLE_TEST, "--nocapture"])
                .env(ROLE, role)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn_locked()
                .expect("spawn a fixture");
            let release = child.stdin.take().expect("stdin");
            let mut lines = BufReader::new(child.stdout.take().expect("stdout")).lines();
            next_report(&mut lines, ready);
            let life = observed(child.id());
            Self {
                child,
                release,
                lines,
                life,
            }
        }

        /// Its resident bytes, read by a different kernel call than the reader under test.
        fn independent_resident(&self) -> u64 {
            let pid = libc::pid_t::try_from(self.life.identity.pid).expect("pid");
            #[cfg(target_os = "macos")]
            {
                let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
                let size = libc::c_int::try_from(size_of::<libc::proc_taskinfo>()).expect("size");
                // SAFETY: `info` is writable storage of exactly `size` bytes.
                let written = unsafe {
                    libc::proc_pidinfo(
                        pid,
                        libc::PROC_PIDTASKINFO,
                        0,
                        info.as_mut_ptr().cast(),
                        size,
                    )
                };
                assert_eq!(
                    written,
                    size,
                    "proc_pidinfo: {}",
                    std::io::Error::last_os_error()
                );
                // SAFETY: proc_pidinfo filled all `size` bytes.
                unsafe { info.assume_init() }.pti_resident_size
            }
            #[cfg(target_os = "linux")]
            {
                let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).expect("statm");
                let pages: u64 = statm
                    .split_whitespace()
                    .nth(1)
                    .expect("resident pages")
                    .parse()
                    .expect("a page count");
                // SAFETY: sysconf takes a plain integer and touches no memory of ours.
                let page = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
                    .expect("a page size");
                pages * page
            }
        }

        fn step(&mut self, report: &str) {
            self.release.write_all(b"x").expect("release");
            next_report(&mut self.lines, report);
        }

        /// Release whichever holds remain (the release and the exit), then reap it.
        fn finish(mut self) {
            self.release.write_all(b"xx").expect("release");
            drop(self.release);
            let status = self.child.wait().expect("reap");
            assert!(status.success(), "allocator: {status}");
        }
    }

    /// Each process's resident memory is its own, matches an independent kernel read, and falls
    /// when it releases memory while its peak stays.
    #[test]
    fn each_process_holds_its_own_resident_memory_and_keeps_its_peak() {
        const MIB: u64 = 1 << 20;
        let mut large = Fixture::start("allocate 96", "allocated");
        let small = Fixture::start("allocate 24", "allocated");
        let mut fold = ProcessUsageFold::default();
        let read = |fold: &mut ProcessUsageFold, allocator: &Fixture| {
            assert_eq!(tick(fold, &allocator.life), Sampled::Read);
            let usage = fold.usage(allocator.life.identity).expect("usage");
            let independent = allocator.independent_resident();
            assert!(
                usage.rss_bytes.get().abs_diff(independent) <= 2 * MIB,
                "read {} bytes resident, the kernel's other call {independent}",
                usage.rss_bytes.get()
            );
            usage
        };
        let held = read(&mut fold, &large);
        let other = read(&mut fold, &small);
        assert!(held.rss_bytes.get() >= 96 * MIB, "{held:?}");
        assert!(
            (24 * MIB..96 * MIB).contains(&other.rss_bytes.get()),
            "the small allocator holds its own, not the large one's or their sum: {other:?}"
        );

        large.step("released");
        let released = read(&mut fold, &large);
        assert!(
            released.rss_bytes.get() + 64 * MIB <= held.rss_bytes.get(),
            "released memory leaves: {released:?} after {held:?}"
        );
        assert_eq!(released.rss_peak_bytes, held.rss_bytes, "the peak stays");
        let other = read(&mut fold, &small);
        assert!(other.rss_peak_bytes.get() < 96 * MIB, "{other:?}");

        large.finish();
        small.finish();
    }

    fn storage(fold: &ProcessUsageFold, fixture: &Fixture) -> (u64, u64) {
        match fold.usage(fixture.life.identity).expect("usage").io {
            ProcessStorageIo::Read {
                read_bytes,
                write_bytes,
            } => (read_bytes.get(), write_bytes.get()),
            unavailable => panic!("this platform's source gave no bytes: {unavailable:?}"),
        }
    }

    /// Each process's storage counters move by the uncached I/O it did itself: a writer and
    /// reader of known sizes, beside a second process doing other I/O, each count their own.
    #[test]
    fn each_process_counts_the_storage_io_it_did_itself() {
        const MIB: u64 = 1 << 20;
        // Beside this test binary: on the build's own storage, never a memory-backed /tmp.
        let directory = std::env::current_exe()
            .expect("test binary")
            .parent()
            .expect("its directory")
            .to_path_buf();
        let path = |name: &str| {
            directory
                .join(format!("process-usage-io-{}-{name}", std::process::id()))
                .display()
                .to_string()
        };
        let (first, second) = (path("first"), path("second"));
        let mut writer_reader = Fixture::start(&format!("io 8 4 {first}"), "ready");
        let mut writer = Fixture::start(&format!("io 2 0 {second}"), "ready");
        let mut fold = ProcessUsageFold::default();
        for fixture in [&writer_reader, &writer] {
            assert_eq!(tick(&mut fold, &fixture.life), Sampled::Read);
        }
        let before = (storage(&fold, &writer_reader), storage(&fold, &writer));
        writer_reader.step("done");
        writer.step("done");
        for fixture in [&writer_reader, &writer] {
            assert_eq!(tick(&mut fold, &fixture.life), Sampled::Read);
        }
        let after = (storage(&fold, &writer_reader), storage(&fold, &writer));
        let delta =
            |before: (u64, u64), after: (u64, u64)| (after.0 - before.0, after.1 - before.1);
        let (first_io, second_io) = (delta(before.0, after.0), delta(before.1, after.1));
        // At least the bytes moved, and at most a mebibyte more: a source may count a storage
        // read above what it returned (the Linux CI's ZFS counts each uncached 128 KiB record
        // read as 135168 bytes, measured), never below.
        let near =
            |bytes: u64, mebibytes: u64| (mebibytes * MIB..=mebibytes * MIB + MIB).contains(&bytes);
        assert!(
            near(first_io.0, 4) && near(first_io.1, 8),
            "the first read 4 MiB and wrote 8 MiB: counted {first_io:?}"
        );
        assert!(
            near(second_io.1, 2) && second_io.0 < MIB,
            "the second wrote 2 MiB and read nothing: counted {second_io:?}"
        );
        writer_reader.finish();
        writer.finish();
        for file in [first, second] {
            std::fs::remove_file(file).expect("remove the fixture file");
        }
    }
}
