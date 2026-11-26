//! The host's cumulative CPU ticks across all online cores, as Mach `HOST_CPU_LOAD_INFO` counts
//! them, or a synthetic reading stream a test drives (05_gateway.md "A land's gate takes tokens
//! back").

use std::io;
use std::num::NonZeroU32;

use procinfo::HostCpuTicks;
use tokio::sync::watch;

/// One snapshot of the host's cumulative CPU ticks and online core count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub ticks: HostCpuTicks,
    pub cores: NonZeroU32,
}

/// One step a driven host CPU source adds to its cumulative tick counters on each read, beside the
/// online core count it reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Reading {
    pub ticks_per_sample: HostCpuTicks,
    pub cores: NonZeroU32,
}

/// Where a supervisor's host CPU readings come from.
#[derive(Clone, Debug, Default)]
pub enum Source {
    /// Mach `HOST_CPU_LOAD_INFO` from the kernel and `std::thread::available_parallelism`.
    #[default]
    Kernel,
    /// Readings a caller sends: a test drives a supervisor's `PRIO_DARWIN_BG` decisions through
    /// it.
    Driven(watch::Receiver<Reading>),
}

impl Source {
    /// Open the feed.
    pub fn open(self) -> Feed {
        Feed {
            source: self,
            accumulated: HostCpuTicks::default(),
        }
    }
}

/// A supervisor's host CPU reader.
pub struct Feed {
    source: Source,
    accumulated: HostCpuTicks,
}

impl Feed {
    /// Take one cumulative tick snapshot from the kernel or advance the driven synthetic counters
    /// by the latest sent reading.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if the kernel refuses `HOST_CPU_LOAD_INFO` or online core count
    /// lookup.
    pub fn read(&mut self) -> io::Result<Snapshot> {
        match &mut self.source {
            Source::Kernel => {
                let ticks = HostCpuTicks::read()?;
                let cores = online_cores()?;
                Ok(Snapshot { ticks, cores })
            }
            Source::Driven(receiver) => {
                let reading = *receiver.borrow_and_update();
                self.accumulated = HostCpuTicks {
                    user: self
                        .accumulated
                        .user
                        .saturating_add(reading.ticks_per_sample.user),
                    system: self
                        .accumulated
                        .system
                        .saturating_add(reading.ticks_per_sample.system),
                    idle: self
                        .accumulated
                        .idle
                        .saturating_add(reading.ticks_per_sample.idle),
                    nice: self
                        .accumulated
                        .nice
                        .saturating_add(reading.ticks_per_sample.nice),
                };
                Ok(Snapshot {
                    ticks: self.accumulated,
                    cores: reading.cores,
                })
            }
        }
    }
}

fn online_cores() -> io::Result<NonZeroU32> {
    let parallelism = std::thread::available_parallelism()?;
    let count = u32::try_from(parallelism.get()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("online core count {} exceeds u32", parallelism.get()),
        )
    })?;
    NonZeroU32::new(count)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "online core count was zero"))
}
