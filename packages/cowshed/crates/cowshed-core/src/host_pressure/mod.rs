//! The host's memory pressure and swap as the kernel publishes them, and whether a job may start
//! under that pressure (05_gateway.md "Host scheduling", "Memory pressure").
//!
//! Memory is the one resource the kernel cannot take back from a running job without killing it,
//! so it is the one a start waits for: a `background` exec does not start while the host reports
//! memory pressure, and starts as soon as a later reading drops. Every other class starts at once
//! and takes no reading. The wait is queued, never refused, and has no deadline: it lasts until the
//! pressure drops or the requester cancels the exec. Measured 2026-10-26: six acceptance runs of one
//! repository started together on an 18-core host with 7 GB of swap took load to 23 and swap to
//! 5.9 GB; CPU at that load is the kernel's to share, the swap is not.
//!
//! A start that waits cannot help once the host is already short. On macOS, which charges no group
//! a memory limit, the supervisor therefore ends one job at a time while the level is `Critical`:
//! the youngest running `background` or `resident` job ([`job_to_end`]), through the cancel path,
//! reported `failed` with failure `memory`. Gate and interactive jobs are never ended. On Linux the
//! kernel does this inside the job's cgroup (`memory.high`, `memory.swap.max` 0), and no loop runs.
//!
//! A supervisor is told of the pressure, never polls it ([`Feed`]): one reading when it opens its
//! feed, then a new one each time the kernel notifies a change.
//! kernel does this inside the job's cgroup (`memory.high`, `memory.swap.max` 0), and no loop runs.
//!
//! The readers are the platform's (`linux.rs`: `/proc/pressure/memory` and `/proc/meminfo`;
//! `macos.rs`: `kern.memorystatus_vm_pressure_level` and `vm.swapusage`), selected here and
//! nowhere else. A reading that cannot be taken is a [`HostPressureError`], never a zero: an
//! unreadable pressure is not an unpressured host. The decision below reads nothing, so its
//! threshold is tested without a pressured host. Allocation: none on the decision; a reader
//! allocates only the text of a malformed reading.

use std::time::Instant;

use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure};

use crate::api::dto::{ExecClass, JobId};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub use platform::{read_memory_pressure, read_swap};

/// The host's memory pressure as the kernel last reported it, or why it cannot be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reading {
    Known(MemoryPressure),
    Unreadable(String),
}

/// The pressure the kernel publishes now, as a [`Reading`].
pub fn reading() -> Reading {
    match read_memory_pressure() {
        Ok(pressure) => Reading::Known(pressure),
        Err(error) => Reading::Unreadable(error.to_string()),
    }
}

/// Where a supervisor's memory-pressure reports come from.
#[derive(Clone, Debug, Default)]
pub enum Source {
    /// The kernel's notification: libdispatch's memory-pressure source on macOS, a PSI trigger
    /// on Linux.
    #[default]
    Kernel,
    /// Readings a caller sends: a test drives a supervisor's admission through it.
    Driven(tokio::sync::watch::Receiver<Reading>),
}

impl Source {
    /// Open the feed. A kernel that refuses the subscription leaves a feed whose one reading
    /// names why: a host whose pressure cannot be followed is not shown to have room.
    pub fn open(self) -> Feed {
        match self {
            Self::Driven(receiver) => Feed {
                receiver,
                _notifier: None,
                _sender: None,
            },
            Self::Kernel => {
                let (sender, receiver) = tokio::sync::watch::channel(reading());
                match platform::notify(sender) {
                    Ok(notifier) => Feed {
                        receiver,
                        _notifier: Some(notifier),
                        _sender: None,
                    },
                    Err(error) => {
                        let (sender, receiver) =
                            tokio::sync::watch::channel(Reading::Unreadable(format!(
                                "cannot subscribe to the kernel's memory-pressure notification: \
                                 {error}"
                            )));
                        Feed {
                            receiver,
                            _notifier: None,
                            _sender: Some(sender),
                        }
                    }
                }
            }
        }
    }
}

/// A supervisor's memory-pressure reports: one reading when it opens, then one at each change the
/// kernel notifies. Dropping it ends the subscription.
pub struct Feed {
    receiver: tokio::sync::watch::Receiver<Reading>,
    /// The kernel subscription, which sends each report.
    _notifier: Option<platform::Notifier>,
    /// The sender of a feed that never changes, kept so the feed stays open.
    _sender: Option<tokio::sync::watch::Sender<Reading>>,
}

impl Feed {
    /// The latest report.
    pub fn current(&mut self) -> Reading {
        self.receiver.borrow_and_update().clone()
    }

    /// The next report. Ended by the kernel's next notification; a feed whose sender is gone
    /// reports nothing more, and this never returns.
    pub async fn changed(&mut self) -> Reading {
        if self.receiver.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        self.current()
    }
}

/// Why a host reading could not be taken. The field is `file`, not `source`: thiserror would
/// take a `source` field as the error's cause.
#[derive(Debug, thiserror::Error)]
pub enum HostPressureError {
    #[error("cannot read {file}: {cause}")]
    Read {
        file: &'static str,
        #[source]
        cause: std::io::Error,
    },
    #[error("{file} is malformed: {text:?}")]
    Malformed { file: &'static str, text: String },
}

/// The Linux `some avg10` of `/proc/pressure/memory`, in hundredths of a percent, above which a
/// background start waits: some task stalled on memory for more than a tenth of the last ten
/// seconds. A starting value, to be replaced by a measurement on a pressured host.
pub const PSI_SOME_AVG10_LIMIT: u16 = 1_000;

/// Whether a start of `class` waits for memory pressure to drop. Only `background` does: a gate
/// moves main, a person or model waits on interactive work, and a resident is started for a request
/// that waits on it, so none of them is held, and none of them can fail on an unreadable reading.
pub const fn waits_for_memory(class: ExecClass) -> bool {
    match class {
        ExecClass::Background => true,
        ExecClass::Interactive
        | ExecClass::Gate
        | ExecClass::Resident
        | ExecClass::InteractiveResident => false,
    }
}

/// What a start that waits for memory does under one reading.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// The host is not under memory pressure: start now.
    Start,
    /// The host is under memory pressure: wait for the kernel's next report. The reading is
    /// carried so the job's wait names it.
    Wait { pressure: MemoryPressure },
}

/// Whether a start that [`waits_for_memory`] may go under `pressure`: macOS at `Warning` or
/// above (non-compressed available memory under half of available) waits, Linux with `some avg10`
/// above [`PSI_SOME_AVG10_LIMIT`] waits.
pub fn may_start(pressure: MemoryPressure) -> Decision {
    let pressured = match pressure {
        MemoryPressure::Darwin { level } => level >= DarwinPressureLevel::Warning,
        MemoryPressure::Psi { some_avg10 } => some_avg10.get() > PSI_SOME_AVG10_LIMIT,
    };
    if pressured {
        Decision::Wait { pressure }
    } else {
        Decision::Start
    }
}

/// The job to end for memory under the macOS `level`, among the running `jobs` (id, class, when its
/// command spawned): at `Critical` the youngest `background` or `resident` job, the one with the
/// least work to lose, ties going to the greater job id; below `Critical`, or with no such job,
/// none. Called once per pressure reading, so a `Critical` that persists ends one more job on each
/// reading the supervisor takes.
pub fn job_to_end(
    level: DarwinPressureLevel,
    jobs: impl IntoIterator<Item = (JobId, ExecClass, Instant)>,
) -> Option<JobId> {
    match level {
        DarwinPressureLevel::Normal | DarwinPressureLevel::Warning => return None,
        DarwinPressureLevel::Critical => {}
    }
    jobs.into_iter()
        .filter(|&(_, class, _)| ends_for_memory(class))
        .max_by_key(|&(job, _, started)| (started, job))
        .map(|(job, _, _)| job)
}

/// Whether a job of `class` may be ended for memory: work nobody waits on. A gate moves main and
/// interactive work (an interactive resident's included) has a person or model waiting on it.
const fn ends_for_memory(class: ExecClass) -> bool {
    match class {
        ExecClass::Background | ExecClass::Resident => true,
        ExecClass::Interactive | ExecClass::Gate | ExecClass::InteractiveResident => false,
    }
}

#[cfg(test)]
mod tests {
    use cowshed_gateway_types::host_status::CentiPercent;

    use std::time::Duration;

    use super::*;

    #[test]
    fn a_start_waits_from_warning_and_above_the_psi_limit() {
        let psi = |centi: u16| MemoryPressure::Psi {
            some_avg10: CentiPercent::new(centi).expect("a centi-percent within 0..=10000"),
        };
        let darwin = |level| MemoryPressure::Darwin { level };
        for (pressure, waits) in [
            (darwin(DarwinPressureLevel::Normal), false),
            (darwin(DarwinPressureLevel::Warning), true),
            (darwin(DarwinPressureLevel::Critical), true),
            (psi(0), false),
            (psi(PSI_SOME_AVG10_LIMIT), false),
            (psi(PSI_SOME_AVG10_LIMIT + 1), true),
        ] {
            let expected = if waits {
                Decision::Wait { pressure }
            } else {
                Decision::Start
            };
            assert_eq!(may_start(pressure), expected, "{pressure:?}");
        }
    }

    #[test]
    fn critical_ends_the_youngest_job_nobody_waits_on() {
        let id = |value| JobId::new(value).expect("a job id within range");
        let at = Instant::now();
        let later = |secs| at + Duration::from_secs(secs);
        let running = [
            (id(1), ExecClass::Background, at),
            (id(2), ExecClass::Resident, later(1)),
            (id(3), ExecClass::Background, later(1)),
            (id(4), ExecClass::Gate, later(2)),
            (id(5), ExecClass::Interactive, later(2)),
            (id(6), ExecClass::InteractiveResident, later(2)),
        ];
        let waited_on = [running[3], running[4], running[5]];
        for (level, jobs, ended) in [
            (DarwinPressureLevel::Critical, &running[..], Some(id(3))),
            (DarwinPressureLevel::Critical, &waited_on[..], None),
            (DarwinPressureLevel::Warning, &running[..], None),
            (DarwinPressureLevel::Normal, &running[..], None),
        ] {
            assert_eq!(job_to_end(level, jobs.iter().copied()), ended, "{level:?}");
        }
    }
}
