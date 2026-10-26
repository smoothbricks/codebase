//! Linux class placement: a job runs in its class's cgroup, and the class cgroup carries the class
//! (05_gateway.md "Host scheduling", "Classes"; 07_api.md "Complete job accounting and
//! observation reconciliation" for the job cgroup itself).
//!
//! # Why the class is a cgroup level
//!
//! cgroup v2 weights compare siblings only: a cgroup's share is a share of its parent's
//! (cgroup-v2.rst, "Weights"). So the class sits on the one level every job meets every other
//! job's class, directly under the delegated root:
//!
//! ```text
//! <delegated>/gate/job-<incarnation>-<id>          gate
//! <delegated>/interactive/job-<incarnation>-<id>   interactive, interactive resident
//! <delegated>/background/job-<incarnation>-<id>    background, resident
//! ```
//!
//! A class cgroup's `cpu.weight.nice` is its classes' nice from the one table
//! ([`super::class_nice`]). Jobs inside a class sit at the default weight and share it equally,
//! so a gate against all background work together is about 3:1 (nice 5 against nice 10) however
//! many background jobs run, and background still runs whenever the gate does not. Weights are
//! work-conserving and never cap a class: no `cpu.max` is written, since a quota strands idle
//! cores exactly as host tokens did (measured 2026-10-22: no gate running, background capped at 9
//! of 18 cores, the other 9 idle).
//!
//! # Memory
//!
//! - `gate`: `memory.low` = `max`, on the class cgroup and on each gate job. It protects
//!   exactly the pages a gate holds from reclaim while anything unprotected remains, and reserves
//!   nothing idle. Protection flows down only through cgroups that hold it (unless cgroup2 is
//!   mounted `memory_recursiveprot`), hence on both levels; and it is capped by the delegated root's
//!   own `memory.low`, which the delegation sets (systemd `MemoryLow=` on the scope), not cowshed.
//! - `background` (background and resident together): `memory.high` = half the host's physical
//!   memory, where the kernel throttles the class and reclaims from it, never killing;
//!   `memory.max` = three quarters, where the kernel's OOM killer ends a job inside this class
//!   only, before the host, a gate or interactive work runs out; `memory.swap.max` = 0, so background
//!   work never swaps the host (measured 2026-10-26: six acceptance runs of one repository took
//!   5.9 of 7 GB swap). Fixed shares: no job declares a memory estimate.
//!
//! # Processes
//!
//! Every non-gate job's own `pids.max` is [`JOB_PIDS_MAX`], so a fork bomb fails with `EAGAIN`
//! inside its job, never host-wide. A gate is never limited (08_testing.md, "The landing gate is
//! never CPU-limited").
//!
//! I/O has no weight here: `io.weight` takes effect only under the root-only `io.cost.qos`.

use std::fmt::Display;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::Path;

use crate::api::dto::ExecClass;
use crate::runtime::job_cgroup::CgroupError;

use super::class_nice;

/// A non-gate job's own `pids.max`: the tasks it may hold at once. A Cargo or nextest tree on a
/// large host holds a few thousand (a rustc per core, each with a thread per codegen unit); 16384
/// is well above that and far below the host's pid space, so a fork bomb fails with `EAGAIN`
/// inside its job while every real build runs unbounded by it.
const JOB_PIDS_MAX: u32 = 16_384;

/// The background class's `memory.high`: half the host. Past it the kernel throttles background
/// work and reclaims from it, never killing; the other half stays for the gate, interactive work
/// and the host itself. A fixed share, since no job declares an estimate.
const fn background_memory_high(physical: u64) -> u64 {
    physical / 2
}

/// The background class's `memory.max`: three quarters of the host, the kernel's backstop. A
/// background job that grows on through the throttle is OOM-killed inside the background class
/// alone, while a quarter of the host is still free for the gate and interactive work.
const fn background_memory_max(physical: u64) -> u64 {
    physical / 4 * 3
}

/// A class cgroup: the classes that share one nice of the table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClassCgroup {
    Gate,
    Interactive,
    Background,
}

// One class cgroup per nice of the table: classes grouped together share a nice, and the groups'
// nices differ, so a table change that splits or merges a group fails here.
const _: () = assert!(
    class_nice(ExecClass::InteractiveResident) == class_nice(ExecClass::Interactive)
        && class_nice(ExecClass::Resident) == class_nice(ExecClass::Background)
        && class_nice(ExecClass::Gate) != class_nice(ExecClass::Interactive)
        && class_nice(ExecClass::Gate) != class_nice(ExecClass::Background)
        && class_nice(ExecClass::Interactive) != class_nice(ExecClass::Background)
);

impl ClassCgroup {
    pub(crate) const ALL: [Self; 3] = [Self::Gate, Self::Interactive, Self::Background];

    pub(crate) const fn of(class: ExecClass) -> Self {
        match class {
            ExecClass::Gate => Self::Gate,
            ExecClass::Interactive | ExecClass::InteractiveResident => Self::Interactive,
            ExecClass::Background | ExecClass::Resident => Self::Background,
        }
    }

    /// The class cgroup's directory name under the delegated root.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Gate => "gate",
            Self::Interactive => "interactive",
            Self::Background => "background",
        }
    }

    const fn nice(self) -> i32 {
        class_nice(match self {
            Self::Gate => ExecClass::Gate,
            Self::Interactive => ExecClass::Interactive,
            Self::Background => ExecClass::Background,
        })
    }

    /// Write this class's weight and memory bounds to its cgroup at `path`. Every write states a
    /// value, never an increment, so placing a class again is the same placement.
    pub(crate) fn place(self, path: &Path) -> Result<(), CgroupError> {
        write(path, "cpu.weight.nice", self.nice())?;
        match self {
            Self::Gate => write(path, "memory.low", "max"),
            Self::Interactive => Ok(()),
            Self::Background => {
                let physical = physical_memory()?;
                write(path, "memory.high", background_memory_high(physical))?;
                write(path, "memory.max", background_memory_max(physical))?;
                write(path, "memory.swap.max", 0)
            }
        }
    }

    /// Write what a job of this class carries on its own cgroup at `path`, before its first process
    /// is placed.
    pub(crate) fn place_job(self, path: &Path) -> Result<(), CgroupError> {
        match self {
            Self::Gate => write(path, "memory.low", "max"),
            Self::Interactive | Self::Background => write(path, "pids.max", JOB_PIDS_MAX),
        }
    }
}

/// One interface write. A single formatted value reaches the file as one `write(2)`, which is how
/// cgroup files take a value.
fn write(directory: &Path, file: &'static str, value: impl Display) -> Result<(), CgroupError> {
    let path = directory.join(file);
    OpenOptions::new()
        .write(true)
        .open(&path)
        .and_then(|mut opened| write!(opened, "{value}"))
        .map_err(|source| CgroupError::Io {
            call: "write",
            path,
            source,
        })
}

/// The host's physical memory in bytes.
fn physical_memory() -> Result<u64, CgroupError> {
    // SAFETY: sysconf takes these constants and no memory.
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    match (u64::try_from(pages), u64::try_from(page_size)) {
        (Ok(count), Ok(size)) if count > 0 && size > 0 => Ok(count.saturating_mul(size)),
        _ => Err(CgroupError::PhysicalMemory { pages, page_size }),
    }
}
