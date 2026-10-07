//! The process-tree records a job's observed processes answer with (07_api.md, "Process-tree
//! observations"). One declaration: the controller protocol, N-API and TypeScript projections are
//! generated from these types.
//!
//! A record names a process by its numeric pid only for display. Which life of a pid a record
//! describes was decided when it was folded, by the kernel birth identity the observer retained
//! (`runtime::process_tree`); a reused pid is a second record, never the first one updated.

use serde::{Deserialize, Serialize};

use super::dto::{CommandArg, ExitStatus, JobId, UtcTimestamp};
use super::resources::{CpuMicros, ResidentBytes, StorageIoBytes};

/// Every process a job owned that its observer saw, the exited ones included, and whether the
/// observer saw all of them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobProcessTree {
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    /// In the order their births were observed.
    pub processes: Vec<JobProcessSample>,
    pub coverage: ProcessCoverage,
}

/// One life of one process: the image it runs now (or ran last), its parent, and how it ended.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobProcessSample {
    pub pid: u32,
    /// The parent's pid when this process was born: within the tree, the retained parent record;
    /// for a root, the process outside the job that started it.
    pub ppid: u32,
    /// The executable of the last observed exec; a child that has not exec'd runs its parent's.
    pub program: String,
    /// The byte-exact argv of that exec.
    pub argv: Vec<CommandArg>,
    pub born_at: UtcTimestamp,
    /// Absent until the exit is observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<ProcessExit>,
}

/// How and when a process ended: the two are observed together, so neither exists alone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessExit {
    pub status: ExitStatus,
    pub exited_at: UtcTimestamp,
}

/// A process is busy over a sample window in which its own CPU time is at least this share of
/// the window's wall time, in thousandths of one core: 1 %, the share under which a consumer
/// counts a tree idle. The one declaration of the threshold; consumers read it from here.
pub const BUSY_CPU_PERMILLE: u32 = 10;

/// What one process has cost itself, as last read: never the usage of the children it waited
/// for, which the job's accounting source counts. Absent from a process whose counters were
/// never read, rather than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessUsage {
    pub cpu_user_us: CpuMicros,
    pub cpu_sys_us: CpuMicros,
    /// Whether its own CPU over the supervisor's last sample window was at least 1 % of one
    /// core.
    pub busy: bool,
    /// What it holds resident now; zero once it has exited.
    pub rss_bytes: ResidentBytes,
    /// The most it was read holding: never another process's, nor the group's sum.
    pub rss_peak_bytes: ResidentBytes,
    pub io: ProcessStorageIo,
}

/// What one process has moved to and from storage, as its kernel's per-process source counts
/// it: macOS `ri_diskio_bytesread`/`ri_diskio_byteswritten`, the I/O the process issued to disk;
/// Linux `/proc/<pid>/io` `read_bytes`/`write_bytes`, reads it caused to be fetched from storage
/// and writes it dirtied for storage (at the time it dirtied them, not at writeback). Reads its
/// cache served count on neither.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ProcessStorageIo {
    Read {
        read_bytes: StorageIoBytes,
        write_bytes: StorageIoBytes,
    },
    /// The source gave no bytes for this process; never zero in their place.
    Unavailable { reason: ProcessIoUnavailable },
}

/// Why a process's storage I/O could not be read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProcessIoUnavailable {
    /// The kernel refused this observer the process's counters (Linux: a process made
    /// non-dumpable, e.g. by a set-id exec, refuses `/proc/<pid>/io`).
    NotPermitted,
    /// The kernel keeps no per-process storage I/O counters (Linux built without
    /// `CONFIG_TASK_IO_ACCOUNTING`).
    NotAccounted,
}

/// Whether the tree holds every process the job owned. A gap is absorbing: once an observation
/// was missed, no later one makes the tree complete again.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ProcessCoverage {
    Complete,
    Gap { reason: ProcessCoverageGap },
}

/// The first observation the tree is known to lack.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ProcessCoverageGap {
    /// The kernel event source reported that it dropped events.
    EventsLost,
    /// A fork, exec or exit named a process whose birth was never observed.
    UnobservedBirth { pid: u32 },
    /// A pid was born again while its previous life had no observed exit.
    UnobservedExit { pid: u32 },
    /// The kernel reported that a member forked without naming or counting the children
    /// (macOS kqueue `NOTE_FORK`): a child reaped before it was enumerated is unseen.
    UncountedFork { pid: u32 },
    /// A member exec'd and its new image was not read: it exited before the read, or the read
    /// failed, in which case the observer also returns that failure with its call and errno.
    UnreadImage { pid: u32 },
    /// A member exited with no read of its counters after its exit: its usage is the last one
    /// observed, not its final usage.
    UnreadFinalUsage { pid: u32 },
}
