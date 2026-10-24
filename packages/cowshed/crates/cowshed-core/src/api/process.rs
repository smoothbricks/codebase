//! The process-tree records a job's observed processes answer with (07_api.md, "Process-tree
//! observations"). One declaration: the controller protocol, N-API and TypeScript projections are
//! generated from these types.
//!
//! A record names a process by its numeric pid only for display. Which life of a pid a record
//! describes was decided when it was folded, by the kernel birth identity the observer retained
//! (`runtime::process_tree`); a reused pid is a second record, never the first one updated.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::dto::{CommandArg, ExitStatus, JobId, UtcTimestamp};
use super::resources::{CpuMicros, OneCoreCpuPermille, ResidentBytes, StorageIoBytes, WallMicros};

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
    /// The argv of that exec, byte-exact within a record's bounds: at most 128 KiB per argument
    /// and 1 MiB in all.
    pub argv: Vec<CommandArg>,
    /// Whether the exec's argv exceeded those bounds and `argv` holds only its first bytes. A
    /// record from before the bound was kept has no such field and was never cut.
    #[serde(default)]
    pub truncated: bool,
    pub born_at: UtcTimestamp,
    /// Absent until its counters are first read; never zeroes in their place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ProcessUsage>,
    /// What it was last observed waiting on; absent while no blocker was observed, never a
    /// fabricated [`ProcessBlockedOn::None`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_on: Option<ProcessBlockedOn>,
    /// Absent until the exit is observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<ProcessExit>,
}

/// What a process was observed waiting on. A lock's detail exists only on a lock, so no other
/// blocker can carry a stale path or holder.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ProcessBlockedOn {
    /// Observed waiting on nothing: running or runnable.
    None,
    /// Waiting for a file lock on `path`, held by `holder` when kernel evidence names it.
    Lock {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        holder: Option<LockHolder>,
    },
    Socket,
    Pipe,
    Child,
    Stdin,
    Disk,
}

/// The process holding a lock another one waits for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LockHolder {
    pub pid: u32,
    /// The cowshed job that owns the holder, when one does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobId>,
    /// The holder's image, only after an identity-fenced read; never a program-name guess.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program: Option<String>,
}

/// How long a live process goes without a record restating its usage before a heartbeat
/// restates it: one minute per process, whatever the sampling or subscriber interval.
pub const PROCESS_HEARTBEAT: Duration = Duration::from_secs(60);

/// One change to a job's process tree, in the order the supervisor folded it. A process-bearing
/// event's `index` is its position in [`JobProcessTree::processes`]: birth-observation order, which
/// never changes. A coverage gap has no index for a life that may never have been observed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum JobProcessEvent {
    Born {
        index: u32,
        process: JobProcessSample,
    },
    Exec {
        index: u32,
        process: JobProcessSample,
    },
    /// A blocker transition, a busy/idle flip, a resident-memory crossing of a power of two, or
    /// a first usage read; an ordinary sample with none of these is not a change.
    Changed(JobProcessDelta),
    /// The whole record of one live process that went [`PROCESS_HEARTBEAT`] without a record
    /// restating its usage.
    Heartbeat {
        index: u32,
        process: JobProcessSample,
    },
    /// The exit, with the usage as last read: final when read after the exit, otherwise the
    /// tree's coverage says so ([`ProcessCoverageGap::UnreadFinalUsage`]). Nothing follows it
    /// for this process.
    Exited {
        index: u32,
        process: JobProcessSample,
    },
    /// The first missed observation: emitted once as coverage changes from complete to a gap.
    Gap { reason: ProcessCoverageGap },
}

/// The one field of one process that changed; every other field is unchanged. A usage read and
/// a blocker read are separate observations, so a change names exactly one of them, and a
/// change of nothing can be neither built nor decoded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged, rename_all_fields = "camelCase", deny_unknown_fields)]
pub enum JobProcessDelta {
    Usage {
        index: u32,
        /// Usage only ever becomes known or moves on; it is never cleared.
        usage: ProcessUsage,
    },
    Blocker {
        index: u32,
        blocked_on: BlockerChange,
    },
}

/// A changed [`JobProcessSample::blocked_on`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum BlockerChange {
    /// A blocker, `none` included, was observed and differs from the last one.
    Set(ProcessBlockedOn),
    /// No blocker is observed any longer: the field becomes absent.
    Clear,
}

impl JobProcessDelta {
    pub fn index(&self) -> u32 {
        match self {
            Self::Usage { index, .. } | Self::Blocker { index, .. } => *index,
        }
    }

    /// Apply this change to the process it names.
    pub fn apply(&self, process: &mut JobProcessSample) {
        match self {
            Self::Usage { usage, .. } => process.usage = Some(*usage),
            Self::Blocker {
                blocked_on: BlockerChange::Set(blocked_on),
                ..
            } => process.blocked_on = Some(blocked_on.clone()),
            Self::Blocker {
                blocked_on: BlockerChange::Clear,
                ..
            } => process.blocked_on = None,
        }
    }
}

/// When a process ended, and how when the kernel said: both come from its one exit event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessExit {
    /// Absent when the kernel withheld it from the observer
    /// ([`ProcessCoverageGap::UnreadExitStatus`]); never a status in its place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ExitStatus>,
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
    /// core: `cpu_window`'s share against [`BUSY_CPU_PERMILLE`], idle while no window closed.
    pub busy: bool,
    /// The supervisor's last closed sample window of its own CPU, with the share `busy` judges;
    /// absent until two reads on the job's clock a positive span apart closed one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_window: Option<ProcessCpuWindow>,
    /// What it holds resident now; zero once it has exited.
    pub rss_bytes: ResidentBytes,
    /// The most it was read holding: never another process's, nor the group's sum.
    pub rss_peak_bytes: ResidentBytes,
    pub io: ProcessStorageIo,
}

/// One process's last closed CPU sample window: the span between the two reads of its own
/// counters that bound it, and the share of one core its own CPU took over that span. A share is
/// comparable only with its window, so the window is part of the value. It is placed on the job's
/// clock, the one [`JobResourceSample::wall_us`](super::resources::JobResourceSample) counts:
/// a read before that clock started (the observer reads a job's root before its spawn returns)
/// opens no window. Windows tile a life from its first read on the clock: each opens where the
/// one before it closed, so a reader tells a window it already saw (the same `closed_us`) from
/// the next one (`opened_us` at the last `closed_us`) and from one it missed (`opened_us` past
/// it).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessCpuWindow {
    /// Where the window opened, in whole microseconds since the job's first owned process
    /// spawned: the origin of `wallUs`, on the supervisor's monotonic clock.
    pub opened_us: WallMicros,
    /// Where it closed, on the same clock; equal to `opened_us` for a window under a microsecond.
    pub closed_us: WallMicros,
    /// Its own user plus system CPU over the window, in thousandths of one core, from the
    /// window's exact span and rounded down.
    pub cpu_permille: OneCoreCpuPermille,
}

impl ProcessCpuWindow {
    /// Whether its share reaches [`BUSY_CPU_PERMILLE`]: what `busy` says of it.
    pub fn busy(&self) -> bool {
        self.cpu_permille.get() >= u64::from(BUSY_CPU_PERMILLE)
    }
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    /// A member's exit was observed but the kernel withheld its status: macOS grants
    /// `NOTE_EXITSTATUS` only for a process the observer may signal, so a root command run
    /// through sudo exits without one.
    UnreadExitStatus { pid: u32 },
}

/// What held a job's process membership while its retirement proved the membership empty, which
/// fixes how far that proof reaches. A job states one only when an owner that held its
/// membership retired it: a job that never spawned, or that ran under a spawn sink with no
/// native membership, states none rather than a proof it lacks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum JobContainment {
    /// The kernel's own membership (a Linux cgroup) held every descendant, including those that
    /// left the process group. Empty membership is proven, not exhaustive process metadata.
    Kernel,
    /// The observer's retained process fold held the scope (macOS has no kernel membership).
    /// Whole-job retirement also holds the unreaped leader's group; subtree retirement uses only
    /// the retained root and its immutable descendants, never the whole group or its siblings.
    /// Unobserved escaped descendants are outside this proof. `lost` names observations known
    /// missing; an empty `lost` still never asserts completeness.
    Observed { lost: Vec<ProcessCoverageGap> },
}

/// A live member retirement signalled before proving its scoped membership empty. This includes
/// a live leader on cancellation or a live subtree root; a member already ended is not signalled.
/// A successful receipt names processes already ended, never survivors left running.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutlivedProcess {
    /// Its position in [`JobProcessTree::processes`] when the hold retained its life. Absent
    /// means the hold could not say (a cgroup keeps no birth order): unknown, never "no index".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
    /// Its pid when retirement signalled it: display only, as [`JobProcessSample::pid`].
    pub pid: u32,
    /// The executable it last ran, when an identity-fenced read named it. Absent when no read
    /// could (it exited first, or the kernel refused the read): never an empty or guessed
    /// program.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// The last signal retirement delivered to it.
    pub signal: i32,
}

impl OutlivedProcess {
    /// Whether it names its image honestly: a program a read named, or none. An empty string
    /// stands for neither, so no record carries one.
    pub fn consistent(&self) -> bool {
        self.image.as_deref() != Some("")
    }
}

/// What retiring process membership proved: what held it and the live members it signalled,
/// including a live leader or subtree root. Empty `outlived` means no process was ended only
/// when `outlived_incomplete` is false. Kernel-proven empty membership does not prove exhaustive
/// metadata: an atomic kill can end members no identity-fenced read named.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobProcessRetirement {
    pub containment: JobContainment,
    pub outlived: Vec<OutlivedProcess>,
    /// Retirement may have ended members absent from `outlived`; never a weaker emptiness proof.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub outlived_incomplete: bool,
}

/// An opaque reference to one retained process life in one monitor. Neither its pid nor a client
/// birth identity is authority: the monitor authenticates the live peer before issuing this.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobSubtreeHandle {
    pub job_id: JobId,
    pub index: u32,
    /// A random 32-lower-hex nonce issued for this retained root, not a process birth identity.
    pub incarnation: String,
}

/// Why a monitor cannot admit or retire this subtree; native operational errors remain errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum JobSubtreeRefusal {
    FoldUnavailable,
    NotMember,
    ForeignHandle,
    /// A successful retirement already consumed this root's capability.
    SpentHandle,
    JobEnded,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum JobSubtreeAdmission {
    Admitted { handle: JobSubtreeHandle },
    Refused { reason: JobSubtreeRefusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum JobSubtreeRetirement {
    Retired { retirement: JobProcessRetirement },
    Refused { reason: JobSubtreeRefusal },
}
/// The process a job's CPU went to most: its pid and the image it last ran.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobProcessLeaf {
    pub pid: u32,
    pub program: String,
    /// The argv of that image, bounded as a process sample's is.
    pub argv: Vec<CommandArg>,
    /// Whether that argv was cut to its bounds, as a process sample's says.
    #[serde(default)]
    pub truncated: bool,
}

/// Thousandths of a whole: the largest share a coverage can be.
pub const PERMILLE_WHOLE: u16 = 1_000;

/// Which observed process, live or exited, a job's CPU went to most, named only when the
/// observed processes' own CPU covers at least the supervisor's configured share of the job's
/// accounted CPU. Never a guessed winner among processes the observation missed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum JobLeafAttribution {
    /// The observed process with the most own CPU (ties to the earlier birth identity, then the
    /// lower pid), and the thousandths of the job's accounted CPU the observed processes cover.
    Attributed {
        leaf: JobProcessLeaf,
        observed_cpu_permille: u16,
    },
    /// The observed processes cover less than the required share: no leaf is named.
    Unattributed {
        observed_cpu_permille: u16,
        required_permille: u16,
    },
    /// The job's source has accounted no CPU yet, so nothing has a share of it.
    NoAccountedCpu,
}

impl JobLeafAttribution {
    /// Whether its thousandths are shares of a whole, and an unattributed coverage lies below
    /// the share it was required to reach.
    pub fn consistent(&self) -> bool {
        match self {
            Self::Attributed {
                observed_cpu_permille,
                ..
            } => *observed_cpu_permille <= PERMILLE_WHOLE,
            Self::Unattributed {
                observed_cpu_permille,
                required_permille,
            } => observed_cpu_permille < required_permille && *required_permille <= PERMILLE_WHOLE,
            Self::NoAccountedCpu => true,
        }
    }
}
