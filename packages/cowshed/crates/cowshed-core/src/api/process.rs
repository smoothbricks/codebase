//! The process-tree records a job's observed processes answer with (07_api.md, "Process-tree
//! observations"). One declaration: the controller protocol, N-API and TypeScript projections are
//! generated from these types.
//!
//! A record names a process by its numeric pid only for display. Which life of a pid a record
//! describes was decided when it was folded, by the kernel birth identity the observer retained
//! (`runtime::process_tree`); a reused pid is a second record, never the first one updated.

use serde::{Deserialize, Serialize};

use super::dto::{CommandArg, ExitStatus, JobId, UtcTimestamp};

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
    /// A member exec'd, and exited before its new image could be read.
    UnreadImage { pid: u32 },
}
