//! What the supervisor takes from Linux: a job's kernel membership is its own cgroup v2
//! (`job_cgroup.rs`; 07_api.md, "Complete job accounting and observation reconciliation").
//! Admission makes the cgroup before the job is recorded, the job's first process enters it
//! before it executes anything, the job's signals and retirement go to the cgroup's members, and
//! the job's totals are the cgroup's counters. The job's class is its class cgroup's weight
//! (`placement/linux.rs`), so no nice is placed on a command.

use std::fmt;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use super::{ProcessSignal, ProcessSpawnRequest, WorkspaceAuthoritySnapshot, off_actor};
use crate::api::dto::{ExecClass, JobId};
use crate::api::process::{JobContainment, JobProcessRetirement, OutlivedProcess};
use crate::api::resources::{
    ChargedMemoryUsage, CpuTotals, JobAccounting, StorageIoTotals, UsageReconciliation,
};
use crate::error::{CowshedError, PriorityFailure, Result};
use crate::runtime::child_fence::ChildFence;
use crate::runtime::job_attribution::Attribution;
use crate::runtime::job_cgroup::{CgroupAuthority, CgroupError, IncarnationCgroups, JobCgroup};
use crate::runtime::job_resources::JobSampler;

/// A cgroup failure as the supervisor answers it: an environment the operator repairs.
fn cgroup_error(error: CgroupError) -> CowshedError {
    let hint = match &error {
        // Retirement's own wait expired: the delegation is fine, a member outlived its kill.
        CgroupError::Unreaped { .. } => {
            "a member that outlives SIGKILL is in an uninterruptible kernel wait, which ends only \
             when that call returns: read its kernel stack (/proc/<pid>/stack, as root) for what \
             it waits on; the job keeps its claims, and killing it again retires it once the \
             member has exited"
        }
        _ => {
            "run the supervisor in a delegated cgroup v2 scope (Delegate=yes), with writable \
             cgroup.procs, cgroup.subtree_control and cgroup.kill; Linux 6.5+ is required"
        }
    };
    CowshedError::environment_missing(format!("job cgroup containment: {error}"), hint)
}

/// The kernel object that holds every process of one job: the job's own cgroup, from its
/// admission through its terminal accounting.
#[derive(Clone, Debug)]
pub struct JobMembership(Arc<JobCgroup>);

impl From<Arc<JobCgroup>> for JobMembership {
    fn from(cgroup: Arc<JobCgroup>) -> Self {
        Self(cgroup)
    }
}

impl JobMembership {
    /// The job's cgroup itself.
    pub fn cgroup(&self) -> &Arc<JobCgroup> {
        &self.0
    }

    /// Signal every member: TERM to each process the cgroup holds now, KILL through
    /// `cgroup.kill`, which also ends members that left the job's process group.
    pub(in crate::runtime) fn signal(&self, signal: ProcessSignal) -> Result<()> {
        match signal {
            ProcessSignal::Term => self.0.terminate(),
            ProcessSignal::Kill => self.0.kill(),
        }
        .map_err(cgroup_error)
    }

    /// End every member -- TERM, `grace`, then KILL -- and prove the membership empty within the
    /// reap bound. Blocks; the receipt names the members it found alive.
    pub(in crate::runtime) fn retire(&self, grace: Duration) -> Result<JobProcessRetirement> {
        let members = self
            .0
            .retire(grace, cowshed_os::deadline::REAP)
            .map_err(cgroup_error)?;
        Ok(JobProcessRetirement {
            containment: JobContainment::Kernel,
            outlived: members
                .members
                .iter()
                .map(|member| OutlivedProcess {
                    index: None,
                    pid: member.pid,
                    image: member.image.clone(),
                    signal: member.signal,
                })
                .collect(),
            outlived_incomplete: members.metadata_gap,
        })
    }

    /// Remove the cgroup once it holds no process and its counters are final. Blocks: it reads
    /// cgroupfs and joins the members for up to the reap bound.
    pub(in crate::runtime) fn finish(&self) -> Result<()> {
        self.0
            .finish(cowshed_os::deadline::REAP)
            .map(drop)
            .map_err(cgroup_error)
    }
}

impl fmt::Display for JobMembership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cgroup {:?} at {}",
            self.0.identity(),
            self.0.path().display()
        )
    }
}

/// The job cgroups of the workspace incarnation a spawn sink admits into: authority over the
/// delegated cgroup is taken at the first admission, and again when the incarnation changes.
#[derive(Default)]
pub(super) struct MembershipAdmissions(Option<IncarnationCgroups>);

impl MembershipAdmissions {
    /// Make `job_id`'s cgroup in its class's cgroup, on the blocking pool. Every native job has
    /// one: a refusal here refuses the job.
    pub(super) async fn admit(
        &mut self,
        authority: &WorkspaceAuthoritySnapshot,
        job_id: JobId,
        class: ExecClass,
    ) -> Result<Option<JobMembership>> {
        let mut cgroups = self.0.take();
        let incarnation = authority.workspace_incarnation.clone();
        let (cgroups, admitted) = off_actor::run(move || {
            let admitted = (|| {
                if !cgroups
                    .as_ref()
                    .is_some_and(|jobs| jobs.incarnation() == &incarnation)
                {
                    let delegated = CgroupAuthority::delegated().map_err(cgroup_error)?;
                    cgroups = Some(delegated.incarnation(&incarnation).map_err(cgroup_error)?);
                }
                cgroups
                    .as_ref()
                    .expect("authority was established above")
                    .admit(job_id, class)
                    .map(|cgroup| Some(JobMembership(Arc::new(cgroup))))
                    .map_err(cgroup_error)
            })();
            (cgroups, admitted)
        })
        .await;
        self.0 = cgroups;
        admitted
    }
}

/// Where a job's command is placed before it runs anything of the job's: in the job's cgroup,
/// which carries its class.
#[derive(Clone, Debug)]
pub(in crate::runtime) struct CommandPlacement(JobMembership);

impl From<JobMembership> for CommandPlacement {
    fn from(membership: JobMembership) -> Self {
        Self(membership)
    }
}

impl CommandPlacement {
    /// The placement `request` was admitted with. A request without a cgroup is refused: no
    /// command runs uncontained.
    pub(in crate::runtime) fn of(request: &ProcessSpawnRequest) -> Result<Self> {
        request.membership.clone().map(Self).ok_or_else(|| {
            CowshedError::environment_missing(
                format!(
                    "job {} has no admitted cgroup: refusing to run an uncontained command",
                    request.job_id.get()
                ),
                "delegate a writable cgroup v2 subtree and admit the job before spawning it",
            )
        })
    }

    /// The kernel membership the command is placed in.
    pub(in crate::runtime) fn membership(&self) -> Option<&JobMembership> {
        Some(&self.0)
    }

    /// Have the child `command` forks enter the job's cgroup before it executes its program.
    pub(in crate::runtime) fn on_exec(&self, command: &mut std::process::Command) -> Result<()> {
        self.0
            .cgroup()
            .placement()
            .map_err(cgroup_error)?
            .on_exec(command);
        Ok(())
    }

    /// A write end of the job cgroup's `cgroup.procs` for a process that forks the command
    /// elsewhere, a warm host: its forked command enters the cgroup through it.
    pub(in crate::runtime) fn descriptor(&self) -> Result<OwnedFd> {
        Ok(self
            .0
            .cgroup()
            .placement()
            .map_err(cgroup_error)?
            .into_descriptor())
    }

    /// Place the held command `pid`. Its cgroup, entered before the hold, carries its class, so
    /// no nice is placed and none is observed.
    pub(in crate::runtime) fn place(
        &self,
        _pid: u32,
    ) -> std::result::Result<Option<i32>, PriorityFailure> {
        Ok(None)
    }

    /// Signal the command's job: every member of its cgroup, never only the group `fence` holds.
    pub(in crate::runtime) fn signal(
        &self,
        _fence: &ChildFence,
        signal: ProcessSignal,
    ) -> Result<()> {
        self.0.signal(signal)
    }
}

/// A job's independent totals, read from its cgroup: every process that ever ran in the job,
/// those reaped before any observer looked included.
pub(super) struct JobTotals {
    cpu: CpuTotals,
    io: StorageIoTotals,
    charged_memory: ChargedMemoryUsage,
}

impl JobTotals {
    /// Read the counters of the job's cgroup. A job that runs in none -- an injected spawn sink's
    /// -- has no source for its totals, and says so rather than state any.
    pub(super) fn read(
        sampler: &mut JobSampler,
        membership: Option<&JobMembership>,
    ) -> Result<Self> {
        let Some(membership) = membership else {
            return Err(CowshedError::environment_missing(
                format!(
                    "job group {} runs in no cgroup of this supervisor, so no source counts its \
                     totals",
                    sampler.leader().pid()
                ),
                "run the job through the supervisor's own spawn sink, which admits each job into \
                 its cgroup",
            ));
        };
        let cgroup = membership.cgroup();
        let cpu = cgroup.cpu().map_err(cgroup_error)?;
        Ok(Self {
            cpu: CpuTotals {
                user_us: cpu.user_us,
                sys_us: cpu.system_us,
            },
            io: cgroup.storage_io().map_err(cgroup_error)?,
            charged_memory: cgroup.charged_memory().map_err(cgroup_error)?,
        })
    }

    pub(super) fn cpu(&self) -> CpuTotals {
        self.cpu
    }

    /// The totals as the job's accounting, reconciled against `attribution`, what its observed
    /// processes counted as their own.
    pub(super) fn accounting(self, attribution: &Attribution) -> JobAccounting {
        JobAccounting::LinuxCgroupV2 {
            cpu: self.cpu,
            io: self.io,
            charged_memory: self.charged_memory,
            reconciliation: UsageReconciliation {
                cpu_us: attribution.cpu_us,
                // The attribution sums the observed processes' CPU alone: it holds no storage
                // bytes of theirs to compare the cgroup's with.
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: attribution.coverage,
            },
        }
    }
}
