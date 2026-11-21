//! What the supervisor takes from Darwin, which has no kernel object that holds a job's
//! processes: a job is the process group its first process leads, as the job's observer follows
//! it (`process_monitor`). The job's class is a nice placed on that first process while it is
//! held (`placement/macos.rs`), its signals go to the group through the fence that holds its
//! leader unreaped, and its totals are its leaders' own and reaped-children rusage
//! (`job_accounting`).

use std::fmt;
use std::time::Duration;

use super::{ProcessSignal, ProcessSpawnRequest, WorkspaceAuthoritySnapshot};
use crate::api::dto::{ExecClass, JobId};
use crate::api::process::JobProcessRetirement;
use crate::api::resources::{CpuTotals, JobAccounting, UsageReconciliation};
use crate::error::{CowshedError, PriorityFailure, Result};
use crate::runtime::child_fence::{ChildFence, NotSignalled};
use crate::runtime::job_accounting::{AccountingError, read_leader};
use crate::runtime::job_attribution::Attribution;
use crate::runtime::job_resources::JobSampler;

/// The kernel object that holds every process of one job. Darwin has none, so no value of this
/// type exists: a job that would carry one carries `None`, and every use of one is unreachable.
#[derive(Clone, Debug)]
pub enum JobMembership {}

impl JobMembership {
    pub(in crate::runtime) fn signal(&self, _signal: ProcessSignal) -> Result<()> {
        match *self {}
    }

    pub(in crate::runtime) fn retire(&self, _grace: Duration) -> Result<JobProcessRetirement> {
        match *self {}
    }

    pub(in crate::runtime) fn finish(&self) -> Result<()> {
        match *self {}
    }
}

impl fmt::Display for JobMembership {
    fn fmt(&self, _formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

/// What a spawn sink admits a job's kernel membership from: nothing, since no job has one.
#[derive(Default)]
pub(super) struct MembershipAdmissions;

impl MembershipAdmissions {
    pub(super) async fn admit(
        &mut self,
        _authority: &WorkspaceAuthoritySnapshot,
        _job_id: JobId,
        _class: ExecClass,
    ) -> Result<Option<JobMembership>> {
        Ok(None)
    }
}

/// Where a job's command is placed before it runs anything of the job's: at its class's nice.
#[derive(Clone, Debug)]
pub(in crate::runtime) struct CommandPlacement {
    nice: i32,
}

impl CommandPlacement {
    /// The placement `request` was admitted with.
    pub(in crate::runtime) fn of(request: &ProcessSpawnRequest) -> Result<Self> {
        Ok(Self::at_nice(request.cpu.nice))
    }

    /// A command placed at `nice`.
    pub(in crate::runtime) fn at_nice(nice: i32) -> Self {
        Self { nice }
    }

    /// The nice the command runs at, its class's.
    pub(in crate::runtime) fn nice(&self) -> i32 {
        self.nice
    }

    /// The kernel membership the command is placed in: none.
    pub(in crate::runtime) fn membership(&self) -> Option<&JobMembership> {
        None
    }

    /// Nothing is placed before exec by the child itself: this process places the held child.
    pub(in crate::runtime) fn on_exec(&self, _command: &mut std::process::Command) -> Result<()> {
        Ok(())
    }

    /// Set the held command `pid` to its nice and read back what its kernel entry holds: the
    /// nice the command is observed at.
    pub(in crate::runtime) fn place(
        &self,
        pid: u32,
    ) -> std::result::Result<Option<i32>, PriorityFailure> {
        crate::runtime::placement::macos::place_nice(pid, self.nice).map(Some)
    }

    /// Signal the command's job: the group its leader leads, while `fence` holds the leader
    /// unreaped.
    pub(in crate::runtime) fn signal(
        &self,
        fence: &ChildFence,
        signal: ProcessSignal,
    ) -> Result<()> {
        fence
            .signal(match signal {
                ProcessSignal::Term => libc::SIGTERM,
                ProcessSignal::Kill => libc::SIGKILL,
            })
            .map_err(NotSignalled::into_error)
    }
}

/// A job's independent totals: the cumulative own and reaped-children CPU of each process that
/// led it (`job_accounting`). The source counts no storage bytes.
pub(super) struct JobTotals {
    cpu: CpuTotals,
}

impl JobTotals {
    /// Read the charged leader's rusage into the job's totals.
    pub(super) fn read(
        sampler: &mut JobSampler,
        _membership: Option<&JobMembership>,
    ) -> Result<Self> {
        let leader = sampler.leader().pid();
        let cpu = sampler
            .rusage()
            .account(read_leader)
            .map_err(|error| unaccounted(leader, error))?;
        Ok(Self { cpu })
    }

    pub(super) fn cpu(&self) -> CpuTotals {
        self.cpu
    }

    /// The totals as the job's accounting, reconciled against `attribution`, what its observed
    /// processes counted as their own.
    pub(super) fn accounting(self, attribution: &Attribution) -> JobAccounting {
        JobAccounting::MacOsRusageChildren {
            cpu: self.cpu,
            io: None,
            reconciliation: UsageReconciliation {
                cpu_us: attribution.cpu_us,
                // The source has no storage bytes to compare.
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: attribution.coverage,
            },
        }
    }
}

/// Why job group `leader`'s CPU totals cannot be stated: the kernel would not answer for a
/// leader, or the readings contradict each other.
fn unaccounted(leader: u32, error: AccountingError) -> CowshedError {
    match error {
        AccountingError::Leader(_) => CowshedError::environment_missing(
            format!("the CPU accounting of job group {leader} is unavailable: {error}"),
            "the job runs on; its CPU totals cannot be stated",
        ),
        AccountingError::OtherLeader { .. }
        | AccountingError::NoneCharged { .. }
        | AccountingError::Unclosed { .. }
        | AccountingError::Inexact(_) => CowshedError::internal(format!(
            "the CPU accounting of job group {leader} is inconsistent: {error}"
        )),
    }
}
