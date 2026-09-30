//! The warm step: the project-declared build `land` starts in its target — main, or the lane
//! base a unit landed into — after moving it.
//!
//! A clone starts warm because its source's build outputs copy with it, so the target has to be
//! built at what landed. `land` hands the target's supervisor the argv its `.cowshed.toml`
//! declares under `[land] warm` and never waits for it. The supervisor keeps the queue: at most
//! one warm job runs, and at most one run waits behind it. A land that arrives while a warm job
//! runs replaces the waiting run, keeping the waiting run's base and taking the new head, so the
//! run that starts next covers every land since the running one began (02_workspaces.md, "Warm
//! main").

use std::collections::HashMap;
use std::path::Path;

use crate::api::dto::{
    CommandArg, ExecCommand, ExecRequest, JobId, RunSandboxMode, StdinSource, WarmAdmission,
    WarmRange,
};
use crate::error::{CowshedError, Result};
use crate::runtime::supervisor::WorkspaceSupervisorHandle;

/// The target's head before the land(s) a warm run covers; unset when the target was unborn.
pub const LAND_BASE_ENV: &str = "COWSHED_LAND_BASE";
/// The head the newest land a warm run covers landed.
pub const LAND_HEAD_ENV: &str = "COWSHED_LAND_HEAD";

const COWSHED_CONFIG_FILE: &str = ".cowshed.toml";

/// One run of the warm step: the declared argv, at the landed range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WarmRun {
    pub(crate) argv: Vec<CommandArg>,
    pub(crate) range: WarmRange,
}

impl WarmRun {
    /// The background job this run is: the argv, run from the target's root with the landed heads
    /// in its environment and nothing else of its own.
    pub(crate) fn request(&self) -> ExecRequest {
        let mut env = HashMap::with_capacity(2);
        if let Some(base) = &self.range.base {
            env.insert(LAND_BASE_ENV.to_owned(), base.as_str().to_owned());
        }
        env.insert(
            LAND_HEAD_ENV.to_owned(),
            self.range.head.as_str().to_owned(),
        );
        ExecRequest {
            command: ExecCommand::Argv(self.argv.clone()),
            cwd: None,
            mode: RunSandboxMode::ReadWrite,
            env,
            trace: None,
            stdin: StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
        }
    }
}

/// The warm queue of one land target: the warm job running, and the one run waiting for it.
#[derive(Debug, Default)]
pub(crate) struct WarmLane {
    running: Option<JobId>,
    waiting: Option<WarmRun>,
}

/// What the lane does with a land's run.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum WarmTurn {
    /// Nothing runs: start this run now.
    Start(WarmRun),
    /// A warm job runs: the land waits behind it, as the answer says.
    Wait(WarmAdmission),
}

impl WarmLane {
    /// Take one land's run: start it when no warm job runs, else fold it into the waiting run.
    pub(crate) fn admit(&mut self, run: WarmRun) -> WarmTurn {
        let Some(behind) = self.running else {
            return WarmTurn::Start(run);
        };
        let waiting = match self.waiting.take() {
            Some(earlier) => WarmRun {
                argv: run.argv,
                range: earlier.range.through(run.range),
            },
            None => run,
        };
        let range = waiting.range.clone();
        self.waiting = Some(waiting);
        WarmTurn::Wait(WarmAdmission::Queued { behind, range })
    }

    /// The warm job `job_id` started.
    pub(crate) fn started(&mut self, job_id: JobId) {
        self.running = Some(job_id);
    }

    /// The warm job running now, if any.
    pub(crate) const fn running(&self) -> Option<JobId> {
        self.running
    }

    /// The running warm job ended: the run waiting behind it, which is the lane's to start next.
    pub(crate) fn ended(&mut self) -> Option<WarmRun> {
        self.running = None;
        self.waiting.take()
    }
}

/// Start the land target's warm step for a land that moved it over `range`: read `[land] warm`
/// from the target's `.cowshed.toml` — main's, or a lane base's — and hand it to the target's
/// supervisor, which `target_supervisor` produces only when the target declares one. `None` when
/// it does not; a land never waits for the build itself.
pub async fn warm_after_land<S, F>(
    target_root: &Path,
    range: WarmRange,
    target_supervisor: S,
) -> Result<Option<WarmAdmission>>
where
    S: FnOnce() -> F,
    F: Future<Output = Result<WorkspaceSupervisorHandle>>,
{
    let Some(argv) = declared_warm(target_root)? else {
        return Ok(None);
    };
    let supervisor = target_supervisor().await?;
    supervisor.warm(argv, range).await.map(Some)
}

/// The `[land] warm` argv the target's `.cowshed.toml` declares, if it declares one.
fn declared_warm(target_root: &Path) -> Result<Option<Vec<CommandArg>>> {
    let path = target_root.join(COWSHED_CONFIG_FILE);
    let input = match std::fs::read_to_string(&path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!("cannot read {}: {error}", path.display()),
                "make the land target's .cowshed.toml readable, then land again",
            ));
        }
    };
    let config = crate::storage::bootstrap::parse_cowshed_config(&input).map_err(|error| {
        CowshedError::usage(
            format!("invalid {}: {error}", path.display()),
            "fix [land] warm in the land target's .cowshed.toml",
        )
    })?;
    Ok(config.land().map(|land| {
        land.warm()
            .iter()
            .map(|arg| CommandArg::from(arg.as_str()))
            .collect()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::GitOid;

    fn range(base: char, head: char) -> WarmRange {
        WarmRange {
            base: Some(GitOid::new(base.to_string().repeat(40)).unwrap()),
            head: GitOid::new(head.to_string().repeat(40)).unwrap(),
        }
    }

    fn run(argv: &str, base: char, head: char) -> WarmRun {
        WarmRun {
            argv: vec![CommandArg::from(argv)],
            range: range(base, head),
        }
    }

    #[test]
    fn a_lane_runs_one_job_and_keeps_one_run_from_the_oldest_base_to_the_newest_head() {
        let job = |id| JobId::new(id).unwrap();
        let mut lane = WarmLane::default();
        assert_eq!(
            lane.admit(run("a", 'a', 'b')),
            WarmTurn::Start(run("a", 'a', 'b'))
        );
        lane.started(job(1));
        assert_eq!(
            lane.admit(run("a", 'b', 'c')),
            WarmTurn::Wait(WarmAdmission::Queued {
                behind: job(1),
                range: range('b', 'c'),
            })
        );
        // The newest land's argv is the one declared at the head the run builds.
        assert_eq!(
            lane.admit(run("b", 'c', 'd')),
            WarmTurn::Wait(WarmAdmission::Queued {
                behind: job(1),
                range: range('b', 'd'),
            })
        );
        assert_eq!(lane.running(), Some(job(1)));
        assert_eq!(lane.ended(), Some(run("b", 'b', 'd')));
        assert_eq!(lane.running(), None);
        assert_eq!(lane.ended(), None, "the waiting run was taken");
        assert_eq!(
            lane.admit(run("b", 'd', 'e')),
            WarmTurn::Start(run("b", 'd', 'e'))
        );
    }
}
