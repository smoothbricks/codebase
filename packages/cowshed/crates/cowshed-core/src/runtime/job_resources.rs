//! A job's resource sampling (07_api.md "Job monitoring"), from the first process the job owns
//! until its terminal sample.
//!
//! The first owned process is the job's spawn: its shell activation on a cold host, otherwise its
//! command. That spawn fixes the job's baselines; the leader moves to the command's group when
//! the command starts, and the baselines stay.

use std::time::Instant;

use super::job_groups::Birth;
use super::supervisor::OwnedProcess;
use crate::api::dto::{JobId, UtcTimestamp};
use crate::api::resources::{JobResourceSample, WallMicros};
use crate::error::{CowshedError, Result};

/// What a job's resource reads answer, through its life.
pub(super) enum Sampling {
    /// No process of the job's exists yet -- or, once the job ended, none ever did.
    Unowned,
    /// The job owns processes: a read observes them now.
    Live(JobSampler),
    /// The job ended: its terminal sample, or why that last observation failed. A failure is
    /// kept as it is: an earlier sample would claim the job's end looked like its middle.
    Frozen(Result<JobResourceSample>),
}

impl Sampling {
    /// The job owns `process`. Its first process starts the sampling; a later one -- its
    /// command, after its activation -- takes the lead and keeps the spawn's baselines. Returns
    /// the sampler to observe, or `None` once the job ended.
    pub(super) fn own(&mut self, job_id: JobId, process: OwnedProcess) -> Option<&JobSampler> {
        match self {
            Self::Unowned => {
                *self = Self::Live(JobSampler::spawned(job_id, process));
                match self {
                    Self::Live(sampler) => Some(sampler),
                    Self::Unowned | Self::Frozen(_) => None,
                }
            }
            Self::Live(sampler) => {
                sampler.lead(process.birth);
                Some(sampler)
            }
            Self::Frozen(_) => None,
        }
    }

    /// The job ended: take its terminal sample through `observe` and keep it, or keep why it
    /// could not be taken. Returns the terminal sample, if one exists.
    pub(super) fn freeze(
        &mut self,
        observe: impl FnOnce(&JobSampler) -> Result<JobResourceSample>,
    ) -> Option<JobResourceSample> {
        if let Self::Live(sampler) = self {
            *self = Self::Frozen(observe(sampler));
        }
        match self {
            Self::Frozen(Ok(sample)) => Some(sample.clone()),
            Self::Unowned | Self::Live(_) | Self::Frozen(Err(_)) => None,
        }
    }

    /// A resources read: a live job observed now through `observe`, an ended job's frozen
    /// outcome, or why there is nothing to sample.
    pub(super) fn read(
        &self,
        job_id: JobId,
        observe: impl FnOnce(&JobSampler) -> Result<JobResourceSample>,
    ) -> Result<JobResourceSample> {
        match self {
            Self::Live(sampler) => observe(sampler),
            Self::Frozen(outcome) => outcome.clone(),
            Self::Unowned => Err(CowshedError::conflict(
                format!(
                    "job {} owns no process: it has not started one, or ended before it did",
                    job_id.get()
                ),
                "read its resources once the job has started; its status says how it ended",
            )),
        }
    }
}

pub(super) struct JobSampler {
    job_id: JobId,
    /// When the job's first owned process spawned.
    spawned: Instant,
    /// The process that leads the job's group now, as its parent observed it.
    leader: Birth,
}

impl JobSampler {
    fn spawned(job_id: JobId, first: OwnedProcess) -> Self {
        Self {
            job_id,
            spawned: first.spawned,
            leader: first.birth,
        }
    }

    fn lead(&mut self, leader: Birth) {
        self.leader = leader;
    }

    /// The job as observed at `now`, stamped `sampled_at`.
    pub(super) fn observe(
        &self,
        now: Instant,
        sampled_at: UtcTimestamp,
    ) -> Result<JobResourceSample> {
        let wall = WallMicros::of(now.duration_since(self.spawned)).map_err(|error| {
            CowshedError::internal(format!(
                "job {} cannot be sampled: {error}",
                self.job_id.get()
            ))
        })?;
        Ok(JobResourceSample::new(
            self.job_id,
            sampled_at,
            wall,
            self.leader.pid(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn owned(pid: u32, spawned: Instant) -> OwnedProcess {
        OwnedProcess {
            birth: Birth::Unobserved {
                pid,
                reason: "a sampling test names no process".into(),
            },
            spawned,
        }
    }

    fn at() -> UtcTimestamp {
        UtcTimestamp::new("2026-10-07T12:00:00Z").expect("timestamp")
    }

    fn job() -> JobId {
        JobId::new(5).expect("job")
    }

    #[test]
    fn the_spawn_fixes_the_wall_baseline_and_the_command_takes_the_lead() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let activating = sampling
            .own(job(), owned(100, spawn))
            .expect("live")
            .observe(spawn + Duration::from_micros(2_500), at())
            .expect("sample");
        assert_eq!(
            (
                activating.job_id,
                activating.leader_pid,
                activating.wall_us.get()
            ),
            (job(), 100, 2_500)
        );
        assert_eq!(activating.wall_ms.get(), 2);

        let running = sampling
            .own(job(), owned(200, spawn + Duration::from_millis(30)))
            .expect("live")
            .observe(spawn + Duration::from_millis(40), at())
            .expect("sample");
        assert_eq!((running.leader_pid, running.wall_ms.get()), (200, 40));
        assert_eq!(
            running.wall_us.get(),
            40_000,
            "the activation's spawn stays the baseline"
        );
    }

    #[test]
    fn a_failed_terminal_observation_is_frozen_as_the_failure() {
        let mut sampling = Sampling::Unowned;
        sampling.own(job(), owned(100, Instant::now()));
        let failure = CowshedError::internal("the injected observation failed");
        assert_eq!(sampling.freeze(|_| Err(failure.clone())), None);
        for read in [
            sampling.read(job(), |_| {
                unreachable!("an ended job is never observed again")
            }),
            sampling.read(job(), |_| {
                unreachable!("an ended job is never observed again")
            }),
        ] {
            assert_eq!(read.unwrap_err().message, failure.message);
        }
        assert!(
            sampling.own(job(), owned(300, Instant::now())).is_none(),
            "an ended job owns nothing new"
        );
    }

    #[test]
    fn a_terminal_sample_is_frozen_and_a_never_owned_job_has_none() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        sampling.own(job(), owned(100, spawn));
        let terminal = sampling
            .freeze(|sampler| sampler.observe(spawn + Duration::from_millis(7), at()))
            .expect("a terminal sample");
        assert_eq!(terminal.wall_ms.get(), 7);
        assert_eq!(
            sampling
                .read(job(), |_| unreachable!("frozen"))
                .expect("frozen"),
            terminal
        );

        let mut never = Sampling::Unowned;
        assert_eq!(never.freeze(|_| unreachable!("nothing to observe")), None);
        assert_eq!(
            never
                .read(job(), |_| unreachable!("nothing to observe"))
                .unwrap_err()
                .code,
            crate::error::ErrorCode::Conflict
        );
    }
}
