//! A job's resource sampling (07_api.md "Job monitoring"), from the first process the job owns
//! until its terminal sample.
//!
//! The first owned process is the job's spawn: its shell activation on a cold host, otherwise its
//! command. That spawn fixes the job's baselines; the leader moves to the command's group when
//! the command starts, and the baselines stay.

use std::time::Instant;

use super::job_groups::Birth;
use crate::api::dto::{JobId, UtcTimestamp};
use crate::api::resources::{JobResourceSample, WallMicros};
use crate::error::{CowshedError, Result};

pub(super) struct JobSampler {
    job_id: JobId,
    /// When the job's first owned process spawned.
    spawned: Instant,
    /// The process that leads the job's group now, as its parent observed it.
    leader: Birth,
}

impl JobSampler {
    /// Sampling for a job whose first process, `leader`, spawned at `spawned`.
    pub(super) fn spawned(job_id: JobId, leader: Birth, spawned: Instant) -> Self {
        Self {
            job_id,
            spawned,
            leader,
        }
    }

    /// The command started after its activation: its group leads the job from now on.
    pub(super) fn lead(&mut self, leader: Birth) {
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

    fn unobserved(pid: u32) -> Birth {
        Birth::Unobserved {
            pid,
            reason: "a sampling test names no process".into(),
        }
    }

    fn at() -> UtcTimestamp {
        UtcTimestamp::new("2026-10-07T12:00:00Z").expect("timestamp")
    }

    #[test]
    fn the_spawn_fixes_the_wall_baseline_and_the_command_takes_the_lead() {
        let job = JobId::new(5).expect("job");
        let spawn = Instant::now();
        let mut sampler = JobSampler::spawned(job, unobserved(100), spawn);

        let activating = sampler
            .observe(spawn + Duration::from_micros(2_500), at())
            .expect("sample");
        assert_eq!(
            (
                activating.job_id,
                activating.leader_pid,
                activating.wall_us.get()
            ),
            (job, 100, 2_500)
        );
        assert_eq!(activating.wall_ms.get(), 2);

        sampler.lead(unobserved(200));
        let running = sampler
            .observe(spawn + Duration::from_millis(40), at())
            .expect("sample");
        assert_eq!((running.leader_pid, running.wall_ms.get()), (200, 40));
        assert_eq!(
            running.wall_us.get(),
            40_000,
            "the activation's spawn stays the baseline"
        );
    }
}
