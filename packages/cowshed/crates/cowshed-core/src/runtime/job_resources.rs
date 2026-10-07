//! A job's resource sampling (07_api.md "Job monitoring"), from the first process the job owns
//! until its terminal sample.
//!
//! The first owned process is the job's spawn: its shell activation on a cold host, otherwise its
//! command. That spawn fixes the job's baselines; the leader moves to the command's group when
//! the command starts, and the baselines stay.

use std::time::Instant;

use super::job_groups::Birth;
use super::supervisor::{OwnedProcess, byte_count};
use crate::api::dto::{JobId, UtcTimestamp};
use crate::api::resources::{
    HostLoadSample, JobAccounting, JobResourceSample, JobStreamWatermark, ResidentBytes,
    ResourceUnitError, StreamBytes, StreamLines, WallMicros,
};
use crate::error::{CowshedError, Result};
use crate::host_load::HostLoadError;

/// What a job's resource reads answer, through its life.
pub(super) enum Sampling {
    /// No process of the job's exists yet -- or, once the job ended, none ever did.
    Unowned,
    /// The job owns processes: a read observes them now.
    Live(JobSampler),
    /// The job ended: the process that last led its group, and its terminal sample or why that
    /// last observation failed. A failure is kept as it is: an earlier sample would claim the
    /// job's end looked like its middle.
    Frozen {
        leader: Birth,
        outcome: Result<JobResourceSample>,
    },
}

impl Sampling {
    /// Whether the job owns processes a read observes now.
    pub(super) fn owns(&self) -> bool {
        matches!(self, Self::Live(_))
    }

    /// The job owns `process`. Its first process starts the sampling; a later one -- its
    /// command, after its activation -- takes the lead and keeps the spawn's baselines. Returns
    /// the sampler to observe, or `None` once the job ended.
    pub(super) fn own(&mut self, job_id: JobId, process: OwnedProcess) -> Option<&mut JobSampler> {
        match self {
            Self::Unowned => {
                *self = Self::Live(JobSampler::spawned(job_id, process));
                match self {
                    Self::Live(sampler) => Some(sampler),
                    Self::Unowned | Self::Frozen { .. } => None,
                }
            }
            Self::Live(sampler) => {
                sampler.lead(process.birth);
                Some(sampler)
            }
            Self::Frozen { .. } => None,
        }
    }

    /// The job ended: take its terminal sample through `observe` and keep it, or keep why it
    /// could not be taken. Returns the terminal sample, if one exists.
    pub(super) fn freeze(
        &mut self,
        observe: impl FnOnce(&mut JobSampler) -> Result<JobResourceSample>,
    ) -> Option<JobResourceSample> {
        if let Self::Live(_) = self
            && let Self::Live(mut sampler) = std::mem::replace(self, Self::Unowned)
        {
            let outcome = observe(&mut sampler);
            *self = Self::Frozen {
                leader: sampler.leader,
                outcome,
            };
        }
        match self {
            Self::Frozen {
                outcome: Ok(sample),
                ..
            } => Some(sample.clone()),
            Self::Unowned
            | Self::Live(_)
            | Self::Frozen {
                outcome: Err(_), ..
            } => None,
        }
    }

    /// A resources read: a live job observed now through `observe`, an ended job's frozen
    /// outcome, or why there is nothing to sample.
    pub(super) fn read(
        &mut self,
        job_id: JobId,
        observe: impl FnOnce(&mut JobSampler) -> Result<JobResourceSample>,
    ) -> Result<JobResourceSample> {
        match self {
            Self::Live(sampler) => observe(sampler),
            Self::Frozen { outcome, .. } => outcome.clone(),
            Self::Unowned => Err(CowshedError::conflict(
                format!(
                    "job {} owns no process: it has not started one, or ended before it did",
                    job_id.get()
                ),
                "read its resources once the job has started; its status says how it ended",
            )),
        }
    }

    /// The sampler of a job that owns processes and has not ended.
    #[cfg(target_os = "macos")]
    pub(super) fn live(&mut self) -> Option<&mut JobSampler> {
        match self {
            Self::Live(sampler) => Some(sampler),
            Self::Unowned | Self::Frozen { .. } => None,
        }
    }

    /// The process that leads the job's group now, or last led it once the job ended; `None`
    /// while no process of the job's exists, or if none ever did.
    pub(super) fn leader(&self) -> Option<&Birth> {
        match self {
            Self::Live(sampler) => Some(sampler.leader()),
            Self::Frozen { leader, .. } => Some(leader),
            Self::Unowned => None,
        }
    }
}

/// What the system said about a job's processes at one sample boundary.
pub(super) struct Observation {
    pub now: Instant,
    pub sampled_at: UtcTimestamp,
    pub host: HostLoadSample,
    /// Every process of the group the sampler's leader leads that was running when its resident
    /// memory was read: the complete membership, less any member that exited before its read.
    pub members: Vec<Member>,
    /// The job's output streams as the supervisor had admitted them by the boundary.
    pub stdout: StreamTally,
    pub stderr: StreamTally,
    /// The job's CPU totals from its platform's independent source, read at the boundary and
    /// within that source's named limits; `None` where no source exists yet.
    pub accounting: Option<JobAccounting>,
}

/// One process of the job's group, as read at a sample boundary.
pub(super) struct Member {
    pub pid: u32,
    /// Its resident memory, read at this boundary.
    pub resident: ResidentBytes,
}

/// One output stream's running counts, folded over each chunk the supervisor admits. A line a
/// chunk leaves open is the next chunk's to end, so a chunk boundary never splits one in two.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct StreamTally {
    /// Admitted bytes: the stream's next read cursor.
    bytes: u64,
    /// Admitted `\n` bytes: the lines ended.
    newlines: u64,
    /// Whether the last admitted byte is not a `\n`: a line no `\n` has ended yet.
    open: bool,
}

impl StreamTally {
    /// Count `chunk`, admitted after every chunk before it.
    pub(super) fn admit(&mut self, chunk: &[u8]) {
        let Some(&last) = chunk.last() else {
            return;
        };
        self.bytes += byte_count(chunk.len());
        self.newlines += byte_count(chunk.iter().filter(|&&byte| byte == b'\n').count());
        self.open = last != b'\n';
    }

    /// The stream's next read cursor.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The stream's counts as its sample carries them: its trailing open line counts.
    fn watermark(&self) -> std::result::Result<JobStreamWatermark, ResourceUnitError> {
        Ok(JobStreamWatermark {
            bytes: StreamBytes::new(self.bytes)?,
            lines: StreamLines::new(self.newlines + u64::from(self.open))?,
        })
    }
}

pub(super) struct JobSampler {
    job_id: JobId,
    /// When the job's first owned process spawned.
    spawned: Instant,
    /// The process that leads the job's group now, as its parent observed it.
    leader: Birth,
    /// The first spawn's observation, never replaced by a later process or sample.
    host_start: std::result::Result<HostLoadSample, HostLoadError>,
    /// The largest group resident sum a sample of this job has observed.
    rss_peak: ResidentBytes,
    /// The job's CPU source: each leader's own and reaped-children rusage, the activation's once.
    #[cfg(target_os = "macos")]
    rusage: super::job_accounting::RusageChildren,
}

impl JobSampler {
    fn spawned(job_id: JobId, first: OwnedProcess) -> Self {
        Self {
            job_id,
            spawned: first.spawned,
            #[cfg(target_os = "macos")]
            rusage: super::job_accounting::RusageChildren::charging(first.birth.clone()),
            leader: first.birth,
            host_start: first.host,
            rss_peak: ResidentBytes::ZERO,
        }
    }

    fn lead(&mut self, leader: Birth) {
        #[cfg(target_os = "macos")]
        self.rusage.lead(leader.clone());
        self.leader = leader;
    }

    /// The job's CPU source, which the supervisor reads at each boundary and closes at the end
    /// of an activation.
    #[cfg(target_os = "macos")]
    pub(super) fn rusage(&mut self) -> &mut super::job_accounting::RusageChildren {
        &mut self.rusage
    }

    /// The process whose group is the job's now.
    pub(super) fn leader(&self) -> &Birth {
        &self.leader
    }

    /// The job's sample from what was observed of it. Every observation is checked before the
    /// resident sum joins the job's peak: a sample that cannot be taken leaves the peak as it was.
    pub(super) fn sample(&mut self, observed: Observation) -> Result<JobResourceSample> {
        let host_start = *self.host_start.as_ref().map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "job {} spawn host observation failed: {error}",
                    self.job_id.get()
                ),
                "the command runs on; inspect the host's load and core reporting",
            )
        })?;
        let refused = |what: &str, error: ResourceUnitError| {
            CowshedError::internal(format!(
                "job {} cannot be sampled: {what}{error}",
                self.job_id.get()
            ))
        };
        let wall = WallMicros::of(observed.now.duration_since(self.spawned))
            .map_err(|error| refused("", error))?;
        let rss = observed
            .members
            .iter()
            .try_fold(ResidentBytes::ZERO, |sum, member| {
                sum.checked_add(member.resident)
            })
            .map_err(|error| refused("", error))?;
        let stdout = observed
            .stdout
            .watermark()
            .map_err(|error| refused("stdout ", error))?;
        let stderr = observed
            .stderr
            .watermark()
            .map_err(|error| refused("stderr ", error))?;
        self.rss_peak = self.rss_peak.max(rss);
        Ok(JobResourceSample {
            job_id: self.job_id,
            sampled_at: observed.sampled_at,
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: self.leader.pid(),
            members: observed.members.iter().map(|member| member.pid).collect(),
            host_start,
            host: observed.host,
            rss_bytes: rss,
            rss_peak_bytes: self.rss_peak,
            stdout,
            stderr,
            accounting: observed.accounting,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::api::resources::MAX_EXACT_INTEGER;

    fn owned(pid: u32, spawned: Instant) -> OwnedProcess {
        OwnedProcess {
            birth: Birth::Unobserved {
                pid,
                reason: "a sampling test names no process".into(),
            },
            spawned,
            host: Ok(HostLoadSample::new(1.25, 8).expect("host snapshot")),
        }
    }

    fn at() -> UtcTimestamp {
        UtcTimestamp::new("2026-10-07T12:00:00Z").expect("timestamp")
    }

    fn job() -> JobId {
        JobId::new(5).expect("job")
    }

    fn seen(now: Instant, members: &[u32]) -> Observation {
        let members: Vec<(u32, u64)> = members.iter().map(|&pid| (pid, 0)).collect();
        held(now, &members)
    }

    /// An observation of `members`, each `(pid, resident bytes)`.
    fn held(now: Instant, members: &[(u32, u64)]) -> Observation {
        Observation {
            now,
            sampled_at: at(),
            host: HostLoadSample::new(1.25, 8).expect("host snapshot"),
            members: members
                .iter()
                .map(|&(pid, resident)| Member {
                    pid,
                    resident: ResidentBytes::new(resident).expect("exact"),
                })
                .collect(),
            stdout: StreamTally::default(),
            stderr: StreamTally::default(),
            accounting: None,
        }
    }

    const MIB: u64 = 1 << 20;

    /// A job whose only process was its activation -- no command ever started -- still names
    /// that activation's group once it ended, failed sample or not, so a read of the ended
    /// job's group never claims the job owned no process.
    #[test]
    fn an_ended_job_keeps_the_leader_its_group_last_had() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        assert_eq!(sampling.leader(), None);
        sampling.own(job(), owned(100, spawn)).expect("live");
        assert_eq!(sampling.leader().map(Birth::pid), Some(100));
        let failed = sampling.freeze(|_| {
            Err(CowshedError::environment_missing(
                "the activation's last observation failed",
                "a test",
            ))
        });
        assert_eq!(failed, None);
        assert_eq!(sampling.leader().map(Birth::pid), Some(100));

        let mut sampling = Sampling::Unowned;
        sampling.own(job(), owned(100, spawn)).expect("live");
        sampling
            .own(job(), owned(200, spawn))
            .expect("the command leads");
        sampling.freeze(|sampler| sampler.sample(seen(spawn, &[])));
        assert_eq!(sampling.leader().map(Birth::pid), Some(200));
    }

    /// The group's peak is the largest simultaneous sum: neither the leader's own memory nor
    /// the sum of every process's separate peak.
    #[test]
    fn the_rss_peak_is_the_largest_group_sum_observed_and_survives_the_end() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sampler = sampling.own(job(), owned(100, spawn)).expect("live");
        let mut fold = |members: &[(u32, u64)]| {
            sampler
                .sample(held(spawn, members))
                .ok()
                .map(|sample| (sample.rss_bytes.get(), sample.rss_peak_bytes.get()))
        };
        assert_eq!(fold(&[(100, 0)]), Some((0, 0)));
        assert_eq!(
            fold(&[(100, 0), (101, 128 * MIB), (102, 128 * MIB)]),
            Some((256 * MIB, 256 * MIB)),
            "two children hold their memory at once"
        );
        assert_eq!(
            fold(&[(100, 0), (103, 128 * MIB)]),
            Some((128 * MIB, 256 * MIB)),
            "a later child's memory is not added to an earlier one's"
        );
        assert_eq!(
            fold(&[(100, 0), (104, 128 * MIB)]),
            Some((128 * MIB, 256 * MIB))
        );
        // A sample that cannot be taken leaves the peak as it was.
        assert_eq!(
            fold(&[(100, MAX_EXACT_INTEGER), (101, MAX_EXACT_INTEGER)]),
            None,
            "a sum no projection holds exactly is an error"
        );
        // Nor does one whose stream count is inexact, however much its members hold.
        let inexact = StreamTally {
            bytes: MAX_EXACT_INTEGER + 1,
            newlines: 0,
            open: true,
        };
        assert!(
            sampler
                .sample(Observation {
                    stdout: inexact,
                    ..held(spawn, &[(100, 512 * MIB)])
                })
                .is_err()
        );

        let terminal = sampling
            .freeze(|sampler| sampler.sample(seen(spawn, &[])))
            .expect("a terminal sample");
        assert_eq!(
            (terminal.rss_bytes.get(), terminal.rss_peak_bytes.get()),
            (0, 256 * MIB),
            "the terminal sample keeps the job's peak"
        );
        assert_eq!(
            sampling
                .read(job(), |_| unreachable!("frozen"))
                .expect("frozen"),
            terminal
        );
    }

    fn tally(chunks: &[&[u8]]) -> StreamTally {
        chunks
            .iter()
            .fold(StreamTally::default(), |mut tally, chunk| {
                tally.admit(chunk);
                tally
            })
    }

    fn counts(watermark: JobStreamWatermark) -> (u64, u64) {
        (watermark.bytes.get(), watermark.lines.get())
    }

    #[test]
    fn a_stream_counts_every_admitted_byte_and_line_whatever_line_a_chunk_splits() {
        let mut stream = StreamTally::default();
        let mut seen = Vec::new();
        for chunk in [&b"a"[..], b"\nb", b"\n", b"", b"c"] {
            stream.admit(chunk);
            seen.push(counts(stream.watermark().expect("exact")));
        }
        assert_eq!(
            seen,
            [(1, 1), (3, 2), (4, 2), (4, 2), (5, 3)],
            "an open line counts once, however many chunks it spans; an empty chunk changes nothing"
        );
        assert_eq!(stream.bytes(), 5, "bytes is the next read cursor");
        assert_eq!(counts(tally(&[]).watermark().expect("exact")), (0, 0));
        assert_eq!(
            counts(tally(&[b"\n\n", b"\n"]).watermark().expect("exact")),
            (3, 3),
            "every newline ends a line"
        );
        assert_eq!(
            tally(&[b"a", b"\nb", b"\n", b"c"]),
            tally(&[b"a\nb\nc"]),
            "the count is the admitted bytes', not their chunking's"
        );
    }

    #[test]
    fn a_sample_carries_the_streams_as_admitted_by_its_boundary() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sample = sampling
            .own(job(), owned(100, spawn))
            .expect("live")
            .sample(Observation {
                stdout: tally(&[b"a", b"\nb", b"\n", b"c"]),
                stderr: tally(&[b"oops\n"]),
                ..seen(spawn, &[100])
            })
            .expect("sample");
        assert_eq!(
            (counts(sample.stdout), counts(sample.stderr)),
            ((5, 3), (5, 1))
        );
        assert!(sample.consistent());
    }

    #[test]
    fn the_spawn_fixes_the_wall_baseline_and_the_command_takes_the_lead() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let activating = sampling
            .own(job(), owned(100, spawn))
            .expect("live")
            .sample(seen(spawn + Duration::from_micros(2_500), &[100, 101]))
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
        assert_eq!(activating.members, [100, 101]);

        let running = sampling
            .own(job(), owned(200, spawn + Duration::from_millis(30)))
            .expect("live")
            .sample(seen(spawn + Duration::from_millis(40), &[200]))
            .expect("sample");
        assert_eq!((running.leader_pid, running.wall_ms.get()), (200, 40));
        assert_eq!(
            running.members,
            [200],
            "the command's group is the job's now"
        );
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
            .freeze(|sampler| sampler.sample(seen(spawn + Duration::from_millis(7), &[])))
            .expect("a terminal sample");
        assert_eq!(
            (terminal.leader_pid, terminal.members.len()),
            (100, 0),
            "the leader is named after its group emptied"
        );
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

    #[test]
    fn host_start_stays_at_first_ownership_while_current_host_changes() {
        let spawn = Instant::now();
        let start = HostLoadSample::new(1.25, 8).expect("start");
        let current = HostLoadSample::new(9.5, 12).expect("current");
        let later = HostLoadSample::new(3.0, 16).expect("later");
        let mut sampling = Sampling::Unowned;
        let activating = sampling
            .own(
                job(),
                OwnedProcess {
                    host: Ok(start),
                    ..owned(100, spawn)
                },
            )
            .expect("live")
            .sample(Observation {
                host: current,
                ..seen(spawn + Duration::from_millis(10), &[100])
            })
            .expect("activation sample");
        assert_eq!((activating.host_start, activating.host), (start, current));

        let running = sampling
            .own(
                job(),
                OwnedProcess {
                    host: Ok(later),
                    ..owned(200, spawn + Duration::from_millis(20))
                },
            )
            .expect("live")
            .sample(Observation {
                host: later,
                ..seen(spawn + Duration::from_millis(30), &[200])
            })
            .expect("command sample");
        assert_eq!((running.host_start, running.host), (start, later));
    }

    #[test]
    fn an_unavailable_spawn_load_remains_a_typed_failure_not_a_later_baseline() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sampler = sampling
            .own(
                job(),
                OwnedProcess {
                    host: Err(HostLoadError::LoadUnavailable { returned: -1 }),
                    ..owned(100, spawn)
                },
            )
            .expect("live process");
        let failure = sampler
            .sample(seen(spawn + Duration::from_millis(10), &[100]))
            .expect_err("the spawn baseline is unavailable");
        assert_eq!(failure.code, crate::error::ErrorCode::EnvironmentMissing);
        assert!(failure.message.contains("spawn host observation failed"));
        assert!(failure.message.contains("getloadavg"));
    }
}
