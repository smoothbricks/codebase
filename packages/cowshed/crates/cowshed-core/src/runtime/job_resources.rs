//! A job's resource sampling (07_api.md "Job monitoring"), from the first process the job owns
//! until its terminal sample.
//!
//! The first owned process is the job's spawn: its shell activation on a cold host, otherwise its
//! command. That spawn fixes the job's baselines; the leader moves to the command's group when
//! the command starts, and the baselines stay.

use std::time::Instant;

use super::job_groups::Birth;
use super::supervisor::{LeafCoverage, OwnedProcess, byte_count};
use crate::api::dto::{JobId, UtcTimestamp};
use crate::api::process::JobLeafAttribution;
use crate::api::resources::{
    CpuMicros, HostLoadSample, JobAccounting, JobResourceSample, JobStreamWatermark,
    JobVolumeUsage, OneCoreCpuPercent, ResidentBytes, ResourceUnitError, StreamBytes, StreamLines,
    WallMicros,
};
use crate::error::{CowshedError, Result};
use crate::host_load::HostLoadError;

/// What a job's resource reads answer, through its life.
pub(super) enum Sampling {
    /// No process of the job's exists yet -- or, once the job ended, none ever did.
    Unowned,
    /// The job owns processes: a read observes them now.
    Live(Box<JobSampler>),
    /// The job ended: the process that last led its group, and its terminal sample or why that
    /// last observation failed. A failure is kept as it is: an earlier sample would claim the
    /// job's end looked like its middle.
    ///
    /// Both payloads are boxed, each allocated once per job (at its first owned process and at
    /// its end, never per sample): which one is larger differs by platform -- the macOS sampler
    /// carries its rusage source and census, a terminal sample its leaf and accounting -- so no
    /// job's state is the size of the larger of the two.
    Frozen {
        leader: Birth,
        outcome: Box<Result<JobResourceSample>>,
    },
}

impl Sampling {
    /// Whether the job owns processes a read observes now.
    pub(super) fn owns(&self) -> bool {
        matches!(self, Self::Live(_))
    }

    /// The job owns `process`. Its first process starts the sampling, which judges the job's
    /// leaf by `coverage`; a later one -- its command, after its activation -- takes the lead
    /// and keeps the spawn's baselines. Returns the sampler to observe, or `None` once the job
    /// ended.
    pub(super) fn own(
        &mut self,
        job_id: JobId,
        process: OwnedProcess,
        coverage: LeafCoverage,
    ) -> Option<&mut JobSampler> {
        match self {
            Self::Unowned => {
                *self = Self::Live(Box::new(JobSampler::spawned(job_id, process, coverage)));
                match self {
                    Self::Live(sampler) => Some(sampler.as_mut()),
                    Self::Unowned | Self::Frozen { .. } => None,
                }
            }
            Self::Live(sampler) => {
                sampler.lead(process.birth);
                Some(sampler.as_mut())
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
            let outcome = Box::new(observe(sampler.as_mut()));
            *self = Self::Frozen {
                leader: sampler.leader,
                outcome,
            };
        }
        match self {
            Self::Frozen { outcome, .. } => outcome.as_ref().as_ref().ok().cloned(),
            Self::Unowned | Self::Live(_) => None,
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
            Self::Live(sampler) => observe(sampler.as_mut()),
            Self::Frozen { outcome, .. } => outcome.as_ref().clone(),
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
    pub(super) fn live(&mut self) -> Option<&mut JobSampler> {
        match self {
            Self::Live(sampler) => Some(sampler.as_mut()),
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
    /// Each of the job's volumes against its admission's baseline, read at this boundary.
    pub volumes: JobVolumeUsage,
    /// The job's output streams as the supervisor had admitted them by the boundary.
    pub stdout: StreamTally,
    pub stderr: StreamTally,
    /// The job's totals from its platform's independent source, read at the boundary and
    /// reconciled against the processes observed at it.
    pub accounting: JobAccounting,
    /// The observed process the job's CPU went to most, or why none is named.
    pub leaf: JobLeafAttribution,
}

/// Who takes a sample: the supervisor's own boundaries -- first ownership, a new leader, its
/// sampling clock, the terminal sample -- close the CPU window and open the next; a read
/// measures from the open window without moving it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Boundary {
    Supervisor,
    Read,
}

/// Where the CPU window a sample measures opened: the supervisor's preceding boundary.
#[derive(Clone, Copy, Debug)]
struct CpuWindow {
    at: Instant,
    cpu: CpuMicros,
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
    /// The CPU window the next sample measures; `None` before the first supervisor boundary.
    window: Option<CpuWindow>,
    /// The share of the job's accounted CPU its observed processes must cover for a leaf.
    coverage: LeafCoverage,
    /// The job's CPU source: each leader's own and reaped-children rusage, the activation's once.
    #[cfg(target_os = "macos")]
    rusage: super::job_accounting::RusageChildren,
    /// What the sampler saw of the group's processes at its boundaries.
    #[cfg(target_os = "macos")]
    census: super::job_attribution::Census,
}

impl JobSampler {
    fn spawned(job_id: JobId, first: OwnedProcess, coverage: LeafCoverage) -> Self {
        Self {
            job_id,
            spawned: first.spawned,
            #[cfg(target_os = "macos")]
            rusage: super::job_accounting::RusageChildren::charging(first.birth.clone()),
            #[cfg(target_os = "macos")]
            census: super::job_attribution::Census::led_by(first.birth.pid()),
            leader: first.birth,
            host_start: first.host,
            rss_peak: ResidentBytes::ZERO,
            window: None,
            coverage,
        }
    }

    fn lead(&mut self, leader: Birth) {
        #[cfg(target_os = "macos")]
        {
            self.rusage.lead(leader.clone());
            self.census.lead(leader.pid());
        }
        self.leader = leader;
    }

    /// The job's CPU source, which the supervisor reads at each boundary and closes at the end
    /// of an activation.
    #[cfg(target_os = "macos")]
    pub(super) fn rusage(&mut self) -> &mut super::job_accounting::RusageChildren {
        &mut self.rusage
    }

    /// What the sampler saw of the group's processes, which the supervisor adds each boundary's
    /// members to.
    #[cfg(target_os = "macos")]
    pub(super) fn census(&mut self) -> &mut super::job_attribution::Census {
        &mut self.census
    }

    /// The share of the job's accounted CPU its observed processes must cover for a leaf.
    #[cfg(target_os = "macos")]
    pub(super) fn coverage(&self) -> LeafCoverage {
        self.coverage
    }

    /// The process whose group is the job's now.
    pub(super) fn leader(&self) -> &Birth {
        &self.leader
    }

    /// The job's sample from what was observed of it at a `boundary`. Every observation is
    /// checked before the resident sum joins the job's peak and a supervisor boundary moves the
    /// CPU window: a sample that cannot be taken leaves both as they were.
    pub(super) fn sample(
        &mut self,
        observed: Observation,
        boundary: Boundary,
    ) -> Result<JobResourceSample> {
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
        // The leaf was judged by this job's threshold, or the sample would claim one it was not.
        if let JobLeafAttribution::Unattributed {
            required_permille, ..
        } = observed.leaf
            && required_permille != self.coverage.permille()
        {
            return Err(CowshedError::internal(format!(
                "job {} cannot be sampled: its leaf was judged by {required_permille}‰, not its \
                 {}‰",
                self.job_id.get(),
                self.coverage.permille()
            )));
        }
        let cpu = observed.accounting.cpu();
        let cpu_total = cpu
            .user_us
            .checked_add(cpu.sys_us)
            .map_err(|error| refused("CPU ", error))?;
        // The first supervisor boundary opens the first window: no time has passed in it. A
        // window with no whole microsecond in it divides nothing and stays open, so the CPU spent
        // in it is measured by the next.
        let (cpu_pct, elapsed) = match self.window {
            None => (OneCoreCpuPercent::ZERO, None),
            Some(window) => {
                let spent = cpu_total
                    .get()
                    .checked_sub(window.cpu.get())
                    .ok_or_else(|| {
                        CowshedError::internal(format!(
                            "job {} cannot be sampled: its CPU fell from {} us to {} us",
                            self.job_id.get(),
                            window.cpu.get(),
                            cpu_total.get()
                        ))
                    })
                    .and_then(|spent| CpuMicros::new(spent).map_err(|error| refused("", error)))?;
                let elapsed = WallMicros::of(observed.now.saturating_duration_since(window.at))
                    .map_err(|error| refused("", error))?;
                (OneCoreCpuPercent::of_window(spent, elapsed), Some(elapsed))
            }
        };
        self.rss_peak = self.rss_peak.max(rss);
        if boundary == Boundary::Supervisor && elapsed.is_none_or(|elapsed| elapsed.get() > 0) {
            self.window = Some(CpuWindow {
                at: observed.now,
                cpu: cpu_total,
            });
        }
        Ok(JobResourceSample {
            job_id: self.job_id,
            sampled_at: observed.sampled_at,
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: self.leader.pid(),
            members: observed.members.iter().map(|member| member.pid).collect(),
            leaf: observed.leaf,
            cpu_user_ms: cpu.user_us.millis(),
            cpu_sys_ms: cpu.sys_us.millis(),
            cpu_pct,
            host_start,
            host: observed.host,
            rss_bytes: rss,
            rss_peak_bytes: self.rss_peak,
            volumes: observed.volumes,
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
    use crate::api::resources::{
        MAX_EXACT_INTEGER, VolumeUnavailable, VolumeUsage, VolumeUsedBytesDelta,
    };

    fn coverage() -> LeafCoverage {
        LeafCoverage::new(900).expect("a share")
    }

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
            volumes: JobVolumeUsage {
                workspace: VolumeUsage::Unavailable {
                    reason: VolumeUnavailable::Unconfigured,
                },
                build: None,
            },
            stdout: StreamTally::default(),
            stderr: StreamTally::default(),
            accounting: accounted(0),
            leaf: JobLeafAttribution::NoAccountedCpu,
        }
    }

    /// The macOS source's totals: `user_us` of user CPU, the observed processes reconciled.
    fn accounted(user_us: u64) -> JobAccounting {
        JobAccounting::MacOsRusageChildren {
            cpu: crate::api::resources::CpuTotals {
                user_us: CpuMicros::new(user_us).expect("exact"),
                sys_us: CpuMicros::new(0).expect("exact"),
            },
            io: None,
            reconciliation: crate::api::resources::UsageReconciliation {
                cpu_us: crate::api::resources::CpuMicrosDelta::new(0).expect("exact"),
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: crate::api::process::ProcessCoverage::Gap {
                    reason: crate::api::process::ProcessCoverageGap::UncountedFork { pid: 100 },
                },
            },
        }
    }

    /// An observation at `now` of a job whose source counted `user_us` of CPU.
    fn burned(now: Instant, user_us: u64) -> Observation {
        Observation {
            accounting: accounted(user_us),
            ..seen(now, &[100])
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
        sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live");
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
        sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live");
        sampling
            .own(job(), owned(200, spawn), coverage())
            .expect("the command leads");
        sampling.freeze(|sampler| sampler.sample(seen(spawn, &[]), Boundary::Supervisor));
        assert_eq!(sampling.leader().map(Birth::pid), Some(200));
    }

    /// The group's peak is the largest simultaneous sum: neither the leader's own memory nor
    /// the sum of every process's separate peak.
    #[test]
    fn the_rss_peak_is_the_largest_group_sum_observed_and_survives_the_end() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sampler = sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live");
        let mut fold = |members: &[(u32, u64)]| {
            sampler
                .sample(held(spawn, members), Boundary::Supervisor)
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
                .sample(
                    Observation {
                        stdout: inexact,
                        ..held(spawn, &[(100, 512 * MIB)])
                    },
                    Boundary::Supervisor
                )
                .is_err()
        );

        let terminal = sampling
            .freeze(|sampler| sampler.sample(seen(spawn, &[]), Boundary::Supervisor))
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
            .own(job(), owned(100, spawn), coverage())
            .expect("live")
            .sample(
                Observation {
                    stdout: tally(&[b"a", b"\nb", b"\n", b"c"]),
                    stderr: tally(&[b"oops\n"]),
                    ..seen(spawn, &[100])
                },
                Boundary::Supervisor,
            )
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
            .own(job(), owned(100, spawn), coverage())
            .expect("live")
            .sample(
                seen(spawn + Duration::from_micros(2_500), &[100, 101]),
                Boundary::Supervisor,
            )
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
            .own(
                job(),
                owned(200, spawn + Duration::from_millis(30)),
                coverage(),
            )
            .expect("live")
            .sample(
                seen(spawn + Duration::from_millis(40), &[200]),
                Boundary::Supervisor,
            )
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

    /// Each volume's usage is what its observation read at the boundary, and the terminal sample
    /// keeps the last of it.
    #[test]
    fn a_sample_carries_each_volume_as_observed_and_the_terminal_keeps_it() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let volumes = |workspace: i64, build: VolumeUsage| JobVolumeUsage {
            workspace: VolumeUsage::Read {
                delta_bytes: VolumeUsedBytesDelta::new(workspace).expect("exact"),
            },
            build: Some(build),
        };
        let failed = VolumeUsage::Unavailable {
            reason: VolumeUnavailable::Failed {
                message: "the build volume left".into(),
            },
        };
        let activating = sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live")
            .sample(
                Observation {
                    volumes: volumes(8 << 20, failed.clone()),
                    ..seen(spawn, &[100])
                },
                Boundary::Supervisor,
            )
            .expect("sample");
        assert_eq!(activating.volumes, volumes(8 << 20, failed.clone()));
        let read = VolumeUsage::Read {
            delta_bytes: VolumeUsedBytesDelta::new(-(4 << 20)).expect("exact"),
        };
        let terminal = sampling
            .freeze(|sampler| {
                sampler.sample(
                    Observation {
                        volumes: volumes(24 << 20, read.clone()),
                        ..seen(spawn, &[])
                    },
                    Boundary::Supervisor,
                )
            })
            .expect("a terminal sample");
        assert_eq!(terminal.volumes, volumes(24 << 20, read));
        assert_eq!(
            sampling
                .read(job(), |_| unreachable!("frozen"))
                .expect("frozen")
                .volumes,
            terminal.volumes
        );
    }

    #[test]
    fn a_failed_terminal_observation_is_frozen_as_the_failure() {
        let mut sampling = Sampling::Unowned;
        sampling.own(job(), owned(100, Instant::now()), coverage());
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
            sampling
                .own(job(), owned(300, Instant::now()), coverage())
                .is_none(),
            "an ended job owns nothing new"
        );
    }

    #[test]
    fn a_terminal_sample_is_frozen_and_a_never_owned_job_has_none() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        sampling.own(job(), owned(100, spawn), coverage());
        let terminal = sampling
            .freeze(|sampler| {
                sampler.sample(
                    seen(spawn + Duration::from_millis(7), &[]),
                    Boundary::Supervisor,
                )
            })
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
                coverage(),
            )
            .expect("live")
            .sample(
                Observation {
                    host: current,
                    ..seen(spawn + Duration::from_millis(10), &[100])
                },
                Boundary::Supervisor,
            )
            .expect("activation sample");
        assert_eq!((activating.host_start, activating.host), (start, current));

        let running = sampling
            .own(
                job(),
                OwnedProcess {
                    host: Ok(later),
                    ..owned(200, spawn + Duration::from_millis(20))
                },
                coverage(),
            )
            .expect("live")
            .sample(
                Observation {
                    host: later,
                    ..seen(spawn + Duration::from_millis(30), &[200])
                },
                Boundary::Supervisor,
            )
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
                coverage(),
            )
            .expect("live process");
        let failure = sampler
            .sample(
                seen(spawn + Duration::from_millis(10), &[100]),
                Boundary::Supervisor,
            )
            .expect_err("the spawn baseline is unavailable");
        assert_eq!(failure.code, crate::error::ErrorCode::EnvironmentMissing);
        assert!(failure.message.contains("spawn host observation failed"));
        assert!(failure.message.contains("getloadavg"));
    }

    /// The share is the CPU spent over the window since the supervisor's preceding boundary:
    /// 200 for two busy cores, 0 for an idle window, and 0 for the first window, in which no
    /// time has passed.
    #[test]
    fn the_cpu_share_is_the_supervisor_window_s_cpu_over_its_wall_time() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sampler = sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live");
        let mut share = |at_ms: u64, user_us: u64| {
            let sample = sampler
                .sample(
                    burned(spawn + Duration::from_millis(at_ms), user_us),
                    Boundary::Supervisor,
                )
                .expect("sample");
            assert!(sample.consistent(), "{sample:?}");
            (sample.cpu_pct.get(), sample.cpu_user_ms.get())
        };
        assert_eq!(share(0, 3_000), (0.0, 3), "the first window is zero-length");
        assert_eq!(
            share(1_000, 2_003_000),
            (200.0, 2_003),
            "two cores for a second"
        );
        assert_eq!(share(2_000, 2_003_000), (0.0, 2_003), "an idle second");
        assert_eq!(
            share(2_000, 2_003_500),
            (0.0, 2_003),
            "a zero-length window"
        );
        assert_eq!(
            share(2_500, 2_253_500),
            (50.1, 2_253),
            "the next window measures the CPU the zero-length one held: 250.5 ms in 500 ms"
        );
    }

    /// Reads measure from the supervisor's open window and never move it: two readers between
    /// two boundaries get the same share for the same CPU, and the next boundary measures from
    /// the previous boundary, not from either read.
    #[test]
    fn reads_never_move_the_cpu_window() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sampler = sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live");
        let mut take = |at_ms: u64, user_us: u64, boundary: Boundary| {
            sampler
                .sample(
                    burned(spawn + Duration::from_millis(at_ms), user_us),
                    boundary,
                )
                .expect("sample")
                .cpu_pct
                .get()
        };
        take(0, 0, Boundary::Supervisor);
        assert_eq!(take(500, 500_000, Boundary::Read), 100.0);
        assert_eq!(take(500, 500_000, Boundary::Read), 100.0, "a second reader");
        assert_eq!(
            take(1_000, 500_000, Boundary::Read),
            50.0,
            "measured from the boundary"
        );
        assert_eq!(
            take(2_000, 1_000_000, Boundary::Supervisor),
            50.0,
            "the boundary's window opened at the previous boundary"
        );
        assert_eq!(take(3_000, 1_000_000, Boundary::Read), 0.0);
    }

    /// CPU that falls is a broken source, never a negative share; the window stays where it was.
    #[test]
    fn falling_cpu_is_an_error_that_leaves_the_window() {
        let spawn = Instant::now();
        let mut sampling = Sampling::Unowned;
        let sampler = sampling
            .own(job(), owned(100, spawn), coverage())
            .expect("live");
        sampler
            .sample(burned(spawn, 1_000), Boundary::Supervisor)
            .expect("sample");
        let failure = sampler
            .sample(
                burned(spawn + Duration::from_millis(10), 999),
                Boundary::Supervisor,
            )
            .expect_err("CPU fell");
        assert_eq!(
            failure.code,
            crate::error::ErrorCode::Internal,
            "{}",
            failure.message
        );
        let sample = sampler
            .sample(
                burned(spawn + Duration::from_millis(20), 11_000),
                Boundary::Supervisor,
            )
            .expect("sample");
        assert_eq!(
            sample.cpu_pct.get(),
            50.0,
            "measured from the last good boundary"
        );
    }
}
