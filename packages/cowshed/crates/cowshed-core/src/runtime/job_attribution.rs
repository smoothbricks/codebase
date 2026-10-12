//! Which observed process a job's CPU went to, and how the job's independent totals compare
//! with what its observed processes counted as their own (07_api.md, "Process-tree observations"
//! and "Complete job accounting and observation reconciliation").
//!
//! The census is what the sampler saw of the job's group at its sample boundaries: each member
//! it found, the image that member ran and its own counters. Polling a group at boundaries never
//! sees a process born and gone between two of them, so the census never claims a complete tree:
//! its coverage names the first observation it is known to lack, and with none known, that the
//! leader may have forked unseen.
//!
//! The fold is pure: the sampler is its only writer, and [`observe`] is the thin kernel shell
//! that feeds it on macOS.

use std::collections::HashMap;

use crate::api::process::{
    JobLeafAttribution, JobProcessLeaf, PERMILLE_WHOLE, ProcessCoverage, ProcessCoverageGap,
    ProcessUsage,
};
use crate::api::resources::{CpuMicros, CpuMicrosDelta, CpuTotals, ResourceUnitError};
use crate::runtime::job_groups::Process;
use crate::runtime::process_tree::{ProcessIdentity, ProcessImage};
use crate::runtime::process_usage::{ProcessUsageFold, UsageFoldError, UsageObservation};
use crate::runtime::supervisor::LeafCoverage;

/// One process life the census saw.
#[derive(Debug)]
struct Life {
    identity: ProcessIdentity,
    /// The image it ran when last read; `None` while no read of it succeeded.
    image: Option<ProcessImage>,
    /// The boundary it was last found a member at.
    seen_at: u64,
    exited: bool,
}

/// What the sampler saw of a job's processes, folded over its boundaries.
#[derive(Debug)]
pub struct Census {
    /// In the order the census first saw them.
    lives: Vec<Life>,
    by_identity: HashMap<ProcessIdentity, usize>,
    /// Each life's own counters, as last read.
    usage: ProcessUsageFold,
    /// The process that leads the job now.
    leader: u32,
    /// The pids of every process that led the job: their births are the supervisor's own
    /// spawns, so the census knows where they came from.
    leaders: Vec<u32>,
    /// The boundary being taken; lives found at it carry it.
    boundary: u64,
    /// The first observation the census is known to lack.
    gap: Option<ProcessCoverageGap>,
}

/// The census's answer for one boundary: the leaf, and how much of the job's CPU the observed
/// processes counted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribution {
    pub leaf: JobLeafAttribution,
    /// The job's CPU total less the observed processes' own CPU.
    pub cpu_us: CpuMicrosDelta,
    pub coverage: ProcessCoverage,
}

/// One observed life borrowed from the fold that retains it. An unread image or
/// unread usage covers no CPU and is never the leaf.
#[derive(Clone, Copy, Debug)]
pub struct ObservedLife<'a> {
    pub identity: ProcessIdentity,
    pub image: Option<&'a ProcessImage>,
    pub usage: Option<ProcessUsage>,
}

/// The checked observed CPU and, when one life was identified, the leaf that
/// ranked first. Computed once from borrowed lives. It does not retain the
/// tree and does not invent coverage.
#[derive(Clone, Debug)]
pub struct AttributionSummary {
    observed_cpu: CpuMicros,
    winner: Option<JobProcessLeaf>,
}

impl AttributionSummary {
    /// Sums identified own CPU and keeps only the winning leaf. An unread image
    /// or unread usage covers nothing and cannot win. A checked add that fails
    /// is the error; nothing is published.
    pub fn from_observed<'a>(
        lives: impl IntoIterator<Item = ObservedLife<'a>>,
    ) -> Result<Self, ResourceUnitError> {
        let mut observed = CpuMicros::new(0)?;
        let mut winner: Option<(CpuMicros, ProcessIdentity, &ProcessImage)> = None;
        for life in lives {
            let (Some(image), Some(usage)) = (life.image, life.usage) else {
                continue;
            };
            let own = usage.cpu_user_us.checked_add(usage.cpu_sys_us)?;
            observed = observed.checked_add(own)?;
            let rank = |own: CpuMicros, identity: ProcessIdentity| {
                (
                    own,
                    std::cmp::Reverse(identity.birth.0),
                    std::cmp::Reverse(identity.pid),
                )
            };
            if winner.is_none_or(|(most, best, _)| rank(own, life.identity) > rank(most, best)) {
                winner = Some((own, life.identity, image));
            }
        }
        Ok(Self {
            observed_cpu: observed,
            winner: winner.map(|(_, identity, image)| JobProcessLeaf {
                pid: identity.pid,
                program: image.program.clone(),
                argv: image.argv.clone(),
                truncated: image.truncated,
            }),
        })
    }

    /// Applies the independent total and the configured threshold. Clones the
    /// winning leaf only when it is named.
    pub fn attribute(
        &self,
        total: CpuTotals,
        required: LeafCoverage,
        coverage: ProcessCoverage,
    ) -> Result<Attribution, ResourceUnitError> {
        let total = total.user_us.checked_add(total.sys_us)?;
        let cpu_us = CpuMicrosDelta::between(self.observed_cpu, total)?;
        let leaf = match covered_permille(self.observed_cpu, total) {
            None => JobLeafAttribution::NoAccountedCpu,
            Some(permille) => match &self.winner {
                Some(leaf) if permille >= required.permille() => JobLeafAttribution::Attributed {
                    leaf: leaf.clone(),
                    observed_cpu_permille: permille,
                },
                Some(_) | None => JobLeafAttribution::Unattributed {
                    observed_cpu_permille: permille,
                    required_permille: required.permille(),
                },
            },
        };
        Ok(Attribution {
            leaf,
            cpu_us,
            coverage,
        })
    }
}

impl Census {
    /// A census of a job led by `leader`, the job's first owned process.
    pub fn led_by(leader: u32) -> Self {
        Self {
            lives: Vec::new(),
            by_identity: HashMap::new(),
            usage: ProcessUsageFold::default(),
            leader,
            leaders: vec![leader],
            boundary: 0,
            gap: None,
        }
    }

    /// `leader` takes the lead: its birth is the supervisor's spawn too.
    pub fn lead(&mut self, leader: u32) {
        self.leader = leader;
        self.leaders.push(leader);
    }

    /// `process` is a member at this boundary, running `image` if that read succeeded. A member
    /// the census never saw born, one no leader spawn accounts for, is a birth it missed.
    pub fn found(&mut self, process: ProcessIdentity, image: Option<ProcessImage>) {
        let index = match self.by_identity.get(&process) {
            Some(&index) => index,
            None => {
                if !self.leaders.contains(&process.pid) {
                    self.missed(ProcessCoverageGap::UnobservedBirth { pid: process.pid });
                }
                self.lives.push(Life {
                    identity: process,
                    image: None,
                    seen_at: self.boundary,
                    exited: false,
                });
                self.by_identity.insert(process, self.lives.len() - 1);
                self.lives.len() - 1
            }
        };
        let life = &mut self.lives[index];
        life.seen_at = self.boundary;
        match image {
            Some(image) => life.image = Some(image),
            None if life.image.is_none() => {
                self.missed(ProcessCoverageGap::UnreadImage { pid: process.pid });
            }
            None => {}
        }
    }

    /// `process` is a member at this boundary whose image the kernel would not give while it
    /// ran: the census lacks that image, and an image an earlier read named may no longer be the
    /// one it runs, so the life is unidentified until a later read names one again.
    pub fn unread_image(&mut self, process: ProcessIdentity) {
        self.found(process, None);
        if let Some(&index) = self.by_identity.get(&process) {
            self.lives[index].image = None;
        }
        self.missed(ProcessCoverageGap::UnreadImage { pid: process.pid });
    }

    /// Each life's own counters, for the sampler to read into.
    pub fn usage(&mut self) -> &mut ProcessUsageFold {
        &mut self.usage
    }

    /// The boundary is taken: a life found at the previous one and not at this one has left the
    /// group, and the census never read its counters after it did.
    pub fn close(&mut self) -> Result<(), UsageFoldError> {
        for life in &mut self.lives {
            if !life.exited && life.seen_at != self.boundary {
                life.exited = true;
                self.usage.apply(UsageObservation::Exited {
                    process: life.identity,
                })?;
            }
        }
        if let Some(gap) = self.usage.gap() {
            self.missed(gap);
        }
        self.boundary += 1;
        Ok(())
    }

    fn missed(&mut self, gap: ProcessCoverageGap) {
        self.gap.get_or_insert(gap);
    }

    /// The leaf among the lives the census identified, against the job's accounted `total`.
    /// The leaf has the most own CPU, ties going to the earlier birth identity, then the lower
    /// pid, whatever order the census found them in; it is named only when those lives' own CPU
    /// covers `required` of the total.
    pub fn attribute(
        &self,
        total: CpuTotals,
        required: LeafCoverage,
    ) -> Result<Attribution, ResourceUnitError> {
        let coverage = ProcessCoverage::Gap {
            reason: self
                .gap
                .unwrap_or(ProcessCoverageGap::UncountedFork { pid: self.leader }),
        };
        let lives = self.lives.iter().map(|life| ObservedLife {
            identity: life.identity,
            image: life.image.as_ref(),
            usage: self.usage.usage(life.identity),
        });
        AttributionSummary::from_observed(lives)?.attribute(total, required, coverage)
    }
}

/// The thousandths of `total` that `observed` covers, rounded down: never more than the whole,
/// since CPU observed beyond the total covers nothing more of it. `None` for a zero total.
fn covered_permille(observed: CpuMicros, total: CpuMicros) -> Option<u16> {
    let total = u128::from(total.get());
    if total == 0 {
        return None;
    }
    let covered = u128::from(observed.get()).min(total);
    // `covered` is at most `total`, so the share is at most the whole and always fits.
    Some(u16::try_from(covered * u128::from(PERMILLE_WHOLE) / total).unwrap_or(PERMILLE_WHOLE))
}

/// Read each member of the job's group into `census` at `at`: its identity, the image it runs
/// and its own counters, each fenced to the life the membership read found. A member whose pid
/// no longer names that life -- it exited, or the pid was given to another process -- is no
/// member; a read failure of a member still running is an error.
pub fn observe(
    census: &mut Census,
    members: &[Process],
    at: std::time::Instant,
) -> std::result::Result<(), ObserveError> {
    use crate::runtime::process_events::{Darwin, ProcessReads as _};
    use crate::runtime::process_tree::BirthToken;
    use crate::runtime::process_usage::sample;

    let reader = Darwin.reader_uid();
    for member in members {
        let pid = member.pid().cast_unsigned();
        let read = |source| ObserveError::Read { pid, source };
        // Read through the identity the membership read found, never the pid alone.
        let Some(before) = member.record().map_err(read)? else {
            continue;
        };
        let process = ProcessIdentity {
            pid,
            birth: BirthToken(before.unique_id),
        };
        // The image names the pid alone: it is this life's only if the member still holds the
        // pid after the read.
        let image = Darwin.image(pid);
        let after = member.record().map_err(read)?;
        match (image, after) {
            (Ok(image), Some(_)) => census.found(process, Some(image)),
            // The life ended around the read: the image may be a stranger's, or none.
            (Ok(_) | Err(_), None) => census.found(process, None),
            // `EINVAL` is also the kernel's refusal of another user's arguments, which its own
            // credential rule says: an image the census lacks, not a failure.
            (Err(source), Some(record))
                if source.raw_os_error() == Some(libc::EINVAL)
                    && reader != 0
                    && reader != record.uid =>
            {
                census.unread_image(process);
            }
            // The census lacks this image whatever later reads find: the gap stays, beside the
            // failure this read returns.
            (Err(source), Some(_)) => {
                census.unread_image(process);
                return Err(ObserveError::Image { pid, source });
            }
        }
        let start = census.usage().start(process);
        if let crate::runtime::process_usage::Sampled::Read(reading) = sample(start, process, at)? {
            census
                .usage()
                .apply(UsageObservation::Read { process, reading })?;
        }
    }
    census.close()?;
    Ok(())
}

/// Why the census could not read the job's members.
#[derive(Debug, thiserror::Error)]
pub enum ObserveError {
    #[error("reading process {pid} failed: {source}")]
    Read { pid: u32, source: std::io::Error },
    #[error("KERN_PROCARGS2 for running process {pid} failed: {source}")]
    Image { pid: u32, source: std::io::Error },
    #[error(transparent)]
    Sample(#[from] crate::runtime::process_usage::SampleError),
    #[error(transparent)]
    Fold(#[from] UsageFoldError),
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::api::dto::CommandArg;
    use crate::api::process::{ProcessIoUnavailable, ProcessStorageIo};
    use crate::api::resources::ResidentBytes;
    use crate::runtime::process_tree::BirthToken;
    use crate::runtime::process_usage::{StartStamp, UsageReading};

    fn life(pid: u32) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(u64::from(pid) * 7),
        }
    }

    fn image(program: &str) -> ProcessImage {
        ProcessImage {
            program: program.to_owned(),
            argv: vec![CommandArg::new(program)],
            truncated: false,
        }
    }

    fn cpu(user: u64, sys: u64) -> CpuTotals {
        CpuTotals {
            user_us: CpuMicros::new(user).expect("exact"),
            sys_us: CpuMicros::new(sys).expect("exact"),
        }
    }

    fn coverage(permille: u16) -> LeafCoverage {
        LeafCoverage::new(permille).expect("a share")
    }

    /// `process`'s own counters read at `at`: `user` and `sys` microseconds.
    fn read(census: &mut Census, process: ProcessIdentity, at: Instant, user: u64, sys: u64) {
        census
            .usage()
            .apply(UsageObservation::Read {
                process,
                reading: UsageReading {
                    at,
                    started: StartStamp(u64::from(process.pid)),
                    cpu_user: CpuMicros::new(user).expect("exact"),
                    cpu_sys: CpuMicros::new(sys).expect("exact"),
                    resident: ResidentBytes::ZERO,
                    io: ProcessStorageIo::Unavailable {
                        reason: ProcessIoUnavailable::NotAccounted,
                    },
                    exited: false,
                },
            })
            .expect("a reading");
    }

    /// One boundary at which `members` were found with `(user, sys)` own CPU each.
    fn boundary(census: &mut Census, at: Instant, members: &[(u32, &str, u64, u64)]) {
        for &(pid, program, user, sys) in members {
            census.found(life(pid), Some(image(program)));
            read(census, life(pid), at, user, sys);
        }
        census.close().expect("the boundary");
    }

    /// The leaf is named exactly from the configured share on: below it the observation is
    /// said to fall short, at and above it the winner is named, whatever the tree's gap.
    #[test]
    fn the_leaf_is_named_only_from_the_required_share_of_the_accounted_cpu() {
        let at = Instant::now();
        let mut census = Census::led_by(100);
        boundary(
            &mut census,
            at,
            &[(100, "/bin/sh", 50, 50), (101, "cc", 799, 0)],
        );
        let total = cpu(800, 200);
        for (observed_extra, permille, attributed) in
            [(0, 899, false), (1, 900, true), (51, 950, true)]
        {
            let mut census = Census::led_by(100);
            boundary(
                &mut census,
                at,
                &[
                    (100, "/bin/sh", 50, 50),
                    (101, "cc", 799 + observed_extra, 0),
                ],
            );
            let answer = census.attribute(total, coverage(900)).expect("exact");
            match answer.leaf {
                JobLeafAttribution::Attributed {
                    leaf,
                    observed_cpu_permille,
                } => {
                    assert!(attributed, "{permille}‰ is below 900‰ yet a leaf was named");
                    assert_eq!(observed_cpu_permille, permille);
                    assert_eq!(leaf.pid, 101);
                    assert_eq!(leaf.program, "cc");
                }
                JobLeafAttribution::Unattributed {
                    observed_cpu_permille,
                    required_permille,
                } => {
                    assert!(
                        !attributed,
                        "{permille}‰ reaches 900‰ yet no leaf was named"
                    );
                    assert_eq!((observed_cpu_permille, required_permille), (permille, 900));
                }
                JobLeafAttribution::NoAccountedCpu => panic!("CPU was accounted"),
            }
            assert!(
                matches!(answer.coverage, ProcessCoverage::Gap { .. }),
                "a polled census is never complete"
            );
        }
        let answer = census.attribute(total, coverage(900)).expect("exact");
        assert_eq!(answer.cpu_us.get(), 1_000 - 899, "the unobserved remainder");
    }

    /// No accounted CPU names no leaf and says why, rather than a share of nothing.
    #[test]
    fn a_job_with_no_accounted_cpu_says_so() {
        let mut census = Census::led_by(100);
        boundary(&mut census, Instant::now(), &[(100, "/bin/sh", 0, 0)]);
        let answer = census.attribute(cpu(0, 0), coverage(900)).expect("exact");
        assert_eq!(answer.leaf, JobLeafAttribution::NoAccountedCpu);
        assert_eq!(answer.cpu_us.get(), 0);
    }

    /// CPU observed beyond the source's total covers the whole and no more; the excess stays
    /// in the signed reconciliation, never clamped away.
    #[test]
    fn observation_beyond_the_total_covers_the_whole_and_keeps_its_signed_excess() {
        let mut census = Census::led_by(100);
        boundary(
            &mut census,
            Instant::now(),
            &[(100, "/bin/sh", 10, 0), (101, "yes", 5_000, 0)],
        );
        let answer = census.attribute(cpu(100, 0), coverage(900)).expect("exact");
        assert_eq!(
            answer.leaf,
            JobLeafAttribution::Attributed {
                leaf: JobProcessLeaf {
                    pid: 101,
                    program: "yes".into(),
                    argv: vec![CommandArg::new("yes")],
                    truncated: false,
                },
                observed_cpu_permille: 1_000,
            }
        );
        assert_eq!(answer.cpu_us.get(), 100 - 5_010);
    }

    /// Equal CPU goes to the earlier birth identity, then the lower pid, whatever order the
    /// census found the lives in; an exited life keeps its last counters and can still win.
    #[test]
    fn ties_go_to_the_earlier_birth_then_the_lower_pid_whatever_the_discovery_order() {
        let at = Instant::now();
        let mut census = Census::led_by(100);
        // Found first, born last: pid 300's birth token is 2100.
        boundary(
            &mut census,
            at,
            &[(100, "/bin/sh", 0, 0), (300, "late", 400, 0)],
        );
        // Found later, born earlier (1400), and it has exited by the next boundary.
        boundary(
            &mut census,
            at + Duration::from_secs(1),
            &[(100, "/bin/sh", 0, 0), (200, "early", 400, 0)],
        );
        boundary(
            &mut census,
            at + Duration::from_secs(2),
            &[(100, "/bin/sh", 0, 0)],
        );
        let answer = census.attribute(cpu(800, 0), coverage(900)).expect("exact");
        let JobLeafAttribution::Attributed { leaf, .. } = answer.leaf else {
            panic!("covered: {:?}", answer.leaf);
        };
        assert_eq!((leaf.pid, leaf.program.as_str()), (200, "early"));

        // Equal CPU and equal birth: the lower pid.
        let mut census = Census::led_by(100);
        for (pid, program) in [(201, "higher"), (150, "lower")] {
            let process = ProcessIdentity {
                pid,
                birth: BirthToken(1_400),
            };
            census.found(process, Some(image(program)));
            read(&mut census, process, at, 500, 0);
        }
        census.close().expect("the boundary");
        let answer = census
            .attribute(cpu(1_000, 0), coverage(900))
            .expect("exact");
        let JobLeafAttribution::Attributed { leaf, .. } = answer.leaf else {
            panic!("covered: {:?}", answer.leaf);
        };
        assert_eq!((leaf.pid, leaf.program.as_str()), (150, "lower"));
    }

    /// The coverage names the first observation the census lacks: a member born unseen, then
    /// a member gone before a read after its exit; with none, that the leader may have forked
    /// between boundaries. Never complete.
    #[test]
    fn the_coverage_names_the_first_missed_observation_and_is_never_complete() {
        let at = Instant::now();
        let mut lone = Census::led_by(100);
        boundary(&mut lone, at, &[(100, "/bin/sh", 5, 5)]);
        lone.lead(200);
        boundary(
            &mut lone,
            at + Duration::from_secs(1),
            &[(200, "/bin/cc", 5, 5)],
        );
        let answer = lone.attribute(cpu(20, 0), coverage(900)).expect("exact");
        assert_eq!(
            answer.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnreadFinalUsage { pid: 100 }
            },
            "the activation left without a read after its exit"
        );

        let mut alone = Census::led_by(100);
        boundary(&mut alone, at, &[(100, "/bin/sh", 5, 5)]);
        assert_eq!(
            alone
                .attribute(cpu(10, 0), coverage(900))
                .expect("exact")
                .coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UncountedFork { pid: 100 }
            }
        );

        let mut forked = Census::led_by(100);
        boundary(
            &mut forked,
            at,
            &[(100, "/bin/sh", 5, 5), (150, "cc", 5, 5)],
        );
        boundary(
            &mut forked,
            at + Duration::from_secs(1),
            &[(100, "/bin/sh", 5, 5)],
        );
        assert_eq!(
            forked
                .attribute(cpu(20, 0), coverage(900))
                .expect("exact")
                .coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnobservedBirth { pid: 150 }
            },
            "the first gap stands"
        );
    }

    /// A member whose image was never read is not an identified process: its CPU covers
    /// nothing and it is never named, and the census says what it lacks.
    #[test]
    fn a_member_without_an_image_is_never_named() {
        let at = Instant::now();
        let mut census = Census::led_by(100);
        census.found(life(100), Some(image("/bin/sh")));
        read(&mut census, life(100), at, 10, 0);
        census.found(life(100), None);
        census.found(life(101), None);
        read(&mut census, life(101), at, 990, 0);
        census.close().expect("the boundary");
        let answer = census
            .attribute(cpu(1_000, 0), coverage(900))
            .expect("exact");
        assert_eq!(
            answer.leaf,
            JobLeafAttribution::Unattributed {
                observed_cpu_permille: 10,
                required_permille: 900,
            }
        );
        assert_eq!(
            answer.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnobservedBirth { pid: 101 }
            }
        );
    }

    #[test]
    fn a_coverage_threshold_is_a_share_of_the_whole() {
        assert_eq!(LeafCoverage::new(0), None);
        assert_eq!(LeafCoverage::new(1_001), None);
        assert_eq!(
            LeafCoverage::new(1_000).map(LeafCoverage::permille),
            Some(1_000)
        );
    }

    /// A running member whose image the kernel refused -- after an earlier read named one, as
    /// when it execs a set-id image -- is unidentified until a read names its image again: never
    /// named the leaf under its old image. The gap stays once a later read succeeds.
    #[test]
    fn an_image_the_kernel_refused_unnames_the_life_and_stays_a_gap() {
        let at = Instant::now();
        let mut census = Census::led_by(100);
        boundary(&mut census, at, &[(100, "/bin/sh", 10, 0)]);
        census.unread_image(life(100));
        read(&mut census, life(100), at + Duration::from_secs(1), 15, 0);
        census.close().expect("the boundary");
        let refused = census.attribute(cpu(15, 0), coverage(900)).expect("exact");
        assert_eq!(
            refused.leaf,
            JobLeafAttribution::Unattributed {
                observed_cpu_permille: 0,
                required_permille: 900,
            },
            "its old image no longer names it"
        );
        boundary(
            &mut census,
            at + Duration::from_secs(2),
            &[(100, "/usr/bin/su", 20, 0)],
        );
        let answer = census.attribute(cpu(20, 0), coverage(900)).expect("exact");
        let JobLeafAttribution::Attributed { leaf, .. } = &answer.leaf else {
            panic!("named again: {:?}", answer.leaf);
        };
        assert_eq!(leaf.program, "/usr/bin/su");
        assert_eq!(
            answer.coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnreadImage { pid: 100 }
            }
        );
    }

    fn own(user: u64, sys: u64) -> ProcessUsage {
        ProcessUsage {
            cpu_user_us: CpuMicros::new(user).expect("exact"),
            cpu_sys_us: CpuMicros::new(sys).expect("exact"),
            busy: false,
            rss_bytes: ResidentBytes::ZERO,
            rss_peak_bytes: ResidentBytes::ZERO,
            io: ProcessStorageIo::Unavailable {
                reason: ProcessIoUnavailable::NotAccounted,
            },
        }
    }

    /// The published summary uses the same threshold as the census and returns the
    /// coverage it was given. It does not invent a gap or a winner it was not shown.
    #[test]
    fn a_published_summary_keeps_the_threshold_and_the_given_coverage() {
        let sh = image("/bin/sh");
        let cc = image("cc");
        let gap = ProcessCoverage::Gap {
            reason: ProcessCoverageGap::UnobservedBirth { pid: 7 },
        };
        let total = cpu(800, 200);
        for (extra, permille, attributed) in
            [(0u64, 899u16, false), (1, 900, true), (51, 950, true)]
        {
            let lives = [
                ObservedLife {
                    identity: life(100),
                    image: Some(&sh),
                    usage: Some(own(50, 50)),
                },
                ObservedLife {
                    identity: life(101),
                    image: Some(&cc),
                    usage: Some(own(799 + extra, 0)),
                },
            ];
            let answer = AttributionSummary::from_observed(lives)
                .expect("exact")
                .attribute(total, coverage(900), gap)
                .expect("exact");
            assert_eq!(answer.coverage, gap, "the given gap is not replaced");
            assert_eq!(
                answer.cpu_us.get(),
                1_000 - 899 - i64::try_from(extra).expect("extra")
            );
            match answer.leaf {
                JobLeafAttribution::Attributed {
                    leaf,
                    observed_cpu_permille,
                } => {
                    assert!(attributed, "{permille}‰ is below 900‰ yet a leaf was named");
                    assert_eq!(
                        (leaf.pid, leaf.program.as_str(), observed_cpu_permille),
                        (101, "cc", permille)
                    );
                }
                JobLeafAttribution::Unattributed {
                    observed_cpu_permille,
                    required_permille,
                } => {
                    assert!(
                        !attributed,
                        "{permille}‰ reaches 900‰ yet no leaf was named"
                    );
                    assert_eq!((observed_cpu_permille, required_permille), (permille, 900));
                }
                JobLeafAttribution::NoAccountedCpu => panic!("CPU was accounted"),
            }
        }
        let unread = [ObservedLife {
            identity: life(100),
            image: None,
            usage: Some(own(1_000, 0)),
        }];
        let hidden = AttributionSummary::from_observed(unread)
            .expect("exact")
            .attribute(total, coverage(900), gap)
            .expect("exact");
        assert_eq!(
            hidden.leaf,
            JobLeafAttribution::Unattributed {
                observed_cpu_permille: 0,
                required_permille: 900,
            }
        );
        assert_eq!(
            hidden.cpu_us.get(),
            1_000,
            "unread usage is not observed CPU"
        );
        let none = AttributionSummary::from_observed([ObservedLife {
            identity: life(100),
            image: Some(&sh),
            usage: Some(own(0, 0)),
        }])
        .expect("exact");
        assert_eq!(
            none.attribute(cpu(0, 0), coverage(900), gap)
                .expect("exact")
                .leaf,
            JobLeafAttribution::NoAccountedCpu
        );
    }
}
