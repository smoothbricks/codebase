//! A job's process tree as the kernel reports it (07_api.md, "Process-tree observations").
//!
//! Each member is registered with the platform's event source, and the observer folds what the
//! source reports of it: that it forked, that it exec'd, that it exited, with its wait status
//! where the kernel gives one. The source is the platform's (`macos.rs`: kqueue `EVFILT_PROC`,
//! which reports all three as events; `linux.rs`: one pidfd per life in an epoll set, which
//! reports exits as events, and a survey of `/proc` for forks and execs at the observer's
//! sampling and custody boundaries), selected here and nowhere else. No source names or counts a
//! fork's children, so each fork is answered by reading the member's children: a child forked
//! and reaped before that read is never seen. A member that forks therefore makes the tree's
//! coverage a gap ([`ProcessCoverageGap::UncountedFork`]), and under a source that learns of
//! forks only by looking, that gap stands from the root's first moment. The job's CPU totals
//! come from the job's accounting source (within that source's named limits), not from this
//! tree.
//!
//! Identity is the kernel's id of one life (macOS `p_uniqueid`; Linux the pid's start time),
//! which no other process holding that pid is given. Every kernel read here names a pid alone,
//! so each is fenced by that id:
//!
//! - A registration names one life, and every report names its life, never its pid alone: a
//!   late report of an older life lands on that life, not on the one now holding its pid. The
//!   life is read before the registration and again after it; only the same id both times
//!   proves the registration is that life's, and anything else is withdrawn unadopted.
//! - An image or a child list is read between two reads of its process's record. Read from a
//!   life that was no longer running by the second read, it is discarded as a stranger's, and
//!   the life's image is a gap ([`ProcessCoverageGap::UnreadImage`]).
//! - An image that could not be read is a gap, and a read that failed is also returned: an
//!   observation error is an operational value, the observer stays usable, and no errno is
//!   swallowed into a gap. The errno the platform's image read answers a reader it refuses may
//!   have other causes too, so a refusal is named by the kernel's credential rule: a reader
//!   whose effective uid is neither 0 nor the process's is refused
//!   ([`ObserveError::ImageNotPermitted`], keeping the errno). Every other failed read is
//!   [`ObserveError::Kernel`] with its call and errno, whether or not the life still ran; a
//!   child list in particular never fails because its parent exited. A failed read of an exec's
//!   or adoption's image, or of the children a fork left, leaves the fold as
//!   [`ObserveError::Unread`] around that error: the fold recorded the gap
//!   ([`ProcessCoverageGap::UnreadImage`], [`ProcessCoverageGap::UncountedFork`]), so the tree
//!   stays a readable answer and only those who follow the failures (the drain's caller, the
//!   event stream) are told.
//! - A kernel gives an exit's wait status only to an observer it permits (macOS: a child, or a
//!   life the observer may SIGKILL; Linux: any reaped life, and an unreaped one the observer's
//!   credentials may read). The exit of any other life is still joined and only its status is
//!   withheld ([`ProcessCoverageGap::UnreadExitStatus`]), never guessed and never a refused
//!   watch: a root command a member ran through sudo, for one.
//! - Custody-only admission and retirement fold fork/exit evidence without reading argv or
//!   final usage; an exec records UnreadImage instead. An exec reported with its known exit
//!   also skips that useless read. Normal observation retains every native image failure above.
//!   Identity, lineage, registration and signal reads remain strict in both purposes.
//! - A child is adopted only when its parent's id is the member's.
//! - The source hands over a batch of reports at once and keeps none of them. Every report of
//!   a batch, and everything each one says, is folded even after another failed, so a failed
//!   image read never drops the exit reported beside it; the batch's failures are returned
//!   together.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::time::{Duration, Instant};

use crate::api::dto::{JobId, UtcTimestamp};
use crate::api::process::{JobProcessEvent, ProcessCoverageGap};
use crate::error::CowshedError;
use crate::runtime::job_groups::{
    Birth, ProcessRecord, job_member_records, process_record, signal_life,
};
use crate::runtime::process_stream::{
    JobProcessFold, JobProcessFoldError, JobProcessObservation, PublishedTree,
};
use crate::runtime::process_tree::{
    BirthToken, ProcessIdentity, ProcessImage, ProcessObservation, ProcessTreeFold,
};
use crate::runtime::process_usage::{SampleError, Sampled, StartStamp};
use crate::runtime::supervisor::{process_termination_from_wait, utc_at_second, utc_now};
use procinfo::process_arguments;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

use platform::{Source, Watch};

/// Custody depends on lives and exit watches, not their current argv.
#[derive(Clone, Copy, Eq, PartialEq)]
enum ObservationPurpose {
    Metadata,
    Custody,
}

/// What the event source reported of one registered life at once. A source coalesces: one
/// report may say all of it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Report {
    process: ProcessIdentity,
    /// It forked an uncounted number of children.
    forked: bool,
    /// It runs another image than it did.
    exec: bool,
    exit: Option<ExitReport>,
}

/// How the event source reported a registered life's exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExitReport {
    /// With the life's wait status, as wide as the widest source reports it.
    Status(isize),
    /// The exit alone: the kernel withheld its status from this observer.
    Withheld,
}

/// Why observing a job's processes failed.
#[derive(Debug, thiserror::Error)]
pub enum ObserveError {
    #[error("{call} for process {pid} failed: {source}")]
    Kernel {
        call: &'static str,
        pid: u32,
        source: io::Error,
    },
    /// The kernel refuses this observer the image of a process running as another user;
    /// `source` is the call's own errno.
    #[error(
        "{} refuses process {pid}'s arguments: it runs as uid {uid}, this observer as uid {reader}: {source}",
        platform::IMAGE
    )]
    ImageNotPermitted {
        pid: u32,
        uid: u32,
        reader: u32,
        source: io::Error,
    },
    #[error("the kernel's process events contradict each other: {0}")]
    Contradiction(#[from] JobProcessFoldError),
    #[error(transparent)]
    Usage(#[from] SampleError),
    #[error(transparent)]
    Cowshed(#[from] CowshedError),
    /// A read that failed after the fold recorded `gap` for what it would have shown: an exec's
    /// image, or the children a fork left. The tree stays readable, gap included, and the failure
    /// is returned beside the gap with its call and errno.
    #[error("{cause}")]
    Unread {
        gap: ProcessCoverageGap,
        cause: Box<ObserveError>,
    },
    /// More than one event of one batch failed; every event of it was folded all the same.
    #[error("{first}; {} more in the same batch: {}", .rest.len(), Listed(.rest))]
    Several {
        first: Box<ObserveError>,
        rest: Vec<ObserveError>,
    },
}

impl ObserveError {
    /// Whether the fold holds everything this failure names as a coverage gap, so that its tree is
    /// still a true, incomplete answer: a failed read it recorded as an [`ObserveError::Unread`]
    /// gap. Any other failure may leave the tree without an observation that no gap records.
    pub fn leaves_tree_readable(&self) -> bool {
        match self {
            Self::Unread { .. } => true,
            Self::Several { first, rest } => {
                first.leaves_tree_readable() && rest.iter().all(Self::leaves_tree_readable)
            }
            Self::Kernel { .. }
            | Self::ImageNotPermitted { .. }
            | Self::Contradiction(_)
            | Self::Usage(_)
            | Self::Cowshed(_) => false,
        }
    }
}

struct Listed<'a>(&'a [ObserveError]);

impl fmt::Display for Listed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}

/// The failures of one batch, all of whose events are folded regardless.
#[derive(Debug, Default)]
struct Failures(Vec<ObserveError>);

impl Failures {
    fn note(&mut self, result: Result<(), ObserveError>) {
        match result {
            Ok(()) => {}
            Err(ObserveError::Several { first, rest }) => {
                self.0.push(*first);
                self.0.extend(rest);
            }
            Err(error) => self.0.push(error),
        }
    }

    fn into_result(self) -> Result<(), ObserveError> {
        let mut failures = self.0.into_iter();
        let Some(first) = failures.next() else {
            return Ok(());
        };
        let rest: Vec<_> = failures.collect();
        if rest.is_empty() {
            return Err(first);
        }
        Err(ObserveError::Several {
            first: Box::new(first),
            rest,
        })
    }
}

/// When an image is read: after the kernel reported an exec, or as a child is adopted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Seen {
    Exec,
    Adopted,
}

/// The kernel's per-process reads, each naming a pid alone. The observer fences every one.
pub trait ProcessReads {
    /// The process's public metadata fenced to one immutable life; None once it has exited.
    fn record(&self, pid: u32) -> io::Result<Option<ProcessRecord>>;
    /// The executable and byte-exact argv of the image `pid` runs now.
    fn image(&self, pid: u32) -> io::Result<ProcessImage>;
    /// The pids whose parent is `pid` now.
    fn children(&self, pid: u32) -> io::Result<Vec<u32>>;
    /// The effective uid the reads are made as.
    fn reader_uid(&self) -> u32;
    /// A reading fenced to this retained life; missing proof never becomes invented counters.
    /// `held` is the descriptor the observer's registration holds of the life where the event
    /// source keeps one (a Linux pidfd), which then is the fence.
    fn usage(
        &self,
        process: ProcessIdentity,
        held: Option<BorrowedFd<'_>>,
        start: Option<StartStamp>,
        at: Instant,
    ) -> Result<Sampled, SampleError>;
    /// The running members of the job's group `leader` leads, read while its parent holds the
    /// leader unreaped ([`crate::runtime::job_groups::job_members`]'s proof): never a guess.
    fn group(&self, leader: &Birth) -> io::Result<Vec<ProcessRecord>>;
    /// Send `signal` to `process` while its pid still holds that life; `false` once it no longer
    /// runs. A later process given the pid is never reached.
    fn signal(&self, process: ProcessIdentity, signal: libc::c_int) -> io::Result<bool>;
}

/// The running kernel, whichever the platform's is: the name is the first one it read.
#[derive(Clone, Copy, Debug, Default)]
pub struct Darwin;

impl ProcessReads for Darwin {
    fn record(&self, pid: u32) -> io::Result<Option<ProcessRecord>> {
        process_record(libc::pid_t::try_from(pid).map_err(io::Error::other)?)
    }

    fn image(&self, pid: u32) -> io::Result<ProcessImage> {
        let arguments = process_arguments(libc::pid_t::try_from(pid).map_err(io::Error::other)?)?;
        Ok(ProcessImage::observed(
            String::from_utf8_lossy(&arguments.executable).into_owned(),
            arguments.argv,
        ))
    }

    fn children(&self, pid: u32) -> io::Result<Vec<u32>> {
        platform::children(pid)
    }

    fn reader_uid(&self) -> u32 {
        // SAFETY: geteuid takes nothing and cannot fail.
        unsafe { libc::geteuid() }
    }

    fn usage(
        &self,
        process: ProcessIdentity,
        held: Option<BorrowedFd<'_>>,
        start: Option<StartStamp>,
        at: Instant,
    ) -> Result<Sampled, SampleError> {
        crate::runtime::process_usage::sample(process, held, start, at)
    }

    fn group(&self, leader: &Birth) -> io::Result<Vec<ProcessRecord>> {
        job_member_records(leader)
    }

    fn signal(&self, process: ProcessIdentity, signal: libc::c_int) -> io::Result<bool> {
        signal_life(process.pid, process.birth.0, signal)
    }
}

/// The process tree of one job, observed from before its root could fork or exec, through the
/// platform's event source: the name is the first source it had.
#[derive(Debug)]
pub struct KqueueObserver<P = Darwin> {
    source: Source,
    reads: P,
    fold: JobProcessFold,
    events: Vec<JobProcessEvent>,
    /// Every life with a registration and the watch the source holds of it: its reports are the
    /// only ones the observer folds.
    registered: HashMap<ProcessIdentity, Watch>,
    /// Each adoption the observer saw it miss, with the member it was missed under: a fork whose
    /// children could not be listed before its parent's life ended, or a listed child that could
    /// not be watched, each under the forking parent. Such a child may run on outside every
    /// registration; a retirement reports these as its `lost`. A loss of the event source is
    /// under no member: it can hide a birth anywhere in the job.
    missed: Vec<(Option<ProcessIdentity>, ProcessCoverageGap)>,
    /// The batch of reports being folded, kept between batches for its allocation.
    reports: Vec<Result<Report, ObserveError>>,
}

impl<P> AsRawFd for KqueueObserver<P> {
    /// Readable when an event waits for [`KqueueObserver::drain`].
    fn as_raw_fd(&self) -> RawFd {
        self.source.as_raw_fd()
    }
}

impl KqueueObserver {
    /// Allocate the event source before any workload can be forked.
    pub fn prepare() -> Result<Self, ObserveError> {
        Self::prepare_with(Darwin)
    }

    /// Watch the held root before it can fork or exec.
    pub fn watch(root: u32) -> Result<Self, ObserveError> {
        Self::watch_with(Darwin, root)
    }
}

impl<P: ProcessReads> KqueueObserver<P> {
    pub fn watch_with(reads: P, root: u32) -> Result<Self, ObserveError> {
        let mut observer = Self::prepare_with(reads)?;
        observer.watch_root(root)?;
        Ok(observer)
    }

    pub(crate) fn prepare_with(reads: P) -> Result<Self, ObserveError> {
        Ok(Self {
            source: Source::open()?,
            reads,
            fold: JobProcessFold::new(Instant::now()),
            events: Vec::new(),
            registered: HashMap::new(),
            missed: Vec::new(),
            reports: Vec::new(),
        })
    }

    /// Register the held root before acknowledging its pre-exec fence.
    pub fn watch_root(&mut self, root: u32) -> Result<(), ObserveError> {
        let not_running = || {
            kernel(
                platform::RECORD,
                root,
                io::Error::new(io::ErrorKind::NotFound, "the held root is not running"),
            )
        };
        let before = self.record(root)?.ok_or_else(not_running)?;
        let record = self.attach(before)?.ok_or_else(not_running)?;
        let process = identity(&record);
        let image = self.image(process)?.ok_or_else(not_running)?;
        self.observe(ProcessObservation::Root {
            process,
            ppid: record.ppid,
            at: utc_at_second(record.started_seconds)?,
            image,
        })?;
        if let Some(gap) = platform::standing_gap(process.pid) {
            self.observe(ProcessObservation::Lost(gap))?;
        }
        self.sample(Instant::now())?;
        Ok(())
    }

    pub fn fold(&self) -> &ProcessTreeFold {
        self.fold.lives()
    }

    pub(crate) fn fold_mut(&mut self) -> &mut ProcessTreeFold {
        self.fold.lives_mut()
    }

    pub(crate) fn attribution_summary(
        &mut self,
    ) -> Result<super::job_attribution::AttributionSummary, crate::api::resources::ResourceUnitError>
    {
        self.fold.attribution_summary()
    }

    /// Job `job_id`'s tree as published now, as last observed and read.
    pub fn published(&mut self, job_id: JobId, at: UtcTimestamp) -> PublishedTree {
        self.fold.published(job_id, at)
    }

    pub fn drain_events(&mut self) -> std::vec::Drain<'_, JobProcessEvent> {
        self.events.drain(..)
    }

    /// Drain the same events with the identities retained at their stable tree indices.
    /// Trace parentage never rejoins these events to a currently running PID.
    pub(crate) fn drain_retained_events(
        &mut self,
    ) -> impl Iterator<
        Item = (
            JobProcessEvent,
            Option<ProcessIdentity>,
            Option<ProcessIdentity>,
        ),
    > {
        let lives = self.fold.lives();
        self.events.drain(..).map(move |event| {
            let index = match &event {
                JobProcessEvent::Born { index, .. }
                | JobProcessEvent::Exec { index, .. }
                | JobProcessEvent::Heartbeat { index, .. }
                | JobProcessEvent::Exited { index, .. } => Some(*index),
                JobProcessEvent::Changed(delta) => Some(delta.index()),
                JobProcessEvent::Gap { .. } => None,
            };
            let node = index
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| lives.node_at(index));
            (
                event,
                node.map(|node| node.identity),
                node.and_then(|node| node.parent),
            )
        })
    }

    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    /// A duplicate of the source's wake descriptor wakes the sole writer; it is not an
    /// observation reader.
    pub fn wake_handle(&self) -> io::Result<OwnedFd> {
        self.source.wake_handle()
    }

    pub fn gap(&mut self, reason: ProcessCoverageGap) -> Result<(), ObserveError> {
        // An explicit event-source loss can hide births as well as exits. Keep it in the
        // containment receipt even if an earlier metadata gap already set tree coverage.
        if reason == ProcessCoverageGap::EventsLost {
            self.missed.push((None, reason));
        }
        self.observe(ProcessObservation::Lost(reason))
    }

    /// The job's clock started at `origin`, the instant its first owned process spawned.
    pub fn clock(&mut self, origin: Instant) -> Result<(), ObserveError> {
        self.fold.clock(origin)?;
        Ok(())
    }

    pub fn watching(&self) -> bool {
        !self.registered.is_empty()
    }

    fn observe(&mut self, observation: ProcessObservation) -> Result<(), ObserveError> {
        self.fold
            .apply(JobProcessObservation::Tree(observation), &mut self.events)?;
        Ok(())
    }

    /// One supervisor-owned sampling boundary, independent of subscribers and tree readers. It
    /// is also where a source that reports forks and execs only when asked is asked.
    pub fn sample(&mut self, at: Instant) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        self.survey_into(&mut failures, ObservationPurpose::Metadata);
        for (&process, watch) in &self.registered {
            if self.fold.lives().live(process.pid) != Some(process) {
                continue;
            }
            match self
                .reads
                .usage(process, watch.held(), self.fold.usage_start(process), at)
            {
                Ok(Sampled::Read(reading)) => failures.note(
                    self.fold
                        .apply(
                            JobProcessObservation::Usage { process, reading },
                            &mut self.events,
                        )
                        .map_err(ObserveError::from),
                ),
                Ok(Sampled::Gone | Sampled::Unproven) => {}
                Err(error) => failures.note(Err(error.into())),
            }
        }
        failures.note(
            self.fold
                .apply(JobProcessObservation::Tick { at }, &mut self.events)
                .map_err(ObserveError::from),
        );
        failures.into_result()
    }

    /// Fold every event already queued, without waiting. An `Err` names what could not be
    /// observed; every event was folded all the same, and the observer stays usable.
    pub fn drain(&mut self) -> Result<(), ObserveError> {
        self.drain_for(ObservationPurpose::Metadata)
    }

    /// Fold custody evidence without asking for an image that cannot improve ownership.
    pub(crate) fn drain_custody(&mut self) -> Result<(), ObserveError> {
        self.drain_for(ObservationPurpose::Custody)
    }

    /// Ask the event source now for what it reports only when asked, and fold it: for a caller
    /// that knows its held root has been released into its exec, whose image no event of such a
    /// source would report before the next sampling boundary.
    pub(crate) fn survey(&mut self) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        self.survey_into(&mut failures, ObservationPurpose::Metadata);
        failures.into_result()
    }

    fn drain_for(&mut self, purpose: ObservationPurpose) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        self.drain_into(&mut failures, purpose);
        if purpose == ObservationPurpose::Custody {
            self.survey_into(&mut failures, purpose);
        }
        failures.into_result()
    }

    /// Wait for at least one event, then fold every event queued. An `Err` is what
    /// [`KqueueObserver::drain`] returns.
    pub fn wait(&mut self) -> Result<(), ObserveError> {
        self.wait_within(None)
    }

    /// [`KqueueObserver::wait`], returning with nothing folded once `within` passes with no
    /// event. The kernel's own wait bounds it; no timer runs beside it.
    pub fn wait_within(&mut self, within: Option<Duration>) -> Result<(), ObserveError> {
        self.wait_for(within, ObservationPurpose::Metadata)
    }

    pub(crate) fn wait_custody(&mut self, within: Duration) -> Result<(), ObserveError> {
        self.wait_for(Some(within), ObservationPurpose::Custody)
    }

    fn wait_for(
        &mut self,
        within: Option<Duration>,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        if self
            .collect(within, &mut failures, purpose)
            .is_some_and(|ready| ready > 0)
        {
            self.drain_into(&mut failures, purpose);
        }
        if purpose == ObservationPurpose::Custody {
            self.survey_into(&mut failures, purpose);
        }
        failures.into_result()
    }

    /// Every life the job owns now (11_shell.md, "Complete-group termination"): each running
    /// member of the group `leader` leads, read under the held leader's proof, and each life the
    /// fold holds running, inside the group or not. A group member the fold does not hold is
    /// adopted first -- through its parent's unique id when the fold holds that parent running,
    /// otherwise as a root whose birth went unobserved -- so its exit is joined like any other.
    /// A membership that cannot be read, a member that cannot be watched, or a running life with
    /// no exit registration is an `Err`: ownership is never guessed and no exit is presumed.
    pub(crate) fn owned(&mut self, leader: &Birth) -> Result<Vec<ProcessIdentity>, ObserveError> {
        let members = self
            .reads
            .group(leader)
            .map_err(|source| kernel(platform::GROUP, leader.pid(), source))?;
        for member in members {
            if !self.fold.lives().holds(identity(&member)) {
                self.adopt_member(member)?;
            }
        }
        self.watched(self.fold.lives().running().collect())
    }

    /// Subtree membership comes only from retained birth ancestry, never from group membership.
    pub(crate) fn subtree(
        &self,
        root: ProcessIdentity,
    ) -> Result<Vec<ProcessIdentity>, ObserveError> {
        self.watched(
            self.fold
                .lives()
                .running()
                .filter(|&process| self.fold.lives().in_subtree(root, process))
                .collect(),
        )
    }

    fn watched(&self, owned: Vec<ProcessIdentity>) -> Result<Vec<ProcessIdentity>, ObserveError> {
        if let Some(unwatched) = owned
            .iter()
            .find(|life| !self.registered.contains_key(life))
        {
            return Err(kernel(
                platform::WATCH,
                unwatched.pid,
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "a running life of the job holds no exit registration",
                ),
            ));
        }
        Ok(owned)
    }

    /// Authenticate the exact life retained and watched here, not the current holder of a pid.
    pub(crate) fn retained_live(&self, process: ProcessIdentity) -> Result<bool, ObserveError> {
        if self.fold.lives().live(process.pid) != Some(process)
            || !self.registered.contains_key(&process)
        {
            return Ok(false);
        }
        Ok(self.current(process)?.is_some())
    }

    /// Adopt a member of the job's group the fold does not hold. The group proof makes it the
    /// job's; the registration is that life's only when its unique id still holds the pid after
    /// it ([`KqueueObserver::attach`]).
    fn adopt_member(&mut self, member: ProcessRecord) -> Result<(), ObserveError> {
        if let Some(parent) = self
            .fold
            .lives()
            .live(member.ppid)
            .filter(|parent| parent.birth.0 == member.parent_unique_id)
        {
            return self.adopt(parent, member.pid, ObservationPurpose::Custody);
        }
        let Some(record) = self.attach(member)? else {
            // It exited: the group no longer holds it.
            return Ok(());
        };
        let process = identity(&record);
        let image = match self.image(process) {
            Ok(Some(image)) => image,
            ended_or_failed => {
                // Its life ended around the read, or the read failed: withdraw the registration
                // unadopted. A life that still runs is read again by the next membership read.
                if let Some(watch) = self.registered.get(&process) {
                    self.source.withdraw(process, watch)?;
                }
                self.registered.remove(&process);
                return ended_or_failed.map(|_| ());
            }
        };
        self.observe(ProcessObservation::Lost(
            ProcessCoverageGap::UnobservedBirth { pid: process.pid },
        ))?;
        self.observe(ProcessObservation::Root {
            process,
            ppid: record.ppid,
            at: utc_at_second(record.started_seconds)?,
            image,
        })?;
        self.adopt_children(process, ObservationPurpose::Custody)
    }

    /// Adopt every child a retained parent has now. Retirement calls this after posting STOP;
    /// admission may call it to cover a queued fork notification. Neither assumes quiescence.
    /// The outer Err is the child list's failure; the inner names failed, missed adoptions.
    pub(crate) fn descendants(
        &mut self,
        parent: ProcessIdentity,
    ) -> Result<Result<(), ObserveError>, ObserveError> {
        Ok(match self.children(parent)? {
            Some(children) => self.adopt_each(parent, children, ObservationPurpose::Custody),
            // Its life ended; its exit is joined from its own registration.
            None => Ok(()),
        })
    }

    /// Send `signal` to `process` while the fold holds it running and its pid still holds that
    /// life; `false` when it no longer runs. A life whose exit was folded is never signalled.
    pub(crate) fn signal(
        &self,
        process: ProcessIdentity,
        signal: libc::c_int,
    ) -> Result<bool, ObserveError> {
        if self.fold.lives().live(process.pid) != Some(process) {
            return Ok(false);
        }
        self.reads
            .signal(process, signal)
            .map_err(|source| kernel(platform::SIGNAL, process.pid, source))
    }

    /// The adoptions this observer saw it miss, in the order it missed them: every one of the
    /// job (`None`), or those a retirement of the subtree under `root` answers for, which are the
    /// ones missed under a life of that subtree and every loss of the event source.
    pub(crate) fn missed(&self, under: Option<ProcessIdentity>) -> Vec<ProcessCoverageGap> {
        let lives = self.fold.lives();
        self.missed
            .iter()
            .filter(|&&(member, _)| match (under, member) {
                (Some(root), Some(member)) => lives.in_subtree(root, member),
                (None, _) | (_, None) => true,
            })
            .map(|&(_, gap)| gap)
            .collect()
    }

    fn drain_into(&mut self, failures: &mut Failures, purpose: ObservationPurpose) {
        while self
            .collect(Some(Duration::ZERO), failures, purpose)
            .is_some_and(|ready| ready > 0)
        {}
    }

    /// Fold one batch of the source's reports, waiting up to `within` for the first (`None`:
    /// until one comes): how many events the kernel handed over, or `None` when the source
    /// itself failed.
    fn collect(
        &mut self,
        within: Option<Duration>,
        failures: &mut Failures,
        purpose: ObservationPurpose,
    ) -> Option<usize> {
        let mut reports = std::mem::take(&mut self.reports);
        let ready = match self
            .source
            .collect(within, &mut self.registered, &mut reports)
        {
            Ok(ready) => {
                failures.note(self.fold_batch(&mut reports, purpose));
                Some(ready)
            }
            Err(error) => {
                reports.clear();
                failures.note(Err(error));
                None
            }
        };
        self.reports = reports;
        ready
    }

    /// Ask the source for what it reports only when asked, and fold it: nothing under a source
    /// that reports every fork and exec as an event.
    fn survey_into(&mut self, failures: &mut Failures, purpose: ObservationPurpose) {
        let mut reports = std::mem::take(&mut self.reports);
        self.source
            .survey(purpose, &mut self.registered, &mut reports);
        failures.note(self.fold_batch(&mut reports, purpose));
        self.reports = reports;
    }

    /// Fold every report of one batch the source handed over, each one even after another
    /// failed: the source keeps none of them. The batch's failures are returned together.
    fn fold_batch(
        &mut self,
        reports: &mut Vec<Result<Report, ObserveError>>,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        for report in reports.drain(..) {
            failures.note(report.and_then(|report| self.fold_report(report, purpose)));
        }
        failures.into_result()
    }

    fn fold_report(
        &mut self,
        report: Report,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let Report {
            process,
            forked,
            exec,
            exit,
        } = report;
        if !self.registered.contains_key(&process) {
            // A registration withdrawn unadopted: its queued reports were never this tree's.
            return Ok(());
        }
        // One report coalesces what its life did; a fork's children are read while the parent
        // still runs, before its exec or exit is folded. Each part is folded even when an
        // earlier one failed.
        let mut failures = Failures::default();
        if forked {
            failures.note(self.forked(process, purpose));
        }
        if exec {
            let purpose = if exit.is_some() {
                ObservationPurpose::Custody
            } else {
                purpose
            };
            failures.note(self.fold_image(process, Seen::Exec, purpose));
        }
        if let Some(exit) = exit {
            failures.note(self.exited(process, exit, purpose));
        }
        failures.into_result()
    }

    /// Fold the image `process` runs now. After a reported exec it is that exec's image; read
    /// at adoption, it is an exec only when it differs from the parent's image the child
    /// started with. An image that could not be read is the `UnreadImage` gap, and a read
    /// that failed is returned beside it with its call and errno.
    fn fold_image(
        &mut self,
        process: ProcessIdentity,
        seen: Seen,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let unread = ProcessObservation::ImageUnread { process };
        if purpose == ObservationPurpose::Custody {
            return self.observe(unread);
        }
        let mut failures = Failures::default();
        let observation = match self.image(process) {
            Ok(Some(image))
                if seen == Seen::Adopted && Some(&image) == self.fold.lives().image(process) =>
            {
                None
            }
            Ok(Some(image)) => Some(ProcessObservation::Exec { process, image }),
            Ok(None) => Some(unread),
            Err(error) => {
                failures.note(Err(ObserveError::Unread {
                    gap: ProcessCoverageGap::UnreadImage { pid: process.pid },
                    cause: Box::new(error),
                }));
                Some(unread)
            }
        };
        if let Some(observation) = observation {
            failures.note(self.observe(observation));
        }
        failures.into_result()
    }

    /// Fold `process`'s exit. Its status is the wait status the source reported with it
    /// ([`ExitReport::Status`]); under [`ExitReport::Withheld`] the kernel gave none and the
    /// status stays unread.
    fn exited(
        &mut self,
        process: ProcessIdentity,
        exit: ExitReport,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        if purpose == ObservationPurpose::Metadata {
            let held = self.registered.get(&process).and_then(Watch::held);
            match self.reads.usage(
                process,
                held,
                self.fold.usage_start(process),
                Instant::now(),
            ) {
                Ok(Sampled::Read(reading)) => failures.note(
                    self.fold
                        .apply(
                            JobProcessObservation::Usage { process, reading },
                            &mut self.events,
                        )
                        .map_err(ObserveError::from),
                ),
                Ok(Sampled::Gone | Sampled::Unproven) => {}
                Err(error) => failures.note(Err(error.into())),
            }
        }
        // An exit ends the source's registration with it.
        self.registered.remove(&process);
        let status = match exit {
            ExitReport::Status(data) => {
                let status = i32::try_from(data).map_err(|_| {
                    kernel(
                        platform::EXIT_STATUS,
                        process.pid,
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("wait status {data} out of range"),
                        ),
                    )
                })?;
                Some(process_termination_from_wait(Ok(
                    std::process::ExitStatus::from_raw(status),
                ))?)
            }
            ExitReport::Withheld => None,
        };
        failures.note(self.observe(ProcessObservation::Exited {
            process,
            status,
            at: utc_now()?,
        }));
        failures.into_result()
    }

    /// `parent` forked an uncounted number of children: adopt every one it has now. Children
    /// that could not be listed -- the parent's life ended first, or the read failed -- were
    /// forked and may run on unwatched: a missed adoption.
    fn forked(
        &mut self,
        parent: ProcessIdentity,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        self.observe(ProcessObservation::Lost(
            ProcessCoverageGap::UncountedFork { pid: parent.pid },
        ))?;
        match self.children(parent) {
            Ok(Some(children)) => self.adopt_each(parent, children, purpose),
            unlisted => {
                self.missed.push((
                    Some(parent),
                    ProcessCoverageGap::UncountedFork { pid: parent.pid },
                ));
                // The gap is already recorded above: a failed list is returned beside it.
                unlisted.map(|_| ()).map_err(|cause| ObserveError::Unread {
                    gap: ProcessCoverageGap::UncountedFork { pid: parent.pid },
                    cause: Box::new(cause),
                })
            }
        }
    }

    /// Adopt every child `parent` has now that the tree does not hold, each one even after
    /// another failed.
    fn adopt_children(
        &mut self,
        parent: ProcessIdentity,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let Some(children) = self.children(parent)? else {
            // It exited; its children went to another parent. The fork gap covers them.
            return Ok(());
        };
        self.adopt_each(parent, children, purpose)
    }

    /// Adopt each listed child, each one even after another failed. A child whose adoption
    /// failed before the tree held it is a missed adoption.
    fn adopt_each(
        &mut self,
        parent: ProcessIdentity,
        children: Vec<u32>,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let mut failures = Failures::default();
        for child in children {
            let adopted = self.adopt(parent, child, purpose);
            if adopted.is_err() && self.fold.lives().live(child).is_none() {
                self.missed.push((
                    Some(parent),
                    ProcessCoverageGap::UnobservedBirth { pid: child },
                ));
            }
            failures.note(adopted);
        }
        failures.into_result()
    }

    fn adopt(
        &mut self,
        parent: ProcessIdentity,
        child: u32,
        purpose: ObservationPurpose,
    ) -> Result<(), ObserveError> {
        let Some(before) = self.record(child)? else {
            // Exited before it could be watched: the fork gap covers it.
            return Ok(());
        };
        let process = identity(&before);
        if self.fold.lives().holds(process) || before.parent_unique_id != parent.birth.0 {
            // Already watched, or a stranger given the pid of a child reaped meanwhile.
            return Ok(());
        }
        let Some(record) = self.attach(before)? else {
            return Ok(());
        };
        self.observe(ProcessObservation::Forked {
            process,
            parent,
            at: utc_at_second(record.started_seconds)?,
        })?;
        // It may have exec'd, and forked, before it was watched.
        let mut failures = Failures::default();
        failures.note(self.fold_image(process, Seen::Adopted, purpose));
        failures.note(self.adopt_children(process, purpose));
        failures.into_result()
    }

    /// Register the life `before` read for its events, then read its pid again. `Some` only
    /// when the same unique id still holds the pid: then the registration is that life's.
    fn attach(&mut self, before: ProcessRecord) -> Result<Option<ProcessRecord>, ObserveError> {
        let process = identity(&before);
        let Some(watch) = self.source.register(process)? else {
            return Ok(None);
        };
        match self.record(process.pid)? {
            Some(after) if after.unique_id == before.unique_id => {
                self.registered.insert(process, watch);
                Ok(Some(after))
            }
            // The pid named another life by the registration, or this one exited: whose
            // registration it is cannot be proven.
            Some(_) | None => {
                self.source.withdraw(process, &watch)?;
                Ok(None)
            }
        }
    }

    /// The record of `process`'s pid while that pid still names that life.
    fn current(&self, process: ProcessIdentity) -> Result<Option<ProcessRecord>, ObserveError> {
        Ok(self
            .record(process.pid)?
            .filter(|record| record.unique_id == process.birth.0))
    }

    /// The image `process` runs, read between two reads of its record. `None` when the image
    /// was not readable at that instant: the life ended, or was between two images.
    fn image(&self, process: ProcessIdentity) -> Result<Option<ProcessImage>, ObserveError> {
        let Some(before) = self.current(process)? else {
            return Ok(None);
        };
        let read = self.reads.image(process.pid);
        let after = self.current(process);
        let source = match read {
            // Read after the life ended, the image may be a stranger's.
            Ok(image) => return Ok(after?.map(|_| image)),
            Err(source) => source,
        };
        let (after, fence) = match after {
            Ok(after) => (after, None),
            Err(fence) => (None, Some(fence)),
        };
        // What the kernel answers a reader it refuses may have other causes too; its own
        // credential rule, applied to the life's records around the call, says when it is one.
        let reader = self.reads.reader_uid();
        let refused = platform::image_refused(&source)
            .then(|| {
                std::iter::once(before)
                    .chain(after)
                    .find(|record| reader != 0 && reader != record.uid)
            })
            .flatten();
        let failed = match refused {
            Some(record) => ObserveError::ImageNotPermitted {
                pid: process.pid,
                uid: record.uid,
                reader,
                source,
            },
            None => kernel(platform::IMAGE, process.pid, source),
        };
        Err(match fence {
            None => failed,
            Some(fence) => ObserveError::Several {
                first: Box::new(failed),
                rest: vec![fence],
            },
        })
    }

    /// The children `parent` has now, read between two reads of its record. `None` when the
    /// parent's life ended around the read, so the list may be a stranger's. A failed read is
    /// always an error: the kernel lists children by filtering every process by parent pid, so
    /// a parent's exit empties the list and never fails it.
    fn children(&self, parent: ProcessIdentity) -> Result<Option<Vec<u32>>, ObserveError> {
        if self.current(parent)?.is_none() {
            return Ok(None);
        }
        let children = self
            .reads
            .children(parent.pid)
            .map_err(|source| kernel(platform::CHILDREN, parent.pid, source))?;
        Ok(self.current(parent)?.map(|_| children))
    }

    fn record(&self, pid: u32) -> Result<Option<ProcessRecord>, ObserveError> {
        self.reads
            .record(pid)
            .map_err(|source| kernel(platform::RECORD, pid, source))
    }
}

fn kernel(call: &'static str, pid: u32, source: io::Error) -> ObserveError {
    ObserveError::Kernel { call, pid, source }
}

fn identity(record: &ProcessRecord) -> ProcessIdentity {
    ProcessIdentity {
        pid: record.pid,
        birth: BirthToken(record.unique_id),
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::Write;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};

    use super::*;
    use crate::api::dto::{CommandArg, ExitStatus};
    use crate::api::process::ProcessCoverage;
    use cowshed_os::fork_lock::Spawn;

    /// `shell -c script`, held before its exec until it is watched. The pre-exec hook reports
    /// the child's pid, then waits for the observer; `extra` becomes the child's fd 3.
    pub(super) fn watched(
        shell: &str,
        script: &str,
        extra: Option<OwnedFd>,
    ) -> (KqueueObserver, Child) {
        let (ready_read, ready_write) = std::io::pipe().unwrap();
        let (gate_read, mut gate_write) = std::io::pipe().unwrap();
        let mut command = Command::new(shell);
        command.args(["-c", script]).stdout(Stdio::piped());
        let ready = ready_write.as_raw_fd();
        let gate = gate_read.as_raw_fd();
        let extra = extra.as_ref().map(AsRawFd::as_raw_fd);
        // SAFETY: the hook calls only async-signal-safe functions.
        unsafe {
            command.pre_exec(move || {
                let pid = libc::getpid().to_ne_bytes();
                if libc::write(ready, pid.as_ptr().cast(), pid.len()) != 4 {
                    return Err(io::Error::last_os_error());
                }
                let mut byte = 0_u8;
                if libc::read(gate, (&raw mut byte).cast(), 1) != 1 {
                    return Err(io::Error::last_os_error());
                }
                // Last, since fd 3 may be `ready` or `gate`. `dup2` onto itself keeps the
                // close-on-exec flag every Rust pipe is opened with, so it is cleared instead.
                let inherited = match extra {
                    None => true,
                    Some(3) => libc::fcntl(3, libc::F_SETFD, 0) == 0,
                    Some(extra) => libc::dup2(extra, 3) == 3,
                };
                if !inherited {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        std::thread::scope(|scope| {
            let spawned = scope.spawn(|| command.spawn_locked().expect("spawn"));
            let mut pid = [0_u8; 4];
            std::io::Read::read_exact(&mut &ready_read, &mut pid).expect("child pid");
            let pid = u32::try_from(i32::from_ne_bytes(pid)).expect("pid");
            let observer = KqueueObserver::watch(pid).expect("watch the held root");
            gate_write.write_all(b"x").expect("release");
            let child = spawned.join().expect("spawner");
            assert_eq!(child.id(), pid);
            (observer, child)
        })
    }

    /// Every failure an `Err` carries, one by one.
    fn flatten(error: ObserveError, into: &mut Vec<ObserveError>) {
        match error {
            ObserveError::Several { first, rest } => {
                into.push(*first);
                into.extend(rest);
            }
            error => into.push(error),
        }
    }

    /// Fold until `pid` exits, keeping every failure the observer returned on the way: they
    /// are values the caller checks, never discarded.
    pub(super) fn until_exited(observer: &mut KqueueObserver, pid: u32) -> Vec<ObserveError> {
        let mut failures = Vec::new();
        while observer.fold().live(pid).is_some() {
            if let Err(error) = observer.wait() {
                flatten(error, &mut failures);
            }
        }
        failures
    }

    /// A file holding `ready`, which a held cat prints only once it runs.
    pub(super) fn marker(name: &str) -> String {
        let marker = std::env::temp_dir().join(format!("cowshed-{name}-{}", std::process::id()));
        std::fs::write(&marker, "ready\n").unwrap();
        marker.to_str().unwrap().to_owned()
    }

    pub(super) fn argv(bytes: &[&str]) -> Vec<CommandArg> {
        bytes
            .iter()
            .map(|&argument| CommandArg::new(argument))
            .collect()
    }

    /// Kernel reads answered from a script, one queued answer per read, so a test sets the
    /// identity a pid names at each instant the observer asks. An unscripted read fails the test.
    #[derive(Default)]
    pub(super) struct Scripted {
        records: std::cell::RefCell<
            std::collections::HashMap<
                u32,
                std::collections::VecDeque<io::Result<Option<ProcessRecord>>>,
            >,
        >,
        images: std::cell::RefCell<
            std::collections::HashMap<u32, std::collections::VecDeque<io::Result<ProcessImage>>>,
        >,
        children: std::cell::RefCell<
            std::collections::HashMap<u32, std::collections::VecDeque<io::Result<Vec<u32>>>>,
        >,
        /// Each membership read of the job's group, in order.
        groups: std::cell::RefCell<std::collections::VecDeque<io::Result<Vec<ProcessRecord>>>>,
        /// Every signal the observer sent, each answered as reaching its life.
        signalled: std::cell::RefCell<Vec<(ProcessIdentity, libc::c_int)>>,
        /// A native counter denial, when scripted; custody must never ask for it.
        usage_error: Option<i32>,
        usage_reads: std::cell::Cell<u32>,
        /// The reader's effective uid; root unless a test sets it.
        reader: u32,
    }

    impl Scripted {
        pub(super) fn record(self, pid: u32, answers: &[Option<ProcessRecord>]) -> Self {
            self.records
                .borrow_mut()
                .entry(pid)
                .or_default()
                .extend(answers.iter().copied().map(Ok));
            self
        }

        fn record_failure(self, pid: u32, errno: i32) -> Self {
            self.records
                .borrow_mut()
                .entry(pid)
                .or_default()
                .push_back(Err(io::Error::from_raw_os_error(errno)));
            self
        }

        fn image(self, pid: u32, answer: io::Result<ProcessImage>) -> Self {
            self.images
                .borrow_mut()
                .entry(pid)
                .or_default()
                .push_back(answer);
            self
        }

        fn children(self, pid: u32, answer: io::Result<Vec<u32>>) -> Self {
            self.children
                .borrow_mut()
                .entry(pid)
                .or_default()
                .push_back(answer);
            self
        }

        fn group(self, answer: io::Result<Vec<ProcessRecord>>) -> Self {
            self.groups.borrow_mut().push_back(answer);
            self
        }

        fn reading_as(self, reader: u32) -> Self {
            Self { reader, ..self }
        }
    }

    fn next<T>(
        script: &std::cell::RefCell<std::collections::HashMap<u32, std::collections::VecDeque<T>>>,
        what: &str,
        pid: u32,
    ) -> T {
        script
            .borrow_mut()
            .get_mut(&pid)
            .and_then(std::collections::VecDeque::pop_front)
            .unwrap_or_else(|| panic!("an unscripted {what} read of process {pid}"))
    }

    impl ProcessReads for Scripted {
        fn record(&self, pid: u32) -> io::Result<Option<ProcessRecord>> {
            next(&self.records, "record", pid)
        }

        fn image(&self, pid: u32) -> io::Result<ProcessImage> {
            next(&self.images, "image", pid)
        }

        fn children(&self, pid: u32) -> io::Result<Vec<u32>> {
            next(&self.children, "child list", pid)
        }

        fn reader_uid(&self) -> u32 {
            self.reader
        }

        fn usage(
            &self,
            process: ProcessIdentity,
            _held: Option<BorrowedFd<'_>>,
            _start: Option<StartStamp>,
            _at: Instant,
        ) -> Result<Sampled, SampleError> {
            self.usage_reads.set(self.usage_reads.get() + 1);
            if let Some(errno) = self.usage_error {
                return Err(SampleError::Read {
                    pid: process.pid,
                    source: io::Error::from_raw_os_error(errno),
                });
            }
            // These metadata/error controls provide no native counter proof.
            Ok(Sampled::Unproven)
        }

        fn group(&self, leader: &Birth) -> io::Result<Vec<ProcessRecord>> {
            self.groups
                .borrow_mut()
                .pop_front()
                .unwrap_or_else(|| panic!("an unscripted group read of leader {}", leader.pid()))
        }

        fn signal(&self, process: ProcessIdentity, signal: libc::c_int) -> io::Result<bool> {
            self.signalled.borrow_mut().push((process, signal));
            Ok(true)
        }
    }

    /// Every scripted process runs as this uid.
    const OWNER: u32 = 501;

    pub(super) fn record(pid: u32, unique_id: u64, parent_unique_id: u64) -> ProcessRecord {
        ProcessRecord {
            pid,
            ppid: 1,
            unique_id,
            parent_unique_id,
            uid: OWNER,
            started_seconds: 1_700_000_000,
        }
    }

    pub(super) fn life(pid: u32, unique_id: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(unique_id),
        }
    }

    fn shell_image(program: &str) -> ProcessImage {
        ProcessImage {
            program: program.into(),
            argv: argv(&[program]).into(),
            truncated: false,
        }
    }

    /// An observer over scripted reads and a real, empty event source, holding `fold` and the
    /// registrations named, none of which the source will report of.
    pub(super) fn scripted(
        reads: Scripted,
        fold: TestFold,
        registered: &[ProcessIdentity],
    ) -> KqueueObserver<Scripted> {
        KqueueObserver {
            source: Source::open().expect("the event source"),
            reads,
            fold: fold.fold,
            events: fold.events,
            registered: registered
                .iter()
                .map(|&process| (process, Watch::scripted()))
                .collect(),
            missed: Vec::new(),
            reports: Vec::new(),
        }
    }

    /// A report that says nothing of `process`, for a test to say what the source reported.
    fn report(process: ProcessIdentity) -> Report {
        Report {
            process,
            forked: false,
            exec: false,
            exit: None,
        }
    }

    /// An exit reported with the wait status of `exit(0)`.
    const EXITED: Option<ExitReport> = Some(ExitReport::Status(0));

    /// A root, a first life of pid 900, and a second life of pid 900 forked before the first
    /// life's exit was folded.
    fn reused_pid_fold() -> (TestFold, ProcessIdentity, ProcessIdentity, ProcessIdentity) {
        let (root, old, new) = (life(800, 1), life(900, 2), life(900, 3));
        let mut fold = TestFold::default();
        let at = utc_at_second(1_700_000_000).unwrap();
        fold.apply(ProcessObservation::Root {
            process: root,
            ppid: 1,
            at: at.clone(),
            image: shell_image("/bin/bash"),
        })
        .unwrap();
        for process in [old, new] {
            fold.apply(ProcessObservation::Forked {
                process,
                parent: root,
                at: at.clone(),
            })
            .unwrap();
        }
        (fold, root, old, new)
    }

    /// The older life's late exit and exec land on that life by its identity; the life now
    /// holding the pid keeps running its own image.
    #[test]
    fn an_older_life_s_queued_events_never_touch_the_life_now_holding_its_pid() {
        let (fold, root, old, new) = reused_pid_fold();
        // When the old life's exec is handled, the pid names the new life.
        let reads = Scripted::default().record(900, &[Some(record(900, 3, 1))]);
        let mut observer = scripted(reads, fold, &[root, old, new]);
        observer
            .fold_report(
                Report {
                    exec: true,
                    ..report(old)
                },
                ObservationPurpose::Metadata,
            )
            .unwrap();
        observer
            .fold_report(
                Report {
                    exit: EXITED,
                    ..report(old)
                },
                ObservationPurpose::Metadata,
            )
            .unwrap();

        assert_eq!(observer.fold().live(900), Some(new));
        let nodes: Vec<_> = observer.fold().nodes().collect();
        let old_node = nodes.iter().find(|node| node.identity == old).unwrap();
        let new_node = nodes.iter().find(|node| node.identity == new).unwrap();
        assert_eq!(
            old_node.exit.and_then(|exit| exit.status.as_ref()),
            Some(&ExitStatus::Exited { code: 0 })
        );
        assert_eq!(new_node.exit, None);
        assert_eq!(
            new_node.image,
            &shell_image("/bin/bash"),
            "no stranger image"
        );
        assert!(
            !observer.registered.contains_key(&old),
            "its exit ended its registration"
        );
    }

    /// A report of a registration the observer withdrew is not the tree's.
    #[test]
    fn an_event_of_a_withdrawn_registration_is_ignored() {
        let (fold, root, old, new) = reused_pid_fold();
        let mut observer = scripted(Scripted::default(), fold, &[root, old, new]);
        let stranger = life(900, 77);
        observer
            .fold_report(
                Report {
                    forked: true,
                    exit: EXITED,
                    ..report(stranger)
                },
                ObservationPurpose::Metadata,
            )
            .unwrap();
        assert_eq!(observer.fold().live(900), Some(new));
        assert_eq!(observer.fold().nodes().count(), 3);
    }

    /// A running process the test holds, so registrations against its pid succeed.
    pub(super) fn held_process() -> (Child, u32) {
        let child = Command::new("cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn_locked()
            .expect("spawn cat");
        let pid = child.id();
        (child, pid)
    }

    pub(super) fn release(mut child: Child) {
        drop(child.stdin.take());
        child.wait_bounded().expect("reap");
    }

    #[test]
    fn subtree_selection_never_reads_a_group_and_requires_exit_registrations() {
        let (mut fold, root, old, sibling) = reused_pid_fold();
        let child = life(901, 4);
        let at = utc_at_second(1_700_000_001).unwrap();
        fold.apply(ProcessObservation::Forked {
            process: child,
            parent: old,
            at: at.clone(),
        })
        .unwrap();
        fold.apply(ProcessObservation::Exited {
            process: old,
            status: Some(ExitStatus::Exited { code: 0 }),
            at,
        })
        .unwrap();
        let mut observer = scripted(Scripted::default(), fold, &[root, sibling, child]);
        // Scripted has no group-read answers: any accidental group expansion panics.
        assert_eq!(observer.subtree(old).unwrap(), [child]);
        observer.registered.remove(&child);
        assert_eq!(
            failed(&observer.subtree(old).unwrap_err()),
            (platform::WATCH, child.pid, None)
        );
    }

    #[test]
    fn live_admission_requires_retained_registration_and_current_exact_identity() {
        let (fold, root, old, current) = reused_pid_fold();
        let reads = Scripted::default()
            .record(
                current.pid,
                &[
                    Some(record(current.pid, 99, 1)),
                    Some(record(current.pid, current.birth.0, 1)),
                ],
            )
            .record_failure(current.pid, libc::EIO);
        let mut observer = scripted(reads, fold, &[root, old, current]);
        assert!(
            !observer.retained_live(old).unwrap(),
            "a stale retained life is not a peer"
        );
        assert!(
            !observer.retained_live(current).unwrap(),
            "a different native life holds this pid"
        );
        assert!(observer.retained_live(current).unwrap());
        assert_eq!(
            failed(&observer.retained_live(current).unwrap_err()),
            (platform::RECORD, current.pid, Some(libc::EIO))
        );
        observer.registered.remove(&current);
        assert!(!observer.retained_live(current).unwrap());
    }

    /// A member of the job's group the fold never saw born is adopted, watched and owned
    /// before anything signals it, and the tree says its birth went unobserved. A membership
    /// that cannot be read is an error, never an empty ownership, and a life whose exit was
    /// folded is never signalled again.
    #[test]
    fn a_group_member_the_fold_never_saw_is_adopted_before_it_is_owned() {
        let (child, pid) = held_process();
        let member = record(pid, 40, 99);
        let process = life(pid, 40);
        let reads = Scripted::default()
            .group(Ok(vec![member]))
            .record(pid, &[Some(member); 5])
            .image(pid, Ok(shell_image("/bin/cat")))
            .children(pid, Ok(Vec::new()))
            .group(Err(io::Error::from_raw_os_error(libc::EPERM)));
        let mut observer = scripted(reads, TestFold::default(), &[]);
        let leader = Birth::Unobserved {
            pid,
            reason: "scripted".to_owned(),
        };
        assert_eq!(observer.owned(&leader).unwrap(), [process]);
        assert!(observer.registered.contains_key(&process));
        assert_eq!(
            observer.fold().image(process).map(|image| &*image.program),
            Some("/bin/cat")
        );
        assert_eq!(
            observer.fold().coverage(),
            &ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnobservedBirth { pid }
            }
        );
        assert_eq!(observer.missed(None), []);
        let refused = observer.owned(&leader).expect_err("an unread membership");
        assert_eq!(failed(&refused), (platform::GROUP, pid, Some(libc::EPERM)));

        assert!(observer.signal(process, libc::SIGTERM).unwrap());
        observer
            .observe(ProcessObservation::Exited {
                process,
                status: Some(ExitStatus::Exited { code: 0 }),
                at: utc_at_second(1_700_000_001).unwrap(),
            })
            .unwrap();
        assert!(!observer.signal(process, libc::SIGKILL).unwrap());
        assert_eq!(
            *observer.reads.signalled.borrow(),
            [(process, libc::SIGTERM)]
        );
        let watch = observer
            .registered
            .remove(&process)
            .expect("its registration");
        assert!(observer.source.withdraw(process, &watch).unwrap());
        release(child);
    }

    /// A fork whose children could not be listed, and a listed child that could not be
    /// watched, are the adoptions a retirement reports lost.
    #[test]
    fn a_fork_the_observer_could_not_follow_is_a_missed_adoption() {
        let (fold, root, _, _) = reused_pid_fold();
        let reads = Scripted::default().record(800, &[None]);
        let mut observer = scripted(reads, fold, &[root]);
        observer
            .fold_report(
                Report {
                    forked: true,
                    ..report(root)
                },
                ObservationPurpose::Metadata,
            )
            .expect("a parent gone before its list is no read failure");
        assert_eq!(
            observer.missed(None),
            [ProcessCoverageGap::UncountedFork { pid: 800 }]
        );

        let (fold, root, _, _) = reused_pid_fold();
        let ours = Some(record(800, 1, 0));
        let reads = Scripted::default()
            .record(800, &[ours, ours])
            .children(800, Ok(vec![901]))
            .record_failure(901, libc::EIO);
        let mut observer = scripted(reads, fold, &[root]);
        let error = observer
            .fold_report(
                Report {
                    forked: true,
                    ..report(root)
                },
                ObservationPurpose::Metadata,
            )
            .expect_err("the child's record read failed");
        assert_eq!(failed(&error), (platform::RECORD, 901, Some(libc::EIO)));
        assert_eq!(
            observer.missed(None),
            [ProcessCoverageGap::UnobservedBirth { pid: 901 }]
        );
    }

    /// A subtree's retirement answers for the adoptions missed under its own lives and for a loss
    /// of the event source, which names no life; a sibling's missed fork is the sibling's and the
    /// whole job's (07_api.md: "Subtree scope is the retained root and its descendants ... not
    /// siblings").
    #[test]
    fn a_subtree_s_lost_adoptions_are_those_missed_under_its_own_lives() {
        let (root, ours, sibling) = (life(800, 1), life(901, 2), life(902, 3));
        let mut fold = TestFold::default();
        let at = utc_at_second(1_700_000_000).unwrap();
        fold.apply(ProcessObservation::Root {
            process: root,
            ppid: 1,
            at: at.clone(),
            image: shell_image("/bin/bash"),
        })
        .unwrap();
        for process in [ours, sibling] {
            fold.apply(ProcessObservation::Forked {
                process,
                parent: root,
                at: at.clone(),
            })
            .unwrap();
        }
        // The sibling forked and its life ended before its children could be listed.
        let reads = Scripted::default().record(902, &[None]);
        let mut observer = scripted(reads, fold, &[root, ours, sibling]);
        observer
            .fold_report(
                Report {
                    forked: true,
                    ..report(sibling)
                },
                ObservationPurpose::Metadata,
            )
            .expect("a parent gone before its list is no read failure");
        observer.gap(ProcessCoverageGap::EventsLost).unwrap();

        let missed_fork = ProcessCoverageGap::UncountedFork { pid: 902 };
        assert_eq!(
            observer.missed(Some(ours)),
            [ProcessCoverageGap::EventsLost]
        );
        assert_eq!(
            observer.missed(Some(sibling)),
            [missed_fork, ProcessCoverageGap::EventsLost]
        );
        for whole in [Some(root), None] {
            assert_eq!(
                observer.missed(whole),
                [missed_fork, ProcessCoverageGap::EventsLost]
            );
        }
    }

    /// An image or child list read while the pid changed life is a stranger's and is never
    /// attributed; read while the life ran throughout, it is.
    #[test]
    fn an_image_or_child_list_read_across_a_change_of_life_is_discarded() {
        let process = life(910, 5);
        let ours = Some(record(910, 5, 1));
        let stranger = Some(record(910, 6, 1));
        let reads = Scripted::default()
            .record(
                910,
                &[ours, stranger, ours, None, ours, ours, ours, stranger],
            )
            .image(910, Ok(shell_image("/stranger")))
            .image(910, Ok(shell_image("/stranger")))
            .image(910, Ok(shell_image("/bin/cat")))
            .children(910, Ok(vec![911, 912]));
        let observer = scripted(reads, TestFold::default(), &[]);
        assert_eq!(observer.image(process).unwrap(), None, "changed life");
        assert_eq!(observer.image(process).unwrap(), None, "exited");
        assert_eq!(
            observer.image(process).unwrap(),
            Some(shell_image("/bin/cat"))
        );
        assert_eq!(observer.children(process).unwrap(), None, "changed life");
    }

    #[test]
    fn custody_exec_and_coalesced_exit_never_read_an_image() {
        for (purpose, exit) in [
            (ObservationPurpose::Custody, None),
            (ObservationPurpose::Metadata, EXITED),
        ] {
            let current = life(900, 3);
            let mut fold = TestFold::default();
            fold.apply(ProcessObservation::Root {
                process: current,
                ppid: 1,
                at: utc_at_second(1_700_000_000).unwrap(),
                image: shell_image("/bin/bash"),
            })
            .unwrap();
            // No image or record read is scripted: either would panic.
            let mut observer = scripted(Scripted::default(), fold, &[current]);
            observer
                .fold_report(
                    Report {
                        exec: true,
                        exit,
                        ..report(current)
                    },
                    purpose,
                )
                .unwrap();
            assert_eq!(observer.fold().image(current), None);
            assert_eq!(observer.fold().live(current.pid).is_none(), exit.is_some());
            assert!(matches!(
                observer.fold().coverage(),
                ProcessCoverage::Gap {
                    reason: ProcessCoverageGap::UnreadImage { pid: 900 }
                }
            ));
        }
    }

    #[test]
    fn custody_exit_joins_without_usage_while_metadata_keeps_eperm() {
        for purpose in [ObservationPurpose::Custody, ObservationPurpose::Metadata] {
            let current = life(900, 3);
            let mut fold = TestFold::default();
            fold.apply(ProcessObservation::Root {
                process: current,
                ppid: 1,
                at: utc_at_second(1_700_000_000).unwrap(),
                image: shell_image("/bin/ps"),
            })
            .unwrap();
            let reads = Scripted {
                usage_error: Some(libc::EPERM),
                ..Scripted::default()
            };
            let mut observer = scripted(reads, fold, &[current]);
            let outcome = observer.fold_report(
                Report {
                    exit: EXITED,
                    ..report(current)
                },
                purpose,
            );
            if purpose == ObservationPurpose::Metadata {
                let ObserveError::Usage(SampleError::Read { pid, source }) = outcome.unwrap_err()
                else {
                    panic!("the native final-usage denial was not retained")
                };
                assert_eq!(pid, current.pid);
                assert_eq!(source.raw_os_error(), Some(libc::EPERM));
                assert_eq!(observer.reads.usage_reads.get(), 1);
            } else {
                outcome.unwrap();
                assert_eq!(observer.reads.usage_reads.get(), 0);
            }
            assert!(observer.fold().live(current.pid).is_none());
            assert!(!observer.registered.contains_key(&current));
            assert_eq!(
                observer.fold().coverage(),
                &ProcessCoverage::Gap {
                    reason: ProcessCoverageGap::UnreadFinalUsage { pid: current.pid },
                }
            );
            assert!(observer.reads.signalled.borrow().is_empty());
        }
    }

    /// An exit reported without its status is joined: the life ends, its registration with it,
    /// and its status is the named gap, never a code the source did not report.
    #[test]
    fn a_withheld_exit_is_joined_with_its_status_as_the_gap() {
        for purpose in [ObservationPurpose::Custody, ObservationPurpose::Metadata] {
            let current = life(900, 3);
            let mut fold = TestFold::default();
            fold.apply(ProcessObservation::Root {
                process: current,
                ppid: 1,
                at: utc_at_second(1_700_000_000).unwrap(),
                image: shell_image("/usr/sbin/lsof"),
            })
            .unwrap();
            let mut observer = scripted(Scripted::default(), fold, &[current]);
            observer
                .fold_report(
                    Report {
                        exit: Some(ExitReport::Withheld),
                        ..report(current)
                    },
                    purpose,
                )
                .expect("a withheld exit is no failure");
            assert!(observer.fold().live(current.pid).is_none());
            assert!(!observer.registered.contains_key(&current));
            let node = observer.fold().nodes().next().expect("the retained life");
            assert_eq!(node.exit.map(|exit| &exit.status), Some(&None));
            assert_eq!(
                observer.fold().coverage(),
                &ProcessCoverage::Gap {
                    reason: ProcessCoverageGap::UnreadExitStatus { pid: current.pid },
                }
            );
        }
    }

    #[test]
    fn custody_folding_still_refuses_unknown_lineage_and_missing_exit_watches() {
        let (fold, root, old, current) = reused_pid_fold();
        let reads = Scripted::default().record_failure(root.pid, libc::EPERM);
        let mut observer = scripted(reads, fold, &[root, old, current]);
        let error = observer
            .fold_report(
                Report {
                    forked: true,
                    ..report(root)
                },
                ObservationPurpose::Custody,
            )
            .unwrap_err();
        assert_eq!(
            failed(&error),
            (platform::RECORD, root.pid, Some(libc::EPERM))
        );
        observer.registered.remove(&current);
        let error = observer.subtree(root).unwrap_err();
        assert_eq!(failed(&error), (platform::WATCH, current.pid, None));
        assert!(observer.reads.signalled.borrow().is_empty());
    }

    pub(super) fn failed(error: &ObserveError) -> (&'static str, u32, Option<i32>) {
        match error {
            ObserveError::Kernel { call, pid, source } => (*call, *pid, source.raw_os_error()),
            ObserveError::Unread { cause, .. } => failed(cause),
            other => panic!("not a kernel error: {other}"),
        }
    }

    /// A failed read is the kernel's error with its call and errno, never swallowed into a gap
    /// or an empty answer, whether or not its life ran on: the errnos a kernel also answers
    /// between two images, and the one it refuses a reader with, included.
    #[test]
    fn a_failed_read_is_returned_with_call_and_errno_even_after_its_life_ended() {
        let process = life(920, 9);
        let ours = Some(record(920, 9, 1));
        for errno in [libc::ENOMEM, libc::EIO, platform::IMAGE_REFUSAL] {
            for after in [ours, None, Some(record(920, 10, 1))] {
                let reads = Scripted::default()
                    .record(920, &[ours, after, ours])
                    .image(920, Err(io::Error::from_raw_os_error(errno)))
                    .children(920, Err(io::Error::from_raw_os_error(libc::EFAULT)));
                let observer = scripted(reads, TestFold::default(), &[]);
                assert_eq!(
                    failed(&observer.image(process).unwrap_err()),
                    (platform::IMAGE, 920, Some(errno))
                );
                assert_eq!(
                    failed(&observer.children(process).unwrap_err()),
                    (platform::CHILDREN, 920, Some(libc::EFAULT))
                );
            }
        }
    }

    /// A record read after the image read that itself fails loses neither failure.
    #[test]
    fn a_failed_fence_after_a_failed_image_read_keeps_both() {
        let process = life(940, 13);
        let ours = Some(record(940, 13, 1));
        let reads = Scripted::default()
            .record(940, &[ours])
            .record_failure(940, libc::EPERM)
            .image(940, Err(io::Error::from_raw_os_error(libc::EIO)));
        let error = scripted(reads, TestFold::default(), &[])
            .image(process)
            .unwrap_err();
        let ObserveError::Several { first, rest } = error else {
            panic!("{error}");
        };
        assert_eq!(
            failed(first.as_ref()),
            (platform::IMAGE, 940, Some(libc::EIO))
        );
        assert_eq!(
            rest.iter().map(failed).collect::<Vec<_>>(),
            [(platform::RECORD, 940, Some(libc::EPERM))]
        );
        // A read that succeeded is unattributable without its fence: the fence's failure.
        let reads = Scripted::default()
            .record(940, &[ours])
            .record_failure(940, libc::EPERM)
            .image(940, Ok(shell_image("/bin/cat")));
        let error = scripted(reads, TestFold::default(), &[])
            .image(process)
            .unwrap_err();
        assert_eq!(failed(&error), (platform::RECORD, 940, Some(libc::EPERM)));
    }

    /// The image read's refusal errno is named a refusal where the kernel's credential rule
    /// refuses the reader, and keeps that errno.
    #[test]
    fn a_refusal_errno_from_the_image_read_is_a_refusal_only_by_credentials() {
        let process = life(930, 11);
        let ours = Some(record(930, 11, 1));
        let einval = || Err(io::Error::from_raw_os_error(platform::IMAGE_REFUSAL));
        let refused =
            |reads: Scripted| match scripted(reads, TestFold::default(), &[]).image(process) {
                Err(ObserveError::ImageNotPermitted {
                    pid,
                    uid,
                    reader,
                    source,
                }) => (pid, uid, reader, source.raw_os_error()),
                other => panic!("{other:?}"),
            };
        let another_user = Scripted::default()
            .reading_as(OWNER + 1)
            .record(930, &[ours, ours])
            .image(930, einval());
        assert_eq!(
            refused(another_user),
            (930, OWNER, OWNER + 1, Some(platform::IMAGE_REFUSAL))
        );
        // A set-id exec between the two reads: the life now runs as root.
        let set_id = Some(ProcessRecord {
            uid: 0,
            ..record(930, 11, 1)
        });
        let exec_set_id = Scripted::default()
            .reading_as(OWNER)
            .record(930, &[ours, set_id])
            .image(930, einval());
        assert_eq!(
            refused(exec_set_id),
            (930, 0, OWNER, Some(platform::IMAGE_REFUSAL))
        );
        // Another user's `EIO` is no refusal: the kernel checked credentials before it.
        let unmapped = Scripted::default()
            .reading_as(OWNER + 1)
            .record(930, &[ours, ours])
            .image(930, Err(io::Error::from_raw_os_error(libc::EIO)));
        let error = scripted(unmapped, TestFold::default(), &[])
            .image(process)
            .unwrap_err();
        assert_eq!(failed(&error), (platform::IMAGE, 930, Some(libc::EIO)));
    }

    /// Failed reads in a batch drop nothing: an exit following a failed exec, and every
    /// later event, are folded, and every failure of the batch is returned.
    #[test]
    fn a_failed_read_never_drops_the_rest_of_its_batch() {
        let (root, first, second) = (life(800, 1), life(900, 2), life(901, 3));
        let mut fold = TestFold::default();
        let at = utc_at_second(1_700_000_000).unwrap();
        fold.apply(ProcessObservation::Root {
            process: root,
            ppid: 1,
            at: at.clone(),
            image: shell_image("/bin/bash"),
        })
        .unwrap();
        for process in [first, second] {
            fold.apply(ProcessObservation::Forked {
                process,
                parent: root,
                at: at.clone(),
            })
            .unwrap();
        }
        let enomem = || Err(io::Error::from_raw_os_error(libc::ENOMEM));
        let reads = Scripted::default()
            .record(900, &[Some(record(900, 2, 1)); 2])
            .image(900, enomem())
            .record(901, &[Some(record(901, 3, 1)); 2])
            .image(901, enomem());
        let mut observer = scripted(reads, fold, &[root, first, second]);
        let error = observer
            .fold_batch(
                &mut vec![
                    Ok(Report {
                        exec: true,
                        ..report(first)
                    }),
                    Ok(Report {
                        exit: EXITED,
                        ..report(first)
                    }),
                    Ok(Report {
                        exec: true,
                        ..report(second)
                    }),
                    Ok(Report {
                        exit: EXITED,
                        ..report(root)
                    }),
                ],
                ObservationPurpose::Metadata,
            )
            .expect_err("two reads failed");
        let ObserveError::Several { first: one, rest } = error else {
            panic!("{error}");
        };
        assert_eq!(
            failed(one.as_ref()),
            (platform::IMAGE, 900, Some(libc::ENOMEM))
        );
        assert_eq!(
            rest.iter().map(failed).collect::<Vec<_>>(),
            [(platform::IMAGE, 901, Some(libc::ENOMEM))]
        );
        let exited = |process| {
            observer
                .fold()
                .nodes()
                .find(|node| node.identity == process)
                .is_some_and(|node| node.exit.is_some())
        };
        assert!(exited(first), "the exit beside the failed exec");
        assert!(exited(root), "the event after both failures");
        assert!(!exited(second));
        assert!(!observer.registered.contains_key(&first));
        assert!(!observer.registered.contains_key(&root));
        assert_eq!(
            observer.fold().coverage(),
            &ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnreadImage { pid: 900 }
            },
            "the failed read is a gap as well as an error"
        );
    }

    pub(super) struct TestFold {
        fold: JobProcessFold,
        events: Vec<JobProcessEvent>,
    }

    impl Default for TestFold {
        fn default() -> Self {
            Self {
                fold: JobProcessFold::new(Instant::now()),
                events: Vec::new(),
            }
        }
    }

    impl TestFold {
        fn apply(&mut self, observation: ProcessObservation) -> Result<(), JobProcessFoldError> {
            self.fold
                .apply(JobProcessObservation::Tree(observation), &mut self.events)
        }
    }
}
