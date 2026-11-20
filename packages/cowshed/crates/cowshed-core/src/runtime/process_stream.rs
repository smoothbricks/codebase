//! A job's process records and the events that change them (07_api.md, "Process-tree
//! observations"; 13_telemetry.md, "Process-tree spans").
//!
//! The fold joins what the observer saw (births, execs, exits: [`ProcessTreeFold`]) with what the
//! sampler read (each life's own usage: [`ProcessUsageFold`], and its blocker) and answers each
//! observation with the [`JobProcessEvent`]s it causes. It is pure: the kernel reads are the
//! observer's and the sampler's, and the clock is the sampler's tick.
//!
//! An event is emitted for a birth, an exec and an exit, and for a change a consumer would act
//! on: a blocker transition, a busy/idle flip, resident memory crossing a power of two, or the
//! first usage read. An ordinary sample that moves only counters is not a change. A live process
//! whose usage no record restated for [`PROCESS_HEARTBEAT`] gets its own heartbeat at the next
//! tick: one a minute for each unchanged live process.
//!
//! A reading taken after a process exited is withheld from every record until the exit carries
//! it, once, as final usage. The process is no longer live: no heartbeat restates it, no blocker
//! read of it is accepted, and an exec observed late still shows the usage read before the exit.
//! Nothing about that process follows its exit.
//!
//! Applying the events in order to the records they name rebuilds the fold's tree
//! ([`JobProcessDelta::apply`] sets and clears): exactly, except for counters that moved without
//! a change since the last event, which the next heartbeat or the exit restates.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::api::dto::{JobId, UtcTimestamp};
use crate::api::process::{
    BlockerChange, JobProcessDelta, JobProcessEvent, JobProcessSample, JobProcessTree,
    PROCESS_HEARTBEAT, ProcessBlockedOn, ProcessCoverage, ProcessUsage,
};
use crate::api::resources::ResidentBytes;
use crate::runtime::process_tree::{
    LifeReadings, ProcessFoldError, ProcessIdentity, ProcessObservation, ProcessTreeFold,
};
use crate::runtime::process_usage::{
    ProcessUsageFold, UsageFoldError, UsageObservation, UsageReading,
};

/// The bounded events of one worker-stream subscription. Its producer retains the native
/// journal; backpressure never coalesces a lifecycle or coverage transition.
#[derive(Debug)]
pub struct JobProcessStream {
    events: tokio::sync::mpsc::Receiver<crate::error::Result<JobProcessEvent>>,
}

impl JobProcessStream {
    pub(crate) fn new(
        events: tokio::sync::mpsc::Receiver<crate::error::Result<JobProcessEvent>>,
    ) -> Self {
        Self { events }
    }

    pub async fn next(&mut self) -> Option<crate::error::Result<JobProcessEvent>> {
        self.events.recv().await
    }
}

/// One thing the observer or the sampler tells the fold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobProcessObservation {
    /// A birth, exec, exit or loss the observer saw.
    Tree(ProcessObservation),
    /// A read of one life's own counters.
    Usage {
        process: ProcessIdentity,
        reading: UsageReading,
    },
    /// What one life was found waiting on; `None` when the sampler found no evidence either
    /// way, which is never [`ProcessBlockedOn::None`].
    Blocker {
        process: ProcessIdentity,
        blocked_on: Option<ProcessBlockedOn>,
    },
    /// The sampler's clock, on its monotonic clock.
    Tick { at: Instant },
}

/// An observation that contradicts what the fold holds: the observer or the sampler is wrong.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum JobProcessFoldError {
    #[error(transparent)]
    Tree(#[from] ProcessFoldError),
    #[error(transparent)]
    Usage(#[from] UsageFoldError),
    #[error("process {pid} was sampled, but the tree holds no such life")]
    NotRetained { pid: u32 },
    #[error("process {pid}'s blocker was read after it stopped running")]
    BlockerNotRunning { pid: u32 },
    #[error("the tree holds more processes than an event can index")]
    IndexOverflow,
    #[error("the sampler's clock ticked at an instant before its previous tick")]
    ClockBackwards,
}

/// What the sampler last read of each life.
#[derive(Debug, Default)]
struct Readings {
    usage: ProcessUsageFold,
    blockers: HashMap<ProcessIdentity, ProcessBlockedOn>,
    /// Lives read after they exited whose exit is not observed yet, with the usage their records
    /// show until it is: what was read before the exit, `None` when nothing was.
    awaiting_exit: HashMap<ProcessIdentity, Option<ProcessUsage>>,
}

impl LifeReadings for Readings {
    fn usage(&self, process: ProcessIdentity) -> Option<ProcessUsage> {
        match self.awaiting_exit.get(&process) {
            Some(before_exit) => *before_exit,
            None => self.usage.usage(process),
        }
    }

    fn blocked_on(&self, process: ProcessIdentity) -> Option<ProcessBlockedOn> {
        self.blockers.get(&process).cloned()
    }
}

/// Whether `process` is live: the tree saw no exit of it, no later life took its pid, and no
/// read found it exited.
fn live(tree: &ProcessTreeFold, readings: &Readings, process: ProcessIdentity) -> bool {
    tree.live(process.pid) == Some(process) && !readings.awaiting_exit.contains_key(&process)
}

#[derive(Debug)]
pub struct JobProcessFold {
    tree: ProcessTreeFold,
    readings: Readings,
    last_tick: Instant,
    /// The tick as of which a record last restated each retained life's usage, by its index:
    /// its heartbeat is due [`PROCESS_HEARTBEAT`] later.
    restated: Vec<Instant>,
    /// Each chunk of [`PUBLISHED_CHUNK`] lives, by birth order, as publications share it.
    published: Vec<PublishedChunk>,
    /// How many lives the last attribution summary read, for the tests that count them.
    #[cfg(all(test, target_os = "macos"))]
    attribution_read: usize,
}

/// Lives per chunk of a [`PublishedTree`].
pub const PUBLISHED_CHUNK: usize = 256;

/// One chunk of lives as the fold keeps it for publication: how many of its lives are not final
/// yet, counted as lives are born and end so no publication rescans a final life to learn it, and
/// once the chunk is full and none is left, its records and its attribution part.
#[derive(Debug, Default)]
struct PublishedChunk {
    unfinal: usize,
    records: Option<Arc<[JobProcessSample]>>,
    #[cfg(target_os = "macos")]
    attribution: Option<super::job_attribution::ObservedPart<usize>>,
}

/// A job's tree as one publication holds it: every record, in birth-observation order, in chunks
/// of [`PUBLISHED_CHUNK`]. A full chunk whose lives were all final when it was built is the same
/// allocation in every publication since; every other chunk is built for this publication. A
/// record is final once its life's exit is folded with what was read after it: nothing about a
/// process follows its exit (07_api.md, "Process-tree observations").
#[derive(Clone, Debug)]
pub struct PublishedTree {
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    pub coverage: ProcessCoverage,
    chunks: Arc<[Arc<[JobProcessSample]>]>,
    built: usize,
}

impl PublishedTree {
    /// The tree a reader is answered, every record copied in order. A publication nobody reads
    /// copies none.
    pub fn tree(&self) -> JobProcessTree {
        let mut processes = Vec::with_capacity(self.chunks.iter().map(|chunk| chunk.len()).sum());
        for chunk in self.chunks.iter() {
            processes.extend(chunk.iter().cloned());
        }
        JobProcessTree {
            job_id: self.job_id,
            sampled_at: self.sampled_at.clone(),
            processes,
            coverage: self.coverage,
        }
    }

    /// How many records this publication built; it shares every other one.
    pub fn built(&self) -> usize {
        self.built
    }
}

impl JobProcessFold {
    /// A fold whose sampler's clock starts at `started`. It names no owner: whoever reads a
    /// tree from it says which job the tree is.
    pub fn new(started: Instant) -> Self {
        Self {
            tree: ProcessTreeFold::default(),
            readings: Readings::default(),
            last_tick: started,
            restated: Vec::new(),
            published: Vec::new(),
            #[cfg(all(test, target_os = "macos"))]
            attribution_read: 0,
        }
    }

    /// The job's clock started at `origin`, the instant its first owned process spawned: usage
    /// windows open on it from then on ([`ProcessUsageFold::clock`]).
    pub fn clock(&mut self, origin: Instant) -> Result<(), JobProcessFoldError> {
        Ok(self.readings.usage.clock(origin)?)
    }

    /// Fold `observation` and append its process event and first coverage gap, if any. A tick
    /// appends a heartbeat for each live process due one. A refused observation appends nothing.
    pub fn apply(
        &mut self,
        observation: JobProcessObservation,
        events: &mut Vec<JobProcessEvent>,
    ) -> Result<(), JobProcessFoldError> {
        let was_complete = matches!(self.tree.coverage(), ProcessCoverage::Complete);
        let event = match observation {
            JobProcessObservation::Tree(observation) => self.observed(observation)?,
            JobProcessObservation::Usage { process, reading } => {
                self.read_usage(process, reading)?
            }
            JobProcessObservation::Blocker {
                process,
                blocked_on,
            } => self.read_blocker(process, blocked_on)?,
            JobProcessObservation::Tick { at } => return self.tick(at, events),
        };
        events.extend(event);
        if was_complete && let ProcessCoverage::Gap { reason } = self.tree.coverage() {
            events.push(JobProcessEvent::Gap { reason: *reason });
        }
        Ok(())
    }

    /// The retained lives, as the observer saw them.
    pub fn lives(&self) -> &ProcessTreeFold {
        &self.tree
    }

    /// Native control metadata is mutated only by the observer's sole writer.
    #[cfg(target_os = "macos")]
    pub(crate) fn lives_mut(&mut self) -> &mut ProcessTreeFold {
        &mut self.tree
    }

    /// The start proven by the life's first usage read, fencing every later native read.
    pub fn usage_start(
        &self,
        process: ProcessIdentity,
    ) -> Option<super::process_usage::StartStamp> {
        self.readings.usage.start(process)
    }

    /// Summarize the same identity-fenced readings that the tree and events expose. A full chunk
    /// whose lives are all final keeps its part the first time it is read, and every later summary
    /// merges that part instead of reading its lives: nothing about a life follows its exit
    /// (07_api.md, "Process-tree observations"). Its cost is one merge per kept chunk and one read
    /// per life of a chunk that can still change.
    #[cfg(target_os = "macos")]
    pub(crate) fn attribution_summary(
        &mut self,
    ) -> Result<super::job_attribution::AttributionSummary, crate::api::resources::ResourceUnitError>
    {
        use super::job_attribution::ObservedPart;
        let lives = self.restated.len();
        #[cfg(test)]
        let mut read = 0;
        let mut whole = ObservedPart::none()?;
        let mut parts = || -> Result<(), crate::api::resources::ResourceUnitError> {
            for (chunk, published) in self.published.iter_mut().enumerate() {
                if let Some(part) = &published.attribution {
                    whole.merge(part)?;
                    continue;
                }
                let start = chunk * PUBLISHED_CHUNK;
                let end = (start + PUBLISHED_CHUNK).min(lives);
                let mut part = ObservedPart::none()?;
                for index in start..end {
                    let node = self
                        .tree
                        .node_at(index)
                        .expect("a chunk covers lives the tree retains");
                    part.add(
                        node.identity,
                        node.image_known.then_some(index),
                        self.readings.usage(node.identity),
                    )?;
                }
                #[cfg(test)]
                {
                    read += end - start;
                }
                if end - start == PUBLISHED_CHUNK && published.unfinal == 0 {
                    published.attribution = Some(part);
                }
                whole.merge(&part)?;
            }
            Ok(())
        };
        if parts().is_err() {
            // The refusal a summary answers is the first one in birth order, which only reading
            // every life in that order names; parts summed in another grouping may refuse elsewhere.
            return super::job_attribution::AttributionSummary::from_observed(
                self.tree
                    .nodes()
                    .map(|node| super::job_attribution::ObservedLife {
                        identity: node.identity,
                        image: node.image_known.then_some(node.image),
                        usage: self.readings.usage(node.identity),
                    }),
            );
        }
        let tree = &self.tree;
        #[cfg(test)]
        {
            self.attribution_read = read;
        }
        Ok(whole.summary(|index| {
            tree.node_at(index)
                .expect("the winner is a life the tree retains")
                .image
        }))
    }

    #[cfg(test)]
    /// Every record, as last observed and read, as job `job_id`'s tree, each one built anew.
    pub fn tree(&self, job_id: JobId, sampled_at: UtcTimestamp) -> JobProcessTree {
        self.tree.tree(job_id, sampled_at, &self.readings)
    }

    /// Every record, as last observed and read, as job `job_id`'s published tree: the records of
    /// each chunk that can still change are built, and every final chunk is shared. Its cost is
    /// one reference per chunk and one record per life of a chunk that is not final.
    pub fn published(&mut self, job_id: JobId, sampled_at: UtcTimestamp) -> PublishedTree {
        let lives = self.restated.len();
        let mut built = 0;
        let chunks = self
            .published
            .iter_mut()
            .enumerate()
            .map(|(chunk, published)| {
                if let Some(records) = &published.records {
                    return Arc::clone(records);
                }
                let start = chunk * PUBLISHED_CHUNK;
                let end = (start + PUBLISHED_CHUNK).min(lives);
                let records: Arc<[JobProcessSample]> = (start..end)
                    .map(|index| self.tree.sample(index, &self.readings))
                    .collect();
                built += end - start;
                if end - start == PUBLISHED_CHUNK && published.unfinal == 0 {
                    published.records = Some(Arc::clone(&records));
                }
                records
            })
            .collect();
        PublishedTree {
            job_id,
            sampled_at,
            coverage: *self.tree.coverage(),
            chunks,
            built,
        }
    }

    fn observed(
        &mut self,
        observation: ProcessObservation,
    ) -> Result<Option<JobProcessEvent>, JobProcessFoldError> {
        let named = match &observation {
            ProcessObservation::Root { process, .. }
            | ProcessObservation::Forked { process, .. } => Some((*process, Observed::Born)),
            ProcessObservation::Exec { process, .. } => Some((*process, Observed::Exec)),
            ProcessObservation::Exited { process, .. } => Some((*process, Observed::Exited)),
            ProcessObservation::ImageUnread { .. } | ProcessObservation::Lost(_) => None,
        };
        self.tree.apply(observation)?;
        let Some((process, observed)) = named else {
            return Ok(None);
        };
        // A life whose birth was never observed is the tree's gap, not an event.
        let Some(index) = self.tree.index(process) else {
            return Ok(None);
        };
        match observed {
            // The tree retained the newborn at the next index.
            Observed::Born => {
                if index.is_multiple_of(PUBLISHED_CHUNK) {
                    self.published.push(PublishedChunk::default());
                }
                self.published[index / PUBLISHED_CHUNK].unfinal += 1;
                self.restated.push(self.last_tick);
            }
            Observed::Exec => self.restated[index] = self.last_tick,
            Observed::Exited => {
                let unread_before = self.readings.usage.gap();
                self.readings
                    .usage
                    .apply(UsageObservation::Exited { process })?;
                if unread_before.is_none()
                    && let Some(gap) = self.readings.usage.gap()
                {
                    self.tree.apply(ProcessObservation::Lost(gap))?;
                }
                // The exit carries what was read after it.
                self.readings.awaiting_exit.remove(&process);
                // Final: nothing about the life can follow its exit.
                self.published[index / PUBLISHED_CHUNK].unfinal -= 1;
            }
        }
        let process = self.tree.sample(index, &self.readings);
        let index = event_index(index)?;
        Ok(Some(match observed {
            Observed::Born => JobProcessEvent::Born { index, process },
            Observed::Exec => JobProcessEvent::Exec { index, process },
            Observed::Exited => JobProcessEvent::Exited { index, process },
        }))
    }

    fn read_usage(
        &mut self,
        process: ProcessIdentity,
        reading: UsageReading,
    ) -> Result<Option<JobProcessEvent>, JobProcessFoldError> {
        let index = self.retained(process)?;
        let before = self.readings.usage.usage(process);
        self.readings
            .usage
            .apply(UsageObservation::Read { process, reading })?;
        if reading.exited {
            // Withheld until the exit carries it. A second read after the exit keeps what the
            // first one withheld.
            self.readings.awaiting_exit.entry(process).or_insert(before);
            return Ok(None);
        }
        let Some(after) = self.readings.usage.usage(process) else {
            return Ok(None);
        };
        let changed = before.is_none_or(|before| {
            before.busy != after.busy || rss_step(before.rss_bytes) != rss_step(after.rss_bytes)
        });
        if !changed {
            return Ok(None);
        }
        self.restated[index] = self.last_tick;
        Ok(Some(JobProcessEvent::Changed(JobProcessDelta::Usage {
            index: event_index(index)?,
            usage: after,
        })))
    }

    fn read_blocker(
        &mut self,
        process: ProcessIdentity,
        blocked_on: Option<ProcessBlockedOn>,
    ) -> Result<Option<JobProcessEvent>, JobProcessFoldError> {
        let index = self.retained(process)?;
        if !live(&self.tree, &self.readings, process) {
            return Err(JobProcessFoldError::BlockerNotRunning { pid: process.pid });
        }
        let change = match (self.readings.blockers.get(&process), blocked_on) {
            (None, None) => return Ok(None),
            (Some(last), Some(observed)) if *last == observed => return Ok(None),
            (_, Some(observed)) => {
                self.readings.blockers.insert(process, observed.clone());
                BlockerChange::Set(observed)
            }
            (Some(_), None) => {
                self.readings.blockers.remove(&process);
                BlockerChange::Clear
            }
        };
        Ok(Some(JobProcessEvent::Changed(JobProcessDelta::Blocker {
            index: event_index(index)?,
            blocked_on: change,
        })))
    }

    fn tick(
        &mut self,
        at: Instant,
        events: &mut Vec<JobProcessEvent>,
    ) -> Result<(), JobProcessFoldError> {
        if at < self.last_tick {
            return Err(JobProcessFoldError::ClockBackwards);
        }
        self.last_tick = at;
        for (index, (node, restated)) in self.tree.nodes().zip(&mut self.restated).enumerate() {
            if at < *restated + PROCESS_HEARTBEAT
                || !live(&self.tree, &self.readings, node.identity)
            {
                continue;
            }
            *restated = at;
            events.push(JobProcessEvent::Heartbeat {
                index: event_index(index)?,
                process: self.tree.sample(index, &self.readings),
            });
        }
        Ok(())
    }

    fn retained(&self, process: ProcessIdentity) -> Result<usize, JobProcessFoldError> {
        self.tree
            .index(process)
            .ok_or(JobProcessFoldError::NotRetained { pid: process.pid })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Observed {
    Born,
    Exec,
    Exited,
}

fn event_index(index: usize) -> Result<u32, JobProcessFoldError> {
    u32::try_from(index).map_err(|_| JobProcessFoldError::IndexOverflow)
}

/// Which power-of-two step `bytes` lies in: `[2^(n-1), 2^n)` is step `n`, and nothing resident
/// is step 0. Moving within a step is not a change; crossing into another one is.
fn rss_step(bytes: ResidentBytes) -> u32 {
    u64::BITS - bytes.get().leading_zeros()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::time::Duration;

    use super::*;
    use crate::api::dto::{CommandArg, ExitStatus};
    use crate::api::process::{
        JobProcessSample, LockHolder, ProcessCoverage, ProcessCoverageGap, ProcessStorageIo,
    };
    use crate::api::resources::{CpuMicros, StorageIoBytes};
    use crate::runtime::process_tree::{BirthToken, ProcessImage};
    use crate::runtime::process_usage::StartStamp;

    fn utc(second: u32) -> UtcTimestamp {
        UtcTimestamp::new(format!(
            "2026-10-07T00:{:02}:{:02}Z",
            second / 60,
            second % 60
        ))
        .expect("a timestamp")
    }

    fn id(pid: u32, birth: u64) -> ProcessIdentity {
        ProcessIdentity {
            pid,
            birth: BirthToken(birth),
        }
    }

    fn image(program: &str) -> ProcessImage {
        ProcessImage {
            program: program.into(),
            argv: vec![CommandArg::new(OsString::from_vec(
                program.as_bytes().to_vec(),
            ))]
            .into(),
            truncated: false,
        }
    }

    fn job() -> JobId {
        JobId::new(9).expect("a job id")
    }

    /// A fold, its start, and every event it emitted, in order.
    struct Run {
        fold: JobProcessFold,
        start: Instant,
        events: Vec<JobProcessEvent>,
    }

    impl Run {
        fn new() -> Self {
            let start = Instant::now();
            let mut fold = JobProcessFold::new(start);
            fold.clock(start).expect("the job's clock starts once");
            Self {
                fold,
                start,
                events: Vec::new(),
            }
        }

        fn at(&self, millis: u64) -> Instant {
            self.start + Duration::from_millis(millis)
        }

        /// Apply `observation`, keep the events it caused, and return them.
        fn emit(&mut self, observation: JobProcessObservation) -> Vec<JobProcessEvent> {
            let mut emitted = Vec::new();
            self.fold
                .apply(observation, &mut emitted)
                .expect("a consistent observation");
            self.events.extend(emitted.iter().cloned());
            emitted
        }

        /// Apply an observation other than a tick and return its process event, if any.
        fn apply(&mut self, observation: JobProcessObservation) -> Option<JobProcessEvent> {
            let emitted = self.emit(observation);
            let mut process_events = emitted
                .into_iter()
                .filter(|event| !matches!(event, JobProcessEvent::Gap { .. }));
            let event = process_events.next();
            assert!(
                process_events.next().is_none(),
                "one observation, one process event"
            );
            event
        }

        /// Apply an observation the fold must refuse, and return why.
        fn refused(&mut self, observation: JobProcessObservation) -> JobProcessFoldError {
            let mut emitted = Vec::new();
            let error = self
                .fold
                .apply(observation, &mut emitted)
                .expect_err("an inconsistent observation");
            assert_eq!(emitted, [], "a refused observation emits nothing");
            error
        }

        fn root(&mut self, process: ProcessIdentity) -> Option<JobProcessEvent> {
            self.apply(JobProcessObservation::Tree(ProcessObservation::Root {
                process,
                ppid: 1,
                at: utc(0),
                image: image("/bin/sh"),
            }))
        }

        fn fork(&mut self, process: ProcessIdentity, parent: ProcessIdentity) {
            self.apply(JobProcessObservation::Tree(ProcessObservation::Forked {
                process,
                parent,
                at: utc(1),
            }))
            .expect("a birth event");
        }

        fn read(
            &mut self,
            process: ProcessIdentity,
            reading: UsageReading,
        ) -> Option<JobProcessEvent> {
            self.apply(JobProcessObservation::Usage { process, reading })
        }

        fn blocked(
            &mut self,
            process: ProcessIdentity,
            blocked_on: Option<ProcessBlockedOn>,
        ) -> Option<JobProcessEvent> {
            self.apply(JobProcessObservation::Blocker {
                process,
                blocked_on,
            })
        }

        fn exit(&mut self, process: ProcessIdentity, second: u32) -> Option<JobProcessEvent> {
            self.apply(JobProcessObservation::Tree(ProcessObservation::Exited {
                process,
                status: Some(ExitStatus::Exited { code: 0 }),
                at: utc(second),
            }))
        }

        fn tick(&mut self, millis: u64) -> Vec<JobProcessEvent> {
            let at = self.at(millis);
            self.emit(JobProcessObservation::Tick { at })
        }

        /// The tree a consumer rebuilds from the events alone.
        fn replayed(&self) -> Vec<JobProcessSample> {
            let mut processes: Vec<JobProcessSample> = Vec::new();
            for event in &self.events {
                match event {
                    JobProcessEvent::Born { index, process } => {
                        assert_eq!(*index as usize, processes.len(), "births arrive in order");
                        processes.push(process.clone());
                    }
                    JobProcessEvent::Exec { index, process }
                    | JobProcessEvent::Heartbeat { index, process }
                    | JobProcessEvent::Exited { index, process } => {
                        processes[*index as usize] = process.clone();
                    }
                    JobProcessEvent::Changed(delta) => {
                        delta.apply(&mut processes[delta.index() as usize]);
                    }
                    JobProcessEvent::Gap { .. } => {}
                }
            }
            processes
        }

        fn assert_replay_matches(&self) {
            assert_eq!(self.replayed(), self.fold.tree(job(), utc(0)).processes);
        }
    }

    fn reading(at: Instant, cpu_us: u64, resident: u64) -> UsageReading {
        UsageReading {
            started: StartStamp(3),
            at,
            cpu_user: CpuMicros::new(cpu_us).expect("a CPU time"),
            cpu_sys: CpuMicros::new(0).expect("a CPU time"),
            resident: ResidentBytes::new(resident).expect("a resident size"),
            io: ProcessStorageIo::Read {
                read_bytes: StorageIoBytes::new(0).expect("bytes"),
                write_bytes: StorageIoBytes::new(0).expect("bytes"),
            },
            exited: false,
        }
    }

    fn lock(path: &str, holder: u32) -> ProcessBlockedOn {
        ProcessBlockedOn::Lock {
            path: path.to_owned(),
            holder: Some(LockHolder {
                pid: holder,
                job: None,
                program: None,
            }),
        }
    }

    fn change(event: Option<JobProcessEvent>) -> JobProcessDelta {
        match event {
            Some(JobProcessEvent::Changed(delta)) => delta,
            other => panic!("expected a change, got {other:?}"),
        }
    }

    fn usage_change(event: Option<JobProcessEvent>) -> ProcessUsage {
        match change(event) {
            JobProcessDelta::Usage { usage, .. } => usage,
            other => panic!("expected a usage change, got {other:?}"),
        }
    }

    /// The index of each heartbeat in `events`, which holds nothing else.
    fn heartbeats(events: &[JobProcessEvent]) -> Vec<u32> {
        events
            .iter()
            .map(|event| match event {
                JobProcessEvent::Heartbeat { index, .. } => *index,
                other => panic!("a tick emits heartbeats only, got {other:?}"),
            })
            .collect()
    }

    /// Every record of one image shares it: the tree's, each event of a life that runs it, and
    /// a forked child's birth until its own exec. A record that copied an argv would keep one
    /// copy per record for as long as its job's record is kept.
    #[test]
    fn every_record_of_an_image_shares_its_argv() {
        let mut run = Run::new();
        let (shell, compiler) = (id(100, 1), id(101, 2));
        run.root(shell).expect("a birth event");
        run.fork(compiler, shell);
        run.apply(JobProcessObservation::Tree(ProcessObservation::Exec {
            process: compiler,
            image: image("/usr/bin/cc"),
        }))
        .expect("an exec event");
        run.exit(compiler, 2).expect("an exit event");
        let tree = run.fold.tree(job(), utc(3));
        let shares = |record: usize, process: &JobProcessSample| {
            let record = &tree.processes[record];
            std::sync::Arc::ptr_eq(&process.program, &record.program)
                && std::sync::Arc::ptr_eq(&process.argv, &record.argv)
        };
        // The exit with no usage read after it is also a coverage gap, which names no record.
        let records: Vec<_> = run
            .events
            .iter()
            .filter(|event| !matches!(event, JobProcessEvent::Gap { .. }))
            .collect();
        assert_eq!(records.len(), 4, "{records:?}");
        for event in records {
            let shared = match event {
                // The child was born running its parent's image.
                JobProcessEvent::Born { index: 1, process } => shares(0, process),
                JobProcessEvent::Born { index, process }
                | JobProcessEvent::Exec { index, process }
                | JobProcessEvent::Exited { index, process } => {
                    shares(usize::try_from(*index).unwrap(), process)
                }
                other => panic!("no other event was observed: {other:?}"),
            };
            assert!(shared, "a record copied its image: {event:?}");
        }
    }

    #[test]
    fn a_cleared_lock_leaves_no_path_or_holder_and_replays_alike() {
        let mut run = Run::new();
        let shell = id(100, 1);
        run.root(shell);

        let set = change(run.blocked(shell, Some(lock("/ws/.git/index.lock", 4242))));
        assert_eq!(
            set,
            JobProcessDelta::Blocker {
                index: 0,
                blocked_on: BlockerChange::Set(lock("/ws/.git/index.lock", 4242)),
            },
            "the lock alone changed"
        );
        assert_eq!(
            run.blocked(shell, Some(lock("/ws/.git/index.lock", 4242))),
            None,
            "the same lock observed again is not a change"
        );

        let released = change(run.blocked(shell, Some(ProcessBlockedOn::None)));
        let mut record = run.fold.tree(job(), utc(0)).processes[0].clone();
        record.blocked_on = Some(lock("/ws/.git/index.lock", 4242));
        released.apply(&mut record);
        assert_eq!(
            record.blocked_on,
            Some(ProcessBlockedOn::None),
            "the lock's path and holder are gone with it"
        );
        run.assert_replay_matches();

        // Evidence lost altogether is a CLEAR, not an omission that keeps `none`.
        let cleared = change(run.blocked(shell, None));
        assert_eq!(
            cleared,
            JobProcessDelta::Blocker {
                index: 0,
                blocked_on: BlockerChange::Clear,
            }
        );
        assert_eq!(run.fold.tree(job(), utc(0)).processes[0].blocked_on, None);
        assert_eq!(run.blocked(shell, None), None, "absent stays absent");
        run.assert_replay_matches();

        // Folding the same observations again answers the same events.
        let mut again = Run::new();
        again.root(shell);
        again.blocked(shell, Some(lock("/ws/.git/index.lock", 4242)));
        again.blocked(shell, Some(lock("/ws/.git/index.lock", 4242)));
        again.blocked(shell, Some(ProcessBlockedOn::None));
        again.blocked(shell, None);
        again.blocked(shell, None);
        assert_eq!(again.events, run.events);
    }

    #[test]
    fn a_busy_flip_is_one_change_and_idle_samples_are_none() {
        let mut run = Run::new();
        let worker = id(200, 2);
        run.root(worker);

        let first = usage_change(run.read(worker, reading(run.at(0), 0, 1 << 20)));
        assert!(!first.busy, "the first read sets usage");
        // 500 ms of CPU over one second: busy, with resident memory unchanged.
        let busy = usage_change(run.read(worker, reading(run.at(1000), 500_000, 1 << 20)));
        assert!(busy.busy);
        // No CPU over the next second: idle, every other counter equal.
        let idle = usage_change(run.read(worker, reading(run.at(2000), 500_000, 1 << 20)));
        assert!(!idle.busy);
        assert_eq!(
            run.read(worker, reading(run.at(3000), 500_000, 1 << 20)),
            None
        );
        assert_eq!(
            run.read(worker, reading(run.at(4000), 500_000, 1 << 20)),
            None
        );
        // CPU that stays under the busy share moves counters only.
        assert_eq!(
            run.read(worker, reading(run.at(5000), 500_001, 1 << 20)),
            None
        );
        assert_eq!(run.events.len(), 4, "born, first read, busy, idle");
        let lagging = run.replayed();
        let usage = lagging[0].usage.expect("usage");
        assert_eq!(
            usage.cpu_user_us.get(),
            500_000,
            "the counter-only move was not sent"
        );
        assert!(!usage.busy);
        assert_eq!(heartbeats(&run.tick(60_000)), [0]);
        run.assert_replay_matches();
    }

    #[test]
    fn resident_memory_changes_only_when_it_crosses_a_power_of_two() {
        let mut run = Run::new();
        let worker = id(300, 3);
        run.root(worker);
        run.read(worker, reading(run.at(0), 0, 1 << 20));
        assert_eq!(
            run.read(worker, reading(run.at(1000), 0, (1 << 21) - 1)),
            None,
            "within one step"
        );
        let grown = usage_change(run.read(worker, reading(run.at(2000), 0, 1 << 21)));
        assert_eq!(grown.rss_bytes.get(), 1 << 21);
        assert_eq!(run.read(worker, reading(run.at(3000), 0, 3 << 20)), None);
        let shrunk = usage_change(run.read(worker, reading(run.at(4000), 0, (1 << 21) - 1)));
        assert_eq!(shrunk.rss_bytes.get(), (1 << 21) - 1);
        run.assert_replay_matches();
    }

    #[test]
    fn each_unchanged_live_process_has_one_heartbeat_a_minute() {
        let mut run = Run::new();
        let shell = id(400, 4);
        let child = id(401, 5);
        let sibling = id(402, 6);
        run.root(shell);
        assert_eq!(run.tick(30_000), []);
        run.fork(child, shell);
        run.fork(sibling, shell);
        assert_eq!(run.tick(59_999), []);
        match run.tick(60_000).as_slice() {
            [JobProcessEvent::Heartbeat { index: 0, process }] => assert_eq!(process.pid, 400),
            other => panic!("expected the shell's heartbeat alone, got {other:?}"),
        }
        assert_eq!(run.tick(60_001), []);
        assert_eq!(run.tick(89_999), []);
        assert_eq!(
            heartbeats(&run.tick(90_000)),
            [1, 2],
            "both children, a minute after their births, in birth order"
        );

        // A record restating the shell's usage restarts its minute.
        assert_eq!(run.tick(100_000), []);
        usage_change(run.read(shell, reading(run.at(100_000), 0, 1 << 20)));
        assert_eq!(
            run.tick(120_000),
            [],
            "the shell's usage was restated at 100 s"
        );
        assert_eq!(heartbeats(&run.tick(150_000)), [1, 2]);
        assert_eq!(heartbeats(&run.tick(160_000)), [0]);

        // A blocker change restates no usage: the shell stays due a minute after 160 s.
        assert_eq!(run.tick(170_000), []);
        change(run.blocked(shell, Some(ProcessBlockedOn::Child)));
        assert_eq!(heartbeats(&run.tick(210_000)), [1, 2]);
        assert_eq!(heartbeats(&run.tick(220_000)), [0]);

        assert_eq!(
            run.refused(JobProcessObservation::Tick {
                at: run.at(100_000)
            }),
            JobProcessFoldError::ClockBackwards
        );
        run.exit(child, 230);
        assert_eq!(heartbeats(&run.tick(280_000)), [0, 2]);
        run.exit(sibling, 290);
        run.exit(shell, 300);
        assert_eq!(run.tick(500_000), [], "nothing lives to restate");
        run.assert_replay_matches();
    }

    #[test]
    fn final_usage_is_emitted_once_by_the_exit_and_never_reopened() {
        let mut run = Run::new();
        let shell = id(500, 5);
        let child = id(501, 6);
        run.root(shell);
        run.fork(child, shell);
        run.read(child, reading(run.at(0), 0, 1 << 20));
        let mut after_exit = reading(run.at(1000), 40_000, 0);
        after_exit.exited = true;
        assert_eq!(
            run.read(child, after_exit),
            None,
            "a read after the exit waits for the exit event"
        );
        match run.exit(child, 2) {
            Some(JobProcessEvent::Exited { index, process }) => {
                assert_eq!(index, 1);
                let usage = process.usage.expect("final usage");
                assert_eq!(usage.cpu_user_us.get(), 40_000);
                assert_eq!(usage.rss_bytes.get(), 0);
                assert!(process.exit.is_some());
            }
            other => panic!("expected the exit, got {other:?}"),
        }
        assert_eq!(
            run.fold.tree(job(), utc(3)).coverage,
            ProcessCoverage::Complete,
            "the final usage was read"
        );

        let mut late = reading(run.at(2000), 40_000, 0);
        late.exited = true;
        assert_eq!(
            run.refused(JobProcessObservation::Usage {
                process: child,
                reading: late,
            }),
            JobProcessFoldError::Usage(UsageFoldError::AfterExit { pid: 501 })
        );
        assert_eq!(
            run.refused(JobProcessObservation::Blocker {
                process: child,
                blocked_on: Some(ProcessBlockedOn::None),
            }),
            JobProcessFoldError::BlockerNotRunning { pid: 501 }
        );
        assert_eq!(
            run.refused(JobProcessObservation::Tree(ProcessObservation::Exited {
                process: child,
                status: Some(ExitStatus::Exited { code: 0 }),
                at: utc(4),
            })),
            JobProcessFoldError::Tree(ProcessFoldError::AfterExit { pid: 501 })
        );
        run.assert_replay_matches();
    }

    #[test]
    fn an_exit_without_a_final_read_is_a_gap_in_the_tree() {
        let mut run = Run::new();
        let shell = id(600, 7);
        run.root(shell);
        run.read(shell, reading(run.at(0), 10, 1 << 20));
        match run.exit(shell, 1) {
            Some(JobProcessEvent::Exited { process, .. }) => {
                assert_eq!(
                    process
                        .usage
                        .expect("the last usage observed")
                        .cpu_user_us
                        .get(),
                    10
                );
            }
            other => panic!("expected the exit, got {other:?}"),
        }
        assert_eq!(
            run.fold.tree(job(), utc(2)).coverage,
            ProcessCoverage::Gap {
                reason: ProcessCoverageGap::UnreadFinalUsage { pid: 600 }
            }
        );
    }

    #[test]
    fn a_sample_of_a_life_the_tree_never_held_is_refused() {
        let mut run = Run::new();
        assert_eq!(
            run.refused(JobProcessObservation::Usage {
                process: id(700, 8),
                reading: reading(run.at(0), 0, 0),
            }),
            JobProcessFoldError::NotRetained { pid: 700 }
        );
        // A birth whose parent was never seen is the tree's gap: no event names it.
        assert_eq!(
            run.apply(JobProcessObservation::Tree(ProcessObservation::Forked {
                process: id(701, 9),
                parent: id(702, 10),
                at: utc(0),
            })),
            None
        );
    }

    #[test]
    fn a_read_after_the_exit_is_withheld_from_every_record_until_the_exit() {
        let mut run = Run::new();
        let shell = id(800, 11);
        let child = id(801, 12);
        run.root(shell);
        run.fork(child, shell);
        usage_change(run.read(child, reading(run.at(0), 0, 1 << 20)));
        let mut after_exit = reading(run.at(1000), 40_000, 0);
        after_exit.exited = true;
        assert_eq!(run.read(child, after_exit), None);
        let shown = |run: &Run| {
            run.fold.tree(job(), utc(1)).processes[1]
                .usage
                .map(|usage| usage.cpu_user_us.get())
        };
        assert_eq!(
            shown(&run),
            Some(0),
            "the snapshot shows the read before the exit"
        );

        // It no longer runs: its blocker cannot be read, and no heartbeat restates it.
        assert_eq!(
            run.refused(JobProcessObservation::Blocker {
                process: child,
                blocked_on: Some(ProcessBlockedOn::None),
            }),
            JobProcessFoldError::BlockerNotRunning { pid: 801 }
        );
        let mut again = reading(run.at(1500), 40_000, 0);
        again.exited = true;
        assert_eq!(run.read(child, again), None);
        assert_eq!(
            shown(&run),
            Some(0),
            "a second read after the exit withholds too"
        );
        assert_eq!(heartbeats(&run.tick(60_000)), [0], "the shell's alone");

        // An exec the observer reports late shows the usage read before the exit.
        match run.apply(JobProcessObservation::Tree(ProcessObservation::Exec {
            process: child,
            image: image("/bin/true"),
        })) {
            Some(JobProcessEvent::Exec { index: 1, process }) => {
                assert_eq!(process.usage.map(|usage| usage.cpu_user_us.get()), Some(0))
            }
            other => panic!("expected the exec, got {other:?}"),
        }
        assert_eq!(heartbeats(&run.tick(120_000)), [0]);

        match run.exit(child, 121) {
            Some(JobProcessEvent::Exited { index: 1, process }) => assert_eq!(
                process.usage.map(|usage| usage.cpu_user_us.get()),
                Some(40_000),
                "the exit carries the final usage"
            ),
            other => panic!("expected the exit, got {other:?}"),
        }
        assert_eq!(
            run.fold.tree(job(), utc(122)).coverage,
            ProcessCoverage::Complete
        );
        run.assert_replay_matches();
    }

    #[test]
    fn the_first_coverage_gap_is_an_event_even_without_a_process_record() {
        let mut run = Run::new();
        let reason = ProcessCoverageGap::UncountedFork { pid: 800 };
        let events = run.emit(JobProcessObservation::Tree(ProcessObservation::Lost(
            reason,
        )));
        assert_eq!(
            serde_json::to_value(&events).expect("events encode"),
            serde_json::json!([{ "kind": "gap", "reason": { "kind": "uncountedFork", "pid": 800 } }])
        );
        assert_eq!(
            run.fold.tree(job(), utc(0)).coverage,
            ProcessCoverage::Gap { reason }
        );
        for later in [reason, ProcessCoverageGap::EventsLost] {
            assert_eq!(
                run.emit(JobProcessObservation::Tree(ProcessObservation::Lost(later))),
                []
            );
        }
        assert_eq!(
            run.fold.tree(job(), utc(1)).coverage,
            ProcessCoverage::Gap { reason }
        );
    }

    #[test]
    fn implicit_gaps_are_emitted_once_beside_their_process_transition() {
        let root = id(800, 11);
        let mut reused = Run::new();
        reused.root(root);
        let events = reused.emit(JobProcessObservation::Tree(ProcessObservation::Root {
            process: id(800, 12),
            ppid: 1,
            at: utc(1),
            image: image("/bin/sh"),
        }));
        assert!(matches!(
            &events[..],
            [
                JobProcessEvent::Born { index: 1, .. },
                JobProcessEvent::Gap {
                    reason: ProcessCoverageGap::UnobservedExit { pid: 800 }
                }
            ]
        ));
        reused.assert_replay_matches();

        let mut unread = Run::new();
        unread.root(root);
        let events = unread.emit(JobProcessObservation::Tree(ProcessObservation::Exited {
            process: root,
            status: Some(ExitStatus::Exited { code: 0 }),
            at: utc(1),
        }));
        assert!(matches!(
            &events[..],
            [
                JobProcessEvent::Exited { index: 0, .. },
                JobProcessEvent::Gap {
                    reason: ProcessCoverageGap::UnreadFinalUsage { pid: 800 }
                }
            ]
        ));
        unread.assert_replay_matches();

        let mut unknown = Run::new();
        let reason = ProcessCoverageGap::UnobservedBirth { pid: 800 };
        assert_eq!(
            unknown.emit(JobProcessObservation::Tree(ProcessObservation::Exec {
                process: root,
                image: image("/bin/sh"),
            })),
            [JobProcessEvent::Gap { reason }]
        );
        assert_eq!(
            unknown.fold.tree(job(), utc(1)).coverage,
            ProcessCoverage::Gap { reason }
        );
    }

    #[test]
    fn an_empty_delta_cannot_be_decoded() {
        for refused in [
            r#"{"index":3}"#,
            r#"{"index":3,"blockedOn":"clear","extra":1}"#,
            // One observation changes one field: a usage and a blocker are two changes.
            r#"{"index":3,"usage":{"cpuUserUs":1,"cpuSysUs":0,"busy":false,"rssBytes":0,"rssPeakBytes":0,"io":{"kind":"read","readBytes":0,"writeBytes":0}},"blockedOn":"clear"}"#,
        ] {
            assert!(
                serde_json::from_str::<JobProcessDelta>(refused).is_err(),
                "{refused} decoded"
            );
        }
        assert!(
            serde_json::from_str::<JobProcessEvent>(r#"{"kind":"changed","index":3}"#).is_err()
        );

        let cleared = JobProcessEvent::Changed(JobProcessDelta::Blocker {
            index: 3,
            blocked_on: BlockerChange::Clear,
        });
        let json = serde_json::to_value(&cleared).expect("an event encodes");
        assert_eq!(
            json,
            serde_json::json!({ "kind": "changed", "index": 3, "blockedOn": "clear" })
        );
        assert_eq!(
            serde_json::from_value::<JobProcessEvent>(json).expect("an event decodes"),
            cleared
        );

        let set = JobProcessEvent::Changed(JobProcessDelta::Blocker {
            index: 0,
            blocked_on: BlockerChange::Set(lock("/ws/lock", 7)),
        });
        let json = serde_json::to_value(&set).expect("an event encodes");
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "changed",
                "index": 0,
                "blockedOn": { "set": { "kind": "lock", "path": "/ws/lock", "holder": { "pid": 7 } } }
            })
        );
        assert_eq!(
            serde_json::from_value::<JobProcessEvent>(json).expect("an event decodes"),
            set
        );

        let usage = JobProcessEvent::Changed(JobProcessDelta::Usage {
            index: 1,
            usage: ProcessUsage {
                cpu_user_us: CpuMicros::new(1).expect("a CPU time"),
                cpu_sys_us: CpuMicros::new(0).expect("a CPU time"),
                busy: false,
                cpu_window: None,
                rss_bytes: ResidentBytes::new(0).expect("a resident size"),
                rss_peak_bytes: ResidentBytes::new(0).expect("a resident size"),
                io: ProcessStorageIo::Read {
                    read_bytes: StorageIoBytes::new(0).expect("bytes"),
                    write_bytes: StorageIoBytes::new(0).expect("bytes"),
                },
            },
        });
        let json = serde_json::to_string(&usage).expect("an event encodes");
        assert_eq!(
            serde_json::from_str::<JobProcessEvent>(&json).expect("an event decodes"),
            usage
        );
    }

    /// The fold's publication now, checked against the tree built anew from the same fold, and its
    /// attribution against the one read anew from every life.
    fn published(run: &mut Run, second: u32) -> PublishedTree {
        let published = run.fold.published(job(), utc(second));
        assert_eq!(
            published.tree(),
            run.fold.tree(job(), utc(second)),
            "a publication answers the fold's own tree"
        );
        #[cfg(target_os = "macos")]
        attributed(run);
        published
    }

    /// The fold's attribution summary now, checked against the summary read anew from every life
    /// in birth order, and how many lives it read.
    #[cfg(target_os = "macos")]
    fn attributed(run: &mut Run) -> usize {
        use crate::runtime::job_attribution::{AttributionSummary, ObservedLife};
        let summary = run.fold.attribution_summary().expect("a summary");
        let fold = &run.fold;
        let anew = AttributionSummary::from_observed(fold.tree.nodes().map(|node| ObservedLife {
            identity: node.identity,
            image: node.image_known.then_some(node.image),
            usage: fold.readings.usage(node.identity),
        }))
        .expect("a summary read anew");
        assert_eq!(
            summary.observed(),
            anew.observed(),
            "a summary from kept parts answers the summary of every life"
        );
        run.fold.attribution_read
    }

    /// A publication summarized the own CPU of every life the job ever had to name its leaf: on
    /// live workspace supervisors, 67,132 lives read for each of 6 publications a second, and
    /// 43,440 for each of 88, nearly all long ended and none of those able to change again. A
    /// full chunk whose lives are all final is read once; a summary reads only the lives of the
    /// other chunks and answers the summary of every life read anew, its leaf included when the
    /// leaf is in a kept chunk.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_attribution_reads_only_the_lives_of_chunks_that_can_still_change() {
        let mut run = Run::new();
        let lives = 3 * PUBLISHED_CHUNK + 8;
        let long_lived = 2 * PUBLISHED_CHUNK + 5;
        let children = ended_lives(&mut run, lives, long_lived);
        assert_eq!(
            attributed(&mut run),
            lives,
            "the first summary reads every life"
        );
        assert_eq!(
            attributed(&mut run),
            2 * PUBLISHED_CHUNK + 8,
            "chunk 1 ended whole: its part is merged, not read"
        );

        // The long-lived life spends the most CPU, then ends: its chunk is read once more and kept,
        // and the leaf it is stays the leaf from the kept part.
        let long = children[long_lived - 1];
        run.read(long, reading(run.at(60), 1_000, 4096));
        assert_eq!(attributed(&mut run), 2 * PUBLISHED_CHUNK + 8);
        run.exit(long, 7).expect("an exit event");
        assert_eq!(attributed(&mut run), 2 * PUBLISHED_CHUNK + 8);
        assert_eq!(
            attributed(&mut run),
            PUBLISHED_CHUNK + 8,
            "only the shell's chunk and the partial tail are read"
        );
        let summary = run.fold.attribution_summary().expect("a summary");
        assert_eq!(
            summary.observed().1.map(|leaf| leaf.pid),
            Some(long.pid),
            "the leaf is the life that spent the most CPU, in a kept chunk"
        );

        // A later life of an ended life's pid joins the tail; the kept parts stay merged.
        run.fork(id(children[PUBLISHED_CHUNK].pid, 9_999), id(100, 1));
        assert_eq!(attributed(&mut run), PUBLISHED_CHUNK + 9);
    }
    /// Births under a running shell, ended in birth order but for one long-lived life, so chunk 0
    /// holds the shell, chunk 2 the long-lived one, and chunk 3 is a partial tail. Half of the
    /// ended lives carry a final usage read after their exit; the rest end unread.
    fn ended_lives(run: &mut Run, lives: usize, long_lived: usize) -> Vec<ProcessIdentity> {
        let shell = id(100, 1);
        run.root(shell).expect("a birth event");
        let children: Vec<ProcessIdentity> = (1..lives)
            .map(|index| id(1000 + index as u32, index as u64 + 1))
            .collect();
        for &child in &children {
            run.fork(child, shell);
        }
        for (offset, &child) in children.iter().enumerate() {
            if offset + 1 == long_lived {
                continue;
            }
            if offset % 2 == 0 {
                let after_exit = UsageReading {
                    exited: true,
                    ..reading(run.at(10), 5, 4096)
                };
                run.read(child, after_exit);
            }
            run.exit(child, 2).expect("an exit event");
        }
        children
    }

    /// A publication on every event batch rebuilt every record the job ever had: 48,920 per
    /// publication late in a consumer's cold gate, where nearly all of them had long ended and none of
    /// those could change again. A full chunk whose lives have all ended with what was read after
    /// their exits is built once; a publication builds only the records of the other chunks.
    #[test]
    fn a_publication_builds_only_the_records_of_chunks_that_can_still_change() {
        let mut run = Run::new();
        let lives = 3 * PUBLISHED_CHUNK + 8;
        let long_lived = 2 * PUBLISHED_CHUNK + 5;
        let children = ended_lives(&mut run, lives, long_lived);
        let shell = id(100, 1);

        let first = published(&mut run, 3);
        assert_eq!(
            first.built(),
            lives,
            "the first publication builds every record"
        );
        let first_tree = first.tree();
        let second = published(&mut run, 4);
        assert_eq!(
            second.built(),
            2 * PUBLISHED_CHUNK + 8,
            "only the chunks of the shell, of the long-lived life and the partial tail can change"
        );
        assert!(Arc::ptr_eq(&first.chunks[1], &second.chunks[1]));

        // Nothing about an ended life follows its exit: every later observation of one in the
        // kept chunk is refused, and the chunk stays the one shared allocation.
        let ended = children[PUBLISHED_CHUNK];
        run.refused(JobProcessObservation::Tree(ProcessObservation::Exec {
            process: ended,
            image: image("/usr/bin/cc"),
        }));
        run.refused(JobProcessObservation::Tree(
            ProcessObservation::ImageUnread { process: ended },
        ));
        run.refused(JobProcessObservation::Tree(ProcessObservation::Exited {
            process: ended,
            status: Some(ExitStatus::Exited { code: 0 }),
            at: utc(9),
        }));
        run.refused(JobProcessObservation::Usage {
            process: ended,
            reading: reading(run.at(50), 9, 4096),
        });
        run.refused(JobProcessObservation::Usage {
            process: ended,
            reading: UsageReading {
                exited: true,
                ..reading(run.at(51), 9, 4096)
            },
        });
        run.refused(JobProcessObservation::Blocker {
            process: ended,
            blocked_on: Some(lock("/tmp/lease", 7)),
        });
        let after_refusals = published(&mut run, 5);
        assert_eq!(after_refusals.built(), 2 * PUBLISHED_CHUNK + 8);
        assert!(Arc::ptr_eq(&first.chunks[1], &after_refusals.chunks[1]));

        // A later life takes the long-lived life's pid before its exit is seen: the earlier life
        // stays unfinal until its own late exit, which ends its chunk, not the tail's.
        let long = children[long_lived - 1];
        run.fork(id(long.pid, 88_888), shell);
        assert_eq!(published(&mut run, 6).built(), 2 * PUBLISHED_CHUNK + 9);
        run.exit(long, 7).expect("an exit event");
        assert_eq!(published(&mut run, 8).built(), 2 * PUBLISHED_CHUNK + 9);
        assert_eq!(published(&mut run, 9).built(), PUBLISHED_CHUNK + 9);

        // A later life of an ended life's pid joins the tail; the earlier life's record stays.
        run.fork(id(children[0].pid, 9_999), shell);
        assert_eq!(published(&mut run, 10).built(), PUBLISHED_CHUNK + 10);

        // A publication a reader still holds answers what it held when it was published.
        assert_eq!(first.tree(), first_tree);
        assert_eq!(first.tree().processes[long_lived].exit, None);
        assert_eq!(first.tree().processes.len(), lives);
    }

    /// Every kind of observation, each followed by a publication that must answer the tree built
    /// anew: execs and usage reads of running lives in shared and unshared chunks, blockers set
    /// and cleared, a read after an exit withheld until the exit, an exit with no such read.
    #[test]
    fn every_publication_answers_the_tree_built_anew() {
        let mut run = Run::new();
        let lives = 2 * PUBLISHED_CHUNK + 3;
        let long_lived = PUBLISHED_CHUNK + 1;
        let children = ended_lives(&mut run, lives, long_lived);
        let shell = id(100, 1);
        let survivor = children[long_lived - 1];
        let mut second = 3;
        let mut step = |run: &mut Run| {
            second += 1;
            published(run, second);
        };
        step(&mut run);
        run.read(shell, reading(run.at(20), 7, 1 << 20));
        step(&mut run);
        run.read(survivor, reading(run.at(30), 9, 1 << 22));
        step(&mut run);
        run.blocked(survivor, Some(lock("/tmp/lease", 7)));
        step(&mut run);
        run.apply(JobProcessObservation::Tree(ProcessObservation::Exec {
            process: survivor,
            image: image("/usr/bin/cc"),
        }))
        .expect("an exec event");
        step(&mut run);
        run.blocked(survivor, None);
        step(&mut run);
        let after_exit = UsageReading {
            exited: true,
            ..reading(run.at(40), 11, 1 << 22)
        };
        run.read(survivor, after_exit);
        step(&mut run);
        run.exit(survivor, 9).expect("an exit event");
        step(&mut run);
        let late = id(5000, 77_777);
        run.fork(late, shell);
        step(&mut run);
        run.exit(late, 10).expect("an exit event");
        step(&mut run);
    }

    /// Readers load publications while the writer folds and publishes with kept chunks shared:
    /// each loaded publication answers exactly the tree the writer built anew beside it.
    #[test]
    fn a_reader_sees_one_whole_publication_while_the_writer_publishes() {
        let mut run = Run::new();
        let lives = 3 * PUBLISHED_CHUNK + 4;
        let children = ended_lives(&mut run, lives, 3 * PUBLISHED_CHUNK + 1);
        let publish = |run: &mut Run, second: u32| {
            let published = run.fold.published(job(), utc(second));
            let anew = run.fold.tree(job(), utc(second));
            Arc::new((published, anew))
        };
        let snapshot = arc_swap::ArcSwap::new(publish(&mut run, 3));
        // The writer publishes once more before the readers start, so they read while it works.
        let started = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    started.wait();
                    for _ in 0..500 {
                        let loaded = snapshot.load();
                        assert_eq!(
                            loaded.0.tree(),
                            loaded.1,
                            "a reader saw a publication other than the tree built anew with it"
                        );
                    }
                });
            }
            for (step, &child) in children.iter().take(64).enumerate() {
                run.fork(id(child.pid, 100_000 + step as u64), id(100, 1));
                snapshot.store(publish(&mut run, 4));
                if step == 0 {
                    started.wait();
                }
            }
        });
        let last = snapshot.load();
        assert_eq!(
            last.0.built(),
            PUBLISHED_CHUNK + 4 + 64,
            "the two ended chunks were shared, not rebuilt"
        );
    }
}
