//! One job's native observation writer (07_api.md, "Process-tree observations").
//!
//! The writer alone owns the kqueue, retained fold and event journal. Readers load one immutable
//! snapshot; a demand stream reads the retained journal without resampling or moving a CPU window.
//! A user event wakes the writer for control requests, never another reader of process events.

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwap;
use tokio::sync::oneshot;

use super::job_attribution::{Attribution, AttributionSummary};
use super::job_spans::{JobSpanPublisher, job_span_thread};
use super::process_events::{KqueueObserver, ObserveError};
use super::process_spans::{ProcessSpanProjection, RetainedProcessEvent};
use super::process_tree::ProcessIdentity;
use super::supervisor::{LeafCoverage, utc_now};
use crate::api::dto::{JobId, JobInfo};
use crate::api::process::{JobProcessEvent, JobProcessTree, ProcessCoverageGap};
use crate::api::resources::{CpuTotals, ResourceUnitError};
use crate::error::{CowshedError, Result};

const DEFAULT_SAMPLE: Duration = Duration::from_secs(1);
const CONTROL_EVENT: usize = usize::MAX;

type EventReply = oneshot::Sender<Option<Result<JobProcessEvent>>>;

enum Request {
    Watch {
        pid: u32,
        reply: mpsc::SyncSender<Result<()>>,
    },
    Gap {
        reason: ProcessCoverageGap,
        reply: mpsc::SyncSender<Result<()>>,
    },
    Finish {
        reply: mpsc::SyncSender<Result<()>>,
    },
    Trace(Box<TraceWriter>),
    Open {
        every: Duration,
        reply: oneshot::Sender<Result<u64>>,
    },
    Next {
        id: u64,
        cursor: usize,
        reply: EventReply,
    },
    Close {
        id: u64,
    },
    Stop,
}

struct Subscriber {
    every: Duration,
    pending: Option<(usize, EventReply)>,
}

struct RetainedEntry {
    event: Result<JobProcessEvent>,
    identity: Option<ProcessIdentity>,
    parent: Option<ProcessIdentity>,
    at: SystemTime,
    /// The last entry of one emitted fold batch, retained for exact trace replay.
    batch_end: bool,
}

struct TraceWriter {
    publisher: JobSpanPublisher,
    projection: ProcessSpanProjection,
}

impl TraceWriter {
    fn record(&mut self, entries: &[RetainedEntry]) {
        let Self { publisher, projection } = self;
        publisher.record_process(entries.iter().filter_map(|entry| {
            entry.event.as_ref().ok().map(|event| {
                projection.project(
                    RetainedProcessEvent {
                        event,
                        identity: entry.identity,
                        parent: entry.parent,
                    },
                    entry.at,
                )
            })
        }));
    }
}

struct Snapshot {
    tree: Result<JobProcessTree>,
    attribution: std::result::Result<AttributionSummary, ResourceUnitError>,
}

struct Driver {
    job_id: JobId,
    wake: OwnedFd,
    requests: mpsc::Sender<Request>,
    snapshot: Arc<ArcSwap<Snapshot>>,
    thread: Option<JoinHandle<()>>,
}

impl Driver {
    fn send(&self, request: Request) -> Result<()> {
        self.requests
            .send(request)
            .map_err(|_| unavailable(self.job_id))?;
        control(self.wake.as_raw_fd(), 0, libc::NOTE_TRIGGER).map_err(|error| {
            CowshedError::internal(format!(
                "cannot wake job {}'s process observer: {error}",
                self.job_id.get()
            ))
        })
    }

    fn acknowledged(
        &self,
        request: impl FnOnce(mpsc::SyncSender<Result<()>>) -> Request,
    ) -> Result<()> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.send(request(reply))?;
        answer.recv().map_err(|_| unavailable(self.job_id))?
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        if let Err(error) = self.send(Request::Stop) {
            eprintln!("cowshed: stopping process observation: {error}");
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            eprintln!(
                "cowshed: job {}'s process observation writer panicked",
                self.job_id.get()
            );
        }
    }
}

/// The job record owns this monitor; clones do not own another writer or sampling window.
#[derive(Clone)]
pub struct ProcessMonitor {
    driver: Arc<Driver>,
}

impl std::fmt::Debug for ProcessMonitor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessMonitor")
            .field("job_id", &self.driver.job_id)
            .finish_non_exhaustive()
    }
}

impl ProcessMonitor {
    /// Prepare before the spawn. A failed source is a failed observation fence, not polling.
    pub fn prepare(job_id: JobId) -> Result<Self> {
        let observer = KqueueObserver::prepare(job_id).map_err(observation_error)?;
        control(observer.as_raw_fd(), libc::EV_ADD | libc::EV_CLEAR, 0).map_err(|error| {
            CowshedError::internal(format!(
                "cannot prepare the process observer's control event: {error}"
            ))
        })?;
        let wake = observer.wake_handle().map_err(|error| {
            CowshedError::internal(format!(
                "cannot retain the process observer's wake descriptor: {error}"
            ))
        })?;
        let snapshot = Arc::new(ArcSwap::from_pointee(Snapshot {
            tree: Err(CowshedError::conflict(
                format!("job {} has no observed process yet", job_id.get()),
                "read its processes once the command has started",
            )),
            attribution: AttributionSummary::from_observed(std::iter::empty()),
        }));
        let (requests, receiver) = mpsc::channel();
        let published = Arc::clone(&snapshot);
        let thread = thread::Builder::new()
            .name(format!("cowshed-process-{}", job_id.get()))
            .spawn(move || Writer::new(observer, published).run(receiver))
            .map_err(|error| {
                CowshedError::internal(format!(
                    "cannot start the process observation writer: {error}"
                ))
            })?;
        Ok(Self {
            driver: Arc::new(Driver {
                job_id,
                wake,
                requests,
                snapshot,
                thread: Some(thread),
            }),
        })
    }

    /// Synchronous because the held child cannot exec until this registration is acknowledged.
    pub(crate) fn watch(&self, pid: u32) -> Result<()> {
        self.driver
            .acknowledged(|reply| Request::Watch { pid, reply })
    }

    pub(crate) fn gap(&self, reason: ProcessCoverageGap) -> Result<()> {
        self.driver
            .acknowledged(|reply| Request::Gap { reason, reply })
    }

    /// Drain the job's accounting boundary. Still-live descendants remain watched, and their
    /// stream ends only when the last retained registration ends.
    pub(crate) fn finish(&self) -> Result<()> {
        self.driver.acknowledged(|reply| Request::Finish { reply })
    }

    /// Attach the existing supervisor writer. The source replays its retained events,
    /// with their original identities and times, before projecting any future event.
    pub(crate) fn trace(&self, publisher: JobSpanPublisher, info: &JobInfo) -> Result<()> {
        self.driver.send(Request::Trace(Box::new(TraceWriter {
            publisher,
            projection: ProcessSpanProjection::new(
                info.trace.trace_id.as_str(),
                job_span_thread(&info.repo_id, &info.workspace_incarnation, info.job_id),
            ),
        })))
    }

    /// One publication load. A read neither touches the writer's fold nor samples the kernel.
    pub fn tree(&self) -> Result<JobProcessTree> {
        self.driver.snapshot.load().tree.clone()
    }

    /// One publication load and the canonical B13 comparison; no resample or CPU-window move.
    pub(crate) fn attribution(
        &self,
        total: CpuTotals,
        required: LeafCoverage,
    ) -> Result<Attribution> {
        let snapshot = self.driver.snapshot.load();
        let tree = snapshot.tree.as_ref().map_err(Clone::clone)?;
        snapshot
            .attribution
            .as_ref()
            .map_err(|error| {
                CowshedError::internal(format!("cannot summarize observed process CPU: {error}"))
            })?
            .attribute(total, required, tree.coverage)
            .map_err(|error| {
                CowshedError::internal(format!("cannot attribute observed process CPU: {error}"))
            })
    }

    pub async fn subscribe(
        &self,
        every: Duration,
    ) -> Result<super::process_stream::JobProcessStream> {
        if every.is_zero() || Instant::now().checked_add(every).is_none() {
            return Err(CowshedError::usage(
                "process sampling interval is outside the clock's range",
                "use a positive, shorter everyMs",
            ));
        }
        let (reply, answer) = oneshot::channel();
        self.driver.send(Request::Open { every, reply })?;
        let id = answer
            .await
            .map_err(|_| unavailable(self.driver.job_id))??;
        let mut subscription = NativeSubscription {
            driver: Arc::clone(&self.driver),
            id,
            cursor: 0,
            ended: false,
        };
        let (events, received) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    biased;
                    () = events.closed() => return,
                    next = subscription.next() => next,
                };
                let Some(event) = next else { return };
                if events.send(event).await.is_err() {
                    return;
                }
            }
        });
        Ok(super::process_stream::JobProcessStream::new(received))
    }
}

/// One private journal position, transported by the canonical worker-stream demand row.
/// There is no public cursor operation and a dropped reader never ends the command.
struct NativeSubscription {
    driver: Arc<Driver>,
    id: u64,
    cursor: usize,
    ended: bool,
}

impl std::fmt::Debug for NativeSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobProcessStream")
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl NativeSubscription {
    pub async fn next(&mut self) -> Option<Result<JobProcessEvent>> {
        if self.ended {
            return None;
        }
        let (reply, answer) = oneshot::channel();
        if let Err(error) = self.driver.send(Request::Next {
            id: self.id,
            cursor: self.cursor,
            reply,
        }) {
            self.ended = true;
            return Some(Err(error));
        }
        match answer.await {
            Ok(Some(event)) => {
                self.cursor += 1;
                Some(event)
            }
            Ok(None) => {
                self.ended = true;
                None
            }
            Err(_) => {
                self.ended = true;
                Some(Err(unavailable(self.driver.job_id)))
            }
        }
    }
}

impl Drop for NativeSubscription {
    fn drop(&mut self) {
        if let Err(error) = self.driver.send(Request::Close { id: self.id }) {
            eprintln!("cowshed: closing process subscription: {error}");
        }
    }
}

struct Writer {
    observer: KqueueObserver,
    snapshot: Arc<ArcSwap<Snapshot>>,
    journal: Vec<RetainedEntry>,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    next_sample: Instant,
    /// The job-accounting boundary is complete. Live registrations still sample.
    finished: bool,
    /// `finished` and no retained registration remains. Readers then see the end.
    ended: bool,
    trace: Option<Box<TraceWriter>>,
}

impl Writer {
    fn new(observer: KqueueObserver, snapshot: Arc<ArcSwap<Snapshot>>) -> Self {
        Self {
            observer,
            snapshot,
            journal: Vec::new(),
            subscribers: HashMap::new(),
            next_id: 1,
            next_sample: Instant::now() + DEFAULT_SAMPLE,
            finished: false,
            ended: false,
            trace: None,
        }
    }

    fn publish(&mut self, outcome: Result<()>) {
        let at = SystemTime::now();
        let first = self.journal.len();
        for (event, identity, parent) in self.observer.drain_retained_events() {
            self.journal.push(RetainedEntry {
                event: Ok(event),
                identity,
                parent,
                at,
                batch_end: false,
            });
        }
        if self.journal.len() > first {
            self.journal.last_mut().expect("the batch emitted an entry").batch_end = true;
        }
        if let Some(trace) = &mut self.trace {
            trace.record(&self.journal[first..]);
        }
        let tree = match outcome {
            Ok(()) => utc_now().map(|at| self.observer.tree(at)),
            Err(error) => Err(error),
        };
        if let Err(error) = &tree {
            self.journal.push(RetainedEntry {
                event: Err(error.clone()),
                identity: None,
                parent: None,
                at,
                batch_end: true,
            });
        }
        self.snapshot.store(Arc::new(Snapshot {
            tree,
            attribution: self.observer.attribution_summary(),
        }));
        if self.finished {
            self.ended = !self.observer.watching();
        }
        for subscriber in self.subscribers.values_mut() {
            if let Some((cursor, _)) = &subscriber.pending
                && (*cursor < self.journal.len() || self.ended)
            {
                let (cursor, reply) = subscriber
                    .pending
                    .take()
                    .expect("a pending demand was checked");
                let _ = reply.send(self.journal.get(cursor).map(|entry| entry.event.clone()));
            }
        }
    }

    fn run(mut self, receiver: mpsc::Receiver<Request>) {
        loop {
            if let Err(error) = self.observer.drain() {
                self.publish(Err(observation_error(error)));
            } else if self.observer.has_events()
                || (self.finished && !self.observer.watching() && !self.ended)
            {
                self.publish(Ok(()));
            }
            while let Ok(request) = receiver.try_recv() {
                match request {
                    Request::Watch { pid, reply } => {
                        let result = self.observer.watch_root(pid).map_err(observation_error);
                        self.publish(result.clone());
                        let _ = reply.send(result);
                    }
                    Request::Gap { reason, reply } => {
                        let result = self.observer.gap(reason).map_err(observation_error);
                        self.publish(result.clone());
                        let _ = reply.send(result);
                    }
                    Request::Finish { reply } => {
                        self.finished = true;
                        let result = self.observer.drain().map_err(observation_error);
                        self.publish(result.clone());
                        let _ = reply.send(result);
                    }
                    Request::Trace(mut trace) => {
                        for batch in self.journal.split_inclusive(|entry| entry.batch_end) {
                            trace.record(batch);
                        }
                        self.trace = Some(trace);
                    }
                    Request::Open { every, reply } => match self.next_id.checked_add(1) {
                        Some(next) => {
                            let id = self.next_id;
                            self.next_id = next;
                            self.subscribers.insert(
                                id,
                                Subscriber {
                                    every,
                                    pending: None,
                                },
                            );
                            let _ = reply.send(Ok(id));
                        }
                        None => {
                            let _ = reply.send(Err(CowshedError::internal(
                                "process subscription identity exhausted",
                            )));
                        }
                    },
                    Request::Next { id, cursor, reply } => {
                        if cursor < self.journal.len() || self.ended {
                            let _ = reply
                                .send(self.journal.get(cursor).map(|entry| entry.event.clone()));
                        } else if let Some(subscriber) = self.subscribers.get_mut(&id) {
                            subscriber.pending = Some((cursor, reply));
                        } else {
                            let _ = reply.send(Some(Err(CowshedError::internal(
                                "process subscription is no longer registered",
                            ))));
                        }
                    }
                    Request::Close { id } => {
                        self.subscribers.remove(&id);
                    }
                    Request::Stop => return,
                }
            }
            if !self.ended && Instant::now() >= self.next_sample {
                if self.observer.watching() {
                    let result = self
                        .observer
                        .sample(Instant::now())
                        .map_err(observation_error);
                    self.publish(result);
                }
                let every = self
                    .subscribers
                    .values()
                    .map(|subscriber| subscriber.every)
                    .min()
                    .unwrap_or(DEFAULT_SAMPLE)
                    .min(DEFAULT_SAMPLE);
                self.next_sample = Instant::now() + every;
            }
            let timeout = if self.ended {
                -1
            } else {
                i32::try_from(
                    self.next_sample
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                )
                .unwrap_or(i32::MAX)
            };
            let mut descriptor = libc::pollfd {
                fd: self.observer.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one writable poll descriptor; its kqueue is owned for this whole call.
            if unsafe { libc::poll(&raw mut descriptor, 1, timeout) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    self.ended = true;
                    self.publish(Err(CowshedError::internal(format!(
                        "cannot wait for process events: {error}"
                    ))));
                    return;
                }
            }
        }
    }
}

fn observation_error(error: ObserveError) -> CowshedError {
    CowshedError::environment_missing(
        format!("process observation failed: {error}"),
        "inspect the retained coverage gap and the native call's error",
    )
}

fn unavailable(job_id: JobId) -> CowshedError {
    CowshedError::internal(format!(
        "job {}'s process observation writer is unavailable",
        job_id.get()
    ))
}

fn control(queue: i32, flags: u16, fflags: u32) -> io::Result<()> {
    let change = libc::kevent {
        ident: CONTROL_EVENT,
        filter: libc::EVFILT_USER,
        flags,
        fflags,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: one readable change, no returned events; the queue is owned by this caller.
    if unsafe { libc::kevent(queue, &change, 1, std::ptr::null_mut(), 0, std::ptr::null()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::process::ProcessCoverage;

    #[tokio::test]
    async fn finishing_a_live_observation_neither_invents_loss_nor_ends_its_reader() {
        let monitor = ProcessMonitor::prepare(JobId::new(1).unwrap()).unwrap();
        monitor.watch(std::process::id()).unwrap();
        assert_eq!(monitor.tree().unwrap().coverage, ProcessCoverage::Complete);
        monitor.finish().unwrap();
        assert_eq!(monitor.tree().unwrap().coverage, ProcessCoverage::Complete);
        let mut events = monitor.subscribe(Duration::from_millis(20)).await.unwrap();
        assert!(
            matches!(events.next().await, Some(Ok(JobProcessEvent::Born { process, .. })) if process.pid == std::process::id())
        );
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        loop {
            match tokio::time::timeout_at(deadline, events.next()).await {
                Ok(Some(Ok(_))) => {}
                Ok(other) => panic!("the live owner's reader ended or failed: {other:?}"),
                Err(_) => break,
            }
        }
        drop(events);
        assert!(monitor.tree().unwrap().processes[0].exit.is_none());
    }
}
