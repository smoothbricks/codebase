//! A job's progress subscription (07_api.md "Job monitoring"): its latest resource sample once
//! there is one, another every interval while the job runs -- whether or not it writes anything
//! -- and its terminal sample exactly once before the subscription closes.
//!
//! A subscription is one task. It reads the job's [`ProgressRead`] at each interval's deadline and
//! as soon as the job ends, and publishes each sample into a single slot. The slot is the
//! coalescing: a reader slower than the interval receives only the latest sample, never a backlog.
//! The terminal sample is the slot's last value, published as the task finishes, so a reader
//! always receives it before the end. A read answers a running job with an observation and an
//! ended one with its frozen terminal sample, never the frozen sample as a running one, so the
//! terminal sample is published once.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::api::resources::JobResourceSample;
use crate::error::{CowshedError, Result};

/// Where one progress read found the job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "phase",
    content = "sample",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum ProgressRead {
    /// Admitted, and no process of its own yet: nothing to sample.
    Unowned,
    /// Running: its processes as observed by this read.
    Running(JobResourceSample),
    /// Ended: its terminal sample.
    Ended(JobResourceSample),
}

/// The samples of one subscription, as its reader takes them.
#[derive(Debug)]
pub struct JobProgressStream {
    latest: watch::Receiver<Option<Result<JobResourceSample>>>,
}

impl JobProgressStream {
    /// The latest sample this reader has not received, waiting for the next one when it has
    /// received them all. After the terminal sample, or the error that ended the subscription,
    /// `None`.
    pub async fn next(&mut self) -> Option<Result<JobResourceSample>> {
        // The slot's version is checked before its closing, so the value published last is
        // received before the end is.
        self.latest.changed().await.ok()?;
        self.latest.borrow_and_update().clone()
    }
}

/// Starts a subscription whose first read was `first`: `read` reads the job again at each
/// deadline, `every` apart, and once `ended` resolves. A read that answers an ended job, or
/// fails, is the subscription's last publication, as is a deadline past the clock's range. The
/// task stops as soon as its reader is dropped. An interval whose first deadline is already past
/// the clock's range is refused.
pub(super) fn subscribe<R, F, E>(
    first: ProgressRead,
    every: Duration,
    read: R,
    ended: E,
) -> Result<JobProgressStream>
where
    R: FnMut() -> F + Send + 'static,
    F: Future<Output = Result<ProgressRead>> + Send + 'static,
    E: Future<Output = ()> + Send + 'static,
{
    let start = Instant::now();
    start
        .checked_add(every)
        .ok_or_else(|| unschedulable(every))?;
    let (slot, latest) = watch::channel(None);
    tokio::spawn(publish(first, start, every, read, ended, slot));
    Ok(JobProgressStream { latest })
}

async fn publish<R, F, E>(
    first: ProgressRead,
    start: Instant,
    every: Duration,
    mut read: R,
    ended: E,
    slot: watch::Sender<Option<Result<JobResourceSample>>>,
) where
    R: FnMut() -> F,
    F: Future<Output = Result<ProgressRead>>,
    E: Future<Output = ()>,
{
    let mut ended = std::pin::pin!(ended);
    let mut ending = false;
    let mut deadline = start;
    let mut progress = Ok(first);
    loop {
        match progress {
            Ok(ProgressRead::Unowned) => {}
            Ok(ProgressRead::Running(sample)) => {
                slot.send_replace(Some(Ok(sample)));
            }
            Ok(ProgressRead::Ended(sample)) => {
                slot.send_replace(Some(Ok(sample)));
                return;
            }
            Err(error) => {
                slot.send_replace(Some(Err(error)));
                return;
            }
        }
        deadline = match following(deadline, every) {
            Some(next) => next,
            None => {
                slot.send_replace(Some(Err(unschedulable(every))));
                return;
            }
        };
        tokio::select! {
            biased;
            () = slot.closed() => return,
            () = &mut ended, if !ending => ending = true,
            () = tokio::time::sleep_until(deadline) => {}
        }
        // A dropped reader ends the subscription even while a read is in flight: the read is
        // abandoned, never awaited for a sample nobody will take.
        progress = tokio::select! {
            biased;
            () = slot.closed() => return,
            progress = read() => progress,
        };
    }
}

/// The deadline after `at`: one interval on, or now when that has already passed, so a read
/// slower than the interval is followed at once rather than by a burst of overdue reads. `None`
/// past the clock's range.
fn following(at: Instant, every: Duration) -> Option<Instant> {
    at.checked_add(every).map(|next| next.max(Instant::now()))
}

fn unschedulable(every: Duration) -> CowshedError {
    CowshedError::usage(
        format!(
            "a sample every {} ms falls past the range of this host's clock",
            every.as_millis()
        ),
        "subscribe with a shorter everyMs",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{Mutex, mpsc, oneshot};

    use super::*;
    use crate::api::dto::{JobId, UtcTimestamp};
    use crate::api::resources::{
        HostLoadSample, JobStreamWatermark, JobVolumeUsage, ResidentBytes, StreamBytes,
        StreamLines, VolumeUnavailable, VolumeUsage, WallMicros,
    };

    fn sample(wall_us: u64) -> JobResourceSample {
        let host = HostLoadSample::new(1.0, 8).expect("host");
        let quiet = JobStreamWatermark {
            bytes: StreamBytes::new(0).expect("bytes"),
            lines: StreamLines::new(0).expect("lines"),
        };
        let wall = WallMicros::new(wall_us).expect("wall");
        JobResourceSample {
            job_id: JobId::new(7).expect("job"),
            sampled_at: UtcTimestamp::new("2026-10-07T12:00:00Z").expect("time"),
            wall_ms: wall.millis(),
            wall_us: wall,
            leader_pid: 4242,
            members: vec![4242],
            host_start: host,
            host,
            rss_bytes: ResidentBytes::ZERO,
            rss_peak_bytes: ResidentBytes::ZERO,
            accounting: None,
            volumes: JobVolumeUsage {
                workspace: VolumeUsage::Unavailable {
                    reason: VolumeUnavailable::Unconfigured,
                },
                build: None,
            },
            stdout: quiet,
            stderr: quiet,
        }
    }

    /// A job the test answers for: each read announces itself on `asked` and returns what the
    /// test sends it next.
    struct Script {
        asked: mpsc::UnboundedReceiver<()>,
        answers: mpsc::UnboundedSender<Result<ProgressRead>>,
    }

    type Read = Box<
        dyn FnMut() -> std::pin::Pin<Box<dyn Future<Output = Result<ProgressRead>> + Send>> + Send,
    >;

    fn script() -> (Script, Read) {
        let (ask, asked) = mpsc::unbounded_channel();
        let (answers, answered) = mpsc::unbounded_channel();
        let answered = Arc::new(Mutex::new(answered));
        let read: Read = Box::new(move || {
            let ask = ask.clone();
            let answered = Arc::clone(&answered);
            Box::pin(async move {
                ask.send(()).expect("the test listens");
                answered
                    .lock()
                    .await
                    .recv()
                    .await
                    .expect("the test answers every read")
            })
        });
        (Script { asked, answers }, read)
    }

    impl Script {
        /// Waits for the subscription's next read and answers it.
        async fn answer(&mut self, progress: Result<ProgressRead>) {
            self.asked
                .recv()
                .await
                .expect("the subscription reads again");
            self.answers.send(progress).expect("the read waits");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_running_job_is_sampled_once_per_interval() {
        let (mut script, read) = script();
        let every = Duration::from_secs(2);
        let start = Instant::now();
        let mut stream = subscribe(
            ProgressRead::Running(sample(0)),
            every,
            read,
            std::future::pending(),
        )
        .expect("subscribe");
        assert_eq!(
            stream.next().await.expect("first").expect("sample"),
            sample(0)
        );
        for tick in 1..=3_u32 {
            script
                .answer(Ok(ProgressRead::Running(sample(u64::from(tick)))))
                .await;
            assert_eq!(
                stream.next().await.expect("periodic").expect("sample"),
                sample(u64::from(tick))
            );
            assert_eq!(start.elapsed(), every * tick, "read at its deadline");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_reader_gets_the_latest_sample_and_never_loses_the_terminal_one() {
        let (mut script, read) = script();
        let mut stream = subscribe(
            ProgressRead::Running(sample(0)),
            Duration::from_secs(1),
            read,
            std::future::pending(),
        )
        .expect("subscribe");
        script.answer(Ok(ProgressRead::Running(sample(1)))).await;
        script.answer(Ok(ProgressRead::Running(sample(2)))).await;
        script.answer(Ok(ProgressRead::Ended(sample(3)))).await;
        // The subscription published the terminal sample and finished: its read closure is gone.
        assert!(script.asked.recv().await.is_none());
        // The reader took nothing while four samples were published: it gets the last, the
        // terminal one, and then the end.
        assert_eq!(
            stream.next().await.expect("terminal").expect("sample"),
            sample(3)
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn the_end_is_read_at_once_not_at_the_next_deadline() {
        let (mut script, read) = script();
        let (end, ended) = oneshot::channel::<()>();
        let mut stream = subscribe(
            ProgressRead::Running(sample(0)),
            Duration::from_secs(3600),
            read,
            async move {
                let _ = ended.await;
            },
        )
        .expect("subscribe");
        assert_eq!(
            stream.next().await.expect("first").expect("sample"),
            sample(0)
        );
        end.send(()).expect("the subscription waits for the end");
        script.answer(Ok(ProgressRead::Ended(sample(9)))).await;
        assert_eq!(
            stream.next().await.expect("terminal").expect("sample"),
            sample(9)
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn an_unowned_job_publishes_nothing_until_it_owns_a_process() {
        let (mut script, read) = script();
        let mut stream = subscribe(
            ProgressRead::Unowned,
            Duration::from_secs(1),
            read,
            std::future::pending(),
        )
        .expect("subscribe");
        script.answer(Ok(ProgressRead::Unowned)).await;
        script.answer(Ok(ProgressRead::Running(sample(5)))).await;
        assert_eq!(
            stream.next().await.expect("first").expect("sample"),
            sample(5)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_read_is_the_last_publication() {
        let (mut script, read) = script();
        let mut stream = subscribe(
            ProgressRead::Running(sample(0)),
            Duration::from_secs(1),
            read,
            std::future::pending(),
        )
        .expect("subscribe");
        assert_eq!(
            stream.next().await.expect("first").expect("sample"),
            sample(0)
        );
        let failure = CowshedError::environment_missing("the group could not be read", "retry");
        script.answer(Err(failure.clone())).await;
        assert_eq!(
            stream.next().await.expect("failure").expect_err("error"),
            failure
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_reader_stops_the_subscription() {
        let (mut script, read) = script();
        let stream = subscribe(
            ProgressRead::Running(sample(0)),
            Duration::from_secs(1),
            read,
            std::future::pending(),
        )
        .expect("subscribe");
        script.answer(Ok(ProgressRead::Running(sample(1)))).await;
        drop(stream);
        // The task ends instead of reading again: its read closure, and the sender it holds,
        // are dropped with it.
        assert!(script.asked.recv().await.is_none());
    }

    /// Dropping the reader abandons a read in flight: the subscription ends without waiting for
    /// an answer that may never come, and drops the read. The clock is paused, so the hour-long
    /// sleep fires only once nothing else can run: the read was kept past its reader.
    #[tokio::test(start_paused = true)]
    async fn dropping_the_reader_abandons_a_read_in_flight() {
        let (reading, mut reads) = mpsc::unbounded_channel::<oneshot::Receiver<()>>();
        let stream = subscribe(
            ProgressRead::Running(sample(0)),
            Duration::from_secs(1),
            move || {
                // The read holds `held` until it is dropped; it never answers.
                let (held, released) = oneshot::channel::<()>();
                reading.send(released).expect("the test watches each read");
                async move {
                    let _held = held;
                    std::future::pending::<Result<ProgressRead>>().await
                }
            },
            std::future::pending(),
        )
        .expect("subscribe");
        let released = reads.recv().await.expect("a read in flight");
        drop(stream);
        tokio::select! {
            biased;
            _ = released => {}
            () = tokio::time::sleep(Duration::from_secs(3600)) => {
                panic!("the read in flight outlived its reader")
            }
        }
    }

    #[tokio::test]
    async fn an_interval_past_the_clock_s_range_is_refused() {
        let (_script, read) = script();
        let refused = subscribe(
            ProgressRead::Running(sample(0)),
            Duration::MAX,
            read,
            std::future::pending(),
        )
        .expect_err("no deadline to keep");
        assert_eq!(refused.code, crate::error::ErrorCode::Usage, "{refused:?}");
    }
}
