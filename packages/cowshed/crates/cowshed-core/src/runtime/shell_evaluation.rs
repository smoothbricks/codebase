//! The workspace's one cold shell evaluation at a time, what its holder tells the commands that
//! wait for it, and the bound on an evaluation that stopped making progress (11_shell.md, "Cold
//! activation is single-flight per workspace").
//!
//! The holder shares its evaluation once it starts it: the host's pid, when it started and its
//! last output line, which the tee of the evaluation's stderr swaps in once per chunk that ends a
//! line. The tee and waiting lines write through the job's single off-runtime stderr lane
//! ([`super::JobStderr`]), never calling `write(2)` on the supervisor's runtime thread, which also
//! drains that pipe. A waiter loads the shared record once per report and is woken only when it
//! changes, so nothing it prints repeats on a timer. An evaluation is idle while it writes no line
//! and no Nix build runs: a build the daemon runs for it writes nothing to its stderr while it
//! waits on the daemon's socket. A live process of a Nix build user is the observable sign of a
//! build; it cannot say whose build it is ([`super::super::nix_builds`]).

use std::future::Future;
use std::io::{self, Read as _};
use std::os::fd::{AsRawFd as _, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use tokio::io::unix::AsyncFd;
use tokio::sync::{Mutex, MutexGuard, Notify, mpsc};

use super::super::nix_builds::{Builds, Unwatched};
use super::JobStderr;
use crate::error::CowshedError;

/// How long an evaluation may write no line while no Nix build runs before it is ended.
pub(super) const STALL_BOUND: Duration = Duration::from_secs(5 * 60);

/// Most bytes [`Tee::finish_stalled`] drains from the evaluation's pipe before dropping the job's
/// stderr lane: one pipe buffer's capacity, so a live descendant outside the host's group cannot
/// hold the drain open.
const STALL_DRAIN_BOUND: usize = 64 * 1024;

/// The most of a line a waiter quotes, in chars.
const QUOTED_CHARS: usize = 200;

/// The most of an unfinished line the tee keeps, in bytes.
const PARTIAL_BYTES: usize = 4 * 1024;

/// The waiting line, before the holder's evaluation is named.
const WAITING: &str = "cowshed: waiting for this workspace's shell evaluation";

/// Where the stall watch reads the time and waits for it. Tests drive their own.
pub(super) trait StallClock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// The process's monotonic clock.
pub(super) struct SystemClock;

impl StallClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep_until(deadline.into()))
    }
}

/// What the Nix daemon is building now. Tests answer their own.
pub(super) trait BuildProbe: Send + Sync {
    fn running(&self) -> io::Result<Builds>;
}

/// The host's Nix daemon builds: live processes of its build users.
pub(super) struct NixBuilds;

impl BuildProbe for NixBuilds {
    fn running(&self) -> io::Result<Builds> {
        super::super::nix_builds::running()
    }
}

/// The workspace's single-flight claim on cold evaluation, and the evaluation its holder runs.
pub(super) struct ActivationGate {
    lock: Mutex<()>,
    /// The holder's evaluation once it started one; none while it verifies or adopts a record.
    holder: ArcSwapOption<Evaluation>,
    /// Woken when an evaluation starts or writes a new last line.
    changed: Notify,
    clock: Arc<dyn StallClock>,
    probe: Arc<dyn BuildProbe>,
}

/// One evaluation as its waiters see it.
pub(super) struct Evaluation {
    /// The evaluating host, whose group the stall bound ends.
    pid: i32,
    started: Instant,
    progress: ArcSwap<Progress>,
}

/// The evaluation's last line and when it ended; its start, before it wrote one.
struct Progress {
    at: Instant,
    line: Option<Box<str>>,
}

impl ActivationGate {
    pub(super) fn new(clock: Arc<dyn StallClock>, probe: Arc<dyn BuildProbe>) -> Self {
        Self {
            lock: Mutex::new(()),
            holder: ArcSwapOption::empty(),
            changed: Notify::new(),
            clock,
            probe,
        }
    }

    /// The gate cowshed runs: the process's clock and the host's Nix builds.
    pub(super) fn system() -> Self {
        Self::new(Arc::new(SystemClock), Arc::new(NixBuilds))
    }

    /// The claim, if no one holds it.
    pub(super) fn try_claim(&self) -> Option<Claim<'_>> {
        self.lock.try_lock().ok().map(|lock| Claim {
            gate: self,
            _lock: lock,
        })
    }

    /// Wait for the claim, saying so on the waiting job's `stderr`: once now, then each time the
    /// holder starts an evaluation or it writes a new last line, never on a timer.
    pub(super) async fn claim(&self, stderr: Option<&JobStderr>) -> Claim<'_> {
        let acquire = self.lock.lock();
        tokio::pin!(acquire);
        let mut shown = None;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let holder = self.holder.load_full();
            let progress = holder
                .as_ref()
                .map(|evaluation| evaluation.progress.load_full());
            let key = holder
                .as_ref()
                .zip(progress.as_ref())
                .map(|(evaluation, progress)| (evaluation.pid, progress.line.clone()));
            if shown.as_ref() != Some(&key) {
                super::report_on(
                    stderr,
                    &self.waiting(holder.as_deref().zip(progress.as_deref())),
                )
                .await;
                shown = Some(key);
            }
            tokio::select! {
                lock = &mut acquire => return Claim { gate: self, _lock: lock },
                () = &mut changed => {}
            }
        }
    }

    /// The line a waiter prints for the holder's evaluation, or for a holder that has not
    /// started one.
    fn waiting(&self, holder: Option<(&Evaluation, &Progress)>) -> String {
        let Some((evaluation, progress)) = holder else {
            return WAITING.to_owned();
        };
        let running = self
            .clock
            .now()
            .saturating_duration_since(evaluation.started)
            .as_secs();
        let pid = evaluation.pid;
        match &progress.line {
            Some(line) => format!("{WAITING} (pid {pid}, running {running}s, last: {line:?})"),
            None => format!("{WAITING} (pid {pid}, running {running}s, no output yet)"),
        }
    }
}

/// The workspace's claim on cold evaluation. Dropping it withdraws the holder's evaluation
/// before the lock opens, so it never withdraws the next holder's.
pub(super) struct Claim<'a> {
    gate: &'a ActivationGate,
    _lock: MutexGuard<'a, ()>,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.gate.holder.store(None);
    }
}

impl Claim<'_> {
    /// Share the evaluation host `pid` starts now with the waiters.
    pub(super) fn evaluating(&self, pid: i32) -> Arc<Evaluation> {
        let now = self.gate.clock.now();
        let evaluation = Arc::new(Evaluation {
            pid,
            started: now,
            progress: ArcSwap::from_pointee(Progress {
                at: now,
                line: None,
            }),
        });
        self.gate.holder.store(Some(Arc::clone(&evaluation)));
        self.gate.changed.notify_waiters();
        evaluation
    }

    /// Forward the evaluation's stderr through `tee` until its host's `reply` arrives, or until
    /// it has written no line for [`STALL_BOUND`] while no Nix build runs. The probe is asked only
    /// when the bound passes; a build it sees restarts the bound.
    pub(super) async fn watch<T>(
        &self,
        evaluation: &Evaluation,
        tee: &mut Tee,
        reply: impl Future<Output = T>,
    ) -> Result<T, Stall> {
        let gate = self.gate;
        tokio::pin!(reply);
        let mut built = evaluation.started;
        loop {
            let progress = evaluation.progress.load_full();
            let mut expired = gate.clock.sleep_until(progress.at.max(built) + STALL_BOUND);
            let line = loop {
                tokio::select! {
                    biased;
                    reply = &mut reply => return Ok(reply),
                    line = tee.forward(), if tee.open => if line.is_some() {
                        break line;
                    },
                    () = &mut expired => break None,
                }
            };
            if let Some(line) = line {
                evaluation.progress.store(Arc::new(Progress {
                    at: gate.clock.now(),
                    line: Some(line),
                }));
                gate.changed.notify_waiters();
                continue;
            }
            let asked = gate.clock.now();
            let probe = Arc::clone(&gate.probe);
            let probed = tokio::task::spawn_blocking(move || probe.running())
                .await
                .unwrap_or_else(|failed| {
                    Err(io::Error::other(format!(
                        "the build probe failed: {failed}"
                    )))
                });
            // A reply that arrived while the probe ran is the evaluation's answer.
            tokio::select! {
                biased;
                reply = &mut reply => return Ok(reply),
                () = std::future::ready(()) => {}
            }
            let unseen = match probed {
                Ok(Builds::Running) => {
                    built = asked;
                    continue;
                }
                Ok(Builds::Idle) => Unseen::Idle,
                Ok(Builds::Unwatched(why)) => Unseen::Unwatched(why),
                Err(error) => Unseen::Unreadable(error),
            };
            return Err(Stall {
                pid: evaluation.pid,
                silent: gate.clock.now().saturating_duration_since(progress.at),
                last: progress.line.clone(),
                unseen,
            });
        }
    }
}

/// An evaluation that wrote no line for the bound while no Nix build was seen.
pub(super) struct Stall {
    pid: i32,
    /// Since its last line, or its start.
    silent: Duration,
    last: Option<Box<str>>,
    /// Why no build was seen.
    unseen: Unseen,
}

/// Why the stall watch saw no Nix build.
enum Unseen {
    /// The daemon's build users ran nothing.
    Idle,
    /// Nothing on this host shows the daemon's builds.
    Unwatched(Unwatched),
    /// The build probe failed.
    Unreadable(io::Error),
}

impl Stall {
    /// The job's error, once the host was ended (`ended`, or what it had ended with).
    pub(super) fn into_error(self, ended: &str) -> CowshedError {
        let minutes = self.silent.as_secs() / 60;
        let build = match &self.unseen {
            Unseen::Idle => "no Nix build was running".to_owned(),
            Unseen::Unwatched(why) => format!("no Nix build was seen running ({why})"),
            Unseen::Unreadable(error) => {
                format!("whether a Nix build was running could not be read ({error})")
            }
        };
        let last = match &self.last {
            Some(line) => format!("{line:?}"),
            None => "it wrote no line".to_owned(),
        };
        CowshedError::environment_missing(
            format!(
                "the workspace shell evaluation wrote nothing for {minutes} min and {build}: {last}"
            ),
            format!(
                "its host {} {ended}; retry the command, and if it stalls again inspect the Nix \
                 daemon and the workspace's .envrc",
                self.pid
            ),
        )
    }
}

/// The evaluation's stderr on its way to the job's: forwarded unchanged through the job's
/// off-runtime writer lane, its last line kept.
pub(super) struct Tee {
    pipe: AsyncFd<std::fs::File>,
    /// The job's stderr writer lane; `None` for a spare, whose output is discarded while its
    /// lines are still tracked for waiters and the stall bound.
    job: Option<JobStderr>,
    lines: Lines,
    /// Until every writer of the pipe closed it, or reading it failed.
    open: bool,
}

impl Tee {
    /// A pipe whose write end is the evaluation's stderr, and the tee that forwards its read end
    /// onto the job's stderr lane (`None` for a spare). Pipe creation runs on the blocking pool
    /// because Darwin acquires the exclusive fork lock around `pipe` + `FD_CLOEXEC`.
    pub(super) async fn new(job: Option<JobStderr>) -> io::Result<(OwnedFd, Self)> {
        let (read, write) = tokio::task::spawn_blocking(|| {
            let (read, write) = super::pipe()?;
            // SAFETY: fcntl on a descriptor this function owns; only its own flags change.
            let flags = unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFL) };
            // SAFETY: as above.
            if flags == -1
                || unsafe { libc::fcntl(read.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                    == -1
            {
                return Err(io::Error::last_os_error());
            }
            Ok((read, write))
        })
        .await
        .map_err(|error| io::Error::other(format!("evaluation tee pipe task failed: {error}")))??;
        let tee = Self {
            pipe: AsyncFd::new(std::fs::File::from(read))?,
            job,
            lines: Lines::default(),
            open: true,
        };
        Ok((write, tee))
    }

    /// Forward the next chunk; the last line it ends, if any.
    ///
    /// Reserves a lane permit before awaiting readability and reading the pipe so cancelling this
    /// future inside [`Claim::watch`]'s `select!` loses no bytes.
    async fn forward(&mut self) -> Option<Box<str>> {
        let permit = match &self.job {
            Some(job) => job.reserve().await,
            None => None,
        };
        let mut buffer = [0_u8; 4096];
        let read = loop {
            let mut ready = match self.pipe.readable().await {
                Ok(ready) => ready,
                Err(error) => break Err(error),
            };
            if let Ok(read) = ready.try_io(|pipe| pipe.get_ref().read(&mut buffer)) {
                break read;
            }
        };
        take_chunk(&mut self.open, &mut self.lines, read, &buffer, permit)
    }

    /// Forward what the evaluation's pipe already holds, stopping at `WouldBlock`, EOF, or `limit`
    /// bytes. Reserves a lane permit before each non-blocking read so the runtime thread never
    /// blocks on a full job stderr pipe.
    async fn drain_buffered(&mut self, limit: Option<usize>) {
        let mut buffer = [0_u8; 4096];
        let mut drained = 0_usize;
        while self.open {
            let want = match limit {
                Some(max) => {
                    let remaining = max.saturating_sub(drained);
                    if remaining == 0 {
                        break;
                    }
                    buffer.len().min(remaining)
                }
                None => buffer.len(),
            };
            let permit = match &self.job {
                Some(job) => job.reserve().await,
                None => None,
            };
            let read = self.pipe.get_ref().read(&mut buffer[..want]);
            if read
                .as_ref()
                .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
            {
                break;
            }
            if let Ok(count) = read {
                drained = drained.saturating_add(count);
            }
            take_chunk(&mut self.open, &mut self.lines, read, &buffer, permit);
        }
    }

    /// End forwarding after the host answered: enqueue everything the evaluation's pipe already
    /// holds onto the job's stderr lane so the job's own output follows it, then keep forwarding
    /// the rest on its own task until the last writer closes the pipe.
    pub(super) async fn finish(mut self) {
        self.drain_buffered(None).await;
        if self.open {
            tokio::spawn(async move {
                while self.open {
                    self.forward().await;
                }
            });
        }
    }

    /// End forwarding after a stall: enqueue what the evaluation's pipe already holds, at most
    /// [`STALL_DRAIN_BOUND`], then drop the job's stderr lane without spawning a background task.
    /// Descendants left outside the host's process group no longer own the job's output, so the
    /// job's stderr reaches EOF and the job seals and fails visibly.
    pub(super) async fn finish_stalled(mut self) {
        self.drain_buffered(Some(STALL_DRAIN_BOUND)).await;
    }
}

/// Forward what one read returned using its reserved `permit`; the last line it ends, if any.
fn take_chunk(
    open: &mut bool,
    lines: &mut Lines,
    read: io::Result<usize>,
    buffer: &[u8],
    permit: Option<mpsc::Permit<'_, Box<[u8]>>>,
) -> Option<Box<str>> {
    let chunk = match read {
        Ok(0) => {
            *open = false;
            return None;
        }
        Ok(read) => &buffer[..read],
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: cannot read the workspace shell evaluation's stderr: {error}"
            );
            *open = false;
            return None;
        }
    };
    if let Some(permit) = permit {
        permit.send(Box::<[u8]>::from(chunk));
    }
    lines.feed(chunk)
}

/// The last line of a byte stream, a carriage return ending a line as a newline does.
#[derive(Default)]
struct Lines {
    partial: Vec<u8>,
}

impl Lines {
    /// Take `chunk`; the last non-blank line it ends, trimmed and cut to [`QUOTED_CHARS`].
    fn feed(&mut self, chunk: &[u8]) -> Option<Box<str>> {
        let mut last = None;
        for piece in chunk.split_inclusive(|&byte| byte == b'\n' || byte == b'\r') {
            let (body, ended) = match piece.split_last() {
                Some((&end, body)) if end == b'\n' || end == b'\r' => (body, true),
                _ => (piece, false),
            };
            let room = PARTIAL_BYTES.saturating_sub(self.partial.len());
            self.partial
                .extend_from_slice(&body[..body.len().min(room)]);
            if !ended {
                continue;
            }
            let text = String::from_utf8_lossy(&self.partial);
            let text = text.trim();
            if !text.is_empty() {
                let mut quoted: String = text.chars().take(QUOTED_CHARS).collect();
                if quoted.len() < text.len() {
                    quoted.push('…');
                }
                last = Some(quoted.into_boxed_str());
            }
            self.partial.clear();
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::{Lines, STALL_BOUND, Stall, Unseen, Unwatched};

    /// A stall where nothing shows the daemon's builds says so, and why.
    #[test]
    fn a_stall_names_unwatched_builds() {
        let error = Stall {
            pid: 7,
            silent: STALL_BOUND,
            last: Some("building mbx".into()),
            unseen: Unseen::Unwatched(Unwatched::NoBuildUsers),
        }
        .into_error("was ended");
        assert_eq!(
            error.message,
            "the workspace shell evaluation wrote nothing for 5 min and no Nix build was seen \
             running (the host has no nixbld build users): \"building mbx\""
        );
    }

    /// A line ends at a newline or a carriage return, may span chunks, and blank ones are not
    /// lines; an unfinished line is not one yet.
    #[test]
    fn the_last_line_is_the_last_ended_one() {
        let mut lines = Lines::default();
        assert_eq!(lines.feed(b"evaluating"), None);
        assert_eq!(
            lines.feed(b" devenv\nbuil").as_deref(),
            Some("evaluating devenv")
        );
        assert_eq!(
            lines.feed(b"ding mbx\r  \n\ncopying").as_deref(),
            Some("building mbx")
        );
        assert_eq!(lines.feed(b" path\r\n").as_deref(), Some("copying path"));
    }

    /// A long line is quoted in part, and an unended one grows no further than its bound.
    #[test]
    fn a_long_line_is_cut() {
        let mut lines = Lines::default();
        let long = "x".repeat(super::PARTIAL_BYTES * 2);
        assert_eq!(lines.feed(long.as_bytes()), None);
        let quoted = lines.feed(b"\n").unwrap();
        assert_eq!(quoted.chars().count(), super::QUOTED_CHARS + 1);
        assert!(quoted.ends_with('…'));
        assert!(lines.partial.is_empty());
    }

    /// A burst larger than a pipe buffer forwards every byte unchanged and in order to EOF even
    /// when the job's stderr is drained by a task on the same `current_thread` runtime as the tee.
    #[test]
    fn a_large_evaluation_burst_forwards_in_order_on_a_single_thread_runtime() {
        use std::io::Write as _;
        use std::sync::mpsc;
        use std::time::Duration;
        use tokio::io::AsyncReadExt as _;

        const PAYLOAD_BYTES: usize = 1 << 20;
        const TEST_BOUND: Duration = Duration::from_secs(20);

        let expected: Vec<u8> = (0_u8..=250).cycle().take(PAYLOAD_BYTES).collect();
        let writer_bytes = expected.clone();
        let (done_tx, done_rx) = mpsc::channel();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current_thread runtime");
            let received = runtime.block_on(async move {
                let (job_read, job_write) = super::super::pipe().expect("job stderr pipe");
                let mut job_reader = tokio::net::unix::pipe::Receiver::from_owned_fd(job_read)
                    .expect("async job stderr reader");
                let reader = tokio::spawn(async move {
                    let mut collected = Vec::with_capacity(PAYLOAD_BYTES);
                    job_reader
                        .read_to_end(&mut collected)
                        .await
                        .expect("read job stderr to EOF");
                    collected
                });

                let lane = super::super::JobStderr::from_file(std::fs::File::from(job_write));
                let (eval_write, mut tee) = super::Tee::new(Some(lane)).await.expect("tee");

                let producer = std::thread::spawn(move || {
                    let mut eval_stderr = std::fs::File::from(eval_write);
                    eval_stderr
                        .write_all(&writer_bytes)
                        .expect("write evaluation stderr burst");
                });

                while tee.open {
                    tee.forward().await;
                }
                tee.finish().await;

                let collected = reader.await.expect("reader task");
                producer.join().expect("evaluation stderr producer");
                collected
            });
            let _ = done_tx.send(received);
        });

        let received = done_rx.recv_timeout(TEST_BOUND).unwrap_or_else(|error| {
            panic!(
                "the tee wedged the current_thread runtime while forwarding a 1 MiB burst \
                 ({error})"
            )
        });
        assert_eq!(
            received.len(),
            expected.len(),
            "every byte reaches job stderr"
        );
        assert!(received == expected, "bytes arrive unchanged and in order");
    }

    /// A full job stderr pipe whose reader never drains must not wedge the supervisor's
    /// `current_thread` runtime: other tasks (standing in for `hello`) keep running and complete
    /// while tee forwarding is stalled on backpressure.
    #[test]
    fn a_stalled_job_stderr_does_not_block_other_tasks_on_the_runtime() {
        use std::io::Write as _;
        use std::sync::mpsc;
        use std::time::Duration;

        const PAYLOAD_BYTES: usize = 1 << 20;
        const TEST_BOUND: Duration = Duration::from_secs(20);

        let payload: Vec<u8> = (0_u8..=250).cycle().take(PAYLOAD_BYTES).collect();
        let (done_tx, done_rx) = mpsc::channel();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current_thread runtime");
            runtime.block_on(async move {
                let (job_read, job_write) = super::super::pipe().expect("job stderr pipe");
                let lane = super::super::JobStderr::from_file(std::fs::File::from(job_write));
                let (eval_write, mut tee) = super::Tee::new(Some(lane)).await.expect("tee");

                let producer = std::thread::spawn(move || {
                    let mut eval_stderr = std::fs::File::from(eval_write);
                    let _ = eval_stderr.write_all(&payload);
                });

                let forwarder = tokio::spawn(async move {
                    while tee.open {
                        tee.forward().await;
                    }
                    tee.finish().await;
                });

                let ticker = tokio::spawn(async move {
                    for _ in 0..20 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                });

                ticker.await.expect("ticker task");
                assert!(
                    !forwarder.is_finished(),
                    "forwarding must still be stalled while job stderr is unread"
                );

                // Drop the job's read end before the runtime shuts down so any blocked
                // `spawn_blocking` write wakes with EPIPE and tears down cleanly.
                drop(job_read);
                let _ = forwarder.await;
                let _ = producer.join();
            });
            drop(runtime);
            let _ = done_tx.send(());
        });

        done_rx.recv_timeout(TEST_BOUND).unwrap_or_else(|error| {
            panic!(
                "the tee wedged the current_thread runtime while job stderr was unread; \
                 the ticker never finished ({error})"
            )
        });
    }
}
