//! A read that can wait on another process's kernel state, run on a reader thread under a bound,
//! so its caller is answered at the bound however long the kernel holds the read
//! (specs/procinfo/01_procinfo.md, "Scope"): Linux's `cmdline`, which the kernel copies out of the
//! process's memory under its memory-map lock.

use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

type Job = Box<dyn FnOnce() + Send>;

/// The reader thread now serving reads, by the generation it was started as.
struct Serving {
    generation: u64,
    jobs: mpsc::Sender<Job>,
}

struct State {
    started: u64,
    serving: Option<Serving>,
}

/// One reader thread at a time, replaced once a read outlives its bound, so no later read queues
/// behind a stuck one. A thread given up on finishes its read when the kernel lets it, finds its
/// caller gone, and ends once the reads already sent to it are done.
pub(crate) struct BoundedReader {
    // WHY lock: a caller on any thread picks the reader now serving, and retires one stuck past its
    // bound; the section clones or replaces one sender and never waits. A thread owning that choice
    // would be one more queue a caller waits on, the very wait the bound exists to end.
    state: Mutex<State>,
}

impl BoundedReader {
    pub(crate) const fn new() -> Self {
        Self {
            state: Mutex::new(State {
                started: 0,
                serving: None,
            }),
        }
    }

    /// `read`'s answer, or `None` when it gave none within `bound`. A read that panics, or a reader
    /// thread that cannot be started, is an error.
    pub(crate) fn read<T: Send + 'static>(
        &self,
        bound: Duration,
        read: impl FnOnce() -> T + Send + 'static,
    ) -> io::Result<Option<T>> {
        let (answer, answered) = mpsc::sync_channel(1);
        let job: Job = Box::new(move || {
            // A caller that gave up has dropped its receiver: no one is left to answer.
            let _given_up = answer.send(read());
        });
        let (generation, jobs) = self.serving()?;
        if jobs.send(job).is_err() {
            self.retire(generation);
            return Err(io::Error::other(
                "procinfo's reader thread had ended; the read was not run",
            ));
        }
        match answered.recv_timeout(bound) {
            Ok(value) => Ok(Some(value)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.retire(generation);
                Ok(None)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::other(
                "the read panicked on procinfo's reader thread",
            )),
        }
    }

    /// The serving thread's generation and queue, started now when none serves.
    fn serving(&self) -> io::Result<(u64, mpsc::Sender<Job>)> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(serving) = &state.serving {
            return Ok((serving.generation, serving.jobs.clone()));
        }
        let (jobs, queue) = mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("procinfo-reader".to_owned())
            .spawn(move || {
                for job in queue {
                    // A panicking read has dropped its answer, which its caller reads as an error;
                    // the thread serves the next read.
                    let _reported = catch_unwind(AssertUnwindSafe(job));
                }
            })?;
        state.started += 1;
        let generation = state.started;
        state.serving = Some(Serving {
            generation,
            jobs: jobs.clone(),
        });
        Ok((generation, jobs))
    }

    /// Stop sending reads to the thread of `generation`, unless another has replaced it already.
    fn retire(&self, generation: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state
            .serving
            .as_ref()
            .is_some_and(|serving| serving.generation == generation)
        {
            state.serving = None;
        }
    }
}
