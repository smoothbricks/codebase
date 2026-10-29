//! The blocking work of the supervisor's actor (specs/cowshed/11_shell.md, "Supervisor"). The
//! actor and every socket task that writes a call's heartbeat share one thread: the supervisor
//! runs a current-thread runtime. A turn that stats, writes, fsyncs, unlinks or makes a fifo on
//! that thread therefore stops every heartbeat of the process for as long as the kernel call
//! takes, and on a loaded APFS container one such call took the whole 1.6 s of a stack sample.
//! So no turn makes one: each runs here, on the blocking pool, and its turn awaits its result
//! with the thread free. The actor's state stays its own: the turn holds it across the await,
//! and no other turn starts before the result is back. The census in `actor_census` keeps file
//! system calls from returning to the actor.

use std::future::Future;

/// Run `work` on the blocking pool and answer what it returns. A panic in `work` is the
/// caller's, resumed here.
pub(super) async fn run<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .unwrap_or_else(|failure| std::panic::resume_unwind(failure.into_panic()))
}

/// Drive `steps`, a future that blocks between its awaits (a host-side spawn preparation that
/// reads the store and awaits a sandboxed probe), to its end on the blocking pool. The runtime
/// the actor runs on still drives its timers and child processes: its thread stays free.
pub(super) async fn drive<F>(steps: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    run(move || runtime.block_on(steps)).await
}

/// A value every use of which blocks (an artifact store, a held build-volume lock file, a
/// jobserver fifo): the actor owns it, touches it only on the blocking pool, and drops it there.
pub(super) struct OffActor<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> OffActor<T> {
    pub(super) fn new(value: T) -> Self {
        Self(Some(value))
    }

    /// Run `work` with the value on the blocking pool. The value is away for the await and back
    /// before this returns, so the turn that awaits it is the only one that can use it.
    pub(super) async fn run<R: Send + 'static>(
        &mut self,
        work: impl FnOnce(&mut T) -> R + Send + 'static,
    ) -> R {
        let mut value = self
            .0
            .take()
            .expect("an off-actor value is back from the blocking pool before its next use");
        let (value, answer) = run(move || {
            let answer = work(&mut value);
            (value, answer)
        })
        .await;
        self.0 = Some(value);
        answer
    }

    /// Drop the value on the blocking pool and answer once it is gone, so what its drop releases
    /// (a lock, a file) is released when this returns.
    pub(super) async fn release(mut self) {
        if let Some(value) = self.0.take() {
            run(move || drop(value)).await;
        }
    }
}

/// What a value whose every other use blocks answers from memory: the part the actor reads in
/// place, between the turns that hand the value to the blocking pool.
pub(super) trait InMemory {
    type View: ?Sized;
    fn in_memory(&self) -> &Self::View;
}

impl<T: InMemory + Send + 'static> OffActor<T> {
    pub(super) fn in_memory(&self) -> &T::View {
        self.0
            .as_ref()
            .expect("an off-actor value is back from the blocking pool before its next use")
            .in_memory()
    }
}

impl<T: Send + 'static> Drop for OffActor<T> {
    /// A value dropped without [`OffActor::release`] goes to the blocking pool, unawaited. With
    /// no runtime there is no actor thread to keep free (a teardown's retirement thread), so it
    /// drops where it is.
    fn drop(&mut self) {
        let Some(value) = self.0.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn_blocking(move || drop(value));
            }
            Err(_) => drop(value),
        }
    }
}
