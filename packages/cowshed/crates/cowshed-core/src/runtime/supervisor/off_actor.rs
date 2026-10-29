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
use std::sync::Arc;
use std::time::Instant;

use super::super::child_fence::ChildFence;
use crate::error::Result;

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

/// A spawn awaiting adoption: its owned child and observed fence travel together across the
/// blocking-pool await. Cancellation or refused pipes end the unadopted group and reap it away
/// from the actor; only adoption hands reaping to the ordinary process watcher.
pub(super) struct Spawned {
    child: Option<std::process::Child>,
    fence: Option<Arc<ChildFence>>,
    pub(super) at: Instant,
    pub(super) nice: Option<i32>,
}

impl Spawned {
    /// Observe the owned, unreaped group leader before returning it to the actor.
    pub(super) fn new(child: std::process::Child, nice: Option<i32>) -> Result<Self> {
        let mut spawned = Self {
            child: Some(child),
            fence: None,
            at: Instant::now(),
            nice,
        };
        spawned.fence = Some(Arc::new(ChildFence::new(spawned.child().id())?));
        Ok(spawned)
    }

    pub(super) fn child(&mut self) -> &mut std::process::Child {
        self.child
            .as_mut()
            .expect("an unadopted spawn owns its child")
    }

    pub(super) fn fence(&self) -> &Arc<ChildFence> {
        self.fence
            .as_ref()
            .expect("a returned spawn has observed its child's fence")
    }

    /// All fallible pipe setup is over. The watcher alone may now reap this leader.
    pub(super) fn adopt(mut self) -> Arc<ChildFence> {
        self.child.take();
        self.fence.take().expect("an adopted spawn has its fence")
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return;
        };
        let pid = i32::try_from(child.id()).expect("an owned child pid fits the kernel pid type");
        // SAFETY: this is our own unreaped child, made a group leader before exec. No watcher
        // can reap it before adoption, so neither its pid nor its group id can be reused.
        if unsafe { libc::kill(-pid, libc::SIGKILL) } == -1 {
            cowshed_os::eprintln!(
                "cowshed: cannot end unadopted job group {pid}: {}",
                std::io::Error::last_os_error()
            );
        }
        // SIGKILL is not synchronous; no actor thread waits for the actual exit. This child
        // and its fence are no longer published, so the detached collector owns reaping alone.
        cowshed_os::deadline::collect_later("unadopted job", child);
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};

    use cowshed_os::fork_lock::Spawn as _;
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;
    use crate::api::dto::ExitStatus;
    use crate::runtime::job_groups::{Birth, Process};

    /// The fixture exits on stdin EOF even if its spawn guard is broken. Its last line is the
    /// negative control: abandoning a child must kill it, not merely drop its pipes.
    fn child() -> Spawned {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", r#"exec 3<&0; /bin/sh -c "printf 'ready\n'; read line; printf 'stdin closed\n'; exit 17" <&3 & wait $!"#])
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        Spawned::new(command.spawn_locked().expect("spawn the fixture"), None)
            .expect("observe its unreaped birth")
    }

    /// Identity-held cleanup remains safe if the guard already reaped the fixture.
    struct End(Process);

    impl Drop for End {
        fn drop(&mut self) {
            self.0.signal(libc::SIGKILL).expect("end the fixture");
        }
    }

    fn output(spawned: &mut Spawned) -> (End, tokio::process::ChildStdout) {
        let pid = spawned.fence().pid();
        assert!(matches!(spawned.fence().birth(), Birth::Observed(_)));
        let end = End(Process::observe(pid)
            .expect("read the fixture's life")
            .expect("it runs"));
        let stdout = tokio::process::ChildStdout::from_std(
            spawned.child().stdout.take().expect("fixture stdout"),
        )
        .expect("watch fixture stdout");
        (end, stdout)
    }

    async fn ready(
        stdout: tokio::process::ChildStdout,
    ) -> tokio::io::BufReader<tokio::process::ChildStdout> {
        let mut stdout = tokio::io::BufReader::new(stdout);
        let mut line = String::new();
        stdout
            .read_line(&mut line)
            .await
            .expect("fixture readiness");
        assert_eq!(line, "ready\n");
        stdout
    }

    #[tokio::test]
    async fn an_adopted_spawn_retains_its_birth_and_transfers_reaping() {
        let mut spawned = run(child).await;
        let birth = spawned.fence().birth().clone();
        let (_end, stdout) = output(&mut spawned);
        let mut stdout = ready(stdout).await;
        let mut stdin = tokio::process::ChildStdin::from_std(
            spawned.child().stdin.take().expect("fixture stdin"),
        )
        .expect("watch fixture stdin");
        let fence = spawned.adopt();
        assert_eq!(
            fence.birth(),
            &birth,
            "adoption carries the observed identity"
        );
        stdin.write_all(b"line\n").await.expect("first input");
        drop(stdin);
        let mut output = String::new();
        stdout
            .read_to_string(&mut output)
            .await
            .expect("fixture output");
        assert_eq!(output, "stdin closed\n");
        assert_eq!(
            fence.exited().await.expect("the leader stays unreaped"),
            ExitStatus::Exited { code: 17 }
        );
        fence.release().expect("the adopter reaps it");
        assert!(fence.reaped());
    }

    #[tokio::test]
    async fn cancelling_an_off_actor_spawn_ends_the_unadopted_group() {
        let (published, received) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let pending = tokio::spawn(run(move || {
            let mut spawned = child();
            published
                .send(output(&mut spawned))
                .unwrap_or_else(|_| panic!("the test awaits its spawn"));
            released.recv().expect("release the blocking work");
            spawned
        }));
        let (_end, stdout) = received.await.expect("blocking work published its spawn");
        let mut stdout = ready(stdout).await;
        pending.abort();
        assert!(
            pending
                .await
                .err()
                .expect("the actor was cancelled")
                .is_cancelled()
        );
        release
            .send(())
            .expect("finish the cancelled blocking work");
        let mut output = String::new();
        stdout
            .read_to_string(&mut output)
            .await
            .expect("the unadopted group exited");
        assert!(
            output.is_empty(),
            "the abandoned child ran after its stdin closed: {output:?}"
        );
    }
}
