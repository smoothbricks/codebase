//! The held-leader fence (11_shell.md "Process identity and signal ownership"): a child this
//! process spawned and alone reaps, held so that signalling its whole group is safe.
//!
//! Until the child is reaped its id cannot name another process, and its group id names its group
//! alone, descendants that outlive it included. It is reaped only inside the critical section every
//! signal takes, so a signal meets either the unreaped child or the record that it is gone, never
//! a process that reused its id. Its exit is observed without reaping it ([`ChildFence::exited`]),
//! and it is reaped only once nothing more is wanted of it ([`ChildFence::release`]). Nothing else
//! may reap it: the child is spawned through `std`, whose handle never reaps on its own, not
//! through a Tokio handle whose dropped children Tokio reaps by pid in the background.
//!
//! The workspace supervisor fences every job leader and activation host with it, and
//! `cowshed hold` gives a caller that is not cowshed the same fence over a command of its own.

use std::fmt;
use std::io;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::sync::{Mutex, PoisonError};

use cowshed_os::fork_lock::Spawn as _;

use super::job_groups::{self, Birth};
use crate::api::dto::ExitStatus;
use crate::error::{CowshedError, Result};

/// A child this process spawned and alone reaps, with the fence that makes signalling its group
/// safe ([module docs](self)).
pub struct ChildFence {
    pid: i32,
    birth: Birth,
    fenced: Mutex<Fenced>,
}

/// What the fence guards: where the child is, and how it ended once the fence reaped it, so a
/// look after the reap still answers its exit.
struct Fenced {
    hold: Hold,
    exit: Option<ExitStatus>,
}

/// Where a fenced child is, as far as signalling its group is concerned: the functional core every
/// [`ChildFence`] operation decides through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Hold {
    /// Unreaped, running or exited: its id names only it, and its group id only its group.
    Held,
    /// Unreaped, and wanted no longer: it is reaped as soon as it is seen exited.
    Released,
    /// Collected: its id may name another process, so no signal follows.
    Reaped,
}

impl Hold {
    /// Whether the group may be signalled now: only while the child is unreaped.
    pub const fn admits_signal(self) -> bool {
        !matches!(self, Self::Reaped)
    }

    /// Nothing more is wanted of the child. A released or reaped child stays so.
    pub const fn released(self) -> Self {
        match self {
            Self::Held => Self::Released,
            other => other,
        }
    }

    /// Whether a child seen exited is collected now: only once it was released.
    pub const fn collects_exited(self) -> bool {
        matches!(self, Self::Released)
    }
}

/// Why a fenced group was not signalled: an answer, not a fault, since a held caller asks for it
/// at any moment of the child's life.
#[derive(Debug)]
pub enum NotSignalled {
    /// The child was reaped: processes still holding its group id cannot be told from another
    /// group that took it.
    Reaped { pgid: i32 },
    /// The child's birth identity was never read: nothing may end a group no one could identify.
    Unidentified { pgid: i32, reason: String },
    /// The kernel refused the signal.
    Refused { pgid: i32, error: io::Error },
}

impl fmt::Display for NotSignalled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reaped { pgid } => write!(
                formatter,
                "process {pgid} has been reaped; processes still holding its group id cannot be \
                 told from another group that took it, so none is signalled"
            ),
            Self::Unidentified { pgid, reason } => write!(
                formatter,
                "process group {pgid} was never identified ({reason}); it is not signalled"
            ),
            Self::Refused { pgid, error } => {
                write!(formatter, "failed to signal process group {pgid}: {error}")
            }
        }
    }
}

impl NotSignalled {
    /// The refusal as a cowshed error: a reaped leader is a conflict, an unidentified one or a
    /// refused signal a missing environment. Named rather than a `From` impl, which would make
    /// every `?` into `CowshedError` from an unannotated closure ambiguous.
    pub fn into_error(self) -> CowshedError {
        let message = self.to_string();
        match self {
            Self::Reaped { pgid } => {
                CowshedError::conflict(message, format!("inspect them with `ps -g {pgid}`"))
            }
            Self::Unidentified { pgid, .. } => CowshedError::environment_missing(
                message,
                format!("inspect it with `ps -g {pgid}`"),
            ),
            Self::Refused { .. } => {
                CowshedError::environment_missing(message, "inspect the job and retry")
            }
        }
    }
}

impl ChildFence {
    /// Spawn `command` as the leader of a new session, and so of a new process group, that this
    /// process stays outside of: a stop or kill of the group never reaches this process, which
    /// therefore always answers for it. `command`'s program, arguments, environment, working
    /// directory and descriptors are its own; it must not name a process group, which a new
    /// session already is. The command is consumed: its one `setsid` hook belongs to this child
    /// alone, so no reuse of it stacks a second one. The child's `std` handle is dropped, never
    /// waited for: a piped
    /// standard stream's parent end goes with it, so hand it inherited, null or owned descriptors.
    pub fn spawn(mut command: std::process::Command) -> io::Result<Self> {
        // SAFETY: `pre_exec` runs in the forked child between `fork` and `exec`, where only
        // async-signal-safe calls are legal; `setsid` is one, and the closure captures nothing.
        // Its success is load-bearing: `signal` sends to `-pid`, which is the child's own group
        // only because the child made itself a session and group leader here.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let child = command.spawn_locked()?;
        Self::new(child.id()).map_err(|error| io::Error::other(error.message))
    }

    /// The fence for `pid`, this process's own child, spawned just now as a group leader and not
    /// yet reaped. Its birth identity is observed here, before anything could reap it.
    pub(crate) fn new(pid: u32) -> Result<Self> {
        let leader = i32::try_from(pid)
            .ok()
            .filter(|pid| *pid > 1)
            .ok_or_else(|| {
                CowshedError::internal("process id is not a signalable process group")
            })?;
        Ok(Self {
            pid: leader,
            birth: Birth::of(pid),
            fenced: Mutex::new(Fenced {
                hold: Hold::Held,
                exit: None,
            }),
        })
    }

    /// The child's pid, the id of the group it leads.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// The child's birth identity, observed before anything could reap it.
    pub fn birth(&self) -> &Birth {
        &self.birth
    }

    /// Where the child is now.
    pub fn hold(&self) -> Hold {
        self.fenced().hold
    }

    fn fenced(&self) -> std::sync::MutexGuard<'_, Fenced> {
        self.fenced.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Signal the group the child leads, only while the child is unreaped.
    pub fn signal(&self, signal: libc::c_int) -> std::result::Result<(), NotSignalled> {
        let fenced = self.fenced();
        if !fenced.hold.admits_signal() {
            return Err(NotSignalled::Reaped { pgid: self.pid });
        }
        if let Birth::Unobserved { reason, .. } = &self.birth {
            return Err(NotSignalled::Unidentified {
                pgid: self.pid,
                reason: reason.clone(),
            });
        }
        job_groups::signal_unreaped_group(self.pid, signal).map_err(|error| NotSignalled::Refused {
            pgid: self.pid,
            error,
        })
    }

    /// Collect the child inside the fence if it has exited, recording how it ended; `None` while
    /// it runs.
    fn collect(&self, fenced: &mut Fenced) -> io::Result<Option<std::process::ExitStatus>> {
        loop {
            let mut status = 0;
            // SAFETY: `pid` is this process's own child and `status` a valid out-pointer.
            let waited = unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) };
            if waited == self.pid {
                let status = std::process::ExitStatus::from_raw(status);
                fenced.hold = Hold::Reaped;
                fenced.exit = exit_of(status);
                return Ok(Some(status));
            }
            if waited == 0 {
                return Ok(None);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                // Something else collected it: its id is free, so no signal may follow.
                fenced.hold = Hold::Reaped;
                return Err(error);
            }
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// How the child ended, if it has, read without reaping it -- unless it was released -- or
    /// as the fence recorded it when it reaped it.
    fn look(&self) -> io::Result<Option<ExitStatus>> {
        let mut fenced = self.fenced();
        if fenced.hold == Hold::Reaped {
            return fenced.exit.clone().map(Some).ok_or_else(|| {
                io::Error::other("the child was reaped by something other than its fence")
            });
        }
        let exit = match job_groups::exit_unreaped(self.pid) {
            Ok(exit) => exit,
            Err(error) => {
                if error.raw_os_error() == Some(libc::ECHILD) {
                    // Something else collected it: its id is free, so no signal may follow.
                    fenced.hold = Hold::Reaped;
                }
                return Err(error);
            }
        };
        if exit.is_some() && fenced.hold.collects_exited() {
            self.collect(&mut fenced)?;
        }
        Ok(exit)
    }

    /// Wait for the child to exit without reaping it: it stays held until [`Self::release`], or
    /// is reaped here when it was already released. A child the fence already reaped answers the
    /// exit it recorded. The child-exit notifications are subscribed before the first look, so
    /// none falls between a look and the wait for the next.
    pub async fn exited(&self) -> Result<ExitStatus> {
        let unobserved = |error: io::Error| {
            CowshedError::integrity(
                format!("cannot wait for process {}: {error}", self.pid),
                "cowshed doctor --json",
            )
        };
        let mut exits = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
            .map_err(unobserved)?;
        loop {
            if let Some(exit) = self.look().map_err(unobserved)? {
                return Ok(exit);
            }
            if exits.recv().await.is_none() {
                return Err(unobserved(io::Error::other(
                    "child-exit notifications ended",
                )));
            }
        }
    }

    /// Nothing more is wanted of the child: reap it now if it has exited, or as soon as it is
    /// seen exiting. Signals are still sent until then.
    pub fn release(&self) -> io::Result<()> {
        let mut fenced = self.fenced();
        if fenced.hold != Hold::Held {
            return Ok(());
        }
        fenced.hold = fenced.hold.released();
        self.collect(&mut fenced).map(drop)
    }

    /// Reap the child, inside the fence, if it has exited; `None` while it runs.
    pub(crate) fn try_reap(&self) -> io::Result<Option<std::process::ExitStatus>> {
        let mut fenced = self.fenced();
        if fenced.hold == Hold::Reaped {
            return Err(io::Error::other("the child was already reaped"));
        }
        self.collect(&mut fenced)
    }

    /// Wait for the child to exit and reap it inside the fence. The child-exit notifications are
    /// subscribed before the first look, so none falls between a look and the wait for the next.
    pub(crate) async fn wait(&self) -> io::Result<std::process::ExitStatus> {
        let mut exits = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())?;
        loop {
            if let Some(status) = self.try_reap()? {
                return Ok(status);
            }
            if exits.recv().await.is_none() {
                return Err(io::Error::other("child-exit notifications ended"));
            }
        }
    }
}

/// The exit a reap's `wait(2)` status records: its code, or the signal that ended it. A reap
/// without `WUNTRACED` reports nothing else; anything else is no exit.
fn exit_of(status: std::process::ExitStatus) -> Option<ExitStatus> {
    match (status.code(), status.signal()) {
        (Some(code), _) => Some(ExitStatus::Exited { code }),
        (None, Some(signal)) => Some(ExitStatus::Signaled {
            signal,
            core_dumped: status.core_dumped(),
        }),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::sync::Arc;

    use proptest::prelude::*;
    use tokio::sync::oneshot;

    use super::*;

    /// One thing that happens to a fenced child: a signal asked, a release, its exit, or a look.
    #[derive(Clone, Copy, Debug)]
    enum Step {
        Signal,
        Release,
        Exit,
        Look,
    }

    fn steps() -> impl Strategy<Value = Vec<Step>> {
        proptest::collection::vec(
            prop_oneof![
                Just(Step::Signal),
                Just(Step::Release),
                Just(Step::Exit),
                Just(Step::Look)
            ],
            0..12,
        )
    }

    proptest! {
        /// Over any history of the child, a signal is admitted exactly until the child is reaped;
        /// the child is reaped only once it has both exited and been released, and then as soon
        /// as both hold; nothing leaves the reaped state, and a release never returns to held.
        #[test]
        fn a_signal_is_admitted_exactly_until_the_reap(steps in steps()) {
            let (mut hold, mut exited, mut released, mut reaped) =
                (Hold::Held, false, false, false);
            for step in steps {
                let before = hold;
                match step {
                    Step::Signal => prop_assert_eq!(hold.admits_signal(), !reaped),
                    Step::Release => {
                        hold = hold.released();
                        released = true;
                        if exited && hold.collects_exited() {
                            hold = Hold::Reaped;
                        }
                    }
                    Step::Exit => exited = true,
                    Step::Look => {
                        if exited && hold.collects_exited() {
                            hold = Hold::Reaped;
                        }
                    }
                }
                if hold == Hold::Reaped && !reaped {
                    prop_assert!(exited && released, "{step:?} reaped too early");
                    reaped = true;
                }
                prop_assert!(before != Hold::Reaped || hold == Hold::Reaped);
                prop_assert!(before == Hold::Held || hold != Hold::Held);
                prop_assert_eq!(hold == Hold::Held, !released);
            }
        }
    }

    /// A test's own child, waited for however the test ends: on every path a test passes, the
    /// fence has reaped it already, and the wait finds it gone.
    struct Spawned(std::process::Child);

    impl Drop for Spawned {
        fn drop(&mut self) {
            if let Err(error) = self.0.wait()
                && error.raw_os_error() != Some(libc::ECHILD)
            {
                cowshed_os::eprintln!("reap the test child {}: {error}", self.0.id());
            }
        }
    }

    /// The same histories against a real child, a shell that exits when told on its stdin: a
    /// signal (the null signal) reaches its group exactly until the fence reaps it, the fence
    /// reaps it only once it was released and seen exited, and exactly once.
    #[test]
    fn a_real_child_is_signalled_exactly_until_its_one_reap() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        proptest!(ProptestConfig::with_cases(24), |(steps in steps())| {
            let mut command = std::process::Command::new("/bin/sh");
            command
                .args(["-c", "read -r line; exit 7"])
                .stdin(std::process::Stdio::piped());
            command.process_group(0);
            let mut child = Spawned(command.spawn_locked().expect("spawn the child"));
            let mut stdin = child.0.stdin.take();
            let fence = ChildFence::new(child.0.id()).expect("fence the child");
            let (mut released, mut reaped) = (false, false);
            for step in steps {
                match step {
                    Step::Signal => prop_assert_eq!(fence.signal(0).is_ok(), !reaped),
                    Step::Release => {
                        released = true;
                        fence.release().expect("release");
                    }
                    Step::Exit => {
                        if let Some(mut stdin) = stdin.take() {
                            stdin.write_all(b"exit\n").expect("tell the child to exit");
                            // Seen exited, never reaped: the steps that follow meet an exited child.
                            job_groups::await_exit_unreaped(fence.pid()).expect("exit");
                        }
                    }
                    Step::Look => {
                        let exit = fence.look().expect("look, before or after the reap");
                        prop_assert_eq!(
                            exit.is_some(),
                            stdin.is_none(),
                            "the child is seen exited exactly once it was told to exit"
                        );
                    }
                }
                let now = fence.hold();
                if now == Hold::Reaped {
                    prop_assert!(released && stdin.is_none());
                    reaped = true;
                }
            }
            drop(stdin);
            fence.release().expect("release");
            // Its stdin is closed now, so the shell's read ends and it exits 7. Released, it is
            // reaped as soon as it is seen exited; reaped already, the fence answers the exit it
            // recorded.
            prop_assert_eq!(
                runtime.block_on(fence.exited()).expect("exited"),
                ExitStatus::Exited { code: 7 }
            );
            prop_assert_eq!(fence.hold(), Hold::Reaped);
            prop_assert_eq!(child.0.wait().unwrap_err().raw_os_error(), Some(libc::ECHILD));
            let refused = matches!(fence.signal(0), Err(NotSignalled::Reaped { .. }));
            prop_assert!(refused, "no signal after the reap");
        });
    }

    /// A process group target must be a strictly positive pid other than init: `kill(-1, ...)`
    /// would signal every process this one may signal, and `kill(0, ...)` its own group.
    #[test]
    fn a_pid_that_is_not_a_process_group_is_refused() {
        for pid in [0, 1] {
            assert!(
                ChildFence::new(pid).is_err(),
                "pid {pid} must not be negated into a process-group target"
            );
        }
    }

    /// A spawned leader leads a session and a group of its own, which this process is outside
    /// of: when the leader stops its whole group, this process still runs and ends the group
    /// through the fence, and the leader's end is the kill's.
    #[tokio::test]
    async fn a_spawned_leader_stops_its_group_without_stopping_its_holder() {
        let mut leader = std::process::Command::new("/bin/sh");
        leader
            .args(["-c", "kill -s STOP 0; exit 3"])
            .stdin(std::process::Stdio::null());
        let fence = ChildFence::spawn(leader).expect("spawn the leader");
        let pid = fence.pid();
        // SAFETY: getsid/getpgid read the kernel's view of an unreaped child and of this process.
        let (session, group, own_session, own_group) = unsafe {
            (
                libc::getsid(pid),
                libc::getpgid(pid),
                libc::getsid(0),
                libc::getpgid(0),
            )
        };
        assert_eq!((session, group), (pid, pid));
        assert_ne!(own_session, pid);
        assert_ne!(own_group, pid);
        // The leader stops itself and its group; nothing it stopped is this process. A stop
        // report reaps nothing.
        let mut status = 0;
        // SAFETY: waits for this process's own fenced child to stop.
        let waited = unsafe { libc::waitpid(pid, &mut status, libc::WUNTRACED) };
        assert_eq!(waited, pid);
        assert!(libc::WIFSTOPPED(status), "the leader stopped its group");
        fence
            .signal(libc::SIGKILL)
            .expect("the stopped group is still held");
        assert_eq!(
            fence.exited().await.expect("exited"),
            ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false
            }
        );
        fence.release().expect("release");
        assert!(matches!(
            fence.signal(libc::SIGKILL),
            Err(NotSignalled::Reaped { .. })
        ));
    }

    /// Each spawn makes its own leader a session of its own: two leaders spawned one after the
    /// other lead two sessions, neither of them this process's.
    #[tokio::test]
    async fn every_spawned_leader_leads_a_session_of_its_own() {
        let spawn = || {
            let mut leader = std::process::Command::new("/bin/sh");
            leader
                .args(["-c", "exec sleep 600"])
                .stdin(std::process::Stdio::null());
            ChildFence::spawn(leader).expect("spawn a leader")
        };
        let (first, second) = (spawn(), spawn());
        for fence in [&first, &second] {
            // SAFETY: getsid reads the kernel's view of this process's own unreaped child.
            assert_eq!(unsafe { libc::getsid(fence.pid()) }, fence.pid());
            fence.signal(libc::SIGKILL).expect("end the held leader");
            assert_eq!(
                fence.exited().await.expect("exited"),
                ExitStatus::Signaled {
                    signal: libc::SIGKILL,
                    core_dumped: false
                }
            );
            fence.release().expect("release");
        }
        assert_ne!(first.pid(), second.pid());
    }

    /// Darwin reports a stopped child to `waitid(WEXITED | WNOWAIT)`, which asked only for exits
    /// (measured on Darwin 25.6). A leader stopped with its report still pending is not an exit:
    /// it is still held, the fence's kill ends it, and that death is its exit.
    #[tokio::test]
    async fn a_stopped_leader_is_not_an_exit() {
        let mut leader = std::process::Command::new("/bin/sh");
        leader
            .args(["-c", "kill -s STOP 0; exit 3"])
            .stdin(std::process::Stdio::null());
        let fence = ChildFence::spawn(leader).expect("spawn the leader");
        let id = libc::id_t::try_from(fence.pid()).expect("a pid");
        // SAFETY: an all-zero `siginfo_t` is valid storage for the call to fill.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: waits for this process's own child to stop, leaving the report pending.
        let stopped =
            unsafe { libc::waitid(libc::P_PID, id, &mut info, libc::WSTOPPED | libc::WNOWAIT) };
        assert_eq!(stopped, 0, "{}", io::Error::last_os_error());
        assert_eq!(
            job_groups::exit_unreaped(fence.pid()).expect("a stop is read without error"),
            None
        );
        fence
            .signal(libc::SIGKILL)
            .expect("the stopped leader is held");
        assert_eq!(
            fence.exited().await.expect("exited"),
            ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false
            }
        );
        fence.release().expect("release");
    }

    /// The reaper and every signal share one fence. A group is signalled while its leader is
    /// unreaped -- running, or exited and not yet collected -- and refused from the moment the
    /// leader is reaped, even though a descendant still holds the id: by then a stranger's group
    /// could take it. A signaller racing the reap sees successes and then only refusals, never a
    /// success after the reap.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_group_is_signalled_only_while_its_reaper_holds_the_leader() {
        use std::io::BufRead as _;
        use std::sync::atomic::{AtomicBool, Ordering};

        // The OwnedGroup test idiom keeps cleanup around the assertions, not after them.
        struct OwnedGroup(std::process::Child, Option<std::process::ChildStdin>);
        impl Drop for OwnedGroup {
            fn drop(&mut self) {
                let pgid = i32::try_from(self.0.id()).expect("owned child pid");
                // SAFETY: this test created the group and keeps its descendant until cleanup.
                if unsafe { libc::killpg(pgid, libc::SIGKILL) } != 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        cowshed_os::eprintln!("end owned signal-test group {pgid}: {error}");
                    }
                }
                if let Err(error) = self.0.wait()
                    && error.raw_os_error() != Some(libc::ECHILD)
                {
                    cowshed_os::eprintln!("reap owned signal-test leader {pgid}: {error}");
                }
                if let Some(mut output) = self.0.stdout.take()
                    && let Err(error) = io::copy(&mut output, &mut io::sink())
                {
                    cowshed_os::eprintln!(
                        "observe owned signal-test group {pgid} output closure: {error}"
                    );
                }
                drop(self.1.take());
            }
        }
        struct OwnedSignaller {
            done: Arc<AtomicBool>,
            thread: Option<std::thread::JoinHandle<Vec<bool>>>,
        }
        impl Drop for OwnedSignaller {
            fn drop(&mut self) {
                self.done.store(true, Ordering::SeqCst);
                if let Some(thread) = self.thread.take()
                    && let Err(panic) = thread.join()
                {
                    cowshed_os::eprintln!(
                        "join owned signal-test signaller during cleanup: {panic:?}"
                    );
                }
            }
        }
        let mut job = OwnedGroup(
            std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    "exec 3<&0; (trap '' TERM; echo READY; IFS= read -r release <&3) & wait",
                ])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .process_group(0)
                .spawn_locked()
                .unwrap(),
            None,
        );
        // Child::wait closes Child.stdin, so retain the lifetime writer in its owner instead.
        job.1 = job.0.stdin.take();
        assert!(
            job.1.is_some(),
            "the owner retains descendant stdin across wait"
        );
        let pgid = i32::try_from(job.0.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(job.0.stdout.as_mut().unwrap())
            .read_line(&mut ready)
            .unwrap();
        let fence = Arc::new(ChildFence::new(job.0.id()).unwrap());
        assert!(fence.birth().leader().is_some());
        fence
            .signal(0)
            .expect("a running leader's group is the job's");
        // SIGKILL the leader only; the descendant keeps the id. Until the fence reaps it the
        // exited leader still holds its id, so the group is still the job's.
        // SAFETY: the unreaped test child.
        unsafe { libc::kill(pgid, libc::SIGKILL) };
        fence
            .exited()
            .await
            .expect("the leader exited but remains unreaped");
        fence
            .signal(0)
            .expect("an exited, unreaped leader's group is the job's");

        let done = Arc::new(AtomicBool::new(false));
        let (observed, observation) = oneshot::channel();
        let mut signaller = OwnedSignaller {
            done: Arc::clone(&done),
            thread: Some({
                let (fence, done) = (Arc::clone(&fence), Arc::clone(&done));
                std::thread::spawn(move || {
                    let initial = fence.signal(0).is_ok();
                    let mut outcomes = vec![initial];
                    observed
                        .send(initial)
                        .expect("the reaper waits for the signaller");
                    while !done.load(Ordering::SeqCst) {
                        outcomes.push(fence.signal(0).is_ok());
                    }
                    outcomes.push(fence.signal(0).is_ok());
                    outcomes
                })
            }),
        };
        assert!(
            observation
                .await
                .expect("the signaller reports before reap"),
            "the concurrent signaller must succeed before the leader is reaped"
        );
        let status = fence.wait().await.unwrap();
        // The fence, not std's handle, owns waitpid. A second reaper must find no child left.
        assert_eq!(job.0.wait().unwrap_err().raw_os_error(), Some(libc::ECHILD));
        done.store(true, Ordering::SeqCst);
        let outcomes = signaller.thread.take().unwrap().join().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert_eq!(outcomes.last(), Some(&false), "no signal after the reap");
        assert!(
            outcomes.windows(2).all(|pair| pair[0] || !pair[1]),
            "a refusal is never followed by a success"
        );
        let refused = fence.signal(libc::SIGTERM).unwrap_err();
        assert!(matches!(refused, NotSignalled::Reaped { pgid: reaped } if reaped == pgid));
        assert_eq!(refused.into_error().code, crate::error::ErrorCode::Conflict);
        assert!(job_groups::group_has_live_members(pgid).unwrap());
        drop(job);
    }

    /// A job's leader is observed exiting, with its exact status, while it stays unreaped: its pid
    /// is still taken, so no other process can be given it, and a kill still reaches the
    /// descendants that outlive it and hold the job's output open. The leader is reaped only once
    /// the job releases it, and nothing is signalled after that.
    #[tokio::test]
    async fn an_exited_leader_is_held_until_its_job_releases_it() {
        use std::io::Read as _;

        let mut job = std::process::Command::new("/bin/sh")
            .args(["-c", "exec 3<&0; read _ <&3 & exit 3"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let mut output = job.stdout.take().unwrap();
        let pgid = i32::try_from(job.id()).unwrap();
        let fence = ChildFence::new(job.id()).unwrap();
        assert_eq!(
            fence.exited().await.unwrap(),
            ExitStatus::Exited { code: 3 }
        );
        // Observing the exit did not reap it: it is observed again, its pid is still taken by it
        // (the null signal reaches a zombie), and the descendant is still in the group the held
        // leader's id names.
        assert_eq!(
            fence.exited().await.unwrap(),
            ExitStatus::Exited { code: 3 }
        );
        // SAFETY: the null signal to this test's own unreaped child.
        assert_eq!(unsafe { libc::kill(pgid, 0) }, 0);
        assert!(!cowshed_os::process::running(pgid).unwrap());
        assert!(job_groups::group_has_live_members(pgid).unwrap());
        fence
            .signal(libc::SIGKILL)
            .expect("an exited, held leader's group is the job's");
        let mut rest = Vec::new();
        output
            .read_to_end(&mut rest)
            .expect("the output ends with the descendant");
        assert!(rest.is_empty());
        fence.release().unwrap();
        assert_eq!(job.wait().unwrap_err().raw_os_error(), Some(libc::ECHILD));
        assert!(matches!(
            fence.signal(libc::SIGTERM),
            Err(NotSignalled::Reaped { .. })
        ));
    }
}
