//! A cold evaluation's waiters see who holds it, and one that stops making progress is ended
//! (11_shell.md, "Cold activation is single-flight per workspace"). The tests play the host:
//! a real process leading its own group stands for it, and the test answers its control socket,
//! so the activation path under test is the real one.

use std::future::Future;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::pin::Pin;
use std::time::Duration;

use super::super::nix_builds::Builds;
use super::super::shell_host::read_frame;
use super::evaluation::{BuildProbe, STALL_BOUND, StallClock};
use super::*;

/// A clock the test moves by hand.
struct FakeClock(tokio::sync::watch::Sender<Instant>);

impl FakeClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(tokio::sync::watch::Sender::new(Instant::now())))
    }

    fn advance(&self, by: Duration) {
        self.0.send_modify(|now| *now += by);
    }
}

impl StallClock for FakeClock {
    fn now(&self) -> Instant {
        *self.0.borrow()
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let mut moved = self.0.subscribe();
        Box::pin(async move {
            let reached = moved.wait_for(|now| *now >= deadline).await.is_ok();
            // A dropped clock never moves again.
            if !reached {
                std::future::pending::<()>().await;
            }
        })
    }
}

/// A build probe that answers what the test set and counts how often it was asked.
struct FakeProbe {
    building: bool,
    asked: tokio::sync::watch::Sender<usize>,
}

impl FakeProbe {
    fn new(building: bool) -> (Arc<Self>, tokio::sync::watch::Receiver<usize>) {
        let (asked, count) = tokio::sync::watch::channel(0);
        (Arc::new(Self { building, asked }), count)
    }
}

impl BuildProbe for FakeProbe {
    fn running(&self) -> io::Result<Builds> {
        self.asked.send_modify(|asked| *asked += 1);
        Ok(if self.building {
            Builds::Running
        } else {
            Builds::Idle
        })
    }
}

/// Each test's own bound on a wait: an evaluation nothing ends hangs until it.
const TEST_BOUND: Duration = Duration::from_secs(20);

/// A workspace with a `.envrc` and no inherited record, so every activation evaluates.
struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("cowshed-stall-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(root.join(".cowshed")).unwrap();
        std::fs::write(root.join(".envrc"), "").unwrap();
        Self(root)
    }

    fn activator(&self, gate: ActivationGate) -> Arc<HostActivator> {
        Arc::new(HostActivator {
            program: ShellHostProgram::dedicated("/usr/bin/false"),
            workspace_mount: self.0.clone(),
            profile: crate::runtime::supervisor::ChildProfile::unbound(
                "",
                &self.0.join("profiles"),
            ),
            read_only: false,
            environment: BTreeMap::new(),
            shared_daemons: Vec::new(),
            grants: Default::default(),
            envrc_directory: Some(self.0.clone()),
            activation: Arc::new(gate),
        })
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            cowshed_os::eprintln!("remove {}: {error}", self.0.display());
        }
    }
}

/// The test's side of a host: the process standing for it and its control socket's other end.
struct FakeHost {
    fence: Arc<ChildFence>,
    socket: StdUnixStream,
    /// The write end of the stand-in's stdin: it reads until this closes.
    _stdin: OwnedFd,
}

impl FakeHost {
    fn start() -> (ExecHost, Self) {
        let (ours, theirs) = StdUnixStream::pair().unwrap();
        // `/bin/sh`'s builtin `read` on a pipe this test holds stands for the host, as the fence's
        // own tests run one: it needs no other program, which a runner may not have at any path.
        let (read, stdin) = pipe().unwrap();
        let mut command = std::process::Command::new("/bin/sh");
        command
            .args(["-c", "read -r line"])
            .stdin(Stdio::from(read))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let fence = Arc::new(ChildFence::spawn(command).unwrap());
        ours.set_nonblocking(true).unwrap();
        let host = ExecHost {
            fence: Arc::clone(&fence),
            process: OwnedProcess {
                birth: fence.birth().clone(),
                spawned: Instant::now(),
                host: crate::host_load::read_host_load(),
            },
            control: tokio::net::UnixStream::from_std(ours).unwrap(),
            closed: false,
        };
        (
            host,
            Self {
                fence,
                socket: theirs,
                _stdin: stdin,
            },
        )
    }

    fn pid(&self) -> i32 {
        self.fence.pid()
    }

    /// Approve the `.envrc`, then take the evaluation's stderr from the activation request.
    fn evaluation(&self) -> std::fs::File {
        let (frame, _) = read_frame(&self.socket)
            .unwrap()
            .expect("an approval request");
        assert_eq!(FrameReader::new(&frame).unwrap().0, REQUEST_APPROVE);
        (&self.socket)
            .write_all(&FrameWriter::new(REPLY_APPROVED).i32(0).finish().unwrap())
            .unwrap();
        let (frame, mut descriptors) = read_frame(&self.socket)
            .unwrap()
            .expect("an activation request");
        assert_eq!(FrameReader::new(&frame).unwrap().0, REQUEST_ACTIVATE);
        std::fs::File::from(descriptors.pop().expect("the evaluation's stderr"))
    }

    /// Answer the activation with an export that sets nothing.
    fn activated(&self) {
        let reply = FrameWriter::new(REPLY_ACTIVATED)
            .u32(0)
            .bytes(b"{}")
            .and_then(FrameWriter::finish)
            .unwrap();
        (&self.socket).write_all(&reply).unwrap();
    }
}

impl Drop for FakeHost {
    fn drop(&mut self) {
        if !self.fence.reaped()
            && let Err(error) = self.fence.signal(libc::SIGKILL)
        {
            cowshed_os::eprintln!("end fake host {}: {error:?}", self.fence.pid());
        }
    }
}

/// A job's stdout and stderr for an activation, and the read end of its stderr.
fn job_stdio() -> (std::fs::File, (OwnedFd, OwnedFd)) {
    let (_, stdout) = pipe().unwrap();
    let (stderr_read, stderr) = pipe().unwrap();
    (std::fs::File::from(stderr_read), (stdout, stderr))
}

fn job(id: u64) -> TreeOwner {
    TreeOwner::Job(JobId::new(id).unwrap())
}

/// Activate `host` for job `id` on its own task.
fn activate(
    activator: &Arc<HostActivator>,
    mut host: ExecHost,
    id: u64,
    stdio: (OwnedFd, OwnedFd),
) -> tokio::task::JoinHandle<HostActivation> {
    let activator = Arc::clone(activator);
    tokio::spawn(async move {
        activator
            .activate_host(&mut host, job(id), Vec::new(), stdio)
            .await
    })
}

/// The lines of a job's stderr, as they arrive.
fn lines(stderr: std::fs::File) -> mpsc::UnboundedReceiver<String> {
    let (sender, receiver) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { return };
            if sender.send(line).is_err() {
                return;
            }
        }
    });
    receiver
}

fn outcome(activation: HostActivation) -> String {
    match activation {
        HostActivation::Ready(Ok(_)) => "ready".to_owned(),
        HostActivation::Ready(Err(reason)) => format!("ready, unstable: {reason}"),
        HostActivation::Failed { status } => format!("failed with status {status}"),
        HostActivation::Broken(error) => format!("broken: {}", error.message),
    }
}

/// A waiter says which process holds the evaluation, how long it has run and the last line it
/// wrote.
#[tokio::test]
async fn a_waiter_names_the_holders_pid_age_and_last_line() {
    let workspace = Workspace::new();
    let clock = FakeClock::new();
    let (probe, _) = FakeProbe::new(false);
    let activator = workspace.activator(ActivationGate::new(clock.clone(), probe));
    let (host, holder) = FakeHost::start();
    let (_holder_stderr, stdio) = job_stdio();
    let holding = activate(&activator, host, 1, stdio);
    let (holder, _evaluation) = tokio::task::spawn_blocking(move || {
        let mut evaluation = holder.evaluation();
        evaluation.write_all(b"building mbx\n").unwrap();
        (holder, evaluation)
    })
    .await
    .unwrap();
    clock.advance(Duration::from_secs(42));

    let (host, _waiter) = FakeHost::start();
    let (stderr, stdio) = job_stdio();
    let waiting = activate(&activator, host, 2, stdio);
    let mut lines = lines(stderr);
    let expected = format!(
        "cowshed: waiting for this workspace's shell evaluation (pid {}, running 42s, last: \
         \"building mbx\")",
        holder.pid()
    );
    let mut seen = Vec::new();
    let named = tokio::time::timeout(TEST_BOUND, async {
        while let Some(line) = lines.recv().await {
            let found = line == expected;
            seen.push(line);
            if found {
                return;
            }
        }
    })
    .await;
    holding.abort();
    waiting.abort();
    assert!(named.is_ok(), "no waiting line names the holder: {seen:?}");
}

/// An evaluation silent for the bound while no Nix build runs fails its job by name, its host
/// is ended, and the waiter takes the claim and evaluates afresh.
#[tokio::test]
async fn a_silent_evaluation_without_a_build_is_ended_and_its_waiter_evaluates() {
    let workspace = Workspace::new();
    let clock = FakeClock::new();
    let (probe, mut asked) = FakeProbe::new(false);
    let activator = workspace.activator(ActivationGate::new(clock.clone(), probe));
    let (host, holder) = FakeHost::start();
    let (_holder_stderr, stdio) = job_stdio();
    let holding = activate(&activator, host, 1, stdio);
    let (holder, _evaluation) = tokio::task::spawn_blocking(move || {
        let evaluation = holder.evaluation();
        (holder, evaluation)
    })
    .await
    .unwrap();
    clock.advance(STALL_BOUND);
    let (host, waiter) = FakeHost::start();
    let (_waiter_stderr, stdio) = job_stdio();
    let waiting = activate(&activator, host, 2, stdio);
    let fresh = tokio::task::spawn_blocking(move || {
        let _evaluation = waiter.evaluation();
        waiter.activated();
        waiter
    });

    let Ok(held) = tokio::time::timeout(TEST_BOUND, holding).await else {
        panic!("the silent evaluation was never ended");
    };
    let held = outcome(held.unwrap());
    assert_eq!(
        held,
        "broken: the workspace shell evaluation wrote nothing for 5 min and no Nix build was \
         running: it wrote no line"
    );
    assert_eq!(
        *asked.borrow_and_update(),
        1,
        "the probe is asked once the bound passed"
    );
    assert!(
        holder.fence.reaped(),
        "the stalled evaluation's host is ended"
    );
    let waited = tokio::time::timeout(TEST_BOUND, waiting)
        .await
        .expect("the waiter takes the claim")
        .unwrap();
    assert!(outcome(waited).starts_with("ready"));
    tokio::time::timeout(TEST_BOUND, fresh)
        .await
        .expect("the waiter evaluates afresh")
        .unwrap();
}

/// An evaluation silent past the bound while a Nix build runs is not ended: each build seen
/// restarts the bound, and the evaluation's answer still arrives.
#[tokio::test]
async fn a_silent_evaluation_while_a_build_runs_is_not_ended() {
    let workspace = Workspace::new();
    let clock = FakeClock::new();
    let (probe, mut asked) = FakeProbe::new(true);
    let activator = workspace.activator(ActivationGate::new(clock.clone(), probe));
    let (host, holder) = FakeHost::start();
    let (_holder_stderr, stdio) = job_stdio();
    let holding = activate(&activator, host, 1, stdio);
    let (holder, _evaluation) = tokio::task::spawn_blocking(move || {
        let evaluation = holder.evaluation();
        (holder, evaluation)
    })
    .await
    .unwrap();

    for round in 1..=3 {
        clock.advance(STALL_BOUND);
        tokio::time::timeout(TEST_BOUND, asked.wait_for(|asked| *asked >= round))
            .await
            .expect("the probe is asked each time the bound passes")
            .unwrap();
    }
    assert!(
        !holding.is_finished(),
        "a building evaluation keeps running"
    );
    let holder = tokio::task::spawn_blocking(move || {
        holder.activated();
        holder
    })
    .await
    .unwrap();
    let held = tokio::time::timeout(TEST_BOUND, holding)
        .await
        .expect("the evaluation's answer arrives")
        .unwrap();
    assert!(outcome(held).starts_with("ready"));
    assert!(!holder.fence.reaped(), "its host was not ended");
}
