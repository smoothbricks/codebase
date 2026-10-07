//! Commands that run in warm exec hosts: the supervisor's side of [`super::shell_host`].
//!
//! A job keeps every part of its contract: its own anonymous stdin/stdout/stderr pipes, pumped
//! and quota-accounted by the supervisor exactly as for a one-shot child; its own process group
//! (the host forks the argv with `setpgid`); its exact exit; and a kill that reaches its group —
//! never the host's — unless the host itself is still activating for that job. The host holds the
//! command's leader unreaped until the job concludes, so a kill also reaches descendants that
//! outlive the leader.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, Interest};
use tokio::sync::{mpsc, oneshot};

use super::job_groups::Birth;
use super::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, FrameReader, FrameWriter, MAX_FRAME_BYTES, REPLY_ACTIVATED,
    REPLY_ACTIVATION_EXITED, REPLY_ACTIVATION_UNUSABLE, REPLY_APPROVED, REPLY_EXITED,
    REPLY_HOST_FAILED, REPLY_RELEASED, REPLY_SCRIPT_SYNTAX, REPLY_SIGNAL_FAILED, REPLY_STARTED,
    REQUEST_ACTIVATE, REQUEST_APPROVE, REQUEST_RELEASE, REQUEST_RUN, REQUEST_SCRIPT,
    REQUEST_SIGNAL, RawWaitStatus, SHELL_HOST_DIRECTORY, ShellHostProgram, send_with_descriptors,
};
use super::shell_pool::{Acquired, Activation, Activator, ShellPool, ShellPoolConfig};
use super::shell_watch::{
    ActivationEvidence, EvaluationClock, FsInstant, FsSeparation, Snapshot, decode_direnv_watches,
};
use super::supervisor::{
    ChildFence, ProcessEvent, ProcessSignal, RunningProcess, SandboxEnvironment, StdinLane,
    process_termination_from_wait, run_system_output, run_system_stdin,
};
use crate::api::dto::{ExitStatus, JobId, Sha256Digest};
use crate::error::{CowshedError, Result};
use crate::exec::{ExecError, SANDBOX_EXEC, classify_spawn_error, prepare_child_descriptors};
use crate::fork_lock::Spawn as _;
use crate::storage::job_artifact::StreamKind;

/// Everything that makes one warm shell interchangeable with another: the same profile (mode
/// and grants), the same sandbox environment, the same `.envrc` or none, the same host program.
fn pool_key(
    profile: &str,
    environment: &BTreeMap<OsString, OsString>,
    envrc_directory: Option<&Path>,
    grant_revision: u64,
    program: &ShellHostProgram,
) -> Sha256Digest {
    let mut bytes = Vec::with_capacity(profile.len() + 4096);
    let mut field = |value: &[u8]| {
        bytes.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_le_bytes());
        bytes.extend_from_slice(value);
    };
    field(profile.as_bytes());
    match envrc_directory {
        Some(directory) => {
            field(b"envrc");
            field(directory.as_os_str().as_bytes());
        }
        None => field(b"bare"),
    }
    field(&grant_revision.to_le_bytes());
    field(program.executable().as_os_str().as_bytes());
    for argument in program.arguments() {
        field(argument.as_bytes());
    }
    for (name, value) in environment {
        field(name.as_bytes());
        field(value.as_bytes());
    }
    Sha256Digest::compute(&bytes)
}

/// The warm shells of one workspace supervisor, one pool per shell identity.
pub(super) struct WorkspaceShells {
    program: ShellHostProgram,
    config: ShellPoolConfig,
    /// Keyed by (read-only, `.envrc` directory or none); a changed identity replaces the pool,
    /// whose idle hosts end with it and whose executing hosts are not returned.
    pools: BTreeMap<(bool, Option<PathBuf>), (Sha256Digest, ShellPool<HostActivator>)>,
}

/// What a warm host runs for one job.
pub(super) enum HostCommand {
    Argv(Vec<OsString>),
    Script(crate::script::RenderedScript),
}

/// One command admitted to run in a warm shell.
pub(super) struct PooledSpawn<'a> {
    pub job_id: JobId,
    pub command: HostCommand,
    pub cwd: PathBuf,
    /// The executed-child profile the supervisor rendered once for its authority.
    pub profile: &'a str,
    pub read_only: bool,
    pub workspace_mount: PathBuf,
    /// The `.envrc` a host activates, or `None` for a workspace with no shell configuration.
    pub envrc_directory: Option<PathBuf>,
    pub environment: SandboxEnvironment,
    pub caller: &'a BTreeMap<String, String>,
    pub grant_revision: u64,
}

impl WorkspaceShells {
    pub(super) fn new(program: ShellHostProgram, config: ShellPoolConfig) -> Self {
        Self {
            program,
            config,
            pools: BTreeMap::new(),
        }
    }

    pub(super) fn spawn(
        &mut self,
        spawn: PooledSpawn<'_>,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        let base = spawn.environment.base();
        let key = pool_key(
            spawn.profile,
            &base,
            spawn.envrc_directory.as_deref(),
            spawn.grant_revision,
            &self.program,
        );
        let family = (spawn.read_only, spawn.envrc_directory.clone());
        let pool = match self.pools.get(&family) {
            Some((current, pool)) if *current == key => pool.clone(),
            _ => {
                let pool = ShellPool::start(
                    Arc::new(HostActivator {
                        program: self.program.clone(),
                        workspace_mount: spawn.workspace_mount.clone(),
                        profile: spawn.profile.to_owned(),
                        environment: base,
                        envrc_directory: spawn.envrc_directory.clone(),
                    }),
                    self.config,
                );
                self.pools.insert(family, (key, pool.clone()));
                pool
            }
        };
        let overlay = spawn.environment.overlay(spawn.caller);
        let (stdin_read, stdin_write) = pipe().map_err(pipe_error)?;
        let (stdout_read, stdout_write) = pipe().map_err(pipe_error)?;
        let (stderr_read, stderr_write) = pipe().map_err(pipe_error)?;
        let job_id = spawn.job_id;
        let stdout =
            tokio::net::unix::pipe::Receiver::from_owned_fd(stdout_read).map_err(pipe_error)?;
        let stderr =
            tokio::net::unix::pipe::Receiver::from_owned_fd(stderr_read).map_err(pipe_error)?;
        let stdin =
            tokio::net::unix::pipe::Sender::from_owned_fd(stdin_write).map_err(pipe_error)?;
        let (stdin_sender, stdin_receiver) = mpsc::channel(1);
        tokio::spawn(run_system_stdin(
            job_id,
            stdin,
            stdin_receiver,
            events.clone(),
        ));
        tokio::spawn(run_system_output(
            job_id,
            StreamKind::Stdout,
            stdout,
            events.clone(),
        ));
        tokio::spawn(run_system_output(
            job_id,
            StreamKind::Stderr,
            stderr,
            events.clone(),
        ));
        let control = Arc::new(JobControl::default());
        let (release, released) = oneshot::channel();
        tokio::spawn(drive(
            pool,
            JobIo {
                stdin: stdin_read,
                stdout: stdout_write,
                stderr: stderr_write,
            },
            RunCommand {
                command: spawn.command,
                cwd: spawn.cwd,
                overlay,
            },
            Arc::clone(&control),
            released,
            job_id,
            events,
        ));
        Ok(Box::new(PooledProcess {
            stdin: StdinLane::new(stdin_sender),
            control,
            _release: release,
        }))
    }
}

fn pipe_error(error: io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot create the job's pipes: {error}"),
        "retry the command; check the supervisor's descriptor limit",
    )
}

/// A pipe whose ends are both close-on-exec from birth: only an explicit `dup2` or `SCM_RIGHTS`
/// hands an end to a process. A spawn that inherited an end would hold the job's input or output
/// open for its own lifetime, and the job's end of stream with it.
fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    pipe_with(|| ())
}

/// [`pipe`], running `between` once the pair exists and before it is handed out. `pipe2` makes
/// the pair close-on-exec in the one system call, so no spawn can come between.
#[cfg(target_os = "linux")]
fn pipe_with(between: impl FnOnce()) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut ends = [0; 2];
    // SAFETY: `ends` has room for the two descriptors pipe2(2) writes.
    if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both descriptors were just created and are owned here alone.
    let ends = unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
    between();
    Ok(ends)
}

/// [`pipe`], running `between` after creating the pair and before marking it close-on-exec.
/// Darwin has no `pipe2`: the two steps run under the exclusive fork lock, so no spawn forks
/// between them ([`crate::fork_lock::create`]).
#[cfg(not(target_os = "linux"))]
fn pipe_with(between: impl FnOnce()) -> io::Result<(OwnedFd, OwnedFd)> {
    crate::fork_lock::create(|| {
        let mut ends = [0; 2];
        // SAFETY: `ends` has room for the two descriptors pipe(2) writes.
        if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both descriptors were just created and are owned here alone.
        let (read, write) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        between();
        for end in [&read, &write] {
            // SAFETY: plain fcntl on an owned descriptor.
            if unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok((read, write))
    })
}

fn null_device() -> io::Result<OwnedFd> {
    Ok(OwnedFd::from(
        std::fs::OpenOptions::new().write(true).open("/dev/null")?,
    ))
}

// ---------------------------------------------------------------------------------------------
// Job control shared with the supervisor actor

/// Which process group a job's signals reach right now, and through whom.
///
/// While a fresh host activates for the job, that host's group is the job (activation output and
/// failure belong to the job): this process is the host's parent and signals it through the
/// host's reap fence ([`ChildFence`]). Once the command starts, the command's own group is: the
/// host forked the command and holds it unreaped until the job concludes, so a signal is sent to
/// the host, which applies it to the command's group. No signal is ever sent to a group whose
/// leader this process cannot vouch for. A signal requested while neither is published -- before
/// the host exists, or between its activation and the command's start -- is recorded and
/// delivered the moment one is published, so no window loses it.
#[derive(Default)]
pub(super) struct JobControl {
    target: Mutex<Target>,
    requested: AtomicI32,
}

#[derive(Default)]
enum Target {
    #[default]
    Pending,
    Host(Arc<ChildFence>),
    /// The command's host, through the run that forwards signals to it.
    Command(mpsc::UnboundedSender<i32>),
    /// The run ended: the command was released, or never started, or its host is gone.
    Finished,
}

impl Target {
    fn deliver(&self, signal: i32) -> Result<()> {
        match self {
            Self::Host(fence) => fence.signal(signal),
            Self::Command(host) => host.send(signal).map_err(|_| {
                CowshedError::conflict(
                    "the job's run no longer forwards signals to its host: the host is gone",
                    "inspect the job's status",
                )
            }),
            // Recorded in `requested`; delivered once a target is published.
            Self::Pending => Ok(()),
            Self::Finished => Err(CowshedError::conflict(
                "the job's run has ended: its command was released or never started, or its \
                 host is gone, so nothing of it is left to signal",
                "inspect the job's status",
            )),
        }
    }
}

impl JobControl {
    /// Publish where signals go from now on, delivering the one already requested, if any.
    fn publish(&self, published: Target) -> Result<()> {
        let mut target = self.target.lock().unwrap_or_else(PoisonError::into_inner);
        let requested = self.requested.load(Ordering::SeqCst);
        let delivered = if requested == 0 {
            Ok(())
        } else {
            published.deliver(requested)
        };
        *target = published;
        delivered
    }

    fn finish(&self) {
        *self.target.lock().unwrap_or_else(PoisonError::into_inner) = Target::Finished;
    }

    fn signal(&self, signal: i32) -> Result<()> {
        self.requested.store(signal, Ordering::SeqCst);
        self.target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .deliver(signal)
    }
}

struct PooledProcess {
    stdin: StdinLane,
    control: Arc<JobControl>,
    /// Dropped with the handle, once the job concluded or its supervisor went away: the run then
    /// has the host reap the command.
    _release: oneshot::Sender<()>,
}

impl RunningProcess for PooledProcess {
    fn birth(&self) -> Option<&Birth> {
        // The command's process arrives with `ProcessEvent::Started`.
        None
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.stdin.try_write(bytes)
    }

    fn close_stdin(&mut self) -> Result<()> {
        self.stdin.close()
    }

    fn end_stdin(&mut self) {
        self.stdin.end();
    }

    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
        self.control.signal(match signal {
            ProcessSignal::Term => libc::SIGTERM,
            ProcessSignal::Kill => libc::SIGKILL,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Driving one command

struct JobIo {
    stdin: OwnedFd,
    stdout: OwnedFd,
    stderr: OwnedFd,
}

struct RunCommand {
    command: HostCommand,
    cwd: PathBuf,
    overlay: Vec<(OsString, OsString)>,
}

enum Outcome {
    Exited(ExitStatus),
    /// A started command's exit was reported as it happened, and its hold has ended.
    Reported,
    /// The command ran but its end was not observed: its host is gone, and with it the only
    /// process that could prove the command's group its own, so the group was not signalled.
    Unobserved(CowshedError),
    /// No command was started; the diagnostic is already on the job's stderr.
    NotLaunched(CowshedError),
    /// The script did not parse; the host wrote the diagnostic to the job's stderr.
    ScriptSyntax,
}

fn decode(status: RawWaitStatus) -> Outcome {
    match process_termination_from_wait(Ok(std::process::ExitStatus::from_raw(status))) {
        Ok(exit) => Outcome::Exited(exit),
        Err(error) => Outcome::Unobserved(error),
    }
}

async fn drive(
    pool: ShellPool<HostActivator>,
    io: JobIo,
    command: RunCommand,
    control: Arc<JobControl>,
    released: oneshot::Receiver<()>,
    job_id: JobId,
    events: mpsc::Sender<ProcessEvent>,
) {
    // A copy of the job's stderr, for saying why no command ran. It is closed the moment the
    // command starts: while this process holds a writer, the job's stderr never ends, and the
    // job never concludes.
    let mut diagnostics = match io.stderr.try_clone() {
        Ok(stderr) => Some(std::fs::File::from(stderr)),
        Err(cause) => {
            let error = CowshedError::environment_missing(
                format!("cannot clone the job's stderr for diagnostics: {cause}"),
                "retry the command; check the supervisor's descriptor limit",
            );
            note(&Some(std::fs::File::from(io.stderr)), &error);
            control.finish();
            let _ = events
                .send(ProcessEvent::LaunchFailed { job_id, error })
                .await;
            return;
        }
    };
    let outcome = run_pooled(
        pool,
        io,
        command,
        &control,
        released,
        &mut diagnostics,
        job_id,
        &events,
    )
    .await;
    control.finish();
    let event = match outcome {
        Outcome::Exited(exit) => ProcessEvent::Exited { job_id, exit },
        Outcome::Reported => return,
        Outcome::Unobserved(error) => ProcessEvent::WaitFailed { job_id, error },
        Outcome::NotLaunched(error) => {
            note(&diagnostics, &error);
            ProcessEvent::LaunchFailed { job_id, error }
        }
        Outcome::ScriptSyntax => ProcessEvent::ScriptSyntax { job_id },
    };
    let _ = events.send(event).await;
}

/// Tell the job, on its own stderr, why its command did not run.
fn note(stderr: &Option<std::fs::File>, error: &CowshedError) {
    use std::io::Write as _;
    if let Some(mut stderr) = stderr.as_ref() {
        let _ = writeln!(stderr, "cowshed: {}", error.message);
    }
}

/// Report a signal the job asked for that did not reach its target.
async fn signal_failed(events: &mpsc::Sender<ProcessEvent>, job_id: JobId, delivered: Result<()>) {
    if let Err(error) = delivered {
        let _ = events
            .send(ProcessEvent::SignalFailed { job_id, error })
            .await;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one job's run: its pool, streams, command, control, release and event lane"
)]
async fn run_pooled(
    pool: ShellPool<HostActivator>,
    io: JobIo,
    command: RunCommand,
    control: &JobControl,
    mut released: oneshot::Receiver<()>,
    diagnostics: &mut Option<std::fs::File>,
    job_id: JobId,
    events: &mpsc::Sender<ProcessEvent>,
) -> Outcome {
    let acquired = match pool.acquire().await {
        Ok(acquired) => acquired,
        Err(error) => return Outcome::NotLaunched(error),
    };
    let mut checkout = match acquired {
        Acquired::Warm(checkout) => checkout,
        Acquired::Activate(ticket) => {
            let activator = ticket.activator();
            let mut host = match activator.spawn_host().await {
                Ok(host) => host,
                Err(error) => return Outcome::NotLaunched(error),
            };
            signal_failed(
                events,
                job_id,
                control.publish(Target::Host(Arc::clone(&host.fence))),
            )
            .await;
            let output = match (io.stdout.try_clone(), io.stderr.try_clone()) {
                (Ok(stdout), Ok(stderr)) => (stdout, stderr),
                (Err(error), _) | (_, Err(error)) => {
                    return Outcome::NotLaunched(pipe_error(error));
                }
            };
            match activator
                .activate_host(&mut host, ticket.predicted(), output)
                .await
            {
                HostActivation::Ready(evidence) => ticket.activated(host, evidence).await,
                HostActivation::Failed { status } => return decode(status),
                HostActivation::Broken(error) => {
                    // The command never ran, and the job says why. The one death that is the
                    // job's own is a signal the host took by itself while it activated for this
                    // job: a kill of the job reaches its activation, and a crash is the job's to
                    // see. A host that ended with a code, or that was still running and ended
                    // here, is a launch that failed.
                    return match host.end().await {
                        HostEnd::Own(status) if libc::WIFSIGNALED(status) => {
                            note(diagnostics, &error);
                            decode(status)
                        }
                        HostEnd::Own(_) | HostEnd::Killed => Outcome::NotLaunched(error),
                    };
                }
            }
        }
    };
    // The activation is over: a signal from here on waits for the command, which leads a group
    // of its own. Signalling the host while it forks the command would kill the host and leave
    // the command running with nothing to signal it.
    signal_failed(events, job_id, control.publish(Target::Pending)).await;
    let host = checkout.host_mut();
    let JobIo {
        stdin,
        stdout,
        stderr,
    } = io;
    // The descriptors travel to the host and are closed here once sent, so end of output is
    // decided by the command's process tree alone.
    let started = host.run(&command, [stdin, stdout, stderr]).await;
    let birth = match started {
        Ok(Started::Running(birth)) => {
            // The command runs and holds the job's streams; nothing here may.
            *diagnostics = None;
            birth
        }
        Ok(Started::Unexecutable(exit)) => return Outcome::Exited(exit),
        Ok(Started::ScriptSyntax) => return Outcome::ScriptSyntax,
        Err(error) => {
            checkout.poison();
            return Outcome::NotLaunched(error);
        }
    };
    // The host applies the job's signals to the command's group while it holds the command; the
    // run forwards them until the command is released.
    let (signals, mut forwarded) = mpsc::unbounded_channel();
    signal_failed(events, job_id, control.publish(Target::Command(signals))).await;
    let _ = events.send(ProcessEvent::Started { job_id, birth }).await;
    let exit = match host.exited(&mut forwarded, job_id, events).await {
        Ok(exit) => exit,
        Err(error) => {
            // Its host is gone, and with it the one process that could prove the command's group
            // its own: nothing signals the group from here.
            checkout.poison();
            return Outcome::Unobserved(error);
        }
    };
    let _ = events.send(ProcessEvent::Exited { job_id, exit }).await;
    // Descendants of the exited leader may run on, holding the job's output open; the leader
    // stays held so a kill still reaches them, until the job concludes and drops its handle.
    if let Err(error) = host
        .released(&mut forwarded, &mut released, job_id, events)
        .await
    {
        checkout.poison();
        let _ = events
            .send(ProcessEvent::WaitFailed { job_id, error })
            .await;
    }
    Outcome::Reported
}

// ---------------------------------------------------------------------------------------------
// The host process and its protocol, supervisor side

pub(super) struct ExecHost {
    /// This process is the host's parent and reaps it only inside this fence.
    fence: Arc<ChildFence>,
    control: tokio::net::UnixStream,
    /// Set once the host closed its end of the control socket, or announced that it exits.
    closed: bool,
}

/// A dropped host's control socket closes with it. An idle host then exits; one activating ends
/// its activation's group; one holding a command ends the command's group -- SIGTERM, a grace,
/// SIGKILL -- reaps it, and exits. The host is not signalled here: a command it holds leads a
/// group of its own, which only the host can still prove the command's. It is reaped in the
/// background, through its fence.
impl Drop for ExecHost {
    fn drop(&mut self) {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let fence = Arc::clone(&self.fence);
            runtime.spawn(async move {
                let _ = fence.wait().await;
            });
        }
    }
}

enum Started {
    /// The command runs; the host observed its group leader before it could reap it.
    Running(Birth),
    /// The argv could not be executed; the host already wrote why to the job's stderr.
    Unexecutable(ExitStatus),
    /// The script did not parse; the host already wrote why to the job's stderr.
    ScriptSyntax,
}

/// The first fields of a script request: its text and its bindings.
fn script_request(script: &crate::script::RenderedScript) -> io::Result<FrameWriter> {
    let count = u32::try_from(script.bindings.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many script bindings"))?;
    let mut frame = FrameWriter::new(REQUEST_SCRIPT)
        .bytes(script.text.as_bytes())?
        .u32(count);
    for binding in &script.bindings {
        frame = match binding {
            crate::script::Binding::Scalar { name, value } => frame
                .bytes(name.as_bytes())?
                .u32(u32::from(BINDING_SCALAR))
                .list(std::iter::once(value.as_bytes()))?,
            crate::script::Binding::Array { name, values } => frame
                .bytes(name.as_bytes())?
                .u32(u32::from(BINDING_ARRAY))
                .list(values.iter().map(String::as_bytes))?,
        };
    }
    Ok(frame)
}

fn protocol_error(what: &str, error: impl std::fmt::Display) -> CowshedError {
    CowshedError::internal(format!("workspace exec host {what}: {error}"))
}

/// Whether an I/O error on the control socket means the host has closed its end: it is
/// exiting, and its own wait status says how.
fn host_closed(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
    )
}

/// How a host that broke its protocol ended.
enum HostEnd {
    /// It ended by itself, with this wait status.
    Own(RawWaitStatus),
    /// It was still running and was killed here.
    Killed,
}

impl ExecHost {
    async fn send(&mut self, frame: Vec<u8>, descriptors: &[RawFd]) -> Result<()> {
        let socket = self.control.as_raw_fd();
        let sent = self
            .control
            .async_io(Interest::WRITABLE, || {
                send_with_descriptors(socket, &frame, descriptors)
            })
            .await;
        let written = match sent {
            Ok(sent) => self.control.write_all(&frame[sent..]).await,
            Err(error) => Err(error),
        };
        written.map_err(|error| {
            self.closed |= host_closed(&error);
            protocol_error("send", error)
        })
    }

    async fn receive(&mut self) -> Result<Vec<u8>> {
        receive_frame(&mut self.control, &mut self.closed).await
    }

    async fn run(&mut self, command: &RunCommand, descriptors: [OwnedFd; 3]) -> Result<Started> {
        let frame = match &command.command {
            HostCommand::Argv(argv) => {
                FrameWriter::new(REQUEST_RUN).list(argv.iter().map(|argument| argument.as_bytes()))
            }
            HostCommand::Script(script) => script_request(script),
        };
        let frame = frame
            .and_then(|frame| frame.bytes(command.cwd.as_os_str().as_bytes()))
            .and_then(|frame| {
                frame.list(
                    command
                        .overlay
                        .iter()
                        .flat_map(|(name, value)| [name.as_bytes(), value.as_bytes()])
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            })
            .and_then(FrameWriter::finish)
            .map_err(|error| protocol_error("request", error))?;
        let raw = descriptors
            .each_ref()
            .map(|descriptor| descriptor.as_raw_fd());
        self.send(frame, &raw).await?;
        drop(descriptors);
        let payload = self.receive().await?;
        let (tag, mut fields) =
            FrameReader::new(&payload).map_err(|error| protocol_error("reply", error))?;
        let started = match tag {
            REPLY_STARTED => {
                Started::Running(fields.birth().map_err(|e| protocol_error("reply", e))?)
            }
            REPLY_EXITED => {
                Started::Unexecutable(fields.exit().map_err(|e| protocol_error("reply", e))?)
            }
            REPLY_SCRIPT_SYNTAX => {
                fields.bytes().map_err(|e| protocol_error("reply", e))?;
                Started::ScriptSyntax
            }
            other => return Err(protocol_error("reply", format!("unexpected tag {other}"))),
        };
        fields
            .finish()
            .map_err(|error| protocol_error("reply", error))?;
        Ok(started)
    }

    /// Wait for the command's leader to exit, forwarding the job's signals meanwhile.
    async fn exited(
        &mut self,
        signals: &mut mpsc::UnboundedReceiver<i32>,
        job_id: JobId,
        events: &mpsc::Sender<ProcessEvent>,
    ) -> Result<ExitStatus> {
        let payload = self.next_reply(signals, None, job_id, events).await?;
        let (tag, mut fields) =
            FrameReader::new(&payload).map_err(|error| protocol_error("reply", error))?;
        if tag != REPLY_EXITED {
            return Err(protocol_error("reply", format!("unexpected tag {tag}")));
        }
        let exit = fields
            .exit()
            .map_err(|error| protocol_error("reply", error))?;
        fields
            .finish()
            .map_err(|error| protocol_error("reply", error))?;
        Ok(exit)
    }

    /// Keep forwarding the job's signals to its exited command's group until `release` resolves,
    /// then have the host reap the command.
    async fn released(
        &mut self,
        signals: &mut mpsc::UnboundedReceiver<i32>,
        release: &mut oneshot::Receiver<()>,
        job_id: JobId,
        events: &mpsc::Sender<ProcessEvent>,
    ) -> Result<()> {
        let payload = self
            .next_reply(signals, Some(release), job_id, events)
            .await?;
        let (tag, fields) =
            FrameReader::new(&payload).map_err(|error| protocol_error("reply", error))?;
        if tag != REPLY_RELEASED {
            return Err(protocol_error("reply", format!("unexpected tag {tag}")));
        }
        fields
            .finish()
            .map_err(|error| protocol_error("reply", error))
    }

    /// The host's next reply about the held command, forwarding the job's signals meanwhile and
    /// reporting each the host could not deliver ([`REPLY_SIGNAL_FAILED`]). With a `release`, the
    /// command is released once it resolves: no signal follows the release request.
    async fn next_reply(
        &mut self,
        signals: &mut mpsc::UnboundedReceiver<i32>,
        release: Option<&mut oneshot::Receiver<()>>,
        job_id: JobId,
        events: &mpsc::Sender<ProcessEvent>,
    ) -> Result<Vec<u8>> {
        let mut closed = false;
        let payload = {
            let (mut replies, mut requests) = self.control.split();
            let release = async move {
                match release {
                    // A dropped sender is a release as much as a sent one.
                    Some(release) => {
                        let _ = release.await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(release);
            let mut releasing = true;
            let mut forwarding = true;
            loop {
                let reply = receive_frame(&mut replies, &mut closed);
                tokio::pin!(reply);
                let payload = loop {
                    tokio::select! {
                        payload = &mut reply => break payload,
                        () = &mut release, if releasing => {
                            releasing = false;
                            forwarding = false;
                            let sent = match FrameWriter::new(REQUEST_RELEASE).finish() {
                                Ok(frame) => requests.write_all(&frame).await,
                                Err(error) => Err(error),
                            };
                            if let Err(error) = sent {
                                // The reply says how the host ended.
                                eprintln!(
                                    "cowshed: cannot release job {}'s command to its host: \
                                     {error}",
                                    job_id.get()
                                );
                            }
                        }
                        signal = signals.recv(), if forwarding => match signal {
                            Some(signal) => {
                                let sent = match FrameWriter::new(REQUEST_SIGNAL).i32(signal).finish() {
                                    Ok(frame) => requests.write_all(&frame).await,
                                    Err(error) => Err(error),
                                };
                                if let Err(error) = sent {
                                    // The reply says how the host ended; the signal reached
                                    // nothing.
                                    forwarding = false;
                                    let _ = events
                                        .send(ProcessEvent::SignalFailed {
                                            job_id,
                                            error: protocol_error("signal", error),
                                        })
                                        .await;
                                }
                            }
                            None => forwarding = false,
                        },
                    }
                };
                let payload = match payload {
                    Ok(payload) => payload,
                    Err(error) => break Err(error),
                };
                match signal_failure(&payload) {
                    Some(error) => {
                        let _ = events
                            .send(ProcessEvent::SignalFailed { job_id, error })
                            .await;
                    }
                    None => break Ok(payload),
                }
            }
        };
        self.closed |= closed;
        payload
    }

    /// End a host that broke its protocol. One that closed its end is exiting and is waited
    /// for; one still running is killed, and its death is this call's, not the job's.
    async fn end(&mut self) -> HostEnd {
        if !self.closed {
            match self.fence.try_reap() {
                Ok(Some(status)) => return HostEnd::Own(status.into_raw()),
                Ok(None) => {
                    let _ = self.fence.signal(libc::SIGKILL);
                    let _ = self.fence.wait().await;
                    return HostEnd::Killed;
                }
                // Not reapable: something else collected the host, so its id may name another
                // process and nothing is signalled.
                Err(_) => return HostEnd::Killed,
            }
        }
        match self.fence.wait().await {
            Ok(status) => HostEnd::Own(status.into_raw()),
            Err(_) => HostEnd::Killed,
        }
    }
}

/// The failure a [`REPLY_SIGNAL_FAILED`] reply names; `None` for any other reply.
fn signal_failure(payload: &[u8]) -> Option<CowshedError> {
    let (REPLY_SIGNAL_FAILED, mut fields) = FrameReader::new(payload).ok()? else {
        return None;
    };
    Some(match (fields.i32(), fields.bytes()) {
        (Ok(signal), Ok(reason)) => CowshedError::environment_missing(
            format!(
                "the workspace shell host could not deliver signal {signal} to the job's \
                 process group: {}",
                String::from_utf8_lossy(reason)
            ),
            "inspect the job's processes",
        ),
        (Err(error), _) | (_, Err(error)) => protocol_error("reply", error),
    })
}

/// One reply frame from a host's control socket. A host that names why it failed is closed, and
/// so is one whose end of the socket is gone.
async fn receive_frame<R: tokio::io::AsyncRead + Unpin>(
    control: &mut R,
    closed: &mut bool,
) -> Result<Vec<u8>> {
    let mut header = [0_u8; 4];
    if let Err(error) = control.read_exact(&mut header).await {
        *closed |= host_closed(&error);
        return Err(protocol_error("reply", error));
    }
    let length = usize::try_from(u32::from_le_bytes(header)).unwrap_or(usize::MAX);
    if length > MAX_FRAME_BYTES {
        return Err(protocol_error("reply", "frame exceeds its bound"));
    }
    let mut payload = vec![0_u8; length];
    if let Err(error) = control.read_exact(&mut payload).await {
        *closed |= host_closed(&error);
        return Err(protocol_error("reply", error));
    }
    // The host names why it could not serve the request, then exits.
    if let Ok((REPLY_HOST_FAILED, mut fields)) = FrameReader::new(&payload) {
        *closed = true;
        let reason = fields
            .bytes()
            .map(|reason| String::from_utf8_lossy(reason).into_owned())
            .unwrap_or_else(|error| format!("an unreadable reason ({error})"));
        return Err(CowshedError::environment_missing(
            format!("the workspace shell host failed: {reason}"),
            "cowshed doctor --json",
        ));
    }
    Ok(payload)
}

enum HostActivation {
    Ready(std::result::Result<ActivationEvidence, String>),
    Failed { status: RawWaitStatus },
    Broken(CowshedError),
}

/// Starts exec hosts for one shell identity and activates them.
pub(super) struct HostActivator {
    program: ShellHostProgram,
    workspace_mount: PathBuf,
    profile: String,
    environment: BTreeMap<OsString, OsString>,
    /// `None` for a workspace with no shell configuration: its hosts hold the sandbox
    /// environment itself, which no workspace file can make stale.
    envrc_directory: Option<PathBuf>,
}

impl HostActivator {
    /// Where the host starts: the `.envrc`'s directory, or the workspace root.
    fn home(&self) -> &Path {
        self.envrc_directory
            .as_deref()
            .unwrap_or(&self.workspace_mount)
    }

    async fn spawn_host(&self) -> Result<ExecHost> {
        let mount = self.workspace_mount.clone();
        let program = self.program.clone();
        let staged = tokio::task::spawn_blocking(move || stage_host_executable(&mount, &program))
            .await
            .map_err(|error| CowshedError::internal(format!("staging task failed: {error}")))??;
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().map_err(pipe_error)?;
        let theirs_raw = theirs.as_raw_fd();
        let mut command = tokio::process::Command::new(SANDBOX_EXEC);
        command
            .arg("-p")
            .arg(&self.profile)
            .arg("--")
            .arg(&staged)
            .args(self.program.arguments())
            .env_clear()
            .envs(&self.environment)
            .env("PWD", self.home())
            .current_dir(self.home())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        prepare_child_descriptors(command.as_std_mut())
            .map_err(ExecError::from)
            .map_err(super::supervisor::map_exec_error)?;
        // SAFETY: runs between fork and exec; setpgid, dup2 and fcntl are async-signal-safe and
        // touch no memory the closure captures beyond one integer. The host leads its own group
        // so a command's group kill never reaches it, and a kill during activation reaches the
        // direnv/nix processes it started. `prepare_child_descriptors` above marked every
        // non-stdio descriptor close-on-exec; dup2 yields a descriptor 3 without that flag.
        unsafe {
            command.pre_exec(move || {
                if libc::setpgid(0, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                let installed = if theirs_raw == super::shell_host::CONTROL_DESCRIPTOR {
                    libc::fcntl(theirs_raw, libc::F_SETFD, 0)
                } else {
                    libc::dup2(theirs_raw, super::shell_host::CONTROL_DESCRIPTOR)
                };
                if installed == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        // Through `std`: this process alone reaps the host, inside its fence ([`ChildFence`]).
        let child = command
            .into_std()
            .spawn_locked()
            .map_err(classify_spawn_error)
            .map_err(ExecError::from)
            .map_err(super::supervisor::map_exec_error)?;
        drop(theirs);
        let fence = Arc::new(ChildFence::new(child.id())?);
        ours.set_nonblocking(true).map_err(pipe_error)?;
        let control = tokio::net::UnixStream::from_std(ours).map_err(pipe_error)?;
        Ok(ExecHost {
            fence,
            control,
            closed: false,
        })
    }

    /// Approve and evaluate the `.envrc` in `host`, writing activation output to `output`.
    async fn activate_host(
        &self,
        host: &mut ExecHost,
        predicted: Vec<PathBuf>,
        (stdout, stderr): (OwnedFd, OwnedFd),
    ) -> HostActivation {
        let Some(envrc_directory) = &self.envrc_directory else {
            // Nothing to evaluate and nothing to watch: the generation never goes stale.
            return HostActivation::Ready(Ok(ActivationEvidence {
                entries: Vec::new(),
                before: Snapshot::default(),
                clock: None,
                after: Snapshot::default(),
            }));
        };
        let envrc = envrc_directory.join(".envrc");
        let request = FrameWriter::new(REQUEST_APPROVE)
            .bytes(envrc.as_os_str().as_bytes())
            .and_then(FrameWriter::finish);
        let request = match request {
            Ok(request) => request,
            Err(error) => return HostActivation::Broken(protocol_error("request", error)),
        };
        let evaluation_stderr = match stderr.try_clone() {
            Ok(stderr) => stderr,
            Err(error) => return HostActivation::Broken(pipe_error(error)),
        };
        if let Err(error) = host
            .send(request, &[stdout.as_raw_fd(), stderr.as_raw_fd()])
            .await
        {
            return HostActivation::Broken(error);
        }
        drop((stdout, stderr));
        match expect_status(host, REPLY_APPROVED).await {
            Ok(0) => {}
            Ok(status) => return HostActivation::Failed { status },
            Err(error) => return HostActivation::Broken(error),
        }
        // Taken after approval, which rewrites direnv's allow file, and before evaluation. The
        // start is read off the workspace filesystem's clocks, which stamp the inputs; waiting
        // for the slowest to move past the approval costs at most one stamping tick.
        let before = Snapshot::take(predicted.iter().map(PathBuf::as_path));
        let clock = self.workspace_mount.join(SHELL_HOST_DIRECTORY);
        let start_clock = clock.clone();
        let started =
            match tokio::task::spawn_blocking(move || FsSeparation::take(&start_clock)).await {
                Ok(Ok(started)) => Ok(started),
                Ok(Err(error)) => Err(format!(
                    "cannot read the workspace filesystem's clock: {error}"
                )),
                Err(error) => Err(format!("the filesystem clock task failed: {error}")),
            };
        let request = FrameWriter::new(REQUEST_ACTIVATE)
            .bytes(envrc_directory.as_os_str().as_bytes())
            .and_then(FrameWriter::finish);
        let request = match request {
            Ok(request) => request,
            Err(error) => return HostActivation::Broken(protocol_error("request", error)),
        };
        if let Err(error) = host.send(request, &[evaluation_stderr.as_raw_fd()]).await {
            return HostActivation::Broken(error);
        }
        drop(evaluation_stderr);
        let payload = match host.receive().await {
            Ok(payload) => payload,
            Err(error) => return HostActivation::Broken(error),
        };
        let parsed = FrameReader::new(&payload).and_then(|(tag, mut fields)| {
            let reply = match tag {
                REPLY_ACTIVATION_EXITED => ActivateReply::Exited(fields.i32()?),
                REPLY_ACTIVATION_UNUSABLE => {
                    ActivateReply::Unusable(String::from_utf8_lossy(fields.bytes()?).into_owned())
                }
                REPLY_ACTIVATED => {
                    let watches = match fields.u32()? {
                        0 => None,
                        _ => Some(String::from_utf8_lossy(fields.bytes()?).into_owned()),
                    };
                    ActivateReply::Activated(watches)
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected tag {other}"),
                    ));
                }
            };
            fields.finish()?;
            Ok(reply)
        });
        match parsed {
            Err(error) => HostActivation::Broken(protocol_error("reply", error)),
            Ok(ActivateReply::Exited(status)) => HostActivation::Failed { status },
            Ok(ActivateReply::Unusable(reason)) => {
                HostActivation::Broken(CowshedError::environment_missing(
                    format!("workspace shell activation produced no usable environment: {reason}"),
                    "repair the workspace .envrc and retry",
                ))
            }
            Ok(ActivateReply::Activated(None)) => {
                HostActivation::Ready(Err("the activation recorded no DIRENV_WATCHES".into()))
            }
            Ok(ActivateReply::Activated(Some(watches))) => HostActivation::Ready(
                decode_direnv_watches(&watches)
                    .map_err(|error| error.to_string())
                    .and_then(|entries| {
                        let after =
                            Snapshot::take(entries.iter().map(|entry| entry.path.as_path()));
                        // Read again once the inputs are snapshotted: a clock that ran backwards
                        // across the evaluation orders no input against its start.
                        let finished = FsInstant::read(&clock).map_err(|error| {
                            format!("cannot read the workspace filesystem's clock: {error}")
                        })?;
                        Ok(ActivationEvidence {
                            entries,
                            before,
                            clock: Some(EvaluationClock {
                                started: started?,
                                finished,
                            }),
                            after,
                        })
                    }),
            ),
        }
    }
}

enum ActivateReply {
    Exited(RawWaitStatus),
    Unusable(String),
    Activated(Option<String>),
}

async fn expect_status(host: &mut ExecHost, expected: u8) -> Result<RawWaitStatus> {
    let payload = host.receive().await?;
    let (tag, mut fields) =
        FrameReader::new(&payload).map_err(|error| protocol_error("reply", error))?;
    if tag != expected {
        return Err(protocol_error("reply", format!("unexpected tag {tag}")));
    }
    let status = fields
        .i32()
        .map_err(|error| protocol_error("reply", error))?;
    fields
        .finish()
        .map_err(|error| protocol_error("reply", error))?;
    Ok(status)
}

#[async_trait]
impl Activator for HostActivator {
    type Host = ExecHost;
    type Output = (OwnedFd, OwnedFd);

    /// A spare's activation output has no job to belong to and is discarded; its failure only
    /// means no spare exists, and the next command activates for itself.
    async fn activate(
        &self,
        predicted: Vec<PathBuf>,
        output: Option<(OwnedFd, OwnedFd)>,
    ) -> Activation<ExecHost> {
        let output = match output {
            Some(output) => output,
            None => match (null_device(), null_device()) {
                (Ok(stdout), Ok(stderr)) => (stdout, stderr),
                (Err(error), _) | (_, Err(error)) => {
                    return Activation::Broken {
                        error: pipe_error(error),
                    };
                }
            },
        };
        let mut host = match self.spawn_host().await {
            Ok(host) => host,
            Err(error) => return Activation::Broken { error },
        };
        match self.activate_host(&mut host, predicted, output).await {
            HostActivation::Ready(evidence) => Activation::Ready { host, evidence },
            HostActivation::Failed { status } => Activation::Failed { status },
            HostActivation::Broken(error) => Activation::Broken { error },
        }
    }
}

/// Copy the host program into the workspace's protected host directory, content-addressed by
/// the source file's identity, and drop every other version there.
///
/// The program is copied rather than run in place because the executed-child profile may deny
/// reading where the controller's binary lives (the project root and every workspace mount are
/// denied to a sibling workspace).
fn stage_host_executable(mount: &Path, program: &ShellHostProgram) -> Result<PathBuf> {
    use std::os::unix::fs::MetadataExt as _;
    let staging_error = |what: &str, error: io::Error| {
        CowshedError::environment_missing(
            format!("cannot stage the workspace exec host ({what}): {error}"),
            "check the controller's cowshed binary and the workspace metadata directory",
        )
    };
    let source = program.executable();
    let metadata = std::fs::metadata(source).map_err(|error| staging_error("inspect", error))?;
    let name = format!(
        "{:x}-{:x}-{:x}-{:x}.{:x}",
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.mtime(),
        metadata.mtime_nsec()
    );
    let directory = mount.join(SHELL_HOST_DIRECTORY);
    crate::storage::verify_no_symlinks(mount, &mount.join(".cowshed")).map_err(|error| {
        CowshedError::integrity(
            format!("unsafe workspace metadata directory: {error}"),
            "repair the workspace metadata directory and reattach",
        )
    })?;
    std::fs::create_dir_all(&directory).map_err(|error| staging_error("directory", error))?;
    let target = directory.join(&name);
    if std::fs::symlink_metadata(&target)
        .is_ok_and(|staged| staged.is_file() && staged.size() == metadata.size())
    {
        return Ok(target);
    }
    let temporary = directory.join(format!(".{name}.{}", uuid::Uuid::new_v4().simple()));
    std::fs::copy(source, &temporary).map_err(|error| staging_error("copy", error))?;
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o555))
        .map_err(|error| staging_error("mode", error))?;
    std::fs::rename(&temporary, &target).map_err(|error| staging_error("publish", error))?;
    if let Ok(entries) = std::fs::read_dir(&directory) {
        for entry in entries.flatten() {
            if entry.file_name() != target.file_name().unwrap_or_default()
                && !entry.file_name().as_bytes().starts_with(b".")
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::os::fd::{AsRawFd as _, OwnedFd, RawFd};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;

    use super::pipe_with;
    use crate::fork_lock::{Spawn as _, held_exclusive};

    /// The kernel's identity of pipe end `descriptor` in process `pid`: the pipe's inode, which
    /// both ends share.
    #[cfg(target_os = "linux")]
    fn pipe_end(pid: u32, descriptor: RawFd) -> Option<u64> {
        let link = std::fs::read_link(format!("/proc/{pid}/fd/{descriptor}")).ok()?;
        link.to_str()?
            .strip_prefix("pipe:[")?
            .strip_suffix(']')?
            .parse()
            .ok()
    }

    /// Every descriptor of process `pid`, as the kernel lists it.
    #[cfg(target_os = "linux")]
    fn descriptors(pid: u32) -> Vec<RawFd> {
        std::fs::read_dir(format!("/proc/{pid}/fd"))
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect()
    }

    /// `struct pipe_fdinfo` of `<sys/proc_info.h>`, which the libc crate does not declare.
    #[cfg(target_os = "macos")]
    #[repr(C)]
    struct PipeFdInfo {
        fi_openflags: u32,
        fi_status: u32,
        fi_offset: libc::off_t,
        fi_type: i32,
        fi_guardflags: u32,
        pipe_stat: libc::vinfo_stat,
        pipe_handle: u64,
        pipe_peerhandle: u64,
        pipe_status: libc::c_int,
        rfu_1: libc::c_int,
    }

    /// `PROC_PIDFDPIPEINFO` of `<sys/proc_info.h>`.
    #[cfg(target_os = "macos")]
    const PROC_PIDFDPIPEINFO: libc::c_int = 6;

    /// The kernel's identity of pipe end `descriptor` in process `pid`: its pipe handle.
    #[cfg(target_os = "macos")]
    fn pipe_end(pid: u32, descriptor: RawFd) -> Option<u64> {
        let mut info = std::mem::MaybeUninit::<PipeFdInfo>::uninit();
        let size = libc::c_int::try_from(size_of::<PipeFdInfo>()).unwrap();
        // SAFETY: `info` provides `size` writable bytes.
        let written = unsafe {
            libc::proc_pidfdinfo(
                libc::pid_t::try_from(pid).unwrap(),
                descriptor,
                PROC_PIDFDPIPEINFO,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        // SAFETY: proc_pidfdinfo filled all `size` bytes.
        (written == size).then(|| unsafe { info.assume_init() }.pipe_handle)
    }

    /// Every descriptor of process `pid`, as the kernel lists it.
    #[cfg(target_os = "macos")]
    fn descriptors(pid: u32) -> Vec<RawFd> {
        let pid = libc::pid_t::try_from(pid).unwrap();
        let entry = size_of::<libc::proc_fdinfo>();
        let mut listed = vec![
            libc::proc_fdinfo {
                proc_fd: 0,
                proc_fdtype: 0
            };
            256
        ];
        let capacity = libc::c_int::try_from(listed.len() * entry).unwrap();
        // SAFETY: `listed` provides `capacity` writable bytes.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                listed.as_mut_ptr().cast(),
                capacity,
            )
        };
        assert!(
            written > 0 && written < capacity,
            "list the descriptors of {pid}"
        );
        listed.truncate(usize::try_from(written).unwrap() / entry);
        listed.into_iter().map(|listed| listed.proc_fd).collect()
    }

    /// Whether this process reads end of stream from `read` right now, without waiting.
    fn at_end_of_stream(read: &OwnedFd) -> bool {
        // SAFETY: plain fcntl on an owned descriptor.
        let flags = unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFL) };
        // SAFETY: plain fcntl on an owned descriptor.
        assert_eq!(
            unsafe { libc::fcntl(read.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let mut byte = 0_u8;
        // SAFETY: `byte` has room for the one byte asked for.
        match unsafe { libc::read(read.as_raw_fd(), (&raw mut byte).cast(), 1) } {
            0 => true,
            _ => {
                let error = io::Error::last_os_error();
                assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
                false
            }
        }
    }

    /// A child spawned while [`pipe`](super::pipe) stands between creating a job's pair and
    /// marking it close-on-exec inherits no end of it, so the job's stream ends the moment the
    /// supervisor closes its writer, not when that unrelated child exits.
    #[test]
    fn a_child_spawned_while_a_job_pipe_is_born_inherits_no_end_of_it() {
        let (spawned, spawn_returned) = mpsc::channel();
        let mut spawn = None;
        let (read, write) = pipe_with(|| {
            // A fenced window makes the spawn wait for it; an open one lets it fork right here,
            // and the test waits for that fork to exec before the pair is marked.
            let fenced = held_exclusive();
            spawn = Some(std::thread::spawn(move || {
                // Blocks reading its stdin, which the test holds, until the test ends it.
                let child = Command::new("/bin/sh")
                    .args(["-c", "read -r line || :"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn_locked();
                spawned.send(()).unwrap();
                child
            }));
            if !fenced {
                spawn_returned.recv().unwrap();
            }
        })
        .unwrap();
        let mut child = spawn.unwrap().join().unwrap().unwrap();
        let ours = [
            pipe_end(std::process::id(), read.as_raw_fd()).unwrap(),
            pipe_end(std::process::id(), write.as_raw_fd()).unwrap(),
        ];
        let inherited: Vec<RawFd> = descriptors(child.id())
            .into_iter()
            .filter(|&descriptor| {
                pipe_end(child.id(), descriptor).is_some_and(|end| ours.contains(&end))
            })
            .collect();
        drop(write);
        let ended = at_end_of_stream(&read);
        drop(child.stdin.take());
        assert!(child.wait().unwrap().success());
        assert_eq!(
            inherited,
            Vec::<RawFd>::new(),
            "the unrelated child holds these ends of the job's pipe"
        );
        assert!(
            ended,
            "the job's stream ends once the supervisor closes its writer"
        );
    }
}

#[cfg(test)]
#[path = "shell_job_diagnostics_tests.rs"]
mod diagnostics_tests;
