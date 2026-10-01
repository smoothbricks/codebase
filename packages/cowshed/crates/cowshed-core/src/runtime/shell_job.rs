//! Commands that run in warm exec hosts: the supervisor's side of [`super::shell_host`].
//!
//! A job keeps every part of its contract: its own anonymous stdin/stdout/stderr pipes, pumped
//! and quota-accounted by the supervisor exactly as for a one-shot child; its own process group
//! (the host forks the argv with `setpgid`); its exact wait status (the host reports the raw
//! `waitpid` status); and a kill that reaches its group — never the host's — unless the host
//! itself is still activating for that job.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, Interest};
use tokio::sync::mpsc;

use super::shell_host::{
    BINDING_ARRAY, BINDING_SCALAR, FrameReader, FrameWriter, MAX_FRAME_BYTES, REPLY_ACTIVATED,
    REPLY_ACTIVATION_EXITED, REPLY_ACTIVATION_UNUSABLE, REPLY_APPROVED, REPLY_EXITED,
    REPLY_HOST_FAILED, REPLY_SCRIPT_SYNTAX, REPLY_STARTED, REQUEST_ACTIVATE, REQUEST_APPROVE,
    REQUEST_RUN, REQUEST_SCRIPT, RawWaitStatus, SHELL_HOST_DIRECTORY, ShellHostProgram,
    send_with_descriptors,
};
use super::shell_pool::{Acquired, Activation, Activator, ShellPool, ShellPoolConfig};
use super::shell_watch::{
    ActivationEvidence, EvaluationClock, FsInstant, Snapshot, decode_direnv_watches,
};
use super::supervisor::{
    ProcessEvent, ProcessSignal, RunningProcess, SandboxEnvironment, StdinLane, kill_process_group,
    process_termination_from_wait, run_system_output, run_system_stdin,
};
use crate::api::dto::{ExitStatus, JobId, Sha256Digest};
use crate::error::{CowshedError, Result};
use crate::exec::{ExecError, SANDBOX_EXEC, classify_spawn_error, prepare_child_descriptors};
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
            job_id,
            events,
        ));
        Ok(Box::new(PooledProcess {
            stdin: StdinLane::new(stdin_sender),
            control,
        }))
    }
}

fn pipe_error(error: io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot create the job's pipes: {error}"),
        "retry the command; check the supervisor's descriptor limit",
    )
}

/// A pipe whose ends are both close-on-exec: only an explicit `dup2` or `SCM_RIGHTS` hands an
/// end to a process.
fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut ends = [0; 2];
    // SAFETY: `ends` has room for the two descriptors pipe(2) writes.
    if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both descriptors were just created and are owned here alone.
    let (read, write) = unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
    for end in [&read, &write] {
        // SAFETY: plain fcntl on an owned descriptor.
        if unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok((read, write))
}

fn null_device() -> io::Result<OwnedFd> {
    Ok(OwnedFd::from(
        std::fs::OpenOptions::new().write(true).open("/dev/null")?,
    ))
}

// ---------------------------------------------------------------------------------------------
// Job control shared with the supervisor actor

const PHASE_MASK: u64 = !0xffff_ffff;
const ACTIVATING: u64 = 1 << 32;
const RUNNING: u64 = 2 << 32;
const FINISHED: u64 = 3 << 32;

/// Which process group a job's signals reach right now, lock-free.
///
/// While a fresh host activates for the job, that host's group is the job (activation output
/// and failure belong to the job); once the command starts, the command's own group is. A
/// signal requested before either exists is recorded and delivered the moment one is
/// published, so no window loses it.
#[derive(Default)]
pub(super) struct JobControl {
    target: AtomicU64,
    requested: AtomicI32,
}

impl JobControl {
    fn publish(&self, phase: u64, pid: u32) {
        self.target.store(phase | u64::from(pid), Ordering::SeqCst);
        let requested = self.requested.load(Ordering::SeqCst);
        if requested != 0 {
            let _ = kill_process_group(pid, requested);
        }
    }

    fn finish(&self) {
        self.target.store(FINISHED, Ordering::SeqCst);
    }

    fn signal(&self, signal: i32) -> Result<()> {
        self.requested.store(signal, Ordering::SeqCst);
        let target = self.target.load(Ordering::SeqCst);
        match target & PHASE_MASK {
            ACTIVATING | RUNNING => {
                kill_process_group(u32::try_from(target & 0xffff_ffff).unwrap_or(0), signal)
            }
            _ => Ok(()),
        }
    }
}

struct PooledProcess {
    stdin: StdinLane,
    control: Arc<JobControl>,
}

impl RunningProcess for PooledProcess {
    fn pid(&self) -> Option<u32> {
        // The command's pid arrives with `ProcessEvent::Started`.
        None
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.stdin.try_write(bytes)
    }

    fn close_stdin(&mut self) -> Result<()> {
        self.stdin.close()
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
    /// The command ran but its end was not observed; its group has been killed.
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
    job_id: JobId,
    events: mpsc::Sender<ProcessEvent>,
) {
    let diagnostics = io.stderr.try_clone().ok();
    let outcome = run_pooled(pool, io, command, &control, job_id, &events).await;
    control.finish();
    let event = match outcome {
        Outcome::Exited(exit) => ProcessEvent::Exited { job_id, exit },
        Outcome::Unobserved(error) => ProcessEvent::WaitFailed { job_id, error },
        Outcome::NotLaunched(error) => {
            if let Some(diagnostics) = diagnostics {
                use std::io::Write as _;
                let _ = writeln!(
                    std::fs::File::from(diagnostics),
                    "cowshed: {}",
                    error.message
                );
            }
            ProcessEvent::LaunchFailed { job_id, error }
        }
        Outcome::ScriptSyntax => ProcessEvent::ScriptSyntax { job_id },
    };
    let _ = events.send(event).await;
}

/// Tell the job, on its own stderr, why its command did not run.
fn note(stderr: &Option<OwnedFd>, error: &CowshedError) {
    use std::io::Write as _;
    if let Some(stderr) = stderr
        && let Ok(stderr) = stderr.try_clone()
    {
        let _ = writeln!(std::fs::File::from(stderr), "cowshed: {}", error.message);
    }
}

async fn run_pooled(
    pool: ShellPool<HostActivator>,
    io: JobIo,
    command: RunCommand,
    control: &JobControl,
    job_id: JobId,
    events: &mpsc::Sender<ProcessEvent>,
) -> Outcome {
    let diagnostics = io.stderr.try_clone().ok();
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
            control.publish(ACTIVATING, host.pid);
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
                            note(&diagnostics, &error);
                            decode(status)
                        }
                        HostEnd::Own(_) | HostEnd::Killed => Outcome::NotLaunched(error),
                    };
                }
            }
        }
    };
    let host = checkout.host_mut();
    let JobIo {
        stdin,
        stdout,
        stderr,
    } = io;
    // The descriptors travel to the host and are closed here once sent, so end of output is
    // decided by the command's process tree alone.
    let started = host.run(&command, [stdin, stdout, stderr]).await;
    let pid = match started {
        Ok(Started::Running(pid)) => pid,
        Ok(Started::Unexecutable(status)) => return decode(status),
        Ok(Started::ScriptSyntax) => return Outcome::ScriptSyntax,
        Err(error) => {
            checkout.poison();
            return Outcome::NotLaunched(error);
        }
    };
    control.publish(RUNNING, pid);
    let _ = events.send(ProcessEvent::Started { job_id, pid }).await;
    match host.exited().await {
        Ok(status) => decode(status),
        Err(error) => {
            checkout.poison();
            let _ = kill_process_group(pid, libc::SIGKILL);
            Outcome::Unobserved(error)
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The host process and its protocol, supervisor side

pub(super) struct ExecHost {
    child: tokio::process::Child,
    pid: u32,
    control: tokio::net::UnixStream,
    /// Set once the host's own exit status was collected: its pid, and so its group id, may
    /// then belong to another process, which no signal of ours may reach.
    reaped: bool,
    /// Set once the host closed its end of the control socket, or announced that it exits.
    closed: bool,
}

/// A dropped host takes its activation with it: the host leads its own process group, which
/// holds `direnv` and whatever the evaluation started, while every command it forked leads a
/// group of its own and keeps running.
impl Drop for ExecHost {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = kill_process_group(self.pid, libc::SIGKILL);
        }
    }
}

enum Started {
    Running(u32),
    /// The argv could not be executed; the host already wrote why to the job's stderr.
    Unexecutable(RawWaitStatus),
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
        let mut header = [0_u8; 4];
        if let Err(error) = self.control.read_exact(&mut header).await {
            self.closed |= host_closed(&error);
            return Err(protocol_error("reply", error));
        }
        let length = usize::try_from(u32::from_le_bytes(header)).unwrap_or(usize::MAX);
        if length > MAX_FRAME_BYTES {
            return Err(protocol_error("reply", "frame exceeds its bound"));
        }
        let mut payload = vec![0_u8; length];
        if let Err(error) = self.control.read_exact(&mut payload).await {
            self.closed |= host_closed(&error);
            return Err(protocol_error("reply", error));
        }
        // The host names why it could not serve the request, then exits.
        if let Ok((REPLY_HOST_FAILED, mut fields)) = FrameReader::new(&payload) {
            self.closed = true;
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
                Started::Running(fields.u32().map_err(|e| protocol_error("reply", e))?)
            }
            REPLY_EXITED => {
                Started::Unexecutable(fields.i32().map_err(|e| protocol_error("reply", e))?)
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

    async fn exited(&mut self) -> Result<RawWaitStatus> {
        let payload = self.receive().await?;
        let (tag, mut fields) =
            FrameReader::new(&payload).map_err(|error| protocol_error("reply", error))?;
        if tag != REPLY_EXITED {
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

    /// End a host that broke its protocol. One that closed its end is exiting and is waited
    /// for; one still running is killed, and its death is this call's, not the job's.
    async fn end(&mut self) -> HostEnd {
        if !self.closed && !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = kill_process_group(self.pid, libc::SIGKILL);
            let _ = self.child.wait().await;
            self.reaped = true;
            return HostEnd::Killed;
        }
        match self.child.wait().await {
            Ok(status) => {
                self.reaped = true;
                HostEnd::Own(status.into_raw())
            }
            // Not reaped: dropping the host still ends its group.
            Err(_) => HostEnd::Killed,
        }
    }
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
            .stderr(Stdio::null())
            .kill_on_drop(false);
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
        let child = command
            .spawn()
            .map_err(classify_spawn_error)
            .map_err(ExecError::from)
            .map_err(super::supervisor::map_exec_error)?;
        drop(theirs);
        let pid = child
            .id()
            .ok_or_else(|| CowshedError::internal("spawned exec host has no process identity"))?;
        ours.set_nonblocking(true).map_err(pipe_error)?;
        let control = tokio::net::UnixStream::from_std(ours).map_err(pipe_error)?;
        Ok(ExecHost {
            child,
            pid,
            control,
            reaped: false,
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
        // start is read off the workspace filesystem's clock, which stamps the inputs; waiting
        // for it to move past the approval costs at most one stamping tick.
        let before = Snapshot::take(predicted.iter().map(PathBuf::as_path));
        let clock = self.workspace_mount.join(SHELL_HOST_DIRECTORY);
        let start_clock = clock.clone();
        let started =
            match tokio::task::spawn_blocking(move || FsInstant::separating(&start_clock)).await {
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
