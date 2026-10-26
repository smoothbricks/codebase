//! `cowshed land-turn <repo> [--workspace <ws>] [--outside-lock <path>] -- <cmd…>`: run one
//! command as this repository's land turn, in the order the lands asked (05_gateway.md "Land
//! turns").
//!
//! Host-only like `disk-command`: no project, no daemon, no runtime. The queue is
//! [`cowshed_core::land_turn`]; this module adds the wait reports a caller reads on stderr, the
//! child's run under the turn, and the termination signals that reach the child.

use std::ffi::OsString;
use std::io::{self, Write};
use std::os::fd::RawFd;
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use cowshed_core::gateway_sessions::canonical_home;
use cowshed_core::held_lock;
use cowshed_core::host_dirs::support_directory;
use cowshed_core::land_turn::{self as queue, OutsideHolders, Wait};
use cowshed_core::{CowshedError, ErrorCode};

use crate::args::LandTurnArgs;
use crate::output::Output;
use crate::run::{InterruptPolicy, Interrupts};

/// How often a waiting land repeats what it waits for: the land watchdog's cadence.
pub const LAND_TURN_REPORT_EVERY: Duration = Duration::from_secs(90);

/// Take this repository's turn, run the command under it, and answer the command's exit code.
/// A failure of cowshed itself is reported on stderr and exits with the wrapper codes `exec`
/// uses, so it is never mistaken for the command's own status.
pub(crate) async fn dispatch<W, E>(
    args: &LandTurnArgs,
    output: &mut Output<W, E>,
    policy: InterruptPolicy,
) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    match run(args, output, policy).await {
        Ok(code) => code,
        Err(error) => {
            let code = i32::from(error.exec_wrapper_exit_code());
            // The exit code is the report a caller that closed stderr still reads; a write that
            // fails here has nowhere else to go.
            let _ = output
                .error(&error.message)
                .and_then(|()| output.hint(&error.hint));
            code
        }
    }
}

async fn run<W, E>(
    args: &LandTurnArgs,
    output: &mut Output<W, E>,
    policy: InterruptPolicy,
) -> Result<i32, CowshedError>
where
    W: Write + Send,
    E: Write + Send,
{
    let Some((program, rest)) = args.argv.split_first() else {
        return Err(CowshedError::internal(
            "land-turn reached its run without a command to run",
        ));
    };
    let command = PreparedCommand::prepare(program, rest)?;
    let home = canonical_home()?;
    let directory = queue::land_turn_root(&support_directory(&home)).join(args.repo.as_str());
    let description = args
        .argv
        .iter()
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");

    let started = Instant::now();
    let (reports, mut received) = tokio::sync::mpsc::unbounded_channel();
    let workspace = args.workspace.clone();
    let outside = args.outside_lock.clone();
    let mut taking = tokio::task::spawn_blocking(move || {
        queue::take(
            &directory,
            &workspace,
            &description,
            outside.as_deref(),
            LAND_TURN_REPORT_EVERY,
            |wait| {
                // The receiver is read until this call returns, so no report is lost; a send
                // fails only once the receiver is gone, and then no caller is left to hear it.
                let _ = reports.send(wait.clone());
            },
        )
    });

    let mut waited = false;
    // A report that cannot be written is kept, not returned: returning while the take still waits
    // would make the runtime's shutdown wait for the queue with nothing left to report to.
    let mut written: Result<(), CowshedError> = Ok(());
    let joined = loop {
        tokio::select! {
            Some(wait) = received.recv() => {
                waited = true;
                written = written.and(report(output, args.repo.as_str(), &wait));
            }
            joined = &mut taking => break joined,
        }
    };
    while let Ok(wait) = received.try_recv() {
        waited = true;
        written = written.and(report(output, args.repo.as_str(), &wait));
    }
    let turn = joined
        .map_err(|error| {
            CowshedError::internal(format!(
                "the land turn thread for {} failed: {error}",
                args.repo
            ))
        })?
        .map_err(|error| {
            CowshedError::internal(format!(
                "cannot take the land turn for {}: {error}",
                args.repo
            ))
        })?;
    let reported = written.and_then(|()| {
        if waited {
            output
                .guidance(&format!(
                    "land turn {}: taken after {} s",
                    args.repo,
                    started.elapsed().as_secs()
                ))
                .map_err(write_failed)?;
        }
        Ok(())
    });
    if let Err(error) = reported {
        turn.end(format!("did not start: {}", error.message));
        return Err(error);
    }
    // Armed once the turn is held and before the child starts, so a signal that arrives from here
    // on reaches the child, and the turn is held until the child ends.
    let mut interrupts = policy.arm_forwarding();

    // The child inherits the turn's descriptors: the kernel keeps the turn until the child has
    // ended too, even if this process is killed first.
    let held = match command.spawn(turn.descriptors()) {
        Ok(held) => held,
        Err(error) => {
            turn.end(format!("did not start: {}", error.message));
            return Err(error);
        }
    };
    let program_name = held.name.clone();
    let (status, relay) = held.wait(&mut interrupts).await;
    // The turn ends with the child, never before it, recording how the child ended.
    let status = match status {
        Ok(status) => {
            let outcome = match (status.code(), status.signal()) {
                (Some(code), _) => format!("exited {code}"),
                (None, Some(signal)) => format!("killed by signal {signal}"),
                (None, None) => "ended with neither an exit code nor a signal".to_owned(),
            };
            turn.end(outcome);
            status
        }
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "cannot wait for {program_name}: {error}"
            )));
        }
    };
    relay?;
    exit_code(status)
}

/// A held command whose environment is already valid, prepared before asking for any lock.
pub(crate) struct PreparedCommand {
    command: tokio::process::Command,
    name: String,
}

impl PreparedCommand {
    pub(crate) fn prepare(program: &OsString, rest: &[OsString]) -> Result<Self, CowshedError> {
        let name = program.to_string_lossy().into_owned();
        let mut command = tokio::process::Command::new(program);
        command.args(rest);
        held_lock::prepare_command_environment(command.as_std_mut()).map_err(|reason| {
            CowshedError::new(
                ErrorCode::EnvironmentMissing,
                reason,
                "unset GIT_CONFIG_COUNT or give it a count, then run it again",
            )
        })?;
        Ok(Self { command, name })
    }

    pub(crate) fn spawn(mut self, descriptors: Vec<RawFd>) -> Result<HeldCommand, CowshedError> {
        held_lock::hand_to(self.command.as_std_mut(), descriptors);
        let child = self
            .command
            .spawn()
            .map_err(|error| spawn_error(&self.name, &error))?;
        // A newly spawned, unpolled child always has its kernel pid, which fits pid_t.
        let pid = child
            .id()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .expect("a newly spawned child has a pid_t process id");
        Ok(HeldCommand {
            child,
            pid,
            name: self.name,
        })
    }
}

/// A command run holding descriptors this process holds a lock through: each is kept open across
/// its exec ([`held_lock::keep_across_exec`]), so the kernel keeps the lock until the command, and
/// every process it hands the descriptor to, has closed it too.
pub(crate) struct HeldCommand {
    child: tokio::process::Child,
    pid: libc::pid_t,
    pub(crate) name: String,
}

impl HeldCommand {
    /// Wait for the command to end, forwarding each termination signal `interrupts` receives. A
    /// signal that cannot be relayed is kept, second, until the command has ended, so whatever
    /// holds the lock with it ends with it and never before it.
    pub(crate) async fn wait(
        mut self,
        interrupts: &mut Interrupts,
    ) -> (io::Result<ExitStatus>, Result<(), CowshedError>) {
        let mut relay: Result<(), CowshedError> = Ok(());
        let status = loop {
            tokio::select! {
                status = self.child.wait() => break status,
                signal = interrupts.received() => {
                    relay = relay.and(forward(self.pid, signal, &self.name));
                }
            }
        };
        (status, relay)
    }
}

/// One wait report, in the words a caller reads on stderr.
fn report<W: Write, E: Write>(
    output: &mut Output<W, E>,
    repo: &str,
    wait: &Wait,
) -> Result<(), CowshedError> {
    let line = match wait {
        Wait::Queued { holder, ahead } => format!(
            "land turn {repo}: {ahead} ahead; {} holds it (pid {}, asked {})",
            holder.workspace,
            holder.pid,
            local_clock(holder.asked_unix_ms),
        ),
        Wait::Outside { lock, holders } => {
            let held = match holders {
                OutsideHolders::Open(open) => format!(
                    "open in {}",
                    open.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                OutsideHolders::NoneOpen => "no other process has it open now: its holder let \
                    go as this looked, or is not this user's to see"
                    .to_owned(),
                OutsideHolders::Unknown(reason) => {
                    format!("the processes with it open could not be listed: {reason}")
                }
            };
            format!(
                "land turn {repo}: next, waiting for {}, held outside the queue; {held}",
                lock.display(),
            )
        }
    };
    output.guidance(&line).map_err(write_failed)
}

/// Send one termination signal to the child. The child may have ended as the signal arrived;
/// then there is nothing left to signal, and its exit status is the answer.
fn forward(pid: libc::pid_t, signal: i32, program: &str) -> Result<(), CowshedError> {
    // SAFETY: `kill` sends `signal` to `pid` and reads or writes no memory of this process.
    if unsafe { libc::kill(pid, signal) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(CowshedError::internal(format!(
                "cannot forward signal {signal} to {program}: {error}"
            )));
        }
    }
    Ok(())
}

/// The exit code a shell reports for the child's end: its own code, or 128 plus the signal that
/// ended it.
pub(crate) fn exit_code(status: ExitStatus) -> Result<i32, CowshedError> {
    if let Some(code) = status.code() {
        return Ok(code);
    }
    match status.signal() {
        Some(signal) => Ok(128 + signal),
        None => Err(CowshedError::internal(
            "the command ended, but the kernel reported neither its exit code nor its signal",
        )),
    }
}

pub(crate) fn spawn_error(program: &str, error: &io::Error) -> CowshedError {
    let message = format!("cannot run {program}: {error}");
    match error.kind() {
        io::ErrorKind::NotFound => CowshedError::new(
            ErrorCode::EnvironmentMissing,
            message,
            "install the command or give its path, then run it again",
        ),
        _ => CowshedError::internal(message),
    }
}

fn write_failed(error: io::Error) -> CowshedError {
    CowshedError::internal(format!(
        "cowshed cannot write its land turn report: {error}"
    ))
}

/// The local wall-clock time of `unix_ms` as `HH:MM:SS`, which is what a person reading the queue
/// compares against their own clock; `??:??:??` when the platform cannot convert the instant.
pub fn local_clock(unix_ms: u64) -> String {
    const UNKNOWN: &str = "??:??:??";
    let Ok(seconds) = libc::time_t::try_from(unix_ms / 1000) else {
        return UNKNOWN.to_owned();
    };
    let mut parts = std::mem::MaybeUninit::<libc::tm>::uninit();
    // SAFETY: `localtime_r` reads `seconds` and writes a whole `tm` into `parts` only when it
    // returns non-null; `parts` is not read unless it did.
    let converted = unsafe { libc::localtime_r(&seconds, parts.as_mut_ptr()) };
    if converted.is_null() {
        return UNKNOWN.to_owned();
    }
    // SAFETY: `localtime_r` returned non-null, so every field of `parts` is initialized.
    let parts = unsafe { parts.assume_init() };
    format!(
        "{:02}:{:02}:{:02}",
        parts.tm_hour, parts.tm_min, parts.tm_sec
    )
}
