//! `cowshed land-turn <repo> [--workspace <ws>] [--outside-lock <path>] -- <cmd…>`: run one
//! command as this repository's land turn, in the order the lands asked (05_gateway.md "Land
//! turns").
//!
//! Host-only like `disk-command`: no project, no daemon, no runtime. The queue is
//! [`cowshed_core::land_turn`]; this module adds the wait reports a caller reads on stderr, the
//! child's run under the turn, and the termination signals that reach the child.

use std::io::{self, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use cowshed_core::gateway_sessions::canonical_home;
use cowshed_core::host_dirs::support_directory;
use cowshed_core::land_turn::{self as queue, Wait};
use cowshed_core::{CowshedError, ErrorCode};

use crate::args::LandTurnArgs;
use crate::output::Output;
use crate::run::InterruptPolicy;

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
    let home = canonical_home()?;
    let directory = queue::land_turn_root(&support_directory(&home)).join(&args.repo);
    let command = args
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
            &command,
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
                written = written.and(report(output, &args.repo, &wait));
            }
            joined = &mut taking => break joined,
        }
    };
    while let Ok(wait) = received.try_recv() {
        waited = true;
        written = written.and(report(output, &args.repo, &wait));
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
    written?;
    if waited {
        output
            .guidance(&format!(
                "land turn {}: taken after {} s",
                args.repo,
                started.elapsed().as_secs()
            ))
            .map_err(write_failed)?;
    }
    // Armed once the turn is held and before the child starts, so a signal that arrives from here
    // on reaches the child, and the turn is held until the child ends.
    let mut interrupts = policy.arm_forwarding();

    let program_name = program.to_string_lossy().into_owned();
    let mut child = tokio::process::Command::new(program)
        .args(rest)
        .spawn()
        .map_err(|error| spawn_error(&program_name, &error))?;
    let Some(pid) = child.id().and_then(|pid| libc::pid_t::try_from(pid).ok()) else {
        return Err(CowshedError::internal(format!(
            "{program_name} started, but its process id is not available to forward signals to"
        )));
    };
    // A signal that cannot be relayed is kept until the child has ended, so the turn still ends
    // with the child and never before it.
    let mut relay: Result<(), CowshedError> = Ok(());
    let status = loop {
        tokio::select! {
            status = child.wait() => break status,
            signal = interrupts.received() => {
                relay = relay.and(forward(output, pid, signal, &program_name));
            }
        }
    };
    // The turn ends with the child, never before it.
    drop(turn);
    let status = status.map_err(|error| {
        CowshedError::internal(format!("cannot wait for {program_name}: {error}"))
    })?;
    relay?;
    exit_code(status)
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
        Wait::Outside { lock } => format!(
            "land turn {repo}: next, waiting for {} held by a land that did not ask for a turn",
            lock.display(),
        ),
    };
    output.guidance(&line).map_err(write_failed)
}

/// Send one termination signal to the child. The child may have ended as the signal arrived;
/// then there is nothing left to signal, and its exit status is the answer.
fn forward<W: Write, E: Write>(
    output: &mut Output<W, E>,
    pid: libc::pid_t,
    signal: i32,
    program: &str,
) -> Result<(), CowshedError> {
    // SAFETY: `kill` sends `signal` to `pid` and reads or writes no memory of this process.
    if unsafe { libc::kill(pid, signal) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            output
                .error(&format!(
                    "cannot forward signal {signal} to {program}: {error}"
                ))
                .map_err(write_failed)?;
        }
    }
    Ok(())
}

/// The exit code a shell reports for the child's end: its own code, or 128 plus the signal that
/// ended it.
fn exit_code(status: ExitStatus) -> Result<i32, CowshedError> {
    if let Some(code) = status.code() {
        return Ok(code);
    }
    match status.signal() {
        Some(signal) => Ok(128 + signal),
        None => Err(CowshedError::internal(
            "the land turn's command ended, but the kernel reported neither its exit code nor its signal",
        )),
    }
}

fn spawn_error(program: &str, error: &io::Error) -> CowshedError {
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
