//! `cowshed land-turn <repo> [--workspace <ws>] [--outside-lock <path>] -- <cmd…>`: run one
//! command as this repository's land turn, in the order the lands asked (05_gateway.md "Land
//! turns").
//!
//! Host-only like `disk-command`: no project, no workspace daemon, no runtime. The queue is the
//! gateway's ([`cowshed_core::land_turn`]); this module adds the wait reports a caller reads on
//! stderr, the child's run under the turn, and the termination signals that reach the child.

use std::ffi::OsString;
use std::io::{self, Write};
use std::os::fd::RawFd;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use cowshed_core::held_lock;
use cowshed_core::land_turn::{self as queue, OutsideHolders, Wait};
use cowshed_core::storage::bootstrap::CanonicalRoots;
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
    let socket = args
        .gateway_socket
        .clone()
        .unwrap_or_else(|| CanonicalRoots::global().control_socket());
    let description = args
        .argv
        .iter()
        .map(|part| part.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");

    let mut turn = take_turn(
        &socket,
        queue::TurnAsk {
            repo: args.repo.clone(),
            workspace: args.workspace.clone(),
            command: description,
        },
        args.outside_lock.clone(),
        output,
    )
    .await?;
    // Armed once the turn is held and before the child starts, so a signal that arrives from here
    // on reaches the child, and the turn is held until the child ends.
    let mut interrupts = policy.arm_forwarding();

    // The child inherits the outside lock's descriptor and binds itself to the turn as it starts.
    let held = match command.spawn(turn.outside_descriptor().into_iter().collect(), Some(&turn)) {
        Ok(held) => held,
        Err(error) => {
            turn.end(format!("did not start: {}", error.message));
            return Err(error);
        }
    };
    let program_name = held.name.clone();
    if let Err(error) = turn.bound(held.pid.unsigned_abs()).await {
        // A command the turn does not hold must not run beside the next land: it is stopped.
        let (stopped, ended) = held.stop().await;
        turn.end(format!("stopped: the gateway did not bind it: {error}"));
        return Err(CowshedError::environment_missing(
            format!(
                "the gateway did not bind {program_name} to land turn {}: {error}; {}; it {ended}",
                args.repo,
                sigterm(&program_name, stopped),
            ),
            "start the host gateway (`cowshed gateway start`), then run it again",
        ));
    }
    let (ended, relay) = held.wait_holding(&mut interrupts, &mut turn).await;
    // The turn ends with the child, never before it, recording how the child ended.
    let status = match ended {
        Held::Exited(Ok(status)) => status,
        Held::Exited(Err(error)) => {
            return Err(CowshedError::internal(format!(
                "cannot wait for {program_name}: {error}"
            )));
        }
        Held::Lost {
            error: lost,
            stopped,
            ended,
        } => {
            let relayed = match &relay {
                Ok(()) => String::new(),
                Err(error) => format!(
                    "; an interrupt did not reach {program_name} ({})",
                    error.message
                ),
            };
            return Err(CowshedError::environment_missing(
                format!(
                    "land turn {} was lost: {lost}; {}; it {ended}{relayed}",
                    args.repo,
                    sigterm(&program_name, stopped),
                ),
                "check the host gateway (`cowshed gateway status`), then land again",
            ));
        }
    };
    let outcome = match (status.code(), status.signal()) {
        (Some(code), _) => format!("exited {code}"),
        (None, Some(signal)) => format!("killed by signal {signal}"),
        (None, None) => "ended with neither an exit code nor a signal".to_owned(),
    };
    turn.end(outcome);
    relay?;
    exit_code(status)
}

/// What became of the SIGTERM sent to `program`, in the words of a refusal.
fn sigterm(program: &str, stopped: Result<(), CowshedError>) -> String {
    match stopped {
        Ok(()) => format!("{program} was sent SIGTERM"),
        Err(error) => format!("SIGTERM did not reach {program} ({})", error.message),
    }
}

/// Take `ask`'s turn from the gateway at `socket`, saying on `output` who holds it and how many
/// are ahead while it waits, and how long it waited once it is taken. A report that cannot be
/// written ends the turn unstarted.
pub(crate) async fn take_turn<W, E>(
    socket: &Path,
    ask: queue::TurnAsk,
    outside: Option<PathBuf>,
    output: &mut Output<W, E>,
) -> Result<queue::Turn, CowshedError>
where
    W: Write + Send,
    E: Write + Send,
{
    let started = Instant::now();
    let label = ask.repo.to_string();
    let mut waited = false;
    // A report that cannot be written is kept, not returned: the turn is ended unstarted once the
    // wait is over, never left queued with nothing to report to.
    let mut written: Result<(), CowshedError> = Ok(());
    let census = cowshed_core::storage::bootstrap::CanonicalRoots::global().census();
    let taken = queue::take(
        socket,
        &ask,
        outside.as_deref().map(|path| queue::OutsideLock {
            path,
            census: &census,
        }),
        LAND_TURN_REPORT_EVERY,
        |wait| {
            waited = true;
            if written.is_ok() {
                written = report(output, &label, wait);
            }
        },
    )
    .await;
    let turn = taken.map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot take the land turn {label}: {error}"),
            "start the host gateway (`cowshed gateway start`), then run it again",
        )
    })?;
    let reported = written.and_then(|()| {
        if waited {
            output
                .guidance(&format!(
                    "land turn {label}: taken after {} s",
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
    Ok(turn)
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

    /// Spawn the command handing it `descriptors`, armed to bind itself to `turn`, when given, as
    /// it starts ([`queue::Turn::bind_at_exec`]): the gateway's answer is read by
    /// [`queue::Turn::bound`].
    pub(crate) fn spawn(
        mut self,
        descriptors: Vec<RawFd>,
        turn: Option<&queue::Turn>,
    ) -> Result<HeldCommand, CowshedError> {
        held_lock::hand_to(self.command.as_std_mut(), descriptors);
        if let Some(turn) = turn {
            turn.bind_at_exec(self.command.as_std_mut());
        }
        let child = self
            .command
            .spawn()
            .map_err(|error| spawn_error(&self.name, &error))?;
        let pid = spawned_pid(&child);
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
        // WHY unbounded: the command is this verb's own work (`cowshed lock`, `cowshed land-turn --
        // <cmd>`); it lasts as long as the command, which the caller's forwarded signals end.
        #[allow(clippy::disallowed_methods)]
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

    /// [`Self::wait`] for a command bound to `turn`: should the gateway close the turn's
    /// connection first, the turn is lost, and the command is stopped ([`Self::stop`]).
    async fn wait_holding(
        mut self,
        interrupts: &mut Interrupts,
        turn: &mut queue::Turn,
    ) -> (Held, Result<(), CowshedError>) {
        let mut relay: Result<(), CowshedError> = Ok(());
        // WHY unbounded: the command is this verb's own work, bound to the turn; it lasts as long as
        // the command, which the caller's forwarded signals end. The turn's loss ends the wait, and
        // the command is then stopped within the reap bound.
        #[allow(clippy::disallowed_methods)]
        let error = loop {
            tokio::select! {
                status = self.child.wait() => return (Held::Exited(status), relay),
                signal = interrupts.received() => {
                    relay = relay.and(forward(self.pid, signal, &self.name));
                }
                error = turn.lost() => break error,
            }
        };
        let (stopped, ended) = self.stop().await;
        (
            Held::Lost {
                error,
                stopped,
                ended,
            },
            relay,
        )
    }

    /// Send the command SIGTERM and reap it within the reap bound ([`reap_stopped`]): whether the
    /// signal reached it, and how it ended or why it was not reaped.
    async fn stop(mut self) -> (Result<(), CowshedError>, String) {
        let stopped = forward(self.pid, libc::SIGTERM, &self.name);
        let ended = reap_stopped("land-turn command", &mut self.child, stopped.is_ok()).await;
        (stopped, ended)
    }
}

/// How a command bound to a land turn ended its wait ([`HeldCommand::wait_holding`]).
enum Held {
    /// It exited first.
    Exited(io::Result<ExitStatus>),
    /// The turn was lost first: the command was sent SIGTERM, whether it reached it is `stopped`,
    /// and `ended` says how it ended or why it was not reaped.
    Lost {
        error: io::Error,
        stopped: Result<(), CowshedError>,
        ended: String,
    },
}

/// One wait report, in the words a caller reads on stderr.
fn report<W: Write, E: Write>(
    output: &mut Output<W, E>,
    repo: &str,
    wait: &Wait,
) -> Result<(), CowshedError> {
    let line = match wait {
        Wait::Queued { holder, ahead } => format!(
            "land turn {repo}: {ahead} ahead; {} holds it (pid {}{}, asked {})",
            holder.workspace,
            holder.pid,
            holder
                .bound
                .iter()
                .map(|pid| format!(", command pid {pid}"))
                .collect::<String>(),
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
pub(crate) fn forward(pid: libc::pid_t, signal: i32, program: &str) -> Result<(), CowshedError> {
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

/// The pid of a child just spawned: a newly spawned, unpolled child always has its kernel pid,
/// which fits `pid_t`.
pub(crate) fn spawned_pid(child: &tokio::process::Child) -> libc::pid_t {
    child
        .id()
        .and_then(|pid| libc::pid_t::try_from(pid).ok())
        .expect("a newly spawned child has a pid_t process id")
}

/// Reap `child`, just sent SIGTERM (`signalled`) or not, within the reap bound, and say how it
/// ended: its status in words, or why its end was not reaped. A child whose reap expires is named
/// and left to tokio's orphan reaper once it is dropped; nothing waits for it.
pub(crate) async fn reap_stopped(
    role: &'static str,
    child: &mut tokio::process::Child,
    signalled: bool,
) -> String {
    let last = if signalled {
        cowshed_os::deadline::Progress::Signalled {
            signal: libc::SIGTERM,
        }
    } else {
        cowshed_os::deadline::Progress::Nothing
    };
    let budget = cowshed_os::deadline::start("reap a stopped command", cowshed_os::deadline::REAP);
    match cowshed_os::deadline::reap_async(role, child, &budget, last).await {
        Ok(status) => crate::declared_land::describe(status),
        Err(error) => format!("was not reaped: {error}"),
    }
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
