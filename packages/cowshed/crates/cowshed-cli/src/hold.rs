//! `cowshed hold --control-fd <n> --state-fd <m> -- <cmd…>`: the held-leader fence
//! ([`ChildFence`]) for a caller that is not cowshed (11_shell.md "Held leaders: `cowshed hold`").
//! A watchdog runs its step through `hold`, signals the step's whole group through the control
//! descriptor, and never signals a pid or a group id itself.
//!
//! Host-only like `lock`: no project, no daemon, no runtime.

use std::fs::File;
use std::io::{BufRead as _, BufReader, Write};
use std::os::fd::RawFd;

use tokio::sync::mpsc;

use cowshed_core::CowshedError;
use cowshed_core::api::dto::ExitStatus;
use cowshed_core::api::hold::{HeldDescriptor, HeldOutput, HoldEvent, HoldRequest, HoldSignal};
use cowshed_core::runtime::child_fence::ChildFence;

use crate::args::HoldArgs;
use crate::lock::{State, StateEvent};
use crate::output::Output;
use crate::run::{InterruptPolicy, Interrupts};

impl StateEvent for HoldEvent {
    const VERB: &'static str = "hold";

    fn failed(reason: String) -> Self {
        Self::Failed { reason }
    }
}

/// The caller's control descriptor, adopted before runtime allocation and CLOEXEC from then on.
pub(crate) struct Control(File);

impl Control {
    pub(crate) fn adopt(descriptor: RawFd) -> Result<Self, CowshedError> {
        crate::lock::adopt(descriptor, "--control-fd", "hold").map(Self)
    }

    /// Read one request per line on a thread of its own, which blocks in `read(2)` on whatever
    /// the caller handed over (a pipe, a socket, a terminal). The channel ends with the
    /// descriptor's end; a line that is no request, or a read that fails, is its last item.
    fn requests(self) -> mpsc::Receiver<Result<HoldRequest, CowshedError>> {
        let (sender, requests) = mpsc::channel(1);
        std::thread::spawn(move || {
            let mut reader = BufReader::new(self.0);
            let mut line = Vec::new();
            loop {
                line.clear();
                let request = match reader.read_until(b'\n', &mut line) {
                    Ok(0) => return,
                    Ok(_) => HoldRequest::parse(line.strip_suffix(b"\n").unwrap_or(&line)).map_err(
                        |reason| {
                            CowshedError::usage(
                                reason,
                                "write `signal <SIG>` or `release`, one per line, to --control-fd",
                            )
                        },
                    ),
                    Err(error) => Err(CowshedError::internal(format!(
                        "cowshed hold cannot read its control descriptor: {error}"
                    ))),
                };
                let last = request.is_err();
                if sender.blocking_send(request).is_err() || last {
                    return;
                }
            }
        });
        requests
    }
}

/// Run the command held, serve its control and state descriptors, and answer the leader's exit
/// status once it is released and reaped. A failure of cowshed itself is said on the state
/// channel -- `failed` when no command runs, `commandFailed` once the leader runs -- and on
/// stderr while cowshed still has one, with the wrapper codes `exec` uses.
pub(crate) async fn dispatch<W, E>(
    args: &HoldArgs,
    mut state: State<HoldEvent>,
    control: Control,
    output: &mut Output<W, E>,
    policy: InterruptPolicy,
) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    match run(args, &mut state, control, policy).await {
        Ok(code) => code,
        Err(error) => {
            state.failed(&error);
            crate::lock::report(output, &error)
        }
    }
}

async fn run(
    args: &HoldArgs,
    state: &mut State<HoldEvent>,
    control: Control,
    policy: InterruptPolicy,
) -> Result<i32, CowshedError> {
    // The holder gives its own standard streams up to the leader: a host process keeps its own.
    if policy == InterruptPolicy::LeaveToHost {
        return Err(CowshedError::usage(
            "hold runs only as the cowshed process: it gives its standard streams to the command it holds",
            "run the cowshed binary itself",
        ));
    }
    let Some((program, rest)) = args.argv.split_first() else {
        return Err(CowshedError::internal(
            "hold reached its run without a command to run",
        ));
    };
    // Named from this process's own copies, which the leader inherits: the same open file
    // descriptions, so every later holder of them shows the same names.
    let output = HeldOutput {
        stdout: name(1)?,
        stderr: name(2)?,
    };
    // Prepared before the spawn, so nothing after it fails short of a descriptor.
    let release = cowshed_os::process::StdioRelease::prepare().map_err(|error| {
        CowshedError::internal(format!(
            "cowshed hold cannot prepare to give up its standard streams: {error}"
        ))
    })?;
    // Armed before the spawn, so a signal from here on reaches the leader's group.
    let mut interrupts = policy.arm_holding();
    let mut command = std::process::Command::new(program);
    command.args(rest);
    let fence = ChildFence::spawn(command)
        .map_err(|error| crate::land_turn::spawn_error(&program.to_string_lossy(), &error))?;
    let mut exit = None;
    match serve(
        &fence,
        output,
        release,
        state,
        control,
        &mut interrupts,
        &mut exit,
    )
    .await
    {
        Ok(exit) => Ok(exit.shell_code()),
        Err(error) => {
            abandon(&fence, exit, &error, state, &mut interrupts).await;
            Err(error)
        }
    }
}

/// The holder failed once its leader ran: say so as `commandFailed` with the leader's pid, then
/// release the leader as the end of the control descriptor does and keep answering on the state
/// channel -- each signal this process receives, then `exited` once the leader is reaped, unless
/// `exited` already said how it ended. When the release or the wait fails, the leader's exit
/// cannot be observed: SIGKILL is sent to its group, as a job's is, and a last `commandFailed`
/// says what failed and what came of the kill; a group it can no longer observe is not waited
/// for. The channel closes after that, so no `failed` follows. A write the channel refuses has
/// nowhere else to go: the wrapper exit code is then the caller's only word.
async fn abandon(
    fence: &ChildFence,
    exit: Option<ExitStatus>,
    error: &CowshedError,
    state: &mut State<HoldEvent>,
    interrupts: &mut Interrupts,
) {
    let pid = fence.pid();
    let _ = state.tell(&HoldEvent::CommandFailed {
        pid,
        reason: error.message.clone(),
    });
    let unobserved = |cause: String| match fence.signal(libc::SIGKILL) {
        Ok(()) => format!("{cause}; its group was sent SIGKILL"),
        Err(refusal) => format!("{cause}; its group was not killed: {refusal}"),
    };
    let ended = match fence.release() {
        Err(error) => Err(unobserved(format!("cannot release process {pid}: {error}"))),
        // Seen exited already, so the release reaped it.
        Ok(()) if exit.is_some() => Ok(None),
        Ok(()) => {
            let exited = fence.exited();
            tokio::pin!(exited);
            loop {
                tokio::select! {
                    biased;
                    exited = &mut exited => break exited.map(Some).map_err(|error| unobserved(error.message)),
                    number = interrupts.received() => {
                        let _ = match HoldSignal::numbered(number) {
                            Some(signal) => send(fence, signal, state),
                            None => state.tell(&HoldEvent::CommandFailed {
                                pid,
                                reason: format!("cowshed hold received signal {number}, which it does not name"),
                            }),
                        };
                    }
                }
            }
        }
    };
    let _ = match ended {
        Ok(Some(status)) => state.tell(&HoldEvent::Exited { status }),
        Ok(None) => Ok(()),
        Err(reason) => state.tell(&HoldEvent::CommandFailed { pid, reason }),
    };
    state.close();
}

/// The kernel's name of this process's `descriptor`, as the leader inherits it.
fn name(descriptor: RawFd) -> Result<HeldDescriptor, CowshedError> {
    cowshed_os::process::descriptor_name(descriptor)
        .map(HeldDescriptor::from)
        .map_err(|error| {
            CowshedError::internal(format!(
                "cowshed hold cannot name its descriptor {descriptor}: {error}"
            ))
        })
}

/// Serve the held leader until it is released and reaped, answering its exit status. `exit` is
/// how the leader ended once `exited` said so, kept for a failure that follows.
async fn serve(
    fence: &ChildFence,
    output: HeldOutput,
    release: cowshed_os::process::StdioRelease,
    state: &mut State<HoldEvent>,
    control: Control,
    interrupts: &mut Interrupts,
    exit: &mut Option<ExitStatus>,
) -> Result<ExitStatus, CowshedError> {
    // The holder keeps no copy of the leader's standard streams, given up before `started` says
    // so: their ends are the group's alone, so the caller sees them end with it.
    release.release().map_err(|error| {
        CowshedError::internal(format!(
            "cowshed hold cannot give up its standard streams: {error}"
        ))
    })?;
    state.tell(&HoldEvent::Started {
        pid: fence.pid(),
        birth: fence.birth().into(),
        output,
    })?;
    let mut requests = control.requests();
    let exited = fence.exited();
    tokio::pin!(exited);
    let (mut released, mut control_open) = (false, true);
    loop {
        tokio::select! {
            biased;
            status = &mut exited, if exit.is_none() => {
                let status = status?;
                state.tell(&HoldEvent::Exited { status: status.clone() })?;
                // Released before it exited: seeing it exit reaped it.
                if released {
                    return Ok(status);
                }
                *exit = Some(status);
            }
            request = requests.recv(), if control_open => {
                let request = match request {
                    Some(request) => request?,
                    // Its end is a release: a caller that dies releases.
                    None => {
                        control_open = false;
                        HoldRequest::Release
                    }
                };
                match request {
                    HoldRequest::Signal(signal) => send(fence, signal, state)?,
                    HoldRequest::Release => {
                        released = true;
                        fence.release().map_err(|error| {
                            CowshedError::internal(format!(
                                "cowshed hold cannot reap process {}: {error}",
                                fence.pid()
                            ))
                        })?;
                        // Released after it exited: the release reaped it.
                        if let Some(status) = exit.take() {
                            return Ok(status);
                        }
                    }
                }
            }
            number = interrupts.received() => {
                let signal = HoldSignal::numbered(number).ok_or_else(|| {
                    CowshedError::internal(format!("cowshed hold received signal {number}, which it does not name"))
                })?;
                send(fence, signal, state)?;
            }
        }
    }
}

/// Send `signal` to the held group and say what came of it.
fn send(
    fence: &ChildFence,
    signal: HoldSignal,
    state: &mut State<HoldEvent>,
) -> Result<(), CowshedError> {
    state.tell(&match fence.signal(signal.number()) {
        Ok(()) => HoldEvent::Signalled { signal },
        Err(refusal) => HoldEvent::NotSignalled {
            signal,
            reason: refusal.to_string(),
        },
    })
}
