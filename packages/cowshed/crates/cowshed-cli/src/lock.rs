//! `cowshed lock <path> [--state-fd <n>] [--owned-pid <pid>]... -- <cmd…>` and
//! `cowshed lock-holders <path>`: the held lock ([`cowshed_core::held_lock`]) for a caller that is not
//! cowshed (02_workspaces.md "Held locks"). A watchdog takes its exact lock through `lock`,
//! reads its state lines, and names who holds it from `lock-holders`.
//!
//! Host-only like `land-turn`: no project, no daemon, no runtime.

use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::os::fd::{FromRawFd as _, RawFd};
use std::path::Path;

use serde::Serialize;

use cowshed_core::CowshedError;
use cowshed_core::api::held_lock::LockWaiterEvent;
use cowshed_core::held_lock::{self, HeldLock, Opened, OwnedLives};

use crate::args::{LockArgs, LockHoldersArgs};
use crate::land_turn::{PreparedCommand, exit_code};
use crate::output::Output;
use crate::run::InterruptPolicy;

/// Take the lock, run the command holding it, and answer the command's exit code. A failure of
/// cowshed itself is reported before spawn on stderr and as `failed` on the state channel.
/// After the state-channel handoff, only that channel reports wrapper failures (`commandFailed`):
/// standard streams belong to the command, so their EOF cannot depend on this controller's exit.
pub(crate) async fn dispatch<W, E>(
    args: &LockArgs,
    mut state: State<LockWaiterEvent>,
    output: &mut Output<W, E>,
    policy: InterruptPolicy,
) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    match run(args, &mut state, output, policy).await {
        Ok(code) => code,
        Err(error) => {
            state.failed(&error);
            report(output, &error)
        }
    }
}

/// Print who the kernel shows holding the lock.
pub(crate) fn dispatch_holders<W, E>(args: &LockHoldersArgs, output: &mut Output<W, E>) -> i32
where
    W: Write + Send,
    E: Write + Send,
{
    let written = held_lock::observe(&args.path)
        .map_err(|error| failure(&args.path, "observe", &error))
        .and_then(|observation| output.bare_record(&observation).map_err(write_failed));
    match written {
        Ok(()) => 0,
        Err(error) => report(output, &error),
    }
}

pub(crate) fn report<W: Write, E: Write>(output: &mut Output<W, E>, error: &CowshedError) -> i32 {
    // The exit code is the report a caller that closed stderr still reads; a write that fails
    // here has nowhere else to go.
    let _ = output
        .error(&error.message)
        .and_then(|()| output.hint(&error.hint));
    i32::from(error.exec_wrapper_exit_code())
}

async fn run<W, E>(
    args: &LockArgs,
    state: &mut State<LockWaiterEvent>,
    output: &mut Output<W, E>,
    policy: InterruptPolicy,
) -> Result<i32, CowshedError>
where
    W: Write + Send,
    E: Write + Send,
{
    let Some((program, rest)) = args.argv.split_first() else {
        return Err(CowshedError::internal(
            "lock reached its run without a command to run",
        ));
    };
    let command = PreparedCommand::prepare(program, rest)?;
    let stdio = state
        .is_open()
        .then(cowshed_os::process::StdioRelease::prepare)
        .transpose()
        .map_err(|error| {
            CowshedError::internal(format!("cannot prepare command stdio handoff: {error}"))
        })?;
    // The lives the caller owns, captured before anything is waited for: the only authority any
    // later signal has, and held for the whole run. The state channel was adopted before setup.
    let owned = OwnedLives::capture(&args.owned_pids).map_err(|error| {
        CowshedError::internal(format!(
            "cannot capture the owned processes {:?}: {error}",
            args.owned_pids
        ))
    })?;
    let held = match take(&args.path, &owned, state, output).await? {
        Taken::Held(held) => held,
        // Signalling refuses this step: the channel has already ended with signalled.
        Taken::Refused(pid) => {
            return Err(CowshedError::conflict(
                format!(
                    "lock {}: signalled its owned owner, pid {pid}, and refused its command",
                    args.path.display()
                ),
                "the caller asked for the step to be cancelled; resolve the owned holder before retrying",
            ));
        }
    };
    // Armed once the lock is held and before the command starts, so a signal from here on reaches
    // the command, and the lock is held until the command ends.
    let mut interrupts = policy.arm_forwarding();
    let command = command.spawn(vec![held.descriptor()])?;
    // Spawn has succeeded. The state descriptor was CLOEXEC, so only this parent still owns it.
    // Relinquish stdio before acknowledging acquisition: a caller may now wait for the command's
    // output EOF before releasing a command-side hold. Retaining our copies would deadlock it.
    let state_reporting = stdio.is_some();
    let released = stdio.map_or(Ok(()), |stdio| {
        stdio.release().map_err(|error| {
            CowshedError::internal(format!(
                "cannot hand standard streams to the command: {error}"
            ))
        })
    });
    let acquired = released
        .and_then(|()| {
            state.tell(&LockWaiterEvent::Acquired {
                inode: held.inode(),
            })
        })
        .map_err(|error| {
            // A command-side hold may need the caller's release before it can end. Report a
            // failed handoff now, not after joining that command; keep child and lock custody.
            let reported = state.tell(&LockWaiterEvent::CommandFailed {
                reason: error.message.clone(),
            });
            state.close();
            reported.err().unwrap_or(error)
        });
    let name = command.name.clone();
    // Neither a handoff failure nor a broken state reader abandons the started command.
    let (status, relay) = command.wait(&mut interrupts).await;
    let result =
        acquired
            .and(status.map_err(|error| {
                CowshedError::internal(format!("cannot wait for {name}: {error}"))
            }))
            .and_then(|status| {
                relay?;
                exit_code(status)
            });
    // The command has ended: the record is emptied and this process's descriptor released. A
    // process the command handed the descriptor to keeps the lock until it closes it too.
    drop(held);
    if !state_reporting {
        return result;
    }
    let code = match result {
        Ok(code) => code,
        Err(error) => {
            let reported = if state.is_open() {
                state.tell(&LockWaiterEvent::CommandFailed {
                    reason: error.message.clone(),
                })
            } else {
                // A handoff failure was already reported before joining the child.
                Ok(())
            };
            // If the separate channel also broke, the nonzero wrapper exit is the remaining
            // observation. Never reopen or retain command stderr to extend its lifetime.
            i32::from(reported.err().unwrap_or(error).exec_wrapper_exit_code())
        }
    };
    state.close();
    Ok(code)
}

/// What came of a wait: the lock held, or the waiter's own refusal once it signalled the owned
/// owner (pid) the caller asked it to cancel.
enum Taken {
    Held(HeldLock),
    Refused(i32),
}

/// Hold the lock: at once when it is free; otherwise say `queued` and wait in `flock(2)` on a
/// blocking thread until the kernel grants it. While it waits, each SIGUSR1 asks it to signal the
/// owner of the inode it is queued on through the lives it captured
/// ([`OwnedLives::signal_owner`]); the state channel says what came of it. Once it signalled, it
/// refuses its step; when it did not, it keeps its place in the queue. A grant ready together with
/// a request is taken first: the step then runs, and the request is never answered, as a request
/// that arrives after the grant never is.
async fn take<W: Write, E: Write>(
    path: &Path,
    owned: &OwnedLives,
    state: &mut State<LockWaiterEvent>,
    output: &mut Output<W, E>,
) -> Result<Taken, CowshedError> {
    let opened = Opened::open(path).map_err(|error| failure(path, "open", &error))?;
    let inode = opened.inode();
    let opened = match opened
        .try_hold()
        .map_err(|error| failure(path, "lock", &error))?
    {
        Ok(held) => return Ok(Taken::Held(held)),
        Err(opened) => opened,
    };
    // Installed before `queued` is said, so no request the caller sends after reading it is lost;
    // it stays installed for the run, so a late request never ends this process.
    let mut requests =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())
            .map_err(|error| CowshedError::internal(format!("cannot watch SIGUSR1: {error}")))?;
    if state.is_open() {
        state.tell(&LockWaiterEvent::Queued { inode })?;
    } else {
        output
            .guidance(&format!(
                "lock {}: held by another holder; waiting for it",
                path.display()
            ))
            .map_err(write_failed)?;
    }
    let mut waiting = tokio::task::spawn_blocking(move || opened.hold());
    loop {
        tokio::select! {
            biased;
            granted = &mut waiting => {
                return granted
                    .map_err(|error| {
                        CowshedError::internal(format!(
                            "the waiter for lock {} failed: {error}",
                            path.display()
                        ))
                    })?
                    .map(Taken::Held)
                    .map_err(|error| failure(path, "lock", &error));
            }
            Some(()) = requests.recv() => {
                match owned.signal_owner(path, inode, libc::SIGTERM) {
                    Ok(Ok(pid)) => {
                        state.tell(&LockWaiterEvent::Signalled { pid })?;
                        state.close();
                        // The blocked flock(2) is left to the kernel: its thread ends when the lock
                        // is granted or this process exits, and a lock granted to it is released
                        // with this process, never handed to the command.
                        return Ok(Taken::Refused(pid));
                    }
                    Ok(Err(reason)) => state.tell(&LockWaiterEvent::NotSignalled { reason })?,
                    Err(error) => state.tell(&LockWaiterEvent::NotSignalled {
                        reason: format!("cannot observe the lock: {error}"),
                    })?,
                }
            }
        }
    }
}

/// A caller's owned state channel: one event per line, as JSON (a [`LockWaiterEvent`] for `lock`,
/// a `HoldEvent` for `hold`). Adopted before runtime allocation, CLOEXEC throughout setup and
/// spawn, closed after the final event.
pub(crate) struct State<E>(Option<File>, PhantomData<fn(&E)>);

/// An event a state channel carries: the verb whose channel it is, and its `failed` last line.
pub(crate) trait StateEvent: Serialize + fmt::Debug {
    const VERB: &'static str;

    fn failed(reason: String) -> Self;
}

impl StateEvent for LockWaiterEvent {
    const VERB: &'static str = "lock";

    fn failed(reason: String) -> Self {
        Self::Failed { reason }
    }
}

impl<E: StateEvent> State<E> {
    pub(crate) fn adopt(descriptor: Option<RawFd>) -> Result<Self, CowshedError> {
        let Some(descriptor) = descriptor else {
            return Ok(Self(None, PhantomData));
        };
        adopt(descriptor, "--state-fd", E::VERB).map(|channel| Self(Some(channel), PhantomData))
    }

    fn is_open(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn tell(&mut self, event: &E) -> Result<(), CowshedError> {
        let Some(channel) = &mut self.0 else {
            return Ok(());
        };
        let mut line = serde_json::to_vec(event).map_err(|error| {
            CowshedError::internal(format!(
                "cowshed {} cannot encode its state event {event:?}: {error}",
                E::VERB
            ))
        })?;
        line.push(b'\n');
        channel.write_all(&line).map_err(|error| {
            CowshedError::internal(format!(
                "cowshed {} cannot write its state event {event:?}: {error}",
                E::VERB
            ))
        })
    }

    pub(crate) fn failed(&mut self, error: &CowshedError) {
        if self.is_open() {
            // This channel may have lost its reader; stderr still reports the original failure.
            let _ = self.tell(&E::failed(error.message.clone()));
            self.close();
        }
    }

    pub(crate) fn close(&mut self) {
        self.0 = None;
    }
}

/// Own `descriptor`, the one a caller handed this process for the channel `flag` names, marked
/// close-on-exec so no command spawned meanwhile inherits it.
pub(crate) fn adopt(descriptor: RawFd, flag: &str, verb: &str) -> Result<File, CowshedError> {
    // SAFETY: F_SETFD takes a plain integer; it fails on a descriptor that is not open.
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(CowshedError::environment_missing(
            format!(
                "{flag} {descriptor} is not an open descriptor: {}",
                io::Error::last_os_error()
            ),
            format!("open the descriptor in the caller before it runs cowshed {verb}"),
        ));
    }
    // SAFETY: the caller handed this process the descriptor for this channel alone, and it is
    // open (checked above); it is owned and closed from here.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn failure(path: &Path, operation: &str, error: &io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} the lock {}: {error}", path.display()),
        "name a lock file this user can create and write, the one every other holder of it takes",
    )
}

fn write_failed(error: io::Error) -> CowshedError {
    CowshedError::internal(format!("cowshed cannot write its lock report: {error}"))
}
