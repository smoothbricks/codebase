//! `cowshed gateway await-turns -- <cmd…>`: run one command, an install, once the host gateway
//! has fenced its land turns for it, in its order among the lands (05_gateway.md "The read fences
//! the turns", "A land installs its own cowshed after its turn").
//!
//! The install asks the gateway to wait: every land queued when it asked ends its turn first,
//! every land that asks meanwhile is parked behind it, and the gateway fences every queue in the
//! step that ends the last ticket before it. The command runs under that fence, which holds until
//! the command exits; `setup`, run as the command, reads the turns under the install slot, finds no
//! ticket and fences them again for its restart. The wait happens before the slot, never under it.
//! Recorded 2026-11-10: an install that deferred whenever it found a land turn held or queued
//! deferred three times in an hour, each rerun losing the gap to the next land's ask.
//!
//! Host-only like `land-turn`: no project, no workspace daemon, no runtime.

use std::io::Write;
use std::time::Instant;

use cowshed_core::CowshedError;
use cowshed_core::api::dto::LandTurnQueue;
use cowshed_core::storage::bootstrap::CanonicalRoots;
use cowshed_gateway::{ControlError, ControlFailureCode, GatewayControlClient, TurnFence};

use crate::args::AwaitTurnsArgs;
use crate::land_turn::{PreparedCommand, exit_code};
use crate::output::Output;
use crate::run::InterruptPolicy;

/// Wait for the turns, run the command under their fence, and answer the command's exit code. A
/// failure of cowshed itself is reported on stderr and exits with the wrapper codes `exec` uses,
/// so it is never mistaken for the command's own status.
pub(crate) async fn dispatch<W, E>(
    args: &AwaitTurnsArgs,
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
    args: &AwaitTurnsArgs,
    output: &mut Output<W, E>,
    policy: InterruptPolicy,
) -> Result<i32, CowshedError>
where
    W: Write + Send,
    E: Write + Send,
{
    let Some((program, rest)) = args.argv.split_first() else {
        return Err(CowshedError::internal(
            "await-turns reached its run without a command to run",
        ));
    };
    let command = PreparedCommand::prepare(program, rest)?;
    let fence = fence(output).await?;
    // Armed once the fence is held and before the child starts, so a signal that arrives from here
    // on reaches the child, and the fence is held until the child ends.
    let mut interrupts = policy.arm_forwarding();
    let held = command.spawn(Vec::new(), None)?;
    let program_name = held.name.clone();
    let (status, relay) = held.wait(&mut interrupts).await;
    // Lifted before this exits, so no ask that reaches the gateway after it meets the fence. A
    // gateway the command restarted closed it already.
    if let Some(fence) = fence
        && let Err(error) = fence.lift().await
    {
        output
            .guidance(&format!(
                "await-turns: the install's fence on the land turns was not lifted ({error}); the \
                 gateway lifts it as this exits"
            ))
            .map_err(write_failed)?;
    }
    let status = status.map_err(|error| {
        CowshedError::internal(format!("cannot wait for {program_name}: {error}"))
    })?;
    relay?;
    exit_code(status)
}

/// The host gateway's fence on the land turns, taken in the install's order, saying on `output`
/// who it waits behind and how long it waited. A gateway that cannot wait, a build from before
/// the wait or one that does not answer, is said, and the command runs unfenced, as an install
/// did before the wait: its `setup` reads the turns under the slot and refuses a held one.
async fn fence<W, E>(output: &mut Output<W, E>) -> Result<Option<TurnFence>, CowshedError>
where
    W: Write + Send,
    E: Write + Send,
{
    let socket = CanonicalRoots::global().control_socket();
    let client = GatewayControlClient::new(socket).map_err(|error| {
        CowshedError::internal(format!("cannot name the host gateway's socket: {error}"))
    })?;
    let started = Instant::now();
    // A report that cannot be written is kept, not returned, until the wait is over.
    let mut written: Result<(), CowshedError> = Ok(());
    let waited = client
        .wait_land_turns(u64::MAX, |queues| {
            written = output.guidance(&waiting_line(queues)).map_err(write_failed);
        })
        .await;
    written?;
    let fence = match waited {
        Ok(fence) => {
            output
                .guidance(&format!(
                    "await-turns: the land turns are fenced for the install after {} s; lands that \
                     ask now wait for it",
                    started.elapsed().as_secs()
                ))
                .map_err(write_failed)?;
            Some(fence)
        }
        Err(ControlError::Rejected {
            code: ControlFailureCode::InvalidRequest,
            message,
        }) => {
            output
                .guidance(&format!(
                    "await-turns: the running gateway cannot wait for its land turns ({message}); \
                     running the command unfenced"
                ))
                .map_err(write_failed)?;
            None
        }
        Err(error) => {
            output
                .guidance(&format!(
                    "await-turns: the running gateway did not fence its land turns ({error}); \
                     running the command unfenced"
                ))
                .map_err(write_failed)?;
            None
        }
    };
    Ok(fence)
}

/// What the install waits behind, from the queues the gateway answered as it began to wait: how
/// many tickets are ahead of it, and who holds each repository's turn.
fn waiting_line(queues: &[LandTurnQueue]) -> String {
    let ahead: usize = queues.iter().map(|queue| queue.tickets.len()).sum();
    let holders = queues
        .iter()
        .filter_map(|queue| {
            queue.tickets.first().map(|holder| {
                format!(
                    "{} holds {} (ticket {}, pid {}, asked {})",
                    holder.workspace,
                    queue.repo,
                    holder.number,
                    holder.pid,
                    crate::land_turn::local_clock(holder.asked_unix_ms),
                )
            })
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "await-turns: the install waits for {ahead} land turn(s) queued before it: {holders}; \
         lands that ask meanwhile wait behind it"
    )
}

fn write_failed(error: std::io::Error) -> CowshedError {
    CowshedError::internal(format!(
        "cowshed cannot write its await-turns report: {error}"
    ))
}
