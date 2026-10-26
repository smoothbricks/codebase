//! The land target's ref turn (02_workspaces.md "`cowshed land`" step 3; 05_gateway.md "A ref
//! turn"): a land-turn queue every writer that moves the target's branch outside cowshed takes
//! while it does (`cowshed land-turn <repo>.ref -- <commit>`). A land takes it for the span the
//! ref fence covers, from re-reading the target head through the fast-forward, and for nothing
//! else, so a writer queued behind it waits for that fence alone, in the order the writers asked,
//! and is told who holds the turn.

use std::os::fd::RawFd;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::api::dto::LandTurnName;
use crate::gateway_sessions::canonical_home;
use crate::host_dirs::support_directory;
use crate::land_turn::{self, OutsideHolders, Turn, Wait};
use crate::{CowshedError, Result};

/// How often a land that waits for the ref turn says who holds it: the ref lock's cadence.
const WAIT_REPORT: Duration = Duration::from_secs(10);

/// A held ref turn. The git that moves the target's ref under it is handed its ticket's
/// descriptor ([`Self::descriptor`]), so the kernel keeps the turn until that git ends. Ending it
/// records how the fence ended; dropping it without an end leaves the turn to be recorded as
/// abandoned by the next queue scan.
pub struct RefTurn {
    turn: Turn,
}

impl RefTurn {
    /// Take the ref turn `queue` for a land of `workspace`, in the queue `cowshed land-turn
    /// <queue>` takes: this user's support directory. Nothing bounds the wait: the holder is a
    /// writer moving the same branch, which a land follows, never overtakes.
    pub async fn acquire(queue: &LandTurnName, workspace: &str) -> Result<Self> {
        let home = canonical_home()?;
        Self::acquire_in(
            &land_turn::land_turn_root(&support_directory(&home)),
            queue,
            workspace,
        )
        .await
    }

    /// [`Self::acquire`] in the land-turn root `root`. The queue is waited for on a blocking
    /// thread, which reports every [`WAIT_REPORT`] who holds the turn and how many are ahead. A
    /// land dropped while it waits leaves that thread its place: it takes the turn when its turn
    /// comes and drops it at once, so the queue behind it moves on.
    pub async fn acquire_in(root: &Path, queue: &LandTurnName, workspace: &str) -> Result<Self> {
        let directory = root.join(queue.as_str());
        let label = workspace.to_owned();
        let command = format!("cowshed land {workspace} (ref fence)");
        let (reports, mut received) = tokio::sync::mpsc::unbounded_channel();
        let mut taking = tokio::task::spawn_blocking(move || {
            land_turn::take(&directory, &label, &command, None, WAIT_REPORT, |wait| {
                // The receiver is read until this call returns; a send fails only once the land
                // that would hear it is gone.
                let _ = reports.send(wait.clone());
            })
        });
        let started = Instant::now();
        let mut waited = false;
        let joined = loop {
            tokio::select! {
                Some(wait) = received.recv() => {
                    waited = true;
                    report(queue, &wait);
                }
                joined = &mut taking => break joined,
            }
        };
        while let Ok(wait) = received.try_recv() {
            waited = true;
            report(queue, &wait);
        }
        let turn = joined
            .map_err(|error| {
                CowshedError::internal(format!("the waiter for ref turn {queue} failed: {error}"))
            })?
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!("cannot take the ref turn {queue}: {error}"),
                    "the land-turn directory in this user's support directory must be writable",
                )
            })?;
        if waited {
            cowshed_os::eprintln!(
                "cowshed: land took the ref turn {queue} after {}s",
                started.elapsed().as_secs()
            );
        }
        Ok(Self { turn })
    }

    /// The descriptor that holds the turn, for the fence's git to inherit
    /// ([`crate::held_lock::keep_across_exec`]).
    pub fn descriptor(&self) -> RawFd {
        self.turn.ticket_descriptor()
    }

    /// End the turn, recording `outcome` (how the fence ended) in the queue's turn log.
    pub fn end(self, outcome: String) {
        self.turn.end(outcome);
    }
}

/// One wait report on stderr, in the words `cowshed land-turn` uses.
fn report(queue: &LandTurnName, wait: &Wait) {
    match wait {
        Wait::Queued { holder, ahead } => cowshed_os::eprintln!(
            "cowshed: land waits for the ref turn {queue}: {ahead} ahead; {} holds it (pid {}, asked {}s ago)",
            holder.workspace,
            holder.pid,
            seconds_since(holder.asked_unix_ms),
        ),
        // A ref turn names no outside lock, so the queue never reports one; said as it is if it did.
        Wait::Outside { lock, holders } => cowshed_os::eprintln!(
            "cowshed: land waits for the ref turn {queue}: {} is held outside the queue{}",
            lock.display(),
            match holders {
                OutsideHolders::Open(open) => format!(", open in {}", open.len()),
                OutsideHolders::NoneOpen => String::new(),
                OutsideHolders::Unknown(reason) => format!(", holders unknown: {reason}"),
            },
        ),
    }
}

fn seconds_since(unix_ms: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
    now.saturating_sub(unix_ms) / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue() -> LandTurnName {
        LandTurnName::new("repo.ref").unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_takes_a_free_ref_turn_and_the_next_writer_takes_it_once_ended() {
        let root = crate::temp_root::TempRoot::new("ref-turn");
        let held = RefTurn::acquire_in(&root, &queue(), "topic").await.unwrap();
        let tickets = land_turn::observe(&root.join("repo.ref")).unwrap();
        assert_eq!(tickets.len(), 1, "the land holds one ticket: {tickets:?}");
        assert_eq!(tickets[0].workspace, "topic");
        assert_eq!(tickets[0].pid, std::process::id());
        held.end("delivered".to_owned());
        let next = RefTurn::acquire_in(&root, &queue(), "doc-commit").await;
        next.expect("an ended ref turn stayed held")
            .end("committed".to_owned());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_that_asks_for_a_held_ref_turn_holds_it_once_the_writer_ends() {
        let root = crate::temp_root::TempRoot::new("ref-turn");
        let writer = land_turn::take(
            &root.join("repo.ref"),
            "doc-commit",
            "git commit",
            None,
            WAIT_REPORT,
            |_| panic!("the writer is first in the queue"),
        )
        .unwrap();
        let waiting = tokio::spawn({
            let root = root.to_path_buf();
            async move { RefTurn::acquire_in(&root, &queue(), "topic").await }
        });
        // The land queues behind the writer: two live tickets, the writer's first.
        let mut tickets = Vec::new();
        for _ in 0..200 {
            tickets = land_turn::observe(&root.join("repo.ref")).unwrap();
            if tickets.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            tickets
                .iter()
                .map(|ticket| ticket.workspace.as_str())
                .collect::<Vec<_>>(),
            ["doc-commit", "topic"],
            "the land did not queue behind the writer"
        );
        assert!(!waiting.is_finished(), "the land took a held ref turn");
        writer.end("committed".to_owned());
        let held = waiting
            .await
            .unwrap()
            .expect("the land never took the released turn");
        held.end("delivered".to_owned());
    }
}
