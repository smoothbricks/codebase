//! The land target's ref turn (02_workspaces.md "`cowshed land`" step 3; 05_gateway.md "A ref
//! turn"): a land-turn queue every writer that moves the target's branch outside cowshed takes
//! while it does (`cowshed land-turn <repo>.ref -- <commit>`). A land takes it for the span the
//! ref fence covers, from re-reading the target head through the fast-forward, and for nothing
//! else, so a writer queued behind it waits for that fence alone, in the order the writers asked,
//! and is told who holds the turn.

use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::api::dto::LandTurnName;
use crate::land_turn::{self, OutsideHolders, Turn, TurnAsk, Wait};
use crate::{CowshedError, Result};

/// Who takes a ref turn: a land's ref fence, for its workspace, or a doc commit. It names the
/// ticket's workspace and command and the verb its waits are said under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefWriter<'a> {
    Land { workspace: &'a str },
    DocCommit,
}

impl RefWriter<'_> {
    fn workspace(self) -> String {
        match self {
            Self::Land { workspace } => workspace.to_owned(),
            Self::DocCommit => "doc-commit".to_owned(),
        }
    }

    fn command(self) -> String {
        match self {
            Self::Land { workspace } => format!("cowshed land {workspace} (ref fence)"),
            Self::DocCommit => "cowshed doc-commit".to_owned(),
        }
    }

    fn verb(self) -> &'static str {
        match self {
            Self::Land { .. } => "land",
            Self::DocCommit => "doc-commit",
        }
    }
}

/// A held ref turn. The git that moves the target's ref under it binds itself to it as it starts
/// ([`Self::bind_at_exec`]),
/// so the gateway keeps the turn until that git exits, whatever becomes of this process. Ending it
/// records how the fence ended; dropping it without an end leaves the turn to be recorded as
/// abandoned.
pub struct RefTurn {
    turn: Turn,
}

impl RefTurn {
    /// Take the ref turn `queue` for `writer` from the gateway at `socket`, where
    /// `cowshed land-turn <queue>` and `cowshed doc-commit` take it too. Nothing bounds the wait:
    /// the holder is a writer moving the same branch, which a land follows, never overtakes. The
    /// wait reports every [`cowshed_os::flock::HOLDER_REPORT`] who holds the turn and how many are
    /// ahead. A land dropped while it waits closes its connection, and the gateway takes its place
    /// out of the queue.
    pub async fn acquire_in(
        socket: &Path,
        queue: &LandTurnName,
        writer: RefWriter<'_>,
    ) -> Result<Self> {
        let ask = TurnAsk {
            repo: queue.clone(),
            workspace: writer.workspace(),
            command: writer.command(),
        };
        let verb = writer.verb();
        let started = Instant::now();
        let mut waited = false;
        let turn = land_turn::take(
            socket,
            &ask,
            None,
            cowshed_os::flock::HOLDER_REPORT,
            |wait| {
                waited = true;
                report(queue, verb, wait);
            },
        )
        .await
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot take the ref turn {queue}: {error}"),
                "start the host gateway (`cowshed gateway start`), then retry",
            )
        })?;
        if waited {
            cowshed_os::eprintln!(
                "cowshed: {verb} took the ref turn {queue} after {}s",
                started.elapsed().as_secs()
            );
        }
        Ok(Self { turn })
    }

    /// Arm `command`, the git this process is about to start to move the ref, to bind itself to the
    /// turn as it starts ([`Turn::bind_at_exec`]): the gateway holds the turn until it exits.
    pub fn bind_at_exec(&self, command: &mut std::process::Command) {
        self.turn.bind_at_exec(command);
    }

    /// The gateway's answer to the bind of the git `pid` just spawned ([`Turn::bound`]).
    pub async fn bound(&mut self, pid: u32) -> Result<()> {
        self.turn.bound(pid).await.map_err(|error| {
            CowshedError::environment_missing(
                format!("the ref turn could not hold the git that moves the ref: {error}"),
                "start the host gateway (`cowshed gateway start`), then retry",
            )
        })
    }

    /// Resolves once the gateway has closed the connection that holds the turn ([`Turn::lost`]).
    /// Cancel-safe, so a holder waits on it beside its git.
    pub async fn lost(&mut self) -> std::io::Error {
        self.turn.lost().await
    }

    /// End the turn, recording `outcome` (how the fence ended) with the gateway.
    pub fn end(self, outcome: String) {
        self.turn.end(outcome);
    }
}

/// One wait report on stderr, in the words `cowshed land-turn` uses.
fn report(queue: &LandTurnName, verb: &str, wait: &Wait) {
    match wait {
        Wait::Queued { holder, ahead } => cowshed_os::eprintln!(
            "cowshed: {verb} waits for the ref turn {queue}: {ahead} ahead; {} holds it (pid {}, asked {}s ago)",
            holder.workspace,
            holder.pid,
            seconds_since(holder.asked_unix_ms),
        ),
        // A ref turn names no outside lock, so the queue never reports one; said as it is if it did.
        Wait::Outside { lock, holders } => cowshed_os::eprintln!(
            "cowshed: {verb} waits for the ref turn {queue}: {} is held outside the queue{}",
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
