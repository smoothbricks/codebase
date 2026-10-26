//! Land-turn vocabulary: the ticket a land queues with and the turn log's event
//! records, as their files record them. The queue machinery lives with the lands.

use serde::{Deserialize, Serialize};
/// One land's place in its repository's queue, as its ticket file records it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ticket {
    /// The order the land asked in: lower holds the turn first.
    pub number: u64,
    /// The process that waits for or holds the turn.
    pub pid: u32,
    /// The workspace the land lands.
    pub workspace: String,
    /// When the land asked, in milliseconds since the Unix epoch.
    pub asked_unix_ms: u64,
    /// The command the turn runs.
    pub command: String,
}
/// What happened to a ticket, as the turn log records it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TurnEventKind {
    /// The land asked and took its place in line.
    Queued,
    /// The land holds the turn.
    Taken,
    /// The land's command ended; `outcome` says how.
    Finished,
    /// The land ended without recording an outcome, found unlocked by whoever read the queue
    /// next. It may have been dropped on an error, killed, crashed, or lost with its session.
    Abandoned,
}

/// One line of a repository's turn log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnEvent {
    /// When it was recorded, in milliseconds since the Unix epoch.
    pub at_unix_ms: u64,
    pub event: TurnEventKind,
    pub ticket: Ticket,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}
