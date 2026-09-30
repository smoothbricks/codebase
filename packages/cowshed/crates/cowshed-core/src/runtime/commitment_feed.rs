//! The commitments a served supervisor recorded, kept for the controllers that forward them
//! into a sink of their own (11_shell.md "Supervisor", 13_telemetry.md).
//!
//! A served supervisor records every commitment to the host's default sink, as any cowshed
//! process does. A controller with a sink of its own — a supervising runtime's durable log —
//! reads the supervisor's commitments since the last cursor it forwarded and acknowledges them
//! by cursor; the supervisor keeps each until acknowledged, up to a bound past which the oldest
//! go and the next read says so.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use super::supervisor::{CommitmentDraft, CommitmentSink};
use crate::error::Result;

/// Commitments kept for forwarding before the oldest are dropped.
const MAX_KEPT: usize = 65_536;
/// Commitments one read returns at most.
pub const MAX_PAGE: usize = 1_024;

#[derive(Clone, Default)]
pub struct CommitmentFeed {
    state: Arc<Mutex<FeedState>>,
    arrived: Arc<Notify>,
}

#[derive(Default)]
struct FeedState {
    /// The cursor the next commitment gets; cursors start at 1.
    next: u64,
    kept: VecDeque<(u64, CommitmentDraft)>,
    /// Every cursor at or below this was dropped unacknowledged.
    dropped_through: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FeedPage {
    pub entries: Vec<FeedEntry>,
    /// Cursors after the one asked for that were dropped before anyone read them.
    pub lost_through: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FeedEntry {
    pub cursor: u64,
    pub draft: CommitmentDraft,
}

impl CommitmentFeed {
    fn push(&self, draft: CommitmentDraft) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next = state.next.max(1);
        let cursor = state.next;
        state.next += 1;
        state.kept.push_back((cursor, draft));
        while state.kept.len() > MAX_KEPT {
            if let Some((dropped, _)) = state.kept.pop_front() {
                state.dropped_through = dropped;
            }
        }
        drop(state);
        self.arrived.notify_waiters();
    }

    /// The commitments after `after`, waiting up to `wait` for one when there is none.
    pub async fn since(&self, after: u64, wait: std::time::Duration) -> FeedPage {
        // Registered before the look, so a commitment pushed between the look and the wait
        // still wakes it.
        let arrived = self.arrived.notified();
        tokio::pin!(arrived);
        arrived.as_mut().enable();
        if let Some(page) = self.page(after) {
            return page;
        }
        let _ = tokio::time::timeout(wait, arrived).await;
        self.page(after).unwrap_or(FeedPage {
            entries: Vec::new(),
            lost_through: None,
        })
    }

    fn page(&self, after: u64) -> Option<FeedPage> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries: Vec<FeedEntry> = state
            .kept
            .iter()
            .filter(|(cursor, _)| *cursor > after)
            .take(MAX_PAGE)
            .map(|(cursor, draft)| FeedEntry {
                cursor: *cursor,
                draft: draft.clone(),
            })
            .collect();
        let lost_through = (state.dropped_through > after).then_some(state.dropped_through);
        (!entries.is_empty() || lost_through.is_some()).then_some(FeedPage {
            entries,
            lost_through,
        })
    }

    /// Forget every commitment up to and including `through`.
    pub fn acknowledge(&self, through: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state
            .kept
            .front()
            .is_some_and(|(cursor, _)| *cursor <= through)
        {
            state.kept.pop_front();
        }
    }
}

/// Records to `inner` — the host's default sink — and keeps the commitment for forwarding.
#[derive(Clone)]
pub struct FeedingSink<S> {
    pub inner: S,
    pub feed: CommitmentFeed,
}

#[async_trait]
impl<S: CommitmentSink + Send> CommitmentSink for FeedingSink<S> {
    async fn record(&mut self, draft: CommitmentDraft) -> Result<()> {
        self.inner.record(draft.clone()).await?;
        self.feed.push(draft);
        Ok(())
    }
}
