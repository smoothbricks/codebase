//! The host CPU budget (05_gateway.md, "Host CPU budget").
//!
//! The host has one budget of CPU tokens, about one per core. A parallel runner (a nextest run, a
//! `bun test --parallel`, a cargo build) asks for as many tokens as it would run threads or
//! processes at once, starts once granted, sizes its parallelism to the grant, and returns the
//! tokens when it exits. Every gate keeps running its tasks in parallel; what the budget bounds is
//! the total, so the host's runnable work stays near its core count instead of the sum of every
//! gate's own idea of the machine.
//!
//! Grants are fair between checkouts, not between requests: the checkout holding the fewest
//! tokens is served first, and while another checkout waits nobody is granted beyond its fair
//! share of the host, so a gate with sixty runners queued cannot crowd out one with three. A grant
//! is all at once and between one and the request's `want`; a request never holds part of a grant
//! while waiting for the rest, so no two requests can deadlock on each other's tokens.
//!
//! Fairness only acts when a grant is made: a running nextest or cargo cannot hand tokens back.
//! So no single grant exceeds half the host ([`Ledger::max_grant`]). A checkout alone still fills
//! the host with two runners, but a checkout that arrives while one runner holds its grant finds
//! the other half, or the next runner's tokens, instead of waiting out the whole run.
//!
//! [`Ledger`] is the whole policy as a pure state machine; [`CpuBudget`] is the thin actor that
//! feeds it requests and departures and answers status.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cowshed_gateway_types::{CpuBudgetStatus, CpuCheckoutStatus};
use tokio::sync::{mpsc, oneshot};

use crate::disk_lease::{Claim, Ticket};

/// How many tokens the host has.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CpuBudgetLimits {
    /// The host total. One token is one runnable thread or process.
    pub tokens: NonZeroUsize,
}

impl Default for CpuBudgetLimits {
    /// One token per core the host reports.
    fn default() -> Self {
        Self {
            tokens: std::thread::available_parallelism()
                .expect("every supported host reports how many CPUs it has"),
        }
    }
}

/// What a [`Ledger`] transition did, for the actor to carry out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Effect {
    /// The ticket may start its runner now with `tokens` threads or processes.
    Grant {
        ticket: Ticket,
        tokens: usize,
        want: usize,
        waited: Duration,
        checkout: Arc<str>,
        claim: Claim,
    },
}

#[derive(Debug)]
struct Waiter {
    ticket: Ticket,
    /// Capped at [`Ledger::max_grant`] when asked.
    want: usize,
    asked: Instant,
    claim: Claim,
}

#[derive(Debug)]
struct Holder {
    ticket: Ticket,
    tokens: usize,
}

/// One checkout's stake: what its runners hold, and who of it waits, oldest first.
#[derive(Debug, Default)]
struct Checkout {
    held: usize,
    running: Vec<Holder>,
    waiting: VecDeque<Waiter>,
}

/// The budget's state.
///
/// Invariants: `free` plus every checkout's `held` is `total`; a checkout is present exactly while
/// it holds or waits; every ticket in `tickets` is a holder or a waiter of the checkout it names.
#[derive(Debug)]
pub(crate) struct Ledger {
    total: usize,
    /// The most one grant holds: half the host, rounded up.
    max_grant: usize,
    free: usize,
    checkouts: HashMap<Arc<str>, Checkout>,
    tickets: HashMap<Ticket, Arc<str>>,
}

impl Ledger {
    pub(crate) fn new(limits: CpuBudgetLimits) -> Self {
        Self {
            total: limits.tokens.get(),
            max_grant: limits.tokens.get().div_ceil(2),
            free: limits.tokens.get(),
            checkouts: HashMap::new(),
            tickets: HashMap::new(),
        }
    }

    /// `ticket` of `checkout` asks for up to `want` tokens. A `want` above [`Self::max_grant`]
    /// asks for that.
    pub(crate) fn ask(
        &mut self,
        now: Instant,
        ticket: Ticket,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        self.checkouts
            .entry(Arc::clone(&checkout))
            .or_default()
            .waiting
            .push_back(Waiter {
                ticket,
                want: want.get().min(self.max_grant),
                asked: now,
                claim,
            });
        self.tickets.insert(ticket, checkout);
        self.admit(now, effects);
    }

    /// `ticket` is done: a holder returns its tokens, a waiter leaves the queue. A ticket the
    /// ledger does not know changes nothing.
    pub(crate) fn leave(&mut self, now: Instant, ticket: Ticket, effects: &mut Vec<Effect>) {
        let Some(name) = self.tickets.remove(&ticket) else {
            return;
        };
        let checkout = self
            .checkouts
            .get_mut(&name)
            .expect("a known ticket's checkout is present");
        if let Some(position) = checkout
            .running
            .iter()
            .position(|holder| holder.ticket == ticket)
        {
            let holder = checkout.running.swap_remove(position);
            checkout.held -= holder.tokens;
            self.free += holder.tokens;
        } else if let Some(position) = checkout
            .waiting
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            checkout.waiting.remove(position);
        }
        if checkout.running.is_empty() && checkout.waiting.is_empty() {
            self.checkouts.remove(&name);
        }
        self.admit(now, effects);
    }

    /// Grant waiters while tokens are free, fairest first.
    ///
    /// The next grant goes to the waiting checkout that holds the fewest tokens (the older head
    /// waiter on a tie), to its oldest waiter. A checkout's fair share is the host total divided
    /// among the checkouts holding or waiting; its claim is what remains of that share, at least
    /// one. While another checkout waits, the grant is exactly the claim or the `want`, whichever
    /// is smaller, so nobody outruns a waiting peer; with nobody else waiting it is every free
    /// token up to `want`. A head waiter whose least grant is not free yet waits for it, and so
    /// does everyone behind it: skipping to a smaller request would starve a large one for good.
    fn admit(&mut self, now: Instant, effects: &mut Vec<Effect>) {
        while self.free > 0 {
            // Some checkout waits whenever the loop gets past this, so the map is not empty.
            let Some((name, checkout)) = self
                .checkouts
                .iter()
                .filter_map(|(name, checkout)| Some((name, checkout, checkout.waiting.front()?)))
                .min_by_key(|(_, checkout, head)| (checkout.held, head.ticket))
                .map(|(name, checkout, _)| (name, checkout))
            else {
                return;
            };
            let share = (self.total / self.checkouts.len()).max(1);
            let head = checkout
                .waiting
                .front()
                .expect("chosen for its head waiter");
            let least = head.want.min(share.saturating_sub(checkout.held).max(1));
            if self.free < least {
                return;
            }
            let peers_wait = self
                .checkouts
                .iter()
                .any(|(other, peer)| other != name && !peer.waiting.is_empty());
            let tokens = if peers_wait {
                least
            } else {
                head.want.min(self.free)
            };
            let name = Arc::clone(name);
            let checkout = self
                .checkouts
                .get_mut(&name)
                .expect("the chosen checkout is present");
            let waiter = checkout
                .waiting
                .pop_front()
                .expect("chosen for its head waiter");
            checkout.held += tokens;
            checkout.running.push(Holder {
                ticket: waiter.ticket,
                tokens,
            });
            self.free -= tokens;
            effects.push(Effect::Grant {
                ticket: waiter.ticket,
                tokens,
                want: waiter.want,
                waited: now.saturating_duration_since(waiter.asked),
                checkout: name,
                claim: waiter.claim,
            });
        }
    }

    pub(crate) fn status(&self) -> CpuBudgetStatus {
        let mut checkouts: Vec<CpuCheckoutStatus> = self
            .checkouts
            .iter()
            .map(|(name, checkout)| CpuCheckoutStatus {
                checkout: name.to_string(),
                held: checkout.held,
                running: checkout.running.len(),
                waiting: checkout.waiting.len(),
            })
            .collect();
        checkouts.sort_unstable_by(|left, right| left.checkout.cmp(&right.checkout));
        CpuBudgetStatus {
            total: self.total,
            held: self.total - self.free,
            checkouts,
        }
    }
}

enum Message {
    Ask {
        ticket: Ticket,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        granted: oneshot::Sender<usize>,
    },
    Leave {
        ticket: Ticket,
    },
    Status {
        reply: oneshot::Sender<CpuBudgetStatus>,
    },
}

/// The gateway's CPU budget: a handle to the actor that owns the [`Ledger`].
#[derive(Clone, Debug)]
pub(crate) struct CpuBudget {
    messages: mpsc::UnboundedSender<Message>,
    tickets: Arc<AtomicU64>,
}

/// One request's stake in the budget, waiting or holding. Dropping it returns what it holds.
#[derive(Debug)]
pub(crate) struct Stake {
    ticket: Ticket,
    messages: mpsc::UnboundedSender<Message>,
}

impl Drop for Stake {
    fn drop(&mut self) {
        // An actor that is gone holds no tokens to return.
        let _ = self.messages.send(Message::Leave {
            ticket: self.ticket,
        });
    }
}

impl CpuBudget {
    /// Start the budget. It runs until the last handle and stake are gone.
    pub(crate) fn start(limits: CpuBudgetLimits) -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(Ledger::new(limits), receiver));
        Self {
            messages,
            tickets: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Ask for up to `want` tokens for `checkout`. The receiver resolves to the grant; the stake
    /// holds the place in the queue or the tokens until it is dropped. A receiver that errors
    /// means the budget is gone.
    pub(crate) fn ask(
        &self,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
    ) -> (Stake, oneshot::Receiver<usize>) {
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        let (granted, receiver) = oneshot::channel();
        let _ = self.messages.send(Message::Ask {
            ticket,
            checkout,
            want,
            claim,
            granted,
        });
        (
            Stake {
                ticket,
                messages: self.messages.clone(),
            },
            receiver,
        )
    }

    /// The ledger as it stands; `None` when the budget is gone.
    pub(crate) async fn status(&self) -> Option<CpuBudgetStatus> {
        let (reply, receiver) = oneshot::channel();
        self.messages.send(Message::Status { reply }).ok()?;
        receiver.await.ok()
    }
}

async fn run(mut ledger: Ledger, mut messages: mpsc::UnboundedReceiver<Message>) {
    let mut grants: HashMap<Ticket, oneshot::Sender<usize>> = HashMap::new();
    let mut effects = Vec::new();
    while let Some(message) = messages.recv().await {
        match message {
            Message::Ask {
                ticket,
                checkout,
                want,
                claim,
                granted,
            } => {
                grants.insert(ticket, granted);
                ledger.ask(Instant::now(), ticket, checkout, want, claim, &mut effects);
            }
            Message::Leave { ticket } => {
                grants.remove(&ticket);
                ledger.leave(Instant::now(), ticket, &mut effects);
            }
            Message::Status { reply } => {
                let _ = reply.send(ledger.status());
            }
        }
        for effect in effects.drain(..) {
            let Effect::Grant {
                ticket,
                tokens,
                want,
                waited,
                checkout,
                claim,
            } = effect;
            // A client gone before its grant leaves next, and its tokens come back with it.
            if let Some(granted) = grants.remove(&ticket) {
                let _ = granted.send(tokens);
            }
            cowshed_core::timing::event("cpu-tokens", || {
                format!(
                    "grant {tokens}/{want} after {waited:?} to {checkout}: pid {} `{}`",
                    claim
                        .pid
                        .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
                    claim.command,
                )
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ledger of `total` tokens driven at explicit instants, collecting each step's grants as
    /// `(ticket, tokens)`.
    struct Clock {
        ledger: Ledger,
        origin: Instant,
        effects: Vec<Effect>,
    }

    impl Clock {
        fn new(total: usize) -> Self {
            Self {
                ledger: Ledger::new(CpuBudgetLimits {
                    tokens: NonZeroUsize::new(total).unwrap(),
                }),
                origin: Instant::now(),
                effects: Vec::new(),
            }
        }

        fn ask(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
            want: usize,
        ) -> Vec<(Ticket, usize)> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.ask(
                now,
                ticket,
                Arc::from(checkout),
                NonZeroUsize::new(want).unwrap(),
                Claim::new(&format!("t{ticket}"), Some(7)),
                &mut self.effects,
            );
            self.grants()
        }

        fn leave(&mut self, ms: u64, ticket: Ticket) -> Vec<(Ticket, usize)> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.leave(now, ticket, &mut self.effects);
            self.grants()
        }

        fn grants(&mut self) -> Vec<(Ticket, usize)> {
            self.effects
                .drain(..)
                .map(|Effect::Grant { ticket, tokens, .. }| (ticket, tokens))
                .collect()
        }

        /// The ledger balances: free plus every checkout's holdings is the total, and each
        /// checkout's `held` is the sum of its holders.
        fn balanced(&self) -> bool {
            let ledger = &self.ledger;
            let held: usize = ledger
                .checkouts
                .values()
                .map(|checkout| checkout.held)
                .sum();
            ledger.free + held == ledger.total
                && ledger.checkouts.values().all(|checkout| {
                    checkout
                        .running
                        .iter()
                        .map(|holder| holder.tokens)
                        .sum::<usize>()
                        == checkout.held
                })
        }
    }

    #[test]
    fn a_lone_request_takes_what_it_wants_and_a_want_past_half_the_host_takes_half() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 4), [(1, 4)]);
        assert_eq!(clock.leave(1, 1), []);
        assert_eq!(clock.ask(2, 2, "a", 64), [(2, 9)]);
        assert_eq!(clock.ledger.status().held, 9);
        assert_eq!(clock.leave(3, 2), []);
        assert_eq!(clock.ledger.status().held, 0);
        assert!(clock.ledger.status().checkouts.is_empty());
        assert!(clock.balanced());
    }

    #[test]
    fn a_newcomer_starts_beside_a_checkout_whose_runner_asked_for_the_whole_host() {
        // A running nextest cannot give tokens back, so fairness at grant time alone let one
        // runner of one checkout hold 17 of 18 while another checkout's gate waited it out.
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "carry", 18), [(1, 9)]);
        assert_eq!(clock.ask(1, 2, "train", 18), [(2, 9)]);
        assert!(clock.balanced());
    }

    #[test]
    fn with_nobody_else_waiting_a_checkout_takes_free_tokens_up_to_its_want_and_the_cap() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 5), [(1, 5)]);
        // Nobody else waits, so b is not held to its share of 9 by fairness, only by the cap.
        assert_eq!(clock.ask(1, 2, "b", 18), [(2, 9)]);
        // a's second runner takes the rest: alone, a checkout still fills the host.
        assert_eq!(clock.ask(2, 3, "a", 18), [(3, 4)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_full_host_hands_freed_tokens_to_the_checkout_holding_least() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 18), [(1, 9)]);
        assert_eq!(clock.ask(0, 2, "a", 18), [(2, 9)]);
        // a's next runner queues first; b arrives later and wants the whole host.
        assert_eq!(clock.ask(1, 3, "a", 1), []);
        assert_eq!(clock.ask(2, 4, "b", 18), []);
        // Two checkouts, a share of 9 each. b holds least, so the 9 a's runner returns are b's,
        // and a's waiter waits for a's next return.
        assert_eq!(clock.leave(3, 1), [(4, 9)]);
        assert_eq!(clock.leave(4, 2), [(3, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_gate_with_many_runners_cannot_crowd_out_one_with_few() {
        let mut clock = Clock::new(12);
        for ticket in 0..60 {
            clock.ask(0, ticket, "busy", 1);
        }
        assert_eq!(clock.ledger.status().held, 12);
        assert_eq!(clock.ask(1, 100, "quiet", 1), []);
        assert_eq!(clock.ask(1, 101, "quiet", 1), []);
        // Every token busy returns goes to quiet until quiet holds as much as busy would allow.
        assert_eq!(clock.leave(2, 0), [(100, 1)]);
        assert_eq!(clock.leave(3, 1), [(101, 1)]);
        assert_eq!(clock.leave(4, 2), [(12, 1)]);
        let status = clock.ledger.status();
        assert_eq!(status.checkouts[0].checkout, "busy");
        assert_eq!(status.checkouts[0].held, 10);
        assert_eq!(status.checkouts[1].held, 2);
        assert!(clock.balanced());
    }

    #[test]
    fn a_large_request_waits_for_its_share_and_is_not_starved_by_small_ones() {
        let mut clock = Clock::new(8);
        assert_eq!(clock.ask(0, 1, "a", 4), [(1, 4)]);
        assert_eq!(clock.ask(0, 2, "a", 3), [(2, 3)]);
        assert_eq!(clock.ask(1, 3, "b", 1), [(3, 1)]);
        assert_eq!(clock.ask(2, 4, "c", 4), []);
        assert_eq!(clock.ask(3, 5, "b", 1), []);
        // One token frees. Three checkouts share 8 as 2 each; c holds least and asked first, so
        // it waits for both tokens of its share, and b's single-token request waits behind it.
        assert_eq!(clock.leave(4, 3), []);
        // a's runner returns its 4: c takes its share of 2 while b waits, then b its token.
        assert_eq!(clock.leave(5, 1), [(4, 2), (5, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_waiter_that_leaves_is_never_granted_and_a_holder_that_leaves_returns_its_tokens() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask(0, 1, "a", 2), [(1, 2)]);
        assert_eq!(clock.ask(0, 2, "a", 2), [(2, 2)]);
        assert_eq!(clock.ask(1, 3, "b", 2), []);
        assert_eq!(clock.leave(2, 3), []);
        assert_eq!(clock.ledger.status().checkouts.len(), 1);
        assert_eq!(clock.leave(3, 1), []);
        assert_eq!(clock.leave(3, 2), []);
        assert_eq!(clock.ledger.status().held, 0);
        // An unknown ticket — a stake dropped twice, or a grant already returned — is ignored.
        assert_eq!(clock.leave(4, 1), []);
        assert!(clock.balanced());
    }

    #[test]
    fn more_checkouts_than_tokens_share_one_token_each_in_arrival_order() {
        let mut clock = Clock::new(2);
        assert_eq!(clock.ask(0, 1, "a", 1), [(1, 1)]);
        assert_eq!(clock.ask(0, 2, "a", 1), [(2, 1)]);
        assert_eq!(clock.ask(1, 3, "b", 8), []);
        assert_eq!(clock.ask(2, 4, "c", 8), []);
        // Three checkouts round a share of zero up to one: b holds nothing and asked first.
        assert_eq!(clock.leave(3, 1), [(3, 1)]);
        // a is gone: c holds least, and with nobody else waiting takes the free token.
        assert_eq!(clock.leave(4, 2), [(4, 1)]);
        assert!(clock.balanced());
    }
}
