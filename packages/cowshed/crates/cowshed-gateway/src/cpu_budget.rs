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
//! Class ranks first: while any gate-class request waits, every returned token goes to gate-class
//! work and background requests stay queued. A land's gate runs as a gate job whose pool relays
//! each of its tasks' asks here at gate class (`jobserver.rs`), ranked by the gate job's own
//! admission: gate work is served oldest job first, and within one job in the order its tasks
//! asked, so an older land takes every returned token its tasks want before a younger land's
//! tasks get one. Existing grants drain whole; their FIFOs, implicit make token and test-thread
//! sizing are never resized.
//!
//! [`Ledger`] is the whole policy as a pure state machine; [`CpuBudget`] is the thin actor that
//! feeds it requests and departures and answers status.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cowshed_gateway_types::{CpuBudgetStatus, CpuCheckoutStatus, CpuGateWaiter, JobserverClass};
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

/// The gate job a request serves, as that job's own admission ticket: the smaller ticket is the
/// older job, and gate-class work is served oldest job first.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct JobAge(Ticket);

/// Where a request stands in the order: its class and, for gate work, the job whose age ranks it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Rank {
    Background,
    /// A gate job's own admission: its ticket is the job's age.
    Gate,
    /// A task a gate job relays: it ranks by that job's age, not by when it asked.
    GateTask(JobAge),
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
    /// A running holder that asked to grow now holds `to` tokens, up from `from`, never past
    /// its `want`: its jobserver may hand out the difference.
    Grow {
        ticket: Ticket,
        from: usize,
        to: usize,
        want: usize,
        checkout: Arc<str>,
    },
}

#[derive(Debug)]
struct Waiter {
    ticket: Ticket,
    class: JobserverClass,
    /// The job that ranks it: the gate job it serves, or its own ticket.
    job: Ticket,
    /// The count asked for, as asked.
    want: usize,
    /// The runner can use tokens handed to it after it starts (a Cargo build's jobserver).
    grows: bool,
    asked: Instant,
    claim: Claim,
}

impl Waiter {
    /// Gate-class order: the older job first, then the request that asked first.
    fn order(&self) -> (Ticket, Ticket) {
        (self.job, self.ticket)
    }
}

#[derive(Debug)]
struct Holder {
    ticket: Ticket,
    class: JobserverClass,
    job: Ticket,
    tokens: usize,
    want: usize,
    grows: bool,
}

impl Holder {
    /// A holder that can still use more than it holds.
    fn short(&self) -> bool {
        self.grows && self.tokens < self.want
    }
}

/// Within one gate job, a waiting task starts before a running one grows.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum GateStep {
    Start,
    Grow,
}

/// The ledger's next step: start a waiter, or grow a running holder.
enum Pick {
    Grant {
        checkout: Arc<str>,
        position: usize,
        tokens: usize,
    },
    Grow {
        checkout: Arc<str>,
        index: usize,
        tokens: usize,
    },
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
    free: usize,
    checkouts: HashMap<Arc<str>, Checkout>,
    tickets: HashMap<Ticket, Arc<str>>,
}

impl Ledger {
    pub(crate) fn new(limits: CpuBudgetLimits) -> Self {
        Self {
            total: limits.tokens.get(),
            free: limits.tokens.get(),
            checkouts: HashMap::new(),
            tickets: HashMap::new(),
        }
    }

    /// Queue the exact admission record, retaining its original request instant.
    fn ask(&mut self, checkout: Arc<str>, waiter: Waiter, effects: &mut Vec<Effect>) {
        let (now, ticket) = (waiter.asked, waiter.ticket);
        self.checkouts
            .entry(Arc::clone(&checkout))
            .or_default()
            .waiting
            .push_back(waiter);
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

    /// Grant waiters while tokens are free: gate class oldest job first, background fairest, and
    /// grow short running builds with what no waiter can take.
    ///
    /// Gate-class work is ranked by the gate job it serves: the oldest job's wants come first,
    /// wherever its requests sit in their checkouts' queues, and within one job a waiting task
    /// takes `min(want, free)` before a running one grows, in the order they asked. A short
    /// gate-class holder — one that asked to grow and holds less than its `want` — takes
    /// `min(want − held, free)` more once no task of its own job or an older one waits. So a land
    /// takes every returned token its tasks want before a younger land gets one, and both before
    /// queued background work; no holder ever passes its own `want`. Background is not starved
    /// meanwhile: while gate-class work wants tokens, a background checkout that waits holding
    /// nothing still gets one token first, as long as background as a whole then still holds
    /// less than half the host, so gate-class work always holds the larger part.
    ///
    /// With no gate-class demand, background is fair between checkouts. The next grant goes to
    /// the waiting checkout that holds the fewest tokens (the older head waiter on a tie), to its
    /// oldest waiter. A checkout's fair share is the host total divided among the checkouts
    /// holding or waiting; its claim is what remains of that share, at least one. While another
    /// checkout waits, the grant is exactly the claim or the `want`, whichever is smaller, so
    /// nobody outruns a waiting peer; with nobody else waiting it is every free token up to
    /// `want`. A head waiter whose least grant is not free yet waits for it, and so does everyone
    /// behind it: skipping to a smaller request would starve a large one for good. Only with no
    /// background request waiting at all does a short background holder grow.
    fn admit(&mut self, now: Instant, effects: &mut Vec<Effect>) {
        while self.free > 0 {
            match self.next_gate().or_else(|| self.next_background()) {
                None => return,
                Some(Pick::Grant {
                    checkout: name,
                    position,
                    tokens,
                }) => {
                    let checkout = self
                        .checkouts
                        .get_mut(&name)
                        .expect("the chosen checkout is present");
                    let waiter = checkout
                        .waiting
                        .remove(position)
                        .expect("the chosen waiter is queued");
                    checkout.held += tokens;
                    checkout.running.push(Holder {
                        ticket: waiter.ticket,
                        class: waiter.class,
                        job: waiter.job,
                        tokens,
                        want: waiter.want,
                        grows: waiter.grows,
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
                Some(Pick::Grow {
                    checkout: name,
                    index,
                    tokens,
                }) => {
                    let checkout = self
                        .checkouts
                        .get_mut(&name)
                        .expect("the chosen checkout is present");
                    let holder = &mut checkout.running[index];
                    let from = holder.tokens;
                    holder.tokens += tokens;
                    checkout.held += tokens;
                    self.free -= tokens;
                    effects.push(Effect::Grow {
                        ticket: holder.ticket,
                        from,
                        to: holder.tokens,
                        want: holder.want,
                        checkout: name,
                    });
                }
            }
        }
    }

    /// The oldest running background holder that can still grow, and by how much.
    fn background_grower(&self) -> Option<Pick> {
        let (name, index, holder) = self
            .checkouts
            .iter()
            .flat_map(|(name, checkout)| {
                checkout
                    .running
                    .iter()
                    .enumerate()
                    .filter(|(_, holder)| {
                        holder.class == JobserverClass::Background && holder.short()
                    })
                    .map(move |(index, holder)| (name, index, holder))
            })
            .min_by_key(|(_, _, holder)| holder.ticket)?;
        Some(Pick::Grow {
            checkout: Arc::clone(name),
            index,
            tokens: (holder.want - holder.tokens).min(self.free),
        })
    }

    /// The next step while gate-class work wants tokens: a starved background checkout's one
    /// token, else the oldest gate job's next want — a waiting task's `min(want, free)` before a
    /// short holder's growth. `None` when no gate-class work wants a token.
    fn next_gate(&self) -> Option<Pick> {
        let free = self.free;
        let ((_, step, _), name, index, tokens) = self
            .checkouts
            .iter()
            .flat_map(|(name, checkout)| {
                let starts = checkout
                    .waiting
                    .iter()
                    .enumerate()
                    .filter(|(_, waiter)| waiter.class == JobserverClass::Gate)
                    .map(move |(position, waiter)| {
                        let key = (waiter.job, GateStep::Start, waiter.ticket);
                        (key, name, position, waiter.want.min(free))
                    });
                let grows = checkout
                    .running
                    .iter()
                    .enumerate()
                    .filter(|(_, holder)| holder.class == JobserverClass::Gate && holder.short())
                    .map(move |(index, holder)| {
                        let key = (holder.job, GateStep::Grow, holder.ticket);
                        (key, name, index, (holder.want - holder.tokens).min(free))
                    });
                starts.chain(grows)
            })
            .min_by_key(|(key, _, _, _)| *key)?;
        let background: usize = self
            .checkouts
            .values()
            .flat_map(|checkout| &checkout.running)
            .filter(|holder| holder.class == JobserverClass::Background)
            .map(|holder| holder.tokens)
            .sum();
        let starved = (2 * (background + 1) < self.total)
            .then(|| {
                self.checkouts
                    .iter()
                    .filter(|(_, checkout)| checkout.held == 0)
                    .filter_map(|(name, checkout)| {
                        let (position, waiter) = checkout
                            .waiting
                            .iter()
                            .enumerate()
                            .find(|(_, waiter)| waiter.class == JobserverClass::Background)?;
                        Some((name, position, waiter.ticket))
                    })
                    .min_by_key(|(_, _, ticket)| *ticket)
            })
            .flatten();
        if let Some((name, position, _)) = starved {
            return Some(Pick::Grant {
                checkout: Arc::clone(name),
                position,
                tokens: 1,
            });
        }
        let checkout = Arc::clone(name);
        Some(match step {
            GateStep::Start => Pick::Grant {
                checkout,
                position: index,
                tokens,
            },
            GateStep::Grow => Pick::Grow {
                checkout,
                index,
                tokens,
            },
        })
    }

    /// The next background grant by checkout fairness; with no background request waiting, the
    /// oldest short background holder's growth; `None` when nobody can be granted now.
    fn next_background(&self) -> Option<Pick> {
        let Some((name, checkout, head)) = self
            .checkouts
            .iter()
            .filter_map(|(name, checkout)| Some((name, checkout, checkout.waiting.front()?)))
            .min_by_key(|(_, checkout, head)| (checkout.held, head.ticket))
        else {
            return self.background_grower();
        };
        let share = (self.total / self.checkouts.len()).max(1);
        let least = head.want.min(share.saturating_sub(checkout.held).max(1));
        let peers_wait = self
            .checkouts
            .iter()
            .any(|(other, peer)| other != name && !peer.waiting.is_empty());
        if peers_wait && self.free < least {
            return None;
        }
        let tokens = if peers_wait {
            least
        } else {
            head.want.min(self.free)
        };
        Some(Pick::Grant {
            checkout: Arc::clone(name),
            position: 0,
            tokens,
        })
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
        let mut gate_queue: Vec<(&Waiter, &Arc<str>)> = self
            .checkouts
            .iter()
            .flat_map(|(name, checkout)| {
                checkout
                    .waiting
                    .iter()
                    .filter(|waiter| waiter.class == JobserverClass::Gate)
                    .map(move |waiter| (waiter, name))
            })
            .collect();
        gate_queue.sort_unstable_by_key(|(waiter, _)| waiter.order());
        CpuBudgetStatus {
            total: self.total,
            held: self.total - self.free,
            checkouts,
            gate_queue: gate_queue
                .into_iter()
                .map(|(waiter, name)| CpuGateWaiter {
                    checkout: name.to_string(),
                    job: waiter.job,
                    want: waiter.want,
                })
                .collect(),
        }
    }
}

enum Message {
    Ask {
        ticket: Ticket,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        class: JobserverClass,
        /// The job whose age ranks it.
        job: Ticket,
        granted: oneshot::Sender<usize>,
        /// Where a growable holder hears its new total; `None` for a fixed grant.
        growth: Option<mpsc::UnboundedSender<usize>>,
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

impl Stake {
    /// The age of the job this stake admits, for the tasks it relays as [`Rank::GateTask`].
    pub(crate) fn job(&self) -> JobAge {
        JobAge(self.ticket)
    }
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
        rank: Rank,
    ) -> (Stake, oneshot::Receiver<usize>) {
        self.ask_with(checkout, want, claim, rank, None)
    }

    /// [`ask`](Self::ask) for a runner that can use tokens handed to it after it starts: after
    /// the grant, the growth receiver yields each new total it holds, never past `want`.
    pub(crate) fn ask_growing(
        &self,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        rank: Rank,
    ) -> (
        Stake,
        oneshot::Receiver<usize>,
        mpsc::UnboundedReceiver<usize>,
    ) {
        let (growth, grown) = mpsc::unbounded_channel();
        let (stake, granted) = self.ask_with(checkout, want, claim, rank, Some(growth));
        (stake, granted, grown)
    }

    fn ask_with(
        &self,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        rank: Rank,
        growth: Option<mpsc::UnboundedSender<usize>>,
    ) -> (Stake, oneshot::Receiver<usize>) {
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        let (class, job) = match rank {
            Rank::Background => (JobserverClass::Background, ticket),
            Rank::Gate => (JobserverClass::Gate, ticket),
            Rank::GateTask(JobAge(job)) => (JobserverClass::Gate, job),
        };
        let (granted, receiver) = oneshot::channel();
        let _ = self.messages.send(Message::Ask {
            ticket,
            checkout,
            want,
            claim,
            class,
            job,
            granted,
            growth,
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
    let mut growers: HashMap<Ticket, mpsc::UnboundedSender<usize>> = HashMap::new();
    let mut effects = Vec::new();
    while let Some(message) = messages.recv().await {
        match message {
            Message::Ask {
                ticket,
                checkout,
                want,
                claim,
                class,
                job,
                granted,
                growth,
            } => {
                grants.insert(ticket, granted);
                let grows = growth.is_some();
                if let Some(growth) = growth {
                    growers.insert(ticket, growth);
                }
                ledger.ask(
                    checkout,
                    Waiter {
                        ticket,
                        want: want.get(),
                        grows,
                        asked: Instant::now(),
                        claim,
                        class,
                        job,
                    },
                    &mut effects,
                );
            }
            Message::Leave { ticket } => {
                grants.remove(&ticket);
                growers.remove(&ticket);
                ledger.leave(Instant::now(), ticket, &mut effects);
            }
            Message::Status { reply } => {
                let _ = reply.send(ledger.status());
            }
        }
        for effect in effects.drain(..) {
            match effect {
                Effect::Grant {
                    ticket,
                    tokens,
                    want,
                    waited,
                    checkout,
                    claim,
                } => {
                    // A client gone before its grant leaves next, and its tokens come back with it.
                    if let Some(granted) = grants.remove(&ticket) {
                        let _ = granted.send(tokens);
                    }
                    cowshed_os::timing::event("cpu-tokens", || {
                        format!(
                            "grant {tokens}/{want} after {waited:?} to {checkout}: pid {} `{}`",
                            claim
                                .pid
                                .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
                            claim.command,
                        )
                    });
                }
                Effect::Grow {
                    ticket,
                    from,
                    to,
                    want,
                    checkout,
                } => {
                    // A holder gone before it hears leaves next, and its tokens come back with it.
                    if let Some(growth) = growers.get(&ticket) {
                        let _ = growth.send(to);
                    }
                    cowshed_os::timing::event("cpu-tokens", || {
                        format!("grow {from}/{want} -> {to}/{want} in {checkout}")
                    });
                }
            }
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
            self.ask_as(
                ms,
                ticket,
                checkout,
                want,
                (JobserverClass::Background, ticket),
                false,
            )
        }

        /// A gate job's own ask: its ticket is the job's age.
        fn ask_land(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
            want: usize,
        ) -> Vec<(Ticket, usize)> {
            self.ask_as(
                ms,
                ticket,
                checkout,
                want,
                (JobserverClass::Gate, ticket),
                false,
            )
        }

        /// A land task's ask: what gate job `job`'s relay pool sends for one child.
        fn ask_task(
            &mut self,
            ms: u64,
            ticket: Ticket,
            job: Ticket,
            checkout: &str,
            want: usize,
        ) -> Vec<(Ticket, usize)> {
            self.ask_as(
                ms,
                ticket,
                checkout,
                want,
                (JobserverClass::Gate, job),
                false,
            )
        }

        /// A land task of gate job `job` that is a Cargo build: it can use tokens handed to it
        /// after it starts.
        fn ask_task_build(
            &mut self,
            ms: u64,
            ticket: Ticket,
            job: Ticket,
            checkout: &str,
            want: usize,
        ) -> Vec<(Ticket, usize)> {
            self.ask_as(
                ms,
                ticket,
                checkout,
                want,
                (JobserverClass::Gate, job),
                true,
            )
        }

        /// A background Cargo build that can grow.
        fn ask_build(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
            want: usize,
        ) -> Vec<(Ticket, usize)> {
            self.ask_as(
                ms,
                ticket,
                checkout,
                want,
                (JobserverClass::Background, ticket),
                true,
            )
        }

        fn ask_as(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
            want: usize,
            (class, job): (JobserverClass, Ticket),
            grows: bool,
        ) -> Vec<(Ticket, usize)> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.ask(
                Arc::from(checkout),
                Waiter {
                    ticket,
                    class,
                    job,
                    want: NonZeroUsize::new(want).unwrap().get(),
                    grows,
                    asked: now,
                    claim: Claim::new(&format!("t{ticket}"), Some(7)),
                },
                &mut self.effects,
            );
            self.grants()
        }

        fn leave(&mut self, ms: u64, ticket: Ticket) -> Vec<(Ticket, usize)> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.leave(now, ticket, &mut self.effects);
            self.grants()
        }

        /// Each step as `(ticket, tokens it now holds)`: a grant's count, or a growth's new total.
        fn grants(&mut self) -> Vec<(Ticket, usize)> {
            self.effects
                .drain(..)
                .map(|effect| match effect {
                    Effect::Grant { ticket, tokens, .. } => (ticket, tokens),
                    Effect::Grow {
                        ticket, to, want, ..
                    } => {
                        assert!(to <= want, "a holder never grows past its want");
                        (ticket, to)
                    }
                })
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
    fn a_lone_request_takes_what_it_wants_even_past_half_the_host() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 4), [(1, 4)]);
        assert_eq!(clock.leave(1, 1), []);
        assert_eq!(clock.ask(2, 2, "a", 64), [(2, 18)]);
        assert_eq!(clock.ledger.status().held, 18);
        assert_eq!(clock.leave(3, 2), []);
        assert_eq!(clock.ledger.status().held, 0);
        assert!(clock.ledger.status().checkouts.is_empty());
        assert!(clock.balanced());
    }

    #[test]
    fn a_newcomer_waits_for_tokens_beside_a_checkout_whose_runner_holds_the_whole_host() {
        // Tokens are never handed back by a running job, so a newcomer whose host is
        // fully held queues for the runner's departure; the foreground band
        // (05_gateway.md, "Dynamic QoS"), not a reserved half, is its remedy.
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "carry", 18), [(1, 18)]);
        assert_eq!(clock.ask(1, 2, "train", 18), []);
        assert_eq!(clock.leave(2, 1), [(2, 18)]);
        assert!(clock.balanced());
    }

    #[test]
    fn with_nobody_else_waiting_a_checkout_takes_free_tokens_up_to_its_want() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 5), [(1, 5)]);
        // Nobody else waits, so b takes every free token up to its want, not a share.
        assert_eq!(clock.ask(1, 2, "b", 18), [(2, 13)]);
        // The host is full now: a's second runner queues until b leaves.
        assert_eq!(clock.ask(2, 3, "a", 18), []);
        assert_eq!(clock.leave(3, 2), [(3, 13)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_lone_waiter_takes_idle_tokens_even_below_its_fair_share() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 12), [(1, 12)]);
        // b's fair share is 9, but no peer waits: the remaining 6 must not sit idle.
        assert_eq!(clock.ask(1, 2, "b", 18), [(2, 6)]);
        assert_eq!(clock.ledger.status().held, 18);
        assert_eq!(clock.leave(2, 2), []);
        assert_eq!(clock.leave(3, 1), []);
        assert!(clock.balanced());
    }

    #[test]
    fn a_full_host_hands_freed_tokens_to_the_checkout_holding_least() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 18), [(1, 18)]);
        assert_eq!(clock.ask(0, 2, "a", 18), []);
        // a's next runner queues first; b arrives later and wants the whole host.
        assert_eq!(clock.ask(1, 3, "a", 1), []);
        assert_eq!(clock.ask(2, 4, "b", 18), []);
        // Two checkouts, a share of 9 each. a's head waiter is older, so the freed
        // tokens go half to it and half to b, and a's small waiter waits for a's
        // next return.
        assert_eq!(clock.leave(3, 1), [(2, 9), (4, 9)]);
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

    #[test]
    fn land_work_takes_every_returned_token_while_background_stays_queued() {
        let mut clock = Clock::new(4);
        for ticket in 1..=4 {
            assert_eq!(clock.ask(0, ticket, "fleet", 1), [(ticket, 1)]);
        }
        assert_eq!(clock.ask(1, 5, "fleet", 1), []);
        assert_eq!(clock.ask(1, 6, "other", 1), []);
        // A land's tasks ask last, through its gate job's relay pool.
        assert_eq!(clock.ask_land(2, 7, "land", 1), []);
        assert_eq!(clock.ask_land(2, 8, "land", 1), []);
        // Each returned token goes to the land, ahead of the older background waiters (which
        // already hold most of the host), and a land task already running does not stop the next
        // one from being admitted.
        assert_eq!(clock.leave(3, 1), [(7, 1)]);
        assert_eq!(clock.leave(4, 2), [(8, 1)]);
        // With no land work waiting, ordinary fairness resumes.
        assert_eq!(clock.leave(5, 3), [(6, 1)]);
        assert_eq!(clock.leave(6, 4), [(5, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_land_task_queued_behind_background_in_its_own_checkout_goes_first() {
        let mut clock = Clock::new(2);
        assert_eq!(clock.ask(0, 1, "ws", 2), [(1, 2)]);
        assert_eq!(clock.ask(1, 2, "ws", 1), []);
        assert_eq!(clock.ask_land(2, 3, "ws", 1), []);
        // The land task is granted first; the token it leaves is background's again.
        assert_eq!(clock.leave(3, 1), [(3, 1), (2, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn an_older_land_is_served_before_a_younger_land_and_both_before_a_feature_build() {
        let mut clock = Clock::new(6);
        // Two land gate jobs start: a's is older than b's.
        assert_eq!(clock.ask_land(0, 1, "a", 1), [(1, 1)]);
        assert_eq!(clock.ask_land(1, 2, "b", 1), [(2, 1)]);
        // A feature shed's builds hold the rest of the host, and its next build waits first.
        assert_eq!(clock.ask(2, 3, "feature", 2), [(3, 2)]);
        assert_eq!(clock.ask(2, 4, "feature", 2), [(4, 2)]);
        assert_eq!(clock.ask(3, 5, "feature", 2), []);
        // Then the younger land's task asks, and the older land's task asks last of all.
        assert_eq!(clock.ask_task(4, 6, 2, "b", 2), []);
        assert_eq!(clock.ask_task(5, 7, 1, "a", 2), []);
        let queue: Vec<(String, u64)> = clock
            .ledger
            .status()
            .gate_queue
            .into_iter()
            .map(|waiter| (waiter.checkout, waiter.job))
            .collect();
        assert_eq!(queue, [("a".to_owned(), 1), ("b".to_owned(), 2)]);
        // Served in the reverse of the order they asked: the older land, the younger land, then
        // the feature build.
        assert_eq!(clock.leave(6, 3), [(7, 2)]);
        assert_eq!(clock.leave(7, 7), [(6, 2)]);
        assert_eq!(clock.leave(8, 6), [(5, 2)]);
        assert!(clock.balanced());
    }

    #[test]
    fn an_older_land_takes_every_token_its_tasks_want_before_a_younger_land_gets_one() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask_task(0, 1, 0, "a", 18), [(1, 18)]);
        assert_eq!(clock.ask_task(1, 2, 9, "b", 18), []);
        assert_eq!(clock.ask_task(1, 3, 0, "a", 18), []);
        assert_eq!(clock.ask(2, 4, "c", 18), []);
        // c holds nothing, so it keeps its one token; then a's second task, though it asked after
        // b's, takes the rest, because a's gate job is older.
        assert_eq!(clock.leave(3, 1), [(4, 1), (3, 17)]);
        assert_eq!(clock.leave(4, 3), [(2, 17)]);
        assert!(clock.balanced());
    }

    #[test]
    fn land_work_holds_the_larger_part_without_starving_background() {
        let mut clock = Clock::new(6);
        assert_eq!(clock.ask(0, 1, "x", 6), [(1, 6)]);
        assert_eq!(clock.ask_land(1, 2, "land", 6), []);
        assert_eq!(clock.ask_land(1, 3, "land", 6), []);
        assert_eq!(clock.ask(2, 4, "y", 1), []);
        assert_eq!(clock.ask(2, 5, "x", 1), []);
        // Each background checkout holding nothing gets one token while background stays under
        // half the host; the land's oldest task takes the remaining four.
        assert_eq!(clock.leave(3, 1), [(4, 1), (5, 1), (2, 4)]);
        assert_eq!(clock.leave(4, 2), [(3, 4)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_land_build_granted_one_token_under_load_grows_as_tokens_return() {
        // The measured case: another shed holds all 18 tokens, one per runner, and more of its
        // work waits. A land's Cargo build asks for 18 and the first return gives it 1.
        let mut clock = Clock::new(18);
        for ticket in 1..=18 {
            assert_eq!(clock.ask(0, ticket, "custody", 1), [(ticket, 1)]);
        }
        assert_eq!(clock.ask(1, 19, "custody", 1), []);
        assert_eq!(clock.ask_task_build(2, 20, 0, "land", 18), []);
        assert_eq!(clock.leave(3, 1), [(20, 1)]);
        // Every later return widens the running build, ahead of the queued background work.
        for (ms, ticket) in (4..).zip(2..=17) {
            let held = usize::try_from(ticket).unwrap();
            assert_eq!(clock.leave(ms, ticket), [(20, held)]);
        }
        // The custody shed now holds nothing while it waits: it keeps its one token, and the
        // build stays at 17 of 18.
        assert_eq!(clock.leave(30, 18), [(19, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_new_land_task_starts_before_a_running_one_of_the_same_land_grows() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask(0, 1, "x", 3), [(1, 3)]);
        assert_eq!(clock.ask_task_build(1, 2, 0, "land", 4), [(2, 1)]);
        assert_eq!(clock.ask_task(2, 3, 0, "land", 2), []);
        // Three tokens return: the waiting land task takes its two first, then the build grows.
        assert_eq!(clock.leave(3, 1), [(3, 2), (2, 2)]);
        assert!(clock.balanced());
    }

    #[test]
    fn an_older_land_build_grows_before_a_younger_land_task_starts() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask(0, 1, "x", 3), [(1, 3)]);
        assert_eq!(clock.ask_task_build(1, 2, 0, "a", 4), [(2, 1)]);
        assert_eq!(clock.ask_task(2, 3, 9, "b", 2), []);
        // Three tokens return: a's build takes them all to reach its want, since a's gate job is
        // older; b's task starts when a returns its tokens.
        assert_eq!(clock.leave(3, 1), [(2, 4)]);
        assert_eq!(clock.leave(4, 2), [(3, 2)]);
        assert!(clock.balanced());
    }

    #[test]
    fn growth_never_passes_the_original_want_and_a_test_runner_never_grows() {
        let mut clock = Clock::new(8);
        assert_eq!(clock.ask(0, 1, "x", 6), [(1, 6)]);
        assert_eq!(clock.ask_task_build(1, 2, 0, "land", 4), [(2, 2)]);
        assert_eq!(clock.ask(1, 3, "x", 1), []);
        // Six return: x, holding nothing, keeps its one token; the build grows by two to its
        // want of four, never further, and the last three stay free for whoever asks.
        assert_eq!(clock.leave(2, 1), [(3, 1), (2, 4)]);
        // nextest sizes its threads once, so a land test runner asks without growth: a token
        // returned while it runs at three of eight stays free rather than held idle by it.
        assert_eq!(clock.ask_task(3, 4, 0, "land", 8), [(4, 3)]);
        assert_eq!(clock.leave(4, 3), []);
        assert_eq!(clock.ledger.status().held, 7);
        assert!(clock.balanced());
    }

    #[test]
    fn a_background_build_grows_only_while_no_background_work_waits() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask(0, 1, "x", 3), [(1, 3)]);
        assert_eq!(clock.ask_build(1, 2, "y", 4), [(2, 1)]);
        assert_eq!(clock.ask(2, 3, "x", 1), []);
        // A background request waits, so the returned tokens are its, not the build's growth.
        assert_eq!(clock.leave(3, 1), [(3, 1), (2, 3)]);
        assert!(clock.balanced());
    }
}
