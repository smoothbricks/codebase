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
//! share of background's cap, so a checkout with sixty runners queued cannot crowd out one with
//! three. That order never starves a request: a checkout's head that the background limit of
//! later background grants has passed is overdue and served first, so a checkout whose grant
//! stays held still gets its turn. A grant is all at once and between one and the request's
//! `want`; a request never holds part of a grant while waiting for the rest, so no two requests
//! can deadlock on each other's tokens.
//!
//! Fairness only acts when a grant is made: a running nextest or cargo cannot hand tokens back,
//! and its FIFO, implicit make token and test-thread sizing are never resized. Admission is
//! work-conserving while no gate-class work holds or waits: background may be granted every
//! token, so none idles while background wants it. Gate-class work quiets the host: beside it each
//! background grant is one token, made only while the work behind the oldest gate job then holds
//! less than half the host ([`Ledger::floor_open`]), and a gate that arrives after background filled
//! the host takes back its share from the next returned tokens: no grant shrinks, and nothing is
//! killed or timed. A fork or attach of a repository whose gate works waits here for that gate to
//! end ([`Ledger::ask_quiet`]), so the quiet a land's gate needs is the gateway's state, not an
//! announcement. Class ranks first: while any gate-class request waits, returned tokens go to gate-class work ahead
//! of queued background. A land's gate runs as a gate job whose pool relays each of its tasks'
//! asks here at gate class (`jobserver.rs`), ranked by the gate job's own admission: gate work is
//! served oldest job first, and within one job in the order its tasks asked, so an older land
//! takes every returned token its tasks want before a younger land's tasks get more than the
//! one-token floor that keeps a younger land in another repository, like background, from starving.
//! Within a repository only one gate job holds tokens, including its relayed tasks.
//!
//! [`Ledger`] is the whole policy as a pure state machine; [`CpuBudget`] is the thin actor that
//! feeds it requests and departures and answers status.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cowshed_gateway_types::{
    CpuBackgroundStake, CpuBudgetStatus, CpuCheckoutStatus, CpuGateJob, CpuGateWaiter, CpuHolder,
    CpuProbation, CpuQuietWaiter, CpuWaitCause, CpuWaiter, InstallRestart, background_limit,
    cpu_budget::{CpuQueueEntry, CpuQueueWait, JobserverClass},
    host_tokens,
};
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
            tokens: host_tokens(),
        }
    }
}

/// The gate job's admission ticket: the smaller ticket is the older job.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct JobAge(Ticket);

/// The admitted class: every gate carries its validated repository identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Class {
    Background,
    Gate(Arc<str>),
}

impl Class {
    fn repo(&self) -> Option<&Arc<str>> {
        match self {
            Self::Background => None,
            Self::Gate(repo) => Some(repo),
        }
    }

    pub(crate) fn rank(&self) -> Rank {
        match self {
            Self::Background => Rank::Background,
            Self::Gate(repo) => Rank::Gate(Arc::clone(repo)),
        }
    }
}

/// Where a request stands in the order: its repository and the job whose age ranks it.
#[derive(Clone, Debug)]
pub(crate) enum Rank {
    Background,
    /// A gate job's own admission: its ticket is the job's age.
    Gate(Arc<str>),
    /// A relayed task keeps both the root gate's repository and its age.
    GateTask {
        repo: Arc<str>,
        job: JobAge,
    },
}

/// What a [`Ledger`] transition did, for the actor to carry out: the ticket may start its runner
/// now with `tokens` threads or processes, a whole grant it keeps until it leaves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Grant {
    ticket: Ticket,
    tokens: usize,
    want: usize,
    waited: Duration,
    checkout: Arc<str>,
    claim: Claim,
    class: Class,
    job: Ticket,
    head: Option<(Arc<str>, Ticket)>,
}

#[derive(Debug)]
struct Waiter {
    ticket: Ticket,
    class: Class,
    /// The job that ranks it: the gate job it serves, or its own ticket.
    job: Ticket,
    /// The count asked for, as asked.
    want: usize,
    asked: Instant,
    /// The background grants made to requests that asked after it while it headed its
    /// checkout's background queue; at the background limit it is overdue ([`Ledger::overdue`]).
    passed: usize,
    claim: Claim,
    /// Whether it may still be lent the probation token: its requester can stop the job it
    /// starts, and it was never lent it ([`Ledger::lend`]).
    lendable: bool,
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
    class: Class,
    job: Ticket,
    tokens: usize,
    want: usize,
    /// Who asked, as the status names the holder.
    claim: Claim,
    /// When it was granted: the lease's age starts here.
    granted: Instant,
}

/// A background head's place in checkout fairness, the smaller first: an overdue head, the oldest
/// first, before every head in fewest-held order (the older head on a tie).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Turn {
    Overdue(Ticket),
    Fair { held: usize, ticket: Ticket },
}

/// Whom a transition lent the probation token, for the actor to tell and to report.
#[derive(Debug)]
pub(crate) struct Lent {
    ticket: Ticket,
    checkout: Arc<str>,
    claim: Claim,
}

/// The ledger's next step: start the waiter at `position` in `checkout`'s queue with `tokens`.
struct Pick {
    checkout: Arc<str>,
    position: usize,
    tokens: usize,
}

/// One checkout's stake: what its runners hold, and who of it waits, oldest first.
#[derive(Debug, Default)]
struct Checkout {
    held: usize,
    running: Vec<Holder>,
    waiting: VecDeque<Waiter>,
}

/// Whose gate work a quiet ask waits for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum QuietScope {
    /// A fork's or an attach's: its repository's.
    Repository(Arc<str>),
    /// The released-image cleanup's, before each cut: any repository's.
    Host,
}

/// A fork or an attach that mounts, waiting for its repository's gate work to end (05_gateway.md
/// "A fork or attach waits for its repository's gate"). It holds no token.
#[derive(Debug)]
struct QuietWaiter {
    ticket: Ticket,
    scope: QuietScope,
    claim: Claim,
    asked: Instant,
}

/// The most tokens the work behind the oldest gate job (background, and younger gate jobs) may
/// hold on a host of `total` tokens: less than half the host, and at least one on a host of two
/// or more, so on the smallest hosts that work still runs on one (05_gateway.md "Room for a gate").
fn behind_limit(total: usize) -> usize {
    // A host of one token has none to spare beside a gate.
    (total.saturating_sub(1) / 2).max(usize::from(total >= 2))
}

/// What a holder's top-up ask hears (05_gateway.md "All at once"), with the ledger as it stood.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TopUp {
    pub(crate) outcome: TopUpOutcome,
    pub(crate) free: usize,
    pub(crate) total: usize,
    pub(crate) queued: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopUpOutcome {
    /// One more token is the holder's.
    Granted,
    /// The holder holds all it asked for, or holds nothing (it left, or still waits).
    Settled,
    /// No token is free.
    Full,
    /// Tokens are free, but a request waits: whatever keeps it waiting, the free tokens are its.
    Waiting,
}

impl TopUpOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::Settled => "settled",
            Self::Full => "full",
            Self::Waiting => "waiting",
        }
    }
}

/// What a request queued by an admission transition hears: the queue it waits in, and why.
#[derive(Debug)]
pub(crate) struct Queued {
    pub(crate) wait: CpuQueueWait,
    pub(crate) cause: CpuWaitCause,
}
/// The budget's state.
///
/// Invariants: `free` plus every checkout's `held` is `total`; a background grant made beside
/// gate-class work is one token, made while the floor is open ([`Ledger::floor_open`]) or no
/// gate-class request waits; a
/// checkout is present exactly while it holds or waits; every ticket in `tickets` is a holder or
/// a waiter of the checkout it names; all gate holders in one repository belong to one job,
/// including holders in different checkouts; every quiet waiter's repository has gate work.
#[derive(Debug)]
pub(crate) struct Ledger {
    total: usize,
    /// [`background_limit`] of `total`: the later background grants that make a waiting
    /// background head overdue.
    background: usize,
    free: usize,
    checkouts: HashMap<Arc<str>, Checkout>,
    tickets: HashMap<Ticket, Arc<str>>,
    /// The waiter the probation token is lent to, and since when: one token beyond `total`, never
    /// counted in `free` (05_gateway.md "A one-token job starts on probation").
    lent: Option<(Ticket, Instant)>,
    /// The forks and attaches waiting for their repository's gate work to end, oldest first.
    quiet: Vec<QuietWaiter>,
    /// Every CPU pool a gate job serves, its root's and each pool served under it, by endpoint: a
    /// quiet ask whose requester's pool is one of the pools of the gate job heading its repository
    /// works for that gate and does not wait for it. A background or resident job of the same
    /// workspace has pools in the same directory and none of these.
    gate_pools: HashMap<Arc<std::path::Path>, Ticket>,
}

impl Ledger {
    /// The host's ledger: background may take the whole host with no gate-class work, and one
    /// token a grant beside it ([`Ledger::floor_open`]).
    pub(crate) fn host(limits: CpuBudgetLimits) -> Self {
        let total = limits.tokens.get();
        Self::with(total, background_limit(total))
    }

    fn with(total: usize, background: usize) -> Self {
        Self {
            total,
            background,
            free: total,
            checkouts: HashMap::new(),
            tickets: HashMap::new(),
            lent: None,
            quiet: Vec::new(),
            gate_pools: HashMap::new(),
        }
    }

    /// Queue the exact admission record, retaining its original request instant.
    fn ask(&mut self, checkout: Arc<str>, waiter: Waiter, grants: &mut Vec<Grant>) {
        let (now, ticket) = (waiter.asked, waiter.ticket);
        self.checkouts
            .entry(Arc::clone(&checkout))
            .or_default()
            .waiting
            .push_back(waiter);
        self.tickets.insert(ticket, checkout);
        self.admit(now, grants);
    }

    /// `ticket` is done: a holder returns its tokens, a waiter leaves the queue. A ticket the
    /// ledger does not know changes nothing.
    pub(crate) fn leave(&mut self, now: Instant, ticket: Ticket, grants: &mut Vec<Grant>) {
        self.return_probation(ticket);
        let Some(name) = self.tickets.remove(&ticket) else {
            // A fork or attach that closed while it waited for its repository's gate.
            self.quiet.retain(|waiter| waiter.ticket != ticket);
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
        let jobs: std::collections::HashSet<Ticket> = self
            .checkouts
            .values()
            .flat_map(|checkout| {
                let held = checkout.running.iter().map(|holder| holder.job);
                held.chain(checkout.waiting.iter().map(|waiter| waiter.job))
            })
            .collect();
        self.gate_pools.retain(|_, job| jobs.contains(job));
        self.admit(now, grants);
    }

    /// Grant waiters while tokens are free: gate class oldest job first, background fairest.
    ///
    /// Among eligible repository heads, the oldest job's wants come first, wherever its requests
    /// sit in their checkouts' queues. Within one job a task takes `min(want, free)` in request
    /// order. A land takes every returned token its tasks want before a younger repository head
    /// gets more than its floor, and both before queued background. The work behind the job being
    /// served is not starved: a background checkout, or a younger head of another repository,
    /// that waits holding nothing, and an overdue background head, each still get one token
    /// first, as long as that work as a whole then still holds less than half the host, so the
    /// job being served and the older ones always hold the larger part.
    ///
    /// With no gate-class work, background is fair between checkouts over every free token; beside
    /// it each background grant is one token, in the same order, while the floor is open
    /// ([`Ledger::floor_open`]). The next grant goes to the waiting
    /// checkout that holds the fewest tokens (the older head waiter on a tie), to its oldest
    /// waiter, except that an overdue head goes first ([`Ledger::overdue`]). A checkout's fair
    /// share is the host divided among the checkouts holding or waiting; its claim
    /// is what remains of that share, at least one. While another checkout
    /// waits, the grant is exactly the claim or the `want`, whichever is smaller, so nobody
    /// outruns a waiting peer; with nobody else waiting it is the whole room up to `want`. A head
    /// waiter whose least grant is not in the room yet waits for it, and so does everyone behind
    /// it: skipping to a smaller request would starve a large one for good.
    fn admit(&mut self, now: Instant, grants: &mut Vec<Grant>) {
        while self.free > 0 {
            let Some(Pick {
                checkout: name,
                position,
                tokens,
            }) = self.next_gate().or_else(|| self.next_background())
            else {
                return;
            };
            let head = self.queue_head().map(|(repo, job)| (Arc::clone(repo), job));
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
                class: waiter.class.clone(),
                job: waiter.job,
                tokens,
                want: waiter.want,
                claim: waiter.claim.clone(),
                granted: now,
            });
            self.free -= tokens;
            self.return_probation(waiter.ticket);
            if waiter.class == Class::Background {
                self.pass_over(waiter.ticket);
            }
            grants.push(Grant {
                ticket: waiter.ticket,
                tokens,
                want: waiter.want,
                waited: now.saturating_duration_since(waiter.asked),
                checkout: name,
                claim: waiter.claim,
                class: waiter.class,
                job: waiter.job,
                head,
            });
        }
    }

    /// One more token for holder `ticket`, whose grant is below its want (05_gateway.md "All at
    /// once"): only a token no request waits for, so a top-up never passes a waiter of any rank.
    pub(crate) fn top_up(&mut self, ticket: Ticket) -> TopUp {
        let queued = self
            .checkouts
            .values()
            .map(|checkout| checkout.waiting.len())
            .sum();
        let outcome = self.grow(ticket, queued);
        TopUp {
            outcome,
            free: self.free,
            total: self.total,
            queued,
        }
    }

    fn grow(&mut self, ticket: Ticket, queued: usize) -> TopUpOutcome {
        let Some(name) = self.tickets.get(&ticket) else {
            return TopUpOutcome::Settled;
        };
        let checkout = &self.checkouts[name];
        let Some(holder) = checkout
            .running
            .iter()
            .find(|holder| holder.ticket == ticket)
        else {
            return TopUpOutcome::Settled;
        };
        if holder.tokens >= holder.want {
            return TopUpOutcome::Settled;
        }
        if self.free == 0 {
            return TopUpOutcome::Full;
        }
        if queued > 0 {
            return TopUpOutcome::Waiting;
        }
        let name = std::sync::Arc::clone(name);
        let checkout = self
            .checkouts
            .get_mut(&name)
            .expect("a known ticket's checkout is present");
        let holder = checkout
            .running
            .iter_mut()
            .find(|holder| holder.ticket == ticket)
            .expect("the holder was found above");
        holder.tokens += 1;
        checkout.held += 1;
        self.free -= 1;
        TopUpOutcome::Granted
    }

    /// Holder `ticket` gives back one topped-up token it holds idle: it goes to the waiters, as
    /// any returned token does. A holder never gives back below one token; a ticket the ledger
    /// does not hold changes nothing.
    pub(crate) fn give_back(&mut self, now: Instant, ticket: Ticket, grants: &mut Vec<Grant>) {
        let Some(name) = self.tickets.get(&ticket) else {
            return;
        };
        let checkout = self
            .checkouts
            .get_mut(name)
            .expect("a known ticket's checkout is present");
        let Some(holder) = checkout
            .running
            .iter_mut()
            .find(|holder| holder.ticket == ticket && holder.tokens > 1)
        else {
            return;
        };
        holder.tokens -= 1;
        checkout.held -= 1;
        self.free += 1;
        self.admit(now, grants);
    }

    /// Lend the probation token, while it is free, to the oldest waiter that may still take it
    /// (05_gateway.md "A one-token job starts on probation"). Called after a transition's
    /// grants, so that waiter is one no free token could serve. It holds no ledger token and keeps
    /// its place: its grant or its leaving returns the probation token, and so does
    /// [`Ledger::probation_over`]. A waiter is lent it at most once.
    pub(crate) fn lend(&mut self, now: Instant) -> Option<Lent> {
        if self.lent.is_some() {
            return None;
        }
        let (checkout, waiter) = self
            .checkouts
            .iter_mut()
            .flat_map(|(name, checkout)| {
                checkout
                    .waiting
                    .iter_mut()
                    .map(move |waiter| (name, waiter))
            })
            .filter(|(_, waiter)| waiter.lendable)
            .min_by_key(|(_, waiter)| waiter.ticket)?;
        waiter.lendable = false;
        self.lent = Some((waiter.ticket, now));
        Some(Lent {
            ticket: waiter.ticket,
            checkout: Arc::clone(checkout),
            claim: waiter.claim.clone(),
        })
    }

    /// `ticket`'s requester stopped its job on probation without a grant: the probation token
    /// returns, and the ask waits where it was.
    pub(crate) fn probation_over(&mut self, ticket: Ticket) {
        self.return_probation(ticket);
    }

    fn return_probation(&mut self, ticket: Ticket) {
        if self.lent.is_some_and(|(lent, _)| lent == ticket) {
            self.lent = None;
        }
    }

    /// A background grant to `granted` passes every checkout's background head that asked
    /// before it. Only a head counts: the requests behind it wait for it first, in order.
    fn pass_over(&mut self, granted: Ticket) {
        for head in self.checkouts.values_mut().filter_map(|checkout| {
            checkout
                .waiting
                .iter_mut()
                .find(|waiter| waiter.class == Class::Background)
        }) {
            if head.ticket < granted {
                head.passed += 1;
            }
        }
    }

    /// Whether the background limit of later background grants has passed `head`. Fewest-held
    /// order alone starves a checkout whose grant stays held while checkouts holding nothing keep
    /// asking, so an overdue head goes first in checkout fairness and takes the floor token:
    /// no head is passed by more than the background limit of later background grants.
    fn overdue(&self, head: &Waiter) -> bool {
        head.passed >= self.background
    }

    /// `head`'s place in `checkout`'s turn for background tokens.
    fn turn(&self, checkout: &Checkout, head: &Waiter) -> Turn {
        if self.overdue(head) {
            Turn::Overdue(head.ticket)
        } else {
            Turn::Fair {
                held: checkout.held,
                ticket: head.ticket,
            }
        }
    }

    /// A running job owns its repository until its last holder leaves. Otherwise the oldest
    /// queued job is the head. A ticket minted earlier but enqueued late cannot preempt a holder.
    fn gate_head(&self, repo: &str) -> Option<Ticket> {
        self.checkouts
            .values()
            .flat_map(|checkout| &checkout.running)
            .filter(|holder| {
                holder
                    .class
                    .repo()
                    .is_some_and(|name| name.as_ref() == repo)
            })
            .map(|holder| holder.job)
            .min()
            .or_else(|| {
                self.checkouts
                    .values()
                    .flat_map(|checkout| &checkout.waiting)
                    .filter(|waiter| {
                        waiter
                            .class
                            .repo()
                            .is_some_and(|name| name.as_ref() == repo)
                    })
                    .map(|waiter| waiter.job)
                    .min()
            })
    }

    fn gate_eligible(&self, waiter: &Waiter) -> bool {
        waiter
            .class
            .repo()
            .is_some_and(|repo| self.gate_head(repo) == Some(waiter.job))
    }

    /// Gate job `job` as a queue entry: its root request, whose ticket is the job's, or once the
    /// root has left while relayed tasks still hold, its oldest task.
    fn gate_job(&self, job: Ticket) -> CpuQueueEntry {
        self.checkouts
            .iter()
            .flat_map(|(name, checkout)| {
                let holders = checkout
                    .running
                    .iter()
                    .map(|holder| (holder.ticket, holder.job, &holder.class, &holder.claim));
                let waiters = checkout
                    .waiting
                    .iter()
                    .map(|waiter| (waiter.ticket, waiter.job, &waiter.class, &waiter.claim));
                holders
                    .chain(waiters)
                    .map(move |(ticket, owner, class, claim)| (ticket, owner, name, class, claim))
            })
            .filter(|(_, owner, ..)| *owner == job)
            .min_by_key(|(ticket, ..)| *ticket)
            .map(|(_, _, name, class, claim)| queue_entry(name, class, job, claim))
            .expect("a gate job the ledger heads has a holder or a waiter")
    }

    /// A CPU pool gate job `job` serves, at `endpoint`, while the job holds or waits.
    pub(crate) fn gate_pool(&mut self, job: Ticket, endpoint: Arc<std::path::Path>) {
        let serves = self.checkouts.values().any(|checkout| {
            checkout.running.iter().any(|holder| holder.job == job)
                || checkout.waiting.iter().any(|waiter| waiter.job == job)
        });
        if serves {
            self.gate_pools.insert(endpoint, job);
        }
    }

    /// A quiet ask of `scope`: `None` when it may run now, otherwise the gate job it waits for;
    /// it holds no token. A fork or attach waits while gate-class work of its repository holds or
    /// waits, unless its requester's `pool` is one of the pools the gate job heading it serves:
    /// that fork works for the gate itself, and waiting would wait for itself (05_gateway.md "A
    /// fork or attach waits for its repository's gate"). The released-image cleanup waits while
    /// gate-class work of any repository holds or waits (05_gateway.md "Deleting released images").
    pub(crate) fn ask_quiet(
        &mut self,
        ticket: Ticket,
        scope: QuietScope,
        pool: Option<&std::path::Path>,
        claim: Claim,
        asked: Instant,
    ) -> Option<CpuQueueEntry> {
        let head = self.quiet_head(&scope)?;
        if pool.is_some_and(|pool| self.gate_pools.get(pool) == Some(&head)) {
            return None;
        }
        let gate = self.gate_job(head);
        self.quiet.push(QuietWaiter {
            ticket,
            scope,
            claim,
            asked,
        });
        Some(gate)
    }

    /// The quiet waiters whose gate work has ended leave the queue and may run.
    pub(crate) fn release_quiet(&mut self, released: &mut Vec<Ticket>) {
        let (waiting, done): (Vec<QuietWaiter>, Vec<QuietWaiter>) = std::mem::take(&mut self.quiet)
            .into_iter()
            .partition(|waiter| self.quiet_head(&waiter.scope).is_some());
        self.quiet = waiting;
        released.extend(done.into_iter().map(|waiter| waiter.ticket));
    }

    /// The gate job a quiet ask of `scope` waits for: its repository's head, or for the host the
    /// oldest gate job of any repository; `None` once no such gate-class request holds or waits.
    fn quiet_head(&self, scope: &QuietScope) -> Option<Ticket> {
        match scope {
            QuietScope::Repository(repo) => self.gate_head(repo),
            QuietScope::Host => self.queue_head().map(|(_, job)| job),
        }
    }

    /// The tokens held by the work behind gate job `head`: background, and younger gate jobs.
    fn behind(&self, head: Ticket) -> usize {
        self.checkouts
            .values()
            .flat_map(|checkout| &checkout.running)
            .filter(|holder| holder.class == Class::Background || holder.job > head)
            .map(|holder| holder.tokens)
            .sum()
    }

    /// Whether the floor is open beside gate job `head`: one more token to the work behind it
    /// leaves that work within [`behind_limit`], under half the host and at least one token, so
    /// `head` and the older jobs keep the larger part and the work behind them is never starved. Beside gate-class work every background grant is this floor's one token
    /// (05_gateway.md "Room for a gate").
    fn floor_open(&self, head: Ticket) -> bool {
        self.behind(head) < behind_limit(self.total)
    }
    /// Whether a gate-class request waits, eligible or not: only then does the floor hold
    /// background back (05_gateway.md "Room for a gate").
    fn gate_waits(&self) -> bool {
        self.checkouts
            .values()
            .flat_map(|checkout| &checkout.waiting)
            .any(|waiter| waiter.class != Class::Background)
    }

    /// The global age head for diagnostics, including a holder with no task currently queued.
    fn queue_head(&self) -> Option<(&Arc<str>, Ticket)> {
        self.checkouts
            .values()
            .flat_map(|checkout| {
                checkout
                    .running
                    .iter()
                    .filter_map(|holder| holder.class.repo().map(|repo| (repo, holder.job)))
                    .chain(
                        checkout.waiting.iter().filter_map(|waiter| {
                            waiter.class.repo().map(|repo| (repo, waiter.job))
                        }),
                    )
            })
            .min_by_key(|(_, job)| *job)
    }

    /// The next grant while eligible gate-class work waits: a starved claimant's one token, else
    /// the oldest repository head's next task. A gate behind its own repository head gets no floor.
    fn next_gate(&self) -> Option<Pick> {
        let (name, position, waiter) = self
            .checkouts
            .iter()
            .flat_map(|(name, checkout)| {
                checkout
                    .waiting
                    .iter()
                    .enumerate()
                    .filter(|(_, waiter)| self.gate_eligible(waiter))
                    .map(move |(position, waiter)| (name, position, waiter))
            })
            .min_by_key(|(_, _, waiter)| waiter.order())?;
        let head = waiter.job;
        let job_holds = &|job: Ticket| {
            self.checkouts
                .values()
                .flat_map(|checkout| &checkout.running)
                .any(|holder| holder.class.repo().is_some() && holder.job == job)
        };
        let starved = self
            .floor_open(head)
            .then(|| {
                self.checkouts
                    .iter()
                    .flat_map(|(name, checkout)| {
                        let background = checkout
                            .waiting
                            .iter()
                            .enumerate()
                            .find(|(_, waiter)| waiter.class == Class::Background)
                            .filter(|(_, waiter)| checkout.held == 0 || self.overdue(waiter));
                        let younger_gates =
                            checkout
                                .waiting
                                .iter()
                                .enumerate()
                                .filter(move |(_, waiter)| {
                                    self.gate_eligible(waiter)
                                        && waiter.job > head
                                        && !job_holds(waiter.job)
                                });
                        background
                            .into_iter()
                            .chain(younger_gates)
                            .map(move |(position, waiter)| (name, position, waiter.ticket))
                    })
                    .min_by_key(|(_, _, ticket)| *ticket)
            })
            .flatten();
        if let Some((name, position, _)) = starved {
            return Some(Pick {
                checkout: Arc::clone(name),
                position,
                tokens: 1,
            });
        }
        Some(Pick {
            checkout: Arc::clone(name),
            position,
            tokens: waiter.want.min(self.free),
        })
    }

    /// The next background grant by checkout fairness; `None` when nobody can be granted now. With
    /// no gate-class work it may take every free token. Beside gate-class work the host is quiet:
    /// the same order picks and the grant is one token; while any gate-class request waits, an
    /// ineligible one too (a younger job of the head's repository), only while the floor is open
    /// beside the oldest gate job (05_gateway.md "Room for a gate"). Admission asks
    /// [`Ledger::next_gate`] first, which serves every eligible gate ask.
    fn next_background(&self) -> Option<Pick> {
        let quiet = self.queue_head().map(|(_, job)| job);
        if quiet.is_some_and(|head| self.gate_waits() && !self.floor_open(head)) {
            return None;
        }
        let (name, checkout, position, head) = self
            .checkouts
            .iter()
            .filter_map(|(name, checkout)| {
                let (position, head) = checkout
                    .waiting
                    .iter()
                    .enumerate()
                    .find(|(_, waiter)| waiter.class == Class::Background)?;
                Some((name, checkout, position, head))
            })
            .min_by_key(|(_, checkout, _, head)| self.turn(checkout, head))?;
        if quiet.is_some() {
            return Some(Pick {
                checkout: Arc::clone(name),
                position,
                tokens: 1,
            });
        }
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
        Some(Pick {
            checkout: Arc::clone(name),
            position,
            tokens,
        })
    }

    /// The wait left by this admission transition, and why; none when it granted immediately.
    fn queued(&self, ticket: Ticket) -> Option<Queued> {
        let checkout = self.tickets.get(&ticket)?;
        let request = self
            .checkouts
            .get(checkout)?
            .waiting
            .iter()
            .find(|waiter| waiter.ticket == ticket)?;
        let mut holders = Vec::new();
        let mut ahead = Vec::new();
        for (name, stake) in &self.checkouts {
            for holder in &stake.running {
                holders.push((
                    holder.job,
                    holder.ticket,
                    queue_entry(name, &holder.class, holder.job, &holder.claim),
                ));
            }
            for waiter in &stake.waiting {
                let same_queue = match &request.class {
                    Class::Gate(repo) => waiter.class.repo() == Some(repo),
                    Class::Background => name == checkout && waiter.class == Class::Background,
                };
                if same_queue && waiter.order() < request.order() {
                    ahead.push((
                        waiter.order(),
                        queue_entry(name, &waiter.class, waiter.job, &waiter.claim),
                    ));
                }
            }
        }
        holders.sort_unstable_by_key(|(job, ticket, _)| (*job, *ticket));
        ahead.sort_unstable_by_key(|(order, _)| *order);
        let cause = match &request.class {
            Class::Gate(repo) => match self.gate_head(repo) {
                Some(head) if head != request.job => CpuWaitCause::RepositoryGate {
                    gate: self.gate_job(head),
                },
                _ => CpuWaitCause::Tokens,
            },
            Class::Background => match self.queue_head() {
                Some((_, head)) => CpuWaitCause::Quiet {
                    gate: self.gate_job(head),
                },
                None => CpuWaitCause::Tokens,
            },
        };
        Some(Queued {
            wait: CpuQueueWait {
                request: queue_entry(checkout, &request.class, request.job, &request.claim),
                position: ahead.len() + 1,
                holders: holders.into_iter().map(|(_, _, entry)| entry).collect(),
                ahead: ahead.into_iter().map(|(_, entry)| entry).collect(),
            },
            cause,
        })
    }

    /// The ledger at `now`, every granted lease named with its age at that instant.
    pub(crate) fn status(&self, now: Instant) -> CpuBudgetStatus {
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
                    .filter(|waiter| waiter.class.repo().is_some())
                    .map(move |waiter| (waiter, name))
            })
            .collect();
        gate_queue.sort_unstable_by_key(|(waiter, _)| waiter.order());
        // Gate-class holders, grouped per gate job and checkout: the oldest job is the head.
        let mut gate_jobs: BTreeMap<(Ticket, &Arc<str>), Vec<&Holder>> = BTreeMap::new();
        let mut background = Vec::new();
        for (name, checkout) in &self.checkouts {
            let mut stake = Vec::new();
            for holder in &checkout.running {
                match &holder.class {
                    Class::Gate(_) => gate_jobs
                        .entry((holder.job, name))
                        .or_default()
                        .push(holder),
                    Class::Background => stake.push(holder),
                }
            }
            let waiters: Vec<CpuWaiter> = checkout
                .waiting
                .iter()
                .filter(|waiter| waiter.class == Class::Background)
                .map(|waiter| CpuWaiter {
                    pid: waiter.claim.pid,
                    command: waiter.claim.command.to_string(),
                    want: waiter.want,
                    waited_ms: millis_since(waiter.asked, now),
                })
                .collect();
            if !stake.is_empty() || !waiters.is_empty() {
                let (held, holders) = leases(stake, now);
                background.push(CpuBackgroundStake {
                    checkout: name.to_string(),
                    held,
                    holders,
                    waiters,
                });
            }
        }
        background.sort_unstable_by(|left, right| left.checkout.cmp(&right.checkout));
        CpuBudgetStatus {
            total: self.total,
            held: self.total - self.free,
            checkouts,
            gate_jobs: gate_jobs
                .into_iter()
                .map(|((job, name), holders)| {
                    let repository = holders[0].class.repo().expect("gate holder").to_string();
                    let (held, holders) = leases(holders, now);
                    CpuGateJob {
                        repository,
                        checkout: name.to_string(),
                        job,
                        held,
                        holders,
                    }
                })
                .collect(),
            gate_queue: gate_queue
                .into_iter()
                .map(|(waiter, name)| CpuGateWaiter {
                    repository: waiter.class.repo().expect("gate waiter").to_string(),
                    checkout: name.to_string(),
                    job: waiter.job,
                    pid: waiter.claim.pid,
                    command: waiter.claim.command.to_string(),
                    want: waiter.want,
                    waited_ms: millis_since(waiter.asked, now),
                })
                .collect(),
            background,
            probation: self.lent.map(|(ticket, since)| {
                let name = self
                    .tickets
                    .get(&ticket)
                    .expect("the probation token is lent to a known ticket");
                let waiter = self.checkouts[name]
                    .waiting
                    .iter()
                    .find(|waiter| waiter.ticket == ticket)
                    .expect("the probation token is lent to a waiter");
                CpuProbation {
                    checkout: name.to_string(),
                    pid: waiter.claim.pid,
                    command: waiter.claim.command.to_string(),
                    lent_ms: millis_since(since, now),
                }
            }),
            quiet: self
                .quiet
                .iter()
                .map(|waiter| CpuQuietWaiter {
                    repository: match &waiter.scope {
                        QuietScope::Repository(repo) => Some(repo.to_string()),
                        QuietScope::Host => None,
                    },
                    pid: waiter.claim.pid,
                    command: waiter.claim.command.to_string(),
                    waited_ms: millis_since(waiter.asked, now),
                })
                .collect(),
        }
    }
}

/// A request as a queue snapshot names it: its repository and class, checkout, job and command.
fn queue_entry(name: &str, class: &Class, job: Ticket, claim: &Claim) -> CpuQueueEntry {
    CpuQueueEntry {
        repository: class.repo().map(|repo| repo.to_string()),
        class: match class {
            Class::Gate(_) => JobserverClass::Gate,
            Class::Background => JobserverClass::Background,
        },
        checkout: name.to_owned(),
        job,
        command: claim.command.to_string(),
    }
}
/// One group's granted leases as the status names them, the oldest grant first, with the tokens
/// they hold together. The age is the lease's, from its grant: the ledger knows no other.
fn leases(mut holders: Vec<&Holder>, now: Instant) -> (usize, Vec<CpuHolder>) {
    holders.sort_unstable_by_key(|holder| (holder.granted, holder.ticket));
    let held = holders.iter().map(|holder| holder.tokens).sum();
    let holders = holders
        .into_iter()
        .map(|holder| CpuHolder {
            pid: holder.claim.pid,
            command: holder.claim.command.to_string(),
            tokens: holder.tokens,
            want: holder.want,
            held_ms: millis_since(holder.granted, now),
        })
        .collect();
    (held, holders)
}

/// Milliseconds from `since` to `now`, as the status reports an age.
fn millis_since(since: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(since).as_millis()).unwrap_or(u64::MAX)
}

enum Message {
    Ask {
        ticket: Ticket,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        class: Class,
        /// The job whose age ranks it.
        job: Ticket,
        granted: oneshot::Sender<usize>,
        queued: Option<oneshot::Sender<Option<Queued>>>,
        /// Told when the ask is lent the probation token; `None` for an ask that may not be.
        lent: Option<oneshot::Sender<()>>,
    },
    /// A fork or an attach that mounts asks whether its repository's gate works.
    Quiet {
        ticket: Ticket,
        scope: QuietScope,
        pool: Option<std::path::PathBuf>,
        claim: Claim,
        /// Told the gate job it waits for, or `None` when it may run at once.
        gate: oneshot::Sender<Option<CpuQueueEntry>>,
        granted: oneshot::Sender<()>,
    },
    Leave {
        ticket: Ticket,
        released: Instant,
    },
    /// A holder below its want asks for one more token ([`Ledger::top_up`]).
    TopUp {
        ticket: Ticket,
        told: oneshot::Sender<TopUp>,
    },
    /// A holder gives back one topped-up token it holds idle ([`Ledger::give_back`]).
    GiveBack {
        ticket: Ticket,
    },
    /// A CPU pool a gate job serves.
    GatePool {
        job: Ticket,
        endpoint: Arc<std::path::Path>,
    },
    /// The requester stopped its job on probation without a grant.
    ProbationOver {
        ticket: Ticket,
    },
    Status {
        reply: oneshot::Sender<CpuBudgetStatus>,
    },
    /// An install is restarting the gateway: every queued ask is refused, and so is every ask
    /// after; `told` resolves to how many were queued once each has left.
    CutOff {
        told: oneshot::Sender<usize>,
    },
}

/// The gateway's CPU budget: a handle to the actor that owns the [`Ledger`].
#[derive(Clone, Debug)]
pub(crate) struct CpuBudget {
    messages: mpsc::UnboundedSender<Message>,
    tickets: Arc<AtomicU64>,
    /// The install restarting the gateway, once [`CpuBudget::cut_off`] named it.
    installing: Arc<std::sync::OnceLock<InstallRestart>>,
}

/// A jobserver ask's stake and what it hears: its grant, the decision of its admission
/// transition, and, for an ask that may be lent it, the probation token (05_gateway.md "A
/// one-token job starts on probation").
pub(crate) struct JobserverAsk {
    pub(crate) stake: Stake,
    pub(crate) granted: oneshot::Receiver<usize>,
    pub(crate) queued: oneshot::Receiver<Option<Queued>>,
    pub(crate) lent: Option<oneshot::Receiver<()>>,
}

/// A quiet ask's stake and what it hears: the gate job it waits for, `None` when its repository
/// is quiet already, and its grant (05_gateway.md "A fork or attach waits for its repository's
/// gate"). Dropping the stake leaves the queue.
pub(crate) struct QuietAsk {
    pub(crate) stake: Stake,
    pub(crate) gate: oneshot::Receiver<Option<CpuQueueEntry>>,
    pub(crate) granted: oneshot::Receiver<()>,
}

/// One request's stake in the budget, waiting or holding. Dropping it returns what it holds.
#[derive(Debug)]
pub(crate) struct Stake {
    ticket: Ticket,
    messages: mpsc::UnboundedSender<Message>,
}

impl Stake {
    /// The handle a lent pool tops this stake's grant up through, and gives tokens back.
    pub(crate) fn grower(&self) -> Grower {
        Grower {
            ticket: self.ticket,
            messages: self.messages.clone(),
        }
    }

    /// The age of the job this stake admits, for the tasks it relays as [`Rank::GateTask`].
    pub(crate) fn job(&self) -> JobAge {
        JobAge(self.ticket)
    }

    /// This ask's requester stopped its job on probation without a grant: the probation token
    /// passes on and the ask keeps its place (05_gateway.md "A one-token job starts on
    /// probation").
    pub(crate) fn probation_over(&self) {
        // An actor that is gone lends nothing.
        let _ = self.messages.send(Message::ProbationOver {
            ticket: self.ticket,
        });
    }
}

/// A granted stake's top-up handle (05_gateway.md "All at once"). It owns nothing: the stake's
/// leaving returns every token it holds, topped up or not.
#[derive(Clone, Debug)]
pub(crate) struct Grower {
    ticket: Ticket,
    messages: mpsc::UnboundedSender<Message>,
}

impl Grower {
    /// Ask for one more token. The receiver errors when the budget is gone.
    pub(crate) fn top_up(&self) -> oneshot::Receiver<TopUp> {
        let (told, hearing) = oneshot::channel();
        // A budget that is gone tops nothing up: the receiver says so.
        let _ = self.messages.send(Message::TopUp {
            ticket: self.ticket,
            told,
        });
        hearing
    }

    /// Give back one topped-up token held idle.
    pub(crate) fn give_back(&self) {
        // A budget that is gone holds no token to return.
        let _ = self.messages.send(Message::GiveBack {
            ticket: self.ticket,
        });
    }
}

impl Drop for Stake {
    fn drop(&mut self) {
        // An actor that is gone holds no tokens to return.
        let _ = self.messages.send(Message::Leave {
            ticket: self.ticket,
            released: Instant::now(),
        });
    }
}

impl CpuBudget {
    /// Start the host's budget. It runs until the last handle and stake are gone.
    pub(crate) fn start(limits: CpuBudgetLimits) -> Self {
        Self::serve(Ledger::host(limits))
    }

    fn serve(ledger: Ledger) -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(ledger, receiver));
        Self {
            messages,
            tickets: Arc::new(AtomicU64::new(0)),
            installing: Arc::new(std::sync::OnceLock::new()),
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
        self.enqueue(checkout, want, claim, rank, None, None)
    }

    /// Jobserver admission also observes the decision made by the very same transition. A
    /// `probation` ask, one a supervisor can stop, also hears when it is lent the probation token.
    pub(crate) fn ask_jobserver(
        &self,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        rank: Rank,
        probation: bool,
    ) -> JobserverAsk {
        let (queued, decision) = oneshot::channel();
        let (lent, lending) = if probation {
            let (lent, lending) = oneshot::channel();
            (Some(lent), Some(lending))
        } else {
            (None, None)
        };
        let (stake, granted) = self.enqueue(checkout, want, claim, rank, Some(queued), lent);
        JobserverAsk {
            stake,
            granted,
            queued: decision,
            lent: lending,
        }
    }

    fn enqueue(
        &self,
        checkout: Arc<str>,
        want: NonZeroUsize,
        claim: Claim,
        rank: Rank,
        queued: Option<oneshot::Sender<Option<Queued>>>,
        lent: Option<oneshot::Sender<()>>,
    ) -> (Stake, oneshot::Receiver<usize>) {
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        let (class, job) = match rank {
            Rank::Background => (Class::Background, ticket),
            Rank::Gate(repo) => (Class::Gate(repo), ticket),
            Rank::GateTask {
                repo,
                job: JobAge(job),
            } => (Class::Gate(repo), job),
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
            queued,
            lent,
        });
        (
            Stake {
                ticket,
                messages: self.messages.clone(),
            },
            receiver,
        )
    }

    /// A fork or an attach that mounts asks to run while no gate-class request of `repo` holds or
    /// waits (05_gateway.md "A fork or attach waits for its repository's gate").
    pub(crate) fn ask_quiet(
        &self,
        scope: QuietScope,
        pool: Option<std::path::PathBuf>,
        claim: Claim,
    ) -> QuietAsk {
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        let (gate, told) = oneshot::channel();
        let (granted, receiver) = oneshot::channel();
        let _ = self.messages.send(Message::Quiet {
            ticket,
            scope,
            pool,
            claim,
            gate,
            granted,
        });
        QuietAsk {
            stake: Stake {
                ticket,
                messages: self.messages.clone(),
            },
            gate: told,
            granted: receiver,
        }
    }

    /// Record a CPU pool gate job `job` serves, so a fork or attach its own work asks for does not
    /// wait for it.
    pub(crate) fn gate_pool(&self, job: JobAge, endpoint: Arc<std::path::Path>) {
        // An actor that is gone records nothing.
        let _ = self.messages.send(Message::GatePool {
            job: job.0,
            endpoint,
        });
    }

    /// The ledger as it stands; `None` when the budget is gone.
    pub(crate) async fn status(&self) -> Option<CpuBudgetStatus> {
        let (reply, receiver) = oneshot::channel();
        self.messages.send(Message::Status { reply }).ok()?;
        receiver.await.ok()
    }

    /// Cut off every queued ask, and every later one, because `install` is restarting the
    /// gateway (05_gateway.md "A queued CPU ask outlives an install"). Each waiter's grant
    /// receiver errors, and [`CpuBudget::installing`] names `install` to it, so it refuses its
    /// client with that install before the restart closes the connection. Resolves to how many
    /// asks were queued, once every one of them has left the queue; holders are untouched.
    pub(crate) async fn cut_off(&self, install: InstallRestart) -> usize {
        let _ = self.installing.set(install);
        let (told, receiver) = oneshot::channel();
        if self.messages.send(Message::CutOff { told }).is_err() {
            return 0;
        }
        receiver.await.unwrap_or(0)
    }

    /// The install restarting the gateway, once the queue was cut off for it.
    pub(crate) fn installing(&self) -> Option<&InstallRestart> {
        self.installing.get()
    }
}

/// What a cut-off ask is told, beside `install` itself: one sentence for every socket.
pub(crate) fn cut_off_by(install: &InstallRestart) -> String {
    format!(
        "install {} ({}) is restarting the gateway; ask the gateway it starts once, within {} ms",
        install.pid,
        install.binary.display(),
        install.serves_within_ms
    )
}

/// The asks a cut-off refused and that have not left the queue yet, and who waits for them all.
struct CutOff {
    waiting: std::collections::HashSet<Ticket>,
    queued: usize,
    told: Option<oneshot::Sender<usize>>,
}

impl CutOff {
    fn left(&mut self, ticket: Ticket) {
        self.waiting.remove(&ticket);
        if self.waiting.is_empty()
            && let Some(told) = self.told.take()
        {
            let _ = told.send(self.queued);
        }
    }
}

async fn run(mut ledger: Ledger, mut messages: mpsc::UnboundedReceiver<Message>) {
    let mut grants: HashMap<Ticket, oneshot::Sender<usize>> = HashMap::new();
    // The asks that may be lent the probation token, told when they are.
    let mut lends: HashMap<Ticket, oneshot::Sender<()>> = HashMap::new();
    // The forks and attaches waiting for their repository's gate work to end, told when it does.
    let mut quiets: HashMap<Ticket, (QuietScope, oneshot::Sender<()>)> = HashMap::new();
    let mut released = Vec::new();
    let mut cut: Option<CutOff> = None;
    let mut effects = Vec::new();
    while let Some(message) = messages.recv().await {
        let started = Instant::now();
        let transition = match &message {
            Message::Ask { ticket, .. } => Some(("ask", *ticket, None)),
            Message::Quiet { ticket, .. } => Some(("quiet", *ticket, None)),
            Message::GatePool { .. } => None,
            Message::Leave { ticket, released } => Some(("release", *ticket, Some(*released))),
            Message::TopUp { .. } => None,
            Message::GiveBack { ticket } => Some(("give-back", *ticket, None)),
            Message::ProbationOver { ticket } => Some(("probation-over", *ticket, None)),
            Message::Status { .. } | Message::CutOff { .. } => None,
        };
        match message {
            Message::Ask {
                ticket,
                checkout,
                want,
                claim,
                class,
                job,
                granted,
                queued,
                lent,
            } => {
                // After a cut-off no ask is queued: dropping its sender refuses it at once.
                if let Some(cut) = &mut cut {
                    if cut.told.is_some() {
                        cut.waiting.insert(ticket);
                    }
                    drop(granted);
                    continue;
                }
                grants.insert(ticket, granted);
                let lendable = lent.is_some();
                if let Some(lent) = lent {
                    lends.insert(ticket, lent);
                }
                ledger.ask(
                    checkout,
                    Waiter {
                        ticket,
                        want: want.get(),
                        asked: Instant::now(),
                        passed: 0,
                        claim,
                        lendable,
                        class,
                        job,
                    },
                    &mut effects,
                );
                if let Some(queued) = queued {
                    let _ = queued.send(ledger.queued(ticket));
                }
            }
            Message::GatePool { job, endpoint } => ledger.gate_pool(job, endpoint),
            Message::Quiet {
                ticket,
                scope,
                pool,
                claim,
                gate,
                granted,
            } => {
                // After a cut-off the restart forgets every gate job: a fork or attach has nothing
                // left to wait for, and the cleanup stops (its senders dropped) rather than cut
                // through the restart.
                if cut.is_some() {
                    if scope != QuietScope::Host {
                        let _ = gate.send(None);
                        let _ = granted.send(());
                    }
                    continue;
                }
                let scope_of_waiter = scope.clone();
                match ledger.ask_quiet(ticket, scope, pool.as_deref(), claim, Instant::now()) {
                    None => {
                        let _ = gate.send(None);
                        let _ = granted.send(());
                    }
                    Some(holder) => {
                        quiets.insert(ticket, (scope_of_waiter, granted));
                        let _ = gate.send(Some(holder));
                    }
                }
            }
            Message::Leave { ticket, .. } => {
                grants.remove(&ticket);
                lends.remove(&ticket);
                quiets.remove(&ticket);
                ledger.leave(Instant::now(), ticket, &mut effects);
                ledger.release_quiet(&mut released);
                if let Some(cut) = &mut cut {
                    cut.left(ticket);
                }
            }
            Message::TopUp { ticket, told } => {
                let _ = told.send(ledger.top_up(ticket));
            }
            Message::GiveBack { ticket } => ledger.give_back(Instant::now(), ticket, &mut effects),
            Message::ProbationOver { ticket } => ledger.probation_over(ticket),
            Message::Status { reply } => {
                let _ = reply.send(ledger.status(Instant::now()));
            }
            Message::CutOff { told } => {
                // Every sender still here is a queued ask: dropped, its waiter refuses.
                let waiting: std::collections::HashSet<Ticket> =
                    grants.drain().map(|(ticket, _)| ticket).collect();
                // The restart forgets the gate work every quiet waiter waits for: a fork or attach
                // runs now; the cleanup's wait is dropped, which stops it until the next gateway.
                for (_, (scope, granted)) in quiets.drain() {
                    if scope != QuietScope::Host {
                        let _ = granted.send(());
                    }
                }
                // Nothing is lent after a cut-off: an install is restarting the gateway.
                lends.clear();
                let mut cutting = CutOff {
                    queued: waiting.len(),
                    waiting,
                    told: Some(told),
                };
                if cutting.waiting.is_empty() {
                    let told = cutting.told.take().expect("set just above");
                    let _ = told.send(0);
                }
                cut = Some(cutting);
            }
        }
        // A waiter this transition left queued may run on the probation token.
        let lent = cut.is_none().then(|| ledger.lend(Instant::now())).flatten();
        if let Some(lent) = &lent
            && let Some(told) = lends.remove(&lent.ticket)
        {
            let _ = told.send(());
        }
        // Deliver all grants before diagnostics: stderr backpressure must not sit between the
        // grants of one transition, and the measured step excludes its own reporting.
        for grant in &effects {
            if let Some(granted) = grants.remove(&grant.ticket) {
                let _ = granted.send(grant.tokens);
            }
            lends.remove(&grant.ticket);
        }
        for ticket in released.drain(..) {
            if let Some((_, granted)) = quiets.remove(&ticket) {
                let _ = granted.send(());
            }
        }
        let elapsed = started.elapsed();
        if let Some((cause, ticket, released)) = transition {
            if let Some(released) = released.filter(|_| !effects.is_empty()) {
                cowshed_os::eprintln!(
                    "cowshed: timing cpu-tokens handoff ticket={ticket} elapsed_ns={} grants={}",
                    released.elapsed().as_nanos(),
                    effects.len(),
                );
            }
            cowshed_os::eprintln!(
                "cowshed: timing cpu-tokens queue-step cause={cause} ticket={ticket} elapsed_ns={} grants={}",
                elapsed.as_nanos(),
                effects.len(),
            );
        }
        if let Some(Lent {
            ticket,
            checkout,
            claim,
        }) = lent
        {
            cowshed_os::eprintln!(
                "cowshed: timing cpu-tokens probation ticket={ticket} checkout={checkout:?} pid={:?} command={:?}",
                claim.pid,
                claim.command,
            );
        }
        for Grant {
            ticket,
            tokens,
            want,
            waited,
            checkout,
            claim,
            class,
            job,
            head,
        } in effects.drain(..)
        {
            cowshed_os::eprintln!(
                "cowshed: timing cpu-tokens grant ticket={ticket} tokens={tokens} want={want} waited_ns={} checkout={checkout:?} pid={:?} command={:?} gate_repo={:?} gate_job={:?} head_repo={:?} head_job={:?}",
                waited.as_nanos(),
                claim.pid,
                claim.command,
                class.repo(),
                class.repo().map(|_| job),
                head.as_ref().map(|(repo, _)| repo),
                head.as_ref().map(|(_, job)| job),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// A ledger of `total` tokens driven at explicit instants, collecting each step's grants as
    /// `(ticket, tokens)` and how long each granted request waited.
    struct Clock {
        ledger: Ledger,
        origin: Instant,
        effects: Vec<Grant>,
        waited: HashMap<Ticket, Duration>,
    }

    impl Clock {
        fn new(total: usize) -> Self {
            Self {
                ledger: Ledger::host(CpuBudgetLimits {
                    tokens: NonZeroUsize::new(total).unwrap(),
                }),
                origin: Instant::now(),
                effects: Vec::new(),
                waited: HashMap::new(),
            }
        }

        fn ask(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
            want: usize,
        ) -> Vec<(Ticket, usize)> {
            self.ask_as(ms, ticket, checkout, want, (Class::Background, ticket))
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
                (Class::Gate(Arc::from(checkout)), ticket),
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
                (Class::Gate(Arc::from(checkout)), job),
            )
        }

        fn ask_as(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
            want: usize,
            (class, job): (Class, Ticket),
        ) -> Vec<(Ticket, usize)> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.ask(
                Arc::from(checkout),
                Waiter {
                    ticket,
                    class,
                    job,
                    want: NonZeroUsize::new(want).unwrap().get(),
                    asked: now,
                    passed: 0,
                    claim: Claim::new(&format!("t{ticket}"), Some(7)),
                    lendable: false,
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

        /// A one-token background ask whose requester can run its job on probation: what its
        /// transition granted, and whom the probation token was lent to after it.
        fn ask_lendable(
            &mut self,
            ms: u64,
            ticket: Ticket,
            checkout: &str,
        ) -> (Vec<(Ticket, usize)>, Option<Ticket>) {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.ask(
                Arc::from(checkout),
                Waiter {
                    ticket,
                    class: Class::Background,
                    job: ticket,
                    want: 1,
                    asked: now,
                    passed: 0,
                    claim: Claim::new(&format!("t{ticket}"), Some(7)),
                    lendable: true,
                },
                &mut self.effects,
            );
            let grants = self.grants();
            (grants, self.lend(now))
        }

        /// A leave, and whom the probation token was lent to after it.
        fn leave_lending(
            &mut self,
            ms: u64,
            ticket: Ticket,
        ) -> (Vec<(Ticket, usize)>, Option<Ticket>) {
            let grants = self.leave(ms, ticket);
            (grants, self.lend(self.origin + Duration::from_millis(ms)))
        }

        /// `ticket`'s probation ended without a grant: whom the token was lent to next.
        fn probation_over(&mut self, ms: u64, ticket: Ticket) -> Option<Ticket> {
            self.ledger.probation_over(ticket);
            self.lend(self.origin + Duration::from_millis(ms))
        }

        fn lend(&mut self, now: Instant) -> Option<Ticket> {
            self.ledger.lend(now).map(|lent| lent.ticket)
        }

        /// A fork or attach of `repo` asks to run: the gate job it waits for, or `None` when it may
        /// run now.
        fn quiet(&mut self, ms: u64, ticket: Ticket, repo: &str) -> Option<Ticket> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger
                .ask_quiet(
                    ticket,
                    QuietScope::Repository(Arc::from(repo)),
                    None,
                    Claim::new(&format!("fork t{ticket}"), Some(7)),
                    now,
                )
                .map(|gate| gate.job)
        }

        /// The forks and attaches the transitions so far released, as the actor finds them.
        fn released(&mut self) -> Vec<Ticket> {
            let mut released = Vec::new();
            self.ledger.release_quiet(&mut released);
            released
        }

        /// Why queued `ticket` waits: the cause's kind and the gate job it names.
        fn cause(&self, ticket: Ticket) -> (&'static str, Option<Ticket>) {
            match self.ledger.queued(ticket).expect("a queued ticket").cause {
                CpuWaitCause::RepositoryGate { gate } => ("repository-gate", Some(gate.job)),
                CpuWaitCause::Quiet { gate } => ("quiet", Some(gate.job)),
                CpuWaitCause::Tokens => ("tokens", None),
            }
        }

        fn top_up(&mut self, ticket: Ticket) -> TopUpOutcome {
            self.ledger.top_up(ticket).outcome
        }

        fn give_back(&mut self, ms: u64, ticket: Ticket) -> Vec<(Ticket, usize)> {
            let now = self.origin + Duration::from_millis(ms);
            self.ledger.give_back(now, ticket, &mut self.effects);
            self.grants()
        }

        /// Each grant as `(ticket, tokens)`; its wait is kept in `waited`.
        fn grants(&mut self) -> Vec<(Ticket, usize)> {
            // Every transition, including a cancellation and a relayed child's release, preserves
            // one running gate job per repository. Different repositories may run together.
            let mut owners = HashMap::new();
            for holder in self
                .ledger
                .checkouts
                .values()
                .flat_map(|checkout| &checkout.running)
            {
                if let Some(repo) = holder.class.repo()
                    && let Some(previous) = owners.insert(repo, holder.job)
                {
                    assert_eq!(previous, holder.job, "two gate jobs hold tokens for {repo}");
                }
            }
            assert!(self.balanced());
            let waited = &mut self.waited;
            self.effects
                .drain(..)
                .map(|grant| {
                    waited.insert(grant.ticket, grant.waited);
                    (grant.ticket, grant.tokens)
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

        /// The ledger as its status reports it `ms` after the clock's origin.
        fn status(&self, ms: u64) -> CpuBudgetStatus {
            self.ledger.status(self.origin + Duration::from_millis(ms))
        }
    }

    /// The probation token (05_gateway.md "A one-token job starts on probation"). Long holders
    /// hold every token: a lendable one-token ask is lent the probation token at once, a second
    /// waits for it, and the first's leaving lends it on. An ask whose probation ends without a
    /// grant passes it on and keeps its place: a returned token grants it exactly one token, its
    /// probation is never lent again, and the ledger never holds more than its total.
    #[test]
    fn a_one_token_ask_runs_on_the_probation_token_while_long_holders_hold_every_token() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask_land(0, 1, "land", 3), [(1, 3)]);
        assert_eq!(clock.ask(0, 2, "shed", 1), [(2, 1)]);
        // A short read is lent the probation token at once; the status names it.
        assert_eq!(clock.ask_lendable(10, 3, "drive"), (vec![], Some(3)));
        assert_eq!(
            clock.status(30).probation,
            Some(CpuProbation {
                checkout: "drive".into(),
                pid: Some(7),
                command: "t3".into(),
                lent_ms: 20,
            })
        );
        // A second one waits for it, and the read's leaving lends it on.
        assert_eq!(clock.ask_lendable(40, 4, "drive"), (vec![], None));
        assert_eq!(clock.leave_lending(50, 3), (vec![], Some(4)));
        // Ticket 4 turns heavy: its probation ends, the token passes to the next lendable ask.
        assert_eq!(clock.ask_lendable(60, 5, "other"), (vec![], None));
        assert_eq!(clock.probation_over(70, 4), Some(5));
        assert_eq!(
            clock.ledger.free, 0,
            "the probation token is beyond the ledger's total"
        );
        // A returned token grants 4 in its place, one token; 5 keeps the probation token.
        assert_eq!(clock.leave_lending(80, 2), (vec![(4, 1)], None));
        assert_eq!(
            clock.status(80).probation.map(|lent| lent.command),
            Some("t5".into())
        );
        // 5's grant returns the probation token, and nobody is left to lend it to.
        assert_eq!(clock.leave_lending(90, 1), (vec![(5, 1)], None));
        assert_eq!(clock.status(90).probation, None);
        assert_eq!(clock.ledger.free, 2);
    }

    /// Every granted lease is named — its client's pid, its command, its tokens of its want and
    /// the age of its grant — oldest grant first, so a reader tells a live holder from an orphan
    /// without guessing from the checkout. Measured 2026-10-18: a ledger that said only
    /// `land-test-hang: held 15, running 1` read as an orphan, and a live 34-minute
    /// `cargo test … fleet_coder` was killed by hand. A waiter is named as a waiter -- its pid,
    /// command, want and how long it has waited -- never as a holder.
    #[test]
    fn the_status_names_each_lease_with_its_pid_command_and_the_age_of_its_grant() {
        let lease = |ticket: Ticket, tokens: usize, want: usize, held_ms: u64| CpuHolder {
            pid: Some(7),
            command: format!("t{ticket}"),
            tokens,
            want,
            held_ms,
        };
        let mut clock = Clock::new(16);
        assert_eq!(clock.ask(0, 1, "shed", 6), [(1, 6)]);
        assert_eq!(clock.ask_land(1_000, 2, "land", 9), [(2, 9)]);
        // Beside the land the shed's next runner is granted one token, the last free one.
        assert_eq!(clock.ask(2_000, 3, "shed", 8), [(3, 1)]);
        let status = clock.status(3_000);
        assert_eq!(
            status.gate_jobs,
            [CpuGateJob {
                repository: "land".into(),
                checkout: "land".into(),
                job: 2,
                held: 9,
                holders: vec![lease(2, 9, 9, 2_000)],
            }]
        );
        assert_eq!(
            status.background,
            [CpuBackgroundStake {
                checkout: "shed".into(),
                held: 7,
                waiters: Vec::new(),
                holders: vec![lease(1, 6, 6, 3_000), lease(3, 1, 8, 1_000)],
            }]
        );

        assert_eq!(clock.ask(4_000, 4, "shed", 1), []);
        assert_eq!(
            clock.status(4_500).background[0].waiters,
            [CpuWaiter {
                pid: Some(7),
                command: "t4".into(),
                want: 1,
                waited_ms: 500,
            }]
        );
        // The first lease returns: the waiter starts, and the build keeps the grant it started on.
        assert_eq!(clock.leave(5_000, 1), [(4, 1)]);
        let status = clock.status(9_000);
        assert_eq!(status.gate_jobs[0].holders, [lease(2, 9, 9, 8_000)]);
        assert_eq!(
            status.background,
            [CpuBackgroundStake {
                checkout: "shed".into(),
                held: 2,
                waiters: Vec::new(),
                holders: vec![lease(3, 1, 8, 7_000), lease(4, 1, 1, 4_000)],
            }]
        );
        assert!(clock.balanced());
    }

    #[test]
    fn a_lone_background_request_and_a_lone_gate_request_each_take_the_whole_host() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 4), [(1, 4)]);
        assert_eq!(clock.leave(1, 1), []);
        // With no gate work, no token is kept idle for a gate that may come.
        assert_eq!(clock.ask(2, 2, "a", 64), [(2, 18)]);
        assert_eq!(clock.status(2).held, 18);
        assert_eq!(clock.leave(3, 2), []);
        assert_eq!(clock.ask_land(4, 3, "a", 64), [(3, 18)]);
        assert_eq!(clock.status(4).held, 18);
        assert_eq!(clock.leave(5, 3), []);
        assert_eq!(clock.status(5).held, 0);
        assert!(clock.status(5).checkouts.is_empty());
        assert!(clock.balanced());
    }

    /// Danny tg#9527, measured 2026-10-22: no gate job ran, background was capped at 9 of 18 tokens,
    /// one shed held all 9, and three other sheds waited while 9 tokens idled. Background now takes
    /// every token while no gate-class work holds or waits. The trade: a grant made then cannot
    /// shrink, so a gate that arrives later is no longer granted in its own ask, as it was under the
    /// half cap (2026-10-19: a lone `cargo nextest run` granted 17 of 18 sat at 0% CPU for 50
    /// minutes beside a land's gate on one token). It takes back its share from the returned tokens:
    /// each goes to gate-class work first, background keeping only its floor token, with no kill and
    /// no timer.
    #[test]
    fn a_gate_arriving_after_background_filled_the_host_takes_the_returned_tokens_first() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "smoke", 18), [(1, 18)]);
        assert_eq!(clock.ask(1_000, 2, "other", 18), []);
        let later = 50 * 60 * 1_000;
        assert_eq!(clock.ask_land(later, 10, "train", 1), []);
        assert_eq!(clock.ask_task(later, 11, 10, "train", 18), []);
        // The background run ends: the background checkout holding nothing keeps its floor token,
        // and the land takes every other returned token.
        assert_eq!(clock.leave(later + 1, 1), [(2, 1), (10, 1), (11, 16)]);
        assert_eq!(clock.status(later + 1).gate_jobs[0].held, 17);
        assert!(clock.balanced());
    }

    #[test]
    fn beside_a_land_each_background_newcomer_takes_one_token_and_the_host_once_it_ends() {
        // Beside a land the host is quiet: each background grant is one token, whatever it wants,
        // so no newcomer starts a run sized to half the host. Once no gate work holds or waits,
        // background takes the whole host again.
        let mut clock = Clock::new(36);
        assert_eq!(clock.ask_land(0, 1, "land", 1), [(1, 1)]);
        assert_eq!(clock.ask(0, 2, "carry", 18), [(2, 1)]);
        assert_eq!(clock.ask(1, 3, "train", 18), [(3, 1)]);
        assert_eq!(clock.leave(2, 1), []);
        assert_eq!(clock.ask(3, 4, "carry", 18), [(4, 18)]);
        assert_eq!(clock.status(3).held, 20);
        assert!(clock.balanced());
    }

    #[test]
    fn with_nobody_else_waiting_a_checkout_takes_its_room_up_to_its_want() {
        let mut clock = Clock::new(36);
        assert_eq!(clock.ask(0, 1, "a", 5), [(1, 5)]);
        // Nobody else waits, so b takes the room up to its want, not a share.
        assert_eq!(clock.ask(1, 2, "b", 18), [(2, 18)]);
        // With no gate work the room is the rest of the host: a's second runner takes it.
        assert_eq!(clock.ask(2, 3, "a", 18), [(3, 13)]);
        assert_eq!(clock.status(2).held, 36);
        assert!(clock.balanced());
    }

    #[test]
    fn a_lone_waiter_takes_the_room_even_below_its_fair_share() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "a", 15), [(1, 15)]);
        // b's fair share is 9 of the host's 18, but no peer waits: the 3 left must not sit idle.
        assert_eq!(clock.ask(1, 2, "b", 18), [(2, 3)]);
        assert_eq!(clock.status(1).held, 18);
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
        let mut clock = Clock::new(24);
        for ticket in 0..60 {
            clock.ask(0, ticket, "busy", 1);
        }
        // With no gate work, busy's runners take the whole host.
        assert_eq!(clock.status(0).held, 24);
        assert_eq!(clock.ask(1, 100, "quiet", 1), []);
        assert_eq!(clock.ask(1, 101, "quiet", 1), []);
        // Every token busy returns goes to quiet until quiet holds as much as busy would allow.
        assert_eq!(clock.leave(2, 0), [(100, 1)]);
        assert_eq!(clock.leave(3, 1), [(101, 1)]);
        assert_eq!(clock.leave(4, 2), [(24, 1)]);
        let status = clock.status(4);
        assert_eq!(status.checkouts[0].checkout, "busy");
        assert_eq!(status.checkouts[0].held, 22);
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
        // One token frees. Three checkouts share the host's 8 as 2 each; c holds least and
        // asked first, so it waits for both tokens of its share, and b's single-token request
        // waits behind it.
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
        assert_eq!(clock.status(2).checkouts.len(), 1);
        assert_eq!(clock.leave(3, 1), []);
        assert_eq!(clock.leave(3, 2), []);
        assert_eq!(clock.status(3).held, 0);
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
    fn land_work_takes_every_returned_token_while_its_tasks_wait() {
        let mut clock = Clock::new(8);
        for ticket in 1..=4 {
            assert_eq!(clock.ask(0, ticket, "fleet", 1), [(ticket, 1)]);
        }
        // With no gate work, background takes the whole host.
        for ticket in 5..=8 {
            assert_eq!(clock.ask(0, ticket, "other", 1), [(ticket, 1)]);
        }
        assert_eq!(clock.ask(1, 9, "fleet", 1), []);
        assert_eq!(clock.ask(1, 10, "other", 1), []);
        // A land's gate job arrives at the full host, and its tasks ask through its relay pool. No
        // grant shrinks, so each waits for a returned token.
        assert_eq!(clock.ask_land(2, 11, "land", 1), []);
        assert_eq!(clock.ask_task(2, 12, 11, "land", 1), []);
        assert_eq!(clock.ask_task(2, 13, 11, "land", 1), []);
        // Each returned token goes to the land, ahead of the older background waiters, while
        // background holds half the host or more, and a land task already running does not stop the
        // next one from being admitted.
        assert_eq!(clock.leave(3, 1), [(11, 1)]);
        assert_eq!(clock.leave(4, 2), [(12, 1)]);
        assert_eq!(clock.leave(5, 3), [(13, 1)]);
        // No gate ask waits now: background takes each returned token, one each beside the land,
        // the checkout holding least first.
        assert_eq!(clock.leave(6, 4), [(9, 1)]);
        assert_eq!(clock.leave(7, 5), [(10, 1)]);
        assert_eq!(clock.leave(8, 6), []);
        assert_eq!(clock.leave(9, 11), []);
        assert_eq!(clock.leave(9, 12), []);
        assert_eq!(clock.leave(9, 13), []);
        assert!(clock.balanced());
    }

    #[test]
    fn a_land_asking_after_its_own_checkouts_background_goes_first() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask_land(0, 0, "other", 2), [(0, 2)]);
        assert_eq!(clock.ask(0, 1, "ws", 1), [(1, 1)]);
        // No gate ask waits: ws's next runner takes the last free token beside the land.
        assert_eq!(clock.ask(1, 2, "ws", 1), [(2, 1)]);
        // ws's own land asks after it at the full host and waits for a returned token.
        assert_eq!(clock.ask_land(2, 4, "ws", 1), []);
        // A returned background token goes to the waiting land, never back to background.
        assert_eq!(clock.ask(3, 5, "ws", 1), []);
        assert_eq!(clock.leave(3, 1), [(4, 1)]);
        // The older land ends: no gate ask waits, and the runner takes a free token.
        assert_eq!(clock.leave(4, 0), [(5, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn an_older_land_is_served_before_a_younger_land_and_both_before_a_feature_build() {
        let mut clock = Clock::new(8);
        // A feature shed's builds start while no gate works, and take a quarter of the host.
        assert_eq!(clock.ask(0, 3, "feature", 2), [(3, 2)]);
        // Two land gate jobs start: a's is older than b's.
        assert_eq!(clock.ask_land(1, 1, "a", 3), [(1, 3)]);
        assert_eq!(clock.ask_land(1, 2, "b", 3), [(2, 3)]);
        // The feature's next build waits first; then the younger land's task asks, and the older
        // land's task asks last of all.
        assert_eq!(clock.ask(2, 4, "feature", 2), []);
        assert_eq!(clock.ask_task(4, 6, 2, "b", 2), []);
        assert_eq!(clock.ask_task(5, 7, 1, "a", 2), []);
        let queue: Vec<(String, u64)> = clock
            .status(5)
            .gate_queue
            .into_iter()
            .map(|waiter| (waiter.checkout, waiter.job))
            .collect();
        assert_eq!(queue, [("a".to_owned(), 1), ("b".to_owned(), 2)]);
        // The feature's returned tokens go to the older land's task first.
        assert_eq!(clock.leave(6, 3), [(7, 2)]);
        // Then the younger land's task, after the floor token of the feature, which holds nothing.
        assert_eq!(clock.leave(7, 7), [(4, 1), (6, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn an_older_land_takes_every_token_its_tasks_want_past_a_younger_lands_floor() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask_task(0, 1, 0, "a", 18), [(1, 18)]);
        assert_eq!(clock.ask_task(1, 2, 9, "b", 18), []);
        assert_eq!(clock.ask_task(1, 3, 0, "a", 18), []);
        assert_eq!(clock.ask(2, 4, "c", 18), []);
        // b's younger gate job and c each hold nothing, so each keeps its one token, oldest ask
        // first; then a's second task, though it asked after b's, takes the rest, because a's
        // gate job is older.
        assert_eq!(clock.leave(3, 1), [(2, 1), (4, 1), (3, 16)]);
        assert_eq!(clock.leave(4, 3), []);
        assert!(clock.balanced());
    }

    #[test]
    fn land_work_holds_the_larger_part_without_starving_background() {
        let mut clock = Clock::new(6);
        // With no gate work, background takes the whole host.
        assert_eq!(clock.ask(0, 1, "x", 6), [(1, 6)]);
        // The land's gate job and its task arrive at the full host and wait for returned tokens.
        assert_eq!(clock.ask_land(1, 2, "land", 6), []);
        assert_eq!(clock.ask_task(1, 3, 2, "land", 6), []);
        assert_eq!(clock.ask(2, 4, "y", 1), []);
        assert_eq!(clock.ask(2, 5, "x", 1), []);
        // Each background checkout holding nothing gets one token while background stays under
        // half the host; the land's gate job takes the rest.
        assert_eq!(clock.leave(3, 1), [(4, 1), (5, 1), (2, 4)]);
        let status = clock.status(3);
        assert_eq!(status.gate_jobs[0].held, 4);
        let background: usize = status.background.iter().map(|stake| stake.held).sum();
        assert_eq!(background, 2);
        assert!(clock.balanced());
    }

    /// The measured failure: an older land's gate job held 13 of 18 tokens in 5 grants with more
    /// of its tasks queued, background held the other 5, and a younger land's gate job, in a
    /// checkout that already held a background token, asked for its orchestrator's one token. With
    /// no floor of its own it waited for the whole older land and died on its admission bound.
    /// Now the first returned token starts it, a second younger land too, neither waits on the
    /// other's token, and their tasks queue behind the older land until it stops wanting.
    #[test]
    fn a_younger_land_starts_on_its_floor_while_an_older_land_holds_most_of_the_host() {
        let mut clock = Clock::new(18);
        for (ticket, checkout) in [
            (1, "code-mode"),
            (2, "selector"),
            (3, "bun143"),
            (4, "caches"),
            (5, "caches"),
        ] {
            assert_eq!(clock.ask(0, ticket, checkout, 1), [(ticket, 1)]);
        }
        assert_eq!(clock.ask_land(1, 10, "infer", 1), [(10, 1)]);
        for ticket in 11..=14 {
            assert_eq!(clock.ask_task(2, ticket, 10, "infer", 3), [(ticket, 3)]);
        }
        assert_eq!(clock.status(2).held, 18);
        assert_eq!(clock.ask_task(3, 15, 10, "infer", 3), []);
        assert_eq!(clock.ask_land(4, 20, "code-mode", 1), []);
        assert_eq!(clock.ask_land(5, 30, "other", 1), []);
        // An older task returns 3: each younger land takes its floor token, oldest first, and the
        // older land's queued task takes the last.
        assert_eq!(clock.leave(6, 11), [(20, 1), (30, 1), (15, 1)]);
        // The younger lands hold their floor now, so their tasks wait behind the older land's.
        assert_eq!(clock.ask_task(7, 21, 20, "code-mode", 3), []);
        assert_eq!(clock.ask_task(7, 16, 10, "infer", 3), []);
        assert_eq!(clock.leave(8, 12), [(16, 3)]);
        // The older land wants nothing more: the older of the younger lands is served next.
        assert_eq!(clock.leave(9, 13), [(21, 3)]);
        assert!(clock.balanced());
    }

    #[test]
    fn same_repository_waits_without_a_floor_while_another_repository_runs() {
        let mut clock = Clock::new(18);
        let gate = |repo: &str, job| (Class::Gate(Arc::from(repo)), job);
        assert_eq!(clock.ask_as(0, 1, "first", 1, gate("owner/a", 1)), [(1, 1)]);
        assert_eq!(
            clock.ask_as(1, 2, "first", 17, gate("owner/a", 1)),
            [(2, 17)]
        );
        assert_eq!(clock.ask_as(2, 3, "second", 1, gate("owner/a", 3)), []);
        assert_eq!(clock.ask_as(3, 4, "other", 1, gate("owner/b", 4)), []);
        assert_eq!(clock.ask_as(4, 5, "first", 17, gate("owner/a", 1)), []);
        // Only the other repository gets a floor; the first repository's head takes the rest.
        assert_eq!(clock.leave(5, 2), [(4, 1), (5, 16)]);
        // A root ending does not release its repository while a relayed task still holds tokens.
        assert_eq!(clock.leave(6, 1), []);
        assert_eq!(clock.leave(7, 5), [(3, 1)]);
        assert_eq!(clock.leave(8, 3), []);
        assert_eq!(clock.leave(9, 4), []);
        assert_eq!(clock.status(9).held, 0);
    }

    #[test]
    fn blocked_gates_cannot_use_background_room_and_cancellation_preserves_repo_order() {
        let mut clock = Clock::new(8);
        let gate = |job| (Class::Gate(Arc::from("owner/a")), job);
        assert_eq!(clock.ask_as(0, 1, "first", 1, gate(1)), [(1, 1)]);
        assert_eq!(clock.ask_as(1, 2, "second", 1, gate(2)), []);
        assert_eq!(clock.ask_as(2, 3, "third", 1, gate(3)), []);
        // Free tokens cannot start a behind-head gate through the background selector.
        assert_eq!(clock.ask(3, 4, "background", 1), [(4, 1)]);
        assert_eq!(clock.leave(4, 2), []);
        assert_eq!(clock.leave(5, 1), [(3, 1)]);
        assert_eq!(clock.leave(6, 3), []);
        assert_eq!(clock.leave(7, 4), []);
    }

    #[test]
    fn a_late_enqueued_older_ticket_cannot_preempt_a_running_repository() {
        let mut clock = Clock::new(8);
        let gate = |job| (Class::Gate(Arc::from("owner/a")), job);
        assert_eq!(clock.ask_as(0, 10, "first", 1, gate(10)), [(10, 1)]);
        assert_eq!(clock.ask_as(1, 5, "late", 1, gate(5)), []);
        assert_eq!(clock.ask_as(2, 11, "first", 1, gate(10)), [(11, 1)]);
        assert_eq!(clock.leave(3, 10), []);
        assert_eq!(clock.leave(4, 11), [(5, 1)]);
        assert_eq!(clock.leave(5, 5), []);
    }

    /// The measured starvation, 2026-10-22 on gateway 89443: scratch checkout
    /// ax-resources-18390-1 held one token for an idle FIFO reader, and its next one-token ask,
    /// ticket 95, waited 2,478 s while 51 later background asks of checkouts holding nothing were
    /// granted, because the fewest-held order put it behind every one of them. A background head
    /// is passed by at most the background limit of later background grants, then served.
    #[test]
    fn a_checkout_holding_a_token_is_passed_by_at_most_the_background_limit_of_later_grants() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "scratch", 1), [(1, 1)]);
        // With no gate work, background fills the host.
        for ticket in 2..=18 {
            assert_eq!(
                clock.ask(0, ticket, &format!("busy{ticket}"), 1),
                [(ticket, 1)]
            );
        }
        assert_eq!(clock.ask(1, 19, "scratch", 1), []);
        let mut holders: VecDeque<Ticket> = (2..=18).collect();
        // Each returned token goes to a later checkout holding nothing, nine times.
        for ticket in 20..=28 {
            assert_eq!(clock.ask(ticket, ticket, &format!("fresh{ticket}"), 1), []);
            let returned = holders.pop_front().expect("a holder returns");
            assert_eq!(clock.leave(ticket, returned), [(ticket, 1)]);
            holders.push_back(ticket);
        }
        // Passed by the background limit's nine, the scratch ask is served before a tenth.
        assert_eq!(clock.ask(29, 29, "fresh29", 1), []);
        let returned = holders.pop_front().expect("a holder returns");
        assert_eq!(clock.leave(29, returned), [(19, 1)]);
        assert!(clock.balanced());
    }

    /// The same starvation beside a land: while a gate wants every returned token, background is
    /// served only by the floor token, and the floor went only to checkouts holding nothing.
    #[test]
    fn an_overdue_background_head_takes_the_floor_token_beside_a_land() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask(0, 1, "scratch", 1), [(1, 1)]);
        for ticket in 2..=6 {
            assert_eq!(
                clock.ask(0, ticket, &format!("busy{ticket}"), 1),
                [(ticket, 1)]
            );
        }
        assert_eq!(clock.ask_land(1, 7, "land", 12), [(7, 12)]);
        assert_eq!(clock.ask_task(1, 8, 7, "land", 18), []);
        assert_eq!(clock.ask(2, 9, "scratch", 1), []);
        let mut holders: VecDeque<Ticket> = (2..=6).collect();
        for ticket in 10..=18 {
            assert_eq!(clock.ask(ticket, ticket, &format!("fresh{ticket}"), 1), []);
            let returned = holders.pop_front().expect("a holder returns");
            assert_eq!(clock.leave(ticket, returned), [(ticket, 1)]);
            holders.push_back(ticket);
        }
        assert_eq!(clock.ask(19, 19, "fresh19", 1), []);
        let returned = holders.pop_front().expect("a holder returns");
        assert_eq!(clock.leave(19, returned), [(9, 1)]);
        assert!(clock.balanced());
    }

    /// Gate work quiets the host (05_gateway.md "Room for a gate"). Each row is a host of `total`
    /// tokens where background already holds `held`, granted before any gate; a land's gate job
    /// then asks `gate` tokens (none: no gate work); and a newcomer asks `want`. With no gate work
    /// it takes its want from every free token; beside a gate that asks nothing more it takes one
    /// free token, however much the work behind the gate holds, and nothing from a full host.
    #[test]
    fn beside_gate_work_a_background_grant_is_one_free_token() {
        // (total, background held, the gate's tokens, the newcomer's want, its grant)
        type Row = (usize, usize, Option<usize>, usize, Option<usize>);
        let rows: [Row; 10] = [
            (18, 0, None, 9, Some(9)),
            (18, 7, None, 4, Some(4)),
            (18, 0, Some(1), 9, Some(1)),
            (18, 7, Some(1), 4, Some(1)),
            (18, 8, Some(1), 4, Some(1)),
            (18, 0, Some(18), 1, None),
            (18, 0, Some(17), 1, Some(1)),
            (3, 0, Some(1), 2, Some(1)),
            (2, 0, Some(1), 1, Some(1)),
            (1, 0, Some(1), 1, None),
        ];
        for (row, (total, held, gate, want, grant)) in rows.into_iter().enumerate() {
            let mut clock = Clock::new(total);
            if held > 0 {
                assert_eq!(clock.ask(0, 1, "busy", held), [(1, held)], "row {row}");
            }
            if let Some(gate) = gate {
                assert_eq!(clock.ask_land(1, 2, "land", gate), [(2, gate)], "row {row}");
            }
            let expected: Vec<(Ticket, usize)> =
                grant.map(|tokens| (3, tokens)).into_iter().collect();
            assert_eq!(clock.ask(2, 3, "newcomer", want), expected, "row {row}");
            assert!(clock.balanced(), "row {row}");
        }
    }

    /// A test's scratch checkout that already holds a token beside a land gets its next one: beside
    /// gate work background is served one token a grant in checkout-fair order, not only to the
    /// checkouts holding nothing, so a test waiting on its scratch job's second job does not wait
    /// for the very gate it runs under. A younger land's tokens count behind the older one.
    #[test]
    fn beside_a_land_a_checkout_holding_a_token_gets_one_more() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask_land(0, 1, "land", 4), [(1, 4)]);
        assert_eq!(clock.ask(1, 2, "scratch", 1), [(2, 1)]);
        assert_eq!(clock.ask(2, 3, "scratch", 2), [(3, 1)]);
        // A younger land of another repository holds 6, and no gate ask waits: the scratch's next
        // job takes a free token beside both.
        assert_eq!(clock.ask_land(3, 4, "younger", 6), [(4, 6)]);
        assert_eq!(clock.ask(4, 5, "scratch", 1), [(5, 1)]);
        assert_eq!(clock.leave(5, 4), []);
        assert!(clock.balanced());
    }

    /// A fork or attach waits for its own repository's gate (05_gateway.md "A fork or attach waits
    /// for its repository's gate"). Each row asks for gate-class requests on a four-token host,
    /// then a fork of one repository asks to run: at once while no gate-class request of that
    /// repository holds or waits, and otherwise waiting for the gate job heading it, the holder
    /// ahead of an older queued job and a job's root ahead of its tasks.
    #[test]
    fn a_fork_waits_only_for_its_own_repositorys_gate_and_names_the_job_heading_it() {
        // (gate requests as (ticket, repository, job, want), the fork's repository, the job it waits for)
        type Row = (
            &'static [(Ticket, &'static str, Ticket, usize)],
            &'static str,
            Option<Ticket>,
        );
        let rows: [Row; 6] = [
            (&[], "owner/a", None),
            (&[(1, "owner/a", 1, 1)], "owner/a", Some(1)),
            (&[(1, "owner/a", 1, 1)], "owner/b", None),
            (
                &[(1, "owner/b", 1, 4), (2, "owner/a", 2, 4)],
                "owner/a",
                Some(2),
            ),
            (
                &[(1, "owner/a", 1, 4), (2, "owner/a", 2, 1)],
                "owner/a",
                Some(1),
            ),
            (
                &[(3, "owner/a", 1, 4), (1, "owner/a", 1, 4)],
                "owner/a",
                Some(1),
            ),
        ];
        for (row, (gates, repo, waits_for)) in rows.into_iter().enumerate() {
            let mut clock = Clock::new(4);
            for &(ticket, gate, job, want) in gates {
                clock.ask_as(
                    0,
                    ticket,
                    &format!("ws{ticket}"),
                    want,
                    (Class::Gate(Arc::from(gate)), job),
                );
            }
            assert_eq!(clock.quiet(1, 100, repo), waits_for, "row {row}");
            assert_eq!(
                clock.status(1).quiet.len(),
                usize::from(waits_for.is_some()),
                "row {row}"
            );
        }
    }

    /// A fork or attach the repository's own gate job asks for names one of that job's pools and
    /// runs at once: it would otherwise wait for itself. A pool beside them in the same workspace
    /// directory, a background job's, waits, and so does a younger gate job's; a gate job's pools
    /// are forgotten with it.
    #[test]
    fn a_gate_jobs_own_fork_runs_at_once_and_any_other_waits() {
        use std::path::Path;
        let mut clock = Clock::new(4);
        let gate = |job| (Class::Gate(Arc::from("owner/a")), job);
        assert_eq!(clock.ask_as(0, 1, "land", 1, gate(1)), [(1, 1)]);
        assert_eq!(clock.ask_as(0, 2, "next", 1, gate(2)), []);
        for (job, endpoint) in [
            (1, "/s/run/cpu/land/root"),
            (1, "/s/run/cpu/land/child"),
            (2, "/s/run/cpu/next/root"),
        ] {
            clock.ledger.gate_pool(job, Arc::from(Path::new(endpoint)));
        }
        let now = clock.origin;
        let mut quiet = |ticket, pool: &str| {
            clock
                .ledger
                .ask_quiet(
                    ticket,
                    QuietScope::Repository(Arc::from("owner/a")),
                    Some(Path::new(pool)),
                    Claim::new("fork", Some(7)),
                    now,
                )
                .map(|gate| gate.job)
        };
        // (ticket, the requester's pool, the gate job it waits for)
        for (ticket, pool, waits) in [
            (10, "/s/run/cpu/land/root", None),
            (11, "/s/run/cpu/land/child", None),
            (12, "/s/run/cpu/land/background", Some(1)),
            (13, "/s/run/cpu/next/root", Some(1)),
        ] {
            assert_eq!(quiet(ticket, pool), waits, "ticket {ticket}");
        }
        assert_eq!(clock.leave(1, 1), [(2, 1)]);
        assert_eq!(
            clock
                .ledger
                .gate_pools
                .values()
                .copied()
                .collect::<Vec<_>>(),
            [2],
            "a gate job's pools are forgotten with it"
        );
        assert!(clock.balanced());
    }

    /// The cleanup's host-wide quiet ask waits while any repository's gate work holds or waits,
    /// for the oldest gate job, and runs once the last of it, in any repository, has left.
    #[test]
    fn the_cleanups_quiet_ask_waits_for_every_repositorys_gate_work() {
        let mut clock = Clock::new(4);
        let gate = |repo: &str, job| (Class::Gate(Arc::from(repo)), job);
        let ask = |clock: &mut Clock, ticket| {
            clock
                .ledger
                .ask_quiet(
                    ticket,
                    QuietScope::Host,
                    None,
                    Claim::new("image-cleanup", None),
                    clock.origin,
                )
                .map(|gate| gate.job)
        };
        assert_eq!(ask(&mut clock, 10), None);
        assert_eq!(clock.ask_as(0, 1, "a", 1, gate("owner/a", 1)), [(1, 1)]);
        assert_eq!(clock.ask_as(0, 2, "b", 1, gate("owner/b", 2)), [(2, 1)]);
        assert_eq!(ask(&mut clock, 11), Some(1));
        assert_eq!(clock.status(0).quiet[0].repository, None);
        clock.leave(1, 1);
        assert!(clock.released().is_empty(), "owner/b's gate still works");
        clock.leave(2, 2);
        assert_eq!(clock.released(), [11]);
        assert!(clock.balanced());
    }

    /// A waiting fork runs once its repository's last gate-class request leaves, not when the
    /// gate's root does while its task still holds; a fork that closes while it waits leaves the
    /// queue and is never released; the status names each waiting one.
    #[test]
    fn a_waiting_fork_runs_when_its_repositorys_last_gate_request_leaves() {
        let mut clock = Clock::new(4);
        let gate = |job| (Class::Gate(Arc::from("owner/a")), job);
        assert_eq!(clock.ask_as(0, 1, "land", 1, gate(1)), [(1, 1)]);
        assert_eq!(clock.ask_as(0, 2, "land", 2, gate(1)), [(2, 2)]);
        assert_eq!(clock.quiet(1, 10, "owner/a"), Some(1));
        assert_eq!(clock.quiet(2, 11, "owner/a"), Some(1));
        assert_eq!(clock.quiet(2, 12, "owner/b"), None);
        assert_eq!(
            clock.status(5).quiet,
            [
                CpuQuietWaiter {
                    repository: Some("owner/a".into()),
                    pid: Some(7),
                    command: "fork t10".into(),
                    waited_ms: 4,
                },
                CpuQuietWaiter {
                    repository: Some("owner/a".into()),
                    pid: Some(7),
                    command: "fork t11".into(),
                    waited_ms: 3,
                },
            ]
        );
        assert_eq!(clock.leave(6, 11), []);
        assert!(clock.released().is_empty());
        assert_eq!(clock.leave(7, 1), []);
        assert!(clock.released().is_empty());
        assert_eq!(clock.leave(8, 2), []);
        assert_eq!(clock.released(), [10]);
        assert!(clock.status(8).quiet.is_empty());
        assert!(clock.balanced());
    }

    /// Measured 2026-10-26 on the train-6 install: one gate job holding 1 token, asking nothing
    /// more, left 17 background asks queued with 9 of 18 tokens free. With no gate ask queued,
    /// background takes free tokens at once, one each beside the gate; a gate ask that queues is
    /// served first.
    #[test]
    fn beside_a_gate_asking_nothing_more_background_takes_the_free_tokens() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask_land(0, 1, "land", 1), [(1, 1)]);
        for n in 0..8 {
            let checkout = format!("bg{n}");
            assert_eq!(clock.ask(1 + n, 10 + n, &checkout, 4), [(10 + n, 1)]);
        }
        assert_eq!(
            clock.ask(20, 30, "late", 4),
            [(30, 1)],
            "no gate ask is queued"
        );
        assert_eq!(clock.ask(21, 31, "later", 4), [(31, 1)]);
        assert_eq!(
            clock.ask_task(22, 2, 1, "land", 18),
            [(2, 7)],
            "the gate takes the rest"
        );
        assert_eq!(clock.ask(23, 32, "last", 4), [], "the host is full");
        assert!(clock.balanced());
    }

    /// The floor binds while any gate-class request waits, an ineligible one included: a younger
    /// gate job of the head's repository waits for the head, which asks nothing more, and
    /// background beyond the floor waits beside it rather than take the room that job needs once
    /// the head ends.
    #[test]
    fn beside_a_waiting_younger_gate_of_the_heads_repository_background_keeps_to_the_floor() {
        let mut clock = Clock::new(18);
        let gate = |job| (Class::Gate(Arc::from("owner/a")), job);
        assert_eq!(clock.ask_as(0, 1, "first", 1, gate(1)), [(1, 1)]);
        assert_eq!(
            clock.ask_as(1, 2, "second", 4, gate(2)),
            [],
            "one gate per repository"
        );
        for n in 0..8 {
            let checkout = format!("bg{n}");
            assert_eq!(clock.ask(2 + n, 10 + n, &checkout, 4), [(10 + n, 1)]);
        }
        assert_eq!(
            clock.ask(20, 30, "late", 4),
            [],
            "the floor is shut while a gate waits"
        );
        assert_eq!(
            clock.leave(21, 2),
            [(30, 1)],
            "no gate waits: background takes a free token"
        );
        assert!(clock.balanced());
    }

    /// The measured starvation (2026-10-26, gate job 949): a land task granted 1 of the 18 it
    /// asked while its siblings held the host keeps tokens flowing once they end. It tops up one
    /// token at a time from tokens nobody waits for, up to its want, until the host is full, and
    /// a token it gives back goes to a waiter first.
    #[test]
    fn a_task_granted_less_than_it_asked_tops_up_from_tokens_nobody_waits_for() {
        let mut clock = Clock::new(18);
        assert_eq!(clock.ask_land(0, 1, "land", 1), [(1, 1)]);
        assert_eq!(clock.ask_task(1, 2, 1, "land", 16), [(2, 16)]);
        assert_eq!(clock.ask_task(2, 3, 1, "land", 18), [(3, 1)]);
        assert_eq!(clock.leave(3, 2), []);
        assert_eq!(
            clock.status(3).held,
            2,
            "nobody waits: the freed tokens sit"
        );
        for _ in 0..16 {
            assert_eq!(clock.top_up(3), TopUpOutcome::Granted);
        }
        assert_eq!(clock.top_up(3), TopUpOutcome::Full);
        assert_eq!(clock.status(3).held, 18);
        assert!(clock.balanced());
        // A waiter queued meanwhile takes the token given back, and no top-up passes it.
        assert_eq!(clock.ask_task(4, 4, 1, "land", 2), []);
        assert_eq!(clock.give_back(5, 3), [(4, 1)]);
        assert_eq!(clock.give_back(6, 3), []);
        assert_eq!(clock.top_up(3), TopUpOutcome::Granted);
        assert_eq!(clock.leave(7, 3), []);
        assert_eq!(
            clock.status(7).held,
            2,
            "leaving returns every topped-up token"
        );
        assert!(clock.balanced());
    }

    /// A top-up takes only tokens no request waits for: a younger gate job of the same repository
    /// waits with tokens free, and a background holder below its want tops up nothing past it;
    /// once it leaves, nothing waits and the holder tops up to its want.
    #[test]
    fn no_top_up_passes_a_waiting_request() {
        let mut clock = Clock::new(18);
        let gate = |repo: &str, job| (Class::Gate(Arc::from(repo)), job);
        assert_eq!(clock.ask_as(0, 1, "a1", 1, gate("owner/a", 1)), [(1, 1)]);
        assert_eq!(clock.ask(1, 10, "bg", 12), [(10, 1)]);
        assert_eq!(clock.ask_as(2, 3, "a2", 1, gate("owner/a", 3)), []);
        assert_eq!(clock.top_up(10), TopUpOutcome::Waiting);
        assert_eq!(
            clock.top_up(1),
            TopUpOutcome::Settled,
            "a holder at its want"
        );
        assert_eq!(clock.leave(3, 3), []);
        for _ in 1..12 {
            assert_eq!(clock.top_up(10), TopUpOutcome::Granted);
        }
        assert_eq!(clock.top_up(10), TopUpOutcome::Settled, "at its want");
        assert!(clock.balanced());
    }

    /// A queued ask hears why it waits (05_gateway.md "One gate per repository"): each row asks in
    /// turn on a four-token host that background fills before any gate, and names the cause its
    /// own admission transition left it with.
    #[test]
    fn a_queued_ask_names_the_gate_it_waits_for() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask(0, 1, "busy", 4), [(1, 4)]);
        // (ticket, checkout, gate repository and job, its cause, the gate job the cause names)
        type Row = (
            Ticket,
            &'static str,
            Option<(&'static str, Ticket)>,
            &'static str,
            Option<Ticket>,
        );
        let rows: [Row; 5] = [
            (2, "other", None, "tokens", None),
            (3, "land", Some(("owner/a", 3)), "tokens", None),
            (
                4,
                "second",
                Some(("owner/a", 4)),
                "repository-gate",
                Some(3),
            ),
            (5, "late", None, "quiet", Some(3)),
            (6, "land-b", Some(("owner/b", 6)), "tokens", None),
        ];
        for (ticket, checkout, gate, kind, job) in rows {
            let class = gate.map_or((Class::Background, ticket), |(repo, job)| {
                (Class::Gate(Arc::from(repo)), job)
            });
            assert_eq!(
                clock.ask_as(ticket, ticket, checkout, 1, class),
                [],
                "ticket {ticket}"
            );
            assert_eq!(clock.cause(ticket), (kind, job), "ticket {ticket}");
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Ask {
            checkout: usize,
            want: usize,
            gate: Option<usize>,
        },
        Leave {
            pick: usize,
        },
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0..6usize, 1..5usize, proptest::option::weighted(0.2, 0..2usize))
                .prop_map(|(checkout, want, gate)| Op::Ask { checkout, want, gate }),
            2 => any::<usize>().prop_map(|pick| Op::Leave { pick }),
        ]
    }

    /// One live request as the work-conserving property observes it from outside the ledger.
    #[derive(Debug)]
    struct Asked {
        checkout: usize,
        gate: bool,
        want: usize,
        /// The tokens it was granted, once granted.
        held: Option<usize>,
    }

    proptest! {
        /// Admission is work-conserving (05_gateway.md "Room for a gate", Danny tg#9527). Over any
        /// host size and sequence of background asks, gate asks and departures: with no gate-class
        /// request holding or waiting, a token stays free while background waits only when two or
        /// more checkouts wait and a waiting request wants more than the free tokens, so background
        /// may hold every token; a gate-class request never waits beside a free token, so a gate
        /// that arrives after background filled the host takes the next returned one; and while a
        /// gate-class request holds or waits, background is granted one token at a time, beyond
        /// the floor only while no gate ask waits, and never waits beside a free token no gate ask
        /// waits for.
        #[test]
        fn background_takes_every_free_token_without_gate_work_and_a_waiting_gate_takes_them_first(
            total in 1..13usize,
            ops in proptest::collection::vec(op(), 1..200),
        ) {
            let mut clock = Clock::new(total);
            let mut live: BTreeMap<Ticket, Asked> = BTreeMap::new();
            let mut gate_jobs: HashMap<usize, Ticket> = HashMap::new();
            for (ticket, op) in (1..).zip(ops) {
                let grants = match op {
                    Op::Ask { checkout, want, gate } => {
                        let name = format!("c{checkout}");
                        live.insert(ticket, Asked { checkout, gate: gate.is_some(), want, held: None });
                        match gate {
                            None => clock.ask(ticket, ticket, &name, want),
                            Some(repo) => {
                                let job = *gate_jobs.entry(repo).or_insert(ticket);
                                let class = Class::Gate(Arc::from(format!("r{repo}")));
                                clock.ask_as(ticket, ticket, &name, want, (class, job))
                            }
                        }
                    }
                    Op::Leave { pick } => {
                        if live.is_empty() {
                            continue;
                        }
                        let gone = *live.keys().nth(pick % live.len()).expect("a live ticket");
                        live.remove(&gone);
                        clock.leave(ticket, gone)
                    }
                };
                for (granted, tokens) in grants {
                    let gate_works = live.values().any(|asked| asked.gate);
                    let asked = live.get_mut(&granted).expect("a granted ticket is live");
                    asked.held = Some(tokens);
                    if !asked.gate && gate_works {
                        let background: usize = live
                            .values()
                            .filter(|asked| !asked.gate)
                            .filter_map(|asked| asked.held)
                            .sum();
                        let gate_waits = live
                            .values()
                            .any(|asked| asked.gate && asked.held.is_none());
                        prop_assert!(
                            tokens == 1 && (background <= behind_limit(total) || !gate_waits),
                            "ticket {granted} was granted {tokens} beside gate work; background holds {background} of {total}"
                        );
                    }
                }
                let free = clock.ledger.free;
                let waiting = || live.values().filter(|asked| asked.held.is_none());
                prop_assert!(
                    free == 0 || !waiting().any(|asked| asked.gate),
                    "a gate-class request waits beside {free} free tokens"
                );
                prop_assert!(
                    free == 0 || !live.values().any(|asked| asked.gate) || waiting().next().is_none(),
                    "a background request waits beside {free} free tokens no gate ask waits for"
                );
                if free > 0 && !live.values().any(|asked| asked.gate) {
                    let checkouts: std::collections::BTreeSet<usize> =
                        waiting().map(|asked| asked.checkout).collect();
                    prop_assert!(
                        checkouts.is_empty()
                            || (checkouts.len() > 1 && waiting().any(|asked| asked.want > free)),
                        "{free} of {total} tokens are free with no gate work while checkouts {checkouts:?} wait"
                    );
                }
            }
        }
    }

    proptest! {
        /// No background ask waits behind an unbounded number of later asks of its rank: each
        /// background request of its own checkout queued ahead of it, and its own turn at the
        /// head, is passed by at most the background limit of later background grants. Gate
        /// grants outrank background and are not counted.
        #[test]
        fn no_background_ask_is_passed_by_more_than_the_limit_of_later_background_grants(
            total in 1..13usize,
            ops in proptest::collection::vec(op(), 1..200),
        ) {
            let bound = background_limit(total);
            let mut clock = Clock::new(total);
            // The waiting background asks: ticket to (checkout, asks of it queued ahead, passes).
            let mut waiting: BTreeMap<Ticket, (usize, usize, usize)> = BTreeMap::new();
            let mut live: Vec<Ticket> = Vec::new();
            let mut gate_jobs: HashMap<usize, Ticket> = HashMap::new();
            for (ticket, op) in (1..).zip(ops) {
                let grants = match op {
                    Op::Ask { checkout, want, gate } => {
                        live.push(ticket);
                        let name = format!("c{checkout}");
                        match gate {
                            None => {
                                let ahead = waiting
                                    .values()
                                    .filter(|(queued, ..)| *queued == checkout)
                                    .count();
                                waiting.insert(ticket, (checkout, ahead, 0));
                                clock.ask(ticket, ticket, &name, want)
                            }
                            Some(repo) => {
                                let job = *gate_jobs.entry(repo).or_insert(ticket);
                                let class = Class::Gate(Arc::from(format!("r{repo}")));
                                clock.ask_as(ticket, ticket, &name, want, (class, job))
                            }
                        }
                    }
                    Op::Leave { pick } => {
                        if live.is_empty() {
                            continue;
                        }
                        let gone = live.swap_remove(pick % live.len());
                        waiting.remove(&gone);
                        clock.leave(ticket, gone)
                    }
                };
                for (granted, _) in grants {
                    if waiting.remove(&granted).is_none() {
                        continue;
                    }
                    for (older, (_, ahead, passed)) in waiting.range_mut(..granted) {
                        *passed += 1;
                        prop_assert!(
                            *passed <= (*ahead + 1) * bound,
                            "ticket {older} was passed by {passed} later background grants, {ahead} of its checkout's asks ahead, bound {bound}"
                        );
                    }
                }
            }
        }
    }
}
