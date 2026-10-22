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
//! work-conserving: while no gate-class work holds or waits, background may be granted every
//! token, so none idles while background wants it. Beside gate-class work no background grant takes
//! the class past half the host ([`background_cap`]), and a gate that arrives after background
//! filled the host takes back its share from the next returned tokens: no grant shrinks, and
//! nothing is killed or timed. Class
//! ranks first: while any gate-class request waits, returned tokens go to gate-class work ahead
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
    CpuWaiter, GateWork, InstallRestart, background_cap, background_limit, host_tokens,
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

/// The budget's state.
///
/// Invariants: `free` plus every checkout's `held` is `total`; no background grant takes the
/// class past the cap in force when it is made ([`Ledger::background_cap`]); a checkout is present exactly while it holds or waits; every
/// ticket in `tickets` is a holder or a waiter of the checkout it names; all gate holders in one
/// repository belong to one job, including holders in different checkouts.
#[derive(Debug)]
pub(crate) struct Ledger {
    total: usize,
    /// [`background_limit`] of `total`: background's cap beside gate-class work, and the later
    /// background grants that make a waiting background head overdue.
    background: usize,
    free: usize,
    checkouts: HashMap<Arc<str>, Checkout>,
    tickets: HashMap<Ticket, Arc<str>>,
}

impl Ledger {
    /// The host's ledger: background's cap is the whole host with no gate-class work and half of
    /// it beside gate-class work ([`background_cap`]).
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
    /// With no gate-class demand, background is fair between checkouts, within its room: the free
    /// tokens within the cap in force ([`Ledger::background_cap`]), every token when no gate-class
    /// work holds or waits. The next grant goes to the waiting
    /// checkout that holds the fewest tokens (the older head waiter on a tie), to its oldest
    /// waiter, except that an overdue head goes first ([`Ledger::overdue`]). A checkout's fair
    /// share is the cap in force divided among the checkouts holding or waiting; its claim
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

    /// Tokens background-class work may still take: the free ones, up to the cap in force
    /// ([`Ledger::background_cap`]). With no gate-class work that is every free token; beside it no
    /// grant takes the class past half the host, so a gate takes the tokens background returns.
    fn background_room(&self) -> usize {
        let held: usize = self
            .checkouts
            .values()
            .flat_map(|checkout| &checkout.running)
            .filter(|holder| holder.class == Class::Background)
            .map(|holder| holder.tokens)
            .sum();
        self.free.min(self.background_cap().saturating_sub(held))
    }

    /// Whether any gate-class request holds tokens or waits for them, a behind-head one included.
    fn gate_work(&self) -> GateWork {
        let gate = |class: &Class| class.repo().is_some();
        if self.checkouts.values().any(|checkout| {
            checkout.running.iter().any(|holder| gate(&holder.class))
                || checkout.waiting.iter().any(|waiter| gate(&waiter.class))
        }) {
            GateWork::HoldsOrWaits
        } else {
            GateWork::Absent
        }
    }

    /// Background's cap in force ([`background_cap`]): the whole host with no gate-class work, half
    /// of it beside gate-class work.
    fn background_cap(&self) -> usize {
        background_cap(self.total, self.gate_work())
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
        let holders = || {
            self.checkouts
                .values()
                .flat_map(|checkout| &checkout.running)
        };
        let behind: usize = holders()
            .filter(|holder| holder.class == Class::Background || holder.job > head)
            .map(|holder| holder.tokens)
            .sum();
        let job_holds = &|job: Ticket| {
            holders().any(|holder| holder.class.repo().is_some() && holder.job == job)
        };
        let starved = (2 * (behind + 1) < self.total)
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

    /// The next background grant by checkout fairness; `None` when nobody can be granted now. It
    /// takes only the background room: free tokens within the class's limit.
    fn next_background(&self) -> Option<Pick> {
        let room = self.background_room();
        if room == 0 {
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
        let share = (self.background_cap() / self.checkouts.len()).max(1);
        let least = head.want.min(share.saturating_sub(checkout.held).max(1));
        let peers_wait = self
            .checkouts
            .iter()
            .any(|(other, peer)| other != name && !peer.waiting.is_empty());
        if peers_wait && room < least {
            return None;
        }
        let tokens = if peers_wait {
            least
        } else {
            head.want.min(room)
        };
        Some(Pick {
            checkout: Arc::clone(name),
            position,
            tokens,
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
                    let (held, holders) = leases(holders, now);
                    CpuGateJob {
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
                    checkout: name.to_string(),
                    job: waiter.job,
                    pid: waiter.claim.pid,
                    command: waiter.claim.command.to_string(),
                    want: waiter.want,
                    waited_ms: millis_since(waiter.asked, now),
                })
                .collect(),
            background,
        }
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
    },
    Leave {
        ticket: Ticket,
        released: Instant,
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
    let mut cut: Option<CutOff> = None;
    let mut effects = Vec::new();
    while let Some(message) = messages.recv().await {
        let started = Instant::now();
        let transition = match &message {
            Message::Ask { ticket, .. } => Some(("ask", *ticket, None)),
            Message::Leave { ticket, released } => Some(("release", *ticket, Some(*released))),
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
                ledger.ask(
                    checkout,
                    Waiter {
                        ticket,
                        want: want.get(),
                        asked: Instant::now(),
                        passed: 0,
                        claim,
                        class,
                        job,
                    },
                    &mut effects,
                );
            }
            Message::Leave { ticket, .. } => {
                grants.remove(&ticket);
                ledger.leave(Instant::now(), ticket, &mut effects);
                if let Some(cut) = &mut cut {
                    cut.left(ticket);
                }
            }
            Message::Status { reply } => {
                let _ = reply.send(ledger.status(Instant::now()));
            }
            Message::CutOff { told } => {
                // Every sender still here is a queued ask: dropped, its waiter refuses.
                let waiting: std::collections::HashSet<Ticket> =
                    grants.drain().map(|(ticket, _)| ticket).collect();
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
        // Deliver all grants before diagnostics: stderr backpressure must not sit between the
        // grants of one transition, and the measured step excludes its own reporting.
        for grant in &effects {
            if let Some(granted) = grants.remove(&grant.ticket) {
                let _ = granted.send(grant.tokens);
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
        assert_eq!(clock.ask(0, 1, "shed", 4), [(1, 4)]);
        assert_eq!(clock.ask_land(1_000, 2, "land", 1), [(2, 1)]);
        assert_eq!(clock.ask(2_000, 3, "shed", 8), [(3, 4)]);
        let status = clock.status(3_000);
        assert_eq!(
            status.gate_jobs,
            [CpuGateJob {
                checkout: "land".into(),
                job: 2,
                held: 1,
                holders: vec![lease(2, 1, 1, 2_000)],
            }]
        );
        assert_eq!(
            status.background,
            [CpuBackgroundStake {
                checkout: "shed".into(),
                held: 8,
                waiters: Vec::new(),
                holders: vec![lease(1, 4, 4, 3_000), lease(3, 4, 8, 1_000)],
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
        assert_eq!(status.gate_jobs[0].holders, [lease(2, 1, 1, 8_000)]);
        assert_eq!(
            status.background,
            [CpuBackgroundStake {
                checkout: "shed".into(),
                held: 5,
                waiters: Vec::new(),
                holders: vec![lease(3, 4, 8, 7_000), lease(4, 1, 1, 4_000)],
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
    fn a_background_newcomer_waits_beside_gate_work_while_background_holds_its_half() {
        // Tokens are never handed back by a running job, so beside a land a background newcomer
        // whose class already holds its half queues for the runner's departure. Once no gate work
        // holds or waits, background's cap is the whole host again.
        let mut clock = Clock::new(36);
        assert_eq!(clock.ask_land(0, 1, "land", 1), [(1, 1)]);
        assert_eq!(clock.ask(0, 2, "carry", 18), [(2, 18)]);
        assert_eq!(clock.ask(1, 3, "train", 18), []);
        assert_eq!(clock.leave(2, 2), [(3, 18)]);
        assert_eq!(clock.ask(3, 4, "carry", 18), []);
        assert_eq!(clock.leave(4, 1), [(4, 18)]);
        assert_eq!(clock.status(4).held, 36);
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
        // A land in a's checkout caps background at half the host.
        assert_eq!(clock.ask_land(0, 1, "a", 1), [(1, 1)]);
        assert_eq!(clock.ask(0, 2, "a", 6), [(2, 6)]);
        // b's fair share is 4 of background's 9, but no peer waits: the 3 left must not sit idle.
        assert_eq!(clock.ask(1, 3, "b", 18), [(3, 3)]);
        assert_eq!(clock.status(1).held, 10);
        assert_eq!(clock.leave(2, 3), []);
        assert_eq!(clock.leave(3, 2), []);
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
    fn land_work_takes_every_returned_token_while_background_stays_queued() {
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
        // Beside the land, background's cap is half the host, which it still holds: a returned
        // token stays free for the land's next task.
        assert_eq!(clock.leave(6, 4), []);
        // Under its cap again, background is served by ordinary fairness: the checkout holding
        // least first.
        assert_eq!(clock.leave(7, 5), [(9, 1)]);
        // The land ends: background's cap is the whole host again.
        assert_eq!(clock.leave(8, 11), []);
        assert_eq!(clock.leave(8, 12), []);
        assert_eq!(clock.leave(8, 13), [(10, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn a_land_task_queued_behind_background_in_its_own_checkout_goes_first() {
        let mut clock = Clock::new(4);
        assert_eq!(clock.ask_land(0, 0, "other", 2), [(0, 2)]);
        assert_eq!(clock.ask(0, 1, "ws", 1), [(1, 1)]);
        assert_eq!(clock.ask(0, 2, "ws", 1), [(2, 1)]);
        assert_eq!(clock.ask(1, 3, "ws", 1), []);
        assert_eq!(clock.ask_land(2, 4, "ws", 1), []);
        // The land task is granted first; the next returned token is background's again.
        assert_eq!(clock.leave(3, 1), [(4, 1)]);
        assert_eq!(clock.leave(4, 2), [(3, 1)]);
        assert!(clock.balanced());
    }

    #[test]
    fn an_older_land_is_served_before_a_younger_land_and_both_before_a_feature_build() {
        let mut clock = Clock::new(8);
        // Two land gate jobs start: a's is older than b's.
        assert_eq!(clock.ask_land(0, 1, "a", 2), [(1, 2)]);
        assert_eq!(clock.ask_land(1, 2, "b", 2), [(2, 2)]);
        // A feature shed's builds hold background's half, and its next build waits first.
        assert_eq!(clock.ask(2, 3, "feature", 2), [(3, 2)]);
        assert_eq!(clock.ask(2, 4, "feature", 2), [(4, 2)]);
        assert_eq!(clock.ask(3, 5, "feature", 2), []);
        // Then the younger land's task asks, and the older land's task asks last of all.
        assert_eq!(clock.ask_task(4, 6, 2, "b", 2), []);
        assert_eq!(clock.ask_task(5, 7, 1, "a", 2), []);
        let queue: Vec<(String, u64)> = clock
            .status(5)
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
        /// gate-class request waits, background is granted only one floor token at a time, and only
        /// while it then holds less than half the host.
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
                    let gate_waits = live.values().any(|asked| asked.gate && asked.held.is_none());
                    let asked = live.get_mut(&granted).expect("a granted ticket is live");
                    asked.held = Some(tokens);
                    if !asked.gate && gate_waits {
                        let background: usize = live
                            .values()
                            .filter(|asked| !asked.gate)
                            .filter_map(|asked| asked.held)
                            .sum();
                        prop_assert!(
                            tokens == 1 && 2 * background < total,
                            "ticket {granted} was granted {tokens} while a gate waits; background holds {background} of {total}"
                        );
                    }
                }
                let free = clock.ledger.free;
                let waiting = || live.values().filter(|asked| asked.held.is_none());
                prop_assert!(
                    free == 0 || !waiting().any(|asked| asked.gate),
                    "a gate-class request waits beside {free} free tokens"
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
