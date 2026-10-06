//! The host disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease").
//!
//! Disk commands come in two classes ([`DiskClass`]) that exclude each other: a `storagekitd`
//! attach does not finish while the mount table keeps changing. Members of one class share the
//! running phase, up to a cap. Once a member of the other class waits, the running phase admits
//! nobody new; when it drains, the next phase goes to the waiting class, and its oldest waiters
//! enter it in arrival order. A holder that keeps a phase past the bound while the other class
//! waits is evicted from the phase and reported with the command it named, so one hung `umount`
//! cannot hold every attach on the host.
//!
//! [`Ledger`] is the whole policy as a pure state machine over explicit instants; [`DiskLeases`]
//! is the thin actor that feeds it requests, departures and deadlines.

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cowshed_gateway_types::{ConfigError, DiskClass};
use tokio::sync::{mpsc, oneshot};

/// How many holders one class's phase admits, and how long a holder may keep a phase the other
/// class is waiting for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiskLeaseLimits {
    /// Holders one phase admits at once. `storagekitd` answers one caller at a time, so more
    /// concurrent attaches only lengthen each one; eight keeps the queue full without that.
    pub per_class: NonZeroUsize,
    /// How long a holder may keep its phase once the other class waits. A leased attach takes
    /// 0.9 s at the median and 1.8 s at p95 under load 170–210, and a mount or unmount well under
    /// that, so a holder ten seconds in is stuck rather than slow.
    pub phase_bound: Duration,
}

impl Default for DiskLeaseLimits {
    fn default() -> Self {
        Self {
            per_class: NonZeroUsize::new(8).expect("8 is non-zero"),
            phase_bound: Duration::from_secs(10),
        }
    }
}

impl DiskLeaseLimits {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.phase_bound.is_zero() {
            return Err(ConfigError::ZeroTimeout);
        }
        Ok(())
    }
}

/// One request's identity for as long as it waits or holds.
pub(crate) type Ticket = u64;

/// What a lease is for, as the gateway reports a holder it evicts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Claim {
    /// The command line the client named, bounded to [`MAX_COMMAND`] bytes.
    pub command: Arc<str>,
    /// The client process, when the socket says.
    pub pid: Option<i32>,
}

/// The longest command description kept; a longer one is cut at a character boundary.
const MAX_COMMAND: usize = 512;

impl Claim {
    pub(crate) fn new(command: &str, pid: Option<i32>) -> Self {
        let mut end = command.len().min(MAX_COMMAND);
        while !command.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            command: Arc::from(&command[..end]),
            pid,
        }
    }
}

/// What a [`Ledger`] transition did, for the actor to carry out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Effect {
    /// The ticket may run its command now.
    Grant(Ticket),
    /// A holder kept its phase past the bound while `waiting` waited; it no longer counts, and
    /// its command may still be running beside the next phase.
    Evict {
        class: DiskClass,
        claim: Claim,
        held: Duration,
        waiting: DiskClass,
    },
    /// A phase drained: who ran in it, for how long, and how many of them it evicted.
    PhaseEnded {
        class: DiskClass,
        elapsed: Duration,
        members: usize,
        evicted: usize,
    },
}

#[derive(Debug)]
struct Holder {
    ticket: Ticket,
    granted: Instant,
    claim: Claim,
}

#[derive(Debug)]
struct Waiter {
    ticket: Ticket,
    class: DiskClass,
    claim: Claim,
}

#[derive(Debug)]
struct Phase {
    class: DiskClass,
    started: Instant,
    holders: Vec<Holder>,
    members: usize,
    evicted: usize,
}

/// The lease's state: the running phase, if any, and everyone waiting, oldest first.
///
/// Invariant: with no phase running, nobody waits — a request to an idle ledger starts a phase.
#[derive(Debug)]
pub(crate) struct Ledger {
    limits: DiskLeaseLimits,
    phase: Option<Phase>,
    waiting: VecDeque<Waiter>,
}

impl Ledger {
    pub(crate) fn new(limits: DiskLeaseLimits) -> Self {
        Self {
            limits,
            phase: None,
            waiting: VecDeque::new(),
        }
    }

    /// `ticket` asks for `class`. It enters the running phase at once when that phase is its own
    /// class, has room, and nobody waits; an idle ledger starts a phase for it. Everyone else
    /// waits in arrival order.
    pub(crate) fn ask(
        &mut self,
        now: Instant,
        ticket: Ticket,
        class: DiskClass,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        let cap = self.limits.per_class.get();
        match &mut self.phase {
            None => {
                let mut phase = Phase::new(class, now);
                phase.admit(ticket, now, claim, effects);
                self.phase = Some(phase);
            }
            Some(phase)
                if phase.class == class && phase.holders.len() < cap && self.waiting.is_empty() =>
            {
                phase.admit(ticket, now, claim, effects);
            }
            Some(_) => self.waiting.push_back(Waiter {
                ticket,
                class,
                claim,
            }),
        }
    }

    /// `ticket` is done: its holder leaves the phase, or its waiter the queue. A ticket the
    /// ledger no longer knows — an evicted holder closing at last — changes nothing.
    pub(crate) fn leave(&mut self, now: Instant, ticket: Ticket, effects: &mut Vec<Effect>) {
        if let Some(position) = self
            .waiting
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            self.waiting.remove(position);
        } else if let Some(phase) = &mut self.phase
            && let Some(position) = phase
                .holders
                .iter()
                .position(|holder| holder.ticket == ticket)
        {
            phase.holders.swap_remove(position);
        } else {
            return;
        }
        self.settle(now, effects);
    }

    /// Evict every holder past the bound while the other class waits, then hand the phase on if
    /// that drained it.
    pub(crate) fn expire(&mut self, now: Instant, effects: &mut Vec<Effect>) {
        let bound = self.limits.phase_bound;
        let Some(phase) = &mut self.phase else {
            return;
        };
        let waiting = phase.class.other();
        if !self.waiting.iter().any(|waiter| waiter.class == waiting) {
            return;
        }
        let class = phase.class;
        let mut index = 0;
        while index < phase.holders.len() {
            let held = now.saturating_duration_since(phase.holders[index].granted);
            if held >= bound {
                let holder = phase.holders.swap_remove(index);
                phase.evicted += 1;
                effects.push(Effect::Evict {
                    class,
                    claim: holder.claim,
                    held,
                    waiting,
                });
            } else {
                index += 1;
            }
        }
        self.settle(now, effects);
    }

    /// When [`Self::expire`] next has anyone to evict: the oldest holder's grant plus the bound,
    /// while the other class waits. `None` while nobody of the other class waits.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        let phase = self.phase.as_ref()?;
        let other = phase.class.other();
        if !self.waiting.iter().any(|waiter| waiter.class == other) {
            return None;
        }
        let oldest = phase.holders.iter().map(|holder| holder.granted).min()?;
        Some(oldest + self.limits.phase_bound)
    }

    /// Restore the invariants after someone left: a drained phase ends and hands over, and a
    /// phase nobody of the other class waits for refills from its own class's waiters.
    fn settle(&mut self, now: Instant, effects: &mut Vec<Effect>) {
        let cap = self.limits.per_class.get();
        let Some(phase) = &mut self.phase else {
            return;
        };
        if phase.holders.is_empty() {
            let ended = phase.class;
            effects.push(Effect::PhaseEnded {
                class: ended,
                elapsed: now.saturating_duration_since(phase.started),
                members: phase.members,
                evicted: phase.evicted,
            });
            self.phase = None;
            let next = if self.waiting.iter().any(|waiter| waiter.class != ended) {
                ended.other()
            } else if self.waiting.is_empty() {
                return;
            } else {
                ended
            };
            let mut phase = Phase::new(next, now);
            phase.admit_waiting(&mut self.waiting, cap, now, effects);
            self.phase = Some(phase);
        } else if !self
            .waiting
            .iter()
            .any(|waiter| waiter.class != phase.class)
        {
            phase.admit_waiting(&mut self.waiting, cap, now, effects);
        }
    }
}

impl Phase {
    fn new(class: DiskClass, now: Instant) -> Self {
        Self {
            class,
            started: now,
            holders: Vec::new(),
            members: 0,
            evicted: 0,
        }
    }

    fn admit(&mut self, ticket: Ticket, now: Instant, claim: Claim, effects: &mut Vec<Effect>) {
        self.holders.push(Holder {
            ticket,
            granted: now,
            claim,
        });
        self.members += 1;
        effects.push(Effect::Grant(ticket));
    }

    /// Move this phase's class's waiters into it, oldest first, until it is full.
    fn admit_waiting(
        &mut self,
        waiting: &mut VecDeque<Waiter>,
        cap: usize,
        now: Instant,
        effects: &mut Vec<Effect>,
    ) {
        let mut index = 0;
        while index < waiting.len() && self.holders.len() < cap {
            if waiting[index].class == self.class {
                let waiter = waiting.remove(index).expect("index is in bounds");
                self.admit(waiter.ticket, now, waiter.claim, effects);
            } else {
                index += 1;
            }
        }
    }
}

enum Message {
    Ask {
        ticket: Ticket,
        class: DiskClass,
        claim: Claim,
        granted: oneshot::Sender<()>,
    },
    Leave {
        ticket: Ticket,
    },
}

/// The gateway's lease scheduler: a handle to the actor that owns the [`Ledger`].
#[derive(Clone, Debug)]
pub(crate) struct DiskLeases {
    messages: mpsc::UnboundedSender<Message>,
    tickets: Arc<AtomicU64>,
}

/// One request's stake in the lease, waiting or holding. Dropping it leaves.
#[derive(Debug)]
pub(crate) struct Tenure {
    ticket: Ticket,
    messages: mpsc::UnboundedSender<Message>,
}

impl Drop for Tenure {
    fn drop(&mut self) {
        // An actor that is gone holds no lease to release.
        let _ = self.messages.send(Message::Leave {
            ticket: self.ticket,
        });
    }
}

impl DiskLeases {
    /// Start the scheduler. It runs until the last handle and tenure are gone.
    pub(crate) fn start(limits: DiskLeaseLimits) -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(Ledger::new(limits), receiver));
        Self {
            messages,
            tickets: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Ask for `class`. The receiver resolves once the lease is granted; the tenure holds the
    /// place in the queue or the phase until it is dropped. A receiver that errors means the
    /// scheduler is gone.
    pub(crate) fn ask(&self, class: DiskClass, claim: Claim) -> (Tenure, oneshot::Receiver<()>) {
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        let (granted, receiver) = oneshot::channel();
        let _ = self.messages.send(Message::Ask {
            ticket,
            class,
            claim,
            granted,
        });
        (
            Tenure {
                ticket,
                messages: self.messages.clone(),
            },
            receiver,
        )
    }
}

async fn run(mut ledger: Ledger, mut messages: mpsc::UnboundedReceiver<Message>) {
    let mut grants: HashMap<Ticket, oneshot::Sender<()>> = HashMap::new();
    let mut effects = Vec::new();
    loop {
        let deadline = ledger.deadline();
        let expiry = async {
            match deadline {
                Some(deadline) => {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            message = messages.recv() => match message {
                Some(Message::Ask { ticket, class, claim, granted }) => {
                    grants.insert(ticket, granted);
                    ledger.ask(Instant::now(), ticket, class, claim, &mut effects);
                }
                Some(Message::Leave { ticket }) => {
                    grants.remove(&ticket);
                    ledger.leave(Instant::now(), ticket, &mut effects);
                }
                None => return,
            },
            () = expiry => ledger.expire(Instant::now(), &mut effects),
        }
        for effect in effects.drain(..) {
            match effect {
                Effect::Grant(ticket) => {
                    // A client gone before its grant leaves next; nothing waits on the answer.
                    if let Some(granted) = grants.remove(&ticket) {
                        let _ = granted.send(());
                    }
                }
                Effect::Evict {
                    class,
                    claim,
                    held,
                    waiting,
                } => eprintln!(
                    "cowshed: disk-lease evicted a {class} holder after {held:?} while {waiting} \
                     waited; its command may still be running beside the {waiting} phase: pid {} \
                     `{}`",
                    claim
                        .pid
                        .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
                    claim.command,
                ),
                Effect::PhaseEnded {
                    class,
                    elapsed,
                    members,
                    evicted,
                } => cowshed_core::timing::event("disk-lease", || {
                    format!("phase {class} {elapsed:?} members={members} evicted={evicted}")
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DiskClass::{Namespace, Storage};

    fn limits(per_class: usize, bound_ms: u64) -> DiskLeaseLimits {
        DiskLeaseLimits {
            per_class: NonZeroUsize::new(per_class).unwrap(),
            phase_bound: Duration::from_millis(bound_ms),
        }
    }

    fn claim(command: &str) -> Claim {
        Claim::new(command, Some(7))
    }

    /// A ledger driven at explicit instants, collecting the tickets each step grants.
    struct Clock {
        ledger: Ledger,
        origin: Instant,
        effects: Vec<Effect>,
    }

    impl Clock {
        fn new(limits: DiskLeaseLimits) -> Self {
            Self {
                ledger: Ledger::new(limits),
                origin: Instant::now(),
                effects: Vec::new(),
            }
        }

        fn at(&self, ms: u64) -> Instant {
            self.origin + Duration::from_millis(ms)
        }

        fn ask(&mut self, ms: u64, ticket: Ticket, class: DiskClass) -> Vec<Ticket> {
            let now = self.at(ms);
            self.ledger.ask(
                now,
                ticket,
                class,
                claim(&format!("t{ticket}")),
                &mut self.effects,
            );
            self.grants()
        }

        fn leave(&mut self, ms: u64, ticket: Ticket) -> Vec<Ticket> {
            let now = self.at(ms);
            self.ledger.leave(now, ticket, &mut self.effects);
            self.grants()
        }

        fn expire(&mut self, ms: u64) -> Vec<Effect> {
            let now = self.at(ms);
            self.ledger.expire(now, &mut self.effects);
            std::mem::take(&mut self.effects)
        }

        fn grants(&mut self) -> Vec<Ticket> {
            self.effects
                .drain(..)
                .filter_map(|effect| match effect {
                    Effect::Grant(ticket) => Some(ticket),
                    _ => None,
                })
                .collect()
        }
    }

    #[test]
    fn one_class_shares_its_phase_up_to_the_cap_and_the_other_waits_for_it() {
        let mut clock = Clock::new(limits(2, 1_000));
        assert_eq!(clock.ask(0, 1, Storage), [1]);
        assert_eq!(clock.ask(0, 2, Storage), [2]);
        assert_eq!(
            clock.ask(0, 3, Storage),
            [] as [Ticket; 0],
            "the phase is full"
        );
        assert_eq!(
            clock.ask(0, 4, Namespace),
            [] as [Ticket; 0],
            "storage runs"
        );
        assert_eq!(
            clock.leave(5, 1),
            [] as [Ticket; 0],
            "namespace waits: no refill"
        );
        assert_eq!(
            clock.leave(6, 2),
            [4],
            "the drained phase goes to namespace"
        );
        assert_eq!(clock.leave(7, 4), [3], "and back to storage");
    }

    #[test]
    fn a_waiting_class_stops_new_entries_so_a_steady_stream_cannot_starve_it() {
        let mut clock = Clock::new(limits(8, 1_000));
        assert_eq!(clock.ask(0, 1, Storage), [1]);
        assert_eq!(clock.ask(1, 2, Namespace), [] as [Ticket; 0]);
        assert_eq!(
            clock.ask(2, 3, Storage),
            [] as [Ticket; 0],
            "room in the phase, but namespace waits"
        );
        assert_eq!(clock.leave(3, 1), [2]);
        assert_eq!(clock.leave(4, 2), [3]);
    }

    #[test]
    fn the_next_phase_takes_the_waiting_class_oldest_first_beside_older_waiters_of_the_other() {
        let mut clock = Clock::new(limits(2, 1_000));
        assert_eq!(clock.ask(0, 1, Storage), [1]);
        for (ticket, class) in [(2, Namespace), (3, Storage), (4, Namespace), (5, Namespace)] {
            assert_eq!(clock.ask(1, ticket, class), [] as [Ticket; 0]);
        }
        assert_eq!(
            clock.leave(2, 1),
            [2, 4],
            "namespace's two oldest fill its phase"
        );
        assert_eq!(
            clock.leave(3, 2),
            [] as [Ticket; 0],
            "storage 3 waits: no refill"
        );
        assert_eq!(
            clock.leave(4, 4),
            [3],
            "alternation: storage's turn before namespace 5"
        );
        assert_eq!(clock.leave(5, 3), [5]);
    }

    #[test]
    fn a_holder_past_the_bound_is_evicted_named_and_the_waiting_class_proceeds() {
        let mut clock = Clock::new(limits(8, 100));
        assert_eq!(clock.ask(0, 1, Namespace), [1]);
        assert_eq!(clock.ledger.deadline(), None, "nobody waits: no bound runs");
        assert_eq!(clock.ask(50, 2, Storage), [] as [Ticket; 0]);
        assert_eq!(clock.ledger.deadline(), Some(clock.at(100)));
        assert!(clock.expire(99).is_empty(), "inside the bound");
        let effects = clock.expire(100);
        assert_eq!(
            effects,
            [
                Effect::Evict {
                    class: Namespace,
                    claim: claim("t1"),
                    held: Duration::from_millis(100),
                    waiting: Storage,
                },
                Effect::PhaseEnded {
                    class: Namespace,
                    elapsed: Duration::from_millis(100),
                    members: 1,
                    evicted: 1,
                },
                Effect::Grant(2),
            ]
        );
        assert_eq!(
            clock.leave(500, 1),
            [] as [Ticket; 0],
            "the evicted holder's late close changes nothing"
        );
        assert_eq!(clock.ask(501, 3, Storage), [3], "storage still runs");
    }

    #[test]
    fn a_waiter_that_leaves_is_forgotten_and_stops_holding_the_phase_shut() {
        let mut clock = Clock::new(limits(1, 1_000));
        assert_eq!(clock.ask(0, 1, Storage), [1]);
        assert_eq!(clock.ask(1, 2, Namespace), [] as [Ticket; 0]);
        assert_eq!(clock.ask(2, 3, Storage), [] as [Ticket; 0]);
        assert_eq!(
            clock.leave(3, 2),
            [] as [Ticket; 0],
            "storage is still full"
        );
        assert_eq!(
            clock.ledger.deadline(),
            None,
            "nobody of the other class waits"
        );
        assert_eq!(clock.leave(4, 1), [3]);
        assert_eq!(clock.leave(5, 3), [] as [Ticket; 0]);
        assert_eq!(
            clock.ask(6, 4, Namespace),
            [4],
            "an idle ledger grants at once"
        );
    }

    #[test]
    fn a_claim_keeps_at_most_the_bounded_command_on_a_character_boundary() {
        let long = "é".repeat(MAX_COMMAND);
        let claim = Claim::new(&long, None);
        assert!(claim.command.len() <= MAX_COMMAND);
        assert!(long.starts_with(&*claim.command));
    }
}
