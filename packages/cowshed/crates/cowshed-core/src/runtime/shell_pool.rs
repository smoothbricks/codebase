//! The warm exec hosts of one workspace shell identity.
//!
//! The shape follows the `ShellPool` actor of jcode (https://github.com/1jehuang/jcode, MIT
//! License): a single owner of all pool state, no lock, a fire-and-forget release from a guard's
//! `Drop`. No jcode source is copied; the policy is cowshed's own:
//!
//! - **One prewarmed spare.** When a command takes it, the next is activated in the
//!   background, so a burst of commands never waits on activation twice in a row.
//! - **A SIEVE cache of hosts that are executing or were just returned.** A returned host is
//!   the cheapest shell there is. The cache is bounded; when an insertion finds it full, the
//!   SIEVE hand evicts the first entry not reused since the hand last passed (an idle host is
//!   dropped, an executing one is simply not returned). jcode's floor/ceiling/idle-TTL and FIFO
//!   waiters do not apply: commands never queue for a shell, they activate one.
//! - **Generations.** Every host belongs to the generation of inputs it was activated from
//!   (`shell_watch`). A kernel event on any input only sets the dirty flag. The next acquire —
//!   never the event — retires every idle host and the spare and activates for itself; a host
//!   checked out when the flag rose is dropped instead of returned. A host whose own
//!   activation saw its inputs move runs its command and is never reused.
//!
//! Activation is the caller's: a command that finds no warm host activates on its own task,
//! with its own stdout/stderr receiving the activation output, and the actor never waits for
//! an activation.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::shell_watch::{ActivationEvidence, Contents, Snapshot, WatchEntry, Watcher, certify};
use crate::error::{CowshedError, Result};

/// Retained hosts per pool, executing and idle together.
pub const DEFAULT_SHELL_CACHE: NonZeroUsize = NonZeroUsize::new(4).expect("non-zero");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShellPoolConfig {
    /// SIEVE cache capacity, counting executing and idle hosts.
    pub capacity: NonZeroUsize,
    /// Keep one activated spare ahead of demand.
    pub prewarm: bool,
}

impl Default for ShellPoolConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SHELL_CACHE,
            prewarm: true,
        }
    }
}

/// How a pool gets a newly activated host. Implemented by the supervisor's real exec hosts and
/// by test doubles.
#[async_trait]
pub trait Activator: Send + Sync + 'static {
    type Host: Send + 'static;
    type Output: Send + 'static;

    /// Start a host and activate it. `predicted` are the previous generation's inputs, whose
    /// identities the activation snapshots just before evaluating.
    async fn activate(
        &self,
        predicted: Vec<PathBuf>,
        output: Option<Self::Output>,
    ) -> Activation<Self::Host>;
}

/// The outcome of one activation.
pub enum Activation<H> {
    /// The environment is loaded. Without freshness evidence the host serves only the command
    /// that activated it.
    Ready {
        host: H,
        evidence: std::result::Result<ActivationEvidence, String>,
    },
    /// Activation ran and failed with this raw wait status; the host is gone.
    Failed { status: i32 },
    /// No host could be started or it broke its protocol.
    Broken { error: CowshedError },
}

type SlotId = u64;

struct Slot<H> {
    id: SlotId,
    /// The generation whose inputs activated this host, or `None` once it may not return.
    generation: Option<u64>,
    /// `Some` while idle in the cache, `None` while executing.
    host: Option<H>,
    visited: bool,
}

/// SIEVE (Zhang et al., NSDI '24) over slots, oldest first.
struct Sieve<H> {
    slots: VecDeque<Slot<H>>,
    hand: usize,
    capacity: usize,
}

impl<H> Sieve<H> {
    fn insert(&mut self, slot: Slot<H>) {
        while self.slots.len() >= self.capacity {
            self.evict();
        }
        self.slots.push_back(slot);
    }

    fn evict(&mut self) {
        loop {
            if self.hand >= self.slots.len() {
                self.hand = 0;
            }
            let slot = &mut self.slots[self.hand];
            if slot.visited {
                slot.visited = false;
                self.hand += 1;
            } else {
                // The hand now rests on the entry after the evicted one.
                self.slots.remove(self.hand);
                return;
            }
        }
    }

    fn position(&self, id: SlotId) -> Option<usize> {
        self.slots.iter().position(|slot| slot.id == id)
    }

    fn remove(&mut self, id: SlotId) {
        if let Some(position) = self.position(id) {
            self.slots.remove(position);
            if position < self.hand {
                self.hand -= 1;
            }
        }
    }

    /// Take the most recently returned idle host of `generation`.
    fn take_idle(&mut self, generation: u64) -> Option<(SlotId, H)> {
        self.slots.iter_mut().rev().find_map(|slot| {
            if slot.generation != Some(generation) {
                return None;
            }
            let host = slot.host.take()?;
            slot.visited = true;
            Some((slot.id, host))
        })
    }

    /// Drop every idle host not of `keep`, and bar every executing host not of `keep` from
    /// returning.
    fn retire_except(&mut self, keep: Option<u64>) {
        self.slots.retain_mut(|slot| {
            if slot.generation.is_some() && slot.generation == keep {
                return true;
            }
            slot.generation = None;
            slot.host.is_none()
        });
        self.hand = self.hand.min(self.slots.len());
    }
}

/// The inputs the current generation was activated from, and their subscription: the pool core
/// knows the generation by its number alone.
struct Generation {
    baseline: Snapshot,
    /// The inputs' bytes certified with `baseline`; `None` when they are not known, and then
    /// any signal makes the generation stale.
    contents: Option<Contents>,
    entries: Vec<WatchEntry>,
    watcher: Watcher,
}

enum Request<H> {
    Acquire {
        reply: oneshot::Sender<Lease<H>>,
    },
    Activated {
        slot: SlotId,
        epoch: u64,
        evidence: std::result::Result<ActivationEvidence, String>,
        reply: oneshot::Sender<bool>,
    },
    Release {
        slot: SlotId,
        host: Option<H>,
        healthy: bool,
    },
    Warmed {
        epoch: u64,
        activation: Activation<H>,
    },
    Ready {
        reply: oneshot::Sender<bool>,
    },
    #[cfg(test)]
    Inspect {
        reply: oneshot::Sender<PoolInspection>,
    },
}

/// What an acquire hands back.
pub enum Lease<H> {
    /// A host of the current generation, ready to run.
    Warm { slot: SlotId, host: H },
    /// No warm host: the caller activates one, then reports it with [`Ticket::activated`].
    Activate {
        slot: SlotId,
        predicted: Vec<PathBuf>,
        epoch: u64,
    },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoolInspection {
    pub idle: usize,
    pub executing: usize,
    pub spare: bool,
    pub warming: bool,
    pub dirty: bool,
    pub generation: Option<u64>,
}

/// How a supervisor's commands got their warm hosts (11_shell.md, "One spare and a SIEVE
/// cache"): an idle host of the current generation, the spare, or a host the command activated
/// for itself. A caller proves which path its commands took from these, not from their timing.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PoolCounts {
    /// Acquires an idle host of the current generation answered.
    pub idle: u64,
    /// Acquires the spare answered.
    pub spare: u64,
    /// Acquires that found no warm host and activated one of their own.
    pub activated: u64,
    /// Shell identities a pool was started for. Past one per identity, a changed identity (a
    /// profile, environment, grant revision or host program) replaced a pool and its hosts.
    pub pools: u64,
}

impl std::ops::Add for PoolCounts {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            idle: self.idle + other.idle,
            spare: self.spare + other.spare,
            activated: self.activated + other.activated,
            pools: self.pools + other.pools,
        }
    }
}

/// The pool actor's tallies, which only it writes.
#[derive(Default)]
struct Tallies {
    idle: AtomicU64,
    spare: AtomicU64,
    activated: AtomicU64,
}

impl Tallies {
    /// Count one acquire answered `how`, and say so on the latency waterfall.
    fn count(&self, tally: &AtomicU64, how: &'static str) {
        tally.fetch_add(1, Ordering::Relaxed);
        cowshed_os::timing::event("pool", || format!("acquire: {how}"));
    }
}

/// Cheap to clone: one sender to the actor.
pub struct ShellPool<A: Activator> {
    requests: mpsc::UnboundedSender<Request<A::Host>>,
    activator: Arc<A>,
    tallies: Arc<Tallies>,
}

impl<A: Activator> Clone for ShellPool<A> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            activator: Arc::clone(&self.activator),
            tallies: Arc::clone(&self.tallies),
        }
    }
}

fn pool_stopped() -> CowshedError {
    CowshedError::internal("the workspace shell pool stopped")
}

impl<A: Activator> ShellPool<A> {
    pub fn start(activator: Arc<A>, config: ShellPoolConfig) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        let tallies = Arc::new(Tallies::default());
        let actor = PoolActor {
            activator: Arc::clone(&activator),
            requests: requests.downgrade(),
            core: PoolCore::new(config),
            generation: None,
            tallies: Arc::clone(&tallies),
        };
        tokio::spawn(actor.run(receiver));
        Self {
            requests,
            activator,
            tallies,
        }
    }

    /// How this pool's acquires were answered so far.
    pub fn counts(&self) -> PoolCounts {
        PoolCounts {
            idle: self.tallies.idle.load(Ordering::Relaxed),
            spare: self.tallies.spare.load(Ordering::Relaxed),
            activated: self.tallies.activated.load(Ordering::Relaxed),
            pools: 1,
        }
    }

    /// Get a warm host or a ticket to activate one; never waits for an activation.
    pub async fn acquire(&self) -> Result<Acquired<A>> {
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(Request::Acquire { reply })
            .map_err(|_| pool_stopped())?;
        Ok(match receive.await.map_err(|_| pool_stopped())? {
            Lease::Warm { slot, host } => Acquired::Warm(self.checkout(slot, host, true)),
            Lease::Activate {
                slot,
                predicted,
                epoch,
            } => Acquired::Activate(Ticket {
                pool: self.clone(),
                slot: Some(slot),
                predicted,
                epoch,
            }),
        })
    }

    /// Whether an acquire now would hand back a warm host: an idle one of the current
    /// generation, or the spare. It takes and reserves nothing, so a command that acquires later
    /// may still find none.
    pub async fn ready(&self) -> Result<bool> {
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(Request::Ready { reply })
            .map_err(|_| pool_stopped())?;
        receive.await.map_err(|_| pool_stopped())
    }

    fn checkout(&self, slot: SlotId, host: A::Host, reusable: bool) -> Checkout<A> {
        Checkout {
            requests: self.requests.clone(),
            slot,
            host: Some(host),
            healthy: reusable,
        }
    }

    #[cfg(test)]
    pub(crate) async fn inspect(&self) -> PoolInspection {
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(Request::Inspect { reply })
            .expect("pool actor");
        receive.await.expect("pool inspection")
    }
}

pub enum Acquired<A: Activator> {
    Warm(Checkout<A>),
    Activate(Ticket<A>),
}

/// The right to activate one host for the pool. Dropping it unused frees its cache slot.
pub struct Ticket<A: Activator> {
    pool: ShellPool<A>,
    slot: Option<SlotId>,
    predicted: Vec<PathBuf>,
    epoch: u64,
}

impl<A: Activator> Ticket<A> {
    pub fn activator(&self) -> &A {
        &self.pool.activator
    }

    pub fn predicted(&self) -> Vec<PathBuf> {
        self.predicted.clone()
    }

    /// Hand the pool the evidence of the activation this ticket paid for; returns the checkout
    /// for its host, reusable only if the evidence proved its inputs stable.
    pub async fn activated(
        mut self,
        host: A::Host,
        evidence: std::result::Result<ActivationEvidence, String>,
    ) -> Checkout<A> {
        let slot = self
            .slot
            .take()
            .expect("an unreported ticket owns its slot");
        let (reply, receive) = oneshot::channel();
        let sent = self.pool.requests.send(Request::Activated {
            slot,
            epoch: self.epoch,
            evidence,
            reply,
        });
        let reusable = sent.is_ok() && receive.await.unwrap_or(false);
        self.pool.checkout(slot, host, reusable)
    }
}

impl<A: Activator> Drop for Ticket<A> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let _ = self.pool.requests.send(Request::Release {
                slot,
                host: None,
                healthy: false,
            });
        }
    }
}

/// One host checked out for one command. Returned to the pool on drop unless poisoned.
pub struct Checkout<A: Activator> {
    requests: mpsc::UnboundedSender<Request<A::Host>>,
    slot: SlotId,
    host: Option<A::Host>,
    healthy: bool,
}

impl<A: Activator> Checkout<A> {
    pub fn host_mut(&mut self) -> &mut A::Host {
        self.host.as_mut().expect("host present until drop")
    }

    /// Never return this host: its process or protocol state is no longer trustworthy.
    pub fn poison(&mut self) {
        self.healthy = false;
    }
}

impl<A: Activator> Drop for Checkout<A> {
    fn drop(&mut self) {
        // Fire and forget: if the actor is gone the host drops here, which ends its process.
        let _ = self.requests.send(Request::Release {
            slot: self.slot,
            host: self.host.take(),
            healthy: self.healthy,
        });
    }
}

struct Spare<H> {
    host: H,
    generation: u64,
}

/// What an acquire is granted ([`PoolCore::acquire`]).
enum Grant<H> {
    /// An idle host of the current generation.
    Idle { slot: SlotId, host: H },
    /// The spare; `warm` is the epoch its replacement activates under, when one must start.
    Spare {
        slot: SlotId,
        host: H,
        warm: Option<u64>,
    },
    /// No warm host: the caller activates one into `slot`. `stale` when no generation was current
    /// and every idle host and the spare were retired for it.
    Activate {
        slot: SlotId,
        epoch: u64,
        stale: bool,
    },
}

/// What installing an activation's evidence takes ([`PoolCore::install`]).
enum Install {
    /// The evidence is the current generation's: its host returns to it.
    Current(u64),
    /// A generation of its own, once its inputs are subscribed ([`PoolCore::generation`]).
    Subscribe { stable: bool },
}

/// The pool's decisions over its own state (11_shell.md, "One spare and a SIEVE cache"). Every
/// fact the actor observes arrives as an argument and every effect leaves as a return value: a
/// grant, a spare to start, a host dropped (a host ends when it drops). It reads no file, starts
/// no task and reads no clock, so random event sequences check it against a model (`tests`).
struct PoolCore<H> {
    prewarm: bool,
    sieve: Sieve<H>,
    /// The generation of the inputs hosts were last activated from.
    generation: Option<u64>,
    next_generation: u64,
    next_slot: SlotId,
    dirty: bool,
    /// Counts observed input changes; an activation compares it across its own run.
    epoch: u64,
    spare: Option<Spare<H>>,
    warming: bool,
}

impl<H> PoolCore<H> {
    fn new(config: ShellPoolConfig) -> Self {
        Self {
            prewarm: config.prewarm,
            sieve: Sieve {
                slots: VecDeque::new(),
                hand: 0,
                capacity: config.capacity.get(),
            },
            generation: None,
            next_generation: 1,
            next_slot: 1,
            dirty: false,
            epoch: 0,
            spare: None,
            warming: false,
        }
    }

    /// The inputs moved: their bytes differ from the generation's, or cannot be certified.
    fn changed(&mut self) {
        self.epoch += 1;
        self.dirty = true;
    }

    fn current(&self) -> Option<u64> {
        if self.dirty {
            return None;
        }
        self.generation
    }

    fn new_slot(&mut self, generation: Option<u64>) -> SlotId {
        let id = self.next_slot;
        self.next_slot += 1;
        self.sieve.insert(Slot {
            id,
            generation,
            host: None,
            visited: false,
        });
        id
    }

    /// The most recently returned idle host of the current generation, else the spare, else a
    /// slot to activate into. With no generation current, every idle host and the spare retire.
    fn acquire(&mut self) -> Grant<H> {
        let Some(current) = self.current() else {
            self.sieve.retire_except(None);
            self.spare = None;
            return Grant::Activate {
                slot: self.new_slot(None),
                epoch: self.epoch,
                stale: true,
            };
        };
        if let Some((slot, host)) = self.sieve.take_idle(current) {
            return Grant::Idle { slot, host };
        }
        if let Some(spare) = self.spare.take_if(|spare| spare.generation == current) {
            let slot = self.new_slot(Some(current));
            return Grant::Spare {
                slot,
                host: spare.host,
                warm: self.warm(),
            };
        }
        Grant::Activate {
            slot: self.new_slot(None),
            epoch: self.epoch,
            stale: false,
        }
    }

    /// What [`Self::acquire`] would grant warm, deciding nothing.
    fn ready(&self) -> bool {
        let Some(current) = self.current() else {
            return false;
        };
        self.sieve
            .slots
            .iter()
            .any(|slot| slot.generation == Some(current) && slot.host.is_some())
            || self
                .spare
                .as_ref()
                .is_some_and(|spare| spare.generation == current)
    }

    /// How an activation that started at `epoch` installs: `stable` that its inputs held still
    /// while it ran, `same` that they are the current generation's.
    fn install(&self, epoch: u64, stable: bool, same: bool) -> Install {
        let stable = stable && epoch == self.epoch;
        match self.current() {
            Some(current) if stable && same => Install::Current(current),
            _ => Install::Subscribe { stable },
        }
    }

    /// A new generation of an activation's inputs, `subscribed` or not; the generation its host
    /// may return to. An unstable activation still names the inputs to predict next time, and
    /// its flag makes the next acquire activate again; inputs that cannot be subscribed leave
    /// every shell to serve one command.
    fn generation(&mut self, stable: bool, subscribed: bool) -> Option<u64> {
        if !subscribed {
            self.dirty = true;
            return None;
        }
        let id = self.next_generation;
        self.next_generation += 1;
        self.generation = Some(id);
        self.dirty = !stable;
        self.sieve.retire_except(stable.then_some(id));
        self.spare = None;
        stable.then_some(id)
    }

    /// The host activated into `slot` belongs to `generation`: whether it may return, and the
    /// epoch a spare starts under when one must.
    fn activated(&mut self, slot: SlotId, generation: Option<u64>) -> (bool, Option<u64>) {
        let Some(position) = self.sieve.position(slot) else {
            return (false, None);
        };
        self.sieve.slots[position].generation = generation;
        match generation {
            Some(_) => (true, self.warm()),
            None => (false, None),
        }
    }

    /// A host back from its command: kept idle when healthy and of the current generation,
    /// dropped otherwise.
    fn release(&mut self, slot: SlotId, host: Option<H>, healthy: bool) {
        let current = self.current();
        let Some(position) = self.sieve.position(slot) else {
            return;
        };
        let entry = &mut self.sieve.slots[position];
        match host {
            Some(host) if healthy && entry.generation.is_some() && entry.generation == current => {
                entry.host = Some(host);
            }
            _ => self.sieve.remove(slot),
        }
    }

    /// The epoch a spare starts activating under, when none exists or is on its way.
    fn warm(&mut self) -> Option<u64> {
        if !self.prewarm || self.spare.is_some() || self.warming || self.current().is_none() {
            return None;
        }
        self.warming = true;
        Some(self.epoch)
    }

    /// The spare's activation produced `host` of `generation`: it is the spare while that
    /// generation is current and no other spare came first.
    fn warmed(&mut self, host: H, generation: Option<u64>) {
        self.warming = false;
        if let Some(generation) = generation
            && self.current() == Some(generation)
            && self.spare.is_none()
        {
            self.spare = Some(Spare { host, generation });
        }
    }

    /// The spare's activation produced no usable host: there is no spare.
    fn warm_failed(&mut self) {
        self.warming = false;
    }
}

/// The pool's I/O: it reads the inputs' kernel events and identities, subscribes and certifies
/// them, starts spare activations and publishes tallies; [`PoolCore`] decides.
struct PoolActor<A: Activator> {
    activator: Arc<A>,
    /// Weak, so the actor ends when the last handle, ticket and checkout are gone.
    requests: mpsc::WeakUnboundedSender<Request<A::Host>>,
    core: PoolCore<A::Host>,
    generation: Option<Generation>,
    tallies: Arc<Tallies>,
}

impl<A: Activator> PoolActor<A> {
    async fn run(mut self, mut receiver: mpsc::UnboundedReceiver<Request<A::Host>>) {
        while let Some(request) = receiver.recv().await {
            match request {
                Request::Acquire { reply } => {
                    let lease = self.acquire();
                    if let Err(Lease::Warm { slot, host }) = reply.send(lease) {
                        // The caller went away: the host goes back as if released.
                        self.release(slot, Some(host), true);
                    }
                }
                Request::Activated {
                    slot,
                    epoch,
                    evidence,
                    reply,
                } => {
                    let reusable = self.activated(slot, epoch, evidence);
                    let _ = reply.send(reusable);
                }
                Request::Release {
                    slot,
                    host,
                    healthy,
                } => self.release(slot, host, healthy),
                Request::Warmed { epoch, activation } => self.warmed(epoch, activation),
                Request::Ready { reply } => {
                    self.refresh(true);
                    let _ = reply.send(self.core.ready());
                }
                #[cfg(test)]
                Request::Inspect { reply } => {
                    let slots = &self.core.sieve.slots;
                    let _ = reply.send(PoolInspection {
                        idle: slots.iter().filter(|slot| slot.host.is_some()).count(),
                        executing: slots.iter().filter(|slot| slot.host.is_none()).count(),
                        spare: self.core.spare.is_some(),
                        warming: self.core.warming,
                        dirty: self.core.dirty,
                        generation: self.core.generation,
                    });
                }
            }
        }
    }

    /// Fold kernel events, and at acquire the stat backstop, into the core. Either only says an
    /// input may have changed; its bytes decide (11_shell.md, "Freshness is direnv's own"). Bytes
    /// equal to the ones the generation certified mean the rewrite changed nothing the shell
    /// read: the generation takes the new identities, resubscribes (a replaced file is a new
    /// vnode) and stays warm.
    fn refresh(&mut self, backstop: bool) {
        let Some(generation) = &mut self.generation else {
            return;
        };
        let signalled = match generation.watcher.drain() {
            Ok(changed) => changed,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: workspace shell input subscription failed ({error}); treating the \
                 warm shells as stale"
                );
                generation.contents = None;
                true
            }
        };
        if !signalled && !(backstop && generation.baseline.changed_since()) {
            return;
        }
        if let Some(contents) = &generation.contents
            && let Ok((after, now)) = certify(generation.baseline.paths())
            && now == *contents
            && let Ok(watcher) = Watcher::subscribe(&generation.entries)
        {
            generation.baseline = after;
            generation.watcher = watcher;
            return;
        }
        self.core.changed();
    }

    /// The current generation's inputs, which an activation snapshots just before evaluating.
    fn predicted(&self) -> Vec<PathBuf> {
        self.generation
            .as_ref()
            .map(|generation| generation.baseline.paths().map(PathBuf::from).collect())
            .unwrap_or_default()
    }

    fn acquire(&mut self) -> Lease<A::Host> {
        self.refresh(true);
        match self.core.acquire() {
            Grant::Idle { slot, host } => {
                self.tallies.count(&self.tallies.idle, "idle host");
                Lease::Warm { slot, host }
            }
            Grant::Spare { slot, host, warm } => {
                self.tallies.count(&self.tallies.spare, "spare");
                self.warm(warm);
                Lease::Warm { slot, host }
            }
            Grant::Activate { slot, epoch, stale } => {
                let why = if stale {
                    "activate (no current generation)"
                } else {
                    "activate (no idle host or spare)"
                };
                self.tallies.count(&self.tallies.activated, why);
                Lease::Activate {
                    slot,
                    predicted: self.predicted(),
                    epoch,
                }
            }
        }
    }

    /// Install what an activation observed; returns the generation its host may return to.
    fn install(&mut self, epoch: u64, evidence: ActivationEvidence) -> Option<u64> {
        self.refresh(false);
        let same = self
            .generation
            .as_ref()
            .is_some_and(|generation| generation.baseline.same_as(&evidence.after));
        let stable = match self.core.install(epoch, evidence.stable(), same) {
            Install::Current(current) => return Some(current),
            Install::Subscribe { stable } => stable,
        };
        match Watcher::subscribe(&evidence.entries) {
            Ok(watcher) => {
                let generation = self.core.generation(stable, true);
                self.generation = Some(Generation {
                    baseline: evidence.after,
                    contents: evidence.contents,
                    entries: evidence.entries,
                    watcher,
                });
                generation
            }
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: cannot subscribe the workspace shell's inputs ({error}); its \
                 shells serve one command each"
                );
                self.core.generation(stable, false)
            }
        }
    }

    fn activated(
        &mut self,
        slot: SlotId,
        epoch: u64,
        evidence: std::result::Result<ActivationEvidence, String>,
    ) -> bool {
        let generation = match evidence {
            Ok(evidence) => self.install(epoch, evidence),
            Err(reason) => {
                cowshed_os::eprintln!(
                    "cowshed: the workspace shell's inputs are unknown ({reason}); this shell \
                 serves one command"
                );
                None
            }
        };
        let (reusable, warm) = self.core.activated(slot, generation);
        self.warm(warm);
        reusable
    }

    fn release(&mut self, slot: SlotId, host: Option<A::Host>, healthy: bool) {
        self.refresh(false);
        self.core.release(slot, host, healthy);
    }

    /// Start the spare activation the core asked for, under its epoch.
    fn warm(&mut self, epoch: Option<u64>) {
        let (Some(epoch), Some(requests)) = (epoch, self.requests.upgrade()) else {
            return;
        };
        let predicted = self.predicted();
        let activator = Arc::clone(&self.activator);
        tokio::spawn(async move {
            let activation = activator.activate(predicted, None).await;
            let _ = requests.send(Request::Warmed { epoch, activation });
        });
    }

    fn warmed(&mut self, epoch: u64, activation: Activation<A::Host>) {
        let Activation::Ready {
            host,
            evidence: Ok(evidence),
        } = activation
        else {
            self.core.warm_failed();
            return;
        };
        let generation = self.install(epoch, evidence);
        self.core.warmed(host, generation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::shell_watch::{EvaluationClock, FsInstant, FsSeparation, WatchEntry};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Hosts are numbers; each activation records the fixture's input files.
    struct Fixture {
        root: PathBuf,
        inputs: Vec<PathBuf>,
        activations: AtomicU64,
        /// While set, activations touch the first input mid-evaluation.
        disturb: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl Activator for Fixture {
        type Host = u64;
        type Output = ();

        async fn activate(&self, predicted: Vec<PathBuf>, _output: Option<()>) -> Activation<u64> {
            let host = self.activations.fetch_add(1, Ordering::SeqCst) + 1;
            let before = Snapshot::take(predicted.iter().map(PathBuf::as_path));
            let started = FsSeparation::take(&self.root).unwrap();
            if self.disturb.load(Ordering::SeqCst) {
                std::fs::write(&self.inputs[0], host.to_string()).unwrap();
            }
            let entries: Vec<WatchEntry> = self
                .inputs
                .iter()
                .map(|path| WatchEntry {
                    path: path.clone(),
                    exists: path.exists(),
                })
                .collect();
            let (after, contents) = ActivationEvidence::observe(&entries);
            let finished = FsInstant::read(&self.root).unwrap();
            Activation::Ready {
                host,
                evidence: Ok(ActivationEvidence {
                    entries,
                    before,
                    clock: Some(EvaluationClock { started, finished }),
                    after,
                    contents,
                }),
            }
        }
    }

    fn fixture(label: &str) -> (Arc<Fixture>, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "cowshed-shell-pool-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let inputs = vec![root.join(".envrc"), root.join("bun.lock")];
        for input in &inputs {
            std::fs::write(input, b"initial").unwrap();
        }
        (
            Arc::new(Fixture {
                root: root.clone(),
                inputs,
                activations: AtomicU64::new(0),
                disturb: std::sync::atomic::AtomicBool::new(false),
            }),
            root,
        )
    }

    /// Run one command's worth of pool traffic: acquire, activate if told to, release.
    async fn command(pool: &ShellPool<Fixture>) -> (u64, bool) {
        match pool.acquire().await.unwrap() {
            Acquired::Warm(mut checkout) => (*checkout.host_mut(), false),
            Acquired::Activate(ticket) => {
                let Activation::Ready { host, evidence } =
                    ticket.activator().activate(ticket.predicted(), None).await
                else {
                    unreachable!("the fixture always activates");
                };
                let mut checkout = ticket.activated(host, evidence).await;
                (*checkout.host_mut(), true)
            }
        }
    }

    async fn settle(pool: &ShellPool<Fixture>) -> PoolInspection {
        loop {
            let inspection = pool.inspect().await;
            if !inspection.warming {
                return inspection;
            }
            tokio::task::yield_now().await;
        }
    }

    fn unwarmed() -> ShellPoolConfig {
        ShellPoolConfig {
            capacity: DEFAULT_SHELL_CACHE,
            prewarm: false,
        }
    }

    #[tokio::test]
    async fn a_returned_host_serves_the_next_command_without_activating() {
        let (fixture, root) = fixture("reuse");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        assert_eq!(command(&pool).await, (1, true));
        assert_eq!(command(&pool).await, (1, false));
        assert_eq!(command(&pool).await, (1, false));
        assert_eq!(fixture.activations.load(Ordering::SeqCst), 1);
        assert_eq!(
            pool.counts(),
            PoolCounts {
                idle: 2,
                spare: 0,
                activated: 1,
                pools: 1,
            },
            "two of three commands reached an idle host"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Readiness is what the next acquire decides, and asking it takes nothing: a cold pool is
    /// not ready, a returned host of the current generation is, and a changed input retires it.
    #[tokio::test]
    async fn a_pool_is_ready_exactly_when_its_next_acquire_is_warm() {
        let (fixture, root) = fixture("ready");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        assert!(!pool.ready().await.unwrap(), "a cold pool activates");
        command(&pool).await;
        assert!(pool.ready().await.unwrap(), "host 1 waits idle");
        assert!(pool.ready().await.unwrap(), "asking took nothing");
        assert_eq!(command(&pool).await, (1, false));
        std::fs::write(&fixture.inputs[0], b"edited").unwrap();
        assert!(
            !pool.ready().await.unwrap(),
            "a changed input retires host 1"
        );
        assert_eq!(command(&pool).await, (2, true));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn two_changed_inputs_before_one_command_cost_one_activation() {
        let (fixture, root) = fixture("coalesce");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        command(&pool).await;
        std::fs::write(&fixture.inputs[0], b"edited").unwrap();
        std::fs::write(&fixture.inputs[1], b"edited").unwrap();
        assert_eq!(command(&pool).await, (2, true));
        assert_eq!(command(&pool).await, (2, false));
        assert_eq!(fixture.activations.load(Ordering::SeqCst), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn an_input_changed_during_activation_is_used_once_then_activated_again() {
        let (fixture, root) = fixture("mid-activation");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        fixture.disturb.store(true, Ordering::SeqCst);
        assert_eq!(command(&pool).await, (1, true));
        assert!(pool.inspect().await.dirty);
        fixture.disturb.store(false, Ordering::SeqCst);
        assert_eq!(command(&pool).await, (2, true), "host 1 was never reusable");
        assert_eq!(command(&pool).await, (2, false));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn an_unwatched_change_costs_nothing() {
        let (fixture, root) = fixture("unwatched");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        command(&pool).await;
        std::fs::write(root.join("README.md"), b"unrelated").unwrap();
        assert_eq!(command(&pool).await, (1, false));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_rewrite_of_the_same_bytes_keeps_the_shell_warm() {
        let (fixture, root) = fixture("same-bytes");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        command(&pool).await;
        // A new file under the same name (a checkout, an editor's save): a new identity, the
        // same bytes. It moves nothing the shell read.
        std::fs::remove_file(&fixture.inputs[0]).unwrap();
        std::fs::write(&fixture.inputs[0], b"initial").unwrap();
        assert_eq!(command(&pool).await, (1, false));
        // The generation took the new identity and resubscribed: a real edit still lands.
        std::fs::write(&fixture.inputs[0], b"edited").unwrap();
        assert_eq!(command(&pool).await, (2, true));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_host_out_when_its_inputs_change_is_dropped_on_return() {
        let (fixture, root) = fixture("checked-out");
        let pool = ShellPool::start(Arc::clone(&fixture), unwarmed());
        command(&pool).await;
        let Acquired::Warm(checkout) = pool.acquire().await.unwrap() else {
            panic!("host 1 is idle");
        };
        std::fs::write(&fixture.inputs[1], b"edited").unwrap();
        drop(checkout);
        let inspection = pool.inspect().await;
        assert_eq!((inspection.idle, inspection.dirty), (0, true));
        assert_eq!(command(&pool).await, (2, true));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn taking_the_spare_warms_the_next_and_a_change_retires_it_without_warming() {
        let (fixture, root) = fixture("spare");
        let pool = ShellPool::start(Arc::clone(&fixture), ShellPoolConfig::default());
        let Acquired::Activate(ticket) = pool.acquire().await.unwrap() else {
            panic!("a cold pool activates");
        };
        let Activation::Ready { host, evidence } =
            ticket.activator().activate(ticket.predicted(), None).await
        else {
            unreachable!();
        };
        let first = ticket.activated(host, evidence).await;
        assert!(
            settle(&pool).await.spare,
            "the spare is warmed once a generation exists"
        );
        // Host 1 is still executing, so the next command takes the spare (host 2) and host 3
        // is warmed behind it.
        assert_eq!(command(&pool).await, (2, false));
        assert!(settle(&pool).await.spare);
        assert_eq!(fixture.activations.load(Ordering::SeqCst), 3);
        std::fs::write(&fixture.inputs[0], b"edited").unwrap();
        // Returning the held host drains kernel events, without the acquire-time stat backstop.
        drop(first);
        let inspection = pool.inspect().await;
        assert!(
            inspection.dirty,
            "the watched edit was processed before sampling activation count"
        );
        assert!(!inspection.warming, "the event starts no spare activation");
        // The processed event alone activates nothing.
        assert_eq!(fixture.activations.load(Ordering::SeqCst), 3);
        assert_eq!(
            command(&pool).await,
            (4, true),
            "idle hosts and the spare retired"
        );
        assert_eq!(
            pool.counts(),
            PoolCounts {
                idle: 0,
                spare: 1,
                activated: 2,
                pools: 1,
            },
            "the cold start and the stale generation activated; the spare answered once"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_full_cache_evicts_by_sieve() {
        let (fixture, root) = fixture("sieve");
        let config = ShellPoolConfig {
            capacity: NonZeroUsize::new(2).unwrap(),
            prewarm: false,
        };
        let pool = ShellPool::start(Arc::clone(&fixture), config);
        // Three concurrent commands against a two-entry cache.
        let mut held = Vec::new();
        for _ in 0..3 {
            let Acquired::Activate(ticket) = pool.acquire().await.unwrap() else {
                panic!("nothing idle while every host is out");
            };
            let Activation::Ready { host, evidence } =
                ticket.activator().activate(ticket.predicted(), None).await
            else {
                unreachable!();
            };
            held.push(ticket.activated(host, evidence).await);
        }
        let inspection = pool.inspect().await;
        assert_eq!(
            inspection.executing, 2,
            "the oldest unvisited entry was evicted"
        );
        drop(held);
        let inspection = pool.inspect().await;
        assert_eq!(inspection.idle, 2, "the evicted host was not returned");
        assert_eq!(command(&pool).await, (3, false), "newest idle host first");
        std::fs::remove_dir_all(root).unwrap();
    }

    /// One event of the pool core's world, as its actor feeds them.
    #[derive(Clone, Debug)]
    enum Event {
        /// An input changed and its bytes did not hold.
        Changed,
        Acquire,
        /// The `n`th checked-out host comes back, healthy or not.
        Return {
            n: usize,
            healthy: bool,
        },
        /// The `n`th activation in flight ends: its inputs held still while it ran (`stable`),
        /// they are the current generation's (`same`), their subscription is made or refused.
        Activated {
            n: usize,
            stable: bool,
            same: bool,
            subscribed: bool,
        },
        /// The `n`th activation ticket is dropped unused.
        Abandoned {
            n: usize,
        },
        /// The spare activation in flight ends, with a usable host or none.
        Warmed {
            usable: bool,
            stable: bool,
            same: bool,
            subscribed: bool,
        },
    }

    fn event() -> impl proptest::strategy::Strategy<Value = Event> {
        use proptest::prelude::*;
        prop_oneof![
            1 => Just(Event::Changed),
            3 => Just(Event::Acquire),
            3 => (any::<usize>(), any::<bool>()).prop_map(|(n, healthy)| Event::Return { n, healthy }),
            3 => (any::<usize>(), any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
                |(n, stable, same, subscribed)| Event::Activated { n, stable, same, subscribed }
            ),
            1 => any::<usize>().prop_map(|n| Event::Abandoned { n }),
            2 => (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
                |(usable, stable, same, subscribed)| Event::Warmed { usable, stable, same, subscribed }
            ),
        ]
    }

    /// The generations as the specification states them (11_shell.md, "Freshness is direnv's
    /// own"): one is current from the stable activation that installed it until any change.
    #[derive(Default)]
    struct Generations {
        current: Option<u64>,
        epoch: u64,
        next: u64,
    }

    impl Generations {
        /// The generation an activation that started at `epoch` gives its host.
        fn install(
            &mut self,
            epoch: u64,
            stable: bool,
            same: bool,
            subscribed: bool,
        ) -> Option<u64> {
            let stable = stable && epoch == self.epoch;
            if let Some(current) = self.current
                && stable
                && same
            {
                return Some(current);
            }
            if !subscribed {
                self.current = None;
                return None;
            }
            self.next += 1;
            self.current = stable.then_some(self.next);
            self.current
        }
    }

    fn install(
        core: &mut PoolCore<u64>,
        epoch: u64,
        stable: bool,
        same: bool,
        subscribed: bool,
    ) -> Option<u64> {
        match core.install(epoch, stable, same) {
            Install::Current(current) => Some(current),
            Install::Subscribe { stable } => core.generation(stable, subscribed),
        }
    }

    proptest::proptest! {
        /// Whatever arrives in whatever order, the core grants only a host of the generation
        /// current at the grant, never one it holds out already; it replaces a spare it hands
        /// out; and it keeps no more hosts than its capacity.
        #[test]
        fn the_pool_core_grants_only_current_hosts_and_replaces_its_spare(
            prewarm in proptest::prelude::any::<bool>(),
            capacity in 1_usize..5,
            events in proptest::collection::vec(event(), 1..150),
        ) {
            let mut core = PoolCore::<u64>::new(ShellPoolConfig {
                capacity: NonZeroUsize::new(capacity).unwrap(),
                prewarm,
            });
            let mut model = Generations::default();
            let mut generation_of = std::collections::HashMap::<u64, Option<u64>>::new();
            let mut next_host = 0_u64;
            // Checked out: (slot, host, whether it may return).
            let mut out = Vec::<(SlotId, u64, bool)>::new();
            // Activation tickets: (slot, epoch).
            let mut tickets = Vec::<(SlotId, u64)>::new();
            let mut warming: Option<u64> = None;
            for event in events {
                match event {
                    Event::Changed => {
                        core.changed();
                        model.epoch += 1;
                        model.current = None;
                    }
                    Event::Acquire => match core.acquire() {
                        Grant::Spare { warm: None, .. } if prewarm => {
                            proptest::prop_assert!(false, "a spare was handed out and not replaced");
                        }
                        Grant::Idle { slot, host } | Grant::Spare { slot, host, warm: None } => {
                            proptest::prop_assert!(model.current.is_some());
                            proptest::prop_assert_eq!(generation_of[&host], model.current);
                            proptest::prop_assert!(out.iter().all(|&(_, held, _)| held != host));
                            out.push((slot, host, true));
                        }
                        Grant::Spare { slot, host, warm: Some(epoch) } => {
                            proptest::prop_assert!(prewarm);
                            proptest::prop_assert_eq!(generation_of[&host], model.current);
                            proptest::prop_assert_eq!(epoch, model.epoch);
                            proptest::prop_assert!(warming.is_none());
                            warming = Some(epoch);
                            out.push((slot, host, true));
                        }
                        Grant::Activate { slot, epoch, stale } => {
                            proptest::prop_assert_eq!(stale, model.current.is_none());
                            proptest::prop_assert_eq!(epoch, model.epoch);
                            tickets.push((slot, epoch));
                        }
                    },
                    Event::Return { n, healthy } if !out.is_empty() => {
                        let (slot, host, reusable) = out.remove(n % out.len());
                        core.release(slot, Some(host), healthy && reusable);
                    }
                    Event::Activated { n, stable, same, subscribed } if !tickets.is_empty() => {
                        let (slot, epoch) = tickets.remove(n % tickets.len());
                        let expected = model.install(epoch, stable, same, subscribed);
                        let generation = install(&mut core, epoch, stable, same, subscribed);
                        proptest::prop_assert_eq!(generation, expected);
                        let (reusable, warm) = core.activated(slot, generation);
                        if let Some(epoch) = warm {
                            proptest::prop_assert!(prewarm && warming.is_none());
                            warming = Some(epoch);
                        }
                        next_host += 1;
                        generation_of.insert(next_host, generation);
                        out.push((slot, next_host, reusable));
                    }
                    Event::Abandoned { n } if !tickets.is_empty() => {
                        let (slot, _) = tickets.remove(n % tickets.len());
                        core.release(slot, None, false);
                    }
                    Event::Warmed { usable, stable, same, subscribed } => {
                        let Some(epoch) = warming.take() else { continue };
                        if !usable {
                            core.warm_failed();
                            continue;
                        }
                        let expected = model.install(epoch, stable, same, subscribed);
                        let generation = install(&mut core, epoch, stable, same, subscribed);
                        proptest::prop_assert_eq!(generation, expected);
                        next_host += 1;
                        generation_of.insert(next_host, generation);
                        core.warmed(next_host, generation);
                    }
                    Event::Return { .. } | Event::Activated { .. } | Event::Abandoned { .. } => {}
                }
                proptest::prop_assert_eq!(core.current(), model.current);
                proptest::prop_assert_eq!(core.warming, warming.is_some());
                proptest::prop_assert!(core.sieve.slots.len() <= capacity);
            }
        }
    }
}
