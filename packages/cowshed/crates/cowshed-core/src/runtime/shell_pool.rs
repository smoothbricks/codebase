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

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};

use super::shell_watch::{ActivationEvidence, Snapshot, Watcher};
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

struct Generation {
    id: u64,
    baseline: Snapshot,
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

/// Cheap to clone: one sender to the actor.
pub struct ShellPool<A: Activator> {
    requests: mpsc::UnboundedSender<Request<A::Host>>,
    activator: Arc<A>,
}

impl<A: Activator> Clone for ShellPool<A> {
    fn clone(&self) -> Self {
        Self {
            requests: self.requests.clone(),
            activator: Arc::clone(&self.activator),
        }
    }
}

fn pool_stopped() -> CowshedError {
    CowshedError::internal("the workspace shell pool stopped")
}

impl<A: Activator> ShellPool<A> {
    pub fn start(activator: Arc<A>, config: ShellPoolConfig) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel();
        let actor = PoolActor {
            activator: Arc::clone(&activator),
            requests: requests.downgrade(),
            config,
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
        };
        tokio::spawn(actor.run(receiver));
        Self {
            requests,
            activator,
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

struct PoolActor<A: Activator> {
    activator: Arc<A>,
    /// Weak, so the actor ends when the last handle, ticket and checkout are gone.
    requests: mpsc::WeakUnboundedSender<Request<A::Host>>,
    config: ShellPoolConfig,
    sieve: Sieve<A::Host>,
    generation: Option<Generation>,
    next_generation: u64,
    next_slot: SlotId,
    dirty: bool,
    /// Counts observed input changes; an activation compares it across its own run.
    epoch: u64,
    spare: Option<Spare<A::Host>>,
    warming: bool,
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
                #[cfg(test)]
                Request::Inspect { reply } => {
                    let _ = reply.send(PoolInspection {
                        idle: self
                            .sieve
                            .slots
                            .iter()
                            .filter(|slot| slot.host.is_some())
                            .count(),
                        executing: self
                            .sieve
                            .slots
                            .iter()
                            .filter(|slot| slot.host.is_none())
                            .count(),
                        spare: self.spare.is_some(),
                        warming: self.warming,
                        dirty: self.dirty,
                        generation: self.generation.as_ref().map(|generation| generation.id),
                    });
                }
            }
        }
    }

    /// Fold kernel events, and at acquire the stat backstop, into the dirty flag.
    fn refresh(&mut self, backstop: bool) {
        let Some(generation) = &self.generation else {
            return;
        };
        let changed = match generation.watcher.drain() {
            Ok(changed) => changed,
            Err(error) => {
                eprintln!(
                    "cowshed: workspace shell input subscription failed ({error}); treating the \
                     warm shells as stale"
                );
                true
            }
        };
        if changed || (backstop && generation.baseline.changed_since()) {
            self.epoch += 1;
            self.dirty = true;
        }
    }

    fn current(&self) -> Option<u64> {
        if self.dirty {
            return None;
        }
        self.generation.as_ref().map(|generation| generation.id)
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

    fn acquire(&mut self) -> Lease<A::Host> {
        self.refresh(true);
        let Some(current) = self.current() else {
            self.sieve.retire_except(None);
            self.spare = None;
            return self.ticket();
        };
        if let Some((slot, host)) = self.sieve.take_idle(current) {
            return Lease::Warm { slot, host };
        }
        if let Some(spare) = self.spare.take_if(|spare| spare.generation == current) {
            let slot = self.new_slot(Some(current));
            self.warm();
            return Lease::Warm {
                slot,
                host: spare.host,
            };
        }
        self.ticket()
    }

    fn ticket(&mut self) -> Lease<A::Host> {
        let predicted = self
            .generation
            .as_ref()
            .map(|generation| generation.baseline.paths().map(PathBuf::from).collect())
            .unwrap_or_default();
        Lease::Activate {
            slot: self.new_slot(None),
            predicted,
            epoch: self.epoch,
        }
    }

    /// Install what an activation observed; returns the generation its host may return to.
    fn install(&mut self, epoch: u64, evidence: ActivationEvidence) -> Option<u64> {
        self.refresh(false);
        let stable = evidence.stable() && epoch == self.epoch;
        if stable
            && let Some(current) = self.current()
            && self
                .generation
                .as_ref()
                .is_some_and(|generation| generation.baseline == evidence.after)
        {
            return Some(current);
        }
        let watcher = match Watcher::subscribe(&evidence.entries) {
            Ok(watcher) => watcher,
            Err(error) => {
                eprintln!(
                    "cowshed: cannot subscribe the workspace shell's inputs ({error}); its \
                     shells serve one command each"
                );
                self.dirty = true;
                return None;
            }
        };
        let id = self.next_generation;
        self.next_generation += 1;
        self.generation = Some(Generation {
            id,
            baseline: evidence.after,
            watcher,
        });
        // An unstable activation still names the inputs to predict next time; its flag makes
        // the next acquire activate again.
        self.dirty = !stable;
        self.sieve.retire_except(stable.then_some(id));
        self.spare = None;
        stable.then_some(id)
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
                eprintln!(
                    "cowshed: the workspace shell's inputs are unknown ({reason}); this shell \
                     serves one command"
                );
                None
            }
        };
        let Some(position) = self.sieve.position(slot) else {
            return false;
        };
        self.sieve.slots[position].generation = generation;
        if generation.is_some() {
            self.warm();
        }
        generation.is_some()
    }

    fn release(&mut self, slot: SlotId, host: Option<A::Host>, healthy: bool) {
        self.refresh(false);
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

    /// Start activating a spare when none exists or is on its way.
    fn warm(&mut self) {
        if !self.config.prewarm || self.spare.is_some() || self.warming {
            return;
        }
        let (Some(_), Some(requests)) = (self.current(), self.requests.upgrade()) else {
            return;
        };
        let predicted = self
            .generation
            .as_ref()
            .map(|generation| generation.baseline.paths().map(PathBuf::from).collect())
            .unwrap_or_default();
        let activator = Arc::clone(&self.activator);
        let epoch = self.epoch;
        self.warming = true;
        tokio::spawn(async move {
            let activation = activator.activate(predicted, None).await;
            let _ = requests.send(Request::Warmed { epoch, activation });
        });
    }

    fn warmed(&mut self, epoch: u64, activation: Activation<A::Host>) {
        self.warming = false;
        let Activation::Ready {
            host,
            evidence: Ok(evidence),
        } = activation
        else {
            return;
        };
        if let Some(generation) = self.install(epoch, evidence)
            && self.current() == Some(generation)
            && self.spare.is_none()
        {
            self.spare = Some(Spare { host, generation });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::shell_watch::{EvaluationClock, FsInstant, WatchEntry};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

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
            let started = FsInstant::separating(&self.root).unwrap();
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
            let after = Snapshot::take(self.inputs.iter().map(PathBuf::as_path));
            let finished = FsInstant::read(&self.root).unwrap();
            Activation::Ready {
                host,
                evidence: Ok(ActivationEvidence {
                    entries,
                    before,
                    clock: Some(EvaluationClock { started, finished }),
                    after,
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
        drop(first);

        std::fs::write(&fixture.inputs[0], b"edited").unwrap();
        // The event alone activates nothing.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(fixture.activations.load(Ordering::SeqCst), 3);
        assert_eq!(
            command(&pool).await,
            (4, true),
            "idle hosts and the spare retired"
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
}
