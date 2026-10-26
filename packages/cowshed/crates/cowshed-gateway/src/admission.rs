//! Admission to the gateway's bounded pools (05_gateway.md, "Interception engine" and "Busy
//! gateway"): which request holds a permit, which waits and in what order, until when it may wait,
//! and what a refused request hears. A pure core (specs/cowshed/08_testing.md, Unit tests): the
//! actor maps its requests, completions, client cancellations and its one queue timer into these
//! transitions with the clock it reads, and carries out what they hand back. Each queued request's
//! payload rides inside the pool, so a transition returns exactly the requests it affects.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use http::{Method, StatusCode};

use crate::{config::GatewayLimits, interfaces::AuditStatus, nx_cache::NxCacheUse};

/// One active permit, unique for the gateway's life.
pub(crate) type PermitId = u64;
/// One queued request, unique for the gateway's life: what its client's cancellation names.
pub(crate) type QueueId = u64;

/// Which bounded active pool a permit is charged to.
///
/// An intercepted CONNECT is a downstream TLS transport: every HTTP request it carries is
/// admitted separately as a [`PermitClass::Request`]. Charging the tunnel to the origin pool its
/// own inner requests need would let `origin_active` live tunnels starve every inner request
/// until the client gives up. Tunnels therefore draw from their own pool, bounded by the same
/// workspace and global limits, and never hold an origin slot; only requests carry one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PermitClass {
    InterceptTunnel,
    Request { origin: String },
}

/// Active permits per pool. Each pool is bounded independently by `workspace_active` and
/// `global_active`, so the active total is at most twice either limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ActiveCounts {
    tunnels: usize,
    requests: usize,
}

impl ActiveCounts {
    fn of(self, class: &PermitClass) -> usize {
        match class {
            PermitClass::InterceptTunnel => self.tunnels,
            PermitClass::Request { .. } => self.requests,
        }
    }

    fn slot(&mut self, class: &PermitClass) -> &mut usize {
        match class {
            PermitClass::InterceptTunnel => &mut self.tunnels,
            PermitClass::Request { .. } => &mut self.requests,
        }
    }

    fn charge(&mut self, class: &PermitClass) {
        *self.slot(class) += 1;
    }

    fn release(&mut self, class: &PermitClass) {
        let slot = self.slot(class);
        *slot = slot.saturating_sub(1);
    }

    fn total(self) -> usize {
        self.tunnels + self.requests
    }
}

/// Every active permit owns one reserved completion slot until the actor receives its
/// completion, so the channel holds both pools' global bounds.
/// [`GatewayLimits::validate`] keeps this product within the channel's permit range.
pub(crate) fn active_permit_capacity(limits: &GatewayLimits) -> usize {
    limits.global_active * 2
}

/// Whether one more permit of `class` fits its pool. `origin_active` is consulted only for
/// requests, the sole class holding an upstream origin slot.
fn pool_admits(
    limits: &GatewayLimits,
    session: ActiveCounts,
    global: ActiveCounts,
    class: &PermitClass,
    origin_active: impl FnOnce(&str) -> usize,
) -> bool {
    session.of(class) < limits.workspace_active
        && global.of(class) < limits.global_active
        && match class {
            PermitClass::InterceptTunnel => true,
            PermitClass::Request { origin } => origin_active(origin) < limits.origin_active,
        }
}

/// An active permit: the pools it is charged to, and the project whose Nx backend serves it.
struct Permit {
    workspace_id: String,
    class: PermitClass,
    generation: u64,
    nx_repo: Option<String>,
}

/// One installed session: its generation and its active permits by pool. A permit admitted under
/// an earlier generation of the workspace stays charged to the global and origin pools only.
struct Session {
    generation: u64,
    active: ActiveCounts,
}

/// A queued request: its workspace, the instant its wait ends, and the caller's payload.
struct Waiter<P> {
    id: QueueId,
    workspace_id: String,
    deadline: Instant,
    payload: P,
}

/// A request waiting for a permit of `class`.
struct PermitWaiter<W> {
    waiter: Waiter<W>,
    class: PermitClass,
    nx_repo: Option<String>,
}

/// An Nx request waiting for its project's backend to start: its pool is known only once the
/// backend serves.
struct BackendWaiter<B> {
    waiter: Waiter<B>,
    repo: String,
}

/// A request that left the queue unadmitted, with its payload.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Queued<W, B> {
    Permit(W),
    Backend(B),
}

/// Why a request was neither admitted nor queued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unadmitted {
    /// Its workspace has no installed session.
    NoSession,
    /// Its workspace's queue or the gateway's is full.
    Full,
}

/// One admission's outcome.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Admitted<W> {
    Active(PermitId, W),
    Queued(QueueId),
    Refused(Unadmitted, W),
}

/// A startup waiter taken from the queue because its project's backend became Ready: its
/// payload, and the place it keeps — id, workspace and deadline — when [`Pool::resume`] takes it
/// back seeded.
pub(crate) struct Resuming<P> {
    waiter: Waiter<P>,
    repo: String,
}

/// A resuming waiter's place without its payload.
pub(crate) struct Place {
    id: QueueId,
    workspace_id: String,
    deadline: Instant,
    repo: String,
}

impl<P> Resuming<P> {
    pub(crate) fn into_parts(self) -> (Place, P) {
        let Self { waiter, repo } = self;
        (
            Place {
                id: waiter.id,
                workspace_id: waiter.workspace_id,
                deadline: waiter.deadline,
                repo,
            },
            waiter.payload,
        )
    }
}

impl Place {
    /// The waiter of this place, now holding `payload`.
    pub(crate) fn hold<P>(self, payload: P) -> Resuming<P> {
        Resuming {
            waiter: Waiter {
                id: self.id,
                workspace_id: self.workspace_id,
                deadline: self.deadline,
                payload,
            },
            repo: self.repo,
        }
    }
}

/// The gateway's admission state. Invariants: every waiter's workspace has a session, since
/// [`Pool::remove`] takes a session's waiters with it; the startup queue is oldest first, and the
/// ordinary queue is in the turn order of its rotation ([`Pool::promote`]).
pub(crate) struct Pool<W, B> {
    limits: GatewayLimits,
    /// How long a request may wait for a permit or its backend.
    wait: Duration,
    sessions: HashMap<String, Session>,
    global: ActiveCounts,
    /// Active requests by workspace, then by upstream origin.
    origins: HashMap<String, HashMap<String, usize>>,
    permits: HashMap<PermitId, Permit>,
    waiting: VecDeque<PermitWaiter<W>>,
    starting: VecDeque<BackendWaiter<B>>,
    next_permit: PermitId,
    next_queue: QueueId,
}

impl<W, B> Pool<W, B> {
    pub(crate) fn new(limits: GatewayLimits, wait: Duration) -> Self {
        Self {
            limits,
            wait,
            sessions: HashMap::new(),
            global: ActiveCounts::default(),
            origins: HashMap::new(),
            permits: HashMap::new(),
            waiting: VecDeque::new(),
            starting: VecDeque::new(),
            next_permit: 1,
            next_queue: 1,
        }
    }

    /// `workspace_id` serves from now as `generation`, with nothing of its own active or queued.
    pub(crate) fn install(&mut self, workspace_id: &str, generation: u64) {
        self.sessions.insert(
            workspace_id.to_owned(),
            Session {
                generation,
                active: ActiveCounts::default(),
            },
        );
    }

    /// `workspace_id`'s session is gone: its waiters leave unadmitted, those waiting for a backend
    /// first. Its admitted permits run on, charged to the global and origin pools until they
    /// complete.
    pub(crate) fn remove(&mut self, workspace_id: &str) -> Vec<Queued<W, B>> {
        self.sessions.remove(workspace_id);
        let mut left: Vec<Queued<W, B>> = take_where(&mut self.starting, |waiter| {
            waiter.waiter.workspace_id == workspace_id
        })
        .into_iter()
        .map(|waiter| Queued::Backend(waiter.waiter.payload))
        .collect();
        left.extend(
            take_where(&mut self.waiting, |waiter| {
                waiter.waiter.workspace_id == workspace_id
            })
            .into_iter()
            .map(|waiter| Queued::Permit(waiter.waiter.payload)),
        );
        left
    }

    /// One request of `workspace_id` for a permit of `class`: active at once when every pool it
    /// draws from has room, else queued until `now` plus the wait bound while both queues have
    /// room, else refused as a full queue.
    pub(crate) fn admit(
        &mut self,
        now: Instant,
        workspace_id: &str,
        class: PermitClass,
        nx_repo: Option<String>,
        payload: W,
    ) -> Admitted<W> {
        if !self.sessions.contains_key(workspace_id) {
            return Admitted::Refused(Unadmitted::NoSession, payload);
        }
        if let Some(generation) = self.fits(workspace_id, &class) {
            let permit = self.charge(workspace_id, generation, class, nx_repo);
            return Admitted::Active(permit, payload);
        }
        if !self.has_queue_room(workspace_id) {
            return Admitted::Refused(Unadmitted::Full, payload);
        }
        let id = self.next_queue_id();
        self.waiting.push_back(PermitWaiter {
            waiter: Waiter {
                id,
                workspace_id: workspace_id.to_owned(),
                deadline: now + self.wait,
                payload,
            },
            class,
            nx_repo,
        });
        Admitted::Queued(id)
    }

    /// One Nx request of `workspace_id` waiting for `repo`'s backend to start, under the same
    /// queue bounds and wait bound as any queued request (05_gateway.md, "Busy gateway").
    pub(crate) fn wait_for_backend(
        &mut self,
        now: Instant,
        workspace_id: &str,
        repo: &str,
        payload: B,
    ) -> Result<QueueId, (Unadmitted, B)> {
        if !self.sessions.contains_key(workspace_id) {
            return Err((Unadmitted::NoSession, payload));
        }
        if !self.has_queue_room(workspace_id) {
            return Err((Unadmitted::Full, payload));
        }
        let id = self.next_queue_id();
        self.starting.push_back(BackendWaiter {
            waiter: Waiter {
                id,
                workspace_id: workspace_id.to_owned(),
                deadline: now + self.wait,
                payload,
            },
            repo: repo.to_owned(),
        });
        Ok(id)
    }

    /// `repo`'s backend became Ready: its startup waiters leave the queue, oldest first, for the
    /// actor to seed against the backend and hand back through [`Self::resume`].
    pub(crate) fn take_backend_waiters(&mut self, repo: &str) -> Vec<Resuming<B>> {
        take_where(&mut self.starting, |waiter| waiter.repo == repo)
            .into_iter()
            .map(|BackendWaiter { waiter, repo }| Resuming { waiter, repo })
            .collect()
    }

    /// The seeded waiters of a Ready backend whose permits are of `class`, oldest first: each takes
    /// a permit at once or waits for one at the back of the ordinary queue, keeping its id and
    /// deadline. Then the ordinary queue is promoted.
    pub(crate) fn resume(
        &mut self,
        class: &PermitClass,
        seeded: Vec<Resuming<W>>,
    ) -> Vec<(PermitId, W)> {
        let mut activated = Vec::new();
        for Resuming { waiter, repo } in seeded {
            match self.fits(&waiter.workspace_id, class) {
                Some(generation) => {
                    let permit =
                        self.charge(&waiter.workspace_id, generation, class.clone(), Some(repo));
                    activated.push((permit, waiter.payload));
                }
                None => self.waiting.push_back(PermitWaiter {
                    waiter,
                    class: class.clone(),
                    nx_repo: Some(repo),
                }),
            }
        }
        activated.extend(self.promote());
        activated
    }
    /// `repo`'s backend did not start: its waiters leave unadmitted, oldest first.
    pub(crate) fn backend_failed(&mut self, repo: &str) -> Vec<B> {
        take_where(&mut self.starting, |waiter| waiter.repo == repo)
            .into_iter()
            .map(|waiter| waiter.waiter.payload)
            .collect()
    }

    /// `permit`'s request ended, however it ended: every pool it held gets its place back, and
    /// the oldest waiters that now fit take permits. An unknown permit changes nothing.
    pub(crate) fn complete(&mut self, permit: PermitId) -> Vec<(PermitId, W)> {
        let Some(permit) = self.permits.remove(&permit) else {
            return Vec::new();
        };
        self.global.release(&permit.class);
        if let Some(session) = self.sessions.get_mut(&permit.workspace_id)
            && session.generation == permit.generation
        {
            session.active.release(&permit.class);
        }
        if let PermitClass::Request { origin } = &permit.class
            && let Some(origins) = self.origins.get_mut(&permit.workspace_id)
        {
            if let Some(active) = origins.get_mut(origin) {
                *active = active.saturating_sub(1);
                if *active == 0 {
                    origins.remove(origin);
                }
            }
            if origins.is_empty() {
                self.origins.remove(&permit.workspace_id);
            }
        }
        self.promote()
    }

    /// The client of queued request `id` went away: it leaves the queue. An id no longer queued,
    /// because it was admitted or refused first, changes nothing.
    pub(crate) fn cancel(&mut self, id: QueueId) -> Option<Queued<W, B>> {
        if let Some(index) = self
            .starting
            .iter()
            .position(|waiter| waiter.waiter.id == id)
        {
            return self
                .starting
                .remove(index)
                .map(|waiter| Queued::Backend(waiter.waiter.payload));
        }
        let index = self
            .waiting
            .iter()
            .position(|waiter| waiter.waiter.id == id)?;
        self.waiting
            .remove(index)
            .map(|waiter| Queued::Permit(waiter.waiter.payload))
    }

    /// Every request whose wait ended by `now` leaves the queue, ordinary queue first.
    pub(crate) fn expire(&mut self, now: Instant) -> Vec<Queued<W, B>> {
        let mut expired: Vec<Queued<W, B>> =
            take_where(&mut self.waiting, |waiter| waiter.waiter.deadline <= now)
                .into_iter()
                .map(|waiter| Queued::Permit(waiter.waiter.payload))
                .collect();
        expired.extend(
            take_where(&mut self.starting, |waiter| waiter.waiter.deadline <= now)
                .into_iter()
                .map(|waiter| Queued::Backend(waiter.waiter.payload)),
        );
        expired
    }

    /// When [`Self::expire`] next has a request to take: the earliest deadline. `None` while
    /// nothing waits.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.waiting
            .iter()
            .map(|waiter| waiter.waiter.deadline)
            .chain(self.starting.iter().map(|waiter| waiter.waiter.deadline))
            .min()
    }

    /// Every waiter leaves unadmitted, ordinary queue first: the gateway drains or failed closed.
    pub(crate) fn withdraw_all(&mut self) -> Vec<Queued<W, B>> {
        let mut left: Vec<Queued<W, B>> = self
            .waiting
            .drain(..)
            .map(|waiter| Queued::Permit(waiter.waiter.payload))
            .collect();
        left.extend(
            self.starting
                .drain(..)
                .map(|waiter| Queued::Backend(waiter.waiter.payload)),
        );
        left
    }
    /// The gateway stops: every permit, waiter and charge is forgotten.
    pub(crate) fn clear(&mut self) {
        self.permits.clear();
        self.origins.clear();
        self.global = ActiveCounts::default();
        for session in self.sessions.values_mut() {
            session.active = ActiveCounts::default();
        }
        self.waiting.clear();
        self.starting.clear();
    }

    /// The Nx work of `repo` admitted or queued now, by workspace: what clearing its store would
    /// cut off (03_caches.md, "Nx accepted-task artifact service").
    pub(crate) fn nx_use(&self, repo: &str) -> NxCacheUse {
        let mut usage = NxCacheUse::default();
        for permit in self.permits.values() {
            if permit.nx_repo.as_deref() == Some(repo) {
                *usage
                    .in_flight
                    .entry(permit.workspace_id.clone())
                    .or_default() += 1;
            }
        }
        let queued = self
            .waiting
            .iter()
            .filter(|waiter| waiter.nx_repo.as_deref() == Some(repo))
            .map(|waiter| &waiter.waiter.workspace_id)
            .chain(
                self.starting
                    .iter()
                    .filter(|waiter| waiter.repo == repo)
                    .map(|waiter| &waiter.waiter.workspace_id),
            );
        for workspace_id in queued {
            *usage.queued.entry(workspace_id.clone()).or_default() += 1;
        }
        usage
    }

    /// Active permits, both pools.
    pub(crate) fn active(&self) -> usize {
        self.global.total()
    }

    /// Queued requests, both queues.
    pub(crate) fn queued(&self) -> usize {
        self.waiting.len() + self.starting.len()
    }

    /// `workspace_id`'s active permits of its current generation, and its queued requests.
    pub(crate) fn load(&self, workspace_id: &str) -> (usize, usize) {
        (
            self.sessions
                .get(workspace_id)
                .map_or(0, |session| session.active.total()),
            self.session_queued(workspace_id),
        )
    }

    /// Admit every ordinary waiter that fits, the gateway's eligible-waiter rotation: the front
    /// waiter takes a permit when its pools have room, and the scan starts over; one that does
    /// not fit goes to the back. The pass ends once a whole turn of the queue admitted nobody, so
    /// a waiter blocked on its origin or workspace never holds back an eligible one behind it.
    fn promote(&mut self) -> Vec<(PermitId, W)> {
        let mut activated = Vec::new();
        let mut inspected = 0;
        while inspected < self.waiting.len() {
            let Some(waiter) = self.waiting.pop_front() else {
                break;
            };
            match self.fits(&waiter.waiter.workspace_id, &waiter.class) {
                Some(generation) => {
                    let PermitWaiter {
                        waiter,
                        class,
                        nx_repo,
                    } = waiter;
                    let permit = self.charge(&waiter.workspace_id, generation, class, nx_repo);
                    activated.push((permit, waiter.payload));
                    inspected = 0;
                }
                None => {
                    self.waiting.push_back(waiter);
                    inspected += 1;
                }
            }
        }
        activated
    }

    /// The generation of `workspace_id`'s session when one more permit of `class` fits every
    /// pool it draws from; `None` when it does not, or the workspace has no session.
    fn fits(&self, workspace_id: &str, class: &PermitClass) -> Option<u64> {
        let session = self.sessions.get(workspace_id)?;
        pool_admits(&self.limits, session.active, self.global, class, |origin| {
            self.origins
                .get(workspace_id)
                .and_then(|origins| origins.get(origin))
                .copied()
                .unwrap_or(0)
        })
        .then_some(session.generation)
    }

    fn has_queue_room(&self, workspace_id: &str) -> bool {
        self.queued() < self.limits.global_queued
            && self.session_queued(workspace_id) < self.limits.workspace_queued
    }

    fn session_queued(&self, workspace_id: &str) -> usize {
        self.waiting
            .iter()
            .map(|waiter| &waiter.waiter.workspace_id)
            .chain(
                self.starting
                    .iter()
                    .map(|waiter| &waiter.waiter.workspace_id),
            )
            .filter(|queued| *queued == workspace_id)
            .count()
    }

    /// Charges a permit of `class` to the session of `workspace_id` at `generation`, which
    /// [`Self::fits`] found room for.
    fn charge(
        &mut self,
        workspace_id: &str,
        generation: u64,
        class: PermitClass,
        nx_repo: Option<String>,
    ) -> PermitId {
        let permit = self.next_permit;
        self.next_permit = self.next_permit.wrapping_add(1).max(1);
        if let Some(session) = self.sessions.get_mut(workspace_id) {
            session.active.charge(&class);
        }
        self.global.charge(&class);
        if let PermitClass::Request { origin } = &class {
            *self
                .origins
                .entry(workspace_id.to_owned())
                .or_default()
                .entry(origin.clone())
                .or_default() += 1;
        }
        self.permits.insert(
            permit,
            Permit {
                workspace_id: workspace_id.to_owned(),
                class,
                generation,
                nx_repo,
            },
        );
        permit
    }

    fn next_queue_id(&mut self) -> QueueId {
        let id = self.next_queue;
        self.next_queue = self.next_queue.wrapping_add(1).max(1);
        id
    }
}

/// Moves every item `leaves` names out of `queue`, keeping both sides in order.
fn take_where<T>(queue: &mut VecDeque<T>, mut leaves: impl FnMut(&T) -> bool) -> Vec<T> {
    let (left, stayed): (VecDeque<T>, VecDeque<T>) = std::mem::take(queue)
        .into_iter()
        .partition(|item| leaves(item));
    *queue = stayed;
    left.into()
}

/// Why a request is refused before it is admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The gateway is draining.
    Draining,
    /// Its workspace's queue or the gateway's is full.
    QueueFull,
    /// Its wait for a permit or its backend ended.
    QueueTimeout,
    /// Its client went away while it waited.
    QueueCancelled,
    /// Its workspace's session was replaced or removed while it waited.
    SessionRotated,
    /// Its project's Nx backend is down, cooling down after a failed start, or being cleared.
    BackendOutage(String),
    /// The Nx cache route takes only `GET` and `PUT`.
    NxMethodDenied,
    /// Its Nx request could not be seeded against the backend that started for it.
    NxDenied {
        message: &'static str,
        hint: Option<String>,
    },
}

/// What a refused request hears, `status`, and what its audit records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Answer<S, C> {
    pub(crate) status: S,
    pub(crate) audit: AuditStatus,
    pub(crate) classification: C,
    pub(crate) message: &'static str,
}

/// A refusal's cause: the status a route other than Nx hears (`None` once its client is gone),
/// the audit status, the classification, whether such a route's audit names that
/// classification, and the message.
struct Cause {
    status: Option<StatusCode>,
    audit: AuditStatus,
    classification: &'static str,
    classified_off_nx: bool,
    message: &'static str,
}

impl Refusal {
    fn cause(&self) -> Cause {
        let (status, audit, classification, classified_off_nx, message) = match self {
            Self::Draining => (
                Some(StatusCode::SERVICE_UNAVAILABLE),
                AuditStatus::Cancelled,
                "gateway-draining",
                true,
                "gateway is draining",
            ),
            Self::QueueFull => (
                Some(StatusCode::TOO_MANY_REQUESTS),
                AuditStatus::Limited,
                "nx-queue-limited",
                false,
                "gateway queue is full",
            ),
            Self::QueueTimeout => (
                Some(StatusCode::GATEWAY_TIMEOUT),
                AuditStatus::TimedOut,
                "queue-timeout",
                true,
                "gateway queue wait timed out",
            ),
            Self::QueueCancelled => (
                None,
                AuditStatus::Cancelled,
                "queue-cancelled",
                true,
                "queued request was cancelled",
            ),
            Self::SessionRotated => (
                Some(StatusCode::SERVICE_UNAVAILABLE),
                AuditStatus::Cancelled,
                "session-rotated",
                true,
                "workspace session rotated",
            ),
            Self::BackendOutage(_) => (
                Some(StatusCode::SERVICE_UNAVAILABLE),
                AuditStatus::Failed,
                "nx-backend-unavailable",
                true,
                "project Nx cache backend is unavailable",
            ),
            Self::NxMethodDenied => (
                Some(StatusCode::FORBIDDEN),
                AuditStatus::Denied,
                "nx-method-denied",
                true,
                "Nx cache accepts only GET and PUT",
            ),
            Self::NxDenied { message, .. } => (
                Some(StatusCode::FORBIDDEN),
                AuditStatus::Denied,
                "nx-denied",
                true,
                *message,
            ),
        };
        Cause {
            status,
            audit,
            classification,
            classified_off_nx,
            message,
        }
    }

    /// The answer to a request of the Nx cache route made with `method`, fresh or waiting for its
    /// backend. The route never blocks a build (05_gateway.md, "Busy gateway"): every refusal is
    /// the method's miss (`GET` 404) or not-stored answer (403), because stock Nx fails a task on
    /// any other status; the audit keeps the cause.
    pub(crate) fn nx_answer(&self, method: &Method) -> Answer<StatusCode, &'static str> {
        let cause = self.cause();
        Answer {
            status: crate::nx_cache::unavailable_status(method),
            audit: cause.audit,
            classification: cause.classification,
            message: cause.message,
        }
    }

    /// The answer to a fresh request of any other route.
    pub(crate) fn proxy_answer(&self) -> Answer<Option<StatusCode>, Option<&'static str>> {
        let cause = self.cause();
        Answer {
            status: cause.status,
            audit: cause.audit,
            classification: cause.classified_off_nx.then_some(cause.classification),
            message: cause.message,
        }
    }

    /// The answer to a request that waited for a permit, of the Nx route made with `nx` or of any
    /// other: the status its cause gives, as its seed's route hears it
    /// (`AdmissionSeed::refusal_status`), and none once its client is gone.
    pub(crate) fn queued_answer(
        &self,
        nx: Option<&Method>,
    ) -> Answer<Option<StatusCode>, Option<&'static str>> {
        let mut answer = self.proxy_answer();
        if let Some(method) = nx {
            answer.status = answer
                .status
                .map(|_| crate::nx_cache::unavailable_status(method));
        }
        answer
    }
    /// The diagnostic a refusal carries to its reply, beside its message.
    pub(crate) fn hint(&self) -> Option<String> {
        match self {
            Self::BackendOutage(reason) => Some(reason.clone()),
            Self::NxDenied { hint, .. } => hint.clone(),
            Self::Draining
            | Self::QueueFull
            | Self::QueueTimeout
            | Self::QueueCancelled
            | Self::SessionRotated
            | Self::NxMethodDenied => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const WAIT: Duration = Duration::from_secs(10);

    fn limits(active: usize, queued: usize, origin: usize) -> GatewayLimits {
        GatewayLimits {
            workspace_active: active,
            global_active: active,
            workspace_queued: queued,
            global_queued: queued,
            origin_active: origin,
            ..GatewayLimits::default()
        }
    }

    fn request(origin: &str) -> PermitClass {
        PermitClass::Request {
            origin: origin.to_owned(),
        }
    }

    /// A pool with workspace `ws` installed at generation 1, driven at explicit instants, each
    /// request's payload a label.
    struct Clock {
        pool: Pool<&'static str, &'static str>,
        origin: Instant,
    }

    impl Clock {
        fn new(limits: GatewayLimits) -> Self {
            let mut pool = Pool::new(limits, WAIT);
            pool.install("ws", 1);
            Self {
                pool,
                origin: Instant::now(),
            }
        }

        fn at(&self, ms: u64) -> Instant {
            self.origin + Duration::from_millis(ms)
        }

        fn admit(
            &mut self,
            ms: u64,
            class: PermitClass,
            label: &'static str,
        ) -> Admitted<&'static str> {
            let now = self.at(ms);
            self.pool.admit(now, "ws", class, None, label)
        }

        /// Admits `label` and returns its permit; a request that does not activate is a failed row.
        fn active(&mut self, ms: u64, class: PermitClass, label: &'static str) -> PermitId {
            match self.admit(ms, class, label) {
                Admitted::Active(permit, admitted) => {
                    assert_eq!(admitted, label);
                    permit
                }
                other => panic!("{label} is not active: {other:?}"),
            }
        }

        /// Admits `label` and returns its queue id; a request that does not wait is a failed row.
        fn queued(&mut self, ms: u64, class: PermitClass, label: &'static str) -> QueueId {
            match self.admit(ms, class, label) {
                Admitted::Queued(id) => id,
                other => panic!("{label} is not queued: {other:?}"),
            }
        }

        fn wait(
            &mut self,
            ms: u64,
            repo: &str,
            label: &'static str,
        ) -> Result<QueueId, (Unadmitted, &'static str)> {
            let now = self.at(ms);
            self.pool.wait_for_backend(now, "ws", repo, label)
        }

        fn complete(&mut self, permit: PermitId) -> Vec<&'static str> {
            labels(self.pool.complete(permit))
        }

        /// `repo`'s backend becomes Ready as `class`, each waiter seeded as itself.
        fn resume(&mut self, repo: &str, class: PermitClass) -> Vec<&'static str> {
            let seeded = self
                .pool
                .take_backend_waiters(repo)
                .into_iter()
                .map(|resuming| {
                    let (place, label) = resuming.into_parts();
                    place.hold(label)
                })
                .collect();
            labels(self.pool.resume(&class, seeded))
        }
    }

    fn labels(activated: Vec<(PermitId, &'static str)>) -> Vec<&'static str> {
        activated.into_iter().map(|(_, label)| label).collect()
    }

    #[test]
    fn a_request_is_admitted_queued_or_refused_and_completion_promotes_the_oldest_waiter() {
        let mut clock = Clock::new(limits(1, 1, 1));
        let first = clock.active(0, request("o"), "first");
        clock.queued(0, request("o"), "second");
        assert_eq!(
            clock.admit(0, request("o"), "third"),
            Admitted::Refused(Unadmitted::Full, "third"),
            "a full queue refuses"
        );
        assert_eq!((clock.pool.active(), clock.pool.queued()), (1, 1));
        assert_eq!(clock.pool.load("ws"), (1, 1));
        assert_eq!(clock.complete(first), ["second"]);
        assert_eq!(clock.pool.load("ws"), (1, 0));
        assert_eq!(
            clock.complete(PermitId::MAX),
            Vec::<&str>::new(),
            "an unknown permit"
        );
    }

    #[test]
    fn a_queued_wait_expires_at_its_deadline_and_frees_its_place() {
        let mut clock = Clock::new(limits(1, 1, 1));
        let first = clock.active(0, request("o"), "first");
        clock.queued(0, request("o"), "waiting");
        let deadline = clock.at(0) + WAIT;
        assert_eq!(clock.pool.deadline(), Some(deadline));
        assert_eq!(
            clock.pool.expire(deadline - Duration::from_nanos(1)),
            Vec::new(),
            "a wait ends at its deadline, not before"
        );
        assert_eq!(clock.pool.expire(deadline), [Queued::Permit("waiting")]);
        assert_eq!((clock.pool.queued(), clock.pool.deadline()), (0, None));
        clock.queued(1, request("o"), "later");
        assert_eq!(
            clock.complete(first),
            ["later"],
            "the expired wait took no permit"
        );
    }

    #[test]
    fn a_cancelled_waiter_leaves_once_and_takes_no_permit() {
        let mut clock = Clock::new(limits(1, 1, 1));
        let first = clock.active(0, request("o"), "first");
        let waiting = clock.queued(0, request("o"), "waiting");
        assert_eq!(clock.pool.cancel(waiting), Some(Queued::Permit("waiting")));
        assert_eq!(
            clock.pool.cancel(waiting),
            None,
            "a cancellation after it left"
        );
        assert_eq!(clock.complete(first), Vec::<&str>::new());
        assert_eq!((clock.pool.active(), clock.pool.queued()), (0, 0));
    }

    #[test]
    fn requests_stay_origin_limited_while_other_origins_and_tunnels_proceed() {
        let mut clock = Clock::new(limits(4, 4, 1));
        let first = clock.active(0, request("x"), "x1");
        clock.queued(0, request("x"), "x2");
        let other = clock.active(0, request("y"), "y1");
        clock.active(0, PermitClass::InterceptTunnel, "tunnel");
        assert_eq!(
            clock.complete(other),
            Vec::<&str>::new(),
            "x is still at its limit"
        );
        assert_eq!(clock.complete(first), ["x2"]);
    }

    #[test]
    fn origin_limit_of_intercept_tunnels_leaves_inner_requests_admissible() {
        let defaults = GatewayLimits::default();
        let mut clock = Clock::new(defaults);
        for _ in 0..defaults.origin_active {
            clock.active(0, PermitClass::InterceptTunnel, "tunnel");
        }
        for _ in 0..defaults.origin_active {
            clock.active(0, request("registry"), "request");
        }
        clock.queued(0, request("registry"), "over the origin limit");
        assert_eq!(clock.pool.load("ws"), (defaults.origin_active * 2, 1));
    }

    #[test]
    fn single_active_limit_admits_one_tunnel_and_its_inner_request() {
        let mut clock = Clock::new(limits(1, 4, 1));
        clock.active(0, PermitClass::InterceptTunnel, "tunnel");
        clock.queued(0, PermitClass::InterceptTunnel, "second tunnel");
        let inner = clock.active(0, request("o"), "inner request");
        clock.queued(0, request("o"), "opaque");
        assert_eq!(
            clock.complete(inner),
            ["opaque"],
            "the request pool frees; the tunnel pool stays full"
        );
    }

    #[test]
    fn a_replaced_session_withdraws_its_waiters_and_its_old_permits_release_only_shared_pools() {
        let mut clock = Clock::new(GatewayLimits {
            workspace_active: 1,
            global_active: 2,
            ..limits(2, 4, 4)
        });
        let old = clock.active(0, request("o"), "old");
        clock.queued(0, request("o"), "rotated");
        assert_eq!(clock.pool.remove("ws"), [Queued::Permit("rotated")]);
        assert_eq!(
            clock.admit(0, request("o"), "no session"),
            Admitted::Refused(Unadmitted::NoSession, "no session")
        );
        clock.pool.install("ws", 2);
        let new = clock.active(0, request("o"), "new");
        clock.queued(0, request("o"), "next");
        assert_eq!(
            clock.complete(old),
            Vec::<&str>::new(),
            "the old generation's permit frees the global pool, not the new session's"
        );
        assert_eq!(clock.pool.load("ws"), (1, 1));
        assert_eq!(clock.complete(new), ["next"]);
    }

    #[test]
    fn startup_waiters_count_against_the_queue_and_resume_only_with_their_project() {
        let mut clock = Clock::new(limits(4, 2, 4));
        assert!(clock.wait(0, "one", "first").is_ok());
        assert!(clock.wait(0, "two", "second").is_ok());
        assert_eq!(
            clock.wait(0, "one", "third"),
            Err((Unadmitted::Full, "third"))
        );
        assert_eq!(
            clock.pool.nx_use("one").queued,
            BTreeMap::from([("ws".to_owned(), 1)])
        );
        assert_eq!(clock.resume("one", request("backend")), ["first"]);
        assert_eq!(
            clock.pool.nx_use("one").in_flight,
            BTreeMap::from([("ws".to_owned(), 1)])
        );
        assert_eq!(clock.pool.queued(), 1, "the other project still waits");
        assert_eq!(clock.pool.backend_failed("two"), ["second"]);
        assert_eq!(clock.pool.queued(), 0);
    }

    #[test]
    fn a_ready_backend_waiter_without_room_keeps_its_deadline_in_the_ordinary_queue() {
        let mut clock = Clock::new(limits(1, 2, 1));
        clock.active(0, request("backend"), "first");
        assert!(clock.wait(0, "repo", "waiting").is_ok());
        assert_eq!(clock.resume("repo", request("backend")), Vec::<&str>::new());
        assert_eq!(clock.pool.queued(), 1);
        assert_eq!(clock.pool.deadline(), Some(clock.at(0) + WAIT));
        assert!(clock.pool.nx_use("repo").queued.contains_key("ws"));
        assert_eq!(
            clock.pool.expire(clock.at(0) + WAIT),
            [Queued::Permit("waiting")]
        );
    }

    #[test]
    fn a_ready_backends_waiter_left_unseeded_holds_no_place() {
        let mut clock = Clock::new(limits(1, 2, 1));
        assert!(clock.wait(0, "repo", "waiting").is_ok());
        assert!(clock.wait(0, "other", "other project").is_ok());
        let taken: Vec<&str> = clock
            .pool
            .take_backend_waiters("repo")
            .into_iter()
            .map(|resuming| resuming.into_parts().1)
            .collect();
        assert_eq!(taken, ["waiting"]);
        assert_eq!((clock.pool.active(), clock.pool.queued()), (0, 1));
    }

    /// The rotation is the gateway's: a waiter that does not fit goes to the back, the scan starts
    /// over after each admission, and a whole turn without one ends the pass. A waiter blocked on
    /// its origin never holds back an eligible one behind it, and the turn order decides who goes
    /// next, not arrival alone.
    #[test]
    fn promotion_rotates_past_blocked_waiters_in_the_gateways_turn_order() {
        let mut clock = Clock::new(limits(2, 4, 1));
        let x = clock.active(0, request("x"), "x");
        let y = clock.active(0, request("y"), "y");
        clock.queued(0, request("x"), "x blocked on its origin");
        clock.queued(0, request("z"), "z");
        clock.queued(0, request("y"), "y after z");
        assert_eq!(
            clock.complete(y),
            ["z"],
            "z passes the waiter blocked on x and takes the freed place"
        );
        assert_eq!(
            clock.complete(x),
            ["y after z"],
            "the turn left y ahead of the waiter on x"
        );
        assert_eq!(clock.pool.load("ws"), (2, 1));
    }

    #[test]
    fn a_drain_withdraws_every_waiter_and_leaves_the_admitted_running() {
        let mut clock = Clock::new(limits(1, 4, 1));
        clock.active(0, request("o"), "running");
        clock.queued(0, request("o"), "waiting");
        assert!(clock.wait(0, "repo", "starting").is_ok());
        assert_eq!(
            clock.pool.withdraw_all(),
            [Queued::Permit("waiting"), Queued::Backend("starting")]
        );
        assert_eq!((clock.pool.active(), clock.pool.queued()), (1, 0));
    }

    #[test]
    fn completion_capacity_covers_both_full_pools() {
        let limits = GatewayLimits::default();
        let full = ActiveCounts {
            tunnels: limits.global_active,
            requests: limits.global_active,
        };
        assert_eq!(active_permit_capacity(&limits), full.total());
        let oversized = GatewayLimits {
            global_active: tokio::sync::Semaphore::MAX_PERMITS / 2 + 1,
            ..limits
        };
        assert!(oversized.validate().is_err());
    }

    /// The Nx route hears its method's miss or not-stored answer for every refusal, never a 429,
    /// 503 or 504 the stock client fails a task on (05_gateway.md, "Busy gateway"); every other
    /// route hears its cause's status; a request that waited for a permit hears its cause as its
    /// route maps it, and nothing once its client left; the audit keeps the cause.
    #[test]
    fn a_refused_request_hears_its_routes_answer_and_its_audit_keeps_the_cause() {
        use AuditStatus::{Cancelled, Denied, Failed, Limited, TimedOut};
        let (get, put, head, post) = (Method::GET, Method::PUT, Method::HEAD, Method::POST);
        let outage = Refusal::BackendOutage("backend exited".to_owned());
        let denied = Refusal::NxDenied {
            message: "Nx cache path is invalid",
            hint: Some("the hash".to_owned()),
        };
        let nx_rows: [(Refusal, &Method, u16, AuditStatus, &str); 13] = [
            (Refusal::QueueFull, &get, 404, Limited, "nx-queue-limited"),
            (Refusal::QueueFull, &put, 403, Limited, "nx-queue-limited"),
            (Refusal::QueueTimeout, &get, 404, TimedOut, "queue-timeout"),
            (Refusal::QueueTimeout, &put, 403, TimedOut, "queue-timeout"),
            (Refusal::Draining, &get, 404, Cancelled, "gateway-draining"),
            (
                Refusal::QueueCancelled,
                &get,
                404,
                Cancelled,
                "queue-cancelled",
            ),
            (
                Refusal::SessionRotated,
                &put,
                403,
                Cancelled,
                "session-rotated",
            ),
            (outage.clone(), &get, 404, Failed, "nx-backend-unavailable"),
            (outage.clone(), &put, 403, Failed, "nx-backend-unavailable"),
            (
                Refusal::NxMethodDenied,
                &head,
                403,
                Denied,
                "nx-method-denied",
            ),
            (
                Refusal::NxMethodDenied,
                &post,
                403,
                Denied,
                "nx-method-denied",
            ),
            (denied.clone(), &get, 404, Denied, "nx-denied"),
            (denied.clone(), &put, 403, Denied, "nx-denied"),
        ];
        for (refusal, method, status, audit, classification) in nx_rows {
            let answer = refusal.nx_answer(method);
            assert_eq!(
                (answer.status.as_u16(), answer.audit, answer.classification),
                (status, audit, classification),
                "{refusal:?} for {method}"
            );
        }
        let proxy_rows: [(Refusal, Option<u16>, AuditStatus, Option<&str>); 5] = [
            (Refusal::QueueFull, Some(429), Limited, None),
            (
                Refusal::QueueTimeout,
                Some(504),
                TimedOut,
                Some("queue-timeout"),
            ),
            (
                Refusal::Draining,
                Some(503),
                Cancelled,
                Some("gateway-draining"),
            ),
            (
                Refusal::SessionRotated,
                Some(503),
                Cancelled,
                Some("session-rotated"),
            ),
            (
                Refusal::QueueCancelled,
                None,
                Cancelled,
                Some("queue-cancelled"),
            ),
        ];
        for (refusal, status, audit, classification) in proxy_rows {
            let answer = refusal.proxy_answer();
            assert_eq!(
                (
                    answer.status.map(|status| status.as_u16()),
                    answer.audit,
                    answer.classification
                ),
                (status, audit, classification),
                "{refusal:?}"
            );
            assert_eq!(refusal.queued_answer(None), answer, "{refusal:?} queued");
        }
        let queued_rows: [(Refusal, &Method, Option<u16>); 5] = [
            (Refusal::QueueTimeout, &get, Some(404)),
            (Refusal::QueueTimeout, &put, Some(403)),
            (Refusal::Draining, &put, Some(403)),
            (Refusal::SessionRotated, &get, Some(404)),
            (Refusal::QueueCancelled, &get, None),
        ];
        for (refusal, method, status) in queued_rows {
            let answer = refusal.queued_answer(Some(method));
            assert_eq!(
                answer.status.map(|status| status.as_u16()),
                status,
                "{refusal:?} queued for {method}"
            );
            assert_eq!(
                answer.classification,
                refusal.proxy_answer().classification,
                "{refusal:?}"
            );
        }
        assert_eq!(outage.hint().as_deref(), Some("backend exited"));
        assert_eq!(denied.hint().as_deref(), Some("the hash"));
        assert_eq!(denied.nx_answer(&get).message, "Nx cache path is invalid");
        assert_eq!(Refusal::QueueFull.hint(), None);
    }
}
