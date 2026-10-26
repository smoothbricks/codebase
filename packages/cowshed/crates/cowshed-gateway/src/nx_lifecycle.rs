//! Each project's Nx cache backend lifecycle (05_gateway.md, "Backend lifecycle isolation" and
//! "Clearing the store"): `Starting`, `Ready`, `Failed { retry_after }` or `Clearing`, per
//! repository. A pure core (specs/cowshed/08_testing.md, Unit tests): the actor maps a
//! request, a start's report, a child's observed exit, a clear and its report into these
//! transitions with the clock it reads, and carries out what they answer. A state holds the
//! actor's start task `T` or started child `P` only as an opaque value: no transition ends one.
//! Every transition that displaces a state hands its [`Handle`] back, and the actor ends it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use http::Method;

use crate::{
    admission::Refusal,
    nx_cache::{NxCacheBackend, NxCacheClearRefusal, NxCacheUse},
};

/// One project's backend.
pub(crate) enum NxBackendState<T, P> {
    /// The one start in flight, shared by every request that waits for it.
    Starting { task: T },
    Ready {
        backend: NxCacheBackend,
        /// `None` only for a backend the host configured directly. A started server's child keeps
        /// its stdin lease, and its death closes that lease.
        process: Option<P>,
    },
    Failed {
        reason: String,
        retry_after: Instant,
    },
    /// The project's store is being cleared: its backend has stopped and no new one starts until
    /// the clear reports.
    Clearing,
}

/// What a displaced state held, for the actor to end: a start's task or a started child.
#[must_use = "the actor ends every start task and child a transition displaces"]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Handle<T, P> {
    Start(T),
    Child(P),
}

impl<T, P> NxBackendState<T, P> {
    /// The handle this state holds, as it leaves the core.
    fn into_handle(self) -> Option<Handle<T, P>> {
        match self {
            Self::Starting { task } => Some(Handle::Start(task)),
            Self::Ready {
                process: Some(process),
                ..
            } => Some(Handle::Child(process)),
            Self::Ready { process: None, .. } | Self::Failed { .. } | Self::Clearing => None,
        }
    }
}

/// What one Nx request does about its project's backend.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NxAdmission {
    /// The backend serves: seed the request against it.
    Ready,
    /// A start is in flight: wait for it.
    Starting,
    /// No backend and none starting: start one ([`NxBackends::starting`]) and wait for it.
    Start,
    /// Refused without starting or waiting.
    Refuse(Refusal),
}

/// One Nx request's admission, and the exited child it replaced, which the actor ends.
#[must_use = "the actor ends the exited child an admission replaced"]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NxDecision<P> {
    pub(crate) admission: NxAdmission,
    pub(crate) exited: Option<P>,
}

/// A start's report of a serving backend.
#[must_use = "the actor ends the handle a report leaves behind"]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Installed<T, P> {
    /// Installed over its start, whose task is handed back.
    Ready { start: T },
    /// Its start was replaced, cleared or forgotten: nothing is installed, and its child is handed
    /// back.
    Stale(P),
}

/// What a project's last session leaving did to its backend.
#[must_use = "the actor ends the handle a release forgot"]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Released<T, P> {
    /// A host-configured backend, a clear in flight, or nothing: kept.
    Kept,
    /// Forgotten, with the start task or child it held.
    Forgot(Option<Handle<T, P>>),
}

/// Every project's backend, and how long a failed start keeps its project refused.
pub(crate) struct NxBackends<T, P> {
    states: HashMap<String, NxBackendState<T, P>>,
    cooldown: Duration,
}

impl<T, P> NxBackends<T, P> {
    /// `cooldown` is how long a failed start refuses its project: `GatewayTimeouts.connect`.
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            states: HashMap::new(),
            cooldown,
        }
    }

    /// One Nx request for `repo` made with `method` at `now`; `exited` says whether the child the
    /// gateway started for `repo` was seen to have exited. An unsupported method is refused before
    /// any backend starts or is waited for. An exited child is replaced in this same admission. A
    /// failed start refuses until its `retry_after`, after which one new start is attempted.
    pub(crate) fn admit(
        &mut self,
        repo: &str,
        method: &Method,
        now: Instant,
        exited: bool,
    ) -> NxDecision<P> {
        if *method != Method::GET && *method != Method::PUT {
            return NxDecision {
                admission: NxAdmission::Refuse(Refusal::NxMethodDenied),
                exited: None,
            };
        }
        let exited = if exited {
            match self.states.remove(repo) {
                Some(NxBackendState::Ready {
                    process: Some(process),
                    ..
                }) => Some(process),
                other => {
                    self.restore(repo, other);
                    None
                }
            }
        } else {
            None
        };
        let admission = match self.states.get(repo) {
            Some(NxBackendState::Ready { .. }) => NxAdmission::Ready,
            Some(NxBackendState::Starting { .. }) => NxAdmission::Starting,
            Some(NxBackendState::Clearing) => NxAdmission::Refuse(Refusal::BackendOutage(
                "the project's Nx cache store is being cleared".to_owned(),
            )),
            Some(NxBackendState::Failed {
                reason,
                retry_after,
            }) if now < *retry_after => NxAdmission::Refuse(Refusal::BackendOutage(reason.clone())),
            Some(NxBackendState::Failed { .. }) | None => NxAdmission::Start,
        };
        NxDecision { admission, exited }
    }

    /// The start [`NxAdmission::Start`] asked for runs as `task`. It replaces only a failed start
    /// or nothing, so the state it displaces holds no handle; any it did is handed back.
    pub(crate) fn starting(&mut self, repo: &str, task: T) -> Option<Handle<T, P>> {
        self.states
            .insert(repo.to_owned(), NxBackendState::Starting { task })
            .and_then(NxBackendState::into_handle)
    }

    /// `repo`'s start reported a serving backend. It is installed only over that start; a report
    /// whose start was replaced, cleared or forgotten installs nothing. The project's waiters
    /// resume only once it is installed.
    pub(crate) fn ready(
        &mut self,
        repo: &str,
        backend: NxCacheBackend,
        process: P,
    ) -> Installed<T, P> {
        match self.states.remove(repo) {
            Some(NxBackendState::Starting { task }) => {
                self.states.insert(
                    repo.to_owned(),
                    NxBackendState::Ready {
                        backend,
                        process: Some(process),
                    },
                );
                Installed::Ready { start: task }
            }
            other => {
                self.restore(repo, other);
                Installed::Stale(process)
            }
        }
    }

    /// `repo`'s start failed at `now`: the project is refused until the cooldown ends. Its start's
    /// task, when it was that project's start; `None` for a report whose start is gone, whose
    /// waiters are then not refused.
    pub(crate) fn start_failed(&mut self, repo: &str, reason: String, now: Instant) -> Option<T> {
        match self.states.remove(repo) {
            Some(NxBackendState::Starting { task }) => {
                self.states.insert(
                    repo.to_owned(),
                    NxBackendState::Failed {
                        reason,
                        retry_after: now + self.cooldown,
                    },
                );
                Some(task)
            }
            other => {
                self.restore(repo, other);
                None
            }
        }
    }

    /// Forget `repo`'s start in flight, if one is, handing its task back: a start that reports
    /// later installs nothing.
    pub(crate) fn abandon(&mut self, repo: &str) -> Option<T> {
        match self.states.remove(repo) {
            Some(NxBackendState::Starting { task }) => Some(task),
            other => {
                self.restore(repo, other);
                None
            }
        }
    }

    /// Forget every start in flight, handing their tasks back: the gateway fails closed.
    pub(crate) fn abandon_starts(&mut self) -> Vec<T> {
        let starting: Vec<String> = self
            .states
            .iter()
            .filter(|(_, state)| matches!(state, NxBackendState::Starting { .. }))
            .map(|(repo, _)| repo.clone())
            .collect();
        starting
            .iter()
            .filter_map(|repo| self.abandon(repo))
            .collect()
    }

    /// The host configures `backend` for its project. A backend the gateway manages — starting,
    /// clearing or serving from a child it started — cannot be replaced this way, so the state it
    /// displaces holds no handle; any it did is handed back.
    pub(crate) fn configure(
        &mut self,
        backend: NxCacheBackend,
    ) -> Result<Option<Handle<T, P>>, &'static str> {
        if matches!(
            self.states.get(&backend.repo_id),
            Some(
                NxBackendState::Starting { .. }
                    | NxBackendState::Clearing
                    | NxBackendState::Ready {
                        process: Some(_),
                        ..
                    }
            )
        ) {
            return Err("the project's Nx cache backend is managed by the gateway");
        }
        backend.validate()?;
        Ok(self
            .states
            .insert(
                backend.repo_id.clone(),
                NxBackendState::Ready {
                    backend,
                    process: None,
                },
            )
            .and_then(NxBackendState::into_handle))
    }

    /// `repo`'s Ready backend: the only backend state a request may use.
    pub(crate) fn backend(&self, repo: &str) -> Option<&NxCacheBackend> {
        match self.states.get(repo) {
            Some(NxBackendState::Ready { backend, .. }) => Some(backend),
            _ => None,
        }
    }

    /// The child the gateway started for `repo`, for the actor to observe its exit.
    pub(crate) fn managed_child(&mut self, repo: &str) -> Option<&mut P> {
        match self.states.get_mut(repo) {
            Some(NxBackendState::Ready {
                process: Some(process),
                ..
            }) => Some(process),
            _ => None,
        }
    }

    /// Clear `repo`'s store while `usage` is its admitted and queued Nx work. Refused while any
    /// work is in flight or queued, while a start runs or a clear already does, and for a backend
    /// the host configured. Otherwise the project moves to `Clearing` and its started child, if
    /// any, is handed back to be stopped.
    pub(crate) fn clear(
        &mut self,
        repo: &str,
        usage: NxCacheUse,
    ) -> Result<Option<P>, NxCacheClearRefusal> {
        if !usage.is_idle() {
            return Err(NxCacheClearRefusal::Busy(usage));
        }
        let (kept, refusal) = match self.states.remove(repo) {
            Some(state @ NxBackendState::Starting { .. }) => (state, NxCacheClearRefusal::Starting),
            Some(NxBackendState::Clearing) => {
                (NxBackendState::Clearing, NxCacheClearRefusal::Clearing)
            }
            Some(state @ NxBackendState::Ready { process: None, .. }) => {
                (state, NxCacheClearRefusal::HostConfigured)
            }
            Some(NxBackendState::Ready { process, .. }) => {
                self.states
                    .insert(repo.to_owned(), NxBackendState::Clearing);
                return Ok(process);
            }
            Some(NxBackendState::Failed { .. }) | None => {
                self.states
                    .insert(repo.to_owned(), NxBackendState::Clearing);
                return Ok(None);
            }
        };
        self.states.insert(repo.to_owned(), kept);
        Err(refusal)
    }

    /// `repo`'s clear reported: the project has no backend until its next request starts one.
    pub(crate) fn cleared(&mut self, repo: &str) {
        if matches!(self.states.get(repo), Some(NxBackendState::Clearing)) {
            self.states.remove(repo);
        }
    }

    /// `repo`'s last session left the gateway: what the gateway started for it is forgotten — a
    /// start in flight, a started child, a failed start's cooldown — and its handle handed back. A
    /// backend the host configured and a clear in flight stay.
    pub(crate) fn release(&mut self, repo: &str) -> Released<T, P> {
        match self.states.get(repo) {
            Some(
                NxBackendState::Starting { .. }
                | NxBackendState::Ready {
                    process: Some(_), ..
                }
                | NxBackendState::Failed { .. },
            ) => Released::Forgot(
                self.states
                    .remove(repo)
                    .and_then(NxBackendState::into_handle),
            ),
            Some(NxBackendState::Ready { process: None, .. } | NxBackendState::Clearing) | None => {
                Released::Kept
            }
        }
    }

    /// Puts back a state a transition took out only to find it was not the one it acts on.
    fn restore(&mut self, repo: &str, state: Option<NxBackendState<T, P>>) {
        if let Some(state) = state {
            self.states.insert(repo.to_owned(), state);
        }
    }

    /// Every project's state, taken with its handle: the gateway drains or stops.
    pub(crate) fn take_all(&mut self) -> Vec<NxBackendState<T, P>> {
        self.states.drain().map(|(_, state)| state).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COOLDOWN: Duration = Duration::from_secs(5);

    /// Backends whose start tasks and children are labels, so each test names the exact handle a
    /// transition hands back.
    type Labels = NxBackends<&'static str, &'static str>;

    fn backends() -> Labels {
        NxBackends::new(COOLDOWN)
    }

    fn backend(repo: &str, port: u16) -> NxCacheBackend {
        NxCacheBackend {
            repo_id: repo.to_owned(),
            address: std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            reclaimed_staging_files: 0,
        }
    }

    /// `repo` served by `child`, started by `start`, which its report hands back.
    fn serving(backends: &mut Labels, repo: &str, start: &'static str, child: &'static str) {
        assert_eq!(backends.starting(repo, start), None);
        assert_eq!(
            backends.ready(repo, backend(repo, 9), child),
            Installed::Ready { start }
        );
    }

    fn admission(backends: &mut Labels, repo: &str, method: &Method, now: Instant) -> NxAdmission {
        let decision = backends.admit(repo, method, now, false);
        assert_eq!(decision.exited, None, "{repo}: no exit was seen");
        decision.admission
    }

    fn outage(reason: &str) -> NxAdmission {
        NxAdmission::Refuse(Refusal::BackendOutage(reason.to_owned()))
    }

    #[test]
    fn an_unsupported_method_is_refused_before_any_backend_starts_or_is_waited_for() {
        let now = Instant::now();
        let mut backends = backends();
        assert_eq!(backends.starting("owner/waiting", "start"), None);
        serving(&mut backends, "owner/serving", "served", "child");
        for method in [Method::HEAD, Method::POST, Method::DELETE] {
            for repo in ["owner/none", "owner/waiting", "owner/serving"] {
                assert_eq!(
                    backends.admit(repo, &method, now, true),
                    NxDecision {
                        admission: NxAdmission::Refuse(Refusal::NxMethodDenied),
                        exited: None,
                    },
                    "{method} {repo}: not even an exited child is replaced"
                );
            }
        }
        assert_eq!(
            admission(&mut backends, "owner/none", &Method::GET, now),
            NxAdmission::Start,
            "the refusals started nothing"
        );
    }

    #[test]
    fn one_start_serves_every_request_of_its_project_and_no_other() {
        let now = Instant::now();
        let mut backends = backends();
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            NxAdmission::Start
        );
        assert_eq!(backends.starting("owner/a", "start"), None);
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            NxAdmission::Starting
        );
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::PUT, now),
            NxAdmission::Starting
        );
        assert_eq!(
            admission(&mut backends, "owner/b", &Method::GET, now),
            NxAdmission::Start,
            "another project starts its own"
        );
        assert_eq!(
            backends.ready("owner/a", backend("owner/a", 9), "child"),
            Installed::Ready { start: "start" }
        );
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            NxAdmission::Ready
        );
        assert_eq!(
            backends
                .backend("owner/a")
                .map(|ready| ready.address.port()),
            Some(9)
        );
        assert!(backends.backend("owner/b").is_none());
    }

    #[test]
    fn a_report_without_its_start_installs_nothing_and_hands_its_child_back() {
        let now = Instant::now();
        let mut backends = backends();
        assert_eq!(
            backends.ready("owner/a", backend("owner/a", 9), "late child"),
            Installed::Stale("late child")
        );
        assert_eq!(
            backends.start_failed("owner/a", "late".to_owned(), now),
            None
        );
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            NxAdmission::Start
        );
        serving(&mut backends, "owner/a", "start", "child");
        assert_eq!(
            backends.ready("owner/a", backend("owner/a", 10), "second child"),
            Installed::Stale("second child"),
            "a second report does not replace the serving backend"
        );
        assert_eq!(
            backends
                .backend("owner/a")
                .map(|ready| ready.address.port()),
            Some(9)
        );
        assert_eq!(backends.starting("owner/b", "b"), None);
        assert_eq!(backends.starting("owner/c", "c"), None);
        assert_eq!(backends.abandon("owner/b"), Some("b"));
        assert_eq!(
            backends.ready("owner/b", backend("owner/b", 9), "b child"),
            Installed::Stale("b child"),
            "abandoned"
        );
        assert_eq!(
            backends.abandon("owner/a"),
            None,
            "only a start is abandoned"
        );
        assert!(backends.backend("owner/a").is_some());
        assert_eq!(backends.abandon_starts(), ["c"]);
        assert_eq!(
            backends.ready("owner/c", backend("owner/c", 9), "c child"),
            Installed::Stale("c child"),
            "every start abandoned"
        );
    }

    #[test]
    fn an_exited_child_is_handed_back_and_replaced_within_the_same_admission() {
        let now = Instant::now();
        let mut backends = backends();
        serving(&mut backends, "owner/a", "start", "child");
        assert_eq!(
            backends.admit("owner/a", &Method::GET, now, true),
            NxDecision {
                admission: NxAdmission::Start,
                exited: Some("child"),
            }
        );
        assert!(
            backends.managed_child("owner/a").is_none(),
            "the exited child is gone"
        );
        assert_eq!(
            backends.configure(backend("owner/host", 9)),
            Ok(None),
            "a host backend"
        );
        assert_eq!(
            backends.admit("owner/host", &Method::GET, now, true),
            NxDecision {
                admission: NxAdmission::Ready,
                exited: None,
            },
            "a host backend has no child whose exit could be seen"
        );
    }

    #[test]
    fn a_failed_start_refuses_its_project_until_the_cooldown_then_starts_again() {
        let failed = Instant::now();
        let mut backends = backends();
        assert_eq!(backends.starting("owner/a", "start"), None);
        assert_eq!(
            backends.start_failed("owner/a", "did not start".to_owned(), failed),
            Some("start")
        );
        let retry = failed + COOLDOWN;
        assert_eq!(
            admission(
                &mut backends,
                "owner/a",
                &Method::GET,
                retry - Duration::from_nanos(1)
            ),
            outage("did not start")
        );
        assert_eq!(
            admission(&mut backends, "owner/b", &Method::GET, failed),
            NxAdmission::Start,
            "other projects are unaffected"
        );
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, retry),
            NxAdmission::Start
        );
    }

    #[test]
    fn the_host_configures_no_backend_the_gateway_manages() {
        let now = Instant::now();
        let mut backends = backends();
        assert_eq!(backends.starting("owner/starting", "start"), None);
        serving(&mut backends, "owner/serving", "served", "child");
        serving(&mut backends, "owner/clearing", "cleared", "cleared child");
        assert_eq!(
            backends.clear("owner/clearing", NxCacheUse::default()),
            Ok(Some("cleared child"))
        );
        for repo in ["owner/starting", "owner/serving", "owner/clearing"] {
            assert!(backends.configure(backend(repo, 10)).is_err(), "{repo}");
        }
        assert_eq!(
            backends
                .backend("owner/serving")
                .map(|ready| ready.address.port()),
            Some(9)
        );
        assert_eq!(backends.starting("owner/failed", "failed"), None);
        assert_eq!(
            backends.start_failed("owner/failed", "down".to_owned(), now),
            Some("failed")
        );
        for repo in ["owner/failed", "owner/none"] {
            assert_eq!(backends.configure(backend(repo, 10)), Ok(None), "{repo}");
            assert_eq!(
                admission(&mut backends, repo, &Method::GET, now),
                NxAdmission::Ready
            );
        }
        assert!(
            backends.configure(backend("owner/none", 0)).is_err(),
            "an invalid backend"
        );
    }

    #[test]
    fn a_clear_waits_for_idle_and_takes_only_what_the_gateway_started() {
        let now = Instant::now();
        let mut backends = backends();
        serving(&mut backends, "owner/a", "start", "child");
        let busy = NxCacheUse {
            in_flight: [("ws".to_owned(), 1)].into(),
            queued: Default::default(),
        };
        assert_eq!(
            backends.clear("owner/a", busy.clone()),
            Err(NxCacheClearRefusal::Busy(busy))
        );
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            NxAdmission::Ready
        );
        assert_eq!(backends.starting("owner/starting", "starting"), None);
        assert_eq!(
            backends.clear("owner/starting", NxCacheUse::default()),
            Err(NxCacheClearRefusal::Starting)
        );
        assert_eq!(backends.configure(backend("owner/host", 9)), Ok(None));
        assert_eq!(
            backends.clear("owner/host", NxCacheUse::default()),
            Err(NxCacheClearRefusal::HostConfigured)
        );
        assert_eq!(
            backends.clear("owner/a", NxCacheUse::default()),
            Ok(Some("child"))
        );
        assert_eq!(
            backends.clear("owner/a", NxCacheUse::default()),
            Err(NxCacheClearRefusal::Clearing)
        );
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            outage("the project's Nx cache store is being cleared")
        );
        backends.cleared("owner/a");
        assert_eq!(
            admission(&mut backends, "owner/a", &Method::GET, now),
            NxAdmission::Start
        );
        assert_eq!(
            backends.clear("owner/never", NxCacheUse::default()),
            Ok(None),
            "a project with no backend clears too"
        );
    }

    #[test]
    fn a_projects_last_session_forgets_what_the_gateway_started_for_it_and_hands_it_back() {
        let now = Instant::now();
        let mut backends = backends();
        assert_eq!(backends.starting("owner/starting", "start"), None);
        serving(&mut backends, "owner/serving", "served", "child");
        assert_eq!(backends.starting("owner/failed", "failed"), None);
        assert_eq!(
            backends.start_failed("owner/failed", "down".to_owned(), now),
            Some("failed")
        );
        assert_eq!(backends.configure(backend("owner/host", 9)), Ok(None));
        serving(&mut backends, "owner/clearing", "cleared", "cleared child");
        assert_eq!(
            backends.clear("owner/clearing", NxCacheUse::default()),
            Ok(Some("cleared child"))
        );
        let forgotten = [
            ("owner/starting", Some(Handle::Start("start"))),
            ("owner/serving", Some(Handle::Child("child"))),
            ("owner/failed", None),
        ];
        for (repo, handle) in forgotten {
            assert_eq!(backends.release(repo), Released::Forgot(handle), "{repo}");
            assert_eq!(
                admission(&mut backends, repo, &Method::GET, now),
                NxAdmission::Start
            );
        }
        for repo in ["owner/host", "owner/clearing", "owner/none"] {
            assert_eq!(backends.release(repo), Released::Kept, "{repo}");
        }
        assert_eq!(
            admission(&mut backends, "owner/host", &Method::GET, now),
            NxAdmission::Ready
        );
    }
}
