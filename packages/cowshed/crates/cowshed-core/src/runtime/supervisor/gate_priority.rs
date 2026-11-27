//! Land's gate token withdrawal and macOS `PRIO_DARWIN_BG` fallback policy, window evaluation,
//! and persistent `gate-watch` client for one workspace supervisor (05_gateway.md "A land's gate
//! takes tokens back").

use std::collections::BTreeSet;
#[cfg(target_os = "macos")]
use std::collections::{HashMap, HashSet};
#[cfg(target_os = "macos")]
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(target_os = "macos")]
use std::sync::Mutex;
#[cfg(target_os = "macos")]
use std::time::{Duration, Instant};

use cowshed_os::deadline;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
#[cfg(target_os = "macos")]
use tokio::sync::oneshot;
use tokio::sync::{Notify, mpsc, watch};

use crate::api::dto::{ExecClass, JobId};
#[cfg(target_os = "macos")]
use crate::api::resources::CpuMicros;
use crate::gate_watch::{GateWatchEvent, GateWatchOpen, GateWatchState};
#[cfg(target_os = "macos")]
use crate::runtime::job_cpu::wait_span_off;
#[cfg(target_os = "macos")]
use crate::runtime::process_monitor::ProcessMonitor;
#[cfg(target_os = "macos")]
use crate::runtime::process_tree::ProcessIdentity;

use super::ProcessEvent;

/// Grace period from drain start before opening the first 3 s CPU measurement window.
#[cfg(target_os = "macos")]
pub(super) const DARWIN_BG_GRACE: Duration = Duration::from_secs(1);

/// Measurement window over which a withdrawn job's process-tree CPU gain is compared against its
/// remaining held tokens (05_gateway.md "A job that does not respect tokens, by what it does").
#[cfg(target_os = "macos")]
pub(super) const DARWIN_BG_WINDOW: Duration = Duration::from_secs(3);

/// Threshold per held token (the implicit first token counted): 1.5 cores = 1500 millicores.
#[cfg(target_os = "macos")]
pub(super) const DARWIN_BG_THRESHOLD_MILLICORES_PER_TOKEN: u64 = 1500;

/// Host idle CPU threshold below which a job running past its tokens is backgrounded, and at or
/// above which a backgrounded job returns to its class: one core idle = 1000 millicores
/// (05_gateway.md "A land's gate takes tokens back").
#[cfg(target_os = "macos")]
pub(super) const DARWIN_BG_HOST_IDLE_THRESHOLD_MILLICORES: u32 = 1000;

/// Whether a job of `class` is withdrawn from while a land's gate runs.
///
/// - `Gate` is never withdrawn from (a train's proof included).
/// - `Background` and `Resident` are always withdrawn from.
/// - `Interactive` and `InteractiveResident` are withdrawn from only once their asks have left
///   the 120 s `interactive` first band (`interactive_past_band`).
pub(super) const fn should_withdraw_class(class: ExecClass, interactive_past_band: bool) -> bool {
    match class {
        ExecClass::Gate => false,
        ExecClass::Background | ExecClass::Resident => true,
        ExecClass::Interactive | ExecClass::InteractiveResident => interactive_past_band,
    }
}

/// How many jobserver tokens a job still holds (the implicit first token counted), given its
/// `granted` count and `withdrawn` bytes drained by the supervisor from its fifo.
#[cfg(target_os = "macos")]
pub(super) fn held_tokens(granted: NonZeroUsize, withdrawn: usize) -> u32 {
    u32::try_from(granted.get().saturating_sub(withdrawn).max(1)).unwrap_or(u32::MAX)
}

/// Average CPU in millicores gained by a job's process tree between two distinct process-tree
/// samples (`start` at window open, `end` at window close). Returns `None` if both readings are
/// from the same sample (never decides from one sample).
#[cfg(target_os = "macos")]
pub(super) fn window_cpu_millicores(
    start: (Instant, CpuMicros),
    end: (Instant, CpuMicros),
) -> Option<u32> {
    if end.0 <= start.0 {
        return None;
    }
    let elapsed_us = end.0.saturating_duration_since(start.0).as_micros();
    if elapsed_us == 0 {
        return None;
    }
    let delta_us = u128::from(end.1.get().saturating_sub(start.1.get()));
    let millicores = delta_us.saturating_mul(1000) / elapsed_us;
    Some(u32::try_from(millicores).unwrap_or(u32::MAX))
}

/// Whether `cpu_millicores` exceeds `1.5` cores per `held_tokens` (strictly greater).
#[cfg(target_os = "macos")]
pub(super) const fn exceeds_token_budget(cpu_millicores: u32, held_tokens: u32) -> bool {
    (cpu_millicores as u64)
        > (held_tokens as u64).saturating_mul(DARWIN_BG_THRESHOLD_MILLICORES_PER_TOKEN)
}

/// Average idle CPU across the host in millicores between two cumulative `HOST_CPU_LOAD_INFO`
/// tick snapshots (`start` at window open, `end` at window close), scaled by `cores` online
/// cores: `idle_delta * cores * 1000 / total_delta`. Returns `None` when `cores == 0`, when any
/// counter went backwards, or when the two readings are identical.
#[cfg(target_os = "macos")]
pub(super) fn host_idle_millicores(
    start: procinfo::HostCpuTicks,
    end: procinfo::HostCpuTicks,
    cores: u32,
) -> Option<u32> {
    if cores == 0 {
        return None;
    }
    let user = end.user.checked_sub(start.user)?;
    let system = end.system.checked_sub(start.system)?;
    let idle = end.idle.checked_sub(start.idle)?;
    let nice = end.nice.checked_sub(start.nice)?;
    let total = u128::from(user) + u128::from(system) + u128::from(idle) + u128::from(nice);
    if total == 0 {
        return None;
    }
    let millicores = u128::from(idle)
        .saturating_mul(u128::from(cores))
        .saturating_mul(1000)
        / total;
    Some(u32::try_from(millicores).unwrap_or(u32::MAX))
}

/// Action decided at the end of a macOS `PRIO_DARWIN_BG` measurement window (05_gateway.md
/// "A land's gate takes tokens back").
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DarwinBgStep {
    Set,
    Clear,
    Keep,
}

/// Pure window decision for macOS `PRIO_DARWIN_BG`:
/// - Not set, job over its token budget, and host had less than one core idle (< 1000
///   millicores) -> `Set`.
/// - Currently set and host had at least one core idle (>= 1000 millicores) -> `Clear`.
/// - Otherwise -> `Keep`.
#[cfg(target_os = "macos")]
pub(super) const fn darwinbg_step(
    set: bool,
    over_budget: bool,
    host_idle_millicores: u32,
) -> DarwinBgStep {
    if !set {
        if over_budget && host_idle_millicores < DARWIN_BG_HOST_IDLE_THRESHOLD_MILLICORES {
            DarwinBgStep::Set
        } else {
            DarwinBgStep::Keep
        }
    } else if host_idle_millicores >= DARWIN_BG_HOST_IDLE_THRESHOLD_MILLICORES {
        DarwinBgStep::Clear
    } else {
        DarwinBgStep::Keep
    }
}

/// Both window-open samples captured at grace (or the previous window close): the job's own
/// process-tree CPU sample and the host's cumulative CPU ticks snapshot.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug)]
pub(super) struct DarwinBgWindowStart {
    pub(super) job_cpu: (Instant, CpuMicros),
    pub(super) host_cpu: crate::host_cpu::Snapshot,
}

/// Active `PRIO_DARWIN_BG` state detached from [`JobDarwinBg`] so the actor can run the off-actor
/// clear without holding a borrow of `self.jobs` across `.await` and while keeping the window task
/// alive when clearing mid-span.
#[cfg(target_os = "macos")]
pub(super) struct ActiveClear {
    applied: Arc<Mutex<HashMap<ProcessIdentity, i32>>>,
    clear_stop: Option<oneshot::Sender<bool>>,
    clear_task: Option<tokio::task::JoinHandle<DarwinBgClearResult>>,
}

#[cfg(target_os = "macos")]
impl ActiveClear {
    /// Complete clearing for the detached active lives: joins the out-of-turn clear task
    /// (triggering it now if still waiting), clears any residual life added after that task
    /// snapshotted (and any child whose ancestor is in the recorded map), and returns all readback
    /// outcomes for actor span recording.
    pub(super) async fn finish(
        mut self,
        monitor: Option<&ProcessMonitor>,
    ) -> Vec<(u32, crate::runtime::placement::DarwinBgOutcome)> {
        if let Some(stop) = self.clear_stop.take() {
            let _ = stop.send(true);
        }
        let (already_cleared, mut outcomes) = match self.clear_task.take() {
            Some(task) => task.await.unwrap_or_default(),
            None => (HashMap::new(), Vec::new()),
        };
        let mut remaining: HashMap<ProcessIdentity, i32> =
            std::mem::take(&mut *self.applied.lock().expect("darwinbg applied lock poisoned"));
        if let Some(monitor) = monitor {
            let mut known = already_cleared.clone();
            known.extend(remaining.iter().map(|(&life, &ceiling)| (life, ceiling)));
            let parents = parent_map(&monitor.parent_links());
            for &life in monitor.running_lives().iter() {
                if !known.contains_key(&life)
                    && let Some(ceiling) = nearest_recorded_ancestor_ceiling(life, &known, &parents)
                {
                    known.insert(life, ceiling);
                    remaining.insert(life, ceiling);
                }
            }
        }
        remaining.retain(|life, _| !already_cleared.contains_key(life));
        if !remaining.is_empty() {
            let to_clear: Vec<_> = remaining.into_iter().collect();
            let extra = super::off_actor::run(move || clear_darwinbg_from_lives(to_clear)).await;
            outcomes.extend(extra);
        }
        outcomes
    }
}

/// Result of one per-job out-of-turn `PRIO_DARWIN_BG` clear task: the exact lives already cleared
/// with their recorded ceilings, and each life's `PROC_PIDTHREADINFO` readback outcome.
#[cfg(target_os = "macos")]
type DarwinBgClearResult = (
    HashMap<ProcessIdentity, i32>,
    Vec<(u32, crate::runtime::placement::DarwinBgOutcome)>,
);

/// Per-job macOS `PRIO_DARWIN_BG` measurement and active state while a land's gate runs.
#[cfg(target_os = "macos")]
pub(super) struct JobDarwinBg {
    window_task: Option<tokio::task::JoinHandle<()>>,
    pub(super) window_start: Option<DarwinBgWindowStart>,
    pub(super) active: bool,
    applied: Arc<Mutex<HashMap<ProcessIdentity, i32>>>,
    /// Lives observed already at ceiling 4 with no recorded ancestor (not cowshed's); never set,
    /// recorded, or re-probed while this backgrounding span holds.
    skipped: HashSet<ProcessIdentity>,
    clear_stop: Option<oneshot::Sender<bool>>,
    clear_task: Option<tokio::task::JoinHandle<DarwinBgClearResult>>,
}

#[cfg(target_os = "macos")]
impl JobDarwinBg {
    /// Start the one-shot 1 s grace timer followed by 3 s measurement windows for `job_id`.
    /// No clear task is spawned until `PRIO_DARWIN_BG` is actually applied (`arm_clear_task`).
    pub(super) fn start(job_id: JobId, events: mpsc::Sender<ProcessEvent>) -> Self {
        let task = tokio::spawn(async move {
            tokio::time::sleep(DARWIN_BG_GRACE).await;
            if events
                .send(ProcessEvent::DarwinBgGraceElapsed { job_id })
                .await
                .is_err()
            {
                return;
            }
            loop {
                tokio::time::sleep(DARWIN_BG_WINDOW).await;
                if events
                    .send(ProcessEvent::DarwinBgWindowElapsed { job_id })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Self {
            window_task: Some(task),
            window_start: None,
            active: false,
            applied: Arc::new(Mutex::new(HashMap::new())),
            skipped: HashSet::new(),
            clear_stop: None,
            clear_task: None,
        }
    }

    pub(super) fn stop_window_task(&mut self) {
        if let Some(task) = self.window_task.take() {
            task.abort();
        }
    }

    pub(super) fn record_applied(&self, lives: &[(ProcessIdentity, i32)]) {
        let mut guard = self.applied.lock().expect("darwinbg applied lock poisoned");
        for &(life, ceiling) in lives {
            guard.entry(life).or_insert(ceiling);
        }
    }

    pub(super) fn record_skipped(&mut self, lives: &[ProcessIdentity]) {
        for &life in lives {
            self.skipped.insert(life);
        }
    }

    pub(super) fn unseen_lives(&self, running: &[ProcessIdentity]) -> Vec<ProcessIdentity> {
        let guard = self.applied.lock().expect("darwinbg applied lock poisoned");
        running
            .iter()
            .copied()
            .filter(|life| !guard.contains_key(life) && !self.skipped.contains(life))
            .collect()
    }

    pub(super) fn applied_map(&self) -> HashMap<ProcessIdentity, i32> {
        self.applied
            .lock()
            .expect("darwinbg applied lock poisoned")
            .clone()
    }

    pub(super) fn snapshot_applied(&self) -> Vec<crate::runtime::job_groups::RecordedDarwinBgLife> {
        let mut entries: Vec<_> = self
            .applied
            .lock()
            .expect("darwinbg applied lock poisoned")
            .iter()
            .map(|(&life, &ceiling)| {
                crate::runtime::job_groups::RecordedDarwinBgLife::new(life, ceiling)
            })
            .collect();
        entries.sort_unstable_by_key(|entry| (entry.pid, entry.birth.0));
        entries
    }

    /// Spawn the per-job out-of-turn clear task once `PRIO_DARWIN_BG` is first applied to this
    /// job. When the gate span watch turns off (without waiting for an actor turn), or when the
    /// actor signals `finish_clear`, the task clears every applied life plus any running child
    /// whose ancestor is in `applied`, on the blocking pool via [`super::off_actor::run`].
    pub(super) fn arm_clear_task(
        &mut self,
        monitor: Option<ProcessMonitor>,
        mut span: watch::Receiver<bool>,
    ) {
        if self.clear_task.is_some() {
            return;
        }
        let applied = Arc::clone(&self.applied);
        let (stop_tx, mut stop_rx) = oneshot::channel::<bool>();
        let task = tokio::spawn(async move {
            let should_clear = tokio::select! {
                biased;
                stop = &mut stop_rx => stop.unwrap_or(false),
                () = wait_span_off(&mut span) => true,
            };
            if !should_clear {
                return (HashMap::new(), Vec::new());
            }
            let mut lives: HashMap<ProcessIdentity, i32> = {
                applied
                    .lock()
                    .expect("darwinbg applied lock poisoned")
                    .clone()
            };
            if let Some(monitor) = &monitor {
                let parents = parent_map(&monitor.parent_links());
                let mut progress = true;
                let running = monitor.running_lives();
                while progress {
                    progress = false;
                    for &life in running.iter() {
                        if !lives.contains_key(&life)
                            && let Some(ceiling) =
                                nearest_recorded_ancestor_ceiling(life, &lives, &parents)
                        {
                            lives.insert(life, ceiling);
                            progress = true;
                        }
                    }
                }
            }
            if lives.is_empty() {
                return (HashMap::new(), Vec::new());
            }
            let to_clear: Vec<_> = lives
                .iter()
                .map(|(&life, &ceiling)| (life, ceiling))
                .collect();
            let outcomes = super::off_actor::run(move || clear_darwinbg_from_lives(to_clear)).await;
            (lives, outcomes)
        });
        self.clear_stop = Some(stop_tx);
        self.clear_task = Some(task);
    }

    /// Detach the active `PRIO_DARWIN_BG` lives and out-of-turn clear task for clearing while
    /// leaving `window_task` running so the job is judged again from its next window.
    pub(super) fn take_active_clear(&mut self) -> ActiveClear {
        self.active = false;
        self.skipped.clear();
        let applied = std::mem::replace(&mut self.applied, Arc::new(Mutex::new(HashMap::new())));
        ActiveClear {
            applied,
            clear_stop: self.clear_stop.take(),
            clear_task: self.clear_task.take(),
        }
    }

    /// Complete clearing for this job inside `end_job_withdrawal`: stops the window task, joins
    /// the out-of-turn clear task (triggering it now if still waiting, e.g. on job exit), clears
    /// any residual life added after that task snapshotted, and returns all readback outcomes for
    /// actor span recording.
    pub(super) async fn finish_clear(
        mut self,
        monitor: Option<&ProcessMonitor>,
    ) -> Vec<(u32, crate::runtime::placement::DarwinBgOutcome)> {
        self.stop_window_task();
        self.take_active_clear().finish(monitor).await
    }
}

#[cfg(target_os = "macos")]
impl Drop for JobDarwinBg {
    fn drop(&mut self) {
        self.stop_window_task();
        if let Some(stop) = self.clear_stop.take() {
            let _ = stop.send(false);
        }
    }
}

#[cfg(target_os = "macos")]
fn parent_map(
    parent_links: &[(ProcessIdentity, ProcessIdentity)],
) -> HashMap<ProcessIdentity, ProcessIdentity> {
    parent_links.iter().copied().collect()
}

#[cfg(target_os = "macos")]
fn nearest_recorded_ancestor_ceiling(
    life: ProcessIdentity,
    recorded: &HashMap<ProcessIdentity, i32>,
    parents: &HashMap<ProcessIdentity, ProcessIdentity>,
) -> Option<i32> {
    let mut current = life;
    let mut steps = 0usize;
    while let Some(&parent) = parents.get(&current) {
        steps += 1;
        if steps > parents.len() {
            break;
        }
        if let Some(&ceiling) = recorded.get(&parent) {
            return Some(ceiling);
        }
        current = parent;
    }
    None
}

/// Pure ceiling resolution for a batch of candidate lives (05_gateway.md "A land's gate takes
/// tokens back"):
/// - A live process whose observed `pth_maxpriority` is strictly above `4` keeps that ceiling.
/// - A live process observed at `4` (or below) inherits its nearest recorded ancestor's ceiling
///   (walking `parent_links`, including any ancestor resolved earlier in the same batch).
/// - With no recorded ancestor, a process observed at `4` is skipped (already BACKGROUND-clamped
///   or self-BG, not cowshed's) and returned in the second vector so it is never set, recorded, or
///   cleared.
/// - An exited process (`None`) is ignored.
#[cfg(target_os = "macos")]
pub(super) fn resolve_batch_ceilings(
    observed: &[(ProcessIdentity, Option<i32>)],
    already_recorded: &HashMap<ProcessIdentity, i32>,
    parent_links: &[(ProcessIdentity, ProcessIdentity)],
) -> (Vec<(ProcessIdentity, i32)>, Vec<ProcessIdentity>) {
    let parents = parent_map(parent_links);
    let mut known = already_recorded.clone();
    let mut resolved = Vec::new();
    let mut pending_at_bg = Vec::new();
    for &(life, ceiling) in observed {
        let Some(ceiling) = ceiling else {
            continue;
        };
        if ceiling > crate::runtime::placement::DARWIN_BG_MAX_PRIORITY {
            known.entry(life).or_insert(ceiling);
            resolved.push((life, ceiling));
        } else {
            pending_at_bg.push(life);
        }
    }
    loop {
        let mut next_pending = Vec::new();
        let mut progress = false;
        for life in pending_at_bg {
            if let Some(ceiling) = nearest_recorded_ancestor_ceiling(life, &known, &parents) {
                known.entry(life).or_insert(ceiling);
                resolved.push((life, ceiling));
                progress = true;
            } else {
                next_pending.push(life);
            }
        }
        pending_at_bg = next_pending;
        if !progress {
            break;
        }
    }
    (resolved, pending_at_bg)
}

/// Read the live pre-set `pth_maxpriority` ceiling of each verified life on the blocking pool,
/// before writing the ledger or calling `setpriority(PRIO_DARWIN_BG)`.
#[cfg(target_os = "macos")]
pub(super) fn observe_pre_set_ceilings(
    lives: Vec<ProcessIdentity>,
) -> Vec<(ProcessIdentity, Option<i32>)> {
    let mut observed = Vec::with_capacity(lives.len());
    for life in lives {
        let Ok(pid_i32) = i32::try_from(life.pid) else {
            continue;
        };
        match crate::runtime::job_groups::life_of(pid_i32) {
            Ok(Some(uid)) if uid == life.birth.0 => {}
            _ => continue,
        }
        match crate::runtime::placement::thread_ceiling(life.pid) {
            Ok(ceiling) => observed.push((life, ceiling)),
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: reading pre-set priority ceiling of pid {} failed: {error}",
                    life.pid
                );
            }
        }
    }
    observed
}

/// Apply `PRIO_DARWIN_BG` to each verified live process in `lives` on the blocking pool, reading
/// back each via `PROC_PIDTHREADINFO`.
#[cfg(target_os = "macos")]
pub(super) fn apply_darwinbg_to_lives(
    lives: Vec<ProcessIdentity>,
) -> Vec<(u32, crate::runtime::placement::DarwinBgOutcome)> {
    let mut outcomes = Vec::with_capacity(lives.len());
    for life in lives {
        let Ok(pid_i32) = i32::try_from(life.pid) else {
            continue;
        };
        match crate::runtime::job_groups::life_of(pid_i32) {
            Ok(Some(uid)) if uid == life.birth.0 => {}
            _ => continue,
        }
        match crate::runtime::placement::set_darwinbg(life.pid) {
            Ok(outcome) => outcomes.push((life.pid, outcome)),
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: setting PRIO_DARWIN_BG on pid {} failed: {error}",
                    life.pid
                );
            }
        }
    }
    outcomes
}

/// Clear `PRIO_DARWIN_BG` from each verified live process in `lives` on the blocking pool,
/// confirming each via `PROC_PIDTHREADINFO` against its recorded pre-set ceiling.
#[cfg(target_os = "macos")]
pub(super) fn clear_darwinbg_from_lives(
    lives: Vec<(ProcessIdentity, i32)>,
) -> Vec<(u32, crate::runtime::placement::DarwinBgOutcome)> {
    let mut outcomes = Vec::with_capacity(lives.len());
    for (life, ceiling) in lives {
        let Ok(pid_i32) = i32::try_from(life.pid) else {
            continue;
        };
        match crate::runtime::job_groups::life_of(pid_i32) {
            Ok(Some(uid)) if uid == life.birth.0 => {}
            _ => continue,
        }
        match crate::runtime::placement::clear_darwinbg(life.pid, Some(ceiling)) {
            Ok(outcome) => outcomes.push((life.pid, outcome)),
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: clearing PRIO_DARWIN_BG on pid {} failed: {error}",
                    life.pid
                );
            }
        }
    }
    outcomes
}

/// One persistent `gate-watch` client task per supervisor, opening its bidirectional connection to
/// `<store>/gateway.sock` when the gateway says `hello` (`Request::Hello` on the supervisor
/// socket), publishing the live gate span on a `watch::Sender<bool>` so drainers and darwinbg
/// clearers react to span-off without waiting for an actor turn, forwarding `GateWatchState` for
/// actor bookkeeping, and waiting for the next `hello` whenever the watch ends.
pub(super) struct GateWatchClient {
    commands: mpsc::UnboundedSender<GateWatchEvent>,
    span: watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl GateWatchClient {
    pub(super) fn start(
        gateway_socket: PathBuf,
        ledger: Option<PathBuf>,
        hello: Arc<Notify>,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (span_tx, span_rx) = watch::channel(false);
        let task = tokio::spawn(run_watch_loop(
            gateway_socket,
            ledger,
            rx,
            hello,
            span_tx,
            events,
        ));
        Self {
            commands: tx,
            span: span_rx,
            task,
        }
    }

    /// Subscribe to the live gate span state published directly by `run_watch_loop`.
    pub(super) fn span(&self) -> watch::Receiver<bool> {
        self.span.clone()
    }

    /// Current live gate span state as last published by `run_watch_loop`.
    pub(super) fn span_active(&self) -> bool {
        *self.span.borrow()
    }

    pub(super) fn gate_start(&self, job_id: JobId) {
        let _ = self.commands.send(GateWatchEvent::GateStart {
            job_id: job_id.get(),
        });
    }

    pub(super) fn gate_end(&self, job_id: JobId) {
        let _ = self.commands.send(GateWatchEvent::GateEnd {
            job_id: job_id.get(),
        });
    }
}

impl Drop for GateWatchClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Ensures the published span watch always flips to `false` on any exit of `run_watch_loop`
/// (closed command channel, closed event channel, or task abort).
struct SpanGuard(watch::Sender<bool>);

impl SpanGuard {
    fn set(&self, active: bool) {
        self.0.send_if_modified(|current| {
            let changed = *current != active;
            *current = active;
            changed
        });
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        self.set(false);
    }
}

async fn write_json_line(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    value: &impl serde::Serialize,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

fn record_local_gate(active_local_gates: &mut BTreeSet<u64>, event: &GateWatchEvent) {
    match event {
        GateWatchEvent::GateStart { job_id } => {
            active_local_gates.insert(*job_id);
        }
        GateWatchEvent::GateEnd { job_id } => {
            active_local_gates.remove(job_id);
        }
    }
}

async fn clear_active(
    last_active: &mut bool,
    span: &SpanGuard,
    events: &mpsc::Sender<ProcessEvent>,
) -> bool {
    // Publish span-off first so drainers and macOS darwinbg clear tasks reset without waiting for
    // the actor's `GateWatchState` turn (or an `events` channel that may be full behind a stall).
    span.set(false);
    if !*last_active {
        return true;
    }
    *last_active = false;
    events
        .send(ProcessEvent::GateWatchState {
            active: false,
            heartbeat: false,
        })
        .await
        .is_ok()
}

async fn run_watch_loop(
    gateway_socket: PathBuf,
    ledger: Option<PathBuf>,
    mut commands: mpsc::UnboundedReceiver<GateWatchEvent>,
    hello: Arc<Notify>,
    span_tx: watch::Sender<bool>,
    events: mpsc::Sender<ProcessEvent>,
) {
    let span = SpanGuard(span_tx);
    let mut active_local_gates = BTreeSet::<u64>::new();
    let mut last_active = false;
    let peer = deadline::Peer::unix("gateway", &gateway_socket);

    loop {
        // Wait for the gateway's `hello` event before opening or re-opening the watch (no retry
        // timer). Local gate start/end events arriving while disconnected update
        // `active_local_gates` so they are replayed once `hello` establishes the watch.
        loop {
            tokio::select! {
                cmd = commands.recv() => {
                    let Some(event) = cmd else {
                        return;
                    };
                    record_local_gate(&mut active_local_gates, &event);
                }
                () = hello.notified() => {
                    break;
                }
            }
        }

        let connect_deadline = deadline::start("gate-watch", deadline::HANDSHAKE);
        let Ok(stream) = deadline::connect_unix(&connect_deadline, &peer, &gateway_socket).await
        else {
            if !clear_active(&mut last_active, &span, &events).await {
                return;
            }
            continue;
        };
        let (reader, mut writer) = stream.into_split();
        let open = GateWatchOpen::new(ledger.clone());
        if write_json_line(&mut writer, &open).await.is_err() {
            if !clear_active(&mut last_active, &span, &events).await {
                return;
            }
            continue;
        }
        let mut replayed = true;
        for &job_id in &active_local_gates {
            if write_json_line(&mut writer, &GateWatchEvent::GateStart { job_id })
                .await
                .is_err()
            {
                replayed = false;
                break;
            }
        }
        if !replayed {
            if !clear_active(&mut last_active, &span, &events).await {
                return;
            }
            continue;
        }

        let mut lines = BufReader::new(reader).lines();
        // Armed once at connect and reset only when a valid gateway frame arrives; local
        // `gate-start`/`gate-end` commands never extend the gateway silence deadline.
        let silence = tokio::time::sleep(deadline::HEARTBEAT_SILENCE);
        tokio::pin!(silence);
        loop {
            tokio::select! {
                cmd = commands.recv() => {
                    let Some(event) = cmd else {
                        return;
                    };
                    record_local_gate(&mut active_local_gates, &event);
                    if write_json_line(&mut writer, &event).await.is_err() {
                        break;
                    }
                }
                line = lines.next_line() => {
                    match line {
                        Ok(Some(line)) => {
                            match serde_json::from_str::<GateWatchState>(&line) {
                                Ok(state) if state.ok => {
                                    silence.as_mut().reset(
                                        tokio::time::Instant::now() + deadline::HEARTBEAT_SILENCE,
                                    );
                                    span.set(state.active);
                                    last_active = state.active;
                                    if events
                                        .send(ProcessEvent::GateWatchState {
                                            active: state.active,
                                            heartbeat: true,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                _ => break,
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                () = &mut silence => {
                    // Silence past `HEARTBEAT_SILENCE` (15 s): treat gate state as off.
                    break;
                }
            }
        }
        if !clear_active(&mut last_active, &span, &events).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn withdrawal_scope_table_matches_spec() {
        let cases = [
            (ExecClass::Gate, false, false),
            (ExecClass::Gate, true, false),
            (ExecClass::Background, false, true),
            (ExecClass::Background, true, true),
            (ExecClass::Resident, false, true),
            (ExecClass::Resident, true, true),
            (ExecClass::Interactive, false, false),
            (ExecClass::Interactive, true, true),
            (ExecClass::InteractiveResident, false, false),
            (ExecClass::InteractiveResident, true, true),
        ];
        for (class, past_band, expected) in cases {
            assert_eq!(
                should_withdraw_class(class, past_band),
                expected,
                "{class:?} past_band={past_band}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn held_tokens_and_window_threshold_table() {
        let four = NonZeroUsize::new(4).unwrap();
        assert_eq!(held_tokens(four, 0), 4);
        assert_eq!(held_tokens(four, 1), 3);
        assert_eq!(held_tokens(four, 2), 2);
        assert_eq!(held_tokens(four, 3), 1);
        assert_eq!(held_tokens(four, 4), 1);

        let t0 = Instant::now();
        let t3 = t0 + Duration::from_secs(3);
        let us = |v| CpuMicros::new(v).unwrap();

        // Same timestamp -> no window (never decide from one sample).
        assert_eq!(
            window_cpu_millicores((t0, us(0)), (t0, us(1_000_000))),
            None
        );
        // 4.5 CPU-seconds over 3 s = 1500 millicores -> does NOT exceed 1.5 cores for 1 token.
        let mc = window_cpu_millicores((t0, us(1_000_000)), (t3, us(5_500_000))).unwrap();
        assert_eq!(mc, 1500);
        assert!(!exceeds_token_budget(mc, 1));
        // 4.503 CPU-seconds over 3 s = 1501 millicores -> exceeds 1.5 cores for 1 token.
        let mc_over = window_cpu_millicores((t0, us(1_000_000)), (t3, us(5_503_000))).unwrap();
        assert_eq!(mc_over, 1501);
        assert!(exceeds_token_budget(mc_over, 1));
        assert!(!exceeds_token_budget(mc_over, 2));
        // 2 tokens allow up to 3000 millicores.
        assert!(!exceeds_token_budget(3000, 2));
        assert!(exceeds_token_budget(3001, 2));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn host_idle_millicores_table() {
        use procinfo::HostCpuTicks;

        let ticks = |user, system, idle, nice| HostCpuTicks {
            user,
            system,
            idle,
            nice,
        };
        let cases = [
            // Identical readings -> None.
            (ticks(10, 20, 30, 40), ticks(10, 20, 30, 40), 8, None),
            // Zero online cores -> None.
            (ticks(0, 0, 0, 0), ticks(100, 0, 100, 0), 0, None),
            // Any counter going backwards -> None.
            (ticks(10, 20, 30, 40), ticks(9, 25, 35, 45), 8, None),
            (ticks(10, 20, 30, 40), ticks(15, 19, 35, 45), 8, None),
            (ticks(10, 20, 30, 40), ticks(15, 25, 29, 45), 8, None),
            (ticks(10, 20, 30, 40), ticks(15, 25, 35, 39), 8, None),
            // Fully busy (0 idle ticks) on 8 cores -> 0 millicores.
            (ticks(100, 50, 200, 0), ticks(800, 150, 200, 0), 8, Some(0)),
            // Fully idle on 8 cores -> 8000 millicores.
            (
                ticks(100, 50, 200, 0),
                ticks(100, 50, 1000, 0),
                8,
                Some(8000),
            ),
            // Half idle on 8 cores -> 4000 millicores.
            (ticks(0, 0, 0, 0), ticks(400, 0, 400, 0), 8, Some(4000)),
            // Exactly 1 core idle on 8 cores (1/8 idle) -> 1000 millicores.
            (ticks(0, 0, 0, 0), ticks(700, 0, 100, 0), 8, Some(1000)),
            // Just below 1 core idle on 8 cores -> 990 millicores.
            (ticks(0, 0, 0, 0), ticks(701, 0, 99, 0), 8, Some(990)),
            // Mixed user/system/nice/idle on 4 cores: 250 / 1000 idle -> 1000 millicores.
            (
                ticks(10, 20, 30, 40),
                ticks(310, 220, 280, 290),
                4,
                Some(1000),
            ),
        ];
        for (start, end, cores, expected) in cases {
            assert_eq!(
                host_idle_millicores(start, end, cores),
                expected,
                "start={start:?} end={end:?} cores={cores}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn darwinbg_step_table() {
        let cases = [
            // Not set, over budget, idle host (>= 1000 mcores) -> never sets.
            (false, true, 1000, DarwinBgStep::Keep),
            (false, true, 1001, DarwinBgStep::Keep),
            (false, true, 4000, DarwinBgStep::Keep),
            // Not set, over budget, no idle core (< 1000 mcores) -> sets.
            (false, true, 0, DarwinBgStep::Set),
            (false, true, 500, DarwinBgStep::Set),
            (false, true, 999, DarwinBgStep::Set),
            // Not set, under budget -> never sets regardless of host idle.
            (false, false, 0, DarwinBgStep::Keep),
            (false, false, 999, DarwinBgStep::Keep),
            (false, false, 1000, DarwinBgStep::Keep),
            (false, false, 4000, DarwinBgStep::Keep),
            // Set, idle core returns (>= 1000 mcores) -> clears regardless of job budget.
            (true, true, 1000, DarwinBgStep::Clear),
            (true, true, 1001, DarwinBgStep::Clear),
            (true, true, 4000, DarwinBgStep::Clear),
            (true, false, 1000, DarwinBgStep::Clear),
            (true, false, 4000, DarwinBgStep::Clear),
            // Set, still no idle core (< 1000 mcores) -> keeps set regardless of job budget.
            (true, true, 0, DarwinBgStep::Keep),
            (true, true, 500, DarwinBgStep::Keep),
            (true, true, 999, DarwinBgStep::Keep),
            (true, false, 0, DarwinBgStep::Keep),
            (true, false, 999, DarwinBgStep::Keep),
        ];
        for (set, over_budget, host_idle, expected) in cases {
            assert_eq!(
                darwinbg_step(set, over_budget, host_idle),
                expected,
                "set={set} over_budget={over_budget} host_idle={host_idle}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn resolve_batch_ceilings_records_observed_inherits_from_ancestor_and_skips_pre_bg() {
        use crate::runtime::process_tree::BirthToken;

        let id = |pid, birth| ProcessIdentity {
            pid,
            birth: BirthToken(birth),
        };
        let root = id(10, 1);
        let child = id(11, 2);
        let grandchild = id(12, 3);
        let pre_bg = id(20, 4);
        let pre_bg_child = id(21, 5);
        let exited = id(30, 6);

        let links = vec![(child, root), (grandchild, child), (pre_bg_child, pre_bg)];

        // Initial set: unclamped/utility lives keep their ceiling; a pre-BG process and its child
        // are skipped; an exited process is ignored.
        let (resolved, skipped) = resolve_batch_ceilings(
            &[
                (root, Some(20)),
                (pre_bg, Some(4)),
                (pre_bg_child, Some(4)),
                (exited, None),
            ],
            &HashMap::new(),
            &links,
        );
        assert_eq!(resolved, vec![(root, 20)]);
        assert_eq!(skipped, vec![pre_bg, pre_bg_child]);

        // Later wake while active: grandchild born at 4 walks past exited/unrecorded `child` to
        // `root`'s recorded ceiling 20; a child of skipped `pre_bg` has no recorded ancestor and is
        // skipped.
        let recorded = HashMap::from([(root, 20)]);
        let (resolved_later, skipped_later) = resolve_batch_ceilings(
            &[(grandchild, Some(4)), (pre_bg_child, Some(4))],
            &recorded,
            &links,
        );
        assert_eq!(resolved_later, vec![(grandchild, 20)]);
        assert_eq!(skipped_later, vec![pre_bg_child]);
    }
}
