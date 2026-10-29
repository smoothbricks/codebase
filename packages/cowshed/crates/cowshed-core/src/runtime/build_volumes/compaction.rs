//! Seed compaction (16_build_volumes.md, "Seed compaction"): necessary main-seed capture
//! or attach/mount work over ten seconds advises a detached compactor. Only the compactor
//! counts the refreshed immutable clone immediately before copying. The copy holds nothing
//! a fork or land waits on, catches up to newer seeds, and holds while a land runs.
//! A failure holds later advice by the same cowshed build until a compaction succeeds.
use std::fs::{self, File, OpenOptions};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use super::{
    BuildVolumes, Owner, RuntimeStorageHost, StoreSpace, SupersededSeeds, TargetLock,
    claim_superseded, io, seeds_of, storage,
};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeRecord, BuildVolumeRole, Registrant, Registration,
};
use crate::runtime::project::SEED_COMPACTION_VERB;
use crate::runtime::supervisor_socket::BuildId;
use crate::storage::apfs::native::{CompactedSeed, CompactionSource, SeedCatchUp, SeedCompaction};
use crate::{CowshedError, Result};
use cowshed_os::fork_lock::Fenced;

/// A seed holding this many extents is compacted. Measured 2026-10-25 on this host: a fork's
/// first write costs 92 to 175 µs per extent of its seed's map (SmoothBricks' 880,639-extent
/// main seed: 81.5 s for the fork's volume and 101.4 s for its own seed's capture; a second project's
/// 749,095: 131.0 s), and a fork pays it twice, so 500,000 extents cost a fork about 140 s. One
/// compaction of that project's 209 GB seed wrote 209 GB in 440 s and left 104,655 extents (31.7 s per
/// first write), so it is repaid after three or four forks. A copy cannot go below the image's
/// data regions (57,320 there), which keeps a compacted seed far under this bound.
pub(crate) const SEED_COMPACTION_EXTENTS: u64 = 500_000;

/// How many bytes a compaction copies between asking whether to go on.
const CHECK_EVERY_BYTES: u64 = 1 << 30;

/// How many times a compaction's copy is caught up to a seed that replaced the one it holds
/// before the compaction gives up. A catch-up writes only what main's seed changed since the last
/// one, so each takes the time of one land's or one refresh's delta, not of the whole seed; a
/// seed replaced this often while such deltas are written is replaced faster than the copy can
/// follow, and the next land's compaction starts again.
pub(crate) const SEED_COMPACTION_CATCH_UPS: usize = 8;

/// How long a compaction's copy holds before it checks again while a land runs.
pub(crate) const SEED_COMPACTION_WAIT: Duration = Duration::from_secs(15);
/// What a compaction's copy is told at a check (16_build_volumes.md, "Seed compaction").
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    /// No land holds a turn: go on copying.
    Copy,
    /// A land holds a turn, and the asker has waited for it a while: check
    /// the seed and ask again.
    Wait,
    /// Stop, because of this: whether to wait cannot be decided, and not knowing is no permission.
    Stop(String),
}

/// In the compaction directory: the one-at-a-time lock, last failure and child log.
const LOCK: &str = "lock";
const FAILED: &str = "failed";
const LOG: &str = "log";

/// The last compaction that failed: the build that ran it and its error. Foreground advice writes it when it
/// cannot decide or start a compaction, the compaction process when it ends in an error, and a
/// success removes it.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Failure {
    build: BuildId,
    error: String,
}

/// A failed compaction foreground advice holds back for: its error, and when it failed.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Held {
    pub error: String,
    pub at: SystemTime,
}

/// Whether a land of `build` holds back for `recorded`, written at `at`: only when `build` is the
/// one that failed. Another build carries whatever fix the failed one lacked, so its land starts
/// a compaction again.
pub(crate) fn held(recorded: Failure, at: SystemTime, build: &BuildId) -> Option<Held> {
    (recorded.build == *build).then_some(Held {
        error: recorded.error,
        at,
    })
}

/// Whole seconds from `at` to now; a time after now (the clock moved back) is 0.
pub(crate) fn seconds_since(at: SystemTime) -> u64 {
    SystemTime::now()
        .duration_since(at)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// Why a compaction's copy waits for lands, from the queues the gateway answered
/// ([`crate::gateway_sessions::GatewayControl::land_turns`]; 05_gateway.md "Land turns"):
/// `Ok(None)` when no repository's turn is held, and a gateway that is not running holds none,
/// since the turns are its own state; `Err`, why the copy stops, when they cannot be read.
pub(crate) fn lands_hold(
    turns: std::result::Result<
        Vec<crate::api::dto::LandTurnQueue>,
        crate::gateway_sessions::GatewayStatusError,
    >,
) -> std::result::Result<Option<String>, String> {
    use crate::gateway_sessions::GatewayStatusError;
    let queues = match turns {
        Ok(queues) => queues,
        Err(GatewayStatusError::Absent) => return Ok(None),
        Err(GatewayStatusError::Waited(failure)) => {
            return Err(format!(
                "whether a land holds a turn cannot be read: {}",
                failure.message
            ));
        }
        Err(GatewayStatusError::Control(error)) => {
            return Err(format!(
                "whether a land holds a turn cannot be read: {error}"
            ));
        }
    };
    let held = queues
        .iter()
        .filter_map(|queue| {
            let (holder, waiting) = queue.tickets.split_first()?;
            Some(format!(
                "{} by {} (pid {}, {} waiting)",
                queue.repo,
                holder.workspace,
                holder.pid,
                waiting.len()
            ))
        })
        .collect::<Vec<_>>();
    Ok((!held.is_empty()).then(|| format!("lands hold turns: {}", held.join(", "))))
}

/// Start `program <verb> <project_root>`, a background cowshed verb of the project (the seed
/// compaction, the collection a land starts), in a session of its own, so the land's terminal and
/// process group end nothing of it, with its stdout discarded and its stderr appended to the log
/// in `directory`, which is created first: the project's first run finds none. It inherits no
/// descriptor but its stdio: a held lock the land's command inherited (02_workspaces.md "Held
/// locks") would otherwise stay held for the whole run. Returns the process and its log.
pub(crate) fn start_background_verb(
    program: &Path,
    verb: &str,
    project_root: &Path,
    directory: &Path,
) -> Result<(Child, PathBuf)> {
    use std::os::unix::process::CommandExt;
    fs::create_dir_all(directory)
        .map_err(|error| io(&format!("create the {verb} directory"), directory, &error))?;
    let log = directory.join(LOG);
    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|error| io(&format!("open the {verb} log"), &log, &error))?;
    let mut command = Command::new(program);
    command
        .arg(verb)
        .arg(project_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(output);
    crate::exec::prepare_child_descriptors(&mut command).map_err(|failure| {
        io(
            &format!("seal the {verb} process's descriptors"),
            program,
            &failure.source,
        )
    })?;
    // SAFETY: `setsid` between fork and exec is async-signal-safe and touches only the child.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let process = cowshed_os::fork_lock::Spawn::spawn_locked(&mut command)
        .map_err(|error| io(&format!("start {verb}"), program, &error))?;
    Ok((process, log))
}
/// Whether necessary work on main's seed advises a background compaction. The receipt
/// covers one capture or the actual attach plus mount, never queueing, fsck or metadata.
pub(crate) fn advised(main: bool, operation_elapsed: Option<Duration>) -> bool {
    main && operation_elapsed.is_some_and(|elapsed| elapsed > Duration::from_secs(10))
}

/// This project's one compaction at a time: an exclusive `flock` of the compaction lock, taken
/// without waiting and let go when this is dropped.
#[must_use = "the compaction turn is let go when this is dropped"]
pub(crate) struct CompactionTurn {
    _lock: Fenced<File>,
    directory: PathBuf,
}

impl CompactionTurn {
    /// A successful publication removes the earlier failure; success is not a timing guard.
    pub fn succeeded(&self) -> Result<()> {
        let failed = self.directory.join(FAILED);
        match fs::remove_file(&failed) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(
                "remove the failed seed compaction's record",
                &failed,
                &error,
            )),
        }
    }
}

/// The copy's and a catch-up's `keep_going`: from the first chunk on, every
/// [`CHECK_EVERY_BYTES`] written, what `admit` answers; while it answers [`Admission::Wait`]
/// the writer holds where it is and asks again.
fn between_checks(
    mut admit: Box<dyn FnMut() -> Admission + Send>,
) -> impl FnMut(u64) -> ControlFlow<String> {
    let mut next = 0_u64;
    move |bytes| {
        if bytes < next {
            return ControlFlow::Continue(());
        }
        next = bytes + CHECK_EVERY_BYTES;
        loop {
            match admit() {
                Admission::Copy => return ControlFlow::Continue(()),
                Admission::Wait => {}
                Admission::Stop(why) => return ControlFlow::Break(why),
            }
        }
    }
}

/// `checks`, first asking at each [`CHECK_EVERY_BYTES`], from the first chunk on, whether the store
/// holding `store` has room for what the writer still writes, `total` less what it wrote: a copy
/// that would take the store under its floor plus margin stops there, and its partial copy goes to
/// cleanup as any stopped copy's does (16_build_volumes.md, "Seed compaction").
fn with_room(
    store: PathBuf,
    total: u64,
    space: StoreSpace,
    mut checks: impl FnMut(u64) -> ControlFlow<String>,
) -> impl FnMut(u64) -> ControlFlow<String> {
    let mut next = 0_u64;
    move |written| {
        if written >= next {
            next = written + CHECK_EVERY_BYTES;
            if let Some(why) = room(&store, total.saturating_sub(written), space) {
                return ControlFlow::Break(why);
            }
        }
        checks(written)
    }
}

/// Why writing `bytes` more into the store holding `store` must not go on, or `None`: it would leave
/// the store under its floor plus margin, or its free space cannot be read, and not knowing is no
/// permission.
fn room(store: &Path, bytes: u64, space: StoreSpace) -> Option<String> {
    use crate::storage::apfs::release::{
        PRESSURE_MARGIN_PERCENT, STORE_FLOOR_PERCENT, writes_leave_floor,
    };
    match space(store) {
        Err(error) => Some(format!(
            "the store's free space cannot be read at {}: {error}",
            store.display()
        )),
        Ok((free, capacity)) => (!writes_leave_floor(free, capacity, bytes)).then(|| {
            format!(
                "{bytes} more bytes into the store's {free} free of {capacity} would leave it under its \
                 {STORE_FLOOR_PERCENT}% floor plus {PRESSURE_MARGIN_PERCENT}% margin, and retiring the old \
                 seed frees none of the copy: its blocks stay shared with every volume cloned from it"
            )
        }),
    }
}
/// What publishing a compacted copy did.
#[derive(Debug)]
pub(crate) enum Published {
    /// `replacement` is the target's seed now, holding `seed`'s bytes, and `seed` and every older
    /// seed are claimed in `superseded`, for [`BuildVolumes::retire_superseded`] once the target's
    /// image lock is let go.
    Replaced {
        seed: BuildVolumeId,
        replacement: BuildVolumeId,
        superseded: SupersededSeeds,
    },
    /// `compacted`'s seed is no longer the target's latest: `next`, a clone of the latest, was
    /// staged under the lock for the copy's catch-up.
    Behind {
        compacted: CompactedSeed,
        next: CompactionSource,
    },
    /// The target has no seed any more.
    Seedless,
}

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    /// Foreground advice uses only the operation receipt. No extent provider is reachable here.
    /// Start failures are values and retain the same-build hold rather than failing a fork or land.
    pub fn advise_compaction(
        &self,
        target: &Owner,
        operation_elapsed: Option<Duration>,
        project_root: &Path,
    ) -> crate::api::dto::LandSeedCompaction {
        use crate::api::dto::LandSeedCompaction;
        if !target.name.is_main() {
            return LandSeedCompaction::NotMain;
        }
        match self.state_of(project_root) {
            Ok(Some((_, state))) if state.mbx.is_some() => return LandSeedCompaction::NotAdvised,
            Ok(_) => {}
            Err(error) => {
                return LandSeedCompaction::Failed {
                    error: error.message,
                };
            }
        }
        if !advised(true, operation_elapsed) {
            return LandSeedCompaction::NotAdvised;
        }
        let start = || -> Result<LandSeedCompaction> {
            if let Some(held) = self.compaction_held()? {
                return Ok(LandSeedCompaction::Held {
                    error: held.error,
                    seconds_ago: seconds_since(held.at),
                });
            }
            let executable = std::env::current_exe().map_err(|error| {
                io("find this cowshed executable", Path::new("cowshed"), &error)
            })?;
            let (mut child, log) = start_background_verb(
                &executable,
                SEED_COMPACTION_VERB,
                project_root,
                &self.layout.compacting(),
            )?;
            let pid = child.id();
            // WHY unbounded: a detached reaper, with no waiter, collects the child when it ends.
            #[allow(clippy::disallowed_methods)]
            let reaper = std::thread::Builder::new()
                .name("seed-compaction-reaper".to_owned())
                .spawn(move || {
                    if let Err(error) = child.wait() {
                        cowshed_os::eprintln!("cowshed: the seed compaction process {pid} could not be reaped: {error}");
                    }
                });
            if let Err(error) = reaper {
                let mut failure = io("reap the seed compaction", &executable, &error);
                if let Err(recording) = self.record_compaction_failure(&failure.message) {
                    failure.message = format!(
                        "{}; recording that failure failed too: {}",
                        failure.message, recording.message
                    );
                }
                cowshed_os::eprintln!(
                    "cowshed: seed compaction process {pid} started, logging to {}, but its reaper could not be created: {}",
                    log.display(),
                    failure.message
                );
            }
            Ok(LandSeedCompaction::Started { pid, log })
        };
        match start() {
            Ok(report) => report,
            Err(mut error) => {
                if let Err(recording) = self.record_compaction_failure(&error.message) {
                    error.message = format!(
                        "{}; recording that failure failed too: {}",
                        error.message, recording.message
                    );
                }
                LandSeedCompaction::Failed {
                    error: error.message,
                }
            }
        }
    }

    /// Take this project's compaction turn without waiting, or `None` while another process holds
    /// it.
    pub fn try_compaction_turn(&self) -> Result<Option<CompactionTurn>> {
        let directory = self.layout.compacting();
        fs::create_dir_all(&directory)
            .map_err(|error| io("create the seed compaction directory", &directory, &error))?;
        // WHY lock: two compactions of one project would each copy the whole seed (hundreds of
        // GB) and race to publish it, and the processes lands start have no live owner to queue
        // through. It is admission, never a wait: a second process answers `Running` and exits.
        // The descriptor is close-on-exec from its `open` and held in a `Fenced`, so no child of
        // this process holds the lease past `exec`, and the process itself starts with every
        // inherited descriptor sealed ([`start_background_verb`]).
        let path = directory.join(LOCK);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|error| io("open the seed compaction lock", &path, &error))?;
        let file = Fenced::new(file);
        match file.try_lock() {
            Ok(()) => Ok(Some(CompactionTurn {
                _lock: file,
                directory,
            })),
            Err(fs::TryLockError::WouldBlock) => Ok(None),
            Err(fs::TryLockError::Error(error)) => {
                Err(io("take the seed compaction lock", &path, &error))
            }
        }
    }

    /// The failure a land of this build holds back for ([`held`]), or `None`.
    pub fn compaction_held(&self) -> Result<Option<Held>> {
        let path = self.layout.compacting().join(FAILED);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(io(
                    "open the failed seed compaction's record",
                    &path,
                    &error,
                ));
            }
        };
        let at = file
            .metadata()
            .and_then(|metadata| metadata.modified())
            .map_err(|error| io("read the failed seed compaction's time", &path, &error))?;
        let recorded: Failure = serde_json::from_reader(file).map_err(|error| {
            CowshedError::integrity(
                format!(
                    "the failed seed compaction's record at {} is unreadable: {error}",
                    path.display()
                ),
                "cowshed doctor --json",
            )
        })?;
        Ok(held(recorded, at, BuildId::current()?))
    }

    /// Record that a compaction by this build failed with `error`: no land of this build starts
    /// another until one succeeds. Written aside and renamed into place, so a reader sees a whole
    /// record or none.
    pub fn record_compaction_failure(&self, error: &str) -> Result<()> {
        let directory = self.layout.compacting();
        fs::create_dir_all(&directory)
            .map_err(|error| io("create the seed compaction directory", &directory, &error))?;
        let record = serde_json::to_vec(&Failure {
            build: BuildId::current()?.clone(),
            error: error.to_owned(),
        })
        .map_err(|error| {
            CowshedError::internal(format!(
                "cannot encode the failed seed compaction's record: {error}"
            ))
        })?;
        let path = directory.join(FAILED);
        let staged = directory.join(format!("{FAILED}.{}", uuid::Uuid::new_v4().simple()));
        fs::write(&staged, record)
            .and_then(|()| fs::rename(&staged, &path))
            .map_err(|error| io("record the failed seed compaction", &path, &error))
    }

    /// Hand what a compaction left in `build/.compacting` to the gateway's cleanup, under the
    /// project's compaction turn and with no image lock held: an interrupted run's, before the
    /// target's image lock is first taken, and this run's, before it answers. Each is one rename
    /// into `build/.deleting` and a nudge; the gateway frees it.
    pub async fn release_compaction_leftovers(&self) -> Result<()> {
        self.blocking(move |host, layout| {
            host.release_seed_compaction_leftovers(layout)
                .map_err(storage)
        })
        .await
    }
    /// Clone `locked`'s target's latest seed aside for its compaction, or `None` when it has
    /// none. Under the target's image lock, so no land or reseed retires the seed mid-clone.
    pub async fn stage_compaction(
        &self,
        locked: &TargetLock<H>,
    ) -> Result<Option<CompactionSource>> {
        let target = locked.target.clone();
        self.blocking(move |host, layout| {
            for live in layout.owned_by(&target.name)? {
                let (mount, _) = host.mount_build_volume(layout, &live).map_err(storage)?;
                if crate::build_volume::BuildVolumeState::read(&mount)?
                    .mbx
                    .is_some()
                {
                    return Ok(None);
                }
            }
            let Some((seed, _)) = layout.seed_of(&target.name, &target.incarnation)? else {
                return Ok(None);
            };
            host.stage_seed_compaction(layout, &seed)
                .map(Some)
                .map_err(storage)
        })
        .await
    }

    /// Copy `source` contiguously with no lock held, asking every [`CHECK_EVERY_BYTES`], from the
    /// first chunk on, whether the store has room for what the copy still writes ([`with_room`]) and
    /// what `admit` answers ([`between_checks`]). A seed that replaces `source`'s meanwhile does not
    /// stop it: publication finds it and the copy is caught up to it.
    pub async fn copy_compaction(
        &self,
        source: CompactionSource,
        minimum_extents: u64,
        admit: Box<dyn FnMut() -> Admission + Send>,
    ) -> Result<SeedCompaction> {
        let store = self.layout.compacting();
        let space = self.store_space;
        self.blocking(move |host, _| {
            let total = source.allocated_bytes().map_err(|error| {
                io(
                    "read the staged seed's allocation",
                    source.clone_path(),
                    &error,
                )
            })?;
            let mut keep_going = with_room(store, total, space, between_checks(admit));
            host.copy_seed_compaction(source, minimum_extents, &mut keep_going)
                .map_err(storage)
        })
        .await
    }

    /// Catch `compacted` up to `next`, staged by [`Self::publish_compaction`], with no lock held:
    /// only what `next` does not share with the seed the copy holds is written, asking for room
    /// and `admit` as the copy does.
    pub async fn catch_up_compaction(
        &self,
        compacted: CompactedSeed,
        next: CompactionSource,
        admit: Box<dyn FnMut() -> Admission + Send>,
    ) -> Result<SeedCatchUp> {
        let store = self.layout.compacting();
        let space = self.store_space;
        self.blocking(move |host, _| {
            let mut keep_going = with_room(store, 0, space, between_checks(admit));
            host.catch_up_seed_compaction(compacted, next, &mut keep_going)
                .map_err(storage)
        })
        .await
    }
    /// Under `locked`: when `compacted`'s seed is still the target's latest, the copy becomes
    /// its seed at the same tree and every older seed is claimed for its retirement once the lock
    /// is let go, as a fork's seed refresh claims the seeds it replaces. When another seed is the
    /// latest, it is cloned aside for the copy's catch-up. Nothing in `build/.compacting` is
    /// released here: the caller releases it with no lock held.
    pub async fn publish_compaction(
        &self,
        locked: &TargetLock<H>,
        compacted: CompactedSeed,
    ) -> Result<Published> {
        let target = locked.target.clone();
        self.blocking(move |host, layout| {
            let Some((seed, record)) = layout.seed_of(&target.name, &target.incarnation)? else {
                return Ok(Published::Seedless);
            };
            if seed != *compacted.seed() {
                let next = host.stage_seed_compaction(layout, &seed).map_err(storage)?;
                return Ok(Published::Behind { compacted, next });
            }
            let previous = seeds_of(layout, &target)?;
            let replacement = BuildVolumeId::mint();
            let registration = Registration::new(
                BuildVolumeRecord::new(
                    record.tree,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
                Registrant::Workspace {
                    incarnation: target.incarnation.clone(),
                },
            );
            host.publish_seed_compaction(layout, compacted, &replacement, registration)
                .map_err(storage)?;
            let superseded = claim_superseded(host, layout, &target, previous)?;
            Ok(Published::Replaced {
                seed,
                replacement,
                superseded,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::test_backstop::ChildBackstop as _;

    #[test]
    fn a_gateway_that_is_not_running_holds_no_land_turn_and_one_unread_stops_the_copy() {
        use crate::api::dto::{LandTurnName, LandTurnQueue};
        use crate::gateway_sessions::GatewayStatusError;
        use crate::land_turn::Ticket;
        let ticket = |number, workspace: &str| Ticket {
            number,
            pid: 40 + u32::try_from(number).unwrap(),
            workspace: workspace.to_owned(),
            asked_unix_ms: 0,
            command: format!("land {workspace}"),
            bound: Vec::new(),
        };
        let queue = |repo: &str, tickets| LandTurnQueue {
            repo: LandTurnName::new(repo).unwrap(),
            tickets,
            abandoned: Vec::new(),
        };
        assert_eq!(lands_hold(Err(GatewayStatusError::Absent)), Ok(None));
        assert_eq!(lands_hold(Ok(Vec::new())), Ok(None));
        assert_eq!(lands_hold(Ok(vec![queue("idle", Vec::new())])), Ok(None));
        assert_eq!(
            lands_hold(Ok(vec![
                queue("acme", vec![ticket(1, "topic"), ticket(2, "next")]),
                queue("other", vec![ticket(3, "fix")]),
            ])),
            Ok(Some(
                "lands hold turns: acme by topic (pid 41, 1 waiting), other by fix (pid 43, 0 waiting)"
                    .to_owned()
            ))
        );
        assert_eq!(
            lands_hold(Err(GatewayStatusError::Control("refused".to_owned()))),
            Err("whether a land holds a turn cannot be read: refused".to_owned())
        );
    }

    fn now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000)
    }

    #[test]
    fn only_necessary_main_seed_work_strictly_over_ten_seconds_advises() {
        for main in [false, true] {
            for (elapsed, slow) in [
                (None, false),
                (Some(Duration::from_secs(9)), false),
                (Some(Duration::from_secs(10)), false),
                (
                    Some(Duration::from_secs(10) + Duration::from_nanos(1)),
                    true,
                ),
            ] {
                assert_eq!(
                    advised(main, elapsed),
                    main && slow,
                    "main={main}, elapsed={elapsed:?}"
                );
            }
        }
    }

    #[test]
    fn a_land_holds_back_only_for_its_own_builds_failure() {
        let failure = |build: &str| Failure {
            build: BuildId::named(build),
            error: "the copy failed".to_owned(),
        };
        assert_eq!(
            held(failure("a"), now(), &BuildId::named("a")),
            Some(Held {
                error: "the copy failed".to_owned(),
                at: now(),
            })
        );
        assert_eq!(held(failure("a"), now(), &BuildId::named("b")), None);
    }

    /// The first compaction of a project starts with no compaction directory, and the process
    /// holds none of the land's descriptors: a land run under `cowshed land-turn` holds its
    /// repository's turn through an inheritable descriptor, and a process that kept it would
    /// hold the turn for its whole copy. The oracle is the child's own descriptor table, not
    /// the ticket's lock: under libtest this process's inheritable descriptors also leak into
    /// other tests' children, so a free lock after `drop(held)` says nothing about this
    /// child. The ticket is duplicated to a known inheritable number and the stand-in
    /// program reports whether that number is open under its own `/dev/fd`, saying so
    /// through one FIFO and waiting on another, so nothing is timed.
    #[test]
    fn the_first_compaction_starts_holding_none_of_the_lands_descriptors() {
        use std::io::{Read as _, Write as _};
        use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
        use std::os::unix::fs::PermissionsExt as _;
        let directory =
            crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-compaction-fds");
        let ticket = directory.join("ticket");
        let held = File::create(&ticket).expect("ticket");
        held.try_lock().expect("lock the fresh ticket");
        // SAFETY: `fcntl` on a descriptor this test owns; the result is checked before the
        // descriptor it may have made is wrapped. F_DUPFD clears FD_CLOEXEC, so the child
        // inherits this number unless start_background_verb seals it.
        let duped = unsafe { libc::fcntl(held.as_raw_fd(), libc::F_DUPFD, 64) };
        assert!(
            duped >= 64,
            "dup the ticket to an inheritable number: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: `duped` is the fresh descriptor checked above, owned by this test alone.
        let _inheritable = unsafe { OwnedFd::from_raw_fd(duped) };
        let program = directory.join("compact");
        fs::write(
            &program,
            format!(
                "#!/bin/sh\nif [ -e /dev/fd/{duped} ]; then printf open > \"$2/ready\"; else printf closed > \"$2/ready\"; fi\nread line < \"$2/go\"\n"
            ),
        )
        .expect("program");
        fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).expect("executable");
        let fifo = |name: &str| {
            let path = directory.join(name);
            let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("path");
            // SAFETY: `name` is a NUL-terminated path that outlives the call.
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0, "mkfifo");
            path
        };
        let (ready, go) = (fifo("ready"), fifo("go"));
        let compacting = directory.join("build/.compacting");
        let (mut process, log) =
            start_background_verb(&program, SEED_COMPACTION_VERB, &directory, &compacting)
                .expect("started");
        assert_eq!(log, compacting.join(LOG));
        let mut said = String::new();
        File::open(&ready)
            .expect("the process's ready FIFO")
            .read_to_string(&mut said)
            .expect("the process reports its descriptor table");
        OpenOptions::new()
            .write(true)
            .open(&go)
            .and_then(|mut go| go.write_all(b"\n"))
            .expect("let the process end");
        let status = process.wait_bounded().expect("reap the process");
        assert_eq!(
            said, "closed",
            "the compaction process started holding the land's ticket descriptor {duped}"
        );
        assert!(status.success(), "the stand-in process ended {status}");
    }
}
