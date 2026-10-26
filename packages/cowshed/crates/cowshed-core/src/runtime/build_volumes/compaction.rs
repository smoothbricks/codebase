//! Seed compaction (16_build_volumes.md, "Seed compaction"): once a land captures main's seed
//! past [`SEED_COMPACTION_EXTENTS`] and no compaction has succeeded within
//! [`SEED_COMPACTION_INTERVAL`], the land starts a background process
//! ([`SEED_COMPACTION_VERB`]) that copies the seed into contiguous space and publishes the copy
//! in its place. Only the refreshes, the clones and the publication take the target's image lock;
//! the copy holds nothing a fork or a land waits on, and it waits while a land runs.
//! A seed that replaces the copied one meanwhile is caught up to, writing only what it changed. A
//! compaction that fails is recorded, and no land of the build that failed starts another until
//! one succeeds.
use std::fs::{self, File, OpenOptions};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use super::{
    BuildVolumes, Owner, RuntimeStorageHost, SupersededSeeds, TargetLock, claim_superseded, io,
    seeds_of, storage,
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

/// At most one successful compaction per project within this long.
pub(crate) const SEED_COMPACTION_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

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

/// In [`crate::build_volume::BuildVolumeLayout::compacting`]: the lock one compaction at a time
/// holds, the file whose modification time is the last success, the last failure's record, and
/// the started process's log.
const LOCK: &str = "lock";
const SUCCEEDED: &str = "succeeded";
const FAILED: &str = "failed";
const LOG: &str = "log";

/// The last compaction that failed: the build that ran it and its error. A land writes it when it
/// cannot decide or start a compaction, the compaction process when it ends in an error, and a
/// success removes it.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Failure {
    build: BuildId,
    error: String,
}

/// A failed compaction a land holds back for: its error, and when it failed.
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

/// Start `program __compact-seed <project_root>` in a session of its own, so the land's terminal
/// and process group end nothing of it, with its stdout discarded and its stderr appended to the
/// log in `compacting`, which is created first: the first compaction of a project finds none. It
/// inherits no descriptor but its stdio: a held lock the land's command inherited (02_workspaces.md
/// "Held locks") would otherwise stay held for the whole copy. Returns the process and its log.
pub(crate) fn start_compaction(
    program: &Path,
    project_root: &Path,
    compacting: &Path,
) -> Result<(Child, PathBuf)> {
    use std::os::unix::process::CommandExt;
    fs::create_dir_all(compacting)
        .map_err(|error| io("create the seed compaction directory", compacting, &error))?;
    let log = compacting.join(LOG);
    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .map_err(|error| io("open the seed compaction log", &log, &error))?;
    let mut command = Command::new(program);
    command
        .arg(SEED_COMPACTION_VERB)
        .arg(project_root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(output);
    crate::exec::prepare_child_descriptors(&mut command).map_err(|failure| {
        io(
            "seal the seed compaction's descriptors",
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
        .map_err(|error| io("start the seed compaction", program, &error))?;
    Ok((process, log))
}
/// Whether a target's latest seed is due a compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactionDue {
    /// The seed holds at least `extents` extents, the bound, and no compaction succeeded within
    /// the interval.
    Due { extents: u64 },
    /// The seed holds `extents`, under the bound.
    Below { extents: u64 },
    /// A compaction succeeded at `at`, within the interval.
    Recent { at: SystemTime },
    /// The target has no published seed, or its backend keeps no extent map.
    NoSeed,
}

/// The decision from the last success, the clock and the seed's extent count, which is read only
/// once the interval has passed (`None`: no seed, or no extent map). A last success later than
/// `now` is recent: the clock moved back, and the interval is not proven to have passed.
pub(crate) fn decide(
    last: Option<SystemTime>,
    now: SystemTime,
    extents: impl FnOnce() -> Result<Option<u64>>,
) -> Result<CompactionDue> {
    if let Some(at) = last
        && now
            .duration_since(at)
            .map_or(true, |elapsed| elapsed < SEED_COMPACTION_INTERVAL)
    {
        return Ok(CompactionDue::Recent { at });
    }
    Ok(match extents()? {
        None => CompactionDue::NoSeed,
        Some(extents) if extents >= SEED_COMPACTION_EXTENTS => CompactionDue::Due { extents },
        Some(extents) => CompactionDue::Below { extents },
    })
}

/// This project's one compaction at a time: an exclusive `flock` of the compaction lock, taken
/// without waiting and let go when this is dropped.
#[must_use = "the compaction turn is let go when this is dropped"]
pub(crate) struct CompactionTurn {
    _lock: Fenced<File>,
    directory: PathBuf,
}

impl CompactionTurn {
    /// Record a success: the interval starts now, and no land holds back for an earlier failure.
    pub fn succeeded(&self) -> Result<()> {
        let succeeded = self.directory.join(SUCCEEDED);
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&succeeded)
            .and_then(|file| file.set_modified(SystemTime::now()))
            .map_err(|error| io("record the seed compaction's success", &succeeded, &error))?;
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
        // inherited descriptor sealed ([`start_compaction`]).
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

    /// Whether `target`'s latest seed is due a compaction ([`decide`]); its extents are counted no
    /// further than the bound.
    pub async fn compaction_due(&self, target: Owner) -> Result<CompactionDue> {
        self.blocking(move |host, layout| {
            let succeeded = layout.compacting().join(SUCCEEDED);
            let last = match fs::metadata(&succeeded) {
                Ok(metadata) => Some(metadata.modified().map_err(|error| {
                    io("read the last seed compaction's time", &succeeded, &error)
                })?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(io("inspect the last seed compaction", &succeeded, &error));
                }
            };
            decide(last, SystemTime::now(), || {
                let Some((seed, _)) = layout.seed_of(&target.name, &target.incarnation)? else {
                    return Ok(None);
                };
                host.seed_extents(layout, &seed, SEED_COMPACTION_EXTENTS)
                    .map(|extents| extents.map(|extents| extents.get()))
                    .map_err(storage)
            })
        })
        .await
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
    /// first chunk on, what `admit` answers ([`between_checks`]). A seed that replaces `source`'s
    /// meanwhile does not stop it: publication finds it and the copy is caught up to it.
    pub async fn copy_compaction(
        &self,
        source: CompactionSource,
        admit: Box<dyn FnMut() -> Admission + Send>,
    ) -> Result<SeedCompaction> {
        self.blocking(move |host, _| {
            host.copy_seed_compaction(source, &mut between_checks(admit))
                .map_err(storage)
        })
        .await
    }

    /// Catch `compacted` up to `next`, staged by [`Self::publish_compaction`], with no lock held:
    /// only what `next` does not share with the seed the copy holds is written, asking `admit`
    /// as the copy does.
    pub async fn catch_up_compaction(
        &self,
        compacted: CompactedSeed,
        next: CompactionSource,
        admit: Box<dyn FnMut() -> Admission + Send>,
    ) -> Result<SeedCatchUp> {
        self.blocking(move |host, _| {
            host.catch_up_seed_compaction(compacted, next, &mut between_checks(admit))
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

    const HOUR: Duration = Duration::from_secs(60 * 60);

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
    fn a_seed_under_the_bound_is_left_alone_and_one_past_it_is_due() {
        assert_eq!(
            decide(None, now(), || Ok(Some(SEED_COMPACTION_EXTENTS - 1))).unwrap(),
            CompactionDue::Below {
                extents: SEED_COMPACTION_EXTENTS - 1
            }
        );
        assert_eq!(
            decide(None, now(), || Ok(Some(SEED_COMPACTION_EXTENTS))).unwrap(),
            CompactionDue::Due {
                extents: SEED_COMPACTION_EXTENTS
            }
        );
        assert_eq!(
            decide(None, now(), || Ok(None)).unwrap(),
            CompactionDue::NoSeed
        );
    }

    #[test]
    fn a_success_within_the_interval_is_recent_without_counting_extents() {
        for last in [now() - 23 * HOUR, now() + HOUR] {
            assert_eq!(
                decide(Some(last), now(), || panic!("counted extents")).unwrap(),
                CompactionDue::Recent { at: last }
            );
        }
        assert_eq!(
            decide(Some(now() - 24 * HOUR), now(), || Ok(Some(
                SEED_COMPACTION_EXTENTS
            )))
            .unwrap(),
            CompactionDue::Due {
                extents: SEED_COMPACTION_EXTENTS
            }
        );
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
    /// repository's turn through an inheritable descriptor, and a process that kept it would hold
    /// the turn for its whole copy. Here the land holds an exclusive lock through such a
    /// descriptor, starts the process and closes its own: the lock is free while the process still
    /// runs. The program stands in for the cowshed binary; it says it started through one FIFO and
    /// waits on another, so nothing is timed.
    #[test]
    fn the_first_compaction_starts_holding_none_of_the_lands_descriptors() {
        use std::io::{Read as _, Write as _};
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::PermissionsExt as _;
        let directory = std::env::temp_dir().join(format!(
            "compaction-fds-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        fs::create_dir_all(&directory).expect("directory");
        let program = directory.join("compact");
        fs::write(
            &program,
            "#!/bin/sh\nprintf started > \"$2/ready\"\nread line < \"$2/go\"\n",
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
        let ticket = directory.join("ticket");
        let held = File::create(&ticket).expect("ticket");
        held.try_lock().expect("lock the fresh ticket");
        // SAFETY: a plain descriptor operation on a descriptor this test owns.
        unsafe {
            assert_eq!(
                libc::fcntl(held.as_raw_fd(), libc::F_SETFD, 0),
                0,
                "inheritable"
            );
        }
        let compacting = directory.join("build/.compacting");
        let (mut process, log) =
            start_compaction(&program, &directory, &compacting).expect("started");
        assert_eq!(log, compacting.join(LOG));
        let mut said = String::new();
        File::open(&ready)
            .expect("the process's ready FIFO")
            .read_to_string(&mut said)
            .expect("the process says it started");
        assert_eq!(said, "started");
        drop(held);
        let again = File::open(&ticket).expect("ticket again");
        let locked = again.try_lock();
        OpenOptions::new()
            .write(true)
            .open(&go)
            .and_then(|mut go| go.write_all(b"\n"))
            .expect("let the process end");
        let status = process.wait_bounded().expect("reap the process");
        assert!(
            locked.is_ok(),
            "the running compaction process keeps the land's lock: {locked:?}"
        );
        assert!(status.success(), "the stand-in process ended {status}");
        fs::remove_dir_all(&directory).expect("cleanup");
    }
}
