//! The project side of build volumes (16_build_volumes.md): a fork clones its target's seed, a
//! land quiesces the landing workspace, freezes the target's seed, and moves the target's one
//! link, a rebase carries its target's Nx cache entries into the rebased workspace's volume, a
//! reseed refreezes a target's seed from its own quiet volume, and collection deletes what
//! nothing links.
//!
//! Every step here runs inside one process's project actor, so no two of that process's steps
//! interleave; another cowshed process's can, which is why collection defers what a workspace
//! still being created may own ([`Links::creating`]). What a crash leaves behind is a volume
//! nothing links, which [`BuildVolumes::collect`] deletes.

use std::collections::BTreeSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::apfs::SystemCommandRunner;
use crate::api::dto::{
    AdoptionSkip, CarrySide, DatabaseHolder, GcCandidate, GcDeferred, GcReason, GitOid, NxCarry,
    RebaseBuildVolume, RebaseCarrySkip, Reseed, ReseedSkip, Sha256Digest,
};
use crate::build_volume::{
    BuildStateRefresh, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole,
    BuildVolumeState, TrackedBuildStateRefusal, cargo, carry, link, nx,
};
use crate::capabilities::BuildStatePath;
use crate::metadata::{ImageCapacity, WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::ApfsStorageError;
use crate::storage::apfs::native::{BuildVolumeRelease, MacOsApfsExecutionHost};
use crate::storage::lifecycle::ResizeOutcome;
use crate::{CowshedError, Result};

type Host = MacOsApfsExecutionHost<SystemCommandRunner>;

/// A workspace at one incarnation: whose seed a seed is.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Owner {
    pub name: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
}

/// What links build volumes right now: every mounted checkout's build link, every existing
/// workspace (whose seed a seed may be), the detached ones, whose links cannot be read, and the
/// ones being created: a create or fork forks its build volume and seed into the staged checkout
/// before the workspace exists, so until its intent completes nothing readable names either.
#[derive(Clone, Debug, Default)]
pub(crate) struct Links {
    pub volumes: BTreeSet<BuildVolumeId>,
    pub owners: BTreeSet<Owner>,
    pub detached: BTreeSet<WorkspaceName>,
    pub creating: BTreeSet<WorkspaceName>,
}

/// The landing workspace's volume once nothing writes it (Land step 4).
#[derive(Clone, Debug)]
pub(crate) struct Quiet {
    id: BuildVolumeId,
    mount: PathBuf,
    state: BuildVolumeState,
}

/// A fork's live volume, cloned from its source's latest seed and mounted while the source's
/// image is cloned and attached beside it (Fork steps 2 and 3): the staged checkout it is for
/// does not exist yet. [`BuildVolumes::finish_fork`] gives the destination its own seed and
/// links the checkout to it; [`BuildVolumes::abandon_fork`] releases it when no checkout staged.
#[derive(Debug)]
pub(crate) struct PreparedFork {
    seed: BuildVolumeId,
    tree: Option<GitOid>,
    live: BuildVolumeId,
    mount: PathBuf,
    started: Instant,
}

/// The target's volume once its Nx state is closed (Land step 5): its daemon stopped and its
/// task database held by nothing.
#[derive(Clone, Debug)]
pub(crate) struct Closed {
    id: BuildVolumeId,
    mount: PathBuf,
    state: BuildVolumeState,
}

/// A carry's first phase, done while the target still ran (Land step 5, "Carry").
#[derive(Debug)]
pub(crate) struct Staging {
    staged: carry::Staged,
    elapsed: Duration,
}

/// How far a target's latest seed is behind the live build volume it links, as the instant of
/// the last write each image holds (16_build_volumes.md, "Targets and seeds").
#[derive(Clone, Debug)]
pub(crate) struct SeedAge {
    pub live: BuildVolumeId,
    /// The tree the live volume's record names: the one a reseed records.
    pub tree: Option<GitOid>,
    /// When the live volume's image was last written, after its volume was flushed.
    pub written: SystemTime,
    /// The latest seed and the instant of the last write it holds; `None` for a target that
    /// has no seed.
    pub seed: Option<(BuildVolumeId, SystemTime)>,
}

impl SeedAge {
    /// Whether the live volume holds a write its seed does not.
    pub fn stale(&self) -> bool {
        self.seed
            .as_ref()
            .is_none_or(|(_, frozen)| self.written > *frozen)
    }

    /// How long before the live volume's last write the seed was frozen: `None` when the seed
    /// holds it, or when there is no seed.
    pub fn behind(&self) -> Option<Duration> {
        let (_, frozen) = self.seed.as_ref()?;
        self.written
            .duration_since(*frozen)
            .ok()
            .filter(|behind| !behind.is_zero())
    }
}

/// What capability detection says about a checkout's build state, for [`BuildVolumes::refresh`].
#[derive(Clone, Debug)]
pub(crate) enum Discovered {
    /// The tracked build inputs still have the fingerprint the volume's state records.
    Unchanged,
    /// Discovery ran at `fingerprint` and named `paths`; a first volume gets `capacity`.
    Changed {
        paths: Vec<BuildStatePath>,
        fingerprint: String,
        capacity: ImageCapacity,
    },
}

/// What collection found and did.
#[derive(Clone, Debug, Default)]
pub(crate) struct Collection {
    pub examined: u64,
    pub reclaimed: u64,
    pub freed_bytes: u64,
    pub candidates: Vec<GcCandidate>,
    pub deferred: Vec<Deferred>,
}

#[derive(Clone)]
pub(crate) struct BuildVolumes {
    host: Arc<Host>,
    pub layout: BuildVolumeLayout,
}

impl BuildVolumes {
    pub fn new(host: Arc<Host>, layout: BuildVolumeLayout) -> Self {
        Self { host, layout }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Host, &BuildVolumeLayout) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (host, layout) = (Arc::clone(&self.host), self.layout.clone());
        crate::storage::lifecycle::dispatch_blocking(move || work(&host, &layout))
            .await
            .map_err(|error| CowshedError::internal(format!("build volume task failed: {error}")))?
    }

    /// The published volume `checkout` links and the state written at its root, or `None` when
    /// it links none or an interrupted first touch left its volume without a record (which only
    /// a fresh discovery finishes). The checkout is mounted, which mounted its volume.
    pub fn state_of(&self, checkout: &Path) -> Result<Option<(BuildVolumeId, BuildVolumeState)>> {
        let Some(id) = self.layout.linked(checkout)? else {
            return Ok(None);
        };
        if self.layout.read_record_present(&id)?.is_none() {
            return Ok(None);
        }
        let state = BuildVolumeState::read(&self.layout.mount(&id))?;
        Ok(Some((id, state)))
    }

    /// Bring `checkout`'s build volume in line with what capability detection names now
    /// (16_build_volumes.md, "One link per checkout"). With `Discovered::Unchanged` the held
    /// paths are re-linked where a tool displaced them. With fresh discovery, new paths join the
    /// volume's state (held ones never move), every path is linked, and the state records the new
    /// fingerprint. A checkout whose volume is not published yet (none, or an interrupted first
    /// touch's) gets its first one at `capacity`, unless it links none and nothing was
    /// discovered; `owner` then also gets its seed, a clone of that volume, when it has none, so
    /// it is a target others fork from (16_build_volumes.md, "Targets and seeds"). A path that
    /// holds tracked source refuses before anything is deleted.
    pub async fn refresh(
        &self,
        owner: Owner,
        checkout: PathBuf,
        discovered: Discovered,
    ) -> Result<std::result::Result<BuildStateRefresh, TrackedBuildStateRefusal>> {
        let linked = self.layout.linked(&checkout)?;
        self.blocking(move |host, layout| {
            let published = match &linked {
                Some(id) => layout.read_record_present(id)?.is_some(),
                None => false,
            };
            let (id, discovered) = match (linked, discovered) {
                (Some(id), discovered) if published => (id, discovered),
                (None, Discovered::Unchanged) => return Ok(Ok(BuildStateRefresh::default())),
                (Some(id), Discovered::Unchanged) => {
                    return Err(CowshedError::internal(format!(
                        "build volume {id} is unpublished, so its state cannot be unchanged"
                    )));
                }
                (None, Discovered::Changed { paths, .. }) if paths.is_empty() => {
                    return Ok(Ok(BuildStateRefresh::default()));
                }
                (
                    _,
                    Discovered::Changed {
                        paths,
                        fingerprint,
                        capacity,
                    },
                ) => {
                    let touched = crate::build_volume::migrate::first_touch(
                        host,
                        layout,
                        crate::build_volume::migrate::FirstTouch {
                            checkout: &checkout,
                            paths: &paths,
                            fingerprint,
                            capacity,
                            record: BuildVolumeRecord::new(
                                None,
                                BuildVolumeRole::Linked {
                                    checkout: owner.name.clone(),
                                },
                            ),
                        },
                    )?;
                    let (id, displaced) = match touched {
                        Ok(touched) => touched,
                        Err(refusal) => return Ok(Err(refusal)),
                    };
                    if layout.seed_of(&owner.name, &owner.incarnation)?.is_none() {
                        host.clone_build_volume(
                            layout,
                            &id,
                            &BuildVolumeId::mint(),
                            &BuildVolumeRecord::new(
                                None,
                                BuildVolumeRole::Seed {
                                    target: owner.name,
                                    incarnation: owner.incarnation,
                                },
                            ),
                        )
                        .map_err(storage)?;
                    }
                    return Ok(Ok(BuildStateRefresh {
                        volume: Some(id),
                        created: true,
                        added: paths
                            .iter()
                            .map(|path| path.checkout.as_path().to_owned())
                            .collect(),
                        displaced,
                        findings: Vec::new(),
                    }));
                }
            };
            let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
            let held = BuildVolumeState::read(&mount)?;
            let (state, added) = match discovered {
                Discovered::Unchanged => (held.clone(), Vec::new()),
                Discovered::Changed {
                    paths, fingerprint, ..
                } => held.with_discovered(&paths, fingerprint, &mount)?,
            };
            let displaced =
                match crate::build_volume::migrate::adopt_paths(&checkout, &mount, &state.paths)? {
                    Ok(displaced) => displaced,
                    Err(refusal) => return Ok(Err(refusal)),
                };
            if state != held {
                state.write(&mount)?;
            }
            Ok(Ok(BuildStateRefresh {
                volume: Some(id),
                created: false,
                added: added
                    .iter()
                    .map(|path| path.checkout.as_path().to_owned())
                    .collect(),
                displaced,
                findings: Vec::new(),
            }))
        })
        .await
    }

    /// Fork steps 2 and 3 (16_build_volumes.md, "Fork"), up to the checkout: `source`'s seed
    /// first catches up with the live volume `source_checkout` links ([`Self::reseed`]; a skip
    /// is said on stderr, and the older seed is forked), then `destination` gets its live volume,
    /// a clone of `source`'s latest seed, mounted, with no daemon record. A source with no seed
    /// gives none. The caller holds `source`'s image lock, which every fork of `source` and its
    /// reseed hold, and runs this beside the clone of `source`'s image, whose stage
    /// [`Self::finish_fork`] then links.
    pub async fn prepare_fork(
        &self,
        source: Owner,
        source_checkout: PathBuf,
        destination: WorkspaceName,
    ) -> Result<Option<PreparedFork>> {
        if let Reseed::Skipped { behind_ms, reason } =
            self.reseed(source.clone(), source_checkout).await?
        {
            eprintln!(
                "cowshed: {}'s seed stays{} behind its build volume, so {destination} misses what {} built since: {reason}; the next fork retries",
                source.name,
                behind_ms.map(|ms| format!(" {ms} ms")).unwrap_or_default(),
                source.name,
            );
        }
        self.blocking(move |host, layout| {
            let Some((seed, record)) = layout.seed_of(&source.name, &source.incarnation)? else {
                return Ok(None);
            };
            let started = Instant::now();
            let (live, mount) = host
                .fork_build_volume(
                    layout,
                    &seed,
                    &BuildVolumeRecord::new(
                        record.tree.clone(),
                        BuildVolumeRole::Linked {
                            checkout: destination,
                        },
                    ),
                )
                .map_err(storage)?;
            Ok(Some(PreparedFork {
                seed,
                tree: record.tree,
                live,
                mount,
                started,
            }))
        })
        .await
    }

    /// The rest of the fork, once the checkout `destination` is about to become is staged at
    /// `checkout`: `destination` gets its own seed, a second clone of the seed its live volume
    /// came from, so it is a target others can fork from, and then the checkout links the live
    /// volume. Without a prepared volume the checkout must link nothing, or something is wrong,
    /// which refuses.
    pub async fn finish_fork(
        &self,
        prepared: Option<PreparedFork>,
        destination: Owner,
        checkout: PathBuf,
    ) -> Result<Option<BuildVolumeId>> {
        self.blocking(move |host, layout| {
            let Some(PreparedFork {
                seed,
                tree,
                live,
                mount,
                started,
            }) = prepared
            else {
                if link::linked(&checkout)?.is_some() {
                    return Err(CowshedError::integrity(
                        format!(
                            "{}'s staged checkout links a build volume, but its source has no seed to fork from",
                            destination.name
                        ),
                        "cowshed doctor --json",
                    ));
                }
                return Ok(None);
            };
            // The seed before the link: a crash between them leaves an unlinked volume, never a
            // target without a seed.
            let own_seed = BuildVolumeId::mint();
            host.clone_build_volume(
                layout,
                &seed,
                &own_seed,
                &BuildVolumeRecord::new(
                    tree,
                    BuildVolumeRole::Seed {
                        target: destination.name.clone(),
                        incarnation: destination.incarnation.clone(),
                    },
                ),
            )
            .map_err(storage)?;
            link::point(&checkout, &mount)?;
            // The own seed holds everything the live volume does but its mount's writes and the
            // dropped daemon record, so it starts as fresh as the live volume.
            let written = host.build_volume_written(layout, &live).map_err(storage)?;
            host.mark_seed_written(layout, &own_seed, written)
                .map_err(storage)?;
            crate::timing::event("build-volume", || {
                format!(
                    "fork {} from seed {seed} in {:?}",
                    destination.name,
                    started.elapsed()
                )
            });
            Ok(Some(live))
        })
        .await
    }

    /// Release what [`Self::prepare_fork`] answered for a checkout that never staged: no checkout
    /// links its live volume and nothing has run in it. The failed clone is the caller's error,
    /// so a release that fails too is said on stderr and left to `gc`.
    pub async fn abandon_fork(&self, prepared: Result<Option<PreparedFork>>) {
        let Ok(Some(prepared)) = prepared else {
            return;
        };
        let live = prepared.live.clone();
        let released = self
            .blocking(move |host, layout| {
                host.release_build_volume(layout, &prepared.live)
                    .map_err(storage)
            })
            .await;
        match released {
            Ok(BuildVolumeRelease::Deleted) => {}
            Ok(BuildVolumeRelease::Busy(diagnostic)) => eprintln!(
                "cowshed: build volume {live} of a fork that never staged stays: still in use: {diagnostic}; `cowshed gc` retries it"
            ),
            Err(error) => eprintln!(
                "cowshed: build volume {live} of a fork that never staged stays: {error}; `cowshed gc` retries it"
            ),
        }
    }

    /// Land step 4, after the landing workspace's supervisor has stopped its jobs: the landing
    /// volume's Nx daemon is stopped and its task database must have no holder left. The quiet
    /// volume then grows to the target's capacity when it is smaller, before the seed is frozen
    /// from it, so an adoption never shrinks the target and the seed inherits the larger cap: a
    /// workspace forked before a resize of its target lands at the target's capacity.
    pub async fn quiesce(
        &self,
        checkout: PathBuf,
        target_checkout: PathBuf,
    ) -> Result<std::result::Result<Quiet, AdoptionSkip>> {
        let id = self.layout.linked(&checkout)?;
        let target = self.layout.linked(&target_checkout)?;
        self.blocking(move |host, layout| {
            let Some(id) = id else {
                return Ok(Err(AdoptionSkip::NoLandingVolume));
            };
            let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            if let Err(busy) = nx::close(&mount, &state)
                .map_err(|error| io("close the landing volume's Nx state", &mount, &error))?
            {
                return Ok(Err(skip(busy, Side::Landing)));
            }
            if let Some(target) = target {
                let capacity = host
                    .build_volume_capacity(layout, &target)
                    .map_err(storage)?;
                match host.resize_build_volume(layout, &id, capacity) {
                    Ok(_) | Err(ApfsStorageError::CapacityNotGrowing { .. }) => {}
                    Err(ApfsStorageError::Apfs(error))
                        if crate::apfs::detach_was_dissented(&error) =>
                    {
                        return Ok(Err(AdoptionSkip::LandingVolumeBusy {
                            reason: error.to_string(),
                        }));
                    }
                    Err(error) => return Err(storage(error)),
                }
            }
            Ok(Ok(Quiet { id, mount, state }))
        })
        .await
    }

    /// Rebase carry (16_build_volumes.md, "Rebase carry"): index in the volume `checkout` links
    /// every Nx cache entry the target's volume indexes and it lacks, with the land's carry. The
    /// copies are staged while both sides run. Then each side's Nx state is closed as a land
    /// closes the target's (its idle daemon stopped, any other holder a skip), and the commit
    /// runs under both sides' Nx open locks once neither database has a holder, so nothing but
    /// the carry writes either while it indexes. A skip deletes what was staged.
    pub async fn rebase_carry(
        &self,
        checkout: PathBuf,
        target_checkout: PathBuf,
    ) -> Result<RebaseBuildVolume> {
        let workspace = self.layout.linked(&checkout)?;
        let target = self.layout.linked(&target_checkout)?;
        self.blocking(move |host, layout| {
            let skipped = |reason| Ok(RebaseBuildVolume::Skipped { reason });
            let Some(workspace) = workspace else {
                return skipped(RebaseCarrySkip::NoWorkspaceVolume);
            };
            let Some(target) = target else {
                return skipped(RebaseCarrySkip::NoTargetVolume);
            };
            let started = Instant::now();
            let into = host
                .mount_build_volume(layout, &workspace)
                .map_err(storage)?;
            let into_state = BuildVolumeState::read(&into)?;
            let from = host.mount_build_volume(layout, &target).map_err(storage)?;
            let from_state = BuildVolumeState::read(&from)?;
            let staged = carry::stage(&from, &from_state, &into, &into_state);
            let staged_in = started.elapsed();
            let sides = [
                (CarrySide::Workspace, into.as_path(), &into_state),
                (CarrySide::Target, from.as_path(), &from_state),
            ];
            let opens = match settle(&sides)? {
                Ok(opens) => opens,
                Err(reason) => {
                    carry::unstage(&into)
                        .map_err(|error| io("unstage the carry", &into, &error))?;
                    return skipped(reason);
                }
            };
            let committing = Instant::now();
            let carried = carry::commit(staged, &into);
            drop(opens);
            Ok(RebaseBuildVolume::Carried {
                carried: nx_carry(carried, staged_in + committing.elapsed(), &workspace),
            })
        })
        .await
    }

    /// Land step 5: `target`'s new seed is a clone of the quiet landing volume, and every older
    /// seed of `target` is deleted.
    pub async fn freeze_seed(&self, quiet: &Quiet, target: Owner, tree: GitOid) -> Result<()> {
        let source = quiet.id.clone();
        self.blocking(move |host, layout| {
            let previous = seeds_of(layout, &target)?;
            host.clone_build_volume(
                layout,
                &source,
                &BuildVolumeId::mint(),
                &BuildVolumeRecord::new(
                    Some(tree),
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
            )
            .map_err(storage)?;
            retire_seeds(host, layout, &target, previous)
        })
        .await
    }

    /// How far `target`'s seed is behind the live volume `checkout` links, or `None` when it
    /// links none, or one an interrupted first touch left unpublished.
    pub async fn seed_age(&self, target: Owner, checkout: PathBuf) -> Result<Option<SeedAge>> {
        let live = self.layout.linked(&checkout)?;
        self.blocking(move |host, layout| match live {
            Some(live) => seed_age(host, layout, &target, live),
            None => Ok(None),
        })
        .await
    }

    /// Reseed `target` from the live volume `checkout` links when that volume holds a write the
    /// seed does not and has no writer. The caller holds `target`'s workspace lock, which every
    /// fork of `target` holds while it clones the seed.
    pub async fn reseed(&self, target: Owner, checkout: PathBuf) -> Result<Reseed> {
        let this = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || this.reseed_now(&target, &checkout))
            .await
            .map_err(|error| CowshedError::internal(format!("build volume task failed: {error}")))?
    }

    /// [`Self::reseed`] on the calling thread, for a caller already off the async runtime.
    pub fn reseed_now(&self, target: &Owner, checkout: &Path) -> Result<Reseed> {
        let Some(live) = self.layout.linked(checkout)? else {
            return Ok(Reseed::NoBuildVolume);
        };
        match seed_age(&self.host, &self.layout, target, live)? {
            Some(age) => reseed(&self.host, &self.layout, target, age),
            None => Ok(Reseed::NoBuildVolume),
        }
    }

    /// Land step 5.2–5.4: when nothing but the target's daemon holds its task database,
    /// stop that daemon. Answers the target's volume, closed, for [`Self::commit`] and
    /// [`Self::adopt`].
    pub async fn close_target(
        &self,
        target_checkout: PathBuf,
    ) -> Result<std::result::Result<Closed, AdoptionSkip>> {
        let previous = self.layout.linked(&target_checkout)?;
        self.blocking(move |host, layout| {
            let Some(id) = previous else {
                return Ok(Err(AdoptionSkip::NoTargetVolume));
            };
            let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            if let Err(busy) = nx::close(&mount, &state)
                .map_err(|error| io("close the target's Nx state", &mount, &error))?
            {
                return Ok(Err(skip(busy, Side::Target)));
            }
            Ok(Ok(Closed { id, mount, state }))
        })
        .await
    }

    /// Land step 5.1, carry phase 1 (16_build_volumes.md, "Carry"), while the target still runs:
    /// stage in the quiet landing volume every Nx cache entry the target's volume indexes and
    /// the landing volume lacks.
    pub async fn stage(&self, quiet: &Quiet, target_checkout: PathBuf) -> Result<Staging> {
        let target = self.layout.linked(&target_checkout)?;
        let quiet = quiet.clone();
        self.blocking(move |host, layout| {
            let started = Instant::now();
            let staged = match target {
                Some(target) => {
                    let mount = host.mount_build_volume(layout, &target).map_err(storage)?;
                    let state = BuildVolumeState::read(&mount)?;
                    carry::stage(&mount, &state, &quiet.mount, &quiet.state)
                }
                None => carry::Staged::default(),
            };
            Ok(Staging {
                staged,
                elapsed: started.elapsed(),
            })
        })
        .await
    }

    /// Land step 5.5, carry phase 2, once the target is closed: index in the landing volume what
    /// [`Self::stage`] staged and the target did not change since, with what the target indexed
    /// after it.
    pub async fn commit(&self, staging: Staging, quiet: &Quiet) -> Result<NxCarry> {
        let quiet = quiet.clone();
        self.blocking(move |_, _| {
            let started = Instant::now();
            let carried = carry::commit(staging.staged, &quiet.mount);
            Ok(nx_carry(
                carried,
                staging.elapsed + started.elapsed(),
                &quiet.id,
            ))
        })
        .await
    }

    /// Delete what [`Self::stage`] staged in the landing volume, for a land whose target could
    /// not be closed: no row will index it, so nothing would ever delete it.
    pub async fn unstage(&self, quiet: &Quiet) -> Result<()> {
        let mount = quiet.mount.clone();
        self.blocking(move |_, _| {
            carry::unstage(&mount).map_err(|error| io("unstage the carry", &mount, &error))
        })
        .await
    }

    /// Land step 6: when nothing opened the closed target's task database since
    /// [`Self::close_target`], give the landing volume the target's daemon records in place of
    /// its own and rename the target's build link onto it. The target's previous volume is
    /// unlinked and released when idle. Answers how long the move took.
    ///
    /// The look and the rename happen under Nx's own open locks on the target's databases
    /// ([`nx::hold_opens`]). The look alone proves nothing past the moment it lists processes:
    /// an Nx client that opened the previous database after that, and before the rename, kept
    /// it, while the daemon it restarts opens the adopted one, so the task history the daemon
    /// records for the client's tasks names task details the adopted database lacks. Under the
    /// locks such a client waits instead, and opens the adopted database. A host daemon a client
    /// started since the close keeps its record across the move
    /// ([`nx::hand_over_daemon_records`]), so the client that waited finds it still serving.
    pub async fn adopt(
        &self,
        quiet: &Quiet,
        previous: Closed,
        target: WorkspaceName,
        target_checkout: PathBuf,
        tree: GitOid,
    ) -> Result<std::result::Result<u64, AdoptionSkip>> {
        let linked = self.layout.linked(&target_checkout)?;
        let quiet = quiet.clone();
        self.blocking(move |host, layout| {
            let started = Instant::now();
            if linked.as_ref() != Some(&previous.id) {
                return Err(CowshedError::internal(format!(
                    "{target}'s build link moved off {} while its land held the target",
                    previous.id
                )));
            }
            let opens = match nx::hold_opens(&previous.mount, &previous.state, &quiet.mount)
                .map_err(|error| io("lock the target's Nx opens", &previous.mount, &error))?
            {
                Ok(opens) => opens,
                Err(opening) => return Ok(Err(target_opening(opening))),
            };
            if let Err(busy) = nx::held(&previous.mount, &previous.state)
                .map_err(|error| io("look at the target's Nx state", &previous.mount, &error))?
            {
                return Ok(Err(skip(busy, Side::Target)));
            }
            nx::discard_daemon_records(&quiet.mount, &quiet.state)
                .map_err(|error| io("discard the landing daemon record", &quiet.mount, &error))?;
            nx::hand_over_daemon_records(&previous.mount, &previous.state, &quiet.mount)
                .map_err(|error| io("hand over the target's daemon record", &quiet.mount, &error))?;
            link::point(&target_checkout, &quiet.mount)?;
            // Every Nx process waiting to open now resolves the link to the adopted volume. The
            // locks are files in the previous volume, so they go before it is released.
            drop(opens);
            let elapsed = started.elapsed();
            layout.write_record(
                &quiet.id,
                &BuildVolumeRecord {
                    tree: Some(tree),
                    role: BuildVolumeRole::Linked {
                        checkout: target.clone(),
                    },
                    ..layout.read_record(&quiet.id)?
                },
            )?;
            let previous = previous.id;
            layout.write_record(
                &previous,
                &BuildVolumeRecord {
                    role: BuildVolumeRole::Unlinked,
                    ..layout.read_record(&previous)?
                },
            )?;
            // A process still on the previous volume keeps it; collection retries it.
            if let BuildVolumeRelease::Busy(reason) =
                host.release_build_volume(layout, &previous).map_err(storage)?
            {
                eprintln!(
                    "cowshed: {target}'s previous build volume {previous} stays until it is idle ({reason}); `cowshed gc` reclaims it then"
                );
            }
            Ok(Ok(millis(elapsed)))
        })
        .await
    }

    /// A workspace whose volume a target adopted and that keeps working (`land --no-retire`)
    /// takes a fresh clone of that target's new seed, so it never writes the target's volume.
    pub async fn refork(
        &self,
        seed_of: Owner,
        workspace: WorkspaceName,
        checkout: PathBuf,
    ) -> Result<()> {
        self.blocking(move |host, layout| {
            let (seed, record) = layout
                .seed_of(&seed_of.name, &seed_of.incarnation)?
                .ok_or_else(|| {
                    CowshedError::internal(format!(
                        "{} has no seed right after it was frozen",
                        seed_of.name
                    ))
                })?;
            let (_, mount) = host
                .fork_build_volume(
                    layout,
                    &seed,
                    &BuildVolumeRecord::new(
                        record.tree,
                        BuildVolumeRole::Linked {
                            checkout: workspace,
                        },
                    ),
                )
                .map_err(storage)?;
            link::point(&checkout, &mount)
        })
        .await
    }

    /// Resize (16_build_volumes.md, "Substrate"), after `owner`'s supervisor has stopped its
    /// jobs: grow `owner`'s seed and then the build volume `checkout` links to `capacity`, so
    /// every later fork of `owner` inherits it. The seed goes first: it has no holder, and a
    /// linked volume that then refuses leaves only a seed larger than it, which a retry skips.
    /// The volume's Nx daemon is stopped as a land stops it; another holder of its task
    /// database, or of any file in it, refuses before its image changes. A seed already at least
    /// `capacity` is left as it is. Answers the linked volume's previous capacity and the one
    /// the kernel now reports.
    pub async fn resize(
        &self,
        owner: Owner,
        checkout: PathBuf,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome> {
        let Some(id) = self.layout.linked(&checkout)? else {
            return Err(CowshedError::not_found(
                format!("workspace {} links no build volume", owner.name),
                format!(
                    "cowshed resize {} <size> grows its image instead",
                    owner.name
                ),
            ));
        };
        self.blocking(move |host, layout| {
            let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            if let Err(busy) = nx::close(&mount, &state)
                .map_err(|error| io("close the build volume's Nx state", &mount, &error))?
            {
                return Err(CowshedError::conflict(
                    format!("build volume {id} of {} is in use: {busy}", owner.name),
                    "stop what holds it, then retry the resize",
                ));
            }
            if let Some((seed, _)) = layout.seed_of(&owner.name, &owner.incarnation)? {
                match host.resize_build_volume(layout, &seed, capacity) {
                    Ok(_) | Err(ApfsStorageError::CapacityNotGrowing { .. }) => {}
                    Err(error) => return Err(resize_error(&owner.name, &seed, error)),
                }
            }
            host.resize_build_volume(layout, &id, capacity)
                .map_err(|error| resize_error(&owner.name, &id, error))
        })
        .await
    }

    /// Delete (16_build_volumes.md, "Garbage collection") what [`plan`] dooms when the kernel
    /// lets go of it. A volume the kernel refuses to detach is deferred to the next pass with
    /// the kernel's words, beside what the plan itself deferred.
    pub async fn collect(&self, links: Links, dry_run: bool) -> Result<Collection> {
        self.blocking(move |host, layout| {
            let plan = plan(layout, &links)?;
            let mut collection = Collection {
                examined: plan.examined,
                deferred: plan
                    .deferred
                    .into_iter()
                    .map(|(id, deferral)| Deferred {
                        path: layout.image(&id),
                        deferral,
                    })
                    .collect(),
                ..Collection::default()
            };
            for Doomed { id, reason, bytes } in plan.doomed {
                let image = layout.image(&id);
                collection.candidates.push(GcCandidate {
                    identity: Sha256Digest::compute(image.as_os_str().as_encoded_bytes()),
                    path: image.clone(),
                    bytes,
                    reason,
                });
                if dry_run {
                    collection.freed_bytes = collection.freed_bytes.saturating_add(bytes);
                    continue;
                }
                match host.release_build_volume(layout, &id).map_err(storage)? {
                    BuildVolumeRelease::Deleted => {
                        collection.reclaimed += 1;
                        collection.freed_bytes = collection.freed_bytes.saturating_add(bytes);
                    }
                    BuildVolumeRelease::Busy(diagnostic) => {
                        collection.deferred.push(Deferred {
                            path: image,
                            deferral: Deferral::Busy(diagnostic),
                        });
                    }
                }
            }
            Ok(collection)
        })
        .await
    }
}

/// Why collection leaves a build volume for a later pass instead of deleting it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Deferral {
    /// The kernel refused a non-forced detach: something still uses the volume.
    Busy(String),
    /// Its record exists but cannot be read, so nothing proves it unreachable.
    RecordUnreadable(String),
    /// Its image's size cannot be read.
    SizeUnreadable(String),
    /// Its record names `0` as its checkout, which is detached: that checkout's link cannot be
    /// read until it is attached, so nothing proves the volume unreachable.
    DetachedCheckout(WorkspaceName),
    /// Its record names `0` as its checkout or seed target, whose create or fork has not
    /// finished: the staged checkout's link cannot be read, and the workspace does not exist
    /// yet, so nothing proves the volume unreachable.
    Creating(WorkspaceName),
    /// It has no record, and workspaces whose links cannot be read yet may name it: the
    /// detached ones until they are attached, the ones being created until their create or fork
    /// finishes.
    Unrecorded {
        detached: Vec<WorkspaceName>,
        creating: Vec<WorkspaceName>,
    },
}

impl Deferral {
    /// Whether this is the ordinary state of a detached or still-forming workspace's own volume
    /// rather than something a person should look at.
    pub fn is_routine(&self) -> bool {
        matches!(self, Self::DetachedCheckout(_) | Self::Creating(_))
    }
}

impl std::fmt::Display for Deferral {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = |formatter: &mut std::fmt::Formatter<'_>, names: &[WorkspaceName]| {
            for (index, name) in names.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(", ")?;
                }
                write!(formatter, "{name}")?;
            }
            Ok(())
        };
        match self {
            Self::Busy(diagnostic) => write!(formatter, "still in use: {diagnostic}"),
            Self::RecordUnreadable(error) => write!(formatter, "its record is unreadable: {error}"),
            Self::SizeUnreadable(error) => write!(formatter, "its size is unreadable: {error}"),
            Self::DetachedCheckout(checkout) => write!(
                formatter,
                "its record names {checkout}, which is detached; decided once {checkout} is attached"
            ),
            Self::Creating(workspace) => write!(
                formatter,
                "its record names {workspace}, whose create or fork has not finished; decided once it has"
            ),
            Self::Unrecorded { detached, creating } => {
                formatter.write_str("it has no record, and the links of")?;
                if !detached.is_empty() {
                    formatter.write_str(" detached ")?;
                    names(formatter, detached)?;
                    formatter.write_str(" (until attached)")?;
                }
                if !detached.is_empty() && !creating.is_empty() {
                    formatter.write_str(" and")?;
                }
                if !creating.is_empty() {
                    formatter.write_str(" still-forming ")?;
                    names(formatter, creating)?;
                    formatter.write_str(" (until their create or fork finishes)")?;
                }
                formatter.write_str(" cannot be read")
            }
        }
    }
}

/// A build volume collection left for a later pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Deferred {
    pub path: PathBuf,
    pub deferral: Deferral,
}

impl From<Deferred> for GcDeferred {
    fn from(deferred: Deferred) -> Self {
        Self {
            path: deferred.path,
            diagnostic: deferred.deferral.to_string(),
        }
    }
}

/// A build volume collection deletes, with why and its allocated size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Doomed {
    pub id: BuildVolumeId,
    pub reason: GcReason,
    pub bytes: u64,
}

/// What collection decides from the records, the links and the images' sizes alone.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Plan {
    pub examined: u64,
    pub doomed: Vec<Doomed>,
    pub deferred: Vec<(BuildVolumeId, Deferral)>,
}

/// Which build volumes are garbage (16_build_volumes.md, "Garbage collection"): each one nothing
/// links that is not a seed, each seed that is not its existing target's latest, and each image
/// an interrupted creation left without a record.
///
/// A live link proves reachability by itself, so a volume one names is kept before its record
/// is read. A detached workspace's link cannot be read, so whatever it may name is deferred with
/// it named, never counted reachable and never deleted; so is whatever a workspace still being
/// created may name, whose staged link no other process can read and whose seed belongs to a
/// workspace that does not exist yet; so is a volume whose record or size cannot be read.
/// Nothing is deleted on a guess.
pub(crate) fn plan(layout: &BuildVolumeLayout, links: &Links) -> Result<Plan> {
    let mut plan = Plan::default();
    let mut latest = std::collections::BTreeMap::<Owner, (String, BuildVolumeId)>::new();
    let mut doomed = Vec::new();
    for id in layout.list()? {
        plan.examined += 1;
        if links.volumes.contains(&id) {
            continue;
        }
        let record = match layout.read_record_present(&id) {
            Ok(Some(record)) => record,
            Ok(None) if links.detached.is_empty() && links.creating.is_empty() => {
                doomed.push((id, GcReason::UnrecordedBuildVolume));
                continue;
            }
            Ok(None) => {
                plan.deferred.push((
                    id,
                    Deferral::Unrecorded {
                        detached: links.detached.iter().cloned().collect(),
                        creating: links.creating.iter().cloned().collect(),
                    },
                ));
                continue;
            }
            Err(error) => {
                plan.deferred
                    .push((id, Deferral::RecordUnreadable(error.to_string())));
                continue;
            }
        };
        match record.role {
            BuildVolumeRole::Seed { target, .. } | BuildVolumeRole::Linked { checkout: target }
                if links.creating.contains(&target) =>
            {
                plan.deferred.push((id, Deferral::Creating(target)));
            }
            BuildVolumeRole::Seed {
                target,
                incarnation,
            } => {
                let owner = Owner {
                    name: target,
                    incarnation,
                };
                if !links.owners.contains(&owner) {
                    doomed.push((id, GcReason::SupersededSeed));
                    continue;
                }
                match latest.get(&owner) {
                    Some((at, _)) if *at >= record.created_at => {
                        doomed.push((id, GcReason::SupersededSeed));
                    }
                    _ => {
                        if let Some((_, older)) = latest.insert(owner, (record.created_at, id)) {
                            doomed.push((older, GcReason::SupersededSeed));
                        }
                    }
                }
            }
            BuildVolumeRole::Linked { checkout } if links.detached.contains(&checkout) => {
                plan.deferred
                    .push((id, Deferral::DetachedCheckout(checkout)));
            }
            BuildVolumeRole::Linked { .. } | BuildVolumeRole::Unlinked => {
                doomed.push((id, GcReason::UnlinkedBuildVolume));
            }
        }
    }
    for (id, reason) in doomed {
        let image = layout.image(&id);
        match std::fs::metadata(&image) {
            Ok(metadata) => plan.doomed.push(Doomed {
                id,
                reason,
                bytes: metadata.blocks().saturating_mul(512),
            }),
            Err(error) => plan.deferred.push((
                id,
                Deferral::SizeUnreadable(format!("{}: {error}", image.display())),
            )),
        }
    }
    Ok(plan)
}

fn seeds_of(layout: &BuildVolumeLayout, owner: &Owner) -> Result<Vec<BuildVolumeId>> {
    let mut seeds = Vec::new();
    for id in layout.list()? {
        if layout
            .read_record_present(&id)?
            .is_some_and(|record| record.is_seed_of(&owner.name, &owner.incarnation))
        {
            seeds.push(id);
        }
    }
    Ok(seeds)
}

/// Delete `previous`, the seeds of `target` a new one replaced. A seed is never mounted, so
/// nothing can hold it.
fn retire_seeds(
    host: &Host,
    layout: &BuildVolumeLayout,
    target: &Owner,
    previous: Vec<BuildVolumeId>,
) -> Result<()> {
    for old in previous {
        if let BuildVolumeRelease::Busy(reason) =
            host.release_build_volume(layout, &old).map_err(storage)?
        {
            return Err(CowshedError::integrity(
                format!(
                    "seed {old} of {} is attached and in use: {reason}",
                    target.name
                ),
                "cowshed doctor --json",
            ));
        }
    }
    Ok(())
}

/// `target`'s seed against its live volume `live`, or `None` when `live` is an interrupted
/// first touch's, unpublished: only a fresh discovery finishes that one.
fn seed_age(
    host: &Host,
    layout: &BuildVolumeLayout,
    target: &Owner,
    live: BuildVolumeId,
) -> Result<Option<SeedAge>> {
    let Some(record) = layout.read_record_present(&live)? else {
        return Ok(None);
    };
    let written = host.build_volume_written(layout, &live).map_err(storage)?;
    let seed = match layout.seed_of(&target.name, &target.incarnation)? {
        Some((seed, _)) => {
            let frozen = host.build_volume_written(layout, &seed).map_err(storage)?;
            Some((seed, frozen))
        }
        None => None,
    };
    Ok(Some(SeedAge {
        live,
        tree: record.tree,
        written,
        seed,
    }))
}

/// Refreeze `target`'s seed from its live volume when the seed is behind it and nothing writes
/// it (16_build_volumes.md, "Targets and seeds"): the same quiesce rule as an adoption for Nx
/// (only the target's daemon may hold its task database, and it is stopped), and every Cargo
/// build lock taken, so no Cargo build runs or starts while the image is cloned. The task
/// databases are looked at once more after the clone; a process that opened one meanwhile may
/// have written it mid-clone, so that clone is deleted and the reseed skipped.
fn reseed(host: &Host, layout: &BuildVolumeLayout, target: &Owner, age: SeedAge) -> Result<Reseed> {
    if !age.stale() {
        return Ok(Reseed::Fresh);
    }
    let behind_ms = age.behind().map(millis);
    let started = Instant::now();
    let mount = host
        .mount_build_volume(layout, &age.live)
        .map_err(storage)?;
    let state = BuildVolumeState::read(&mount)?;
    let skipped = |reason| Ok(Reseed::Skipped { behind_ms, reason });
    if let Err(busy) = nx::close(&mount, &state)
        .map_err(|error| io("close the target's Nx state", &mount, &error))?
    {
        return skipped(reseed_skip(busy));
    }
    let _builds = match cargo::hold(&mount, &state)
        .map_err(|error| io("take the target's Cargo build locks", &mount, &error))?
    {
        Ok(held) => held,
        Err(lock) => {
            let holders = nx::holders(&lock)
                .map_err(|error| io("list the Cargo build's processes", &lock, &error))?;
            return skipped(ReseedSkip::Building {
                lock,
                holders: holders.into_iter().map(database_holder).collect(),
            });
        }
    };
    let previous = seeds_of(layout, target)?;
    let seed = BuildVolumeId::mint();
    host.clone_build_volume(
        layout,
        &age.live,
        &seed,
        &BuildVolumeRecord::new(
            age.tree,
            BuildVolumeRole::Seed {
                target: target.name.clone(),
                incarnation: target.incarnation.clone(),
            },
        ),
    )
    .map_err(storage)?;
    if let Err(busy) = nx::held(&mount, &state)
        .map_err(|error| io("look at the target's Nx state", &mount, &error))?
    {
        host.release_build_volume(layout, &seed).map_err(storage)?;
        return skipped(reseed_skip(busy));
    }
    retire_seeds(host, layout, target, previous)?;
    let elapsed_ms = millis(started.elapsed());
    crate::timing::event("build-volume", || {
        format!(
            "reseed {} from {} in {elapsed_ms} ms",
            target.name, age.live
        )
    });
    Ok(Reseed::Reseeded {
        behind_ms,
        elapsed_ms,
    })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn database_holder(holder: nx::Holder) -> DatabaseHolder {
    DatabaseHolder {
        pid: holder.pid,
        command: holder.command,
    }
}

fn reseed_skip(busy: nx::Busy) -> ReseedSkip {
    match busy {
        nx::Busy::Held { database, holders } => ReseedSkip::Held {
            database,
            holders: holders.into_iter().map(database_holder).collect(),
        },
        nx::Busy::DaemonStayed { daemon } => ReseedSkip::DaemonStayed {
            daemon: database_holder(daemon),
        },
    }
}

#[derive(Clone, Copy)]
enum Side {
    Landing,
    Target,
}

fn skip(busy: nx::Busy, side: Side) -> AdoptionSkip {
    match (busy, side) {
        (nx::Busy::Held { database, holders }, Side::Landing) => AdoptionSkip::LandingHeld {
            database,
            holders: holders.into_iter().map(database_holder).collect(),
        },
        (nx::Busy::Held { database, holders }, Side::Target) => AdoptionSkip::TargetHeld {
            database,
            holders: holders.into_iter().map(database_holder).collect(),
        },
        (nx::Busy::DaemonStayed { daemon }, Side::Landing) => AdoptionSkip::LandingDaemonStayed {
            daemon: database_holder(daemon),
        },
        (nx::Busy::DaemonStayed { daemon }, Side::Target) => AdoptionSkip::TargetDaemonStayed {
            daemon: database_holder(daemon),
        },
    }
}

fn target_opening(opening: nx::Opening) -> AdoptionSkip {
    AdoptionSkip::TargetOpening {
        database: opening.database,
        holders: opening.holders.into_iter().map(database_holder).collect(),
    }
}

/// What a carry into the volume `into` reports, taking `elapsed` in all; also said as a timing
/// event.
fn nx_carry(carried: carry::Carried, elapsed: Duration, into: &BuildVolumeId) -> NxCarry {
    let carry = NxCarry {
        entries: carried.entries,
        bytes: carried.bytes,
        elapsed_ms: millis(elapsed),
        stopped: carried.stopped,
    };
    crate::timing::event("build-volume", || {
        format!(
            "carried {} Nx entries ({} bytes) into {into} in {} ms",
            carry.entries, carry.bytes, carry.elapsed_ms
        )
    });
    carry
}

/// The rebase carry's commit precondition on each of `sides`, the volume mounted at its path
/// with its state: its Nx state closed ([`nx::close`] stops only an idle daemon), then its open
/// locks held and its task databases held by nothing. Answers the held locks, which keep every
/// Nx process from opening either database until they are dropped, or the first side that
/// failed it, with every lock taken until then let go.
fn settle(
    sides: &[(CarrySide, &Path, &BuildVolumeState)],
) -> Result<std::result::Result<Vec<nx::Opens>, RebaseCarrySkip>> {
    for &(side, mount, state) in sides {
        if let Err(busy) =
            nx::close(mount, state).map_err(|error| io("close the Nx state", mount, &error))?
        {
            return Ok(Err(rebase_skip(busy, side)));
        }
    }
    let mut opens = Vec::with_capacity(sides.len());
    for &(side, mount, state) in sides {
        match nx::hold_opens(mount, state, mount)
            .map_err(|error| io("lock the Nx opens", mount, &error))?
        {
            Ok(held) => opens.push(held),
            Err(opening) => {
                return Ok(Err(RebaseCarrySkip::Opening {
                    side,
                    database: opening.database,
                    holders: opening.holders.into_iter().map(database_holder).collect(),
                }));
            }
        }
    }
    for &(side, mount, state) in sides {
        if let Err(busy) =
            nx::held(mount, state).map_err(|error| io("look at the Nx state", mount, &error))?
        {
            return Ok(Err(rebase_skip(busy, side)));
        }
    }
    Ok(Ok(opens))
}

fn rebase_skip(busy: nx::Busy, side: CarrySide) -> RebaseCarrySkip {
    match busy {
        nx::Busy::Held { database, holders } => RebaseCarrySkip::Held {
            side,
            database,
            holders: holders.into_iter().map(database_holder).collect(),
        },
        nx::Busy::DaemonStayed { daemon } => RebaseCarrySkip::DaemonStayed {
            side,
            daemon: database_holder(daemon),
        },
    }
}

/// A resize that asked for no growth is the caller's mistake, and a volume the kernel would not
/// let go of is in use; anything else is broken storage.
fn resize_error(
    workspace: &WorkspaceName,
    id: &BuildVolumeId,
    error: ApfsStorageError,
) -> CowshedError {
    match error {
        error @ ApfsStorageError::CapacityNotGrowing { .. } => CowshedError::usage(
            format!("build volume {id} of {workspace}: {error}"),
            format!("cowshed resize {workspace} --build <capacity larger than the current one>"),
        ),
        ApfsStorageError::Apfs(error) if crate::apfs::detach_was_dissented(&error) => {
            CowshedError::conflict(
                format!("build volume {id} of {workspace} is in use: {error}"),
                "stop what holds it, then retry the resize",
            )
        }
        error => storage(error),
    }
}

fn storage(error: crate::storage::apfs::ApfsStorageError) -> CowshedError {
    CowshedError::environment_missing(
        format!("build volume storage failed: {error}"),
        "cowshed doctor --json",
    )
}

fn io(operation: &str, path: &Path, error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} at {}: {error}", path.display()),
        "cowshed doctor --json",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::{ProjectPaths, RepoId};
    use std::fs;

    struct Store {
        root: PathBuf,
        layout: BuildVolumeLayout,
    }

    impl Store {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowshed-build-gc-{}",
                uuid::Uuid::new_v4().simple()
            ));
            let project = ProjectPaths::with_mount_root(
                &root,
                root.join("mnt"),
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap();
            let layout = BuildVolumeLayout::new(&project).unwrap();
            fs::create_dir_all(layout.images()).unwrap();
            Self { root, layout }
        }

        fn image(&self) -> BuildVolumeId {
            let id = BuildVolumeId::mint();
            fs::write(self.layout.image(&id), vec![7u8; 8192]).unwrap();
            id
        }

        fn volume(&self, role: BuildVolumeRole) -> BuildVolumeId {
            let id = self.image();
            self.layout
                .write_record(&id, &BuildVolumeRecord::new(None, role))
                .unwrap();
            id
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn name(value: &str) -> WorkspaceName {
        WorkspaceName::new(value).unwrap()
    }

    fn linked(checkout: &str) -> BuildVolumeRole {
        BuildVolumeRole::Linked {
            checkout: name(checkout),
        }
    }

    fn doomed(plan: &Plan) -> Vec<(BuildVolumeId, GcReason)> {
        plan.doomed
            .iter()
            .map(|doomed| (doomed.id.clone(), doomed.reason))
            .collect()
    }

    /// A seed is behind exactly when the live volume was written after it was frozen; a
    /// target with no seed is behind by no measurable amount, and still reseeds.
    #[test]
    fn a_seed_is_behind_only_when_the_live_volume_was_written_after_it() {
        let frozen = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let age = |written: SystemTime, seed: Option<SystemTime>| SeedAge {
            live: BuildVolumeId::mint(),
            tree: None,
            written,
            seed: seed.map(|frozen| (BuildVolumeId::mint(), frozen)),
        };
        let fresh = age(frozen, Some(frozen));
        assert!(!fresh.stale());
        assert_eq!(fresh.behind(), None);
        let older = age(frozen - Duration::from_secs(5), Some(frozen));
        assert!(!older.stale(), "a seed newer than every write holds them");
        assert_eq!(older.behind(), None);
        let behind = age(frozen + Duration::from_millis(1_500), Some(frozen));
        assert!(behind.stale());
        assert_eq!(behind.behind(), Some(Duration::from_millis(1_500)));
        let unseeded = age(frozen, None);
        assert!(unseeded.stale());
        assert_eq!(unseeded.behind(), None);
    }

    /// A live link is the proof of reachability, so it protects its volume before any record is
    /// read: a crash can leave a linked volume whose record is missing, and deleting it would
    /// pull the build state out from under a checkout.
    #[test]
    fn a_live_link_keeps_its_volume_recorded_or_not() {
        let store = Store::new();
        let unrecorded = store.image();
        let recorded_elsewhere = store.volume(linked("other"));
        let garbage = store.volume(BuildVolumeRole::Unlinked);
        let links = Links {
            volumes: [unrecorded, recorded_elsewhere].into_iter().collect(),
            ..Links::default()
        };
        let plan = plan(&store.layout, &links).unwrap();
        assert_eq!(
            doomed(&plan),
            [(garbage, GcReason::UnlinkedBuildVolume)],
            "{plan:?}"
        );
        assert_eq!(plan.doomed[0].bytes, 8192);
        assert_eq!(plan.deferred, []);
        assert_eq!(plan.examined, 3);
    }

    /// A detached workspace's link cannot be read, so a record naming it proves nothing: the
    /// volume is deferred with that workspace named, never counted reachable or deleted. An
    /// image without a record may be any detached workspace's, so it waits for them too.
    #[test]
    fn a_detached_checkout_defers_what_it_may_link() {
        let store = Store::new();
        let named = store.volume(linked("topic"));
        let unrecorded = store.image();
        let links = Links {
            detached: [name("topic")].into_iter().collect(),
            ..Links::default()
        };
        let mut plan = plan(&store.layout, &links).unwrap();
        plan.deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (named, Deferral::DetachedCheckout(name("topic"))),
            (
                unrecorded,
                Deferral::Unrecorded {
                    detached: vec![name("topic")],
                    creating: Vec::new(),
                },
            ),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(plan.deferred, expected);
        assert_eq!(doomed(&plan), []);
        assert!(plan.deferred.iter().all(|(_, deferral)| match deferral {
            Deferral::DetachedCheckout(_) => deferral.is_routine(),
            _ => !deferral.is_routine(),
        }));
        // With nothing detached, the same unrecorded image is an interrupted creation.
        let plan = super::plan(&store.layout, &Links::default()).unwrap();
        assert!(
            doomed(&plan).contains(&(
                plan.doomed
                    .iter()
                    .find(|doomed| doomed.reason == GcReason::UnrecordedBuildVolume)
                    .unwrap()
                    .id
                    .clone(),
                GcReason::UnrecordedBuildVolume
            )),
            "{plan:?}"
        );
    }

    /// A create or fork forks its volume and seed into a staged checkout no other process can
    /// read, before its workspace exists. Until its intent completes, whatever may be its own —
    /// its live volume, its seed, an image still being cloned for it — is deferred, never
    /// collected. Another process's `rm` collected exactly these, and the new workspace's mount
    /// then refused a link to a volume that was gone.
    #[test]
    fn a_forming_workspace_defers_its_volume_seed_and_unrecorded_clones() {
        let store = Store::new();
        let live = store.volume(linked("lane"));
        let seed = store.volume(BuildVolumeRole::Seed {
            target: name("lane"),
            incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
        });
        let cloning = store.image();
        let garbage = store.volume(linked("gone"));
        let links = Links {
            creating: [name("lane")].into_iter().collect(),
            ..Links::default()
        };
        let mut plan = plan(&store.layout, &links).unwrap();
        plan.deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let unrecorded = Deferral::Unrecorded {
            detached: Vec::new(),
            creating: vec![name("lane")],
        };
        let mut expected = vec![
            (live.clone(), Deferral::Creating(name("lane"))),
            (seed.clone(), Deferral::Creating(name("lane"))),
            (cloning.clone(), unrecorded.clone()),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(plan.deferred, expected);
        assert_eq!(
            doomed(&plan),
            [(garbage.clone(), GcReason::UnlinkedBuildVolume)]
        );
        assert!(Deferral::Creating(name("lane")).is_routine());
        assert!(!unrecorded.is_routine());
        assert_eq!(
            unrecorded.to_string(),
            "it has no record, and the links of still-forming lane (until their create or fork \
             finishes) cannot be read"
        );
        // Without the forming workspace, all three read as garbage: the collection that left
        // a new workspace linking a volume nobody owned.
        let mut doomed = doomed(&super::plan(&store.layout, &Links::default()).unwrap());
        doomed.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (live, GcReason::UnlinkedBuildVolume),
            (seed, GcReason::SupersededSeed),
            (cloning, GcReason::UnrecordedBuildVolume),
            (garbage, GcReason::UnlinkedBuildVolume),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(doomed, expected);
    }

    /// The `cowshed new` that another process's `rm` broke, on real APFS: a fork clones main's
    /// seed into a staged checkout, and a collection runs before the workspace is published.
    /// Told the workspace is forming, collection keeps its live volume and its seed, and the
    /// fork owns what its link names. Without that, the same collection deletes both, and the
    /// fork's mount refuses its link exactly as the broken `cowshed new` did.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_collection_keeps_a_forming_forks_volume_and_seed() {
        let scratch = Scratch::new("build-forming-fork");
        let main = owner("main", '0');
        let (main_checkout, main_live, _) = scratch.linked_checkout("main", 1);
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &BuildVolumeId::mint(),
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                ),
            )
            .expect("seed main");
        let lane = owner("lane", '1');
        let staged = scratch.root.path().join("staged-lane");
        fs::create_dir_all(staged.join(".cowshed")).unwrap();
        let prepared = scratch
            .volumes
            .prepare_fork(main.clone(), main_checkout, lane.name.clone())
            .await
            .expect("prepare the fork from main's seed");
        let forked = scratch
            .volumes
            .finish_fork(prepared, lane.clone(), staged.clone())
            .await
            .expect("fork from main's seed")
            .expect("main has a seed");
        let (lane_seed, _) = scratch
            .layout
            .seed_of(&lane.name, &lane.incarnation)
            .unwrap()
            .expect("the fork seeded its destination");
        // What another process's collection sees: main, mounted and linked; the lane only in the
        // journal, as a create past its mutation fence.
        let links = |creating: &[&Owner]| Links {
            volumes: [main_live.clone()].into_iter().collect(),
            owners: [main.clone()].into_iter().collect(),
            creating: creating.iter().map(|owner| owner.name.clone()).collect(),
            ..Links::default()
        };

        let kept = scratch
            .volumes
            .collect(links(&[&lane]), false)
            .await
            .unwrap();
        assert_eq!(kept.reclaimed, 0, "{:?}", kept.candidates);
        let mut deferred = kept
            .deferred
            .iter()
            .map(|deferred| (deferred.path.clone(), deferred.deferral.clone()))
            .collect::<Vec<_>>();
        deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (
                scratch.layout.image(&forked),
                Deferral::Creating(lane.name.clone()),
            ),
            (
                scratch.layout.image(&lane_seed),
                Deferral::Creating(lane.name.clone()),
            ),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(deferred, expected);
        assert_eq!(
            scratch.layout.linked(&staged).unwrap(),
            Some(forked.clone())
        );
        assert_eq!(
            scratch.layout.resolve_link(&lane.name, &forked).unwrap(),
            crate::build_volume::LinkResolution::Keep,
            "the fork owns the volume its link names"
        );

        // The collection that ran beside the broken `cowshed new`, which knew nothing of the fork.
        let collected = scratch.volumes.collect(links(&[]), false).await.unwrap();
        assert_eq!(collected.reclaimed, 2, "{:?}", collected.candidates);
        let refusal = scratch
            .layout
            .resolve_link(&lane.name, &forked)
            .unwrap_err();
        assert_eq!(
            refusal.message,
            format!(
                "lane's build link names {forked}, which lane does not own, and 0 build volumes \
                 are recorded as lane's"
            )
        );
        scratch.release_all();
    }

    /// A record or size that cannot be read is an error about that one volume: it is deferred
    /// with the error, the pass goes on, and nothing is deleted on a guess.
    #[test]
    fn unreadable_records_and_sizes_defer_and_never_delete() {
        let store = Store::new();
        // A record that loops: `exists()` answers false for it, which once read as "no record".
        let looping = store.image();
        std::os::unix::fs::symlink(store.layout.record(&looping), store.layout.record(&looping))
            .unwrap();
        let corrupt = store.image();
        fs::write(store.layout.record(&corrupt), b"{not json").unwrap();
        // An image whose size cannot be read: a link to nothing.
        let sizeless = BuildVolumeId::mint();
        std::os::unix::fs::symlink(store.root.join("gone"), store.layout.image(&sizeless)).unwrap();
        store
            .layout
            .write_record(
                &sizeless,
                &BuildVolumeRecord::new(None, BuildVolumeRole::Unlinked),
            )
            .unwrap();
        let garbage = store.volume(BuildVolumeRole::Unlinked);
        let plan = plan(&store.layout, &Links::default()).unwrap();
        assert_eq!(
            doomed(&plan),
            [(garbage, GcReason::UnlinkedBuildVolume)],
            "{plan:?}"
        );
        let deferral = |id: &BuildVolumeId| {
            plan.deferred
                .iter()
                .find(|(deferred, _)| deferred == id)
                .map(|(_, deferral)| deferral.clone())
        };
        assert!(
            matches!(deferral(&looping), Some(Deferral::RecordUnreadable(_))),
            "{plan:?}"
        );
        assert!(
            matches!(deferral(&corrupt), Some(Deferral::RecordUnreadable(_))),
            "{plan:?}"
        );
        assert!(
            matches!(deferral(&sizeless), Some(Deferral::SizeUnreadable(_))),
            "{plan:?}"
        );
    }

    /// The bytes the filesystem mounted at `mount` holds, as the kernel reports them.
    #[cfg(target_os = "macos")]
    fn filesystem_bytes(mount: &Path) -> u64 {
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(mount.as_os_str().as_bytes()).unwrap();
        // SAFETY: `statfs` writes only into `stat`, a zeroed plain-data struct of its own type,
        // and reads `path`, a NUL-terminated string that outlives the call.
        let mut stat = unsafe { std::mem::zeroed::<libc::statfs>() };
        // SAFETY: as above.
        assert_eq!(unsafe { libc::statfs(path.as_ptr(), &raw mut stat) }, 0);
        stat.f_blocks * u64::from(stat.f_bsize)
    }

    /// `cowshed resize --build` on real APFS: the linked volume grows, its seed grows with it, so
    /// a fork made afterwards inherits the new capacity. Kept to one resize: each one reads the
    /// image's limits through `diskutil`, which stalls for seconds under a loaded test run.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_build_resize_grows_the_volume_and_seed_so_a_later_fork_inherits_it() {
        use crate::apfs::SystemCommandRunner;
        use crate::capabilities::BuildStatePath;
        use crate::storage::apfs::ApfsSubstrateConfig;
        const GIB: u64 = ImageCapacity::GIBIBYTE;

        let root = crate::scratch_apfs::ScratchRoot::new("build-resize").expect("scratch root");
        let store = root.path().join("store");
        fs::create_dir_all(&store).unwrap();
        let project = ProjectPaths::with_mount_root(
            &store,
            root.path().join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let host = Arc::new(
            MacOsApfsExecutionHost::new(
                SystemCommandRunner,
                ApfsSubstrateConfig::new(&store, root.path().join("checkout")),
            )
            .unwrap(),
        );
        let layout = BuildVolumeLayout::new(&project).unwrap();
        let volumes = BuildVolumes::new(Arc::clone(&host), layout.clone());
        let main = Owner {
            name: WorkspaceName::main(),
            incarnation: WorkspaceIncarnation::new("0".repeat(32)).unwrap(),
        };
        let checkout = |name: &str| {
            let checkout = root.path().join(name);
            fs::create_dir_all(checkout.join(".cowshed")).unwrap();
            checkout
        };
        let main_checkout = checkout("main");

        // Main's volume at 1 GiB, linked from its checkout, with a seed cloned from it.
        let live = BuildVolumeId::mint();
        let mount = test_volume(&host, &layout, &live, 1);
        BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", ".nx/workspace-data").unwrap()],
            fingerprint: None,
        }
        .write(&mount)
        .unwrap();
        layout
            .write_record(&live, &BuildVolumeRecord::new(None, linked("main")))
            .unwrap();
        link::point(&main_checkout, &mount).unwrap();
        host.clone_build_volume(
            &layout,
            &live,
            &BuildVolumeId::mint(),
            &BuildVolumeRecord::new(
                None,
                BuildVolumeRole::Seed {
                    target: main.name.clone(),
                    incarnation: main.incarnation.clone(),
                },
            ),
        )
        .expect("seed main");
        assert!(filesystem_bytes(&mount) <= GIB);

        let grown = volumes
            .resize(
                main.clone(),
                main_checkout.clone(),
                ImageCapacity::from_gibibytes(2),
            )
            .await
            .expect("grow main's build volume");
        assert_eq!(grown.previous, ImageCapacity::from_gibibytes(1));
        assert!(
            grown.capacity >= ImageCapacity::from_gibibytes(2),
            "{grown:?}"
        );
        assert!(
            filesystem_bytes(&mount) > 3 * GIB / 2,
            "the remounted volume's filesystem grew: {}",
            filesystem_bytes(&mount)
        );
        assert_eq!(
            volumes.layout.linked(&main_checkout).unwrap(),
            Some(live.clone())
        );

        // A fork made afterwards clones main's seed, and so starts at the new capacity.
        let topic = Owner {
            name: WorkspaceName::new("topic").unwrap(),
            incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
        };
        let topic_checkout = checkout("topic");
        let prepared = volumes
            .prepare_fork(main.clone(), main_checkout.clone(), topic.name.clone())
            .await
            .expect("prepare the fork from main's seed");
        let forked = volumes
            .finish_fork(prepared, topic, topic_checkout.clone())
            .await
            .expect("fork from main's seed")
            .expect("main has a seed");
        assert!(
            filesystem_bytes(&layout.mount(&forked)) > 3 * GIB / 2,
            "the fork inherited the grown seed: {}",
            filesystem_bytes(&layout.mount(&forked))
        );

        for id in layout.list().unwrap() {
            assert_eq!(
                host.release_build_volume(&layout, &id).unwrap(),
                BuildVolumeRelease::Deleted
            );
        }
    }

    /// A scratch APFS store with one project's build volumes, for the refusal and adoption tests.
    #[cfg(target_os = "macos")]
    struct Scratch {
        root: crate::scratch_apfs::ScratchRoot,
        host: Arc<Host>,
        layout: BuildVolumeLayout,
        volumes: BuildVolumes,
    }

    #[cfg(target_os = "macos")]
    impl Scratch {
        fn new(name: &str) -> Self {
            use crate::storage::apfs::ApfsSubstrateConfig;
            let root = crate::scratch_apfs::ScratchRoot::new(name).expect("scratch root");
            let store = root.path().join("store");
            fs::create_dir_all(&store).unwrap();
            let project = ProjectPaths::with_mount_root(
                &store,
                root.path().join("mnt"),
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap();
            let host = Arc::new(
                MacOsApfsExecutionHost::new(
                    SystemCommandRunner,
                    ApfsSubstrateConfig::new(&store, root.path().join("checkout")),
                )
                .unwrap(),
            );
            let layout = BuildVolumeLayout::new(&project).unwrap();
            let volumes = BuildVolumes::new(Arc::clone(&host), layout.clone());
            Self {
                root,
                host,
                layout,
                volumes,
            }
        }

        /// `name`'s checkout, linked to a new live volume of `gib` GiB recorded as its own.
        fn linked_checkout(&self, name: &str, gib: u64) -> (PathBuf, BuildVolumeId, PathBuf) {
            let checkout = self.root.path().join(name);
            fs::create_dir_all(checkout.join(".cowshed")).unwrap();
            let id = BuildVolumeId::mint();
            let mount = test_volume(&self.host, &self.layout, &id, gib);
            BuildVolumeState::default().write(&mount).unwrap();
            self.layout
                .write_record(&id, &BuildVolumeRecord::new(None, linked(name)))
                .unwrap();
            link::point(&checkout, &mount).unwrap();
            (checkout, id, mount)
        }

        fn release_all(&self) {
            for id in self.layout.list().unwrap() {
                assert_eq!(
                    self.host.release_build_volume(&self.layout, &id).unwrap(),
                    BuildVolumeRelease::Deleted
                );
            }
        }
    }

    /// Build volume `id`, mounted, its image recording `gib` GiB: a clone of the run's blank
    /// image put in place and mounted the way a fork's volume is. Creation is not what these
    /// tests prove. Above the 1 GiB test cap only the clone's ASIF header grows and its container
    /// stays the template's: these tests compare image capacities, which the header records, and
    /// a mint at another capacity would cost the store a template of its own — a `diskutil image
    /// create`, a formatting attach and a detach.
    #[cfg(target_os = "macos")]
    fn test_volume(
        host: &Host,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        gib: u64,
    ) -> PathBuf {
        use crate::apfs::{CommandRunner, SystemCommandRunner};
        let image = layout.image(id);
        crate::blank_image::blank_image(&image);
        let capacity = ImageCapacity::from_gibibytes(gib);
        if capacity > crate::blank_image::CAPACITY {
            SystemCommandRunner
                .grow_image(&image, capacity)
                .expect("grow the clone's image header");
        }
        host.mount_build_volume(layout, id)
            .expect("mount a build volume")
    }

    #[cfg(target_os = "macos")]
    fn owner(name: &str, digit: char) -> Owner {
        Owner {
            name: WorkspaceName::new(name).unwrap(),
            incarnation: WorkspaceIncarnation::new(digit.to_string().repeat(32)).unwrap(),
        }
    }

    /// A resize that asks for no growth is the caller's mistake, decided from the image's own
    /// limits before anything is detached.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_build_resize_that_does_not_grow_is_usage() {
        let scratch = Scratch::new("build-resize-usage");
        let (checkout, id, _) = scratch.linked_checkout("main", 1);
        let refused = scratch
            .volumes
            .resize(
                owner("main", '0'),
                checkout,
                ImageCapacity::from_gibibytes(1),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code, crate::ErrorCode::Usage, "{refused}");
        assert_eq!(
            scratch
                .host
                .build_volume_capacity(&scratch.layout, &id)
                .unwrap(),
            ImageCapacity::from_gibibytes(1)
        );
        scratch.release_all();
    }

    /// A volume with a file held open refuses the resize as a Conflict: the kernel will not let
    /// go of it, and the image keeps its capacity.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_build_resize_of_a_held_volume_is_a_conflict() {
        let scratch = Scratch::new("build-resize-held");
        let (checkout, id, mount) = scratch.linked_checkout("main", 1);
        let held = fs::File::create(mount.join("held")).unwrap();
        let refused = scratch
            .volumes
            .resize(
                owner("main", '0'),
                checkout,
                ImageCapacity::from_gibibytes(2),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code, crate::ErrorCode::Conflict, "{refused}");
        assert_eq!(
            scratch
                .host
                .build_volume_capacity(&scratch.layout, &id)
                .unwrap(),
            ImageCapacity::from_gibibytes(1)
        );
        drop(held);
        scratch.release_all();
    }

    /// Adoption never shrinks a target (16_build_volumes.md, "Substrate"): a landing volume
    /// smaller than the target's grows to the target's capacity while it is quiet, before the
    /// seed is frozen from it, so the target and every later fork end at the larger capacity. A
    /// landing volume already larger keeps its own.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_adoption_ends_at_the_larger_of_the_two_capacities() {
        let scratch = Scratch::new("build-adopt-capacity");
        let capacity = |id: &BuildVolumeId| {
            scratch
                .host
                .build_volume_capacity(&scratch.layout, id)
                .unwrap()
        };
        let (main_checkout, _, _) = scratch.linked_checkout("main", 2);
        let (topic_checkout, topic, _) = scratch.linked_checkout("topic", 1);
        let quiet = scratch
            .volumes
            .quiesce(topic_checkout, main_checkout.clone())
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert!(capacity(&topic) >= ImageCapacity::from_gibibytes(2));
        let tree = GitOid::new("a".repeat(40)).unwrap();
        scratch
            .volumes
            .freeze_seed(&quiet, owner("main", '0'), tree.clone())
            .await
            .unwrap();
        let (seed, _) = scratch
            .layout
            .seed_of(&WorkspaceName::main(), &owner("main", '0').incarnation)
            .unwrap()
            .unwrap();
        assert!(capacity(&seed) >= ImageCapacity::from_gibibytes(2));
        let closed = scratch
            .volumes
            .close_target(main_checkout.clone())
            .await
            .unwrap()
            .expect("nothing holds the target's volume");
        scratch
            .volumes
            .adopt(
                &quiet,
                closed,
                WorkspaceName::main(),
                main_checkout.clone(),
                tree,
            )
            .await
            .unwrap()
            .expect("nothing opened the target's volume since");
        assert_eq!(
            scratch.volumes.layout.linked(&main_checkout).unwrap(),
            Some(topic.clone())
        );

        // A landing volume larger than its target keeps its capacity.
        let (other_checkout, other, _) = scratch.linked_checkout("other", 3);
        scratch
            .volumes
            .quiesce(other_checkout, main_checkout)
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert_eq!(capacity(&other), ImageCapacity::from_gibibytes(3));
        scratch.release_all();
    }
}
