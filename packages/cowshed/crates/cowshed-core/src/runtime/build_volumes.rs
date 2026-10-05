//! The project side of build volumes (16_build_volumes.md): a fork clones its target's seed, a
//! land quiesces the landing workspace, freezes the target's seed, and moves the target's one
//! link, and collection deletes what nothing links.
//!
//! Every step here runs inside the project actor, so no two of them interleave; what a crash
//! leaves behind is a volume nothing links, which [`BuildVolumes::collect`] deletes.

use std::collections::BTreeSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::apfs::SystemCommandRunner;
use crate::api::dto::{
    AdoptionSkip, DatabaseHolder, GcCandidate, GcDeferred, GcReason, GitOid, Sha256Digest,
};
use crate::build_volume::{
    BuildStateRefresh, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole,
    BuildVolumeState, TrackedBuildStateRefusal, link, nx,
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
/// workspace (whose seed a seed may be), and the detached ones, whose links cannot be read.
#[derive(Clone, Debug, Default)]
pub(crate) struct Links {
    pub volumes: BTreeSet<BuildVolumeId>,
    pub owners: BTreeSet<Owner>,
    pub detached: BTreeSet<WorkspaceName>,
}

/// The landing workspace's volume once nothing writes it (Land step 4).
#[derive(Clone, Debug)]
pub(crate) struct Quiet {
    id: BuildVolumeId,
    mount: PathBuf,
    state: BuildVolumeState,
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
    layout: BuildVolumeLayout,
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

    /// The build volume `checkout` links, or `None` when it links none.
    pub fn linked(&self, checkout: &Path) -> Result<Option<BuildVolumeId>> {
        let Some(target) = link::linked(checkout)? else {
            return Ok(None);
        };
        self.layout.volume_at(&target).map(Some).ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "{} names {}, which is not one of this project's build volumes",
                    checkout.join(link::BUILD_LINK).display(),
                    target.display()
                ),
                "cowshed doctor --json",
            )
        })
    }

    /// The volume `checkout` links and the state written at its root, or `None` when it links
    /// none. The checkout is mounted, which mounted its volume.
    pub fn state_of(&self, checkout: &Path) -> Result<Option<(BuildVolumeId, BuildVolumeState)>> {
        let Some(id) = self.linked(checkout)? else {
            return Ok(None);
        };
        let state = BuildVolumeState::read(&self.layout.mount(&id))?;
        Ok(Some((id, state)))
    }

    /// The mountpoint of the volume `workspace`'s mounted `checkout` links, verified as the
    /// workspace's own by its record: the build-volume grant a job of the checkout runs with.
    /// `None` when the checkout links no volume. Mounting already re-pointed a stale link, so
    /// a link that still names a volume the workspace does not own is an integrity failure,
    /// never a grant.
    pub fn grant(&self, workspace: &WorkspaceName, checkout: &Path) -> Result<Option<PathBuf>> {
        let Some(id) = self.linked(checkout)? else {
            return Ok(None);
        };
        match self.layout.resolve_link(workspace, &id)? {
            crate::build_volume::LinkResolution::Keep => Ok(Some(self.layout.mount(&id))),
            resolution => Err(CowshedError::integrity(
                format!(
                    "{workspace}'s mounted build link names {id}, which mounting should have \
                     resolved ({resolution:?})"
                ),
                format!("cowshed detach {workspace}, then retry"),
            )),
        }
    }

    /// Bring `checkout`'s build volume in line with what capability detection names now
    /// (16_build_volumes.md, "One link per checkout"). With `Discovered::Unchanged` the held
    /// paths are re-linked where a tool displaced them. With fresh discovery, new paths join the
    /// volume's state (held ones never move), every path is linked, and the state records the new
    /// fingerprint; a checkout that links no volume yet gets its first one at `capacity`, unless
    /// nothing was discovered. A path that holds tracked source refuses before anything is
    /// deleted.
    pub async fn refresh(
        &self,
        workspace: WorkspaceName,
        checkout: PathBuf,
        discovered: Discovered,
    ) -> Result<std::result::Result<BuildStateRefresh, TrackedBuildStateRefusal>> {
        let linked = self.linked(&checkout)?;
        self.blocking(move |host, layout| {
            let (id, discovered) = match (linked, discovered) {
                (Some(id), discovered) => (id, discovered),
                (None, Discovered::Unchanged) => return Ok(Ok(BuildStateRefresh::default())),
                (None, Discovered::Changed { paths, .. }) if paths.is_empty() => {
                    return Ok(Ok(BuildStateRefresh::default()));
                }
                (
                    None,
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
                                    checkout: workspace,
                                },
                            ),
                        },
                    )?;
                    return Ok(touched.map(|(id, displaced)| BuildStateRefresh {
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

    /// Fork (16_build_volumes.md, "Fork"): the checkout staged at `checkout`, which `destination`
    /// is about to become, gets its own clone of `source`'s latest seed, mounted, with no daemon
    /// record, and linked; and `destination` gets its own seed, a second clone of the same seed,
    /// so it is a target others can fork from. A source with no seed gives none: its checkout
    /// links nothing either, unless something is wrong, which refuses.
    pub async fn fork(
        &self,
        source: Owner,
        destination: Owner,
        checkout: PathBuf,
    ) -> Result<Option<BuildVolumeId>> {
        self.blocking(move |host, layout| {
            let Some((seed, record)) = layout.seed_of(&source.name, &source.incarnation)? else {
                if link::linked(&checkout)?.is_some() {
                    return Err(CowshedError::integrity(
                        format!(
                            "workspace {} links a build volume but has no seed to fork from",
                            source.name
                        ),
                        "cowshed doctor --json",
                    ));
                }
                return Ok(None);
            };
            let started = Instant::now();
            // The seed first: a crash after it leaves an unlinked volume, never a target
            // without a seed.
            let own_seed = BuildVolumeId::mint();
            host.clone_build_volume(
                layout,
                &seed,
                &own_seed,
                &BuildVolumeRecord::new(
                    record.tree.clone(),
                    BuildVolumeRole::Seed {
                        target: destination.name.clone(),
                        incarnation: destination.incarnation.clone(),
                    },
                ),
            )
            .map_err(storage)?;
            let (live, mount) = host
                .fork_build_volume(
                    layout,
                    &seed,
                    &BuildVolumeRecord::new(
                        record.tree,
                        BuildVolumeRole::Linked {
                            checkout: destination.name.clone(),
                        },
                    ),
                )
                .map_err(storage)?;
            link::point(&checkout, &mount)?;
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
        let id = self.linked(&checkout)?;
        let target = self.linked(&target_checkout)?;
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

    /// Land step 5: `target`'s new seed is a clone of the quiet landing volume, and every older
    /// seed of `target` is deleted.
    pub async fn freeze_seed(&self, quiet: &Quiet, target: Owner, tree: GitOid) -> Result<()> {
        let source = quiet.id.clone();
        self.blocking(move |host, layout| {
            let previous = seeds_of(layout, &target)?;
            let seed = BuildVolumeId::mint();
            host.clone_build_volume(
                layout,
                &source,
                &seed,
                &BuildVolumeRecord::new(
                    Some(tree),
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
            )
            .map_err(storage)?;
            for old in previous {
                // A seed is never mounted, so nothing can hold it.
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
        })
        .await
    }

    /// Land step 6: when nothing but the target's daemon holds its task database, stop that
    /// daemon, drop the landing volume's daemon record, and rename the target's build link onto
    /// the landing volume. The target's previous volume is unlinked and released when idle.
    /// Answers how long the move took.
    pub async fn adopt(
        &self,
        quiet: &Quiet,
        target: WorkspaceName,
        target_checkout: PathBuf,
        tree: GitOid,
    ) -> Result<std::result::Result<u64, AdoptionSkip>> {
        let previous = self.linked(&target_checkout)?;
        let quiet = quiet.clone();
        self.blocking(move |host, layout| {
            let Some(previous) = previous else {
                return Ok(Err(AdoptionSkip::NoTargetVolume));
            };
            let started = Instant::now();
            let previous_mount = host.mount_build_volume(layout, &previous).map_err(storage)?;
            let previous_state = BuildVolumeState::read(&previous_mount)?;
            if let Err(busy) = nx::close(&previous_mount, &previous_state)
                .map_err(|error| io("close the target's Nx state", &previous_mount, &error))?
            {
                return Ok(Err(skip(busy, Side::Target)));
            }
            nx::discard_daemon_records(&quiet.mount, &quiet.state)
                .map_err(|error| io("discard the landing daemon record", &quiet.mount, &error))?;
            link::point(&target_checkout, &quiet.mount)?;
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
            Ok(Ok(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)))
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
        let Some(id) = self.linked(&checkout)? else {
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
            if !dry_run {
                host.sweep_build_volume_staging(layout).map_err(storage)?;
            }
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
    /// It has no record, and these detached workspaces' links cannot be read: any may name it.
    UnrecordedWhileDetached(Vec<WorkspaceName>),
}

impl Deferral {
    /// Whether this is the ordinary state of a detached workspace's own volume rather than
    /// something a person should look at.
    pub fn is_routine(&self) -> bool {
        matches!(self, Self::DetachedCheckout(_))
    }
}

impl std::fmt::Display for Deferral {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(diagnostic) => write!(formatter, "still in use: {diagnostic}"),
            Self::RecordUnreadable(error) => write!(formatter, "its record is unreadable: {error}"),
            Self::SizeUnreadable(error) => write!(formatter, "its size is unreadable: {error}"),
            Self::DetachedCheckout(checkout) => write!(
                formatter,
                "its record names {checkout}, which is detached; decided once {checkout} is attached"
            ),
            Self::UnrecordedWhileDetached(detached) => {
                formatter.write_str("it has no record, and the links of detached ")?;
                for (index, name) in detached.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{name}")?;
                }
                formatter.write_str(" cannot be read until they are attached")
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
/// it named, never counted reachable and never deleted; so is a volume whose record or size
/// cannot be read. Nothing is deleted on a guess.
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
            Ok(None) if links.detached.is_empty() => {
                doomed.push((id, GcReason::UnrecordedBuildVolume));
                continue;
            }
            Ok(None) => {
                let detached = links.detached.iter().cloned().collect();
                plan.deferred
                    .push((id, Deferral::UnrecordedWhileDetached(detached)));
                continue;
            }
            Err(error) => {
                plan.deferred
                    .push((id, Deferral::RecordUnreadable(error.to_string())));
                continue;
            }
        };
        match record.role {
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

#[derive(Clone, Copy)]
enum Side {
    Landing,
    Target,
}

fn skip(busy: nx::Busy, side: Side) -> AdoptionSkip {
    let holder = |holder: nx::Holder| DatabaseHolder {
        pid: holder.pid,
        command: holder.command,
    };
    match (busy, side) {
        (nx::Busy::Held { database, holders }, Side::Landing) => AdoptionSkip::LandingHeld {
            database,
            holders: holders.into_iter().map(holder).collect(),
        },
        (nx::Busy::Held { database, holders }, Side::Target) => AdoptionSkip::TargetHeld {
            database,
            holders: holders.into_iter().map(holder).collect(),
        },
        (nx::Busy::DaemonStayed { daemon }, Side::Landing) => AdoptionSkip::LandingDaemonStayed {
            daemon: holder(daemon),
        },
        (nx::Busy::DaemonStayed { daemon }, Side::Target) => AdoptionSkip::TargetDaemonStayed {
            daemon: holder(daemon),
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
                Deferral::UnrecordedWhileDetached(vec![name("topic")]),
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
    /// a fork made afterwards inherits the new capacity; a request that does not grow is the
    /// caller's mistake, and a volume with a file open refuses and keeps its capacity.
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
                ApfsSubstrateConfig::new(
                    &store,
                    root.path().join("caches"),
                    root.path().join("checkout"),
                ),
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
        let mount = host
            .create_build_volume(&layout, &live, ImageCapacity::from_gibibytes(1))
            .expect("create main's build volume");
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
        assert_eq!(volumes.linked(&main_checkout).unwrap(), Some(live.clone()));

        // A fork made afterwards clones main's seed, and so starts at the new capacity.
        let topic = Owner {
            name: WorkspaceName::new("topic").unwrap(),
            incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
        };
        let topic_checkout = checkout("topic");
        let forked = volumes
            .fork(main.clone(), topic, topic_checkout.clone())
            .await
            .expect("fork from main's seed")
            .expect("main has a seed");
        assert!(
            filesystem_bytes(&layout.mount(&forked)) > 3 * GIB / 2,
            "the fork inherited the grown seed: {}",
            filesystem_bytes(&layout.mount(&forked))
        );

        let not_growing = volumes
            .resize(
                main.clone(),
                main_checkout.clone(),
                ImageCapacity::from_gibibytes(2),
            )
            .await
            .unwrap_err();
        assert_eq!(not_growing.code, crate::ErrorCode::Usage, "{not_growing}");

        let held = fs::File::create(mount.join("held")).unwrap();
        let busy = volumes
            .resize(
                main.clone(),
                main_checkout.clone(),
                ImageCapacity::from_gibibytes(3),
            )
            .await
            .unwrap_err();
        assert_eq!(busy.code, crate::ErrorCode::Conflict, "{busy}");
        assert!(
            filesystem_bytes(&mount) < 5 * GIB / 2,
            "a refused resize keeps the capacity"
        );
        drop(held);

        for id in layout.list().unwrap() {
            assert_eq!(
                host.release_build_volume(&layout, &id).unwrap(),
                BuildVolumeRelease::Deleted
            );
        }
    }
}
