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
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, BuildVolumeState, link,
    nx,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::native::{BuildVolumeRelease, MacOsApfsExecutionHost};
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

/// What collection found and did.
#[derive(Clone, Debug, Default)]
pub(crate) struct Collection {
    pub examined: u64,
    pub reclaimed: u64,
    pub freed_bytes: u64,
    pub candidates: Vec<GcCandidate>,
    pub deferred: Vec<GcDeferred>,
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
            let live = BuildVolumeId::mint();
            host.clone_build_volume(
                layout,
                &seed,
                &live,
                &BuildVolumeRecord::new(
                    record.tree,
                    BuildVolumeRole::Linked {
                        checkout: destination.name.clone(),
                    },
                ),
            )
            .map_err(storage)?;
            let mount = host.mount_build_volume(layout, &live).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            nx::discard_daemon_records(&mount, &state)
                .map_err(|error| io("discard the seed's Nx daemon record", &mount, &error))?;
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
    /// volume's Nx daemon is stopped and its task database must have no holder left.
    pub async fn quiesce(
        &self,
        checkout: PathBuf,
    ) -> Result<std::result::Result<Quiet, AdoptionSkip>> {
        let id = self.linked(&checkout)?;
        self.blocking(move |host, layout| {
            let Some(id) = id else {
                return Ok(Err(AdoptionSkip::NoLandingVolume));
            };
            let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            match nx::close(&mount, &state)
                .map_err(|error| io("close the landing volume's Nx state", &mount, &error))?
            {
                Ok(()) => Ok(Ok(Quiet { id, mount, state })),
                Err(busy) => Ok(Err(skip(busy, Side::Landing))),
            }
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
            let live = BuildVolumeId::mint();
            host.clone_build_volume(
                layout,
                &seed,
                &live,
                &BuildVolumeRecord::new(
                    record.tree,
                    BuildVolumeRole::Linked {
                        checkout: workspace,
                    },
                ),
            )
            .map_err(storage)?;
            let mount = host.mount_build_volume(layout, &live).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            nx::discard_daemon_records(&mount, &state)
                .map_err(|error| io("discard the seed's Nx daemon record", &mount, &error))?;
            link::point(&checkout, &mount)
        })
        .await
    }

    /// Mount every linked volume a detached image could not have mounted itself, then delete
    /// (16_build_volumes.md, "Garbage collection") each build volume nothing links that is not
    /// a seed and that the kernel lets go of, each seed that is not its existing target's
    /// latest, and each image an interrupted creation left without a record. A volume the
    /// kernel refuses to detach is deferred to the next pass with the kernel's words.
    pub async fn collect(&self, links: Links, dry_run: bool) -> Result<Collection> {
        self.blocking(move |host, layout| {
            if !dry_run {
                host.sweep_build_volume_staging(layout).map_err(storage)?;
            }
            let mut collection = Collection::default();
            let mut latest = std::collections::BTreeMap::<Owner, (String, BuildVolumeId)>::new();
            let mut doomed = Vec::new();
            for id in layout.list()? {
                collection.examined += 1;
                let record = match layout.read_record(&id) {
                    Ok(record) => record,
                    Err(_) if !layout.record(&id).exists() => {
                        doomed.push((id, GcReason::UnrecordedBuildVolume));
                        continue;
                    }
                    Err(error) => return Err(error),
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
                                if let Some((_, older)) =
                                    latest.insert(owner, (record.created_at, id))
                                {
                                    doomed.push((older, GcReason::SupersededSeed));
                                }
                            }
                        }
                    }
                    BuildVolumeRole::Linked { checkout } if links.detached.contains(&checkout) => {}
                    BuildVolumeRole::Linked { .. } | BuildVolumeRole::Unlinked => {
                        if !links.volumes.contains(&id) {
                            doomed.push((id, GcReason::UnlinkedBuildVolume));
                        }
                    }
                }
            }
            for (id, reason) in doomed {
                let image = layout.image(&id);
                let bytes = std::fs::metadata(&image)
                    .map(|metadata| metadata.blocks().saturating_mul(512))
                    .unwrap_or(0);
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
                        collection.deferred.push(GcDeferred {
                            path: image,
                            diagnostic,
                        });
                    }
                }
            }
            Ok(collection)
        })
        .await
    }
}

fn seeds_of(layout: &BuildVolumeLayout, owner: &Owner) -> Result<Vec<BuildVolumeId>> {
    let mut seeds = Vec::new();
    for id in layout.list()? {
        if layout.record(&id).exists()
            && layout
                .read_record(&id)?
                .is_seed_of(&owner.name, &owner.incarnation)
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
