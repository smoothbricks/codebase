//! The project side of build volumes (16_build_volumes.md): a fork first refreshes its target's
//! seed for the tree it checks out, from a workspace that landed that tree outside `cowshed land`
//! or from the target's own quiet volume, then clones it; a land quiesces the landing workspace,
//! freezes the target's seed, and moves the target's one link onto the landing volume, copying
//! nothing into it; and collection deletes what nothing links.
//!
//! Every step here runs inside one process's project actor, so no two of that process's steps
//! interleave; another cowshed process's can, which is why collection defers what a workspace
//! still being created may own ([`Links::creating`]). What a crash leaves behind is a volume
//! nothing links, which [`BuildVolumes::collect`] deletes.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use super::storage_host::RuntimeStorageHost;
use crate::api::dto::{
    AdoptionSkip, DatabaseHolder, GcCandidate, GcDeferred, GcReason, GitOid, Sha256Digest,
};
use crate::api::held_lock::{LockHolding, LockOperation};
use crate::build_volume::migrate::{MintedBeside, MintedVolume};
use crate::build_volume::{
    BuildStateRefresh, BuildVolumeHold, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord,
    BuildVolumeRole, BuildVolumeStage, BuildVolumeState, CapturedBuildVolume, Registrant,
    Registration, ReleaseClaim, TrackedBuildStateRefusal, cargo, link, nx,
};
use crate::capabilities::BuildStatePath;
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
#[cfg(test)]
use crate::storage::apfs::ApfsExecutionHost;
use crate::storage::apfs::ApfsStorageError;
use crate::storage::apfs::native::{
    BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease, MacOsApfsExecutionHost,
};
use crate::storage::lifecycle::ResizeOutcome;
use crate::{CowshedError, Result};
use cowshed_apfs::SystemCommandRunner;
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::store_lock::LockMode;

type Host = MacOsApfsExecutionHost<SystemCommandRunner>;

mod compaction;
mod nx_clear;
mod reseed;
pub(crate) use compaction::{
    Admission, CompactionDue, CompactionTurn, Published, SEED_COMPACTION_CATCH_UPS,
    SEED_COMPACTION_WAIT, seconds_since, start_compaction,
};
pub(crate) use reseed::ReseedWorkspace;

pub(crate) use compaction::lands_hold;
/// A workspace at one incarnation: whose seed a seed is.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Owner {
    pub name: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
}

/// What links build volumes right now: every mounted checkout's build link, every existing
/// workspace (whose seed a seed may be), the detached ones, whose links cannot be read, and the
/// ones being created: a create or fork forks its build volume and seed into the staged checkout,
/// and an adopt mints main's first one beside main's image, before the workspace exists, so until
/// its intent completes nothing readable names either.
///
/// `checkouts` is where each existing workspace's checkout is, so a release can read that
/// checkout's link again under its claim: a land that moves a target's link onto a volume this
/// snapshot saw linked by nothing makes the volume the target's after the snapshot was taken.
///
/// `journaled` is every build volume id an unfinished re-adoption's journal record names: that
/// record, not a workspace name, is the authority for what it publishes.
#[derive(Clone, Debug, Default)]
pub(crate) struct Links {
    pub volumes: BTreeSet<BuildVolumeId>,
    pub owners: BTreeSet<Owner>,
    pub detached: BTreeSet<WorkspaceName>,
    pub creating: BTreeSet<WorkspaceName>,
    pub journaled: BTreeSet<BuildVolumeId>,
    pub checkouts: BTreeMap<WorkspaceName, PathBuf>,
}

/// The landing workspace's volume, held for its land from before the landing workspace's jobs
/// stop until the target's link and record name it (Land steps 4–6): no collection, in this
/// process or another, releases it meanwhile, whatever links it read before the move.
#[derive(Debug)]
pub(crate) struct Landing {
    id: BuildVolumeId,
    hold: BuildVolumeHold,
}

/// The landing volume after supervised jobs and Nx settle (Land step 4), held against GC and
/// replacement job admission until capture and adoption finish.
#[derive(Clone, Debug)]
pub(crate) struct Quiet {
    id: BuildVolumeId,
    mount: PathBuf,
    state: BuildVolumeState,
    hold: Arc<BuildVolumeHold>,
    _stopped: Arc<super::supervisor_socket::BoundSocket>,
}

impl Quiet {
    pub(crate) fn id(&self) -> &BuildVolumeId {
        &self.id
    }

    /// Reopen admission before replacement supervisors start, retaining the landing's GC hold
    /// through the remaining build-volume steps (16_build_volumes.md, Land steps 4–7).
    pub(crate) fn release_admission(self) -> Arc<BuildVolumeHold> {
        self.hold
    }
}

/// A fork's live volume as its source's image lock leaves it (Fork step 3): a capture of the
/// source's latest seed, registered but not yet published or mounted.
/// [`BuildVolumes::ready_fork`] readies it once that lock is let go;
/// [`BuildVolumes::abandon_captured_fork`] releases it when the fork's own image is not captured.
#[derive(Debug)]
pub(crate) struct CapturedFork {
    seed: BuildVolumeId,
    tree: Option<GitOid>,
    captured: CapturedBuildVolume,
    started: Instant,
}

/// A fork's live volume, published and mounted beside its own image's attach and mount, after its
/// source's image lock is let go (Fork steps 2 and 3), with `own_seed`, the destination's seed,
/// cloned from it before the mount: the staged checkout it is for does not exist yet.
/// [`BuildVolumes::finish_fork`] links the checkout to it; [`BuildVolumes::abandon_fork`]
/// releases both when no checkout staged.
#[derive(Debug)]
pub(crate) struct PreparedFork {
    seed: BuildVolumeId,
    own_seed: BuildVolumeId,
    live: BuildVolumeId,
    mount: PathBuf,
    started: Instant,
}

/// A kept workspace's fresh volume as the land's target lock leaves it: captured from the
/// target's latest seed, registered, not yet published, mounted or linked. Dropped unready,
/// it stays registered for collection.
#[derive(Debug)]
#[must_use = "ready_refork links the kept workspace's fresh volume"]
pub(crate) struct Refork {
    captured: CapturedBuildVolume,
    checkout: PathBuf,
}
/// The target's volume once its Nx state is closed (Land step 5): its daemon stopped and its
/// task database held by nothing.
#[derive(Clone, Debug)]
pub(crate) struct Closed {
    id: BuildVolumeId,
    mount: PathBuf,
    state: BuildVolumeState,
}

/// The target's image lock, held by its land from before it freezes the target's seed until the
/// target's link names the adopted volume and a kept workspace has its clone of the new seed
/// (Land steps 5.4 to 6.4): every fork of the target holds the same lock from before it reads
/// the target's seed and link until it has linked its own (Fork steps 1 to 3), and so does every
/// reseed of the target, so neither reads a seed or a volume the land deletes. The steps that
/// need it take it by reference; only [`BuildVolumes::try_lock_target`] and [`Contended::wait`]
/// make one.
#[must_use = "the target's image lock is let go when this is dropped"]
pub(crate) struct TargetLock<H: RuntimeStorageHost = Host> {
    target: Owner,
    _guard: H::LockGuard,
}

/// The target's image lock, held by another cowshed operation on the target when the land asked
/// for it.
#[must_use = "wait takes the target's image lock"]
pub(crate) struct Contended<H: RuntimeStorageHost = Host> {
    volumes: BuildVolumes<H>,
    target: Owner,
    lock: PathBuf,
    holding: LockHolding,
}

impl<H: RuntimeStorageHost> Contended<H> {
    /// Wait until whatever holds the target's image lock lets go of it, and take it.
    pub async fn wait(self) -> Result<TargetLock<H>> {
        let Self {
            volumes,
            target,
            lock,
            holding,
        } = self;
        let guard = volumes
            .blocking(move |host, _| {
                host.lock_images(&[lock], LockMode::Wait, &holding)
                    .map_err(storage)?
                    .ok_or_else(|| {
                        CowshedError::internal("a blocking image lock answered that it was held")
                    })
            })
            .await?;
        Ok(TargetLock {
            target,
            _guard: guard,
        })
    }
}

/// What [`BuildVolumes::freeze_seed`] did with the target's seed after Nx and existing Cargo
/// profiles admitted capture (Land step 5.4).
#[derive(Debug)]
pub(crate) enum Freeze {
    /// The seed is captured from the guarded landing volume, and every older seed claimed for
    /// its retirement once the target's image lock is let go.
    Frozen { superseded: SupersededSeeds },
    /// A conflicting storage claim refused capture; the previous seed stays.
    SealBusy { reason: String },
}

/// The seeds of `target` a new seed replaced (16_build_volumes.md, "Garbage collection"), each
/// claimed for its release under the target's image lock, after which no fork or land of the
/// target reads them: [`BuildVolumes::retire_superseded`] commits each to `build/.deleting`
/// once that lock is let go, so no seed's rename, sync or nudge of the gateway runs under it.
/// Dropped unretired, the claims go and the seeds stay recorded under the target, for the next
/// seed's retirement or `gc`.
#[derive(Debug)]
#[must_use = "retire_superseded releases the seeds once the target's image lock is let go"]
pub(crate) struct SupersededSeeds {
    target: WorkspaceName,
    claims: Vec<ReleaseClaim>,
}

/// What [`BuildVolumes::adopt`] did: how long the move took, and the target's previous volume.
#[derive(Debug)]
#[must_use = "its previous volume is released by release_previous"]
pub(crate) struct Adopted {
    pub elapsed_ms: u64,
    pub previous: Superseded,
}

/// A target's previous build volume once an adoption moved its link off it and recorded it
/// [`BuildVolumeRole::Superseded`] under the target at its incarnation:
/// [`BuildVolumes::release_previous`] releases it unless a job admitted on it before the move
/// still holds it, and then the release when that job's hold ends does.
#[derive(Debug)]
#[must_use = "release_previous releases the target's previous build volume"]
pub(crate) struct Superseded {
    id: BuildVolumeId,
    owner: Owner,
}

/// How near a volume's recorded tree is to the tree a fork checks out (16_build_volumes.md,
/// "Which volume a seed holds"). The derived order is the preference: the fork's own tree, then
/// fewer paths differing from it, then a tree cowshed cannot compare at all.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Nearness {
    Equal,
    /// This many paths differ between the two trees.
    Paths(u64),
    /// No recorded tree, or one the target's repository cannot compare.
    Unknown,
}

/// Where cowshed placed a target's live volume, against the tree a fork checks out: the tree the
/// target's checkout had when cowshed linked the volume (its record's `checkedOut`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Placed {
    /// At the fork's tree: the target's branch has not moved since.
    Here,
    /// At this other tree: the target's checkout moved since cowshed placed the volume.
    Elsewhere(GitOid),
    /// The record predates `checkedOut`, or the volume was placed at no tree cowshed knows.
    Unrecorded,
}

impl Placed {
    fn of(checked_out: Option<&GitOid>, tree: &GitOid) -> Self {
        match checked_out {
            Some(placed) if placed == tree => Self::Here,
            Some(placed) => Self::Elsewhere(placed.clone()),
            None => Self::Unrecorded,
        }
    }
}

/// A target's latest seed: the tree it was frozen at, how near that is to the fork's tree, and
/// the instant of the last write it holds.
#[derive(Clone, Debug)]
pub(crate) struct Frozen {
    pub id: BuildVolumeId,
    pub tree: Option<GitOid>,
    pub nearness: Nearness,
    pub written: SystemTime,
}

/// A target's latest seed against the live build volume it links, both measured against the tree
/// a fork checks out and as the instant of the last write each image holds (16_build_volumes.md,
/// "Targets and seeds").
#[derive(Clone, Debug)]
pub(crate) struct SeedAge {
    pub live: BuildVolumeId,
    /// The tree the live volume's record names: the one a reseed records.
    pub tree: Option<GitOid>,
    pub nearness: Nearness,
    pub placed: Placed,
    /// When the live volume's image was last written, after its volume was flushed.
    pub written: SystemTime,
    /// `None` for a target that has no seed.
    pub seed: Option<Frozen>,
}

impl SeedAge {
    /// Whether a reseed replaces the seed with a capture of the live volume: the live volume is
    /// nearer the fork's tree, or as near and holds a write the seed does not. A seed nearer
    /// than the live volume is kept however much later the live volume was written.
    pub fn reseeds(&self) -> bool {
        self.seed.as_ref().is_none_or(|seed| {
            (self.nearness, Reverse(self.written)) < (seed.nearness, Reverse(seed.written))
        })
    }

    /// The tree cowshed placed the live volume at, when the target's branch moved past every
    /// volume cowshed knows without a `cowshed land` (16_build_volumes.md, "Which volume a seed
    /// holds", step 1): its checkout moved away from that tree, and neither volume was built at
    /// the fork's tree. A record that cannot say where its volume was placed is never read as
    /// moved: the fork is colder, never seeded from another workspace's volume.
    pub fn moved_from(&self) -> Option<&GitOid> {
        let Placed::Elsewhere(placed) = &self.placed else {
            return None;
        };
        let built_here = self.nearness == Nearness::Equal
            || self
                .seed
                .as_ref()
                .is_some_and(|seed| seed.nearness == Nearness::Equal);
        (!built_here).then_some(placed)
    }

    /// How long before the live volume's last write the seed was frozen: `None` when the seed
    /// holds it, or when there is no seed.
    pub fn behind(&self) -> Option<Duration> {
        let seed = self.seed.as_ref()?;
        self.written
            .duration_since(seed.written)
            .ok()
            .filter(|behind| !behind.is_zero())
    }
}

/// The tree a fork checks out, and the live workspaces that may have landed it in the target
/// outside `cowshed land` (16_build_volumes.md, "Which volume a seed holds").
#[derive(Clone, Debug)]
pub(crate) struct Aim {
    pub tree: GitOid,
    /// Every live workspace but the target when `tree` is the target's `HEAD^{tree}`; empty
    /// otherwise, since only the target's own tree can have landed in it.
    pub landed: Vec<Peer>,
}

/// A live workspace whose checkout may have the fork's tree checked out.
#[derive(Clone, Debug)]
pub(crate) struct Peer {
    pub name: WorkspaceName,
    pub checkout: PathBuf,
    /// Its image lock: the one its own forks, lands and removal take.
    pub lock: PathBuf,
}

/// What a seed refresh (Fork step 2) did, each live workspace at the fork's tree that did not
/// seed the target, with why, and the seeds a new one replaced, for their retirement once the
/// target's image lock is let go.
#[derive(Debug)]
pub(crate) struct Refreshed {
    pub passed: Vec<(WorkspaceName, LandedSkip)>,
    pub outcome: Refresh,
    pub superseded: Option<SupersededSeeds>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Refresh {
    /// The target links no build volume: it has no seed to keep up.
    NoBuildVolume,
    /// The seed stays: it is nearer the fork's tree than the live volume, or as near and holds
    /// every write the live volume has had.
    Kept,
    /// The seed is now a capture of the target's live volume. It was frozen `behind_ms` before
    /// the live volume's last write, or the target had none.
    Reseeded {
        behind_ms: Option<u64>,
        elapsed_ms: u64,
    },
    /// The target's branch moved to `tree` outside `cowshed land`, and the seed is now a capture
    /// of the build volume of `workspace`, which has that tree checked out.
    Landed {
        workspace: WorkspaceName,
        tree: GitOid,
        elapsed_ms: u64,
    },
    /// The seed stays behind: the live volume has a writer, and a clone of it could hold a
    /// half-written Cargo unit or Nx database. The next fork tries again.
    Skipped {
        behind_ms: Option<u64>,
        reason: CaptureSkip,
    },
}

/// Why a live workspace at the fork's tree did not seed the target; the seed stays as it was.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LandedSkip {
    /// Another operation on the workspace (its own fork, land or removal) holds its image lock.
    Locked,
    /// Its `HEAD^{tree}` moved off the fork's tree before its volume was captured: a seed is
    /// never recorded at a tree its volume was not built at.
    Moved {
        tree: GitOid,
    },
    /// Its `HEAD^{tree}` could not be read.
    Unreadable {
        reason: String,
    },
    /// It links no build volume.
    NoBuildVolume,
    Capture(CaptureSkip),
}

impl std::fmt::Display for LandedSkip {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Locked => formatter.write_str(
                "another cowshed operation on it holds its image lock; the next fork retries",
            ),
            Self::Moved { tree } => write!(formatter, "its HEAD moved to tree {tree}"),
            Self::Unreadable { reason } => write!(formatter, "its HEAD^{{tree}}: {reason}"),
            Self::NoBuildVolume => formatter.write_str("it links no build volume"),
            Self::Capture(skip) => write!(formatter, "{skip}"),
        }
    }
}

/// Why a quiet volume could not be captured as a seed: something still writes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CaptureSkip {
    /// A process holds the volume's task database at admission.
    Held {
        database: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// An Nx process holds a task database's open lock, before or during its open.
    Opening {
        database: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// The volume's Nx daemon outlived its stop.
    DaemonStayed { daemon: DatabaseHolder },
    /// A Cargo build holds a target directory's build lock.
    Building {
        lock: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// A job or conflicting storage claim holds the source's build hold.
    VolumeBusy { reason: String },
}

impl std::fmt::Display for CaptureSkip {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let holders = |formatter: &mut std::fmt::Formatter<'_>, holders: &[DatabaseHolder]| {
            for (index, holder) in holders.iter().enumerate() {
                let separator = if index == 0 { "" } else { ", " };
                write!(
                    formatter,
                    "{separator}pid {} ({})",
                    holder.pid, holder.command
                )?;
            }
            Ok(())
        };
        match self {
            Self::Held {
                database,
                holders: held,
            } => {
                write!(formatter, "{} is open in ", database.display())?;
                holders(formatter, held)
            }
            Self::Opening {
                database,
                holders: held,
            } => {
                write!(formatter, "{} is being opened", database.display())?;
                if !held.is_empty() {
                    formatter.write_str(" by ")?;
                    holders(formatter, held)?;
                }
                Ok(())
            }
            Self::DaemonStayed { daemon } => write!(
                formatter,
                "the Nx daemon pid {} ({}) did not exit after SIGTERM",
                daemon.pid, daemon.command
            ),
            Self::Building {
                lock,
                holders: held,
            } => {
                write!(formatter, "a Cargo build holds {}", lock.display())?;
                if !held.is_empty() {
                    formatter.write_str(": ")?;
                }
                holders(formatter, held)
            }
            Self::VolumeBusy { reason } => {
                write!(formatter, "the build volume could not be sealed: {reason}")
            }
        }
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
    pub released_bytes: u64,
    pub candidates: Vec<GcCandidate>,
    pub deferred: Vec<Deferred>,
}

/// What [`BuildVolumes::mint_first`] did beside main's image, owned by the adopt until it
/// settles or abandons it.
#[must_use = "settle or abandon releases a minted volume main's first touch did not link"]
pub(crate) enum FirstMint<H: RuntimeStorageHost = Host> {
    /// Nothing minted: nothing in the checkout can name build state, or a replayed adoption,
    /// whose first touch mints its own.
    Nothing,
    Minted(OwnedMint<H>),
    Failed(CowshedError),
}

/// A volume minted beside main's image that nothing has decided yet, in the slot main's first
/// touch holds locked while it runs ([`Lent`]): its release takes the same lock, so it waits for
/// a first touch that is running and then decides on the link that first touch left. Dropped
/// before [`FirstMint::settle`] or [`FirstMint::abandon`] emptied the slot (an adopt cancelled
/// in its staging, initializer, commit or conclusion), it is released unless the checkout links
/// it: on the blocking lane when a runtime is there to run it, at once otherwise, and at once
/// too when that runtime refuses or cancels the task ([`DroppedMint`]).
pub(crate) struct OwnedMint<H: RuntimeStorageHost = Host> {
    volumes: BuildVolumes<H>,
    checkout: PathBuf,
    slot: MintSlot,
}

/// A volume minted beside main's image, registered and not yet published, with the creation hold
/// that keeps every release in any process off it, by its exact id, until its release takes it.
pub(crate) struct Minted {
    volume: MintedVolume,
    _creating: BuildVolumeHold,
}

/// The volume an [`OwnedMint`] holds until its release takes it.
pub(crate) type MintSlot = Arc<Mutex<Option<Minted>>>;

/// The slot's volume, under its lock. The lock guards no invariant a panic can break: a
/// poisoned slot still names the volume or nothing.
fn lock(slot: &MintSlot) -> MutexGuard<'_, Option<Minted>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<H: RuntimeStorageHost> OwnedMint<H> {
    /// Release the slot's volume unless the checkout links it, once a first touch that holds the
    /// slot has finished. Blocking.
    fn release_now(&self, host: &H, layout: &BuildVolumeLayout, whose: &str) {
        release_slot(host, layout, &self.slot, &self.checkout, whose);
    }
}

fn release_slot(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    slot: &MintSlot,
    checkout: &Path,
    whose: &str,
) {
    // The creation hold goes first, so the release can claim the volume.
    if let Some(Minted { volume, _creating }) = lock(slot).take() {
        drop(_creating);
        release_minted(host, layout, &volume, checkout, whose);
    }
}

/// The release a dropped [`OwnedMint`] owes, run when this is dropped: by the blocking task it is
/// handed to, or by a runtime that refuses that task or shuts down before it starts, which drops
/// it unrun. Plain values, not an `OwnedMint`, so its drop never spawns again.
struct DroppedMint<H: RuntimeStorageHost> {
    volumes: BuildVolumes<H>,
    checkout: PathBuf,
    slot: MintSlot,
}

impl<H: RuntimeStorageHost> Drop for DroppedMint<H> {
    fn drop(&mut self) {
        release_slot(
            self.volumes.host.as_ref(),
            &self.volumes.layout,
            &self.slot,
            &self.checkout,
            CANCELLED,
        );
    }
}

impl<H: RuntimeStorageHost> Drop for OwnedMint<H> {
    fn drop(&mut self) {
        // Settled already: nothing to spawn. A first touch holding the slot leaves it to the
        // release, which waits for it on the blocking lane.
        if let Ok(slot) = self.slot.try_lock()
            && slot.is_none()
        {
            return;
        }
        let dropped = DroppedMint {
            volumes: self.volumes.clone(),
            checkout: self.checkout.clone(),
            slot: Arc::clone(&self.slot),
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(move || drop(dropped))),
            Err(_) => drop(dropped),
        }
    }
}

/// Whose volume [`release_minted`] says it released when an adopt was dropped before it decided.
const CANCELLED: &str = "a cancelled adoption's first";

/// What main's first touch is lent of a [`FirstMint`]: the minted volume's slot, which the first
/// touch holds locked while it runs, so no release of the volume overlaps it.
#[derive(Clone)]
pub(crate) enum Lent {
    Nothing,
    Volume(MintSlot),
    Failed(CowshedError),
}

impl<H: RuntimeStorageHost> FirstMint<H> {
    /// What main's first touch is lent; this keeps the volume until it settles.
    pub fn lend(&self) -> Lent {
        match self {
            Self::Nothing => Lent::Nothing,
            Self::Minted(owned) => Lent::Volume(Arc::clone(&owned.slot)),
            Self::Failed(error) => Lent::Failed(error.clone()),
        }
    }

    /// Release the minted volume unless main's checkout links it now: whatever main's first
    /// touch did not take, whether it refused, failed, found a volume already linked, minted its
    /// own at another capacity, found nothing to hold, or never ran. Kept when linked, even with
    /// no record yet: the link protects it and a retried first touch finds it. A link that cannot
    /// be read or a release that fails keeps it too, said on stderr, for collection once the
    /// adopt's intent is resolved.
    pub async fn settle(self) {
        self.release("main's unlinked first").await;
    }

    /// Release what was minted for an adoption that never committed; a mint that failed is said
    /// on stderr beside the adoption's own failure, which the caller answers.
    pub async fn abandon(self) {
        if let Self::Failed(error) = &self {
            cowshed_os::eprintln!(
                "cowshed: main's first build volume could not be minted beside its image either: {error}"
            );
        }
        self.release("an adoption that never committed its").await;
    }

    async fn release(self, whose: &'static str) {
        let Self::Minted(owned) = self else {
            return;
        };
        let volumes = owned.volumes.clone();
        let released = volumes
            .blocking(move |host, layout| {
                owned.release_now(host, layout, whose);
                Ok(())
            })
            .await;
        if let Err(error) = released {
            cowshed_os::eprintln!(
                "cowshed: {whose} build volume minted beside main stays: {error}; `cowshed gc` retries it"
            );
        }
    }
}

impl Lent {
    /// Run `touch` with what was minted, holding the slot locked throughout.
    fn hold<T>(&self, touch: impl FnOnce(MintedBeside) -> T) -> T {
        match self {
            Self::Nothing => touch(MintedBeside::Nothing),
            Self::Failed(error) => touch(MintedBeside::Failed(error.clone())),
            Self::Volume(slot) => {
                let held = lock(slot);
                touch(held.as_ref().map_or(MintedBeside::Nothing, |minted| {
                    MintedBeside::Volume(minted.volume.clone())
                }))
            }
        }
    }

    /// Main's first touch published `id`. When that is the minted volume, the adopt's hold on it
    /// ends here, as its creation's does once published (16_build_volumes.md, "Garbage
    /// collection"): the slot lets it go, creation hold and all, so the claim of main's seed
    /// capture is free to take it, and settling finds nothing left to decide. The checkout's
    /// link keeps it from then on.
    fn published(&self, id: &BuildVolumeId) {
        if let Self::Volume(slot) = self {
            let mut held = lock(slot);
            if held.as_ref().is_some_and(|minted| &minted.volume.id == id) {
                *held = None;
            }
        }
    }
}

pub(crate) struct BuildVolumes<H: RuntimeStorageHost = Host> {
    host: Arc<H>,
    pub layout: BuildVolumeLayout,
    /// Blocking work and a cancelled mint's destructor can outlive the project actor.
    retirement_lease: Arc<crate::project_retirement::ProjectReadLease>,
}

impl<H: RuntimeStorageHost> Clone for BuildVolumes<H> {
    fn clone(&self) -> Self {
        Self {
            host: Arc::clone(&self.host),
            layout: self.layout.clone(),
            retirement_lease: Arc::clone(&self.retirement_lease),
        }
    }
}

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    pub fn new(
        host: Arc<H>,
        layout: BuildVolumeLayout,
        retirement_lease: crate::project_retirement::ProjectReadLease,
    ) -> Self {
        Self {
            host,
            layout,
            retirement_lease: Arc::new(retirement_lease),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(&H, &BuildVolumeLayout) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let volumes = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let _retirement_lease = volumes.retirement_lease;
            work(&volumes.host, &volumes.layout)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("build volume task failed: {error}")))?
    }

    /// `target`'s image lock at `lock`, for its land's steps that delete what a fork or reseed
    /// of `target` reads ([`TargetLock`]); [`Contended`] when another operation on `target` holds
    /// it now, for the caller to say so before it waits. `holding` is the request it is held for.
    pub async fn try_lock_target(
        &self,
        target: Owner,
        lock: PathBuf,
        holding: LockHolding,
    ) -> Result<std::result::Result<TargetLock<H>, Contended<H>>> {
        let path = lock.clone();
        let asked = holding.clone();
        let guard = self
            .blocking(move |host, _| {
                host.lock_images(&[path], LockMode::Try, &asked)
                    .map_err(storage)
            })
            .await?;
        Ok(match guard {
            Some(guard) => Ok(TargetLock {
                target,
                _guard: guard,
            }),
            None => Err(Contended {
                volumes: self.clone(),
                target,
                lock,
                holding,
            }),
        })
    }

    /// The published volume `checkout` links and the state written at its root, or `None` when
    /// it links none or an interrupted first touch left its volume registered and unpublished
    /// (which only a fresh discovery finishes). The checkout is mounted, which mounted its volume.
    pub fn state_of(&self, checkout: &Path) -> Result<Option<(BuildVolumeId, BuildVolumeState)>> {
        let Some(id) = self.layout.linked(checkout)? else {
            return Ok(None);
        };
        if !self.layout.read_record(&id)?.is_published() {
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
    /// touch's) gets its first one at `capacity`, or the one `lent` names when it links none
    /// ([`MintedBeside`]), unless it links none and nothing was discovered; `owner` then also gets
    /// its seed, a clone of that volume, when it has none, so it is a target others fork from
    /// (16_build_volumes.md, "Targets and seeds"). A path that holds tracked source refuses
    /// before anything is deleted. A volume `lent` names that this does not link stays its
    /// [`FirstMint`]'s to release; its slot is held locked for the first touch, so no release
    /// overlaps it.
    pub async fn refresh(
        &self,
        owner: Owner,
        checkout: PathBuf,
        discovered: Discovered,
        lent: Lent,
    ) -> Result<std::result::Result<BuildStateRefresh, TrackedBuildStateRefusal>> {
        let linked = self.layout.linked(&checkout)?;
        self.blocking(move |host, layout| {
            // A linked volume always has a record: one registered and unpublished is a first
            // touch to finish; one with none at all is bytes nobody registered, which refuses.
            let published = match &linked {
                Some(id) => layout.read_record(id)?.is_published(),
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
                    let touched = lent.hold(|beside| {
                        crate::build_volume::migrate::first_touch(
                            host,
                            layout,
                            crate::build_volume::migrate::FirstTouch {
                                checkout: &checkout,
                                paths: &paths,
                                fingerprint,
                                capacity,
                                beside,
                                registration: Registration::new(
                                    BuildVolumeRecord::new(
                                        None,
                                        BuildVolumeRole::Linked {
                                            checkout: owner.name.clone(),
                                        },
                                    ),
                                    Registrant::Workspace {
                                        incarnation: owner.incarnation.clone(),
                                    },
                                ),
                            },
                        )
                    })?;
                    let (id, displaced) = match touched {
                        Ok(touched) => touched,
                        Err(refusal) => return Ok(Err(refusal)),
                    };
                    lent.published(&id);
                    if layout.seed_of(&owner.name, &owner.incarnation)?.is_none() {
                        host.clone_build_volume(
                            layout,
                            &id,
                            &BuildVolumeId::mint(),
                            &Registration::new(
                                BuildVolumeRecord::new(
                                    None,
                                    BuildVolumeRole::Seed {
                                        target: owner.name,
                                        incarnation: owner.incarnation.clone(),
                                    },
                                ),
                                Registrant::Workspace {
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

    /// Main's first build volume, minted and mounted beside main's own image while `adopt`
    /// copies `source` into it, so the two attaches wait on `storagekitd` once
    /// (16_build_volumes.md, "Targets and seeds"): at the capacity `source`'s `.cowshed.toml`
    /// asks, and none when nothing in `source` can name build state, whose first touch makes no
    /// volume. Its record is registered to main, by the adopt's intent, before its image exists,
    /// and nothing links it until main's first touch at `checkout` links and publishes it; until
    /// then the adopt's unfinished intent keeps collection off it ([`Links::creating`]). A
    /// failed mint releases what it registered and is answered, for the first touch to answer
    /// in turn if it needs a volume.
    ///
    /// The volume is the answer's to release: dropped unsettled, it releases itself. An adopt
    /// dropped while the mint runs leaves nobody to answer, so the mint releases what it made.
    pub async fn mint_first(&self, source: PathBuf, checkout: PathBuf) -> FirstMint<H> {
        let (answer, answered) = tokio::sync::oneshot::channel();
        let volumes = self.clone();
        let minting = self.blocking(move |host, layout| {
            let minted = may_mint(&source).and_then(|capacity| {
                capacity
                    .map(|capacity| {
                        let id = BuildVolumeId::mint();
                        // Main has no incarnation until the adopt publishes it, so the volume is
                        // registered by the adopt's intent and held by its exact id, by the slot,
                        // until its release takes it: never by main's name.
                        let (_, creating) = host
                            .create_build_volume(
                                layout,
                                &id,
                                capacity,
                                &Registration::new(
                                    BuildVolumeRecord::new(
                                        None,
                                        BuildVolumeRole::Linked {
                                            checkout: WorkspaceName::main(),
                                        },
                                    ),
                                    Registrant::Intent,
                                ),
                            )
                            .map_err(storage)?;
                        Ok(Minted {
                            volume: MintedVolume { id, capacity },
                            _creating: creating,
                        })
                    })
                    .transpose()
            });
            let first = match minted {
                Ok(None) => FirstMint::Nothing,
                Ok(Some(volume)) => FirstMint::Minted(OwnedMint {
                    volumes,
                    checkout,
                    slot: Arc::new(Mutex::new(Some(volume))),
                }),
                Err(error) => FirstMint::Failed(error),
            };
            if let Err(FirstMint::Minted(unanswered)) = answer.send(first) {
                unanswered.release_now(host, layout, CANCELLED);
            }
            Ok(())
        });
        if let Err(error) = minting.await {
            return FirstMint::Failed(error);
        }
        answered.await.unwrap_or_else(|_| {
            FirstMint::Failed(CowshedError::internal(
                "main's first build volume mint ended without an answer",
            ))
        })
    }

    /// Fork steps 2 and 3 (16_build_volumes.md, "Fork") as far as they read `source`'s seed,
    /// which is as far as `source`'s image lock covers them: the seed is first refreshed for
    /// the tree `aim` names ([`Self::refresh_seed`]; what it did instead is said on stderr, and
    /// the older seed is forked), then captured into `destination`'s live volume, registered
    /// but not yet proven, published or mounted. A source with no seed gives none. The
    /// caller holds `source`'s image lock and runs this beside the capture of `source`'s image;
    /// [`Self::ready_fork`] does the rest once it lets that lock go. Beside the capture, the
    /// seeds the refresh replaced, claimed: the caller retires them ([`Self::retire_superseded`])
    /// once its create has let every lock go, whether or not the fork succeeded.
    pub async fn capture_fork(
        &self,
        source: Owner,
        source_checkout: PathBuf,
        destination: WorkspaceName,
        aim: Aim,
    ) -> (Result<Option<CapturedFork>>, Option<SupersededSeeds>) {
        // Where the new workspace's live volume is placed: the tree its checkout starts at.
        let placed = aim.tree.clone();
        let mut refreshed = match self
            .refresh_seed(
                source.clone(),
                source_checkout,
                aim,
                LockHolding::of(LockOperation::Fork, &destination),
            )
            .await
        {
            Ok(refreshed) => refreshed,
            Err(error) => return (Err(error), None),
        };
        say_refresh(&source.name, &destination, &refreshed);
        let superseded = refreshed.superseded.take();
        let captured = self
            .blocking(move |host, layout| {
                let Some((seed, record)) = layout.seed_of(&source.name, &source.incarnation)?
                else {
                    return Ok(None);
                };
                let started = Instant::now();
                let captured = host
                    .capture_fork_build_volume(
                        layout,
                        &seed,
                        // The fork's intent answers for the volume until it publishes `destination`.
                        &Registration::new(
                            BuildVolumeRecord {
                                checked_out: Some(placed),
                                ..BuildVolumeRecord::new(
                                    record.tree.clone(),
                                    BuildVolumeRole::Linked {
                                        checkout: destination,
                                    },
                                )
                            },
                            Registrant::Intent,
                        ),
                    )
                    .map_err(storage)?;
                Ok(Some(CapturedFork {
                    seed,
                    tree: record.tree,
                    captured,
                    started,
                }))
            })
            .await;
        (captured, superseded)
    }

    /// The captured live volume proven and published, `destination`'s own seed cloned from it
    /// before anything mounts it, so the seed holds exactly what the source's seed held and
    /// `destination` is a target others can fork from, and the live volume mounted with no daemon
    /// record. It runs beside the attach and mount of the fork's own image: none of it reads the
    /// source, so it runs after the source's image lock is let go. The seed comes before the
    /// link: a crash between them leaves an unlinked volume, never a target without a seed. The
    /// staged checkout's [`Self::finish_fork`] then links the live volume.
    pub async fn ready_fork(
        &self,
        captured: Result<Option<CapturedFork>>,
        destination: Owner,
    ) -> Result<Option<PreparedFork>> {
        let Some(CapturedFork {
            seed,
            tree,
            captured,
            started,
        }) = captured?
        else {
            return Ok(None);
        };
        self.blocking(move |host, layout| {
            let own_seed = BuildVolumeId::mint();
            let registration = Registration::new(
                BuildVolumeRecord::new(
                    tree,
                    BuildVolumeRole::Seed {
                        target: destination.name,
                        incarnation: destination.incarnation,
                    },
                ),
                Registrant::Intent,
            );
            let (live, mount) = host
                .ready_fork_build_volume(layout, captured, Some((&own_seed, &registration)))
                .map_err(storage)?;
            Ok(Some(PreparedFork {
                seed,
                own_seed,
                live,
                mount,
                started,
            }))
        })
        .await
    }

    /// Release what [`Self::capture_fork`] answered for a fork whose own image was never
    /// captured. The failed capture is the caller's error, so a release that fails too is said
    /// on stderr and left to `gc`.
    pub async fn abandon_captured_fork(&self, captured: Result<Option<CapturedFork>>) {
        let Ok(Some(CapturedFork { captured, .. })) = captured else {
            return;
        };
        let live = captured.id.clone();
        let released = self
            .blocking(move |host, layout| {
                host.abandon_captured_build_volume(layout, captured)
                    .map_err(storage)
            })
            .await;
        match released {
            Ok(release) => say_release(&live, "a fork that never staged", &release),
            Err(error) => cowshed_os::eprintln!(
                "cowshed: build volume {live} of a fork that never staged stays: {error}; `cowshed gc` retries it"
            ),
        }
    }

    /// The rest of the fork, once the checkout `destination` is about to become is staged at
    /// `checkout`: the checkout links the live volume whose own seed [`Self::ready_fork`] made.
    /// Without a prepared volume the checkout must link nothing, or something is wrong, which
    /// refuses.
    pub async fn finish_fork(
        &self,
        prepared: Option<PreparedFork>,
        destination: Owner,
        checkout: PathBuf,
    ) -> Result<Option<BuildVolumeId>> {
        self.blocking(move |host, layout| {
            let Some(PreparedFork {
                seed,
                own_seed,
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
            link::point(&checkout, &mount)?;
            // The own seed holds everything the live volume does but its mount's writes and the
            // dropped daemon record, so it starts as fresh as the live volume.
            let written = host.build_volume_written(layout, &live).map_err(storage)?;
            host.mark_seed_written(layout, &own_seed, written)
                .map_err(storage)?;
            cowshed_os::timing::event("build-volume", || {
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

    /// Release what [`Self::ready_fork`] answered for a checkout that never staged: no checkout
    /// links its live volume and nothing has run in it. The failed clone is the caller's error,
    /// so a release that fails too is said on stderr and left to `gc`.
    pub async fn abandon_fork(&self, prepared: Result<Option<PreparedFork>>) {
        let Ok(Some(prepared)) = prepared else {
            return;
        };
        let live = prepared.live.clone();
        let own_seed = prepared.own_seed.clone();
        for (id, whose) in [
            (live, "a fork that never staged"),
            (own_seed, "the seed of a fork that never staged"),
        ] {
            let releasing = id.clone();
            let released = self
                .blocking(move |host, layout| {
                    host.release_build_volume(layout, &releasing)
                        .map_err(storage)
                })
                .await;
            match released {
                Ok(release) => say_release(&id, whose, &release),
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: build volume {id} of {whose} stays: {error}; `cowshed gc` retries it"
                ),
            }
        }
    }

    /// Hold the volume `checkout` links for its land (Land step 4), or `None` when it links
    /// none. Taken before the landing workspace's jobs stop, so the volume is never without a
    /// hold between their release of it and the target's link naming it.
    pub fn hold_landing(&self, checkout: &Path) -> Result<Option<Landing>> {
        let Some(id) = self.layout.linked(checkout)? else {
            return Ok(None);
        };
        match self.layout.hold(&id) {
            Ok(hold) => Ok(Some(Landing { id, hold })),
            Err(error) if error.kind() == std::io::ErrorKind::ResourceBusy => {
                Err(CowshedError::conflict(
                    format!("cannot hold the landing build volume for the land: {error}"),
                    "another cowshed process released it as unlinked; `cowshed doctor --json` \
                     reports the landing workspace's build link",
                ))
            }
            Err(error) => Err(io(
                "hold the landing build volume",
                &self.layout.hold_path(&id),
                &error,
            )),
        }
    }

    /// Land step 4, after the landing workspace's supervisor has stopped its jobs: the landing
    /// volume's Nx daemon is stopped and its task database must have no holder left. The quiet
    /// volume then grows to the target's capacity when it is smaller, before the seed is frozen
    /// from it, so an adoption never shrinks the target and the seed inherits the larger cap: a
    /// workspace forked before a resize of its target lands at the target's capacity. The
    /// landing hold and stopped socket lease go with the quiet volume.
    pub async fn quiesce(
        &self,
        landing: Landing,
        stopped: super::supervisor_socket::BoundSocket,
        target_checkout: PathBuf,
    ) -> Result<std::result::Result<Quiet, AdoptionSkip>> {
        let target = self.layout.linked(&target_checkout)?;
        self.blocking(move |host, layout| {
            let Landing { id, hold } = landing;
            let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
            let state = BuildVolumeState::read(&mount)?;
            // Read-only: the landing's daemon was stopped by the land's ledger-proven plan before this
            // point; quiesce never rereads a record to signal anything.
            if let Err(busy) = nx::held(&mount, &state)
                .map_err(|error| io("look at the landing volume's Nx state", &mount, &error))?
            {
                return Ok(Err(skip(busy, Side::Landing)));
            }
            if H::HAS_PHYSICAL_VOLUMES
                && let Some(target) = target
            {
                let capacity = host
                    .build_volume_capacity(layout, &target)
                    .map_err(storage)?;
                match host.resize_build_volume(layout, &id, capacity) {
                    Ok(_) | Err(ApfsStorageError::CapacityNotGrowing { .. }) => {}
                    Err(ApfsStorageError::Apfs(error))
                        if cowshed_apfs::detach_was_dissented(&error) =>
                    {
                        return Ok(Err(AdoptionSkip::LandingVolumeBusy {
                            reason: error.to_string(),
                        }));
                    }
                    Err(error) => return Err(storage(error)),
                }
            }
            Ok(Ok(Quiet {
                id,
                mount,
                state,
                hold: Arc::new(hold),
                _stopped: Arc::new(stopped),
            }))
        })
        .await
    }

    /// Land step 5.4, under the target's image lock: close Nx, retain its open locks and the
    /// existing Cargo profile locks through mounted capture and publication, then claim every
    /// older seed for [`Self::retire_superseded`] once the lock is let go. Other contributed cache/index handles do not refuse capture. Quiet retains
    /// the stopped supervisor's admission lease and shared GC hold throughout this operation.
    pub async fn freeze_seed(
        &self,
        locked: &TargetLock<H>,
        quiet: &Quiet,
        tree: GitOid,
    ) -> Result<std::result::Result<Freeze, AdoptionSkip>> {
        let quiet = quiet.clone();
        let target = locked.target.clone();
        self.blocking(move |host, layout| {
            let registration = Registration::new(
                BuildVolumeRecord::new(
                    Some(tree),
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
                Registrant::Workspace {
                    incarnation: target.incarnation.clone(),
                },
            );
            seal(host, layout, &quiet, &target, &registration)
        })
        .await
    }

    /// `target`'s seed against the live volume `checkout` links, both measured against `tree`,
    /// or `None` when it links none, or one an interrupted first touch left unpublished.
    pub async fn seed_age(
        &self,
        target: Owner,
        checkout: PathBuf,
        tree: GitOid,
    ) -> Result<Option<SeedAge>> {
        let live = self.layout.linked(&checkout)?;
        self.blocking(move |host, layout| match live {
            Some(live) => seed_age(host, layout, &target, live, &checkout, &tree),
            None => Ok(None),
        })
        .await
    }

    /// Fork step 2 (16_build_volumes.md, "Which volume a seed holds"): when neither `target`'s
    /// seed nor the live volume `checkout` links was built at `aim`'s tree, capture the volume of
    /// a live workspace that has it checked out ([`seed_from_landed`]); otherwise, or
    /// when none is captured, reseed from the live volume when that is nearer the tree, or as near
    /// and written since, and has no writer. The caller holds `target`'s image lock, which every
    /// fork of `target` holds while it clones the seed. A landed peer's image lock is taken for
    /// `holding`, the request the refresh serves.
    pub async fn refresh_seed(
        &self,
        target: Owner,
        checkout: PathBuf,
        aim: Aim,
        holding: LockHolding,
    ) -> Result<Refreshed> {
        let this = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let Some(age) = this.age_now(&target, &checkout, &aim.tree)? else {
                return Ok(Refreshed {
                    passed: Vec::new(),
                    outcome: Refresh::NoBuildVolume,
                    superseded: None,
                });
            };
            let mut passed = Vec::new();
            // Step 1 is for main alone. Plain git outside cowshed fast-forwards main to a tree a
            // workspace built; a session's checkout moves only by its own work, so its own volume
            // is what built its tree, and another workspace at that tree (a fork of it, say) holds
            // less. That is the reseed's case, whatever the session's tree.
            if target.name.is_main()
                && age.moved_from().is_some()
                && let Some((landed, superseded)) = seed_from_landed(
                    &*this.host,
                    &this.layout,
                    &target,
                    &aim,
                    &holding,
                    &mut passed,
                )?
            {
                return Ok(Refreshed {
                    passed,
                    outcome: landed,
                    superseded: Some(superseded),
                });
            }
            let (outcome, superseded) = reseed(&*this.host, &this.layout, &target, age)?;
            Ok(Refreshed {
                passed,
                outcome,
                superseded,
            })
        })
        .await
        .map_err(|error| CowshedError::internal(format!("build volume task failed: {error}")))?
    }

    fn age_now(&self, target: &Owner, checkout: &Path, tree: &GitOid) -> Result<Option<SeedAge>> {
        match self.layout.linked(checkout)? {
            Some(live) => seed_age(&*self.host, &self.layout, target, live, checkout, tree),
            None => Ok(None),
        }
    }

    /// Land step 5.1–5.3: when nothing but the target's daemon holds its task database,
    /// stop that daemon. Answers the target's volume, closed, for [`Self::adopt`].
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

    /// Land step 6, under the target's image lock: when nothing opened the closed target's task
    /// database since [`Self::close_target`], give the landing volume the target's daemon records
    /// in place of its own and rename the target's build link onto it. The target's previous
    /// volume is recorded superseded under the target at its incarnation and answered, for
    /// [`Self::release_previous`] beside what the land does in the target next. Answers how long
    /// the move took.
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
        locked: &TargetLock<H>,
        quiet: &Quiet,
        previous: Closed,
        target_checkout: PathBuf,
        tree: GitOid,
    ) -> Result<std::result::Result<Adopted, AdoptionSkip>> {
        let owner = locked.target.clone();
        let target = owner.name.clone();
        let linked = self.layout.linked(&target_checkout)?;
        let quiet = quiet.clone();
        self.blocking(move |_, layout| {
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
            nx::hand_over_daemon_records(&previous.mount, &previous.state, &quiet.mount).map_err(
                |error| io("hand over the target's daemon record", &quiet.mount, &error),
            )?;
            link::point(&target_checkout, &quiet.mount)?;
            // Every Nx process waiting to open now resolves the link to the adopted volume. The
            // locks are files in the previous volume, so they go before it is released.
            drop(opens);
            let elapsed = started.elapsed();
            layout.write_record(
                &quiet.id,
                &BuildVolumeRecord {
                    checked_out: Some(tree.clone()),
                    tree: Some(tree),
                    role: BuildVolumeRole::Linked {
                        checkout: target.clone(),
                    },
                    ..layout.read_record(&quiet.id)?
                },
            )?;
            let previous = previous.id;
            // Still the target's, at its incarnation, until a release deletes it: a job admitted
            // on it before the move holds it, and its end releases it.
            layout.write_record(
                &previous,
                &BuildVolumeRecord {
                    role: BuildVolumeRole::Superseded {
                        owner: owner.name.clone(),
                        incarnation: owner.incarnation.clone(),
                    },
                    ..layout.read_record(&previous)?
                },
            )?;
            Ok(Ok(Adopted {
                elapsed_ms: millis(elapsed),
                previous: Superseded {
                    id: previous,
                    owner,
                },
            }))
        })
        .await
    }

    /// Retire `superseded`, the seeds a new one replaced, once the target's image lock that
    /// claimed them is let go: each is committed to `build/.deleting` and its record removed, the
    /// gateway freeing the bytes. The land or fork it serves has its seed, so a failure is said
    /// on stderr and the seeds stay recorded under the target, for the next seed's retirement or
    /// `gc`.
    pub async fn retire_superseded(&self, superseded: SupersededSeeds) {
        if superseded.claims.is_empty() {
            return;
        }
        let target = superseded.target.clone();
        let started = Instant::now();
        let retired = self
            .blocking(move |host, layout| retire_seeds(host, layout, superseded))
            .await;
        cowshed_os::timing::event("build-volume", || {
            format!(
                "retire {target}'s superseded seeds in {:?}",
                started.elapsed()
            )
        });
        if let Err(error) = retired {
            cowshed_os::eprintln!(
                "cowshed: {target}'s superseded seeds stay recorded as its: {error}; the next seed's retirement or `cowshed gc` retires them"
            );
        }
    }

    /// Release a target's previous volume after its adoption, inside the land: nothing links it,
    /// so only a job admitted on it before the move keeps it. Held, it stays recorded superseded
    /// under the target, and that job's end releases it
    /// ([`Self::release_superseded`]); a failed release keeps that record too and is the land's
    /// error. It needs no target lock: no fork or reseed of the target that took the lock after
    /// the move reads it, and none that held it before the land took it still runs. The
    /// release commits the image to `build/.deleting` and returns; the gateway frees its bytes.
    pub async fn release_previous(&self, previous: Superseded) -> Result<()> {
        self.blocking(move |host, layout| {
            let Superseded { id, owner } = previous;
            let release = host.release_build_volume(layout, &id).map_err(storage)?;
            say_release(&id, &format!("{}'s previous", owner.name), &release);
            Ok(())
        })
        .await
    }

    /// The release a job's end owes the volume it was admitted on (16_build_volumes.md,
    /// "Garbage collection"): when that volume is recorded superseded, its owner's link having
    /// moved off it while the job ran, it is released now unless another job still holds it,
    /// whose end then releases it in turn. A volume recorded anything else is not this release's
    /// to decide, and is left as it is. A refusal or failure leaves the superseded record naming
    /// its owner, for the owner's retirement or the next collection.
    pub async fn release_superseded(&self, id: BuildVolumeId) -> Result<()> {
        self.blocking(move |host, layout| {
            let Some(BuildVolumeRecord {
                role: BuildVolumeRole::Superseded { owner, .. },
                ..
            }) = layout.read_record_present(&id)?
            else {
                return Ok(());
            };
            let release = host.release_build_volume(layout, &id).map_err(storage)?;
            if let BuildVolumeRelease::Deleted { .. } = release {
                say_release(&id, &format!("{owner}'s superseded"), &release);
            }
            Ok(())
        })
        .await
    }

    /// Retirement's own release (16_build_volumes.md, "Garbage collection"), before `owner`'s
    /// image is retired: the live volume `checkout` links, when its record still names `owner`
    /// as its checkout (a target that adopted it renamed that, and keeps it), then every other
    /// volume recorded as `owner`'s at its incarnation: its seeds and its superseded previous
    /// volumes, and whatever a creation it registered left unfinished. A volume that stays
    /// refuses, naming why, so an image is never retired ahead of the volumes it owned and
    /// nothing is left for a later sweep to find ownerless. Each release commits its image to
    /// `build/.deleting` and returns; the gateway frees the bytes.
    pub async fn release_owned(&self, owner: Owner, checkout: PathBuf) -> Result<()> {
        let linked = self.layout.linked(&checkout)?;
        self.blocking(move |host, layout| {
            let mut owned = Vec::new();
            if let Some(id) = linked
                && let Some(BuildVolumeRecord {
                    role: BuildVolumeRole::Linked { checkout: by },
                    ..
                }) = layout.read_record_present(&id)?
                && by == owner.name
            {
                owned.push((id, "live"));
            }
            for id in layout.collection_ids()? {
                if owned.iter().any(|(live, _)| *live == id) {
                    continue;
                }
                let Some(record) = layout.read_record_present(&id)? else {
                    continue;
                };
                if let Some(whose) = owned_at(&record, &owner) {
                    owned.push((id, whose));
                }
            }
            // The live volume goes last, so a refusal of any other leaves the checkout's link
            // naming a volume that is still there.
            owned.sort_by_key(|(_, whose)| *whose == "live");
            for (id, whose) in owned {
                match host.release_build_volume(layout, &id).map_err(storage)? {
                    release @ BuildVolumeRelease::Deleted { .. } => {
                        say_release(&id, &format!("{}'s {whose}", owner.name), &release);
                    }
                    // A seed is never mounted and no job is admitted on one: its claim is a
                    // superseded seed's retirement by an earlier land or fork, which frees it.
                    BuildVolumeRelease::Held { open } if whose == "seed" && open.is_empty() => {}
                    BuildVolumeRelease::Held { open } => {
                        return Err(CowshedError::conflict(
                            format!(
                                "{}'s {whose} build volume {id} stays while a job admitted on it runs{}; {} is not retired",
                                owner.name,
                                if open.is_empty() {
                                    String::new()
                                } else {
                                    format!(" (open in {})", nx::Holders(&open))
                                },
                                owner.name,
                            ),
                            "end that job, then run the same removal again",
                        ));
                    }
                }
            }
            Ok(())
        })
        .await
    }

    /// A workspace whose volume a target adopted and that keeps working (`land --no-retire`)
    /// takes a fresh clone of that target's latest seed (the one the land froze, or the previous
    /// one a busy seal kept), so it never writes the target's volume. Only the capture runs here,
    /// under the target's image lock, so no reseed of the target deletes the seed it clones;
    /// [`Self::ready_refork`] publishes, mounts and links it once the land lets that lock go.
    pub async fn refork(
        &self,
        locked: &TargetLock<H>,
        workspace: Owner,
        checkout: PathBuf,
    ) -> Result<Refork> {
        let seed_of = locked.target.clone();
        self.blocking(move |host, layout| {
            let (seed, record) = layout
                .seed_of(&seed_of.name, &seed_of.incarnation)?
                .ok_or_else(|| {
                    CowshedError::internal(format!(
                        "{} has no seed for a kept workspace to refork from",
                        seed_of.name
                    ))
                })?;
            let captured = host
                .capture_fork_build_volume(
                    layout,
                    &seed,
                    &Registration::new(
                        BuildVolumeRecord::new(
                            record.tree,
                            BuildVolumeRole::Linked {
                                checkout: workspace.name,
                            },
                        ),
                        Registrant::Workspace {
                            incarnation: workspace.incarnation,
                        },
                    ),
                )
                .map_err(storage)?;
            Ok(Refork { captured, checkout })
        })
        .await
    }

    /// The kept workspace's captured volume proven, published and mounted, and its
    /// checkout linked to it: nothing here reads the target, so it runs after the land lets the
    /// target's image lock go.
    pub async fn ready_refork(&self, refork: Refork) -> Result<()> {
        let Refork { captured, checkout } = refork;
        self.blocking(move |host, layout| {
            let (live, mount) = host
                .ready_fork_build_volume(layout, captured, None)
                .map_err(storage)?;
            if let Err(error) = link::point(&checkout, &mount) {
                match host.release_build_volume(layout, &live) {
                    Ok(release) => say_release(&live, "a refork that never linked", &release),
                    Err(cleanup) => cowshed_os::eprintln!(
                        "cowshed: build volume {live} of a refork that never linked stays: {cleanup}; `cowshed gc` retries it"
                    ),
                }
                return Err(error);
            }
            Ok(())
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

    /// Rewrite every seed of this incarnation and its linked build source. Keeping this with
    /// workspace defrag makes every extent refusal's named remedy reach the actual culprit.
    pub async fn defragment(
        &self,
        owner: Owner,
    ) -> Result<Vec<(PathBuf, crate::storage::lifecycle::DefragmentOutcome)>> {
        self.blocking(move |host, layout| {
            let images = defragment_images(layout, &owner)?;
            let mut outcomes = Vec::with_capacity(images.len());
            for (id, linked) in images {
                let claim = match host.claim_build_volume(layout, &id).map_err(storage)? {
                    BuildVolumeClaim::Claimed(claim) => claim,
                    BuildVolumeClaim::Held { open } => {
                        return Err(CowshedError::conflict(
                            format!(
                                "cannot defragment build image {} of {}: {}",
                                layout.image(&id).display(),
                                owner.name,
                                volume_busy_reason(&layout.image(&id), &open)
                            ),
                            format!("stop its admitted jobs, then cowshed defrag {}", owner.name),
                        ));
                    }
                };
                if defragment_role(&layout.read_record(&id)?, &owner) != Some(linked) {
                    return Err(CowshedError::conflict(
                        format!(
                            "build image {} changed owner or publication state before defrag",
                            layout.image(&id).display()
                        ),
                        format!("cowshed doctor, then cowshed defrag {}", owner.name),
                    ));
                }
                if linked {
                    let mount = host.mount_build_volume(layout, &id).map_err(storage)?;
                    let state = BuildVolumeState::read(&mount)?;
                    if let Err(busy) = nx::close(&mount, &state)
                        .map_err(|error| io("close the build volume's Nx state", &mount, &error))?
                    {
                        return Err(CowshedError::conflict(
                            format!(
                                "cannot defragment build image {} of {}: {busy}",
                                layout.image(&id).display(),
                                owner.name
                            ),
                            format!("stop what holds it, then cowshed defrag {}", owner.name),
                        ));
                    }
                }
                let outcome = host
                    .defragment_build_volume(layout, &claim)
                    .map_err(storage)?;
                outcomes.push((layout.image(&id), outcome));
            }
            Ok(outcomes)
        })
        .await
    }

    /// Delete (16_build_volumes.md, "Garbage collection") what [`plan`] dooms of `images`, listed
    /// before `links` was read. Each doomed volume is claimed first and decided again under the
    /// claim: one whose record now names a checkout that links it ([`relinked`]) is kept, since a
    /// land moved that checkout onto it after `links` was read. A volume a job or a land still
    /// holds, or whose release fails, is deferred to the next pass with the reason, beside what
    /// the plan itself deferred; one release that fails never keeps the others. Every process a
    /// forced unmount cut off is named on stderr.
    pub async fn collect(
        &self,
        images: Vec<BuildVolumeId>,
        links: Links,
        dry_run: bool,
    ) -> Result<Collection> {
        self.blocking(move |host, layout| collect_planned(host, layout, &images, &links, dry_run))
            .await
    }

    /// Every build volume registered and not yet published, by its exact id: what a crash
    /// between registration and publication can leave. Read from the records alone, so an open
    /// with nothing registered pays one record read per volume and no inventory.
    pub async fn registered(&self) -> Result<Vec<BuildVolumeId>> {
        self.blocking(move |_, layout| {
            let mut registered = Vec::new();
            for id in layout.collection_ids()? {
                if layout
                    .read_record_present(&id)?
                    .is_some_and(|record| !record.is_published())
                {
                    registered.push(id);
                }
            }
            Ok(registered)
        })
        .await
    }

    /// Next-open recovery of interrupted creations (16_build_volumes.md, "Garbage collection"):
    /// each of `registered`, listed before `links` was read, whose registrant is gone is released
    /// by its exact id now, as the open's own step, never left for a periodic or manual pass. One
    /// still forming (a create, fork or adopt in any process, a first touch a detached checkout
    /// may still finish) is its registrant's, and one a creator still holds defers under the
    /// release's claim. Nothing here frees image bytes: the gateway's cleanup
    /// frees what releases commit (05_gateway.md, "Deleting released images").
    pub async fn recover_registered(
        &self,
        registered: Vec<BuildVolumeId>,
        links: Links,
    ) -> Result<Collection> {
        self.blocking(move |host, layout| collect_planned(host, layout, &registered, &links, false))
            .await
    }
}

/// [`plan`] `images` against `links` and release what it dooms, each under its own claim and
/// decided again there; or, for a dry run, say what each release would answer.
fn collect_planned(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    images: &[BuildVolumeId],
    links: &Links,
    dry_run: bool,
) -> Result<Collection> {
    let plan = plan(layout, images, links)?;
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
    for doomed in plan.doomed {
        let (id, reason, bytes) = (doomed.id.clone(), doomed.reason, doomed.bytes);
        let image = layout.image(&id);
        let deferral = if dry_run {
            // What the release would answer, asked without claiming.
            match (still_doomed(layout, links, &doomed), layout.held(&id)) {
                (Ok(Some(deferral)), _) => Some(deferral),
                (Err(error), _) => Some(Deferral::ReleaseFailed(format!(
                    "cannot tell whether it is still garbage: {error}"
                ))),
                (Ok(None), Ok(false)) => None,
                (Ok(None), Ok(true)) => Some(Deferral::Held(Vec::new())),
                (Ok(None), Err(error)) => Some(Deferral::ReleaseFailed(format!(
                    "cannot tell whether a job holds it: {error}"
                ))),
            }
        } else {
            match release_if_still_doomed(host, layout, links, &doomed) {
                Ok(Ok(refused)) => {
                    say_release(&id, "collected", &BuildVolumeRelease::Deleted { refused });
                    collection.reclaimed += 1;
                    None
                }
                Ok(Err(deferral)) => Some(deferral),
                Err(error) => Some(Deferral::ReleaseFailed(error.to_string())),
            }
        };
        let candidate = GcCandidate {
            identity: Sha256Digest::compute(image.as_os_str().as_encoded_bytes()),
            path: image.clone(),
            bytes,
            reason,
        };
        match deferral {
            None => {
                collection.candidates.push(candidate);
                collection.released_bytes = collection.released_bytes.saturating_add(bytes);
            }
            Some(deferral) => {
                // A collection names what it tried to release; a volume a checkout links is
                // not garbage, and a dry run tries nothing.
                if !dry_run && !matches!(deferral, Deferral::Linked(_)) {
                    collection.candidates.push(candidate);
                }
                collection.deferred.push(Deferred {
                    path: image,
                    deferral,
                });
            }
        }
    }
    Ok(collection)
}

/// Why `doomed` is no longer garbage, read now: its record is not the one the plan judged it by
/// (a creation published it, a land handed the volume to its target), or a checkout's build
/// link names it. A live link proves reachability by itself, whatever any record says.
fn still_doomed(
    layout: &BuildVolumeLayout,
    links: &Links,
    doomed: &Doomed,
) -> Result<Option<Deferral>> {
    if layout.read_record_present(&doomed.id)?.as_ref() != Some(&doomed.seen) {
        return Ok(Some(Deferral::Changed));
    }
    if doomed.image_missing {
        match std::fs::symlink_metadata(layout.image(&doomed.id)) {
            Ok(_) => return Ok(Some(Deferral::Changed)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io(
                    "inspect a build volume's absent image",
                    &layout.image(&doomed.id),
                    &error,
                ));
            }
        }
    }
    for (checkout, path) in &links.checkouts {
        if layout.linked(path)?.as_ref() == Some(&doomed.id) {
            return Ok(Some(Deferral::Linked(checkout.clone())));
        }
    }
    Ok(None)
}

/// Release `doomed` unless, under the release's claim, something holds it or [`still_doomed`]
/// says it is no longer garbage. Everything that makes a volume someone's (a job, a land moving
/// a link onto it, a clone writing its record) holds it while it does, so under the claim that
/// work is done or has not begun. Answers the unforced unmount's refusal of a release, or why
/// the volume stays.
fn release_if_still_doomed(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    links: &Links,
    doomed: &Doomed,
) -> Result<std::result::Result<Option<BuildVolumeRefusal>, Deferral>> {
    let claim = match host
        .claim_build_volume(layout, &doomed.id)
        .map_err(storage)?
    {
        BuildVolumeClaim::Claimed(claim) => claim,
        BuildVolumeClaim::Held { open } => return Ok(Err(Deferral::Held(open))),
    };
    if let Some(deferral) = still_doomed(layout, links, doomed)? {
        return Ok(Err(deferral));
    }
    host.release_claimed(layout, claim).map(Ok).map_err(storage)
}

/// Why collection leaves a build volume for a later pass instead of deleting it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Deferral {
    /// A job admitted on the volume still runs and holds it; `0` names the processes that had
    /// the volume open when the release looked.
    Held(Vec<cowshed_os::process::Holder>),
    /// Its release failed; the next pass tries again.
    ReleaseFailed(String),
    /// Its record exists but cannot be read, so nothing proves it unreachable.
    RecordUnreadable(String),
    /// Its image's size cannot be read.
    SizeUnreadable(String),
    /// Its record names `0` as its checkout, which is detached: that checkout's link cannot be
    /// read until it is attached, so nothing proves the volume unreachable.
    DetachedCheckout(WorkspaceName),
    /// Its record names `0` as its checkout or seed target, whose create, fork or adopt has not
    /// finished: the staged checkout's link cannot be read, and the workspace does not exist
    /// yet, so nothing proves the volume unreachable.
    Creating(WorkspaceName),
    /// Its image has no owner record. Every build image is registered before its first byte,
    /// so these bytes predate owner registration and nothing proves whose they are: the image
    /// is named on every pass and never released on a guess.
    Unregistered,
    /// Under the release's claim, `0`'s build link names it: whatever this collection read
    /// before (a land moved `0` onto it, a mount re-pointed `0`'s stale link), it is `0`'s.
    Linked(WorkspaceName),
    /// Under the release's claim, its record is not the one this collection judged it by: a
    /// clone finished writing it, or a land handed the volume to its target. The next pass
    /// decides it from what it is now.
    Changed,
}

impl Deferral {
    /// Whether this is the ordinary state of a detached or still-forming workspace's own volume
    /// rather than something a person should look at.
    pub fn is_routine(&self) -> bool {
        matches!(self, Self::DetachedCheckout(_) | Self::Creating(_))
    }

    /// Whether a later collection is still to decide the volume: not when a checkout links it.
    pub fn is_pending(&self) -> bool {
        !matches!(self, Self::Linked(_))
    }

    /// Whether a live authority still answers for the volume — a job or creator's hold, a
    /// forming intent, a detached checkout's unreadable link, a link or record that moved under
    /// the claim — rather than a release that failed or a fact that could not be read.
    pub fn is_owned(&self) -> bool {
        match self {
            Self::Held(_)
            | Self::DetachedCheckout(_)
            | Self::Creating(_)
            | Self::Linked(_)
            | Self::Changed => true,
            Self::ReleaseFailed(_)
            | Self::RecordUnreadable(_)
            | Self::SizeUnreadable(_)
            | Self::Unregistered => false,
        }
    }
}

impl std::fmt::Display for Deferral {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held(open) => {
                formatter.write_str(
                    "a job admitted on it still runs, or a land or clone making it someone's still holds it",
                )?;
                if !open.is_empty() {
                    write!(formatter, ", open in {}", nx::Holders(open))?;
                }
                Ok(())
            }
            Self::ReleaseFailed(error) => write!(formatter, "its release failed: {error}"),
            Self::RecordUnreadable(error) => write!(formatter, "its record is unreadable: {error}"),
            Self::SizeUnreadable(error) => write!(formatter, "its size is unreadable: {error}"),
            Self::DetachedCheckout(checkout) => write!(
                formatter,
                "its record names {checkout}, which is detached; decided once {checkout} is attached"
            ),
            Self::Creating(workspace) => write!(
                formatter,
                "its record names {workspace}, whose create, fork or adopt has not finished; decided once it has"
            ),
            Self::Linked(checkout) => write!(
                formatter,
                "{checkout}'s build link names it now, so it is {checkout}'s"
            ),
            Self::Changed => formatter.write_str(
                "its record changed since this collection read it; the next pass decides it",
            ),
            Self::Unregistered => formatter.write_str(
                "its image has no owner record, so it predates owner registration and nothing proves whose it is; it is never released on a guess",
            ),
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

/// A build volume collection deletes, with why, its allocated size, and the record the plan
/// judged it by: its release decides it again under its claim, and a record that changed since
/// says the volume is no longer what the plan judged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Doomed {
    pub id: BuildVolumeId,
    pub reason: GcReason,
    pub bytes: u64,
    pub seen: BuildVolumeRecord,
    /// The image was already gone when planned: the release retires the record alone, and an
    /// image appearing since makes the planned release stale.
    pub image_missing: bool,
}

/// What collection decides from the records, the links and the images' sizes alone.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Plan {
    pub examined: u64,
    pub doomed: Vec<Doomed>,
    pub deferred: Vec<(BuildVolumeId, Deferral)>,
}

/// Which build volumes are garbage (16_build_volumes.md, "Garbage collection"): each one nothing
/// links that is not a seed, each seed that is not its existing target's latest, and each one a
/// creation registered and never finished, named by its exact registered id.
///
/// A live link proves reachability by itself, so a volume one names is kept before its record
/// is read. A detached workspace's link cannot be read, so whatever it may name is deferred with
/// it named, never counted reachable and never deleted; so is whatever a workspace still being
/// created may name, whose staged link no other process can read and whose seed belongs to a
/// workspace that does not exist yet; so is a volume whose record or size cannot be read.
/// Nothing is deleted on a guess.
///
/// Only `images` are decided: those listed before `links` was read. A create, fork or adopt
/// joins [`Links::creating`] before it makes a volume, and stays there until it has published
/// the workspace that links it, so a volume listed first is named by one of the later reads.
/// One made after the listing is in no `images`; deciding it from links read before it existed
/// collected a fresh fork's volume as unlinked garbage.
pub(crate) fn plan(
    layout: &BuildVolumeLayout,
    images: &[BuildVolumeId],
    links: &Links,
) -> Result<Plan> {
    let mut plan = Plan::default();
    let mut latest = std::collections::BTreeMap::<Owner, (BuildVolumeId, BuildVolumeRecord)>::new();
    let mut doomed = Vec::new();
    for id in images.iter().cloned() {
        plan.examined += 1;
        if links.volumes.contains(&id) {
            continue;
        }
        let record = match layout.read_record_present(&id) {
            Ok(Some(record)) => record,
            Ok(None) => {
                plan.deferred.push((id, Deferral::Unregistered));
                continue;
            }
            Err(error) => {
                plan.deferred
                    .push((id, Deferral::RecordUnreadable(error.to_string())));
                continue;
            }
        };
        // A registered creation is never decided by a workspace name, which a later verb may
        // reuse. Its live creator holds it by its exact id, so its release defers under the
        // claim; an unfinished re-adoption names it by its exact id in the journal; a detached
        // checkout's link, which cannot be read, may name it. Anything else is an interrupted
        // creation, released by its id.
        if let BuildVolumeStage::Registered { .. } = &record.stage {
            match &record.role {
                _ if links.journaled.contains(&id) => {
                    plan.deferred
                        .push((id, Deferral::Creating(record.role.owner().clone())));
                }
                BuildVolumeRole::Linked { checkout } if links.detached.contains(checkout) => {
                    plan.deferred
                        .push((id, Deferral::DetachedCheckout(checkout.clone())));
                }
                BuildVolumeRole::Linked { .. }
                | BuildVolumeRole::Seed { .. }
                | BuildVolumeRole::Superseded { .. } => {
                    doomed.push((id, GcReason::InterruptedBuildVolume, record));
                }
            }
            continue;
        }
        if matches!(&record.role, BuildVolumeRole::Seed { target, .. } if !links.creating.contains(target))
        {
            match std::fs::symlink_metadata(layout.image(&id)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    plan.doomed.push(Doomed {
                        id,
                        reason: GcReason::SupersededSeed,
                        bytes: 0,
                        seen: record,
                        image_missing: true,
                    });
                    continue;
                }
                Err(error) => {
                    plan.deferred
                        .push((id, Deferral::SizeUnreadable(error.to_string())));
                    continue;
                }
                Ok(_) => {}
            }
        }
        match &record.role {
            BuildVolumeRole::Seed { target, .. } | BuildVolumeRole::Linked { checkout: target }
                if links.creating.contains(target) =>
            {
                plan.deferred.push((id, Deferral::Creating(target.clone())));
            }
            BuildVolumeRole::Seed {
                target,
                incarnation,
            } => {
                let owner = Owner {
                    name: target.clone(),
                    incarnation: incarnation.clone(),
                };
                if !links.owners.contains(&owner) {
                    doomed.push((id, GcReason::SupersededSeed, record));
                    continue;
                }
                match latest.get(&owner) {
                    Some((_, newest)) if newest.created_at >= record.created_at => {
                        doomed.push((id, GcReason::SupersededSeed, record));
                    }
                    _ => {
                        if let Some((older, seen)) = latest.insert(owner, (id, record)) {
                            doomed.push((older, GcReason::SupersededSeed, seen));
                        }
                    }
                }
            }
            BuildVolumeRole::Linked { checkout } if links.detached.contains(checkout) => {
                plan.deferred
                    .push((id, Deferral::DetachedCheckout(checkout.clone())));
            }
            // Its owner's link moved off it: only a job admitted before the move can hold it,
            // which the release's claim answers.
            BuildVolumeRole::Linked { .. } | BuildVolumeRole::Superseded { .. } => {
                doomed.push((id, GcReason::UnlinkedBuildVolume, record));
            }
        }
    }
    for (id, reason, seen) in doomed {
        let image = layout.image(&id);
        // A hold is asked before the image is read: a creator holds its volume from registration
        // and may sit in its first write with the image's vnode held, which a stat waits out
        // (measured 2026-10-25: 37 s of an `rm`'s open). The hold is a file of its own. The
        // release would defer under its claim all the same.
        match layout.held(&id) {
            Ok(false) => {}
            Ok(true) => {
                plan.deferred.push((id, Deferral::Held(Vec::new())));
                continue;
            }
            Err(error) => {
                plan.deferred.push((
                    id,
                    Deferral::ReleaseFailed(format!("cannot tell whether a job holds it: {error}")),
                ));
                continue;
            }
        }
        // Only absence lets a volume go without its size: an image that does not exist cannot be
        // waiting for anything, and deferring its record deferred it on every pass forever. An
        // image that exists but cannot be read is deferred with the error, never released on a
        // guess (16_build_volumes.md, "Garbage collection").
        match std::fs::symlink_metadata(&image) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                plan.doomed.push(Doomed {
                    id,
                    reason,
                    bytes: 0,
                    seen,
                    image_missing: true,
                });
                continue;
            }
            Err(error) => {
                plan.deferred.push((
                    id,
                    Deferral::SizeUnreadable(format!("{}: {error}", image.display())),
                ));
                continue;
            }
            Ok(_) => {}
        }
        let allocated = std::fs::metadata(&image).and_then(|metadata| {
            if metadata.is_dir() {
                crate::storage::directory::allocated_tree_bytes(&image)
            } else {
                Ok(metadata.blocks().saturating_mul(512))
            }
        });
        match allocated {
            Ok(bytes) => plan.doomed.push(Doomed {
                id,
                reason,
                bytes,
                seen,
                image_missing: false,
            }),
            Err(error) => plan.deferred.push((
                id,
                Deferral::SizeUnreadable(format!("{}: {error}", image.display())),
            )),
        }
    }
    Ok(plan)
}

/// Every seed recorded for `owner` at its incarnation, published or still registered: what a new
/// seed replaces, so a capture an earlier crash left unfinished goes with the seed it replaced.
fn seeds_of(layout: &BuildVolumeLayout, owner: &Owner) -> Result<Vec<BuildVolumeId>> {
    let mut seeds = Vec::new();
    for id in layout.list()? {
        if layout.read_record_present(&id)?.is_some_and(|record| {
            matches!(&record.role, BuildVolumeRole::Seed { target, incarnation }
                if *target == owner.name && *incarnation == owner.incarnation)
        }) {
            seeds.push(id);
        }
    }
    Ok(seeds)
}

/// Which of `owner`'s volumes `record` is, for its retirement, or `None` when it is not
/// `owner`'s to release: a seed of it at its incarnation, a previous volume superseded under
/// that incarnation, or a volume a creation registered under that incarnation left unfinished.
/// The live volume its checkout links is decided by that link, not here.
fn owned_at(record: &BuildVolumeRecord, owner: &Owner) -> Option<&'static str> {
    let at = |name: &WorkspaceName, incarnation: &WorkspaceIncarnation| {
        *name == owner.name && *incarnation == owner.incarnation
    };
    match (&record.role, &record.stage) {
        (
            BuildVolumeRole::Seed {
                target,
                incarnation,
            },
            _,
        ) if at(target, incarnation) => Some("seed"),
        (
            BuildVolumeRole::Superseded {
                owner: name,
                incarnation,
            },
            _,
        ) if at(name, incarnation) => Some("superseded"),
        (
            BuildVolumeRole::Linked { checkout },
            BuildVolumeStage::Registered {
                by: Registrant::Workspace { incarnation },
            },
        ) if at(checkout, incarnation) => Some("unfinished"),
        (
            BuildVolumeRole::Linked { .. }
            | BuildVolumeRole::Seed { .. }
            | BuildVolumeRole::Superseded { .. },
            _,
        ) => None,
    }
}

fn defragment_images(
    layout: &BuildVolumeLayout,
    owner: &Owner,
) -> Result<Vec<(BuildVolumeId, bool)>> {
    let mut images = Vec::new();
    for id in layout.list()? {
        let record = layout.read_record(&id)?;
        if let Some(linked) = defragment_role(&record, owner) {
            images.push((id, linked));
        }
    }
    Ok(images)
}

fn defragment_role(record: &BuildVolumeRecord, owner: &Owner) -> Option<bool> {
    if !record.is_published() {
        return None;
    }
    match &record.role {
        BuildVolumeRole::Linked { checkout } if checkout == &owner.name => Some(true),
        BuildVolumeRole::Seed {
            target,
            incarnation,
        } if target == &owner.name && incarnation == &owner.incarnation => Some(false),
        BuildVolumeRole::Linked { .. }
        | BuildVolumeRole::Seed { .. }
        | BuildVolumeRole::Superseded { .. } => None,
    }
}

/// Doctor's physical build sources, from the existing owner records, not a source-only proxy.
pub(crate) fn extent_images(
    layout: &BuildVolumeLayout,
    owners: &BTreeMap<WorkspaceName, WorkspaceIncarnation>,
) -> Result<Vec<(WorkspaceName, &'static str, PathBuf)>> {
    let mut images = Vec::new();
    for id in layout.list()? {
        let record = layout.read_record(&id)?;
        if !record.is_published() {
            continue;
        }
        let (owner, code) = match record.role {
            BuildVolumeRole::Linked { checkout } if owners.contains_key(&checkout) => {
                (checkout, "build-source-extents")
            }
            BuildVolumeRole::Seed {
                target,
                incarnation,
            } if owners.get(&target) == Some(&incarnation) => (target, "build-seed-extents"),
            BuildVolumeRole::Linked { .. }
            | BuildVolumeRole::Seed { .. }
            | BuildVolumeRole::Superseded { .. } => continue,
        };
        images.push((owner, code, layout.image(&id)));
    }
    Ok(images)
}

/// Capture the quiet landing volume under the Nx and existing Cargo profile guards.
/// Contributed paths outside those protocols are exempt. A host Cargo build can still create
/// a new profile after enumeration; closing that gap requires a producer-side lease.
fn seal(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    quiet: &Quiet,
    target: &Owner,
    registration: &Registration,
) -> Result<std::result::Result<Freeze, AdoptionSkip>> {
    if let Err(busy) = nx::close(&quiet.mount, &quiet.state)
        .map_err(|error| io("close the landing volume's Nx state", &quiet.mount, &error))?
    {
        return Ok(Err(skip(busy, Side::Landing)));
    }
    let _opens = match nx::hold_opens(&quiet.mount, &quiet.state, &quiet.mount)
        .map_err(|error| io("lock the landing volume's Nx opens", &quiet.mount, &error))?
    {
        Ok(opens) => opens,
        Err(opening) => {
            return Ok(Err(AdoptionSkip::LandingOpening {
                database: opening.database,
                holders: opening.holders.into_iter().map(database_holder).collect(),
            }));
        }
    };
    let _builds = match cargo::hold(&quiet.mount, &quiet.state)
        .map_err(|error| io("take the landing Cargo profile locks", &quiet.mount, &error))?
    {
        Ok(held) => held,
        Err(lock) => {
            let holders = nx::holders(&lock)
                .map_err(|error| io("list the landing Cargo build's processes", &lock, &error))?;
            return Ok(Err(AdoptionSkip::LandingBuilding {
                lock,
                holders: holders.into_iter().map(database_holder).collect(),
            }));
        }
    };
    let previous = seeds_of(layout, target)?;
    if let Err(busy) = nx::held(&quiet.mount, &quiet.state).map_err(|error| {
        io(
            "look at the landing volume's Nx state",
            &quiet.mount,
            &error,
        )
    })? {
        return Ok(Err(skip(busy, Side::Landing)));
    }
    match host.clone_quiet_build_volume(layout, quiet, &BuildVolumeId::mint(), registration) {
        Ok(()) => Ok(Ok(Freeze::Frozen {
            superseded: claim_superseded(host, layout, target, previous)?,
        })),
        Err(ApfsStorageError::BuildVolumeHeld { image, open, .. }) => Ok(Ok(Freeze::SealBusy {
            reason: volume_busy_reason(&image, &open),
        })),
        Err(error) => Err(storage(error)),
    }
}

/// Why a conflicting storage claim refused capture, naming every reported holder.
fn volume_busy_reason(image: &Path, open: &[cowshed_os::process::Holder]) -> String {
    let holders = open
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "build volume {} is in use by {holders}; nothing was forced: close what holds it and run the command again",
        image.display()
    )
}

/// Claim `previous`, the seeds of `target` a new one replaced, under the target's image lock
/// (16_build_volumes.md, "Garbage collection"), for [`retire_seeds`] once it is let go. A seed is
/// never mounted and no job is admitted on one, so a seed already claimed is one an earlier land
/// or fork claimed and is retiring now, its lock let go: it is left to that.
fn claim_superseded(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    previous: Vec<BuildVolumeId>,
) -> Result<SupersededSeeds> {
    let mut claims = Vec::with_capacity(previous.len());
    for old in previous {
        match host.claim_build_volume(layout, &old).map_err(storage)? {
            BuildVolumeClaim::Claimed(claim) => claims.push(claim),
            BuildVolumeClaim::Held { .. } => {}
        }
    }
    Ok(SupersededSeeds {
        target: target.name.clone(),
        claims,
    })
}

/// Release `superseded`, claimed by [`claim_superseded`]: each is committed to
/// `build/.deleting` and its owner record retired, and the gateway's cleanup frees the bytes, so
/// neither a land nor a fork's seed refresh waits on freeing a seed nothing reads any more. A
/// seed is never mounted, so a release refused for a holder is an integrity fault.
fn retire_seeds(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    superseded: SupersededSeeds,
) -> Result<()> {
    let SupersededSeeds { target, claims } = superseded;
    // Every seed is tried, so one that fails keeps no other claimed past this retirement; the
    // first failure answers.
    let mut failed = None;
    for claim in claims {
        let old = claim.id().clone();
        match host.release_claimed(layout, claim) {
            Ok(refused) => say_release(
                &old,
                &format!("{target}'s superseded seed"),
                &BuildVolumeRelease::Deleted { refused },
            ),
            Err(ApfsStorageError::BuildVolumeHeld { .. }) => {
                failed.get_or_insert(CowshedError::integrity(
                    format!(
                        "seed {old} of {target} is held by a job, but no job is ever admitted on a seed"
                    ),
                    "cowshed doctor --json",
                ));
            }
            Err(error) => {
                failed.get_or_insert(storage(error));
            }
        }
    }
    failed.map_or(Ok(()), Err)
}

/// The capacity main's first build volume is minted at beside its image, from `source`'s
/// `.cowshed.toml`, or `None` when nothing in `source` can name build state.
fn may_mint(source: &Path) -> Result<Option<ImageCapacity>> {
    if !crate::capabilities::may_name_build_state(source)? {
        return Ok(None);
    }
    Ok(Some(
        crate::storage::bootstrap::main_cowshed_config(source)?.build_capacity(),
    ))
}

/// Release a volume minted beside main's image unless `checkout` links it, and say on stderr
/// what the release did or why the volume stays.
fn release_minted(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    volume: &MintedVolume,
    checkout: &Path,
    whose: &str,
) {
    let id = &volume.id;
    let released = layout.linked(checkout).and_then(|linked| {
        if linked.as_ref() == Some(id) {
            return Ok(None);
        }
        host.release_build_volume(layout, id)
            .map(Some)
            .map_err(storage)
    });
    match released {
        Ok(None) => {}
        Ok(Some(release)) => say_release(id, whose, &release),
        Err(error) => cowshed_os::eprintln!(
            "cowshed: {whose} build volume {id}, minted beside main, stays registered to the adopt, which no longer holds it: {error}; the next open of the project releases it by its id"
        ),
    }
}

/// Say on stderr what a release did beyond deleting a volume nothing had open: each process its
/// forced unmount cut off, pid and argv, so the holder is fixed where it lives (an indexer or an
/// editor that keeps a checkout's build state open after its link moved), or that a job's hold
/// keeps the volume until the job ends.
fn say_release(id: &BuildVolumeId, whose: &str, release: &BuildVolumeRelease) {
    let outcome = |forced: bool| {
        if forced {
            "nothing of cowshed's owned it, so its unmount was forced once the grace ran out"
        } else {
            "it was let go within the grace"
        }
    };
    match release {
        BuildVolumeRelease::Deleted { refused: None } => {}
        BuildVolumeRelease::Deleted {
            refused: Some(BuildVolumeRefusal { open, forced }),
        } if open.is_empty() => cowshed_os::eprintln!(
            "cowshed: {whose} build volume {id}: its unforced unmount was refused with no visible holder; {}",
            outcome(*forced)
        ),
        BuildVolumeRelease::Deleted {
            refused: Some(BuildVolumeRefusal { open, forced }),
        } => cowshed_os::eprintln!(
            "cowshed: {whose} build volume {id} was open in {} when its unforced unmount was refused; {}",
            nx::Holders(open),
            outcome(*forced)
        ),
        BuildVolumeRelease::Held { open } if open.is_empty() => cowshed_os::eprintln!(
            "cowshed: {whose} build volume {id} stays while a job admitted on it runs; the next rm, land or gc after it ends reclaims it"
        ),
        BuildVolumeRelease::Held { open } => cowshed_os::eprintln!(
            "cowshed: {whose} build volume {id} stays while a job admitted on it runs (open in {}); the next rm, land or gc after it ends reclaims it",
            nx::Holders(open)
        ),
    }
}

/// `target`'s seed against its live volume `live`, both measured against `tree` in the target's
/// repository at `checkout`, or `None` when `live` is an interrupted first touch's, unpublished:
/// only a fresh discovery finishes that one.
fn seed_age(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    live: BuildVolumeId,
    checkout: &Path,
    tree: &GitOid,
) -> Result<Option<SeedAge>> {
    let Some(record) = layout.read_record_present(&live)? else {
        return Ok(None);
    };
    let written = host.build_volume_written(layout, &live).map_err(storage)?;
    let seed = match layout.seed_of(&target.name, &target.incarnation)? {
        Some((id, seed)) => Some(Frozen {
            written: host.build_volume_written(layout, &id).map_err(storage)?,
            nearness: nearness(checkout, seed.tree.as_ref(), tree),
            tree: seed.tree,
            id,
        }),
        None => None,
    };
    Ok(Some(SeedAge {
        live,
        nearness: nearness(checkout, record.tree.as_ref(), tree),
        placed: Placed::of(record.checked_out.as_ref(), tree),
        tree: record.tree,
        written,
        seed,
    }))
}

/// How near `recorded` is to `tree` in the repository at `checkout`: the number of paths
/// `git diff-tree -r` reports between them. A tree the repository cannot compare is said on
/// stderr and is farther than any count.
fn nearness(checkout: &Path, recorded: Option<&GitOid>, tree: &GitOid) -> Nearness {
    use cowshed_os::fork_lock::Run as _;
    let Some(recorded) = recorded else {
        return Nearness::Unknown;
    };
    if recorded == tree {
        return Nearness::Equal;
    }
    // WHY unbounded: a git of this host's own repository with hooks, fsmonitor, pager, editors and
    // prompts off (`git_command_at`): it waits on no other process.
    #[allow(clippy::disallowed_methods)]
    let compared = crate::git::git_command_at(checkout)
        .args(["diff-tree", "-r", "--no-renames", "--name-only", "-z"])
        .args([recorded.as_str(), tree.as_str()])
        .output_locked();
    match compared {
        Ok(output) if output.status.success() => {
            let paths = output
                .stdout
                .split(|byte| *byte == 0)
                .filter(|path| !path.is_empty())
                .count();
            Nearness::Paths(u64::try_from(paths).unwrap_or(u64::MAX))
        }
        Ok(output) => {
            cowshed_os::eprintln!(
                "cowshed: cannot compare build tree {recorded} with {tree} in {}: {}; it counts as farther than any comparable tree",
                checkout.display(),
                crate::git::git_ended(&output)
            );
            Nearness::Unknown
        }
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: cannot compare build tree {recorded} with {tree} in {}: {}; it counts as farther than any comparable tree",
                checkout.display(),
                crate::git::git_spawn_error(&error)
            );
            Nearness::Unknown
        }
    }
}

/// The tree `checkout` has checked out, `HEAD^{tree}`.
fn head_tree(checkout: &Path) -> std::result::Result<GitOid, String> {
    use cowshed_os::fork_lock::Run as _;
    // WHY unbounded: a git of this host's own repository with hooks, fsmonitor, pager, editors and
    // prompts off (`git_command_at`): it waits on no other process.
    #[allow(clippy::disallowed_methods)]
    let output = crate::git::git_command_at(checkout)
        .args(["rev-parse", "--verify", "HEAD^{tree}"])
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error).to_string())?;
    if !output.status.success() {
        return Err(crate::git::git_ended(&output));
    }
    let tree = String::from_utf8(output.stdout).map_err(|error| error.to_string())?;
    GitOid::new(tree.trim_end()).map_err(|error| error.to_string())
}

/// An external land (16_build_volumes.md, "Which volume a seed holds", step 1): capture as
/// `target`'s seed the build volume of a live workspace in `aim` that has `aim`'s tree checked
/// out, warmest first, each under its own image lock taken without waiting and its tree read
/// again under it. `None` when none was captured; every candidate passed over is in `passed`.
/// The caller holds `target`'s image lock; each candidate's is taken for `holding`.
fn seed_from_landed(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    aim: &Aim,
    holding: &LockHolding,
    passed: &mut Vec<(WorkspaceName, LandedSkip)>,
) -> Result<Option<(Refresh, SupersededSeeds)>> {
    // The unlocked read only selects; the read under the candidate's lock decides.
    let mut candidates = Vec::new();
    for peer in &aim.landed {
        match head_tree(&peer.checkout) {
            Ok(tree) if tree == aim.tree => {}
            Ok(_) => continue,
            Err(reason) => {
                passed.push((peer.name.clone(), LandedSkip::Unreadable { reason }));
                continue;
            }
        }
        let Some(live) = layout.linked(&peer.checkout)? else {
            passed.push((peer.name.clone(), LandedSkip::NoBuildVolume));
            continue;
        };
        let written = host.build_volume_written(layout, &live).map_err(storage)?;
        candidates.push((Reverse(written), peer));
    }
    candidates.sort_by_key(|(written, _)| *written);
    for (_, peer) in candidates {
        let Some(_held) = host
            .lock_images(std::slice::from_ref(&peer.lock), LockMode::Try, holding)
            .map_err(storage)?
        else {
            passed.push((peer.name.clone(), LandedSkip::Locked));
            continue;
        };
        match head_tree(&peer.checkout) {
            Ok(tree) if tree == aim.tree => {}
            Ok(tree) => {
                passed.push((peer.name.clone(), LandedSkip::Moved { tree }));
                continue;
            }
            Err(reason) => {
                passed.push((peer.name.clone(), LandedSkip::Unreadable { reason }));
                continue;
            }
        }
        let Some(live) = layout.linked(&peer.checkout)? else {
            passed.push((peer.name.clone(), LandedSkip::NoBuildVolume));
            continue;
        };
        match capture(host, layout, target, &live, Some(aim.tree.clone()))? {
            Ok((elapsed_ms, superseded)) => {
                return Ok(Some((
                    Refresh::Landed {
                        workspace: peer.name.clone(),
                        tree: aim.tree.clone(),
                        elapsed_ms,
                    },
                    superseded,
                )));
            }
            Err(skip) => passed.push((peer.name.clone(), LandedSkip::Capture(skip))),
        }
    }
    Ok(None)
}

/// A reseed (16_build_volumes.md, "Which volume a seed holds", step 2): capture the target's
/// live volume when it is nearer the fork's tree than the seed, or as near and written since;
/// with the seeds a capture replaced.
fn reseed(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    age: SeedAge,
) -> Result<(Refresh, Option<SupersededSeeds>)> {
    if !age.reseeds() {
        return Ok((Refresh::Kept, None));
    }
    let behind_ms = age.behind().map(millis);
    Ok(match capture(host, layout, target, &age.live, age.tree)? {
        Ok((elapsed_ms, superseded)) => (
            Refresh::Reseeded {
                behind_ms,
                elapsed_ms,
            },
            Some(superseded),
        ),
        Err(reason) => (Refresh::Skipped { behind_ms, reason }, None),
    })
}

/// Freeze `source`, recorded at `tree`, as `target`'s seed after Nx and existing Cargo profiles
/// admit capture, then claim every older seed of `target` ([`claim_superseded`]) for its
/// retirement once the target's image lock is let go: the fork this refresh serves never waits
/// on releasing them under it, and no step of it reads them again. Measured on build 48a8ffff,
/// an inline free of one 300 GiB superseded seed held a provision about 12 minutes in
/// `ftruncate`. The elapsed milliseconds and the claimed seeds. Their
/// guards and the exclusive build hold stay alive through mounted capture and publication.
/// Exempt cache/index handles may stay open. A skip leaves every seed as it was.
fn capture(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    source: &BuildVolumeId,
    tree: Option<GitOid>,
) -> Result<std::result::Result<(u64, SupersededSeeds), CaptureSkip>> {
    let started = Instant::now();
    let mount = host.mount_build_volume(layout, source).map_err(storage)?;
    let state = BuildVolumeState::read(&mount)?;
    // Claim before stopping Nx: the source's keeper must not admit a replacement job.
    // Retain the claim through mounted capture and immutable seed publication.
    let claim = match host.claim_build_volume(layout, source).map_err(storage)? {
        BuildVolumeClaim::Claimed(claim) => claim,
        BuildVolumeClaim::Held { open } => {
            let hold = layout.hold_path(source);
            let claimants = nx::holders(&hold)
                .map_err(|error| io("list the build hold's holders", &hold, &error))?;
            return Ok(Err(CaptureSkip::VolumeBusy {
                reason: build_hold_busy_reason(&layout.image(source), &hold, &claimants, &open),
            }));
        }
    };
    if let Err(busy) = nx::close(&mount, &state)
        .map_err(|error| io("close the volume's Nx state", &mount, &error))?
    {
        return Ok(Err(capture_skip(busy)));
    }
    let _opens = match nx::hold_opens(&mount, &state, &mount)
        .map_err(|error| io("lock the volume's Nx opens", &mount, &error))?
    {
        Ok(opens) => opens,
        Err(opening) => {
            return Ok(Err(CaptureSkip::Opening {
                database: opening.database,
                holders: opening.holders.into_iter().map(database_holder).collect(),
            }));
        }
    };
    let _builds = match cargo::hold(&mount, &state)
        .map_err(|error| io("take the volume's Cargo build locks", &mount, &error))?
    {
        Ok(held) => held,
        Err(lock) => {
            let holders = nx::holders(&lock)
                .map_err(|error| io("list the Cargo build's processes", &lock, &error))?;
            return Ok(Err(CaptureSkip::Building {
                lock,
                holders: holders.into_iter().map(database_holder).collect(),
            }));
        }
    };
    let previous = seeds_of(layout, target)?;
    if let Err(busy) = nx::held(&mount, &state)
        .map_err(|error| io("look at the volume's Nx state", &mount, &error))?
    {
        return Ok(Err(capture_skip(busy)));
    }
    match host.clone_claimed_build_volume(
        layout,
        &claim,
        &BuildVolumeId::mint(),
        &Registration::new(
            BuildVolumeRecord::new(
                tree,
                BuildVolumeRole::Seed {
                    target: target.name.clone(),
                    incarnation: target.incarnation.clone(),
                },
            ),
            Registrant::Workspace {
                incarnation: target.incarnation.clone(),
            },
        ),
    ) {
        Ok(()) => {}
        Err(ApfsStorageError::BuildVolumeHeld { image, open, .. }) => {
            return Ok(Err(CaptureSkip::VolumeBusy {
                reason: volume_busy_reason(&image, &open),
            }));
        }
        Err(error) => return Err(storage(error)),
    }
    let superseded = claim_superseded(host, layout, target, previous)?;
    let elapsed_ms = millis(started.elapsed());
    cowshed_os::timing::event("build-volume", || {
        format!("seed {} from {source} in {elapsed_ms} ms", target.name)
    });
    Ok(Ok((elapsed_ms, superseded)))
}

/// Say on stderr what a seed refresh of `target` did beyond keeping a seed that needed nothing:
/// each live workspace at the fork's tree it passed over and why, a capture of a landed
/// workspace's volume, or a reseed that a writer refused, which `destination` then misses.
pub(crate) fn say_refresh(
    target: &WorkspaceName,
    destination: &WorkspaceName,
    refreshed: &Refreshed,
) {
    for (workspace, reason) in &refreshed.passed {
        cowshed_os::eprintln!(
            "cowshed: {workspace} has the tree {target} moved to checked out, but does not seed {target}: {reason}"
        );
    }
    match &refreshed.outcome {
        Refresh::NoBuildVolume | Refresh::Kept | Refresh::Reseeded { .. } => {}
        Refresh::Landed {
            workspace,
            tree,
            elapsed_ms,
        } => cowshed_os::eprintln!(
            "cowshed: {target}'s branch moved to tree {tree} outside cowshed land; its seed is now {workspace}'s build volume ({elapsed_ms} ms)"
        ),
        Refresh::Skipped { behind_ms, reason } => cowshed_os::eprintln!(
            "cowshed: {target}'s seed stays{} behind its build volume, so {destination} misses what {target} built since: {reason}; the next fork retries",
            behind_ms.map(|ms| format!(" {ms} ms")).unwrap_or_default(),
        ),
    }
}

/// Why a reseed could not claim its source's build hold, naming every holder of that hold: a
/// job admitted on the volume (its supervisor's process holds the hold for it) or another
/// cowshed operation claiming the volume, and anything with a file open inside it.
fn build_hold_busy_reason(
    image: &Path,
    hold: &Path,
    claimants: &[cowshed_os::process::Holder],
    open: &[cowshed_os::process::Holder],
) -> String {
    let named = |holders: &[cowshed_os::process::Holder]| {
        holders
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut reason = format!(
        "build volume {} has a job admitted on it or another cowshed operation claiming it: its build hold {} is held by {}",
        image.display(),
        hold.display(),
        match claimants {
            [] => "a process this user cannot see".to_owned(),
            claimants => named(claimants),
        }
    );
    if !open.is_empty() {
        reason.push_str(&format!(", and {} has it open", named(open)));
    }
    reason.push_str("; nothing was stopped: the next fork retries once that job ends");
    reason
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn database_holder(holder: cowshed_os::process::Holder) -> DatabaseHolder {
    DatabaseHolder {
        pid: holder.pid,
        command: holder.command,
    }
}

fn capture_skip(busy: nx::Busy) -> CaptureSkip {
    match busy {
        nx::Busy::Held { database, holders } => CaptureSkip::Held {
            database,
            holders: holders.into_iter().map(database_holder).collect(),
        },
        nx::Busy::DaemonStayed { daemon } => CaptureSkip::DaemonStayed {
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
        ApfsStorageError::Apfs(error) if cowshed_apfs::detach_was_dissented(&error) => {
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

pub(super) fn io(operation: &str, path: &Path, error: &std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} at {}: {error}", path.display()),
        "cowshed doctor --json",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::{ProjectPaths, RepoId};
    use cowshed_os::test_backstop::{
        AsyncChildBackstop as _, AsyncCommandBackstop as _, ChildBackstop as _,
        CommandBackstop as _,
    };
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

        /// A volume `by` registered and never published: its image may hold anything.
        fn registered(&self, role: BuildVolumeRole, by: Registrant) -> BuildVolumeId {
            let id = self.image();
            self.layout
                .write_record(
                    &id,
                    Registration::new(BuildVolumeRecord::new(None, role), by).registered(),
                )
                .unwrap();
            id
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn extent_diagnostics_and_defrag_cover_build_sources_and_exact_owner_seeds() {
        let store = Store::new();
        let owner = Owner {
            name: WorkspaceName::main(),
            incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
        };
        let linked = store.volume(BuildVolumeRole::Linked {
            checkout: owner.name.clone(),
        });
        let seed = store.volume(BuildVolumeRole::Seed {
            target: owner.name.clone(),
            incarnation: owner.incarnation.clone(),
        });
        let older = store.volume(BuildVolumeRole::Seed {
            target: owner.name.clone(),
            incarnation: WorkspaceIncarnation::new("2".repeat(32)).unwrap(),
        });
        let superseded = store.volume(BuildVolumeRole::Superseded {
            owner: owner.name.clone(),
            incarnation: owner.incarnation.clone(),
        });
        let incomplete = store.volume(BuildVolumeRole::Seed {
            target: owner.name.clone(),
            incarnation: owner.incarnation.clone(),
        });
        let mut record = store.layout.read_record(&incomplete).unwrap();
        record.stage = crate::build_volume::BuildVolumeStage::Registered {
            by: crate::build_volume::Registrant::Intent,
        };
        store.layout.write_record(&incomplete, &record).unwrap();
        assert_eq!(defragment_role(&record, &owner), None);
        let mut selected = defragment_images(&store.layout, &owner).unwrap();
        selected.sort();
        let mut expected = vec![(linked.clone(), true), (seed.clone(), false)];
        expected.sort();
        assert_eq!(
            selected, expected,
            "detached checkout still names its registered build source"
        );
        let owners = BTreeMap::from([(owner.name.clone(), owner.incarnation.clone())]);
        let diagnostic = extent_images(&store.layout, &owners).unwrap();
        assert!(diagnostic.contains(&(
            owner.name.clone(),
            "build-source-extents",
            store.layout.image(&linked)
        )));
        assert!(diagnostic.contains(&(
            owner.name.clone(),
            "build-seed-extents",
            store.layout.image(&seed)
        )));
        assert!(
            diagnostic
                .iter()
                .all(|(_, _, path)| path != &store.layout.image(&older))
        );
        assert!(
            diagnostic
                .iter()
                .all(|(_, _, path)| path != &store.layout.image(&superseded))
        );
        assert!(
            diagnostic
                .iter()
                .all(|(_, _, path)| path != &store.layout.image(&incomplete))
        );
    }

    fn name(value: &str) -> WorkspaceName {
        WorkspaceName::new(value).unwrap()
    }

    fn linked(checkout: &str) -> BuildVolumeRole {
        BuildVolumeRole::Linked {
            checkout: name(checkout),
        }
    }

    fn superseded(owner: &str) -> BuildVolumeRole {
        BuildVolumeRole::Superseded {
            owner: name(owner),
            incarnation: WorkspaceIncarnation::new("0".repeat(32)).unwrap(),
        }
    }

    /// What a test's clone or fork registers: the forming verb's, as a fork's would be.
    fn registered(record: BuildVolumeRecord) -> Registration {
        Registration::new(record, Registrant::Intent)
    }

    fn doomed(plan: &Plan) -> Vec<(BuildVolumeId, GcReason)> {
        plan.doomed
            .iter()
            .map(|doomed| (doomed.id.clone(), doomed.reason))
            .collect()
    }

    /// A fork's aim at a tree no volume in these tests records, with no workspace that may have
    /// landed it: every volume is `Unknown`, so the reseed decides by write instants alone.
    fn unlanded_aim() -> Aim {
        Aim {
            tree: GitOid::new("f".repeat(40)).unwrap(),
            landed: Vec::new(),
        }
    }

    fn age(live: (Nearness, SystemTime), seed: Option<(Nearness, SystemTime)>) -> SeedAge {
        SeedAge {
            live: BuildVolumeId::mint(),
            tree: None,
            nearness: live.0,
            placed: Placed::Elsewhere(GitOid::new("e".repeat(40)).unwrap()),
            written: live.1,
            seed: seed.map(|(nearness, written)| Frozen {
                id: BuildVolumeId::mint(),
                tree: None,
                nearness,
                written,
            }),
        }
    }

    /// Between two volumes as near the fork's tree, a seed is behind exactly when the live
    /// volume was written after it was frozen; a target with no seed is behind by no measurable
    /// amount, and still reseeds.
    #[test]
    fn a_seed_is_behind_only_when_the_live_volume_was_written_after_it() {
        let frozen = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        for nearness in [Nearness::Equal, Nearness::Paths(3), Nearness::Unknown] {
            let at = |written| age((nearness, written), Some((nearness, frozen)));
            let fresh = at(frozen);
            assert!(!fresh.reseeds());
            assert_eq!(fresh.behind(), None);
            let older = at(frozen - Duration::from_secs(5));
            assert!(!older.reseeds(), "a seed newer than every write holds them");
            assert_eq!(older.behind(), None);
            let behind = at(frozen + Duration::from_millis(1_500));
            assert!(behind.reseeds());
            assert_eq!(behind.behind(), Some(Duration::from_millis(1_500)));
        }
        let unseeded = age((Nearness::Unknown, frozen), None);
        assert!(unseeded.reseeds());
        assert_eq!(unseeded.behind(), None);
    }

    /// A seed nearer the fork's tree than the live volume is kept however much later the live
    /// volume was written: a landed seed beats a live volume built at an older tree. A live
    /// volume nearer than the seed reseeds it even when the seed was frozen after its last write.
    #[test]
    fn the_volume_nearer_the_forks_tree_seeds_it_before_the_newer_one() {
        let frozen = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let later = frozen + Duration::from_secs(600);
        let landed = age(
            (Nearness::Paths(40), later),
            Some((Nearness::Equal, frozen)),
        );
        assert!(!landed.reseeds());
        assert!(
            landed.moved_from().is_none(),
            "the seed was built at the fork's tree"
        );
        let nearer = age(
            (Nearness::Paths(2), later),
            Some((Nearness::Paths(9), frozen)),
        );
        assert!(nearer.reseeds());
        let comparable = age(
            (Nearness::Paths(9), later),
            Some((Nearness::Unknown, frozen)),
        );
        assert!(
            comparable.reseeds(),
            "any count is nearer than an unknown tree"
        );
        let adopted = age((Nearness::Equal, frozen), Some((Nearness::Paths(1), later)));
        assert!(
            adopted.reseeds(),
            "an older live volume at the fork's own tree"
        );
        assert!(adopted.moved_from().is_none());
        let moved = age(
            (Nearness::Paths(1), later),
            Some((Nearness::Paths(1), frozen)),
        );
        assert!(
            moved.moved_from().is_some(),
            "neither volume was built at the fork's tree"
        );
        assert!(
            age((Nearness::Unknown, frozen), None)
                .moved_from()
                .is_some()
        );
        // A checkout still at the tree cowshed placed its volume at did not move, and a record
        // that cannot say where it was placed is never read as moved.
        let unmoved = SeedAge {
            placed: Placed::Here,
            ..age(
                (Nearness::Paths(1), later),
                Some((Nearness::Paths(1), frozen)),
            )
        };
        assert!(
            unmoved.moved_from().is_none(),
            "the target's branch has not moved"
        );
        let unrecorded = SeedAge {
            placed: Placed::Unrecorded,
            ..age(
                (Nearness::Paths(1), later),
                Some((Nearness::Paths(1), frozen)),
            )
        };
        assert!(
            unrecorded.moved_from().is_none(),
            "a record that predates checkedOut"
        );
    }

    /// A git repository at `path` whose commit holds `files`, by its `HEAD^{tree}`.
    fn commit(path: &Path, files: &[(&str, &str)]) -> GitOid {
        let git = |args: &[&str]| {
            let output = crate::git::git_command_at(path)
                .args([
                    "-c",
                    "user.name=cowshed",
                    "-c",
                    "user.email=cowshed@example.invalid",
                ])
                .args(args)
                .output_bounded()
                .unwrap();
            assert!(output.status.success(), "git {args:?}: {output:?}");
            String::from_utf8(output.stdout).unwrap()
        };
        if !path.join(".git").exists() {
            fs::create_dir_all(path).unwrap();
            git(&["init", "--quiet"]);
        }
        for (name, content) in files {
            fs::write(path.join(name), content).unwrap();
            git(&["add", "--", name]);
        }
        git(&["commit", "--quiet", "--allow-empty", "-m", "files"]);
        GitOid::new(git(&["rev-parse", "HEAD^{tree}"]).trim_end()).unwrap()
    }

    /// Nearness counts the paths `git diff-tree` reports between two trees, and a tree the
    /// repository does not hold, or none, is farther than any count.
    #[test]
    fn nearness_counts_the_paths_two_trees_differ_in() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-nearness-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let first = commit(&root, &[("a", "1"), ("b", "1")]);
        let second = commit(&root, &[("a", "2"), ("c", "1")]);
        let absent = GitOid::new("e".repeat(40)).unwrap();
        assert_eq!(nearness(&root, Some(&second), &second), Nearness::Equal);
        assert_eq!(nearness(&root, Some(&first), &second), Nearness::Paths(2));
        assert_eq!(nearness(&root, Some(&absent), &second), Nearness::Unknown);
        assert_eq!(nearness(&root, None, &second), Nearness::Unknown);
        assert_eq!(head_tree(&root), Ok(second));
        fs::remove_dir_all(&root).unwrap();
    }

    /// A live link is the proof of reachability, so it protects its volume before any record is
    /// read: an interrupted first touch leaves a linked volume whose record is still registered,
    /// and deleting it would pull the build state out from under a checkout.
    #[test]
    fn a_live_link_keeps_its_volume_registered_or_published() {
        let store = Store::new();
        let touching = store.registered(
            linked("topic"),
            Registrant::Workspace {
                incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
            },
        );
        let recorded_elsewhere = store.volume(linked("other"));
        let garbage = store.volume(superseded("main"));
        let links = Links {
            volumes: [touching, recorded_elsewhere].into_iter().collect(),
            ..Links::default()
        };
        let plan = plan(&store.layout, &store.layout.list().unwrap(), &links).unwrap();
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
    /// volume is deferred with that workspace named, never counted reachable or deleted, and so
    /// is a first touch it registered, which its next refresh may still finish. A creation an
    /// intent registered under another name whose intent is gone is interrupted, and is
    /// collected by its id: a detached shed does not block what it does not name.
    #[test]
    fn a_detached_checkout_defers_only_what_it_names() {
        let store = Store::new();
        let named = store.volume(linked("topic"));
        let touching = store.registered(
            linked("topic"),
            Registrant::Workspace {
                incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
            },
        );
        let interrupted = store.registered(linked("gone"), Registrant::Intent);
        let links = Links {
            detached: [name("topic")].into_iter().collect(),
            ..Links::default()
        };
        let mut plan = plan(&store.layout, &store.layout.list().unwrap(), &links).unwrap();
        plan.deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (named, Deferral::DetachedCheckout(name("topic"))),
            (touching, Deferral::DetachedCheckout(name("topic"))),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(plan.deferred, expected);
        assert_eq!(
            doomed(&plan),
            [(interrupted, GcReason::InterruptedBuildVolume)]
        );
        assert!(
            plan.deferred
                .iter()
                .all(|(_, deferral)| deferral.is_routine())
        );
    }

    /// Every build image is registered before its first byte, so an image with no record
    /// predates owner registration: nothing proves whose it is, and it is named on every pass,
    /// never released on a guess and never mistaken for anyone's interrupted creation.
    #[test]
    fn an_unregistered_image_is_named_and_never_released() {
        let store = Store::new();
        let unregistered = store.image();
        let plan = plan(
            &store.layout,
            &store.layout.list().unwrap(),
            &Links {
                creating: [name("main")].into_iter().collect(),
                ..Links::default()
            },
        )
        .unwrap();
        assert_eq!(doomed(&plan), []);
        assert_eq!(plan.deferred, [(unregistered, Deferral::Unregistered)]);
        assert!(!Deferral::Unregistered.is_routine());
        assert!(Deferral::Unregistered.is_pending());
    }

    /// A create or fork forks its volume and seed into a staged checkout no other process can
    /// read, and an adopt mints main's first volume beside main's image, before the workspace
    /// exists. Until its intent completes, whatever it registered or recorded as its own — its
    /// live volume, its seed, a capture still running — is deferred, never collected. A creation
    /// registered under a name no verb is forming is interrupted and collected by its id.
    /// Another process's `rm` collected the forming shed's own volumes, and the new workspace's
    /// mount then refused a link to a volume that was gone.
    #[test]
    fn a_forming_workspace_defers_only_the_volumes_it_names() {
        let store = Store::new();
        let live = store.volume(linked("lane"));
        let seed = store.volume(BuildVolumeRole::Seed {
            target: name("lane"),
            incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
        });
        let cloning = store.registered(
            BuildVolumeRole::Seed {
                target: name("lane"),
                incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
            },
            Registrant::Intent,
        );
        let interrupted = store.registered(linked("gone"), Registrant::Intent);
        let garbage = store.volume(linked("gone"));
        let links = Links {
            creating: [name("lane")].into_iter().collect(),
            ..Links::default()
        };
        let images = store.layout.list().unwrap();
        let mut plan = plan(&store.layout, &images, &links).unwrap();
        plan.deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (live.clone(), Deferral::Creating(name("lane"))),
            (seed.clone(), Deferral::Creating(name("lane"))),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(plan.deferred, expected);
        // A registration is never kept by the forming name: only its creator's hold on its id
        // keeps it, under the release's claim, so the plan dooms it like any interrupted one.
        let mut collected = doomed(&plan);
        collected.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected_doomed = vec![
            (cloning.clone(), GcReason::InterruptedBuildVolume),
            (interrupted.clone(), GcReason::InterruptedBuildVolume),
            (garbage.clone(), GcReason::UnlinkedBuildVolume),
        ];
        expected_doomed.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(collected, expected_doomed);
        assert!(Deferral::Creating(name("lane")).is_routine());
        // Without the forming workspace, all of them read as garbage: the collection that left
        // a new workspace linking a volume nobody owned. The unfinished capture is never the
        // latest seed: it goes as interrupted, not as superseding the published one.
        let mut doomed = doomed(&super::plan(&store.layout, &images, &Links::default()).unwrap());
        doomed.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (live, GcReason::UnlinkedBuildVolume),
            (seed, GcReason::SupersededSeed),
            (cloning, GcReason::InterruptedBuildVolume),
            (interrupted, GcReason::InterruptedBuildVolume),
            (garbage, GcReason::UnlinkedBuildVolume),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(doomed, expected);
    }

    /// A registered seed is never anyone's latest: while the target exists, its published seed
    /// stays its latest beside an unfinished capture created after it, which goes alone.
    #[test]
    fn an_unfinished_capture_never_supersedes_the_published_seed() {
        let store = Store::new();
        let lane = Owner {
            name: name("lane"),
            incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
        };
        let role = BuildVolumeRole::Seed {
            target: lane.name.clone(),
            incarnation: lane.incarnation.clone(),
        };
        let published = store.volume(role.clone());
        let capture = store.registered(
            role,
            Registrant::Workspace {
                incarnation: lane.incarnation.clone(),
            },
        );
        let links = Links {
            owners: [lane.clone()].into_iter().collect(),
            ..Links::default()
        };
        let plan = plan(&store.layout, &store.layout.list().unwrap(), &links).unwrap();
        assert_eq!(doomed(&plan), [(capture, GcReason::InterruptedBuildVolume)]);
        assert_eq!(
            store
                .layout
                .seed_of(&lane.name, &lane.incarnation)
                .unwrap()
                .map(|(id, _)| id),
            Some(published)
        );
    }

    /// A volume a live creator or job holds is deferred by the plan itself, before anything stats
    /// its image: a creator may sit in its first write holding the image's vnode, and a stat of
    /// the image waits as long (measured 2026-10-25: an `rm`'s open spent 37 s in one `lstat` of
    /// a concurrent fork's volume). Its hold, a file of its own, answers at once.
    #[test]
    fn a_held_volume_is_deferred_before_its_image_is_read() {
        let store = Store::new();
        let forming = store.registered(linked("lane"), Registrant::Intent);
        let held = store.layout.hold(&forming).unwrap();
        let planned = plan(
            &store.layout,
            std::slice::from_ref(&forming),
            &Links::default(),
        )
        .unwrap();
        drop(held);
        assert_eq!(doomed(&planned), Vec::new(), "{planned:?}");
        assert_eq!(
            planned.deferred,
            vec![(forming, Deferral::Held(Vec::new()))]
        );
    }

    /// The journal holds one intent per name, so a reused name's later fork must never be
    /// authority for what an earlier, crashed fork of that name registered: even while the
    /// later fork forms, the stale registrations are interrupted and doomed by their ids (only a
    /// live creator's hold on an id defers it, under the release's claim), they are never its
    /// seed or its link's re-point, and its own published volume stays. Only an exact id an
    /// unfinished re-adoption's journaled record names is deferred as forming.
    #[test]
    fn a_reused_name_is_never_authority_for_an_earlier_registration() {
        let store = Store::new();
        let lane = Owner {
            name: name("lane"),
            incarnation: WorkspaceIncarnation::new("2".repeat(32)).unwrap(),
        };
        let stale_seed = store.registered(
            BuildVolumeRole::Seed {
                target: lane.name.clone(),
                incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
            },
            Registrant::Intent,
        );
        let stale_live = store.registered(linked("lane"), Registrant::Intent);
        let live = store.volume(linked("lane"));
        let journaled = store.registered(linked("main"), Registrant::Intent);
        let forming = Links {
            creating: [lane.name.clone(), name("main")].into_iter().collect(),
            journaled: [journaled.clone()].into_iter().collect(),
            ..Links::default()
        };
        let images = store.layout.list().unwrap();
        let plan_forming = plan(&store.layout, &images, &forming).unwrap();
        let mut released = doomed(&plan_forming);
        released.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (stale_seed.clone(), GcReason::InterruptedBuildVolume),
            (stale_live.clone(), GcReason::InterruptedBuildVolume),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(released, expected, "{plan_forming:?}");
        let mut deferred = plan_forming.deferred.clone();
        deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (live.clone(), Deferral::Creating(lane.name.clone())),
            (journaled, Deferral::Creating(name("main"))),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(deferred, expected);
        assert_eq!(
            store.layout.seed_of(&lane.name, &lane.incarnation).unwrap(),
            None
        );
        assert_eq!(
            store.layout.resolve_link(&lane.name, &stale_seed).unwrap(),
            crate::build_volume::LinkResolution::Repoint(live),
            "a stale link re-points at the published volume, never at an unfinished one"
        );
    }

    /// A record that cannot be read never lets its volume go: the release a job's end owes and a
    /// retirement's own release both refuse with the error, and the image and the unreadable
    /// record are left byte for byte, for an operator to repair.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_an_unreadable_record_refuses_release_and_keeps_its_volume() {
        let scratch = Scratch::directory("build-corrupt-record");
        let topic = owner("topic", '1');
        let (checkout, live, _) = scratch.linked_checkout("topic", 1);
        let corrupt = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &corrupt,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Superseded {
                        owner: topic.name.clone(),
                        incarnation: topic.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        fs::write(scratch.layout.record(&corrupt), b"{not json").unwrap();

        let refused = scratch
            .volumes
            .release_superseded(corrupt.clone())
            .await
            .expect_err("an unreadable record is never read as nothing to release");
        assert!(
            refused
                .message
                .contains(&scratch.layout.record(&corrupt).display().to_string()),
            "{refused}"
        );
        let refused = scratch
            .volumes
            .release_owned(topic, checkout)
            .await
            .expect_err("a retirement refuses while one of the records it reads is unreadable");
        assert!(
            refused
                .message
                .contains(&scratch.layout.record(&corrupt).display().to_string()),
            "{refused}"
        );
        assert!(scratch.layout.image(&corrupt).exists());
        assert_eq!(
            fs::read(scratch.layout.record(&corrupt)).unwrap(),
            b"{not json"
        );
        // Repaired, the volume is releasable again.
        scratch
            .layout
            .write_record(&corrupt, &BuildVolumeRecord::new(None, linked("topic")))
            .unwrap();
        scratch.release_all();
    }

    /// A seed an earlier land or fork claimed for its retirement is that retirement's to
    /// release: a retiring workspace's own release leaves it and releases the rest.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_a_retirement_leaves_a_seed_another_retirement_claims() {
        let scratch = Scratch::directory("build-claimed-seed");
        let topic = owner("topic", '1');
        let (checkout, live, _) = scratch.linked_checkout("topic", 1);
        let seed = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &seed,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: topic.name.clone(),
                        incarnation: topic.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        let BuildVolumeClaim::Claimed(claim) = scratch
            .host
            .claim_build_volume(&scratch.layout, &seed)
            .unwrap()
        else {
            panic!("nothing holds the seed");
        };
        assert_eq!(
            scratch
                .layout
                .seed_of(&topic.name, &topic.incarnation)
                .unwrap(),
            None,
            "a newest seed a release claims leaves no seed, never an older one"
        );
        let target = topic.name.clone();
        scratch
            .volumes
            .release_owned(topic, checkout)
            .await
            .expect("the claimed seed is left to its retirement");
        assert!(
            !scratch.layout.image(&live).exists(),
            "the live volume is released"
        );
        assert!(
            scratch.layout.image(&seed).exists(),
            "the claimed seed is left"
        );
        retire_seeds(
            &*scratch.host,
            &scratch.layout,
            SupersededSeeds {
                target,
                claims: vec![claim],
            },
        )
        .unwrap();
        assert!(
            !scratch.layout.image(&seed).exists(),
            "its retirement releases it"
        );
        scratch.release_all();
    }

    /// Collection decides only the images it listed before it read what links them. A verb that
    /// crosses its mutation fence and makes a volume after the collection read the journal is
    /// absent from those links, and its volume is not one of the images listed: it is left to a
    /// later pass, whose links are read after the volume exists. Listed after the links, it was
    /// decided from links that predate it, and collected as an interrupted creation.
    #[test]
    fn an_image_made_after_the_listing_is_left_to_a_later_pass() {
        let store = Store::new();
        let garbage = store.volume(superseded("main"));
        // The collection lists the images, then reads the journal and the workspaces: no verb
        // is forming yet.
        let images = store.layout.list().unwrap();
        let links = Links::default();
        // Only then does a verb cross its fence and make a volume of its forming workspace.
        let minted = store.volume(linked("main"));
        let plan = plan(&store.layout, &images, &links).unwrap();
        assert_eq!(
            doomed(&plan),
            [(garbage.clone(), GcReason::UnlinkedBuildVolume)],
            "{plan:?}"
        );
        assert_eq!(plan.examined, 1);
        assert_eq!(plan.deferred, []);
        // The next pass lists it, and reads the journal after: main is forming until its verb
        // publishes it.
        let next = Links {
            creating: [name("main")].into_iter().collect(),
            ..Links::default()
        };
        let plan = super::plan(&store.layout, &store.layout.list().unwrap(), &next).unwrap();
        assert_eq!(doomed(&plan), [(garbage, GcReason::UnlinkedBuildVolume)]);
        assert_eq!(plan.deferred, [(minted, Deferral::Creating(name("main")))]);
    }

    /// The `cowshed fork` that another process's `rm` broke, on real APFS: a fork clones main's
    /// seed into a staged checkout, and a collection runs before the workspace is published.
    /// Told the workspace is forming, collection keeps its live volume and its seed, and the
    /// fork owns what its link names. Without that, the same collection deletes both, and the
    /// fork's mount refuses its link exactly as the broken `cowshed fork` did.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_collection_keeps_a_forming_forks_volume_and_seed() {
        let scratch = Scratch::directory("build-forming-fork");
        let main = owner("main", '0');
        let (main_checkout, main_live, _) = scratch.linked_checkout("main", 1);
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &BuildVolumeId::mint(),
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                )),
            )
            .expect("seed main");
        let lane = owner("lane", '1');
        let staged = scratch.root.path().join("staged-lane");
        fs::create_dir_all(staged.join(".cowshed")).unwrap();
        let prepared = scratch
            .volumes
            .capture_fork_retired(
                main.clone(),
                main_checkout,
                lane.name.clone(),
                unlanded_aim(),
            )
            .await
            .expect("capture the fork from main's seed");
        let prepared = scratch
            .volumes
            .ready_fork(Ok(prepared), lane.clone())
            .await
            .expect("ready the fork's volume");
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
            .collect(scratch.layout.list().unwrap(), links(&[&lane]), false)
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

        // The collection that ran beside the broken `cowshed fork`, which knew nothing of the fork.
        let collected = scratch
            .volumes
            .collect(scratch.layout.list().unwrap(), links(&[]), false)
            .await
            .unwrap();
        assert_eq!(collected.reclaimed, 2, "{:?}", collected.candidates);
        // The fork's link names what that collection took: its mount no longer refuses, and
        // has no seed left either, so the mount removes the link.
        assert_eq!(
            scratch.layout.resolve_link(&lane.name, &forked).unwrap(),
            crate::build_volume::LinkResolution::Vanished
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
            .write_record(&sizeless, &BuildVolumeRecord::new(None, superseded("main")))
            .unwrap();
        let garbage = store.volume(superseded("main"));
        let plan = plan(
            &store.layout,
            &store.layout.list().unwrap(),
            &Links::default(),
        )
        .unwrap();
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

    /// A record whose image is gone is released with it, never deferred: an image that does not
    /// exist cannot be waiting for anything, and deferring its record retried it on every pass
    /// forever. An image that exists but whose size cannot be read is still deferred.
    #[test]
    fn an_absent_image_releases_its_record_and_an_unreadable_present_one_defers() {
        let store = Store::new();
        let absent = BuildVolumeId::mint();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Linked {
                checkout: WorkspaceName::new("retired").unwrap(),
            },
        );
        store.layout.write_record(&absent, &record).unwrap();
        // Present, as a link, but its size is unreadable: the link names nothing.
        let sizeless = BuildVolumeId::mint();
        std::os::unix::fs::symlink(store.root.join("gone"), store.layout.image(&sizeless)).unwrap();
        store
            .layout
            .write_record(&sizeless, &BuildVolumeRecord::new(None, superseded("main")))
            .unwrap();
        let plan = plan(
            &store.layout,
            &store.layout.collection_ids().unwrap(),
            &Links::default(),
        )
        .unwrap();
        assert_eq!(
            plan.doomed,
            [Doomed {
                id: absent,
                reason: GcReason::UnlinkedBuildVolume,
                bytes: 0,
                seen: record,
                image_missing: true,
            }],
            "{plan:?}"
        );
        assert!(
            matches!(
                plan.deferred.as_slice(),
                [(id, Deferral::SizeUnreadable(_))] if *id == sizeless
            ),
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
        use crate::capabilities::BuildStatePath;
        use crate::storage::apfs::ApfsSubstrateConfig;
        use cowshed_apfs::SystemCommandRunner;
        use cowshed_os::namespace::HostNamespace;
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
                SystemCommandRunner::new(HostNamespace::Test(root.namespace())),
                ApfsSubstrateConfig::new(
                    HostNamespace::Test(root.namespace()),
                    &store,
                    root.path().join("checkout"),
                ),
            )
            .unwrap(),
        );
        let layout = BuildVolumeLayout::new(&project).unwrap();
        let volumes = BuildVolumes::new(
            Arc::clone(&host),
            layout.clone(),
            crate::project_retirement::acquire_project_reader(
                &store,
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap(),
        );
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
        let registration = Registration::new(
            BuildVolumeRecord::new(None, linked("main")),
            Registrant::Workspace {
                incarnation: main.incarnation.clone(),
            },
        );
        let mount = test_volume(&host, &layout, &live, 1, &registration);
        BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", ".nx/workspace-data").unwrap()],
            fingerprint: None,
        }
        .write(&mount)
        .unwrap();
        link::point(&main_checkout, &mount).unwrap();
        // Both refusals use the same live volume before the successful resize.
        let refused = volumes
            .resize(
                main.clone(),
                main_checkout.clone(),
                ImageCapacity::from_gibibytes(1),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code, crate::ErrorCode::Usage, "{refused}");
        let held = fs::File::create(mount.join("held")).unwrap();
        let refused = volumes
            .resize(
                main.clone(),
                main_checkout.clone(),
                ImageCapacity::from_gibibytes(2),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code, crate::ErrorCode::Conflict, "{refused}");
        assert_eq!(
            host.build_volume_capacity(&layout, &live).unwrap(),
            ImageCapacity::from_gibibytes(1),
            "refusals leave the original capacity"
        );
        drop(held);

        host.clone_build_volume(
            &layout,
            &live,
            &BuildVolumeId::mint(),
            &registered(BuildVolumeRecord::new(
                None,
                BuildVolumeRole::Seed {
                    target: main.name.clone(),
                    incarnation: main.incarnation.clone(),
                },
            )),
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
            .capture_fork_retired(
                main.clone(),
                main_checkout.clone(),
                topic.name.clone(),
                unlanded_aim(),
            )
            .await
            .expect("capture the fork from main's seed");
        let prepared = volumes
            .ready_fork(Ok(prepared), topic.clone())
            .await
            .expect("ready the fork's volume");
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
            let release = host.release_build_volume(&layout, &id).unwrap();
            assert!(
                matches!(release, BuildVolumeRelease::Deleted { .. }),
                "{release:?}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    enum FixtureRoot {
        Apfs(crate::scratch_apfs::ScratchRoot),
        Directory(crate::scratch_directory::ScratchRoot),
    }

    #[cfg(target_os = "macos")]
    impl FixtureRoot {
        fn path(&self) -> &Path {
            match self {
                Self::Apfs(root) => root.path(),
                Self::Directory(root) => root.path(),
            }
        }
    }

    /// A real store with one project's build volumes, using the scenario's storage backend.
    #[cfg(target_os = "macos")]
    struct Scratch<H: RuntimeStorageHost = Host> {
        root: FixtureRoot,
        host: Arc<H>,
        layout: BuildVolumeLayout,
        volumes: BuildVolumes<H>,
        mint: fn(&H, &BuildVolumeLayout, &BuildVolumeId, u64, &Registration) -> PathBuf,
    }

    #[cfg(target_os = "macos")]
    impl Scratch {
        fn new(name: &str) -> Self {
            Self::with_root(
                FixtureRoot::Apfs(
                    crate::scratch_apfs::ScratchRoot::new(name).expect("scratch APFS root"),
                ),
                |config| {
                    MacOsApfsExecutionHost::new(SystemCommandRunner::new(config.namespace), config)
                },
                test_volume,
            )
        }
    }

    #[cfg(target_os = "macos")]
    impl Scratch<crate::storage::directory::DirectoryExecutionHost> {
        fn directory(name: &str) -> Self {
            Self::with_root(
                FixtureRoot::Directory(
                    crate::scratch_directory::ScratchRoot::new(name)
                        .expect("scratch directory root"),
                ),
                crate::storage::directory::DirectoryExecutionHost::new,
                |host, layout, id, gib, registration| {
                    host.create_build_volume(
                        layout,
                        id,
                        ImageCapacity::from_gibibytes(gib),
                        registration,
                    )
                    .expect("create a directory-backed build volume")
                    .0
                },
            )
        }
    }

    #[cfg(target_os = "macos")]
    impl<H: RuntimeStorageHost> Scratch<H> {
        fn with_root(
            root: FixtureRoot,
            make_host: impl FnOnce(
                crate::storage::apfs::ApfsSubstrateConfig,
            ) -> std::result::Result<H, ApfsStorageError>,
            mint: fn(&H, &BuildVolumeLayout, &BuildVolumeId, u64, &Registration) -> PathBuf,
        ) -> Self {
            use crate::storage::apfs::ApfsSubstrateConfig;
            use cowshed_os::namespace::{HostNamespace, TestNamespace};
            let store = root.path().join("store");
            let namespace = match &root {
                FixtureRoot::Apfs(apfs) => HostNamespace::Test(apfs.namespace()),
                // A directory root names no namespace of its own, so this run mints one.
                FixtureRoot::Directory(_) => {
                    HostNamespace::Test(TestNamespace::mint().expect("a test namespace"))
                }
            };
            fs::create_dir_all(&store).unwrap();
            let project = ProjectPaths::with_mount_root(
                &store,
                root.path().join("mnt"),
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap();
            let host = Arc::new(
                make_host(ApfsSubstrateConfig::new(
                    namespace,
                    &store,
                    root.path().join("checkout"),
                ))
                .unwrap(),
            );
            let layout = BuildVolumeLayout::new(&project).unwrap();
            let volumes = BuildVolumes::new(
                Arc::clone(&host),
                layout.clone(),
                crate::project_retirement::acquire_project_reader(
                    &store,
                    &RepoId::parse("acme/widget").unwrap(),
                )
                .unwrap(),
            );
            Self {
                root,
                host,
                layout,
                volumes,
                mint,
            }
        }

        /// `name`'s checkout, linked to a new live volume registered as its own before its image
        /// exists and published once it is minted.
        fn linked_checkout(&self, name: &str, gib: u64) -> (PathBuf, BuildVolumeId, PathBuf) {
            let checkout = self.root.path().join(name);
            fs::create_dir_all(checkout.join(".cowshed")).unwrap();
            let id = BuildVolumeId::mint();
            let registration = registered(BuildVolumeRecord::new(None, linked(name)));
            let creating = self.layout.register(&id, &registration).unwrap();
            let mount = (self.mint)(&self.host, &self.layout, &id, gib, &registration);
            BuildVolumeState::default().write(&mount).unwrap();
            self.layout.publish(&id, &registration).unwrap();
            drop(creating);
            link::point(&checkout, &mount).unwrap();
            (checkout, id, mount)
        }

        /// The volume `checkout` links, held for a land.
        fn landing(&self, checkout: &Path) -> Landing {
            self.volumes
                .hold_landing(checkout)
                .unwrap()
                .expect("the landing checkout links a volume")
        }

        /// The stopped supervisor's actual admission lease, kept by Quiet through capture.
        async fn stopped(&self, checkout: &Path) -> super::super::supervisor_socket::BoundSocket {
            let workspace =
                WorkspaceName::new(checkout.file_name().unwrap().to_str().unwrap()).unwrap();
            let socket = super::super::supervisor_socket::socket_path(
                &self.root.path().join("store"),
                &RepoId::parse("acme/widget").unwrap(),
                &workspace,
            );
            super::super::supervisor_socket::bind(&socket)
                .await
                .expect("bind the stopped workspace's supervisor socket")
        }

        /// Where `name`'s image lock is: the one its forks, reseeds and lands take.
        fn image_lock(&self, name: &WorkspaceName) -> PathBuf {
            crate::storage::StorageLayout::with_mount_root(
                self.root.path().join("store"),
                self.root.path().join("mnt"),
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap()
            .canonical_image(name)
            .unwrap()
            .lock()
            .to_owned()
        }

        /// `target`'s image lock, taken as its land takes it, when nothing else holds it.
        async fn lock(&self, target: &Owner) -> TargetLock<H> {
            match self
                .volumes
                .try_lock_target(
                    target.clone(),
                    self.image_lock(&target.name),
                    LockHolding::of(LockOperation::Land, &target.name),
                )
                .await
                .unwrap()
            {
                Ok(locked) => locked,
                Err(_) => panic!("nothing else holds {}'s image lock", target.name),
            }
        }

        fn release_all(&self) {
            for id in self.layout.list().unwrap() {
                let release = self.host.release_build_volume(&self.layout, &id).unwrap();
                assert!(
                    matches!(release, BuildVolumeRelease::Deleted { .. }),
                    "{release:?}"
                );
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn directory_cancelled_build_work_keeps_project_retirement_fenced_until_it_ends() {
        use crate::project_retirement::{RetirementIntent, acquire_collector};
        let Scratch { root, volumes, .. } = Scratch::directory("retirement-build-owner");
        let store = root.path().join("store");
        let repo = RepoId::parse("acme/widget").unwrap();
        fs::create_dir_all(volumes.layout.project_root()).unwrap();
        fs::write(
            volumes
                .layout
                .project_root()
                .join(crate::repository::REPOSITORY_BINDING_FILE),
            b"retained fixture binding",
        )
        .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::channel();
            let caller = tokio::spawn(async move {
                volumes
                    .blocking(move |_, _| {
                        let _ = started.send(());
                        released
                            .recv()
                            .map_err(|error| CowshedError::internal(error.to_string()))?;
                        Ok(())
                    })
                    .await
            });
            ready.await.unwrap();
            RetirementIntent::authorize(&store, &repo).unwrap();
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            assert!(
                acquire_collector(&store, &repo).is_err(),
                "the caller is gone, but its blocking task still owns project storage"
            );
            release.send(()).unwrap();
        });
        // Owned runtime shutdown joins its blocking tasks: no sleeps or PID inference.
        drop(runtime);
        assert!(acquire_collector(&store, &repo).is_ok());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_nx_state_clear_refuses_running_work_then_empties_cache_and_task_data() {
        let scratch = Scratch::directory("nx-state-clear");
        let (checkout, live, mount) = scratch.linked_checkout("topic", 1);
        BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
            ],
            fingerprint: None,
        }
        .write(&mount)
        .unwrap();
        let cache = mount.join("nx/cache");
        let data = mount.join("nx/workspace-data");
        fs::create_dir_all(cache.join("terminalOutputs")).unwrap();
        fs::write(cache.join("terminalOutputs/101"), b"terminal output").unwrap();
        fs::create_dir_all(cache.join("101")).unwrap();
        fs::write(cache.join("101/out.js"), b"cached output").unwrap();
        fs::write(cache.join("run.json"), b"{}").unwrap();
        fs::create_dir_all(&data).unwrap();
        let database = data.join("T-v3.db");
        fs::write(&database, b"").unwrap();
        fs::write(data.join("project-graph.json"), b"{}").unwrap();
        let topic = owner("topic", '1');
        let image_lock = scratch.image_lock(&topic.name);
        fs::create_dir_all(image_lock.parent().unwrap()).unwrap();
        let clear = || {
            scratch
                .volumes
                .clear_nx_state(topic.name.clone(), checkout.clone(), image_lock.clone())
        };
        let refused = |refusal: CowshedError, named: &str| {
            assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
            assert!(refusal.message.contains(named), "{refusal}");
            assert!(refusal.message.contains("nothing was cleared"), "{refusal}");
        };

        // A job admitted on the volume, an Nx client with the task database open, and another
        // lifecycle operation each refuse by name, and nothing is deleted.
        let job = scratch.layout.admit(&live).unwrap();
        refused(clear().await.unwrap_err(), &std::process::id().to_string());
        drop(job);
        let open = fs::File::open(&database).unwrap();
        refused(clear().await.unwrap_err(), &database.display().to_string());
        drop(open);
        let locked = scratch.lock(&topic).await;
        refused(
            clear().await.unwrap_err(),
            &image_lock.display().to_string(),
        );
        drop(locked);
        assert!(cache.join("101/out.js").exists());
        assert!(database.exists());

        let cleared = clear().await.unwrap();
        assert_eq!(cleared.workspace, topic.name);
        assert_eq!(cleared.volume, live.to_string());
        assert_eq!(
            cleared.cache_entries, 3,
            "terminalOutputs, 101 and run.json"
        );
        assert_eq!(
            cleared.workspace_data_entries, 2,
            "the task database and the graph"
        );
        assert_eq!(fs::read_dir(&cache).unwrap().count(), 0);
        let left: Vec<_> = fs::read_dir(&data)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(left, ["T-v3.lock"], "only Nx's own open lock stays");
        assert_eq!(
            scratch.layout.linked(&checkout).unwrap(),
            Some(live.clone())
        );
        drop(scratch.layout.admit(&live).expect("the claim is let go"));
        scratch.release_all();
    }

    #[cfg(target_os = "macos")]
    fn reseed_workspace<H: RuntimeStorageHost>(
        scratch: &Scratch<H>,
        owner: Owner,
        checkout: PathBuf,
    ) -> ReseedWorkspace {
        let layout = crate::storage::StorageLayout::with_mount_root(
            scratch.root.path().join("store"),
            scratch.root.path().join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let image = layout.canonical_image(&owner.name).unwrap();
        fs::create_dir_all(image.image().parent().unwrap()).unwrap();
        fs::write(image.image(), b"source-image-identity").unwrap();
        ReseedWorkspace {
            intent_lease: layout
                .project()
                .sessions
                .join(format!("{}.intent.lock", owner.name)),
            owner,
            checkout,
            image,
        }
    }

    /// The same public contract runs on real directory payloads and native APFS images.
    #[cfg(target_os = "macos")]
    async fn reseed_contract<H: RuntimeStorageHost>(scratch: &Scratch<H>) -> (u64, u64) {
        use crate::git_preservation;
        let main = owner("main", '0');
        let topic = owner("topic", '1');
        let (main_checkout, main_live, main_mount) = scratch.linked_checkout("main", 1);
        fs::write(main_mount.join("from-seed"), b"landed incremental state").unwrap();
        let seed = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &seed,
                &registered(BuildVolumeRecord::new(
                    Some(GitOid::new("a".repeat(40)).unwrap()),
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        // Main has newer live work, deliberately absent from its stale frozen seed.
        fs::write(main_mount.join("never-refresh-this"), b"live work").unwrap();
        let seed_record = scratch.layout.read_record(&seed).unwrap();
        let seed_clock = fs::metadata(scratch.layout.image(&seed))
            .unwrap()
            .modified()
            .unwrap();
        let (checkout, old, old_mount) = scratch.linked_checkout("topic", 1);
        fs::write(old_mount.join("accumulated"), vec![7_u8; 262_144]).unwrap();
        let own_seed = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &old,
                &own_seed,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: topic.name.clone(),
                        incarnation: topic.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        let own_seed_record = scratch.layout.read_record(&own_seed).unwrap();
        let own_seed_clock = fs::metadata(scratch.layout.image(&own_seed))
            .unwrap()
            .modified()
            .unwrap();
        commit(
            &checkout,
            &[(".gitignore", ".cowshed/\n"), ("source", "committed\n")],
        );
        fs::write(checkout.join("source"), b"dirty tracked source\n").unwrap();
        fs::write(checkout.join("untracked"), b"untracked source\n").unwrap();
        let before = git_preservation::capture(&checkout, &checkout).unwrap();
        let index = fs::read(checkout.join(".git/index")).unwrap();
        let workspace = reseed_workspace(scratch, topic, checkout.clone());
        let from = reseed_workspace(scratch, main, main_checkout);
        let preview = scratch
            .volumes
            .preview_reseed(workspace.clone(), from.clone())
            .await
            .unwrap();
        let report = preview.preview();
        assert!(report.dry_run);
        assert_eq!(report.released_bytes, 0);
        assert!(report.previous_bytes > 0);
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old.clone()));

        // Both an admitted job's shared hold and a land/capture's exclusive claim refuse.
        let job = scratch.layout.admit(&old).unwrap();
        let refusal = scratch
            .volumes
            .execute_reseed(preview.clone())
            .await
            .unwrap_err();
        assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
        assert!(
            refusal.message.contains(&std::process::id().to_string()),
            "{refusal}"
        );
        assert!(refusal.message.contains(".dmg.hold"), "{refusal}");
        assert!(
            refusal.message.contains(" ("),
            "holder command is named: {refusal}"
        );
        drop(job);
        let land = scratch.layout.claim_release(&old).unwrap().unwrap();
        let refusal = scratch
            .volumes
            .execute_reseed(preview.clone())
            .await
            .unwrap_err();
        assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
        assert!(
            refusal.message.contains(&std::process::id().to_string()),
            "{refusal}"
        );
        drop(land);
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old.clone()));
        git_preservation::verify(&checkout, &checkout, &before).unwrap();

        // A filesystem reader is not forcibly evicted either.
        let open = fs::File::open(old_mount.join("accumulated")).unwrap();
        let refusal = scratch.volumes.execute_reseed(preview).await.unwrap_err();
        assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
        assert!(
            refusal.message.contains(&std::process::id().to_string()),
            "{refusal}"
        );
        drop(open);
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old.clone()));
        assert!(old_mount.join("accumulated").exists());

        let plan = scratch
            .volumes
            .preview_reseed(workspace, from)
            .await
            .unwrap();
        let expected_deleted = plan.preview().previous_bytes;
        let completed = scratch.volumes.execute_reseed(plan).await.unwrap();
        let replacement =
            BuildVolumeId::parse(completed.replacement_volume.as_ref().unwrap()).unwrap();
        assert_eq!(completed.released_bytes, completed.previous_bytes);
        assert!(completed.released_bytes > 0);
        assert!(!completed.dry_run);
        assert_eq!(
            scratch.layout.linked(&checkout).unwrap(),
            Some(replacement.clone())
        );
        assert!(!scratch.layout.image(&old).exists());
        assert_eq!(
            fs::read(scratch.layout.mount(&replacement).join("from-seed")).unwrap(),
            b"landed incremental state"
        );
        assert!(
            !scratch
                .layout
                .mount(&replacement)
                .join("accumulated")
                .exists()
        );
        assert!(
            !scratch
                .layout
                .mount(&replacement)
                .join("never-refresh-this")
                .exists()
        );
        assert_eq!(scratch.layout.read_record(&seed).unwrap(), seed_record);
        assert_eq!(
            fs::metadata(scratch.layout.image(&seed))
                .unwrap()
                .modified()
                .unwrap(),
            seed_clock,
            "reseed neither mounts nor refreshes its source seed"
        );
        assert_eq!(
            scratch.layout.read_record(&own_seed).unwrap(),
            own_seed_record
        );
        assert_eq!(
            fs::metadata(scratch.layout.image(&own_seed))
                .unwrap()
                .modified()
                .unwrap(),
            own_seed_clock
        );
        git_preservation::verify(&checkout, &checkout, &before).unwrap();
        assert_eq!(fs::read(checkout.join(".git/index")).unwrap(), index);
        assert_eq!(
            fs::read(checkout.join("source")).unwrap(),
            b"dirty tracked source\n"
        );
        assert_eq!(
            fs::read(checkout.join("untracked")).unwrap(),
            b"untracked source\n"
        );
        let log =
            fs::read_to_string(scratch.layout.project_root().join("deletion-log.jsonl")).unwrap();
        assert!(
            log.contains("release-build-volume") && log.contains(old.as_str()),
            "{log}"
        );
        scratch.release_all();
        (expected_deleted, completed.released_bytes)
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_reseed_refuses_named_holders_reclaims_and_preserves_git() {
        let (expected, deleted) =
            reseed_contract(&Scratch::directory("build-reseed-contract")).await;
        assert_eq!(
            deleted, expected,
            "execution reports exactly the observed allocated image bytes"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_reseed_refuses_named_holders_reclaims_and_preserves_git() {
        reseed_contract(&Scratch::new("build-reseed-contract")).await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_reseed_refuses_absent_seed_forming_intent_and_stale_plan() {
        use crate::storage::recovery::{
            LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal,
        };
        let scratch = Scratch::directory("build-reseed-refusals");
        let (checkout, old, _) = scratch.linked_checkout("topic", 1);
        let (main_checkout, main_live, _) = scratch.linked_checkout("main", 1);
        let workspace = reseed_workspace(&scratch, owner("topic", '1'), checkout.clone());
        let from = reseed_workspace(&scratch, owner("main", '0'), main_checkout);
        let error = scratch
            .volumes
            .preview_reseed(workspace.clone(), from.clone())
            .await
            .unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::NotFound, "{error}");
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old.clone()));
        let seed = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &seed,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: from.owner.name.clone(),
                        incarnation: from.owner.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        let plan = scratch
            .volumes
            .preview_reseed(workspace.clone(), from.clone())
            .await
            .unwrap();
        let journal_path = scratch.layout.project_root().join(LIFECYCLE_INTENTS_FILE);
        let mut journal = LifecycleIntentJournal::default();
        journal.begin(LifecycleIntent::Fork {
            source: from.owner.name.clone(),
            destination: workspace.owner.name.clone(),
            rename: false,
        });
        journal.mark_mutating(&workspace.owner.name).unwrap();
        crate::metadata::write_json(&journal_path, &journal).unwrap();
        let error = scratch.volumes.execute_reseed(plan).await.unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Conflict, "{error}");
        assert!(
            error.message.contains("lifecycle journal entry for topic"),
            "{error}"
        );
        assert!(error.message.contains("Fork"), "{error}");
        journal.clear(&workspace.owner.name);
        crate::metadata::write_json(&journal_path, &journal).unwrap();

        let plan = scratch
            .volumes
            .preview_reseed(workspace.clone(), from.clone())
            .await
            .unwrap();
        let mut record = scratch.layout.read_record(&seed).unwrap();
        record.tree = Some(GitOid::new("b".repeat(40)).unwrap());
        scratch.layout.write_record(&seed, &record).unwrap();
        let error = scratch.volumes.execute_reseed(plan).await.unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Conflict, "{error}");
        assert!(error.message.contains("stale"), "{error}");
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old.clone()));
        let plan = scratch
            .volumes
            .preview_reseed(workspace, from.clone())
            .await
            .unwrap();
        let locked = scratch
            .host
            .lock_images(
                &[from.image.lock().to_owned()],
                LockMode::Try,
                &LockHolding::of(LockOperation::Fork, &from.owner.name),
            )
            .unwrap()
            .unwrap();
        let error = scratch.volumes.execute_reseed(plan).await.unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Conflict, "{error}");
        assert!(
            error.message.contains(&std::process::id().to_string()),
            "{error}"
        );
        assert!(error.message.contains("lifecycle/image lock"), "{error}");
        drop(locked);
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old));
        scratch.release_all();
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_owned_index_refresh_relinks_legacy_directories_without_copying() {
        for changed in [false, true] {
            let scratch = Scratch::directory("index-legacy");
            tracked_checkout(scratch.root.path(), "main", AT_THE_CAP, &["source.rs"]);
            let (checkout, live, mount) = scratch.linked_checkout("main", 1);
            let indexes = [
                BuildStatePath::new(".codegraph", "codegraph").unwrap(),
                BuildStatePath::new("packages/app/.codegraph", "packages/app/codegraph").unwrap(),
            ];
            let mut paths = vec![
                BuildStatePath::new("target", "target").unwrap(),
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
            ];
            paths.extend(indexes.iter().cloned());
            let state = BuildVolumeState {
                paths,
                fingerprint: Some("fixture".into()),
            };
            let mut held = state.clone();
            if changed {
                held.paths.retain(|path| !indexes.contains(path));
                held.fingerprint = Some("source-side-cutover".into());
            }
            held.write(&mount).unwrap();
            link::link_paths(&checkout, &mount, &state.paths).unwrap();
            for path in &state.paths {
                fs::write(
                    mount.join(path.volume.as_path()).join("warm"),
                    b"volume bytes",
                )
                .unwrap();
            }
            for path in &indexes {
                let at = checkout.join(path.checkout.as_path());
                fs::remove_file(&at).unwrap();
                fs::create_dir(&at).unwrap();
                fs::write(at.join("legacy"), b"discard, never copy").unwrap();
            }
            let refreshed = scratch
                .volumes
                .refresh(
                    owner("main", '0'),
                    checkout.clone(),
                    if changed {
                        discovered(state.paths.clone())
                    } else {
                        Discovered::Unchanged
                    },
                    Lent::Nothing,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(refreshed.volume, Some(live));
            let expected_added = if changed {
                indexes
                    .iter()
                    .map(|path| path.checkout.as_path().to_owned())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            assert_eq!(refreshed.added, expected_added);
            assert_eq!(
                refreshed
                    .displaced
                    .iter()
                    .map(|finding| finding.path.clone())
                    .collect::<Vec<_>>(),
                indexes
                    .iter()
                    .map(|path| path.checkout.as_path().to_owned())
                    .collect::<Vec<_>>()
            );
            assert_eq!(BuildVolumeState::read(&mount).unwrap(), state);
            for path in &state.paths {
                let at = checkout.join(path.checkout.as_path());
                assert_eq!(
                    fs::read_link(&at).unwrap(),
                    link::relative_target(path.checkout.as_path(), path.volume.as_path())
                );
                assert_eq!(fs::read(at.join("warm")).unwrap(), b"volume bytes");
                assert!(!at.join("legacy").exists());
                assert!(!mount.join(path.volume.as_path()).join("legacy").exists());
            }
            crate::build_volume::discard::finish(&checkout, |_| {}).unwrap();
            assert!(
                crate::build_volume::discard::pending(&checkout)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(fs::read(checkout.join("source.rs")).unwrap(), b"{}\n");
            assert!(
                fs::read_dir(checkout.join(".cowshed"))
                    .unwrap()
                    .all(|entry| !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("index-cutover-"))
            );
            scratch.release_all();
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_owned_index_refresh_keeps_exact_links_and_volume_bytes_untouched() {
        use std::os::unix::fs::MetadataExt as _;
        for changed in [false, true] {
            let scratch = Scratch::directory("index-exact");
            let (checkout, live, mount) = scratch.linked_checkout("main", 1);
            let state = BuildVolumeState {
                paths: vec![
                    BuildStatePath::new(".codegraph", "codegraph").unwrap(),
                    BuildStatePath::new("packages/app/.codegraph", "packages/app/codegraph")
                        .unwrap(),
                    BuildStatePath::new("target", "target").unwrap(),
                    BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                ],
                fingerprint: Some("fixture".into()),
            };
            state.write(&mount).unwrap();
            link::link_paths(&checkout, &mount, &state.paths).unwrap();
            let before = state
                .paths
                .iter()
                .map(|path| {
                    let at = checkout.join(path.checkout.as_path());
                    fs::write(at.join("warm"), b"volume bytes").unwrap();
                    let metadata = fs::symlink_metadata(&at).unwrap();
                    (
                        metadata.ino(),
                        metadata.modified().unwrap(),
                        fs::metadata(at.join("warm")).unwrap().modified().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let refreshed = scratch
                .volumes
                .refresh(
                    owner("main", '0'),
                    checkout.clone(),
                    if changed {
                        discovered(state.paths.clone())
                    } else {
                        Discovered::Unchanged
                    },
                    Lent::Nothing,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(refreshed.volume, Some(live));
            assert!(refreshed.displaced.is_empty());
            assert!(refreshed.added.is_empty());
            assert_eq!(BuildVolumeState::read(&mount).unwrap(), state);
            for (path, (inode, written, bytes_written)) in state.paths.iter().zip(before) {
                let at = checkout.join(path.checkout.as_path());
                let metadata = fs::symlink_metadata(&at).unwrap();
                assert_eq!(
                    fs::read_link(&at).unwrap(),
                    link::relative_target(path.checkout.as_path(), path.volume.as_path())
                );
                assert_eq!(metadata.ino(), inode);
                assert_eq!(metadata.modified().unwrap(), written);
                assert_eq!(fs::read(at.join("warm")).unwrap(), b"volume bytes");
                assert_eq!(
                    fs::metadata(at.join("warm")).unwrap().modified().unwrap(),
                    bytes_written
                );
            }
            assert!(
                fs::read_dir(checkout.join(".cowshed"))
                    .unwrap()
                    .all(|entry| !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("index-cutover-"))
            );
            scratch.release_all();
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_owned_index_refresh_preserves_a_busy_holder_and_fixed_link() {
        use crate::build_volume::sqlite::Connection;
        use std::os::unix::fs::MetadataExt as _;
        let scratch = Scratch::new("index-refresh");
        let (checkout, live, mount) = scratch.linked_checkout("main", 1);
        let index = BuildStatePath::new(".codegraph", "codegraph").unwrap();
        let state = BuildVolumeState {
            paths: vec![index.clone()],
            fingerprint: Some("fixture".into()),
        };
        state.write(&mount).unwrap();
        link::link_paths(&checkout, &mount, &state.paths).unwrap();
        let at = checkout.join(".codegraph");
        let before = fs::symlink_metadata(&at).unwrap();
        let database = at.join("codegraph.db");
        fs::write(&database, b"").unwrap();
        let holder = Connection::open(&database).unwrap();
        holder.execute("PRAGMA journal_mode=WAL", &[]).unwrap();
        holder
            .execute("CREATE TABLE evidence (value TEXT)", &[])
            .unwrap();
        holder
            .execute("INSERT INTO evidence VALUES ('before')", &[])
            .unwrap();
        let refreshed = scratch
            .volumes
            .refresh(
                owner("main", '0'),
                checkout.clone(),
                Discovered::Unchanged,
                Lent::Nothing,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refreshed.volume, Some(live.clone()));
        assert!(refreshed.displaced.is_empty());
        assert_eq!(BuildVolumeState::read(&mount).unwrap(), state);
        assert_eq!(
            fs::read_link(&at).unwrap(),
            link::relative_target(index.checkout.as_path(), index.volume.as_path())
        );
        assert_eq!(fs::symlink_metadata(&at).unwrap().ino(), before.ino());
        assert_eq!(
            fs::symlink_metadata(&at).unwrap().modified().unwrap(),
            before.modified().unwrap()
        );
        holder
            .execute("INSERT INTO evidence VALUES ('after refresh')", &[])
            .unwrap();
        let mut rows = holder.prepare("SELECT count(*) FROM evidence").unwrap();
        assert!(rows.step().unwrap());
        assert_eq!(rows.integer(0), 2);
        drop(rows);
        drop(holder);
        assert_eq!(
            fs::canonicalize(&database).unwrap(),
            mount.join("codegraph/codegraph.db")
        );
        assert_eq!(scratch.layout.list().unwrap(), [live]);
        scratch.release_all();
    }

    /// Land and direct capture both keep exempt readers on the mounted source. The seed holds
    /// exact warm bytes and SQLite rows, including committed WAL state, not a source-side copy.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_seed_capture_preserves_open_contributed_caches() {
        use crate::build_volume::sqlite::Connection;
        use std::io::Write as _;

        let scratch = Scratch::new("seed-open-caches");
        let (checkout, live, mount) = scratch.linked_checkout("topic", 1);
        let target_checkout = scratch.root.path().join("main");
        fs::create_dir_all(target_checkout.join(".cowshed")).unwrap();
        let state = BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".codegraph", "codegraph").unwrap(),
                BuildStatePath::new(".lint-cache", "lint-cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
                BuildStatePath::new("target", "target").unwrap(),
            ],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        link::link_paths(&checkout, &mount, &state.paths).unwrap();
        fs::create_dir_all(mount.join("target/debug")).unwrap();
        fs::write(mount.join("target/debug/.cargo-lock"), b"").unwrap();
        fs::write(mount.join("nx/workspace-data/T-v3.db"), b"").unwrap();
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&checkout),
                scratch.stopped(&checkout).await,
                target_checkout,
            )
            .await
            .unwrap()
            .expect("Nx and Cargo are idle");
        let target = owner("main", '0');
        let locked = scratch.lock(&target).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            retired_or_skipped(&scratch.volumes, frozen).await,
            Ok(Froze::Frozen)
        );
        let mut previous = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap()
            .unwrap()
            .0;
        let database = checkout.join(".codegraph/codegraph.db");
        fs::write(&database, b"").unwrap();
        let holder = Connection::open(&database).unwrap();
        holder.execute("PRAGMA journal_mode=WAL", &[]).unwrap();
        holder.execute("PRAGMA wal_autocheckpoint=0", &[]).unwrap();
        holder
            .execute("CREATE TABLE evidence (sequence INTEGER, value TEXT)", &[])
            .unwrap();
        holder
            .execute(
                "INSERT INTO evidence VALUES (1, 'warm-one'), (2, 'warm-two')",
                &[],
            )
            .unwrap();
        holder.execute("BEGIN", &[]).unwrap();
        let mut reader = holder
            .prepare("SELECT sequence, value FROM evidence ORDER BY sequence")
            .unwrap();
        assert!(reader.step().unwrap());
        assert_eq!((reader.integer(0), reader.text(1)), (1, "warm-one".into()));
        let marker = b"warm unrelated lint cache\n";
        let cache_path = checkout.join(".lint-cache/warm");
        let mut cache = fs::File::create(&cache_path).unwrap();
        cache.write_all(marker).unwrap();
        cache.sync_all().unwrap();

        let mut quiet = Some(quiet);
        for digit in ['b', 'c'] {
            let tree = GitOid::new(digit.to_string().repeat(40)).unwrap();
            if let Some(quiet) = quiet.take() {
                let frozen = scratch
                    .volumes
                    .freeze_seed(&locked, &quiet, tree.clone())
                    .await
                    .unwrap();
                assert_eq!(
                    retired_or_skipped(&scratch.volumes, frozen).await,
                    Ok(Froze::Frozen),
                    "the landing capture admits exempt open caches"
                );
            } else {
                let (_, superseded) = capture(
                    &*scratch.host,
                    &scratch.layout,
                    &target,
                    &live,
                    Some(tree.clone()),
                )
                .unwrap()
                .expect("direct capture admits the same exempt open caches");
                retire_seeds(&*scratch.host, &scratch.layout, superseded).unwrap();
            }
            let (seed, record) = scratch
                .layout
                .seed_of(&target.name, &target.incarnation)
                .unwrap()
                .expect("the replacement seed is published");
            assert_ne!(seed, previous);
            assert_eq!(record.tree, Some(tree.clone()));
            assert!(!scratch.layout.image(&previous).exists());
            assert_eq!(
                seeds_of(&scratch.layout, &target).unwrap().as_slice(),
                std::slice::from_ref(&seed)
            );
            let fork = BuildVolumeId::mint();
            scratch
                .host
                .clone_build_volume(
                    &scratch.layout,
                    &seed,
                    &fork,
                    &registered(BuildVolumeRecord::new(Some(tree), linked("fork"))),
                )
                .unwrap();
            let fork_mount = scratch
                .host
                .mount_build_volume(&scratch.layout, &fork)
                .unwrap();
            assert_eq!(
                fs::read(fork_mount.join("lint-cache/warm")).unwrap(),
                marker
            );
            let captured = Connection::open(&fork_mount.join("codegraph/codegraph.db")).unwrap();
            let mut rows = captured
                .prepare("SELECT sequence, value FROM evidence ORDER BY sequence")
                .unwrap();
            let mut values = Vec::new();
            while rows.step().unwrap() {
                values.push((rows.integer(0), rows.text(1)));
            }
            assert_eq!(
                values,
                [(1, "warm-one".to_owned()), (2, "warm-two".to_owned())]
            );
            let mut live_rows = holder.prepare("SELECT count(*) FROM evidence").unwrap();
            assert!(live_rows.step().unwrap());
            assert_eq!(
                live_rows.integer(0),
                2,
                "the original connection stays live"
            );
            previous = seed;
        }
        assert!(reader.step().unwrap(), "the original read cursor survives");
        assert_eq!((reader.integer(0), reader.text(1)), (2, "warm-two".into()));
        assert!(!reader.step().unwrap());
        drop(reader);
        holder.execute("COMMIT", &[]).unwrap();
        holder
            .execute("INSERT INTO evidence VALUES (3, 'after capture')", &[])
            .unwrap();
        let mut rows = holder.prepare("SELECT count(*) FROM evidence").unwrap();
        assert!(rows.step().unwrap());
        assert_eq!(rows.integer(0), 3);
        cache
            .write_all(b"still open after both captures\n")
            .unwrap();
        assert_eq!(
            fs::read(&cache_path).unwrap(),
            b"warm unrelated lint cache\nstill open after both captures\n"
        );
        assert_eq!(
            fs::canonicalize(&database).unwrap(),
            mount.join("codegraph/codegraph.db")
        );
        drop(rows);
        drop(holder);
        drop(cache);
        drop(locked);
        scratch.release_all();
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_gc_collects_an_imageless_seed_sidecar_and_rechecks_its_image() {
        let scratch = Scratch::new("seed-sidecar-gc");
        let seed = BuildVolumeId::mint();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: WorkspaceName::main(),
                incarnation: owner("main", '0').incarnation,
            },
        );
        fs::create_dir_all(scratch.layout.images()).unwrap();
        scratch.layout.write_record(&seed, &record).unwrap();
        assert!(
            scratch.layout.list().unwrap().is_empty(),
            "not a usable seed"
        );
        let ids = scratch.layout.collection_ids().unwrap();
        assert_eq!(ids, std::slice::from_ref(&seed));
        let links = Links::default();
        let preview = plan(&scratch.layout, &ids, &links).unwrap();
        assert_eq!(preview.doomed.len(), 1);
        assert_eq!(preview.doomed[0].bytes, 0);
        fs::write(scratch.layout.image(&seed), "new image").unwrap();
        assert_eq!(
            still_doomed(&scratch.layout, &links, &preview.doomed[0]).unwrap(),
            Some(Deferral::Changed),
            "an image appearing after planning stays"
        );
        fs::remove_file(scratch.layout.image(&seed)).unwrap();
        let dry = scratch
            .volumes
            .collect(ids.clone(), links.clone(), true)
            .await
            .unwrap();
        assert_eq!(dry.candidates.len(), 1);
        assert!(
            scratch.layout.record(&seed).exists(),
            "preview does not delete"
        );
        let collected = scratch.volumes.collect(ids, links, false).await.unwrap();
        assert_eq!(collected.reclaimed, 1);
        assert_eq!(collected.released_bytes, 0);
        assert!(!scratch.layout.record(&seed).exists());
        let log = fs::read_to_string(
            scratch
                .layout
                .project_root()
                .join(crate::storage::deletion_log::DELETION_LOG_FILE),
        )
        .unwrap();
        assert!(
            log.contains("release-imageless-build-volume")
                && log.contains(&*scratch.layout.record(&seed).to_string_lossy()),
            "the record's retirement is journaled as one whose image was gone: {log}"
        );
        scratch.release_all();
    }

    #[cfg(target_os = "macos")]
    struct CargoWitness {
        /// The build script's independently reported parent, checked against the kernel.
        pid: libc::pid_t,
    }

    /// A real Cargo build stopped inside its build script, after Cargo has taken the profile's
    /// shared `.cargo-lock`. Socket readiness, not a delay, is the proof that it is building.
    #[cfg(target_os = "macos")]
    async fn running_cargo(
        source: &Path,
        mount: &Path,
        profile: &str,
    ) -> (tokio::process::Child, tokio::net::UnixStream, CargoWitness) {
        use cowshed_os::fork_lock::Spawn as _;
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;
        use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _};

        fs::create_dir_all(source.join("src")).unwrap();
        fs::write(
            source.join("Cargo.toml"),
            "[package]\nname = \"capture-writer-fixture\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
             [workspace]\n[profile.guard]\ninherits = \"dev\"\n",
        )
        .unwrap();
        fs::write(source.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").unwrap();
        fs::write(
            source.join("build.rs"),
            r#"use std::io::{Read, Write};
unsafe extern "C" { fn getppid() -> i32; }
fn main() {
    let path = std::env::var_os("CAPTURE_WRITER_SOCKET").unwrap();
    let mut stream = std::os::unix::net::UnixStream::connect(path).unwrap();
    // SAFETY: getppid has no preconditions and cannot fail.
    let cargo = unsafe { getppid() };
    writeln!(stream, "{cargo}").unwrap();
    for name in ["CARGO", "CARGO_MAKEFLAGS", "BOUNDED_EXEC_CPU_TOKENS"] {
        writeln!(stream, "{}", std::env::var(name).unwrap_or_default()).unwrap();
    }
    stream.write_all(b"R").unwrap();
    stream.read_exact(&mut [0]).unwrap();
}
"#,
        )
        .unwrap();
        let socket = source.join("writer.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let mut child = tokio::process::Command::new("cargo")
            .args(["build", "--offline", "--profile", profile])
            .current_dir(source)
            .env("CARGO_TARGET_DIR", mount.join("target"))
            .env("CAPTURE_WRITER_SOCKET", &socket)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("run the real Cargo build");
        let (stream, _) = tokio::select! {
            connected = listener.accept() => connected.expect("Cargo build script connects"),
            status = child.wait_bounded() => {
                let output = child.wait_with_output_bounded().await.expect("Cargo diagnostics");
                panic!("Cargo exited before its build script was ready: {status:?}; {output:?}");
            }
        };
        let mut stream = tokio::io::BufReader::new(stream);
        let mut fields = [String::new(), String::new(), String::new(), String::new()];
        for field in &mut fields {
            stream.read_line(field).await.unwrap();
            assert_eq!(
                field.pop(),
                Some('\n'),
                "the build-script witness is complete"
            );
        }
        let mut ready = [0];
        stream.read_exact(&mut ready).await.unwrap();
        assert_eq!(ready, *b"R");
        let [pid, executable, makeflags, tokens] = fields;
        let pid: libc::pid_t = pid.parse().expect("the build script's actual parent PID");
        assert!(pid > 0, "the actual Cargo PID is positive");
        let arguments = cowshed_os::process::process_arguments(pid)
            .expect("the kernel names the build script's live Cargo parent");
        let kernel_executable = PathBuf::from(OsString::from_vec(arguments.executable));
        let cargo = PathBuf::from(executable)
            .canonicalize()
            .expect("the build script's CARGO");
        assert_eq!(kernel_executable.canonicalize().unwrap(), cargo);
        assert_eq!(cargo.file_name().unwrap(), "cargo");
        assert!(
            arguments.argv.windows(4).any(|args| {
                args[0] == b"build"
                    && args[1] == b"--offline"
                    && args[2] == b"--profile"
                    && args[3] == profile.as_bytes()
            }),
            "the kernel names the actual Cargo build: {:?}",
            arguments.argv
        );
        let started_pid = child.id().expect("the entry Cargo process is still live");
        let parent = tokio::process::Command::new("/bin/ps")
            .args(["-o", "ppid=,pgid=", "-p", &pid.to_string()])
            .output_bounded()
            .await
            .expect("the actual Cargo kernel parent and process group");
        assert!(parent.status.success(), "{parent:?}");
        let lineage = String::from_utf8(parent.stdout).unwrap();
        let mut lineage = lineage.split_whitespace();
        let parent_pid: u32 = lineage.next().expect("the kernel parent").parse().unwrap();
        let group: u32 = lineage
            .next()
            .expect("the kernel process group")
            .parse()
            .unwrap();
        assert!(group > 0);
        assert!(lineage.next().is_none(), "exactly one kernel process row");
        if u32::try_from(pid) != Ok(started_pid) {
            assert_eq!(
                parent_pid, started_pid,
                "the real Cargo is the entry process's child, not an unrelated holder"
            );
        }
        let fifo = makeflags
            .split_whitespace()
            .find_map(|flag| flag.strip_prefix("--jobserver-auth=fifo:"))
            .map(PathBuf::from);
        // Cargo makes no jobserver of its own that it names: a fifo here is one a holder
        // configured, a real kernel FIFO only its user can open.
        if let Some(fifo) = &fifo {
            let metadata = fs::metadata(fifo).expect("the actual Cargo's live jobserver fifo");
            assert!(
                metadata.file_type().is_fifo(),
                "{} is a kernel FIFO",
                fifo.display()
            );
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
        cowshed_os::eprintln!(
            "Cargo fixture witness: entry_pid={started_pid}, actual_cargo_pid={pid}, kernel_parent_pid={parent_pid}, kernel_process_group={group}, executable={}, makeflags={makeflags:?}, budget_tokens={tokens:?}",
            cargo.display()
        );
        (child, stream.into_inner(), CargoWitness { pid })
    }

    #[cfg(target_os = "macos")]
    async fn freeze_with_running_cargo(profile: &str, enumerate_before_start: bool) {
        use tokio::io::AsyncWriteExt as _;

        let scratch = Scratch::directory("seed-cargo");
        let (main_checkout, _, _) = scratch.linked_checkout("main", 1);
        let (topic_checkout, _, mount) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new("target", "target").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        fs::create_dir_all(mount.join("target/debug")).unwrap();
        fs::write(mount.join("target/debug/.cargo-lock"), b"").unwrap();
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout,
            )
            .await
            .unwrap()
            .expect("the supervised jobs and Nx state are quiet");
        let target = owner("main", '0');
        let locked = scratch.lock(&target).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
            .await
            .unwrap()
            .expect("the initial capture has no writer");
        assert_eq!(retired(&scratch.volumes, frozen).await, Froze::Frozen);
        let original_seed = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap()
            .unwrap()
            .0;
        let enumerated = enumerate_before_start.then(|| {
            cargo::hold(&mount, &state)
                .unwrap()
                .expect("no Cargo build has started yet")
        });
        let source = scratch.root.path().join("cargo-source");
        let (child, mut writer, witness) = running_cargo(&source, &mount, profile).await;
        let expected_lock = mount
            .join("target")
            .join(if profile == "dev" { "debug" } else { profile })
            .join(cargo::BUILD_LOCK);
        // Cargo can create a previously absent profile while all previously enumerated profile
        // locks are held. They are an admission check, not a fence for future directories.
        drop(enumerated);
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, GitOid::new("b".repeat(40)).unwrap())
            .await;
        writer.write_all(b"G").await.unwrap();
        drop(writer);
        let output = child.wait_with_output_bounded().await.unwrap();
        assert!(output.status.success(), "{output:?}");
        let kept_seed = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap()
            .unwrap()
            .0;
        drop(locked);
        drop(quiet);
        scratch.release_all();
        assert_eq!(
            kept_seed, original_seed,
            "a refusal must not replace the seed"
        );
        let Err(AdoptionSkip::LandingBuilding { lock, holders }) =
            frozen.expect("a running Cargo build is an admission skip")
        else {
            panic!("the real Cargo writer must be named");
        };
        assert_eq!(lock, expected_lock);
        assert!(
            holders.iter().any(|holder| {
                holder.pid == witness.pid
                    && holder.command.contains("cargo build --offline --profile")
            }),
            "the skip must identify the actual Cargo process: {holders:?}"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_seed_freeze_skips_a_real_cargo_build_after_quiesce() {
        freeze_with_running_cargo("dev", false).await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_seed_freeze_skips_a_new_cargo_profile_created_after_enumeration() {
        freeze_with_running_cargo("guard", true).await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_seed_freeze_admits_an_open_contributed_cache_after_quiesce() {
        let scratch = Scratch::directory("seed-open-cache");
        let (main_checkout, _, _) = scratch.linked_checkout("main", 1);
        let (topic_checkout, _, mount) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".lint-cache", "lint-cache").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        fs::create_dir_all(mount.join("lint-cache")).unwrap();
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout,
            )
            .await
            .unwrap()
            .expect("the landing volume was quiet");
        let target = owner("main", '0');
        let locked = scratch.lock(&target).await;
        let mut held = fs::File::create(mount.join("lint-cache/warm")).unwrap();
        std::io::Write::write_all(&mut held, b"warm").unwrap();
        let tree = GitOid::new("a".repeat(40)).unwrap();
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, tree.clone())
            .await;
        let seed = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap();
        let still_open = std::io::Write::write_all(&mut held, b" after capture");
        let contents = fs::read(mount.join("lint-cache/warm")).unwrap();
        drop(held);
        drop(quiet);
        drop(locked);
        scratch.release_all();
        assert!(matches!(frozen.unwrap(), Ok(Freeze::Frozen { .. })));
        assert_eq!(
            seed.expect("the exempt cache permits publication").1.tree,
            Some(tree)
        );
        still_open.expect("the cache descriptor survives capture");
        assert_eq!(contents, b"warm after capture");
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_capture_names_nx_open_lock_contention_for_land_and_reseed() {
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::BufRead as _;

        let scratch = Scratch::directory("seed-nx-opening");
        let (main_checkout, _, _) = scratch.linked_checkout("main", 1);
        let (checkout, live, mount) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        let data = mount.join("nx/workspace-data");
        fs::create_dir_all(&data).unwrap();
        let database = data.join("T-v3.db");
        fs::write(&database, b"").unwrap();
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&checkout),
                scratch.stopped(&checkout).await,
                main_checkout,
            )
            .await
            .unwrap()
            .expect("nothing has opened Nx");
        let target = owner("main", '0');
        let locked = scratch.lock(&target).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            retired_or_skipped(&scratch.volumes, frozen).await,
            Ok(Froze::Frozen)
        );
        let previous = seeds_of(&scratch.layout, &target).unwrap();
        let lock = database.with_extension("lock");
        // Stock Nx holds this flock while opening the database. The ready line proves the
        // real open lock is held, without pretending the database itself is already open.
        let mut opening = std::process::Command::new("/usr/bin/lockf")
            .arg("-k")
            .arg(&lock)
            .args(["/bin/sh", "-c", "echo held; read _"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = i32::try_from(opening.id()).unwrap();
        let mut ready = String::new();
        std::io::BufReader::new(opening.stdout.as_mut().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready, "held\n");
        let tree = GitOid::new("b".repeat(40)).unwrap();
        let landed = scratch
            .volumes
            .freeze_seed(&locked, &quiet, tree.clone())
            .await;
        drop(quiet);
        let direct = capture(&*scratch.host, &scratch.layout, &target, &live, Some(tree));
        let kept = seeds_of(&scratch.layout, &target).unwrap();
        drop(opening.stdin.take());
        opening.wait_bounded().unwrap();
        drop(locked);
        scratch.release_all();
        assert_eq!(kept, previous, "neither refusal replaces the seed");
        let Err(AdoptionSkip::LandingOpening {
            database: landing_database,
            holders: landing_holders,
        }) = landed.unwrap()
        else {
            panic!("land names the contended Nx open lock as landingOpening");
        };
        let Err(CaptureSkip::Opening {
            database: direct_database,
            holders: direct_holders,
        }) = direct.unwrap()
        else {
            panic!("direct capture names the contended Nx open lock as Opening");
        };
        for (named, holders) in [
            (landing_database, landing_holders),
            (direct_database, direct_holders),
        ] {
            assert_eq!(named, database);
            assert!(
                holders.iter().any(|holder| {
                    holder.pid == pid && holder.command.starts_with("/usr/bin/lockf")
                }),
                "the refusal names the actual opener: {holders:?}"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn directory_capture_refuses_an_open_nx_database_without_replacing_the_seed() {
        use crate::build_volume::sqlite::Connection;

        let scratch = Scratch::directory("capture-nx-held");
        let (_, live, mount) = scratch.linked_checkout("main", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        fs::create_dir_all(mount.join("nx/workspace-data")).unwrap();
        let database = mount.join("nx/workspace-data/T-v3.db");
        fs::write(&database, b"").unwrap();
        let target = owner("main", '0');
        let (_, superseded) = capture(&*scratch.host, &scratch.layout, &target, &live, None)
            .unwrap()
            .expect("the initial capture has no holder");
        retire_seeds(&*scratch.host, &scratch.layout, superseded).unwrap();
        let previous = seeds_of(&scratch.layout, &target).unwrap();
        let holder = Connection::open(&database).unwrap();
        holder
            .execute("CREATE TABLE evidence (value TEXT)", &[])
            .unwrap();
        let captured = capture(
            &*scratch.host,
            &scratch.layout,
            &target,
            &live,
            Some(GitOid::new("a".repeat(40)).unwrap()),
        );
        let kept = seeds_of(&scratch.layout, &target).unwrap();
        holder
            .execute("INSERT INTO evidence VALUES ('still live')", &[])
            .unwrap();
        drop(holder);
        scratch.release_all();
        assert_eq!(kept, previous);
        let Err(CaptureSkip::Held {
            database: named,
            holders,
        }) = captured.unwrap()
        else {
            panic!("an Nx database holder is not an exempt index reader");
        };
        assert_eq!(named, database);
        assert!(
            holders
                .iter()
                .any(|holder| holder.pid == i32::try_from(std::process::id()).unwrap()),
            "the refusal names the live SQLite holder: {holders:?}"
        );
    }

    /// An Nx client that opens the landing volume's task database after quiesce is named by
    /// capture admission as `landingHeld`, never treated as an exempt contributed cache
    /// (16_build_volumes.md, Land step 5.4).
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_seed_freeze_skips_a_late_nx_database_holder_after_quiesce() {
        use cowshed_os::fork_lock::Spawn as _;

        let scratch = Scratch::directory("seed-late-nx-holder");
        let (main_checkout, _, _) = scratch.linked_checkout("main", 1);
        let (topic_checkout, _, mount) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        fs::create_dir_all(mount.join("nx/workspace-data")).unwrap();
        let database = mount.join("nx/workspace-data/T-v3.db");
        fs::write(&database, b"").unwrap();
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout,
            )
            .await
            .unwrap()
            .expect("the landing volume's task database had no holder at quiesce");
        let mut holder = std::process::Command::new("/usr/bin/tail")
            .arg("-f")
            .arg(&database)
            .stdout(std::process::Stdio::null())
            .spawn_locked()
            .unwrap();
        let pid = i32::try_from(holder.id()).expect("a pid fits i32");
        while !nx::holders(&database)
            .unwrap()
            .iter()
            .any(|held| held.pid == pid)
        {
            if let Some(status) = holder.try_wait().unwrap() {
                panic!(
                    "tail exited ({status}) before it opened {}",
                    database.display()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let target = owner("main", '0');
        let locked = scratch.lock(&target).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
            .await;
        let seed = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap();
        holder.kill().unwrap();
        holder.wait_bounded().unwrap();
        drop(quiet);
        drop(locked);
        scratch.release_all();
        assert!(seed.is_none(), "a skipped freeze must publish no seed");
        let Err(AdoptionSkip::LandingHeld {
            database: held,
            holders,
        }) = frozen.unwrap()
        else {
            panic!("a late Nx database holder on the landing volume skips the adoption");
        };
        assert_eq!(held, database);
        assert_eq!(
            holders,
            [DatabaseHolder {
                pid,
                command: format!("/usr/bin/tail -f {}", database.display()),
            }]
        );
    }

    /// The landing checkout's own Nx daemon, recorded in the landing volume and started by a
    /// client after quiesce, is stopped again by capture admission before the mounted snapshot
    /// (16_build_volumes.md, Land step 5.4).
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_seed_freeze_stops_a_late_landing_daemon_before_capture() {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::ExitStatusExt as _;

        let scratch = Scratch::directory("seed-late-daemon");
        let (main_checkout, _, _) = scratch.linked_checkout("main", 1);
        let (topic_checkout, _, mount) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        let data = mount.join("nx/workspace-data");
        fs::create_dir_all(data.join(nx::DAEMON_DIRECTORY)).unwrap();
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout,
            )
            .await
            .unwrap()
            .expect("the landing volume had no Nx client at quiesce");
        // Short, under the scratch parent: a Unix socket path is bounded at 104 bytes on macOS.
        let sockets =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cs-seal");
        let socket = sockets.join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let mut daemon = std::process::Command::new("/bin/cat")
            .current_dir(&data)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        fs::write(
            data.join(nx::DAEMON_DIRECTORY).join("server-process.json"),
            serde_json::json!({ "processId": daemon.id(), "socketPath": socket }).to_string(),
        )
        .unwrap();
        let target = owner("main", '0');
        let locked = scratch.lock(&target).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
            .await;
        let seed = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap();
        let stopped = daemon.try_wait().unwrap();
        if stopped.is_none() {
            daemon.kill().unwrap();
            daemon.wait_bounded().unwrap();
        }
        drop(listener);
        drop(quiet);
        drop(locked);
        scratch.release_all();
        assert_eq!(
            stopped.and_then(|status| status.signal()),
            Some(libc::SIGTERM),
            "the recorded landing daemon is stopped as `nx daemon --stop` stops it"
        );
        assert!(
            matches!(frozen.unwrap(), Ok(Freeze::Frozen { .. })),
            "with its own daemon stopped the landing volume seals"
        );
        let (_, record) = seed.expect("capture publishes the seed after the daemon stops");
        assert_eq!(record.tree, Some(GitOid::new("a".repeat(40)).unwrap()));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn directory_reseed_replaces_the_seed_with_an_open_contributed_cache() {
        let scratch = Scratch::directory("reseed-open-cache");
        let (_, live, mount) = scratch.linked_checkout("main", 1);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".lint-cache", "lint-cache").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        fs::create_dir_all(mount.join("lint-cache")).unwrap();
        let target = owner("main", '0');
        let previous = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &previous,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        let mut held = fs::File::create(mount.join("lint-cache/warm")).unwrap();
        std::io::Write::write_all(&mut held, b"warm").unwrap();
        let tree = GitOid::new("a".repeat(40)).unwrap();
        // Clock ordering is supplied explicitly; this exercises admission and publication,
        // not the independent image-mtime comparison.
        let reseeded = reseed_retired(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live,
                tree: Some(tree.clone()),
                nearness: Nearness::Unknown,
                placed: Placed::Unrecorded,
                written: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                seed: Some(Frozen {
                    id: previous.clone(),
                    tree: None,
                    nearness: Nearness::Unknown,
                    written: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
                }),
            },
        );
        let (seed, record) = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap()
            .unwrap();
        assert!(!scratch.layout.image(&previous).exists());
        let still_open = std::io::Write::write_all(&mut held, b" after capture");
        let contents = fs::read(mount.join("lint-cache/warm")).unwrap();
        drop(held);
        scratch.release_all();
        assert!(matches!(reseeded.unwrap(), Refresh::Reseeded { .. }));
        assert_ne!(seed, previous);
        assert_eq!(record.tree, Some(tree));
        still_open.expect("the cache descriptor survives reseeding");
        assert_eq!(contents, b"warm after capture");
    }

    /// A fork's seed refresh never waits on freeing the seed it supersedes (16_build_volumes.md,
    /// "Garbage collection"). A process works inside the old seed's tree, which keeps any
    /// deletion of it from running; the refresh still publishes the new seed and answers, the
    /// old one committed to `build/.deleting` with its owner record retired and none of its bytes
    /// deleted. The store's deleter leaves it while it is held and deletes it once it is not.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_seed_refresh_journals_a_held_superseded_seed_that_recovery_finishes() {
        use cowshed_os::fork_lock::Spawn as _;
        let scratch = Scratch::directory("refresh-journals-seed");
        let (_, live, _) = scratch.linked_checkout("main", 1);
        let target = owner("main", '0');
        let previous = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &previous,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        let old = scratch.layout.image(&previous);
        fs::write(old.join("old-seed"), b"superseded").unwrap();
        let mut holder = std::process::Command::new("/bin/cat")
            .current_dir(&old)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let tree = GitOid::new("a".repeat(40)).unwrap();
        // Clock ordering is supplied explicitly; this exercises the release, not the
        // image-mtime comparison.
        let reseeded = reseed_retired(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live,
                tree: Some(tree.clone()),
                nearness: Nearness::Unknown,
                placed: Placed::Unrecorded,
                written: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                seed: Some(Frozen {
                    id: previous.clone(),
                    tree: None,
                    nearness: Nearness::Unknown,
                    written: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
                }),
            },
        );
        let journaled = (
            old.exists(),
            fs::read(scratch.layout.released(&previous).join("old-seed")),
            scratch.layout.read_record_present(&previous).unwrap(),
            scratch.host.finish_build_releases(&scratch.layout),
        );
        holder.kill().unwrap();
        holder.wait_bounded().unwrap();
        assert!(
            matches!(reseeded.unwrap(), Refresh::Reseeded { .. }),
            "the refresh completes while the superseded seed is held"
        );
        let (seed, record) = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap()
            .unwrap();
        assert_ne!(seed, previous);
        assert_eq!(record.tree, Some(tree));
        assert!(!journaled.0, "the superseded seed is off its name");
        assert_eq!(
            journaled.1.unwrap(),
            b"superseded",
            "the refresh deleted none of the superseded seed"
        );
        assert_eq!(journaled.2, None, "the commit retires its owner record");
        assert_eq!(
            journaled.3.unwrap(),
            crate::storage::apfs::release::Drained::default(),
            "nothing is deleted from under its holder"
        );
        let finished = scratch.host.finish_build_releases(&scratch.layout).unwrap();
        assert_eq!(finished.images, 1, "the deleter frees the committed seed");
        assert!(finished.bytes > 0, "{finished:?}");
        assert!(!scratch.layout.released(&previous).exists());
        scratch.release_all();
    }

    /// Fork `destination` from `main`'s seed refreshed for `aim`, as `fork` does, and read the
    /// marker output in the fork's live volume, if any.
    #[cfg(target_os = "macos")]
    async fn fork_marker<H: RuntimeStorageHost>(
        scratch: &Scratch<H>,
        main: &Owner,
        main_checkout: &Path,
        destination: &str,
        aim: Aim,
    ) -> Option<String> {
        let destination = owner(destination, '2');
        let staged = scratch
            .root
            .path()
            .join(format!("staged-{}", destination.name));
        fs::create_dir_all(staged.join(".cowshed")).unwrap();
        let prepared = scratch
            .volumes
            .capture_fork_retired(
                main.clone(),
                main_checkout.to_owned(),
                destination.name.clone(),
                aim,
            )
            .await
            .expect("capture the fork from main's seed");
        let prepared = scratch
            .volumes
            .ready_fork(Ok(prepared), destination.clone())
            .await
            .expect("ready the fork's volume");
        let forked = scratch
            .volumes
            .finish_fork(prepared, destination, staged)
            .await
            .expect("fork from main's seed")
            .expect("main has a seed");
        fs::read_to_string(scratch.layout.mount(&forked).join("marker")).ok()
    }

    /// A fork of a workspace that has not moved since cowshed placed its build volume carries
    /// that volume (16_build_volumes.md, "Which volume a seed holds"). `topic` was forked from
    /// main while main's seed was behind main's tree, so neither `topic`'s live volume nor its
    /// seed was built at the tree it has checked out, and main has that tree checked out too.
    /// Nothing moved `topic`'s branch, so that is no external land: the fork of `topic`
    /// reseeds from `topic`'s own volume and inherits what `topic` built, never main's volume.
    #[cfg(target_os = "macos")]
    async fn a_fork_of_an_unmoved_workspace_carries_its_build_volume<H: RuntimeStorageHost>(
        scratch: Scratch<H>,
    ) {
        let main = owner("main", '0');
        let (main_checkout, main_live, main_mount) = scratch.linked_checkout("main", 1);
        let behind = commit(&main_checkout, &[("lib.rs", "1")]);
        scratch
            .layout
            .write_record(
                &main_live,
                &BuildVolumeRecord::new(Some(behind.clone()), linked("main")),
            )
            .unwrap();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &BuildVolumeId::mint(),
                &registered(BuildVolumeRecord::new(
                    Some(behind),
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                )),
            )
            .expect("seed main");
        fs::write(main_mount.join("marker"), "main").unwrap();
        // Main moves on and its seed stays behind, as when a land's seal was skipped.
        let tree = commit(&main_checkout, &[("lib.rs", "2")]);

        // `topic` is forked from main at main's tree.
        let topic = owner("topic", '1');
        let topic_checkout = scratch.root.path().join("topic");
        let cloned = crate::git::git_command_at(scratch.root.path())
            .args(["clone", "--quiet"])
            .arg(&main_checkout)
            .arg(&topic_checkout)
            .output_bounded()
            .unwrap();
        assert!(cloned.status.success(), "{cloned:?}");
        fs::create_dir_all(topic_checkout.join(".cowshed")).unwrap();
        let prepared = scratch
            .volumes
            .capture_fork_retired(
                main.clone(),
                main_checkout.clone(),
                topic.name.clone(),
                Aim {
                    tree: tree.clone(),
                    landed: Vec::new(),
                },
            )
            .await
            .expect("capture topic's fork from main");
        let prepared = scratch
            .volumes
            .ready_fork(Ok(prepared), topic.clone())
            .await
            .expect("ready topic's volume");
        let topic_live = scratch
            .volumes
            .finish_fork(prepared, topic.clone(), topic_checkout.clone())
            .await
            .expect("fork topic from main")
            .expect("main has a seed");
        // What topic builds at its tree, which nothing in main built.
        fs::write(scratch.layout.mount(&topic_live).join("marker"), "topic").unwrap();

        let copy = fork_marker(
            &scratch,
            &topic,
            &topic_checkout,
            "copy",
            Aim {
                tree,
                landed: vec![Peer {
                    name: main.name.clone(),
                    checkout: main_checkout.clone(),
                    lock: scratch.image_lock(&main.name),
                }],
            },
        )
        .await;
        assert_eq!(
            copy.as_deref(),
            Some("topic"),
            "a fork of an unmoved workspace inherits that workspace's build volume"
        );

        // `topic` commits and builds its own work, so its checkout moves from where cowshed
        // placed its volume, and `sibling`, forked from it, checks out the same tree. Nothing
        // outside cowshed lands into a session: the next fork of `topic` still takes `topic`'s
        // own volume, never `sibling`'s, which lacks what `topic` built after that fork.
        let moved = commit(&topic_checkout, &[("work.rs", "1")]);
        fs::write(scratch.layout.mount(&topic_live).join("marker"), "first").unwrap();
        let sibling = owner("sibling", '3');
        let sibling_checkout = scratch.root.path().join("sibling");
        let cloned = crate::git::git_command_at(scratch.root.path())
            .args(["clone", "--quiet"])
            .arg(&topic_checkout)
            .arg(&sibling_checkout)
            .output_bounded()
            .unwrap();
        assert!(cloned.status.success(), "{cloned:?}");
        fs::create_dir_all(sibling_checkout.join(".cowshed")).unwrap();
        let main_peer = Peer {
            name: main.name.clone(),
            checkout: main_checkout.clone(),
            lock: scratch.image_lock(&main.name),
        };
        let prepared = scratch
            .volumes
            .capture_fork_retired(
                topic.clone(),
                topic_checkout.clone(),
                sibling.name.clone(),
                Aim {
                    tree: moved.clone(),
                    landed: vec![main_peer.clone()],
                },
            )
            .await
            .expect("capture sibling's fork from topic");
        let prepared = scratch
            .volumes
            .ready_fork(Ok(prepared), sibling.clone())
            .await
            .expect("ready sibling's volume");
        scratch
            .volumes
            .finish_fork(prepared, sibling.clone(), sibling_checkout.clone())
            .await
            .expect("fork sibling from topic")
            .expect("topic has a seed");
        fs::write(scratch.layout.mount(&topic_live).join("marker"), "built").unwrap();
        let built = fork_marker(
            &scratch,
            &topic,
            &topic_checkout,
            "built-copy",
            Aim {
                tree: moved,
                landed: vec![
                    main_peer,
                    Peer {
                        name: sibling.name.clone(),
                        checkout: sibling_checkout,
                        lock: scratch.image_lock(&sibling.name),
                    },
                ],
            },
        )
        .await;
        assert_eq!(
            built.as_deref(),
            Some("built"),
            "a fork of a committed-and-built workspace inherits that workspace's build volume"
        );
        scratch.release_all();
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_a_fork_of_an_unmoved_workspace_carries_its_build_volume() {
        a_fork_of_an_unmoved_workspace_carries_its_build_volume(Scratch::directory(
            "build-unmoved-fork",
        ))
        .await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_a_fork_of_an_unmoved_workspace_carries_its_build_volume() {
        a_fork_of_an_unmoved_workspace_carries_its_build_volume(Scratch::new("build-unmoved-fork"))
            .await;
    }
    /// An external land (16_build_volumes.md, "Which volume a seed holds"): main's branch moves
    /// by plain git to the tree `lander` has checked out, and `lander`'s build volume holds a
    /// marker output nothing in main built. A fork of main at that tree inherits the marker
    /// without building, and keeps inheriting it after main's own volume is written again. The
    /// control (no candidate), the plant (a candidate whose volume holds a marker at another
    /// tree) and a candidate whose image lock is held all leave main's seed where it was.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_a_fork_after_an_external_land_inherits_the_landed_volume() {
        let scratch = Scratch::new("build-external-land");
        let main = owner("main", '0');
        let (main_checkout, main_live, main_mount) = scratch.linked_checkout("main", 1);
        let before = commit(&main_checkout, &[("lib.rs", "1")]);
        scratch
            .layout
            .write_record(
                &main_live,
                // Placed at `before`, as a land's adoption places it.
                &BuildVolumeRecord {
                    checked_out: Some(before.clone()),
                    ..BuildVolumeRecord::new(Some(before.clone()), linked("main"))
                },
            )
            .unwrap();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &BuildVolumeId::mint(),
                &registered(BuildVolumeRecord::new(
                    Some(before.clone()),
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                )),
            )
            .expect("seed main");
        let peer = |name: &str, checkout: &Path| Peer {
            name: WorkspaceName::new(name).unwrap(),
            checkout: checkout.to_owned(),
            lock: scratch.image_lock(&WorkspaceName::new(name).unwrap()),
        };
        let (lander_checkout, _, lander_mount) = scratch.linked_checkout("lander", 1);
        let landed = commit(&lander_checkout, &[("lib.rs", "2")]);
        fs::write(lander_mount.join("marker"), "M").unwrap();
        let lander = peer("lander", &lander_checkout);
        let (drifter_checkout, _, drifter_mount) = scratch.linked_checkout("drifter", 1);
        commit(&drifter_checkout, &[("lib.rs", "3")]);
        fs::write(drifter_mount.join("marker"), "D").unwrap();
        let drifter = peer("drifter", &drifter_checkout);
        // The external land: main's checkout reaches the landed tree by plain git.
        assert_eq!(commit(&main_checkout, &[("lib.rs", "2")]), landed);
        let aim = |landed_by: Vec<Peer>| Aim {
            tree: landed.clone(),
            landed: landed_by,
        };
        let seed_tree = || {
            scratch
                .layout
                .seed_of(&main.name, &main.incarnation)
                .unwrap()
                .expect("main has a seed")
                .1
                .tree
        };

        // Control: no live workspace offered, so the fork is built from main's own state.
        let control = fork_marker(&scratch, &main, &main_checkout, "control", aim(vec![])).await;
        assert_eq!(control, None);
        assert_eq!(seed_tree(), Some(before.clone()));

        // Plant: a workspace at another tree never seeds main, whatever its volume holds.
        let plant = fork_marker(
            &scratch,
            &main,
            &main_checkout,
            "plant",
            aim(vec![drifter.clone()]),
        )
        .await;
        assert_eq!(plant, None);
        assert_eq!(seed_tree(), Some(before.clone()));

        // A landed workspace whose own operation holds its image lock is passed over, named.
        let held = scratch
            .host
            .lock_images(
                std::slice::from_ref(&lander.lock),
                LockMode::Try,
                &LockHolding::of(LockOperation::Land, &lander.name),
            )
            .unwrap()
            .expect("nothing else holds lander's image lock");
        let refreshed = scratch
            .volumes
            .refresh_seed(
                main.clone(),
                main_checkout.clone(),
                aim(vec![lander.clone()]),
                LockHolding::of(LockOperation::Fork, &WorkspaceName::new("fresh").unwrap()),
            )
            .await
            .unwrap();
        drop(held);
        assert_eq!(
            refreshed.passed,
            vec![(lander.name.clone(), LandedSkip::Locked)]
        );
        assert!(
            !matches!(refreshed.outcome, Refresh::Landed { .. }),
            "{refreshed:?}"
        );
        assert_eq!(seed_tree(), Some(before.clone()));

        // The fork at the landed tree inherits the landed volume's marker without building.
        let inherited = fork_marker(
            &scratch,
            &main,
            &main_checkout,
            "inherited",
            aim(vec![drifter.clone(), lander.clone()]),
        )
        .await;
        assert_eq!(inherited.as_deref(), Some("M"));
        assert_eq!(seed_tree(), Some(landed.clone()));

        // Main's own volume, built at the old tree, is written again: the landed seed is nearer
        // the fork's tree and stays, though the live volume was written after it.
        fs::write(main_mount.join("later"), "written after the land").unwrap();
        let after = fork_marker(&scratch, &main, &main_checkout, "after", aim(vec![])).await;
        assert_eq!(after.as_deref(), Some("M"));
        assert_eq!(seed_tree(), Some(landed));
        scratch.release_all();
    }
    /// Adoption copies nothing from the target (16_build_volumes.md, "Land" step 6): the
    /// target's volume indexes an Nx entry the landing volume lacks, and once the target has
    /// adopted the landing volume, that volume holds neither the entry's files nor its row. What
    /// other lands ran reaches a fork through the project's Nx cache (05_gateway.md,
    /// "Publication"), never through the adopted volume.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_adoption_copies_nothing_from_the_target() {
        let scratch = Scratch::directory("adopt-copies-nothing");
        let (main_checkout, _, target) = scratch.linked_checkout("main", 1);
        let (topic_checkout, topic, landing) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
            ],
            fingerprint: None,
        };
        let sqlite = |mount: &Path, statement: &str| {
            let output = std::process::Command::new("/usr/bin/sqlite3")
                .arg(mount.join("nx/workspace-data/task-v3.db"))
                .arg(statement)
                .output_bounded()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            output.stdout
        };
        // Stock Nx's two cache tables, and one entry each side ran: the target's "1", the
        // landing workspace's "2".
        for (mount, hash) in [(&target, "1"), (&landing, "2")] {
            state.write(mount).unwrap();
            fs::create_dir_all(mount.join("nx/workspace-data")).unwrap();
            sqlite(
                mount,
                &format!(
                    "CREATE TABLE task_details (hash TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, target TEXT NOT NULL, configuration TEXT); \
                     CREATE TABLE cache_outputs (hash TEXT PRIMARY KEY NOT NULL, code INTEGER NOT NULL, size INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (hash) REFERENCES task_details(hash)); \
                     INSERT INTO task_details VALUES ('{hash}', 'a', 'build', NULL); \
                     INSERT INTO cache_outputs (hash, code, size) VALUES ('{hash}', 0, 4);"
                ),
            );
            fs::create_dir_all(mount.join("nx/cache").join(hash)).unwrap();
            fs::write(mount.join("nx/cache").join(hash).join("output"), b"warm").unwrap();
        }
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout.clone(),
            )
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        let tree = GitOid::new("a".repeat(40)).unwrap();
        let locked = scratch.lock(&owner("main", '0')).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, tree.clone())
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert_eq!(retired(&scratch.volumes, frozen).await, Froze::Frozen);
        let closed = scratch
            .volumes
            .close_target(main_checkout.clone())
            .await
            .unwrap()
            .expect("nothing holds the target's volume");
        let adopted = scratch
            .volumes
            .adopt(&locked, &quiet, closed, main_checkout.clone(), tree)
            .await
            .unwrap()
            .expect("nothing opened the target's volume since");
        drop(quiet);
        drop(locked);
        scratch
            .volumes
            .release_previous(adopted.previous)
            .await
            .unwrap();
        let linked = scratch.layout.linked(&main_checkout).unwrap();
        let indexed = sqlite(&landing, "SELECT hash FROM cache_outputs ORDER BY hash;");
        let copied = landing.join("nx/cache/1").exists();
        scratch.release_all();
        assert_eq!(linked, Some(topic), "the target adopted the landing volume");
        assert_eq!(
            String::from_utf8_lossy(&indexed),
            "2\n",
            "the adopted volume indexes only its own entry"
        );
        assert!(!copied, "the target's entry stays with its previous volume");
    }

    /// A shed's daemon keeper answers the reseed's stop of its daemon by admitting a start job
    /// (16_build_volumes.md, "Targets and seeds", step 1). The recorded daemon here is that
    /// restart: on SIGTERM it asks for the source's build hold exactly as job admission does (a
    /// shared, non-waiting flock), hands any hold it gets to a child that keeps it, and only then
    /// exits, so the attempt happens before the stop returns. The reseed's claim, taken before the
    /// stop, refuses that admission, and the reseed captures.
    #[cfg(target_os = "macos")]
    #[test]
    fn directory_reseed_claims_its_source_before_the_keeper_can_restart_the_daemon() {
        use cowshed_os::fork_lock::Spawn as _;

        let scratch = Scratch::directory("reseed-keeper");
        let (_, live, mount) = scratch.linked_checkout("w1", 1);
        let target = owner("w1", '1');
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        state.write(&mount).unwrap();
        let data = mount.join("nx/workspace-data");
        fs::create_dir_all(data.join(nx::DAEMON_DIRECTORY)).unwrap();
        let previous = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &previous,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        // Short, under the scratch parent: a Unix socket path is bounded at 104 bytes on macOS.
        let sockets =
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), "cs-keep");
        let socket = sockets.join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let admission = sockets.join("admission");
        let mut daemon = std::process::Command::new("/usr/bin/perl")
            .args([
                "-e",
                r#"use Fcntl qw(:flock);
                   my ($hold, $out) = @ARGV;
                   $SIG{TERM} = sub {
                       open(my $h, '+<', $hold) or die "open $hold: $!";
                       open(my $o, '>', $out) or die "open $out: $!";
                       if (flock($h, LOCK_SH | LOCK_NB)) {
                           my $child = fork();
                           if ($child == 0) { select(undef, undef, undef, undef) while 1 }
                           print $o "admitted $child";
                       } else {
                           print $o "refused";
                       }
                       close $o;
                       exit 0;
                   };
                   $| = 1;
                   print "ready\n";
                   select(undef, undef, undef, undef) while 1;"#,
            ])
            .arg(scratch.layout.hold_path(&live))
            .arg(&admission)
            .current_dir(&data)
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        // The daemon is recorded once its SIGTERM handler is installed: before that, the stop
        // would end it with the default action and no restart would be attempted.
        let mut ready = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(daemon.stdout.take().expect("piped stdout")),
            &mut ready,
        )
        .unwrap();
        assert_eq!(ready, "ready\n", "the stand-in daemon installs its handler");
        fs::write(
            data.join(nx::DAEMON_DIRECTORY).join("server-process.json"),
            serde_json::json!({ "processId": daemon.id(), "socketPath": socket }).to_string(),
        )
        .unwrap();
        let reseeded = reseed_retired(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live,
                tree: None,
                nearness: Nearness::Unknown,
                placed: Placed::Unrecorded,
                written: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                seed: Some(Frozen {
                    id: previous.clone(),
                    tree: None,
                    nearness: Nearness::Unknown,
                    written: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
                }),
            },
        );
        let stopped = daemon.try_wait().unwrap();
        if stopped.is_none() {
            daemon.kill().unwrap();
            daemon.wait_bounded().unwrap();
        }
        let attempt = fs::read_to_string(&admission).unwrap_or_default();
        if let Some(child) = attempt.strip_prefix("admitted ") {
            let child: libc::pid_t = child.parse().unwrap();
            // SAFETY: kill takes a pid and a signal number and touches no memory.
            unsafe { libc::kill(child, libc::SIGKILL) };
        }
        drop(listener);
        let seeds = seeds_of(&scratch.layout, &target).unwrap();
        scratch.release_all();
        assert!(stopped.is_some(), "the reseed stops the recorded daemon");
        assert_eq!(
            attempt, "refused",
            "the restart's admission meets the reseed's claim"
        );
        assert!(
            matches!(reseeded.unwrap(), Refresh::Reseeded { .. }),
            "the reseed captures"
        );
        assert!(
            !seeds.contains(&previous) && seeds.len() == 1,
            "the new seed replaces the previous one: {seeds:?}"
        );
    }

    /// A job admitted on the source before the reseed skips it, and the skip names the process
    /// holding the build hold rather than an empty list of open files.
    #[cfg(target_os = "macos")]
    #[test]
    fn directory_reseed_skipped_by_an_admitted_job_names_the_holds_holder() {
        let scratch = Scratch::directory("reseed-admitted");
        let (_, live, _) = scratch.linked_checkout("w1", 1);
        let target = owner("w1", '1');
        let previous = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &previous,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        let job = scratch.layout.admit(&live).expect("a job's hold");
        let reseeded = reseed_retired(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live: live.clone(),
                tree: None,
                nearness: Nearness::Unknown,
                placed: Placed::Unrecorded,
                written: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                seed: Some(Frozen {
                    id: previous.clone(),
                    tree: None,
                    nearness: Nearness::Unknown,
                    written: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
                }),
            },
        );
        drop(job);
        let seeds = seeds_of(&scratch.layout, &target).unwrap();
        scratch.release_all();
        assert_eq!(seeds, [previous], "the skip keeps the previous seed");
        let Refresh::Skipped {
            reason: CaptureSkip::VolumeBusy { reason },
            ..
        } = reseeded.unwrap()
        else {
            panic!("an admitted job is a reseed skip");
        };
        assert!(
            reason.contains(&scratch.layout.hold_path(&live).display().to_string())
                && reason.contains(&format!("pid {}", std::process::id()))
                && reason.contains("a job admitted on it"),
            "{reason}"
        );
    }

    /// Build volume `id`, mounted, its image recording `gib` GiB: a clone of the run's blank
    /// image put in place, once `registration` is registered, and mounted the way a fork's
    /// volume is. Creation is not what these tests prove. Above the 1 GiB test cap only the
    /// clone's sparse RAW file length grows and its container stays the template's: these tests
    /// compare virtual image capacities, not guest free space, and a mint at another capacity
    /// would cost the store a template of its own — a sparse file, a formatting
    /// attach and a detach.
    #[cfg(target_os = "macos")]
    fn test_volume(
        host: &Host,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        gib: u64,
        registration: &Registration,
    ) -> PathBuf {
        use cowshed_apfs::CommandRunner;
        use cowshed_os::namespace::HostNamespace;
        let HostNamespace::Test(namespace) = host.backend().runner().namespace() else {
            unreachable!("a runtime test host runs in its test namespace")
        };
        let _creating = layout.register(id, registration).unwrap();
        let image = layout.image(id);
        crate::blank_image::blank_image(namespace, &image);
        let capacity = ImageCapacity::from_gibibytes(gib);
        if capacity > crate::blank_image::CAPACITY {
            host.backend()
                .runner()
                .grow_image(&image, capacity)
                .expect("grow the clone's sparse image length");
        }
        layout.publish(id, registration).unwrap();
        host.mount_build_volume(layout, id)
            .expect("mount a build volume")
    }

    #[cfg(target_os = "macos")]
    /// What a freeze did, once its superseded seeds are retired as a land retires them after it
    /// lets the target's image lock go.
    #[derive(Debug, Eq, PartialEq)]
    enum Froze {
        Frozen,
        SealBusy { reason: String },
    }

    async fn retired<H: RuntimeStorageHost>(volumes: &BuildVolumes<H>, freeze: Freeze) -> Froze {
        match freeze {
            Freeze::Frozen { superseded } => {
                volumes.retire_superseded(superseded).await;
                Froze::Frozen
            }
            Freeze::SealBusy { reason } => Froze::SealBusy { reason },
        }
    }

    async fn retired_or_skipped<H: RuntimeStorageHost>(
        volumes: &BuildVolumes<H>,
        freeze: std::result::Result<Freeze, AdoptionSkip>,
    ) -> std::result::Result<Froze, AdoptionSkip> {
        match freeze {
            Ok(freeze) => Ok(retired(volumes, freeze).await),
            Err(skip) => Err(skip),
        }
    }

    /// [`reseed`] with the seeds it superseded retired at once, as a fork retires them once it
    /// lets the target's image lock go.
    fn reseed_retired(
        host: &impl RuntimeStorageHost,
        layout: &BuildVolumeLayout,
        target: &Owner,
        age: SeedAge,
    ) -> Result<Refresh> {
        let (refresh, superseded) = reseed(host, layout, target, age)?;
        if let Some(superseded) = superseded {
            retire_seeds(host, layout, superseded)?;
        }
        Ok(refresh)
    }

    impl<H: RuntimeStorageHost> BuildVolumes<H> {
        /// [`Self::capture_fork`], the seeds its refresh replaced retired as the fork's caller
        /// retires them.
        async fn capture_fork_retired(
            &self,
            source: Owner,
            source_checkout: PathBuf,
            destination: WorkspaceName,
            aim: Aim,
        ) -> Result<Option<CapturedFork>> {
            let (captured, superseded) = self
                .capture_fork(source, source_checkout, destination, aim)
                .await;
            if let Some(superseded) = superseded {
                self.retire_superseded(superseded).await;
            }
            captured
        }
    }

    fn owner(name: &str, digit: char) -> Owner {
        Owner {
            name: WorkspaceName::new(name).unwrap(),
            incarnation: WorkspaceIncarnation::new(digit.to_string().repeat(32)).unwrap(),
        }
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
        let (main_checkout, previous, _) = scratch.linked_checkout("main", 2);
        let (topic_checkout, topic, _) = scratch.linked_checkout("topic", 1);
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout.clone(),
            )
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert!(capacity(&topic) >= ImageCapacity::from_gibibytes(2));
        let tree = GitOid::new("a".repeat(40)).unwrap();
        let locked = scratch.lock(&owner("main", '0')).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, tree.clone())
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert_eq!(retired(&scratch.volumes, frozen).await, Froze::Frozen);
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
        let adopted = scratch
            .volumes
            .adopt(&locked, &quiet, closed, main_checkout.clone(), tree)
            .await
            .unwrap()
            .expect("nothing opened the target's volume since");
        // The land lets go of the landing volume once main's link and record name it.
        drop(quiet);
        assert_eq!(
            scratch.volumes.layout.linked(&main_checkout).unwrap(),
            Some(topic.clone())
        );
        // The previous volume stays the target's, at its incarnation, recorded superseded by
        // the move, and is released by its caller.
        assert!(matches!(
            scratch.layout.read_record(&previous).unwrap().role,
            BuildVolumeRole::Superseded { owner, .. } if owner.is_main()
        ));
        scratch
            .volumes
            .release_previous(adopted.previous)
            .await
            .unwrap();
        assert!(!scratch.layout.image(&previous).exists());

        // A landing volume larger than its target keeps its capacity.
        let (other_checkout, other, _) = scratch.linked_checkout("other", 3);
        scratch
            .volumes
            .quiesce(
                scratch.landing(&other_checkout),
                scratch.stopped(&other_checkout).await,
                main_checkout,
            )
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert_eq!(capacity(&other), ImageCapacity::from_gibibytes(3));
        scratch.release_all();
    }

    /// A git checkout under `root` with `tracked` files (each `{}`) and a `.cowshed.toml`
    /// asking `toml`, everything added to the index.
    #[cfg(target_os = "macos")]
    fn tracked_checkout(root: &Path, name: &str, toml: &str, tracked: &[&str]) -> PathBuf {
        let checkout = root.join(name);
        fs::create_dir_all(checkout.join(".cowshed")).unwrap();
        fs::write(checkout.join(".cowshed.toml"), toml).unwrap();
        for file in tracked {
            let path = checkout.join(file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "{}\n").unwrap();
        }
        for args in [&["init", "--quiet"][..], &["add", "--", "."][..]] {
            let output = crate::git::git_command_at(&checkout)
                .args(args)
                .output_bounded()
                .unwrap();
            assert!(output.status.success(), "git {args:?}: {output:?}");
        }
        checkout
    }

    /// At the test cap, so a mint clones the run's template rather than making one.
    #[cfg(target_os = "macos")]
    const AT_THE_CAP: &str = "[build]\ncapacity = \"1g\"\n";

    #[cfg(target_os = "macos")]
    fn minted<H: RuntimeStorageHost>(first: &FirstMint<H>) -> MintedVolume {
        match first {
            FirstMint::Minted(owned) => lock(&owned.slot)
                .as_ref()
                .map(|minted| minted.volume.clone())
                .expect("an unsettled mint holds its volume"),
            FirstMint::Nothing => panic!("nothing was minted beside main's image"),
            FirstMint::Failed(error) => panic!("the mint beside main's image failed: {error}"),
        }
    }

    #[cfg(target_os = "macos")]
    fn discovered(paths: Vec<BuildStatePath>) -> Discovered {
        Discovered::Changed {
            paths,
            fingerprint: "fixture".to_owned(),
            capacity: ImageCapacity::from_gibibytes(1),
        }
    }

    /// Main's first build volume is minted beside main's image and handed to its first touch:
    /// with build state discovered, the first touch links that volume, records it and seeds main
    /// from it, and settling keeps it. With nothing discovered, nothing links it, and settling
    /// deletes it before the adoption completes, so it never outlives the intent that protects it
    /// from collection. A checkout in which nothing can name build state gets none minted at all:
    /// no tracked manifest, or only tracked build inputs whose state is the host's.
    /// Once linked, that volume wins over another mint, which settling releases unused.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_main_links_the_volume_minted_beside_its_image_or_releases_it() {
        let scratch = Scratch::directory("build-minted-first");
        let main = owner("main", '0');
        for (name, tracked) in [
            ("bare", &[][..]),
            ("go", &["go.mod"][..]),
            ("cargo-config", &[".cargo/config.toml"][..]),
        ] {
            let checkout = tracked_checkout(scratch.root.path(), name, AT_THE_CAP, tracked);
            let first = scratch.volumes.mint_first(checkout.clone(), checkout).await;
            assert!(matches!(first, FirstMint::Nothing), "{name}");
        }
        assert_eq!(scratch.layout.list().unwrap(), []);

        // A tracked package that is not installed: discovery may name its caches, and names none.
        let checkout = tracked_checkout(scratch.root.path(), "main", AT_THE_CAP, &["package.json"]);
        let first = scratch
            .volumes
            .mint_first(checkout.clone(), checkout.clone())
            .await;
        let unused = minted(&first);
        assert_eq!(unused.capacity, ImageCapacity::from_gibibytes(1));
        let refresh = scratch
            .volumes
            .refresh(
                main.clone(),
                checkout.clone(),
                discovered(Vec::new()),
                first.lend(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refresh.volume, None, "nothing to hold links nothing");
        first.settle().await;
        assert!(!scratch.layout.image(&unused.id).exists());
        assert_eq!(scratch.layout.list().unwrap(), []);

        let first = scratch
            .volumes
            .mint_first(checkout.clone(), checkout.clone())
            .await;
        let linked_now = minted(&first);
        let refresh = scratch
            .volumes
            .refresh(
                main.clone(),
                checkout.clone(),
                discovered(vec![BuildStatePath::new("target", "target").unwrap()]),
                first.lend(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refresh.volume, Some(linked_now.id.clone()));
        assert!(refresh.created);
        first.settle().await;
        assert_eq!(
            scratch.layout.linked(&checkout).unwrap(),
            Some(linked_now.id.clone())
        );
        assert_eq!(
            scratch
                .layout
                .read_record_present(&linked_now.id)
                .unwrap()
                .map(|record| record.role),
            Some(linked("main")),
            "recorded once the first touch linked it"
        );
        assert!(
            scratch
                .layout
                .seed_of(&main.name, &main.incarnation)
                .unwrap()
                .is_some(),
            "main is seeded from its first volume"
        );

        // A later first-touch attempt must use the link, not its newly minted candidate.
        let first = scratch
            .volumes
            .mint_first(checkout.clone(), checkout.clone())
            .await;
        let unused = minted(&first);
        let refresh = scratch
            .volumes
            .refresh(
                main.clone(),
                checkout.clone(),
                discovered(vec![BuildStatePath::new("target", "target").unwrap()]),
                first.lend(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            refresh.volume,
            Some(linked_now.id.clone()),
            "the linked volume wins"
        );
        first.settle().await;
        assert!(!scratch.layout.image(&unused.id).exists());
        assert!(scratch.layout.image(&linked_now.id).exists());
        scratch.release_all();
    }

    /// A tracked-source refusal leaves the volume minted beside main's image to settling,
    /// which releases it. An interrupted first touch's link protects its volume even before
    /// the record is published.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_main_releases_the_volume_minted_beside_its_image_its_first_touch_left() {
        let scratch = Scratch::directory("build-minted-left");
        let main = owner("main", '0');
        let source = tracked_checkout(scratch.root.path(), "source", AT_THE_CAP, &["package.json"]);

        let refusing = tracked_checkout(
            scratch.root.path(),
            "refusing",
            AT_THE_CAP,
            &["target/lib.rs"],
        );
        let first = scratch
            .volumes
            .mint_first(source.clone(), refusing.clone())
            .await;
        let refused = minted(&first);
        let refusal = scratch
            .volumes
            .refresh(
                main.clone(),
                refusing.clone(),
                discovered(vec![BuildStatePath::new("target", "target").unwrap()]),
                first.lend(),
            )
            .await
            .unwrap();
        assert!(refusal.is_err(), "target holds tracked source: {refusal:?}");
        first.settle().await;
        assert!(!scratch.layout.image(&refused.id).exists());

        let interrupted = tracked_checkout(scratch.root.path(), "interrupted", AT_THE_CAP, &[]);
        let first = scratch
            .volumes
            .mint_first(source.clone(), interrupted.clone())
            .await;
        let kept = minted(&first);
        link::point(&interrupted, &scratch.layout.mount(&kept.id)).unwrap();
        first.settle().await;
        assert!(scratch.layout.image(&kept.id).exists(), "linked, so kept");
        assert_eq!(
            scratch
                .layout
                .read_record_present(&kept.id)
                .unwrap()
                .map(|record| (record.is_published(), record.role)),
            Some((false, linked("main"))),
            "registered to main by the adopt before its image existed, and never published"
        );
        scratch.release_all();
    }

    /// A first touch rejects a volume minted for an earlier capacity request, mints its own,
    /// and settling releases the unused first mint. This checks request selection, not physical
    /// growth, which the resize and adoption scenarios prove on APFS.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_main_mints_its_own_when_the_volume_minted_beside_it_is_another_capacity() {
        let scratch = Scratch::directory("build-minted-resized");
        let source = tracked_checkout(scratch.root.path(), "source", AT_THE_CAP, &["package.json"]);
        // The source asked 1 GiB when main's volume was minted and asks 2 GiB once copied.
        let resized = tracked_checkout(scratch.root.path(), "resized", AT_THE_CAP, &[]);
        let first = scratch
            .volumes
            .mint_first(source.clone(), resized.clone())
            .await;
        let stale = minted(&first);
        assert_eq!(stale.capacity, ImageCapacity::from_gibibytes(1));
        let refresh = scratch
            .volumes
            .refresh(
                owner("resized", '1'),
                resized.clone(),
                Discovered::Changed {
                    paths: vec![BuildStatePath::new("target", "target").unwrap()],
                    fingerprint: "fixture".to_owned(),
                    capacity: ImageCapacity::from_gibibytes(2),
                },
                first.lend(),
            )
            .await
            .unwrap()
            .unwrap();
        let own = refresh.volume.expect("the first touch mints its own");
        assert_ne!(own, stale.id);
        first.settle().await;
        assert!(!scratch.layout.image(&stale.id).exists());
        assert_eq!(scratch.layout.linked(&resized).unwrap(), Some(own));
        scratch.release_all();
    }

    /// A failed mint beside main's image is no reason to mint again: a first touch that needs a
    /// volume answers the mint's own error and makes none; one that needs none (nothing
    /// discovered) is not failed by it.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_failed_mint_beside_main_fails_only_a_first_touch_that_needs_it() {
        use cowshed_os::namespace::{HostNamespace, TestNamespace};
        let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
        let store = Store::new();
        let checkout = tracked_checkout(
            &store.root,
            "main",
            "[build]\ncapacity = \"not a size\"\n",
            &["package.json"],
        );
        let host = Arc::new(
            MacOsApfsExecutionHost::new(
                SystemCommandRunner::new(namespace),
                crate::storage::apfs::ApfsSubstrateConfig::new(namespace, &store.root, &checkout),
            )
            .unwrap(),
        );
        let volumes = BuildVolumes::new(
            host,
            store.layout.clone(),
            crate::project_retirement::acquire_project_reader(
                &store.root,
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap(),
        );
        let first = volumes.mint_first(checkout.clone(), checkout.clone()).await;
        let FirstMint::Failed(cause) = &first else {
            panic!("an unreadable capacity fails the mint");
        };
        let cause = cause.clone();
        let refresh = volumes
            .refresh(
                owner("main", '0'),
                checkout.clone(),
                discovered(Vec::new()),
                first.lend(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(refresh.volume, None);
        let failed = volumes
            .refresh(
                owner("main", '0'),
                checkout.clone(),
                discovered(vec![BuildStatePath::new("target", "target").unwrap()]),
                first.lend(),
            )
            .await
            .unwrap_err();
        assert_eq!(failed, cause);
        assert_eq!(store.layout.list().unwrap(), [], "no second mint");
        assert_eq!(store.layout.linked(&checkout).unwrap(), None);
        first.abandon().await;
    }

    /// An adopt dropped before it settled what it minted beside main's image leaves no volume
    /// behind: dropped while its mint waits for the blocking lane, the mint still runs, and
    /// releases what it made since nobody is there to take it. Dropped after, the unsettled
    /// answer releases it on the blocking lane, which dropping the runtime runs or cancels -- and
    /// a cancelled release runs as its task is dropped; under a runtime already shut down, which
    /// refuses the task, it runs at once.
    #[cfg(target_os = "macos")]
    #[test]
    fn directory_a_dropped_adopt_releases_the_volume_minted_beside_main() {
        use std::task::Poll;
        let scratch = Scratch::directory("build-minted-dropped");
        let checkout = tracked_checkout(scratch.root.path(), "main", AT_THE_CAP, &["package.json"]);

        // The runtime's one blocking thread is held, so the mint is queued behind it: one poll
        // leaves it unanswered, and it runs only once the adopt is gone.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (open, gate) = std::sync::mpsc::channel::<()>();
        runtime.block_on(async {
            let held = tokio::task::spawn_blocking(move || gate.recv());
            {
                let mut mint = std::pin::pin!(
                    scratch
                        .volumes
                        .mint_first(checkout.clone(), checkout.clone())
                );
                std::future::poll_fn(|context| {
                    assert!(mint.as_mut().poll(context).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            open.send(()).unwrap();
            held.await.unwrap().unwrap();
            // Queued behind the mint on the one blocking thread: done once the mint is.
            tokio::task::spawn_blocking(|| ()).await.unwrap();
        });
        drop(runtime);
        assert_eq!(scratch.layout.list().unwrap(), []);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let first = runtime.block_on(
            scratch
                .volumes
                .mint_first(checkout.clone(), checkout.clone()),
        );
        let dropped = minted(&first);
        runtime.block_on(async move { drop(first) });
        drop(runtime);
        assert!(!scratch.layout.image(&dropped.id).exists());
        assert_eq!(scratch.layout.list().unwrap(), []);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let first = runtime.block_on(
            scratch
                .volumes
                .mint_first(checkout.clone(), checkout.clone()),
        );
        let dropped = minted(&first);
        drop(runtime);
        let entered = handle.enter();
        drop(first);
        drop(entered);
        assert!(!scratch.layout.image(&dropped.id).exists());
        assert_eq!(scratch.layout.list().unwrap(), []);
    }

    /// A collection snapshots main's old link before a land, then runs both while the land
    /// holds its volume quiet and after main adopts it. Neither may take the adopted volume.
    /// If main's link later names a lost volume, opening settles it by reforking its seed.
    /// This is a publication/GC race; physical growth is covered by the adoption scenario.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_a_collection_planned_before_a_land_adopted_never_takes_the_adopted_volume() {
        let scratch = Scratch::directory("build-adopt-rm");
        let main = owner("main", '0');
        let (main_checkout, main_live, _) = scratch.linked_checkout("main", 1);
        let (topic_checkout, topic, topic_mount) = scratch.linked_checkout("topic", 1);
        fs::write(topic_mount.join("warm-cache"), b"landed build state\n").unwrap();
        // What the `rm` of the landing workspace read: every image, then main, mounted, linking
        // its own volume; the landing workspace already retired.
        let rm_listed = scratch.layout.list().unwrap();
        let rm_saw = || Links {
            volumes: [main_live.clone()].into_iter().collect(),
            owners: [main.clone()].into_iter().collect(),
            checkouts: [(main.name.clone(), main_checkout.clone())]
                .into_iter()
                .collect(),
            ..Links::default()
        };
        let kept = |collection: &Collection| {
            collection
                .deferred
                .iter()
                .find(|deferred| deferred.path == scratch.layout.image(&topic))
                .map(|deferred| deferred.deferral.clone())
        };

        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout.clone(),
            )
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        let during = scratch
            .volumes
            .collect(rm_listed.clone(), rm_saw(), false)
            .await
            .unwrap();
        assert!(
            scratch.layout.image(&topic).exists(),
            "a collection took the landing volume while its land held it quiet"
        );
        assert!(
            matches!(kept(&during), Some(Deferral::Held(_))),
            "{:?}",
            during.deferred
        );
        let tree = GitOid::new("a".repeat(40)).unwrap();
        let locked = scratch.lock(&main).await;
        let frozen = scratch
            .volumes
            .freeze_seed(&locked, &quiet, tree.clone())
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        assert_eq!(retired(&scratch.volumes, frozen).await, Froze::Frozen);
        let closed = scratch
            .volumes
            .close_target(main_checkout.clone())
            .await
            .unwrap()
            .expect("nothing holds the target's volume");
        let adopted = scratch
            .volumes
            .adopt(&locked, &quiet, closed, main_checkout.clone(), tree)
            .await
            .unwrap()
            .expect("nothing opened the target's volume since");
        drop(quiet);
        scratch
            .volumes
            .release_previous(adopted.previous)
            .await
            .unwrap();

        let after = scratch
            .volumes
            .collect(rm_listed, rm_saw(), false)
            .await
            .unwrap();
        assert_eq!(
            kept(&after),
            Some(Deferral::Linked(main.name.clone())),
            "{:?}",
            after.deferred
        );
        assert_eq!(
            scratch.layout.linked(&main_checkout).unwrap(),
            Some(topic.clone())
        );
        assert_eq!(
            scratch.layout.resolve_link(&main.name, &topic).unwrap(),
            crate::build_volume::LinkResolution::Keep,
            "main owns the volume it adopted"
        );
        assert!(scratch.layout.image(&topic).exists());
        assert_eq!(
            fs::read_link(scratch.layout.image(&topic)).unwrap(),
            topic_mount,
            "main's adopted payload is still bound"
        );
        assert_eq!(
            fs::read(topic_mount.join("warm-cache")).unwrap(),
            b"landed build state\n"
        );

        // The incident's end state: main's link names a volume that is gone.
        let released = scratch
            .host
            .release_build_volume(&scratch.layout, &topic)
            .unwrap();
        assert!(
            matches!(released, BuildVolumeRelease::Deleted { .. }),
            "{released:?}"
        );
        scratch
            .host
            .settle_build_link(
                &scratch.layout,
                &main.name,
                &main.incarnation,
                &main_checkout,
                &topic,
            )
            .expect("main reforks from the seed its land froze");
        let relinked = scratch
            .layout
            .linked(&main_checkout)
            .unwrap()
            .expect("main links a volume again");
        assert_ne!(relinked, topic);
        assert_eq!(
            scratch.layout.resolve_link(&main.name, &relinked).unwrap(),
            crate::build_volume::LinkResolution::Keep,
            "main owns the volume it reforked"
        );
        let remount = scratch.layout.mount(&relinked);
        assert_eq!(
            fs::read_link(scratch.layout.image(&relinked)).unwrap(),
            remount,
            "main's reforked payload is bound"
        );
        assert_eq!(
            fs::read(remount.join("warm-cache")).unwrap(),
            b"landed build state\n",
            "the refork recovers the landed seed's contents"
        );
        scratch.release_all();
    }

    /// A land into main while a `cowshed fork` of main runs: the fork reads main's seed and link
    /// under main's image lock, and clones that seed after. A land that froze main's seed and
    /// released main's previous volume in between deleted both under the new, which failed on
    /// the seed it read ('sync source image …: No such file or directory'). The land waits for
    /// main's image lock before it freezes main's seed, so the new always succeeds, and the land
    /// goes on once the new has linked its fork.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_a_new_of_the_target_succeeds_whatever_a_concurrent_land_does() {
        let scratch = Scratch::directory("build-land-new");
        let main = owner("main", '0');
        let (main_checkout, main_live, _) = scratch.linked_checkout("main", 1);
        let first_seed = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &first_seed,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                )),
            )
            .expect("seed main");
        let (topic_checkout, topic, _) = scratch.linked_checkout("topic", 1);
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&topic_checkout),
                scratch.stopped(&topic_checkout).await,
                main_checkout.clone(),
            )
            .await
            .unwrap()
            .expect("nothing holds the landing volume");
        let closed = scratch
            .volumes
            .close_target(main_checkout.clone())
            .await
            .unwrap()
            .expect("nothing holds the target's volume");
        let tree = GitOid::new("a".repeat(40)).unwrap();

        // The new: main's image lock, as its create takes it, then main's seed and link.
        let new_holds = scratch
            .host
            .lock_images(
                &[scratch.image_lock(&main.name)],
                LockMode::Wait,
                &LockHolding::of(LockOperation::Create, &WorkspaceName::new("fresh").unwrap()),
            )
            .unwrap()
            .expect("a blocking lock");
        assert_eq!(
            scratch
                .volumes
                .refresh_seed(
                    main.clone(),
                    main_checkout.clone(),
                    unlanded_aim(),
                    LockHolding::of(LockOperation::Create, &WorkspaceName::new("fresh").unwrap()),
                )
                .await
                .unwrap()
                .outcome,
            Refresh::Kept
        );
        let (seed, record) = scratch
            .layout
            .seed_of(&main.name, &main.incarnation)
            .unwrap()
            .expect("main has a seed");
        assert_eq!(seed, first_seed);

        // The land reaches main's seed and link here, finds main's image lock held, and waits.
        let contended = match scratch
            .volumes
            .try_lock_target(
                main.clone(),
                scratch.image_lock(&main.name),
                LockHolding::of(LockOperation::Land, &WorkspaceName::new("lander").unwrap()),
            )
            .await
            .unwrap()
        {
            Ok(_) => panic!("the land took main's image lock while a new of main held it"),
            Err(contended) => contended,
        };
        let (waiting, waits) = tokio::sync::oneshot::channel();
        let land = {
            let volumes = scratch.volumes.clone();
            let main_checkout = main_checkout.clone();
            tokio::spawn(async move {
                waiting.send(()).expect("the new waits for the land");
                let locked = contended.wait().await?;
                let frozen = volumes
                    .freeze_seed(&locked, &quiet, tree.clone())
                    .await?
                    .expect("nothing holds the landing volume");
                let Freeze::Frozen { superseded } = frozen else {
                    panic!("nothing holds the landing volume: {frozen:?}");
                };
                let adopted = volumes
                    .adopt(&locked, &quiet, closed, main_checkout, tree)
                    .await?
                    .expect("nothing opened the target's volume since");
                drop(quiet);
                drop(locked);
                volumes.retire_superseded(superseded).await;
                volumes.release_previous(adopted.previous).await
            })
        };
        // The new goes on only once the land runs: whatever the land does next, it does while
        // the new clones.
        waits.await.expect("the land runs");

        // The rest of the new's fork, from what it read under main's lock.
        let lane = owner("lane", '1');
        let staged = scratch.root.path().join("staged-lane");
        fs::create_dir_all(staged.join(".cowshed")).unwrap();
        let started = Instant::now();
        let captured = scratch
            .host
            .capture_fork_build_volume(
                &scratch.layout,
                &seed,
                &registered(BuildVolumeRecord::new(record.tree.clone(), linked("lane"))),
            )
            .expect("the new captures the seed it read under main's image lock");
        let own_seed = BuildVolumeId::mint();
        let (live, mount) = scratch
            .host
            .ready_fork_build_volume(
                &scratch.layout,
                captured,
                Some((
                    &own_seed,
                    &registered(BuildVolumeRecord::new(
                        record.tree,
                        BuildVolumeRole::Seed {
                            target: lane.name.clone(),
                            incarnation: lane.incarnation.clone(),
                        },
                    )),
                )),
            )
            .expect("the new readies its capture");
        assert!(
            scratch.layout.image(&main_live).exists(),
            "a land released the volume main linked while a new of main held main's image lock"
        );
        let forked = scratch
            .volumes
            .finish_fork(
                Some(PreparedFork {
                    seed: seed.clone(),
                    own_seed,
                    live: live.clone(),
                    mount,
                    started,
                }),
                lane.clone(),
                staged.clone(),
            )
            .await
            .expect("the new finishes its fork");
        assert_eq!(forked, Some(live.clone()));
        assert!(
            !land.is_finished(),
            "the land went on while a new of main held main's image lock"
        );
        drop(new_holds);
        land.await
            .unwrap()
            .expect("the land, once the new let go of main");

        assert_eq!(scratch.layout.linked(&staged).unwrap(), Some(live));
        assert!(
            scratch
                .layout
                .seed_of(&lane.name, &lane.incarnation)
                .unwrap()
                .is_some(),
            "the new seeded its workspace"
        );
        assert_eq!(scratch.layout.linked(&main_checkout).unwrap(), Some(topic));
        let (main_seed, _) = scratch
            .layout
            .seed_of(&main.name, &main.incarnation)
            .unwrap()
            .expect("the land froze main's seed");
        assert_ne!(main_seed, first_seed);
        assert!(!scratch.layout.image(&first_seed).exists());
        assert!(!scratch.layout.image(&main_live).exists());
        scratch.release_all();
    }

    /// GC over a record whose image is gone terminates: the record is released, journaled as
    /// `release-imageless-build-volume` naming the record and the missing image, and the next
    /// pass finds nothing left to decide.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_gc_releases_a_record_whose_image_is_gone_and_journals_it() {
        use crate::storage::deletion_log::{
            DELETION_LOG_FILE, DeletionKind, DeletionLogEntry, DeletionOp,
        };
        let scratch = Scratch::directory("imageless-record-gc");
        let absent = BuildVolumeId::mint();
        fs::create_dir_all(scratch.layout.images()).unwrap();
        scratch
            .layout
            .write_record(
                &absent,
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Linked {
                        checkout: WorkspaceName::new("retired").unwrap(),
                    },
                ),
            )
            .unwrap();
        let ids = scratch.layout.collection_ids().unwrap();
        assert_eq!(ids, std::slice::from_ref(&absent));
        let collected = scratch
            .volumes
            .collect(ids, Links::default(), false)
            .await
            .unwrap();
        assert_eq!(
            (collected.reclaimed, collected.released_bytes),
            (1, 0),
            "{:?}",
            collected.deferred
        );
        assert!(collected.deferred.is_empty(), "{:?}", collected.deferred);
        assert!(!scratch.layout.record(&absent).exists());
        assert_eq!(
            scratch.layout.collection_ids().unwrap(),
            [],
            "the next pass has nothing left to retry"
        );
        let log =
            fs::read_to_string(scratch.layout.project_root().join(DELETION_LOG_FILE)).unwrap();
        let entries = log
            .lines()
            .map(|line| serde_json::from_str::<DeletionLogEntry>(line).unwrap())
            .map(|entry| {
                (
                    entry.op,
                    entry.kind,
                    entry.workspace,
                    entry.image,
                    entry.artifact,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            entries,
            [(
                DeletionOp::ReleaseImagelessBuildVolume,
                DeletionKind::BuildVolume,
                "retired".to_owned(),
                Some(scratch.layout.image(&absent)),
                scratch.layout.record(&absent),
            )]
        );
        scratch.release_all();
    }

    /// Every build volume release is a line in the project's deletion log, saying whether the
    /// volume was a seed and whose its sidecar said it was: a land's release of main's previous
    /// volume once left no trace at all, and then named nobody; superseded, it names main. A held
    /// volume is not released, and leaves no line.
    #[cfg(target_os = "macos")]
    #[test]
    fn directory_every_build_volume_release_is_journaled() {
        use crate::storage::deletion_log::{
            DELETION_LOG_FILE, DeletionKind, DeletionLogEntry, DeletionOp,
        };
        let scratch = Scratch::directory("build-release-journal");
        let main = owner("main", '0');
        let (_, live, _) = scratch.linked_checkout("main", 1);
        let clone = |role: BuildVolumeRole| {
            let id = BuildVolumeId::mint();
            scratch
                .host
                .clone_build_volume(
                    &scratch.layout,
                    &live,
                    &id,
                    &registered(BuildVolumeRecord::new(None, role)),
                )
                .expect("clone main's volume");
            id
        };
        let seed = clone(BuildVolumeRole::Seed {
            target: main.name.clone(),
            incarnation: main.incarnation.clone(),
        });
        let superseded = clone(superseded("main"));
        let job = scratch.layout.admit(&live).expect("a job's hold");
        assert!(matches!(
            scratch.host.release_build_volume(&scratch.layout, &live),
            Ok(BuildVolumeRelease::Held { .. })
        ));
        drop(job);
        for id in [&seed, &superseded, &live] {
            let release = scratch.host.release_build_volume(&scratch.layout, id);
            assert!(
                matches!(release, Ok(BuildVolumeRelease::Deleted { .. })),
                "{release:?}"
            );
        }

        let log =
            fs::read_to_string(scratch.layout.project_root().join(DELETION_LOG_FILE)).unwrap();
        let entries = log
            .lines()
            .map(|line| serde_json::from_str::<DeletionLogEntry>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(
            entries
                .iter()
                .all(|entry| entry.image.as_ref() == Some(&entry.artifact)),
            "{entries:?}"
        );
        let journaled = entries
            .into_iter()
            .map(|entry| (entry.op, entry.kind, entry.workspace, entry.artifact))
            .collect::<Vec<_>>();
        let release = |kind, workspace: &str, id: &BuildVolumeId| {
            (
                DeletionOp::ReleaseBuildVolume,
                kind,
                workspace.to_owned(),
                scratch.layout.image(id),
            )
        };
        assert_eq!(
            journaled,
            vec![
                release(DeletionKind::BuildSeed, "main", &seed),
                release(DeletionKind::BuildVolume, "main", &superseded),
                release(DeletionKind::BuildVolume, "main", &live),
            ]
        );
    }

    /// A target's previous volume a job still holds after an adoption stays recorded as the
    /// target's at its incarnation: the land's own release is refused by the hold and touches
    /// nothing, no collection takes it while the job runs, and the release a job's end owes it
    /// deletes it the moment the hold is gone. A volume recorded anything else is not that
    /// release's to decide.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_a_held_previous_volume_keeps_its_owner_until_the_jobs_end_releases_it() {
        let scratch = Scratch::directory("build-superseded-held");
        let main = owner("main", '0');
        let (_, previous, _) = scratch.linked_checkout("main", 1);
        let (_, live, _) = scratch.linked_checkout("topic", 1);
        let record = scratch.layout.read_record(&previous).unwrap();
        scratch
            .layout
            .write_record(
                &previous,
                &BuildVolumeRecord {
                    role: BuildVolumeRole::Superseded {
                        owner: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                    ..record
                },
            )
            .unwrap();
        let superseded = scratch.layout.read_record(&previous).unwrap();
        let job = scratch
            .layout
            .admit(&previous)
            .expect("a job admitted before the move");

        scratch
            .volumes
            .release_previous(Superseded {
                id: previous.clone(),
                owner: main.clone(),
            })
            .await
            .expect("a held previous volume is not the land's failure");
        assert_eq!(scratch.layout.read_record(&previous).unwrap(), superseded);
        assert!(scratch.layout.image(&previous).exists());
        let collected = scratch
            .volumes
            .collect(
                scratch.layout.list().unwrap(),
                Links {
                    volumes: [live.clone()].into_iter().collect(),
                    owners: [main.clone()].into_iter().collect(),
                    ..Links::default()
                },
                false,
            )
            .await
            .unwrap();
        assert!(
            collected.deferred.iter().any(|deferred| {
                deferred.path == scratch.layout.image(&previous)
                    && matches!(deferred.deferral, Deferral::Held(_))
            }),
            "{:?}",
            collected.deferred
        );
        assert_eq!(scratch.layout.read_record(&previous).unwrap(), superseded);

        // The job's end: a release of anything not superseded is no business of this one.
        scratch
            .volumes
            .release_superseded(live.clone())
            .await
            .unwrap();
        assert!(scratch.layout.image(&live).exists());
        scratch
            .volumes
            .release_superseded(previous.clone())
            .await
            .unwrap();
        assert!(
            scratch.layout.image(&previous).exists(),
            "still held: the release declines and changes nothing"
        );
        assert_eq!(scratch.layout.read_record(&previous).unwrap(), superseded);
        drop(job);
        scratch
            .volumes
            .release_superseded(previous.clone())
            .await
            .unwrap();
        assert!(!scratch.layout.image(&previous).exists());
        assert!(
            scratch
                .layout
                .read_record_present(&previous)
                .unwrap()
                .is_none()
        );
        scratch.release_all();
    }

    /// A retirement releases everything its workspace owns at its incarnation before its image
    /// goes — the live volume its checkout links, its seed, a previous volume superseded under
    /// it, a first touch it registered and never finished — and nothing another incarnation of
    /// the same name, or another workspace, owns. A volume a job holds refuses the retirement
    /// and keeps its owner record.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_retirement_releases_every_volume_it_owns_and_refuses_while_one_is_held() {
        let scratch = Scratch::directory("build-retire-owned");
        let topic = owner("topic", '1');
        let (checkout, live, _) = scratch.linked_checkout("topic", 1);
        let clone = |role: BuildVolumeRole| {
            let id = BuildVolumeId::mint();
            scratch
                .host
                .clone_build_volume(
                    &scratch.layout,
                    &live,
                    &id,
                    &registered(BuildVolumeRecord::new(None, role)),
                )
                .expect("clone topic's volume");
            id
        };
        let seed = clone(BuildVolumeRole::Seed {
            target: topic.name.clone(),
            incarnation: topic.incarnation.clone(),
        });
        let previous = clone(BuildVolumeRole::Superseded {
            owner: topic.name.clone(),
            incarnation: topic.incarnation.clone(),
        });
        let other_incarnation = clone(BuildVolumeRole::Seed {
            target: topic.name.clone(),
            incarnation: WorkspaceIncarnation::new("2".repeat(32)).unwrap(),
        });
        let unfinished = BuildVolumeId::mint();
        drop(
            scratch
                .layout
                .register(
                    &unfinished,
                    &Registration::new(
                        BuildVolumeRecord::new(None, linked("topic")),
                        Registrant::Workspace {
                            incarnation: topic.incarnation.clone(),
                        },
                    ),
                )
                .unwrap(),
        );
        let (_, main_live, _) = scratch.linked_checkout("main", 1);

        let job = scratch
            .layout
            .admit(&previous)
            .expect("a job on the previous volume");
        let refusal = scratch
            .volumes
            .release_owned(topic.clone(), checkout.clone())
            .await
            .expect_err("a held volume refuses the retirement");
        assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
        assert!(refusal.message.contains(previous.as_str()), "{refusal}");
        assert!(
            scratch.layout.read_record(&previous).unwrap().role
                == BuildVolumeRole::Superseded {
                    owner: topic.name.clone(),
                    incarnation: topic.incarnation.clone(),
                },
            "the refused volume keeps its owner"
        );
        drop(job);
        scratch
            .volumes
            .release_owned(topic.clone(), checkout)
            .await
            .expect("the same retirement again");
        for id in [&live, &seed, &previous, &unfinished] {
            assert!(
                scratch.layout.read_record_present(id).unwrap().is_none(),
                "{id} was topic's at its incarnation"
            );
            assert!(!scratch.layout.image(id).exists(), "{id}");
        }
        for id in [&other_incarnation, &main_live] {
            assert!(
                scratch.layout.read_record_present(id).unwrap().is_some(),
                "{id}"
            );
        }
        scratch.release_all();
    }

    /// A creation registered before its image existed is found again by its exact id: the
    /// open after a crash releases one whose registrant is gone — a capture that died after its
    /// bytes, a fork whose intent no longer forms, a registration with no bytes at all — and
    /// leaves one a forming intent or a held creation still owns, and an unregistered image,
    /// which nothing proves anyone's.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_next_open_releases_interrupted_registrations_by_their_ids() {
        let scratch = Scratch::directory("build-registered-recovery");
        let main = owner("main", '0');
        let (_, live, _) = scratch.linked_checkout("main", 1);
        let registration = |role: BuildVolumeRole, by: Registrant| {
            Registration::new(BuildVolumeRecord::new(None, role), by)
        };
        let seed_role = BuildVolumeRole::Seed {
            target: main.name.clone(),
            incarnation: main.incarnation.clone(),
        };
        // A capture that died after its bytes and before its publication.
        let capture = BuildVolumeId::mint();
        let capture_registration = registration(
            seed_role.clone(),
            Registrant::Workspace {
                incarnation: main.incarnation.clone(),
            },
        );
        let creating = scratch
            .layout
            .register(&capture, &capture_registration)
            .unwrap();
        drop(creating);
        fs::create_dir(scratch.layout.image(&capture)).unwrap();
        fs::write(scratch.layout.image(&capture).join("partial"), b"torn").unwrap();
        // A registration that died before any byte.
        let bare = BuildVolumeId::mint();
        drop(
            scratch
                .layout
                .register(&bare, &registration(linked("gone"), Registrant::Intent))
                .unwrap(),
        );
        // An earlier crashed fork of a name a later fork now forms: the name is no authority.
        let forming = BuildVolumeId::mint();
        drop(
            scratch
                .layout
                .register(&forming, &registration(linked("lane"), Registrant::Intent))
                .unwrap(),
        );
        // An unfinished re-adoption's journaled build id, and a creation still running, which
        // holds its own: each keeps its exact id.
        let journaled = BuildVolumeId::mint();
        drop(
            scratch
                .layout
                .register(
                    &journaled,
                    &registration(linked("main"), Registrant::Intent),
                )
                .unwrap(),
        );
        let running = BuildVolumeId::mint();
        let still_creating = scratch
            .layout
            .register(&running, &registration(seed_role, Registrant::Intent))
            .unwrap();
        let unregistered = BuildVolumeId::mint();
        fs::create_dir(scratch.layout.image(&unregistered)).unwrap();

        let registered = scratch.volumes.registered().await.unwrap();
        let mut expected = vec![
            capture.clone(),
            bare.clone(),
            forming.clone(),
            journaled.clone(),
            running.clone(),
        ];
        expected.sort();
        assert_eq!(registered, expected, "exact ids from the records alone");
        let links = Links {
            volumes: [live.clone()].into_iter().collect(),
            owners: [main.clone()].into_iter().collect(),
            creating: [name("lane")].into_iter().collect(),
            journaled: [journaled.clone()].into_iter().collect(),
            ..Links::default()
        };
        let recovered = scratch
            .volumes
            .recover_registered(registered, links)
            .await
            .unwrap();
        assert_eq!(recovered.reclaimed, 3, "{:?}", recovered.deferred);
        for id in [&capture, &bare, &forming] {
            assert!(!scratch.layout.image(id).exists(), "{id}");
            assert!(
                scratch.layout.read_record_present(id).unwrap().is_none(),
                "{id}"
            );
        }
        assert!(
            scratch
                .layout
                .read_record_present(&journaled)
                .unwrap()
                .is_some()
        );
        assert!(
            recovered
                .deferred
                .iter()
                .all(|deferred| deferred.deferral.is_owned())
        );
        assert!(
            scratch
                .layout
                .read_record_present(&running)
                .unwrap()
                .is_some()
        );
        assert!(scratch.layout.image(&unregistered).exists());
        assert!(
            recovered.deferred.iter().any(|deferred| {
                deferred.path == scratch.layout.image(&running)
                    && matches!(deferred.deferral, Deferral::Held(_))
            }),
            "{:?}",
            recovered.deferred
        );
        assert_eq!(
            scratch
                .layout
                .seed_of(&main.name, &main.incarnation)
                .unwrap(),
            None,
            "no registered capture ever became main's seed"
        );
        drop(still_creating);
        fs::remove_dir(scratch.layout.image(&unregistered)).unwrap();
        scratch.release_all();
    }

    /// A clone that fails after its registration releases what it registered: no image and no
    /// record of it are left behind, and its source keeps its own.
    #[cfg(target_os = "macos")]
    #[test]
    fn directory_a_clone_refused_after_registration_leaves_nothing_registered() {
        let scratch = Scratch::directory("build-clone-refused");
        let (_, live, _) = scratch.linked_checkout("main", 1);
        let destination = BuildVolumeId::mint();
        // Its bytes would predate their owner: the registration itself refuses.
        fs::create_dir(scratch.layout.image(&destination)).unwrap();
        let error = scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &destination,
                &registered(BuildVolumeRecord::new(None, linked("topic"))),
            )
            .expect_err("an image already at the id refuses");
        assert!(error.to_string().contains("predate"), "{error}");
        assert!(
            scratch
                .layout
                .read_record_present(&destination)
                .unwrap()
                .is_none()
        );
        fs::remove_dir(scratch.layout.image(&destination)).unwrap();
        // A source with no owner record refuses before anything is registered.
        let ownerless = BuildVolumeId::mint();
        fs::create_dir(scratch.layout.image(&ownerless)).unwrap();
        let target = BuildVolumeId::mint();
        assert!(
            scratch
                .host
                .clone_build_volume(
                    &scratch.layout,
                    &ownerless,
                    &target,
                    &registered(BuildVolumeRecord::new(None, linked("topic"))),
                )
                .is_err()
        );
        assert!(!scratch.layout.image(&target).exists());
        assert!(
            scratch
                .layout
                .read_record_present(&target)
                .unwrap()
                .is_none()
        );
        assert!(scratch.layout.read_record(&live).unwrap().is_published());
        fs::remove_dir(scratch.layout.image(&ownerless)).unwrap();
        scratch.release_all();
    }

    /// Seed compaction (16_build_volumes.md, "Seed compaction") over a real fragmented seed: a
    /// stop removes the copy and leaves the seed; a held copy asks again until it may go on; a
    /// copy publishes as main's latest seed with far fewer extents, the same bytes and holes and
    /// the copied seed's freshness mark, and the old seed goes to the build journal. A seed a land
    /// publishes between the copy and its publication, or during the copy, is caught up to:
    /// only what it changed is written, and the copy is published in its place with its mark.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_seed_compaction_publishes_a_fragmented_seed_and_catches_a_replaced_one_up() {
        use crate::storage::apfs::native::{SeedCatchUp, SeedCompaction};
        const MIB: u64 = 1024 * 1024;
        let scratch = Scratch::new("seed-compaction");
        let target = owner("main", '0');
        let seed_registration = |tree: char| {
            Registration::new(
                BuildVolumeRecord::new(
                    Some(GitOid::new(tree.to_string().repeat(40)).unwrap()),
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
                Registrant::Workspace {
                    incarnation: target.incarnation.clone(),
                },
            )
        };
        let seed = BuildVolumeId::mint();
        let registration = seed_registration('a');
        let creating = scratch.layout.register(&seed, &registration).unwrap();
        let fragmenting = scratch.root.path().join("fragmenting");
        fs::create_dir_all(&fragmenting).unwrap();
        let (fragmented, _) =
            crate::storage::apfs::extents::tests::fragmented_sparse_file(&fragmenting);
        fs::rename(&fragmented, scratch.layout.image(&seed)).unwrap();
        scratch.layout.publish(&seed, &registration).unwrap();
        drop(creating);
        let bytes = fs::read(scratch.layout.image(&seed)).unwrap();
        let written = fs::metadata(scratch.layout.image(&seed))
            .unwrap()
            .modified()
            .unwrap();
        let compacting = scratch.layout.compacting();
        let left = || {
            let mut left = fs::read_dir(&compacting)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .filter(|name| name.ends_with(".dmg"))
                .collect::<Vec<_>>();
            left.sort();
            left
        };
        let stage = || async {
            let locked = scratch.lock(&target).await;
            scratch
                .volumes
                .stage_compaction(&locked)
                .await
                .unwrap()
                .expect("main has a seed")
        };

        let stopped = scratch
            .volumes
            .copy_compaction(
                stage().await,
                Box::new(|| super::Admission::Stop("a land holds a turn".to_owned())),
            )
            .await
            .unwrap();
        assert!(
            matches!(&stopped, SeedCompaction::Stopped { why, .. } if why == "a land holds a turn"),
            "{stopped:?}"
        );
        assert_eq!(
            left(),
            [format!("{seed}.dmg")],
            "a stop removes the copy; the seed's clone stays for the caller to release"
        );
        let clone_length = fs::metadata(compacting.join(format!("{seed}.dmg")))
            .unwrap()
            .len();
        scratch
            .volumes
            .release_compaction_leftovers()
            .await
            .unwrap();
        assert_eq!(left(), Vec::<String>::new());
        // The gateway's cleanup is the only cutter on the container: the compaction renames its
        // leftover into build/.deleting, whole, and frees none of it.
        let handed: Vec<u64> = fs::read_dir(scratch.layout.deleting())
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .collect();
        assert_eq!(handed, [clone_length], "handed off whole, never cut");
        assert_eq!(
            scratch
                .layout
                .seed_of(&target.name, &target.incarnation)
                .unwrap()
                .unwrap()
                .0,
            seed
        );

        let asked = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counted = std::sync::Arc::clone(&asked);
        let SeedCompaction::Copied(compacted) = scratch
            .volumes
            .copy_compaction(
                stage().await,
                Box::new(move || {
                    // Held twice, as while a land runs, then let go on.
                    if counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed) < 2 {
                        super::Admission::Wait
                    } else {
                        super::Admission::Copy
                    }
                }),
            )
            .await
            .unwrap()
        else {
            panic!("nothing stops the copy");
        };
        assert_eq!(
            asked.load(std::sync::atomic::Ordering::Relaxed),
            3,
            "a held copy asks again until it may go on"
        );
        assert!(compacted.before.get() >= 256, "{compacted:?}");
        assert!(
            compacted.after.get() * 10 <= compacted.before.get(),
            "{compacted:?}"
        );
        assert_eq!(compacted.bytes, 8 * 1024 * 1024, "only the data is copied");
        let locked = scratch.lock(&target).await;
        let Published::Replaced {
            seed: replaced,
            replacement,
            superseded,
        } = scratch
            .volumes
            .publish_compaction(&locked, compacted)
            .await
            .unwrap()
        else {
            panic!("main's seed is still the one copied");
        };
        drop(locked);
        scratch.volumes.retire_superseded(superseded).await;
        assert_eq!(replaced, seed);
        let (latest, record) = scratch
            .layout
            .seed_of(&target.name, &target.incarnation)
            .unwrap()
            .unwrap();
        assert_eq!(latest, replacement);
        assert!(record.is_published());
        assert_eq!(record.tree, Some(GitOid::new("a".repeat(40)).unwrap()));
        let image = scratch.layout.image(&replacement);
        assert!(
            fs::read(&image).unwrap() == bytes,
            "the copy holds the seed's bytes"
        );
        let metadata = fs::metadata(&image).unwrap();
        assert_eq!(
            metadata.modified().unwrap(),
            written,
            "the seed's freshness mark"
        );
        assert!(
            metadata.blocks() * 512 <= 9 * 1024 * 1024,
            "holes stay holes: {} bytes allocated",
            metadata.blocks() * 512
        );
        assert!(
            !scratch.layout.image(&seed).exists(),
            "the old seed is retired"
        );
        assert!(scratch.layout.read_record_present(&seed).unwrap().is_none());
        assert_eq!(
            left(),
            [format!("{seed}.dmg")],
            "the copied seed's clone stays for the caller to release"
        );
        scratch
            .volumes
            .release_compaction_leftovers()
            .await
            .unwrap();
        assert_eq!(left(), Vec::<String>::new());

        // A land publishes main's next seed: `tree` names it, `fill` makes its image.
        let land = |tree: char, fill: &dyn Fn(&Path)| {
            let landed = BuildVolumeId::mint();
            // Records name whole seconds, so a land in the same second as the compaction's
            // publication would tie it: date the land's seed after it.
            let registration = Registration::new(
                BuildVolumeRecord {
                    created_at: format!(
                        "2099-01-01T00:00:{:02}Z",
                        u32::from(tree) - u32::from('a')
                    ),
                    ..seed_registration(tree).registered().clone()
                },
                Registrant::Workspace {
                    incarnation: target.incarnation.clone(),
                },
            );
            let creating = scratch.layout.register(&landed, &registration).unwrap();
            fill(&scratch.layout.image(&landed));
            scratch.layout.publish(&landed, &registration).unwrap();
            drop(creating);
            let image = scratch.layout.image(&landed);
            let written = fs::metadata(&image).unwrap().modified().unwrap();
            (landed, fs::read(&image).unwrap(), written)
        };
        // Publication finds `landed` main's latest, the copy is caught up to it, writing
        // `written` bytes, and published in its place with its bytes and freshness mark.
        let catch_up = |compacted: crate::storage::apfs::native::CompactedSeed,
                        landed: BuildVolumeId,
                        bytes: Vec<u8>,
                        mark: std::time::SystemTime,
                        written: u64| {
            let (scratch, target, left) = (&scratch, &target, &left);
            async move {
                let locked = scratch.lock(target).await;
                let Published::Behind { compacted, next } = scratch
                    .volumes
                    .publish_compaction(&locked, compacted)
                    .await
                    .unwrap()
                else {
                    panic!("a land replaced the copied seed");
                };
                drop(locked);
                assert_eq!(next.seed(), &landed);
                let SeedCatchUp::Caught {
                    compacted,
                    written: caught,
                    ..
                } = scratch
                    .volumes
                    .catch_up_compaction(compacted, next, Box::new(|| super::Admission::Copy))
                    .await
                    .unwrap()
                else {
                    panic!("nothing stops the catch-up");
                };
                assert_eq!(caught, written, "only what the land changed is written");
                let locked = scratch.lock(target).await;
                let Published::Replaced {
                    seed: replaced,
                    replacement,
                    superseded,
                } = scratch
                    .volumes
                    .publish_compaction(&locked, compacted)
                    .await
                    .unwrap()
                else {
                    panic!("the land's seed is still main's latest");
                };
                drop(locked);
                scratch.volumes.retire_superseded(superseded).await;
                assert_eq!(replaced, landed);
                let image = scratch.layout.image(&replacement);
                assert!(
                    fs::read(&image).unwrap() == bytes,
                    "the copy holds the land's bytes"
                );
                assert_eq!(
                    fs::metadata(&image).unwrap().modified().unwrap(),
                    mark,
                    "the land's seed's freshness mark"
                );
                assert!(
                    !scratch.layout.image(&landed).exists(),
                    "the land's seed is retired"
                );
                scratch
                    .volumes
                    .release_compaction_leftovers()
                    .await
                    .unwrap();
                assert_eq!(left(), Vec::<String>::new());
                replacement
            }
        };

        // Between the copy and its publication: a land whose seed shares all but one MiB.
        let SeedCompaction::Copied(compacted) = scratch
            .volumes
            .copy_compaction(stage().await, Box::new(|| super::Admission::Copy))
            .await
            .unwrap()
        else {
            panic!("nothing stops the copy");
        };
        let (landed, bytes, mark) = land('b', &|image| {
            assert!(
                std::process::Command::new("/bin/cp")
                    .arg("-c")
                    .arg(scratch.layout.image(&replacement))
                    .arg(image)
                    .status_bounded()
                    .expect("cp -c")
                    .success()
            );
            let file = fs::OpenOptions::new().write(true).open(image).unwrap();
            std::os::unix::fs::FileExt::write_all_at(&file, &vec![0xa5; 1024 * 1024], 0).unwrap();
        });
        catch_up(compacted, landed, bytes, mark, MIB).await;

        // During the copy: it is not stopped, and catches up to a land that shares nothing.
        let source = stage().await;
        let (landed, bytes, mark) = land('c', &|image| fs::write(image, vec![7_u8; 4096]).unwrap());
        let SeedCompaction::Copied(compacted) = scratch
            .volumes
            .copy_compaction(source, Box::new(|| super::Admission::Copy))
            .await
            .unwrap()
        else {
            panic!("a land does not stop the copy");
        };
        catch_up(compacted, landed, bytes, mark, 4096).await;
        scratch.release_all();
    }

    /// Seed compaction's lineage (16_build_volumes.md, "Seed compaction"): main moves onto a clone
    /// of its own seed through the workspace reseed of main from main, which takes main's image
    /// lock once, only while main's volume holds no write after the seed's mark; a later write,
    /// made before the move or while its replacement is cloned, before the seal, refuses the
    /// move as a conflict, keeps the volume and the write, and releases the replacement.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn real_apfs_compaction_moves_main_onto_its_seed_only_without_a_newer_write() {
        let scratch = Scratch::new("compaction-lineage");
        let main = owner("main", '0');
        let (checkout, live, mount) = scratch.linked_checkout("main", 1);
        fs::write(mount.join("in-seed"), b"captured").unwrap();
        let seed = BuildVolumeId::mint();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &live,
                &seed,
                &registered(BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                )),
            )
            .unwrap();
        fs::write(mount.join("after-seed"), b"live work").unwrap();

        // Flushed, as the move's own comparison flushes it: left in the kernel's cache, the write
        // can reach the image between the preview and the move, and the move then refuses its
        // plan as stale (its image allocation changed) before it compares the write's time.
        scratch
            .host
            .build_volume_written(&scratch.layout, &live)
            .unwrap();
        let workspace = reseed_workspace(&scratch, main, checkout.clone());
        let plan = scratch
            .volumes
            .preview_reseed(workspace.clone(), workspace.clone())
            .await
            .unwrap();
        assert_eq!(plan.seed(), &seed);
        let refusal = scratch.volumes.move_onto_seed(plan).await.unwrap_err();
        assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
        assert!(
            refusal.message.contains("after the seed's mark"),
            "{refusal}"
        );
        assert_eq!(
            scratch.layout.linked(&checkout).unwrap(),
            Some(live.clone())
        );
        assert!(mount.join("after-seed").exists(), "the write is kept");

        // A write while the replacement is cloned, after the first comparison.
        let mark = |scratch: &Scratch| {
            let written = scratch
                .host
                .build_volume_written(&scratch.layout, &live)
                .unwrap();
            scratch
                .host
                .mark_seed_written(&scratch.layout, &seed, written)
                .unwrap();
        };
        mark(&scratch);
        let plan = scratch
            .volumes
            .preview_reseed(workspace.clone(), workspace.clone())
            .await
            .unwrap();
        let racing = mount.join("during-clone");
        let write = racing.clone();
        let refusal = scratch
            .volumes
            .move_onto_seed_racing(plan, move || fs::write(write, b"raced").unwrap())
            .await
            .unwrap_err();
        assert_eq!(refusal.code, crate::ErrorCode::Conflict, "{refusal}");
        assert!(
            refusal.message.contains("after the seed's mark"),
            "{refusal}"
        );
        assert_eq!(
            scratch.layout.linked(&checkout).unwrap(),
            Some(live.clone())
        );
        assert_eq!(
            fs::read(&racing).unwrap(),
            b"raced",
            "the raced write is kept"
        );
        let mut left = scratch.layout.list().unwrap();
        left.sort();
        let mut expected = vec![live.clone(), seed.clone()];
        expected.sort();
        assert_eq!(left, expected, "the unpublished replacement is released");

        // The seed holds every write once its mark is the live volume's last.
        mark(&scratch);
        let plan = scratch
            .volumes
            .preview_reseed(workspace.clone(), workspace)
            .await
            .unwrap();
        let moved = scratch.volumes.move_onto_seed(plan).await.unwrap();
        let volume = BuildVolumeId::parse(moved.replacement_volume.as_ref().unwrap()).unwrap();
        assert_eq!(moved.previous_volume, live.to_string());
        assert_eq!(
            scratch.layout.linked(&checkout).unwrap(),
            Some(volume.clone())
        );
        assert_eq!(
            fs::read(scratch.layout.mount(&volume).join("in-seed")).unwrap(),
            b"captured"
        );
        assert!(
            !scratch.layout.image(&live).exists(),
            "main's old volume is released"
        );
        scratch.release_all();
    }
}
