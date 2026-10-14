//! The project side of build volumes (16_build_volumes.md): a fork first refreshes its target's
//! seed for the tree it checks out, from a workspace that landed that tree outside `cowshed land`
//! or from the target's own quiet volume, then clones it; a land quiesces the landing workspace,
//! freezes the target's seed, and moves the target's one link; a rebase carries its target's Nx
//! cache entries into the rebased workspace's volume; and collection deletes what nothing links.
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
    AdoptionSkip, CarrySide, DatabaseHolder, GcCandidate, GcDeferred, GcReason, GitOid, LandSeed,
    NxCarry, RebaseBuildVolume, RebaseCarrySkip, Sha256Digest,
};
use crate::build_volume::migrate::{MintedBeside, MintedVolume};
use crate::build_volume::{
    BuildStateRefresh, BuildVolumeHold, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord,
    BuildVolumeRole, BuildVolumeState, TrackedBuildStateRefusal, cargo, carry, link, nx,
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

mod reseed;
pub(crate) use reseed::ReseedWorkspace;

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
#[derive(Clone, Debug, Default)]
pub(crate) struct Links {
    pub volumes: BTreeSet<BuildVolumeId>,
    pub owners: BTreeSet<Owner>,
    pub detached: BTreeSet<WorkspaceName>,
    pub creating: BTreeSet<WorkspaceName>,
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

/// The target's image lock, held by its land from before it freezes the target's seed until the
/// target's link names the adopted volume and a kept workspace has its clone of the new seed
/// (Land steps 5.6 to 6.4): every fork of the target holds the same lock from before it reads
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
}

impl<H: RuntimeStorageHost> Contended<H> {
    /// Wait until whatever holds the target's image lock lets go of it, and take it.
    pub async fn wait(self) -> Result<TargetLock<H>> {
        let Self {
            volumes,
            target,
            lock,
        } = self;
        let guard = volumes
            .blocking(move |host, _| {
                host.lock_images(&[lock], LockMode::Wait)
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
/// profiles admitted capture (Land step 5.6).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Freeze {
    /// The seed is captured from the guarded landing volume and every older seed retired.
    Frozen,
    /// A conflicting storage claim refused capture; the previous seed stays.
    SealBusy { reason: String },
}

impl From<Freeze> for LandSeed {
    fn from(freeze: Freeze) -> Self {
        match freeze {
            Freeze::Frozen => Self::Frozen,
            Freeze::SealBusy { reason } => Self::SealBusy { reason },
        }
    }
}

/// What [`BuildVolumes::adopt`] did: how long the move took, and the target's previous volume.
#[derive(Debug)]
#[must_use = "its previous volume is released by release_previous"]
pub(crate) struct Adopted {
    pub elapsed_ms: u64,
    pub previous: Unlinked,
}

/// A target's previous build volume once an adoption moved its link off it and recorded it
/// Unlinked: [`BuildVolumes::release_previous`] releases it, and `gc` does if that fails or never
/// runs.
#[derive(Debug)]
#[must_use = "release_previous releases the target's previous build volume"]
pub(crate) struct Unlinked {
    id: BuildVolumeId,
    target: WorkspaceName,
}

/// A carry's first phase, done while the target still ran (Land step 5, "Carry").
#[derive(Debug)]
pub(crate) struct Staging {
    staged: carry::Staged,
    // The source filesystem must survive the gap between stage and commit.
    source_hold: Option<BuildVolumeHold>,
    elapsed: Duration,
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

    /// Whether neither volume was built at the fork's tree: when that tree is the target's
    /// `HEAD^{tree}`, the target's branch moved without a `cowshed land`.
    pub fn unlanded(&self) -> bool {
        self.nearness != Nearness::Equal
            && self
                .seed
                .as_ref()
                .is_none_or(|seed| seed.nearness != Nearness::Equal)
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

/// What a seed refresh (Fork step 2) did, and each live workspace at the fork's tree that did
/// not seed the target, with why.
#[derive(Clone, Debug)]
pub(crate) struct Refreshed {
    pub passed: Vec<(WorkspaceName, LandedSkip)>,
    pub outcome: Refresh,
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
    pub freed_bytes: u64,
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

/// The volume an [`OwnedMint`] holds until its release takes it.
pub(crate) type MintSlot = Arc<Mutex<Option<MintedVolume>>>;

/// The slot's volume, under its lock. The lock guards no invariant a panic can break: a
/// poisoned slot still names the volume or nothing.
fn lock(slot: &MintSlot) -> MutexGuard<'_, Option<MintedVolume>> {
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
    if let Some(volume) = lock(slot).take() {
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
            eprintln!(
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
            eprintln!(
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
                touch(
                    held.clone()
                        .map_or(MintedBeside::Nothing, MintedBeside::Volume),
                )
            }
        }
    }
}

pub(crate) struct BuildVolumes<H: RuntimeStorageHost = Host> {
    host: Arc<H>,
    pub layout: BuildVolumeLayout,
}

impl<H: RuntimeStorageHost> Clone for BuildVolumes<H> {
    fn clone(&self) -> Self {
        Self {
            host: Arc::clone(&self.host),
            layout: self.layout.clone(),
        }
    }
}

impl<H: RuntimeStorageHost> BuildVolumes<H> {
    pub fn new(host: Arc<H>, layout: BuildVolumeLayout) -> Self {
        Self { host, layout }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(&H, &BuildVolumeLayout) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (host, layout) = (Arc::clone(&self.host), self.layout.clone());
        crate::storage::lifecycle::dispatch_blocking(move || work(&host, &layout))
            .await
            .map_err(|error| CowshedError::internal(format!("build volume task failed: {error}")))?
    }

    /// `target`'s image lock at `lock`, for its land's steps that delete what a fork or reseed
    /// of `target` reads ([`TargetLock`]); [`Contended`] when another operation on `target` holds
    /// it now, for the caller to say so before it waits.
    pub async fn try_lock_target(
        &self,
        target: Owner,
        lock: PathBuf,
    ) -> Result<std::result::Result<TargetLock<H>, Contended<H>>> {
        let path = lock.clone();
        let guard = self
            .blocking(move |host, _| host.lock_images(&[path], LockMode::Try).map_err(storage))
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
            }),
        })
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
                                record: BuildVolumeRecord::new(
                                    None,
                                    BuildVolumeRole::Linked {
                                        checkout: owner.name.clone(),
                                    },
                                ),
                            },
                        )
                    })?;
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

    /// Main's first build volume, minted and mounted beside main's own image while `adopt`
    /// copies `source` into it, so the two attaches wait on `storagekitd` once
    /// (16_build_volumes.md, "Targets and seeds"): at the capacity `source`'s `.cowshed.toml`
    /// asks, and none when nothing in `source` can name build state, whose first touch makes no
    /// volume. Nothing links it and it has no record until main's first touch at `checkout` links
    /// it; until then the adopt's unfinished intent keeps collection off it
    /// ([`Links::creating`]). A failed mint is answered, for the first touch to answer in turn if
    /// it needs a volume.
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
                        host.create_build_volume(layout, &id, capacity)
                            .map_err(storage)?;
                        Ok(MintedVolume { id, capacity })
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

    /// Fork steps 2 and 3 (16_build_volumes.md, "Fork"), up to the checkout: `source`'s seed is
    /// first refreshed for the tree `aim` names ([`Self::refresh_seed`]; what it did instead is said
    /// on stderr, and the older seed is forked), then `destination` gets its live volume, a clone
    /// of `source`'s latest seed, mounted, with no daemon record. A source with no seed gives
    /// none. The caller holds `source`'s image lock, which every fork of `source` holds, and runs
    /// this beside the clone of `source`'s image, whose stage [`Self::finish_fork`] then links.
    pub async fn prepare_fork(
        &self,
        source: Owner,
        source_checkout: PathBuf,
        destination: WorkspaceName,
        aim: Aim,
    ) -> Result<Option<PreparedFork>> {
        let refreshed = self
            .refresh_seed(source.clone(), source_checkout, aim)
            .await?;
        say_refresh(&source.name, &destination, &refreshed);
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
            Ok(release) => say_release(&live, "a fork that never staged", &release),
            Err(error) => eprintln!(
                "cowshed: build volume {live} of a fork that never staged stays: {error}; `cowshed gc` retries it"
            ),
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

    /// Rebase carry (16_build_volumes.md, "Rebase carry"): index in the volume `checkout` links
    /// the Nx cache entries the target's volume indexes and it lacks, among those `selections`
    /// names: for each Nx state of the workspace's volume (its `.nx`, relative to the checkout),
    /// the task hashes stock Nx computed for the rebased tree there and the tasks it defers. A
    /// selection that could not be computed is the skip it carries, after the volume skips; a
    /// target whose task databases index no cache entry skips before either side closes. The
    /// copies are staged while both sides run. Then each side's Nx state is closed as a land
    /// closes the target's (its idle daemon stopped, any other holder a skip), and the commit
    /// runs under both sides' Nx open locks once neither database has a holder, so nothing but
    /// the carry writes either while it indexes. Both build holds span staging through
    /// commit: Nx open locks do not fence a reseed or release of either filesystem.
    /// A skip deletes what was staged.
    pub async fn rebase_carry(
        &self,
        checkout: PathBuf,
        target_checkout: PathBuf,
        selections: std::result::Result<BTreeMap<PathBuf, nx::TaskSelection>, RebaseCarrySkip>,
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
            let selections = match selections {
                Ok(selections) => selections,
                Err(reason) => return skipped(reason),
            };
            let _workspace_hold = layout.hold(&workspace).map_err(|error| {
                io(
                    "hold the rebase carry destination",
                    &layout.hold_path(&workspace),
                    &error,
                )
            })?;
            let _target_hold = layout.hold(&target).map_err(|error| {
                io(
                    "hold the rebase carry source",
                    &layout.hold_path(&target),
                    &error,
                )
            })?;
            let started = Instant::now();
            let into = host
                .mount_build_volume(layout, &workspace)
                .map_err(storage)?;
            let into_state = BuildVolumeState::read(&into)?;
            let from = host.mount_build_volume(layout, &target).map_err(storage)?;
            let from_state = BuildVolumeState::read(&from)?;
            // The volume's image is sparse: what it reports free, its store may not hold.
            let staged = carry::stage_rebase(
                &from,
                &from_state,
                &into,
                &into_state,
                &selections,
                layout.images(),
            );
            let staged_in = started.elapsed();
            // Nothing to select from is not nothing missing: say so before closing either side.
            if let Some(states) = staged.unindexed() {
                carry::unstage(&into).map_err(|error| io("unstage the carry", &into, &error))?;
                return skipped(RebaseCarrySkip::Unindexed { states });
            }
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
            let deferred = carried.deferred;
            Ok(RebaseBuildVolume::Carried {
                carried: nx_carry(carried, staged_in + committing.elapsed(), &workspace),
                deferred,
            })
        })
        .await
    }

    /// Land step 5.6, under the target's image lock: close Nx, retain its open locks and the
    /// existing Cargo profile locks through mounted capture and publication, then retire every
    /// older seed. Other contributed cache/index handles do not refuse capture. Quiet retains
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
            let record = BuildVolumeRecord::new(
                Some(tree),
                BuildVolumeRole::Seed {
                    target: target.name.clone(),
                    incarnation: target.incarnation.clone(),
                },
            );
            seal(host, layout, &quiet, &target, &record)
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
    /// fork of `target` holds while it clones the seed.
    pub async fn refresh_seed(
        &self,
        target: Owner,
        checkout: PathBuf,
        aim: Aim,
    ) -> Result<Refreshed> {
        let this = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let Some(age) = this.age_now(&target, &checkout, &aim.tree)? else {
                return Ok(Refreshed {
                    passed: Vec::new(),
                    outcome: Refresh::NoBuildVolume,
                });
            };
            let mut passed = Vec::new();
            if age.unlanded()
                && let Some(landed) =
                    seed_from_landed(&*this.host, &this.layout, &target, &aim, &mut passed)?
            {
                return Ok(Refreshed {
                    passed,
                    outcome: landed,
                });
            }
            let outcome = reseed(&*this.host, &this.layout, &target, age)?;
            Ok(Refreshed { passed, outcome })
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
    /// the landing volume lacks, and give it the target's code-graph index when it has none.
    /// A `stopped` reason stages no Nx entries; independent index carry still runs.
    pub async fn stage(
        &self,
        quiet: &Quiet,
        target_checkout: PathBuf,
        stopped: Option<String>,
    ) -> Result<Staging> {
        let target = self.layout.linked(&target_checkout)?;
        let quiet = quiet.clone();
        self.blocking(move |host, layout| {
            let source_hold = target
                .as_ref()
                .map(|target| {
                    layout.hold(target).map_err(|error| {
                        io(
                            "hold the land carry source",
                            &layout.hold_path(target),
                            &error,
                        )
                    })
                })
                .transpose()?;
            let started = Instant::now();
            let staged = match target {
                Some(target) => {
                    let mount = host.mount_build_volume(layout, &target).map_err(storage)?;
                    let state = BuildVolumeState::read(&mount)?;
                    let staged = match stopped {
                        Some(reason) => carry::Staged::stopped(reason),
                        None => carry::stage(&mount, &state, &quiet.mount, &quiet.state),
                    };
                    staged
                }
                None => stopped.map_or_else(carry::Staged::default, carry::Staged::stopped),
            };
            Ok(Staging {
                staged,
                source_hold,
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
            drop(staging.source_hold);
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

    /// Land step 6, under the target's image lock: when nothing opened the closed target's task
    /// database since [`Self::close_target`], give the landing volume the target's daemon records
    /// in place of its own and rename the target's build link onto it. The target's previous
    /// volume is recorded Unlinked and answered, for [`Self::release_previous`] beside what the
    /// land does in the target next. Answers how long the move took.
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
        let target = locked.target.name.clone();
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
            Ok(Ok(Adopted {
                elapsed_ms: millis(elapsed),
                previous: Unlinked {
                    id: previous,
                    target,
                },
            }))
        })
        .await
    }

    /// Release a target's previous volume after its adoption: nothing links it, so only a job
    /// admitted on it before the move keeps it, and the collection after that job ends
    /// reclaims it. It needs no target lock: no fork or reseed of the target that took the lock
    /// after the move reads it, and none that held it before the land took it still runs.
    pub async fn release_previous(&self, previous: Unlinked) -> Result<()> {
        self.blocking(move |host, layout| {
            let Unlinked { id, target } = previous;
            let release = host.release_build_volume(layout, &id).map_err(storage)?;
            say_release(&id, &format!("{target}'s previous"), &release);
            Ok(())
        })
        .await
    }

    /// A workspace whose volume a target adopted and that keeps working (`land --no-retire`)
    /// takes a fresh clone of that target's latest seed (the one the land froze, or the previous
    /// one a busy seal kept), so it never writes the target's volume. Under the target's image
    /// lock, so no reseed of the target deletes the seed it clones.
    pub async fn refork(
        &self,
        locked: &TargetLock<H>,
        workspace: WorkspaceName,
        checkout: PathBuf,
    ) -> Result<()> {
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
        self.blocking(move |host, layout| {
            let plan = plan(layout, &images, &links)?;
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
                    match (still_doomed(layout, &links, &doomed), layout.held(&id)) {
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
                    match release_if_still_doomed(host, layout, &links, &doomed) {
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
                        collection.freed_bytes = collection.freed_bytes.saturating_add(bytes);
                    }
                    Some(deferral) => {
                        // A collection names what it tried to release; a volume a checkout
                        // links is not garbage, and a dry run tries nothing.
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
            collect_legacy_seed_sidecars(layout, &links, dry_run, &mut collection)?;
            Ok(collection)
        })
        .await
    }
}

/// A payload-absent legacy seed's metadata, never a legacy runtime image.
struct LegacySeedSidecar {
    id: BuildVolumeId,
    image: PathBuf,
    sidecar: PathBuf,
    seen: BuildVolumeRecord,
}

/// Spec 01's sole legacy metadata exception. Existing payloads and non-seed records stay.
fn collect_legacy_seed_sidecars(
    layout: &BuildVolumeLayout,
    links: &Links,
    dry_run: bool,
    collection: &mut Collection,
) -> Result<()> {
    let entries = match std::fs::read_dir(layout.images()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io("list legacy seed sidecars", layout.images(), &error)),
    };
    for entry in entries {
        let entry =
            entry.map_err(|error| io("list legacy seed sidecars", layout.images(), &error))?;
        let name = entry.file_name();
        let Some(id) = name
            .to_str()
            .and_then(|name| name.strip_suffix(".asif.json"))
            .and_then(BuildVolumeId::parse)
        else {
            continue;
        };
        let image = layout.images().join(format!("{id}.asif"));
        match std::fs::symlink_metadata(&image) {
            Ok(_) => continue, // Retained payload and metadata are never touched.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                collection.deferred.push(Deferred {
                    path: entry.path(),
                    deferral: Deferral::SizeUnreadable(error.to_string()),
                });
                continue;
            }
        }
        let sidecar = entry.path();
        let read = (|| {
            if !entry
                .file_type()
                .map_err(|error| error.to_string())?
                .is_file()
            {
                return Err("legacy seed sidecar is not a regular file".to_owned());
            }
            crate::metadata::read_json::<BuildVolumeRecord>(&sidecar)
                .map_err(|error| error.to_string())
        })();
        let seen = match read {
            Ok(record) if matches!(record.role, BuildVolumeRole::Seed { .. }) => record,
            Ok(_) => continue,
            Err(reason) => {
                collection.deferred.push(Deferred {
                    path: sidecar,
                    deferral: Deferral::RecordUnreadable(reason),
                });
                continue;
            }
        };
        collection.examined += 1;
        let candidate = LegacySeedSidecar {
            id,
            image,
            sidecar,
            seen,
        };
        let released = (|| {
            let _claim = if dry_run {
                if layout
                    .held_re_adopt_source(&candidate.id, &candidate.image)
                    .map_err(|error| io("probe the legacy seed hold", &candidate.image, &error))?
                {
                    return Ok(Some(Deferral::Held(Vec::new())));
                }
                None
            } else {
                let Some(claim) = layout
                    .claim_re_adopt_source(&candidate.id, &candidate.image)
                    .map_err(|error| io("claim legacy seed metadata", &candidate.image, &error))?
                else {
                    return Ok(Some(Deferral::Held(Vec::new())));
                };
                Some(claim)
            };
            if let Some(deferral) = legacy_seed_deferral(layout, links, &candidate)? {
                return Ok(Some(deferral));
            }
            if !dry_run {
                let hold = crate::metadata::append_suffix(&candidate.image, ".hold");
                match std::fs::remove_file(&hold) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(io("remove obsolete legacy seed hold", &hold, &error));
                    }
                }
                std::fs::remove_file(&candidate.sidecar).map_err(|error| {
                    io(
                        "remove orphan legacy seed metadata",
                        &candidate.sidecar,
                        &error,
                    )
                })?;
                cowshed_os::fsio::sync_directory(layout.images()).map_err(|error| {
                    io(
                        "sync legacy seed metadata collection",
                        layout.images(),
                        &error,
                    )
                })?;
            }
            Ok::<_, CowshedError>(None)
        })();
        match released {
            Ok(None) => {
                collection.candidates.push(GcCandidate {
                    identity: Sha256Digest::compute(
                        candidate.sidecar.as_os_str().as_encoded_bytes(),
                    ),
                    path: candidate.sidecar,
                    bytes: 0,
                    reason: GcReason::SupersededSeed,
                });
                if !dry_run {
                    collection.reclaimed += 1;
                }
            }
            Ok(Some(deferral)) => collection.deferred.push(Deferred {
                path: candidate.sidecar,
                deferral,
            }),
            Err(error) => collection.deferred.push(Deferred {
                path: candidate.sidecar,
                deferral: Deferral::ReleaseFailed(error.to_string()),
            }),
        }
    }
    Ok(())
}

/// Re-read under the actual legacy hold claim, not the earlier inventory snapshot.
fn legacy_seed_deferral(
    layout: &BuildVolumeLayout,
    links: &Links,
    candidate: &LegacySeedSidecar,
) -> Result<Option<Deferral>> {
    let metadata = match std::fs::symlink_metadata(&candidate.sidecar) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Some(Deferral::Changed));
        }
        Err(error) => {
            return Err(io(
                "recheck legacy seed metadata",
                &candidate.sidecar,
                &error,
            ));
        }
    };
    if !metadata.is_file() {
        return Ok(Some(Deferral::Changed));
    }
    let current: BuildVolumeRecord =
        crate::metadata::read_json(&candidate.sidecar).map_err(|error| {
            CowshedError::integrity(error.to_string(), "inspect the legacy seed sidecar")
        })?;
    if current != candidate.seen {
        return Ok(Some(Deferral::Changed));
    }
    match std::fs::symlink_metadata(&candidate.image) {
        Ok(_) => return Ok(Some(Deferral::Changed)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(io(
                "recheck legacy seed image absence",
                &candidate.image,
                &error,
            ));
        }
    }
    let BuildVolumeRole::Seed { target, .. } = &current.role else {
        return Ok(Some(Deferral::Changed));
    };
    let journal = crate::storage::recovery::LifecycleIntentJournal::load(
        &layout
            .project_root()
            .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE),
    )?;
    if links.creating.contains(target) || journal.forming().any(|workspace| workspace == target) {
        return Ok(Some(Deferral::Creating(target.clone())));
    }
    if links.detached.contains(target) {
        return Ok(Some(Deferral::DetachedCheckout(target.clone())));
    }
    for (workspace, checkout) in &links.checkouts {
        if layout.linked(checkout)?.as_ref() == Some(&candidate.id) {
            return Ok(Some(Deferral::Linked(workspace.clone())));
        }
    }
    if links.volumes.contains(&candidate.id) {
        return Ok(Some(Deferral::Changed));
    }
    Ok(None)
}

/// Why `doomed` is no longer garbage, read now: its record is not the one the plan judged it by
/// (a creation wrote it, a land handed the volume to its target), or a checkout's build link
/// names it. A live link proves reachability by itself, whatever any record says.
fn still_doomed(
    layout: &BuildVolumeLayout,
    links: &Links,
    doomed: &Doomed,
) -> Result<Option<Deferral>> {
    if layout.read_record_present(&doomed.id)? != doomed.seen {
        return Ok(Some(Deferral::Changed));
    }
    if doomed.image_missing {
        match std::fs::symlink_metadata(layout.image(&doomed.id)) {
            Ok(_) => return Ok(Some(Deferral::Changed)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io(
                    "inspect orphan seed image",
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
    /// It has no record, and workspaces whose links cannot be read yet may name it: the
    /// detached ones until they are attached, the ones being created until their create, fork
    /// or adopt finishes.
    Unrecorded {
        detached: Vec<WorkspaceName>,
        creating: Vec<WorkspaceName>,
    },
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
                    formatter.write_str(" (until their create, fork or adopt finishes)")?;
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

/// A build volume collection deletes, with why, its allocated size, and the record the plan
/// judged it by (`None` for an unrecorded image): its release decides it again under its claim,
/// and a record that changed since says the volume is no longer what the plan judged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Doomed {
    pub id: BuildVolumeId,
    pub reason: GcReason,
    pub bytes: u64,
    pub seen: Option<BuildVolumeRecord>,
    /// An orphan sidecar: a newly appearing image makes the planned release stale.
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
/// links that is not a seed, each seed that is not its existing target's latest, and each image
/// an interrupted creation left without a record.
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
            // An image with no record is an interrupted creation — a mint that died before its
            // record was written — so no checkout and no forming verb names it, and a detached or
            // still-forming workspace must not block its collection: only a volume a forming verb
            // recorded is deferred. A create or fork writes its PendingFence record before its
            // payload appears, so its volumes are never unrecorded and are deferred by the
            // Creating arm below. The one exception is an adopt or re-adopt of main mid-mint: its
            // first volume exists before its record does, so while main is forming an unrecorded
            // image may still be that mint and waits for the adopt to finish.
            Ok(None) if links.creating.contains(&WorkspaceName::main()) => {
                plan.deferred.push((
                    id,
                    Deferral::Unrecorded {
                        detached: Vec::new(),
                        creating: links.creating.iter().cloned().collect(),
                    },
                ));
                continue;
            }
            Ok(None) => {
                doomed.push((id, GcReason::UnrecordedBuildVolume, None));
                continue;
            }
            Err(error) => {
                plan.deferred
                    .push((id, Deferral::RecordUnreadable(error.to_string())));
                continue;
            }
        };
        if matches!(&record.role, BuildVolumeRole::Seed { target, .. } if !links.creating.contains(target))
        {
            match std::fs::symlink_metadata(layout.image(&id)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    plan.doomed.push(Doomed {
                        id,
                        reason: GcReason::SupersededSeed,
                        bytes: 0,
                        seen: Some(record),
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
                    doomed.push((id, GcReason::SupersededSeed, Some(record)));
                    continue;
                }
                match latest.get(&owner) {
                    Some((_, newest)) if newest.created_at >= record.created_at => {
                        doomed.push((id, GcReason::SupersededSeed, Some(record)));
                    }
                    _ => {
                        if let Some((older, seen)) = latest.insert(owner, (id, record)) {
                            doomed.push((older, GcReason::SupersededSeed, Some(seen)));
                        }
                    }
                }
            }
            BuildVolumeRole::Linked { checkout } if links.detached.contains(checkout) => {
                plan.deferred
                    .push((id, Deferral::DetachedCheckout(checkout.clone())));
            }
            BuildVolumeRole::Linked { .. } | BuildVolumeRole::Unlinked => {
                doomed.push((id, GcReason::UnlinkedBuildVolume, Some(record)));
            }
        }
    }
    for (id, reason, seen) in doomed {
        let image = layout.image(&id);
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

/// Capture the quiet landing volume under the Nx and existing Cargo profile guards.
/// Contributed paths outside those protocols are exempt. A host Cargo build can still create
/// a new profile after enumeration; closing that gap requires a producer-side lease.
fn seal(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    quiet: &Quiet,
    target: &Owner,
    record: &BuildVolumeRecord,
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
    match host.clone_quiet_build_volume(layout, quiet, &BuildVolumeId::mint(), record) {
        Ok(()) => {
            retire_seeds(host, layout, target, previous)?;
            Ok(Ok(Freeze::Frozen))
        }
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

/// Delete `previous`, the seeds of `target` a new one replaced. A seed is never mounted and no
/// job is admitted on one, so a hold on it is an integrity fault.
fn retire_seeds(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    previous: Vec<BuildVolumeId>,
) -> Result<()> {
    for old in previous {
        match host.release_build_volume(layout, &old).map_err(storage)? {
            release @ BuildVolumeRelease::Deleted { .. } => {
                say_release(
                    &old,
                    &format!("{}'s superseded seed", target.name),
                    &release,
                );
            }
            BuildVolumeRelease::Held { .. } => {
                return Err(CowshedError::integrity(
                    format!(
                        "seed {old} of {} is held by a job, but no job is ever admitted on a seed",
                        target.name
                    ),
                    "cowshed doctor --json",
                ));
            }
        }
    }
    Ok(())
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
        Err(error) => eprintln!(
            "cowshed: {whose} build volume {id}, minted beside main, stays: {error}; `cowshed gc` retries it"
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
        } if open.is_empty() => eprintln!(
            "cowshed: {whose} build volume {id}: its unforced unmount was refused with no visible holder; {}",
            outcome(*forced)
        ),
        BuildVolumeRelease::Deleted {
            refused: Some(BuildVolumeRefusal { open, forced }),
        } => eprintln!(
            "cowshed: {whose} build volume {id} was open in {} when its unforced unmount was refused; {}",
            nx::Holders(open),
            outcome(*forced)
        ),
        BuildVolumeRelease::Held { open } if open.is_empty() => eprintln!(
            "cowshed: {whose} build volume {id} stays while a job admitted on it runs; the next rm, land or gc after it ends reclaims it"
        ),
        BuildVolumeRelease::Held { open } => eprintln!(
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
            eprintln!(
                "cowshed: cannot compare build tree {recorded} with {tree} in {}: {}; it counts as farther than any comparable tree",
                checkout.display(),
                crate::git::git_ended(&output)
            );
            Nearness::Unknown
        }
        Err(error) => {
            eprintln!(
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
/// The caller holds `target`'s image lock.
fn seed_from_landed(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    aim: &Aim,
    passed: &mut Vec<(WorkspaceName, LandedSkip)>,
) -> Result<Option<Refresh>> {
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
            .lock_images(std::slice::from_ref(&peer.lock), LockMode::Try)
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
            Ok(elapsed_ms) => {
                return Ok(Some(Refresh::Landed {
                    workspace: peer.name.clone(),
                    tree: aim.tree.clone(),
                    elapsed_ms,
                }));
            }
            Err(skip) => passed.push((peer.name.clone(), LandedSkip::Capture(skip))),
        }
    }
    Ok(None)
}

/// A reseed (16_build_volumes.md, "Which volume a seed holds", step 2): capture the target's
/// live volume when it is nearer the fork's tree than the seed, or as near and written since.
fn reseed(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    age: SeedAge,
) -> Result<Refresh> {
    if !age.reseeds() {
        return Ok(Refresh::Kept);
    }
    let behind_ms = age.behind().map(millis);
    Ok(match capture(host, layout, target, &age.live, age.tree)? {
        Ok(elapsed_ms) => Refresh::Reseeded {
            behind_ms,
            elapsed_ms,
        },
        Err(reason) => Refresh::Skipped { behind_ms, reason },
    })
}

/// Freeze `source`, recorded at `tree`, as `target`'s seed after Nx and existing Cargo profiles
/// admit capture, then retire every older seed of `target`; the elapsed milliseconds. Their
/// guards and the exclusive build hold stay alive through mounted capture and publication.
/// Exempt cache/index handles may stay open. A skip leaves every seed as it was.
fn capture(
    host: &impl RuntimeStorageHost,
    layout: &BuildVolumeLayout,
    target: &Owner,
    source: &BuildVolumeId,
    tree: Option<GitOid>,
) -> Result<std::result::Result<u64, CaptureSkip>> {
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
        &BuildVolumeRecord::new(
            tree,
            BuildVolumeRole::Seed {
                target: target.name.clone(),
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
    retire_seeds(host, layout, target, previous)?;
    let elapsed_ms = millis(started.elapsed());
    cowshed_os::timing::event("build-volume", || {
        format!("seed {} from {source} in {elapsed_ms} ms", target.name)
    });
    Ok(Ok(elapsed_ms))
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
        eprintln!(
            "cowshed: {workspace} has the tree {target} moved to checked out, but does not seed {target}: {reason}"
        );
    }
    match &refreshed.outcome {
        Refresh::NoBuildVolume | Refresh::Kept | Refresh::Reseeded { .. } => {}
        Refresh::Landed {
            workspace,
            tree,
            elapsed_ms,
        } => eprintln!(
            "cowshed: {target}'s branch moved to tree {tree} outside cowshed land; its seed is now {workspace}'s build volume ({elapsed_ms} ms)"
        ),
        Refresh::Skipped { behind_ms, reason } => eprintln!(
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

/// What a carry into the volume `into` reports, taking `elapsed` in all; also said as a timing
/// event.
fn nx_carry(carried: carry::Carried, elapsed: Duration, into: &BuildVolumeId) -> NxCarry {
    let carry = NxCarry {
        entries: carried.entries,
        bytes: carried.bytes,
        elapsed_ms: millis(elapsed),
        stopped: carried.stopped,
    };
    cowshed_os::timing::event("build-volume", || {
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

    #[test]
    fn legacy_orphan_seed_gc_preserves_payloads_holds_links_and_forming_work() {
        use crate::storage::recovery::{
            LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentCompletion,
            LifecycleIntentJournal,
        };
        let store = Store::new();
        let target = WorkspaceName::new("lane").unwrap();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: target.clone(),
                incarnation: owner("lane", '0').incarnation,
            },
        );
        let candidate = |record: &BuildVolumeRecord| {
            let id = BuildVolumeId::mint();
            let image = store.layout.images().join(format!("{id}.asif"));
            let sidecar = crate::metadata::append_suffix(&image, ".json");
            crate::metadata::write_json(&sidecar, record).unwrap();
            LegacySeedSidecar {
                id,
                image,
                sidecar,
                seen: record.clone(),
            }
        };
        let orphan = candidate(&record);
        let hold_path = crate::metadata::append_suffix(&orphan.image, ".hold");
        let links = Links::default();
        let mut preview = Collection::default();
        collect_legacy_seed_sidecars(&store.layout, &links, true, &mut preview).unwrap();
        assert_eq!(preview.candidates.len(), 1);
        assert!(orphan.sidecar.exists());
        assert!(!hold_path.exists(), "preview never creates a hold file");
        let mut collected = Collection::default();
        collect_legacy_seed_sidecars(&store.layout, &links, false, &mut collected).unwrap();
        assert_eq!(collected.reclaimed, 1);
        assert_eq!(collected.freed_bytes, 0);
        assert!(!orphan.sidecar.exists());
        assert!(
            !orphan.image.exists(),
            "metadata cleanup never creates or attaches a payload"
        );
        assert!(
            !hold_path.exists(),
            "collection releases the hold with the sidecar"
        );

        let retained = candidate(&record);
        fs::write(&retained.image, "retained legacy payload").unwrap();
        let nonseed = candidate(&BuildVolumeRecord::new(None, linked("lane")));
        let held = candidate(&record);
        let held_path = crate::metadata::append_suffix(&held.image, ".hold");
        let holder = cowshed_os::fork_lock::Fenced::new(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&held_path)
                .unwrap(),
        );
        holder.lock_shared().unwrap();
        let mut held_preview = Collection::default();
        collect_legacy_seed_sidecars(&store.layout, &links, true, &mut held_preview).unwrap();
        assert!(
            held_preview
                .deferred
                .iter()
                .any(|entry| matches!(entry.deferral, Deferral::Held(_)))
        );
        assert!(held.sidecar.exists() && held_path.exists());
        let mut refused = Collection::default();
        collect_legacy_seed_sidecars(&store.layout, &links, false, &mut refused).unwrap();
        assert_eq!(refused.reclaimed, 0);
        assert!(
            refused
                .deferred
                .iter()
                .any(|entry| matches!(entry.deferral, Deferral::Held(_)))
        );
        assert!(held.sidecar.exists());
        assert!(nonseed.sidecar.exists(), "only strict Seed records qualify");
        assert_eq!(
            fs::read_to_string(&retained.image).unwrap(),
            "retained legacy payload"
        );
        assert!(retained.sidecar.exists());
        drop(holder);

        let checkout = store.root.join("lane");
        fs::create_dir_all(checkout.join(".cowshed")).unwrap();
        fs::create_dir_all(store.layout.mount(&held.id)).unwrap();
        link::point(&checkout, &store.layout.mount(&held.id)).unwrap();
        let mut linked = Links::default();
        linked.checkouts.insert(target.clone(), checkout);
        let claim = store
            .layout
            .claim_re_adopt_source(&held.id, &held.image)
            .unwrap()
            .unwrap();
        assert_eq!(
            legacy_seed_deferral(&store.layout, &linked, &held).unwrap(),
            Some(Deferral::Linked(target.clone()))
        );
        // A formation written after inventory was read is re-read under the same actual hold.
        let journal_path = store.layout.project_root().join(LIFECYCLE_INTENTS_FILE);
        let mut journal = LifecycleIntentJournal::default();
        journal.begin(LifecycleIntent::Fork {
            source: WorkspaceName::main(),
            destination: target.clone(),
            rename: false,
        });
        journal.mark_mutating(&target).unwrap();
        crate::metadata::write_json(&journal_path, &journal).unwrap();
        assert_eq!(
            legacy_seed_deferral(&store.layout, &links, &held).unwrap(),
            Some(Deferral::Creating(target.clone()))
        );
        journal
            .complete(
                &target,
                LifecycleIntentCompletion::Workspace(owner("lane", '1').incarnation),
            )
            .unwrap();
        crate::metadata::write_json(&journal_path, &journal).unwrap();
        fs::write(&held.image, "a payload appeared").unwrap();
        assert_eq!(
            legacy_seed_deferral(&store.layout, &links, &held).unwrap(),
            Some(Deferral::Changed)
        );
        fs::remove_file(&held.image).unwrap();
        let mut changed = record.clone();
        changed.tree = Some(GitOid::new("1".repeat(40)).unwrap());
        crate::metadata::write_json(&held.sidecar, &changed).unwrap();
        assert_eq!(
            legacy_seed_deferral(&store.layout, &links, &held).unwrap(),
            Some(Deferral::Changed)
        );
        crate::metadata::write_json(&held.sidecar, &record).unwrap();
        drop(claim);
        let mut finished = Collection::default();
        collect_legacy_seed_sidecars(&store.layout, &links, false, &mut finished).unwrap();
        assert_eq!(finished.reclaimed, 1);
        assert!(!held.sidecar.exists());
        assert!(!held_path.exists());
        assert!(retained.sidecar.exists() && retained.image.exists());
        assert!(nonseed.sidecar.exists());
    }

    #[test]
    fn a_legacy_seed_sidecar_with_an_already_absent_hold_is_collected() {
        let store = Store::new();
        let target = WorkspaceName::new("lane").unwrap();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target,
                incarnation: owner("lane", '0').incarnation,
            },
        );
        let id = BuildVolumeId::mint();
        let image = store.layout.images().join(format!("{id}.asif"));
        let sidecar = crate::metadata::append_suffix(&image, ".json");
        crate::metadata::write_json(&sidecar, &record).unwrap();
        let hold = crate::metadata::append_suffix(&image, ".hold");
        assert!(!image.exists() && !hold.exists());
        let links = Links::default();
        let mut collected = Collection::default();
        collect_legacy_seed_sidecars(&store.layout, &links, false, &mut collected).unwrap();
        assert_eq!(collected.reclaimed, 1);
        assert!(!sidecar.exists());
        assert!(!hold.exists());
        assert!(!image.exists());
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
        assert!(!landed.unlanded(), "the seed was built at the fork's tree");
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
        assert!(!adopted.unlanded());
        let moved = age(
            (Nearness::Paths(1), later),
            Some((Nearness::Paths(1), frozen)),
        );
        assert!(
            moved.unlanded(),
            "neither volume was built at the fork's tree"
        );
        assert!(age((Nearness::Unknown, frozen), None).unlanded());
    }

    /// A git repository at `path` whose commit holds `files`, by its `HEAD^{tree}`.
    fn commit(path: &Path, files: &[(&str, &str)]) -> GitOid {
        use cowshed_os::fork_lock::Run as _;
        let git = |args: &[&str]| {
            let output = crate::git::git_command_at(path)
                .args([
                    "-c",
                    "user.name=cowshed",
                    "-c",
                    "user.email=cowshed@example.invalid",
                ])
                .args(args)
                .output_locked()
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
    /// volume is deferred with that workspace named, never counted reachable or deleted. An
    /// image without a record is named by no detached workspace, so it is an interrupted
    /// creation and is collected — a detached shed does not block what it does not name.
    #[test]
    fn a_detached_checkout_defers_only_what_it_names() {
        let store = Store::new();
        let named = store.volume(linked("topic"));
        let unrecorded = store.image();
        let links = Links {
            detached: [name("topic")].into_iter().collect(),
            ..Links::default()
        };
        let plan = plan(&store.layout, &store.layout.list().unwrap(), &links).unwrap();
        assert_eq!(
            plan.deferred,
            [(named, Deferral::DetachedCheckout(name("topic")))]
        );
        assert_eq!(
            doomed(&plan),
            [(unrecorded, GcReason::UnrecordedBuildVolume)]
        );
        assert!(
            plan.deferred
                .iter()
                .all(|(_, deferral)| deferral.is_routine())
        );
    }

    /// A create or fork forks its volume and seed into a staged checkout no other process can
    /// read, and an adopt mints main's first volume beside main's image, before the workspace
    /// exists. Until its intent completes, whatever it recorded as its own — its live volume and
    /// its seed — is deferred, never collected. An image without a record is named by no forming
    /// verb, so it is collected and a forming shed does not block what it does not name; only an
    /// adopt of main mid-mint defers an unrecorded image. Another process's `rm` collected the
    /// forming shed's own volumes, and the new workspace's mount then refused a link to a volume
    /// that was gone.
    #[test]
    fn a_forming_workspace_defers_only_the_volumes_it_names() {
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
        let images = store.layout.list().unwrap();
        let mut plan = plan(&store.layout, &images, &links).unwrap();
        plan.deferred.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected = vec![
            (live.clone(), Deferral::Creating(name("lane"))),
            (seed.clone(), Deferral::Creating(name("lane"))),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(plan.deferred, expected);
        let mut collected = doomed(&plan);
        collected.sort_by(|left, right| left.0.cmp(&right.0));
        let mut expected_doomed = vec![
            (cloning.clone(), GcReason::UnrecordedBuildVolume),
            (garbage.clone(), GcReason::UnlinkedBuildVolume),
        ];
        expected_doomed.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(collected, expected_doomed);
        assert!(Deferral::Creating(name("lane")).is_routine());
        // Without the forming workspace, all three read as garbage: the collection that left
        // a new workspace linking a volume nobody owned.
        let mut doomed = doomed(&super::plan(&store.layout, &images, &Links::default()).unwrap());
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

    /// Collection decides only the images it listed before it read what links them. A verb that
    /// crosses its mutation fence and makes a volume after the collection read the journal is
    /// absent from those links, and its volume is not one of the images listed: it is left to a
    /// later pass, whose links are read after the volume exists. Listed after the links, it was
    /// decided from links that predate it, and collected as an interrupted creation.
    #[test]
    fn an_image_made_after_the_listing_is_left_to_a_later_pass() {
        let store = Store::new();
        let garbage = store.volume(BuildVolumeRole::Unlinked);
        // The collection lists the images, then reads the journal and the workspaces: no verb
        // is forming yet.
        let images = store.layout.list().unwrap();
        let links = Links::default();
        // Only then does an adopt cross its fence and mint main's first volume.
        let minted = store.image();
        let plan = plan(&store.layout, &images, &links).unwrap();
        assert_eq!(
            doomed(&plan),
            [(garbage.clone(), GcReason::UnlinkedBuildVolume)],
            "{plan:?}"
        );
        assert_eq!(plan.examined, 1);
        assert_eq!(plan.deferred, []);
        // The next pass lists it, and reads the journal after: main is forming until its first
        // touch links the volume.
        let next = Links {
            creating: [name("main")].into_iter().collect(),
            ..Links::default()
        };
        let plan = super::plan(&store.layout, &store.layout.list().unwrap(), &next).unwrap();
        assert_eq!(doomed(&plan), [(garbage, GcReason::UnlinkedBuildVolume)]);
        assert_eq!(
            plan.deferred,
            [(
                minted,
                Deferral::Unrecorded {
                    detached: Vec::new(),
                    creating: vec![name("main")],
                }
            )]
        );
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
            .prepare_fork(
                main.clone(),
                main_checkout,
                lane.name.clone(),
                unlanded_aim(),
            )
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
            .write_record(
                &sizeless,
                &BuildVolumeRecord::new(None, BuildVolumeRole::Unlinked),
            )
            .unwrap();
        let garbage = store.volume(BuildVolumeRole::Unlinked);
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
            .prepare_fork(
                main.clone(),
                main_checkout.clone(),
                topic.name.clone(),
                unlanded_aim(),
            )
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
        mint: fn(&H, &BuildVolumeLayout, &BuildVolumeId, u64) -> PathBuf,
    }

    #[cfg(target_os = "macos")]
    impl Scratch {
        fn new(name: &str) -> Self {
            Self::with_root(
                FixtureRoot::Apfs(
                    crate::scratch_apfs::ScratchRoot::new(name).expect("scratch APFS root"),
                ),
                |config| MacOsApfsExecutionHost::new(SystemCommandRunner, config),
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
                |host, layout, id, gib| {
                    host.create_build_volume(layout, id, ImageCapacity::from_gibibytes(gib))
                        .expect("create a directory-backed build volume")
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
            mint: fn(&H, &BuildVolumeLayout, &BuildVolumeId, u64) -> PathBuf,
        ) -> Self {
            use crate::storage::apfs::ApfsSubstrateConfig;
            let store = root.path().join("store");
            fs::create_dir_all(&store).unwrap();
            let project = ProjectPaths::with_mount_root(
                &store,
                root.path().join("mnt"),
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap();
            let host = Arc::new(
                make_host(ApfsSubstrateConfig::new(
                    &store,
                    root.path().join("checkout"),
                ))
                .unwrap(),
            );
            let layout = BuildVolumeLayout::new(&project).unwrap();
            let volumes = BuildVolumes::new(Arc::clone(&host), layout.clone());
            Self {
                root,
                host,
                layout,
                volumes,
                mint,
            }
        }

        /// `name`'s checkout, linked to a new live volume recorded as its own.
        fn linked_checkout(&self, name: &str, gib: u64) -> (PathBuf, BuildVolumeId, PathBuf) {
            let checkout = self.root.path().join(name);
            fs::create_dir_all(checkout.join(".cowshed")).unwrap();
            let id = BuildVolumeId::mint();
            let mount = (self.mint)(&self.host, &self.layout, &id, gib);
            BuildVolumeState::default().write(&mount).unwrap();
            self.layout
                .write_record(&id, &BuildVolumeRecord::new(None, linked(name)))
                .unwrap();
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
                .try_lock_target(target.clone(), self.image_lock(&target.name))
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
                &BuildVolumeRecord::new(
                    Some(GitOid::new("a".repeat(40)).unwrap()),
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                ),
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
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: topic.name.clone(),
                        incarnation: topic.incarnation.clone(),
                    },
                ),
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
        assert_eq!(report.freed_bytes, 0);
        assert!(report.previous_bytes > 0);
        assert_eq!(scratch.layout.linked(&checkout).unwrap(), Some(old.clone()));

        // Both an admitted job's shared hold and a land/capture's exclusive claim refuse.
        let job = scratch.layout.hold(&old).unwrap();
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
        assert_eq!(completed.freed_bytes, completed.previous_bytes);
        assert!(completed.freed_bytes > 0);
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
        (expected_deleted, completed.freed_bytes)
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
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: from.owner.name.clone(),
                        incarnation: from.owner.incarnation.clone(),
                    },
                ),
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
            .lock_images(&[from.image.lock().to_owned()], LockMode::Try)
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
        assert_eq!(
            scratch
                .volumes
                .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
                .await
                .unwrap(),
            Ok(Freeze::Frozen)
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
                assert_eq!(
                    scratch
                        .volumes
                        .freeze_seed(&locked, &quiet, tree.clone())
                        .await
                        .unwrap(),
                    Ok(Freeze::Frozen),
                    "the landing capture admits exempt open caches"
                );
            } else {
                capture(
                    &*scratch.host,
                    &scratch.layout,
                    &target,
                    &live,
                    Some(tree.clone()),
                )
                .unwrap()
                .expect("direct capture admits the same exempt open caches");
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
                    &BuildVolumeRecord::new(Some(tree), linked("fork")),
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
        fs::create_dir_all(scratch.layout.record(&seed).parent().unwrap()).unwrap();
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
        assert_eq!(collected.freed_bytes, 0);
        assert!(!scratch.layout.record(&seed).exists());
        scratch.release_all();
    }

    #[cfg(target_os = "macos")]
    struct CargoWitness {
        /// The build script's independently reported parent, checked against the kernel.
        pid: libc::pid_t,
        /// Only a grant observed for this fixture's checkout, never an inherited token count.
        grant_fifo: Option<PathBuf>,
    }

    #[cfg(target_os = "macos")]
    async fn cargo_budget_for(source: &Path) -> Option<cowshed_gateway_types::CpuCheckoutStatus> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let mut stream =
            tokio::net::UnixStream::connect(cowshed_gateway_types::control_socket_path())
                .await
                .expect("connect to the actual jobserver grant's gateway");
        stream
            .write_all(b"{\"op\":\"cpu-budget\"}\n")
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.unwrap();
        let answer: serde_json::Value = serde_json::from_str(answer.trim_end()).unwrap();
        let budget: cowshed_gateway_types::CpuBudgetStatus =
            serde_json::from_value(answer["cpuBudget"].clone()).expect("the actual CPU ledger");
        budget
            .checkouts
            .into_iter()
            .find(|entry| entry.checkout == source.to_string_lossy())
    }

    /// A real Cargo build stopped inside its build script, after Cargo has taken the profile's
    /// shared `.cargo-lock`. Socket readiness, not a delay, is the proof that it is building.
    #[cfg(target_os = "macos")]
    async fn running_cargo(
        source: &Path,
        mount: &Path,
        profile: &str,
    ) -> (tokio::process::Child, tokio::net::UnixStream, CargoWitness) {
        use cowshed_os::fork_lock::{RunAsync as _, Spawn as _};
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;
        use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _};

        // Preserve the caller's real budget environment. A direct binary run with neither
        // inherited variable can prove an actual checkout grant; a harness count cannot.
        let outside_budget = std::env::var_os("CARGO_MAKEFLAGS").is_none()
            && std::env::var_os("BOUNDED_EXEC_CPU_TOKENS").is_none();
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
            .expect("run the real Cargo build through the ambient launcher");
        let (stream, _) = tokio::select! {
            connected = listener.accept() => connected.expect("Cargo build script connects"),
            status = child.wait() => {
                let output = child.wait_with_output().await.expect("Cargo diagnostics");
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
            .output_locked()
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
                "the real Cargo is the launcher child, not an unrelated holder"
            );
        }
        let fifo = makeflags
            .split_whitespace()
            .find_map(|flag| flag.strip_prefix("--jobserver-auth=fifo:"))
            .map(PathBuf::from);
        let grant_fifo = fifo.as_ref().filter(|_| outside_budget).cloned();
        if let Some(fifo) = &fifo {
            let metadata = fs::metadata(fifo).expect("the actual Cargo's live jobserver fifo");
            assert!(
                metadata.file_type().is_fifo(),
                "{} is a kernel FIFO",
                fifo.display()
            );
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
        if grant_fifo.is_some() {
            let tokens: std::num::NonZeroUsize =
                tokens.parse().expect("the actual grant's token count");
            let held = cargo_budget_for(source)
                .await
                .expect("the actual checkout holds a grant");
            assert_eq!(
                held.held,
                tokens.get(),
                "the FIFO count matches the gateway's held grant"
            );
            assert_eq!(
                held.running, 1,
                "only this fixture's Cargo launcher holds its grant"
            );
            eprintln!("Cargo fixture held checkout grant: {held:?}");
        }
        eprintln!(
            "Cargo fixture witness: entry_pid={started_pid}, actual_cargo_pid={pid}, kernel_parent_pid={parent_pid}, kernel_process_group={group}, executable={}, makeflags={makeflags:?}, budget_tokens={tokens:?}, own_grant_fifo={grant_fifo:?}",
            cargo.display()
        );
        (child, stream.into_inner(), CargoWitness { pid, grant_fifo })
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
        assert_eq!(frozen, Freeze::Frozen);
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
        let output = child.wait_with_output().await.unwrap();
        assert!(output.status.success(), "{output:?}");
        if let Some(fifo) = &witness.grant_fifo {
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let held = cargo_budget_for(&source).await;
                if !fifo.exists()
                    && held
                        .as_ref()
                        .is_none_or(|entry| entry.held == 0 && entry.running == 0)
                {
                    eprintln!(
                        "Cargo fixture grant released: fifo={}, checkout={held:?}",
                        fifo.display()
                    );
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "the real Cargo build's grant outlives it: {held:?}; fifo={}",
                    fifo.display()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
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
        assert_eq!(frozen.unwrap(), Ok(Freeze::Frozen));
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
        assert_eq!(
            scratch
                .volumes
                .freeze_seed(&locked, &quiet, GitOid::new("a".repeat(40)).unwrap())
                .await
                .unwrap(),
            Ok(Freeze::Frozen)
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
        opening.wait().unwrap();
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
        capture(&*scratch.host, &scratch.layout, &target, &live, None)
            .unwrap()
            .expect("the initial capture has no holder");
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
    /// (16_build_volumes.md, Land step 5.6).
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
        let started = Instant::now();
        while !nx::holders(&database)
            .unwrap()
            .iter()
            .any(|held| held.pid == pid)
        {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "tail never opened {}",
                database.display()
            );
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
        holder.wait().unwrap();
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
    /// (16_build_volumes.md, Land step 5.6).
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
        // Short, under /tmp: a Unix socket path is bounded at 104 bytes on macOS.
        let sockets = Path::new("/tmp").join(format!(
            "cs-seal-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        fs::create_dir_all(&sockets).unwrap();
        let socket = sockets.join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let mut daemon = std::process::Command::new("/bin/sleep")
            .arg("600")
            .current_dir(&data)
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
            daemon.wait().unwrap();
        }
        drop(listener);
        fs::remove_dir_all(&sockets).unwrap();
        drop(quiet);
        drop(locked);
        scratch.release_all();
        assert_eq!(
            stopped.and_then(|status| status.signal()),
            Some(libc::SIGTERM),
            "the recorded landing daemon is stopped as `nx daemon --stop` stops it"
        );
        assert_eq!(
            frozen.unwrap(),
            Ok(Freeze::Frozen),
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
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
            )
            .unwrap();
        let mut held = fs::File::create(mount.join("lint-cache/warm")).unwrap();
        std::io::Write::write_all(&mut held, b"warm").unwrap();
        let tree = GitOid::new("a".repeat(40)).unwrap();
        // Clock ordering is supplied explicitly; this exercises admission and publication,
        // not the independent image-mtime comparison.
        let reseeded = reseed(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live,
                tree: Some(tree.clone()),
                nearness: Nearness::Unknown,
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

    /// Fork `destination` from `main`'s seed refreshed for `aim`, as `fork` does, and read the
    /// marker output in the fork's live volume, if any.
    #[cfg(target_os = "macos")]
    async fn fork_marker(
        scratch: &Scratch,
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
            .prepare_fork(
                main.clone(),
                main_checkout.to_owned(),
                destination.name.clone(),
                aim,
            )
            .await
            .expect("prepare the fork from main's seed");
        let forked = scratch
            .volumes
            .finish_fork(prepared, destination, staged)
            .await
            .expect("fork from main's seed")
            .expect("main has a seed");
        fs::read_to_string(scratch.layout.mount(&forked).join("marker")).ok()
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
                &BuildVolumeRecord::new(Some(before.clone()), linked("main")),
            )
            .unwrap();
        scratch
            .host
            .clone_build_volume(
                &scratch.layout,
                &main_live,
                &BuildVolumeId::mint(),
                &BuildVolumeRecord::new(
                    Some(before.clone()),
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                ),
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
            .lock_images(std::slice::from_ref(&lander.lock), LockMode::Try)
            .unwrap()
            .expect("nothing else holds lander's image lock");
        let refreshed = scratch
            .volumes
            .refresh_seed(
                main.clone(),
                main_checkout.clone(),
                aim(vec![lander.clone()]),
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
    /// Land stages before taking the target image lock. Its staging value must keep
    /// the source mounted through the separate close/commit calls, not just the copy.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_land_carry_retains_source_hold_until_commit() {
        let scratch = Scratch::directory("land-carry-hold");
        let (source_checkout, source, _) = scratch.linked_checkout("main", 1);
        let (checkout, destination, _) = scratch.linked_checkout("topic", 1);
        let quiet = scratch
            .volumes
            .quiesce(
                scratch.landing(&checkout),
                scratch.stopped(&checkout).await,
                source_checkout.clone(),
            )
            .await
            .unwrap()
            .expect("the landing volume is idle");
        let staged = scratch
            .volumes
            .stage(&quiet, source_checkout, None)
            .await
            .unwrap();
        let source_during = scratch.layout.claim_release(&source).unwrap().is_none();
        let destination_during = scratch
            .layout
            .claim_release(&destination)
            .unwrap()
            .is_none();
        let result = scratch.volumes.commit(staged, &quiet).await.unwrap();
        let source_after = scratch.layout.claim_release(&source).unwrap().is_some();
        drop(quiet);
        let destination_after = scratch
            .layout
            .claim_release(&destination)
            .unwrap()
            .is_some();
        scratch.release_all();
        assert!(
            source_during,
            "staging must retain the source build hold until commit"
        );
        assert!(
            destination_during,
            "the quiet landing volume retains its existing hold"
        );
        assert!(
            source_after && destination_after,
            "finished carry releases its holds"
        );
        assert!(result.stopped.is_none(), "{result:?}");
    }

    /// Closing the idle Nx daemon creates a gap with no database holder. A reseed's
    /// exclusive build claim must still fail on both volumes until carry commits.
    /// On SIGTERM this fixture attempts exactly that claim, and hides only its own
    /// source database when admitted, modeling the database path during a detached seal.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn directory_rebase_carry_holds_both_volumes_across_daemon_stop() {
        use cowshed_os::fork_lock::{Run as _, Spawn as _};

        let scratch = Scratch::directory("carry-seal");
        let (source_checkout, source, from) = scratch.linked_checkout("main", 1);
        let (checkout, destination, into) = scratch.linked_checkout("topic", 1);
        let state = BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
            ],
            fingerprint: None,
        };
        let schema = "CREATE TABLE task_details (hash TEXT PRIMARY KEY NOT NULL, project TEXT NOT NULL, target TEXT NOT NULL, configuration TEXT); CREATE TABLE cache_outputs (hash TEXT PRIMARY KEY NOT NULL, code INTEGER NOT NULL, size INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (hash) REFERENCES task_details(hash));";
        for mount in [&from, &into] {
            state.write(mount).unwrap();
            fs::create_dir_all(mount.join("nx/workspace-data")).unwrap();
            let made = std::process::Command::new("/usr/bin/sqlite3")
                .arg(mount.join("nx/workspace-data/task.db"))
                .arg(schema)
                .output_locked()
                .unwrap();
            assert!(made.status.success(), "{made:?}");
        }
        let source_database = from.join("nx/workspace-data/task.db");
        let seeded = std::process::Command::new("/usr/bin/sqlite3")
            .arg(&source_database)
            .arg("INSERT INTO task_details VALUES ('1', 'a', 'build', NULL); INSERT INTO cache_outputs (hash,code,size) VALUES ('1',0,4);")
            .output_locked()
            .unwrap();
        assert!(seeded.status.success(), "{seeded:?}");
        fs::create_dir_all(from.join("nx/cache/1")).unwrap();
        fs::write(from.join("nx/cache/1/output"), b"warm").unwrap();
        for id in [&source, &destination] {
            drop(scratch.layout.hold(id).unwrap());
        }
        let sockets = Path::new("/tmp").join(format!(
            "cs-carry-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        fs::create_dir_all(&sockets).unwrap();
        let socket = sockets.join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let admission = sockets.join("admission");
        let data = from.join("nx/workspace-data");
        fs::create_dir_all(data.join(nx::DAEMON_DIRECTORY)).unwrap();
        let mut daemon = std::process::Command::new("/usr/bin/perl")
            .args([
                "-e",
                r#"use Fcntl qw(:flock);
                my ($source, $destination, $database, $out) = @ARGV;
                $SIG{TERM} = sub {
                    open(my $o, '>', $out) or die "open $out: $!";
                    for my $hold ($source, $destination) {
                        open(my $h, '+<', $hold) or die "open $hold: $!";
                        if (flock($h, LOCK_EX | LOCK_NB)) {
                            print $o "claimed\n";
                            if ($hold eq $source) {
                                rename($database, "$database.sealed") or die "hide $database: $!";
                            }
                        } else { print $o "refused\n"; }
                    }
                    close $o;
                    exit 0;
                };
                $| = 1; print "ready\n";
                select(undef, undef, undef, 600) while 1;"#,
            ])
            .arg(scratch.layout.hold_path(&source))
            .arg(scratch.layout.hold_path(&destination))
            .arg(&source_database)
            .arg(&admission)
            .current_dir(&data)
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let mut ready = String::new();
        std::io::BufRead::read_line(
            &mut std::io::BufReader::new(daemon.stdout.take().unwrap()),
            &mut ready,
        )
        .unwrap();
        assert_eq!(ready, "ready\n");
        fs::write(
            data.join(nx::DAEMON_DIRECTORY).join("server-process.json"),
            serde_json::json!({ "processId": daemon.id(), "socketPath": socket }).to_string(),
        )
        .unwrap();
        let result = scratch
            .volumes
            .rebase_carry(
                checkout,
                source_checkout,
                Ok(BTreeMap::from([(
                    PathBuf::from(".nx"),
                    nx::TaskSelection {
                        hashes: BTreeSet::from(["1".to_owned()]),
                        deferred: BTreeSet::new(),
                    },
                )])),
            )
            .await;
        let stopped = daemon.try_wait().unwrap();
        if stopped.is_none() {
            daemon.kill().unwrap();
            daemon.wait().unwrap();
        }
        let attempted = fs::read_to_string(&admission).unwrap_or_default();
        let output = fs::read(into.join("nx/cache/1/output")).ok();
        let rows = std::process::Command::new("/usr/bin/sqlite3")
            .arg(into.join("nx/workspace-data/task.db"))
            .arg("SELECT count(*) FROM cache_outputs WHERE hash = '1';")
            .output_locked()
            .unwrap();
        drop(listener);
        fs::remove_dir_all(sockets).unwrap();
        scratch.release_all();
        assert!(stopped.is_some(), "the carry stopped the recorded daemon");
        assert_eq!(
            attempted, "refused\nrefused\n",
            "both volume claims must be refused; carry: {result:?}"
        );
        let RebaseBuildVolume::Carried { carried, .. } = result.unwrap() else {
            panic!("carry was skipped")
        };
        assert_eq!(carried.entries, 1, "{carried:?}");
        assert!(carried.stopped.is_none(), "{carried:?}");
        assert_eq!(output.as_deref(), Some(b"warm".as_slice()));
        assert!(rows.status.success(), "{rows:?}");
        assert_eq!(rows.stdout, b"1\n");
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
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
            )
            .unwrap();
        // Short, under /tmp: a Unix socket path is bounded at 104 bytes on macOS.
        let sockets = Path::new("/tmp").join(format!(
            "cs-keep-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        fs::create_dir_all(&sockets).unwrap();
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
                           if ($child == 0) { sleep 600; exit 0 }
                           print $o "admitted $child";
                       } else {
                           print $o "refused";
                       }
                       close $o;
                       exit 0;
                   };
                   $| = 1;
                   print "ready\n";
                   select(undef, undef, undef, 600) while 1;"#,
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
        let reseeded = reseed(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live,
                tree: None,
                nearness: Nearness::Unknown,
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
            daemon.wait().unwrap();
        }
        let attempt = fs::read_to_string(&admission).unwrap_or_default();
        if let Some(child) = attempt.strip_prefix("admitted ") {
            let child: libc::pid_t = child.parse().unwrap();
            // SAFETY: kill takes a pid and a signal number and touches no memory.
            unsafe { libc::kill(child, libc::SIGKILL) };
        }
        drop(listener);
        fs::remove_dir_all(&sockets).unwrap();
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
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: target.name.clone(),
                        incarnation: target.incarnation.clone(),
                    },
                ),
            )
            .unwrap();
        let job = scratch.layout.hold(&live).expect("a job's hold");
        let reseeded = reseed(
            &*scratch.host,
            &scratch.layout,
            &target,
            SeedAge {
                live: live.clone(),
                tree: None,
                nearness: Nearness::Unknown,
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
    /// image put in place and mounted the way a fork's volume is. Creation is not what these
    /// tests prove. Above the 1 GiB test cap only the clone's sparse RAW file length grows and its
    /// container stays the template's: these tests compare virtual image capacities, not guest free space, and
    /// a mint at another capacity would cost the store a template of its own — a `diskutil image
    /// create`, a formatting attach and a detach.
    #[cfg(target_os = "macos")]
    fn test_volume(
        host: &Host,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        gib: u64,
    ) -> PathBuf {
        use cowshed_apfs::{CommandRunner, SystemCommandRunner};
        let image = layout.image(id);
        crate::blank_image::blank_image(&image);
        let capacity = ImageCapacity::from_gibibytes(gib);
        if capacity > crate::blank_image::CAPACITY {
            SystemCommandRunner
                .grow_image(&image, capacity)
                .expect("grow the clone's sparse image length");
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
        assert_eq!(frozen, Freeze::Frozen);
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
        // The previous volume is recorded Unlinked by the move and released by its caller.
        assert!(matches!(
            scratch.layout.read_record(&previous).unwrap().role,
            BuildVolumeRole::Unlinked
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
        use cowshed_os::fork_lock::Run as _;
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
                .output_locked()
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
                .clone()
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
        assert_eq!(scratch.layout.read_record_present(&kept.id).unwrap(), None);
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
        let store = Store::new();
        let checkout = tracked_checkout(
            &store.root,
            "main",
            "[build]\ncapacity = \"not a size\"\n",
            &["package.json"],
        );
        let host = Arc::new(
            MacOsApfsExecutionHost::new(
                SystemCommandRunner,
                crate::storage::apfs::ApfsSubstrateConfig::new(&store.root, &checkout),
            )
            .unwrap(),
        );
        let volumes = BuildVolumes::new(host, store.layout.clone());
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
        assert_eq!(frozen, Freeze::Frozen);
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
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: main.name.clone(),
                        incarnation: main.incarnation.clone(),
                    },
                ),
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
            .lock_images(&[scratch.image_lock(&main.name)], LockMode::Wait)
            .unwrap()
            .expect("a blocking lock");
        assert_eq!(
            scratch
                .volumes
                .refresh_seed(main.clone(), main_checkout.clone(), unlanded_aim())
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
            .try_lock_target(main.clone(), scratch.image_lock(&main.name))
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
                assert_eq!(frozen, Freeze::Frozen);
                let adopted = volumes
                    .adopt(&locked, &quiet, closed, main_checkout, tree)
                    .await?
                    .expect("nothing opened the target's volume since");
                drop(quiet);
                drop(locked);
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
        let (live, mount) = scratch
            .host
            .fork_build_volume(
                &scratch.layout,
                &seed,
                &BuildVolumeRecord::new(record.tree.clone(), linked("lane")),
            )
            .expect("the new clones the seed it read under main's image lock");
        assert!(
            scratch.layout.image(&main_live).exists(),
            "a land released the volume main linked while a new of main held main's image lock"
        );
        let forked = scratch
            .volumes
            .finish_fork(
                Some(PreparedFork {
                    seed: seed.clone(),
                    tree: record.tree,
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

    /// Every build volume release is a line in the project's deletion log, saying whether the
    /// volume was a seed and whose its sidecar said it was: a land's release of main's previous
    /// volume once left no trace at all. A held volume is not released, and leaves no line.
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
                    &BuildVolumeRecord::new(None, role),
                )
                .expect("clone main's volume");
            id
        };
        let seed = clone(BuildVolumeRole::Seed {
            target: main.name.clone(),
            incarnation: main.incarnation.clone(),
        });
        let unlinked = clone(BuildVolumeRole::Unlinked);
        let job = scratch.layout.hold(&live).expect("a job's hold");
        assert!(matches!(
            scratch.host.release_build_volume(&scratch.layout, &live),
            Ok(BuildVolumeRelease::Held { .. })
        ));
        drop(job);
        for id in [&seed, &unlinked, &live] {
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
                release(DeletionKind::BuildVolume, "", &unlinked),
                release(DeletionKind::BuildVolume, "main", &live),
            ]
        );
    }
}
