//! Rebuild-only setup migration and recovery after a tool displaced a fixed build-state link.
//!
//! The volume's owner record is registered before its image exists and the checkout's pointer
//! is published next, so a retry finds the same volume and GC preserves it. What happens at each
//! build-state path is one pure decision, [`decide`], from whose state the path holds and what
//! is there; the shell here only observes each path, decides every one before it changes any,
//! and performs. A tool's own directory is discarded; a declared directory holding anything is
//! kept intact ([`kept`]); a file or a foreign link refuses. A partially removed directory
//! resumes without copying or touching source.

use std::fs;
use std::io;
use std::path::Path;
use std::time::SystemTime;

use super::{
    BuildStateTool, BuildVolumeId, BuildVolumeLayout, BuildVolumeState, DiscardOrigin,
    DiscardedBuildStateFinding, KeptBuildStateFinding, Registration, TrackedBuildStateRefusal,
    discard, kept, link, nx, tracked_source,
};
use crate::capabilities::BuildStatePath;
use crate::runtime::storage_host::RuntimeStorageHost;
use crate::{CowshedError, Result};
use cowshed_apfs::image::ImageCapacity;

pub struct FirstTouch<'a> {
    pub checkout: &'a Path,
    pub paths: &'a [BuildStatePath],
    pub fingerprint: String,
    /// The realpath of the mbx discovery resolved in the checkout's canonical job environment.
    pub mbx: Option<std::path::PathBuf>,
    /// The capacity the checkout's `.cowshed.toml` asks of its first volume.
    pub capacity: ImageCapacity,
    pub beside: MintedBeside,
    /// The checkout's owner record: registered before a volume it mints exists, published once
    /// every contributed path is linked into the volume.
    pub registration: Registration,
}

/// What `adopt` minted beside main's image for main's first touch, which decides it only when
/// the checkout links no volume and needs one: the first touch of any other checkout, or of a
/// replayed adoption, mints its own.
#[derive(Clone, Debug)]
pub enum MintedBeside {
    /// Nothing: no adopt minted one, or nothing in the checkout can name build state.
    Nothing,
    /// Mounted, linked by nothing, its record registered to main and not yet published. The
    /// first touch links it when it was minted at the capacity asked now, and mints its own
    /// otherwise; the adopt releases it when the checkout does not link it.
    Volume(MintedVolume),
    /// The mint failed. A first touch that needs a volume answers this error and does not mint
    /// again: it is the volume's creation failing, as when the first touch minted it.
    Failed(CowshedError),
}

/// An empty build volume minted and mounted at its layout mountpoint at `capacity`, its record
/// registered and not yet published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MintedVolume {
    pub id: BuildVolumeId,
    pub capacity: ImageCapacity,
}

/// The checkout's first build volume (or the one an interrupted first touch already linked),
/// with every contributed path linked into it (a declared one once it exists, or at the next
/// refresh once its volume holds it with entries), and the real directories it discarded or kept.
pub(crate) fn first_touch<H: RuntimeStorageHost>(
    host: &H,
    layout: &BuildVolumeLayout,
    migration: FirstTouch<'_>,
) -> Result<std::result::Result<(BuildVolumeId, Adopted), TrackedBuildStateRefusal>> {
    let FirstTouch {
        checkout,
        paths,
        fingerprint,
        mbx,
        capacity,
        beside,
        registration,
    } = migration;
    // Decided before the volume is mounted: a minted volume holds nothing, and an interrupted
    // first touch's holds a declared directory only behind the link it made.
    let plan = preflight(checkout, None, paths)?;
    if let Some(refusal) = tracked_source(checkout, &plan.removed())? {
        return Ok(Err(refusal));
    }
    // A volume minted here stays held by its creation until its record is published below.
    let (id, _creating) = match link::linked(checkout)? {
        Some(mount) => {
            let id = layout.volume_at(&mount).ok_or_else(|| {
                CowshedError::integrity(
                    format!(
                        "{} names {}, not one of this project's build volumes",
                        checkout.join(link::BUILD_LINK).display(),
                        mount.display()
                    ),
                    "repair the build link and retry cowshed setup",
                )
            })?;
            (id, None)
        }
        None => {
            let (id, creating) = match beside {
                MintedBeside::Volume(minted) if minted.capacity == capacity => (minted.id, None),
                MintedBeside::Failed(error) => return Err(error),
                MintedBeside::Nothing | MintedBeside::Volume(_) => {
                    let id = BuildVolumeId::mint();
                    let (_, creating) = host
                        .create_build_volume(layout, &id, capacity, &registration)
                        .map_err(storage_error)?;
                    (id, Some(creating))
                }
            };
            // Linked while a creation still holds it: from here the link protects the
            // registered volume from GC and lets a retry find it.
            link::point(checkout, &layout.mount(&id))?;
            (id, creating)
        }
    };
    let (mount, _) = host
        .mount_build_volume(layout, &id)
        .map_err(storage_error)?;
    let previous = BuildVolumeState::read(&mount)?;
    let (state, _, _) = previous.with_discovered(paths, &[], mbx, fingerprint, &mount)?;
    let adopted = adopt_preflighted(
        host.census(),
        checkout,
        &mount,
        paths,
        &previous.paths,
        &plan,
    )?;
    state.write(&mount)?;
    // Published only after every source directory has become its fixed link.
    layout.publish(&id, &registration)?;
    Ok(Ok((id, adopted)))
}

/// What adopting a checkout's build-state paths did to the real directories standing where their
/// links belong.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Adopted {
    /// Removed: a tool's own directory, discarded, or an empty one.
    pub discarded: Vec<DiscardedBuildStateFinding>,
    /// A declared directory holding entries, moved aside intact: no refresh or `gc` deletes it.
    pub kept: Vec<KeptBuildStateFinding>,
}

/// Make every build-state path what [`decide`] says, after deciding all of them: a file or a
/// foreign link anywhere refuses, and a path holding tracked source refuses, before any path
/// changes. A declared path nothing occupies is linked only where the volume mounted at
/// `volume_root` holds its directory with entries: tools read a directory's existence as
/// meaning (a pin worktree is there or it is not), so a declared link appears only where a tool
/// made the directory, in this checkout (at the next refresh after) or in one whose volume this
/// checkout cloned or adopted. `held` is what the volume's state held before this refresh, which
/// gives each removed or kept directory its [`DiscardOrigin`].
pub fn adopt_paths(
    census: &crate::census::CensusScope,
    checkout: &Path,
    volume_root: &Path,
    paths: &[BuildStatePath],
    held: &[BuildStatePath],
) -> Result<std::result::Result<Adopted, TrackedBuildStateRefusal>> {
    let plan = preflight(checkout, Some(volume_root), paths)?;
    if let Some(refusal) = tracked_source(checkout, &plan.removed())? {
        return Ok(Err(refusal));
    }
    adopt_preflighted(census, checkout, volume_root, paths, held, &plan).map(Ok)
}

fn adopt_preflighted(
    census: &crate::census::CensusScope,
    checkout: &Path,
    volume_root: &Path,
    paths: &[BuildStatePath],
    held: &[BuildStatePath],
    plan: &Plan<'_>,
) -> Result<Adopted> {
    if plan
        .removed()
        .into_iter()
        .any(|state| BuildStateTool::of(state) == BuildStateTool::Nx)
    {
        let source_paths = paths
            .iter()
            .map(|state| {
                BuildStatePath::from_paths(state.checkout.as_path(), state.checkout.as_path())
            })
            .collect::<Result<Vec<_>>>()?;
        let source_state = BuildVolumeState {
            paths: source_paths,
            fingerprint: None,
            mbx: None,
        };
        if let Err(busy) = nx::close(census, checkout, &source_state)
            .map_err(|error| io_error("close Nx state before migration", checkout, error))?
        {
            return Err(CowshedError::conflict(
                format!("cannot migrate build state: {busy}"),
                "let the named database holders finish, then retry",
            ));
        }
    }
    // WHY rename first: a shell entering the checkout (its toolchain stamp) or a build can write
    // into a build-state directory while it is being deleted, and a recursive delete of a
    // directory something is still filling fails with "Directory not empty". `rename(2)` moves
    // the whole tree aside atomically, into `.cowshed/discard` (or, for a declared directory,
    // `.cowshed/kept`) on the same volume and outside the source tree, the link takes the path
    // at once, and a discarded tree is deleted in the background: a late writer lands in the
    // aside tree or follows the new link, never in a half-deleted path, and no job waits on the
    // delete (`discard`).
    // Excluded first: a process that dies after a move aside leaves nothing `git status` shows.
    let linked = plan.linked();
    link::exclude_links(checkout, &linked)?;
    let mut adopted = Adopted::default();
    // `rmdir(2)` first: it is the one step a writer racing the refresh can fail, and failing
    // before any rename leaves no directory moved and unlinked behind the conflict.
    for &(state, action) in &plan.steps {
        if action != Action::ReplaceEmpty {
            continue;
        }
        let source = state.checkout.as_path();
        let at = checkout.join(source);
        let origin = discard_origin(volume_root, held, state)?;
        // Never a recursive delete: an entry a tool wrote after the directory was seen empty
        // fails the refresh instead of being lost with it.
        fs::remove_dir(&at).map_err(|error| {
            if error.kind() == io::ErrorKind::DirectoryNotEmpty {
                CowshedError::conflict(
                    format!(
                        "{} gained an entry while the refresh replaced it as an empty directory, \
                         so it was left as it is",
                        at.display()
                    ),
                    "retry: the next refresh decides on what is there now",
                )
            } else {
                io_error("remove empty build-state directory", &at, error)
            }
        })?;
        adopted.discarded.push(DiscardedBuildStateFinding {
            path: source.to_owned(),
            tool: BuildStateTool::of(state),
            origin,
        });
    }
    for &(state, action) in &plan.steps {
        let source = state.checkout.as_path();
        let at = checkout.join(source);
        match action {
            Action::LeaveAbsent | Action::Keep | Action::Link | Action::ReplaceEmpty => {}
            Action::Discard => {
                let origin = discard_origin(volume_root, held, state)
                    .map_err(|error| naming_kept(&adopted, error))?;
                discard::move_aside(checkout, source).map_err(|error| {
                    naming_kept(&adopted, io_error("move old build state aside", &at, error))
                })?;
                adopted.discarded.push(DiscardedBuildStateFinding {
                    path: source.to_owned(),
                    tool: BuildStateTool::of(state),
                    origin,
                });
            }
            Action::MoveAside => {
                let origin = discard_origin(volume_root, held, state)
                    .map_err(|error| naming_kept(&adopted, error))?;
                let kept =
                    kept::keep_aside(checkout, source, SystemTime::now()).map_err(|error| {
                        naming_kept(
                            &adopted,
                            io_error("keep declared build state aside", &at, error),
                        )
                    })?;
                adopted.kept.push(KeptBuildStateFinding {
                    path: source.to_owned(),
                    kept,
                    origin,
                });
            }
        }
    }
    link::link_paths(checkout, volume_root, &linked)
        .map_err(|error| naming_kept(&adopted, error))?;
    // Also whatever an earlier refresh moved aside and did not live to delete.
    discard::reap(checkout);
    Ok(adopted)
}

/// Why `state`'s real directory stood where its link belongs. Displaced only where the volume
/// held the path and holds its directory: [`link::link_paths`] makes that directory with the
/// link, and a declared path is held before its first link. Anything else predates its link.
fn discard_origin(
    volume_root: &Path,
    held: &[BuildStatePath],
    state: &BuildStatePath,
) -> Result<DiscardOrigin> {
    if !held.iter().any(|path| path.checkout == state.checkout) {
        return Ok(DiscardOrigin::Predated);
    }
    Ok(match behind(Some(volume_root), state)? {
        link::VolumeHolds::Nothing => DiscardOrigin::Predated,
        link::VolumeHolds::EmptyDirectory | link::VolumeHolds::Directory => {
            DiscardOrigin::Displaced
        }
    })
}

/// What a refresh finds at one build-state path, observed before it changes any.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Occupant {
    /// Nothing, with what the volume holds behind the path: a declared path is linked only over
    /// a volume directory holding entries.
    Absent(link::VolumeHolds),
    /// The path's fixed link: a symlink whose text is [`link::relative_target`], dangling or not.
    FixedLink,
    /// A symlink with any other text, dangling or not.
    ForeignLink,
    /// A real directory with no entry.
    EmptyDirectory,
    /// A real directory with at least one entry.
    Directory,
    /// A regular file, FIFO, socket or device.
    File,
}

/// What a refresh does at one build-state path ([`decide`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    /// A declared path nothing occupies, whose volume directory holds no entry, stays so: no
    /// link, no volume directory, no parent.
    LeaveAbsent,
    /// The fixed link stays; its volume directory is made if it is missing.
    Keep,
    /// The fixed link is made where nothing is: a capability's path, or a declared one whose
    /// volume directory holds entries.
    Link,
    /// A declared path's empty directory is removed by `rmdir(2)`, which fails rather than delete
    /// an entry written since it was observed, and the link takes the path.
    ReplaceEmpty,
    /// A tool's own directory, empty or not, which its next build rebuilds, is discarded (renamed into
    /// `.cowshed/discard` and deleted in the background), and the link takes the path.
    Discard,
    /// A declared directory is renamed, intact, into `.cowshed/kept`, which nothing of cowshed's
    /// deletes, and the link takes the path.
    MoveAside,
}

/// A path holding what no refresh may remove or link over: a file, or a link it did not make.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Refuse;

/// The whole policy of a refresh at one build-state path (16_build_volumes.md, "What a refresh
/// does at each path"): from the tool whose state the path holds and what is there, what the
/// refresh does. Pure; [`adopt_paths`] only observes and performs. Its invariant, which the
/// property tests hold it to: a file and a link are never removed, a declared path loses nothing
/// but an empty directory, only a tool's own directory is discarded, and a declared path
/// nothing occupies is linked only over a volume directory holding entries.
pub const fn decide(
    tool: BuildStateTool,
    occupant: Occupant,
) -> std::result::Result<Action, Refuse> {
    match occupant {
        Occupant::Absent(holds) => Ok(match (tool, holds) {
            (
                BuildStateTool::Declared,
                link::VolumeHolds::Nothing | link::VolumeHolds::EmptyDirectory,
            ) => Action::LeaveAbsent,
            (BuildStateTool::Declared, link::VolumeHolds::Directory)
            | (BuildStateTool::Cargo | BuildStateTool::Nx | BuildStateTool::JavaScript, _) => {
                Action::Link
            }
        }),
        Occupant::FixedLink => Ok(Action::Keep),
        Occupant::EmptyDirectory => Ok(match tool {
            BuildStateTool::Declared => Action::ReplaceEmpty,
            BuildStateTool::Cargo | BuildStateTool::Nx | BuildStateTool::JavaScript => {
                Action::Discard
            }
        }),
        Occupant::Directory => Ok(match tool {
            BuildStateTool::Declared => Action::MoveAside,
            BuildStateTool::Cargo | BuildStateTool::Nx | BuildStateTool::JavaScript => {
                Action::Discard
            }
        }),
        Occupant::ForeignLink | Occupant::File => Err(Refuse),
    }
}

/// Every build-state path with what the refresh does there, all decided before any changes.
struct Plan<'a> {
    steps: Vec<(&'a BuildStatePath, Action)>,
}

impl<'a> Plan<'a> {
    /// The paths whose real directory leaves its path: what the tracked-source guard reads.
    fn removed(&self) -> Vec<&'a BuildStatePath> {
        self.steps
            .iter()
            .filter(|(_, action)| {
                matches!(
                    action,
                    Action::ReplaceEmpty | Action::Discard | Action::MoveAside
                )
            })
            .map(|&(state, _)| state)
            .collect()
    }

    /// The paths whose fixed link the refresh makes or keeps: all but a declared path left
    /// absent.
    fn linked(&self) -> Vec<BuildStatePath> {
        self.steps
            .iter()
            .filter(|(_, action)| *action != Action::LeaveAbsent)
            .map(|(state, _)| (*state).clone())
            .collect()
    }
}

/// Observe and decide every path. `volume_root` is the mounted build volume the links name; a
/// first touch decides before it mounts one and passes none.
fn preflight<'a>(
    checkout: &Path,
    volume_root: Option<&Path>,
    paths: &'a [BuildStatePath],
) -> Result<Plan<'a>> {
    super::disjoint(paths).map_err(|(left, right)| {
        super::overlapping(&checkout.join(super::STATE_FILE), left, right)
    })?;
    let steps = paths
        .iter()
        .map(|state| {
            let occupant = observe(checkout, volume_root, state)?;
            match decide(BuildStateTool::of(state), occupant) {
                Ok(action) => Ok((state, action)),
                Err(Refuse) => Err(CowshedError::integrity(
                    format!(
                        "{} holds a file or foreign symlink instead of contributed build state",
                        checkout.join(state.checkout.as_path()).display()
                    ),
                    "move the foreign entry aside and retry",
                )),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Plan { steps })
}

/// What is at `state`'s path in `checkout`, and behind an absent one, what the volume at
/// `volume_root` holds ([`behind`]). A declared path is looked at before
/// [`link::contained_parent`], which checks and makes missing parents: a declared path the
/// refresh leaves absent makes nothing, its parents included, and one it links over the
/// volume's directory has its parents checked and made like a capability's.
fn observe(
    checkout: &Path,
    volume_root: Option<&Path>,
    state: &BuildStatePath,
) -> Result<Occupant> {
    // What observe saw behind a declared path it found absent and the refresh will link: the
    // parent check below runs for it, and the observation is not taken twice.
    let mut linked_over = None;
    if state.is_declared() {
        let at = checkout.join(state.checkout.as_path());
        match fs::symlink_metadata(&at) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let absent = Occupant::Absent(behind(volume_root, state)?);
                if matches!(
                    decide(BuildStateTool::Declared, absent),
                    Ok(Action::LeaveAbsent)
                ) {
                    return Ok(absent);
                }
                linked_over = Some(absent);
            }
            Err(error) => return Err(io_error("inspect build state", &at, error)),
            Ok(_) => {}
        }
    }
    let (parent, name) =
        link::contained_parent(checkout, state.checkout.as_path()).map_err(|error| {
            io_error(
                "prepare build-state parent",
                &checkout.join(state.checkout.as_path()),
                error,
            )
        })?;
    let source = parent.join(name);
    let metadata = match fs::symlink_metadata(&source) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return match linked_over {
                Some(absent) => Ok(absent),
                None => behind(volume_root, state).map(Occupant::Absent),
            };
        }
        Err(error) => return Err(io_error("inspect build state", &source, error)),
    };
    if metadata.file_type().is_symlink() {
        let target = fs::read_link(&source)
            .map_err(|error| io_error("read build-state link", &source, error))?;
        return Ok(
            if target == link::relative_target(state.checkout.as_path(), state.volume.as_path()) {
                Occupant::FixedLink
            } else {
                Occupant::ForeignLink
            },
        );
    }
    if !metadata.is_dir() {
        return Ok(Occupant::File);
    }
    match fs::read_dir(&source)
        .map_err(|error| io_error("read build-state directory", &source, error))?
        .next()
    {
        None => Ok(Occupant::EmptyDirectory),
        Some(Ok(_)) => Ok(Occupant::Directory),
        Some(Err(error)) => Err(io_error("read build-state directory", &source, error)),
    }
}

/// What the volume at `volume_root` holds behind an absent path. A first touch observes before
/// its volume is mounted: a minted volume holds nothing, and an interrupted first touch's holds
/// a declared directory only behind the link it made, which [`observe`] finds as that link.
fn behind(volume_root: Option<&Path>, state: &BuildStatePath) -> Result<link::VolumeHolds> {
    let Some(volume_root) = volume_root else {
        return Ok(link::VolumeHolds::Nothing);
    };
    link::volume_holds(volume_root, state).map_err(|error| {
        io_error(
            "inspect build-state volume directory",
            &volume_root.join(state.volume.as_path()),
            error,
        )
    })
}

/// `error`, also naming every declared directory this refresh already moved aside: a failure
/// after one moved returns no findings, so the error is the only place that says where it went.
fn naming_kept(adopted: &Adopted, mut error: CowshedError) -> CowshedError {
    for kept in &adopted.kept {
        error.message.push_str(&format!(
            "; {} was already moved aside intact to {}",
            kept.path.display(),
            kept.kept.display()
        ));
    }
    error
}
fn io_error(operation: &str, path: &Path, error: io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} at {}: {error}", path.display()),
        "repair the named migration path and retry cowshed setup",
    )
}

fn storage_error(error: crate::storage::apfs::ApfsStorageError) -> CowshedError {
    CowshedError::environment_missing(
        format!("build-state migration storage failed: {error}"),
        "cowshed doctor --json",
    )
}

#[cfg(test)]
mod tests;
