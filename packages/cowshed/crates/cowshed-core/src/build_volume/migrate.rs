//! Rebuild-only setup migration and recovery after a tool displaced a fixed build-state link.
//!
//! The checkout's pointer is published first so a retry finds the same volume and GC preserves
//! it. Only contributed build-state directories are removed; fixed links, state and the image
//! sidecar follow. A partially removed directory resumes without copying or touching source.

use std::fs;
use std::io;
use std::path::Path;

use super::{
    BuildStateTool, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeState,
    DisplacedBuildStateFinding, TrackedBuildStateRefusal, discard, link, nx, tracked_source,
};
use crate::apfs::SystemCommandRunner;
use crate::capabilities::BuildStatePath;
use crate::metadata::ImageCapacity;
use crate::storage::apfs::native::MacOsApfsExecutionHost;
use crate::{CowshedError, Result};

pub struct FirstTouch<'a> {
    pub checkout: &'a Path,
    pub paths: &'a [BuildStatePath],
    pub fingerprint: String,
    /// The capacity the checkout's `.cowshed.toml` asks of its first volume.
    pub capacity: ImageCapacity,
    pub beside: MintedBeside,
    pub record: BuildVolumeRecord,
}

/// What `adopt` minted beside main's image for main's first touch, which decides it only when
/// the checkout links no volume and needs one: the first touch of any other checkout, or of a
/// replayed adoption, mints its own.
#[derive(Clone, Debug)]
pub enum MintedBeside {
    /// Nothing: no adopt minted one, or nothing in the checkout can name build state.
    Nothing,
    /// Mounted, linked by nothing and without a record. The first touch links it when it was
    /// minted at the capacity asked now, and mints its own otherwise; the adopt releases it when
    /// the checkout does not link it.
    Volume(MintedVolume),
    /// The mint failed. A first touch that needs a volume answers this error and does not mint
    /// again: it is the volume's creation failing, as when the first touch minted it.
    Failed(CowshedError),
}

/// An empty build volume minted and mounted at its layout mountpoint at `capacity`, without a
/// record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MintedVolume {
    pub id: BuildVolumeId,
    pub capacity: ImageCapacity,
}

/// The checkout's first build volume (or the one an interrupted first touch already linked),
/// with every contributed path linked into it (a declared one once it exists), and the real
/// directories it discarded.
pub fn first_touch(
    host: &MacOsApfsExecutionHost<SystemCommandRunner>,
    layout: &BuildVolumeLayout,
    migration: FirstTouch<'_>,
) -> Result<
    std::result::Result<(BuildVolumeId, Vec<DisplacedBuildStateFinding>), TrackedBuildStateRefusal>,
> {
    let FirstTouch {
        checkout,
        paths,
        fingerprint,
        capacity,
        beside,
        record,
    } = migration;
    let found = preflight(checkout, paths)?;
    if let Some(refusal) = tracked_source(checkout, &found.real)? {
        return Ok(Err(refusal));
    }
    let id = match link::linked(checkout)? {
        Some(mount) => layout.volume_at(&mount).ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "{} names {}, not one of this project's build volumes",
                    checkout.join(link::BUILD_LINK).display(),
                    mount.display()
                ),
                "repair the build link and retry cowshed setup",
            )
        })?,
        None => {
            let id = match beside {
                MintedBeside::Volume(minted) if minted.capacity == capacity => minted.id,
                MintedBeside::Failed(error) => return Err(error),
                MintedBeside::Nothing | MintedBeside::Volume(_) => {
                    let id = BuildVolumeId::mint();
                    host.create_build_volume(layout, &id, capacity)
                        .map_err(storage_error)?;
                    id
                }
            };
            // The actual link protects this unrecorded image from GC and lets a retry find it.
            link::point(checkout, &layout.mount(&id))?;
            id
        }
    };
    let mount = host
        .mount_build_volume(layout, &id)
        .map_err(storage_error)?;
    let previous = BuildVolumeState::read(&mount)?;
    let (state, _) = previous.with_discovered(paths, fingerprint, &mount)?;
    let displaced = adopt_preflighted(checkout, &mount, paths, &found)?;
    state.write(&mount)?;
    // Published only after every source directory has become its fixed link.
    layout.write_record(&id, &record)?;
    Ok(Ok((id, displaced)))
}

/// Exact links are unchanged. Contributed real directories are discarded and relinked, never
/// copied. Files and foreign links refuse before any build-state directory is removed. A
/// declared path nothing occupies stays absent: tools read a directory's existence as meaning
/// (a pin worktree is there or it is not), so its link and volume directory appear only once a
/// tool has made the directory, at the next refresh after.
pub fn adopt_paths(
    checkout: &Path,
    volume_root: &Path,
    paths: &[BuildStatePath],
) -> Result<std::result::Result<Vec<DisplacedBuildStateFinding>, TrackedBuildStateRefusal>> {
    let found = preflight(checkout, paths)?;
    if let Some(refusal) = tracked_source(checkout, &found.real)? {
        return Ok(Err(refusal));
    }
    adopt_preflighted(checkout, volume_root, paths, &found).map(Ok)
}

fn adopt_preflighted(
    checkout: &Path,
    volume_root: &Path,
    paths: &[BuildStatePath],
    found: &Found<'_>,
) -> Result<Vec<DisplacedBuildStateFinding>> {
    let real = &found.real;
    if real
        .iter()
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
        };
        if let Err(busy) = nx::close(checkout, &source_state)
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
    // the whole tree aside atomically, into `.cowshed/discard` on the same volume and outside
    // the source tree, the link takes the path at once, and the aside copy is deleted in the
    // background: a late writer lands in the aside tree or follows the new link, never in a
    // half-deleted path, and no job waits on the delete (`discard`).
    // Excluded first: a process that dies after a move aside leaves nothing `git status` shows.
    link::exclude_links(checkout, &found.linked)?;
    let mut findings = Vec::with_capacity(real.len());
    for state in real {
        let source = state.checkout.as_path();
        discard::move_aside(checkout, source).map_err(|error| {
            io_error("move old build state aside", &checkout.join(source), error)
        })?;
        findings.push(DisplacedBuildStateFinding {
            path: source.to_owned(),
            likely_tool: BuildStateTool::of(state),
        });
    }
    link::link_paths(checkout, volume_root, &found.linked)?;
    // Also whatever an earlier refresh moved aside and did not live to delete.
    discard::reap(checkout);
    Ok(findings)
}

/// What preflight found at the build-state paths.
struct Found<'a> {
    /// Real directories: discarded, then linked.
    real: Vec<&'a BuildStatePath>,
    /// The paths to link: every one except a declared path nothing occupies yet.
    linked: Vec<BuildStatePath>,
}

fn preflight<'a>(checkout: &Path, paths: &'a [BuildStatePath]) -> Result<Found<'a>> {
    super::disjoint(paths).map_err(|(left, right)| {
        super::overlapping(&checkout.join(super::STATE_FILE), left, right)
    })?;
    let mut real = Vec::new();
    let mut linked = Vec::with_capacity(paths.len());
    for state in paths {
        if state.is_declared() {
            // Looked at before `contained_parent`, which makes missing parents: an absent
            // declared path makes nothing, its parents included.
            let at = checkout.join(state.checkout.as_path());
            match fs::symlink_metadata(&at) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(io_error("inspect build state", &at, error)),
                Ok(_) => {}
            }
        }
        linked.push(state.clone());
        let (parent, name) =
            link::contained_parent(checkout, state.checkout.as_path()).map_err(|error| {
                io_error(
                    "prepare build-state parent",
                    &checkout.join(state.checkout.as_path()),
                    error,
                )
            })?;
        let source = parent.join(name);
        match fs::symlink_metadata(&source) {
            Ok(metadata) if metadata.is_dir() => real.push(state),
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    && fs::read_link(&source)
                        .map_err(|error| io_error("read build-state link", &source, error))?
                        == link::relative_target(
                            state.checkout.as_path(),
                            state.volume.as_path(),
                        ) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("inspect build state", &source, error)),
            Ok(_) => {
                return Err(CowshedError::integrity(
                    format!(
                        "{} holds a file or foreign symlink instead of contributed build state",
                        source.display()
                    ),
                    "move the foreign entry aside and retry",
                ));
            }
        }
    }
    Ok(Found { real, linked })
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
