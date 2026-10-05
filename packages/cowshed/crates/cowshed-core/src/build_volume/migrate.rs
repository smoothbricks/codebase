//! Rebuild-only setup migration and recovery after a tool displaced a fixed build-state link.
//!
//! The checkout's pointer is published first so a retry finds the same volume and GC preserves
//! it. Only contributed build-state directories are removed; fixed links, state and the image
//! sidecar follow. A partially removed directory resumes without copying or touching source.

use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use super::{
    BuildStateTool, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeState,
    DisplacedBuildStateFinding, TrackedBuildStateRefusal, link, nx,
};
use crate::apfs::SystemCommandRunner;
use crate::capabilities::BuildStatePath;
use crate::fork_lock::Run;
use crate::metadata::ImageCapacity;
use crate::storage::apfs::native::MacOsApfsExecutionHost;
use crate::{CowshedError, Result};

pub struct FirstTouch<'a> {
    pub checkout: &'a Path,
    pub paths: &'a [BuildStatePath],
    pub fingerprint: String,
    pub capacity: ImageCapacity,
    pub record: BuildVolumeRecord,
}

/// The checkout's first build volume (or the one an interrupted first touch already linked),
/// with every contributed path linked into it, and the real directories it discarded.
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
        record,
    } = migration;
    let real = preflight(checkout, paths)?;
    if let Some(refusal) = tracked_source(checkout, &real)? {
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
            let id = BuildVolumeId::mint();
            let mount = host
                .create_build_volume(layout, &id, capacity)
                .map_err(storage_error)?;
            // The actual link protects this unrecorded image from GC and lets a retry find it.
            link::point(checkout, &mount)?;
            id
        }
    };
    let mount = host
        .mount_build_volume(layout, &id)
        .map_err(storage_error)?;
    let previous = BuildVolumeState::read(&mount)?;
    let (state, _) = previous.with_discovered(paths, fingerprint, &mount)?;
    let displaced = adopt_preflighted(checkout, &mount, paths, &real)?;
    state.write(&mount)?;
    // Published only after every source directory has become its fixed link.
    layout.write_record(&id, &record)?;
    Ok(Ok((id, displaced)))
}

/// Exact links are unchanged. Contributed real directories are discarded and relinked, never
/// copied. Files and foreign links refuse before any build-state directory is removed.
pub fn adopt_paths(
    checkout: &Path,
    volume_root: &Path,
    paths: &[BuildStatePath],
) -> Result<std::result::Result<Vec<DisplacedBuildStateFinding>, TrackedBuildStateRefusal>> {
    let real = preflight(checkout, paths)?;
    if let Some(refusal) = tracked_source(checkout, &real)? {
        return Ok(Err(refusal));
    }
    adopt_preflighted(checkout, volume_root, paths, &real).map(Ok)
}

fn adopt_preflighted(
    checkout: &Path,
    volume_root: &Path,
    paths: &[BuildStatePath],
    real: &[&BuildStatePath],
) -> Result<Vec<DisplacedBuildStateFinding>> {
    if real.iter().any(|state| tool(state) == BuildStateTool::Nx) {
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
    let mut findings = Vec::with_capacity(real.len());
    for state in real {
        let source = checkout.join(state.checkout.as_path());
        fs::remove_dir_all(&source)
            .map_err(|error| io_error("discard old build state", &source, error))?;
        findings.push(DisplacedBuildStateFinding {
            path: state.checkout.as_path().to_owned(),
            likely_tool: tool(state),
        });
    }
    link::link_paths(checkout, volume_root, paths)?;
    Ok(findings)
}

fn preflight<'a>(checkout: &Path, paths: &'a [BuildStatePath]) -> Result<Vec<&'a BuildStatePath>> {
    super::disjoint(paths).map_err(|(left, right)| {
        super::overlapping(&checkout.join(super::STATE_FILE), left, right)
    })?;
    let mut real = Vec::new();
    for state in paths {
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
    Ok(real)
}

fn tracked_source(
    checkout: &Path,
    real: &[&BuildStatePath],
) -> Result<Option<TrackedBuildStateRefusal>> {
    if real.is_empty() {
        return Ok(None);
    }
    let output = crate::git::git_command_at(checkout)
        .arg("--literal-pathspecs")
        .args(["ls-files", "-z", "--"])
        .args(real.iter().map(|state| state.checkout.as_path()))
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error))?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            format!(
                "cannot prove migration paths contain no tracked source: {}",
                String::from_utf8_lossy(&output.stderr).trim_end()
            ),
            "repair the checkout's Git index and retry; nothing has been deleted",
        ));
    }
    for state in real {
        let tracked_files: Vec<PathBuf> = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| Path::new(std::ffi::OsStr::from_bytes(name)))
            .filter(|path| path.starts_with(state.checkout.as_path()))
            .map(Path::to_owned)
            .collect();
        if !tracked_files.is_empty() {
            return Ok(Some(TrackedBuildStateRefusal {
                path: state.checkout.as_path().to_owned(),
                tracked_files,
            }));
        }
    }
    Ok(None)
}

fn tool(state: &BuildStatePath) -> BuildStateTool {
    let checkout = state.checkout.as_path();
    if checkout
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == ".nx")
    {
        BuildStateTool::Nx
    } else if checkout
        .file_name()
        .is_some_and(|name| name == ".codegraph")
    {
        BuildStateTool::Codegraph
    } else {
        BuildStateTool::Cargo
    }
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
