//! Rekey a keyless workspace: rebuild its CA identity without moving its image.
//!
//! One broken workspace must never take down the store. When a canonical
//! workspace loses its CA companion (or the companion stops parsing), the
//! recovery pass quarantines the grants sidecar beside a `tombstone.json`
//! under `<project>/quarantine/<ws>-<unix_ts>/` and leaves the image in
//! place. [`rekey_workspace`] consumes that tombstone (or, when the sidecar
//! never left the canonical path, the live sidecar) and heals the workspace:
//!
//! 1. prove the workspace is mounted — the in-image marker must name this
//!    exact workspace incarnation, because minting writes the certificate
//!    and token into that mount and a bare directory would be shadowed on
//!    the next attach;
//! 2. republish the grants sidecar beside the still-in-place image,
//!    preserving workspace and incarnation and, on the quarantine path,
//!    bumping the revision by exactly one so any gateway removal tombstone
//!    for the old revision is superseded (the live path preserves the
//!    revision: nothing was removed, so there is nothing to supersede);
//! 3. mint fresh credentials through [`mint_workspace_credentials`](crate::workspace_credentials::mint_workspace_credentials),
//!    which owns the companion's `0600` + fsync + parent-fsync durability
//!    ordering — this module never re-spells it;
//! 4. remove the quarantine entry, then verify: the sidecar reads back
//!    `active` for this exact incarnation and the companion satisfies the
//!    published [`MissingCaCompanion`](super::ApfsStorageError::MissingCaCompanion)
//!    gate (present, regular file, mode `0600`, valid private key).
//!
//! Rotation invalidates in-flight job certificates: they were signed by the
//! lost CA generation, and the new companion anchors a new one. The revision
//! derivation reads the quarantined copy rather than any canonical residue,
//! so a crash between the sidecar write and the companion write retries to
//! the same revision instead of drifting.
//!
//! This module is mount-agnostic on purpose: the caller supplies the mount
//! point (a live degraded mount in production, a scratch directory carrying
//! the marker in tests) and this module proves it before minting.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::metadata::{
    DetachedWorkspaceMetadata, ImageFormat, PublicationState, WorkspaceIncarnation,
    WorkspaceMarker, WorkspaceName, WorkspaceRole, image_from_sidecar_path, read_json,
    sidecar_path,
};
use crate::repository::RepoId;
use crate::storage::{StorageLayout, WORKSPACE_MARKER_PATH};

use super::ApfsStorageError;

/// Layout label carried on the [`MissingCaCompanion`](super::ApfsStorageError::MissingCaCompanion)
/// gate this module verifies against after minting.
pub const REKEY_LAYOUT: &str = "rekey";

/// What one successful [`rekey_workspace`] rebuilt, and where.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RekeyReport {
    /// The workspace that is attachable again.
    pub workspace: WorkspaceName,
    /// Preserved across the rotation: rekey never mints an incarnation.
    pub incarnation: WorkspaceIncarnation,
    /// The republished grants revision (quarantined revision + 1, or the
    /// unchanged live revision when the sidecar never left).
    pub revision: u64,
    /// The republished canonical grants sidecar.
    pub sidecar: PathBuf,
    /// The freshly minted canonical CA companion (mode `0600`).
    pub companion: PathBuf,
    /// The consumed quarantine entry, when the quarantine path ran.
    pub tombstone_removed: Option<PathBuf>,
}

/// How [`rekey_workspace`] can refuse. Every variant maps to a
/// [`CowshedError`](crate::CowshedError) through [`RekeyError::into_cowshed_error`];
/// no arm hints at `doctor` — the remedy is always `rekey` itself, `attach`,
/// or the named missing input.
#[derive(Debug, Error)]
pub enum RekeyError {
    /// Neither a quarantine entry nor a live sidecar names this workspace.
    #[error("no rekeyable identity for workspace '{workspace}': {detail}")]
    NoIdentity { workspace: String, detail: String },
    /// The companion is already present and valid: there is nothing to rotate.
    #[error("workspace '{workspace}' already has a valid CA companion at {companion}")]
    AlreadyKeyed {
        workspace: String,
        companion: PathBuf,
    },
    /// The mount does not prove this workspace: absent or mismatched marker.
    #[error("workspace '{workspace}' is not mounted at {mount}: {detail}")]
    NotMounted {
        workspace: String,
        mount: PathBuf,
        detail: String,
    },
    /// The tombstone names an image this project does not own, or the
    /// quarantined records disagree with each other.
    #[error("quarantine identity for workspace '{workspace}' is inconsistent: {detail}")]
    IdentityMismatch { workspace: String, detail: String },
    /// The quarantined revision is already at `u64::MAX` and cannot bump.
    #[error("quarantined revision {revision} for workspace '{workspace}' cannot bump")]
    RevisionOverflow { workspace: String, revision: u64 },
    /// The sidecar carries no info snapshot, so no attachable record can be rebuilt.
    #[error("quarantined sidecar for workspace '{workspace}' has no info snapshot")]
    MissingInfoSnapshot { workspace: String },
    /// A store read or write failed.
    #[error("rekey I/O for workspace '{workspace}': {detail}")]
    Io { workspace: String, detail: String },
    /// The published companion gate failed after minting.
    #[error(transparent)]
    Storage(#[from] ApfsStorageError),
    /// Credential minting failed.
    #[error("rekey mint for workspace '{workspace}': {detail}")]
    Mint { workspace: String, detail: String },
}

impl RekeyError {
    /// Operational mapping. Integrity for broken data, not-found for unknown
    /// workspaces, conflict when there is nothing to rotate,
    /// environment-missing when the mount is absent. `internal` is
    /// deliberately unused: its hint names `doctor`, which is never the
    /// remedy for a rekey refusal.
    pub fn into_cowshed_error(self) -> crate::CowshedError {
        use crate::{CowshedError, ErrorCode};
        match self {
            Self::NoIdentity { workspace, detail } => CowshedError::new(
                ErrorCode::NotFound,
                format!("no rekeyable identity for workspace '{workspace}': {detail}"),
                "cowshed ls",
            ),
            Self::AlreadyKeyed { workspace, .. } => CowshedError::conflict(
                format!("workspace '{workspace}' already has a valid CA companion"),
                format!("cowshed attach {workspace}"),
            ),
            Self::NotMounted {
                workspace, mount, ..
            } => {
                let remedy = if workspace == "main" {
                    "cowshed mount main --repo-id <owner/repo>".to_owned()
                } else {
                    format!("cowshed attach {workspace}")
                };
                CowshedError::environment_missing(
                    format!(
                        "workspace '{workspace}' is not mounted at {}: rekey mints fresh CA into the live mount",
                        mount.display()
                    ),
                    remedy,
                )
            }
            Self::IdentityMismatch { workspace, detail } => CowshedError::integrity(
                format!(
                    "quarantine identity for workspace '{workspace}' is inconsistent: {detail}"
                ),
                format!("cowshed rekey {workspace}"),
            ),
            Self::RevisionOverflow {
                workspace,
                revision,
            } => CowshedError::integrity(
                format!("quarantined revision {revision} for workspace '{workspace}' cannot bump"),
                format!("cowshed rekey {workspace}"),
            ),
            Self::MissingInfoSnapshot { workspace } => CowshedError::integrity(
                format!("quarantined sidecar for workspace '{workspace}' has no info snapshot"),
                format!("cowshed rekey {workspace}"),
            ),
            Self::Io { workspace, detail } => CowshedError::integrity(
                format!("rekey I/O for workspace '{workspace}': {detail}"),
                format!("cowshed rekey {workspace}"),
            ),
            Self::Mint { workspace, detail } => CowshedError::integrity(
                format!("rekey mint for workspace '{workspace}': {detail}"),
                format!("cowshed rekey {workspace}"),
            ),
            Self::Storage(error) => {
                CowshedError::integrity(error.to_string(), "cowshed rekey <ws>")
            }
        }
    }
}

/// Rebuild the CA identity of one keyless workspace and make it attachable.
///
/// See the [module](self) docs for the ordering, the revision policy, and
/// why the mount must already exist.
pub fn rekey_workspace(
    layout: &StorageLayout,
    workspace: &WorkspaceName,
    mount_point: &Path,
) -> Result<RekeyReport, RekeyError> {
    let project = layout.project();
    let source = locate_source(layout, workspace)?;

    let format = ImageFormat::from_image_path(&source.image).map_err(|error| {
        RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: error.to_string(),
        }
    })?;
    if !project.contains(&source.image) {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: format!("image escapes the project: {}", source.image.display()),
        });
    }
    if !source.image.is_file() {
        return Err(RekeyError::NoIdentity {
            workspace: workspace.to_string(),
            detail: format!("image is gone: {}", source.image.display()),
        });
    }
    let canonical = layout.canonical_image(workspace, format).map_err(|error| {
        RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: error.to_string(),
        }
    })?;
    if canonical.image() != source.image {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: format!(
                "tombstone image {} is not the canonical image {}",
                source.image.display(),
                canonical.image().display()
            ),
        });
    }
    let companion = canonical.ca_private_key().to_owned();

    // A valid companion means there is no rotation to perform. An invalid one
    // is rotated over: minting rewrites it atomically.
    if is_valid_companion(&companion) {
        return Err(RekeyError::AlreadyKeyed {
            workspace: workspace.to_string(),
            companion,
        });
    }

    prove_mount(
        workspace,
        &source.base.workspace_incarnation,
        &source.base.repo_id,
        format,
        mount_point,
    )?;

    let revision = match source.origin {
        SourceOrigin::Quarantine => {
            source
                .base
                .grants
                .revision
                .checked_add(1)
                .ok_or(RekeyError::RevisionOverflow {
                    workspace: workspace.to_string(),
                    revision: source.base.grants.revision,
                })?
        }
        SourceOrigin::Live => source.base.grants.revision,
    };

    let snapshot =
        source
            .base
            .require_info_snapshot()
            .map_err(|_| RekeyError::MissingInfoSnapshot {
                workspace: workspace.to_string(),
            })?;
    let role = WorkspaceRole::for_name(workspace);
    if snapshot.role != role {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: format!(
                "info snapshot role {:?} does not match workspace {}",
                snapshot.role, workspace
            ),
        });
    }

    // Fence step one: the sidecar is durable before the companion exists. A
    // crash in the gap leaves a sidecar-only record the quarantine pass
    // recognizes, and the revision derives from the quarantined copy, so a
    // retry converges instead of drifting.
    let republished = DetachedWorkspaceMetadata {
        grants: crate::metadata::GrantSet {
            revision,
            ..source.base.grants.clone()
        },
        publication_state: PublicationState::Active,
        updated_at: utc_now_string(workspace)?,
        ..source.base.clone()
    };
    republished
        .write_for_image(&source.image)
        .map_err(|error| RekeyError::Io {
            workspace: workspace.to_string(),
            detail: format!(
                "publish sidecar {}: {error}",
                sidecar_path(&source.image).display()
            ),
        })?;

    // Fence step two: mint owns the companion's 0600 + fsync + parent-fsync
    // ordering and pairs the in-image certificate and token with the new key.
    let lifecycle = crate::storage::lifecycle::LifecycleWorkspace::new(
        republished.repo_id.clone(),
        republished.workspace.clone(),
        republished.workspace_incarnation.clone(),
        crate::storage::lifecycle::Revision::new(revision),
        crate::storage::lifecycle::Revision::new(revision),
        role,
        format,
    )
    .map_err(|error| RekeyError::IdentityMismatch {
        workspace: workspace.to_string(),
        detail: error.to_string(),
    })?;
    crate::workspace_credentials::mint_workspace_credentials(
        &lifecycle,
        mount_point,
        mount_point,
        republished.platform,
        republished.grants.port_block,
        &companion,
    )
    .map_err(|error| RekeyError::Mint {
        workspace: workspace.to_string(),
        detail: error.to_string(),
    })?;

    // The tombstone is consumed only once the rotation is durable.
    let tombstone_removed = match source.origin {
        SourceOrigin::Quarantine => {
            let entry = source.entry_dir.expect("quarantine source has an entry");
            fs::remove_dir_all(&entry).map_err(|error| RekeyError::Io {
                workspace: workspace.to_string(),
                detail: format!("remove quarantine entry {}: {error}", entry.display()),
            })?;
            crate::fsio::sync_directory(&project.quarantine).map_err(|error| RekeyError::Io {
                workspace: workspace.to_string(),
                detail: format!(
                    "sync quarantine root {}: {error}",
                    project.quarantine.display()
                ),
            })?;
            Some(entry)
        }
        SourceOrigin::Live => None,
    };

    verify_rekeyed(&source.image, &companion, &republished)?;

    Ok(RekeyReport {
        workspace: workspace.clone(),
        incarnation: republished.workspace_incarnation.clone(),
        revision,
        sidecar: sidecar_path(&source.image),
        companion,
        tombstone_removed,
    })
}

/// Where the rebuildable identity came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceOrigin {
    Quarantine,
    Live,
}

struct IdentitySource {
    origin: SourceOrigin,
    image: PathBuf,
    base: DetachedWorkspaceMetadata,
    entry_dir: Option<PathBuf>,
}

/// Find the rebuildable identity: the newest quarantine entry naming this
/// workspace, else the live canonical sidecar. Canonical paths stay the
/// single source of image truth; the tombstone only selects which sidecar
/// copy carries the identity.
fn locate_source(
    layout: &StorageLayout,
    workspace: &WorkspaceName,
) -> Result<IdentitySource, RekeyError> {
    if let Some(entry) = newest_quarantine_entry(layout.project(), workspace)? {
        return quarantine_source(workspace, &entry);
    }
    live_source(layout, workspace)
}

/// The newest `<ws>-*` quarantine entry whose `tombstone.json` parses and
/// names this workspace. Fixed-width unix timestamps sort lexicographically,
/// with the `-ordinal` collision suffix sorting after its base.
fn newest_quarantine_entry(
    project: &crate::repository::ProjectPaths,
    workspace: &WorkspaceName,
) -> Result<Option<PathBuf>, RekeyError> {
    let entries = match fs::read_dir(&project.quarantine) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(RekeyError::Io {
                workspace: workspace.to_string(),
                detail: format!("list quarantine {}: {error}", project.quarantine.display()),
            });
        }
    };
    let prefix = format!("{}-", workspace.as_str());
    let mut matching: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| RekeyError::Io {
            workspace: workspace.to_string(),
            detail: format!("list quarantine {}: {error}", project.quarantine.display()),
        })?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix) || !path.is_dir() {
            continue;
        }
        let tombstone = read_tombstone(&path.join("tombstone.json"));
        if tombstone.is_some_and(|record| record.workspace.as_ref() == Some(workspace)) {
            matching.push(path);
        }
    }
    matching.sort();
    Ok(matching.pop())
}

/// Quarantine tombstone as written by the recovery pass (v1 camelCase),
/// read tolerantly: unknown fields are ignored and the identity fields are
/// optional, so a future writer revision degrades to an identity error
/// rather than a parse failure.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TombstoneFile {
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    repo_id: Option<RepoId>,
    #[serde(default)]
    workspace: Option<WorkspaceName>,
    #[serde(default)]
    incarnation: Option<WorkspaceIncarnation>,
    #[serde(default)]
    revision: Option<u64>,
    #[serde(default)]
    image: Option<PathBuf>,
    #[serde(default)]
    sidecar: Option<String>,
}

fn read_tombstone(path: &Path) -> Option<TombstoneFile> {
    let record: TombstoneFile = read_json(path).ok()?;
    if record.version.is_some_and(|version| version != 1) {
        return None;
    }
    Some(record)
}

/// Identity from the quarantine entry: the tombstone selects, the
/// quarantined sidecar copy authorizes. Tombstone fields win only by
/// agreement — a disagreement is a refusal, not a merge.
fn quarantine_source(
    workspace: &WorkspaceName,
    entry: &Path,
) -> Result<IdentitySource, RekeyError> {
    let tombstone_path = entry.join("tombstone.json");
    let record = read_tombstone(&tombstone_path).ok_or_else(|| RekeyError::NoIdentity {
        workspace: workspace.to_string(),
        detail: format!("tombstone does not parse: {}", tombstone_path.display()),
    })?;
    let image = record.image.clone().ok_or_else(|| RekeyError::NoIdentity {
        workspace: workspace.to_string(),
        detail: format!("tombstone names no image: {}", tombstone_path.display()),
    })?;
    let quarantined_sidecar = match record.sidecar.clone() {
        Some(sidecar) if Path::new(&sidecar).is_absolute() => PathBuf::from(sidecar),
        _ => {
            let file_name = image.file_name().ok_or_else(|| RekeyError::NoIdentity {
                workspace: workspace.to_string(),
                detail: format!("tombstone image has no file name: {}", image.display()),
            })?;
            entry.join(format!("{}.grants.json", file_name.to_string_lossy()))
        }
    };
    let quarantined_image =
        image_from_sidecar_path(&quarantined_sidecar).ok_or_else(|| RekeyError::NoIdentity {
            workspace: workspace.to_string(),
            detail: format!(
                "quarantined sidecar is not a sidecar path: {}",
                quarantined_sidecar.display()
            ),
        })?;
    let base = DetachedWorkspaceMetadata::read_for_image(&quarantined_image).map_err(|error| {
        RekeyError::NoIdentity {
            workspace: workspace.to_string(),
            detail: format!(
                "quarantined sidecar {} does not read: {error}",
                quarantined_sidecar.display()
            ),
        }
    })?;
    if base.workspace != *workspace {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: format!("quarantined sidecar names {}", base.workspace.as_str()),
        });
    }
    if let Some(repo_id) = record.repo_id
        && base.repo_id != repo_id
    {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: "tombstone repo disagrees with the quarantined sidecar".to_owned(),
        });
    }
    if let Some(incarnation) = record.incarnation
        && base.workspace_incarnation != incarnation
    {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: "tombstone incarnation disagrees with the quarantined sidecar".to_owned(),
        });
    }
    if let Some(revision) = record.revision
        && base.grants.revision != revision
    {
        return Err(RekeyError::IdentityMismatch {
            workspace: workspace.to_string(),
            detail: "tombstone revision disagrees with the quarantined sidecar".to_owned(),
        });
    }
    Ok(IdentitySource {
        origin: SourceOrigin::Quarantine,
        image,
        base,
        entry_dir: Some(entry.to_owned()),
    })
}

/// Identity from the live canonical sidecar, for the companion-missing case
/// the recovery pass has not quarantined yet. Both image formats are
/// probed; two live images for one workspace is a refusal, not a guess.
fn live_source(
    layout: &StorageLayout,
    workspace: &WorkspaceName,
) -> Result<IdentitySource, RekeyError> {
    let mut found: Option<(PathBuf, DetachedWorkspaceMetadata)> = None;
    for format in [ImageFormat::Asif, ImageFormat::Sparse] {
        let paths = layout.canonical_image(workspace, format).map_err(|error| {
            RekeyError::IdentityMismatch {
                workspace: workspace.to_string(),
                detail: error.to_string(),
            }
        })?;
        if !paths.image().is_file() {
            continue;
        }
        let base = DetachedWorkspaceMetadata::read_for_image(paths.image()).map_err(|error| {
            RekeyError::NoIdentity {
                workspace: workspace.to_string(),
                detail: format!(
                    "live sidecar {} does not read: {error}",
                    sidecar_path(paths.image()).display()
                ),
            }
        })?;
        if found.is_some() {
            return Err(RekeyError::IdentityMismatch {
                workspace: workspace.to_string(),
                detail: "two live images claim this workspace".to_owned(),
            });
        }
        found = Some((paths.image().to_owned(), base));
    }
    let (image, base) = found.ok_or_else(|| RekeyError::NoIdentity {
        workspace: workspace.to_string(),
        detail: "no quarantine entry names this workspace and no live image exists".to_owned(),
    })?;
    Ok(IdentitySource {
        origin: SourceOrigin::Live,
        image,
        base,
        entry_dir: None,
    })
}

/// The mount must carry this workspace's marker: proof the mint below lands
/// inside the real volume rather than a directory the next attach shadows.
fn prove_mount(
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    repo: &RepoId,
    format: ImageFormat,
    mount_point: &Path,
) -> Result<(), RekeyError> {
    let marker_path = mount_point.join(WORKSPACE_MARKER_PATH);
    let marker =
        WorkspaceMarker::read_from(&marker_path).map_err(|error| RekeyError::NotMounted {
            workspace: workspace.to_string(),
            mount: mount_point.to_owned(),
            detail: format!("marker {} does not read: {error}", marker_path.display()),
        })?;
    if marker.repo_id != *repo
        || marker.workspace != *workspace
        || marker.workspace_incarnation != *incarnation
        || marker.image_format != format
    {
        return Err(RekeyError::NotMounted {
            workspace: workspace.to_string(),
            mount: mount_point.to_owned(),
            detail: format!(
                "marker at {} names {}/{}/{}",
                marker_path.display(),
                marker.repo_id.as_str(),
                marker.workspace.as_str(),
                marker.workspace_incarnation.as_str(),
            ),
        });
    }
    Ok(())
}

/// Present, regular file, mode `0600`, and parsing as a private key.
fn is_valid_companion(companion: &Path) -> bool {
    let metadata = match fs::symlink_metadata(companion) {
        Ok(metadata) => metadata,
        Err(_) => return false,
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o777 != 0o600 {
            return false;
        }
    }
    #[cfg(not(unix))]
    {
        if !metadata.file_type().is_file() {
            return false;
        }
    }
    crate::workspace_credentials::validate_private_key(companion).is_ok()
}

/// The post-rotation proof: the sidecar admits this exact incarnation at the
/// new revision, and the companion satisfies the published
/// [`MissingCaCompanion`](super::ApfsStorageError::MissingCaCompanion) gate.
fn verify_rekeyed(
    image: &Path,
    companion: &Path,
    expected: &DetachedWorkspaceMetadata,
) -> Result<(), RekeyError> {
    let metadata =
        DetachedWorkspaceMetadata::read_for_image(image).map_err(|error| RekeyError::Io {
            workspace: expected.workspace.to_string(),
            detail: format!("verify sidecar {}: {error}", sidecar_path(image).display()),
        })?;
    if metadata != *expected {
        return Err(RekeyError::Io {
            workspace: expected.workspace.to_string(),
            detail: format!(
                "republished sidecar {} disagrees with the rotation",
                sidecar_path(image).display()
            ),
        });
    }
    let gate = match fs::symlink_metadata(companion) {
        Ok(metadata) => metadata,
        Err(_) => {
            return Err(ApfsStorageError::MissingCaCompanion {
                layout: REKEY_LAYOUT,
                image: image.to_owned(),
                companion: companion.to_owned(),
            }
            .into());
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if !gate.file_type().is_file() || gate.permissions().mode() & 0o777 != 0o600 {
            return Err(ApfsStorageError::MissingCaCompanion {
                layout: REKEY_LAYOUT,
                image: image.to_owned(),
                companion: companion.to_owned(),
            }
            .into());
        }
    }
    crate::workspace_credentials::validate_private_key(companion).map_err(|error| {
        RekeyError::Mint {
            workspace: expected.workspace.to_string(),
            detail: format!("minted companion does not validate: {error}"),
        }
    })?;
    Ok(())
}

fn utc_now_string(workspace: &WorkspaceName) -> Result<String, RekeyError> {
    crate::runtime::supervisor::utc_now()
        .map(|timestamp| timestamp.as_str().to_owned())
        .map_err(|error| RekeyError::Io {
            workspace: workspace.to_string(),
            detail: error.to_string(),
        })
}
