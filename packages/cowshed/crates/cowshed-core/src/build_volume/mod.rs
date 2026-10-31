//! Build volumes (16_build_volumes.md): the build tools' own incremental state — Cargo target
//! directories, Nx's cache and task database, a per-tree indexer — on a volume of its own beside
//! a workspace's source image.
//!
//! A build volume is a RAW (`.dmg`) image at `<project>/build/<id>.dmg` with its record at
//! `<id>.dmg.json`, mounted `nobrowse` at `<host mount root>/.build/<owner>/<repo>/<id>`. A
//! checkout reaches it through exactly one symlink, [`BUILD_LINK`], whose target is that
//! mountpoint ([`link`]). The mountpoint names the volume, so the link is the whole record of
//! which checkout uses which volume: garbage collection reads links, never derives ownership
//! from the record.
//!
//! The record says what the link cannot: the tree the volume was last built at, whether it is a
//! seed (a target's frozen build state, never written), and when it was made.

pub mod cache_adapter;
pub mod cargo;
pub mod discard;
pub mod kept;
pub mod link;
#[cfg(target_os = "macos")]
pub mod migrate;
pub mod nx;
#[cfg(target_os = "macos")]
#[cfg(test)]
pub(crate) mod sqlite;

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::api::dto::GitOid;
use crate::capabilities::BuildStatePath;
use crate::metadata::{MetadataError, WorkspaceIncarnation, WorkspaceName, read_json, write_json};
use crate::repository::ProjectPaths;
use crate::storage::StorageLayoutError;
use crate::storage::apfs::release::{Listing, released_in_store};
use cowshed_apfs::image::IMAGE_EXTENSION;
use cowshed_os::fork_lock::Fenced;

pub use link::BUILD_LINK;

/// The project's build-volume images, beside `sessions/`.
pub(crate) const IMAGES_DIRECTORY: &str = "build";
/// Every project's build-volume mountpoints, beneath the host mount root and outside every
/// workspace mount: `<host mount root>/.build/<owner>/<repo>/<id>`.
const MOUNTS_DIRECTORY: &str = ".build";
/// The record beside each image: `<id>.dmg.json`.
pub(crate) const RECORD_SUFFIX: &str = ".json";
/// The lock beside each image that every job admitted on the volume holds shared for its whole
/// run, and that a release takes exclusively: `<id>.dmg.hold` ([`BuildVolumeHold`]).
pub(crate) const HOLD_SUFFIX: &str = ".hold";
/// Where a seed compaction works ([`BuildVolumeLayout::compacting`]).
const COMPACTING_DIRECTORY: &str = ".compacting";
/// Where the background collection a land starts logs ([`BuildVolumeLayout::collecting`]).
const COLLECTING_DIRECTORY: &str = ".collecting";
/// The volume's own statement of the build-state paths it holds, at its root. It travels with
/// every clone, so a fork knows what its source linked without rediscovering it.
pub const STATE_FILE: &str = "cowshed-build-state.json";
pub(crate) const RECORD_VERSION: u32 = 1;
/// The capacity of a build volume created from nothing, unless `.cowshed.toml` `[build] capacity`
/// says otherwise (16_build_volumes.md, "Substrate"). Sparse: a cap on build-cache growth, not an
/// allocation.
pub const DEFAULT_BUILD_VOLUME_CAPACITY: cowshed_apfs::image::ImageCapacity =
    cowshed_apfs::image::ImageCapacity::from_gibibytes(100);

/// A build volume's identity: 32 lowercase hex digits, minted once and never reused.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BuildVolumeId(String);

impl BuildVolumeId {
    pub fn mint() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    /// The id `value` spells, or `None` for anything else: a name in a build directory that is
    /// not exactly an id is never a build volume.
    pub fn parse(value: &str) -> Option<Self> {
        (value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then(|| Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BuildVolumeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for BuildVolumeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BuildVolumeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid build volume id {value:?}")))
    }
}

/// What a build volume is to the project. Every role names the workspace that owns the volume,
/// so no record ever describes an image nobody answers for.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum BuildVolumeRole {
    /// The live volume of `checkout`, which links it.
    Linked { checkout: WorkspaceName },
    /// The live volume `owner` at `incarnation` linked until an adoption or a reseed moved its
    /// link onto a replacement. It stays `owner`'s: a job admitted on it before the move holds
    /// it, and it is retired only by a release that deleted it (16_build_volumes.md, "Garbage
    /// collection"). A release that is refused or fails leaves this record as it is.
    Superseded {
        owner: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    },
    /// The latest seed of the target `target` at `incarnation`: frozen, never mounted, never
    /// written; only cloned.
    Seed {
        target: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    },
}

impl BuildVolumeRole {
    /// The workspace that owns the volume: the checkout that links it, the workspace whose link
    /// moved off it, or the target whose seed it is.
    pub fn owner(&self) -> &WorkspaceName {
        match self {
            Self::Linked { checkout } => checkout,
            Self::Superseded { owner, .. } => owner,
            Self::Seed { target, .. } => target,
        }
    }
}

/// Who answers for a build volume registered before its image held what its role promises
/// ([`BuildVolumeStage::Registered`]): the authority that finishes the volume or releases it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Registrant {
    /// The existing workspace the role names, at `incarnation`: its first touch, a reseed of
    /// it, its refork, or a capture of its seed. Another incarnation of the same name is another
    /// workspace and never finishes this volume.
    Workspace { incarnation: WorkspaceIncarnation },
    /// The unfinished create, fork, adopt or re-adopt the lifecycle intent journal records for
    /// the workspace the role names, which has no incarnation until that verb publishes it. Its
    /// authority is exact, never the workspace's name, which a later verb may reuse: the live
    /// creator's hold on this id, which keeps every release off it until the creator publishes
    /// it or dies, or, for a re-adoption, this id in its own journaled record. Without either it
    /// is an interrupted creation, released by its id.
    Intent,
}

/// Whether a build volume's image holds what its record's role promises.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum BuildVolumeStage {
    /// Registered to its owner before any byte of its image existed, and not yet finished: the
    /// image may be absent, partial, unmarked or not yet linked. Never a seed anyone forks from.
    /// Only `by` finishes it; once `by` is gone it is an interrupted creation, which collection
    /// releases by its exact id (16_build_volumes.md, "Garbage collection").
    Registered { by: Registrant },
    /// The image holds what the role promises.
    #[default]
    Published,
}

impl BuildVolumeStage {
    pub fn is_published(&self) -> bool {
        matches!(self, Self::Published)
    }
}

/// The record beside a build volume's image. It is written before the image exists
/// ([`BuildVolumeLayout::register`]) and removed only once the image is gone, so every build
/// image in the images directory has a record naming its owner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildVolumeRecord {
    pub version: u32,
    /// The git tree (`HEAD^{tree}`) the volume was last built at, when cowshed knows it: a seed
    /// is frozen at the landed tree, a fork inherits its seed's.
    pub tree: Option<GitOid>,
    pub role: BuildVolumeRole,
    /// RFC 3339 UTC.
    pub created_at: String,
    /// The tree the linking checkout had checked out when cowshed placed this volume there: a
    /// fork's tree at its fork, the landed tree at an adoption. `tree` says what was built into
    /// the volume; this says where it was put. `None` on a record that predates it, or for a
    /// placement at no tree cowshed knows (16_build_volumes.md, "Which volume a seed holds").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_out: Option<GitOid>,
    #[serde(default, skip_serializing_if = "BuildVolumeStage::is_published")]
    pub stage: BuildVolumeStage,
}

impl BuildVolumeRecord {
    pub fn new(tree: Option<GitOid>, role: BuildVolumeRole) -> Self {
        Self {
            version: RECORD_VERSION,
            tree,
            role,
            created_at: crate::storage::deletion_log::rfc3339_utc(std::time::SystemTime::now()),
            checked_out: None,
            stage: BuildVolumeStage::Published,
        }
    }

    pub fn is_published(&self) -> bool {
        self.stage.is_published()
    }

    /// A published seed of `target` at `incarnation`: a registered one may still be forming.
    pub fn is_seed_of(&self, target: &WorkspaceName, incarnation: &WorkspaceIncarnation) -> bool {
        self.is_published()
            && matches!(&self.role, BuildVolumeRole::Seed { target: seeded, incarnation: at }
            if seeded == target && at == incarnation)
    }
}

/// A build volume's owner record before its image exists: what every operation that writes a
/// build image takes, so no image-writing API can make bytes nobody owns. The registering
/// operation writes [`Self::registered`] ([`BuildVolumeLayout::register`]), makes the image, and
/// writes [`Self::published`] once the image holds what the role promises
/// ([`BuildVolumeLayout::publish`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Registration {
    registered: BuildVolumeRecord,
}

impl Registration {
    /// `record`, registered by `by`; its stage is `by`'s, whatever `record` said.
    pub fn new(record: BuildVolumeRecord, by: Registrant) -> Self {
        Self {
            registered: BuildVolumeRecord {
                stage: BuildVolumeStage::Registered { by },
                ..record
            },
        }
    }

    pub fn role(&self) -> &BuildVolumeRole {
        &self.registered.role
    }

    /// The record written before the image exists.
    pub fn registered(&self) -> &BuildVolumeRecord {
        &self.registered
    }

    /// The record written once the image holds what the role promises.
    pub fn published(&self) -> BuildVolumeRecord {
        BuildVolumeRecord {
            stage: BuildVolumeStage::Published,
            ..self.registered.clone()
        }
    }
}

/// A clone whose bytes are captured and whose owner record is registered, but which is not yet
/// proven forkable or published: what a fork takes under its source's image lock,
/// so that lock covers the capture alone (16_build_volumes.md, "Fork" step 3). The creation's
/// hold keeps collection off it until its host finishes and publishes it, or releases it once a
/// failure abandons it. Dropped unfinished, it stays registered for collection, as a crash
/// leaves it.
#[derive(Debug)]
pub struct CapturedBuildVolume {
    pub id: BuildVolumeId,
    pub registration: Registration,
    /// The last write the captured bytes hold: the clone's own, before it is written.
    pub written: std::time::SystemTime,
    /// Whose volume the bytes came from: a refused extent proof names its defragmentation.
    pub source_owner: WorkspaceName,
    /// Necessary native capture work, excluding registration, metadata and extent proofs.
    /// A directory test substrate performs no native image operation.
    pub capture_elapsed: Option<std::time::Duration>,
    pub creating: BuildVolumeHold,
}

/// The build-state paths a volume holds, as written at its root ([`STATE_FILE`]), the
/// fingerprint of the tracked build inputs they were discovered from
/// (`capabilities::tracked_manifest_fingerprint`): while it matches, nothing is rediscovered, and
/// the realpath of mbx reached through the canonical Cargo route, recording target ownership
/// rather than enabling the capability (15_capabilities.md, "mbx detection").
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildVolumeState {
    pub paths: Vec<BuildStatePath>,
    pub fingerprint: Option<String>,
    pub mbx: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateWire {
    version: u32,
    paths: Vec<PathWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mbx: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PathWire {
    checkout: String,
    volume: String,
    #[serde(default)]
    retention: crate::capabilities::SeedRetention,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    clone_invalid: Vec<String>,
    /// `[build] keep` names this declared path: written only when it does.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    keep: bool,
}

impl BuildVolumeState {
    /// The state written at `volume_root`, or the empty state for a volume never linked.
    pub fn read(volume_root: &Path) -> crate::Result<Self> {
        Ok(Self::read_optional(volume_root)?.unwrap_or_default())
    }

    /// Read the recorded state without treating an uninitialized volume as an empty path set.
    pub fn read_optional(volume_root: &Path) -> crate::Result<Option<Self>> {
        let path = volume_root.join(STATE_FILE);
        let wire = match read_json::<StateWire>(&path) {
            Ok(wire) => wire,
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(record_error(&path, &error)),
        };
        if wire.version != RECORD_VERSION {
            return Err(unknown_version(&path, wire.version));
        }
        let paths = wire
            .paths
            .iter()
            .map(path_from_wire)
            .collect::<crate::Result<Vec<_>>>()?;
        disjoint(&paths).map_err(|(left, right)| overlapping(&path, left, right))?;
        Ok(Some(Self {
            paths,
            fingerprint: wire.fingerprint,
            mbx: wire.mbx,
        }))
    }

    pub fn write(&self, volume_root: &Path) -> crate::Result<()> {
        let path = volume_root.join(STATE_FILE);
        let wire = StateWire {
            version: RECORD_VERSION,
            paths: self
                .paths
                .iter()
                .map(|state| PathWire {
                    checkout: state.checkout.as_path().to_string_lossy().into_owned(),
                    volume: state.volume.as_path().to_string_lossy().into_owned(),
                    retention: state.retention,
                    clone_invalid: state
                        .clone_invalid
                        .iter()
                        .map(|path| path.as_path().to_string_lossy().into_owned())
                        .collect(),
                    keep: state.keep,
                })
                .collect(),
            fingerprint: self.fingerprint.clone(),
            mbx: self.mbx.clone(),
        };
        write_json(&path, &wire).map_err(|error| record_error(&path, &error))
    }

    /// This state with each `discovered` path it does not hold yet added and each `released` path
    /// dropped, recorded at `fingerprint` with the `mbx` discovery resolved, and the paths added
    /// and dropped. A path already held keeps its volume name: links are fixed once made
    /// (16_build_volumes.md, "One link per checkout"); only a discovery that found the path's owner
    /// gone off the volume releases it (a Cargo target under mbx). A new path that overlaps a held
    /// one is refused, naming both. A held path takes each discovered path's seed policy and
    /// `[build] keep` from its contributor.
    /// Complete canonical discovery resolves an unowned legacy path to rebuildable state,
    /// without moving its fixed volume name or changing already-known retained ownership.
    pub fn with_discovered(
        &self,
        discovered: &[BuildStatePath],
        released: &[BuildStatePath],
        mbx: Option<PathBuf>,
        fingerprint: String,
        volume_root: &Path,
    ) -> crate::Result<(Self, Vec<BuildStatePath>, Vec<BuildStatePath>)> {
        let is_released = |held: &BuildStatePath| {
            released
                .iter()
                .any(|path| path.checkout.as_path() == held.checkout.as_path())
        };
        let dropped: Vec<BuildStatePath> = self
            .paths
            .iter()
            .filter(|held| is_released(held))
            .cloned()
            .collect();
        let mut paths: Vec<BuildStatePath> = self
            .paths
            .iter()
            .filter(|held| !is_released(held))
            .cloned()
            .collect();
        let added: Vec<BuildStatePath> = discovered
            .iter()
            .filter(|path| {
                !paths
                    .iter()
                    .any(|held| held.checkout.as_path() == path.checkout.as_path())
            })
            .cloned()
            .collect();
        for held in &mut paths {
            if let Some(current) = discovered
                .iter()
                .find(|path| path.checkout == held.checkout)
            {
                held.retention = current.retention;
                held.clone_invalid.clone_from(&current.clone_invalid);
                held.keep = current.keep;
            } else if held.retention == crate::capabilities::SeedRetention::Unrecorded {
                held.retention = crate::capabilities::SeedRetention::Rebuild;
                held.clone_invalid.clear();
            }
        }
        paths.extend(added.iter().cloned());
        disjoint(&paths)
            .map_err(|(left, right)| overlapping(&volume_root.join(STATE_FILE), left, right))?;
        Ok((
            Self {
                paths,
                fingerprint: Some(fingerprint),
                mbx,
            },
            added,
            dropped,
        ))
    }

    /// Resolve legacy seed metadata from current capability contributions, without changing paths or live bytes.
    pub fn with_seed_retention(&self, policies: &[BuildStatePath]) -> Self {
        let mut state = self.clone();
        for held in &mut state.paths {
            if held.retention == crate::capabilities::SeedRetention::Unrecorded {
                match policies
                    .iter()
                    .find(|policy| policy.checkout == held.checkout)
                {
                    Some(policy) => {
                        held.retention = policy.retention;
                        held.clone_invalid.clone_from(&policy.clone_invalid);
                    }
                    None => {
                        held.retention = crate::capabilities::SeedRetention::Rebuild;
                        held.clone_invalid.clear();
                    }
                }
            }
        }
        state
    }

    /// Drop only capability-owned process files from a newly minted volume, never its source.
    pub fn discard_clone_invalid(&self, volume_root: &Path) -> crate::Result<()> {
        if !self.paths.iter().any(|path| !path.clone_invalid.is_empty()) {
            return Ok(());
        }
        let volume = fs::canonicalize(volume_root)
            .map_err(|error| io_error("resolve new clone volume", volume_root, &error))?;
        for path in &self.paths {
            if path.clone_invalid.is_empty() {
                continue;
            }
            let root = volume_root.join(path.volume.as_path());
            let canonical = match fs::canonicalize(&root) {
                Ok(root) => root,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(io_error("resolve clone-invalid owner", &root, &error)),
            };
            if !canonical.starts_with(&volume) {
                return Err(crate::CowshedError::integrity(
                    format!(
                        "clone-invalid owner {} escapes new volume {}",
                        canonical.display(),
                        volume.display()
                    ),
                    "repair the capability's clone-invalid owner path",
                ));
            }
            for invalid in &path.clone_invalid {
                let at = root.join(invalid.as_path());
                let parent = at
                    .parent()
                    .expect("a validated relative clone path has a parent");
                let resolved = match fs::canonicalize(parent) {
                    Ok(parent) => parent,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        return Err(io_error("resolve clone-invalid parent", parent, &error));
                    }
                };
                if !resolved.starts_with(&canonical) {
                    return Err(crate::CowshedError::integrity(
                        format!(
                            "clone-invalid path {} escapes its owner {}",
                            at.display(),
                            canonical.display()
                        ),
                        "repair the capability's clone-invalid path",
                    ));
                }
                match fs::remove_file(&at) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(io_error("discard clone-invalid process file", &at, &error));
                    }
                }
            }
        }
        Ok(())
    }

    /// Empty what an mbx fork's clone of its target's live volume must not inherit: every path
    /// not retained, captured while the target's jobs may have been writing it
    /// (16_build_volumes.md, "Fork"). A rebuildable directory is left empty for its link; one
    /// declared but not retained is left absent. A non-mbx volume is a seed's clone, captured
    /// under its writers' exclusion, and keeps everything.
    pub fn discard_rebuildable(&self, volume_root: &Path) -> crate::Result<()> {
        if self.mbx.is_none() {
            return Ok(());
        }
        for path in self.paths.iter().filter(|path| !path.retained()) {
            let directory = volume_root.join(path.volume.as_path());
            match fs::symlink_metadata(&directory) {
                Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&directory),
                Ok(_) => fs::remove_file(&directory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
            .map_err(|error| io_error("discard a fork's rebuildable state", &directory, &error))?;
            if !path.is_declared() {
                fs::create_dir_all(&directory).map_err(|error| {
                    io_error(
                        "recreate a fork's rebuildable directory",
                        &directory,
                        &error,
                    )
                })?;
            }
        }
        Ok(())
    }

    /// Every Nx `workspace-data` directory the volume holds, relative to its root: where Nx keeps
    /// its task database and its daemon's record (`d/`).
    pub fn nx_workspace_data(&self) -> impl Iterator<Item = &Path> {
        self.nx_named("workspace-data")
    }

    /// Every Nx cache directory the volume holds, relative to its root.
    pub fn nx_cache(&self) -> impl Iterator<Item = &Path> {
        self.nx_named("cache")
    }

    /// Every Nx directory `.nx/<name>` the volume holds, relative to its root.
    fn nx_named(&self, name: &'static str) -> impl Iterator<Item = &Path> {
        self.nx_states(name).map(|state| state.volume.as_path())
    }

    /// The Nx states `.nx/<name>` the volume holds, by the one predicate every Nx view shares.
    fn nx_states(&self, name: &'static str) -> impl Iterator<Item = &BuildStatePath> {
        self.paths.iter().filter(move |state| {
            BuildStateTool::of(state) == BuildStateTool::Nx
                && state.checkout.as_path().file_name() == Some(std::ffi::OsStr::new(name))
        })
    }

    /// Every checkout-relative project root that holds an Nx `.nx/workspace-data` in the volume: the
    /// directory above the project's `.nx`, where Nx reads its `nx.json`. The empty path is the
    /// checkout root itself.
    pub fn nx_project_roots(&self) -> impl Iterator<Item = &Path> {
        self.nx_states("workspace-data").map(|state| {
            state
                .checkout
                .as_path()
                .parent()
                .and_then(Path::parent)
                .expect("BuildStateTool::of classifies only <project>/.nx/<name> as Nx, so the project root exists")
        })
    }

    /// Every Cargo target directory the volume holds, relative to its root.
    pub fn cargo_targets(&self) -> impl Iterator<Item = &Path> {
        self.paths
            .iter()
            .filter(|state| BuildStateTool::of(state) == BuildStateTool::Cargo)
            .map(|state| state.volume.as_path())
    }
}

fn path_from_wire(wire: &PathWire) -> crate::Result<BuildStatePath> {
    let mut path = BuildStatePath::new(&wire.checkout, &wire.volume)?;
    path.retention = wire.retention;
    path.clone_invalid = wire
        .clone_invalid
        .iter()
        .map(|value| crate::capabilities::RelPath::new(value))
        .collect::<crate::Result<_>>()?;
    if wire.keep && !path.is_declared() {
        return Err(crate::CowshedError::integrity(
            format!(
                "build-state path {} records [build] keep, which only declared build state has",
                wire.checkout
            ),
            "cowshed doctor --json",
        ));
    }
    path.keep = wire.keep;
    Ok(path)
}

/// The first two paths that overlap on either side, or `()` when none do: one build-state path
/// inside another would link a tool's state through another tool's.
fn disjoint(paths: &[BuildStatePath]) -> Result<(), (&BuildStatePath, &BuildStatePath)> {
    let overlap = |a: &Path, b: &Path| a.starts_with(b) || b.starts_with(a);
    for (index, left) in paths.iter().enumerate() {
        for right in &paths[index + 1..] {
            if overlap(left.checkout.as_path(), right.checkout.as_path())
                || overlap(left.volume.as_path(), right.volume.as_path())
            {
                return Err((left, right));
            }
        }
    }
    Ok(())
}

fn overlapping(path: &Path, left: &BuildStatePath, right: &BuildStatePath) -> crate::CowshedError {
    crate::CowshedError::integrity(
        format!(
            "{} names overlapping build-state paths {} and {}",
            path.display(),
            left.checkout.as_path().display(),
            right.checkout.as_path().display()
        ),
        "cowshed doctor --json",
    )
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BuildStateTool {
    Cargo,
    Nx,
    /// `.cowshed.toml` `[build] state` that `[build] keep` names: declared state that holds work,
    /// such as a patch worktree, which a refresh moves aside intact rather than discard.
    Declared,
    /// `.cowshed.toml` `[build] state` that `[build] keep` does not name: a cache of a tool no
    /// capability detects, which a refresh discards and links like a capability's.
    DeclaredCache,
    /// A JavaScript cache: an installed package's `node_modules/.cache`, or the module compile
    /// cache Node and Bun keep at `NODE_COMPILE_CACHE`.
    #[serde(rename = "javascript")]
    JavaScript,
}

impl BuildStateTool {
    /// The tool whose state `path` is: declared state by its own volume namespace, kept or a
    /// cache by its `[build] keep`, Nx's directories under `.nx`, a JavaScript cache (an
    /// installed package's `node_modules/.cache` or the module compile cache), and otherwise a
    /// Cargo target directory.
    pub fn of(path: &BuildStatePath) -> Self {
        let checkout = path.checkout.as_path();
        let parent_named = |name: &str| {
            checkout
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|parent| parent == name)
        };
        if path.is_declared() {
            if path.keep {
                Self::Declared
            } else {
                Self::DeclaredCache
            }
        } else if parent_named(".nx") {
            Self::Nx
        } else if checkout == Path::new(crate::workspace_environment::NODE_COMPILE_CACHE_CHECKOUT)
            || (checkout.file_name().is_some_and(|name| name == ".cache")
                && parent_named("node_modules"))
        {
            Self::JavaScript
        } else {
            Self::Cargo
        }
    }
}

/// Why a real directory stood where a build-state link belongs, which decides what its discard or
/// its keeping reports (16_build_volumes.md, "One link per checkout", Refresh).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DiscardOrigin {
    /// The volume never linked the path: the checkout's first touch, a path discovery contributes
    /// for the first time, or a declared path's first appearance. The directory is the checkout's
    /// own state from before the link, and nothing displaced it.
    Predated,
    /// The volume held the path and its directory, so the link existed: a tool removed it and
    /// wrote there since, off the volume.
    Displaced,
}

/// A real directory holding entries at a kept declared build-state path (`[build] keep`), which a
/// refresh moved aside intact, never copied and never deleted, and linked over
/// (16_build_volumes.md, "What a refresh does at each path"). No refresh and no `gc` deletes it,
/// and `rm` and `land` refuse its workspace while it is there, so it stays until the operator
/// deletes it or abandons the workspace; `cowshed doctor` reports it until then ([`kept::list`]).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeptBuildStateFinding {
    /// Checkout-relative: where the directory stood.
    pub path: PathBuf,
    /// Absolute: where it is now, in the checkout's [`kept::KEPT_DIRECTORY`].
    pub kept: PathBuf,
    pub origin: DiscardOrigin,
}

impl std::fmt::Display for KeptBuildStateFinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (path, kept) = (self.path.display(), self.kept.display());
        let stood = match self.origin {
            DiscardOrigin::Predated => "which predates its link",
            DiscardOrigin::Displaced => "which had displaced its volume link",
        };
        write!(
            formatter,
            "moved the real directory {path}, {stood}, aside intact to {kept} and linked {path} \
             onto the build volume; no refresh or gc deletes it, and rm and land refuse this \
             workspace until it is gone: take what you need from it, re-run whatever made {path} \
             so it fills the link, then delete it ({})",
            kept::removal(&self.kept)
        )
    }
}

/// A real directory a refresh discarded, never copied, where a build-state link belongs, and
/// linked over: a tool's own directory or a declared cache, which its next run rebuilds, or an
/// empty kept declared one. A kept declared directory holding anything is moved aside instead
/// ([`KeptBuildStateFinding`]).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscardedBuildStateFinding {
    pub path: PathBuf,
    /// The tool whose state the path is.
    pub tool: BuildStateTool,
    pub origin: DiscardOrigin,
}

impl std::fmt::Display for DiscardedBuildStateFinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = self.path.display();
        let rebuild = match self.tool {
            BuildStateTool::Cargo | BuildStateTool::Nx | BuildStateTool::JavaScript => {
                "the next build rebuilds what is missing"
            }
            BuildStateTool::DeclaredCache => {
                "[build] keep does not name it, so it is a cache its tool's next run rebuilds"
            }
            BuildStateTool::Declared => {
                "it was empty, so nothing was lost; what fills it now writes onto the volume, and \
                 every fork inherits it warm"
            }
        };
        match self.origin {
            DiscardOrigin::Predated => write!(
                formatter,
                "discarded the real directory {path}, which predates its link, and linked it onto \
                 the build volume; {rebuild}"
            ),
            DiscardOrigin::Displaced => {
                let tool = match self.tool {
                    BuildStateTool::Cargo => "cargo clean or another Cargo command",
                    BuildStateTool::Nx => "an Nx reset or cache command",
                    BuildStateTool::JavaScript => {
                        "a cleanup or reinstall that removed its link before a Node or Bun process \
                         wrote there"
                    }
                    BuildStateTool::Declared => "whatever reconstructs the declared state",
                    BuildStateTool::DeclaredCache => {
                        "a git clean or rm -rf that removed its link before its tool wrote there"
                    }
                };
                write!(
                    formatter,
                    "discarded the real build-state directory {path} and restored its volume link; \
                     {tool} may have displaced it; {rebuild}"
                )
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackedBuildStateRefusal {
    pub path: PathBuf,
    pub tracked_files: Vec<PathBuf>,
}

impl std::fmt::Display for TrackedBuildStateRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "refused to discard build-state path {} because it contains tracked source:",
            self.path.display()
        )?;
        for path in &self.tracked_files {
            write!(formatter, " {}", path.display())?;
        }
        Ok(())
    }
}

/// The first of `paths` that holds tracked source, with every tracked file under it, or `None`.
/// One `git ls-files` query answers them all. Build state is discarded, never copied, so a path
/// holding a tracked file is never build state: migration refuses it before anything is
/// deleted, and discovery refuses to declare it.
pub(crate) fn tracked_source(
    checkout: &Path,
    paths: &[&BuildStatePath],
) -> crate::Result<Option<TrackedBuildStateRefusal>> {
    use cowshed_os::fork_lock::Run as _;
    use std::os::unix::ffi::OsStrExt as _;
    if paths.is_empty() {
        return Ok(None);
    }
    // WHY unbounded: a git of this host's own repository with hooks, fsmonitor, pager, editors and
    // prompts off (`git_command_at`): it waits on no other process.
    #[allow(clippy::disallowed_methods)]
    let output = crate::git::git_command_at(checkout)
        .arg("--literal-pathspecs")
        .args(["ls-files", "-z", "--"])
        .args(paths.iter().map(|state| state.checkout.as_path()))
        .output_locked()
        .map_err(|error| crate::git::git_spawn_error(&error))?;
    if !output.status.success() {
        return Err(crate::CowshedError::environment_missing(
            format!(
                "cannot prove build-state paths contain no tracked source: {}",
                crate::git::git_ended(&output)
            ),
            "repair the checkout's Git index and retry; nothing has been deleted",
        ));
    }
    for state in paths {
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

/// What refreshing a checkout's build state did (16_build_volumes.md, "One link per checkout"):
/// the volume it links afterwards, whether that volume was created now (the checkout's first
/// touch), the build-state paths linked for the first time, the real directories it discarded
/// and the declared ones it moved aside and kept where a link belongs, each with its origin, and
/// what discovery could not decide.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildStateRefresh {
    pub volume: Option<BuildVolumeId>,
    pub created: bool,
    /// Checkout-relative.
    pub added: Vec<PathBuf>,
    pub discarded: Vec<DiscardedBuildStateFinding>,
    pub kept: Vec<KeptBuildStateFinding>,
    pub findings: Vec<crate::capabilities::BuildStateFinding>,
}

/// What a checkout's build link should become before its volume is mounted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkResolution {
    /// The link names the checkout's own volume.
    Keep,
    /// The link is stale; it names this volume, the one recorded as the checkout's.
    Repoint(BuildVolumeId),
    /// The link names a volume a target adopted, and the checkout owns none: a `land
    /// --no-retire` that stopped between the adoption and the landing workspace's refork. The
    /// checkout takes a fresh clone of this seed, the adopting target's latest, exactly as the
    /// refork would have given it.
    Refork(BuildVolumeId),
    /// The link names a volume whose image no longer exists, and the checkout owns none: it
    /// was collected or deleted out from under the checkout. Mounting gives the checkout a fresh
    /// clone of its own latest seed, or no link at all when it has none, so its next build-state
    /// refresh makes its first volume; what the lost volume held beyond that seed is gone.
    Vanished,
}

/// A name in the images directory that belongs to one build volume: its image or its record.
/// The hold beside them is a zero-length lock, not part of what a volume is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VolumeFile {
    /// `<id>.dmg`.
    Image(BuildVolumeId),
    /// `<id>.dmg.json`.
    Record(BuildVolumeId),
}

impl VolumeFile {
    pub(crate) fn of(name: &std::ffi::OsStr) -> Option<Self> {
        let name = name.to_str()?;
        let (image, file): (&str, fn(BuildVolumeId) -> Self) =
            match name.strip_suffix(RECORD_SUFFIX) {
                Some(image) => (image, Self::Record),
                None => (name, Self::Image),
            };
        image
            .strip_suffix(IMAGE_EXTENSION)?
            .strip_suffix('.')
            .and_then(BuildVolumeId::parse)
            .map(file)
    }
}

/// One read-only seed-record observation, indexed by target and incarnation. Record failures
/// stay in enumeration order: earlier matching claim failures still take precedence over them.
#[derive(Debug, Default)]
pub(crate) struct SeedRecords {
    records: Vec<(BuildVolumeId, BuildVolumeRecord)>,
    targets: BTreeMap<WorkspaceName, BTreeMap<WorkspaceIncarnation, Vec<usize>>>,
    error: Option<crate::CowshedError>,
}

impl SeedRecords {
    fn observe(
        records: impl IntoIterator<Item = crate::Result<(BuildVolumeId, Option<BuildVolumeRecord>)>>,
    ) -> Self {
        let mut observed = Self::default();
        for read in records {
            let (id, record) = match read {
                Ok(read) => read,
                Err(error) => {
                    observed.error = Some(error);
                    break;
                }
            };
            let Some(record) = record else { continue };
            if let BuildVolumeRole::Seed {
                target,
                incarnation,
            } = &record.role
                && record.is_published()
            {
                let incarnations = if observed.targets.contains_key(target) {
                    observed.targets.get_mut(target).expect("observed target")
                } else {
                    observed.targets.entry(target.clone()).or_default()
                };
                let seeds = if incarnations.contains_key(incarnation) {
                    incarnations
                        .get_mut(incarnation)
                        .expect("observed incarnation")
                } else {
                    incarnations.entry(incarnation.clone()).or_default()
                };
                seeds.push(observed.records.len());
                observed.records.push((id, record));
            }
        }
        observed
    }

    fn latest(
        &self,
        target: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
        mut claimed: impl FnMut(&BuildVolumeId) -> crate::Result<bool>,
    ) -> crate::Result<Option<usize>> {
        let mut latest: Option<(usize, bool)> = None;
        if let Some(seeds) = self.targets.get(target).and_then(|at| at.get(incarnation)) {
            for &index in seeds {
                let (id, record) = &self.records[index];
                let unclaimed = !claimed(id)?;
                if latest.is_none_or(|(previous, free)| {
                    (&record.created_at, unclaimed) >= (&self.records[previous].1.created_at, free)
                }) {
                    latest = Some((index, unclaimed));
                }
            }
        }
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(latest
            .filter(|(_, unclaimed)| *unclaimed)
            .map(|(index, _)| index))
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn seed_of(
        &self,
        layout: &BuildVolumeLayout,
        target: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> crate::Result<Option<&(BuildVolumeId, BuildVolumeRecord)>> {
        Ok(self
            .latest(target, incarnation, |id| layout.seed_claimed(id))?
            .map(|index| &self.records[index]))
    }
}

#[cfg(test)]
thread_local! {
    static SEED_IO_COUNTS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

/// Actual build-directory listings and record reads on this test thread, not estimated syscalls.
#[cfg(test)]
pub(crate) fn seed_io_counts() -> (usize, usize) {
    SEED_IO_COUNTS.with(std::cell::Cell::get)
}
/// Where one project's build volumes live.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildVolumeLayout {
    project: PathBuf,
    images: PathBuf,
    mounts: PathBuf,
}

impl BuildVolumeLayout {
    pub fn new(project: &ProjectPaths) -> Result<Self, StorageLayoutError> {
        // The project's mount subtree is `<host mount root>/<owner>/<repo>`, already encoded and
        // checked; its build mounts take the same `<owner>/<repo>` under `.build`.
        let encoded = project
            .mount_root
            .strip_prefix(&project.host_mount_root)
            .map_err(|_| StorageLayoutError::EscapesStoreRoot)?;
        Ok(Self {
            project: project.project_root.clone(),
            images: project.project_root.join(IMAGES_DIRECTORY),
            mounts: project.host_mount_root.join(MOUNTS_DIRECTORY).join(encoded),
        })
    }

    /// The project's store directory, whose deletion log journals every build volume release.
    pub fn project_root(&self) -> &Path {
        &self.project
    }

    /// Journal a release of volume `id` in the project's deletion log (16_build_volumes.md,
    /// "Garbage collection"), naming the workspace its sidecar `recorded` as its owner (the
    /// checkout that linked it, the workspace whose link moved off it, the target whose seed it
    /// was), or nobody when no sidecar was there. A release that deleted the image writes
    /// `release-build-volume` for the image; one that found the image already gone and deleted
    /// the record writes `release-imageless-build-volume` for the record, naming the missing
    /// image.
    pub(crate) fn journal_release(
        &self,
        id: &BuildVolumeId,
        recorded: Option<&BuildVolumeRole>,
        image_deleted: bool,
    ) {
        use crate::storage::deletion_log::{DeletionKind, DeletionOp, log_deletion};
        let (kind, workspace) = match recorded {
            Some(BuildVolumeRole::Seed { target, .. }) => {
                (DeletionKind::BuildSeed, target.as_str())
            }
            Some(BuildVolumeRole::Linked { checkout: owner })
            | Some(BuildVolumeRole::Superseded { owner, .. }) => {
                (DeletionKind::BuildVolume, owner.as_str())
            }
            None => (DeletionKind::BuildVolume, ""),
        };
        let image = self.image(id);
        let (op, artifact) = if image_deleted {
            (DeletionOp::ReleaseBuildVolume, image.clone())
        } else {
            (DeletionOp::ReleaseImagelessBuildVolume, self.record(id))
        };
        log_deletion(&self.project, op, kind, workspace, Some(&image), &artifact);
    }

    pub fn images(&self) -> &Path {
        &self.images
    }

    pub fn mounts(&self) -> &Path {
        &self.mounts
    }

    pub fn image(&self, id: &BuildVolumeId) -> PathBuf {
        self.images.join(format!("{id}.{IMAGE_EXTENSION}"))
    }

    /// Where this project's released images wait for the gateway's cleanup, `build/.deleting`
    /// ([`crate::storage::apfs::release::DELETING_DIRECTORY`]).
    pub fn deleting(&self) -> PathBuf {
        self.images
            .join(crate::storage::apfs::release::DELETING_DIRECTORY)
    }

    /// `id`'s entry in [`Self::deleting`] once its release is committed.
    pub fn released(&self, id: &BuildVolumeId) -> PathBuf {
        self.deleting().join(format!("{id}.{IMAGE_EXTENSION}"))
    }

    /// Where a seed compaction works (16_build_volumes.md, "Seed compaction"),
    /// `build/.compacting`: its lock, its last success, the clone of the seed it copies and the
    /// copy. No enumeration of images, records or the release journal reads it.
    pub fn compacting(&self) -> PathBuf {
        self.images.join(COMPACTING_DIRECTORY)
    }

    /// Where the background build-volume collection a land starts logs (16_build_volumes.md,
    /// Land 6.5), `build/.collecting`. No enumeration of images, records or the release journal
    /// reads it.
    pub fn collecting(&self) -> PathBuf {
        self.images.join(COLLECTING_DIRECTORY)
    }

    /// Release `id`'s detached image inside the operation that retires or replaces it, and say
    /// whether an image was there (16_build_volumes.md, "Garbage collection"): one rename into
    /// [`Self::deleting`] commits it, and this returns once that rename is
    /// durable, freeing nothing; the gateway's cleanup frees the bytes. A failure before the rename leaves the image
    /// and record for the same operation's retry.
    pub fn release_image(&self, id: &BuildVolumeId) -> io::Result<bool> {
        crate::storage::apfs::release::commit_release(&self.image(id), &self.released(id))
    }

    pub fn record(&self, id: &BuildVolumeId) -> PathBuf {
        self.images
            .join(format!("{id}.{IMAGE_EXTENSION}{RECORD_SUFFIX}"))
    }

    pub fn mount(&self, id: &BuildVolumeId) -> PathBuf {
        self.mounts.join(id.as_str())
    }

    /// The lock file jobs hold `id` by ([`Self::hold`]); a release deletes it with the image.
    pub fn hold_path(&self, id: &BuildVolumeId) -> PathBuf {
        self.images
            .join(format!("{id}.{IMAGE_EXTENSION}{HOLD_SUFFIX}"))
    }

    /// Hold `id` for a land, for as long as the answer lives: while any hold lives, no
    /// release unmounts the volume (16_build_volumes.md, "Garbage collection"). An exclusive claim
    /// on the volume (a release, a seed capture, a reseed, a cache clear, a dry-run probe) is waited
    /// out, because only what it leaves says whether the volume is still there. Refused with
    /// [`io::ErrorKind::ResourceBusy`] only once a release has deleted the volume: a release deletes
    /// the image before it lets go of its claim, so a hold that waited for it finds no image, and
    /// the hold file it made is removed again. Every claimant takes its claim without waiting, so
    /// this waits for the claimant's own work, never for another hold.
    // WHY unbounded: queued, not silent: an exclusive claimant (a release, seed capture, reseed or
    // cache clear) holds it across its own work, and this hold names that claimant once per
    // HOLDER_REPORT while it keeps its place; the claimant's release at its work's end, or the
    // kernel's at its exit, ends the wait.
    #[allow(clippy::disallowed_methods)]
    pub fn hold(
        &self,
        census: &crate::census::CensusScope,
        id: &BuildVolumeId,
    ) -> io::Result<BuildVolumeHold> {
        let path = self.hold_path(id);
        let file = open_hold(&path)?;
        cowshed_os::flock::queued(
            &path,
            cowshed_os::flock::HOLDER_REPORT,
            |lock| crate::held_lock::holders(census, lock),
            || loop {
                match file.lock_shared() {
                    // A signal woke the wait; the claim it waits for is unchanged.
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    taken => return taken,
                }
            },
        )?;
        self.present(id, &path, file)
    }

    /// Hold `id` for one job admitted on it, without waiting (16_build_volumes.md, "Targets and
    /// seeds"): refused with [`io::ErrorKind::ResourceBusy`] while any exclusive claim has the
    /// volume, so no job starts during a release or a seed capture, and once a release has deleted
    /// it.
    pub fn admit(&self, id: &BuildVolumeId) -> io::Result<BuildVolumeHold> {
        let path = self.hold_path(id);
        let file = open_hold(&path)?;
        match file.try_lock_shared() {
            Ok(()) => self.present(id, &path, file),
            Err(fs::TryLockError::WouldBlock) => Err(claimed(id)),
            Err(fs::TryLockError::Error(error)) => Err(error),
        }
    }

    /// `file`, locked shared, as a hold of `id` while its image is there. Otherwise a release
    /// deleted the volume, and the hold file this made is removed again.
    fn present(
        &self,
        id: &BuildVolumeId,
        path: &Path,
        file: Fenced<fs::File>,
    ) -> io::Result<BuildVolumeHold> {
        if self.image(id).exists() {
            return Ok(BuildVolumeHold { _file: file });
        }
        drop(file);
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            format!("build volume {id} was released"),
        ))
    }

    /// Hold `id` for the operation creating it, before its image exists
    /// ([`Self::register`]): no collection that listed its registered record releases it while
    /// the creation runs. Nothing claims an id before its image exists, so the lock is never
    /// contended; one that is refuses as [`io::ErrorKind::ResourceBusy`].
    pub fn hold_new(&self, id: &BuildVolumeId) -> io::Result<BuildVolumeHold> {
        let file = open_hold(&self.hold_path(id))?;
        match file.try_lock_shared() {
            Ok(()) => Ok(BuildVolumeHold { _file: file }),
            Err(fs::TryLockError::WouldBlock) => Err(claimed(id)),
            Err(fs::TryLockError::Error(error)) => Err(error),
        }
    }

    /// Whether a job holds `id` now, asked without taking or creating anything: what a dry run
    /// reports in place of a release. The probe's own momentary lock is fenced too, so no child
    /// spawned meanwhile keeps it and refuses a hold taken after this answer.
    pub fn held(&self, id: &BuildVolumeId) -> io::Result<bool> {
        hold_is_held(&self.hold_path(id))
    }

    /// Claim `id` for its release: `None` while a job holds it. The claim keeps every new hold
    /// out until it is dropped, which the release does once the image and its hold file are gone.
    pub fn claim_release(&self, id: &BuildVolumeId) -> io::Result<Option<ReleaseClaim>> {
        let file = open_hold(&self.hold_path(id))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(ReleaseClaim {
                id: id.clone(),
                _file: file,
            })),
            Err(fs::TryLockError::WouldBlock) => Ok(None),
            Err(fs::TryLockError::Error(error)) => Err(error),
        }
    }

    /// Whether every hold on volume `id` is gone within `bound`: an exclusive lock on its hold file
    /// granted as the lock's waiter, then let go at once. A probe right after the last hold's close
    /// can still find it held through a transient kernel copy of the closed descriptor that another
    /// thread's spawn made (cowshed-os `fork_lock`: such copies close moments later on their own, with
    /// no owner and no event); only a waiter's grant proves the release. `false` when a hold
    /// outlives `bound`: a job still runs on the volume.
    pub fn holds_gone_within(
        &self,
        id: &BuildVolumeId,
        bound: cowshed_os::deadline::Bound,
    ) -> io::Result<bool> {
        let path = self.hold_path(id);
        let file = open_hold(&path)?;
        let deadline = cowshed_os::deadline::start("wait out a build volume's last hold", bound);
        match cowshed_os::flock::exclusive(file, &path, &deadline, |_| String::new()) {
            Ok(_granted) => Ok(true),
            Err(cowshed_os::flock::LockError::Expired(_)) => Ok(false),
            Err(cowshed_os::flock::LockError::Io { source, .. }) => Err(source),
        }
    }

    /// [`Self::claim_release`] granted as the lock's waiter, within the test backstop. A probe
    /// right after the last hold's close can still see a transient kernel copy of its descriptor
    /// from another thread's spawn (cowshed-os `fork_lock`); the waiter's grant proves the release.
    #[cfg(all(test, target_os = "macos"))]
    pub(crate) fn claim_release_granted(&self, id: &BuildVolumeId) -> io::Result<ReleaseClaim> {
        use cowshed_os::test_backstop::FileBackstop as _;
        let file = open_hold(&self.hold_path(id))?;
        file.lock_bounded()?;
        Ok(ReleaseClaim {
            id: id.clone(),
            _file: file,
        })
    }

    /// Whether a release holds its exclusive claim on volume `id` ([`Self::claim_release`]). A
    /// volume with no hold file was never claimed.
    pub fn claimed(&self, id: &BuildVolumeId) -> io::Result<bool> {
        let file = match fs::File::open(self.hold_path(id)).map(Fenced::new) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        match file.try_lock_shared() {
            Ok(()) => Ok(false),
            Err(fs::TryLockError::WouldBlock) => Ok(true),
            Err(fs::TryLockError::Error(error)) => Err(error),
        }
    }

    /// The volume a build link's target names, or `None` when the target is not one of this
    /// project's build mountpoints. Root aliases compare by their resolved parent directories.
    pub fn volume_at(&self, target: &Path) -> Option<BuildVolumeId> {
        if target.parent()?.canonicalize().ok()? != self.mounts.canonicalize().ok()? {
            return None;
        }
        BuildVolumeId::parse(target.file_name()?.to_str()?)
    }

    /// The build volume `checkout` links, or `None` when it links none. A link outside
    /// this controller-owned project's mount directory is never accepted as authority.
    pub fn linked(&self, checkout: &Path) -> crate::Result<Option<BuildVolumeId>> {
        let Some(target) = link::linked(checkout)? else {
            return Ok(None);
        };
        self.volume_at(&target).map(Some).ok_or_else(|| {
            crate::CowshedError::integrity(
                format!(
                    "{} names {}, which is not one of this project's build volumes; expected a volume under {} (root aliases are resolved)",
                    checkout.join(link::BUILD_LINK).display(),
                    target.display(),
                    self.mounts.display(),
                ),
                "cowshed doctor --json",
            )
        })
    }

    /// Resolve the current physical grant for one job, including an Nx keeper restart.
    /// Mounting already resolved stale links; a volume this workspace does not own refuses.
    pub fn grant(
        &self,
        workspace: &WorkspaceName,
        checkout: &Path,
    ) -> crate::Result<Option<PathBuf>> {
        let Some(id) = self.linked(checkout)? else {
            return Ok(None);
        };
        match self.resolve_link(workspace, &id)? {
            LinkResolution::Keep => Ok(Some(self.mount(&id))),
            resolution => Err(crate::CowshedError::integrity(
                format!(
                    "{workspace}'s mounted build link names {id}, which mounting should have \
                     resolved ({resolution:?})"
                ),
                format!("cowshed detach {workspace}, then retry"),
            )),
        }
    }

    pub fn read_record(&self, id: &BuildVolumeId) -> crate::Result<BuildVolumeRecord> {
        self.read_record_present(id)?.ok_or_else(|| {
            crate::CowshedError::integrity(
                format!(
                    "build volume {id} has no record at {}",
                    self.record(id).display()
                ),
                "cowshed doctor --json",
            )
        })
    }

    /// The record of `id`, or `None` when it has none. Every other failure to read it is an
    /// error: an unreadable record says nothing about the volume.
    pub fn read_record_present(
        &self,
        id: &BuildVolumeId,
    ) -> crate::Result<Option<BuildVolumeRecord>> {
        #[cfg(test)]
        SEED_IO_COUNTS.with(|counts| {
            let (listings, reads) = counts.get();
            counts.set((listings, reads + 1));
        });
        let path = self.record(id);
        let record: BuildVolumeRecord = match read_json(&path) {
            Ok(record) => record,
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(record_error(&path, &error)),
        };
        if record.version != RECORD_VERSION {
            return Err(unknown_version(&path, record.version));
        }
        Ok(Some(record))
    }

    pub fn write_record(
        &self,
        id: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> crate::Result<()> {
        let path = self.record(id);
        write_json(&path, record).map_err(|error| record_error(&path, &error))
    }

    /// Register `id` to `registration`'s owner before any byte of its image exists
    /// (16_build_volumes.md, "Garbage collection"): hold it as its creation, then durably write
    /// its registered record. The answered hold keeps every release off the volume until the
    /// creator drops it; a creator that dies leaves the record naming its owner and registrant,
    /// which is how a later pass finds the interrupted creation by its exact id, never by
    /// scanning for images nobody owns. An image already at `id` refuses: its bytes would predate
    /// their registration. A record already there refuses unless it is this registration, which
    /// a journaled replay (re-adoption) writes again.
    pub fn register(
        &self,
        id: &BuildVolumeId,
        registration: &Registration,
    ) -> crate::Result<BuildVolumeHold> {
        self.require_imageless(id)?;
        let hold = self.hold_new(id).map_err(|error| {
            io_error(
                "hold a build volume while it is created",
                &self.hold_path(id),
                &error,
            )
        })?;
        self.write_registration(id, registration)?;
        Ok(hold)
    }

    /// [`Self::register`] under the caller's exclusive claim on `claim.id()`, which keeps every
    /// hold and release off the volume as a creation's own hold would, and which a second hold
    /// would only collide with: a re-adoption replays its build volume's registration under the
    /// claim it took on that id.
    pub fn register_claimed(
        &self,
        claim: &ReleaseClaim,
        registration: &Registration,
    ) -> crate::Result<()> {
        self.require_imageless(claim.id())?;
        self.write_registration(claim.id(), registration)
    }

    /// Refuse an image already at `id`, whose bytes would predate their registration, and make
    /// the project's images directory: its first registration comes before any image exists.
    fn require_imageless(&self, id: &BuildVolumeId) -> crate::Result<()> {
        let image = self.image(id);
        match fs::symlink_metadata(&image) {
            Ok(_) => {
                return Err(crate::CowshedError::integrity(
                    format!(
                        "build volume {id} cannot be registered: {} already exists, so its bytes would predate their owner",
                        image.display()
                    ),
                    "cowshed doctor --json",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("inspect a build volume image", &image, &error)),
        }
        fs::create_dir_all(&self.images)
            .map_err(|error| io_error("create the build images directory", &self.images, &error))
    }

    /// Write `registration`'s registered record for `id`, or accept the same one a replay wrote
    /// before; any other record refuses.
    fn write_registration(
        &self,
        id: &BuildVolumeId,
        registration: &Registration,
    ) -> crate::Result<()> {
        match self.read_record_present(id)? {
            None => self.write_record(id, registration.registered())?,
            Some(found) if &found == registration.registered() => {}
            Some(found) => {
                return Err(crate::CowshedError::integrity(
                    format!(
                        "build volume {id} cannot be registered to {}: its record already names {:?}",
                        registration.role().owner(),
                        found.role
                    ),
                    "cowshed doctor --json",
                ));
            }
        }
        Ok(())
    }

    /// Publish `id`: its image now holds what `registration`'s role promises.
    pub fn publish(&self, id: &BuildVolumeId, registration: &Registration) -> crate::Result<()> {
        self.write_record(id, &registration.published())
    }

    /// Every published build volume: each `<id>.dmg` directly in the images directory.
    pub fn list(&self) -> crate::Result<Vec<BuildVolumeId>> {
        self.list_entries(false)
    }

    /// Every published build volume whose record says `checkout` links it: the live volume a
    /// checkout owns, and any an interrupted land left recorded as its as well.
    pub fn owned_by(&self, checkout: &WorkspaceName) -> crate::Result<Vec<BuildVolumeId>> {
        let mut owned = Vec::new();
        for id in self.list()? {
            if let Some(BuildVolumeRecord {
                role: BuildVolumeRole::Linked { checkout: owner },
                ..
            }) = self.read_record_present(&id)?
                && owner == *checkout
            {
                owned.push(id);
            }
        }
        Ok(owned)
    }

    /// GC's snapshot includes sidecars as well as images; seed selection never does.
    pub fn collection_ids(&self) -> crate::Result<Vec<BuildVolumeId>> {
        self.list_entries(true)
    }

    fn list_entries(&self, include_records: bool) -> crate::Result<Vec<BuildVolumeId>> {
        #[cfg(test)]
        SEED_IO_COUNTS.with(|counts| {
            let (listings, reads) = counts.get();
            counts.set((listings + 1, reads));
        });
        let entries = match fs::read_dir(&self.images) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_error("list build volumes", &self.images, &error)),
        };
        let mut ids = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|error| io_error("list build volumes", &self.images, &error))?;
            match VolumeFile::of(&entry.file_name()) {
                Some(VolumeFile::Image(id)) => ids.push(id),
                Some(VolumeFile::Record(id)) if include_records => ids.push(id),
                Some(VolumeFile::Record(_)) | None => {}
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// The volume `checkout`'s build link should name, given that it names `linked`
    /// (16_build_volumes.md, "One link per checkout"). The link is kept when its volume is
    /// recorded as `checkout`'s, published or still registered (a first touch the link already
    /// names, which the next refresh finishes). Otherwise the link is stale: an interrupted land
    /// can leave a target naming the
    /// landing volume before its record moved. It is then re-pointed at the one volume recorded
    /// as `checkout`'s. When `checkout` owns none and `linked` is another checkout's live
    /// volume, a target adopted it from this checkout before a `land --no-retire` reforked it:
    /// the checkout reforks from that target's latest seed. When `checkout` owns none and
    /// `linked`'s image is gone, whatever its record still says, nobody owns what the link names:
    /// [`LinkResolution::Vanished`]. Anything else refuses, so a checkout never writes a volume
    /// another checkout, a target or a seed owns.
    pub fn resolve_link(
        &self,
        checkout: &WorkspaceName,
        linked: &BuildVolumeId,
    ) -> crate::Result<LinkResolution> {
        let owns = |record: &BuildVolumeRecord| matches!(&record.role, BuildVolumeRole::Linked { checkout: owner } if owner == checkout);
        // A creation registers its record before the image exists and a release deletes the
        // image first, so a link to a missing image names a volume that no longer exists: its
        // record, when one is left, speaks for nothing.
        let vanished = !self.image(linked).exists();
        let adopter = match self.read_record_present(linked)? {
            _ if vanished => None,
            Some(record) if owns(&record) => return Ok(LinkResolution::Keep),
            None => {
                return Err(crate::CowshedError::integrity(
                    format!(
                        "{checkout}'s build link names build volume {linked}, whose image {} has no owner record: it was made outside owner registration, so nothing proves whose it is",
                        self.image(linked).display()
                    ),
                    format!(
                        "remove {checkout}'s {BUILD_LINK} link so its next build-state refresh makes a registered volume; `cowshed gc --dry-run` names the unregistered image"
                    ),
                ));
            }
            Some(BuildVolumeRecord {
                role: BuildVolumeRole::Linked { checkout: adopter },
                stage: BuildVolumeStage::Published,
                ..
            }) => Some(adopter),
            Some(_) => None,
        };
        let mut owned = Vec::new();
        let mut adopter_seeds = Vec::new();
        for id in self.list()? {
            // Only a finished volume is someone's to mount or fork from: a registered one may
            // still be forming, or be an interrupted creation collection releases.
            match self.read_record_present(&id)? {
                Some(record) if record.is_published() && owns(&record) => owned.push(id),
                Some(record)
                    if record.is_published()
                        && matches!(&record.role, BuildVolumeRole::Seed { target, .. } if adopter.as_ref() == Some(target)) =>
                {
                    adopter_seeds.push((record.created_at, id));
                }
                _ => {}
            }
        }
        if owned.is_empty() {
            if let Some((_, seed)) = adopter_seeds.into_iter().max() {
                return Ok(LinkResolution::Refork(seed));
            }
            if vanished {
                return Ok(LinkResolution::Vanished);
            }
        }
        match owned.as_slice() {
            [id] => Ok(LinkResolution::Repoint(id.clone())),
            _ => Err(crate::CowshedError::integrity(
                format!(
                    "{checkout}'s build link names {linked}, which {checkout} does not own, and {} \
                     build volumes are recorded as {checkout}'s{}",
                    owned.len(),
                    owned.iter().map(|id| format!(" {id}")).collect::<String>()
                ),
                "cowshed doctor --json",
            )),
        }
    }

    /// The latest seed of `target` at `incarnation`. Two seeds of one target are an
    /// interrupted reseed: the newer is the latest. A seed a release claims is never the latest
    /// (16_build_volumes.md, "Garbage collection"): on a tie of creation times, recorded to the
    /// second, the unclaimed seed is the newer, as a superseded seed's successor is; a newest seed
    /// a release claims (its target retiring) leaves the target no seed, never an older one.
    pub fn seed_of(
        &self,
        target: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> crate::Result<Option<(BuildVolumeId, BuildVolumeRecord)>> {
        let mut observed = self.seed_records();
        let latest = observed.latest(target, incarnation, |id| self.seed_claimed(id))?;
        Ok(latest.map(|index| observed.records.swap_remove(index)))
    }

    /// Observe records without claiming images or granting mutation authority. An unreadable
    /// record stops the observation, but is reported only by a target that reaches seed lookup.
    pub(crate) fn seed_records(&self) -> SeedRecords {
        match self.list() {
            Ok(ids) => SeedRecords::observe(
                ids.into_iter()
                    .map(|id| self.read_record_present(&id).map(|record| (id, record))),
            ),
            Err(error) => SeedRecords {
                error: Some(error),
                ..SeedRecords::default()
            },
        }
    }

    fn seed_claimed(&self, id: &BuildVolumeId) -> crate::Result<bool> {
        self.claimed(id).map_err(|error| {
            crate::CowshedError::integrity(
                format!(
                    "build volume hold {} is unreadable: {error}",
                    self.hold_path(id).display()
                ),
                "cowshed doctor --json",
            )
        })
    }
}

/// One job's claim on the build volume it was admitted with ([`BuildVolumeLayout::admit`]), or a
/// land's ([`BuildVolumeLayout::hold`]): a
/// shared lock that keeps every release from unmounting the volume until it is dropped, when the
/// job ends. The kernel drops it with the process that held it, so a crashed supervisor leaves
/// no stale claim. It is fenced (`fork_lock`): a child spawned while the claim was open copied
/// its descriptor, and an unfenced drop would leave that copy holding the volume until the child
/// runs `exec`; the drop waits for such a spawn instead, so the claim is gone when it returns.
#[derive(Debug)]
pub struct BuildVolumeHold {
    _file: Fenced<fs::File>,
}

/// A release's exclusive claim on build volume `id` ([`BuildVolumeLayout::claim_release`]).
#[derive(Debug)]
pub struct ReleaseClaim {
    id: BuildVolumeId,
    _file: Fenced<fs::File>,
}

impl ReleaseClaim {
    /// The volume this claim keeps every new hold off.
    pub fn id(&self) -> &BuildVolumeId {
        &self.id
    }
}

/// The released build images of every project in the store at `store`, as the gateway's cleanup
/// lists them at its start (05_gateway.md, "Deleting released images"): each
/// `<store>/<owner>/<repo>/build/.deleting`. Only `<id>.dmg` files are listed; anything else is
/// left and named, as is a directory that cannot be read.
pub fn released_build_volumes(store: &Path) -> Listing {
    released_in_store(
        store,
        IMAGES_DIRECTORY,
        crate::storage::apfs::release::DELETING_DIRECTORY,
        is_image_name,
    )
}

/// Whether `name` is a build image's file name, `<id>.dmg`.
pub(crate) fn is_image_name(name: &str) -> bool {
    name.strip_suffix(&format!(".{IMAGE_EXTENSION}"))
        .and_then(BuildVolumeId::parse)
        .is_some()
}

/// Open (creating) `path`, the lock file every hold and release claim of one volume shares,
/// fenced from the moment it is opened.
fn open_hold(path: &Path) -> io::Result<Fenced<fs::File>> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map(Fenced::new)
}

/// The refusal of a hold taken without waiting while an exclusive claim has `id`. The lock does
/// not say which claimant has it.
fn claimed(id: &BuildVolumeId) -> io::Error {
    io::Error::new(
        io::ErrorKind::ResourceBusy,
        format!(
            "build volume {id} is claimed exclusively by a release, a seed capture, a reseed, a cache clear or a hold probe"
        ),
    )
}

fn hold_is_held(path: &Path) -> io::Result<bool> {
    let file = match fs::File::open(path) {
        Ok(file) => Fenced::new(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(fs::TryLockError::WouldBlock) => Ok(true),
        Err(fs::TryLockError::Error(error)) => Err(error),
    }
}

fn unknown_version(path: &Path, version: u32) -> crate::CowshedError {
    crate::CowshedError::integrity(
        format!(
            "{} is version {version}; this cowshed reads version {RECORD_VERSION}",
            path.display()
        ),
        "run the cowshed that wrote it, or `cowshed doctor --json`",
    )
}

fn record_error(path: &Path, error: &MetadataError) -> crate::CowshedError {
    crate::CowshedError::integrity(
        format!(
            "build volume record {} is unreadable: {error}",
            path.display()
        ),
        "cowshed doctor --json",
    )
}

fn io_error(operation: &str, path: &Path, error: &io::Error) -> crate::CowshedError {
    crate::CowshedError::environment_missing(
        format!("cannot {operation} at {}: {error}", path.display()),
        "check the cowshed store's permissions and retry",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::RepoId;

    fn layout() -> BuildVolumeLayout {
        let project = ProjectPaths::with_mount_root(
            "/store",
            "/Users/tester/Dev/.cowshed",
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        BuildVolumeLayout::new(&project).unwrap()
    }

    #[test]
    fn retained_metadata_rediscovery_keeps_live_paths_and_bytes() {
        let root =
            crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-retained-metadata");
        fs::create_dir_all(root.join("volume/old-index")).unwrap();
        let volume = root.join("volume");
        let recorded = br#"{"version":1,"paths":[{"checkout":".index","volume":"old-index"},{"checkout":".removed-cache","volume":"removed-cache"}],"fingerprint":"old"}"#;
        fs::write(volume.join(STATE_FILE), recorded).unwrap();
        fs::write(volume.join("old-index/marker"), b"live retained bytes").unwrap();
        let old = BuildVolumeState::read(&volume).unwrap();
        assert_eq!(
            old.paths[0].retention,
            crate::capabilities::SeedRetention::Unrecorded
        );
        let policy = BuildStatePath::new(".index", "index")
            .unwrap()
            .retain()
            .discard_on_clone("owner.pid")
            .unwrap();
        let (next, added, dropped) = old
            .with_discovered(
                std::slice::from_ref(&policy),
                &[],
                None,
                "new".into(),
                &volume,
            )
            .unwrap();
        assert!(added.is_empty() && dropped.is_empty());
        assert_eq!(
            next.paths[0].volume, old.paths[0].volume,
            "metadata never moves a fixed link"
        );
        assert!(next.paths[0].retained());
        assert_eq!(next.paths[0].clone_invalid, policy.clone_invalid);
        assert_eq!(
            next.paths[1].retention,
            crate::capabilities::SeedRetention::Rebuild,
            "canonical discovery resolves an unowned legacy cache without another refresh loop"
        );
        assert_eq!(
            old.with_seed_retention(std::slice::from_ref(&policy)).paths,
            next.paths
        );
        assert_eq!(
            fs::read(volume.join("old-index/marker")).unwrap(),
            b"live retained bytes"
        );
        assert_eq!(fs::read(volume.join(STATE_FILE)).unwrap(), recorded);
        fs::create_dir(root.join("roundtrip")).unwrap();
        next.write(&root.join("roundtrip")).unwrap();
        assert_eq!(
            BuildVolumeState::read(&root.join("roundtrip")).unwrap(),
            next
        );
    }

    /// `[build] keep` lives in the volume's state: a refresh at an unchanged fingerprint reads its
    /// paths from that record alone, so a kept declared path stays kept across it, and the next
    /// discovery moves a held path's keep with its declaration.
    #[test]
    fn a_declared_path_s_keep_is_recorded_and_follows_its_declaration() {
        let root = crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-declared-keep");
        let volume = root.join("volume");
        fs::create_dir_all(&volume).unwrap();
        let pin = BuildStatePath::declared("packages/pin/.bun-pin").unwrap();
        let traces = BuildStatePath::declared(".cache/lmao").unwrap();
        let tools = |state: &BuildVolumeState| {
            state
                .paths
                .iter()
                .map(BuildStateTool::of)
                .collect::<Vec<_>>()
        };
        let (kept, _, _) = BuildVolumeState::default()
            .with_discovered(
                &[pin.clone().kept(), traces.clone()],
                &[],
                None,
                "kept".into(),
                &volume,
            )
            .unwrap();
        kept.write(&volume).unwrap();
        let read = BuildVolumeState::read(&volume).unwrap();
        assert_eq!(read, kept);
        assert_eq!(
            tools(&read),
            [BuildStateTool::Declared, BuildStateTool::DeclaredCache]
        );
        let (unkept, _, _) = read
            .with_discovered(&[pin, traces], &[], None, "unkept".into(), &volume)
            .unwrap();
        assert_eq!(
            tools(&unkept),
            [BuildStateTool::DeclaredCache, BuildStateTool::DeclaredCache],
            "a path dropped from [build] keep is a cache from its next discovery"
        );
        fs::write(
            volume.join(STATE_FILE),
            br#"{"version":1,"paths":[{"checkout":"target","volume":"target","keep":true}]}"#,
        )
        .unwrap();
        let refused = BuildVolumeState::read(&volume).unwrap_err();
        assert!(
            refused.message.contains("target records [build] keep"),
            "a capability's path is never kept: {refused:?}"
        );
    }

    #[test]
    fn clone_invalid_cleanup_changes_only_the_new_process_file() {
        let root = crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-clone-invalid");
        let source = root.join("source");
        let cloned = root.join("cloned");
        for volume in [&source, &cloned] {
            fs::create_dir_all(volume.join("index")).unwrap();
            for name in ["data", "data-wal", "data-shm", "owner.pid"] {
                fs::write(volume.join("index").join(name), name.as_bytes()).unwrap();
            }
        }
        let state = BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".index", "index")
                    .unwrap()
                    .retain()
                    .discard_on_clone("owner.pid")
                    .unwrap(),
            ],
            ..Default::default()
        };
        state.discard_clone_invalid(&cloned).unwrap();
        assert!(!cloned.join("index/owner.pid").exists());
        for name in ["data", "data-wal", "data-shm", "owner.pid"] {
            assert_eq!(
                fs::read(source.join("index").join(name)).unwrap(),
                name.as_bytes()
            );
            if name != "owner.pid" {
                assert_eq!(
                    fs::read(cloned.join("index").join(name)).unwrap(),
                    name.as_bytes()
                );
            }
        }
        state
            .discard_clone_invalid(&cloned)
            .expect("a missing process file needs no cleanup");
    }

    #[test]
    fn clone_invalid_cleanup_refuses_symlink_escapes() {
        let root =
            crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-clone-invalid-escape");
        let outside = root.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("owner.pid"), b"not owned by clone").unwrap();
        for (name, nested) in [("root-escape", false), ("parent-escape", true)] {
            let volume = root.join(name);
            fs::create_dir(&volume).unwrap();
            let invalid = if nested {
                fs::create_dir(volume.join("index")).unwrap();
                std::os::unix::fs::symlink(&outside, volume.join("index/escape")).unwrap();
                "escape/owner.pid"
            } else {
                std::os::unix::fs::symlink(&outside, volume.join("index")).unwrap();
                "owner.pid"
            };
            let state = BuildVolumeState {
                paths: vec![
                    BuildStatePath::new(".index", "index")
                        .unwrap()
                        .retain()
                        .discard_on_clone(invalid)
                        .unwrap(),
                ],
                ..Default::default()
            };
            let refusal = state.discard_clone_invalid(&volume).unwrap_err();
            assert!(refusal.message.contains("escapes"), "{refusal}");
            assert_eq!(
                fs::read(outside.join("owner.pid")).unwrap(),
                b"not owned by clone"
            );
        }
    }

    #[test]
    fn ids_are_exactly_32_lowercase_hex_digits() {
        let id = BuildVolumeId::mint();
        assert_eq!(BuildVolumeId::parse(id.as_str()), Some(id));
        for invalid in [
            "",
            "main",
            "0123456789ABCDEF0123456789abcdef",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "../23456789abcdef0123456789abcdef",
        ] {
            assert_eq!(BuildVolumeId::parse(invalid), None, "{invalid:?}");
        }
    }

    #[test]
    fn images_live_in_the_project_and_mounts_beside_every_project_tree() {
        let layout = layout();
        let id = BuildVolumeId::parse("0123456789abcdef0123456789abcdef").unwrap();
        assert_eq!(
            layout.image(&id),
            Path::new("/store/acme/widget/build/0123456789abcdef0123456789abcdef.dmg")
        );
        assert_eq!(
            layout.record(&id),
            Path::new("/store/acme/widget/build/0123456789abcdef0123456789abcdef.dmg.json")
        );
        assert_eq!(
            layout.mount(&id),
            Path::new(
                "/Users/tester/Dev/.cowshed/.build/acme/widget/0123456789abcdef0123456789abcdef"
            )
        );
    }

    #[cfg(unix)]
    #[test]
    fn build_links_accept_root_aliases_but_refuse_other_physical_namespaces() {
        use std::os::unix::fs::symlink;

        let root =
            std::env::temp_dir().join(format!("cowshed-build-alias-{}", uuid::Uuid::new_v4()));
        let physical = root.join("physical");
        let alias = root.join("preferred");
        let repo = RepoId::parse("acme/widget").unwrap();
        let project = ProjectPaths::with_mount_root(root.join("store"), &physical, &repo).unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        fs::create_dir_all(layout.mounts()).unwrap();
        symlink(&physical, &alias).unwrap();
        let id = BuildVolumeId::mint();
        let target = layout.mount(&id);
        fs::create_dir(&target).unwrap();
        let alias_target = alias.join(".build/acme/widget").join(id.as_str());
        let checkout = root.join("checkout");
        fs::create_dir_all(checkout.join(".cowshed")).unwrap();
        symlink(&alias_target, checkout.join(link::BUILD_LINK)).unwrap();

        assert_eq!(layout.volume_at(&target), Some(id.clone()));
        assert_eq!(layout.linked(&checkout).unwrap(), Some(id.clone()));
        let aliased = BuildVolumeLayout::new(
            &ProjectPaths::with_mount_root(root.join("store"), &alias, &repo).unwrap(),
        )
        .unwrap();
        assert_eq!(aliased.volume_at(&target), Some(id.clone()));
        assert_eq!(layout.volume_at(&target.join("target")), None);

        let other = physical.join(".build/acme/other");
        fs::create_dir_all(&other).unwrap();
        fs::remove_file(checkout.join(link::BUILD_LINK)).unwrap();
        symlink(other.join(id.as_str()), checkout.join(link::BUILD_LINK)).unwrap();
        let error = layout.linked(&checkout).unwrap_err();
        assert!(
            error
                .message
                .contains("not one of this project's build volumes")
        );
        assert_eq!(layout.volume_at(&other.join(id.as_str())), None);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn records_round_trip_and_name_their_role() {
        let main = WorkspaceName::main();
        let incarnation = WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef").unwrap();
        let seed = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: main.clone(),
                incarnation: incarnation.clone(),
            },
        );
        let json = serde_json::to_string(&seed).unwrap();
        assert!(json.contains("\"kind\":\"seed\""), "{json}");
        let parsed: BuildVolumeRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, seed);
        assert!(parsed.is_seed_of(&main, &incarnation));
        assert!(!parsed.is_seed_of(&WorkspaceName::new("lane").unwrap(), &incarnation));
        let linked: BuildVolumeRecord = serde_json::from_str(
            r#"{"version":1,"tree":null,"role":{"kind":"linked","checkout":"topic"},"createdAt":"2026-10-05T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(
            linked.role,
            BuildVolumeRole::Linked {
                checkout: WorkspaceName::new("topic").unwrap()
            }
        );
        // A record written before stages existed is a published one, and a published record
        // writes no stage, so it reads back unchanged everywhere.
        assert!(linked.is_published());
        assert!(!serde_json::to_string(&linked).unwrap().contains("stage"));

        // A registration names its owner and its registrant before any image exists, and its
        // published record is the same record, finished.
        let registration = Registration::new(
            seed.clone(),
            Registrant::Workspace {
                incarnation: incarnation.clone(),
            },
        );
        let json = serde_json::to_string(registration.registered()).unwrap();
        assert!(json.contains("\"registered\""), "{json}");
        let parsed: BuildVolumeRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(&parsed, registration.registered());
        assert!(!parsed.is_published());
        assert!(
            !parsed.is_seed_of(&main, &incarnation),
            "a registered seed may still be forming and is nobody's to fork"
        );
        assert_eq!(registration.published(), seed);
        assert_eq!(registration.role().owner(), &main);

        // A superseded volume keeps its owner and incarnation: the old ownerless kind is gone.
        let superseded = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Superseded {
                owner: main.clone(),
                incarnation: incarnation.clone(),
            },
        );
        let json = serde_json::to_string(&superseded).unwrap();
        assert!(json.contains("\"kind\":\"superseded\""), "{json}");
        assert_eq!(
            serde_json::from_str::<BuildVolumeRecord>(&json).unwrap(),
            superseded
        );
        assert!(
            serde_json::from_str::<BuildVolumeRecord>(
                r#"{"version":1,"tree":null,"role":{"kind":"unlinked"},"createdAt":"2026-10-05T00:00:00Z"}"#,
            )
            .is_err(),
            "an ownerless record is refused, never read as nobody's"
        );
    }

    /// Registration precedes every image byte and answers a hold that keeps a release off the
    /// volume while it forms; an existing image refuses, and so does a record naming another
    /// owner, while the same registration replayed is accepted.
    #[test]
    fn registration_precedes_the_image_and_refuses_bytes_that_predate_it() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-register-{}",
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
        let topic = WorkspaceName::new("topic").unwrap();
        let registration = Registration::new(
            BuildVolumeRecord::new(
                None,
                BuildVolumeRole::Linked {
                    checkout: topic.clone(),
                },
            ),
            Registrant::Intent,
        );

        let id = BuildVolumeId::mint();
        let creating = layout.register(&id, &registration).unwrap();
        assert!(!layout.image(&id).exists(), "registration makes no image");
        assert_eq!(
            layout.read_record(&id).unwrap(),
            registration.registered().clone()
        );
        assert!(
            layout.claim_release(&id).unwrap().is_none(),
            "no release claims a volume while its creation holds it"
        );
        // A replay of the same registration converges; another owner's refuses.
        drop(creating);
        drop(layout.register(&id, &registration).unwrap());
        let other = Registration::new(
            BuildVolumeRecord::new(
                None,
                BuildVolumeRole::Linked {
                    checkout: WorkspaceName::main(),
                },
            ),
            Registrant::Intent,
        );
        let error = layout.register(&id, &other).unwrap_err();
        assert!(error.message.contains("already names"), "{error:?}");
        layout.publish(&id, &registration).unwrap();
        assert!(layout.read_record(&id).unwrap().is_published());

        let bare = BuildVolumeId::mint();
        fs::write(layout.image(&bare), b"bytes nobody registered").unwrap();
        let error = layout.register(&bare, &registration).unwrap_err();
        assert!(error.message.contains("predate their owner"), "{error:?}");
        assert!(layout.read_record_present(&bare).unwrap().is_none());

        // A re-adoption registers under the exclusive claim it already holds: a fresh shared
        // hold on the same file is refused while that claim lives, and the claim alone keeps
        // every release out.
        let claimed = BuildVolumeId::mint();
        let claim = layout.claim_release(&claimed).unwrap().unwrap();
        layout.register_claimed(&claim, &registration).unwrap();
        assert_eq!(
            layout.read_record(&claimed).unwrap(),
            registration.registered().clone()
        );
        assert!(layout.claim_release(&claimed).unwrap().is_none());
        assert_eq!(
            layout.hold_new(&claimed).unwrap_err().kind(),
            io::ErrorKind::ResourceBusy
        );
        layout.register_claimed(&claim, &registration).unwrap();
        assert!(layout.register_claimed(&claim, &other).is_err());
        fs::write(layout.image(&claimed), b"registered bytes").unwrap();
        assert!(layout.register_claimed(&claim, &registration).is_err());
        drop(claim);
        assert!(layout.claim_release(&claimed).unwrap().is_some());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn fresh_seed_lookup_observes_records_once_and_never_falls_back_from_claimed_newest() {
        let root = crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "cs-seed-records");
        let project = ProjectPaths::with_mount_root(
            root.join("store"),
            root.join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        fs::create_dir_all(layout.images()).unwrap();
        let main = WorkspaceName::main();
        let incarnation = WorkspaceIncarnation::new("0".repeat(32)).unwrap();
        let seed = |second| {
            let id = BuildVolumeId::mint();
            let record = BuildVolumeRecord {
                version: RECORD_VERSION,
                tree: None,
                role: BuildVolumeRole::Seed {
                    target: main.clone(),
                    incarnation: incarnation.clone(),
                },
                created_at: format!("2026-10-05T00:00:{second:02}Z"),
                checked_out: None,
                stage: BuildVolumeStage::Published,
            };
            fs::write(layout.image(&id), b"").unwrap();
            layout.write_record(&id, &record).unwrap();
            id
        };
        let predecessor = seed(0);
        let claimed = layout.claim_release(&predecessor).unwrap().unwrap();
        let successor = seed(0);
        let missing = BuildVolumeId::mint();
        fs::write(layout.image(&missing), b"").unwrap();
        let before = seed_io_counts();
        assert_eq!(
            layout
                .seed_of(&main, &incarnation)
                .unwrap()
                .map(|(id, _)| id),
            Some(successor)
        );
        let after = seed_io_counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (1, 3));
        let retiring = seed(1);
        let retirement = layout.claim_release(&retiring).unwrap().unwrap();
        let before = seed_io_counts();
        assert_eq!(
            layout.seed_of(&main, &incarnation).unwrap(),
            None,
            "a new lifecycle lookup observes the new claimed seed, not the old observation"
        );
        let after = seed_io_counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (1, 4));
        drop((claimed, retirement));
    }

    #[test]
    fn latest_seed_decision_table() {
        let main = WorkspaceName::main();
        let incarnation = WorkspaceIncarnation::new("0".repeat(32)).unwrap();
        let record = |second, role| BuildVolumeRecord {
            version: RECORD_VERSION,
            tree: None,
            role,
            created_at: format!("2026-10-05T00:00:{second:02}Z"),
            checked_out: None,
            stage: BuildVolumeStage::Published,
        };
        let older = record(
            0,
            BuildVolumeRole::Seed {
                target: main.clone(),
                incarnation: incarnation.clone(),
            },
        );
        let newer = BuildVolumeRecord {
            created_at: "2026-10-05T00:00:01Z".to_owned(),
            ..older.clone()
        };
        let foreign = record(
            2,
            BuildVolumeRole::Seed {
                target: WorkspaceName::new("lane").unwrap(),
                incarnation: incarnation.clone(),
            },
        );
        let previous = record(
            2,
            BuildVolumeRole::Seed {
                target: main.clone(),
                incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
            },
        );
        let live = record(
            2,
            BuildVolumeRole::Linked {
                checkout: main.clone(),
            },
        );
        let forming = BuildVolumeRecord {
            stage: BuildVolumeStage::Registered {
                by: Registrant::Intent,
            },
            ..newer.clone()
        };
        for (name, rows, expected, expected_claims) in [
            ("empty", vec![], None, vec![]),
            ("missing record", vec![(None, false)], None, vec![]),
            (
                "newer wins",
                vec![(Some(older.clone()), false), (Some(newer.clone()), false)],
                Some(1),
                vec![0, 1],
            ),
            (
                "older later",
                vec![(Some(newer.clone()), false), (Some(older.clone()), false)],
                Some(0),
                vec![0, 1],
            ),
            (
                "newest claimed",
                vec![(Some(older.clone()), false), (Some(newer.clone()), true)],
                None,
                vec![0, 1],
            ),
            (
                "claimed newest first",
                vec![(Some(newer.clone()), true), (Some(older.clone()), false)],
                None,
                vec![0, 1],
            ),
            (
                "same second successor",
                vec![(Some(newer.clone()), true), (Some(newer.clone()), false)],
                Some(1),
                vec![0, 1],
            ),
            (
                "same second claimed later",
                vec![(Some(newer.clone()), false), (Some(newer.clone()), true)],
                Some(0),
                vec![0, 1],
            ),
            (
                "exact tie keeps enumeration order",
                vec![(Some(newer.clone()), false), (Some(newer.clone()), false)],
                Some(1),
                vec![0, 1],
            ),
            (
                "all claimed",
                vec![(Some(newer.clone()), true), (Some(newer.clone()), true)],
                None,
                vec![0, 1],
            ),
            (
                "only current published owner",
                vec![
                    (Some(older.clone()), false),
                    (Some(foreign), true),
                    (Some(previous), true),
                    (Some(live), true),
                    (Some(forming), true),
                    (None, true),
                ],
                Some(0),
                vec![0],
            ),
        ] {
            let holds = rows.iter().map(|(_, claimed)| *claimed).collect::<Vec<_>>();
            let observed =
                SeedRecords::observe(rows.into_iter().enumerate().map(|(index, (record, _))| {
                    Ok((
                        BuildVolumeId::parse(&format!("{index:032x}")).unwrap(),
                        record,
                    ))
                }));
            let mut claims = Vec::new();
            let selected = observed
                .latest(&main, &incarnation, |id| {
                    let index = usize::from_str_radix(id.as_str(), 16).unwrap();
                    claims.push(index);
                    Ok(holds[index])
                })
                .unwrap();
            assert_eq!(
                selected.map(
                    |index| usize::from_str_radix(observed.records[index].0.as_str(), 16).unwrap()
                ),
                expected,
                "{name}"
            );
            assert_eq!(
                claims, expected_claims,
                "{name}: unrelated holds must not be probed"
            );
        }
    }

    #[test]
    fn seed_record_and_matching_claim_errors_keep_enumeration_priority() {
        let main = WorkspaceName::main();
        let incarnation = WorkspaceIncarnation::new("0".repeat(32)).unwrap();
        let seed = BuildVolumeRecord {
            version: RECORD_VERSION,
            tree: None,
            role: BuildVolumeRole::Seed {
                target: main.clone(),
                incarnation: incarnation.clone(),
            },
            created_at: "2026-10-05T00:00:00Z".to_owned(),
            checked_out: None,
            stage: BuildVolumeStage::Published,
        };
        let id = BuildVolumeId::parse(&"0".repeat(32)).unwrap();
        let read_error = crate::CowshedError::integrity("record unreadable", "inspect record");
        let claim_error = crate::CowshedError::integrity("claim unreadable", "inspect hold");
        for (name, rows, refuse_claim, expected, expected_claims) in [
            (
                "record before matching seed",
                vec![
                    Err(read_error.clone()),
                    Ok((id.clone(), Some(seed.clone()))),
                ],
                true,
                read_error.clone(),
                0,
            ),
            (
                "matching claim before record",
                vec![
                    Ok((id.clone(), Some(seed.clone()))),
                    Err(read_error.clone()),
                ],
                true,
                claim_error.clone(),
                1,
            ),
            (
                "successful claim before record",
                vec![
                    Ok((id.clone(), Some(seed.clone()))),
                    Err(read_error.clone()),
                ],
                false,
                read_error.clone(),
                1,
            ),
        ] {
            let observed = SeedRecords::observe(rows);
            let mut claims = 0;
            let found = observed.latest(&main, &incarnation, |_| {
                claims += 1;
                if refuse_claim {
                    Err(claim_error.clone())
                } else {
                    Ok(false)
                }
            });
            assert_eq!(found, Err(expected), "{name}");
            assert_eq!(claims, expected_claims, "{name}");
            let foreign = WorkspaceName::new("unrelated").unwrap();
            assert_eq!(
                observed.latest(&foreign, &incarnation, |_| panic!("unrelated claim")),
                Err(read_error.clone()),
                "record failures precede owner filtering"
            );
        }
    }

    #[test]
    fn state_names_nx_directories_by_their_checkout_spelling() {
        let state = BuildVolumeState {
            paths: vec![
                BuildStatePath::new("target", "target").unwrap(),
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
                BuildStatePath::new("apps/web/.nx/workspace-data", "apps/web/nx/workspace-data")
                    .unwrap(),
            ],
            fingerprint: Some("f1".to_owned()),
            mbx: None,
        };
        assert_eq!(
            state.nx_workspace_data().collect::<Vec<_>>(),
            [
                Path::new("nx/workspace-data"),
                Path::new("apps/web/nx/workspace-data")
            ]
        );
        assert_eq!(
            state.nx_cache().collect::<Vec<_>>(),
            [Path::new("nx/cache")]
        );
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-state-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        assert_eq!(
            BuildVolumeState::read(&root).unwrap(),
            BuildVolumeState::default()
        );
        state.write(&root).unwrap();
        assert_eq!(BuildVolumeState::read(&root).unwrap(), state);
        fs::remove_dir_all(&root).unwrap();
    }

    /// Rediscovery adds only what the state does not hold yet; a held path keeps its volume name
    /// (links never move), and a new path inside a held one is refused by name.
    #[test]
    fn rediscovered_paths_are_added_never_moved() {
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new("target", "target").unwrap()],
            fingerprint: Some("old".to_owned()),
            mbx: None,
        };
        let discovered = [
            BuildStatePath::new("target", "elsewhere").unwrap(),
            BuildStatePath::new("tools/x/target", "tools-x-target").unwrap(),
        ];
        let (next, added, dropped) = state
            .with_discovered(&discovered, &[], None, "new".to_owned(), Path::new("/v"))
            .unwrap();
        assert_eq!(added, [discovered[1].clone()]);
        assert_eq!(next.paths, [state.paths[0].clone(), discovered[1].clone()]);
        assert_eq!(next.fingerprint.as_deref(), Some("new"));
        let inside = [BuildStatePath::new("target/sub", "sub").unwrap()];
        let error = state
            .with_discovered(&inside, &[], None, "new".to_owned(), Path::new("/v"))
            .unwrap_err();
        assert!(error.message.contains("target/sub"), "{error:?}");
        assert!(dropped.is_empty());
        assert_eq!(next.mbx, None);
    }

    /// Once the canonical environment resolves mbx, discovery releases the Cargo target mbx owns:
    /// the state drops it, names it as dropped, and records the mbx; nothing else moves.
    #[test]
    fn a_target_mbx_owns_leaves_the_state_and_the_mbx_is_recorded() {
        let nx = BuildStatePath::new(".nx/cache", "nx/cache").unwrap();
        let target = BuildStatePath::new("target", "target").unwrap();
        let state = BuildVolumeState {
            paths: vec![nx.clone(), target.clone()],
            fingerprint: Some("old".to_owned()),
            mbx: None,
        };
        let pin = PathBuf::from("/pinned/mbx-1.28.0/bin/mbx");
        let (next, added, dropped) = state
            .with_discovered(
                std::slice::from_ref(&nx),
                std::slice::from_ref(&target),
                Some(pin.clone()),
                "new".to_owned(),
                Path::new("/v"),
            )
            .unwrap();
        assert!(added.is_empty());
        assert_eq!(dropped, [target]);
        assert_eq!(next.paths, [nx]);
        assert_eq!(next.mbx, Some(pin));
    }

    /// A land interrupted between moving a target's link and its records leaves a link naming a
    /// volume the checkout does not own: mounting re-points it at
    /// the checkout's own volume, and refuses when there is none to point at.
    #[test]
    fn a_stale_build_link_is_repointed_at_the_checkouts_own_volume() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-resolve-{}",
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
        let topic = WorkspaceName::new("topic").unwrap();
        let volume = |role: BuildVolumeRole| {
            let id = BuildVolumeId::mint();
            fs::write(layout.image(&id), b"").unwrap();
            layout
                .write_record(&id, &BuildVolumeRecord::new(None, role))
                .unwrap();
            id
        };
        let linked = |name: &WorkspaceName| BuildVolumeRole::Linked {
            checkout: name.clone(),
        };
        let own = volume(linked(&topic));
        // A first touch the link names, registered and not yet finished: the next refresh
        // finishes it, so the link stays.
        let touching = BuildVolumeId::mint();
        fs::write(layout.image(&touching), b"").unwrap();
        layout
            .write_record(
                &touching,
                Registration::new(
                    BuildVolumeRecord::new(None, linked(&topic)),
                    Registrant::Workspace {
                        incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
                    },
                )
                .registered(),
            )
            .unwrap();
        let unregistered = BuildVolumeId::mint();
        fs::write(layout.image(&unregistered), b"").unwrap();
        let adopted = volume(linked(&WorkspaceName::main()));
        let collected = BuildVolumeId::mint();
        assert_eq!(
            layout.resolve_link(&topic, &own).unwrap(),
            LinkResolution::Keep
        );
        assert_eq!(
            layout.resolve_link(&topic, &touching).unwrap(),
            LinkResolution::Keep,
            "an unfinished first touch keeps the link its refresh resumes from"
        );
        let error = layout.resolve_link(&topic, &unregistered).unwrap_err();
        assert!(error.message.contains("no owner record"), "{error:?}");
        for stale in [&adopted, &collected] {
            assert_eq!(
                layout.resolve_link(&topic, stale).unwrap(),
                LinkResolution::Repoint(own.clone()),
                "{stale}"
            );
        }
        let orphan = WorkspaceName::new("orphan").unwrap();
        let error = layout.resolve_link(&orphan, &adopted).unwrap_err();
        assert!(error.message.contains("0 build volumes"), "{error:?}");

        // A `land --no-retire` that stopped after main adopted the orphan's volume and before
        // the orphan's refork: the orphan reforks from main's latest seed instead of refusing.
        let seed = |created_at: &str| {
            let id = BuildVolumeId::mint();
            fs::write(layout.image(&id), b"").unwrap();
            layout
                .write_record(
                    &id,
                    &BuildVolumeRecord {
                        created_at: created_at.to_owned(),
                        ..BuildVolumeRecord::new(
                            None,
                            BuildVolumeRole::Seed {
                                target: WorkspaceName::main(),
                                incarnation: crate::metadata::WorkspaceIncarnation::new(
                                    "0".repeat(32),
                                )
                                .unwrap(),
                            },
                        )
                    },
                )
                .unwrap();
            id
        };
        seed("2026-10-05T00:00:00Z");
        let latest = seed("2026-10-05T00:00:01Z");
        assert_eq!(
            layout.resolve_link(&orphan, &adopted).unwrap(),
            LinkResolution::Refork(latest.clone())
        );
        // A checkout that owns a volume is re-pointed at it, never reforked.
        assert_eq!(
            layout.resolve_link(&topic, &adopted).unwrap(),
            LinkResolution::Repoint(own.clone())
        );
        // A link to a seed never reforks: nobody adopted it from here.
        let error = layout.resolve_link(&orphan, &latest).unwrap_err();
        assert!(error.message.contains("0 build volumes"), "{error:?}");
        // A link to a volume whose image is gone names nothing anybody owns, whatever record it
        // left: what main's link named once a collection took the volume main had adopted.
        assert_eq!(
            layout.resolve_link(&orphan, &collected).unwrap(),
            LinkResolution::Vanished
        );
        let gone = WorkspaceName::new("gone").unwrap();
        let released = volume(linked(&gone));
        fs::remove_file(layout.image(&released)).unwrap();
        assert_eq!(
            layout.resolve_link(&gone, &released).unwrap(),
            LinkResolution::Vanished,
            "a record left beside no image does not keep the link"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// A job's hold on its build volume is free the moment its drop returns, even while a spawn
    /// of this process that copied the hold's descriptor has not run `exec` yet: the drop waits
    /// for that spawn (`fork_lock`). Unfenced, the parked child's copy keeps the shared lock, so
    /// a release finds the volume held by a job that ended -- what `held` saw after `wait` in
    /// the supervisor suite whenever another test was mid-spawn.
    ///
    /// No timer orders the race. The child is parked before the hold is dropped; the dropping
    /// thread either returns first, having closed its descriptor without waiting (unfenced), or
    /// is queued for the exclusive fork lock behind the parked spawn, which a reader then sees
    /// refused ([`cowshed_os::fork_lock::held_exclusive`]) -- only then is the child let go.
    /// Unfenced, a probe while the child is still parked sees its copy. Fenced, the release is
    /// proven once the child is reaped: Darwin reports a forked child's `exec` before `exec` has
    /// closed every close-on-exec copy (cowshed-os `fork_lock`), so a probe at the spawn's return
    /// can still find the copy, which made this test fail under gate load.
    #[test]
    fn a_dropped_hold_is_free_though_a_spawn_that_copied_it_is_in_flight() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-hold-{}",
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
        let id = BuildVolumeId::mint();
        fs::write(layout.image(&id), b"").unwrap();
        let hold = layout.admit(&id).unwrap();
        assert!(layout.held(&id).unwrap());

        let in_flight = cowshed_os::fork_lock::in_flight::InFlight::start();
        let (dropped, was_dropped) = std::sync::mpsc::channel();
        let release = std::thread::spawn(move || {
            drop(hold);
            dropped.send(()).unwrap();
        });
        let unfenced = loop {
            if was_dropped.try_recv().is_ok() {
                break true;
            }
            if cowshed_os::fork_lock::held_exclusive() {
                break false;
            }
            std::thread::yield_now();
        };
        // Only an unfenced drop returns while the child is parked, and its copy then keeps the hold.
        let held_beside_the_parked_child = unfenced && layout.held(&id).unwrap();
        // `finish` reaps the child, whose exit has closed every copy it held.
        in_flight.finish();
        if !unfenced {
            was_dropped.recv().unwrap();
        }
        release.join().unwrap();
        assert!(
            !held_beside_the_parked_child,
            "a hold dropped beside a spawn in flight was not fenced: the parked child keeps it held"
        );
        assert!(
            !layout.held(&id).unwrap(),
            "a released hold keeps nothing held once the spawn that copied it is done"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// A hold refuses only once a release has deleted the volume. Every other exclusive claim on
    /// the hold file keeps the volume: a seed capture, a reseed or cache clear, a dry-run probe,
    /// a release that reads the volume again under its claim and declines it. A land's hold
    /// behind one of those waits for it and then holds the volume; it never refuses
    /// it as being released. A hold that waited behind a release that did delete the volume
    /// refuses as released.
    ///
    /// No timer orders the race: the claim is let go only once the holding thread has either
    /// answered while it was still held (the false refusal) or sleeps in the kernel on the lock.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_hold_waits_out_an_exclusive_claim_and_refuses_only_a_released_volume() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-claimed-{}",
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
        let id = BuildVolumeId::mint();
        fs::write(layout.image(&id), b"").unwrap();

        let kept = hold_behind_claim(&layout, &id, || {});
        assert!(
            kept.is_ok(),
            "a hold behind a claim that keeps the volume holds it: {kept:?}"
        );
        drop(kept);

        let released = hold_behind_claim(&layout, &id, || {
            assert!(layout.release_image(&id).unwrap());
            fs::remove_file(layout.hold_path(&id)).unwrap();
        })
        .unwrap_err();
        assert_eq!(released.kind(), io::ErrorKind::ResourceBusy);
        assert_eq!(
            released.to_string(),
            format!("build volume {id} was released")
        );
        assert!(
            !layout.hold_path(&id).exists(),
            "a refused hold leaves no hold file"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// Claim `id` exclusively, start a hold of it on another thread, and let the claim go, after
    /// `settle`, once that hold has answered or sleeps in the kernel on the hold file's lock.
    #[cfg(target_os = "macos")]
    fn hold_behind_claim(
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        settle: impl FnOnce(),
    ) -> io::Result<BuildVolumeHold> {
        let claim = layout
            .claim_release(id)
            .unwrap()
            .expect("nothing holds the volume");
        let (port, ported) = std::sync::mpsc::channel();
        let (answer, answered) = std::sync::mpsc::channel();
        let holder = {
            let (layout, id) = (layout.clone(), id.clone());
            std::thread::spawn(move || {
                // SAFETY: names this thread's own Mach port and transfers no right.
                port.send(unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) })
                    .unwrap();
                answer
                    .send(layout.hold(&crate::census::this_test(), &id))
                    .unwrap();
            })
        };
        let port = ported.recv().unwrap();
        let early = loop {
            if let Ok(held) = answered.try_recv() {
                break Some(held);
            }
            if waits_in_kernel(port) {
                break None;
            }
            std::thread::yield_now();
        };
        let held = early.unwrap_or_else(|| {
            settle();
            drop(claim);
            answered.recv().unwrap()
        });
        holder.join().unwrap();
        held
    }

    /// Whether `thread` sleeps interruptibly in the kernel, as a flock waiter does. A thread
    /// that already ended answers no: it answered before it ended.
    #[cfg(target_os = "macos")]
    fn waits_in_kernel(thread: libc::mach_port_t) -> bool {
        let mut info = std::mem::MaybeUninit::<libc::thread_basic_info>::zeroed();
        let mut count = libc::THREAD_BASIC_INFO_COUNT;
        let flavor = libc::thread_flavor_t::try_from(libc::THREAD_BASIC_INFO).unwrap();
        // SAFETY: `info` has room for the `count` integers the basic flavor writes.
        let status =
            unsafe { libc::thread_info(thread, flavor, info.as_mut_ptr().cast(), &mut count) };
        // SAFETY: a successful call wrote the whole basic info.
        status == libc::KERN_SUCCESS
            && unsafe { info.assume_init() }.run_state == libc::TH_STATE_WAITING
    }

    #[test]
    fn release_commits_the_image_and_frees_nothing_before_its_owner_record_is_removed() {
        let root =
            std::env::temp_dir().join(format!("cowshed-build-unlink-{}", uuid::Uuid::new_v4()));
        let project = ProjectPaths::with_mount_root(
            root.join("store"),
            root.join("mounts"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        fs::create_dir_all(layout.images()).unwrap();
        let id = BuildVolumeId::mint();
        fs::write(layout.image(&id), vec![7_u8; 64 * 1024]).unwrap();
        fs::write(layout.record(&id), b"retained ownership").unwrap();

        assert!(layout.release_image(&id).unwrap());
        assert!(
            !layout.image(&id).exists(),
            "the commit takes the image off its name"
        );
        assert_eq!(
            fs::read(layout.released(&id)).unwrap(),
            vec![7_u8; 64 * 1024],
            "the release frees nothing before it answers"
        );
        assert_eq!(fs::read(layout.record(&id)).unwrap(), b"retained ownership");
        assert!(
            layout.release_image(&id).unwrap(),
            "a retry answers the commit already made"
        );
        fs::remove_file(layout.released(&id)).unwrap();
        assert!(
            !layout.release_image(&id).unwrap(),
            "with no image and no entry, nothing is released"
        );

        // A real refusal must preserve the ownership evidence and the image's name.
        fs::create_dir(layout.image(&id)).unwrap();
        fs::write(layout.image(&id).join("held"), b"not a regular image").unwrap();
        assert!(layout.release_image(&id).is_err());
        assert!(layout.image(&id).join("held").exists());
        assert_eq!(fs::read(layout.record(&id)).unwrap(), b"retained ownership");
        fs::remove_dir_all(&root).unwrap();
    }

    /// The listing at the gateway's start names every build image a project's deleting directory
    /// holds, and only build images: a stray is left and named, a link is never followed, and an
    /// unreadable directory is named rather than reported empty.
    #[test]
    fn the_store_pass_deletes_only_build_images_in_every_deleting_directory() {
        use crate::storage::apfs::release::{Left, Why};

        let root =
            std::env::temp_dir().join(format!("cowshed-build-trash-{}", uuid::Uuid::new_v4()));
        let store = root.join("store");
        let project = ProjectPaths::with_mount_root(
            &store,
            root.join("mounts"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        fs::create_dir_all(layout.deleting()).unwrap();
        let image = layout
            .deleting()
            .join(format!("{}.dmg", BuildVolumeId::mint()));
        fs::write(&image, vec![7_u8; 64 * 1024]).unwrap();
        let stray = layout.deleting().join("notes.txt");
        fs::write(&stray, b"not an image").unwrap();

        let listing = released_build_volumes(&store);
        assert_eq!(listing.entries, std::slice::from_ref(&image));
        assert_eq!(
            listing.left,
            [Left {
                path: stray.clone(),
                why: Why::NotImage
            }],
            "only build images are listed; a stray is named"
        );

        // A dangling entry is not a regular image and is never followed or listed.
        let refused = layout.released(&BuildVolumeId::mint());
        std::os::unix::fs::symlink(&stray, &refused).unwrap();
        let listing = released_build_volumes(&store);
        assert_eq!(listing.entries, [image]);
        assert!(
            listing.left.contains(&Left {
                path: refused.clone(),
                why: Why::NotImage
            }),
            "{listing:?}"
        );
        assert!(fs::symlink_metadata(&refused).is_ok());

        fs::remove_dir_all(layout.deleting()).unwrap();
        fs::write(layout.deleting(), b"not a directory").unwrap();
        let listing = released_build_volumes(&store);
        assert!(
            listing
                .left
                .iter()
                .any(|left| left.path == layout.deleting() && matches!(left.why, Why::Failed(_))),
            "an unreadable directory is named, not reported as nothing to release: {listing:?}"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// A detach frees exactly the build volumes whose record names the checkout as the one that
    /// links it: never a seed, a superseded volume, another checkout's, or one with no record.
    #[test]
    fn a_checkout_owns_the_volumes_its_records_name_it_linked_to() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-owned-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let project = ProjectPaths::with_mount_root(
            &root,
            root.join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        assert!(
            layout
                .owned_by(&WorkspaceName::new("topic").unwrap())
                .unwrap()
                .is_empty()
        );
        fs::create_dir_all(layout.images()).unwrap();
        let topic = WorkspaceName::new("topic").unwrap();
        let other = WorkspaceName::new("other").unwrap();
        let volume = |role: Option<BuildVolumeRole>| {
            let id = BuildVolumeId::mint();
            fs::write(layout.image(&id), b"").unwrap();
            if let Some(role) = role {
                layout
                    .write_record(&id, &BuildVolumeRecord::new(None, role))
                    .unwrap();
            }
            id
        };
        let linked = |name: &WorkspaceName| BuildVolumeRole::Linked {
            checkout: name.clone(),
        };
        let own = volume(Some(linked(&topic)));
        let interrupted = volume(Some(linked(&topic)));
        volume(Some(linked(&other)));
        volume(Some(BuildVolumeRole::Superseded {
            owner: topic.clone(),
            incarnation: WorkspaceIncarnation::new("00000000000000000000000000000001").unwrap(),
        }));
        volume(Some(BuildVolumeRole::Seed {
            target: topic.clone(),
            incarnation: WorkspaceIncarnation::new("00000000000000000000000000000001").unwrap(),
        }));
        volume(None);
        let mut owned = layout.owned_by(&topic).unwrap();
        owned.sort();
        let mut expected = vec![own, interrupted];
        expected.sort();
        assert_eq!(owned, expected);
        assert_eq!(layout.owned_by(&other).unwrap().len(), 1);
        fs::remove_dir_all(&root).unwrap();
    }

    /// A JavaScript cache is its own tool: a displaced one is never reported as Cargo's, and no
    /// Cargo lock walk reaches into it.
    #[test]
    fn javascript_caches_are_their_own_tool_never_cargo() {
        use crate::workspace_environment::{
            NODE_COMPILE_CACHE_CHECKOUT, NODE_COMPILE_CACHE_DIRECTORY,
        };
        let path = |checkout: &str, volume: &str| BuildStatePath::new(checkout, volume).unwrap();
        let compile_cache = path(NODE_COMPILE_CACHE_CHECKOUT, NODE_COMPILE_CACHE_DIRECTORY);
        for javascript in [
            path("node_modules/.cache", "node_modules/.cache"),
            path(
                "packages/app/node_modules/.cache",
                "packages/app/node_modules/.cache",
            ),
            compile_cache.clone(),
        ] {
            assert_eq!(
                BuildStateTool::of(&javascript),
                BuildStateTool::JavaScript,
                "{javascript:?}"
            );
        }
        assert_eq!(
            BuildStateTool::of(&path("target", "target")),
            BuildStateTool::Cargo
        );
        let state = BuildVolumeState {
            paths: vec![path("target", "target"), compile_cache],
            fingerprint: None,
            mbx: None,
        };
        assert_eq!(
            state.cargo_targets().collect::<Vec<_>>(),
            [Path::new("target")]
        );
        let finding = DiscardedBuildStateFinding {
            path: PathBuf::from(NODE_COMPILE_CACHE_CHECKOUT),
            tool: BuildStateTool::JavaScript,
            origin: DiscardOrigin::Displaced,
        };
        assert!(finding.to_string().contains("Node or Bun"), "{finding}");
        assert_eq!(
            serde_json::to_value(BuildStateTool::JavaScript).unwrap(),
            "javascript"
        );
    }
}
