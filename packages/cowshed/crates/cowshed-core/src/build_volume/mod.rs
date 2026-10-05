//! Build volumes (16_build_volumes.md): the build tools' own incremental state — Cargo target
//! directories, Nx's cache and task database, a per-tree indexer — on a volume of its own beside
//! a workspace's source image.
//!
//! A build volume is an ASIF image at `<project>/build/<id>.asif` with its record at
//! `<id>.asif.json`, mounted `nobrowse` at `<host mount root>/.build/<owner>/<repo>/<id>`. A
//! checkout reaches it through exactly one symlink, [`BUILD_LINK`], whose target is that
//! mountpoint ([`link`]). The mountpoint names the volume, so the link is the whole record of
//! which checkout uses which volume: garbage collection reads links, never derives ownership
//! from the record.
//!
//! The record says what the link cannot: the tree the volume was last built at, whether it is a
//! seed (a target's frozen build state, never written), and when it was made.

pub mod cargo;
pub mod discard;
pub mod link;
#[cfg(target_os = "macos")]
pub mod migrate;
pub mod nx;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::api::dto::GitOid;
use crate::capabilities::BuildStatePath;
use crate::metadata::{
    IMAGE_EXTENSION, MetadataError, WorkspaceIncarnation, WorkspaceName, read_json, write_json,
};
use crate::repository::ProjectPaths;
use crate::storage::StorageLayoutError;

pub use link::BUILD_LINK;

/// The project's build-volume images, beside `sessions/`.
const IMAGES_DIRECTORY: &str = "build";
/// Every project's build-volume mountpoints, beneath the host mount root and outside every
/// workspace mount: `<host mount root>/.build/<owner>/<repo>/<id>`.
const MOUNTS_DIRECTORY: &str = ".build";
/// The record beside each image: `<id>.asif.json`.
const RECORD_SUFFIX: &str = ".json";
/// The volume's own statement of the build-state paths it holds, at its root. It travels with
/// every clone, so a fork knows what its source linked without rediscovering it.
pub const STATE_FILE: &str = "cowshed-build-state.json";
const RECORD_VERSION: u32 = 1;
/// The capacity of a build volume created from nothing, unless `.cowshed.toml` `[build] capacity`
/// says otherwise (16_build_volumes.md, "Substrate"). Sparse: a cap on build-cache growth, not an
/// allocation.
pub const DEFAULT_BUILD_VOLUME_CAPACITY: crate::metadata::ImageCapacity =
    crate::metadata::ImageCapacity::from_gibibytes(100);

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

/// What a build volume is to the project.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum BuildVolumeRole {
    /// The live volume of `checkout`, which links it.
    Linked { checkout: WorkspaceName },
    /// A live volume no checkout took over: its checkout retired, or a target adopted another.
    Unlinked,
    /// The latest seed of the target `target` at `incarnation`: frozen, never mounted, never
    /// written; only cloned.
    Seed {
        target: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    },
}

/// The record beside a build volume's image.
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
}

impl BuildVolumeRecord {
    pub fn new(tree: Option<GitOid>, role: BuildVolumeRole) -> Self {
        Self {
            version: RECORD_VERSION,
            tree,
            role,
            created_at: crate::storage::deletion_log::rfc3339_utc(std::time::SystemTime::now()),
        }
    }

    pub fn is_seed_of(&self, target: &WorkspaceName, incarnation: &WorkspaceIncarnation) -> bool {
        matches!(&self.role, BuildVolumeRole::Seed { target: seeded, incarnation: at }
            if seeded == target && at == incarnation)
    }
}

/// The build-state paths a volume holds, as written at its root ([`STATE_FILE`]), and the
/// fingerprint of the tracked build inputs they were discovered from
/// (`capabilities::tracked_manifest_fingerprint`): while it matches, nothing is rediscovered.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildVolumeState {
    pub paths: Vec<BuildStatePath>,
    pub fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateWire {
    version: u32,
    paths: Vec<PathWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PathWire {
    checkout: String,
    volume: String,
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
                })
                .collect(),
            fingerprint: self.fingerprint.clone(),
        };
        write_json(&path, &wire).map_err(|error| record_error(&path, &error))
    }

    /// This state with each `discovered` path it does not hold yet added, recorded at
    /// `fingerprint`, and the paths added. A path already held keeps its volume name: links are
    /// fixed once made (16_build_volumes.md, "One link per checkout"). A new path that overlaps
    /// a held one is refused, naming both.
    pub fn with_discovered(
        &self,
        discovered: &[BuildStatePath],
        fingerprint: String,
        volume_root: &Path,
    ) -> crate::Result<(Self, Vec<BuildStatePath>)> {
        let added: Vec<BuildStatePath> = discovered
            .iter()
            .filter(|path| {
                !self
                    .paths
                    .iter()
                    .any(|held| held.checkout.as_path() == path.checkout.as_path())
            })
            .cloned()
            .collect();
        let mut paths = self.paths.clone();
        paths.extend(added.iter().cloned());
        disjoint(&paths)
            .map_err(|(left, right)| overlapping(&volume_root.join(STATE_FILE), left, right))?;
        Ok((
            Self {
                paths,
                fingerprint: Some(fingerprint),
            },
            added,
        ))
    }

    /// Every Nx `workspace-data` directory the volume holds, relative to its root: where Nx keeps
    /// its task database and its daemon's record (`d/`).
    pub fn nx_workspace_data(&self) -> impl Iterator<Item = &Path> {
        self.paths.iter().filter_map(|state| {
            let checkout = state.checkout.as_path();
            (checkout.file_name() == Some(std::ffi::OsStr::new("workspace-data"))
                && checkout
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == ".nx"))
            .then(|| state.volume.as_path())
        })
    }

    /// Every Nx cache directory the volume holds, relative to its root.
    pub fn nx_cache(&self) -> impl Iterator<Item = &Path> {
        self.paths.iter().filter_map(|state| {
            let checkout = state.checkout.as_path();
            (checkout.file_name() == Some(std::ffi::OsStr::new("cache"))
                && checkout
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == ".nx"))
            .then(|| state.volume.as_path())
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
    BuildStatePath::new(&wire.checkout, &wire.volume)
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
    Codegraph,
}

impl BuildStateTool {
    /// The tool whose state `path` is: Nx's directories under `.nx`, the indexer's
    /// `.codegraph`, and otherwise a Cargo target directory, the only other contribution.
    pub fn of(path: &BuildStatePath) -> Self {
        let checkout = path.checkout.as_path();
        if checkout
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == ".nx")
        {
            Self::Nx
        } else if checkout
            .file_name()
            .is_some_and(|name| name == ".codegraph")
        {
            Self::Codegraph
        } else {
            Self::Cargo
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DisplacedBuildStateFinding {
    pub path: PathBuf,
    pub likely_tool: BuildStateTool,
}

impl std::fmt::Display for DisplacedBuildStateFinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tool = match self.likely_tool {
            BuildStateTool::Cargo => "cargo clean or another Cargo command",
            BuildStateTool::Nx => "an Nx reset or cache command",
            BuildStateTool::Codegraph => "the indexer",
        };
        write!(
            formatter,
            "discarded the real build-state directory {} and restored its volume link; {tool} may have displaced it; the next build rebuilds what is missing",
            self.path.display()
        )
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

/// What refreshing a checkout's build state did (16_build_volumes.md, "One link per checkout"):
/// the volume it links afterwards, whether that volume was created now (the checkout's first
/// touch), the build-state paths linked for the first time, the real directories a tool left
/// where a link belongs, and what discovery could not decide.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildStateRefresh {
    pub volume: Option<BuildVolumeId>,
    pub created: bool,
    /// Checkout-relative.
    pub added: Vec<PathBuf>,
    pub displaced: Vec<DisplacedBuildStateFinding>,
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
}

/// Where one project's build volumes live.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuildVolumeLayout {
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
            images: project.project_root.join(IMAGES_DIRECTORY),
            mounts: project.host_mount_root.join(MOUNTS_DIRECTORY).join(encoded),
        })
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

    pub fn record(&self, id: &BuildVolumeId) -> PathBuf {
        self.images
            .join(format!("{id}.{IMAGE_EXTENSION}{RECORD_SUFFIX}"))
    }

    pub fn mount(&self, id: &BuildVolumeId) -> PathBuf {
        self.mounts.join(id.as_str())
    }

    /// The volume a build link's target names, or `None` when the target is not one of this
    /// project's build mountpoints.
    pub fn volume_at(&self, target: &Path) -> Option<BuildVolumeId> {
        if target.parent()? != self.mounts {
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
                    "{} names {}, which is not one of this project's build volumes",
                    checkout.join(link::BUILD_LINK).display(),
                    target.display(),
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

    /// The record of `id`, or `None` when it has none (an unpublished creation). Every other
    /// failure to read it is an error: an unreadable record says nothing about the volume.
    pub fn read_record_present(
        &self,
        id: &BuildVolumeId,
    ) -> crate::Result<Option<BuildVolumeRecord>> {
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

    /// Every published build volume: each `<id>.asif` directly in the images directory.
    pub fn list(&self) -> crate::Result<Vec<BuildVolumeId>> {
        let entries = match fs::read_dir(&self.images) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_error("list build volumes", &self.images, &error)),
        };
        let mut ids = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|error| io_error("list build volumes", &self.images, &error))?;
            let name = entry.file_name();
            let Some(stem) = name
                .to_str()
                .and_then(|name| name.strip_suffix(&format!(".{IMAGE_EXTENSION}")))
            else {
                continue;
            };
            if let Some(id) = BuildVolumeId::parse(stem) {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids)
    }

    /// The volume `checkout`'s build link should name, given that it names `linked`
    /// (16_build_volumes.md, "One link per checkout"). The link is kept when its volume is
    /// recorded as linked by `checkout`, or has no record yet (a migration or fork in progress,
    /// whose record is written last). Otherwise the link is stale: a restored checkpoint carries
    /// the link it had when taken, and an interrupted land can leave a target naming the
    /// landing volume before its record moved. It is then re-pointed at the one volume recorded
    /// as `checkout`'s. When `checkout` owns none and `linked` is another checkout's live
    /// volume, a target adopted it from this checkout before a `land --no-retire` reforked it:
    /// the checkout reforks from that target's latest seed. Anything else refuses, so a checkout
    /// never writes a volume another checkout, a target or a seed owns.
    pub fn resolve_link(
        &self,
        checkout: &WorkspaceName,
        linked: &BuildVolumeId,
    ) -> crate::Result<LinkResolution> {
        let owns = |record: &BuildVolumeRecord| matches!(&record.role, BuildVolumeRole::Linked { checkout: owner } if owner == checkout);
        let adopter = match self.read_record_present(linked)? {
            Some(record) if owns(&record) => return Ok(LinkResolution::Keep),
            None if self.image(linked).exists() => return Ok(LinkResolution::Keep),
            Some(BuildVolumeRecord {
                role: BuildVolumeRole::Linked { checkout: adopter },
                ..
            }) => Some(adopter),
            _ => None,
        };
        let mut owned = Vec::new();
        let mut adopter_seeds = Vec::new();
        for id in self.list()? {
            match self.read_record_present(&id)? {
                Some(record) if owns(&record) => owned.push(id),
                Some(BuildVolumeRecord {
                    role: BuildVolumeRole::Seed { target, .. },
                    created_at,
                    ..
                }) if adopter.as_ref() == Some(&target) => adopter_seeds.push((created_at, id)),
                _ => {}
            }
        }
        if owned.is_empty()
            && let Some((_, seed)) = adopter_seeds.into_iter().max()
        {
            return Ok(LinkResolution::Refork(seed));
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
    /// interrupted reseed: the newer is the latest.
    pub fn seed_of(
        &self,
        target: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> crate::Result<Option<(BuildVolumeId, BuildVolumeRecord)>> {
        let mut seeds = Vec::new();
        for id in self.list()? {
            // An image without its record is an unpublished creation: never a seed.
            let Some(record) = self.read_record_present(&id)? else {
                continue;
            };
            if record.is_seed_of(target, incarnation) {
                seeds.push((id, record));
            }
        }
        seeds.sort_by(|left, right| left.1.created_at.cmp(&right.1.created_at));
        Ok(seeds.pop())
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
            Path::new("/store/acme/widget/build/0123456789abcdef0123456789abcdef.asif")
        );
        assert_eq!(
            layout.record(&id),
            Path::new("/store/acme/widget/build/0123456789abcdef0123456789abcdef.asif.json")
        );
        assert_eq!(
            layout.mount(&id),
            Path::new(
                "/Users/tester/Dev/.cowshed/.build/acme/widget/0123456789abcdef0123456789abcdef"
            )
        );
        assert_eq!(layout.volume_at(&layout.mount(&id)), Some(id.clone()));
        assert_eq!(
            layout.volume_at(Path::new(
                "/Users/tester/Dev/.cowshed/.build/acme/other/0123456789abcdef0123456789abcdef"
            )),
            None
        );
        assert_eq!(
            layout.volume_at(&layout.mount(&id).join("target")),
            None,
            "a path inside a volume is not a volume"
        );
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
    }

    #[test]
    fn the_latest_seed_of_a_target_wins_and_other_targets_are_invisible() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-seeds-{}",
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
        let main = WorkspaceName::main();
        let incarnation = WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef").unwrap();
        let seed = |created_at: &str, target: &WorkspaceName| {
            let id = BuildVolumeId::mint();
            fs::write(layout.image(&id), b"").unwrap();
            let mut record = BuildVolumeRecord::new(
                None,
                BuildVolumeRole::Seed {
                    target: target.clone(),
                    incarnation: incarnation.clone(),
                },
            );
            record.created_at = created_at.to_owned();
            layout.write_record(&id, &record).unwrap();
            id
        };
        assert_eq!(layout.seed_of(&main, &incarnation).unwrap(), None);
        seed("2026-10-05T00:00:00Z", &main);
        let latest = seed("2026-10-05T00:00:01Z", &main);
        seed("2026-10-05T00:00:02Z", &WorkspaceName::new("lane").unwrap());
        // An image whose record was never published is no seed of anyone.
        fs::write(layout.image(&BuildVolumeId::mint()), b"").unwrap();
        assert_eq!(
            layout
                .seed_of(&main, &incarnation)
                .unwrap()
                .map(|(id, _)| id),
            Some(latest)
        );
        fs::remove_dir_all(&root).unwrap();
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
        };
        let discovered = [
            BuildStatePath::new("target", "elsewhere").unwrap(),
            BuildStatePath::new("tools/x/target", "tools-x-target").unwrap(),
        ];
        let (next, added) = state
            .with_discovered(&discovered, "new".to_owned(), Path::new("/v"))
            .unwrap();
        assert_eq!(added, [discovered[1].clone()]);
        assert_eq!(next.paths, [state.paths[0].clone(), discovered[1].clone()]);
        assert_eq!(next.fingerprint.as_deref(), Some("new"));
        let inside = [BuildStatePath::new("target/sub", "sub").unwrap()];
        let error = state
            .with_discovered(&inside, "new".to_owned(), Path::new("/v"))
            .unwrap_err();
        assert!(error.message.contains("target/sub"), "{error:?}");
    }

    /// A restored checkpoint, or a land interrupted between moving a target's link and its
    /// records, leaves a link naming a volume the checkout does not own: mounting re-points it at
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
        let linked = |name: &WorkspaceName| {
            Some(BuildVolumeRole::Linked {
                checkout: name.clone(),
            })
        };
        let own = volume(linked(&topic));
        let unrecorded = volume(None);
        let adopted = volume(linked(&WorkspaceName::main()));
        let collected = BuildVolumeId::mint();
        assert_eq!(
            layout.resolve_link(&topic, &own).unwrap(),
            LinkResolution::Keep
        );
        assert_eq!(
            layout.resolve_link(&topic, &unrecorded).unwrap(),
            LinkResolution::Keep,
            "a creation in progress keeps the link that protects it"
        );
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
            let id = volume(None);
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
            LinkResolution::Refork(latest)
        );
        // A checkout that owns a volume is re-pointed at it, never reforked.
        assert_eq!(
            layout.resolve_link(&topic, &adopted).unwrap(),
            LinkResolution::Repoint(own.clone())
        );
        // A link to a seed or a collected volume never reforks: nobody adopted it from here.
        let error = layout.resolve_link(&orphan, &collected).unwrap_err();
        assert!(error.message.contains("0 build volumes"), "{error:?}");
        fs::remove_dir_all(&root).unwrap();
    }
}
