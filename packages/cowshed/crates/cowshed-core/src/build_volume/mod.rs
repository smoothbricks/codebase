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
#[cfg(target_os = "macos")]
pub mod carry;
pub mod discard;
pub mod link;
#[cfg(target_os = "macos")]
pub mod migrate;
pub mod nx;
#[cfg(target_os = "macos")]
pub(crate) mod sqlite;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::api::dto::GitOid;
use crate::capabilities::BuildStatePath;
use crate::metadata::{MetadataError, WorkspaceIncarnation, WorkspaceName, read_json, write_json};
use crate::repository::ProjectPaths;
use crate::storage::StorageLayoutError;
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
/// Where a released volume's image waits to be deleted: `build/.trash/<id>.dmg`, inside the
/// images directory and so on its volume, so moving an image there is one `rename(2)`
/// (16_build_volumes.md, "Garbage collection"). Deleting a written image frees its whole extent
/// tree, which took minutes inside a land (measured 2026-10-21); the move takes none of that.
pub(crate) const TRASH_DIRECTORY: &str = ".trash";
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
    /// The tree the linking checkout had checked out when cowshed placed this volume there: a
    /// fork's tree at its fork, the landed tree at an adoption. `tree` says what was built into
    /// the volume; this says where it was put. `None` on a record that predates it, or for a
    /// placement at no tree cowshed knows (16_build_volumes.md, "Which volume a seed holds").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_out: Option<GitOid>,
}

impl BuildVolumeRecord {
    pub fn new(tree: Option<GitOid>, role: BuildVolumeRole) -> Self {
        Self {
            version: RECORD_VERSION,
            tree,
            role,
            created_at: crate::storage::deletion_log::rfc3339_utc(std::time::SystemTime::now()),
            checked_out: None,
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
    /// `.cowshed.toml` `[build] state`: a tool no capability detects.
    Declared,
}

impl BuildStateTool {
    /// The tool whose state `path` is: declared state by its own volume namespace, Nx's
    /// directories under `.nx`, and otherwise a Cargo target directory.
    pub fn of(path: &BuildStatePath) -> Self {
        let checkout = path.checkout.as_path();
        if path.is_declared() {
            Self::Declared
        } else if checkout
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == ".nx")
        {
            Self::Nx
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
            BuildStateTool::Declared => {
                return write!(
                    formatter,
                    "discarded the real directory {} that .cowshed.toml [build] state declares \
                     and linked it onto the build volume; declared state is rebuildable, so \
                     re-run whatever reconstructs it once, and from then on it lives on the \
                     volume and every fork inherits it warm",
                    self.path.display()
                );
            }
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
    /// "Garbage collection"), naming the workspace its sidecar `recorded` (the checkout that linked
    /// it, the target whose seed it was), or nobody for one recorded unlinked or with no readable
    /// sidecar. A release that deleted the image writes `release-build-volume` for the image; one
    /// that found the image already gone and deleted the record writes
    /// `release-imageless-build-volume` for the record, naming the missing image.
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
            Some(BuildVolumeRole::Linked { checkout }) => {
                (DeletionKind::BuildVolume, checkout.as_str())
            }
            Some(BuildVolumeRole::Unlinked) | None => (DeletionKind::BuildVolume, ""),
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

    /// The project's build-volume trash: released images waiting to be deleted
    /// ([`empty_trash`]).
    pub fn trash(&self) -> PathBuf {
        self.images.join(TRASH_DIRECTORY)
    }

    /// Where `id`'s image goes when it is released ([`Self::trash_image`]).
    pub fn trashed_image(&self, id: &BuildVolumeId) -> PathBuf {
        self.trash().join(format!("{id}.{IMAGE_EXTENSION}"))
    }

    /// Move `id`'s image into the trash, if it is there: `true` when it was. One `rename(2)` on
    /// the images directory's own volume, which never copies: a rename the kernel refuses across
    /// volumes (`EXDEV`) is returned as the error it is, never redone as a copy. The canonical
    /// image is gone once this returns, which is what a release promises a later hold.
    pub fn trash_image(&self, id: &BuildVolumeId) -> io::Result<bool> {
        use std::os::unix::fs::DirBuilderExt as _;
        let trash = self.trash();
        match fs::DirBuilder::new().mode(0o700).create(&trash) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        match fs::rename(self.image(id), self.trashed_image(id)) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
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

    /// Hold `id` for a land or a carry, for as long as the answer lives: while any hold lives, no
    /// release unmounts the volume (16_build_volumes.md, "Garbage collection"). An exclusive claim
    /// on the volume (a release, a seed capture, a reseed, a cache clear, a dry-run probe) is waited
    /// out, because only what it leaves says whether the volume is still there. Refused with
    /// [`io::ErrorKind::ResourceBusy`] only once a release has deleted the volume: a release deletes
    /// the image before it lets go of its claim, so a hold that waited for it finds no image, and
    /// the hold file it made is removed again. Every claimant takes its claim without waiting, so
    /// this waits for the claimant's own work, never for another hold.
    pub fn hold(&self, id: &BuildVolumeId) -> io::Result<BuildVolumeHold> {
        let path = self.hold_path(id);
        let file = open_hold(&path)?;
        loop {
            match file.lock_shared() {
                Ok(()) => break,
                // A signal woke the wait; the claim it waits for is unchanged.
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
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

    /// Hold `id` for the operation creating it, before its image exists: a clone holds the
    /// volume it makes until its record is written, so no collection that listed the image in
    /// between releases it as an interrupted creation. Nothing claims an id before its image
    /// exists, so the lock is never contended; one that is refuses as
    /// [`io::ErrorKind::ResourceBusy`].
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

    /// Claim build volume `id` by the hold file beside its actual image, in whatever format it
    /// was written (`<image>.hold`). Re-adoption and payload-absent legacy seed metadata GC use
    /// this same claim; a pre-RAW job holds this file, not the RAW name. `None` while held.
    #[cfg(target_os = "macos")]
    pub(crate) fn claim_re_adopt_source(
        &self,
        id: &BuildVolumeId,
        image: &Path,
    ) -> io::Result<Option<ReleaseClaim>> {
        let path = self.source_hold_path(id, image)?;
        let file = open_hold(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(ReleaseClaim {
                id: id.clone(),
                _file: file,
            })),
            Err(fs::TryLockError::WouldBlock) => Ok(None),
            Err(fs::TryLockError::Error(error)) => Err(error),
        }
    }

    #[cfg(target_os = "macos")]
    /// The same actual-image hold probed for preview, without creating a missing hold file.
    pub(crate) fn held_re_adopt_source(
        &self,
        id: &BuildVolumeId,
        image: &Path,
    ) -> io::Result<bool> {
        hold_is_held(&self.source_hold_path(id, image)?)
    }

    #[cfg(target_os = "macos")]
    fn source_hold_path(&self, id: &BuildVolumeId, image: &Path) -> io::Result<PathBuf> {
        if image.parent() != Some(self.images.as_path())
            || image.file_stem().and_then(|stem| stem.to_str()) != Some(id.as_str())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{} is not build volume {} in {}",
                    image.display(),
                    id.as_str(),
                    self.images.display(),
                ),
            ));
        }
        Ok(crate::metadata::append_suffix(image, HOLD_SUFFIX))
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

    /// Every published build volume: each `<id>.dmg` directly in the images directory.
    pub fn list(&self) -> crate::Result<Vec<BuildVolumeId>> {
        self.list_entries(false)
    }

    /// GC's snapshot includes sidecars as well as images; seed selection never does.
    pub fn collection_ids(&self) -> crate::Result<Vec<BuildVolumeId>> {
        self.list_entries(true)
    }

    fn list_entries(&self, include_records: bool) -> crate::Result<Vec<BuildVolumeId>> {
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
    /// recorded as linked by `checkout`, or has no record yet (a migration or fork in progress,
    /// whose record is written last). Otherwise the link is stale: a restored checkpoint carries
    /// the link it had when taken, and an interrupted land can leave a target naming the
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
        // A creation makes the image before anything links it and a release deletes the image
        // first, so a link to a missing image names a volume that no longer exists: its record,
        // when one is left, speaks for nothing.
        let vanished = !self.image(linked).exists();
        let adopter = match self.read_record_present(linked)? {
            _ if vanished => None,
            Some(record) if owns(&record) => return Ok(LinkResolution::Keep),
            None => return Ok(LinkResolution::Keep),
            Some(BuildVolumeRecord {
                role: BuildVolumeRole::Linked { checkout: adopter },
                ..
            }) => Some(adopter),
            Some(_) => None,
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

/// One job's claim on the build volume it was admitted with ([`BuildVolumeLayout::admit`]), or a
/// land's or carry's ([`BuildVolumeLayout::hold`]): a
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

/// What emptying a build-volume trash deleted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EmptiedTrash {
    pub images: usize,
    /// The bytes those images held on disk, read before each was deleted.
    pub bytes: u64,
}

/// Delete every image in the build-volume trash at `trash` (`<project>/build/.trash`), one at
/// a time, at background disk priority on the calling thread, so the extent walk each deletion
/// costs yields to every other reader on the volume. A trash that does not exist is empty.
/// Only image files of the trash's own naming are deleted; anything else there is left and said.
pub fn empty_trash(trash: &Path) -> io::Result<EmptiedTrash> {
    let entries = match fs::read_dir(trash) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(EmptiedTrash::default());
        }
        Err(error) => return Err(error),
    };
    let _background = BackgroundDiskPriority::set();
    let mut emptied = EmptiedTrash::default();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let image = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(&format!(".{IMAGE_EXTENSION}")))
            .and_then(BuildVolumeId::parse)
            .is_some();
        let metadata = entry.metadata()?;
        if !image || !metadata.is_file() {
            cowshed_os::eprintln!(
                "cowshed: {} is not a trashed build-volume image; it is left",
                path.display()
            );
            continue;
        }
        let bytes = {
            use std::os::unix::fs::MetadataExt as _;
            metadata.blocks() * 512
        };
        match fs::remove_file(&path) {
            Ok(()) => {
                emptied.images += 1;
                emptied.bytes += bytes;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(emptied)
}

/// [`empty_trash`] for every project in the store at `store`: `<store>/<owner>/<repo>/build/.trash`.
/// The daemon runs this at its start and then on an interval, so a released image waits for at
/// most one interval, and for the next daemon start when no daemon runs; `cowshed gc` empties its
/// own project's trash too. A project whose trash cannot be emptied is said and skipped.
pub fn empty_store_trash(store: &Path) -> EmptiedTrash {
    let mut emptied = EmptiedTrash::default();
    let owners = match fs::read_dir(store) {
        Ok(owners) => owners,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: cannot list the store {} for build-volume trash: {error}",
                store.display()
            );
            return emptied;
        }
    };
    for owner in owners.flatten() {
        let Ok(repos) = fs::read_dir(owner.path()) else {
            continue;
        };
        for repo in repos.flatten() {
            let trash = repo.path().join(IMAGES_DIRECTORY).join(TRASH_DIRECTORY);
            match empty_trash(&trash) {
                Ok(project) => {
                    emptied.images += project.images;
                    emptied.bytes += project.bytes;
                }
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: cannot empty the build-volume trash {}: {error}",
                    trash.display()
                ),
            }
        }
    }
    emptied
}

/// This thread's disk I/O at background priority (`IOPOL_THROTTLE`) until dropped. Elsewhere
/// than macOS the scheduler has no such policy, and this changes nothing.
struct BackgroundDiskPriority {
    #[cfg(target_os = "macos")]
    previous: libc::c_int,
}

#[cfg(target_os = "macos")]
mod iopolicy {
    // <sys/resource.h>: not in the `libc` crate.
    pub const IOPOL_TYPE_DISK: libc::c_int = 0;
    pub const IOPOL_SCOPE_THREAD: libc::c_int = 1;
    pub const IOPOL_THROTTLE: libc::c_int = 3;
    unsafe extern "C" {
        pub fn getiopolicy_np(iotype: libc::c_int, scope: libc::c_int) -> libc::c_int;
        pub fn setiopolicy_np(
            iotype: libc::c_int,
            scope: libc::c_int,
            policy: libc::c_int,
        ) -> libc::c_int;
    }
}

impl BackgroundDiskPriority {
    fn set() -> Self {
        #[cfg(target_os = "macos")]
        {
            use iopolicy::*;
            // SAFETY: both calls take plain integers and change only this thread's policy.
            let previous = unsafe { getiopolicy_np(IOPOL_TYPE_DISK, IOPOL_SCOPE_THREAD) };
            if unsafe { setiopolicy_np(IOPOL_TYPE_DISK, IOPOL_SCOPE_THREAD, IOPOL_THROTTLE) } != 0 {
                cowshed_os::eprintln!(
                    "cowshed: build-volume trash is emptied at normal disk priority: {}",
                    io::Error::last_os_error()
                );
            }
            Self { previous }
        }
        #[cfg(not(target_os = "macos"))]
        Self {}
    }
}

impl Drop for BackgroundDiskPriority {
    fn drop(&mut self) {
        #[cfg(target_os = "macos")]
        if self.previous >= 0 {
            use iopolicy::*;
            // SAFETY: as in `set`.
            unsafe { setiopolicy_np(IOPOL_TYPE_DISK, IOPOL_SCOPE_THREAD, self.previous) };
        }
    }
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
    /// thread either answers first, having closed its descriptor without waiting (unfenced), or
    /// is queued for the exclusive fork lock behind the parked spawn, which a reader then sees
    /// refused ([`cowshed_os::fork_lock::held_exclusive`]) -- only then is the child let go.
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
        let (answer, answered) = std::sync::mpsc::channel();
        let release = {
            let (layout, id) = (layout.clone(), id.clone());
            std::thread::spawn(move || {
                drop(hold);
                answer.send(layout.held(&id).unwrap()).unwrap();
            })
        };
        let early = loop {
            if let Ok(held) = answered.try_recv() {
                break Some(held);
            }
            if cowshed_os::fork_lock::held_exclusive() {
                break None;
            }
            std::thread::yield_now();
        };
        in_flight.finish();
        let held = early.unwrap_or_else(|| answered.recv().unwrap());
        release.join().unwrap();
        assert!(
            !held,
            "a released hold keeps nothing held, whatever spawn copied it"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// A hold refuses only once a release has deleted the volume. Every other exclusive claim on
    /// the hold file keeps the volume: a seed capture, a reseed or cache clear, a dry-run probe,
    /// a release that reads the volume again under its claim and declines it. A land's or a
    /// carry's hold behind one of those waits for it and then holds the volume; it never refuses
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
            assert!(layout.trash_image(&id).unwrap());
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
                answer.send(layout.hold(&id)).unwrap();
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

    /// A release moves the image into the trash in one rename (the same inode, no copy), the
    /// canonical image is gone at once, and emptying the store's trash deletes exactly the trashed
    /// images: a file of any other name there is left.
    #[test]
    fn a_released_image_waits_in_the_trash_and_only_trashed_images_are_deleted() {
        use std::os::unix::fs::MetadataExt as _;

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
        fs::create_dir_all(layout.images()).unwrap();
        let id = BuildVolumeId::mint();
        fs::write(layout.image(&id), vec![7_u8; 64 * 1024]).unwrap();
        let inode = fs::metadata(layout.image(&id)).unwrap().ino();

        assert!(
            layout.trash_image(&id).unwrap(),
            "the image was there to move"
        );
        assert!(!layout.image(&id).exists(), "the canonical image is gone");
        assert_eq!(
            fs::metadata(layout.trashed_image(&id)).unwrap().ino(),
            inode
        );
        assert!(
            !layout.trash_image(&id).unwrap(),
            "a second release finds nothing"
        );
        assert_eq!(
            layout.list_entries(true).unwrap(),
            Vec::<BuildVolumeId>::new(),
            "a trashed image is no volume"
        );
        let stray = layout.trash().join("notes.txt");
        fs::write(&stray, b"not an image").unwrap();

        let emptied = empty_store_trash(&store);
        assert_eq!(emptied.images, 1);
        assert!(emptied.bytes > 0);
        assert!(!layout.trashed_image(&id).exists());
        assert!(stray.exists(), "only trashed images are deleted");
        assert_eq!(empty_store_trash(&store), EmptiedTrash::default());
        fs::remove_dir_all(&root).unwrap();
    }
}
