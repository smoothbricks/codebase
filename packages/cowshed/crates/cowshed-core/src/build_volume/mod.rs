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

pub mod link;
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
/// Unpublished images: created here, renamed into [`IMAGES_DIRECTORY`] only once complete.
const STAGING_DIRECTORY: &str = ".staging";
/// The record beside each image: `<id>.asif.json`.
const RECORD_SUFFIX: &str = ".json";
/// The volume's own statement of the build-state paths it holds, at its root. It travels with
/// every clone, so a fork knows what its source linked without rediscovering it.
pub const STATE_FILE: &str = "cowshed-build-state.json";
const RECORD_VERSION: u32 = 1;

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

/// The build-state paths a volume holds, as written at its root ([`STATE_FILE`]).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BuildVolumeState {
    pub paths: Vec<BuildStatePath>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StateWire {
    version: u32,
    paths: Vec<PathWire>,
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
        let path = volume_root.join(STATE_FILE);
        let wire = match read_json::<StateWire>(&path) {
            Ok(wire) => wire,
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(record_error(&path, &error)),
        };
        let paths = wire
            .paths
            .iter()
            .map(|path| path_from_wire(path))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self { paths })
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
        };
        write_json(&path, &wire).map_err(|error| record_error(&path, &error))
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
}

fn path_from_wire(wire: &PathWire) -> crate::Result<BuildStatePath> {
    BuildStatePath::new(&wire.checkout, &wire.volume)
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

    /// The staged stem a new image is created at, without its extension.
    pub fn staged_stem(&self, id: &BuildVolumeId) -> PathBuf {
        self.images.join(STAGING_DIRECTORY).join(id.as_str())
    }

    pub fn staging(&self) -> PathBuf {
        self.images.join(STAGING_DIRECTORY)
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

    pub fn read_record(&self, id: &BuildVolumeId) -> crate::Result<BuildVolumeRecord> {
        let path = self.record(id);
        read_json(&path).map_err(|error| record_error(&path, &error))
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

    /// The latest seed of `target` at `incarnation`. Two seeds of one target are an
    /// interrupted reseed: the newer is the latest.
    pub fn seed_of(
        &self,
        target: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> crate::Result<Option<(BuildVolumeId, BuildVolumeRecord)>> {
        let mut seeds = Vec::new();
        for id in self.list()? {
            let record = match self.read_record(&id) {
                Ok(record) => record,
                // An image without its record is an unpublished creation: never a seed.
                Err(_) if !self.record(&id).exists() => continue,
                Err(error) => return Err(error),
            };
            if record.is_seed_of(target, incarnation) {
                seeds.push((id, record));
            }
        }
        seeds.sort_by(|left, right| left.1.created_at.cmp(&right.1.created_at));
        Ok(seeds.pop())
    }
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
}
