use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::metadata::{ImageFormat, MetadataError, WorkspaceName};
use crate::repository::{PathLayoutError, ProjectPaths, RepoId};

pub mod apfs;
pub mod bootstrap;
pub mod commitment_store;
pub mod job_artifact;
pub mod lifecycle;
pub mod recovery;

pub const WORKSPACE_MARKER_PATH: &str = ".cowshed/workspace.json";
const STAGING_DIRECTORY: &str = ".staging";

/// A validated, path-safe checkpoint label.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointLabel(String);

impl CheckpointLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, StorageLayoutError> {
        let value = value.into();
        let bytes = value.as_bytes();
        let valid = (1..=128).contains(&bytes.len())
            && !value.starts_with("pre-restore-")
            && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
            && bytes.iter().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(StorageLayoutError::InvalidCheckpointLabel(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CheckpointLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for CheckpointLabel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CheckpointLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// All controller-owned sibling paths associated with one image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImagePaths {
    image: PathBuf,
    sidecar: PathBuf,
    lock: PathBuf,
    ca_private_key: PathBuf,
}

impl ImagePaths {
    fn new(image: PathBuf) -> Self {
        Self {
            sidecar: append_suffix(&image, ".grants.json"),
            lock: append_suffix(&image, ".lock"),
            ca_private_key: append_suffix(&image, ".ca.key"),
            image,
        }
    }

    pub fn image(&self) -> &Path {
        &self.image
    }

    pub fn sidecar(&self) -> &Path {
        &self.sidecar
    }

    pub fn lock(&self) -> &Path {
        &self.lock
    }

    pub fn ca_private_key(&self) -> &Path {
        &self.ca_private_key
    }
}

/// Canonical controller paths for one primary repository identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageLayout {
    project: ProjectPaths,
}

impl StorageLayout {
    pub fn new(store_root: impl AsRef<Path>, repo_id: &RepoId) -> Result<Self, StorageLayoutError> {
        Ok(Self {
            project: ProjectPaths::new(store_root, repo_id)?,
        })
    }

    pub fn project(&self) -> &ProjectPaths {
        &self.project
    }

    pub fn main_image(&self, format: ImageFormat) -> Result<ImagePaths, StorageLayoutError> {
        self.image_below(&self.project.project_root, "main", format)
    }

    pub fn staged_main_image(&self, format: ImageFormat) -> Result<ImagePaths, StorageLayoutError> {
        let staging = checked_child(&self.project.project_root, STAGING_DIRECTORY)?;
        self.image_below(&staging, "main", format)
    }

    pub fn session_image(
        &self,
        workspace: &WorkspaceName,
        format: ImageFormat,
    ) -> Result<ImagePaths, StorageLayoutError> {
        if workspace.is_main() {
            return Err(StorageLayoutError::MainIsNotSession);
        }
        self.image_below(&self.project.sessions, workspace.as_str(), format)
    }

    pub fn checkpoint_image(
        &self,
        workspace: &WorkspaceName,
        label: &CheckpointLabel,
        format: ImageFormat,
    ) -> Result<ImagePaths, StorageLayoutError> {
        let workspace_directory = checked_child(&self.project.checkpoints, workspace.as_str())?;
        self.image_below(&workspace_directory, label.as_str(), format)
    }

    pub fn workspace_mount(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<PathBuf, StorageLayoutError> {
        checked_child(&self.project.mount_root, workspace.as_str())
    }

    fn image_below(
        &self,
        directory: &Path,
        stem: &str,
        format: ImageFormat,
    ) -> Result<ImagePaths, StorageLayoutError> {
        let file_name = format!("{stem}{}", format.image_extension());
        let image = checked_child(directory, &file_name)?;
        if !self.project.contains(&image) {
            return Err(StorageLayoutError::EscapesStoreRoot);
        }
        Ok(ImagePaths::new(image))
    }
}

/// One canonical session image discovered from the direct children of `sessions/`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredSessionImage {
    workspace: WorkspaceName,
    format: ImageFormat,
    path: PathBuf,
}

impl DiscoveredSessionImage {
    pub fn workspace(&self) -> &WorkspaceName {
        &self.workspace
    }

    pub fn format(&self) -> ImageFormat {
        self.format
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Select only published session images from a directory listing.
///
/// Sidecars, locks, staging directories, temporary names, invalid workspace names, and
/// unsupported extensions are deliberately invisible. A workspace published in both formats
/// is rejected instead of choosing one arbitrarily.
pub fn discover_session_images(
    entries: impl IntoIterator<Item = PathBuf>,
) -> Result<Vec<DiscoveredSessionImage>, StorageLayoutError> {
    let mut discovered = BTreeMap::<WorkspaceName, DiscoveredSessionImage>::new();
    for path in entries {
        let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        let Some((stem, format)) = image_name(file_name) else {
            continue;
        };
        let Ok(workspace) = WorkspaceName::session(stem) else {
            continue;
        };
        let image = DiscoveredSessionImage {
            workspace: workspace.clone(),
            format,
            path,
        };
        if let Some(previous) = discovered.insert(workspace.clone(), image) {
            return Err(StorageLayoutError::DuplicateWorkspaceFormats {
                workspace,
                first: previous.format,
                second: format,
            });
        }
    }
    Ok(discovered.into_values().collect())
}

fn image_name(file_name: &str) -> Option<(&str, ImageFormat)> {
    for format in [ImageFormat::Asif, ImageFormat::Sparse] {
        if let Some(stem) = file_name.strip_suffix(format.image_extension())
            && !stem.is_empty()
        {
            return Some((stem, format));
        }
    }
    None
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn checked_child(root: &Path, component: &str) -> Result<PathBuf, StorageLayoutError> {
    if component.is_empty() {
        return Err(StorageLayoutError::UnsafeComponent(component.to_owned()));
    }
    let mut components = Path::new(component).components();
    if !matches!(components.next(), Some(Component::Normal(_))) {
        return Err(StorageLayoutError::UnsafeComponent(component.to_owned()));
    }
    if components.next().is_some() {
        return Err(StorageLayoutError::UnsafeComponent(component.to_owned()));
    }

    let candidate = root.join(component);
    let relative = candidate
        .strip_prefix(root)
        .map_err(|_| StorageLayoutError::EscapesStoreRoot)?;
    let mut joined_components = relative.components();
    if !matches!(joined_components.next(), Some(Component::Normal(_))) {
        return Err(StorageLayoutError::EscapesStoreRoot);
    }
    if joined_components.next().is_some() {
        return Err(StorageLayoutError::EscapesStoreRoot);
    }
    Ok(candidate)
}

/// Verify a mapped path component-by-component without canonicalizing or following links.
///
/// The root must exist. Missing descendants are accepted so the same check can guard creation,
/// but every existing component from the root down is inspected with `symlink_metadata`.
pub fn verify_no_symlinks(root: &Path, candidate: &Path) -> Result<(), StorageLayoutError> {
    let relative = candidate
        .strip_prefix(root)
        .map_err(|_| StorageLayoutError::EscapesStoreRoot)?;
    if relative.components().next().is_none()
        || !relative
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(StorageLayoutError::EscapesStoreRoot);
    }

    reject_symlink(root)?;
    let mut current = root.to_owned();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StorageLayoutError::SymlinkComponent(current));
            }
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(StorageLayoutError::Io {
                    path: current,
                    source,
                });
            }
        }
    }
    Ok(())
}

fn reject_symlink(path: &Path) -> Result<(), StorageLayoutError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| StorageLayoutError::Io {
        path: path.to_owned(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        Err(StorageLayoutError::SymlinkComponent(path.to_owned()))
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum StorageLayoutError {
    #[error("invalid checkpoint label {0:?}")]
    InvalidCheckpointLabel(String),
    #[error("workspace `main` is not a session")]
    MainIsNotSession,
    #[error("unsafe storage path component {0:?}")]
    UnsafeComponent(String),
    #[error("derived storage path escapes its root")]
    EscapesStoreRoot,
    #[error("storage path contains symbolic link component {}", .0.display())]
    SymlinkComponent(PathBuf),
    #[error("storage path inspection failed for {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("workspace {workspace} exists in both {first:?} and {second:?} formats")]
    DuplicateWorkspaceFormats {
        workspace: WorkspaceName,
        first: ImageFormat,
        second: ImageFormat,
    },
    #[error(transparent)]
    PathLayout(#[from] PathLayoutError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn layout() -> StorageLayout {
        StorageLayout::new(
            "/Users/test/.cowshed",
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn maps_every_image_format_and_complete_sibling_suffix() {
        let layout = layout();
        let raven = WorkspaceName::session("raven").unwrap();
        for (format, extension) in [
            (ImageFormat::Asif, ".asif"),
            (ImageFormat::Sparse, ".sparseimage"),
        ] {
            let paths = layout.session_image(&raven, format).unwrap();
            assert!(paths.image.to_string_lossy().ends_with(extension));
            assert_eq!(
                paths.sidecar,
                PathBuf::from(format!(
                    "/Users/test/.cowshed/acme/widget/sessions/raven{extension}.grants.json"
                ))
            );
            assert_eq!(
                paths.lock,
                PathBuf::from(format!(
                    "/Users/test/.cowshed/acme/widget/sessions/raven{extension}.lock"
                ))
            );
            assert_eq!(
                paths.ca_private_key,
                PathBuf::from(format!(
                    "/Users/test/.cowshed/acme/widget/sessions/raven{extension}.ca.key"
                ))
            );
            assert_eq!(ImageFormat::from_image_path(&paths.image).unwrap(), format);
            format.validate_path(&paths.image).unwrap();
        }
    }

    #[test]
    fn maps_main_staging_checkpoint_and_mount_paths() {
        let layout = layout();
        let raven = WorkspaceName::session("raven").unwrap();
        let label = CheckpointLabel::new("ci-fail.2026-07-11").unwrap();
        assert_eq!(
            layout.main_image(ImageFormat::Asif).unwrap().image,
            Path::new("/Users/test/.cowshed/acme/widget/main.asif")
        );
        assert_eq!(
            layout.staged_main_image(ImageFormat::Sparse).unwrap().image,
            Path::new("/Users/test/.cowshed/acme/widget/.staging/main.sparseimage")
        );
        assert_eq!(
            layout
                .checkpoint_image(&raven, &label, ImageFormat::Asif)
                .unwrap()
                .image,
            Path::new("/Users/test/.cowshed/acme/widget/checkpoints/raven/ci-fail.2026-07-11.asif")
        );
        assert_eq!(
            layout.workspace_mount(&raven).unwrap(),
            Path::new("/Users/test/.cowshed/mnt/acme/widget/raven")
        );
        assert_eq!(WORKSPACE_MARKER_PATH, ".cowshed/workspace.json");
    }

    #[test]
    fn rejects_every_unsafe_child_shape() {
        for component in ["", ".", "..", "../escape", "nested/child", "/absolute"] {
            assert!(
                checked_child(Path::new("/store"), component).is_err(),
                "accepted {component:?}"
            );
        }
    }

    #[test]
    fn labels_are_immutable_validated_components() {
        for valid in ["a", "ci-fail", "2026-07-11t120000z", "release_1.2"] {
            let label = CheckpointLabel::new(valid).unwrap();
            assert_eq!(label.to_string(), valid);
            let encoded = serde_json::to_string(&label).unwrap();
            assert_eq!(
                serde_json::from_str::<CheckpointLabel>(&encoded).unwrap(),
                label
            );
        }
        for invalid in [
            "",
            ".",
            "..",
            "Upper",
            "-leading",
            "slash/name",
            "a b",
            "pre-restore-user",
            "pre-restore-00000000000000000000000000000002",
        ] {
            assert!(
                CheckpointLabel::new(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn enumeration_returns_only_published_images_and_rejects_duplicates() {
        let entries = [
            "/store/sessions/raven.asif",
            "/store/sessions/owl.sparseimage",
            "/store/sessions/raven.asif.grants.json",
            "/store/sessions/raven.asif.lock",
            "/store/sessions/main.asif",
            "/store/sessions/.staging",
            "/store/sessions/.raven.asif",
            "/store/sessions/raven.tmp.asif",
            "/store/sessions/upper.Asif",
        ]
        .map(PathBuf::from);
        let images = discover_session_images(entries).unwrap();
        assert_eq!(
            images
                .iter()
                .map(|image| (image.workspace.as_str(), image.format))
                .collect::<Vec<_>>(),
            vec![("owl", ImageFormat::Sparse), ("raven", ImageFormat::Asif)]
        );

        assert!(matches!(
            discover_session_images([
                PathBuf::from("raven.asif"),
                PathBuf::from("raven.sparseimage")
            ]),
            Err(StorageLayoutError::DuplicateWorkspaceFormats { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn path_validation_refuses_symlinks_without_resolving_them() {
        use std::os::unix::fs::symlink;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-storage-symlink-{}-{nonce}",
            std::process::id()
        ));
        let real = root.join("real");
        fs::create_dir_all(&real).unwrap();
        symlink(&real, root.join("linked")).unwrap();
        let linked_root = root.with_extension("link");
        symlink(&root, &linked_root).unwrap();

        verify_no_symlinks(&root, &real.join("not-created-yet")).unwrap();
        assert!(matches!(
            verify_no_symlinks(&root, &root.join("linked/child")),
            Err(StorageLayoutError::SymlinkComponent(_))
        ));
        assert!(matches!(
            verify_no_symlinks(&root, &root.join("../escape")),
            Err(StorageLayoutError::EscapesStoreRoot)
        ));
        assert!(matches!(
            verify_no_symlinks(&linked_root, &linked_root.join("real")),
            Err(StorageLayoutError::SymlinkComponent(_))
        ));
        assert!(matches!(
            verify_no_symlinks(&root, &root.join("real").join("x".repeat(1024))),
            Err(StorageLayoutError::Io { .. })
        ));
        assert!(matches!(
            verify_no_symlinks(&root.join("missing"), &root.join("missing/child")),
            Err(StorageLayoutError::Io { .. })
        ));

        fs::remove_file(linked_root).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    proptest! {
        #[test]
        fn validated_components_remain_contained(
            owner in "[a-z0-9][a-z0-9._-]{0,31}",
            repo in "[a-z0-9][a-z0-9._-]{0,31}",
            workspace in "[a-z0-9][a-z0-9-]{0,31}",
        ) {
            let repo_id = RepoId::parse(&format!("{owner}/{repo}")).unwrap();
            let layout = StorageLayout::new("/store", &repo_id).unwrap();
            let workspace = WorkspaceName::new(workspace).unwrap();
            let image = if workspace.is_main() {
                layout.main_image(ImageFormat::Asif).unwrap()
            } else {
                layout.session_image(&workspace, ImageFormat::Asif).unwrap()
            };
            prop_assert!(layout.project().contains(&image.image));
            prop_assert!(!image.image.components().any(|part| matches!(part, Component::ParentDir)));
        }

        #[test]
        fn staged_names_are_never_enumerated(stem in "[a-z0-9-]{1,32}") {
            let names = [
                PathBuf::from(format!(".staging-{stem}.asif")),
                PathBuf::from(format!(".{stem}.sparseimage")),
                PathBuf::from(format!("{stem}.asif.tmp")),
            ];
            prop_assert!(discover_session_images(names).unwrap().is_empty());
        }

        #[test]
        fn canonical_remote_result_is_stable(
            owner in "[A-Za-z0-9][A-Za-z0-9._-]{0,31}",
            repo in "[A-Za-z0-9][A-Za-z0-9._-]{0,31}",
        ) {
            let remote = format!("https://user:secret@example.com//{owner}///{repo}.git?x=1#fragment");
            let first = crate::repository::normalize_remote_url(&remote).unwrap();
            let reparsed = RepoId::parse(first.as_str()).unwrap();
            prop_assert_eq!(first, reparsed);
        }
    }
}
