use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::metadata::{
    CheckoutLayout, CheckoutLayoutRecord, ImageFormat, MetadataError, SlotBindings,
    SlotBindingsRecord, WorkspaceName,
};
use crate::repository::{PathLayoutError, ProjectPaths, RepoId};

pub mod apfs;
pub mod audit;
pub mod bootstrap;
pub mod fstab;
pub mod host_config;
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

    /// The default label for an unlabeled checkpoint: the UTC second of the request, rendered in
    /// the label alphabet (`2026-07-11t120309z`). Workspace revision is the wrong key — it does
    /// not advance between checkpoints, so a revision-derived default collides deterministically
    /// on the second unlabeled checkpoint. A checkpoint image may be the only crash-consistent
    /// copy, so an existing label is never reused or overwritten: a same-second collision takes
    /// `-2`, `-3`, … instead.
    pub fn utc_default(now: SystemTime, mut is_taken: impl FnMut(&str) -> bool) -> Self {
        let seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let (year, month, day) = civil_from_days(seconds / 86_400);
        let clock = seconds % 86_400;
        let base = format!(
            "{year:04}-{month:02}-{day:02}t{:02}{:02}{:02}z",
            clock / 3_600,
            clock % 3_600 / 60,
            clock % 60,
        );
        if !is_taken(&base) {
            return Self(base);
        }
        (2_u64..)
            .map(|ordinal| format!("{base}-{ordinal}"))
            .find(|candidate| !is_taken(candidate))
            .map(Self)
            .expect("existing checkpoint labels are finite")
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
        let store_root = store_root.as_ref();
        let host_config = host_config::HostConfig::load_for_store(store_root)?;
        Self::with_mount_root(store_root, host_config.mount_root(), repo_id)
    }

    pub fn with_mount_root(
        store_root: impl AsRef<Path>,
        host_mount_root: impl AsRef<Path>,
        repo_id: &RepoId,
    ) -> Result<Self, StorageLayoutError> {
        Ok(Self {
            project: ProjectPaths::with_mount_root(store_root, host_mount_root, repo_id)?,
        })
    }

    pub fn project(&self) -> &ProjectPaths {
        &self.project
    }
    /// Establishes the per-project storage boundary before any project-local metadata is written.
    ///
    /// Path derivation remains side-effect free; only the adopt provisioning path calls this.
    pub(crate) fn provision_project(&self) -> Result<(), StorageLayoutError> {
        verify_no_symlinks(&self.project.store_root, &self.project.project_root)?;
        fs::create_dir_all(&self.project.project_root).map_err(|source| StorageLayoutError::Io {
            path: self.project.project_root.clone(),
            source,
        })
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

    /// Where this workspace mounts.
    ///
    /// A workspace bound to a build slot mounts at that slot's stable path instead of its own
    /// name, so successive tenants of the slot present one absolute path to every compiler cache
    /// (`SlotId` documents the measurement). The binding record is read here rather than cached on
    /// the caller because this is the one derivation every mount, unmount and reverse lookup goes
    /// through: a stale copy would point half the system at the wrong directory, and the read is a
    /// single small file next to path joins that already spawn `diskutil`.
    pub fn workspace_mount(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<PathBuf, StorageLayoutError> {
        match self.slot_bindings()?.slot_of(workspace) {
            Some(slot) => checked_child(&self.project.mount_root, &slot.mount_name()),
            None => checked_child(&self.project.mount_root, workspace.as_str()),
        }
    }

    /// Every slot occupancy for this project. An absent record is no occupancies.
    pub fn slot_bindings(&self) -> Result<SlotBindings, StorageLayoutError> {
        match crate::metadata::read_json::<SlotBindingsRecord>(&self.project.slot_bindings) {
            Ok(record) => Ok(record.into_bindings()?),
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Ok(SlotBindings::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn record_slot_bindings(&self, bindings: &SlotBindings) -> Result<(), MetadataError> {
        crate::metadata::write_json(
            &self.project.slot_bindings,
            &SlotBindingsRecord::new(bindings),
        )
    }

    /// Where this project's `main` mounts.
    ///
    /// The record adopt wrote is the authority. Without one there is exactly one inference worth
    /// making and it is conclusive in the direction it fires: only the symlink layout ever creates
    /// `<mount-root>/<owner>/<repo>/main`, because under direct mount main mounts at the checkout
    /// path and that directory is never made. A project with no record and no configured main
    /// mountpoint is either not adopted yet — in which case the answer is whatever adopt is about
    /// to choose — or it is a detached symlink-layout project whose mountpoint `gc` has since
    /// removed, and guessing
    /// between those would silently point every resolver at the wrong path. That case is an error,
    /// not a default. Callers record whatever they resolve, so the inference runs at most once.
    pub fn checkout_layout(&self) -> Result<CheckoutLayout, StorageLayoutError> {
        if let Ok(record) =
            crate::metadata::read_json::<CheckoutLayoutRecord>(&self.project.checkout_layout)
        {
            record.validate()?;
            return Ok(record.checkout_layout);
        }
        if self
            .workspace_mount(&WorkspaceName::new("main").expect("fixed main"))?
            .is_dir()
        {
            return Ok(CheckoutLayout::Symlink);
        }
        for format in [ImageFormat::Asif, ImageFormat::Sparse] {
            if self.main_image(format)?.image().exists() {
                return Err(StorageLayoutError::UnrecordedCheckoutLayout);
            }
        }
        Ok(CheckoutLayout::default())
    }

    pub fn record_checkout_layout(&self, layout: CheckoutLayout) -> Result<(), MetadataError> {
        crate::metadata::write_json(
            &self.project.checkout_layout,
            &CheckoutLayoutRecord::new(layout),
        )
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

/// Days since 1970-01-01 to a proleptic-Gregorian civil date (Howard Hinnant's algorithm,
/// restricted to the non-negative domain). Total over every `u64` second count, unlike
/// `libc::gmtime_r`, which rejects out-of-range `time_t` — a label generator must not fail.
const fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + (month <= 2) as u64;
    (year, month, day)
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
    #[error(
        "the project is adopted but records no checkout layout, and its main mountpoint is absent"
    )]
    UnrecordedCheckoutLayout,
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
    HostConfig(#[from] host_config::HostConfigError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::SlotId;
    use proptest::prelude::*;

    fn layout() -> StorageLayout {
        StorageLayout::with_mount_root(
            "/private/cowshed/store",
            "/Users/test/.cowshed/mnt",
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap()
    }

    fn layout_under(root: &Path) -> StorageLayout {
        StorageLayout::new(root, &RepoId::parse("acme/widget").unwrap()).unwrap()
    }

    fn temp_store(name: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowshed-layout-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn adopt_provisioning_creates_project_root_before_the_first_journal_write() {
        use crate::api::dto::AdoptOptions;
        use crate::storage::recovery::{
            LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal,
        };

        let root = temp_store("adopt-provisioning");
        let layout = layout_under(&root);
        let project_root = layout.project().project_root.clone();
        assert!(!project_root.exists());

        layout.provision_project().expect("provision project root");
        let mut journal = LifecycleIntentJournal::default();
        journal.begin(LifecycleIntent::Adopt {
            options: AdoptOptions::default(),
        });
        journal
            .persist(&project_root.join(LIFECYCLE_INTENTS_FILE))
            .expect("first project-local write");

        assert!(project_root.is_dir());
        assert!(project_root.join(LIFECYCLE_INTENTS_FILE).is_file());
    }

    #[cfg(unix)]
    #[test]
    fn project_provisioning_refuses_a_symlinked_repository_boundary() {
        let root = temp_store("provisioning-symlink");
        let outside = temp_store("provisioning-symlink-target");
        std::os::unix::fs::symlink(&outside, root.join("acme")).unwrap();
        let layout = layout_under(&root);

        let error = layout.provision_project().unwrap_err();

        assert!(matches!(
            error,
            StorageLayoutError::SymlinkComponent(path) if path == root.join("acme")
        ));
        assert!(!outside.join("widget").exists());
    }

    #[test]
    fn recorded_checkout_layout_wins_over_every_inference() {
        let root = temp_store("recorded");
        let layout = layout_under(&root);
        fs::create_dir_all(&layout.project().project_root).unwrap();
        // Plant the shape that would otherwise be read as the symlink layout.
        fs::create_dir_all(
            layout
                .workspace_mount(&WorkspaceName::new("main").unwrap())
                .unwrap(),
        )
        .unwrap();
        layout
            .record_checkout_layout(CheckoutLayout::DirectMount)
            .unwrap();
        assert_eq!(
            layout.checkout_layout().unwrap(),
            CheckoutLayout::DirectMount
        );
    }

    #[test]
    fn an_unrecorded_layout_is_inferred_only_where_the_inference_is_conclusive() {
        let root = temp_store("inferred");
        let layout = layout_under(&root);
        fs::create_dir_all(&layout.project().project_root).unwrap();

        // Not adopted at all: no image, no mountpoint. Adopt is about to choose and record.
        assert_eq!(
            layout.checkout_layout().unwrap(),
            CheckoutLayout::default(),
            "an unadopted project has no layout to get wrong"
        );

        // Adopted, but nothing says where main mounts. Guessing here would silently point every
        // resolver at the wrong path, so it is an error.
        let image = layout.main_image(ImageFormat::Sparse).unwrap();
        fs::create_dir_all(image.image().parent().unwrap()).unwrap();
        fs::write(image.image(), b"image").unwrap();
        assert!(matches!(
            layout.checkout_layout(),
            Err(StorageLayoutError::UnrecordedCheckoutLayout)
        ));

        // Only the symlink layout ever creates this directory, so its presence is conclusive.
        fs::create_dir_all(
            layout
                .workspace_mount(&WorkspaceName::new("main").unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(layout.checkout_layout().unwrap(), CheckoutLayout::Symlink);
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
                    "/private/cowshed/store/acme/widget/sessions/raven{extension}.grants.json"
                ))
            );
            assert_eq!(
                paths.lock,
                PathBuf::from(format!(
                    "/private/cowshed/store/acme/widget/sessions/raven{extension}.lock"
                ))
            );
            assert_eq!(
                paths.ca_private_key,
                PathBuf::from(format!(
                    "/private/cowshed/store/acme/widget/sessions/raven{extension}.ca.key"
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
            Path::new("/private/cowshed/store/acme/widget/main.asif")
        );
        assert_eq!(
            layout.staged_main_image(ImageFormat::Sparse).unwrap().image,
            Path::new("/private/cowshed/store/acme/widget/.staging/main.sparseimage")
        );
        assert_eq!(
            layout
                .checkpoint_image(&raven, &label, ImageFormat::Asif)
                .unwrap()
                .image,
            Path::new(
                "/private/cowshed/store/acme/widget/checkpoints/raven/ci-fail.2026-07-11.asif"
            )
        );
        assert_eq!(
            layout.workspace_mount(&raven).unwrap(),
            Path::new("/Users/test/.cowshed/mnt/acme/widget/raven")
        );
        assert_eq!(WORKSPACE_MARKER_PATH, ".cowshed/workspace.json");
    }

    #[test]
    fn configured_host_root_changes_only_workspace_mount_derivation() {
        let root = temp_store("configured-mount-root");
        let mount_root = root
            .parent()
            .unwrap()
            .join(format!("cowshed-custom-mounts-{}", std::process::id()));
        let plan = host_config::plan_mount_root_change(&root, &mount_root, []).unwrap();
        host_config::execute_mount_root_change(&plan).unwrap();

        let layout = layout_under(&root);
        let raven = WorkspaceName::session("raven").unwrap();
        assert_eq!(
            layout.workspace_mount(&raven).unwrap(),
            mount_root.join("acme/widget/raven")
        );
        assert_eq!(layout.project().host_mount_root, mount_root);
        assert_eq!(layout.project().project_root, root.join("acme/widget"));

        fs::remove_dir_all(&root).unwrap();
        fs::remove_dir_all(layout.project().host_mount_root.clone()).unwrap();
    }

    #[test]
    fn a_slot_bound_workspace_mounts_at_the_slot_and_its_successor_inherits_the_path() {
        let root = temp_store("slots");
        let layout = layout_under(&root);
        fs::create_dir_all(&layout.project().project_root).unwrap();
        let first = WorkspaceName::session("raven").unwrap();
        let second = WorkspaceName::session("kestrel").unwrap();
        let slot = SlotId::new(3).unwrap();

        // Unbound: the name is the path.
        assert!(layout.slot_bindings().unwrap().is_empty());
        assert!(
            layout
                .workspace_mount(&first)
                .unwrap()
                .ends_with("mnt/acme/widget/raven")
        );

        let mut bindings = layout.slot_bindings().unwrap();
        bindings.bind(slot, first.clone()).unwrap();
        layout.record_slot_bindings(&bindings).unwrap();
        let bound = layout.workspace_mount(&first).unwrap();
        assert!(bound.ends_with("mnt/acme/widget/slot@3"), "{bound:?}");

        // The whole point: the next tenant of the slot sees byte-identical bytes for its build
        // path, which is what makes cargo's `-C metadata` and every sccache key match.
        let mut bindings = layout.slot_bindings().unwrap();
        assert_eq!(bindings.release(&first), Some(slot));
        bindings.bind(slot, second.clone()).unwrap();
        layout.record_slot_bindings(&bindings).unwrap();
        assert_eq!(layout.workspace_mount(&second).unwrap(), bound);
        assert!(
            layout
                .workspace_mount(&first)
                .unwrap()
                .ends_with("mnt/acme/widget/raven")
        );
    }

    #[test]
    fn slot_occupancy_is_exclusive_in_both_directions() {
        let raven = WorkspaceName::session("raven").unwrap();
        let kestrel = WorkspaceName::session("kestrel").unwrap();
        let mut bindings = SlotBindings::default();
        bindings
            .bind(SlotId::new(0).unwrap(), raven.clone())
            .unwrap();

        // Re-binding the same pair is how create and repair stay idempotent.
        bindings
            .bind(SlotId::new(0).unwrap(), raven.clone())
            .unwrap();

        assert!(matches!(
            bindings.bind(SlotId::new(0).unwrap(), kestrel.clone()),
            Err(MetadataError::SlotAlreadyBound { slot: 0, .. })
        ));
        assert!(matches!(
            bindings.bind(SlotId::new(1).unwrap(), raven.clone()),
            Err(MetadataError::WorkspaceAlreadySlotted { slot: 0, .. })
        ));
        assert!(matches!(
            bindings.bind(SlotId::new(1).unwrap(), WorkspaceName::new("main").unwrap()),
            Err(MetadataError::MainIsNotSlottable)
        ));
        assert_eq!(bindings.release(&kestrel), None);
        assert_eq!(bindings.release(&raven), Some(SlotId::new(0).unwrap()));
        assert!(bindings.is_empty());
    }

    #[test]
    fn a_slot_mountpoint_is_recognised_from_its_path_alone() {
        let slot = SlotId::new(7).unwrap();
        let mount = Path::new("/Users/test/.cowshed/mnt/acme/widget").join(slot.mount_name());
        assert_eq!(SlotId::from_mount_path(&mount), Some(slot));
        // A name-mounted workspace is not a slot, and no workspace name can imitate one: `@` is
        // outside the `WorkspaceName` grammar.
        assert_eq!(
            SlotId::from_mount_path(Path::new("/Users/test/.cowshed/mnt/acme/widget/slot-7")),
            None
        );
        assert!(WorkspaceName::new("slot@7").is_err());
        assert!(SlotId::new(SlotId::MAX + 1).is_err());
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

    fn at(seconds: u64) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(seconds)
    }

    #[test]
    fn the_default_label_is_the_documented_utc_second() {
        for (seconds, expected) in [
            (0, "1970-01-01t000000z"),
            (1_783_771_200, "2026-07-11t120000z"),
            // Leap day and both neighbours: a leap-year error shifts one of the three.
            (1_835_395_199, "2028-02-28t235959z"),
            (1_835_395_200, "2028-02-29t000000z"),
            (1_835_481_600, "2028-03-01t000000z"),
            // Year boundary at exactly midnight UTC.
            (1_893_455_999, "2029-12-31t235959z"),
            (1_893_456_000, "2030-01-01t000000z"),
            // 2100 is the century rule's first bite: divisible by 4 but not a leap year.
            (4_107_542_399, "2100-02-28t235959z"),
            (4_107_542_400, "2100-03-01t000000z"),
            // 2400 restores the 400-year exception, so its February 29th exists.
            (13_574_585_228, "2400-02-29t060708z"),
        ] {
            let label = CheckpointLabel::utc_default(at(seconds), |_| false);
            assert_eq!(label.as_str(), expected, "at {seconds}s");
            // Every generated default must survive the validator it bypasses.
            assert_eq!(CheckpointLabel::new(expected).unwrap(), label);
        }
    }

    #[test]
    fn a_same_second_collision_takes_the_next_ordinal_and_never_reuses_a_label() {
        let now = at(1_783_771_200);
        let base = CheckpointLabel::utc_default(now, |_| false);
        assert_eq!(base.as_str(), "2026-07-11t120000z");

        let taken = [base.as_str().to_owned()];
        let second =
            CheckpointLabel::utc_default(now, |candidate| taken.iter().any(|l| l == candidate));
        assert_eq!(second.as_str(), "2026-07-11t120000z-2");

        let taken = [taken[0].clone(), second.as_str().to_owned()];
        let third =
            CheckpointLabel::utc_default(now, |candidate| taken.iter().any(|l| l == candidate));
        assert_eq!(third.as_str(), "2026-07-11t120000z-3");
        assert_eq!(CheckpointLabel::new(third.as_str()).unwrap(), third);
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
