use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use thiserror::Error;

use cowshed_types::image::IMAGE_EXTENSION;
use cowshed_types::lifecycle::CheckpointLabel;
use cowshed_types::metadata::{
    CheckoutRootRecord, MetadataError, SlotBindings, SlotBindingsRecord, WorkspaceName,
    append_suffix, sidecar_path,
};
use cowshed_types::repository::{PathLayoutError, ProjectPaths, RepoId};

pub mod apfs;
pub mod audit;
pub mod bootstrap;
pub mod deletion_log;
pub mod directory;
pub mod fstab;
pub mod host_config;
pub mod job_artifact;
pub mod lifecycle;
pub mod recovery;
pub mod recovery_image;
pub mod trace_columns;
pub mod trace_segment;

pub const WORKSPACE_MARKER_PATH: &str = ".cowshed/workspace.json";
use recovery::STAGING_NAMESPACE;

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
            sidecar: sidecar_path(&image),
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
    /// Establishes the per-project storage boundary before any project-local metadata is written,
    /// and marks the owner directory as one (`bootstrap::OWNER_MARKER_FILE`), so store discovery
    /// reads it.
    ///
    /// Path derivation remains side-effect free; only the macOS adopt path calls this in production.
    /// Test builds retain the platform-neutral filesystem boundary checks on every host.
    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn provision_project(&self) -> Result<(), StorageLayoutError> {
        verify_no_symlinks(&self.project.store_root, &self.project.project_root)?;
        fs::create_dir_all(&self.project.project_root).map_err(|source| {
            StorageLayoutError::Io {
                path: self.project.project_root.clone(),
                source,
            }
        })?;
        let owner = self
            .project
            .project_root
            .parent()
            .expect("a project root is <store>/<owner>/<repo>");
        bootstrap::write_owner_marker(owner).map_err(|source| StorageLayoutError::Io {
            path: owner.join(bootstrap::OWNER_MARKER_FILE),
            source,
        })
    }

    pub fn main_image(&self) -> Result<ImagePaths, StorageLayoutError> {
        self.image_below(&self.project.project_root, "main")
    }

    pub fn staged_main_image(&self) -> Result<ImagePaths, StorageLayoutError> {
        let staging = checked_child(&self.project.project_root, STAGING_NAMESPACE)?;
        self.image_below(&staging, "main")
    }

    pub fn session_image(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<ImagePaths, StorageLayoutError> {
        if workspace.is_main() {
            return Err(StorageLayoutError::MainIsNotSession);
        }
        self.image_below(&self.project.sessions, workspace.as_str())
    }

    /// The canonical image for a workspace: main lives at the project root, every session under
    /// `sessions/`. The one statement of that rule.
    pub fn canonical_image(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<ImagePaths, StorageLayoutError> {
        if workspace.is_main() {
            self.main_image()
        } else {
            self.session_image(workspace)
        }
    }

    pub fn checkpoint_image(
        &self,
        workspace: &WorkspaceName,
        label: &CheckpointLabel,
    ) -> Result<ImagePaths, StorageLayoutError> {
        let workspace_directory = checked_child(&self.project.checkpoints, workspace.as_str())?;
        self.image_below(&workspace_directory, label.as_str())
    }

    /// The `TMPDIR` of every process the workspace's sandbox runs: in the project's store
    /// directory, so no workspace reaches another's (the store is denied to every sandbox and
    /// this directory is the one carve-back), and outside every checkout, so a tool that must put
    /// scratch files outside the project it builds finds a writable place. Keyed by name like the
    /// workspace's checkpoints, and reclaimed with its retired image.
    pub fn exec_temp_dir(&self, workspace: &WorkspaceName) -> Result<PathBuf, StorageLayoutError> {
        checked_child(&self.project.exec_temp, workspace.as_str())
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

    /// Where one workspace of one project mounts: main at the adopted checkout itself, every other
    /// workspace under `mnt/`. One rule, because every fact pass, mount, and reverse lookup goes
    /// through it, and a project whose two answers disagreed would be reported as broken by
    /// whichever derivation ran second.
    pub fn main_aware_workspace_mount(
        &self,
        checkout_path: &Path,
        workspace: &WorkspaceName,
    ) -> Result<PathBuf, StorageLayoutError> {
        if workspace.is_main() {
            return Ok(checkout_path.to_owned());
        }
        self.workspace_mount(workspace)
    }

    /// Every slot occupancy for this project. An absent record is no occupancies.
    pub fn slot_bindings(&self) -> Result<SlotBindings, StorageLayoutError> {
        match cowshed_types::metadata::read_json::<SlotBindingsRecord>(&self.project.slot_bindings)
        {
            Ok(record) => Ok(record.into_bindings()?),
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Ok(SlotBindings::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn record_slot_bindings(&self, bindings: &SlotBindings) -> Result<(), MetadataError> {
        cowshed_types::metadata::write_json(
            &self.project.slot_bindings,
            &SlotBindingsRecord::new(bindings),
        )
    }

    /// The checkout root main's removal recorded, or `None` when main has never been removed.
    /// A present record that does not parse fails closed.
    pub fn recorded_checkout_root(&self) -> Result<Option<PathBuf>, MetadataError> {
        match cowshed_types::metadata::read_json::<CheckoutRootRecord>(&self.project.checkout_root)
        {
            Ok(record) => Ok(Some(record.checkout_root().to_owned())),
            Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub fn record_checkout_root(&self, checkout_root: &Path) -> Result<(), MetadataError> {
        cowshed_types::metadata::write_json(
            &self.project.checkout_root,
            &CheckoutRootRecord::new(checkout_root)?,
        )
    }

    fn image_below(&self, directory: &Path, stem: &str) -> Result<ImagePaths, StorageLayoutError> {
        let file_name = format!("{stem}.{IMAGE_EXTENSION}");
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
    path: PathBuf,
}

impl DiscoveredSessionImage {
    pub fn workspace(&self) -> &WorkspaceName {
        &self.workspace
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Select only published session images from a directory listing.
///
/// Sidecars, locks, staging directories, temporary names, invalid workspace names, and every
/// file that is not `<name>.dmg` are deliberately invisible.
pub fn discover_session_images(
    entries: impl IntoIterator<Item = PathBuf>,
) -> Vec<DiscoveredSessionImage> {
    let mut discovered = entries
        .into_iter()
        .filter_map(|path| {
            let stem = path
                .file_name()
                .and_then(OsStr::to_str)
                .and_then(image_stem)?;
            let workspace = WorkspaceName::session(stem).ok()?;
            Some(DiscoveredSessionImage { workspace, path })
        })
        .collect::<Vec<_>>();
    discovered.sort_by(|left, right| left.workspace.cmp(&right.workspace));
    discovered
}

/// Whether `file_name` is an image's lifecycle lock, `<stem>.dmg.lock` ([`ImagePaths::lock`]):
/// a lock whatever its length, since its taker records itself on it and a taker that ended
/// without unwinding leaves its record (`held_lock::ImageLockTaker`).
pub fn is_image_lock(file_name: &str) -> bool {
    file_name
        .strip_suffix(".lock")
        .and_then(image_stem)
        .is_some()
}

/// The workspace a canonical image `<workspace>.dmg` belongs to (`main.dmg` is main's).
pub fn image_workspace(image: &Path) -> Option<WorkspaceName> {
    let stem = image
        .file_name()
        .and_then(OsStr::to_str)
        .and_then(image_stem)?;
    WorkspaceName::new(stem).ok()
}

/// The non-empty stem of `<stem>.dmg`.
fn image_stem(file_name: &str) -> Option<&str> {
    file_name
        .strip_suffix(IMAGE_EXTENSION)
        .and_then(|name| name.strip_suffix('.'))
        .filter(|stem| !stem.is_empty())
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
    // A non-canonical root voids the lexical containment below (`..` in the root lets a candidate
    // strip-prefix cleanly while resolving elsewhere), so it is refused the same way
    // `is_lexically_contained` in repository.rs refuses it.
    if !cowshed_types::repository::is_lexically_canonical(root) {
        return Err(StorageLayoutError::EscapesStoreRoot);
    }
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
    #[error(transparent)]
    InvalidCheckpointLabel(#[from] cowshed_types::lifecycle::InvalidCheckpointLabel),
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
    use cowshed_types::metadata::SlotId;
    use proptest::prelude::*;

    fn layout() -> StorageLayout {
        StorageLayout::with_mount_root(
            cowshed_gateway_types::STORE_ROOT,
            "/Users/test/.cowshed/mnt",
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap()
    }

    fn layout_under(root: &Path) -> StorageLayout {
        StorageLayout::new(root, &RepoId::parse("acme/widget").unwrap()).unwrap()
    }

    /// A scratch store root removed when dropped, so a failed assertion unwinds through the
    /// cleanup a passing test runs.
    struct TempStore(PathBuf);

    impl std::ops::Deref for TempStore {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.0
        }
    }

    impl AsRef<Path> for TempStore {
        fn as_ref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempStore {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                cowshed_os::eprintln!(
                    "layout test store {} was not removed: {error}",
                    self.0.display()
                );
            }
        }
    }

    fn temp_store(name: &str) -> TempStore {
        let root =
            std::env::temp_dir().join(format!("cowshed-layout-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        TempStore(root)
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
        LifecycleIntentJournal::update(&project_root.join(LIFECYCLE_INTENTS_FILE), |journal| {
            journal.begin(LifecycleIntent::Adopt {
                options: AdoptOptions::default(),
            });
            Ok(())
        })
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
    fn maps_the_dmg_image_and_its_complete_sibling_suffixes() {
        let layout = layout();
        let raven = WorkspaceName::session("raven").unwrap();
        let paths = layout.session_image(&raven).unwrap();
        assert_eq!(
            paths.image,
            Path::new("/private/cowshed/store/acme/widget/sessions/raven.dmg")
        );
        assert_eq!(
            paths.sidecar,
            Path::new("/private/cowshed/store/acme/widget/sessions/raven.dmg.grants.json")
        );
        assert_eq!(
            paths.lock,
            Path::new("/private/cowshed/store/acme/widget/sessions/raven.dmg.lock")
        );
        assert_eq!(
            paths.ca_private_key,
            Path::new("/private/cowshed/store/acme/widget/sessions/raven.dmg.ca.key")
        );
        cowshed_types::metadata::validate_image_path(&paths.image).unwrap();
    }

    #[test]
    fn maps_main_staging_checkpoint_and_mount_paths() {
        let layout = layout();
        let raven = WorkspaceName::session("raven").unwrap();
        let label = CheckpointLabel::new("ci-fail.2026-07-11").unwrap();
        assert_eq!(
            layout.main_image().unwrap().image,
            Path::new("/private/cowshed/store/acme/widget/main.dmg")
        );
        assert_eq!(
            layout.staged_main_image().unwrap().image,
            Path::new("/private/cowshed/store/acme/widget/.staging/main.dmg")
        );
        assert_eq!(
            layout.checkpoint_image(&raven, &label).unwrap().image,
            Path::new(
                "/private/cowshed/store/acme/widget/checkpoints/raven/ci-fail.2026-07-11.dmg"
            )
        );
        assert_eq!(
            layout.workspace_mount(&raven).unwrap(),
            Path::new("/Users/test/.cowshed/mnt/acme/widget/raven")
        );
        assert_eq!(WORKSPACE_MARKER_PATH, ".cowshed/workspace.json");
    }

    #[test]
    fn custom_host_root_is_refused_before_workspace_path_derivation() {
        let root = temp_store("retired-custom-root");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(host_config::HOST_CONFIG_FILE);
        fs::write(
            &path,
            br#"{"version":1,"mountRoot":"/indexed/custom-root"}"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let before = fs::read(&path).unwrap();
        let error = StorageLayout::new(&root, &RepoId::parse("acme/widget").unwrap()).unwrap_err();
        assert!(matches!(
            error,
            StorageLayoutError::HostConfig(host_config::HostConfigError::RetiredMountRoot { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(!root.join("mnt").exists());
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
    fn enumeration_returns_only_published_dmg_images() {
        let entries = [
            "/store/sessions/raven.dmg",
            "/store/sessions/owl.sparseimage",
            "/store/sessions/owl.sparseimage.grants.json",
            "/store/sessions/finch.dmg",
            "/store/sessions/raven.dmg.grants.json",
            "/store/sessions/raven.dmg.lock",
            "/store/sessions/main.dmg",
            "/store/sessions/.staging",
            "/store/sessions/.raven.dmg",
            "/store/sessions/raven.tmp.dmg",
            "/store/sessions/upper.Dmg",
        ]
        .map(PathBuf::from);
        let images = discover_session_images(entries);
        assert_eq!(
            images
                .iter()
                .map(|image| (image.workspace.as_str(), image.path()))
                .collect::<Vec<_>>(),
            [
                ("finch", Path::new("/store/sessions/finch.dmg")),
                ("raven", Path::new("/store/sessions/raven.dmg")),
            ],
            "sorted by workspace; a .sparseimage is not a workspace image"
        );
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
                layout.main_image().unwrap()
            } else {
                layout.session_image(&workspace).unwrap()
            };
            prop_assert!(layout.project().contains(&image.image));
            prop_assert!(!image.image.components().any(|part| matches!(part, Component::ParentDir)));
        }

        #[test]
        fn staged_names_are_never_enumerated(stem in "[a-z0-9-]{1,32}") {
            let names = [
                PathBuf::from(format!(".staging-{stem}.dmg")),
                PathBuf::from(format!(".{stem}.sparseimage")),
                PathBuf::from(format!("{stem}.dmg.tmp")),
            ];
            prop_assert!(discover_session_images(names).is_empty());
        }

        #[test]
        fn canonical_remote_result_is_stable(
            owner in "[A-Za-z0-9][A-Za-z0-9._-]{0,31}",
            repo in "[A-Za-z0-9][A-Za-z0-9._-]{0,31}",
        ) {
            let remote = format!("https://user:secret@example.com//{owner}///{repo}.git?x=1#fragment");
            let first = cowshed_types::repository::normalize_remote_url(&remote).unwrap();
            let reparsed = RepoId::parse(first.as_str()).unwrap();
            prop_assert_eq!(first, reparsed);
        }

    }
}
