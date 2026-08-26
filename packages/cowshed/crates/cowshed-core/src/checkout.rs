//! The project checkout path: where it is recorded, and how it moves.
//!
//! A project's checkout path is written down in three independent places, none of which is
//! derivable from the others:
//!
//! - the **in-image marker** (`.cowshed/workspace.json` at main's mount root), which is what a
//!   cold controller reads to answer "which repository is this directory";
//! - the **detached sidecar** beside main's canonical image, whose `infoSnapshot.projectRoot` is
//!   what the gateway inventory scans to answer the same question without mounting anything;
//! - the **layout record** (`checkout-layout.json`), which says whether the checkout path *is*
//!   main's mountpoint or merely a symlink to it.
//!
//! Every operation that changes where the checkout lives has to move all three together, so they
//! are moved by the functions here rather than open-coded at each call site.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::metadata::{
    CheckoutLayout, CheckoutLayoutRecord, DetachedWorkspaceMetadata, MetadataError,
    WorkspaceMarker, read_json, sidecar_path, write_json,
};
use crate::storage::WORKSPACE_MARKER_PATH;

/// Where one project's recorded checkout path is durably held.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckoutRecord {
    /// Main's mount root; the in-image marker sits under it. The volume must be mounted.
    pub mount_point: PathBuf,
    /// Main's canonical image; the detached sidecar sits beside it.
    pub image: PathBuf,
}

impl CheckoutRecord {
    /// Rewrite the recorded project root in both the marker and the sidecar.
    ///
    /// Idempotent, and reports whether it changed anything, so a convergence caller can stay
    /// silent when the record already agrees with the world.
    ///
    /// The marker is written first because it is the record a cold open consults from the checkout
    /// directory itself: if the process dies between the two writes, the marker already names the
    /// destination and the sidecar is repaired by the next convergence. The reverse order would
    /// leave the authoritative-on-open record naming a path that is about to stop existing.
    pub fn rewrite_project_root(&self, project_root: &Path) -> Result<bool, MetadataError> {
        if !project_root.is_absolute() {
            return Err(MetadataError::InvalidPath(project_root.to_owned()));
        }
        let marker_path = self.mount_point.join(WORKSPACE_MARKER_PATH);
        let mut marker = WorkspaceMarker::read_from(&marker_path)?;
        let sidecar = sidecar_path(&self.image);
        let mut metadata = DetachedWorkspaceMetadata::read_for_image(&self.image)?;
        let snapshot_root = metadata
            .info_snapshot
            .as_ref()
            .map(|info| info.project_root.clone());
        if marker.project_root == project_root && snapshot_root.as_deref() == Some(project_root) {
            return Ok(false);
        }
        marker.project_root = project_root.to_owned();
        marker.validate()?;
        write_json(&marker_path, &marker)?;
        if let Some(info) = metadata.info_snapshot.as_mut() {
            info.project_root = project_root.to_owned();
        }
        metadata.validate(&self.image)?;
        write_json(&sidecar, &metadata)?;
        Ok(true)
    }

    /// The project root the record currently names, read from the marker.
    pub fn recorded_project_root(&self) -> Result<PathBuf, MetadataError> {
        WorkspaceMarker::read_from(&self.mount_point.join(WORKSPACE_MARKER_PATH))
            .map(|marker| marker.project_root)
    }
}

/// Point `destination` at `target` before `source` stops pointing at it.
///
/// Under the symlink layout the checkout is a symlink and the mount never moves, so the move is
/// nothing but this relink — and creating the new link before removing the old one makes it
/// gapless for free: there is no instant at which neither path resolves to the tree. A crash
/// between the two steps leaves both, which is a harmless extra alias rather than a lost checkout.
pub fn relink_checkout(source: &Path, destination: &Path, target: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, destination)?;
    fs::remove_file(source)
}

/// Does `path` name the same directory as `mount_point`?
///
/// Both sides are resolved, so a symlinked checkout matches the mount it points at. A path that
/// cannot be resolved does not match: an unresolvable checkout is a repair case, never a silent
/// equality.
pub fn resolves_to(path: &Path, mount_point: &Path) -> bool {
    match (fs::canonicalize(path), fs::canonicalize(mount_point)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// The checkout path as the caller names it, found by walking up from `observed`.
///
/// The deepest ancestor that resolves to main's mount is the answer: under the symlink layout that
/// is the user's symlink, under direct mount it is the mountpoint itself. Walking from the bottom
/// matters — a nested symlink chain can have several matching ancestors, and only the innermost
/// one is the checkout root rather than something above it that happens to resolve there too.
pub fn observed_checkout(observed: &Path, mount_point: &Path) -> Option<PathBuf> {
    let mut candidate = Some(observed);
    while let Some(path) = candidate {
        if resolves_to(path, mount_point) {
            return Some(path.to_owned());
        }
        candidate = path.parent();
    }
    None
}

/// The layout an observed checkout path actually exhibits.
///
/// A symlink at the checkout path is the symlink layout by construction; anything else that
/// resolves to the mount is the mount itself, which is direct mount. This is an observation, not a
/// preference — it is what makes a hand-rearranged checkout converge onto a truthful record.
pub fn observed_layout(checkout: &Path) -> CheckoutLayout {
    match fs::symlink_metadata(checkout) {
        Ok(metadata) if metadata.file_type().is_symlink() => CheckoutLayout::Symlink,
        _ => CheckoutLayout::DirectMount,
    }
}
/// Read an adopted project's layout, upgrading the pre-record layout in place.
///
/// Direct mount is the only supported layout for newly adopted projects and was also the physical
/// layout used by projects adopted before this record existed. Only absence is migratable:
/// malformed or unsupported records are evidence we cannot safely reinterpret and remain errors.
/// [`write_json`] publishes the version-one record atomically; a later observation only reads it.
pub fn load_checkout_layout(path: &Path) -> Result<CheckoutLayout, MetadataError> {
    match read_json::<CheckoutLayoutRecord>(path) {
        Ok(record) => {
            record.validate()?;
            Ok(record.checkout_layout)
        }
        Err(MetadataError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            let layout = CheckoutLayout::DirectMount;
            write_json(path, &CheckoutLayoutRecord::new(layout))?;
            Ok(layout)
        }
        Err(error) => Err(error),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        ImageFormat, METADATA_VERSION, Platform, PublicationState, WorkspaceIncarnation,
        WorkspaceInfoSnapshot, WorkspaceName, WorkspaceRole,
    };
    use crate::repository::RepoId;

    struct TempDirectory(PathBuf);

    impl TempDirectory {
        fn new(test: &str) -> Self {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "cowshed-checkout-{test}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("temp directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn incarnation() -> WorkspaceIncarnation {
        WorkspaceIncarnation::new("00000000000000000000000000000001").expect("incarnation")
    }

    fn fixture(root: &Path, project_root: &Path) -> CheckoutRecord {
        let mount_point = root.join("mount");
        let image = root.join("main.asif");
        let repo_id = RepoId::parse("acme/widget").expect("repo");
        let workspace = WorkspaceName::new("main").expect("main");
        let marker_path = mount_point.join(WORKSPACE_MARKER_PATH);
        fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("marker directory");
        write_json(
            &marker_path,
            &WorkspaceMarker {
                version: METADATA_VERSION,
                repo_id: repo_id.clone(),
                project_root: project_root.to_owned(),
                workspace: workspace.clone(),
                workspace_incarnation: incarnation(),
                role: WorkspaceRole::Main,
                image_format: ImageFormat::Asif,
                base_commit: "0123456789abcdef".to_owned(),
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                created_trace: "fixture".to_owned(),
                lineage: Some(Vec::new()),
            },
        )
        .expect("write marker");
        fs::write(&image, b"image").expect("image");
        write_json(
            &sidecar_path(&image),
            &DetachedWorkspaceMetadata {
                version: METADATA_VERSION,
                repo_id,
                workspace,
                workspace_incarnation: incarnation(),
                image_format: ImageFormat::Asif,
                platform: Platform::Macos,
                publication_state: PublicationState::Active,
                updated_at: "2026-07-13T00:00:00Z".to_owned(),
                grants: crate::metadata::GrantSet::closed_baseline(Some(
                    crate::metadata::PortBlock::new(49152, 16).expect("port block"),
                ))
                .expect("grants"),
                info_snapshot: Some(WorkspaceInfoSnapshot {
                    project_root: project_root.to_owned(),
                    role: WorkspaceRole::Main,
                    base_commit: "0123456789abcdef".to_owned(),
                    branch: None,
                    created_at: "2026-07-13T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-13T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
                }),
            },
        )
        .expect("write sidecar");
        CheckoutRecord { mount_point, image }
    }

    #[test]
    fn rewriting_the_project_root_moves_marker_and_sidecar_together_and_is_idempotent() {
        let temp = TempDirectory::new("rewrite");
        let record = fixture(temp.path(), Path::new("/old/checkout"));

        assert!(
            record
                .rewrite_project_root(Path::new("/new/checkout"))
                .expect("rewrite")
        );
        assert_eq!(
            record.recorded_project_root().expect("recorded"),
            Path::new("/new/checkout")
        );
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&record.image)
                .expect("sidecar")
                .require_info_snapshot()
                .expect("snapshot")
                .project_root,
            Path::new("/new/checkout")
        );

        assert!(
            !record
                .rewrite_project_root(Path::new("/new/checkout"))
                .expect("rewrite again"),
            "a record that already agrees is left untouched"
        );
    }

    #[test]
    fn a_relative_project_root_is_refused_before_either_record_is_touched() {
        let temp = TempDirectory::new("refuse-relative");
        let record = fixture(temp.path(), Path::new("/old/checkout"));

        assert!(matches!(
            record.rewrite_project_root(Path::new("relative/checkout")),
            Err(MetadataError::InvalidPath(_))
        ));
        assert_eq!(
            record.recorded_project_root().expect("recorded"),
            Path::new("/old/checkout")
        );
    }

    #[test]
    fn relinking_creates_the_destination_before_it_removes_the_source() {
        let temp = TempDirectory::new("relink");
        let target = temp.path().join("mount");
        fs::create_dir(&target).expect("target");
        let source = temp.path().join("old");
        let destination = temp.path().join("new");
        std::os::unix::fs::symlink(&target, &source).expect("source link");

        relink_checkout(&source, &destination, &target).expect("relink");

        assert!(!source.exists(), "the old alias is gone");
        assert_eq!(
            fs::read_link(&destination).expect("read link"),
            target,
            "the new alias points at the mount"
        );
    }

    #[test]
    fn relinking_onto_an_occupied_destination_leaves_the_source_alone() {
        let temp = TempDirectory::new("relink-occupied");
        let target = temp.path().join("mount");
        fs::create_dir(&target).expect("target");
        let source = temp.path().join("old");
        let destination = temp.path().join("new");
        std::os::unix::fs::symlink(&target, &source).expect("source link");
        fs::create_dir(&destination).expect("occupant");

        assert!(relink_checkout(&source, &destination, &target).is_err());
        assert!(
            fs::symlink_metadata(&source)
                .expect("source survives")
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn the_observed_checkout_is_the_deepest_ancestor_that_resolves_to_the_mount() {
        let temp = TempDirectory::new("observed");
        let mount = temp.path().join("mnt/acme/widget/main");
        fs::create_dir_all(mount.join("crates/core")).expect("tree");
        let checkout = temp.path().join("checkout");
        std::os::unix::fs::symlink(&mount, &checkout).expect("checkout link");

        assert_eq!(
            observed_checkout(&checkout.join("crates/core"), &mount),
            Some(checkout.clone())
        );
        assert_eq!(observed_checkout(&mount, &mount), Some(mount.clone()));
        assert_eq!(observed_checkout(temp.path(), &mount), None);
        assert_eq!(observed_layout(&checkout), CheckoutLayout::Symlink);
        assert_eq!(observed_layout(&mount), CheckoutLayout::DirectMount);
    }
    #[test]
    fn an_absent_layout_materializes_version_one_direct_mount() {
        let temp = TempDirectory::new("legacy-layout");
        let path = temp.path().join("checkout-layout.json");

        assert_eq!(
            load_checkout_layout(&path).expect("legacy layout"),
            CheckoutLayout::DirectMount
        );
        let record =
            crate::metadata::read_json::<crate::metadata::CheckoutLayoutRecord>(&path)
                .expect("materialized record");
        record.validate().expect("version one record");
        assert_eq!(record.checkout_layout, CheckoutLayout::DirectMount);
    }

    #[test]
    fn a_malformed_present_layout_fails_closed() {
        let temp = TempDirectory::new("malformed-layout");
        let path = temp.path().join("checkout-layout.json");
        fs::write(&path, b"{not json").expect("malformed record");

        assert!(matches!(
            load_checkout_layout(&path),
            Err(MetadataError::Json { .. })
        ));
        assert_eq!(
            fs::read(&path).expect("malformed record remains"),
            b"{not json"
        );
    }

    #[test]
    fn materializing_a_legacy_layout_is_idempotent() {
        use std::os::unix::fs::MetadataExt as _;

        let temp = TempDirectory::new("idempotent-layout");
        let path = temp.path().join("checkout-layout.json");
        load_checkout_layout(&path).expect("first observation");
        let inode = fs::metadata(&path).expect("first record").ino();

        assert_eq!(
            load_checkout_layout(&path).expect("second observation"),
            CheckoutLayout::DirectMount
        );
        assert_eq!(
            fs::metadata(&path).expect("same record").ino(),
            inode,
            "a successful read must not replace the explicit record"
        );
    }

}
