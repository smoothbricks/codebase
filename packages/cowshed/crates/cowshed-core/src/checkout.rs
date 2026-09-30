//! The project checkout path: where it is recorded, and how it moves.
//!
//! Main mounts at the project's checkout path itself, and that path is written down in two
//! independent places, neither derivable from the other:
//!
//! - the **in-image marker** (`.cowshed/workspace.json` at main's mount root), which is what a
//!   cold controller reads to answer "which repository is this directory";
//! - the **detached sidecar** beside main's canonical image, whose `infoSnapshot.projectRoot` is
//!   what the gateway inventory scans to answer the same question without mounting anything.
//!
//! Every operation that changes where the checkout lives has to move both together, so they are
//! moved by the functions here rather than open-coded at each call site.

use std::fs;
use std::path::{Path, PathBuf};

use crate::metadata::{
    DetachedWorkspaceMetadata, MetadataError, WorkspaceMarker, sidecar_path, write_json,
};
use crate::repository::RepoId;
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
            return Err(MetadataError::InvalidPath {
                path: project_root.to_owned(),
                reason: "path is not absolute",
            });
        }
        let marker_path = self.mount_point.join(WORKSPACE_MARKER_PATH);
        let mut marker = WorkspaceMarker::read_from(&marker_path)?;
        let sidecar = sidecar_path(&self.image);
        let mut metadata = DetachedWorkspaceMetadata::read_for_image(&self.image)?;
        if marker.project_root == project_root
            && metadata.info_snapshot.project_root == project_root
        {
            return Ok(false);
        }
        marker.project_root = project_root.to_owned();
        marker.validate()?;
        write_json(&marker_path, &marker)?;
        metadata.info_snapshot.project_root = project_root.to_owned();
        metadata.validate(&self.image)?;
        write_json(&sidecar, &metadata)?;
        Ok(true)
    }
    /// Rewrite the recorded repository identity in both the marker and the sidecar.
    ///
    /// The marker is published first because a cold open uses it to identify the project. A
    /// journaled identity transaction retries the sidecar and path namespace until both records
    /// agree; writing the marker first keeps an interrupted update attributable to the target.
    pub fn rewrite_repo_id(&self, repo_id: &RepoId) -> Result<bool, MetadataError> {
        let marker_path = self.mount_point.join(WORKSPACE_MARKER_PATH);
        let mut marker = WorkspaceMarker::read_from(&marker_path)?;
        let sidecar = sidecar_path(&self.image);
        let mut metadata = DetachedWorkspaceMetadata::read_for_image(&self.image)?;
        if marker.repo_id == *repo_id && metadata.repo_id == *repo_id {
            return Ok(false);
        }
        marker.repo_id = repo_id.clone();
        marker.validate()?;
        write_json(&marker_path, &marker)?;
        metadata.repo_id = repo_id.clone();
        metadata.validate(&self.image)?;
        write_json(&sidecar, &metadata)?;
        Ok(true)
    }

    /// Rewrite the store-side repository identity while this workspace is detached.
    pub fn rewrite_detached_repo_id(&self, repo_id: &RepoId) -> Result<bool, MetadataError> {
        let sidecar = sidecar_path(&self.image);
        let mut metadata = DetachedWorkspaceMetadata::read_for_image(&self.image)?;
        if metadata.repo_id == *repo_id {
            return Ok(false);
        }
        metadata.repo_id = repo_id.clone();
        metadata.validate(&self.image)?;
        write_json(&sidecar, &metadata)?;
        Ok(true)
    }

    /// Rewrite the store-side checkout fact while main is detached.
    ///
    /// A missing direct-mount checkout has no readable in-image marker by definition. Its detached
    /// sidecar is still the registry authority and is published atomically, so moving that fact
    /// forward lets the image be mounted at the destination. Once mounted,
    /// [`Self::rewrite_project_root`] updates the marker and confirms both copies agree.
    pub fn rewrite_detached_project_root(
        &self,
        project_root: &Path,
    ) -> Result<bool, MetadataError> {
        if !project_root.is_absolute() {
            return Err(MetadataError::InvalidPath {
                path: project_root.to_owned(),
                reason: "path is not absolute",
            });
        }
        let sidecar = sidecar_path(&self.image);
        let mut metadata = DetachedWorkspaceMetadata::read_for_image(&self.image)?;
        let snapshot = &mut metadata.info_snapshot;
        if snapshot.project_root == project_root {
            return Ok(false);
        }
        snapshot.project_root = project_root.to_owned();
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

/// Does `path` name the same directory as `mount_point`?
///
/// Both sides are resolved, so a checkout spelt differently — another case, a firmlinked parent —
/// matches the mount it is. A path that cannot be resolved does not match: an unresolvable
/// checkout is a repair case, never a silent equality.
pub fn resolves_to(path: &Path, mount_point: &Path) -> bool {
    match (fs::canonicalize(path), fs::canonicalize(mount_point)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// The checkout path as the caller names it, found by walking up from `observed`.
///
/// The deepest ancestor that resolves to main's mount and is not a symlink is the answer: main
/// mounts at the checkout itself, so a symlink the user made to it elsewhere is an alias of the
/// checkout, never the checkout, and recording one would move main's mountpoint onto a symlink.
/// Walking from the bottom matters — only the innermost match is the checkout root rather than
/// something above it that happens to resolve there too.
pub fn observed_checkout(observed: &Path, mount_point: &Path) -> Option<PathBuf> {
    let mut candidate = Some(observed);
    while let Some(path) = candidate {
        if resolves_to(path, mount_point) {
            return fs::symlink_metadata(path)
                .is_ok_and(|metadata| !metadata.file_type().is_symlink())
                .then(|| path.to_owned());
        }
        candidate = path.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        MARKER_VERSION, Platform, PublicationState, SIDECAR_VERSION, WorkspaceIncarnation,
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
                version: MARKER_VERSION,
                repo_id: repo_id.clone(),
                project_root: project_root.to_owned(),
                workspace: workspace.clone(),
                workspace_incarnation: incarnation(),
                role: WorkspaceRole::Main,
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
                version: SIDECAR_VERSION,
                repo_id,
                workspace,
                workspace_incarnation: incarnation(),
                platform: Platform::Macos,
                publication_state: PublicationState::Active,
                updated_at: "2026-07-13T00:00:00Z".to_owned(),
                grants: crate::metadata::GrantSet::closed_baseline(Some(
                    crate::metadata::PortBlock::new(49_136, 16).expect("port block"),
                ))
                .expect("grants"),
                info_snapshot: WorkspaceInfoSnapshot {
                    project_root: project_root.to_owned(),
                    role: WorkspaceRole::Main,
                    base_commit: "0123456789abcdef".to_owned(),
                    branch: None,
                    created_at: "2026-07-13T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-13T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
                },
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
                .info_snapshot
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
    fn a_missing_direct_mount_rewrites_the_detached_sidecar_without_opening_the_old_path() {
        let temp = TempDirectory::new("detached-rewrite");
        let record = fixture(temp.path(), Path::new("/old/checkout"));
        fs::remove_dir_all(&record.mount_point).expect("remove old direct mount");

        assert!(
            record
                .rewrite_detached_project_root(Path::new("/new/checkout"))
                .expect("rewrite detached record")
        );
        assert!(!record.mount_point.exists());
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&record.image)
                .expect("sidecar")
                .info_snapshot
                .project_root,
            Path::new("/new/checkout")
        );
    }

    #[test]
    fn a_relative_project_root_is_refused_before_either_record_is_touched() {
        let temp = TempDirectory::new("refuse-relative");
        let record = fixture(temp.path(), Path::new("/old/checkout"));

        let error = record
            .rewrite_project_root(Path::new("relative/checkout"))
            .expect_err("relative project root");
        assert_eq!(
            error.to_string(),
            "invalid metadata path relative/checkout: path is not absolute"
        );
        assert_eq!(
            record.recorded_project_root().expect("recorded"),
            Path::new("/old/checkout")
        );
    }

    /// Main mounts at the checkout itself, so the checkout a caller stands in is the mount reached
    /// through its own path — spelt however the caller spelt it — and never a symlink the user
    /// made to it elsewhere: a symlink cannot be the mountpoint, and recording one would move
    /// main's mountpoint onto it.
    #[test]
    fn the_observed_checkout_is_the_mount_itself_never_a_symlink_to_it() {
        let temp = TempDirectory::new("observed");
        let mount = temp.path().join("checkout");
        fs::create_dir_all(mount.join("crates/core")).expect("tree");
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&mount, &alias).expect("user symlink");

        assert_eq!(
            observed_checkout(&mount.join("crates/core"), &mount),
            Some(mount.clone())
        );
        assert_eq!(observed_checkout(&alias.join("crates/core"), &mount), None);
        assert_eq!(observed_checkout(temp.path(), &mount), None);
    }
}
