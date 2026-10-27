//! Explicit abandonment of a recorded project, independent of its checkout and volatile owners.
//! The record fences admission; supervisor/socket and file leases prove quiescence. Nothing here
//! guesses whether a PID is stale, follows a checkout path, or deletes preserved image/Git bytes.

#[cfg(target_os = "macos")]
mod macos {
    use std::collections::BTreeSet;
    use std::fs::{self, File, OpenOptions};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use cowshed_apfs::{
        CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend, SystemCommandRunner,
    };
    use cowshed_os::fork_lock::Fenced;

    use crate::metadata::WorkspaceName;
    use crate::project_retirement::{
        ProjectRetirementReport, RetirementIntent, RetirementPhase, acquire_collector,
        acquire_retirement_run,
    };
    use crate::repository::{RepoId, SESSIONS_DIRECTORY};
    use crate::runtime::{project::require_lost_groups_released, supervisor_socket};
    use crate::storage::bootstrap::ValidatedHostStorage;
    use crate::storage::lifecycle::dispatch_blocking;
    use crate::storage::recovery::{IntentLease, LIFECYCLE_INTENTS_FILE, LifecycleIntentJournal};
    use crate::{CowshedError, ErrorCode, Result};

    const DRAIN_BOUND: Duration = Duration::from_secs(30);

    struct Image {
        path: PathBuf,
        device: u64,
        inode: u64,
    }

    struct Contents {
        workspaces: BTreeSet<WorkspaceName>,
        images: Vec<Image>,
    }

    fn io_error(operation: &str, path: &Path, error: std::io::Error) -> CowshedError {
        CowshedError::environment_missing(
            format!("{operation} {}: {error}", path.display()),
            "retain the retirement record and retry after resolving the named storage error",
        )
    }

    /// Store images and workspace names come from durable namespace entries, never volatile roots.
    /// Symlinks are retained as evidence but never traversed to acquire authority outside the tree.
    fn contents(project: &Path) -> Result<Contents> {
        let mut found = Contents {
            workspaces: BTreeSet::from([WorkspaceName::main()]),
            images: Vec::new(),
        };
        let sessions = project.join(SESSIONS_DIRECTORY);
        let mut pending = vec![project.to_owned()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory)
                .map_err(|error| io_error("read project tree", &directory, error))?
            {
                let entry =
                    entry.map_err(|error| io_error("read project entry", &directory, error))?;
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path)
                    .map_err(|error| io_error("inspect project entry", &path, error))?;
                if metadata.is_dir() {
                    pending.push(path);
                    continue;
                }
                if !metadata.is_file() {
                    continue;
                }
                // Every RAW image in the tree: workspaces and retained re-adoption inputs.
                if cowshed_apfs::image::is_image_path(&path) {
                    if directory == sessions {
                        let name =
                            path.file_stem()
                                .and_then(|name| name.to_str())
                                .ok_or_else(|| {
                                    CowshedError::integrity(
                                        format!(
                                            "workspace image {} has no UTF-8 name",
                                            path.display()
                                        ),
                                        "retain the project and repair its record",
                                    )
                                })?;
                        found
                            .workspaces
                            .insert(WorkspaceName::new(name).map_err(|error| {
                                CowshedError::integrity(
                                    format!("invalid recorded workspace {name}: {error}"),
                                    "retain the project and repair its record",
                                )
                            })?);
                    }
                    found.images.push(Image {
                        path,
                        device: metadata.dev(),
                        inode: metadata.ino(),
                    });
                }
            }
        }
        let journal = LifecycleIntentJournal::load(&project.join(LIFECYCLE_INTENTS_FILE))?;
        for (workspace, _) in journal.records() {
            found.workspaces.insert(workspace.clone());
        }
        found
            .images
            .sort_by(|left, right| left.path.cmp(&right.path));
        Ok(found)
    }

    async fn drain_workspace(
        store: &Path,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<supervisor_socket::BoundSocket> {
        let path = supervisor_socket::socket_path(store, repo, workspace);
        let present = match supervisor_socket::hello_if_present(&path).await {
            Ok(Some(hello)) => {
                if hello.authority.repo_id != *repo || hello.authority.workspace != *workspace {
                    return Err(CowshedError::integrity(
                        format!(
                            "retirement socket {} serves another workspace",
                            path.display()
                        ),
                        "retain both projects and repair supervisor identity",
                    ));
                }
                true
            }
            Ok(None) => false,
            // Drain is the existing cross-build protocol, bound to the kernel socket peer.
            Err(error) if error.code == ErrorCode::Conflict => true,
            Err(error) => return Err(error),
        };
        if present {
            let drained = supervisor_socket::drain(&path).await?;
            let exited = dispatch_blocking(move || drained.exit.within(DRAIN_BOUND))
                .await
                .map_err(|error| {
                    CowshedError::internal(format!("waiting for supervisor exit: {error}"))
                })?
                .map_err(|error| io_error("observe drained supervisor", &path, error))?;
            if !exited {
                return Err(CowshedError::conflict(
                    format!(
                        "workspace {workspace} has not finished draining after {} seconds; retirement remains fenced",
                        DRAIN_BOUND.as_secs()
                    ),
                    format!(
                        "let its owned jobs finish, then retry: cowshed forget {repo} --abandon"
                    ),
                ));
            }
        }
        let bound = supervisor_socket::bind(&path).await?;
        require_lost_groups_released(workspace, &bound, "forget").await?;
        Ok(bound)
    }

    /// Hold the same no-follow shared inode lock used by detached image capture. The driver takes
    /// an exclusive lock, so no late external attach can occupy an image between detach and rename.
    fn hold_detached(image: &Image) -> Result<Fenced<File>> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_SHLOCK | libc::O_NONBLOCK | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&image.path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    CowshedError::conflict(
                        format!(
                            "image {} is attached or exclusively held; it was not archived",
                            image.path.display()
                        ),
                        "let the image owner release it, then retry the recorded retirement",
                    )
                } else {
                    io_error("hold detached image", &image.path, error)
                }
            })?;
        let metadata = file
            .metadata()
            .map_err(|error| io_error("read held image identity", &image.path, error))?;
        if !metadata.is_file() || metadata.dev() != image.device || metadata.ino() != image.inode {
            return Err(CowshedError::conflict(
                format!(
                    "image {} changed identity during retirement",
                    image.path.display()
                ),
                "retain the retirement record and retry after the competing image operation finishes",
            ));
        }
        file.sync_all()
            .map_err(|error| io_error("flush preserved image", &image.path, error))?;
        Ok(Fenced::new(file))
    }

    pub async fn forget_project(
        storage: ValidatedHostStorage,
        repo_id: RepoId,
    ) -> Result<ProjectRetirementReport> {
        let store = storage.store().to_owned();
        let authorize_store = store.clone();
        let authorize_repo = repo_id.clone();
        let intent = dispatch_blocking(move || {
            RetirementIntent::authorize(&authorize_store, &authorize_repo)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("authorizing project retirement: {error}"))
        })??;
        execute(storage, intent).await
    }

    pub(super) async fn execute(
        storage: ValidatedHostStorage,
        mut intent: RetirementIntent,
    ) -> Result<ProjectRetirementReport> {
        let store = storage.store().to_owned();
        let repo_id = intent.repo_id.clone();
        let _run = acquire_retirement_run(&store, &intent)?;
        intent.verify_registration(&store)?;
        if intent.phase == RetirementPhase::Archived {
            let archive = intent.archive_path(&store);
            let images = contents(&archive)?.images.len();
            return Ok(ProjectRetirementReport {
                repo_id,
                archive,
                images,
            });
        }
        let project = intent.project_path(&store);
        // Recovery of the one crash window after the rename and before recording Archived.
        if !project
            .try_exists()
            .map_err(|error| io_error("inspect recorded project", &project, error))?
        {
            let collector = acquire_collector(&store, &repo_id)?;
            intent.archive_project(&store, &collector)?;
            let archive = intent.archive_path(&store);
            let images = contents(&archive)?.images.len();
            return Ok(ProjectRetirementReport {
                repo_id,
                archive,
                images,
            });
        }
        let before = contents(&project)?;
        let mut sockets = Vec::with_capacity(before.workspaces.len());
        for workspace in &before.workspaces {
            sockets.push(drain_workspace(&store, &repo_id, workspace).await?);
        }
        // This refuses a controller that still owns the project even after its supervisors ended.
        // No process-table absence or vanished fixture marker substitutes for the kernel lease.
        let collector = acquire_collector(&store, &repo_id)?;
        intent.verify_registration(&store)?;
        let found = contents(&project)?;
        if found.workspaces != before.workspaces {
            return Err(CowshedError::conflict(
                "the project's recorded workspace set changed while it drained; nothing was detached",
                format!("cowshed forget {repo_id} --abandon"),
            ));
        }
        let mut lifecycle_leases = Vec::with_capacity(found.workspaces.len());
        for workspace in &found.workspaces {
            let lease = IntentLease::try_claim(&project.join(SESSIONS_DIRECTORY), workspace)?
                .ok_or_else(|| {
                    CowshedError::conflict(
                        format!("workspace {workspace} still owns a lifecycle operation"),
                        format!("cowshed forget {repo_id} --abandon"),
                    )
                })?;
            lifecycle_leases.push(lease);
        }
        if intent.phase == RetirementPhase::Fenced {
            intent.phase = RetirementPhase::Drained;
            intent.persist(&store)?;
        }
        let image_count = found.images.len();
        let detached_store = store.clone();
        let detached_project = project.clone();
        let runner = SystemCommandRunner::new(storage.namespace());
        let detached = dispatch_blocking(move || -> Result<_> {
            let backend = MacOsApfsBackend::new(runner);
            let mut holds = Vec::with_capacity(found.images.len());
            for image in &found.images {
                backend.release_image(&image.path, DetachIntent::WhenIdle).map_err(|error| {
                    let message = format!("unforced release of {}: {error}", image.path.display());
                    if cowshed_apfs::detach_was_dissented(&error) {
                        CowshedError::conflict(message, "close the named holders, then retry the recorded retirement")
                    } else {
                        CowshedError::storage_failure(message, &error, "resolve the storage error, then retry the recorded retirement")
                    }
                })?;
                holds.push(hold_detached(image)?);
            }
            let remaining = runner.attached_disk_images()
                .map_err(|error| io_error("read image inventory after retirement detach", &detached_store, error))?;
            if let Some(image) = remaining.iter().find(|image| matches!(&image.source, DiskImageSource::File(path) if path.starts_with(&detached_project))) {
                return Err(CowshedError::conflict(
                    format!("project still has an attached image after unforced release: {:?}", image.source),
                    "retain its retirement record and resolve the attachment before retrying",
                ));
            }
            Ok(holds)
        }).await.map_err(|error| CowshedError::internal(format!("detaching retired project images: {error}")))??;
        if intent.phase == RetirementPhase::Drained {
            intent.phase = RetirementPhase::Detached;
            intent.persist(&store)?;
        }
        intent.archive_project(&store, &collector)?;
        let archive = intent.archive_path(&store);
        // All guards outlive the archive commit: image holds, lifecycle claims and socket leases.
        drop((detached, lifecycle_leases, sockets, collector));
        Ok(ProjectRetirementReport {
            repo_id,
            archive,
            images: image_count,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::project_retirement::acquire_project_reader;
        use crate::storage::bootstrap::CanonicalRoots;
        use cowshed_os::namespace::TestNamespace;

        fn recorded(
            root: &Path,
            namespace: TestNamespace,
            repo: &RepoId,
        ) -> (ValidatedHostStorage, PathBuf) {
            let store = root.join("s");
            let project = crate::storage::StorageLayout::new(&store, repo)
                .unwrap()
                .project()
                .project_root
                .clone();
            fs::create_dir_all(&project).unwrap();
            // Deliberately corrupt registration and no checkout/volatile owner marker.
            fs::write(project.join("repository.json"), b"retained corrupt binding").unwrap();
            fs::write(project.join("evidence"), b"do not lose these bytes").unwrap();
            (
                ValidatedHostStorage::new(
                    root.join("home"),
                    Vec::new(),
                    CanonicalRoots::fixture(namespace, store),
                ),
                project,
            )
        }

        #[tokio::test]
        async fn missing_checkout_and_owner_marker_never_authorize_collection() {
            let root = crate::temp_root::TempRoot::new("pr");
            let repo = RepoId::parse("acme/missing").unwrap();
            let (storage, project) = recorded(
                &root,
                TestNamespace::mint().expect("a test namespace"),
                &repo,
            );
            let outcomes = super::super::resume_retirements(storage.clone())
                .await
                .unwrap();
            assert!(outcomes.is_empty());
            assert!(
                RetirementIntent::load(storage.store(), &repo)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                fs::read(project.join("evidence")).unwrap(),
                b"do not lose these bytes"
            );
        }

        #[tokio::test]
        async fn explicit_abandon_preserves_corrupt_registration_and_other_projects() {
            let root = crate::temp_root::TempRoot::new("pr");
            let repo = RepoId::parse("acme/abandoned").unwrap();
            let other = RepoId::parse("acme/active").unwrap();
            let namespace = TestNamespace::mint().expect("a test namespace");
            let (storage, project) = recorded(&root, namespace, &repo);
            // A failed mint may leave opaque partial image bytes, with no live disk attachment.
            fs::write(project.join("main.dmg"), b"partial image evidence").unwrap();
            let (_, other_project) = recorded(&root, namespace, &other);
            let _other_owner = acquire_project_reader(storage.store(), &other).unwrap();
            let report = forget_project(storage.clone(), repo.clone()).await.unwrap();
            assert_eq!(report.repo_id, repo);
            assert_eq!(report.images, 1);
            assert!(!project.exists());
            assert_eq!(
                fs::read(report.archive.join("repository.json")).unwrap(),
                b"retained corrupt binding"
            );
            assert_eq!(
                fs::read(report.archive.join("evidence")).unwrap(),
                b"do not lose these bytes"
            );
            assert_eq!(
                fs::read(report.archive.join("main.dmg")).unwrap(),
                b"partial image evidence"
            );
            assert_eq!(
                fs::read(other_project.join("evidence")).unwrap(),
                b"do not lose these bytes"
            );
            let repeated = forget_project(storage.clone(), repo.clone()).await.unwrap();
            assert_eq!(repeated, report);
            assert_eq!(
                RetirementIntent::load(storage.store(), &repo)
                    .unwrap()
                    .unwrap()
                    .phase,
                RetirementPhase::Archived
            );
        }

        #[tokio::test]
        async fn a_live_controller_blocks_archive_and_only_positive_intent_resumes() {
            let root = crate::temp_root::TempRoot::new("pr");
            let repo = RepoId::parse("acme/held").unwrap();
            let (storage, project) = recorded(
                &root,
                TestNamespace::mint().expect("a test namespace"),
                &repo,
            );
            let owner = acquire_project_reader(storage.store(), &repo).unwrap();
            let refusal = forget_project(storage.clone(), repo.clone())
                .await
                .unwrap_err();
            assert_eq!(refusal.code, ErrorCode::Conflict);
            assert!(project.exists());
            assert_eq!(
                RetirementIntent::load(storage.store(), &repo)
                    .unwrap()
                    .unwrap()
                    .phase,
                RetirementPhase::Fenced
            );
            assert!(acquire_project_reader(storage.store(), &repo).is_err());
            drop(owner);
            let mut outcomes = super::super::resume_retirements(storage).await.unwrap();
            assert_eq!(outcomes.len(), 1);
            let (retired, report) = outcomes.pop().unwrap();
            assert_eq!(retired, repo);
            let report = report.unwrap();
            assert_eq!(
                fs::read(report.archive.join("evidence")).unwrap(),
                b"do not lose these bytes"
            );
            assert!(!project.exists());
        }

        #[tokio::test]
        async fn real_apfs_forget_refuses_a_busy_image_then_preserves_it_detached() {
            use cowshed_apfs::{ApfsBackend, CommandRequest, MountAccess};
            let root = crate::scratch_apfs::ScratchRoot::new("pr").unwrap();
            let repo = RepoId::parse("acme/forget-native").unwrap();
            let (storage, project) = recorded(root.path(), root.namespace(), &repo);
            let runner = SystemCommandRunner::new(storage.namespace());
            let image = project.join("main.dmg");
            let count = || {
                let output = runner
                    .run(
                        &CommandRequest::new(
                            "/usr/sbin/ioclasscount",
                            [
                                "AppleDiskImageDevice",
                                "DIDeviceIOUserClient",
                                "AppleAPFSVolume",
                                "IOMedia",
                            ],
                        ),
                        "count image classes",
                    )
                    .unwrap()
                    .output;
                assert!(output.succeeded(), "{output:?}");
                String::from_utf8(output.stdout).unwrap()
            };
            cowshed_os::eprintln!("forget-native before: {}", count().trim());
            crate::blank_image::blank_image(root.namespace(), &image);
            let mount = root.path().join("mnt");
            fs::create_dir(&mount).unwrap();
            let backend = MacOsApfsBackend::new(runner);
            let attachment = backend.attach_verified(&image).unwrap();
            backend
                .mount(&attachment, &mount, MountAccess::ReadWrite, false)
                .unwrap();
            drop(attachment);
            fs::write(
                mount.join("retained-work"),
                b"owned work survives retirement",
            )
            .unwrap();
            let held = File::open(mount.join("retained-work")).unwrap();
            let busy = forget_project(storage.clone(), repo.clone())
                .await
                .unwrap_err();
            assert_eq!(busy.code, ErrorCode::Conflict, "{busy}");
            assert!(project.exists());
            assert!(runner.attached_disk_images().unwrap().iter().any(
                |entry| matches!(&entry.source, DiskImageSource::File(path) if path == &image)
            ));
            assert_eq!(
                RetirementIntent::load(storage.store(), &repo)
                    .unwrap()
                    .unwrap()
                    .phase,
                RetirementPhase::Drained
            );
            drop(held);
            let report = forget_project(storage.clone(), repo.clone()).await.unwrap();
            assert!(!project.exists());
            assert_eq!(report.images, 1);
            assert!(report.archive.join("main.dmg").is_file());
            assert_eq!(
                fs::read(report.archive.join("repository.json")).unwrap(),
                b"retained corrupt binding"
            );
            assert!(!runner.attached_disk_images().unwrap().iter().any(
                |entry| matches!(&entry.source, DiskImageSource::File(path) if path == &image || path.starts_with(&report.archive))
            ));
            cowshed_os::eprintln!("forget-native after: {}", count().trim());
        }
    }
}

#[cfg(target_os = "macos")]
pub use macos::forget_project;

#[cfg(not(target_os = "macos"))]
pub async fn forget_project(
    _storage: crate::storage::bootstrap::ValidatedHostStorage,
    _repo_id: crate::repository::RepoId,
) -> crate::Result<super::ProjectRetirementReport> {
    Err(crate::CowshedError::environment_missing(
        "record-driven project retirement requires the native macOS image substrate",
        "retain the project registration and run this operation on its macOS host",
    ))
}

/// Forward recovery only for existing positive authorizations. It never turns a missing record,
/// stale PID, vanished checkout or interrupted fixture into permission to abandon a project.
pub async fn resume_retirements(
    storage: crate::storage::bootstrap::ValidatedHostStorage,
) -> crate::Result<
    Vec<(
        crate::repository::RepoId,
        crate::Result<super::ProjectRetirementReport>,
    )>,
> {
    let store = storage.store().to_owned();
    let intents = crate::storage::lifecycle::dispatch_blocking(move || {
        super::outstanding_retirements(&store)
    })
    .await
    .map_err(|error| {
        crate::CowshedError::internal(format!("reading authorized project retirements: {error}"))
    })??;
    let mut outcomes = Vec::with_capacity(intents.len());
    for intent in intents {
        let repo_id = intent.repo_id.clone();
        #[cfg(target_os = "macos")]
        let outcome = macos::execute(storage.clone(), intent).await;
        #[cfg(not(target_os = "macos"))]
        let outcome = Err(crate::CowshedError::environment_missing(
            "recorded image retirement requires its native macOS host",
            "retain the explicit retirement record and resume it on that host",
        ));
        outcomes.push((repo_id, outcome));
    }
    Ok(outcomes)
}
