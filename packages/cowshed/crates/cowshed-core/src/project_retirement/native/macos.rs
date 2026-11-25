//! Record-driven retirement on the macOS image substrate: drain each workspace, detach and hold
//! its image, archive the project directory. A project kept on ZFS datasets has no image to
//! detach and hold, and is refused before anything is recorded.

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
            let entry = entry.map_err(|error| io_error("read project entry", &directory, error))?;
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
                                    format!("workspace image {} has no UTF-8 name", path.display()),
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
    census: &crate::census::CensusScope,
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
                format!("let its owned jobs finish, then retry: cowshed forget {repo} --abandon"),
            ));
        }
    }
    // A drained supervisor's exit is a release the binder waits out; with none present there
    // was none, and the binder probes as a first binder does.
    let bound = if present {
        supervisor_socket::bind_released(&path, census).await?
    } else {
        supervisor_socket::bind(&path).await?
    };
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

/// Refuse a project kept on ZFS datasets, before its retirement is authorized: this retirement
/// drains and archives disk images, and archiving a dataset project's directory would rename
/// its mount records away from datasets that stay mounted.
async fn require_image_project(storage: &ValidatedHostStorage, repo_id: &RepoId) -> Result<()> {
    let config = crate::gateway_inventory::unresolved_project_config(storage);
    let asked = repo_id.clone();
    let datasets = dispatch_blocking(move || {
        crate::storage::directory::DirectoryExecutionHost::for_datasets(config, &asked, None)
            .map(|host| host.is_some())
    })
    .await
    .map_err(|error| CowshedError::internal(format!("reading what {repo_id} is kept on: {error}")))?
    .map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot tell what {repo_id} is kept on: {error}"),
            "cowshed doctor --json",
        )
    })?;
    if datasets {
        return Err(CowshedError::environment_missing(
            format!(
                "record-driven project retirement drains and archives disk images, and \
                 {repo_id} is kept on ZFS datasets"
            ),
            "retain the project registration; cowshed unadopt, from its checkout, retires a \
             project kept on datasets",
        ));
    }
    Ok(())
}

pub async fn forget_project(
    storage: ValidatedHostStorage,
    repo_id: RepoId,
) -> Result<ProjectRetirementReport> {
    require_image_project(&storage, &repo_id).await?;
    let store = storage.store().to_owned();
    let authorize_store = store.clone();
    let authorize_repo = repo_id.clone();
    let census = storage.census();
    let intent = dispatch_blocking(move || {
        RetirementIntent::authorize(&authorize_store, &census, &authorize_repo)
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
    let census = storage.census();
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
        intent.archive_project(&store, &census, &collector)?;
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
        sockets.push(drain_workspace(&store, &repo_id, workspace, &census).await?);
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
        intent.persist(&store, &census)?;
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
        intent.persist(&store, &census)?;
    }
    intent.archive_project(&store, &census, &collector)?;
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
                CanonicalRoots::fixture(namespace, store, crate::calm_pressure::calm()),
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

    /// A lifecycle operation that holds a workspace's socket lease with no supervisor serving
    /// it refuses the drain at once: no supervisor was seen let the socket go, so the binder
    /// probes as a first binder does (11_shell.md "Protocol"), and the refusal is the probe's,
    /// with no waiter's expiry in it.
    #[tokio::test]
    async fn a_held_socket_lease_with_no_supervisor_refuses_the_drain_at_once() {
        let root = crate::temp_root::TempRoot::new("pr");
        let repo = RepoId::parse("acme/leased").unwrap();
        let (storage, _project) = recorded(
            &root,
            TestNamespace::mint().expect("a test namespace"),
            &repo,
        );
        let workspace = WorkspaceName::new("main").unwrap();
        let socket = supervisor_socket::socket_path(storage.store(), &repo, &workspace);
        let _held = supervisor_socket::lease_unserved(&socket)
            .unwrap()
            .expect("no supervisor serves the workspace");
        let refused = drain_workspace(storage.store(), &repo, &workspace, &storage.census())
            .await
            .expect_err("a held lease refuses the drain");
        assert_eq!(refused.code, ErrorCode::Conflict, "{refused:?}");
        assert_eq!(
            refused.message,
            format!(
                "a supervisor or lifecycle operation owns {}",
                socket.display()
            )
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
        use cowshed_apfs::{ApfsBackend, MountAccess};
        use std::os::unix::fs::MetadataExt as _;
        let root = crate::scratch_apfs::ScratchRoot::new("pr").unwrap();
        let repo = RepoId::parse("acme/forget-native").unwrap();
        let (storage, project) = recorded(root.path(), root.namespace(), &repo);
        let runner = SystemCommandRunner::new(storage.namespace());
        let image = project.join("main.dmg");
        crate::blank_image::blank_image(root.namespace(), &image);
        let image_inode = fs::metadata(&image).unwrap().ino();
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
        assert!(
            runner.attached_disk_images().unwrap().iter().any(
                |entry| matches!(&entry.source, DiskImageSource::File(path) if path == &image)
            )
        );
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
        let retained = fs::metadata(report.archive.join("main.dmg")).unwrap();
        assert!(retained.is_file());
        assert_eq!(
            retained.ino(),
            image_inode,
            "forget archives the original backing file"
        );
        assert_eq!(
            fs::read(report.archive.join("repository.json")).unwrap(),
            b"retained corrupt binding"
        );
        assert!(!runner.attached_disk_images().unwrap().iter().any(
            |entry| matches!(&entry.source, DiskImageSource::File(path) if path == &image || path.starts_with(&report.archive))
        ));
    }

    /// `forget` reconciles group ledgers of workspaces recorded only by retired or staged
    /// sidecars in the project tree (e.g. `sessions/.trash` and `.staging`): an unresolved group
    /// refuses the forget while retaining its ledger, and once released both ledgers are retired.
    #[tokio::test]
    async fn forget_reconciles_ledgers_of_retired_and_staged_workspace_sidecars() {
        use cowshed_os::fork_lock::Spawn as _;
        use cowshed_os::test_backstop::ChildBackstop as _;
        use std::io::{BufRead as _, Read as _};
        use std::os::unix::process::CommandExt as _;

        struct HeldGroup {
            pgid: i32,
            release: Option<std::process::ChildStdin>,
            output: Option<std::io::BufReader<std::process::ChildStdout>>,
        }
        impl Drop for HeldGroup {
            fn drop(&mut self) {
                drop(self.release.take());
            }
        }

        let root = crate::temp_root::TempRoot::new("pr");
        let repo = RepoId::parse("acme/forget-ledgers").unwrap();
        let (storage, project) = recorded(
            &root,
            TestNamespace::mint().expect("a test namespace"),
            &repo,
        );

        let write_sidecar = |image: &Path, ws: &str, inc: &str, state| {
            fs::create_dir_all(image.parent().unwrap()).unwrap();
            fs::write(image, b"partial image evidence").unwrap();
            crate::metadata::DetachedWorkspaceMetadata {
                version: crate::metadata::SIDECAR_VERSION,
                repo_id: repo.clone(),
                workspace: WorkspaceName::session(ws).unwrap(),
                workspace_incarnation: crate::metadata::WorkspaceIncarnation::new(inc).unwrap(),
                platform: crate::metadata::Platform::Macos,
                publication_state: state,
                updated_at: "2026-07-14T00:00:00Z".to_owned(),
                grants: crate::metadata::GrantSet::default(),
                info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                    project_root: PathBuf::from("/project"),
                    role: crate::metadata::WorkspaceRole::Workspace,
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    branch: None,
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-14T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
                },
            }
            .write_for_image(image)
            .unwrap();
        };

        write_sidecar(
            &project.join("sessions/.trash/retired-0198f2c0b7e34dc795f17b238b331c80.dmg"),
            "retired",
            "0198f2c0b7e34dc795f17b238b331c80",
            crate::metadata::PublicationState::Active,
        );
        write_sidecar(
            &project.join(".staging/staged-0198f2c0b7e34dc795f17b238b331c81.dmg"),
            "staged",
            "0198f2c0b7e34dc795f17b238b331c81",
            crate::metadata::PublicationState::PendingFence,
        );

        let ledger_of = |ws: &str| {
            crate::runtime::job_groups::ledger_path(&supervisor_socket::socket_path(
                storage.store(),
                &repo,
                &WorkspaceName::session(ws).unwrap(),
            ))
        };
        let retired_ledger = ledger_of("retired");
        let staged_ledger = ledger_of("staged");
        fs::create_dir_all(storage.store().join("run")).unwrap();
        crate::runtime::job_groups::Recorder::new(retired_ledger.clone())
            .record(std::iter::empty(), &[])
            .unwrap();

        let mut child = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "exec 3<&0; (printf 'READY\\n'; cat <&3; printf 'RELEASED\\n') & exit 0",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let birth = crate::runtime::job_groups::GroupLeader::observe(child.id()).unwrap();
        crate::runtime::job_groups::Recorder::new(staged_ledger.clone())
            .record(
                [crate::runtime::job_groups::JobGroupClaim::group(17, birth)].into_iter(),
                &[],
            )
            .unwrap();
        let release = child.stdin.take().unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        output.read_line(&mut ready).unwrap();
        assert_eq!(ready, "READY\n");
        assert!(child.wait_bounded().unwrap().success());
        let mut held = HeldGroup {
            pgid: birth.pgid(),
            release: Some(release),
            output: Some(output),
        };
        let taken = crate::runtime::job_groups::take_lost(&staged_ledger, Duration::ZERO).unwrap();
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].pgid(), held.pgid);

        let refused = forget_project(storage.clone(), repo.clone())
            .await
            .expect_err("unresolved group for staged workspace sidecar must refuse forget");
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert!(project.exists());
        assert!(staged_ledger.exists());
        assert_eq!(
            crate::runtime::job_groups::unresolved(&staged_ledger)
                .unwrap()
                .len(),
            1
        );

        drop(held.release.take());
        let mut tail = String::new();
        held.output
            .take()
            .unwrap()
            .read_to_string(&mut tail)
            .unwrap();
        assert_eq!(tail, "RELEASED\n");
        while !crate::runtime::job_groups::unresolved(&staged_ledger)
            .unwrap()
            .is_empty()
        {
            std::thread::yield_now();
        }

        let report = forget_project(storage.clone(), repo.clone())
            .await
            .expect("forget succeeds once unresolved group exits");
        assert!(!project.exists());
        assert_eq!(report.repo_id, repo);
        assert!(
            !retired_ledger.exists(),
            "forget retires empty ledger of retired trash sidecar"
        );
        assert!(
            !staged_ledger.exists(),
            "forget retires released ledger of staged sidecar"
        );
    }
}
