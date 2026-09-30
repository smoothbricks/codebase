//! A workspace answered from the live state its daemon already holds (06_cli.md "Resident
//! workspaces").
//!
//! `path` and `exec` name one workspace. When that workspace is mounted and its daemon-owned
//! supervisor already serves the workspace's current authority, every fact the two verbs depend
//! on is live: the mount, the supervisor's socket, and the authority each call names. Opening the
//! project controller re-derives all of it — host storage validation, the inventory of every
//! workspace in the project, the binding against Git's remotes — only to reach the same
//! supervisor.
//!
//! [`resolve`] reads the records that decide this one workspace's answer, each fresh on every
//! call and none kept, and declines whenever one of them disagrees with the live state. A
//! declined verb opens the controller exactly as before, so the controller still answers every
//! case that needs work done: a detached or unserved workspace, a grant the supervisor does not
//! serve yet, unfinished lifecycle work. Nothing here lists a directory: a project with a
//! thousand retired sessions costs what a project with one does.
//!
//! What a resident answer does not re-check is what the serving supervisor already proved when
//! it started: the project binding it opened under. A binding changes only through a cowshed
//! verb, which changes the records read here; a Git remote edited by hand is reconciled by the
//! next verb that opens the controller.

use std::path::{Path, PathBuf};

use crate::api::dto::GitOid;
use crate::metadata::{
    CheckoutLayoutRecord, DetachedWorkspaceMetadata, PublicationState, WorkspaceMarker,
    WorkspaceName, sidecar_path,
};
use crate::repository::RepoId;
use crate::runtime::supervisor::{WorkspaceAuthoritySnapshot, WorkspaceSupervisorHandle};
use crate::runtime::supervisor_socket::{self, Hello};
use crate::storage::{StorageLayout, WORKSPACE_MARKER_PATH};

/// Git's own discovery honours these; a caller that sets one is answered by Git, through the
/// controller, rather than by a walk that would not.
const GIT_DISCOVERY_ENVIRONMENT: [&str; 5] = [
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
];

/// Why a verb opens the controller instead of answering from live state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decline {
    /// The caller steers Git's repository discovery through its environment.
    GitEnvironment,
    /// No repository contains the invocation directory.
    NoRepository,
    /// The repository containing the invocation directory is not a mounted cowshed workspace.
    NotAWorkspace,
    /// A record this answer depends on is missing, unreadable, or disagrees with another.
    Records,
    /// A lifecycle operation on the workspace, on `main`, or on the project identity is
    /// unfinished; opening the controller is what finishes it.
    UnfinishedWork,
    /// A linked-worktree workspace, whose precondition on `main` the controller checks.
    GitWorktree,
    /// Nothing is mounted at the workspace's mount path.
    NotMounted,
    /// What is mounted there is not the workspace's active incarnation.
    StaleMount,
    /// No supervisor of this build answers the workspace's socket.
    Unserved,
    /// The supervisor serves another incarnation or grant revision than the records name.
    ServesOtherAuthority,
}

impl Decline {
    pub fn reason(self) -> &'static str {
        match self {
            Self::GitEnvironment => "git discovery is steered by the environment",
            Self::NoRepository => "no repository contains the invocation directory",
            Self::NotAWorkspace => "the invocation repository is not a mounted workspace",
            Self::Records => "a project record is missing or inconsistent",
            Self::UnfinishedWork => "lifecycle work is unfinished",
            Self::GitWorktree => "the workspace is a linked worktree",
            Self::NotMounted => "the workspace is not mounted",
            Self::StaleMount => "the mounted image is not the workspace's active incarnation",
            Self::Unserved => "no supervisor serves the workspace",
            Self::ServesOtherAuthority => "the supervisor serves another authority",
        }
    }
}

/// The host facts that are not records: the mount table and the supervisor's socket.
pub trait LiveProbe {
    /// Whether a filesystem is mounted exactly at `path`.
    fn mounted_at(&self, path: &Path) -> bool;
    /// Who serves the supervisor socket at `socket`.
    fn hello(&self, socket: &Path) -> impl Future<Output = crate::Result<Hello>>;
}

/// The running host.
pub struct HostProbe;

impl LiveProbe for HostProbe {
    fn mounted_at(&self, path: &Path) -> bool {
        mount_point_of(path).is_some_and(|mounted| mounted == path)
    }

    fn hello(&self, socket: &Path) -> impl Future<Output = crate::Result<Hello>> {
        supervisor_socket::hello(socket)
    }
}

/// Where the filesystem holding `path` is mounted.
#[cfg(target_os = "macos")]
fn mount_point_of(path: &Path) -> Option<PathBuf> {
    use std::ffi::{CStr, CString, OsStr};
    use std::os::unix::ffi::OsStrExt as _;

    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `statfs` is plain old data; all-zero is a valid value for the kernel to overwrite.
    let mut stats: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is NUL-terminated and `stats` is a valid, writable `statfs`.
    if unsafe { libc::statfs(path.as_ptr(), &mut stats) } != 0 {
        return None;
    }
    // SAFETY: Darwin NUL-terminates `f_mntonname` within its fixed-size array.
    let mounted = unsafe { CStr::from_ptr(stats.f_mntonname.as_ptr()) };
    Some(PathBuf::from(OsStr::from_bytes(mounted.to_bytes())))
}

/// Only the macOS runtime serves workspaces.
#[cfg(not(target_os = "macos"))]
fn mount_point_of(_path: &Path) -> Option<PathBuf> {
    None
}

/// A mounted workspace whose supervisor serves its current authority.
pub struct Resident {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub mount: PathBuf,
    pub base_commit: Option<GitOid>,
    /// Calls reach the serving supervisor under the authority it reported.
    pub supervisor: WorkspaceSupervisorHandle,
    socket: PathBuf,
}

impl Resident {
    pub fn authority(&self) -> &WorkspaceAuthoritySnapshot {
        self.supervisor.snapshot()
    }

    /// The socket the supervisor serves.
    pub fn socket(&self) -> &Path {
        &self.socket
    }
}

/// Follow the supervisor at `socket` to the authority it serves now, after it refused a call
/// naming `current`. A grant change advances a serving supervisor in place and keeps its jobs;
/// another incarnation is another workspace, which the caller's job does not belong to, so that
/// and an unchanged authority both answer `None`.
pub async fn follow(
    socket: &Path,
    current: &WorkspaceAuthoritySnapshot,
    probe: &impl LiveProbe,
) -> crate::Result<Option<WorkspaceSupervisorHandle>> {
    let hello = probe.hello(socket).await?;
    if hello.authority.repo_id != current.repo_id
        || hello.authority.workspace != current.workspace
        || hello.authority.workspace_incarnation != current.workspace_incarnation
        || hello.authority == *current
    {
        return Ok(None);
    }
    Ok(Some(supervisor_socket::connect(
        socket.to_path_buf(),
        hello.authority,
    )))
}

/// Answer `workspace` of the project containing `start` from live state, or say why not.
///
/// Every read names its file; none lists a directory. `store_root` is the store the daemon
/// serves (`STORE_ROOT` in production).
pub async fn resolve(
    store_root: &Path,
    start: &Path,
    workspace: &WorkspaceName,
    probe: &impl LiveProbe,
) -> Result<Resident, Decline> {
    if GIT_DISCOVERY_ENVIRONMENT
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        return Err(Decline::GitEnvironment);
    }
    let project_span = crate::timing::span("resident", "project");
    let root = repository_root(start).ok_or(Decline::NoRepository)?;
    // The invocation root names the project: main's checkout under direct mount, or any mounted
    // workspace, whose marker records that checkout.
    let origin = WorkspaceMarker::read_from(&root.join(WORKSPACE_MARKER_PATH))
        .map_err(|_| Decline::NotAWorkspace)?;
    let repo = origin.repo_id.clone();
    let layout = StorageLayout::new(store_root, &repo).map_err(|_| Decline::Records)?;
    let project = layout.project();

    // What opening the controller would finish first: an identity change of the store, and
    // any unfinished intent of the named workspace or of `main` (RecoveryScope::Workspaces).
    if crate::storage::recovery::RepositoryIdentityIntent::path(store_root)
        .symlink_metadata()
        .is_ok()
    {
        return Err(Decline::UnfinishedWork);
    }
    drop(project_span);
    let journal_span = crate::timing::span("resident", "journal");
    let journal = crate::storage::recovery::LifecycleIntentJournal::load(
        &project
            .project_root
            .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE),
    )
    .map_err(|_| Decline::Records)?;
    if [workspace, &WorkspaceName::main()].into_iter().any(|name| {
        journal
            .get(name)
            .is_some_and(|record| record.completion.is_none())
    }) {
        return Err(Decline::UnfinishedWork);
    }
    drop(journal_span);

    let workspace_span = crate::timing::span("resident", "workspace");
    let checkout = crate::metadata::read_json::<CheckoutLayoutRecord>(&project.checkout_layout)
        .map_err(|_| Decline::Records)?;
    let mount = layout
        .main_aware_workspace_mount(checkout.checkout_layout, &origin.project_root, workspace)
        .map_err(|_| Decline::Records)?;
    if !probe.mounted_at(&mount) {
        return Err(Decline::NotMounted);
    }
    let marker = WorkspaceMarker::read_from(&mount.join(WORKSPACE_MARKER_PATH))
        .map_err(|_| Decline::StaleMount)?;
    if marker.repo_id != repo
        || marker.workspace != *workspace
        || marker.project_root != origin.project_root
    {
        return Err(Decline::StaleMount);
    }
    let metadata = current_metadata(store_root, &layout, &marker)?;
    if metadata
        .info_snapshot
        .as_ref()
        .is_some_and(|info| info.git_worktree)
    {
        return Err(Decline::GitWorktree);
    }
    // The invocation root must itself be a live workspace, as the controller's origin check
    // requires; when it is the named workspace, the reads above already proved it.
    if origin.workspace != *workspace {
        current_metadata(store_root, &layout, &origin)?;
    }
    let base_commit = metadata
        .info_snapshot
        .as_ref()
        .map(|info| GitOid::new(info.base_commit.clone()))
        .transpose()
        .map_err(|_| Decline::Records)?;

    // The revision a supervisor serves covers the project's standing grants too.
    let policy = crate::project_policy::ProjectPolicy::read(&project.policy)
        .map_err(|_| Decline::Records)?;
    let effective = crate::project_policy::effective_grants(&metadata.grants, &policy.grants)
        .map_err(|_| Decline::Records)?;
    let needed = WorkspaceAuthoritySnapshot {
        repo_id: repo.clone(),
        workspace: workspace.clone(),
        workspace_incarnation: metadata.workspace_incarnation.clone(),
        grant_revision: effective.revision,
        // Not compared: the lifecycle revision a supervisor started under is its own, and every
        // call names the one it reports.
        lifecycle_revision: 0,
    };
    drop(workspace_span);
    let socket = supervisor_socket::socket_path(store_root, &repo, workspace);
    let hello = crate::timing::spanned("resident", "hello", probe.hello(&socket))
        .await
        .map_err(|_| Decline::Unserved)?;
    if !crate::runtime::supervisor_manager::serves(&hello.authority, &needed) {
        return Err(Decline::ServesOtherAuthority);
    }
    Ok(Resident {
        repo_id: repo,
        workspace: workspace.clone(),
        mount,
        base_commit,
        supervisor: supervisor_socket::connect(socket.clone(), hello.authority),
        socket,
    })
}

/// The nearest ancestor of `start` holding `.git`, as Git's discovery finds it: the walk stops
/// at a filesystem boundary, and the directory the boundary is crossed from is the last one
/// Git examines.
fn repository_root(start: &Path) -> Option<&Path> {
    use std::os::unix::fs::MetadataExt as _;

    let device = start.metadata().ok()?.dev();
    for directory in start.ancestors() {
        if directory.metadata().ok()?.dev() != device {
            return None;
        }
        if directory.join(".git").symlink_metadata().is_ok() {
            return Some(directory);
        }
    }
    None
}

/// The active sidecar of the incarnation `marker` names, read without following links.
fn current_metadata(
    store_root: &Path,
    layout: &StorageLayout,
    marker: &WorkspaceMarker,
) -> Result<DetachedWorkspaceMetadata, Decline> {
    let image = layout
        .canonical_image(&marker.workspace, marker.image_format)
        .map_err(|_| Decline::Records)?;
    let image = image.image();
    let sidecar = sidecar_path(image);
    crate::storage::verify_no_symlinks(store_root, &sidecar).map_err(|_| Decline::Records)?;
    image.symlink_metadata().map_err(|_| Decline::Records)?;
    let metadata =
        DetachedWorkspaceMetadata::read_for_image(image).map_err(|_| Decline::Records)?;
    if metadata.publication_state != PublicationState::Active
        || metadata.repo_id != marker.repo_id
        || metadata.workspace != marker.workspace
        || metadata.workspace_incarnation != marker.workspace_incarnation
        || metadata.image_format != marker.image_format
    {
        return Err(Decline::StaleMount);
    }
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        CheckoutLayout, GrantSet, ImageFormat, MARKER_VERSION, NEW_PORT_BLOCK_SIZE, Platform,
        PortBlock, SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceRole,
        write_json,
    };
    use crate::storage::recovery::{
        LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal,
    };
    use std::os::unix::fs::PermissionsExt as _;

    const RETIRED_SESSIONS: usize = 1000;

    /// A store holding main (mounted at its checkout) and `raven`, beside a thousand retired
    /// sessions whose leftover files are garbage. Every directory resolution could list is
    /// execute-only while the fixture is sealed: opening a named file works, listing fails.
    struct Store {
        root: PathBuf,
        store: PathBuf,
        checkout: PathBuf,
        raven_mount: PathBuf,
        layout: StorageLayout,
        sealed: Vec<PathBuf>,
    }

    fn repo() -> RepoId {
        RepoId::parse("acme/widget").expect("repo")
    }

    fn raven() -> WorkspaceName {
        WorkspaceName::new("raven").expect("workspace")
    }

    fn incarnation(value: u8) -> WorkspaceIncarnation {
        WorkspaceIncarnation::new(format!("{value:032x}")).expect("incarnation")
    }

    impl Store {
        fn new() -> Self {
            let root = std::fs::canonicalize(std::env::temp_dir())
                .expect("temp")
                .join(format!(
                    "cowshed-resident-{}",
                    uuid::Uuid::new_v4().simple()
                ));
            let store = root.join("store");
            let checkout = root.join("checkout");
            std::fs::create_dir_all(&store).expect("store");
            std::fs::create_dir_all(checkout.join(".git")).expect("checkout");
            let host = store.join("host.json");
            std::fs::write(
                &host,
                format!(
                    r#"{{"version":1,"mountRoot":"{}"}}"#,
                    root.join("mnt").display()
                ),
            )
            .expect("host config");
            std::fs::set_permissions(&host, std::fs::Permissions::from_mode(0o600))
                .expect("host config mode");
            let layout = StorageLayout::new(&store, &repo()).expect("layout");
            let project = layout.project().clone();
            std::fs::create_dir_all(&project.sessions).expect("sessions");
            write_json(
                &project.checkout_layout,
                &CheckoutLayoutRecord::new(CheckoutLayout::DirectMount),
            )
            .expect("checkout layout");
            let raven_mount = layout.workspace_mount(&raven()).expect("mount path");
            let fixture = Self {
                root,
                store,
                checkout,
                raven_mount,
                layout,
                sealed: Vec::new(),
            };
            fixture.workspace(&WorkspaceName::main(), &fixture.checkout, 1, 2);
            fixture.workspace(&raven(), &fixture.raven_mount, 2, 5);
            for index in 0..RETIRED_SESSIONS {
                let leftover = project
                    .sessions
                    .join(format!("retired-{index}.sparseimage"));
                std::fs::write(sidecar_path(&leftover), b"{ not a sidecar").expect("garbage");
                std::fs::write(
                    project
                        .sessions
                        .join(format!("retired-{index}.sparseimage.lock")),
                    b"",
                )
                .expect("lock");
            }
            fixture
        }

        fn workspace(&self, name: &WorkspaceName, mount: &Path, id: u8, revision: u64) {
            let image = self
                .layout
                .canonical_image(name, ImageFormat::Sparse)
                .expect("image")
                .image()
                .to_path_buf();
            std::fs::write(&image, b"").expect("image");
            let port_block = PortBlock::new(
                40_960 + u16::from(id) * NEW_PORT_BLOCK_SIZE,
                NEW_PORT_BLOCK_SIZE,
            )
            .expect("port block");
            let mut grants = GrantSet::closed_baseline(Some(port_block)).expect("grants");
            grants.revision = revision;
            DetachedWorkspaceMetadata {
                version: SIDECAR_VERSION,
                repo_id: repo(),
                workspace: name.clone(),
                workspace_incarnation: incarnation(id),
                image_format: ImageFormat::Sparse,
                platform: Platform::Macos,
                publication_state: PublicationState::Active,
                updated_at: "2026-07-14T00:00:00Z".to_owned(),
                grants,
                info_snapshot: Some(WorkspaceInfoSnapshot {
                    project_root: self.checkout.clone(),
                    role: WorkspaceRole::for_name(name),
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    branch: None,
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-14T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
                }),
            }
            .write_for_image(&image)
            .expect("sidecar");
            let marker = mount.join(WORKSPACE_MARKER_PATH);
            std::fs::create_dir_all(marker.parent().expect("marker parent")).expect("marker dir");
            write_json(
                &marker,
                &WorkspaceMarker {
                    version: MARKER_VERSION,
                    repo_id: repo(),
                    project_root: self.checkout.clone(),
                    workspace: name.clone(),
                    workspace_incarnation: incarnation(id),
                    role: WorkspaceRole::for_name(name),
                    image_format: ImageFormat::Sparse,
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    created_trace: "fixture".to_owned(),
                    lineage: Some(Vec::new()),
                },
            )
            .expect("marker");
        }

        /// Make every store directory unlistable, as `readdir` sees it.
        fn seal(&mut self) {
            let project = self.layout.project();
            for directory in [&project.sessions, &project.project_root, &self.store] {
                std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o300))
                    .expect("seal");
                assert!(
                    std::fs::read_dir(directory).is_err(),
                    "sealed directory lists"
                );
                self.sealed.push(directory.clone());
            }
        }

        fn begin_intent(&self, intent: LifecycleIntent) {
            LifecycleIntentJournal::update(
                &self
                    .layout
                    .project()
                    .project_root
                    .join(LIFECYCLE_INTENTS_FILE),
                |journal| {
                    journal.begin(intent);
                    Ok(())
                },
            )
            .expect("journal");
        }

        /// The authority a supervisor serving raven's current records reports.
        fn current(&self) -> WorkspaceAuthoritySnapshot {
            WorkspaceAuthoritySnapshot {
                repo_id: repo(),
                workspace: raven(),
                workspace_incarnation: incarnation(2),
                grant_revision: 5,
                lifecycle_revision: 3,
            }
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            for directory in &self.sealed {
                let _ = std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700));
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    /// The mount table and one supervisor, as the host would report them.
    struct Probe {
        mounted: Vec<PathBuf>,
        socket: PathBuf,
        serving: WorkspaceAuthoritySnapshot,
    }

    impl LiveProbe for Probe {
        fn mounted_at(&self, path: &Path) -> bool {
            self.mounted.iter().any(|mounted| mounted == path)
        }

        async fn hello(&self, socket: &Path) -> crate::Result<Hello> {
            if socket != self.socket {
                return Err(crate::CowshedError::environment_missing(
                    "no supervisor",
                    "start one",
                ));
            }
            Ok(Hello {
                authority: self.serving.clone(),
                pid: 1,
            })
        }
    }

    fn probe(store: &Store, serving: WorkspaceAuthoritySnapshot) -> Probe {
        Probe {
            mounted: vec![store.checkout.clone(), store.raven_mount.clone()],
            socket: supervisor_socket::socket_path(&store.store, &repo(), &raven()),
            serving,
        }
    }

    /// The regression guard for the resident verbs: answering one workspace reads that
    /// workspace's records by name and never lists the store, the project, or its sessions — so
    /// a project's retired sessions cannot make `path` or `exec` slower, and no enumeration can
    /// creep back in unnoticed.
    #[tokio::test]
    async fn a_resident_workspace_resolves_without_listing_any_store_directory() {
        let mut store = Store::new();
        store.seal();
        let serving = store.current();
        let resident = resolve(
            &store.store,
            &store.checkout,
            &raven(),
            &probe(&store, serving.clone()),
        )
        .await
        .expect("resident");
        assert_eq!(resident.mount, store.raven_mount);
        assert_eq!(resident.authority(), &serving);
    }

    /// The effective revision is the workspace's grant revision plus the project's: a
    /// supervisor that has not advanced past a grant change must not be answered from.
    #[tokio::test]
    async fn a_supervisor_behind_the_recorded_grants_is_not_served_from() {
        let store = Store::new();
        let serving = store.current();
        write_json(
            &store.layout.project().policy,
            &crate::project_policy::ProjectPolicy {
                grants: crate::project_policy::ProjectGrants {
                    revision: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .expect("policy");
        let declined = resolve(
            &store.store,
            &store.checkout,
            &raven(),
            &probe(&store, serving),
        )
        .await
        .err();
        assert_eq!(declined, Some(Decline::ServesOtherAuthority));
    }

    /// Unfinished lifecycle work on the named workspace or on main is the controller's to
    /// finish before any verb runs.
    #[tokio::test]
    async fn unfinished_work_on_the_workspace_or_main_opens_the_controller() {
        for target in [raven(), WorkspaceName::main()] {
            let store = Store::new();
            store.begin_intent(LifecycleIntent::Create {
                workspace: target,
                options: Default::default(),
            });
            let declined = resolve(
                &store.store,
                &store.checkout,
                &raven(),
                &probe(&store, store.current()),
            )
            .await
            .err();
            assert_eq!(declined, Some(Decline::UnfinishedWork));
        }
    }

    /// A mount point whose marker names another incarnation is not the workspace's.
    #[tokio::test]
    async fn a_mount_of_another_incarnation_is_not_the_workspace() {
        let store = Store::new();
        let stale = store.raven_mount.join(WORKSPACE_MARKER_PATH);
        let mut marker = WorkspaceMarker::read_from(&stale).expect("marker");
        marker.workspace_incarnation = incarnation(9);
        write_json(&stale, &marker).expect("marker");
        let declined = resolve(
            &store.store,
            &store.checkout,
            &raven(),
            &probe(&store, store.current()),
        )
        .await
        .err();
        assert_eq!(declined, Some(Decline::StaleMount));
    }
}
