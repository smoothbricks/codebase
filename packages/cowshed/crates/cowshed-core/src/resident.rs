//! A workspace answered from the live state its daemon already holds (06_cli.md "Resident
//! workspaces").
//!
//! `path` and `exec` name one workspace. When that workspace is mounted, every fact `path`
//! answers is live: the mount and the records that place it. `exec` also needs the workspace's
//! daemon-owned supervisor to serve its current authority, and then every fact it depends on is
//! live as well: the supervisor's socket and the authority each call names. Opening the project
//! controller re-derives all of it — host storage validation, the inventory of every workspace in
//! the project, the binding against Git's remotes — only to reach the same mount and supervisor.
//!
//! [`mounted`] reads the records that decide this one workspace's answer, each fresh on every
//! call and none kept, and declines whenever one of them disagrees with the live state; it asks
//! the supervisor nothing, so `path` answers while a supervisor of another build drains its jobs
//! (11_shell.md "Draining a supervisor of another build"). [`resolve`] adds the supervisor's
//! hello for `exec`. A declined verb opens the controller exactly as before, so the controller
//! still answers every case that needs work done: a detached or unserved workspace, a grant the
//! supervisor does not serve yet, unfinished lifecycle work ([`lifecycle_settled`]). Nothing here
//! lists a directory: a project with a thousand retired sessions costs what a project with one
//! does.
//!
//! What a resident answer does not re-check is what the serving supervisor already proved when
//! it started: the project binding it opened under. A binding changes only through a cowshed
//! verb, which changes the records read here; a Git remote edited by hand is reconciled by the
//! next verb that opens the controller.
//!
//! A job is admitted only onto build state the controller's refresh would leave as it is
//! (16_build_volumes.md, "Refresh"; [`build_state_current`]): the tracked build inputs still have
//! the fingerprint the checkout's build volume records, and every fixed link that volume's state
//! names is in place. Anything else — inputs that moved, a volume that records none or no volume
//! at all, a link a tool displaced — declines, and the controller's refresh rediscovers or relinks
//! before it admits the job. The workspace's supervisor judges it, and the project's lifecycle
//! journal with it ([`Resident::as_is`]): its process keeps the Git listings the fingerprint reads
//! while the index keeps its identity, and the journal's reading while the journal keeps its own,
//! where a CLI process would list and parse them again for every exec. `path` admits no job and
//! checks the journal itself.

use std::path::{Path, PathBuf};

use crate::api::dto::GitOid;
use crate::api::{JobId, JobInfo};
use crate::gateway_sessions::GatewayControl;
use crate::metadata::{
    DetachedWorkspaceMetadata, PublicationState, WorkspaceMarker, WorkspaceName, sidecar_path,
};
use crate::repository::RepoId;
use crate::runtime::supervisor::LogChunk;
use crate::runtime::supervisor::{WorkspaceAuthoritySnapshot, WorkspaceSupervisorHandle};
use crate::runtime::supervisor_socket::{self, Hello};
use crate::storage::job_artifact::StreamKind;
use crate::storage::{StorageLayout, WORKSPACE_MARKER_PATH};
use serde::{Deserialize, Serialize};

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
    /// The supervisor serves another incarnation than the records name, or grants older than
    /// theirs.
    ServesOtherAuthority,
    /// The build link names a volume that is not this workspace's, or is not mounted: mounting
    /// through the controller resolves it.
    BuildVolume,
    /// The checkout links no build volume, or its volume records no fingerprint of the build
    /// inputs: only discovery, which the controller runs, says what build state it has.
    BuildStateUnrecorded,
    /// The tracked build inputs no longer have the fingerprint the build volume records.
    BuildInputsMoved,
    /// The tracked build inputs cannot be fingerprinted; the controller's refresh reports why.
    BuildInputsUnreadable,
    /// A build-state link is missing, displaced, or reaches no directory in the volume.
    BuildLinkDisplaced,
    /// The supervisor drains: the daemon is of another build, which the controller's build
    /// check names (11_shell.md, "Draining a supervisor of another build").
    Draining,
    /// The supervisor did not judge the build state.
    AsIsUnanswered,
    /// The gateway does not hold the workspace's session at the revision its supervisor serves:
    /// the controller's pre-exec reconcile installs it.
    GatewaySession,
    /// The gateway did not say whether it holds the workspace's session.
    GatewayUnanswered,
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
            Self::BuildVolume => "the build link needs the controller to resolve it",
            Self::BuildStateUnrecorded => "no build volume records the checkout's build inputs",
            Self::BuildInputsMoved => "the build inputs moved off the recorded fingerprint",
            Self::BuildInputsUnreadable => "the build inputs cannot be fingerprinted",
            Self::BuildLinkDisplaced => "a build-state link is displaced or missing",
            Self::Draining => "the supervisor drains for a daemon of another build",
            Self::AsIsUnanswered => "the supervisor did not judge the workspace as it is",
            Self::GatewaySession => "the gateway session is not current",
            Self::GatewayUnanswered => "the gateway did not answer for the workspace's session",
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

/// A mounted workspace whose records agree with its mount: everything `path` answers.
pub struct Mounted {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub mount: PathBuf,
    pub base_commit: Option<GitOid>,
    layout: StorageLayout,
    metadata: DetachedWorkspaceMetadata,
}

impl Mounted {
    /// [`lifecycle_settled`] for this workspace's project, read now: what `path` checks itself,
    /// asking no supervisor.
    pub fn lifecycle_settled(&self) -> Result<(), Decline> {
        lifecycle_settled(
            &self
                .layout
                .project()
                .project_root
                .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE),
            &self.workspace,
        )
        .map_err(Decline::from)
    }
}

/// A mounted workspace whose supervisor serves its current authority.
pub struct Resident {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub mount: PathBuf,
    pub base_commit: Option<GitOid>,
    /// The mountpoint of the workspace's own build volume, the grant its jobs run with
    /// (`WorkspaceSupervisorHandle::exec`); `None` when the checkout links none.
    pub build_volume: Option<PathBuf>,
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

    /// Whether an exec may be admitted here as it is, which is exactly when the controller's
    /// preparation would do nothing for it (06_cli.md, "Resident workspaces"): `gateway` holds
    /// the workspace's session at the revision its supervisor serves, so the pre-exec reconcile
    /// installs nothing, and the build state is current. Every front end's resident exec asks
    /// this, the CLI's and the controller's that Node-API callers reach.
    pub async fn admissible(&self, gateway: &dyn GatewayControl) -> Result<(), Decline> {
        let authority = self.authority();
        let session = crate::gateway_sessions::stable_workspace_id(
            &self.repo_id,
            self.workspace.as_str(),
            &authority.workspace_incarnation,
        );
        match cowshed_os::timing::spanned(
            "resident",
            "gateway",
            gateway.session_current(&session, authority.grant_revision),
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => return Err(Decline::GatewaySession),
            Err(_) => return Err(Decline::GatewayUnanswered),
        }
        cowshed_os::timing::spanned("resident", "as-is", self.as_is()).await
    }

    /// Whether a job may be admitted onto the workspace as it is, as its supervisor judges it: no
    /// lifecycle work is unfinished ([`lifecycle_settled`]) and the build state is current
    /// ([`build_state_current`]).
    pub async fn as_is(&self) -> Result<(), Decline> {
        match self.supervisor.as_is(self.build_volume.clone()).await {
            Ok(None) => Ok(()),
            Ok(Some(decline)) => Err(decline.into()),
            Err(_) => Err(Decline::AsIsUnanswered),
        }
    }
}

/// Why a job may not be admitted onto a workspace as it is ([`lifecycle_settled`],
/// [`build_state_current`]), as the workspace's supervisor answers it (`asIs`, 11_shell.md,
/// "Protocol").
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AsIsDecline {
    UnfinishedWork,
    Unrecorded,
    Records,
    InputsMoved,
    InputsUnreadable,
    LinkDisplaced,
}

impl From<AsIsDecline> for Decline {
    fn from(decline: AsIsDecline) -> Self {
        match decline {
            AsIsDecline::UnfinishedWork => Self::UnfinishedWork,
            AsIsDecline::Unrecorded => Self::BuildStateUnrecorded,
            AsIsDecline::Records => Self::Records,
            AsIsDecline::InputsMoved => Self::BuildInputsMoved,
            AsIsDecline::InputsUnreadable => Self::BuildInputsUnreadable,
            AsIsDecline::LinkDisplaced => Self::BuildLinkDisplaced,
        }
    }
}

/// Whether the project's lifecycle journal at `journal` holds no unfinished intent of `workspace`
/// or of `main`: what opening the controller would finish before any verb runs
/// (`RecoveryScope::Workspaces`). The journal's reading is kept while it keeps its identity
/// (11_shell.md, "Kept readings"; every write replaces it): a supervisor that answers every exec
/// of its workspace parses a journal of a thousand finished intents once, not once per exec.
pub fn lifecycle_settled(journal: &Path, workspace: &WorkspaceName) -> Result<(), AsIsDecline> {
    use crate::storage::recovery::LifecycleIntentJournal;
    use std::sync::{Arc, LazyLock};
    static READ: LazyLock<crate::input_memo::InputMemo<PathBuf, Arc<LifecycleIntentJournal>>> =
        LazyLock::new(crate::input_memo::InputMemo::new);
    let journal = READ
        .read(&journal.to_owned(), &[journal.to_owned()], || {
            LifecycleIntentJournal::load(journal).map(Arc::new)
        })
        .map_err(|_| AsIsDecline::Records)?;
    if [workspace, &WorkspaceName::main()].into_iter().any(|name| {
        journal
            .get(name)
            .is_some_and(|record| record.completion.is_none())
    }) {
        return Err(AsIsDecline::UnfinishedWork);
    }
    Ok(())
}

/// Whether a job may be admitted onto the build state of the checkout mounted at `mount`, whose
/// build volume is `volume`, as it is: exactly when the controller's refresh (16_build_volumes.md,
/// "Refresh") would change nothing. Every link the volume's state names is in place, and the
/// tracked build inputs still have the fingerprint it records. Reads only; the fingerprint costs
/// the checkout's Git listings (one `git ls-files`, one more per declared pattern, kept while the
/// index keeps its identity) plus a hash of the tracked manifests. A pending discard is left to
/// the next controller refresh and `cowshed gc`.
pub fn build_state_current(mount: &Path, volume: Option<&Path>) -> Result<(), AsIsDecline> {
    use crate::build_volume::{BuildVolumeState, link};
    let volume = volume.ok_or(AsIsDecline::Unrecorded)?;
    let state = BuildVolumeState::read(volume).map_err(|_| AsIsDecline::Records)?;
    let recorded = state
        .fingerprint
        .as_deref()
        .ok_or(AsIsDecline::Unrecorded)?;
    if !link::settled(mount, volume, &state.paths) {
        return Err(AsIsDecline::LinkDisplaced);
    }
    let fingerprint = crate::capabilities::tracked_manifest_fingerprint(mount)
        .map_err(|_| AsIsDecline::InputsUnreadable)?;
    if fingerprint != recorded {
        return Err(AsIsDecline::InputsMoved);
    }
    Ok(())
}

/// One lane to a resident job's supervisor, for every front end that runs a job through a
/// resident workspace. A grant change while the job runs advances the supervisor in place, and
/// it then refuses calls that name the authority it served before; the lane follows it to the
/// new one, which still holds the job.
#[derive(Clone)]
pub struct JobLink {
    socket: PathBuf,
    handle: WorkspaceSupervisorHandle,
}

impl JobLink {
    pub fn new(resident: &Resident) -> Self {
        Self {
            socket: resident.socket.clone(),
            handle: resident.supervisor.clone(),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Prove the lane still reaches a supervisor serving its job's incarnation, with one `hello`,
    /// and take a grant advance it served since; an error when nothing serves that incarnation
    /// any more (the supervisor retired, or another incarnation stands there).
    pub async fn current(&mut self, probe: &impl LiveProbe) -> crate::Result<()> {
        let hello = probe.hello(&self.socket).await?;
        let lane = self.handle.snapshot();
        if hello.authority.repo_id != lane.repo_id
            || hello.authority.workspace != lane.workspace
            || hello.authority.workspace_incarnation != lane.workspace_incarnation
        {
            return Err(crate::CowshedError::conflict(
                "the workspace's supervisor serves another incarnation than the job's",
                "read the job through its workspace again",
            ));
        }
        if hello.authority != *lane {
            self.handle = supervisor_socket::connect(self.socket.clone(), hello.authority);
        }
        Ok(())
    }

    /// The supervisor handle, under the authority the lane last followed.
    pub fn handle(&self) -> &WorkspaceSupervisorHandle {
        &self.handle
    }

    /// Follow the supervisor to the authority it serves now after it refused `error`: only a
    /// stale-authority `Conflict` is followed, and only to a newer authority of the same
    /// incarnation.
    async fn followed(&mut self, error: crate::CowshedError) -> crate::Result<()> {
        if error.code != crate::ErrorCode::Conflict {
            return Err(error);
        }
        match follow(&self.socket, self.handle.snapshot(), &HostProbe).await? {
            Some(handle) => {
                self.handle = handle;
                Ok(())
            }
            None => Err(error),
        }
    }

    pub async fn info(&mut self, job: JobId) -> crate::Result<JobInfo> {
        match self.handle.info(job).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.info(job).await
            }
            done => done,
        }
    }

    pub async fn wait(&mut self, job: JobId) -> crate::Result<JobInfo> {
        match self.handle.wait(job).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.wait(job).await
            }
            done => done,
        }
    }

    pub async fn kill(&mut self, job: JobId) -> crate::Result<()> {
        match self.handle.kill(job).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.kill(job).await
            }
            done => done,
        }
    }

    pub async fn detach(&mut self, job: JobId) -> crate::Result<()> {
        match self.handle.detach(job).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.detach(job).await
            }
            done => done,
        }
    }

    pub async fn log_read(
        &mut self,
        job: JobId,
        stream: StreamKind,
        offset: u64,
        follow: bool,
    ) -> crate::Result<LogChunk> {
        match self.handle.log_read(job, stream, offset, follow).await {
            Err(error) => {
                self.followed(error).await?;
                self.handle.log_read(job, stream, offset, follow).await
            }
            done => done,
        }
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

/// Answer `workspace` of the project containing `start` from its records and the mount table
/// alone, or say why not. The supervisor is never asked: `path` runs no work there.
///
/// Every read names its file; none lists a directory. `store_root` is the store the daemon
/// serves (`STORE_ROOT` in production).
pub fn mounted(
    store_root: &Path,
    start: &Path,
    workspace: &WorkspaceName,
    probe: &impl LiveProbe,
) -> Result<Mounted, Decline> {
    if GIT_DISCOVERY_ENVIRONMENT
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        return Err(Decline::GitEnvironment);
    }
    let project_span = cowshed_os::timing::span("resident", "project");
    let root = repository_root(start).ok_or(Decline::NoRepository)?;
    // The invocation root names the project: main's checkout under direct mount, or any mounted
    // workspace, whose marker records that checkout.
    let origin = WorkspaceMarker::read_from(&root.join(WORKSPACE_MARKER_PATH))
        .map_err(|_| Decline::NotAWorkspace)?;
    let repo = origin.repo_id.clone();
    let layout = StorageLayout::new(store_root, &repo).map_err(|_| Decline::Records)?;

    // What opening the controller would finish first: an identity change of the store. The
    // project's unfinished intents are the other half ([`lifecycle_settled`]), which `path`
    // reads itself and `exec`'s supervisor judges.
    if crate::storage::recovery::RepositoryIdentityIntent::path(store_root)
        .symlink_metadata()
        .is_ok()
    {
        return Err(Decline::UnfinishedWork);
    }
    drop(project_span);

    let _workspace_span = cowshed_os::timing::span("resident", "workspace");
    let mount = layout
        .main_aware_workspace_mount(&origin.project_root, workspace)
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
    if metadata.info_snapshot.git_worktree {
        return Err(Decline::GitWorktree);
    }
    // The invocation root must itself be a live workspace, as the controller's origin check
    // requires; when it is the named workspace, the reads above already proved it.
    if origin.workspace != *workspace {
        current_metadata(store_root, &layout, &origin)?;
    }
    let base_commit = Some(
        GitOid::new(metadata.info_snapshot.base_commit.clone()).map_err(|_| Decline::Records)?,
    );
    Ok(Mounted {
        repo_id: repo,
        workspace: workspace.clone(),
        mount,
        base_commit,
        layout,
        metadata,
    })
}

/// Answer `workspace` of the project containing `start` from live state, or say why not: the
/// [`mounted`] workspace, once its supervisor serves the workspace's current authority.
pub async fn resolve(
    store_root: &Path,
    start: &Path,
    workspace: &WorkspaceName,
    probe: &impl LiveProbe,
) -> Result<Resident, Decline> {
    let Mounted {
        repo_id: repo,
        mount,
        base_commit,
        layout,
        metadata,
        ..
    } = mounted(store_root, start, workspace, probe)?;
    let grants_span = cowshed_os::timing::span("resident", "grants");
    let project = layout.project();
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
    let build_volume = build_volume_grant(project, workspace, &mount, probe)?;
    drop(grants_span);
    let socket = supervisor_socket::socket_path(store_root, &repo, workspace);
    // A draining supervisor admits nothing; the controller's build check says why.
    if socket.with_extension("drain").symlink_metadata().is_ok() {
        return Err(Decline::Draining);
    }
    let hello = cowshed_os::timing::spanned("resident", "hello", probe.hello(&socket))
        .await
        .map_err(|_| Decline::Unserved)?;
    match crate::runtime::supervisor_manager::standing(&hello.authority, &needed) {
        crate::runtime::supervisor_manager::Standing::Serves => {}
        crate::runtime::supervisor_manager::Standing::Behind
        | crate::runtime::supervisor_manager::Standing::Elsewhere => {
            return Err(Decline::ServesOtherAuthority);
        }
    }
    Ok(Resident {
        repo_id: repo,
        workspace: workspace.clone(),
        mount,
        base_commit,
        build_volume,
        supervisor: supervisor_socket::connect(socket.clone(), hello.authority),
        socket,
    })
}

/// The build-volume grant of `workspace` mounted at `mount`, as the controller would resolve it
/// (`BuildVolumeLayout::grant`): the volume its build link names, when that volume's record says it
/// is the workspace's live volume and it is mounted. A link the controller would re-point, or a
/// volume it would mount, declines.
fn build_volume_grant(
    project: &crate::repository::ProjectPaths,
    workspace: &WorkspaceName,
    mount: &Path,
    probe: &impl LiveProbe,
) -> Result<Option<PathBuf>, Decline> {
    use crate::build_volume::{BuildVolumeLayout, BuildVolumeRole, link};
    let Some(target) = link::linked(mount).map_err(|_| Decline::Records)? else {
        return Ok(None);
    };
    let layout = BuildVolumeLayout::new(project).map_err(|_| Decline::Records)?;
    let id = layout.volume_at(&target).ok_or(Decline::BuildVolume)?;
    let record = layout
        .read_record_present(&id)
        .map_err(|_| Decline::Records)?
        .ok_or(Decline::BuildVolume)?;
    let volume = layout.mount(&id);
    match record.role {
        BuildVolumeRole::Linked { checkout } if checkout == *workspace => {}
        _ => return Err(Decline::BuildVolume),
    }
    if !probe.mounted_at(&volume) {
        return Err(Decline::BuildVolume);
    }
    Ok(Some(volume))
}

/// The nearest ancestor of `start` holding `.git`, as Git's discovery finds it: the walk stops
/// at a filesystem boundary, and the directory the boundary is crossed from is the last one
/// Git examines.
pub fn repository_root(start: &Path) -> Option<&Path> {
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
        .canonical_image(&marker.workspace)
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
    {
        return Err(Decline::StaleMount);
    }
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        GrantSet, MARKER_VERSION, NEW_PORT_BLOCK_SIZE, Platform, PortBlock, SIDECAR_VERSION,
        WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceRole, write_json,
    };
    use crate::storage::recovery::{
        LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal,
    };
    use cowshed_os::test_backstop::CommandBackstop as _;
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
            let layout = StorageLayout::new(&store, &repo()).expect("layout");
            let project = layout.project().clone();
            std::fs::create_dir_all(&project.sessions).expect("sessions");
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
                let leftover = project.sessions.join(format!("retired-{index}.dmg"));
                std::fs::write(sidecar_path(&leftover), b"{ not a sidecar").expect("garbage");
                std::fs::write(
                    project.sessions.join(format!("retired-{index}.dmg.lock")),
                    b"",
                )
                .expect("lock");
            }
            fixture
        }

        fn workspace(&self, name: &WorkspaceName, mount: &Path, id: u8, revision: u64) {
            let image = self
                .layout
                .canonical_image(name)
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
                platform: Platform::Macos,
                publication_state: PublicationState::Active,
                updated_at: "2026-07-14T00:00:00Z".to_owned(),
                grants,
                info_snapshot: WorkspaceInfoSnapshot {
                    project_root: self.checkout.clone(),
                    role: WorkspaceRole::for_name(name),
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    branch: None,
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-14T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
                },
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
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    created_trace: "fixture".to_owned(),
                    lineage: Some(Vec::new()),
                    owned_links_revision: 0,
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
                &crate::census::this_test(),
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

    /// The mount table, and a supervisor of another build at raven's socket, still draining its
    /// jobs after an install: its hello names a build this one is not.
    struct Draining {
        mounted: Vec<PathBuf>,
    }

    impl LiveProbe for Draining {
        fn mounted_at(&self, path: &Path) -> bool {
            self.mounted.iter().any(|mounted| mounted == path)
        }

        async fn hello(&self, socket: &Path) -> crate::Result<Hello> {
            Err(crate::CowshedError::conflict(
                format!(
                    "the workspace supervisor at {} is cowshed build another build; this cowshed \\
                     is build this build",
                    socket.display()
                ),
                "it admits nothing more and retires once its running jobs end",
            ))
        }
    }

    /// `path` runs no work in the workspace, so it asks its supervisor nothing: it answers the
    /// mount while a supervisor of another build drains there (11_shell.md "Draining a supervisor
    /// of another build"), and only `exec`, which would run work there, goes to the controller.
    #[tokio::test]
    async fn path_answers_while_a_supervisor_of_another_build_drains() {
        let store = Store::new();
        let draining = Draining {
            mounted: vec![store.checkout.clone(), store.raven_mount.clone()],
        };
        let answered =
            mounted(&store.store, &store.checkout, &raven(), &draining).expect("mounted");
        assert_eq!(answered.mount, store.raven_mount);
        let declined = resolve(&store.store, &store.checkout, &raven(), &draining)
            .await
            .err();
        assert_eq!(declined, Some(Decline::Unserved));
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

    /// A grant change landing between the read of the records and the hello leaves the
    /// supervisor ahead of them: it serves grants newer than the ones read, so it is answered
    /// from, under the authority it reports.
    #[tokio::test]
    async fn a_supervisor_ahead_of_the_records_read_is_served_from() {
        let store = Store::new();
        let ahead = WorkspaceAuthoritySnapshot {
            grant_revision: store.current().grant_revision + 1,
            ..store.current()
        };
        let resident = resolve(
            &store.store,
            &store.checkout,
            &raven(),
            &probe(&store, ahead.clone()),
        )
        .await
        .expect("resident");
        assert_eq!(resident.authority(), &ahead);
    }

    /// A lane proves its supervisor before each job call: one serving the job's incarnation at a
    /// newer grant revision is followed in place; one serving another incarnation, or none, ends
    /// the lane, and the controller path answers the call instead.
    #[tokio::test]
    async fn a_lane_follows_its_supervisor_and_ends_when_none_serves_its_incarnation() {
        let store = Store::new();
        let resident = resolve(
            &store.store,
            &store.checkout,
            &raven(),
            &probe(&store, store.current()),
        )
        .await
        .expect("resident");
        let mut link = JobLink::new(&resident);
        let mut ahead = store.current();
        ahead.grant_revision += 1;
        link.current(&probe(&store, ahead.clone()))
            .await
            .expect("the job's incarnation, served at a newer grant revision");
        assert_eq!(link.handle().snapshot(), &ahead, "the advance is followed");
        let mut other = store.current();
        other.workspace_incarnation = incarnation(9);
        assert!(
            link.current(&probe(&store, other)).await.is_err(),
            "another incarnation"
        );
        let unserved = Probe {
            socket: PathBuf::from("/nowhere"),
            ..probe(&store, store.current())
        };
        assert!(link.current(&unserved).await.is_err(), "no supervisor");
    }

    /// Unfinished lifecycle work on the named workspace or on main is the controller's to
    /// finish before any verb runs, and a journal kept while unchanged is read again once an
    /// intent begins.
    #[tokio::test]
    async fn unfinished_work_on_the_workspace_or_main_opens_the_controller() {
        for target in [raven(), WorkspaceName::main()] {
            let store = Store::new();
            store.begin_intent(LifecycleIntent::Create {
                workspace: target,
                options: Default::default(),
            });
            let mounted = mounted(
                &store.store,
                &store.checkout,
                &raven(),
                &probe(&store, store.current()),
            )
            .expect("mounted");
            assert_eq!(
                mounted.lifecycle_settled().err(),
                Some(Decline::UnfinishedWork)
            );
        }
        let store = Store::new();
        let journal = store
            .layout
            .project()
            .project_root
            .join(LIFECYCLE_INTENTS_FILE);
        store.begin_intent(LifecycleIntent::Create {
            workspace: WorkspaceName::new("finch").expect("name"),
            options: Default::default(),
        });
        crate::input_memo::settle(&journal);
        assert_eq!(lifecycle_settled(&journal, &raven()), Ok(()));
        assert_eq!(lifecycle_settled(&journal, &raven()), Ok(()), "kept");
        store.begin_intent(LifecycleIntent::Create {
            workspace: raven(),
            options: Default::default(),
        });
        assert_eq!(
            lifecycle_settled(&journal, &raven()),
            Err(AsIsDecline::UnfinishedWork),
            "a journal that changed is read again"
        );
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

    /// Raven's checkout made a Git repository with its own build volume holding `paths`, linked
    /// and recorded at the fingerprint its tracked build inputs have now, as a refresh leaves
    /// it. Answers the volume's mountpoint.
    fn linked_volume(store: &Store, paths: &[crate::capabilities::BuildStatePath]) -> PathBuf {
        use crate::build_volume::{
            BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, BuildVolumeState,
            link,
        };
        let checkout = &store.raven_mount;
        let init = crate::git::git_command_at(checkout)
            .args(["init", "--quiet"])
            .output_bounded()
            .expect("git init");
        assert!(init.status.success(), "git init: {init:?}");
        let layout = BuildVolumeLayout::new(store.layout.project()).expect("build layout");
        let id = BuildVolumeId::mint();
        let volume = layout.mount(&id);
        std::fs::create_dir_all(&volume).expect("volume");
        std::fs::create_dir_all(layout.images()).expect("images");
        layout
            .write_record(
                &id,
                &BuildVolumeRecord::new(None, BuildVolumeRole::Linked { checkout: raven() }),
            )
            .expect("record");
        link::point(checkout, &volume).expect("build link");
        link::link_paths(checkout, &volume, paths).expect("links");
        BuildVolumeState {
            paths: paths.to_vec(),
            fingerprint: Some(
                crate::capabilities::tracked_manifest_fingerprint(checkout).expect("fingerprint"),
            ),
            mbx: None,
        }
        .write(&volume)
        .expect("state");
        volume
    }

    async fn admission(store: &Store, volume: Option<&Path>) -> Result<(), Decline> {
        let mut probe = probe(store, store.current());
        probe.mounted.extend(volume.map(Path::to_path_buf));
        let resident = resolve(&store.store, &store.checkout, &raven(), &probe)
            .await
            .expect("resident");
        build_state_current(&resident.mount, resident.build_volume.as_deref())
            .map_err(Decline::from)
    }

    /// A resident `exec` admits a job only onto build state the controller's refresh would
    /// leave as it is (16_build_volumes.md, "Refresh"); `path`, which admits none, still
    /// resolves. A declared path that appeared moves the fingerprint, and a tool that replaced
    /// a link with a directory displaced it: both send the job through the controller, whose
    /// refresh links them. A checkout that links no volume has no recorded fingerprint, so
    /// only discovery says whether it has build state.
    #[tokio::test]
    async fn a_resident_exec_is_admitted_only_onto_build_state_a_refresh_would_leave() {
        let store = Store::new();
        assert_eq!(
            admission(&store, None).await,
            Err(Decline::BuildStateUnrecorded)
        );
        let target = crate::capabilities::BuildStatePath::new("target", "target").expect("path");
        let volume = linked_volume(&store, &[target]);
        assert_eq!(admission(&store, Some(&volume)).await, Ok(()));

        let config = store.raven_mount.join(".cowshed.toml");
        std::fs::write(&config, "[build]\nstate = [\"upstream\"]\n").expect("config");
        std::fs::create_dir(store.raven_mount.join("upstream")).expect("declared path");
        assert_eq!(
            admission(&store, Some(&volume)).await,
            Err(Decline::BuildInputsMoved)
        );
        std::fs::remove_file(&config).expect("config");
        assert_eq!(admission(&store, Some(&volume)).await, Ok(()));

        let link = store.raven_mount.join("target");
        std::fs::remove_file(&link).expect("link");
        std::fs::create_dir(&link).expect("rebuilt directory");
        assert_eq!(
            admission(&store, Some(&volume)).await,
            Err(Decline::BuildLinkDisplaced)
        );
    }
}
