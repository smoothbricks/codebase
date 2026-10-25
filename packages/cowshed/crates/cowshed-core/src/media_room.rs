//! Making room under IOKit's match-notification cap (01a_apfs.md, "The IOMedia match-notification
//! ceiling"; 02_workspaces.md, "Making room for an attach").
//!
//! Every attached image registers three IOMedia, and IOKit queues at most 1024 matches on one
//! unfiltered IOMedia notification ([`cowshed_apfs::media_cap`]). Cowshed attaches its images in
//! the kernel, which waits on no such notification, so the ceiling is a soft one: before a
//! command attaches, it detaches workspaces nothing uses, least recently used first, until its
//! images fit. When they cannot fit, or the media cannot be counted, it says so on stderr and the
//! command attaches anyway; making room never fails the command it makes room for.
//!
//! The decision is [`plan`], a pure function of one media reading and the attached workspaces;
//! [`make_room`] reads the kernel, lists the store's workspaces, carries out the plan through the
//! ordinary unforced unmount and detach, and reads again. A workspace is idle by the supervisor's
//! own definition (11_shell.md, "Supervisor"): no supervisor serves it, since one retires only
//! after 30 minutes with no named session, no job and no Nx daemon. Its detach holds the
//! supervisor socket's lease, so none can start on it meanwhile, and asks the kernel for an
//! unforced unmount, which a process that holds the mount refuses. A detached workspace attaches
//! again on its next use, as any detached workspace does.

use std::collections::BTreeSet;
use std::time::SystemTime;

use cowshed_apfs::media_cap::{MEDIA_PER_IMAGE, MediaReading};

use crate::api::operations::OperationRequest;
use crate::metadata::WorkspaceName;
use crate::repository::RepoId;

/// Why an attached workspace keeps its images.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Busy {
    /// A project's main is never detached.
    Main,
    /// The command making room is about to use it.
    Target,
    /// A supervisor serves it, or a lifecycle operation holds its supervisor socket's lease.
    Supervisor,
    /// A land or a removal of it holds its land lease.
    Lifecycle,
    /// The kernel refused its unforced unmount: a process holds the mount.
    Held,
    /// Its detach failed for another reason, said on stderr when it did.
    Unreleased,
}

impl Busy {
    fn reason(self) -> &'static str {
        match self {
            Self::Main => "main, never detached",
            Self::Target => "this command's own",
            Self::Supervisor => "its supervisor serves it",
            Self::Lifecycle => "a land or removal holds it",
            Self::Held => "a process holds its mount",
            Self::Unreleased => "its detach failed",
        }
    }
}

/// Whether making room may detach an attached workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Occupancy {
    /// Nothing uses it; its images were last written at `last_used`.
    Idle {
        last_used: SystemTime,
    },
    Busy(Busy),
}

/// One attached workspace, as making room sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate<K> {
    pub key: K,
    /// The images it holds attached: its own, and its build volume's while that is mounted.
    pub images: u64,
    pub occupancy: Occupancy,
}

impl<K> Candidate<K> {
    /// The IOMedia its images hold, which detaching it frees.
    pub fn media(&self) -> u64 {
        self.images.saturating_mul(MEDIA_PER_IMAGE)
    }
}

/// What making room does next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Plan<K> {
    /// The images fit under the cap as the host stands.
    Fits,
    /// Detach these idle workspaces, least recently used first: together they free at least the
    /// excess, and without the last of them they would not.
    Detach(Vec<K>),
    /// Every idle workspace together frees `idle_media`, less than the `excess`: detach nothing,
    /// since the images cross the ceiling either way.
    Short { excess: u64, idle_media: u64 },
}

/// The workspaces to detach so that `images` more attach under `cap`, as `reading` and
/// `candidates` stand. Only idle ones are chosen, the least recently used first (ties by key, so
/// one reading always plans alike), and only as many as free the excess; when all of them free
/// less, none is.
pub fn plan<K: Clone + Ord>(
    cap: u64,
    reading: MediaReading,
    images: u64,
    candidates: &[Candidate<K>],
) -> Plan<K> {
    let excess = reading.excess(cap, images);
    if excess == 0 {
        return Plan::Fits;
    }
    let mut idle: Vec<(SystemTime, &Candidate<K>)> = candidates
        .iter()
        .filter_map(|candidate| match candidate.occupancy {
            Occupancy::Idle { last_used } => Some((last_used, candidate)),
            Occupancy::Busy(_) => None,
        })
        .collect();
    idle.sort_by(|(left_used, left), (right_used, right)| {
        left_used
            .cmp(right_used)
            .then_with(|| left.key.cmp(&right.key))
    });
    let mut freed = 0_u64;
    let mut chosen = Vec::new();
    for (_, candidate) in idle {
        if freed >= excess {
            break;
        }
        freed = freed.saturating_add(candidate.media());
        chosen.push(candidate.key.clone());
    }
    if freed >= excess {
        Plan::Detach(chosen)
    } else {
        Plan::Short {
            excess,
            idle_media: freed,
        }
    }
}

/// What a command about to attach asks of the ceiling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Demand {
    /// The images the command may attach, at most.
    pub images: u64,
    /// The command's own workspaces, never detached to make room for it.
    pub keep: Option<Keep>,
}

/// A project's workspaces a command names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Keep {
    pub repo: RepoId,
    pub workspaces: BTreeSet<WorkspaceName>,
}

/// The images a detached workspace attaches with: its own and its build volume's.
pub const WORKSPACE_IMAGES: u64 = 2;

/// What a controller operation may attach, read from its request alone (02_workspaces.md,
/// "Making room for an attach").
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AttachDemand {
    /// The images it attaches whatever is mounted: adoption's main and its first build volume,
    /// the build image or seed a resize or a reseed attaches beside the workspace's own.
    pub fresh: u64,
    /// The workspaces it works in or creates, a fork's or rename's destination included: each one
    /// detached attaches [`WORKSPACE_IMAGES`], and none is detached to make room for it.
    pub workspaces: Vec<WorkspaceName>,
}

impl AttachDemand {
    fn of(fresh: u64, workspaces: impl IntoIterator<Item = WorkspaceName>) -> Self {
        Self {
            fresh,
            workspaces: workspaces.into_iter().collect(),
        }
    }
}

/// What `operation` may attach. Reads, job and session traffic, detaches, removals, grants of the
/// whole project and the project's own maintenance attach nothing, and one attach of an
/// aggregate makes no room: detaching some sessions to attach others frees nothing. Host and
/// sccache operations route outside the project actor, which refuses them, so they add no
/// project attach demand. Recovering an image renames its files without attaching it.
pub fn attach_demand(operation: &OperationRequest) -> AttachDemand {
    use OperationRequest as Op;
    let one = |workspace: &WorkspaceName| AttachDemand::of(0, [workspace.clone()]);
    let into = |workspace: &WorkspaceName, into: Option<&crate::api::dto::WorkspaceTarget>| {
        AttachDemand::of(
            0,
            [
                workspace.clone(),
                into.map_or_else(WorkspaceName::main, |target| target.workspace().clone()),
            ],
        )
    };
    match operation {
        // The destination is new, and so detached, or a resumed one already mounted.
        Op::CoordinatorFork(request) => AttachDemand::of(
            0,
            [
                request.workspace.clone(),
                request.source.clone().unwrap_or_else(WorkspaceName::main),
            ],
        ),
        Op::CoordinatorRename(request) => {
            AttachDemand::of(0, [request.destination.clone(), request.source.clone()])
        }
        Op::CoordinatorAdopt(_) => AttachDemand::of(WORKSPACE_IMAGES, []),
        Op::CoordinatorMoveCheckout(_) | Op::CoordinatorChangeRepoId(_) => {
            AttachDemand::of(0, [WorkspaceName::main()])
        }
        Op::CoordinatorResize(request) => AttachDemand::of(1, [request.workspace.clone()]),
        Op::CoordinatorReseed(request) if request.dry_run => AttachDemand::default(),
        Op::CoordinatorReseed(request) => {
            AttachDemand::of(1, [request.workspace.clone(), request.from.clone()])
        }
        Op::CoordinatorRebase(request) => into(&request.workspace, request.into.as_ref()),
        Op::CoordinatorLand(request) if request.options.preflight => AttachDemand::default(),
        Op::CoordinatorLand(request) => into(&request.workspace, request.into.as_ref()),
        Op::WorkspaceAttach(request) if request.options.aggregate => AttachDemand::default(),
        Op::WorkspaceAttach(request) => one(&request.workspace),
        Op::CoordinatorGrant(request) | Op::CoordinatorRevoke(request) => one(&request.workspace),
        Op::CoordinatorRestore(request) => one(&request.workspace),
        Op::CoordinatorDefragment(request)
        | Op::CoordinatorRekey(request)
        | Op::CoordinatorClearNxState(request)
        | Op::CoordinatorWorker(request)
        | Op::CoordinatorRefreshBuildState(request) => one(&request.workspace),
        Op::CoordinatorExec(params) | Op::WorkerExec(params) => one(&params.workspace),
        Op::WorkerShell(request) => one(&request.workspace),
        Op::WorkerCheckpoint(request) => one(&request.workspace),
        Op::WorkerPush(request) => one(&request.workspace),
        Op::ProjectOpen(_)
        | Op::ProjectWorkspace(_)
        | Op::ProjectWorkspaceAt(_)
        | Op::ProjectList(_)
        | Op::ProjectWorkspaceInSlot(_)
        | Op::WorkspaceInfoRead(_)
        | Op::WorkspaceGrants(_)
        | Op::WorkspaceBuildVolume(_)
        | Op::CoordinatorProjectGrants(_)
        | Op::CoordinatorGrantProject(_)
        | Op::CoordinatorRevokeProject(_)
        | Op::CoordinatorDetach(_)
        | Op::CoordinatorAssignSlot(_)
        | Op::CoordinatorDestroy(_)
        | Op::CoordinatorGc(_)
        | Op::CoordinatorRecoverImage(_)
        | Op::CoordinatorRemoveProject(_)
        | Op::CoordinatorRepoMirror(_)
        | Op::CoordinatorSetCheckpointQuota(_)
        | Op::CoordinatorDoctor(_)
        | Op::CoordinatorServeSupervisor(_)
        | Op::WorkerStdinChunk(_)
        | Op::WorkerStdinClose(_)
        | Op::WorkerListJobs(_)
        | Op::WorkerJob(_)
        | Op::WorkerJobByKey(_)
        | Op::JobStatus(_)
        | Op::JobResourcesRead(_)
        | Op::JobSealed(_)
        | Op::JobLogs(_)
        | Op::JobTailRead(_)
        | Op::JobListeningPortsRead(_)
        | Op::JobProcesses(_)
        | Op::JobProcessEvents(_)
        | Op::JobProgress(_)
        | Op::JobAttachWrite(_)
        | Op::JobAttachClose(_)
        | Op::JobDetach(_)
        | Op::JobWait(_)
        | Op::JobKill(_)
        | Op::SessionClose(_)
        | Op::HostListProjects(_)
        | Op::HostAttachAll(_)
        | Op::HostDetachAll(_)
        | Op::HostForgetAbandoned(_)
        | Op::HostMountMain(_)
        | Op::HostGatewayStatus(_)
        | Op::HostCredentialStatus(_)
        | Op::HostCredentialRemove(_)
        | Op::HostIdentityAdd(_)
        | Op::HostSkillInstall(_)
        | Op::HostBuildState(_)
        | Op::HostClearNxStore(_)
        | Op::HostGatewayStart(_)
        | Op::HostGatewayStop(_)
        | Op::HostLockHolders(_)
        | Op::HostTreeHolders(_)
        | Op::HostFdHolders(_)
        | Op::HostRetireDescendants(_)
        | Op::HostDiskCommand(_)
        | Op::HostCredentialAdd(_)
        | Op::SccacheStart(_)
        | Op::SccacheStop(_)
        | Op::SccacheStatusRead(_) => AttachDemand::default(),
    }
}

/// An attached workspace, store-wide: its project's repository and its name.
pub type Key = (RepoId, WorkspaceName);

/// What making room did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Made {
    /// The workspaces it detached, in order.
    pub detached: Vec<Key>,
    /// Every reading a decision was made on, in order: the first, then one after each round of
    /// detaches.
    pub readings: Vec<MediaReading>,
    /// Why the images may still cross the ceiling, said on stderr as a warning: the media could
    /// not be counted, the store's workspaces could not be listed, or the idle ones free too
    /// little. `None` when they fit.
    pub unmet: Option<crate::CowshedError>,
}

pub use native::make_room;

mod native {
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::path::Path;
    use std::time::SystemTime;

    use cowshed_apfs::SystemCommandRunner;
    use cowshed_apfs::media_cap::MediaReading;

    use super::{Busy, Candidate, Demand, Key, Made, Occupancy, Plan, plan};
    use crate::build_volume::BuildVolumeLayout;
    use crate::gateway_inventory::GatewayStoreBackend;
    use crate::gateway_sessions::{
        GatewayControl, NativeSessionInventory, effective_uid, reconcile_project,
    };
    use crate::runtime::land_lease::{self, Holding};
    use crate::runtime::supervisor_socket;
    use crate::storage::StorageLayout;
    use crate::storage::apfs::native::MacOsApfsExecutionHost;
    use crate::storage::apfs::{ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig};
    use crate::storage::bootstrap::ValidatedHostStorage;
    use crate::storage::lifecycle::{
        LifecycleWorkspace, MountState, Substrate as _, dispatch_blocking,
    };
    use crate::{AdoptedProject, CowshedError, ErrorCode, NativeGatewayInventory, Result};

    type Host = MacOsApfsExecutionHost<SystemCommandRunner>;

    /// One adopted project's handles: its substrate, which detaches a workspace with its build
    /// volume.
    struct Project {
        repo: AdoptedProject,
        /// The project's session directory, where each workspace's land lease lives.
        sessions: std::path::PathBuf,
        substrate: ApfsSubstrate<Host>,
    }

    /// An attached session workspace.
    struct Attached {
        workspace: LifecycleWorkspace,
    }

    /// Make room for `demand` under `cap` ([`super::plan`]), detaching idle workspaces of every
    /// project in `storage`'s store, then reconcile `gateway`'s view of each project one belonged
    /// to. A host that fits is read once and nothing else is touched. Best effort: what keeps
    /// the images from fitting (a count or a store that cannot be read, or idle workspaces that
    /// free too little, named with the IOMedia each attached workspace holds and why it stays)
    /// is said on stderr as a warning and answered as [`Made::unmet`], never as a failure.
    pub async fn make_room(
        storage: &ValidatedHostStorage,
        demand: &Demand,
        cap: u64,
        gateway: &dyn GatewayControl,
    ) -> Made {
        let mut made = Made::default();
        if let Err(unmet) = detach_until_fit(storage, demand, cap, &mut made).await {
            cowshed_os::eprintln!("cowshed: warning: {}; next: {}", unmet.message, unmet.hint);
            made.unmet = Some(unmet);
        }
        if made.detached.is_empty() {
            return made;
        }
        let inventory =
            NativeSessionInventory::with_backend(storage.clone(), GatewayStoreBackend::Native);
        let repos: std::collections::BTreeSet<&crate::repository::RepoId> =
            made.detached.iter().map(|(repo, _)| repo).collect();
        for repo in repos {
            if let Err(error) = reconcile_project(gateway, &inventory, repo, effective_uid()).await
            {
                cowshed_os::eprintln!(
                    "cowshed: the gateway's view of {repo} is stale after making room: {error}"
                );
            }
        }
        made
    }

    /// Detach idle workspaces until `demand` fits, recording what was read and detached in
    /// `made`; `Err` says why the images still do not fit.
    async fn detach_until_fit(
        storage: &ValidatedHostStorage,
        demand: &Demand,
        cap: u64,
        made: &mut Made,
    ) -> Result<()> {
        let Some(mut reading) = read().await? else {
            return Ok(());
        };
        made.readings.push(reading);
        if reading.excess(cap, demand.images) == 0 {
            return Ok(());
        }
        let (projects, mut candidates, attached) = census(storage, demand).await?;
        loop {
            match plan(cap, reading, demand.images, &candidates) {
                Plan::Fits => return Ok(()),
                Plan::Short { excess, idle_media } => {
                    return Err(shortfall(
                        cap,
                        reading,
                        demand.images,
                        excess,
                        idle_media,
                        &candidates,
                    ));
                }
                Plan::Detach(keys) => {
                    for key in keys {
                        let (Some(project), Some(workspace)) =
                            (projects.get(&key.0), attached.get(&key))
                        else {
                            return Err(CowshedError::internal(format!(
                                "making room planned {}/{}, which its census never listed",
                                key.0, key.1
                            )));
                        };
                        let kept = detach(storage, project, workspace).await;
                        match kept {
                            Some(busy) => {
                                for candidate in &mut candidates {
                                    if candidate.key == key {
                                        candidate.occupancy = Occupancy::Busy(busy);
                                    }
                                }
                            }
                            None => {
                                candidates.retain(|candidate| candidate.key != key);
                                made.detached.push(key);
                            }
                        }
                    }
                    reading = read().await?.ok_or_else(|| {
                        CowshedError::internal("the kernel's IOKit media count vanished mid-way")
                    })?;
                    made.readings.push(reading);
                }
            }
        }
    }

    /// The media now, read off the async runtime: the registry walk and the process table scan
    /// block. `None` where the kernel has no IOKit match notifications, and so no cap.
    async fn read() -> Result<Option<MediaReading>> {
        match dispatch_blocking(cowshed_apfs::media_cap::read)
            .await
            .map_err(|error| {
                CowshedError::internal(format!("the media reading stopped: {error}"))
            })? {
            Ok(reading) => Ok(Some(reading)),
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => Ok(None),
            Err(error) => Err(CowshedError::environment_missing(
                format!(
                    "could not read the registered IOMedia and the running image attaches that \
                         IOKit's match-notification ceiling is counted from, so no room was made: \
                         {error}"
                ),
                "cowshed doctor (media-notification-cap)",
            )),
        }
    }

    /// Every attached workspace of every adopted project, as making room sees it. A project
    /// whose workspaces cannot be read is said on stderr and offers none.
    async fn census(
        storage: &ValidatedHostStorage,
        demand: &Demand,
    ) -> Result<(
        BTreeMap<crate::repository::RepoId, Project>,
        Vec<Candidate<Key>>,
        BTreeMap<Key, Attached>,
    )> {
        let adopted = NativeGatewayInventory::new(storage.clone())
            .adopted_projects()
            .await
            .map_err(|error| {
                CowshedError::integrity(
                    format!(
                        "making room for an attach could not list the store's projects: {error}"
                    ),
                    "cowshed doctor --json",
                )
            })?;
        let mounted = dispatch_blocking(cowshed_apfs::mounts::kernel_mounts)
            .await
            .map_err(|error| CowshedError::internal(format!("the mount read stopped: {error}")))?
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "making room for an attach could not read the kernel's mounts: {error}"
                    ),
                    "cowshed doctor",
                )
            })?
            .into_iter()
            .map(|mount| mount.mount_point)
            .collect::<std::collections::BTreeSet<_>>();
        let mut projects = BTreeMap::new();
        let mut candidates = Vec::new();
        let mut attached = BTreeMap::new();
        for repo in adopted {
            let listed = list_project(storage, &repo, demand, &mounted).await;
            match listed {
                Ok((project, listed)) => {
                    for (candidate, workspace) in listed {
                        attached.insert(candidate.key.clone(), workspace);
                        candidates.push(candidate);
                    }
                    projects.insert(repo.repo_id.clone(), project);
                }
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: making room for an attach skips project {}: {error}",
                    repo.repo_id
                ),
            }
        }
        Ok((projects, candidates, attached))
    }

    /// One project's attached workspaces.
    async fn list_project(
        storage: &ValidatedHostStorage,
        repo: &AdoptedProject,
        demand: &Demand,
        mounted: &std::collections::BTreeSet<std::path::PathBuf>,
    ) -> std::result::Result<(Project, Vec<(Candidate<Key>, Attached)>), String> {
        let text = |error: &dyn Display| error.to_string();
        let config = ApfsSubstrateConfig::for_storage(storage, &repo.project_root);
        let substrate = ApfsSubstrate::new(
            config.clone(),
            Host::new(SystemCommandRunner::new(config.namespace), config.clone())
                .map_err(|error| text(&error))?,
        );
        let layout =
            StorageLayout::new(storage.store(), &repo.repo_id).map_err(|error| text(&error))?;
        let builds = BuildVolumeLayout::new(layout.project()).map_err(|error| text(&error))?;
        let derived = substrate
            .list(&repo.repo_id)
            .await
            .map_err(|error| text(&error))?;
        let mut listed = Vec::new();
        for workspace in derived {
            if !matches!(workspace.mount_state, MountState::Mounted { .. }) {
                continue;
            }
            let name = workspace.workspace.name().clone();
            let mount = layout
                .main_aware_workspace_mount(&repo.project_root, &name)
                .map_err(|error| text(&error))?;
            let build = builds
                .linked(&mount)
                .map_err(|error| text(&error))?
                .filter(|id| mounted.contains(&builds.mount(id)));
            let socket = supervisor_socket::socket_path(storage.store(), &repo.repo_id, &name);
            let occupancy =
                if name.is_main() {
                    Occupancy::Busy(Busy::Main)
                } else if demand.keep.as_ref().is_some_and(|keep| {
                    keep.repo == repo.repo_id && keep.workspaces.contains(&name)
                }) {
                    Occupancy::Busy(Busy::Target)
                } else if supervisor_socket::serving_since(&socket)
                    .map_err(|error| text(&error))?
                    .is_some()
                {
                    Occupancy::Busy(Busy::Supervisor)
                } else {
                    let image = layout.session_image(&name).map_err(|error| text(&error))?;
                    let mut last_used = modified(image.image())?;
                    if let Some(id) = &build {
                        last_used = last_used.max(modified(&builds.image(id))?);
                    }
                    Occupancy::Idle { last_used }
                };
            let images = if build.is_some() { 2 } else { 1 };
            listed.push((
                Candidate {
                    key: (repo.repo_id.clone(), name),
                    images,
                    occupancy,
                },
                Attached {
                    workspace: workspace.workspace,
                },
            ));
        }
        Ok((
            Project {
                repo: repo.clone(),
                sessions: layout.project().sessions.clone(),
                substrate,
            },
            listed,
        ))
    }

    /// When `image`'s backing file was last written: a mounted volume's writes reach it, so it
    /// says when the workspace was last used.
    fn modified(image: &Path) -> std::result::Result<SystemTime, String> {
        std::fs::metadata(image)
            .and_then(|metadata| metadata.modified())
            .map_err(|error| format!("read when {} was last written: {error}", image.display()))
    }

    /// Detach one idle workspace and its build volume; `Some` says why it stays attached. Under
    /// its supervisor socket's lease no supervisor starts on it meanwhile, and the unmount is the
    /// unforced one `cowshed detach` makes, which a process holding the mount refuses.
    async fn detach(
        storage: &ValidatedHostStorage,
        project: &Project,
        attached: &Attached,
    ) -> Option<Busy> {
        let name = attached.workspace.name();
        let repo = &project.repo.repo_id;
        let socket = supervisor_socket::socket_path(storage.store(), repo, name);
        let _lease = match supervisor_socket::lease_unserved(&socket) {
            Ok(Some(lease)) => lease,
            Ok(None) => return Some(Busy::Supervisor),
            Err(error) => return Some(unreleased(repo, name, &error)),
        };
        // A land or an rm of the workspace holds this lease while it runs; held here, neither
        // starts until the detach is done.
        let _land = match land_lease::take(&project.sessions, name.as_str(), Holding::MakeRoom) {
            Ok(lease) => lease,
            Err(refused) if refused.code == ErrorCode::Conflict => return Some(Busy::Lifecycle),
            Err(error) => return Some(unreleased(repo, name, &error)),
        };
        match project.substrate.unmount(&attached.workspace).await {
            Ok(()) => {}
            Err(ApfsStorageError::Apfs(error)) if cowshed_apfs::detach_was_dissented(&error) => {
                return Some(Busy::Held);
            }
            Err(error) => return Some(unreleased(repo, name, &error)),
        }
        cowshed_os::eprintln!(
            "cowshed: detached {repo}/{name} and its build volume, which nothing used, to keep \
             IOKit's media match-notification queue under its cap; its next use attaches them \
             again"
        );
        None
    }

    fn unreleased(
        repo: &crate::repository::RepoId,
        name: &crate::metadata::WorkspaceName,
        error: &dyn Display,
    ) -> Busy {
        cowshed_os::eprintln!(
            "cowshed: making room for an attach could not detach {repo}/{name}: {error}"
        );
        Busy::Unreleased
    }

    /// The workspaces a shortfall names: ten, then how many more.
    const NAMED: usize = 10;

    /// Why the images still cross the ceiling: the count, the excess, and every attached
    /// workspace with the IOMedia it holds and why it stays.
    fn shortfall(
        cap: u64,
        reading: MediaReading,
        images: u64,
        excess: u64,
        idle_media: u64,
        candidates: &[Candidate<Key>],
    ) -> CowshedError {
        let mut held: Vec<&Candidate<Key>> = candidates.iter().collect();
        held.sort_by(|left, right| left.key.cmp(&right.key));
        let mut named = held
            .iter()
            .take(NAMED)
            .map(|candidate| {
                let why = match candidate.occupancy {
                    Occupancy::Idle { .. } => "idle",
                    Occupancy::Busy(busy) => busy.reason(),
                };
                format!(
                    "{}/{} {} IOMedia ({why})",
                    candidate.key.0,
                    candidate.key.1,
                    candidate.media()
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        if held.len() > NAMED {
            named.push_str(&format!("; and {} more", held.len() - NAMED));
        }
        CowshedError::conflict(
            format!(
                "attaching {images} disk image(s) takes the host past IOKit's {cap}-entry IOMedia \
                 match-notification queue: {reading}, {excess} IOMedia over, and the workspaces \
                 nothing uses free only {idle_media}; attaching anyway, since an in-kernel attach \
                 waits on no such notification. Attached workspaces: {named}"
            ),
            "stop the work in a workspace you are done with, then `cowshed detach <ws>` or \
             `cowshed rm <ws>`: each frees the IOMedia listed; `cowshed doctor` reports \
             media-notification-cap",
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use cowshed_apfs::media_cap::{MEDIA_PER_IMAGE, MediaReading, NOTIFICATION_CAP};
    use proptest::prelude::*;

    use super::{Busy, Candidate, Occupancy, Plan, plan};

    fn occupancy() -> impl Strategy<Value = Occupancy> {
        prop_oneof![
            4 => (0_u64..1_000).prop_map(|seconds| Occupancy::Idle {
                last_used: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
            }),
            1 => Just(Occupancy::Busy(Busy::Main)),
            1 => Just(Occupancy::Busy(Busy::Target)),
            1 => Just(Occupancy::Busy(Busy::Supervisor)),
            1 => Just(Occupancy::Busy(Busy::Held)),
            1 => Just(Occupancy::Busy(Busy::Unreleased)),
        ]
    }

    /// Up to 40 attached workspaces, each holding one or two images, keyed by position.
    fn candidates() -> impl Strategy<Value = Vec<Candidate<u32>>> {
        prop::collection::vec((1_u64..=2, occupancy()), 0..40).prop_map(|workspaces| {
            (0_u32..)
                .zip(workspaces)
                .map(|(key, (images, occupancy))| Candidate {
                    key,
                    images,
                    occupancy,
                })
                .collect()
        })
    }

    fn reading() -> impl Strategy<Value = MediaReading> {
        (800_u64..1_100, 0_u64..4).prop_map(|(registered, attaching)| MediaReading {
            registered,
            attaching,
        })
    }

    fn idle_media(candidates: &[Candidate<u32>]) -> u64 {
        candidates
            .iter()
            .filter(|candidate| matches!(candidate.occupancy, Occupancy::Idle { .. }))
            .map(Candidate::media)
            .sum()
    }

    fn last_used(candidate: &Candidate<u32>) -> Option<SystemTime> {
        match candidate.occupancy {
            Occupancy::Idle { last_used } => Some(last_used),
            Occupancy::Busy(_) => None,
        }
    }

    proptest! {
        /// A plan fits exactly when the reading leaves room, detaches only idle workspaces, frees
        /// at least the excess and no more workspaces than that needs, and falls short exactly
        /// when every idle workspace together frees too little.
        #[test]
        fn a_plan_frees_enough_from_idle_workspaces_or_falls_short(
            reading in reading(),
            images in 0_u64..6,
            candidates in candidates(),
        ) {
            let excess = reading.excess(NOTIFICATION_CAP, images);
            match plan(NOTIFICATION_CAP, reading, images, &candidates) {
                Plan::Fits => prop_assert_eq!(excess, 0),
                Plan::Short { excess: short, idle_media: freed } => {
                    prop_assert_eq!(short, excess);
                    prop_assert_eq!(freed, idle_media(&candidates));
                    prop_assert!(freed < excess);
                }
                Plan::Detach(keys) => {
                    prop_assert!(excess > 0);
                    prop_assert!(!keys.is_empty());
                    let chosen: Vec<&Candidate<u32>> = keys
                        .iter()
                        .map(|key| &candidates[*key as usize])
                        .collect();
                    for candidate in &chosen {
                        prop_assert!(
                            matches!(candidate.occupancy, Occupancy::Idle { .. }),
                            "{candidate:?} is not idle"
                        );
                    }
                    let freed: u64 = chosen.iter().map(|candidate| candidate.media()).sum();
                    prop_assert!(freed >= excess);
                    let last = chosen[chosen.len() - 1].media();
                    prop_assert!(freed - last < excess, "the last detach was not needed");
                    prop_assert!(idle_media(&candidates) >= excess);
                }
            }
        }

        /// The chosen workspaces are the least recently used idle ones, in that order: none left
        /// idle was used before one chosen.
        #[test]
        fn a_plan_detaches_the_least_recently_used_first(
            reading in reading(),
            images in 0_u64..6,
            candidates in candidates(),
        ) {
            if let Plan::Detach(keys) = plan(NOTIFICATION_CAP, reading, images, &candidates) {
                let used: Vec<SystemTime> = keys
                    .iter()
                    .filter_map(|key| last_used(&candidates[*key as usize]))
                    .collect();
                prop_assert!(used.windows(2).all(|pair| pair[0] <= pair[1]));
                let newest_chosen = used[used.len() - 1];
                for candidate in &candidates {
                    if let Some(time) = last_used(candidate)
                        && !keys.contains(&candidate.key)
                    {
                        prop_assert!(time >= newest_chosen, "{candidate:?} is older than a chosen one");
                    }
                }
            }
        }

        /// Main, the command's own workspaces and every busy one are never chosen, whatever the
        /// excess.
        #[test]
        fn a_plan_never_detaches_a_busy_workspace(
            images in 0_u64..400,
            candidates in candidates(),
        ) {
            let reading = MediaReading { registered: 1_100, attaching: 0 };
            if let Plan::Detach(keys) = plan(NOTIFICATION_CAP, reading, images, &candidates) {
                for key in keys {
                    let candidate = &candidates[key as usize];
                    prop_assert!(
                        matches!(candidate.occupancy, Occupancy::Idle { .. }),
                        "{:?} is not idle",
                        candidate
                    );
                }
            }
        }
    }

    /// The worked case the spec states: two idle workspaces of six media each, three media over
    /// the cap, detach only the older one.
    #[test]
    fn three_media_over_detaches_only_the_older_idle_workspace() {
        let at = |seconds| Occupancy::Idle {
            last_used: SystemTime::UNIX_EPOCH + Duration::from_secs(seconds),
        };
        let candidates = [
            Candidate {
                key: "main",
                images: 2,
                occupancy: Occupancy::Busy(Busy::Main),
            },
            Candidate {
                key: "newer",
                images: 2,
                occupancy: at(20),
            },
            Candidate {
                key: "older",
                images: 2,
                occupancy: at(10),
            },
        ];
        let committed = MediaReading {
            registered: 0,
            attaching: 0,
        }
        .committed();
        let reading = MediaReading {
            registered: NOTIFICATION_CAP - committed - MEDIA_PER_IMAGE + 3,
            attaching: 0,
        };
        assert_eq!(
            plan(NOTIFICATION_CAP, reading, 1, &candidates),
            Plan::Detach(vec!["older"])
        );
    }

    /// A controller operation's demand comes from its request alone: a fork keeps its destination
    /// and its source, main when it names none; a land keeps what it lands into, main when
    /// it names none, but a land preflight attaches nothing; a reseed attaches one image beside
    /// its own, none for a dry run; a worker
    /// keeps its workspace; a detach and a read attach nothing.
    #[test]
    fn an_operations_attach_demand_follows_its_request() {
        use crate::api::dto::{
            AttachOptions, ForkOptions, LandOptions, WorkspaceIncarnation, WorkspaceTarget,
        };
        use crate::api::operations::{
            ForkRequest, LandRequest, OperationRequest as Op, ReseedRequest,
            WorkspaceAttachRequest, WorkspaceRequest,
        };
        use crate::metadata::WorkspaceName;
        use crate::repository::RepoId;

        use super::{AttachDemand, attach_demand};

        let repo = RepoId::parse("fixture/demand").expect("repository identity");
        let session = |name: &str| WorkspaceName::session(name).expect("session name");
        let demand = |fresh, workspaces: &[WorkspaceName]| AttachDemand {
            fresh,
            workspaces: workspaces.to_vec(),
        };
        let fork = |source: Option<WorkspaceName>| {
            attach_demand(&Op::CoordinatorFork(ForkRequest {
                repo_id: repo.clone(),
                workspace: session("raven"),
                source,
                options: ForkOptions::default(),
            }))
        };
        assert_eq!(
            fork(None),
            demand(0, &[session("raven"), WorkspaceName::main()])
        );
        assert_eq!(
            fork(Some(session("crow"))),
            demand(0, &[session("raven"), session("crow")])
        );
        let land = |into: Option<WorkspaceTarget>, preflight| {
            attach_demand(&Op::CoordinatorLand(LandRequest {
                repo_id: repo.clone(),
                workspace: session("raven"),
                into,
                options: LandOptions {
                    preflight,
                    ..LandOptions::default()
                },
            }))
        };
        assert_eq!(
            land(None, false),
            demand(0, &[session("raven"), WorkspaceName::main()])
        );
        let lane = WorkspaceTarget::new(
            session("lane"),
            WorkspaceIncarnation::new("1".repeat(32)).expect("incarnation"),
        );
        assert_eq!(
            land(Some(lane.clone()), false),
            demand(0, &[session("raven"), session("lane")])
        );
        assert_eq!(land(None, true), AttachDemand::default());
        assert_eq!(land(Some(lane), true), AttachDemand::default());
        let reseed = |dry_run| {
            attach_demand(&Op::CoordinatorReseed(ReseedRequest {
                repo_id: repo.clone(),
                workspace: session("raven"),
                from: session("lane"),
                dry_run,
            }))
        };
        assert_eq!(reseed(true), AttachDemand::default());
        assert_eq!(
            reseed(false),
            demand(1, &[session("raven"), session("lane")])
        );
        let request = || WorkspaceRequest {
            repo_id: repo.clone(),
            workspace: session("raven"),
        };
        assert_eq!(
            attach_demand(&Op::CoordinatorWorker(request())),
            demand(0, &[session("raven")])
        );
        assert_eq!(
            attach_demand(&Op::CoordinatorRefreshBuildState(request())),
            demand(0, &[session("raven")])
        );
        let attach = |aggregate| {
            attach_demand(&Op::WorkspaceAttach(WorkspaceAttachRequest {
                repo_id: repo.clone(),
                workspace: session("raven"),
                options: AttachOptions {
                    aggregate,
                    ..AttachOptions::default()
                },
            }))
        };
        assert_eq!(attach(false), demand(0, &[session("raven")]));
        assert_eq!(attach(true), AttachDemand::default());
        for nothing in [
            Op::CoordinatorDetach(request()),
            Op::WorkspaceInfoRead(request()),
        ] {
            assert_eq!(
                attach_demand(&nothing),
                AttachDemand::default(),
                "{nothing:?}"
            );
        }
    }
}
