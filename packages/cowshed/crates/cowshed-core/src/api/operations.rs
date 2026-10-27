//! The controller's operations, declared once.
//!
//! Each row of the [`operations!`] table names one controller method: who may call it, which raw
//! byte lane it carries, its request record and its result record. The table is the single
//! source of truth for the controller protocol (07_api.md, "DTO freeze"): the macro expands it
//! into the [`Operation`] marker types the capability client calls through, the exhaustive
//! [`OperationRequest`] the controller decodes every request into, and the [`OPERATIONS`] table
//! the connection validates authority and lanes against. `cowshed-api-gen` reads the same table
//! for the N-API and TypeScript projections. Adding a field to a request record or a row to the
//! table changes every projection together; no adapter restates a field list.

use super::dto::{
    AdmissionKey, AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota,
    CheckpointResult, CommandArg, DefragmentResult, DoctorReport, EmptyResult, ExecClass,
    ExecScope, ForkOptions, GcOptions, GcReport, GrantDelta, GrantSet, JobId, JobInfo,
    JobJournalCursor, JobListeningPorts, JobTail, JobTailLimits, LandOptions, LandReport,
    MirrorInfo, NxStateClearResult, OutputPublication, ProjectGrantDelta, PushOptions, PushReport,
    RebaseOptions, RebaseReport, RecoveredImage, RekeyResult, RemoveOptions, RemoveProjectOptions,
    RemoveProjectReport, RemoveReport, ReseedResult, ResizeResult, ResizeVolume, RunSandboxMode,
    ScriptCommand, SealedJob, TraceContext, UnprovenWorkspace, WorkspaceIncarnation, WorkspaceInfo,
    WorkspacePath, WorkspaceTarget,
};
use super::dto::{
    BinaryData, BuildStateListing, CredentialReport, DiskCommandEvent, DiskCommandTool,
    GatewayStatus, IdentityReport, MountResult, NxStoreClearResult, ProjectWorkspaces,
    SccacheStatus, SeedCompactionReport, SkillInstallReport,
};
use super::fd_holder::FdHolders;
use super::held_lock::{DecimalU64, LockObservation};
use super::hold::KernelAddress;
use super::process::{JobProcessEvent, JobProcessTree, JobSubtreeHandle, JobSubtreeRetirement};
use super::reader::{Projected, project};
use super::resources::{JobResourceSample, SampleInterval};
use super::tree_holder::TreeHolder;
use crate::build_volume::BuildStateRefresh;
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::WorkspaceName;
use crate::project_policy::ProjectGrants;
use crate::project_retirement::ProjectRetirementReport;
use crate::repository::{RepoId, RepositoryBinding};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Who may call an operation over a controller connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scope {
    /// Coordinator connections only.
    Coordinator,
    /// Coordinator and worker connections; a worker's request carries its own workspace fence.
    Worker,
    /// Routed only from inside the controller process; never accepted from a connection.
    Internal,
    /// Host connections only (`cowshed host`): the host's own verbs, which no project controller
    /// serves and no project connection may call.
    Host,
    /// `cowshed sccache host` connections only: the sccache daemon's control, served by the build
    /// the package launcher routes the `sccache` verb to.
    Sccache,
}

/// The single bounded raw-byte frame that may accompany an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lane {
    /// JSON request and JSON result only.
    Json,
    /// The request may carry one raw-byte frame after its JSON frame.
    Upload,
    /// The result carries one raw-byte frame starting at the request's declared offset.
    Download,
    /// The call answers with events, one for each demand its caller sends, then ends with an
    /// answer; its result is the type of each event.
    Stream,
}

/// The declared facts of one operation, for validation that runs before a request is decoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationInfo {
    pub method: &'static str,
    pub scope: Scope,
    pub lane: Lane,
}

/// The declared operation named `method`.
pub fn operation(method: &str) -> Option<&'static OperationInfo> {
    OPERATIONS.iter().find(|info| info.method == method)
}

/// Encodes a request as the JSON params of its call, serialized once and framed verbatim.
pub fn encode_request<O: Operation>(request: &O::Request) -> Result<Box<RawValue>> {
    serde_json::value::to_raw_value(request).map_err(|error| {
        CowshedError::usage(
            format!(
                "{} parameters are not representable as JSON: {error}",
                O::METHOD
            ),
            "use UTF-8 paths and validated cowshed option values",
        )
    })
}

/// Decodes a JSON result this build produced, strictly, as the operation's declared result: the
/// controller routing a call to itself. A peer's result is read with [`read_result`].
pub fn decode_result<O: Operation>(value: Value) -> Result<O::Result> {
    serde_json::from_value(value).map_err(|error| {
        CowshedError::new(
            ErrorCode::Internal,
            format!(
                "controller returned an invalid {} response: {error}",
                O::METHOD
            ),
            "cowshed doctor --json",
        )
    })
}

/// Reads a peer controller's JSON result or stream event as the operation's declared result
/// (07_api.md, "Reader projection"): every object member the result's declaration does not name
/// is dropped, at every depth, and the strict decode then certifies the projection, so a missing
/// field or an unknown variant still refuses by name.
pub fn read_result<O: Operation>(mut value: Value) -> Result<O::Result> {
    project(&mut value, <O::Result as Projected>::READER);
    decode_result::<O>(value)
}

/// Encodes an operation's declared result as its call's JSON result.
pub fn encode_result<O: Operation>(result: &O::Result) -> Result<Value> {
    serde_json::to_value(result).map_err(|error| {
        CowshedError::internal(format!("serialize the {} result: {error}", O::METHOD))
    })
}

fn decode_params<T: DeserializeOwned>(method: &str, params: &Value) -> Result<T> {
    T::deserialize(params).map_err(|error| {
        CowshedError::usage(
            format!("invalid {method} parameters: {error}"),
            "upgrade the client and controller together",
        )
    })
}

fn encode_params(method: &str, request: &impl Serialize) -> Result<Value> {
    serde_json::to_value(request).map_err(|error| {
        CowshedError::usage(
            format!("{method} parameters are not representable as JSON: {error}"),
            "use UTF-8 paths and validated cowshed option values",
        )
    })
}

/// Expands the operation table. Each row is
/// `scope lane "method" Marker(Request) -> Result;` where `scope` is `coordinator`, `worker`,
/// `internal` or `host`, and `lane` is `json`, `upload`, `download(offset_field)` or `stream`.
macro_rules! operations {
    (@scope coordinator) => { Scope::Coordinator };
    (@scope worker) => { Scope::Worker };
    (@scope internal) => { Scope::Internal };
    (@scope host) => { Scope::Host };
    (@scope sccache) => { Scope::Sccache };
    (@lane json) => { Lane::Json };
    (@lane upload) => { Lane::Upload };
    (@lane download ($offset:ident)) => { Lane::Download };
    (@lane stream) => { Lane::Stream };
    (@offset $request:ident json) => {{ let _ = $request; None }};
    (@offset $request:ident upload) => {{ let _ = $request; None }};
    (@offset $request:ident download ($offset:ident)) => { Some($request.$offset) };
    (@offset $request:ident stream) => {{ let _ = $request; None }};
    (@streams $marker:ident stream) => { impl StreamOperation for $marker {} };
    (@streams $marker:ident $lane:ident) => {};
    ($(
        $(#[doc = $doc:literal])+
        $scope:ident $lane:ident $(($offset:ident))? $method:literal
            $marker:ident($request:ty) -> $result:ty;
    )+) => {
        /// One declared controller operation. Sealed: the table below is every operation there
        /// is, so no crate can mint a marker that names another operation's method.
        pub trait Operation: sealed::Sealed + Send + Sync + 'static {
            const METHOD: &'static str;
            const SCOPE: Scope;
            const LANE: Lane;
            /// Plain data, so any task may carry a call across its awaits.
            type Request: Serialize + DeserializeOwned + Send + Sync + 'static;
            type Result: Serialize + DeserializeOwned + Projected + Send + Sync + 'static;

            /// The decoded form the controller routes.
            fn request(request: Self::Request) -> OperationRequest;
        }

        /// An operation of the stream lane: its call answers with events of its declared
        /// result, one per demand, then ends. Implemented exactly by the table's `stream` rows.
        pub trait StreamOperation: Operation {}

        $(
            $(#[doc = $doc])+
            #[derive(Debug)]
            pub enum $marker {}

            impl sealed::Sealed for $marker {}

            impl Operation for $marker {
                const METHOD: &'static str = $method;
                const SCOPE: Scope = operations!(@scope $scope);
                const LANE: Lane = operations!(@lane $lane $(($offset))?);
                type Request = $request;
                type Result = $result;

                fn request(request: Self::Request) -> OperationRequest {
                    OperationRequest::$marker(request)
                }
            }

            operations!(@streams $marker $lane);
        )+

        /// Every declared operation's facts, in declaration order.
        pub const OPERATIONS: &[OperationInfo] = &[$(
            OperationInfo {
                method: $method,
                scope: operations!(@scope $scope),
                lane: operations!(@lane $lane $(($offset))?),
            },
        )+];

        /// A decoded controller request: exactly one declared operation and its typed request.
        #[derive(Debug, PartialEq)]
        pub enum OperationRequest {
            $($marker($request),)+
        }

        impl OperationRequest {
            /// Decodes `params` as the declared request of `method`.
            pub fn decode(method: &str, params: &Value) -> Result<Self> {
                match method {
                    $($method => decode_params(method, params).map(Self::$marker),)+
                    method => Err(CowshedError::usage(
                        format!("unknown controller method {method}"),
                        "upgrade the client and controller together",
                    )),
                }
            }

            /// The JSON params this request travels as.
            pub fn params(&self) -> Result<Value> {
                match self {
                    $(Self::$marker(request) => encode_params($method, request),)+
                }
            }

            pub fn method(&self) -> &'static str {
                match self {
                    $(Self::$marker(_) => $method,)+
                }
            }

            /// The offset a download operation's raw-byte frame starts at.
            pub fn download_offset(&self) -> Option<u64> {
                match self {
                    $(Self::$marker(request) => operations!(@offset request $lane $(($offset))?),)+
                }
            }
        }
    };
}

mod sealed {
    pub trait Sealed {}
}

/// Which handle serves which operation, and how it builds the request. Generated beside the table
/// it is generated from, so it names the request records and their field types as the table does.
#[path = "served.generated.rs"]
pub(super) mod served;

operations! {
    /// Resolves a path to the project this controller is bound to.
    coordinator json "project.open" ProjectOpen(ProjectOpenRequest) -> ProjectOpened;
    /// Resolves one workspace by name.
    coordinator json "project.workspace" ProjectWorkspace(WorkspaceRequest) -> WorkspaceView;
    /// Resolves the workspace an existing path belongs to.
    coordinator json "project.workspaceAt" ProjectWorkspaceAt(WorkspaceAtRequest) -> WorkspaceView;
    /// Lists every published workspace.
    coordinator json "project.list" ProjectList(ListRequest) -> WorkspaceListView;
    /// Resolves the workspace that currently occupies one build slot.
    coordinator json "project.workspaceInSlot" ProjectWorkspaceInSlot(WorkspaceInSlotRequest) -> WorkspaceView;
    /// Refreshes one workspace's information.
    coordinator json "workspace.info" WorkspaceInfoRead(WorkspaceRequest) -> WorkspaceInfo;
    /// Attaches a detached workspace.
    coordinator json "workspace.attach" WorkspaceAttach(WorkspaceAttachRequest) -> EmptyResult;
    /// Reads one workspace's grants; a worker reads its own, fenced on its incarnation.
    worker json "workspace.grants" WorkspaceGrants(WorkspaceGrantsRequest) -> GrantSet;
    /// The build volume a job of the workspace incarnation would be granted now.
    coordinator json "workspace.buildVolume" WorkspaceBuildVolume(WorkerScope) -> BuildVolume;
    /// Renames a session workspace.
    coordinator json "coordinator.rename" CoordinatorRename(SourceDestinationRequest) -> WorkspaceView;
    /// Adopts the project's checkout as main.
    coordinator json "coordinator.adopt" CoordinatorAdopt(AdoptRequest) -> AdoptView;
    /// Mints a warm workspace from main or an explicit source.
    coordinator json "coordinator.fork" CoordinatorFork(ForkRequest) -> WorkspaceView;
    /// Moves the project's checkout.
    coordinator json "coordinator.moveCheckout" CoordinatorMoveCheckout(MoveCheckoutRequest) -> WorkspaceView;
    /// Changes the adopted project's repository identity.
    coordinator json "coordinator.changeRepoId" CoordinatorChangeRepoId(ChangeRepoIdRequest) -> WorkspaceView;
    /// Adds grants to one workspace.
    coordinator json "coordinator.grant" CoordinatorGrant(GrantRequest) -> GrantSet;
    /// Removes grants from one workspace.
    coordinator json "coordinator.revoke" CoordinatorRevoke(GrantRequest) -> GrantSet;
    /// Reads the project's standing grants.
    coordinator json "coordinator.projectGrants" CoordinatorProjectGrants(RepoRequest) -> ProjectGrants;
    /// Adds project-wide grants.
    coordinator json "coordinator.grantProject" CoordinatorGrantProject(ProjectGrantRequest) -> ProjectGrants;
    /// Removes project-wide grants.
    coordinator json "coordinator.revokeProject" CoordinatorRevokeProject(ProjectGrantRequest) -> ProjectGrants;
    /// Rebases a workspace onto what it lands into.
    coordinator json "coordinator.rebase" CoordinatorRebase(RebaseRequest) -> RebaseReport;
    /// Lands a workspace into its target.
    coordinator json "coordinator.land" CoordinatorLand(LandRequest) -> LandReport;

    /// Restores a workspace to a checkpoint.
    coordinator json "coordinator.restore" CoordinatorRestore(RestoreRequest) -> EmptyResult;
    /// Grows a workspace's image or build volume.
    coordinator json "coordinator.resize" CoordinatorResize(ResizeRequest) -> ResizeResult;
    /// Rewrites a workspace's image contiguously.
    coordinator json "coordinator.defragment" CoordinatorDefragment(WorkspaceRequest) -> DefragmentResult;
    /// Rebuilds one keyless workspace's CA identity at its preserved incarnation.
    coordinator json "coordinator.rekey" CoordinatorRekey(WorkspaceRequest) -> RekeyResult;
    /// Replaces a workspace's build volume with a clone of an existing target seed.
    coordinator json "coordinator.reseed" CoordinatorReseed(ReseedRequest) -> ReseedResult;
    /// Deletes a workspace's Nx cache and task database contents on its build volume.
    coordinator json "coordinator.clearNxState" CoordinatorClearNxState(WorkspaceRequest) -> NxStateClearResult;
    /// Detaches a workspace.
    coordinator json "coordinator.detach" CoordinatorDetach(WorkspaceRequest) -> EmptyResult;
    /// Assigns a workspace's gateway port slot.
    coordinator json "coordinator.assignSlot" CoordinatorAssignSlot(SlotRequest) -> EmptyResult;
    /// Destroys a session workspace.
    coordinator json "coordinator.destroy" CoordinatorDestroy(DestroyRequest) -> RemoveReport;
    /// Collects unreferenced storage.
    coordinator json "coordinator.gc" CoordinatorGc(GcRequest) -> GcReport;
    /// Moves one recovery image, with its companions, out of the store.
    coordinator json "coordinator.recoverImage" CoordinatorRecoverImage(RecoverImageRequest) -> RecoveredImage;
    /// Removes the adopted project end to end.
    coordinator json "coordinator.removeProject" CoordinatorRemoveProject(RemoveProjectRequest) -> RemoveProjectReport;
    /// Points a workspace's repository mirror at a URL.
    coordinator json "coordinator.repoMirror" CoordinatorRepoMirror(MirrorRequest) -> MirrorInfo;
    /// Sets a workspace's checkpoint quota.
    coordinator json "coordinator.setCheckpointQuota" CoordinatorSetCheckpointQuota(QuotaRequest) -> EmptyResult;
    /// Reports the project's health findings.
    coordinator json "coordinator.doctor" CoordinatorDoctor(RepoRequest) -> DoctorReport;
    /// Mints a worker capability for one workspace.
    coordinator json "coordinator.worker" CoordinatorWorker(WorkspaceRequest) -> WorkerView;
    /// Admits one job under coordinator authority, the only route that may ask for the gate class or
    /// the host-gate scope. Inline stdin travels as the upload frame.
    coordinator upload "coordinator.exec" CoordinatorExec(ExecParams) -> JobId;
    /// Starts serving a workspace's supervisor.
    internal json "coordinator.serveSupervisor" CoordinatorServeSupervisor(WorkspaceRequest) -> EmptyResult;
    /// Refreshes a mounted workspace's build state.
    internal json "coordinator.refreshBuildState" CoordinatorRefreshBuildState(WorkspaceRequest) -> BuildStateRefresh;
    /// Checks the refreshed immutable main seed and compacts it when it reaches the extent bound.
    internal json "coordinator.compactSeed" CoordinatorCompactSeed(RepoRequest) -> SeedCompactionReport;
    /// Admits one job; inline stdin travels as the upload frame.
    worker upload "worker.exec" WorkerExec(ExecParams) -> JobId;
    /// Writes one chunk of a streamed stdin at its byte offset.
    worker upload "worker.stdinChunk" WorkerStdinChunk(StdinWriteRequest) -> EmptyResult;
    /// Ends a streamed stdin.
    worker json "worker.stdinClose" WorkerStdinClose(JobRequest) -> EmptyResult;
    /// Opens a shell session.
    worker json "worker.shell" WorkerShell(SessionRequest) -> EmptyResult;
    /// Lists the workspace incarnation's jobs.
    worker json "worker.listJobs" WorkerListJobs(WorkerScope) -> Vec<JobInfo>;
    /// Resolves one job.
    worker json "worker.job" WorkerJob(JobRequest) -> JobInfo;
    /// Names the job an admission key admitted in the workspace incarnation, or none when its
    /// durable records prove the key admitted no job.
    worker json "worker.jobByKey" WorkerJobByKey(AdmissionKeyRequest) -> Option<JobId>;
    /// Takes a checkpoint.
    worker json "worker.checkpoint" WorkerCheckpoint(CheckpointRequest) -> CheckpointResult;
    /// Pushes the workspace's branch.
    worker json "worker.push" WorkerPush(PushRequest) -> PushReport;
    /// Reads one job's status.
    worker json "job.status" JobStatus(JobRequest) -> JobInfo;
    /// Reads the job's current resource sample, or its frozen terminal sample; unowned jobs refuse it.
    worker json "job.resources" JobResourcesRead(JobRequest) -> JobResourceSample;
    /// Reads one ended job's sealed record.
    worker json "job.sealed" JobSealed(JobRequest) -> SealedJob;
    /// Reads one bounded raw chunk from an offset. Without follow, an empty chunk is the current
    /// written end even when eof is false; follow waits on supervisor output notifications.
    worker download(offset) "job.logs" JobLogs(LogsRequest) -> LogsChunk;
    /// Reads a bounded slice of both streams after a cursor, or their latest bounded tail.
    worker json "job.tail" JobTailRead(TailRequest) -> JobTail;
    /// Reads the TCP ports the job's process group listens on, from the kernel.
    worker json "job.listeningPorts" JobListeningPortsRead(JobRequest) -> JobListeningPorts;
    /// Reads the retained native process tree with its absorbing coverage evidence.
    worker json "job.processes" JobProcesses(JobRequest) -> JobProcessTree;
    /// Streams ordered retained process transitions and the first coverage gap, without coalescing.
    worker stream "job.processEvents" JobProcessEvents(ProgressRequest) -> JobProcessEvent;
    /// Streams one job's resource samples: the latest at once, one every interval while it runs,
    /// then its terminal sample once.
    worker stream "job.progress" JobProgress(ProgressRequest) -> JobResourceSample;
    /// Writes to an attached job's stdin at its byte offset, answered once the bytes reached the
    /// job's stdin pipe.
    worker upload "job.attachWrite" JobAttachWrite(StdinWriteRequest) -> EmptyResult;
    /// Ends an attached job's stdin: one EOF however often it is called; the job continues.
    worker json "job.attachClose" JobAttachClose(JobRequest) -> EmptyResult;
    /// Detaches from a job.
    worker json "job.detach" JobDetach(JobRequest) -> EmptyResult;
    /// Waits for a job to end.
    worker json "job.wait" JobWait(JobRequest) -> JobInfo;
    /// Kills a job's complete process group.
    worker json "job.kill" JobKill(JobRequest) -> EmptyResult;
    /// Closes a shell session.
    worker json "session.close" SessionClose(SessionRequest) -> EmptyResult;
    /// Lists every adopted project on the host with its published workspaces.
    host json "host.listProjects" HostListProjects(HostListRequest) -> Vec<ProjectWorkspaces>;
    /// Attaches every detached session workspace of every adopted project.
    host json "host.attachAll" HostAttachAll(HostAttachAllRequest) -> Vec<WorkspaceInfo>;
    /// Detaches every attached session workspace of every adopted project.
    host json "host.detachAll" HostDetachAll(HostRequest) -> EmptyResult;
    /// Retires one project no controller can open, from store records; the call is the abandonment.
    host json "host.forgetAbandoned" HostForgetAbandoned(RepoRequest) -> ProjectRetirementReport;
    /// Mounts one project's main from store records.
    host json "host.mountMain" HostMountMain(RepoRequest) -> MountResult;
    /// Reports the gateway service's health.
    host json "host.gatewayStatus" HostGatewayStatus(HostRequest) -> GatewayStatus;
    /// Lists a project's enrolled credential routes.
    host json "host.credentialStatus" HostCredentialStatus(ProjectPathRequest) -> CredentialReport;
    /// Removes one of a project's credentials.
    host json "host.credentialRemove" HostCredentialRemove(CredentialRemoveRequest) -> CredentialReport;
    /// Binds a remote the project's main checkout already configures.
    host json "host.identityAdd" HostIdentityAdd(IdentityAddRequest) -> IdentityReport;
    /// Installs the cowshed skill into the named agent harnesses.
    host json "host.skillInstall" HostSkillInstall(SkillInstallRequest) -> SkillInstallReport;
    /// Reads a checkout's recorded build-state paths, opening no controller.
    host json "host.buildState" HostBuildState(ProjectPathRequest) -> BuildStateListing;
    /// Clears the project's Nx accepted-task store in the gateway.
    host json "host.clearNxStore" HostClearNxStore(ProjectPathRequest) -> NxStoreClearResult;
    /// Installs the running build and starts the gateway onto it.
    host json "host.gatewayStart" HostGatewayStart(HostRequest) -> GatewayStatus;
    /// Stops the gateway; with `purge`, also removes the installed binary.
    host json "host.gatewayStop" HostGatewayStop(GatewayStopRequest) -> EmptyResult;
    /// Who the kernel shows holding a lock file.
    host json "host.lockHolders" HostLockHolders(HostPathRequest) -> LockObservation;
    /// Every process of this user holding something inside a directory tree.
    host json "host.treeHolders" HostTreeHolders(HostPathRequest) -> Vec<TreeHolder>;
    /// Every process with a descriptor on one of the kernel objects a caller names.
    host json "host.fdHolders" HostFdHolders(FdHoldersRequest) -> FdHolders;
    /// Asks the job supervisor this host connection runs under to retire a retained subtree.
    host json "host.retireDescendants" HostRetireDescendants(RetireDescendantsRequest) -> JobSubtreeRetirement;
    /// Enrols a scoped gateway registry credential for a project; `upload` secrets travel as the
    /// upload frame.
    host upload "host.credentialAdd" HostCredentialAdd(CredentialAddRequest) -> CredentialReport;
    /// Runs one diskutil or hdiutil argv through cowshed's disk runner, its output after its end.
    host stream "host.diskCommand" HostDiskCommand(DiskCommandRequest) -> DiskCommandEvent;
    /// Installs (or repairs) the sccache LaunchAgent and waits for its socket.
    sccache json "sccache.start" SccacheStart(SccacheStartRequest) -> SccacheStatus;
    /// Removes the sccache LaunchAgent; the daemon stops.
    sccache json "sccache.stop" SccacheStop(HostRequest) -> EmptyResult;
    /// Reports the sccache daemon's state.
    sccache json "sccache.status" SccacheStatusRead(HostRequest) -> SccacheStatus;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectOpenRequest {
    /// Absolute, lexically normalized path inside the project.
    pub path: String,
}

/// The bound project's identity. Its fields are the controller's own shared values, so answering
/// an open copies them once, into the wire form.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectOpened {
    pub repo_id: RepoId,
    pub binding: Arc<RepositoryBinding>,
    pub git_root: Arc<Path>,
    pub store_root: Arc<Path>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepoRequest {
    pub repo_id: RepoId,
}

/// Which landing measurement a listing carries (`cowshed ls --landing`/`--landed`).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ListLanding {
    /// Measure every attached session workspace against main's landing branch.
    Measure,
    /// Measure, then keep only the session workspaces whose work is provably fully landed.
    Landed,
}

/// The project's published workspaces, measured for landing only when the caller asks to pay
/// for it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListRequest {
    pub repo_id: RepoId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landing: Option<ListLanding>,
}

/// The build slot whose current tenant a caller resolves (`cowshed path --slot <n>`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceInSlotRequest {
    pub repo_id: RepoId,
    pub slot: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
}

/// A workspace's grants: a coordinator names the workspace, a worker also proves the
/// incarnation its connection is fenced to.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceGrantsRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_incarnation: Option<WorkspaceIncarnation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceAtRequest {
    pub repo_id: RepoId,
    pub path: String,
}

/// One workspace's information and grants, as one call resolved them.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceView {
    pub info: WorkspaceInfo,
    pub grants: GrantSet,
}

impl AsRef<WorkspaceInfo> for WorkspaceView {
    fn as_ref(&self) -> &WorkspaceInfo {
        &self.info
    }
}

impl AsMut<WorkspaceInfo> for WorkspaceView {
    fn as_mut(&mut self) -> &mut WorkspaceInfo {
        &mut self.info
    }
}

/// What `project.list` answers: a view of each workspace whose mount state is proven, and each
/// unproven one by name, which has no row (07_api.md, "Frozen wire projections").
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceListView {
    pub workspaces: Vec<WorkspaceView>,
    pub unproven: Vec<UnprovenWorkspace>,
}

/// The workspace a worker capability was minted for. The wire form is the [`WorkspaceView`]; the
/// type is what lets only the call that mints a worker capability yield one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct WorkerView(pub WorkspaceView);

/// What `coordinator.adopt` made: main, and, when the call replaced main, what the replacement
/// kept (02_workspaces.md, "Re-adoption: space reclamation", step 5).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdoptView {
    pub workspace: WorkspaceView,
    /// Null unless this call re-adopted main.
    pub re_adoption: Option<ReAdoption>,
}

/// A completed re-adoption: the incarnation that replaced main, and every path it kept of what it
/// replaced. Once the replacement is checked, removing exactly those paths returns their space.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReAdoption {
    pub replacement: WorkspaceIncarnation,
    pub retained: Vec<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceAttachRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub options: AttachOptions,
}

/// One workspace incarnation: the fence every worker request carries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerScope {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
}

/// The build volume a job would be granted, or `None` when the checkout links none. Always an
/// object: an RPC envelope carries no result for a bare `null`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildVolume {
    pub volume: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceDestinationRequest {
    pub repo_id: RepoId,
    pub source: WorkspaceName,
    pub destination: WorkspaceName,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdoptRequest {
    pub repo_id: RepoId,
    pub options: AdoptOptions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForkRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<WorkspaceName>,
    pub options: ForkOptions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MoveCheckoutRequest {
    pub repo_id: RepoId,
    pub destination: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChangeRepoIdRequest {
    pub repo_id: RepoId,
    pub new_repo_id: RepoId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GrantRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub delta: GrantDelta,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectGrantRequest {
    pub repo_id: RepoId,
    pub delta: ProjectGrantDelta,
}

/// A unit's rebase: the unit, what it lands into (main when absent), and its options.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RebaseRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub into: Option<WorkspaceTarget>,
    pub options: RebaseOptions,
}

/// A unit's land: the unit, what it lands into (main when absent), and its options.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub into: Option<WorkspaceTarget>,
    pub options: LandOptions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RestoreRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub label: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResizeRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub capacity: String,
    pub volume: ResizeVolume,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReseedRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub from: WorkspaceName,
    pub dry_run: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlotRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub slot: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DestroyRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub options: RemoveOptions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GcRequest {
    pub repo_id: RepoId,
    pub options: GcOptions,
}

/// `cowshed gc --recover <image> --to <dir>`: move the recovery image at `image`, one of the
/// project's `sessions/.trash/<ws>-<incarnation>.dmg`, with its companions and a manifest, into
/// `<to>/<ws>-<incarnation>/` outside the store. Both paths are absolute.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoverImageRequest {
    pub repo_id: RepoId,
    pub image: PathBuf,
    pub to: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveProjectRequest {
    pub repo_id: RepoId,
    pub options: RemoveProjectOptions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MirrorRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuotaRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub quota: CheckpointQuota,
}

/// Where an admitted job's stdin comes from. Inline bytes travel as the call's upload frame; a
/// stream's chunks follow admission as `worker.stdinChunk` calls from the client that admitted
/// it. Open stdin has no producer: it stays open after admission, its attachment writes it with
/// `job.attachWrite`, and `job.attachClose` ends it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ExecStdin {
    Empty,
    Inline,
    Stream,
    Open,
    WorkspaceFile {
        #[serde(rename = "workspacePath")]
        workspace_path: WorkspacePath,
    },
}

fn is_background(class: &ExecClass) -> bool {
    *class == ExecClass::Background
}

fn is_workspace(scope: &ExecScope) -> bool {
    *scope == ExecScope::Workspace
}

/// One job admission: the worker fence, the session it runs in, exactly one of `argv` and
/// `script`, and the job's environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecParams {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argv: Option<Vec<CommandArg>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<ScriptCommand>,
    pub cwd: Option<WorkspacePath>,
    pub mode: RunSandboxMode,
    pub env: HashMap<String, String>,
    pub trace: Option<TraceContext>,
    pub stdin: ExecStdin,
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_key: Option<AdmissionKey>,
    /// The class the job is admitted under (05_gateway.md "Classes"), omitted for background, which
    /// is what ordinary exec asks for: `interactive` for a command its requester awaits, and
    /// `resident` for one that serves requests, which a worker may ask for; only the
    /// coordinator.exec route admits the gate class.
    #[serde(default, skip_serializing_if = "is_background")]
    pub class: ExecClass,
    /// The count the job's jobserver holds, at most the host's cores; omitted for the host's cores
    /// (05_gateway.md "Parallelism inside a job").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub want: Option<NonZeroUsize>,
    /// Where the command runs; omitted for the workspace sandbox.
    #[serde(default, skip_serializing_if = "is_workspace")]
    pub scope: ExecScope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdmissionKeyRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub admission_key: AdmissionKey,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
}

/// One write of a job's stdin: the job's fence, and the byte of the job's input at which the
/// call's upload frame starts. A write continues the input at its next byte; one within input
/// the job already holds is answered without delivering anything twice, but only when its bytes
/// equal what the job holds there.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StdinWriteRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub offset: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub session: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub options: CheckpointOptions,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub options: PushOptions,
}

/// Which captured stream a log read walks.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStream {
    Stdout,
    Stderr,
}

impl From<JobStream> for crate::storage::job_artifact::StreamKind {
    fn from(stream: JobStream) -> Self {
        match stream {
            JobStream::Stdout => Self::Stdout,
            JobStream::Stderr => Self::Stderr,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogsRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub stream: JobStream,
    pub follow: bool,
    pub offset: u64,
}

/// The JSON half of a `job.logs` answer; the bytes follow as its raw-byte frame.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogsChunk {
    /// The stream has closed, not merely reached its current written end.
    pub eof: bool,
    pub next_offset: u64,
}

/// A bounded tail of one job: after `cursor` when it is named, else the latest one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TailRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    /// Absent for the latest tail; omitted from the wire, never `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<JobJournalCursor>,
    pub limits: JobTailLimits,
}

/// A progress subscription to one job, sampled every `everyMs`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProgressRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub every_ms: SampleInterval,
}

/// A host operation that names nothing: the host is its whole subject.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostRequest {}

/// Every adopted project's listing (`ls --all`), measured for landing when asked.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostListRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landing: Option<ListLanding>,
}

/// `attach --all [--browse]`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostAttachAllRequest {
    #[serde(default)]
    pub browse: bool,
}

/// A host operation whose subject is a project: its checkout, resolved as `--project` is.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectPathRequest {
    pub project: PathBuf,
}

/// `credential rm --origin`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialRemoveRequest {
    pub project: PathBuf,
    pub origin: String,
}

/// `identity add <remote>`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityAddRequest {
    pub project: PathBuf,
    pub remote: String,
}

/// `[--project <root>] skill install [--harness]`: no harness installs every detected one.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SkillInstallRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub harnesses: Vec<String>,
    /// Install into this project's harness directories instead of the user's (`--project`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<PathBuf>,
}

/// `disk-command <tool> [--deadline-ms] -- <argv>`. The verb's inherited standard input is
/// `stdin`: the tool's whole input, closed after it, empty when absent, so the tool reads end of
/// input at once; a host connection never hands a child its own.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DiskCommandRequest {
    pub tool: DiskCommandTool,
    /// The total admission-and-child budget of an ordinary verb, 120 s when absent, exactly as
    /// `--deadline-ms`: an image verb keeps its warning-only, never-kill policy whatever it says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<std::num::NonZeroU64>,
    pub argv: Vec<CommandArg>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<BinaryData>,
    /// `--registry-entry`: the IORegistry entry ID `hdiutil detach <device>` is bound to; the
    /// detach runs only while that entry still names the device, read inside the disk admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_entry: Option<u64>,
    /// `--gateway-socket`: the absolute control socket of the authenticated gateway that owns
    /// disk admission; absent selects the host gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_socket: Option<PathBuf>,
}

/// `credential add`: one exact HTTPS origin, the registry path prefixes it may reach, the methods
/// (absent or empty: GET and HEAD), and where the secret comes from.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialAddRequest {
    pub project: PathBuf,
    pub origin: String,
    pub path_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub methods: Vec<String>,
    pub secret: CredentialSecretSource,
}

/// Where `credential add`'s secret comes from: named, never carried in a request field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum CredentialSecretSource {
    /// `--secret-env <name> [--secret-command]`: the variable in `cowshed host`'s environment,
    /// which is the `openHost` caller's, withheld from every child of the project from then on;
    /// when it is unset or empty, `command` is run instead.
    Env {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        command: Option<Vec<String>>,
    },
    /// `--secret-command <argv>`: a helper run with no stdin, its trimmed stdout the secret.
    Command { argv: Vec<String> },
    /// `--secret-stdin`: the call's upload frame.
    Upload,
}

/// `sccache start [--capacity]`: the cache cap, as `--capacity` spells it (`40g`, `1t`); absent
/// derives it from the store.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SccacheStartRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
}

/// `gateway stop [--purge]`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayStopRequest {
    pub purge: bool,
}

/// A host operation whose subject is a path the caller names, read as the CLI reads its argument.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostPathRequest {
    pub path: PathBuf,
}

/// `fd-holders [--device <0x…>]… [--inode <n>]…`: the kernel objects whose holders the census
/// names, as `hold`'s `started.output` names them; at least one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FdHoldersRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<KernelAddress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inodes: Vec<DecimalU64>,
}

/// `retire-descendants --grace-ms`: the handle is the verb's standard input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetireDescendantsRequest {
    pub handle: JobSubtreeHandle,
    /// Nonnegative milliseconds; zero retires at once, without a grace interval.
    pub grace_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// One canonical request for every declared operation. The controller's generated codec,
    /// the connection tests and the N-API projection all read this one corpus.
    const CORPUS: &str = include_str!("operations.corpus.json");

    fn corpus() -> BTreeMap<String, Value> {
        serde_json::from_str(CORPUS).expect("the corpus is a JSON object of requests")
    }

    #[test]
    fn the_corpus_names_every_declared_operation_once() {
        let corpus = corpus();
        let mut declared: Vec<&str> = OPERATIONS
            .iter()
            .map(|operation| operation.method)
            .collect();
        declared.sort_unstable();
        let named: Vec<&str> = corpus.keys().map(String::as_str).collect();
        assert_eq!(named, declared);
    }

    #[test]
    fn every_corpus_request_round_trips_through_the_generated_codec() {
        let mut drifted = BTreeMap::new();
        for (method, params) in corpus() {
            let request = OperationRequest::decode(&method, &params)
                .unwrap_or_else(|error| panic!("{method} corpus request: {error:?}"));
            assert_eq!(request.method(), method);
            let encoded = request.params().expect("encode");
            if encoded != params {
                drifted.insert(method.clone(), encoded);
            }
            let declared = operation(&method).expect("declared");
            assert_eq!(
                request.download_offset().is_some(),
                declared.lane == Lane::Download,
                "{method}: only a download names its offset"
            );
        }
        assert!(
            drifted.is_empty(),
            "corpus requests that do not encode back to themselves, as encoded:\n{}",
            serde_json::to_string_pretty(&drifted).expect("JSON")
        );
    }

    #[test]
    fn fork_is_the_only_public_mint_and_omitted_source_means_main() {
        let params = serde_json::json!({
            "repoId": "acme/widget", "workspace": "raven",
            "options": { "clean": false, "browse": false, "register": false, "gitWorktree": false }
        });
        let request = OperationRequest::decode("coordinator.fork", &params).expect("fork");
        let OperationRequest::CoordinatorFork(fork) = &request else {
            panic!("expected fork")
        };
        assert_eq!(fork.source, None);
        assert_eq!(request.params().unwrap(), params);
        assert_eq!(
            OperationRequest::decode("coordinator.create", &params)
                .unwrap_err()
                .code,
            crate::error::ErrorCode::Usage
        );
    }

    #[test]
    fn reseed_is_coordinator_only_and_requires_explicit_target_and_preview_mode() {
        let declaration = operation("coordinator.reseed").expect("reseed operation");
        assert_eq!(declaration.scope, Scope::Coordinator);
        assert_eq!(declaration.lane, Lane::Json);
        let params = serde_json::json!({
            "repoId": "acme/widget", "workspace": "raven", "from": "main", "dryRun": true
        });
        let request = OperationRequest::decode("coordinator.reseed", &params).expect("reseed");
        let OperationRequest::CoordinatorReseed(reseed) = &request else {
            panic!("expected reseed")
        };
        assert_eq!(reseed.workspace.as_str(), "raven");
        assert_eq!(reseed.from, WorkspaceName::main());
        assert!(reseed.dry_run);
        assert_eq!(request.params().unwrap(), params);
        for field in ["from", "dryRun"] {
            let mut missing = params.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(OperationRequest::decode("coordinator.reseed", &missing).is_err());
        }
        let mut extra = params.clone();
        extra["refreshSeed"] = serde_json::json!(true);
        assert!(OperationRequest::decode("coordinator.reseed", &extra).is_err());
    }

    /// The latest tail names no cursor: the field is omitted, never `null`, both ways.
    #[test]
    fn a_latest_tail_omits_its_cursor() {
        let mut params = corpus()
            .remove("job.tail")
            .expect("job.tail corpus request");
        params.as_object_mut().expect("an object").remove("cursor");
        let request = OperationRequest::decode("job.tail", &params).expect("decode");
        let OperationRequest::JobTailRead(tail) = &request else {
            panic!("decoded {request:?}");
        };
        assert_eq!(tail.cursor, None);
        assert_eq!(request.params().expect("encode"), params);
    }

    /// A request that names no scheduling is ordinary work: background, in the workspace sandbox,
    /// wanting its class's own count. Its defaults are omitted again when it is encoded.
    #[test]
    fn an_exec_that_names_no_scheduling_is_ordinary_background_work() {
        let params = corpus()
            .remove("worker.exec")
            .expect("worker.exec corpus request");
        for field in ["class", "want", "scope"] {
            assert!(
                params.get(field).is_none(),
                "{field} is omitted by the corpus"
            );
        }
        let request = OperationRequest::decode("worker.exec", &params).expect("decode");
        let OperationRequest::WorkerExec(exec) = &request else {
            panic!("decoded {request:?}");
        };
        assert_eq!(exec.class, ExecClass::Background);
        assert_eq!(exec.want, None);
        assert_eq!(exec.scope, ExecScope::Workspace);
        assert_eq!(request.params().expect("encode"), params);
    }

    /// The coordinator route's corpus request names every scheduling field, in the camel case the
    /// wire spells them, and decodes into exactly those values.
    #[test]
    fn a_coordinator_exec_carries_its_gate_scheduling() {
        let params = corpus()
            .remove("coordinator.exec")
            .expect("coordinator.exec corpus request");
        let request = OperationRequest::decode("coordinator.exec", &params).expect("decode");
        let OperationRequest::CoordinatorExec(exec) = &request else {
            panic!("decoded {request:?}");
        };
        assert_eq!(exec.class, ExecClass::Gate);
        assert_eq!(exec.want.map(NonZeroUsize::get), Some(8));
        assert_eq!(exec.scope, ExecScope::HostGate);
        assert_eq!(request.params().expect("encode"), params);
    }

    /// Zero tokens and an unknown class or scope are refused as usage before any admission: the
    /// wire has no spelling for them.
    #[test]
    fn exec_scheduling_refuses_what_the_wire_cannot_spell() {
        for (field, value) in [
            ("want", serde_json::json!(0)),
            ("class", serde_json::json!("foreground")),
            ("scope", serde_json::json!("host")),
        ] {
            let mut params = corpus()
                .remove("coordinator.exec")
                .expect("coordinator.exec corpus request");
            params[field] = value;
            let error = OperationRequest::decode("coordinator.exec", &params)
                .expect_err("the declaration refuses it");
            assert_eq!(error.code, crate::error::ErrorCode::Usage, "{field}");
        }
    }

    /// The gate class has exactly one route in: coordinator.exec is coordinator-scoped and carries
    /// inline stdin as an upload, as worker.exec does.
    #[test]
    fn the_gate_route_is_coordinator_scoped_and_carries_an_upload() {
        let gate = operation("coordinator.exec").expect("declared");
        assert_eq!((gate.scope, gate.lane), (Scope::Coordinator, Lane::Upload));
        let worker = operation("worker.exec").expect("declared");
        assert_eq!((worker.scope, worker.lane), (Scope::Worker, Lane::Upload));
    }

    #[test]
    fn an_unknown_method_is_a_usage_error() {
        let error = OperationRequest::decode("job.teleport", &serde_json::json!({}))
            .expect_err("undeclared");
        assert_eq!(error.code, crate::error::ErrorCode::Usage);
    }

    /// A scratch declaration: `job.kill` as it would read with its request's `jobId` field
    /// renamed. The table expands it into its own codec beside the real one.
    #[expect(
        dead_code,
        reason = "only the scratch declaration's generated decoder is exercised"
    )]
    mod scratch {
        use super::*;

        #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct MutatedJobRequest {
            pub repo_id: RepoId,
            pub workspace: WorkspaceName,
            pub workspace_incarnation: WorkspaceIncarnation,
            pub job: JobId,
        }

        operations! {
            /// Kills a job's complete process group.
            worker json "job.kill" JobKill(MutatedJobRequest) -> EmptyResult;
        }
    }

    #[test]
    fn a_mutated_request_field_changes_the_generated_codec_and_breaks_the_corpus() {
        let params = corpus()
            .remove("job.kill")
            .expect("job.kill corpus request");
        assert!(OperationRequest::decode("job.kill", &params).is_ok());
        let error = scratch::OperationRequest::decode("job.kill", &params)
            .expect_err("the mutated declaration refuses the committed corpus request");
        assert_eq!(error.code, crate::error::ErrorCode::Usage);
        assert!(
            error.message.contains("invalid job.kill parameters"),
            "{}",
            error.message
        );
        assert_eq!(scratch::OPERATIONS.len(), 1);
    }
}
