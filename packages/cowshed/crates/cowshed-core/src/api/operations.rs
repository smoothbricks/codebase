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
    AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota, CheckpointResult, CommandArg,
    CreateOptions, DefragmentResult, DoctorReport, EmptyResult, GcOptions, GcReport, GrantDelta,
    GrantSet, JobId, JobInfo, JobJournalCursor, JobListeningPorts, JobTail, JobTailLimits,
    LandOptions, LandReport, MirrorInfo, OutputPublication, ProjectGrantDelta, PushOptions,
    PushReport, RebaseOptions, RebaseReport, RemoveOptions, RemoveProjectOptions,
    RemoveProjectReport, RemoveReport, ReseedResult, ResizeResult, ResizeVolume, RunSandboxMode,
    ScriptCommand, SealedJob, TraceContext, WorkspaceIncarnation, WorkspaceInfo, WorkspacePath,
    WorkspaceTarget,
};
use crate::build_volume::BuildStateRefresh;
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::WorkspaceName;
use crate::project_policy::ProjectGrants;
use crate::repository::{RepoId, RepositoryBinding};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;
use std::collections::HashMap;
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

/// Decodes a call's JSON result as the operation's declared result.
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
/// `scope lane "method" Marker(Request) -> Result;` where `scope` is `coordinator`, `worker` or
/// `internal`, and `lane` is `json`, `upload` or `download(offset_field)`.
macro_rules! operations {
    (@scope coordinator) => { Scope::Coordinator };
    (@scope worker) => { Scope::Worker };
    (@scope internal) => { Scope::Internal };
    (@lane json) => { Lane::Json };
    (@lane upload) => { Lane::Upload };
    (@lane download ($offset:ident)) => { Lane::Download };
    (@offset $request:ident json) => {{ let _ = $request; None }};
    (@offset $request:ident upload) => {{ let _ = $request; None }};
    (@offset $request:ident download ($offset:ident)) => { Some($request.$offset) };
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
            type Result: Serialize + DeserializeOwned + Send + Sync + 'static;

            /// The decoded form the controller routes.
            fn request(request: Self::Request) -> OperationRequest;
        }

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
    coordinator json "project.list" ProjectList(RepoRequest) -> Vec<WorkspaceView>;
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
    coordinator json "coordinator.adopt" CoordinatorAdopt(AdoptRequest) -> WorkspaceView;
    /// Creates a session workspace from main.
    coordinator json "coordinator.create" CoordinatorCreate(CreateRequest) -> WorkspaceView;
    /// Forks a workspace into a new session workspace.
    coordinator json "coordinator.fork" CoordinatorFork(SourceDestinationRequest) -> WorkspaceView;
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
    /// Refreezes a target's seed from its live build volume.
    coordinator json "coordinator.reseed" CoordinatorReseed(WorkspaceRequest) -> ReseedResult;
    /// Detaches a workspace.
    coordinator json "coordinator.detach" CoordinatorDetach(WorkspaceRequest) -> EmptyResult;
    /// Assigns a workspace's gateway port slot.
    coordinator json "coordinator.assignSlot" CoordinatorAssignSlot(SlotRequest) -> EmptyResult;
    /// Destroys a session workspace.
    coordinator json "coordinator.destroy" CoordinatorDestroy(DestroyRequest) -> RemoveReport;
    /// Collects unreferenced storage.
    coordinator json "coordinator.gc" CoordinatorGc(GcRequest) -> GcReport;
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
    /// Starts serving a workspace's supervisor.
    internal json "coordinator.serveSupervisor" CoordinatorServeSupervisor(WorkspaceRequest) -> EmptyResult;
    /// Refreshes a mounted workspace's build state.
    internal json "coordinator.refreshBuildState" CoordinatorRefreshBuildState(WorkspaceRequest) -> BuildStateRefresh;
    /// Admits one job; inline stdin travels as the upload frame.
    worker upload "worker.exec" WorkerExec(ExecParams) -> JobId;
    /// Writes one chunk of a streamed stdin.
    worker upload "worker.stdinChunk" WorkerStdinChunk(JobRequest) -> EmptyResult;
    /// Ends a streamed stdin.
    worker json "worker.stdinClose" WorkerStdinClose(JobRequest) -> EmptyResult;
    /// Opens a shell session.
    worker json "worker.shell" WorkerShell(SessionRequest) -> EmptyResult;
    /// Lists the workspace incarnation's jobs.
    worker json "worker.listJobs" WorkerListJobs(WorkerScope) -> Vec<JobInfo>;
    /// Resolves one job.
    worker json "worker.job" WorkerJob(JobRequest) -> JobInfo;
    /// Takes a checkpoint.
    worker json "worker.checkpoint" WorkerCheckpoint(CheckpointRequest) -> CheckpointResult;
    /// Pushes the workspace's branch.
    worker json "worker.push" WorkerPush(PushRequest) -> PushReport;
    /// Reads one job's status.
    worker json "job.status" JobStatus(JobRequest) -> JobInfo;
    /// Reads one ended job's sealed record.
    worker json "job.sealed" JobSealed(JobRequest) -> SealedJob;
    /// Reads one bounded raw chunk from an offset. Without follow, an empty chunk is the current
    /// written end even when eof is false; follow waits on supervisor output notifications.
    worker download(offset) "job.logs" JobLogs(LogsRequest) -> LogsChunk;
    /// Reads a bounded slice of both streams after a cursor, or their latest bounded tail.
    worker json "job.tail" JobTailRead(TailRequest) -> JobTail;
    /// Reads the TCP ports the job's process group listens on, from the kernel.
    worker json "job.listeningPorts" JobListeningPortsRead(JobRequest) -> JobListeningPorts;
    /// Writes to an attached job's stdin.
    worker upload "job.attachWrite" JobAttachWrite(JobRequest) -> EmptyResult;
    /// Detaches from a job.
    worker json "job.detach" JobDetach(JobRequest) -> EmptyResult;
    /// Waits for a job to end.
    worker json "job.wait" JobWait(JobRequest) -> JobInfo;
    /// Kills a job's complete process group.
    worker json "job.kill" JobKill(JobRequest) -> EmptyResult;
    /// Closes a shell session.
    worker json "session.close" SessionClose(SessionRequest) -> EmptyResult;
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

/// The workspace a worker capability was minted for. The wire form is the [`WorkspaceView`]; the
/// type is what lets only the call that mints a worker capability yield one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct WorkerView(pub WorkspaceView);

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
pub struct CreateRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub options: CreateOptions,
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
/// stream's chunks follow admission as `worker.stdinChunk` calls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ExecStdin {
    Empty,
    Inline,
    Stream,
    WorkspaceFile {
        #[serde(rename = "workspacePath")]
        workspace_path: WorkspacePath,
    },
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
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobRequest {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
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
