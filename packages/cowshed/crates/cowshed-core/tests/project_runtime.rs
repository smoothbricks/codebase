use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_core::api::dto::{
    AbandonedWork, AdoptOptions, AttachOptions, CheckpointInfo, CheckpointOptions, CheckpointQuota,
    CheckpointResult, CommandArg, CreateOptions, DefragmentResult, DoctorReport, ExecCommand,
    ExecRequest, Finding, FindingSeverity, GcOptions, GcReport, GitOid, GrantDelta, GrantSet,
    JobId, JobInfo, JobJournalCursor, JobState, JobTail, JobTailBytes, JobTailLimits, LandOptions,
    LandReport, MirrorInfo, PortBlock, PushOptions, PushReport, RebaseOptions, RemoveOptions,
    RemoveProjectOptions, RemoveProjectReport, RemoveReport, Reseed, ReseedResult, ResizeResult,
    ResizeVolume, RunSandboxMode, StdinSource, StepReport, WorkspaceInfo, WorkspaceState,
    WorkspaceTarget,
};
use cowshed_core::api::operations::{JobLogs, LogsRequest, Operation, OperationRequest};
use cowshed_core::api::server::{ConnectionAuthority, RouterHandle, serve_controller_connection};
use cowshed_core::metadata::{
    NEW_PORT_BLOCK_SIZE, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use cowshed_core::repository::{BoundIdentity, OwnedRepoIds, RepoId, RepositoryBinding};
use cowshed_core::runtime::job_groups::Birth;
use cowshed_core::runtime::supervisor::{
    ArtifactStoreSink, CommitmentDraft, CommitmentSink, OwnedProcess, ProcessEvent, ProcessSignal,
    ProcessSpawnRequest, RunningProcess, SpawnSink, WorkspaceAuthoritySnapshot,
    WorkspaceSupervisor, WorkspaceSupervisorConfig, WorkspaceSupervisorHandle,
};
use cowshed_core::runtime::{
    JobAnswer, ProjectDescriptor, ProjectRuntime, ProjectRuntimeHost, RuntimeLogChunk,
    WorkspaceSnapshot,
};
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::job_artifact::{ArtifactConfig, StreamKind};
use cowshed_core::storage::lifecycle::{Conflict, LifecycleFact, Revision};
use cowshed_core::timing::{timed, timed_async};
use cowshed_core::{Cowshed, CowshedError, ErrorCode, JobStream, Result, Retry};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};
use url::Url;

#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    SnapshotBatch,
    SecretScan,
    Initialize(WorkspaceName),
    Publish(WorkspaceName),
    Stop(WorkspaceName),
    Retire(WorkspaceName),
    Reclaim(WorkspaceName),
    RestorePending(WorkspaceName),
    RestoreEvidence(WorkspaceName),
    RestoreActivate(WorkspaceName),
    GitSafety(WorkspaceName),
    Bundle(WorkspaceName),
    Detach(WorkspaceName),
    AtomicCheckoutRestore(PathBuf),
    RemoveBinding,
    SettleReclaims,
    Gc,
    Exec {
        workspace: WorkspaceName,
        mount: PathBuf,
        argv: Vec<Vec<u8>>,
        admission_key: Option<cowshed_core::api::AdmissionKey>,
    },
    JobByKey(WorkspaceName, cowshed_core::api::AdmissionKey),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DurableWorkspace {
    name: WorkspaceName,
    incarnation: WorkspaceIncarnation,
    mount: Option<PathBuf>,
    attached: bool,
    lifecycle_revision: u64,
    topology_revision: u64,
    grants: GrantSet,
    checkpoints: Vec<CheckpointInfo>,
    active_bytes: u64,
    checkpoint_bytes: std::collections::BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DurableState {
    workspaces: Vec<DurableWorkspace>,
    pending_restore: Option<DurableWorkspace>,
    pending_has_evidence: bool,
    checkpoint_quotas: std::collections::BTreeMap<String, CheckpointQuota>,
    #[serde(default)]
    project_grants: cowshed_core::api::dto::ProjectGrants,
}

#[derive(Clone, Debug)]
struct FakeRemoval {
    dirty: std::collections::BTreeSet<WorkspaceName>,
    unlanded: std::collections::BTreeSet<WorkspaceName>,
    change_head_at_fence: std::collections::BTreeSet<WorkspaceName>,
    main_in_progress: bool,
    pre_cowshed_present: bool,
    restore_collision: bool,
    restore_swapped: bool,
    fail_after_detach_once: bool,
    fail_after_swap_once: bool,
    /// Clones a create never published: listed by `unpublished_workspaces`, retired by `remove`.
    unpublished: std::collections::BTreeSet<WorkspaceName>,
    /// The next collection finds its plan stale.
    gc_stale_once: bool,
}

impl Default for FakeRemoval {
    fn default() -> Self {
        Self {
            dirty: std::collections::BTreeSet::new(),
            unlanded: std::collections::BTreeSet::new(),
            change_head_at_fence: std::collections::BTreeSet::new(),
            main_in_progress: false,
            pre_cowshed_present: true,
            restore_collision: false,
            restore_swapped: false,
            fail_after_detach_once: false,
            fail_after_swap_once: false,
            unpublished: std::collections::BTreeSet::new(),
            gc_stale_once: false,
        }
    }
}

struct RecoveryRace {
    fact: AtomicUsize,
    attempts: AtomicUsize,
    first_read: Notify,
    mutated: Notify,
}

enum RecoveryBehavior {
    None,
    Contended(Arc<RecoveryRace>),
    Mutate(Arc<RecoveryRace>),
    AlwaysLifecycleConflict(Arc<AtomicUsize>),
    ImmediateFailure(Arc<AtomicUsize>),
}

/// One running job whose end the test decides. Its stdout holds `first\n` from the start;
/// its end, both streams' EOF and its terminal status all wait for [`HeldJob::end`].
struct HeldJob {
    incarnation: WorkspaceIncarnation,
    ended: tokio::sync::watch::Sender<bool>,
}

impl HeldJob {
    const FIRST: &'static [u8] = b"first\n";

    fn new(incarnation: WorkspaceIncarnation) -> Arc<Self> {
        Arc::new(Self {
            incarnation,
            ended: tokio::sync::watch::channel(false).0,
        })
    }

    fn end(&self) {
        self.ended.send_replace(true);
    }

    async fn until_ended(&self) {
        let mut ended = self.ended.subscribe();
        ended
            .wait_for(|ended| *ended)
            .await
            .expect("the job's sender outlives its waits");
    }

    fn info(&self) -> JobInfo {
        let ended = *self.ended.borrow();
        let mut value = json!({
            "repoId": "acme/widget",
            "workspaceIncarnation": self.incarnation,
            "jobId": 1,
            "state": if ended { "exited" } else { "running" },
            "grantRevision": 1,
            "argv": [{"encoding": "utf8", "data": "build"}],
            "cwd": null,
            "started": "2026-07-13T00:00:00Z",
            "stdout": {
                "storage": {"kind": "captured", "artifact": {"kind": "inline", "data": {"encoding": "utf8", "data": "first\n"}}},
                "bytes": 6,
                "sha256": "b640e840b19d378660b32fb51ae18d67dccb4a8596a29e7bd72c1b2ae5928f41",
                "summary": {"version": 1, "text": "", "truncated": false}
            },
            "stderr": {
                "storage": {"kind": "captured", "artifact": {"kind": "inline", "data": {"encoding": "utf8", "data": ""}}},
                "bytes": 0,
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "summary": {"version": 1, "text": "", "truncated": false}
            },
            "trace": {"traceId": "4bf92f3577b34da6a3ce929d0e0e4736", "spanId": "00f067aa0ba902b7"},
            "stdin": {"kind": "empty", "bytes": 0, "complete": true}
        });
        if ended {
            value["durationMs"] = json!(1);
            value["exit"] = json!({"kind": "exited", "code": 0});
        }
        serde_json::from_value(value).expect("a job record the wire accepts")
    }

    /// What a read at `offset` of `stream` returns now, or `None` when a follow read waits.
    fn chunk(&self, stream: JobStream, offset: u64) -> Option<RuntimeLogChunk> {
        let bytes: &[u8] = match stream {
            JobStream::Stdout => Self::FIRST,
            JobStream::Stderr => b"",
        };
        let start = usize::try_from(offset).expect("a test offset");
        let rest = &bytes[start.min(bytes.len())..];
        let ended = *self.ended.borrow();
        (!rest.is_empty() || ended).then(|| RuntimeLogChunk {
            bytes: Bytes::copy_from_slice(rest),
            next_offset: offset + rest.len() as u64,
            eof: ended,
        })
    }
}

struct FakeHost {
    descriptor: ProjectDescriptor,
    state_path: PathBuf,
    state: DurableState,
    events: mpsc::UnboundedSender<Event>,
    fail_create_initializer: bool,
    fail_restore_fence_once: bool,
    doctor_findings: Vec<Finding>,
    reclaim_gate: Option<Arc<Notify>>,
    removal: FakeRemoval,
    recovery_behavior: RecoveryBehavior,
    held_job: Option<Arc<HeldJob>>,
    /// The supervisor serving each workspace whose jobs are real; a workspace without one runs
    /// none.
    supervisors: std::collections::BTreeMap<WorkspaceName, WorkspaceSupervisorHandle>,
    /// When set, a create holds inside its clone step until this is notified.
    create_gate: Option<Arc<Notify>>,
    /// Background reclaims `remove` started, for `settle_reclaims`.
    reclaims: Vec<tokio::task::JoinHandle<()>>,
    /// Abandon bundles removals left in the trash, for `delete_abandon_bundles`.
    bundles: Vec<PathBuf>,
}

impl FakeHost {
    fn new(
        root: &Path,
        events: mpsc::UnboundedSender<Event>,
        fail_create_initializer: bool,
        fail_restore_fence_once: bool,
        doctor_findings: Vec<Finding>,
    ) -> Self {
        std::fs::create_dir_all(root.join("checkout")).expect("create fake checkout");

        let repo_id = RepoId::parse("acme/widget").expect("fixed repo id");
        let binding = RepositoryBinding::new(vec![BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .expect("fixed binding");
        Self {
            descriptor: ProjectDescriptor {
                repo_id,
                binding: std::sync::Arc::new(binding),
                git_root: std::sync::Arc::from(root.join("checkout")),
                storage: cowshed_core::storage::bootstrap::ValidatedHostStorage::new(
                    root.join("home"),
                    cowshed_core::storage::bootstrap::CanonicalRoots::at(root.join("store")),
                ),
            },
            state_path: root.join("durable.json"),
            state: DurableState::default(),
            events,
            fail_create_initializer,
            fail_restore_fence_once,
            doctor_findings,
            reclaim_gate: None,
            removal: FakeRemoval::default(),
            recovery_behavior: RecoveryBehavior::None,
            held_job: None,
            supervisors: std::collections::BTreeMap::new(),
            create_gate: None,
            reclaims: Vec::new(),
            bundles: Vec::new(),
        }
    }

    fn load(&mut self) -> Result<()> {
        match std::fs::read(&self.state_path) {
            Ok(bytes) => {
                self.state = serde_json::from_slice(&bytes).map_err(|error| {
                    CowshedError::integrity(
                        format!("fake durable state is malformed: {error}"),
                        "remove the test fixture",
                    )
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(CowshedError::internal(error.to_string())),
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        if let Some(parent) = self.state_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| CowshedError::internal(error.to_string()))?;
        }
        let bytes = serde_json::to_vec(&self.state)
            .map_err(|error| CowshedError::internal(error.to_string()))?;
        std::fs::write(&self.state_path, bytes)
            .map_err(|error| CowshedError::internal(error.to_string()))
    }

    fn workspace(&self, name: &WorkspaceName) -> Result<&DurableWorkspace> {
        self.state
            .workspaces
            .iter()
            .find(|workspace| &workspace.name == name)
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("workspace {name} does not exist"),
                    "list workspaces",
                )
            })
    }

    fn workspace_mut(&mut self, name: &WorkspaceName) -> Result<&mut DurableWorkspace> {
        self.state
            .workspaces
            .iter_mut()
            .find(|workspace| &workspace.name == name)
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("workspace {name} does not exist"),
                    "list workspaces",
                )
            })
    }

    fn snapshot(&self, workspace: &DurableWorkspace) -> WorkspaceSnapshot {
        WorkspaceSnapshot {
            info: WorkspaceInfo {
                repo_id: self.descriptor.repo_id.clone(),
                workspace: workspace.name.clone(),
                workspace_incarnation: workspace.incarnation.clone(),
                role: if workspace.name.is_main() {
                    WorkspaceRole::Main
                } else {
                    WorkspaceRole::Workspace
                },
                mount: workspace.mount.clone().unwrap_or_else(|| {
                    if workspace.name.is_main() {
                        self.descriptor.git_root.to_path_buf()
                    } else {
                        self.descriptor
                            .storage
                            .store()
                            .join("mnt")
                            .join(workspace.name.as_str())
                    }
                }),
                state: if workspace.attached {
                    WorkspaceState::Attached
                } else {
                    WorkspaceState::Detached
                },
                branch: None,
                base_commit: None,
                created_at: None,
                checkpoints: workspace.checkpoints.clone(),
                snapshot_stale: false,
                landing: None,
            },
            grants: workspace.grants.clone(),
            lifecycle_revision: workspace.lifecycle_revision,
            topology_revision: workspace.topology_revision,
        }
    }

    fn next_workspace(&self, name: WorkspaceName) -> DurableWorkspace {
        let next = self
            .state
            .workspaces
            .iter()
            .map(|workspace| workspace.topology_revision)
            .max()
            .unwrap_or(0)
            + 1;
        DurableWorkspace {
            name,
            incarnation: incarnation(next),
            lifecycle_revision: 1,
            mount: None,
            attached: true,
            topology_revision: next,
            grants: GrantSet::closed_baseline(Some(
                PortBlock::new(
                    40_960 + u16::try_from((next - 1) * 16).expect("test port"),
                    16,
                )
                .expect("test block"),
            ))
            .expect("test grants"),
            checkpoints: Vec::new(),
            active_bytes: 10,
            checkpoint_bytes: std::collections::BTreeMap::new(),
        }
    }

    fn require_incarnation(
        &self,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> Result<()> {
        let current = self.workspace(workspace)?;
        if &current.incarnation != incarnation {
            return Err(CowshedError::conflict(
                "stale workspace incarnation",
                "reacquire a worker handle",
            ));
        }
        Ok(())
    }

    fn worker_unavailable() -> CowshedError {
        CowshedError::environment_missing(
            "fake host does not run child processes",
            "exercise lifecycle operations in this fixture",
        )
    }
}

fn stale_lifecycle_conflict() -> CowshedError {
    let repo = RepoId::parse("acme/widget").expect("fixed repo id");
    let name = WorkspaceName::new("main").expect("fixed workspace");
    CowshedError::lifecycle_conflict(Conflict::Stale {
        index: 1,
        expected: Box::new(LifecycleFact::Absent {
            repo: repo.clone(),
            name: name.clone(),
            topology_revision: Revision::new(1),
        }),
        actual: Box::new(LifecycleFact::Absent {
            repo,
            name,
            topology_revision: Revision::new(2),
        }),
    })
}

#[async_trait]
impl ProjectRuntimeHost for FakeHost {
    fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    async fn recover(&mut self) -> Result<()> {
        match &self.recovery_behavior {
            RecoveryBehavior::None => {}
            RecoveryBehavior::Contended(race) => {
                race.attempts.fetch_add(1, Ordering::SeqCst);
                let expected = race.fact.load(Ordering::SeqCst);
                if expected == 0 {
                    race.first_read.notify_one();
                    race.mutated.notified().await;
                }
                let actual = race.fact.load(Ordering::SeqCst);
                if actual != expected {
                    return Err(CowshedError::lifecycle_conflict(Conflict::FactCount {
                        expected: expected + 1,
                        actual: actual + 1,
                    }));
                }
            }
            RecoveryBehavior::Mutate(race) => {
                race.fact.fetch_add(1, Ordering::SeqCst);
                race.mutated.notify_one();
            }
            RecoveryBehavior::AlwaysLifecycleConflict(attempts) => {
                attempts.fetch_add(1, Ordering::SeqCst);
                return Err(stale_lifecycle_conflict());
            }
            RecoveryBehavior::ImmediateFailure(attempts) => {
                attempts.fetch_add(1, Ordering::SeqCst);
                return Err(CowshedError::environment_missing(
                    "fixture storage is unreadable",
                    "repair fixture permissions",
                ));
            }
        }
        self.load()?;
        if self.state.pending_has_evidence
            && let Some(pending) = self.state.pending_restore.take()
        {
            let name = pending.name.clone();
            if let Some(current) = self
                .state
                .workspaces
                .iter_mut()
                .find(|workspace| workspace.name == name)
            {
                *current = pending;
            }
            self.events.send(Event::RestoreActivate(name)).ok();
            self.state.pending_has_evidence = false;
            self.persist()?;
        }
        Ok(())
    }

    async fn snapshots(&mut self) -> Result<Vec<WorkspaceSnapshot>> {
        self.events.send(Event::SnapshotBatch).ok();
        Ok(self
            .state
            .workspaces
            .iter()
            .map(|workspace| self.snapshot(workspace))
            .collect())
    }

    async fn build_volume(&mut self, workspace: WorkspaceName) -> Result<Option<PathBuf>> {
        Ok(Some(
            self.descriptor
                .storage
                .store()
                .join(".build")
                .join(workspace.as_str()),
        ))
    }

    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceSnapshot> {
        let matches = self
            .state
            .workspaces
            .iter()
            .filter(|workspace| workspace.attached)
            .filter(|workspace| path.starts_with(&self.snapshot(workspace).info.mount))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [workspace] => Ok(self.snapshot(workspace)),
            [] => Err(CowshedError::not_found(
                "path is not inside an active workspace mount",
                "retry from an attached workspace",
            )),
            _ => Err(CowshedError::conflict(
                "path is inside multiple active workspace mounts",
                "repair overlapping mounts",
            )),
        }
    }

    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceSnapshot> {
        let name = WorkspaceName::new("main").expect("main");
        if self
            .state
            .workspaces
            .iter()
            .any(|workspace| workspace.name == name)
        {
            return Err(CowshedError::conflict(
                "main is already adopted",
                "use cowshed list",
            ));
        }
        self.events.send(Event::SecretScan).ok();
        let scan = cowshed_core::secrets::scan_tree(&self.descriptor.git_root, &[])
            .map_err(|error| CowshedError::internal(error.to_string()))?;
        if !scan.findings.is_empty() && !options.quarantine {
            return Err(CowshedError::conflict(
                "repository contains secrets",
                "retry with quarantine",
            ));
        }
        if options.quarantine {
            for path in scan
                .findings
                .iter()
                .map(|finding| &finding.path)
                .collect::<std::collections::BTreeSet<_>>()
            {
                let source = self.descriptor.git_root.join(path);
                let destination = self
                    .descriptor
                    .storage
                    .store()
                    .join("quarantine")
                    .join(path);
                std::fs::create_dir_all(destination.parent().expect("quarantine parent"))
                    .map_err(|error| CowshedError::internal(error.to_string()))?;
                std::fs::rename(&source, &destination)
                    .map_err(|error| CowshedError::internal(error.to_string()))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o600))
                        .map_err(|error| CowshedError::internal(error.to_string()))?;
                }
            }
        }
        let workspace = self.next_workspace(name.clone());
        self.events.send(Event::Initialize(name.clone())).ok();
        self.state.workspaces.push(workspace.clone());
        self.persist()?;
        self.events.send(Event::Publish(name)).ok();
        Ok(self.snapshot(&workspace))
    }

    async fn create(
        &mut self,
        workspace: WorkspaceName,
        _options: CreateOptions,
    ) -> Result<WorkspaceSnapshot> {
        if self
            .state
            .workspaces
            .iter()
            .any(|current| current.name == workspace)
        {
            return Err(CowshedError::conflict(
                format!("workspace {workspace} already exists"),
                "choose another name",
            ));
        }
        // The real host's steps, in the shape it reports them: a clone step holding the first
        // write into the clone, then the initializer.
        let gate = self.create_gate.clone();
        timed_async("new", "clone", async {
            timed("apfs", format_args!("canonical/first-write"), || {
                Ok::<_, CowshedError>(())
            })?;
            if let Some(gate) = gate {
                gate.notified().await;
            }
            Ok::<_, CowshedError>(())
        })
        .await?;
        let prepared = self.next_workspace(workspace.clone());
        self.events.send(Event::Initialize(workspace.clone())).ok();
        let fail = std::mem::take(&mut self.fail_create_initializer);
        timed_async("new", "initialize", async {
            if fail {
                Err(CowshedError::internal(
                    "injected create initializer failure",
                ))
            } else {
                Ok(())
            }
        })
        .await?;
        self.state.workspaces.push(prepared.clone());
        self.persist()?;
        self.events.send(Event::Publish(workspace)).ok();
        Ok(self.snapshot(&prepared))
    }

    async fn fork(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        self.workspace(&source)?;
        self.create(destination, CreateOptions::default()).await
    }

    async fn rename(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        self.workspace(&source)?;
        self.create(destination, CreateOptions::default()).await
    }

    async fn move_checkout(&mut self, destination: PathBuf) -> Result<WorkspaceSnapshot> {
        // The checkout path is the descriptor's project root, and main's mount is derived from it,
        // so the returned mount is what witnesses that the move reached the host at all.
        if *destination == *self.descriptor.git_root {
            return Err(CowshedError::usage(
                "the checkout is already there",
                "choose a different destination path",
            ));
        }
        self.descriptor.git_root = std::sync::Arc::from(destination);
        let main = WorkspaceName::new("main").expect("main");
        let current = self.workspace(&main)?;
        Ok(self.snapshot(current))
    }

    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()> {
        // Convergence, in miniature: an observed main checkout that differs from the recorded one
        // replaces it. Sessions never converge — only main's checkout has a recorded path.
        if let Some(observed) = options.observed_path
            && workspace.is_main()
            && *observed != *self.descriptor.git_root
        {
            self.descriptor.git_root = std::sync::Arc::from(observed);
        }
        self.workspace_mut(&workspace)?.attached = true;
        self.persist()
    }

    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()> {
        self.workspace_mut(&workspace)?.attached = false;
        self.persist()
    }

    async fn resize(
        &mut self,
        workspace: WorkspaceName,
        capacity: String,
        volume: ResizeVolume,
    ) -> Result<ResizeResult> {
        self.workspace(&workspace)?;
        Ok(ResizeResult {
            workspace,
            volume,
            previous_capacity: "100g".to_owned(),
            capacity,
        })
    }

    async fn defragment(&mut self, workspace: WorkspaceName) -> Result<DefragmentResult> {
        self.workspace(&workspace)?;
        Ok(DefragmentResult {
            workspace,
            previous_extents: 2,
            extents: 1,
            bytes: 0,
        })
    }

    async fn reseed(&mut self, workspace: WorkspaceName) -> Result<ReseedResult> {
        self.workspace(&workspace)?;
        Ok(ReseedResult {
            workspace,
            outcome: Reseed::NoBuildVolume,
        })
    }

    async fn checkpoint(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: Option<WorkspaceIncarnation>,
        options: CheckpointOptions,
    ) -> Result<CheckpointResult> {
        let current = self.workspace(&workspace)?;
        if expected_incarnation
            .as_ref()
            .is_some_and(|expected| expected != &current.incarnation)
        {
            return Err(CowshedError::conflict(
                "stale workspace incarnation",
                "refresh the worker",
            ));
        }
        let explicitly_labeled = options.label.is_some();
        let label = options
            .label
            .unwrap_or_else(|| format!("checkpoint-{}", current.lifecycle_revision + 1));
        if current
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.label == label)
        {
            return Err(CowshedError::conflict(
                "checkpoint label already exists",
                "choose another label",
            ));
        }
        if let Some(quota) = self.state.checkpoint_quotas.get(workspace.as_str()) {
            let existing_bytes = current
                .checkpoint_bytes
                .values()
                .try_fold(0_u64, |sum, bytes| sum.checked_add(*bytes))
                .ok_or_else(|| {
                    CowshedError::integrity("checkpoint byte overflow", "test fixture")
                })?;
            let projected_bytes = existing_bytes
                .checked_add(current.active_bytes)
                .ok_or_else(|| {
                    CowshedError::integrity("checkpoint byte overflow", "test fixture")
                })?;
            let projected_count = u64::try_from(current.checkpoints.len()).map_err(|_| {
                CowshedError::integrity("checkpoint count overflow", "test fixture")
            })? + 1;
            if projected_count > u64::from(quota.max_count) || projected_bytes > quota.max_bytes {
                return Err(CowshedError::conflict(
                    "checkpoint quota exceeded",
                    "raise quota or remove checkpoints",
                ));
            }
        }
        let current = self.workspace_mut(&workspace)?;
        current.lifecycle_revision += 1;
        current.checkpoints.push(CheckpointInfo {
            label: label.clone(),
            revision: current.lifecycle_revision,
            pinned: options.keep || explicitly_labeled,
        });
        current
            .checkpoint_bytes
            .insert(label.clone(), current.active_bytes);
        self.persist()?;
        Ok(CheckpointResult { label })
    }

    async fn restore(&mut self, workspace: WorkspaceName, _label: String) -> Result<()> {
        if let Some(pending) = self.state.pending_restore.clone() {
            if pending.name != workspace {
                return Err(CowshedError::conflict(
                    "another restore is pending",
                    "recover it first",
                ));
            }
            self.events
                .send(Event::RestoreEvidence(workspace.clone()))
                .ok();
            self.state.pending_has_evidence = true;
            self.persist()?;
            let pending = self.state.pending_restore.take().expect("checked pending");
            *self.workspace_mut(&workspace)? = pending;
            self.state.pending_has_evidence = false;
            self.events
                .send(Event::RestoreActivate(workspace.clone()))
                .ok();
            self.persist()?;
            return Ok(());
        }
        let current = self.workspace(&workspace)?.clone();
        let mut replacement = current.clone();
        replacement.incarnation = incarnation(current.topology_revision + 100);
        replacement.lifecycle_revision += 1;
        self.state.pending_restore = Some(replacement);
        self.events
            .send(Event::RestorePending(workspace.clone()))
            .ok();
        self.persist()?;
        if std::mem::take(&mut self.fail_restore_fence_once) {
            return Err(CowshedError::environment_missing(
                "injected restore fence failure",
                "retry restore",
            ));
        }
        self.events
            .send(Event::RestoreEvidence(workspace.clone()))
            .ok();
        self.state.pending_has_evidence = true;
        self.persist()?;
        let pending = self.state.pending_restore.take().expect("just staged");
        *self.workspace_mut(&workspace)? = pending;
        self.state.pending_has_evidence = false;
        self.events.send(Event::RestoreActivate(workspace)).ok();
        self.persist()
    }

    async fn remove(
        &mut self,
        workspace: WorkspaceName,
        options: RemoveOptions,
    ) -> Result<RemoveReport> {
        if options.force && options.restore {
            return Err(CowshedError::usage(
                "force and restore are ambiguous",
                "choose one removal mode",
            ));
        }
        if options.restore && !workspace.is_main() {
            return Err(CowshedError::usage(
                "restore requires main",
                "remove the session without restore",
            ));
        }
        if self.removal.unpublished.remove(&workspace) {
            self.events.send(Event::Retire(workspace)).ok();
            return Ok(RemoveReport::default());
        }
        let index = self
            .state
            .workspaces
            .iter()
            .position(|current| current.name == workspace)
            .ok_or_else(|| CowshedError::not_found("workspace missing", "list workspaces"))?;
        self.events.send(Event::GitSafety(workspace.clone())).ok();
        let mut abandoned = None;

        if options.restore {
            if !self.removal.pre_cowshed_present && !self.removal.restore_swapped {
                return Err(CowshedError::conflict(
                    "retained pre-cowshed checkout is missing",
                    "restore the retained tree",
                ));
            }
            if self.removal.restore_collision {
                return Err(CowshedError::conflict(
                    "canonical project path contains unrelated data",
                    "move the collision aside",
                ));
            }
            self.events.send(Event::Stop(workspace.clone())).ok();
            self.events.send(Event::Detach(workspace.clone())).ok();
            if std::mem::take(&mut self.removal.fail_after_detach_once) {
                return Err(CowshedError::environment_missing(
                    "injected crash after detach",
                    "retry restore",
                ));
            }
            if !self.removal.restore_swapped {
                self.events
                    .send(Event::AtomicCheckoutRestore(
                        self.descriptor.git_root.to_path_buf(),
                    ))
                    .ok();
                self.removal.pre_cowshed_present = false;
                self.removal.restore_swapped = true;
                if std::mem::take(&mut self.removal.fail_after_swap_once) {
                    return Err(CowshedError::environment_missing(
                        "injected crash after checkout swap",
                        "retry restore",
                    ));
                }
            }
        } else {
            if workspace.is_main() && options.abandon {
                return Err(CowshedError::usage(
                    "abandon applies to session workspaces",
                    "remove a session with abandon",
                ));
            }
            if workspace.is_main() && !options.force {
                return Err(CowshedError::conflict(
                    "main removal without restore destroys the warm main image",
                    "recover the pre-adoption checkout instead",
                ));
            }
            if workspace.is_main()
                && (self.removal.main_in_progress || self.removal.dirty.contains(&workspace))
            {
                return Err(CowshedError::conflict(
                    "main is not clean",
                    "finish or discard Git work",
                ));
            }
            // Transient state is force's business; unlanded commits are abandon's, and neither
            // flag stands in for the other.
            if !workspace.is_main() && !options.force && self.removal.dirty.contains(&workspace) {
                return Err(CowshedError::conflict(
                    "workspace has uncommitted Git work",
                    "commit the work and land it",
                ));
            }
            if !workspace.is_main()
                && !options.abandon
                && self.removal.unlanded.contains(&workspace)
            {
                return Err(CowshedError::conflict(
                    "workspace head is not contained by main",
                    "land the workspace",
                ));
            }
            self.events.send(Event::Stop(workspace.clone())).ok();
            if self.removal.change_head_at_fence.contains(&workspace) {
                return Err(CowshedError::conflict(
                    "workspace HEAD changed during removal",
                    "review and retry",
                ));
            }
            if self.removal.unlanded.contains(&workspace) {
                abandoned = Some(AbandonedWork {
                    head: GitOid::new("4".repeat(40)).expect("fixed head"),
                    target_branch: "main".to_owned(),
                    target_head: Some(GitOid::new("1".repeat(40)).expect("fixed tip")),
                    unlanded_commits: 3,
                    bundle: self.descriptor.storage.store().join(format!(
                        "sessions/.trash/{workspace}-{}.bundle",
                        "4".repeat(40)
                    )),
                });
                self.bundles.push(
                    abandoned
                        .as_ref()
                        .map(|work| work.bundle.clone())
                        .expect("just abandoned"),
                );
                self.events.send(Event::Bundle(workspace.clone())).ok();
            }
        }

        self.state.workspaces.remove(index);
        self.persist()?;
        self.events.send(Event::Retire(workspace.clone())).ok();
        if options.restore {
            self.events.send(Event::RemoveBinding).ok();
        }
        let events = self.events.clone();
        let reclaim_gate = self.reclaim_gate.clone();
        self.reclaims.push(tokio::spawn(async move {
            if let Some(gate) = reclaim_gate {
                gate.notified().await;
            }
            events.send(Event::Reclaim(workspace)).ok();
        }));
        Ok(RemoveReport { abandoned })
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        self.events.send(Event::Gc).ok();
        if std::mem::take(&mut self.removal.gc_stale_once) {
            return Err(CowshedError::retryable(
                Retry::GcPlanStale,
                "garbage-collection plan became stale",
                "retry",
            ));
        }
        Ok(GcReport {
            examined: u64::try_from(self.state.workspaces.len()).expect("test length"),
            reclaimed: 0,
            retained_pinned: 0,
            retained_active: 0,
            freed_bytes: 0,
            dry_run: options.dry_run,
            candidates: Vec::new(),
            deferred: Vec::new(),
        })
    }

    async fn unpublished_workspaces(&mut self) -> Result<Vec<WorkspaceName>> {
        Ok(self.removal.unpublished.iter().cloned().collect())
    }

    async fn settle_reclaims(&mut self) -> Result<()> {
        self.events.send(Event::SettleReclaims).ok();
        for reclaim in std::mem::take(&mut self.reclaims) {
            reclaim
                .await
                .map_err(|error| CowshedError::internal(error.to_string()))?;
        }
        Ok(())
    }

    async fn delete_abandon_bundles(&mut self) -> Result<Vec<PathBuf>> {
        Ok(std::mem::take(&mut self.bundles))
    }

    async fn grant(
        &mut self,
        workspace: WorkspaceName,
        delta: GrantDelta,
        _revoke: bool,
    ) -> Result<GrantSet> {
        let current = self.workspace_mut(&workspace)?;
        if delta
            .expected_revision
            .is_some_and(|expected| expected != current.grants.revision)
        {
            return Err(CowshedError::conflict(
                "stale grant revision",
                "refresh grants and retry",
            ));
        }
        current.grants.revision += 1;
        let result = current.grants.clone();
        self.persist()?;
        Ok(result)
    }

    async fn project_grants(&mut self) -> Result<cowshed_core::api::dto::ProjectGrants> {
        Ok(self.state.project_grants.clone())
    }

    async fn grant_project(
        &mut self,
        delta: cowshed_core::api::dto::ProjectGrantDelta,
        revoke: bool,
    ) -> Result<cowshed_core::api::dto::ProjectGrants> {
        let grants = &mut self.state.project_grants;
        if revoke {
            grants.read.retain(|path| !delta.read.contains(path));
            grants.egress.retain(|rule| !delta.egress.contains(rule));
        } else {
            grants.read.extend(delta.read);
            grants.egress.extend(delta.egress);
        }
        grants.revision += 1;
        let result = grants.clone();
        self.persist()?;
        Ok(result)
    }

    async fn assign_slot(&mut self, workspace: WorkspaceName, slot: u32) -> Result<()> {
        let current = self.workspace_mut(&workspace)?;
        let base = u16::try_from(
            slot.checked_mul(u32::from(NEW_PORT_BLOCK_SIZE))
                .ok_or_else(|| {
                    CowshedError::usage("slot overflows port space", "choose a smaller slot")
                })?,
        )
        .map_err(|_| CowshedError::usage("slot overflows port space", "choose a smaller slot"))?;
        current.grants.port_block = Some(
            PortBlock::new(base, NEW_PORT_BLOCK_SIZE)
                .map_err(|error| CowshedError::usage(error.to_string(), "choose another slot"))?,
        );
        current.grants.revision += 1;
        self.persist()
    }

    async fn set_checkpoint_quota(
        &mut self,
        workspace: WorkspaceName,
        quota: CheckpointQuota,
    ) -> Result<()> {
        self.workspace(&workspace)?;
        self.state
            .checkpoint_quotas
            .insert(workspace.to_string(), quota);
        self.persist()
    }

    async fn rebase(
        &mut self,
        workspace: WorkspaceName,
        _into: Option<WorkspaceTarget>,
        options: RebaseOptions,
    ) -> Result<cowshed_core::api::dto::RebaseReport> {
        let current = self.workspace(&workspace)?;
        if options
            .expected_workspace_incarnation
            .as_ref()
            .is_some_and(|expected| expected != &current.incarnation)
        {
            return Err(CowshedError::conflict(
                "stale workspace incarnation",
                "refresh and retry",
            ));
        }
        Ok(cowshed_core::api::dto::RebaseReport {
            oid: GitOid::new("1111111111111111111111111111111111111111")
                .map_err(|error| CowshedError::internal(error.to_string()))?,
            build_volume: cowshed_core::api::dto::RebaseBuildVolume::Skipped {
                reason: cowshed_core::api::dto::RebaseCarrySkip::NoWorkspaceVolume,
            },
        })
    }

    async fn land(
        &mut self,
        workspace: WorkspaceName,
        _into: Option<WorkspaceTarget>,
        _options: LandOptions,
    ) -> Result<LandReport> {
        self.workspace(&workspace)?;
        Ok(LandReport {
            landed_head: GitOid::new("1111111111111111111111111111111111111111")
                .expect("fixed oid"),
            target_branch: "main".into(),
            previous_target_head: None,
            target_was_checked_out: true,
            retired: false,
            build_volume: cowshed_core::api::dto::LandBuildVolume {
                seeded: false,
                adoption: cowshed_core::api::dto::Adoption::Skipped {
                    reason: cowshed_core::api::dto::AdoptionSkip::NoLandingVolume,
                },
            },
        })
    }

    async fn push(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: WorkspaceIncarnation,
        _options: PushOptions,
    ) -> Result<PushReport> {
        self.require_incarnation(&workspace, &expected_incarnation)?;
        Ok(PushReport {
            source_head: GitOid::new("1111111111111111111111111111111111111111")
                .expect("fixed oid"),
            destination_ref: "refs/heads/main".into(),
            previous_destination_head: None,
        })
    }

    async fn repo_mirror(&mut self, workspace: WorkspaceName, url: Url) -> Result<MirrorInfo> {
        self.workspace(&workspace)?;
        Ok(MirrorInfo {
            url: url.to_string(),
            mirror: self.descriptor.storage.store().join("mirror.git"),
        })
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        Ok(DoctorReport::from_findings(self.doctor_findings.clone()))
    }

    async fn open_worker(&mut self, workspace: WorkspaceName) -> Result<WorkspaceSnapshot> {
        self.workspace(&workspace)
            .map(|workspace| self.snapshot(workspace))
    }

    async fn open_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _name: Option<String>,
    ) -> Result<()> {
        self.require_incarnation(&workspace, &incarnation)
    }

    async fn close_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _name: Option<String>,
    ) -> Result<()> {
        self.require_incarnation(&workspace, &incarnation)
    }

    async fn exec(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _session: Option<String>,
        request: cowshed_core::api::dto::ExecRequest,
    ) -> Result<JobId> {
        self.require_incarnation(&workspace, &incarnation)?;
        if let Some(supervisor) = self.supervisors.get(&workspace) {
            return supervisor.exec(None, None, request).await;
        }
        let mount = self.snapshot(self.workspace(&workspace)?).info.mount;
        self.events
            .send(Event::Exec {
                workspace,
                mount,
                argv: request
                    .command
                    .argv()
                    .expect("the runtime tests exec argv jobs")
                    .iter()
                    .map(|argument| argument.as_os_str().as_bytes().to_vec())
                    .collect(),
                admission_key: request.admission_key,
            })
            .ok();
        JobId::new(1).map_err(|error| CowshedError::internal(error.to_string()))
    }

    async fn stdin_write(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _job: JobId,
        _bytes: Bytes,
    ) -> Result<()> {
        self.require_incarnation(&workspace, &incarnation)?;
        Err(Self::worker_unavailable())
    }

    async fn stdin_close(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _job: JobId,
    ) -> Result<()> {
        self.require_incarnation(&workspace, &incarnation)?;
        Err(Self::worker_unavailable())
    }

    async fn list_jobs(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    ) -> Result<Vec<JobInfo>> {
        self.require_incarnation(&workspace, &incarnation)?;
        Ok(Vec::new())
    }

    async fn job_info(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo> {
        self.require_incarnation(&workspace, &incarnation)?;
        if let Some(supervisor) = self.supervisors.get(&workspace) {
            return supervisor.info(job).await;
        }
        match &self.held_job {
            Some(held) => Ok(held.info()),
            None => Err(Self::worker_unavailable()),
        }
    }

    async fn sealed_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<cowshed_core::api::SealedJob> {
        self.require_incarnation(&workspace, &incarnation)?;
        match self.supervisors.get(&workspace) {
            Some(supervisor) => supervisor.sealed(job).await,
            None => Err(Self::worker_unavailable()),
        }
    }

    async fn job_by_key(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        key: cowshed_core::api::AdmissionKey,
    ) -> Result<JobId> {
        self.require_incarnation(&workspace, &incarnation)?;
        self.events.send(Event::JobByKey(workspace, key)).ok();
        JobId::new(1).map_err(|error| CowshedError::internal(error.to_string()))
    }

    async fn wait_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobAnswer<JobInfo>> {
        self.require_incarnation(&workspace, &incarnation)?;
        if let Some(supervisor) = self.supervisors.get(&workspace).cloned() {
            return Ok(Box::pin(async move { supervisor.wait(job).await }));
        }
        let Some(held) = self.held_job.clone() else {
            return Err(Self::worker_unavailable());
        };
        Ok(Box::pin(async move {
            held.until_ended().await;
            Ok(held.info())
        }))
    }

    async fn kill_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _job: JobId,
    ) -> Result<JobAnswer<()>> {
        self.require_incarnation(&workspace, &incarnation)?;
        Err(Self::worker_unavailable())
    }

    async fn detach_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _job: JobId,
    ) -> Result<()> {
        self.require_incarnation(&workspace, &incarnation)
    }

    async fn read_log(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        stream: JobStream,
        offset: u64,
        follow: bool,
    ) -> Result<JobAnswer<RuntimeLogChunk>> {
        self.require_incarnation(&workspace, &incarnation)?;
        if let Some(supervisor) = self.supervisors.get(&workspace).cloned() {
            return Ok(Box::pin(async move {
                let chunk = supervisor
                    .log_read(job, StreamKind::from(stream), offset, follow)
                    .await?;
                Ok(RuntimeLogChunk {
                    bytes: chunk.bytes,
                    next_offset: chunk.next_offset,
                    eof: chunk.eof,
                })
            }));
        }
        let held = self.held_job.clone();
        Ok(Box::pin(async move {
            let Some(held) = held else {
                return Ok(RuntimeLogChunk {
                    bytes: Bytes::new(),
                    next_offset: offset,
                    eof: true,
                });
            };
            if let Some(chunk) = held.chunk(stream, offset) {
                return Ok(chunk);
            }
            if !follow {
                return Ok(RuntimeLogChunk {
                    bytes: Bytes::new(),
                    next_offset: offset,
                    eof: false,
                });
            }
            held.until_ended().await;
            Ok(held
                .chunk(stream, offset)
                .expect("an ended job's streams are at EOF"))
        }))
    }

    async fn read_tail(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        cursor: Option<JobJournalCursor>,
        limits: JobTailLimits,
    ) -> Result<JobAnswer<JobTail>> {
        self.require_incarnation(&workspace, &incarnation)?;
        let Some(supervisor) = self.supervisors.get(&workspace).cloned() else {
            return Err(Self::worker_unavailable());
        };
        Ok(Box::pin(async move {
            supervisor.tail(job, cursor, limits).await
        }))
    }

    async fn read_listening_ports(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobAnswer<cowshed_core::api::dto::JobListeningPorts>> {
        self.require_incarnation(&workspace, &incarnation)?;
        let Some(supervisor) = self.supervisors.get(&workspace).cloned() else {
            return Err(Self::worker_unavailable());
        };
        Ok(Box::pin(
            async move { supervisor.listening_ports(job).await },
        ))
    }

    async fn progress_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        every: cowshed_core::api::SampleInterval,
    ) -> Result<JobAnswer<cowshed_core::runtime::job_progress::JobProgressStream>> {
        self.require_incarnation(&workspace, &incarnation)?;
        let Some(supervisor) = self.supervisors.get(&workspace).cloned() else {
            return Err(Self::worker_unavailable());
        };
        Ok(Box::pin(
            async move { supervisor.progress(job, every).await },
        ))
    }
}

/// Each test binds its root before the runtime that uses it, so the root drops after the runtime.
fn test_root() -> TempRoot {
    let root = TempRoot::new("cowshed-project-runtime");
    std::fs::create_dir(root.join("checkout")).expect("create test checkout");
    root
}

fn incarnation(value: u64) -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(format!("{value:032x}")).expect("valid incarnation")
}

fn coordinator(repo_id: RepoId) -> ConnectionAuthority {
    ConnectionAuthority::Coordinator { repo_id }
}

async fn route(
    router: &RouterHandle,
    authority: ConnectionAuthority,
    method: &str,
    params: Value,
) -> Result<Value> {
    let response = router
        .route(
            authority,
            OperationRequest::decode(method, &params)?,
            None,
            None,
        )
        .await?;
    let (value, binary) = response.into_answer()?;
    assert!(binary.is_none());
    Ok(value)
}

async fn start(
    root: &Path,
    fail_create: bool,
    fail_restore: bool,
    findings: Vec<Finding>,
) -> (
    ProjectRuntime,
    RouterHandle,
    RepoId,
    mpsc::UnboundedReceiver<Event>,
) {
    start_with_removal(
        root,
        fail_create,
        fail_restore,
        findings,
        FakeRemoval::default(),
    )
    .await
}

async fn start_with_removal(
    root: &Path,
    fail_create: bool,
    fail_restore: bool,
    findings: Vec<Finding>,
    removal: FakeRemoval,
) -> (
    ProjectRuntime,
    RouterHandle,
    RepoId,
    mpsc::UnboundedReceiver<Event>,
) {
    let (events, receiver) = mpsc::unbounded_channel();
    let mut host = FakeHost::new(root, events, fail_create, fail_restore, findings);
    host.removal = removal;
    let repo = host.descriptor.repo_id.clone();
    let runtime = ProjectRuntime::start(host).await.expect("start runtime");
    let router = runtime.router();
    (runtime, router, repo, receiver)
}

async fn adopt(router: &RouterHandle, repo: &RepoId) -> Value {
    route(
        router,
        coordinator(repo.clone()),
        "coordinator.adopt",
        json!({ "repoId": repo, "options": AdoptOptions::default() }),
    )
    .await
    .expect("adopt")
}

async fn checkpoint_as_worker(
    router: &RouterHandle,
    repo: &RepoId,
    workspace: &str,
    incarnation: &WorkspaceIncarnation,
    options: CheckpointOptions,
) -> Result<Value> {
    route(
        router,
        ConnectionAuthority::Worker {
            repo_id: repo.clone(),
            workspace: WorkspaceName::new(workspace).expect("workspace"),
            workspace_incarnation: incarnation.clone(),
        },
        "worker.checkpoint",
        json!({
            "repoId": repo,
            "workspace": workspace,
            "workspaceIncarnation": incarnation,
            "options": options
        }),
    )
    .await
}

#[tokio::test]
async fn router_decodes_tagged_non_utf8_argv_without_a_string_boundary() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    let adopted = adopt(&router, &repo).await;
    while events.try_recv().is_ok() {}
    let incarnation = adopted["info"]["workspaceIncarnation"].clone();
    let raw = vec![0xff, b'a', 0x80];
    let argv = vec![
        CommandArg::from(OsString::from_vec(raw.clone())),
        CommandArg::from("--flag"),
    ];
    let params = json!({
        "repoId": repo,
        "workspace": "main",
        "workspaceIncarnation": incarnation,
        "session": null,
        "argv": serde_json::to_value(&argv).unwrap(),
        "cwd": null,
        "mode": "readWrite",
        "env": {},
        "trace": null,
        "stdin": {"kind":"empty"},
        "stdoutCopy": null,
        "stderrCopy": null
    });
    assert_eq!(
        route(
            &router,
            coordinator(repo.clone()),
            "worker.exec",
            params.clone()
        )
        .await
        .unwrap(),
        json!(1)
    );
    assert_eq!(events.recv().await, Some(Event::SnapshotBatch));
    let Some(Event::Exec {
        workspace,
        mount,
        argv: decoded,
        admission_key: None,
    }) = events.recv().await
    else {
        panic!("missing exec event");
    };
    assert_eq!(workspace, WorkspaceName::new("main").expect("main"));
    assert_eq!(mount, root.join("checkout"));
    assert_eq!(decoded, vec![raw, b"--flag".to_vec()]);

    for invalid_argv in [
        json!([{"encoding":"base64","data":"%%%"}]),
        json!([{"encoding":"utf8","data":"\u{0}"}]),
    ] {
        let mut invalid = params.clone();
        invalid["argv"] = invalid_argv;
        let error = route(&router, coordinator(repo.clone()), "worker.exec", invalid)
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::Usage);
        assert!(events.try_recv().is_err(), "invalid argv reached the host");
    }
}

/// The controller carries an exec's admission key to the host exactly as the caller named it,
/// and routes `worker.jobByKey` to the host under the caller's incarnation fence; a malformed key
/// reaches no host.
#[tokio::test]
async fn router_carries_an_admission_key_to_exec_and_routes_job_by_key() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    let adopted = adopt(&router, &repo).await;
    while events.try_recv().is_ok() {}
    let incarnation = adopted["info"]["workspaceIncarnation"].clone();
    let key = cowshed_core::api::AdmissionKey::new("op-1").expect("key");
    let exec = json!({
        "repoId": repo,
        "workspace": "main",
        "workspaceIncarnation": incarnation,
        "argv": serde_json::to_value(vec![CommandArg::from("build")]).unwrap(),
        "mode": "readWrite",
        "env": {},
        "stdin": {"kind":"empty"},
        "stdoutCopy": null,
        "stderrCopy": null,
        "admissionKey": "op-1"
    });
    assert_eq!(
        route(&router, coordinator(repo.clone()), "worker.exec", exec)
            .await
            .unwrap(),
        json!(1)
    );
    assert_eq!(events.recv().await, Some(Event::SnapshotBatch));
    let Some(Event::Exec { admission_key, .. }) = events.recv().await else {
        panic!("missing exec event");
    };
    assert_eq!(admission_key, Some(key.clone()));

    let by_key = json!({
        "repoId": repo,
        "workspace": "main",
        "workspaceIncarnation": incarnation,
        "admissionKey": "op-1"
    });
    assert_eq!(
        route(
            &router,
            coordinator(repo.clone()),
            "worker.jobByKey",
            by_key.clone()
        )
        .await
        .unwrap(),
        json!(1)
    );
    assert_eq!(events.recv().await, Some(Event::SnapshotBatch));
    assert_eq!(
        events.recv().await,
        Some(Event::JobByKey(
            WorkspaceName::new("main").expect("main"),
            key
        ))
    );

    let mut empty = by_key;
    empty["admissionKey"] = json!("");
    let error = route(&router, coordinator(repo.clone()), "worker.jobByKey", empty)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Usage);
    assert!(
        events.try_recv().is_err(),
        "a malformed key reached the host"
    );
}

#[tokio::test]
async fn log_binary_metadata_carries_the_exact_next_offset() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    let adopted = adopt(&router, &repo).await;
    let incarnation: WorkspaceIncarnation =
        serde_json::from_value(adopted["info"]["workspaceIncarnation"].clone())
            .expect("incarnation");
    let response = router
        .route(
            ConnectionAuthority::Worker {
                repo_id: repo.clone(),
                workspace: WorkspaceName::new("main").expect("main"),
                workspace_incarnation: incarnation.clone(),
            },
            JobLogs::request(LogsRequest {
                repo_id: repo.clone(),
                workspace: WorkspaceName::new("main").expect("main"),
                workspace_incarnation: incarnation.clone(),
                job_id: JobId::try_from(1).expect("job id"),
                stream: JobStream::Stdout,
                follow: false,
                offset: 7,
            }),
            None,
            None,
        )
        .await
        .expect("log route");
    let (metadata, bytes) = response.into_answer().expect("one answer");
    assert_eq!(metadata, json!({ "eof": true, "nextOffset": 7 }));
    assert_eq!(bytes, Some(Bytes::new()));
}

/// A client holds one connection, and a job's wait lasts as long as the job. Over that one
/// connection, while the wait is pending, the job's output and its status must still arrive:
/// the router never holds a job's end, and the connection answers each call as it completes.
/// This is the client the CLI and the Node addon share.
#[tokio::test]
async fn one_connection_answers_output_and_status_while_a_wait_is_pending() {
    let root = test_root();
    let (events, _events) = mpsc::unbounded_channel();
    let held = HeldJob::new(incarnation(1));
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.held_job = Some(Arc::clone(&held));
    let repo = host.descriptor.repo_id.clone();
    let runtime = ProjectRuntime::start(host).await.expect("start runtime");
    let router = runtime.router();
    let adopted = adopt(&router, &repo).await;
    assert_eq!(
        adopted["info"]["workspaceIncarnation"],
        json!(held.incarnation)
    );
    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let _connection = tokio::spawn(serve_controller_connection(
        server.into(),
        coordinator(repo.clone()),
        router.clone(),
    ));
    let (cowshed, token) = Cowshed::connect(client.into()).await.expect("handshake");
    let project = cowshed.open(root.join("checkout")).await.expect("open");
    let coordinator = cowshed.coordinator(&project, token).expect("coordinator");
    let worker = coordinator.worker("main").await.expect("worker");
    let job = worker
        .exec(ExecRequest {
            command: ExecCommand::Argv(vec![CommandArg::from("build")]),
            cwd: None,
            mode: RunSandboxMode::ReadWrite,
            env: std::collections::HashMap::new(),
            trace: None,
            stdin: StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
            admission_key: None,
        })
        .await
        .expect("exec");

    // The wait is sent first; everything after it must not queue behind it.
    let wait = job.wait();
    tokio::pin!(wait);
    let observed = async {
        let mut stdout = job.logs(JobStream::Stdout, 0, true).await?;
        let first = stdout
            .next()
            .await
            .expect("stdout yields before the job ends")?;
        let status = job.status().await?;
        Ok::<_, CowshedError>((first, status.state))
    };
    let observed = tokio::select! {
        biased;
        ended = &mut wait => panic!("the wait answered before the job ended: {ended:?}"),
        observed = tokio::time::timeout(Duration::from_secs(5), observed) => observed,
    };
    let (first, state) = observed
        .expect("output and status arrive while the wait is pending")
        .expect("the calls succeed");
    assert_eq!(&first[..], HeldJob::FIRST);
    assert_eq!(state, JobState::Running);

    held.end();
    let ended = tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the wait answers once the job ends")
        .expect("wait");
    assert_eq!(ended.state, JobState::Exited);
}

/// A job process the test speaks for: the supervisor hands each spawned job's event sender to
/// the test, which admits the job's output and ends it through that sender, in the order it
/// decides.
struct ScriptedSpawner {
    spawned: mpsc::UnboundedSender<mpsc::Sender<ProcessEvent>>,
}

#[async_trait]
impl SpawnSink for ScriptedSpawner {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        self.spawned.send(events).expect("spawn observer");
        Ok(Box::new(ScriptedProcess {
            // Not a process: its pid names nothing this test owns, so no group is identified.
            process: OwnedProcess {
                birth: Birth::Unobserved {
                    pid: 10_000 + u32::try_from(request.job_id.get()).expect("test job id"),
                    reason: "a scripted process leads no group".into(),
                },
                spawned: Instant::now(),
                host: cowshed_core::host_load::read_host_load(),
            },
        }))
    }
}

struct ScriptedProcess {
    process: OwnedProcess,
}

impl RunningProcess for ScriptedProcess {
    fn process(&self) -> Option<&OwnedProcess> {
        Some(&self.process)
    }

    fn try_write_stdin(&mut self, _bytes: Bytes) -> Result<bool> {
        Ok(true)
    }

    fn close_stdin(&mut self) -> Result<()> {
        Ok(())
    }

    fn end_stdin(&mut self) {}

    fn signal_process_tree(&mut self, _signal: ProcessSignal) -> Result<()> {
        Ok(())
    }
}

struct AcceptedCommitments;

#[async_trait]
impl CommitmentSink for AcceptedCommitments {
    async fn record(&mut self, _draft: CommitmentDraft) -> Result<()> {
        Ok(())
    }
}

/// The config of a supervisor of the main workspace at `root/workspace`, mounted under `root`.
fn supervisor_config(root: &Path) -> WorkspaceSupervisorConfig {
    supervisor_config_of(
        root,
        WorkspaceName::new("main").expect("main"),
        incarnation(1),
    )
}

/// The config of a supervisor of `workspace` at `incarnation`, at `root/workspace`.
fn supervisor_config_of(
    root: &Path,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
) -> WorkspaceSupervisorConfig {
    let workspace_root = root.join("workspace");
    std::fs::create_dir(&workspace_root).expect("workspace root");
    let authority = WorkspaceAuthoritySnapshot {
        repo_id: RepoId::parse("acme/widget").expect("fixed repo id"),
        workspace,
        workspace_incarnation,
        grant_revision: 1,
        lifecycle_revision: 1,
    };
    WorkspaceSupervisorConfig {
        owned_repo_ids: OwnedRepoIds::sole(authority.repo_id.clone()),
        authority,
        workspace_root: workspace_root.clone(),
        default_cwd: None,
        sandbox: SandboxConfig {
            home: root.join("home"),
            mount_root: root.to_path_buf(),
            workspace_mount: workspace_root.clone(),
            exec_temp_dir: root.join("tmp"),
            port_block: cowshed_core::metadata::PortBlock::new(49_136, 16).expect("port block"),
            retained_port_blocks: Vec::new(),
            mode: cowshed_core::sandbox::RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            capabilities: Default::default(),
        },
        build_volume_layout: None,
        artifacts: ArtifactConfig::default(),
        term_grace: Duration::from_millis(10),
        actor_capacity: 8,
        event_capacity: 8,
        credential_env_names: std::collections::BTreeSet::new(),
        shell_host: None,
        shell_pool: Default::default(),
        group_ledger: None,
        telemetry_root: None,
        inherited_groups: Vec::new(),
        volume_labels: None,
        workspace_volume: None,
    }
}

/// A real workspace supervisor over the real artifact store under `root`, whose job processes
/// are scripted.
fn scripted_supervisor(
    root: &Path,
    spawned: mpsc::UnboundedSender<mpsc::Sender<ProcessEvent>>,
) -> WorkspaceSupervisorHandle {
    supervisor_spawning(root, Box::new(ScriptedSpawner { spawned }))
}

/// A real workspace supervisor over the real artifact store under `root`, whose job processes
/// `spawner` starts.
fn supervisor_spawning(root: &Path, spawner: Box<dyn SpawnSink>) -> WorkspaceSupervisorHandle {
    let config = supervisor_config(root);
    let artifacts = ArtifactStoreSink::open(
        config.workspace_root.clone(),
        &config.owned_repo_ids,
        &config.authority,
        config.artifacts.clone(),
    )
    .expect("open artifact store");
    WorkspaceSupervisor::start_with_sinks(
        config,
        spawner,
        Box::new(artifacts),
        Box::new(AcceptedCommitments),
    )
    .expect("start supervisor")
}

/// The production workspace supervisor of main under `root`: its jobs are real sandboxed
/// children.
#[cfg(target_os = "macos")]
fn system_supervisor(root: &Path) -> WorkspaceSupervisorHandle {
    system_supervisor_of(supervisor_config(root))
}

/// The production workspace supervisor `config` describes.
#[cfg(target_os = "macos")]
fn system_supervisor_of(config: WorkspaceSupervisorConfig) -> WorkspaceSupervisorHandle {
    std::fs::create_dir_all(&config.sandbox.home).expect("home");
    let token = config
        .workspace_root
        .join(cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH);
    std::fs::create_dir_all(token.parent().expect("token directory")).expect("token directory");
    std::fs::write(
        &token,
        cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32]).encode(),
    )
    .expect("workspace token");
    // A credential asset is private to its owner, or the supervisor refuses it.
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("workspace token mode");
    WorkspaceSupervisor::start(config, AcceptedCommitments).expect("start supervisor")
}

/// Jobs of the main workspace, reached the way an embedder reaches them: the capability client,
/// its controller connection, the project router and the host, down to a real supervisor.
struct SupervisedJobs {
    worker: cowshed_core::api::WorkspaceHandle,
    /// The coordinator the worker was minted through, for another workspace's worker.
    #[cfg(target_os = "macos")]
    coordinator: cowshed_core::api::Coordinator,
    /// What serves another embedder's connection to the same router.
    #[cfg(target_os = "macos")]
    controller: Controller,
    _runtime: ProjectRuntime,
}

impl SupervisedJobs {
    async fn connect(root: &TempRoot, supervisor: WorkspaceSupervisorHandle) -> Self {
        Self::connect_all(
            root,
            [(WorkspaceName::new("main").expect("main"), supervisor)],
        )
        .await
    }

    /// Like [`Self::connect`], with a supervisor for each named workspace of the project.
    async fn connect_all(
        root: &TempRoot,
        supervisors: impl IntoIterator<Item = (WorkspaceName, WorkspaceSupervisorHandle)>,
    ) -> Self {
        let (events, _events) = mpsc::unbounded_channel();
        let mut host = FakeHost::new(root, events, false, false, Vec::new());
        host.supervisors.extend(supervisors);
        let repo = host.descriptor.repo_id.clone();
        let runtime = ProjectRuntime::start(host).await.expect("start runtime");
        let router = runtime.router();
        adopt(&router, &repo).await;
        let controller = Controller { repo, router };
        let (cowshed, token) = Cowshed::connect(controller.endpoint().into())
            .await
            .expect("handshake");
        let project = cowshed.open(root.join("checkout")).await.expect("open");
        let coordinator = cowshed.coordinator(&project, token).expect("coordinator");
        let worker = coordinator.worker("main").await.expect("worker");
        Self {
            worker,
            #[cfg(target_os = "macos")]
            coordinator,
            #[cfg(target_os = "macos")]
            controller,
            _runtime: runtime,
        }
    }

    async fn exec(&self, command: ExecCommand) -> cowshed_core::api::JobHandle {
        exec_in(&self.worker, command).await
    }
}

/// Admits `command` to `worker`'s workspace, as an embedder would.
async fn exec_in(
    worker: &cowshed_core::api::WorkspaceHandle,
    command: ExecCommand,
) -> cowshed_core::api::JobHandle {
    worker
        .exec(ExecRequest {
            command,
            cwd: None,
            mode: RunSandboxMode::ReadWrite,
            env: std::collections::HashMap::new(),
            trace: None,
            stdin: StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
            admission_key: None,
        })
        .await
        .expect("exec")
}

/// A project's router, serving coordinator connections the way a controller serves an embedder.
struct Controller {
    repo: RepoId,
    router: RouterHandle,
}

impl Controller {
    /// The client end of a new coordinator connection.
    fn endpoint(&self) -> std::os::unix::net::UnixStream {
        let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        tokio::spawn(serve_controller_connection(
            server.into(),
            coordinator(self.repo.clone()),
            self.router.clone(),
        ));
        client
    }
}

/// [`SupervisedJobs`] whose processes the test speaks for ([`ScriptedSpawner`]).
struct ScriptedJobs {
    jobs: SupervisedJobs,
    spawned: mpsc::UnboundedReceiver<mpsc::Sender<ProcessEvent>>,
}

impl ScriptedJobs {
    async fn start(root: &TempRoot) -> Self {
        let (spawner, spawned) = mpsc::unbounded_channel();
        let jobs = SupervisedJobs::connect(root, scripted_supervisor(root, spawner)).await;
        Self { jobs, spawned }
    }

    /// Admits a job and hands back the sender its scripted process speaks through.
    async fn exec(&mut self) -> (cowshed_core::api::JobHandle, mpsc::Sender<ProcessEvent>) {
        let job = self
            .jobs
            .exec(ExecCommand::Argv(vec![CommandArg::from("build")]))
            .await;
        let process = self.spawned.recv().await.expect("the job's process");
        (job, process)
    }
}

/// Returns once the job's `stream` holds `end` admitted bytes readable through the controller,
/// answering those after `from`, which must already be admitted. A followed read from an
/// admitted offset waits for the next bytes, so this is a barrier, never a poll; a read past
/// the admitted end would instead be refused.
async fn until_admitted(
    job: &cowshed_core::api::JobHandle,
    stream: JobStream,
    from: u64,
    end: u64,
) -> Vec<u8> {
    let want = usize::try_from(end - from).expect("test length");
    let mut follow = job
        .logs(stream, from, true)
        .await
        .expect("follow the stream");
    let mut bytes = Vec::with_capacity(want);
    while bytes.len() < want {
        let chunk = follow
            .next()
            .await
            .expect("the stream reaches the end before it closes")
            .expect("read the stream");
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes.len(), want, "no byte past the end is admitted yet");
    bytes
}

/// Admits `bytes` to the job's `stream`, a pipe-read at a time, and returns once all of it is
/// readable through the controller.
async fn admit(
    job: &cowshed_core::api::JobHandle,
    process: &mpsc::Sender<ProcessEvent>,
    stream: JobStream,
    admitted: u64,
    bytes: &[u8],
) {
    for piece in bytes.chunks(64 * 1024) {
        process
            .send(ProcessEvent::Output {
                job_id: job.id(),
                stream: StreamKind::from(stream),
                bytes: Bytes::copy_from_slice(piece),
            })
            .await
            .expect("the job admits output");
    }
    let end = admitted + u64::try_from(bytes.len()).expect("test length");
    assert_eq!(until_admitted(job, stream, admitted, end).await, bytes);
}

/// Ends the job as its process would: an exit, then both streams' EOF.
async fn end(job: &cowshed_core::api::JobHandle, process: &mpsc::Sender<ProcessEvent>) {
    process
        .send(ProcessEvent::Exited {
            job_id: job.id(),
            exit: cowshed_core::api::dto::ExitStatus::Exited { code: 0 },
        })
        .await
        .expect("the job exits");
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
        process
            .send(ProcessEvent::OutputEof {
                job_id: job.id(),
                stream,
            })
            .await
            .expect("the job's stream ends");
    }
    assert_eq!(job.wait().await.expect("wait").state, JobState::Exited);
}

fn tail_limits(bytes: u32, lines: u32) -> JobTailLimits {
    JobTailLimits {
        bytes_per_stream: JobTailBytes::new(bytes).expect("tail bytes"),
        lines_per_stream: std::num::NonZeroU32::new(lines).expect("tail lines"),
    }
}

/// Over the controller, a job's latest bounded tail ends at the end of its journal, a tail after
/// the cursor it returned holds exactly what was admitted since, and a cursor past the admitted
/// bytes is a usage error. The 1 MiB journal is far past the store's inline cap, so the sealed
/// job answers the same tails out of its promoted file.
#[tokio::test]
async fn a_tail_ends_at_the_journal_end_and_resumes_at_its_cursor() {
    const MIB: u64 = 1024 * 1024;
    let root = test_root();
    let mut jobs = ScriptedJobs::start(&root).await;
    let (job, process) = jobs.exec().await;
    let line = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-\n";
    let journal: Vec<u8> = line
        .iter()
        .copied()
        .cycle()
        .take(usize::try_from(MIB).expect("test length"))
        .collect();
    admit(&job, &process, JobStream::Stdout, 0, &journal).await;

    let limits = tail_limits(4096, 1024);
    let latest = job.tail(None, limits).await.expect("latest tail");
    assert_eq!(
        latest.next,
        JobJournalCursor {
            stdout: MIB,
            stderr: 0
        }
    );
    assert_eq!(latest.stdout.as_bytes(), &journal[journal.len() - 4096..]);
    assert!(
        latest.stdout_truncated,
        "earlier stdout lies before the tail"
    );
    assert!(latest.stderr.as_bytes().is_empty());
    assert!(!latest.stderr_truncated);

    let more = [b'+'; 100];
    admit(&job, &process, JobStream::Stdout, MIB, &more).await;
    let resumed = job
        .tail(Some(latest.next), limits)
        .await
        .expect("tail(next)");
    assert_eq!(resumed.stdout.as_bytes(), &more[..]);
    assert_eq!(
        resumed.next,
        JobJournalCursor {
            stdout: MIB + 100,
            stderr: 0
        }
    );
    assert!(!resumed.stdout_truncated);

    let past = JobJournalCursor {
        stdout: MIB + 101,
        stderr: 0,
    };
    let error = job
        .tail(Some(past), limits)
        .await
        .expect_err("past the end");
    assert_eq!(error.code, ErrorCode::Usage, "{error:?}");

    end(&job, &process).await;
    assert_eq!(
        job.tail(Some(latest.next), limits)
            .await
            .expect("sealed tail(next)"),
        resumed,
        "the sealed file answers as the live journal did"
    );
    let sealed = job.tail(None, limits).await.expect("sealed latest tail");
    let mut expected = journal[journal.len() - 3996..].to_vec();
    expected.extend_from_slice(&more);
    assert_eq!(sealed.stdout.as_bytes(), &expected[..]);
    assert_eq!(sealed.next, resumed.next);
    let error = job
        .tail(Some(past), limits)
        .await
        .expect_err("sealed, past the end");
    assert_eq!(error.code, ErrorCode::Usage, "{error:?}");

    // Lines bound a tail as bytes do: the latest two lines are the last full line and the
    // unterminated one after it; one line after a cursor ends at its newline.
    let two = job
        .tail(None, tail_limits(4096, 2))
        .await
        .expect("two lines");
    let mut expected = line.to_vec();
    expected.extend_from_slice(&more);
    assert_eq!(two.stdout.as_bytes(), &expected[..]);
    assert!(two.stdout_truncated);
    let cursor = JobJournalCursor {
        stdout: MIB - 128,
        stderr: 0,
    };
    let one = job
        .tail(Some(cursor), tail_limits(4096, 1))
        .await
        .expect("one line");
    assert_eq!(one.stdout.as_bytes(), &line[..]);
    assert_eq!(one.next.stdout, MIB - 64);
    assert!(
        one.stdout_truncated,
        "more admitted stdout follows the line"
    );
}

/// The same tail contract over a real sandboxed child, through the controller: the child writes
/// 1 MiB, then blocks reading a FIFO the test holds and copies what the test writes into it to
/// stdout. The latest 4 KiB tail ends at the admitted end; once the test releases 100 bytes,
/// `tail(next)` returns exactly them; a cursor past the end is a usage error; and the sealed job,
/// its stdout promoted to a file, answers the same tails.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_real_child_s_tail_ends_at_its_journal_end_and_resumes_at_its_cursor() {
    use std::io::Write as _;
    const MIB: u64 = 1024 * 1024;
    let root = test_root();
    let jobs = SupervisedJobs::connect(&root, system_supervisor(&root)).await;
    let gate = root.join("workspace").join("gate");
    use cowshed_core::fork_lock::Run as _;
    let made = std::process::Command::new("/usr/bin/mkfifo")
        .arg(&gate)
        .status_locked()
        .expect("run mkfifo");
    assert!(made.success(), "mkfifo {}", gate.display());
    let job = jobs
        .exec(ExecCommand::Argv(
            [
                "/bin/sh",
                "-c",
                "/usr/bin/head -c 1048576 /dev/zero | /usr/bin/tr '\\000' a && /bin/cat gate",
            ]
            .into_iter()
            .map(CommandArg::from)
            .collect(),
        ))
        .await;
    let journal = until_admitted(&job, JobStream::Stdout, 0, MIB).await;
    assert!(journal.iter().all(|byte| *byte == b'a'));

    // A non-follow reader drains the admitted prefix, not the lifetime of the child blocked
    // on the FIFO. Resuming and reading at the current end must also finish without a timer.
    for offset in [0, MIB - 7, MIB] {
        let mut logs = job
            .logs(JobStream::Stdout, offset, false)
            .await
            .expect("read current stdout");
        let mut bytes = Vec::new();
        while let Some(chunk) = logs.next().await {
            bytes.extend_from_slice(&chunk.expect("stdout chunk"));
        }
        assert_eq!(bytes, journal[usize::try_from(offset).expect("offset")..]);
        assert_eq!(
            job.status().await.expect("held job").state,
            JobState::Running
        );
    }
    let mut stderr = job
        .logs(JobStream::Stderr, 0, false)
        .await
        .expect("read current stderr");
    assert!(stderr.next().await.is_none());
    assert_eq!(
        job.status().await.expect("held job").state,
        JobState::Running
    );

    let mut followed = job
        .logs(JobStream::Stdout, MIB, true)
        .await
        .expect("follow past the current end");

    let limits = tail_limits(4096, 1024);
    let latest = job.tail(None, limits).await.expect("latest tail");
    assert!(
        latest.stderr.as_bytes().is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(latest.stderr.as_bytes())
    );
    assert_eq!(
        latest.next,
        JobJournalCursor {
            stdout: MIB,
            stderr: 0
        }
    );
    assert_eq!(latest.stdout.as_bytes(), &[b'a'; 4096][..]);
    assert!(latest.stdout_truncated);

    let more = [b'+'; 100];
    let released = gate.clone();
    let gate_writer = tokio::task::spawn_blocking(move || {
        // Opening blocks until the child opens the FIFO to read; keep it open until the
        // followed reader has received these bytes while the child still lives.
        let mut gate = std::fs::OpenOptions::new()
            .write(true)
            .open(released)
            .expect("open the gate");
        gate.write_all(&more).expect("release 100 bytes");
        gate
    })
    .await
    .expect("release task");
    assert_eq!(
        until_admitted(&job, JobStream::Stdout, MIB, MIB + 100).await,
        more
    );
    let mut followed_bytes = Vec::new();
    while followed_bytes.len() < more.len() {
        followed_bytes.extend_from_slice(
            &followed
                .next()
                .await
                .expect("followed bytes")
                .expect("chunk"),
        );
    }
    assert_eq!(followed_bytes, more);
    assert_eq!(
        job.status().await.expect("held job").state,
        JobState::Running
    );
    drop(gate_writer);
    assert!(followed.next().await.is_none());
    let resumed = job
        .tail(Some(latest.next), limits)
        .await
        .expect("tail(next)");
    assert_eq!(resumed.stdout.as_bytes(), &more[..]);
    assert_eq!(
        resumed.next,
        JobJournalCursor {
            stdout: MIB + 100,
            stderr: 0
        }
    );
    assert!(!resumed.stdout_truncated);
    let past = JobJournalCursor {
        stdout: MIB + 101,
        stderr: 0,
    };
    let error = job
        .tail(Some(past), limits)
        .await
        .expect_err("past the end");
    assert_eq!(error.code, ErrorCode::Usage, "{error:?}");

    let ended = job.wait().await.expect("wait");
    assert_eq!(
        ended.exit,
        Some(cowshed_core::api::dto::ExitStatus::Exited { code: 0 })
    );
    assert_eq!(ended.stdout.bytes, MIB + 100);
    assert_eq!(
        job.tail(Some(latest.next), limits)
            .await
            .expect("sealed tail(next)"),
        resumed
    );
    let sealed = job.tail(None, limits).await.expect("sealed latest tail");
    let mut expected = vec![b'a'; 3996];
    expected.extend_from_slice(&more);
    assert_eq!(sealed.stdout.as_bytes(), &expected[..]);
    let error = job
        .tail(Some(past), limits)
        .await
        .expect_err("sealed, past the end");
    assert_eq!(error.code, ErrorCode::Usage, "{error:?}");
}

/// What a listening job runs: it listens on an ephemeral IPv4 loopback port, prints the port,
/// and holds it until the test closes the workspace's `gate` FIFO.
#[cfg(target_os = "macos")]
const LISTENER: &str = r#"use strict;
use IO::Socket::IP;
my $listener = IO::Socket::IP->new(LocalHost => '127.0.0.1', LocalPort => 0, Listen => 8)
  or die "listen: $@";
$| = 1;
print $listener->sockport, "\n";
open(my $gate, '<', 'gate') or die "gate: $!";
my $released = <$gate>;
"#;

/// A real job of a workspace whose grandchild listens, the port it listens on and the FIFO
/// that ends it.
#[cfg(target_os = "macos")]
struct ListeningJob {
    job: cowshed_core::api::JobHandle,
    port: u16,
    gate: PathBuf,
}

/// Admits a job to `worker`'s workspace, whose supervisor runs jobs in `workspace`, whose `sh`
/// runs a child `sh` that runs [`LISTENER`], and returns once the listener has printed its
/// port: the listening socket exists from then on.
#[cfg(target_os = "macos")]
async fn listening_job(
    workspace: &Path,
    worker: &cowshed_core::api::WorkspaceHandle,
) -> ListeningJob {
    use cowshed_core::fork_lock::Run as _;
    std::fs::write(workspace.join("listen.pl"), LISTENER).expect("the listener program");
    let gate = workspace.join("gate");
    let made = std::process::Command::new("/usr/bin/mkfifo")
        .arg(&gate)
        .status_locked()
        .expect("run mkfifo");
    assert!(made.success(), "mkfifo {}", gate.display());
    let job = exec_in(
        worker,
        ExecCommand::Argv(
            [
                "/bin/sh",
                "-c",
                "/bin/sh -c '/usr/bin/perl listen.pl & wait' & wait",
            ]
            .into_iter()
            .map(CommandArg::from)
            .collect(),
        ),
    )
    .await;
    let mut stdout = job
        .logs(JobStream::Stdout, 0, true)
        .await
        .expect("follow stdout");
    let mut line = Vec::new();
    while !line.contains(&b'\n') {
        let Some(chunk) = stdout.next().await else {
            let ended = job.wait().await.expect("the ended job");
            panic!("the listener never printed its port: {ended:?}");
        };
        line.extend_from_slice(&chunk.expect("read stdout"));
    }
    let port = std::str::from_utf8(&line)
        .expect("a UTF-8 line")
        .trim()
        .parse()
        .expect("a port");
    ListeningJob { job, port, gate }
}

/// Closes the gate the listener reads, which ends it and then its job.
#[cfg(target_os = "macos")]
async fn release(gate: PathBuf) {
    tokio::task::spawn_blocking(move || {
        // Opening blocks until the listener has the FIFO open to read; closing it is its EOF.
        std::fs::OpenOptions::new()
            .write(true)
            .open(gate)
            .expect("open the gate");
    })
    .await
    .expect("release task");
}

/// `job.listeningPorts()` as an embedder's Node process answers it through the built N-API
/// addon, over its own coordinator connection to the same router.
#[cfg(target_os = "macos")]
async fn napi_listening_ports(
    jobs: &SupervisedJobs,
    root: &TempRoot,
    job: JobId,
) -> cowshed_core::api::dto::JobListeningPorts {
    use cowshed_core::fork_lock::Spawn as _;
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    let package = Path::new(&manifest).join("../..");
    let module = Url::from_file_path(package.join("dist/ts/index.js").canonicalize().expect(
        "the built TypeScript package (nx run cowshed:tsc-js) under packages/cowshed/dist/ts",
    ))
    .expect("a module URL");
    let client = format!(
        "import {{ connectCoordinator, coordinatorEndpoint }} from {module};\n\
         const coordinator = await connectCoordinator(coordinatorEndpoint(3), {checkout});\n\
         const worker = await coordinator.worker('main');\n\
         const job = await worker.job({job});\n\
         console.log(JSON.stringify(await job.listeningPorts()));\n\
         process.exit(0);\n",
        module = json!(module.as_str()),
        checkout = json!(root.join("checkout")),
        job = job.get(),
    );
    let node = std::env::var_os("PATH")
        .and_then(|path| {
            std::env::split_paths(&path)
                .map(|directory| directory.join("node"))
                .find(|node| node.is_file())
        })
        .expect("node on the test's PATH");
    let endpoint = std::os::fd::OwnedFd::from(jobs.controller.endpoint());
    let output = tokio::task::spawn_blocking(move || {
        // The endpoint arrives as stdin; the shell hands it to Node as descriptor 3, the way a
        // trusted spawner hands an embedder its controller endpoint.
        std::process::Command::new("/bin/sh")
            .args([
                "-c",
                r#"exec "$0" --input-type=module --eval "$1" 3<&0 </dev/null"#,
            ])
            .arg(node)
            .arg(client)
            .env("NAPI_DEBUG_ADDON", "1")
            .current_dir(&package)
            .stdin(std::process::Stdio::from(endpoint))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn_locked()?
            .wait_with_output()
    })
    .await
    .expect("node task")
    .expect("run node");
    assert!(
        output.status.success(),
        "node: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("node answers the job's listening ports")
}

/// `listeningPorts()` over real sandboxed jobs, through the controller and through N-API: a job
/// whose grandchild listens on an ephemeral port names exactly that port in one read, while a
/// listener this test holds on the host and one in another workspace's job are not named. The
/// other workspace's job answers its own port alone. Once its listener ended, the job names
/// none.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_job_s_listening_ports_are_its_own_group_s() {
    // Bound to the dual-stack wildcard, its port is no job's IPv4 loopback listener's.
    let host = std::net::TcpListener::bind("[::]:0").expect("host listener");
    let host_port = host.local_addr().expect("host listener address").port();
    let root = test_root();
    // A second workspace of the same project, with its own name and incarnation, served by its
    // own production supervisor under its own root.
    let feature = WorkspaceName::new("feature").expect("feature");
    let feature_root = test_root();
    let jobs = SupervisedJobs::connect_all(
        &root,
        [
            (
                WorkspaceName::new("main").expect("main"),
                system_supervisor(&root),
            ),
            (
                feature.clone(),
                system_supervisor_of(supervisor_config_of(
                    &feature_root,
                    feature.clone(),
                    incarnation(2),
                )),
            ),
        ],
    )
    .await;
    jobs.coordinator
        .create(feature.as_str(), CreateOptions::default())
        .await
        .expect("create the feature workspace");
    let feature_worker = jobs
        .coordinator
        .worker(feature.as_str())
        .await
        .expect("the feature workspace's worker");
    let (main_info, feature_info) = (jobs.worker.info(), feature_worker.info());
    assert_ne!(main_info.workspace, feature_info.workspace);
    assert_ne!(
        main_info.workspace_incarnation,
        feature_info.workspace_incarnation
    );
    let listening = listening_job(&root.join("workspace"), &jobs.worker).await;
    let other = listening_job(&feature_root.join("workspace"), &feature_worker).await;
    assert_ne!(listening.port, other.port);

    let ports = listening
        .job
        .listening_ports()
        .await
        .expect("the job's listening ports");
    assert_eq!(ports.job_id, listening.job.id());
    assert_eq!(ports.ports, vec![listening.port]);
    assert!(!ports.ports.contains(&host_port));
    assert!(!ports.ports.contains(&other.port));
    assert_eq!(
        other
            .job
            .listening_ports()
            .await
            .expect("the other workspace's job's listening ports")
            .ports,
        vec![other.port]
    );

    let answered = napi_listening_ports(&jobs, &root, listening.job.id()).await;
    assert_eq!(answered.job_id, listening.job.id());
    assert_eq!(answered.ports, vec![listening.port]);

    for ListeningJob { job, gate, .. } in [listening, other] {
        release(gate).await;
        let ended = job.wait().await.expect("wait");
        assert_eq!(
            ended.exit,
            Some(cowshed_core::api::dto::ExitStatus::Exited { code: 0 })
        );
        assert_eq!(
            job.listening_ports()
                .await
                .expect("an ended job's listening ports")
                .ports,
            Vec::<u16>::new()
        );
    }
    drop(host);
}

/// Distinct unnamed sessions cannot share a keyed admission, even when their authored
/// arguments are identical. The first job is a real child and its counter is the effect oracle.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_keyed_job_distinguishes_unnamed_sessions() {
    use cowshed_core::error::{AdmissionField, AdmissionRefusal};
    let root = test_root();
    let config = supervisor_config(&root);
    let supervisor = system_supervisor_of(config.clone());
    let first_session = supervisor.open_session(None).await.expect("first session");
    let second_session = supervisor.open_session(None).await.expect("second session");
    let request = || ExecRequest {
        command: ExecCommand::Argv(
            ["/bin/sh", "-c", "printf 'once\\n' >> counter"]
                .into_iter()
                .map(CommandArg::from)
                .collect(),
        ),
        cwd: None,
        mode: RunSandboxMode::ReadWrite,
        env: std::collections::HashMap::new(),
        trace: None,
        stdin: StdinSource::Empty,
        stdout_copy: None,
        stderr_copy: None,
        admission_key: Some(cowshed_core::api::AdmissionKey::new("session-op").unwrap()),
    };
    let job = supervisor
        .exec(Some(&first_session), None, request())
        .await
        .expect("first admission");
    let ended = supervisor.wait(job).await.expect("first job ends");
    assert_eq!(
        ended.exit,
        Some(cowshed_core::api::ExitStatus::Exited { code: 0 })
    );
    let repeated = supervisor
        .exec(Some(&second_session), None, request())
        .await;
    let counter = std::fs::read(root.join("workspace").join("counter")).expect("counter");
    supervisor.retire().await.expect("retire supervisor");
    let refusal = repeated.expect_err("another unnamed session is not the same request");
    assert_eq!(refusal.code, ErrorCode::Conflict);
    assert_eq!(
        refusal.admission_source(),
        Some(&AdmissionRefusal::KeyConflict {
            job_id: job,
            fields: vec![AdmissionField::Session],
        })
    );
    assert_eq!(counter, b"once\n");

    // The new supervisor allocates its local session counters from one again. That reused
    // counter must not alias the admission's earlier unnamed session.
    let restarted = system_supervisor_of(config);
    let new_session = restarted.open_session(None).await.expect("new session");
    let repeated = restarted.exec(Some(&new_session), None, request()).await;
    let counter = std::fs::read(root.join("workspace").join("counter")).expect("counter");
    restarted
        .retire()
        .await
        .expect("retire restarted supervisor");
    let refusal = repeated.expect_err("a restarted unnamed session is a different request");
    assert_eq!(refusal.code, ErrorCode::Conflict);
    assert_eq!(
        refusal.admission_source(),
        Some(&AdmissionRefusal::KeyConflict {
            job_id: job,
            fields: vec![AdmissionField::Session],
        })
    );
    assert_eq!(counter, b"once\n");
}

/// A job whose leader is a real child the test holds: `/bin/cat` of a FIFO, leading a process
/// group of its own, blocked opening the FIFO until the test opens it to write. The test speaks
/// for the job's lifecycle through its event sender, as [`ScriptedSpawner`] does; its resource
/// samples read the real group.
struct GatedSpawner {
    gate: PathBuf,
    spawned: mpsc::UnboundedSender<(mpsc::Sender<ProcessEvent>, std::process::Child)>,
}

#[async_trait]
impl SpawnSink for GatedSpawner {
    async fn spawn(
        &mut self,
        _request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        use cowshed_core::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        // `cat` through PATH: a Nix-built Linux host has no /bin/cat.
        let child = std::process::Command::new("cat")
            .arg(&self.gate)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .process_group(0)
            .spawn_locked()
            .expect("a gated child");
        let process = OwnedProcess {
            // Identified while this test holds it unreaped.
            birth: Birth::of(child.id()),
            spawned: Instant::now(),
            host: cowshed_core::host_load::read_host_load(),
        };
        self.spawned.send((events, child)).expect("spawn observer");
        Ok(Box::new(ScriptedProcess { process }))
    }
}

/// Over the controller, `progress(everyMs)` sends a running job's latest sample at once and one
/// more every interval while the job writes nothing, then its terminal sample exactly once --
/// the one its sealed record keeps -- and closes. A subscription nobody reads holds up no other
/// call, and dropping one leaves the job running.
#[tokio::test]
async fn progress_samples_a_silent_job_periodically_then_its_sealed_terminal_once() {
    use std::io::Write as _;
    let root = test_root();
    let gate = root.join("gate");
    // The FIFO is made in-process: a Linux host need not have /usr/bin/mkfifo.
    let path =
        std::ffi::CString::new(gate.as_os_str().as_bytes()).expect("a gate path without NUL");
    // SAFETY: `path` is a NUL-terminated path under this test's own root.
    let made = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
    assert_eq!(
        made,
        0,
        "mkfifo {}: {}",
        gate.display(),
        std::io::Error::last_os_error()
    );
    let (spawner, mut spawned) = mpsc::unbounded_channel();
    let jobs = SupervisedJobs::connect(
        &root,
        supervisor_spawning(
            &root,
            Box::new(GatedSpawner {
                gate: gate.clone(),
                spawned: spawner,
            }),
        ),
    )
    .await;
    let job = jobs
        .exec(ExecCommand::Argv(vec![CommandArg::from("build")]))
        .await;
    let (process, mut child) = spawned.recv().await.expect("the job's process");
    let leader = child.id();
    let every = cowshed_core::api::SampleInterval::new(50).expect("interval");

    // Unread, a subscription holds up nothing else on the connection; dropped, it leaves the
    // job as it was.
    let unread = job.progress(every).await.expect("subscribe");
    assert_eq!(job.status().await.expect("status").state, JobState::Running);
    drop(unread);
    assert_eq!(job.status().await.expect("status").state, JobState::Running);

    let mut progress = job.progress(every).await.expect("subscribe");
    let mut running = Vec::new();
    // The latest sample at once, then three periodic ones while the job is silent.
    for _ in 0..4 {
        let sample = progress
            .next()
            .await
            .expect("a sample while the job runs")
            .expect("sampled");
        assert_eq!(
            (sample.job_id, sample.leader_pid, sample.members.clone()),
            (job.id(), leader, vec![leader]),
            "a running sample of the gated group"
        );
        assert_eq!(
            (sample.stdout.bytes.get(), sample.stderr.bytes.get()),
            (0, 0),
            "the job wrote nothing"
        );
        running.push(sample);
    }
    for pair in running.windows(2) {
        assert!(
            pair[0].wall_us < pair[1].wall_us && pair[0].sampled_at <= pair[1].sampled_at,
            "each sample is a later observation: {pair:?}"
        );
    }
    assert_eq!(job.status().await.expect("status").state, JobState::Running);

    // Release the child: opening the FIFO to write lets `cat` open it, and closing it ends
    // `cat` at end of file.
    let released = gate.clone();
    tokio::task::spawn_blocking(move || {
        let mut gate = std::fs::OpenOptions::new()
            .write(true)
            .open(released)
            .expect("open the gate");
        gate.write_all(b"done").expect("release the child");
    })
    .await
    .expect("release task");
    // The leader stays unreaped until the job concludes, as a real parent holds it, so the
    // terminal sample can still read its rusage.
    // SAFETY: an all-zero siginfo is a valid value of the plain C struct.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waiting for the test's own child without reaping it.
    let waited = unsafe {
        libc::waitid(
            libc::P_PID,
            leader,
            &mut info,
            libc::WEXITED | libc::WNOWAIT,
        )
    };
    assert_eq!(waited, 0, "waitid: {}", std::io::Error::last_os_error());
    end(&job, &process).await;
    let (sealed, _) = jobs.worker.sealed(job.id()).await.expect("sealed record");
    assert!(child.wait().expect("reap the child").success());
    let sealed = sealed.resources.expect("a sealed terminal sample");

    let mut after = Vec::new();
    while let Some(sample) = progress.next().await {
        after.push(sample.expect("sampled"));
    }
    let (terminal, live) = after.split_last().expect("the terminal sample");
    assert_eq!(terminal, &sealed, "the last sample is the sealed one");
    assert_eq!(
        (terminal.leader_pid, terminal.members.clone()),
        (leader, Vec::new()),
        "the leader is named after its group emptied"
    );
    assert!(
        live.iter().all(|sample| sample.members == [leader]),
        "only running samples precede the terminal one, which is sent once: {after:?}"
    );
    assert!(
        progress.next().await.is_none(),
        "the stream stays closed after its terminal sample"
    );
}

/// Every byte `stream` yields until it closes.
async fn drain(mut stream: cowshed_core::RawByteStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.expect("read the attached stream"));
    }
    bytes
}

/// Over the controller, an attachment resumes each stream at the offset its cursor names: a
/// reader that already holds the first `n` stdout bytes is never sent one of them again, while
/// stderr, named at zero, arrives whole. Attaching starts no process.
#[tokio::test]
async fn an_attachment_resumes_each_stream_at_its_cursor() {
    let root = test_root();
    let mut jobs = ScriptedJobs::start(&root).await;
    let (job, process) = jobs.exec().await;
    let seen = b"already seen\n";
    let n = u64::try_from(seen.len()).expect("test length");
    admit(&job, &process, JobStream::Stdout, 0, seen).await;
    admit(&job, &process, JobStream::Stderr, 0, b"warning\n").await;

    let attachment = job
        .attach(Some(JobJournalCursor {
            stdout: n,
            stderr: 0,
        }))
        .await
        .expect("attach");
    assert!(
        jobs.spawned.try_recv().is_err(),
        "attaching started no process"
    );
    admit(&job, &process, JobStream::Stdout, n, b"new\n").await;
    end(&job, &process).await;

    let (_stdin, stdout, stderr) = attachment.into_parts();
    let (stdout, stderr) = tokio::join!(drain(stdout), drain(stderr));
    assert_eq!(stdout, b"new\n", "no stdout byte before the cursor arrives");
    assert_eq!(stderr, b"warning\n");
}

/// A create that asks for its steps hears each one as it happens, over the controller connection
/// the embedder holds: while the create is still held inside its clone step, the clone and the
/// first write inside it have already arrived, nested; the rest arrive before the answer, and the
/// stream ends with the call. A hung create therefore names the step it hangs in.
#[tokio::test]
async fn a_reported_create_hears_its_steps_while_it_runs() {
    let root = test_root();
    let (events, _events) = mpsc::unbounded_channel();
    let gate = Arc::new(Notify::new());
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.create_gate = Some(Arc::clone(&gate));
    let repo = host.descriptor.repo_id.clone();
    let runtime = ProjectRuntime::start(host).await.expect("start runtime");
    let router = runtime.router();
    adopt(&router, &repo).await;
    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let _connection = tokio::spawn(serve_controller_connection(
        server.into(),
        coordinator(repo.clone()),
        router.clone(),
    ));
    let (cowshed, token) = Cowshed::connect(client.into()).await.expect("handshake");
    let project = cowshed.open(root.join("checkout")).await.expect("open");
    let coordinator = cowshed.coordinator(&project, token).expect("coordinator");

    let started = |step, parent, scope: &str, name: &str| StepReport::Started {
        step,
        parent,
        scope: scope.to_owned(),
        name: name.to_owned(),
    };
    let ended = |step| StepReport::Ended { step, error: None };
    let (steps, mut reports) = mpsc::unbounded_channel();
    let create = coordinator.create_reporting("feature", CreateOptions::default(), steps);
    tokio::pin!(create);
    let heard_while_held = async {
        let mut heard = Vec::new();
        while heard.len() < 3 {
            heard.push(
                reports
                    .recv()
                    .await
                    .expect("a step while the create is held"),
            );
        }
        heard
    };
    let heard_while_held = tokio::select! {
        biased;
        created = &mut create => panic!("the create answered while held: {created:?}"),
        heard = tokio::time::timeout(Duration::from_secs(5), heard_while_held) => heard,
    }
    .expect("the steps so far arrive while the create is held");
    assert_eq!(
        heard_while_held,
        [
            started(0, None, "new", "clone"),
            started(1, Some(0), "apfs", "canonical/first-write"),
            ended(1),
        ]
    );

    gate.notify_one();
    let created = tokio::time::timeout(Duration::from_secs(5), create)
        .await
        .expect("the create answers once released")
        .expect("create");
    assert_eq!(created.info().workspace.as_str(), "feature");
    let mut rest = Vec::new();
    while let Some(report) = reports.recv().await {
        rest.push(report);
    }
    assert_eq!(
        rest,
        [ended(0), started(2, None, "new", "initialize"), ended(2)]
    );
}

#[tokio::test]
async fn adopt_option_identity_mismatch_rejects_before_host_mutation() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.adopt",
        json!({
            "repoId": repo,
            "options": AdoptOptions {
                repo_id: Some(RepoId::parse("other/repository").expect("mismatch identity")),
                ..AdoptOptions::default()
            }
        }),
    )
    .await
    .expect_err("mismatching adopt identity must fail");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(error.message.contains("provisional project binding"));
    assert!(
        events.try_recv().is_err(),
        "identity mismatch reached the runtime host"
    );
}

#[tokio::test]
async fn secret_refusal_precedes_image_initialization_and_quarantine_preserves_paths() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    let secret = root.join("checkout/.env");
    std::fs::write(&secret, "API_TOKEN=not-for-images").expect("write secret");

    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.adopt",
        json!({ "repoId": repo, "options": AdoptOptions::default() }),
    )
    .await
    .expect_err("secret must refuse adopt");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(events.recv().await, Some(Event::SecretScan));
    assert!(
        events.try_recv().is_err(),
        "refusal reached image initialization"
    );

    let adopted = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.adopt",
        json!({
            "repoId": repo,
            "options": AdoptOptions { quarantine: true, ..AdoptOptions::default() }
        }),
    )
    .await
    .expect("quarantined adopt");
    assert_eq!(adopted["info"]["workspace"], "main");
    assert!(!secret.exists());
    let quarantined = root.join("store/quarantine/.env");
    assert_eq!(
        std::fs::read_to_string(&quarantined).expect("quarantined bytes"),
        "API_TOKEN=not-for-images"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&quarantined)
                .expect("quarantine metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert_eq!(events.recv().await, Some(Event::SecretScan));
    assert!(matches!(events.recv().await, Some(Event::Initialize(_))));
    assert!(matches!(events.recv().await, Some(Event::Publish(_))));
}

#[tokio::test]
async fn adopt_then_create_list_and_path_use_one_immutable_snapshot() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "task", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");

    while events.try_recv().is_ok() {}
    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list");
    assert_eq!(listed.as_array().expect("array").len(), 2);
    let listed_workspaces = listed.as_array().expect("array");
    let main = listed_workspaces
        .iter()
        .find(|workspace| workspace["info"]["workspace"] == "main")
        .expect("main snapshot");
    let task_snapshot = listed_workspaces
        .iter()
        .find(|workspace| workspace["info"]["workspace"] == "task")
        .expect("task snapshot");
    assert_eq!(
        PathBuf::from(main["info"]["mount"].as_str().expect("main mount")),
        root.join("checkout")
    );
    assert_eq!(
        PathBuf::from(
            task_snapshot["info"]["mount"]
                .as_str()
                .expect("session mount")
        ),
        root.join("store/mnt/task")
    );
    assert_eq!(events.recv().await, Some(Event::SnapshotBatch));
    assert!(events.try_recv().is_err(), "list made an N+1 host call");

    let task = route(
        &router,
        coordinator(repo.clone()),
        "project.workspace",
        json!({ "repoId": repo, "workspace": "task" }),
    )
    .await
    .expect("workspace path");
    assert_eq!(
        PathBuf::from(task["info"]["mount"].as_str().expect("mount")),
        root.join("store/mnt/task")
    );
    assert_eq!(listed.as_array().expect("array").len(), 2);
}

#[tokio::test]
async fn workspace_at_uses_active_mount_facts_and_attach_preserves_exec_mount() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    let adopted = adopt(&router, &repo).await;
    let mount = PathBuf::from(adopted["info"]["mount"].as_str().expect("mount"));
    assert_eq!(mount, root.join("checkout"));
    let nested = mount.join("src/deep/module");

    let resolved = route(
        &router,
        coordinator(repo.clone()),
        "project.workspaceAt",
        json!({ "repoId": repo, "path": nested }),
    )
    .await
    .expect("nested path resolution");
    assert_eq!(resolved["info"]["workspace"], "main");

    let created = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "task", "options": CreateOptions::default() }),
    )
    .await
    .expect("create session");
    route(
        &router,
        coordinator(repo.clone()),
        "workspace.attach",
        json!({
            "repoId": repo,
            "workspace": "task",
            "options": AttachOptions::default(),
        }),
    )
    .await
    .expect("attach session");
    let session_mount = root.join("store/mnt/task");
    while events.try_recv().is_ok() {}
    route(
        &router,
        coordinator(repo.clone()),
        "worker.exec",
        json!({
            "repoId": repo,
            "workspace": "task",
            "workspaceIncarnation": created["info"]["workspaceIncarnation"],
            "session": null,
            "argv": [{"encoding":"utf8","data":"true"}],
            "cwd": null,
            "mode": "readWrite",
            "env": {},
            "trace": null,
            "stdin": {"kind":"empty"},
            "stdoutCopy": null,
            "stderrCopy": null
        }),
    )
    .await
    .expect("session exec");
    assert_eq!(events.recv().await, Some(Event::SnapshotBatch));
    let Some(Event::Exec {
        workspace,
        mount: exec_mount,
        ..
    }) = events.recv().await
    else {
        panic!("missing session exec event");
    };
    assert_eq!(workspace, WorkspaceName::new("task").expect("task"));
    assert_eq!(exec_mount, session_mount);

    let marker_only = root.join("marker-only");
    std::fs::create_dir_all(marker_only.join(".cowshed")).expect("marker directory");
    std::fs::write(
        marker_only.join(".cowshed/workspace.json"),
        serde_json::to_vec(&resolved["info"]).expect("marker bytes"),
    )
    .expect("marker");
    for rejected in [marker_only.join("child"), root.join("another-project")] {
        let error = route(
            &router,
            coordinator(repo.clone()),
            "project.workspaceAt",
            json!({ "repoId": repo, "path": rejected }),
        )
        .await
        .expect_err("non-mount path must not resolve");
        assert_eq!(error.code, ErrorCode::NotFound);
    }

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.detach",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("detach");
    let error = route(
        &router,
        coordinator(repo.clone()),
        "project.workspaceAt",
        json!({ "repoId": repo, "path": mount.join("src") }),
    )
    .await
    .expect_err("detached workspace must not resolve");
    assert_eq!(error.code, ErrorCode::NotFound);
}

/// `mv main` and `attach` are the two ways the project's checkout path changes, and both are
/// routed rather than inferred: the coordinator carries a destination path, and attach carries the
/// caller's observation. Main's mount is derived from the recorded checkout, so querying it after
/// each call witnesses that the new path actually landed in the host.
#[tokio::test]
async fn moving_the_checkout_and_attaching_from_an_alias_both_move_the_recorded_path() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    let adopted = adopt(&router, &repo).await;
    assert_eq!(
        adopted["info"]["mount"].as_str().expect("mount"),
        root.join("checkout").to_string_lossy()
    );

    let moved_to = root.join("moved");
    let moved = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.moveCheckout",
        json!({ "repoId": repo, "destination": moved_to }),
    )
    .await
    .expect("move checkout");
    assert_eq!(moved["info"]["workspace"], "main");
    assert_eq!(
        moved["info"]["mount"].as_str().expect("mount"),
        moved_to.to_string_lossy()
    );

    // A second move to the same place has nothing to do, which is only observable because the
    // first one really moved the record rather than reporting that it had.
    assert!(
        route(
            &router,
            coordinator(repo.clone()),
            "coordinator.moveCheckout",
            json!({ "repoId": repo, "destination": moved_to }),
        )
        .await
        .is_err()
    );

    // Attach converges onto the checkout the caller observed, which is how a hand-moved checkout
    // gets its record repaired without a `mv`.
    let observed = root.join("alias");
    route(
        &router,
        coordinator(repo.clone()),
        "workspace.attach",
        json!({
            "repoId": repo,
            "workspace": "main",
            "options": {"browse": false, "observedPath": observed},
        }),
    )
    .await
    .expect("attach with an observation");
    let attached = route(
        &router,
        coordinator(repo.clone()),
        "workspace.info",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("info after observed attach");
    assert_eq!(
        attached["mount"].as_str().expect("mount"),
        observed.to_string_lossy()
    );

    // An attach with no observation offers nothing to converge onto and must leave the record be.
    route(
        &router,
        coordinator(repo.clone()),
        "workspace.attach",
        json!({
            "repoId": repo,
            "workspace": "main",
            "options": AttachOptions::default(),
        }),
    )
    .await
    .expect("attach without an observation");
    let attached = route(
        &router,
        coordinator(repo.clone()),
        "workspace.info",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("info after unobserved attach");
    assert_eq!(
        attached["mount"].as_str().expect("mount"),
        observed.to_string_lossy()
    );
}

/// A coordinator verb invoked from inside a workspace opens the project it belongs to. The
/// workspace mount is a standalone repository, so the path the caller sends is that mount and not
/// the project's checkout; demanding they be the same string refused the very callers that infer
/// their workspace from the cwd. A path outside the project is still refused.
#[tokio::test]
async fn opening_the_project_accepts_a_workspace_mount_and_still_refuses_a_stranger() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    let checkout = root.join("checkout");

    let opened = route(
        &router,
        coordinator(repo.clone()),
        "project.open",
        json!({ "path": checkout }),
    )
    .await
    .expect("main's checkout opens the project");
    assert_eq!(
        opened["gitRoot"].as_str().expect("git root"),
        checkout.to_string_lossy()
    );

    // A workspace mount carrying this project's marker belongs to it, though its path differs.
    let mount = root.join("mnt/task");
    std::fs::create_dir_all(mount.join(".cowshed")).expect("workspace mount");
    std::fs::write(
        mount.join(".cowshed/workspace.json"),
        serde_json::to_vec(&json!({
            "version": 1,
            "repoId": repo,
            "projectRoot": checkout,
            "workspace": "task",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "baseCommit": "0123456789abcdef",
            "createdAt": "2026-07-13T00:00:00Z",
            "createdTrace": "fixture",
        }))
        .expect("marker bytes"),
    )
    .expect("write marker");
    route(
        &router,
        coordinator(repo.clone()),
        "project.open",
        json!({ "path": mount }),
    )
    .await
    .expect("a workspace mount opens its own project");

    // A directory with no marker and no relation to the project is still not this project.
    let stranger = root.join("stranger");
    std::fs::create_dir_all(&stranger).expect("stranger");
    assert!(
        route(
            &router,
            coordinator(repo.clone()),
            "project.open",
            json!({ "path": stranger }),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn workspace_at_rejects_ambiguous_nested_active_mounts() {
    let root = test_root();
    let (events, _receiver) = mpsc::unbounded_channel();
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    let overlap = root.join("overlap");
    let mut outer = host.next_workspace(WorkspaceName::new("main").expect("main"));
    outer.mount = Some(overlap.clone());
    let mut inner = host.next_workspace(WorkspaceName::new("inner").expect("inner"));
    inner.mount = Some(overlap.join("nested"));
    host.state.workspaces = vec![outer, inner];
    let runtime = ProjectRuntime::start(host).await.expect("runtime");
    let router = runtime.router();
    let repo = runtime.descriptor().repo_id.clone();

    let error = route(
        &router,
        coordinator(repo.clone()),
        "project.workspaceAt",
        json!({ "repoId": repo, "path": overlap.join("nested/src") }),
    )
    .await
    .expect_err("overlapping active mounts must be ambiguous");
    assert_eq!(error.code, ErrorCode::Conflict);
}

/// A coordinator reads the build volume a job of the workspace would be granted, fenced on the
/// incarnation it holds and refused once the workspace is detached, when no build link is read.
#[tokio::test]
async fn the_build_volume_answer_is_the_hosts_grant_for_one_attached_incarnation() {
    let root = test_root();
    let (runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    let created = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "built", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    let held = created["info"]["workspaceIncarnation"].clone();
    let ask = |incarnation: Value| json!({ "repoId": repo, "workspace": "built", "workspaceIncarnation": incarnation });

    let volume = route(
        &router,
        coordinator(repo.clone()),
        "workspace.buildVolume",
        ask(held.clone()),
    )
    .await
    .expect("build volume of the attached incarnation");
    let expected = runtime
        .descriptor()
        .storage
        .store()
        .join(".build")
        .join("built");
    assert_eq!(volume, json!({ "volume": expected }));

    let stale = route(
        &router,
        coordinator(repo.clone()),
        "workspace.buildVolume",
        ask(json!(incarnation(9_999))),
    )
    .await
    .expect_err("another incarnation is never answered for");
    assert_eq!(stale.code, ErrorCode::Conflict);
    assert!(stale.fence_source().is_some(), "{stale:?}");

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.detach",
        json!({ "repoId": repo, "workspace": "built" }),
    )
    .await
    .expect("detach");
    let detached = route(
        &router,
        coordinator(repo.clone()),
        "workspace.buildVolume",
        ask(held),
    )
    .await
    .expect_err("a detached checkout's link is not read");
    assert_eq!(detached.code, ErrorCode::Conflict);
    assert!(detached.fence_source().is_none(), "{detached:?}");
}

/// A worker handle minted on the coordinator's own connection holds its incarnation, and its
/// grants read is fenced on it although the connection is not a worker's: a stale incarnation is
/// refused, while a reference that holds none still reads by name.
#[tokio::test]
async fn a_grants_read_that_holds_an_incarnation_is_fenced_on_a_coordinator_connection() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    let created = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "fenced", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    let held = created["info"]["workspaceIncarnation"].clone();
    let read = |incarnation: Option<Value>| {
        let mut params = json!({ "repoId": repo, "workspace": "fenced" });
        if let Some(incarnation) = incarnation {
            params["workspaceIncarnation"] = incarnation;
        }
        route(
            &router,
            coordinator(repo.clone()),
            "workspace.grants",
            params,
        )
    };

    let current = read(Some(held)).await.expect("the held incarnation reads");
    assert_eq!(current, created["grants"]);
    let stale = read(Some(json!(incarnation(9_999))))
        .await
        .expect_err("another incarnation is never answered for");
    assert_eq!(stale.code, ErrorCode::Conflict);
    assert!(stale.fence_source().is_some(), "{stale:?}");
    let named = read(None)
        .await
        .expect("a reference that holds none reads by name");
    assert_eq!(named, created["grants"]);
}

#[tokio::test]
async fn labeled_checkpoint_is_pinned_and_projected_without_mount_inspection() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    let workspace = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "retry", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    let incarnation: WorkspaceIncarnation =
        serde_json::from_value(workspace["info"]["workspaceIncarnation"].clone())
            .expect("incarnation");
    let worker = ConnectionAuthority::Worker {
        repo_id: repo.clone(),
        workspace: WorkspaceName::new("retry").expect("name"),
        workspace_incarnation: incarnation.clone(),
    };
    let checkpoint = route(
        &router,
        worker,
        "worker.checkpoint",
        json!({
            "repoId": repo,
            "workspace": "retry",
            "workspaceIncarnation": incarnation,
            "options": CheckpointOptions { label: Some("handoff".into()), keep: false }
        }),
    )
    .await
    .expect("checkpoint");
    assert_eq!(checkpoint["label"], "handoff");

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.detach",
        json!({ "repoId": repo, "workspace": "retry" }),
    )
    .await
    .expect("detach");
    let info = route(
        &router,
        coordinator(repo.clone()),
        "workspace.info",
        json!({ "repoId": repo, "workspace": "retry" }),
    )
    .await
    .expect("detached info");
    assert_eq!(
        info["checkpoints"],
        json!([{ "label": "handoff", "revision": 2, "pinned": true }])
    );
}

#[tokio::test]
async fn checkpoint_quota_charges_active_plus_all_workspace_checkpoints_at_exact_boundary() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    let main = adopt(&router, &repo).await;
    let other = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "other", "options": CreateOptions::default() }),
    )
    .await
    .expect("create other");
    let main_incarnation: WorkspaceIncarnation =
        serde_json::from_value(main["info"]["workspaceIncarnation"].clone())
            .expect("main incarnation");
    let other_incarnation: WorkspaceIncarnation =
        serde_json::from_value(other["info"]["workspaceIncarnation"].clone())
            .expect("other incarnation");

    for (workspace, quota) in [
        (
            "main",
            CheckpointQuota {
                max_count: 3,
                max_bytes: 30,
            },
        ),
        (
            "other",
            CheckpointQuota {
                max_count: 1,
                max_bytes: 10,
            },
        ),
    ] {
        route(
            &router,
            coordinator(repo.clone()),
            "coordinator.setCheckpointQuota",
            json!({ "repoId": repo, "workspace": workspace, "quota": quota }),
        )
        .await
        .expect("set quota");
    }

    checkpoint_as_worker(
        &router,
        &repo,
        "main",
        &main_incarnation,
        CheckpointOptions {
            label: Some("pinned".into()),
            keep: false,
        },
    )
    .await
    .expect("pinned checkpoint");
    checkpoint_as_worker(
        &router,
        &repo,
        "main",
        &main_incarnation,
        CheckpointOptions::default(),
    )
    .await
    .expect("automatic checkpoint");
    checkpoint_as_worker(
        &router,
        &repo,
        "other",
        &other_incarnation,
        CheckpointOptions::default(),
    )
    .await
    .expect("other workspace exact boundary");
    checkpoint_as_worker(
        &router,
        &repo,
        "main",
        &main_incarnation,
        CheckpointOptions::default(),
    )
    .await
    .expect("main exact count and byte boundary ignores other workspace");

    let exact = route(
        &router,
        coordinator(repo.clone()),
        "workspace.info",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("main info");
    assert_eq!(
        exact["checkpoints"].as_array().expect("checkpoints").len(),
        3
    );
    assert_eq!(exact["checkpoints"][0]["pinned"], true);
    assert_eq!(exact["checkpoints"][1]["pinned"], false);

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.setCheckpointQuota",
        json!({
            "repoId": repo,
            "workspace": "main",
            "quota": CheckpointQuota { max_count: 4, max_bytes: 39 }
        }),
    )
    .await
    .expect("lower byte headroom");
    let before = std::fs::read(root.join("durable.json")).expect("durable before denial");
    let error = checkpoint_as_worker(
        &router,
        &repo,
        "main",
        &main_incarnation,
        CheckpointOptions::default(),
    )
    .await
    .expect_err("one byte over quota");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(
        std::fs::read(root.join("durable.json")).expect("durable after denial"),
        before,
        "quota denial must publish no checkpoint fact or metadata"
    );
}

#[tokio::test]
async fn same_name_creates_serialize_to_one_success_and_one_conflict() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    let params =
        json!({ "repoId": repo, "workspace": "same", "options": CreateOptions::default() });
    let left = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        params.clone(),
    );
    let right = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        params,
    );
    let (left, right) = tokio::join!(left, right);
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.err().or_else(|| right.err()).expect("one conflict");
    assert_eq!(error.code, ErrorCode::Conflict);
}

#[tokio::test]
async fn initializer_failure_never_publishes_or_lists_workspace() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, true, false, Vec::new()).await;
    adopt(&router, &repo).await;
    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "hidden", "options": CreateOptions::default() }),
    )
    .await
    .expect_err("injected failure");
    assert_eq!(error.code, ErrorCode::Internal);
    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list");
    assert_eq!(listed.as_array().expect("array").len(), 1);
    let mut saw_initialize = false;
    let mut saw_publish = false;
    while let Ok(event) = events.try_recv() {
        saw_initialize |= event == Event::Initialize(WorkspaceName::new("hidden").expect("name"));
        saw_publish |= event == Event::Publish(WorkspaceName::new("hidden").expect("name"));
    }
    assert!(saw_initialize);
    assert!(!saw_publish);
}

#[tokio::test]
async fn restore_stays_pending_after_fence_failure_and_retry_activates_after_evidence() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, true, Vec::new()).await;
    adopt(&router, &repo).await;
    let before = route(
        &router,
        coordinator(repo.clone()),
        "project.workspace",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("before");
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.restore",
        json!({ "repoId": repo, "workspace": "main", "label": "checkpoint-1" }),
    )
    .await
    .expect_err("fence failure");
    let pending_view = route(
        &router,
        coordinator(repo.clone()),
        "project.workspace",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("pending remains hidden");
    assert_eq!(
        pending_view["info"]["workspaceIncarnation"],
        before["info"]["workspaceIncarnation"]
    );
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.restore",
        json!({ "repoId": repo, "workspace": "main", "label": "checkpoint-1" }),
    )
    .await
    .expect("retry");
    let events: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
    let evidence = events
        .iter()
        .position(|event| matches!(event, Event::RestoreEvidence(_)))
        .expect("evidence event");
    let activate = events
        .iter()
        .position(|event| matches!(event, Event::RestoreActivate(_)))
        .expect("activation event");
    assert!(evidence < activate);
}

#[tokio::test]
async fn stale_incarnation_and_revision_reject_before_mutation() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    let main = adopt(&router, &repo).await;
    while events.try_recv().is_ok() {}
    let stale = incarnation(999);
    let error = route(
        &router,
        ConnectionAuthority::Worker {
            repo_id: repo.clone(),
            workspace: WorkspaceName::new("main").expect("main"),
            workspace_incarnation: stale.clone(),
        },
        "worker.checkpoint",
        json!({
            "repoId": repo,
            "workspace": "main",
            "workspaceIncarnation": stale,
            "options": CheckpointOptions::default()
        }),
    )
    .await
    .expect_err("stale incarnation");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(events.recv().await, Some(Event::SnapshotBatch));
    assert!(events.try_recv().is_err());

    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.grant",
        json!({
            "repoId": repo,
            "workspace": "main",
            "delta": GrantDelta { expected_revision: Some(999), ..GrantDelta::default() }
        }),
    )
    .await
    .expect_err("stale revision");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(main["grants"]["revision"].is_number());
}

#[tokio::test]
async fn remove_stops_supervisor_before_retirement() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "gone", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    while events.try_recv().is_ok() {}
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({ "repoId": repo, "workspace": "gone", "options": RemoveOptions::default() }),
    )
    .await
    .expect("remove");
    assert_eq!(
        events.recv().await,
        Some(Event::GitSafety(WorkspaceName::new("gone").expect("name")))
    );
    assert_eq!(
        events.recv().await,
        Some(Event::Stop(WorkspaceName::new("gone").expect("name")))
    );
    assert_eq!(
        events.recv().await,
        Some(Event::Retire(WorkspaceName::new("gone").expect("name")))
    );
}

/// Dirt is `--force`'s business and nothing else's: it must not authorize a commit loss.
#[tokio::test]
async fn session_removal_refuses_dirty_work_until_forced() {
    let root = test_root();
    let name = WorkspaceName::new("unsafe").expect("name");
    let mut removal = FakeRemoval::default();
    removal.dirty.insert(name.clone());
    let (_runtime, router, repo, mut events) =
        start_with_removal(&root, false, false, Vec::new(), removal).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "unsafe", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    while events.try_recv().is_ok() {}

    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({ "repoId": repo, "workspace": "unsafe", "options": RemoveOptions::default() }),
    )
    .await
    .expect_err("dirty work must be refused");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(events.recv().await, Some(Event::GitSafety(name.clone())));
    assert!(
        events.try_recv().is_err(),
        "safe refusal must not stop or retire"
    );

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({
            "repoId": repo,
            "workspace": "unsafe",
            "options": RemoveOptions { force: true, restore: false, abandon: false }
        }),
    )
    .await
    .expect("force overrides transient state");
    assert_eq!(events.recv().await, Some(Event::GitSafety(name.clone())));
    assert_eq!(events.recv().await, Some(Event::Stop(name.clone())));
    assert_eq!(events.recv().await, Some(Event::Retire(name)));
}

/// The incident this gate exists for: a scripted `--force` removal of a workspace whose commits
/// main does not contain. `--force` must not get past it; only `--abandon` may, and it leaves a
/// bundle and a report behind.
#[tokio::test]
async fn unlanded_session_removal_survives_force_and_only_abandon_destroys_it() {
    let root = test_root();
    let name = WorkspaceName::new("unsafe").expect("name");
    let mut removal = FakeRemoval::default();
    removal.unlanded.insert(name.clone());
    let (_runtime, router, repo, mut events) =
        start_with_removal(&root, false, false, Vec::new(), removal).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "unsafe", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    while events.try_recv().is_ok() {}

    for options in [
        RemoveOptions::default(),
        RemoveOptions {
            force: true,
            restore: false,
            abandon: false,
        },
    ] {
        let error = route(
            &router,
            coordinator(repo.clone()),
            "coordinator.destroy",
            json!({ "repoId": repo, "workspace": "unsafe", "options": options }),
        )
        .await
        .expect_err("unlanded commits must survive both plain and forced removal");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(
            !error.message.contains("--force")
                && !error.message.contains("--abandon")
                && !error.hint.contains("--force")
                && !error.hint.contains("--abandon"),
            "a refusal must not teach the flag that overrides it: {error:?}"
        );
        assert_eq!(events.recv().await, Some(Event::GitSafety(name.clone())));
        assert!(
            events.try_recv().is_err(),
            "a landed-ancestry refusal must not stop or retire"
        );
    }

    let report: RemoveReport = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({
            "repoId": repo,
            "workspace": "unsafe",
            "options": RemoveOptions { force: false, restore: false, abandon: true }
        }),
    )
    .await
    .map(|value| serde_json::from_value(value).expect("removal report"))
    .expect("abandon is the sole authorization for unlanded commits");
    let abandoned = report.abandoned.expect("abandonment must be reported");
    assert_eq!(abandoned.target_branch, "main");
    assert_eq!(abandoned.unlanded_commits, 3);
    assert!(
        abandoned
            .bundle
            .to_string_lossy()
            .ends_with(&format!("unsafe-{}.bundle", "4".repeat(40))),
        "the bundle path names the workspace and the abandoned tip: {abandoned:?}"
    );
    assert_eq!(events.recv().await, Some(Event::GitSafety(name.clone())));
    assert_eq!(events.recv().await, Some(Event::Stop(name.clone())));
    assert_eq!(events.recv().await, Some(Event::Bundle(name.clone())));
    assert_eq!(events.recv().await, Some(Event::Retire(name)));
}

#[tokio::test]
async fn head_change_at_retirement_fence_preserves_the_workspace() {
    let root = test_root();
    let name = WorkspaceName::new("moving").expect("name");
    let mut removal = FakeRemoval::default();
    removal.change_head_at_fence.insert(name.clone());
    let (_runtime, router, repo, mut events) =
        start_with_removal(&root, false, false, Vec::new(), removal).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "moving", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    while events.try_recv().is_ok() {}

    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({ "repoId": repo, "workspace": "moving", "options": RemoveOptions::default() }),
    )
    .await
    .expect_err("changed HEAD must fence retirement");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(events.recv().await, Some(Event::GitSafety(name.clone())));
    assert_eq!(events.recv().await, Some(Event::Stop(name)));
    assert!(events.try_recv().is_err(), "workspace was not retired");
}

#[tokio::test]
async fn plain_main_removal_requires_force_and_still_requires_clean_git() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    while events.try_recv().is_ok() {}
    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({ "repoId": repo, "workspace": "main", "options": RemoveOptions::default() }),
    )
    .await
    .expect_err("main requires force");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(
        events.recv().await,
        Some(Event::GitSafety(WorkspaceName::new("main").expect("name")))
    );
    assert!(events.try_recv().is_err());

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({
            "repoId": repo,
            "workspace": "main",
            "options": RemoveOptions { force: true, restore: false, abandon: false }
        }),
    )
    .await
    .expect("clean forced main removal");

    let dirty_root = test_root();
    let main = WorkspaceName::new("main").expect("main");
    let mut removal = FakeRemoval::default();
    removal.dirty.insert(main.clone());
    let (_runtime, router, repo, mut dirty_events) =
        start_with_removal(&dirty_root, false, false, Vec::new(), removal).await;
    adopt(&router, &repo).await;
    while dirty_events.try_recv().is_ok() {}
    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({
            "repoId": repo,
            "workspace": "main",
            "options": RemoveOptions { force: true, restore: false, abandon: false }
        }),
    )
    .await
    .expect_err("force does not waive main cleanliness");
    assert_eq!(error.code, ErrorCode::Conflict);
    assert_eq!(dirty_events.recv().await, Some(Event::GitSafety(main)));
    assert!(dirty_events.try_recv().is_err());
}

#[tokio::test]
async fn main_restore_detaches_swaps_exact_checkout_and_then_retires() {
    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    while events.try_recv().is_ok() {}

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({
            "repoId": repo,
            "workspace": "main",
            "options": RemoveOptions { force: false, restore: true, abandon: false }
        }),
    )
    .await
    .expect("adoption rollback");
    let main = WorkspaceName::new("main").expect("main");
    assert_eq!(events.recv().await, Some(Event::GitSafety(main.clone())));
    assert_eq!(events.recv().await, Some(Event::Stop(main.clone())));
    assert_eq!(events.recv().await, Some(Event::Detach(main.clone())));
    assert_eq!(
        events.recv().await,
        Some(Event::AtomicCheckoutRestore(root.join("checkout")))
    );
    assert_eq!(events.recv().await, Some(Event::Retire(main)));

    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list after rollback");
    assert!(listed.as_array().expect("workspaces").is_empty());
}

#[tokio::test]
async fn main_restore_missing_tree_collision_and_force_ambiguity_mutate_nothing() {
    for refusal in ["missing", "collision"] {
        let root = test_root();
        let removal = FakeRemoval {
            pre_cowshed_present: refusal != "missing",
            restore_collision: refusal == "collision",
            ..FakeRemoval::default()
        };
        let (_runtime, router, repo, mut events) =
            start_with_removal(&root, false, false, Vec::new(), removal).await;
        adopt(&router, &repo).await;
        while events.try_recv().is_ok() {}
        let error = route(
            &router,
            coordinator(repo.clone()),
            "coordinator.destroy",
            json!({
                "repoId": repo,
                "workspace": "main",
                "options": RemoveOptions { force: false, restore: true, abandon: false }
            }),
        )
        .await
        .expect_err("unsafe restore fact");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert_eq!(
            events.recv().await,
            Some(Event::GitSafety(WorkspaceName::new("main").expect("main")))
        );
        assert!(events.try_recv().is_err());
    }

    let root = test_root();
    let (_runtime, router, repo, mut events) = start(&root, false, false, Vec::new()).await;
    adopt(&router, &repo).await;
    while events.try_recv().is_ok() {}
    let error = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({
            "repoId": repo,
            "workspace": "main",
            "options": RemoveOptions { force: true, restore: true, abandon: false }
        }),
    )
    .await
    .expect_err("ambiguous authority");
    assert_eq!(error.code, ErrorCode::Usage);
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn main_restore_retries_after_each_detach_and_swap_crash_boundary() {
    for boundary in ["detach", "swap"] {
        let root = test_root();
        let removal = FakeRemoval {
            fail_after_detach_once: boundary == "detach",
            fail_after_swap_once: boundary == "swap",
            ..FakeRemoval::default()
        };
        let (_runtime, router, repo, mut events) =
            start_with_removal(&root, false, false, Vec::new(), removal).await;
        adopt(&router, &repo).await;
        while events.try_recv().is_ok() {}
        let options = RemoveOptions {
            force: false,
            restore: true,
            abandon: false,
        };
        let error = route(
            &router,
            coordinator(repo.clone()),
            "coordinator.destroy",
            json!({ "repoId": repo, "workspace": "main", "options": options }),
        )
        .await
        .expect_err("injected crash");
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        while events.try_recv().is_ok() {}

        route(
            &router,
            coordinator(repo.clone()),
            "coordinator.destroy",
            json!({ "repoId": repo, "workspace": "main", "options": options }),
        )
        .await
        .expect("retry completes rollback");
        let retried = std::iter::from_fn(|| events.try_recv().ok()).collect::<Vec<_>>();
        assert!(
            retried
                .iter()
                .any(|event| matches!(event, Event::Retire(_)))
        );
        let swap_count = retried
            .iter()
            .filter(|event| matches!(event, Event::AtomicCheckoutRestore(_)))
            .count();
        assert_eq!(
            swap_count,
            usize::from(boundary == "detach"),
            "a completed atomic swap is never repeated"
        );
    }
}

#[tokio::test]
async fn destroy_returns_after_logical_retirement_before_background_reclaim() {
    let root = test_root();
    let (events, mut receiver) = mpsc::unbounded_channel();
    let gate = Arc::new(Notify::new());
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.reclaim_gate = Some(Arc::clone(&gate));
    let runtime = ProjectRuntime::start(host).await.expect("runtime");
    let router = runtime.router();
    let repo = runtime.descriptor().repo_id.clone();
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "retired", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");
    while receiver.try_recv().is_ok() {}

    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.destroy",
        json!({ "repoId": repo, "workspace": "retired", "options": RemoveOptions::default() }),
    )
    .await
    .expect("logical retirement");
    let name = WorkspaceName::new("retired").expect("name");
    assert_eq!(receiver.recv().await, Some(Event::GitSafety(name.clone())));
    assert_eq!(receiver.recv().await, Some(Event::Stop(name.clone())));
    assert_eq!(receiver.recv().await, Some(Event::Retire(name.clone())));
    assert!(
        receiver.try_recv().is_err(),
        "reclaim ran before destroy returned"
    );

    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list after retirement");
    assert!(
        listed
            .as_array()
            .expect("workspaces")
            .iter()
            .all(|workspace| workspace["info"]["workspace"] != "retired")
    );
    assert_eq!(receiver.recv().await, Some(Event::SnapshotBatch));
    gate.notify_one();
    assert_eq!(receiver.recv().await, Some(Event::Reclaim(name)));
}

#[tokio::test]
async fn doctor_aggregates_binding_metadata_mount_pending_and_integrity_findings() {
    let root = test_root();
    let findings = [
        ("binding", FindingSeverity::Error),
        ("metadata", FindingSeverity::Warning),
        ("mount", FindingSeverity::Error),
        ("pending", FindingSeverity::Warning),
        ("integrity", FindingSeverity::Error),
    ]
    .into_iter()
    .map(|(code, severity)| Finding {
        code: code.into(),
        severity,
        message: format!("{code} finding"),
        hint: "repair fixture".into(),
        path: None,
    })
    .collect();
    let (_runtime, router, repo, _events) = start(&root, false, false, findings).await;
    let report = route(
        &router,
        coordinator(repo.clone()),
        "coordinator.doctor",
        json!({ "repoId": repo }),
    )
    .await
    .expect("doctor");
    assert_eq!(report["healthy"], false);
    let codes: Vec<_> = report["findings"]
        .as_array()
        .expect("findings")
        .iter()
        .map(|finding| finding["code"].as_str().expect("code"))
        .collect();
    assert_eq!(
        codes,
        ["binding", "metadata", "mount", "pending", "integrity"]
    );
}

#[tokio::test]
async fn crash_reopen_recovers_published_and_pending_state() {
    let root = test_root();
    let (runtime, router, repo, _events) = start(&root, false, true, Vec::new()).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.restore",
        json!({ "repoId": repo, "workspace": "main", "label": "checkpoint-1" }),
    )
    .await
    .expect_err("fence fails");
    drop(router);
    runtime.shutdown().await.expect("shutdown");

    let (events, mut receiver) = mpsc::unbounded_channel();
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.load().expect("load pending");
    host.state.pending_has_evidence = true;
    host.persist().expect("persist evidence");
    let runtime = ProjectRuntime::start(host).await.expect("reopen");
    assert!(matches!(
        receiver.recv().await,
        Some(Event::RestoreActivate(_))
    ));
    let router = runtime.router();
    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list after reopen");
    assert_eq!(listed.as_array().expect("array").len(), 1);
    assert_eq!(
        PathBuf::from(
            listed[0]["info"]["mount"]
                .as_str()
                .expect("recovered main mount")
        ),
        root.join("checkout")
    );
}

#[tokio::test]
async fn controller_startup_replans_after_cross_runtime_lifecycle_conflict() {
    let root = test_root();
    let race = Arc::new(RecoveryRace {
        fact: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        first_read: Notify::new(),
        mutated: Notify::new(),
    });
    let (events, _receiver) = mpsc::unbounded_channel();
    let mut starting = FakeHost::new(&root, events.clone(), false, false, Vec::new());
    starting.recovery_behavior = RecoveryBehavior::Contended(Arc::clone(&race));
    let startup = tokio::spawn(ProjectRuntime::start(starting));

    race.first_read.notified().await;
    let mut mutating = FakeHost::new(&root, events, false, false, Vec::new());
    mutating.recovery_behavior = RecoveryBehavior::Mutate(Arc::clone(&race));
    let concurrent = ProjectRuntime::start(mutating)
        .await
        .expect("concurrent lifecycle runtime starts");
    concurrent.shutdown().await.expect("stop mutating runtime");

    let runtime = startup
        .await
        .expect("startup task joins")
        .expect("controller reaches ready after refreshing its plan");
    assert_eq!(race.attempts.load(Ordering::SeqCst), 2);
    runtime.shutdown().await.expect("stop recovered runtime");
}

#[tokio::test]
async fn controller_startup_does_not_retry_non_lifecycle_failures() {
    let root = test_root();
    let attempts = Arc::new(AtomicUsize::new(0));
    let (events, _receiver) = mpsc::unbounded_channel();
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.recovery_behavior = RecoveryBehavior::ImmediateFailure(Arc::clone(&attempts));

    let error = match ProjectRuntime::start(host).await {
        Ok(_) => panic!("non-conflict startup failure must remain fatal"),
        Err(error) => error,
    };
    assert_eq!(error.message, "fixture storage is unreadable");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn controller_startup_names_exhausted_lifecycle_fact() {
    let root = test_root();
    let attempts = Arc::new(AtomicUsize::new(0));
    let (events, _receiver) = mpsc::unbounded_channel();
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.recovery_behavior = RecoveryBehavior::AlwaysLifecycleConflict(Arc::clone(&attempts));

    let error = match ProjectRuntime::start(host).await {
        Ok(_) => panic!("permanent contention must exhaust its bounded attempts"),
        Err(error) => error,
    };
    assert_eq!(
        error.message,
        "controller startup exhausted 8 lifecycle attempts; authoritative fact 1 kept changing"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 8);
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn production_open_modes_return_typed_environment_error_off_macos() {
    let adopt_error = match ProjectRuntime::open_for_adopt("/tmp/project", None).await {
        Ok(_) => panic!("non-macOS adopt open must fail"),
        Err(error) => error,
    };
    assert_eq!(adopt_error.code, ErrorCode::EnvironmentMissing);

    let existing_error = match ProjectRuntime::open_existing(
        "/tmp/project",
        cowshed_core::runtime::RecoveryScope::Store,
    )
    .await
    {
        Ok(_) => panic!("non-macOS existing-only open must fail"),
        Err(error) => error,
    };
    assert_eq!(existing_error.code, ErrorCode::EnvironmentMissing);
}

async fn remove_project(
    router: &RouterHandle,
    repo: &RepoId,
    options: RemoveProjectOptions,
) -> Result<RemoveProjectReport> {
    route(
        router,
        coordinator(repo.clone()),
        "coordinator.removeProject",
        json!({ "repoId": repo, "options": options }),
    )
    .await
    .map(|value| serde_json::from_value(value).expect("project removal report"))
}

/// Removing a project retires every session, listed or never published, then waits for its own
/// background reclaims before collecting: a collection that raced them would plan against images
/// still being reclaimed and find its plan stale.
#[tokio::test]
async fn removing_a_project_settles_its_reclaims_before_collecting_then_restores_main() {
    let root = test_root();
    let (events, mut receiver) = mpsc::unbounded_channel();
    let gate = Arc::new(Notify::new());
    let mut host = FakeHost::new(&root, events, false, false, Vec::new());
    host.reclaim_gate = Some(Arc::clone(&gate));
    let unsafe_name = WorkspaceName::new("unsafe").expect("name");
    let stray = WorkspaceName::new("stray").expect("name");
    host.removal.unlanded.insert(unsafe_name.clone());
    host.removal.unpublished.insert(stray.clone());
    let runtime = ProjectRuntime::start(host).await.expect("runtime");
    let router = runtime.router();
    let repo = runtime.descriptor().repo_id.clone();
    adopt(&router, &repo).await;
    for name in ["landed", "unsafe"] {
        route(
            &router,
            coordinator(repo.clone()),
            "coordinator.create",
            json!({ "repoId": repo, "workspace": name, "options": CreateOptions::default() }),
        )
        .await
        .expect("create");
    }
    while receiver.try_recv().is_ok() {}

    let removal = tokio::spawn({
        let router = router.clone();
        let repo = repo.clone();
        async move {
            remove_project(
                &router,
                &repo,
                RemoveProjectOptions {
                    force: false,
                    abandon: true,
                },
            )
            .await
        }
    });
    let landed = WorkspaceName::new("landed").expect("name");
    let mut retired = Vec::new();
    loop {
        match receiver.recv().await.expect("event") {
            Event::Retire(name) => retired.push(name),
            Event::SettleReclaims => break,
            Event::Gc => panic!("collected before settling the reclaims it started"),
            _ => {}
        }
    }
    assert_eq!(retired, [landed.clone(), stray, unsafe_name.clone()]);
    // Two image reclaims are held at the gate; the collection must wait for both.
    gate.notify_one();
    gate.notify_one();
    let mut reclaimed = Vec::new();
    loop {
        match receiver.recv().await.expect("event") {
            Event::Reclaim(name) => reclaimed.push(name),
            Event::Gc => break,
            _ => {}
        }
    }
    reclaimed.sort();
    assert_eq!(reclaimed, [landed.clone(), unsafe_name.clone()]);
    gate.notify_one();

    let report = removal.await.expect("join").expect("project removal");
    let removed: Vec<_> = report
        .removed
        .iter()
        .map(|removed| removed.workspace.as_str())
        .collect();
    assert_eq!(removed, ["landed", "stray", "unsafe"]);
    let abandoned = report.removed[2]
        .report
        .abandoned
        .as_ref()
        .expect("unsafe abandoned");
    assert_eq!(
        report.deleted_bundles,
        std::slice::from_ref(&abandoned.bundle)
    );
    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list after removal");
    assert!(
        listed.as_array().expect("workspaces").is_empty(),
        "main was restored: {listed}"
    );
}

/// A collection that finds its plan stale is a typed retryable refusal, and the same call
/// finishes the removal: nothing it already did is repeated or refused.
#[tokio::test]
async fn a_stale_collection_is_a_retryable_refusal_and_the_retry_finishes_the_removal() {
    let removal = FakeRemoval {
        gc_stale_once: true,
        ..FakeRemoval::default()
    };
    let root = test_root();
    let (_runtime, router, repo, _events) =
        start_with_removal(&root, false, false, Vec::new(), removal).await;
    adopt(&router, &repo).await;
    route(
        &router,
        coordinator(repo.clone()),
        "coordinator.create",
        json!({ "repoId": repo, "workspace": "landed", "options": CreateOptions::default() }),
    )
    .await
    .expect("create");

    let stale = remove_project(&router, &repo, RemoveProjectOptions::default())
        .await
        .expect_err("the first collection is stale");
    assert_eq!(stale.code, ErrorCode::Conflict);
    assert_eq!(stale.retry_source(), Some(Retry::GcPlanStale));
    let listed = route(
        &router,
        coordinator(repo.clone()),
        "project.list",
        json!({ "repoId": repo }),
    )
    .await
    .expect("list");
    assert_eq!(
        listed.as_array().expect("workspaces").len(),
        1,
        "main is not restored yet"
    );

    let report = remove_project(&router, &repo, RemoveProjectOptions::default())
        .await
        .expect("the retry finishes");
    assert!(
        report.removed.is_empty(),
        "the session went on the first call"
    );
    assert!(
        report.deleted_bundles.is_empty(),
        "no bundles without abandon"
    );
}
