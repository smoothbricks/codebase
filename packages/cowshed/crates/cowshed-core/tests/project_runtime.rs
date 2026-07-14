use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_core::api::dto::{
    AdoptOptions, AttachOptions, CheckpointInfo, CheckpointOptions, CheckpointQuota,
    CheckpointResult, CommandArg, CreateOptions, DoctorReport, EnsureAction, EnsureReport, Finding,
    FindingSeverity, GcOptions, GcReport, GitOid, GrantDelta, GrantSet, ImageFormat, JobId,
    JobInfo, LandOptions, LandReport, MirrorInfo, PortBlock, PushOptions, PushReport,
    RebaseOptions, RemoveOptions, WorkspaceInfo, WorkspaceState,
};
use cowshed_core::api::server::{ConnectionAuthority, RouterHandle};
use cowshed_core::metadata::{WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use cowshed_core::repository::{BoundIdentity, RepoId, RepositoryBinding};
use cowshed_core::runtime::{
    ProjectDescriptor, ProjectRuntime, ProjectRuntimeHost, RuntimeJobStream, RuntimeLogChunk,
    WorkspaceSnapshot,
};
use cowshed_core::{CowshedError, ErrorCode, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};
use url::Url;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

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
    Exec(Vec<Vec<u8>>),
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
                binding,
                git_root: root.join("checkout"),
                store_root: root.join("store"),
            },
            state_path: root.join("durable.json"),
            state: DurableState::default(),
            events,
            fail_create_initializer,
            fail_restore_fence_once,
            doctor_findings,
            reclaim_gate: None,
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
                image_format: ImageFormat::Asif,
                mount: workspace.mount.clone().unwrap_or_else(|| {
                    self.descriptor
                        .store_root
                        .join("mnt")
                        .join(workspace.name.as_str())
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
                    49_152 + u16::try_from((next - 1) * 16).expect("test port"),
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

#[async_trait]
impl ProjectRuntimeHost for FakeHost {
    fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    async fn recover(&mut self) -> Result<()> {
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
                let destination = self.descriptor.store_root.join("quarantine").join(path);
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
        let prepared = self.next_workspace(workspace.clone());
        self.events.send(Event::Initialize(workspace.clone())).ok();
        if std::mem::take(&mut self.fail_create_initializer) {
            return Err(CowshedError::internal(
                "injected create initializer failure",
            ));
        }
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

    async fn ensure(&mut self, workspace: WorkspaceName) -> Result<EnsureReport> {
        let current = self.workspace(&workspace)?;
        let mount = self.snapshot(current).info.mount;
        Ok(EnsureReport {
            workspace,
            go_env: mount.join(".cowshed/cache/go/env"),
            workspace_token: mount.join(".cowshed/token"),
            port_block: current.grants.port_block,
            mount,
            action: EnsureAction::AlreadyMounted,
        })
    }

    async fn attach(&mut self, workspace: WorkspaceName, _options: AttachOptions) -> Result<()> {
        self.workspace_mut(&workspace)?.attached = true;
        self.persist()
    }

    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()> {
        self.workspace_mut(&workspace)?.attached = false;
        self.persist()
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

    async fn remove(&mut self, workspace: WorkspaceName, _options: RemoveOptions) -> Result<()> {
        if workspace.is_main() {
            return Err(CowshedError::conflict(
                "main cannot be removed",
                "remove only session workspaces",
            ));
        }
        let index = self
            .state
            .workspaces
            .iter()
            .position(|current| current.name == workspace)
            .ok_or_else(|| CowshedError::not_found("workspace missing", "list workspaces"))?;
        self.events.send(Event::Stop(workspace.clone())).ok();
        self.state.workspaces.remove(index);
        self.persist()?;
        self.events.send(Event::Retire(workspace.clone())).ok();
        let events = self.events.clone();
        let reclaim_gate = self.reclaim_gate.clone();
        std::mem::drop(tokio::spawn(async move {
            if let Some(gate) = reclaim_gate {
                gate.notified().await;
            }
            events.send(Event::Reclaim(workspace)).ok();
        }));
        Ok(())
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        Ok(GcReport {
            examined: u64::try_from(self.state.workspaces.len()).expect("test length"),
            reclaimed: 0,
            retained_pinned: 0,
            freed_bytes: 0,
            dry_run: options.dry_run,
            candidates: Vec::new(),
        })
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

    async fn assign_slot(&mut self, workspace: WorkspaceName, slot: u32) -> Result<()> {
        let current = self.workspace_mut(&workspace)?;
        let base = u16::try_from(slot.checked_mul(16).ok_or_else(|| {
            CowshedError::usage("slot overflows port space", "choose a smaller slot")
        })?)
        .map_err(|_| CowshedError::usage("slot overflows port space", "choose a smaller slot"))?;
        current.grants.port_block = Some(
            PortBlock::new(base, 16)
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

    async fn rebase(&mut self, workspace: WorkspaceName, options: RebaseOptions) -> Result<GitOid> {
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
        GitOid::new("1111111111111111111111111111111111111111")
            .map_err(|error| CowshedError::internal(error.to_string()))
    }

    async fn land(
        &mut self,
        workspace: WorkspaceName,
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
            mirror: self.descriptor.store_root.join("mirror.git"),
        })
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        Ok(DoctorReport {
            healthy: !self
                .doctor_findings
                .iter()
                .any(|finding| finding.severity == FindingSeverity::Error),
            findings: self.doctor_findings.clone(),
        })
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
        self.events
            .send(Event::Exec(
                request
                    .argv
                    .iter()
                    .map(|argument| argument.as_os_str().as_bytes().to_vec())
                    .collect(),
            ))
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
        _job: JobId,
    ) -> Result<JobInfo> {
        self.require_incarnation(&workspace, &incarnation)?;
        Err(Self::worker_unavailable())
    }

    async fn wait_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _job: JobId,
    ) -> Result<JobInfo> {
        self.require_incarnation(&workspace, &incarnation)?;
        Err(Self::worker_unavailable())
    }

    async fn kill_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        _job: JobId,
    ) -> Result<()> {
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
        _job: JobId,
        _stream: RuntimeJobStream,
        offset: u64,
        _follow: bool,
    ) -> Result<RuntimeLogChunk> {
        self.require_incarnation(&workspace, &incarnation)?;
        Ok(RuntimeLogChunk {
            bytes: Bytes::new(),
            next_offset: offset,
            eof: true,
        })
    }
}

fn test_root() -> PathBuf {
    let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "cowshed-project-runtime-{}-{id}",
        std::process::id()
    ));
    match std::fs::remove_dir_all(&root) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("remove stale test root: {error}"),
    }
    std::fs::create_dir_all(root.join("checkout")).expect("create test root");
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
        .route(authority, method.to_owned(), params, None)
        .await?;
    let (value, binary) = response.into_parts();
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
    let (events, receiver) = mpsc::unbounded_channel();
    let host = FakeHost::new(root, events, fail_create, fail_restore, findings);
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
    let Some(Event::Exec(decoded)) = events.recv().await else {
        panic!("missing exec event");
    };
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
            "job.logs".to_owned(),
            json!({
                "repoId": repo,
                "workspace": "main",
                "workspaceIncarnation": incarnation,
                "jobId": 1,
                "stream": "stdout",
                "follow": false,
                "offset": 7
            }),
            None,
        )
        .await
        .expect("log route");
    let (metadata, bytes) = response.into_parts();
    assert_eq!(metadata, json!({ "eof": true, "nextOffset": 7 }));
    assert_eq!(bytes, Some(Bytes::new()));
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
    assert!(
        task["info"]["mount"]
            .as_str()
            .expect("mount")
            .ends_with("/task")
    );
    assert_eq!(listed.as_array().expect("array").len(), 2);
}

#[tokio::test]
async fn workspace_at_uses_active_mount_facts_and_ensure_returns_authoritative_env_fields() {
    let root = test_root();
    let (_runtime, router, repo, _events) = start(&root, false, false, Vec::new()).await;
    let adopted = adopt(&router, &repo).await;
    let mount = PathBuf::from(adopted["info"]["mount"].as_str().expect("mount"));
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

    let ensured = route(
        &router,
        coordinator(repo.clone()),
        "workspace.ensure",
        json!({ "repoId": repo, "workspace": "main" }),
    )
    .await
    .expect("ensure");
    assert_eq!(
        ensured["goEnv"],
        mount
            .join(".cowshed/cache/go/env")
            .to_string_lossy()
            .as_ref()
    );
    assert_eq!(
        ensured["workspaceToken"],
        mount.join(".cowshed/token").to_string_lossy().as_ref()
    );
    assert_eq!(ensured["portBlock"], json!({"base": 49152, "size": 16}));

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
        Some(Event::Stop(WorkspaceName::new("gone").expect("name")))
    );
    assert_eq!(
        events.recv().await,
        Some(Event::Retire(WorkspaceName::new("gone").expect("name")))
    );
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
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn production_open_modes_return_typed_environment_error_off_macos() {
    let adopt_error = match ProjectRuntime::open_for_adopt("/tmp/project").await {
        Ok(_) => panic!("non-macOS adopt open must fail"),
        Err(error) => error,
    };
    assert_eq!(adopt_error.code, ErrorCode::EnvironmentMissing);

    let existing_error = match ProjectRuntime::open_existing("/tmp/project").await {
        Ok(_) => panic!("non-macOS existing-only open must fail"),
        Err(error) => error,
    };
    assert_eq!(existing_error.code, ErrorCode::EnvironmentMissing);
}
