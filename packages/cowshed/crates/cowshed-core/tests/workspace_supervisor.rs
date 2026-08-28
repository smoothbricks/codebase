use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_core::api::{
    CONTROLLER_COMMITMENT_VERSION, CommandArg, ControllerCommitment, ExecRequest, JobId, JobState,
    MAX_COMMAND_ARG_BYTES, OutputLimitInfo, OutputPublication, OutputStorage, OutputSummary,
    ProtectedOutput, RunSandboxMode, Sha256Digest, StdinSource, StreamInfo, WorkspacePath,
};
use cowshed_core::error::{CowshedError, ErrorCode, Result};
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::job_artifact::ArtifactConfig;
use tokio::sync::mpsc;

use cowshed_core::runtime::supervisor::{
    ArtifactSeal, ArtifactSink, ArtifactStoreSink, ArtifactWrite, CheckpointBarrier,
    CommitmentDraft, CommitmentSink, DevenvCommandOutput, OutputStream, ProcessEvent, ProcessExit,
    ProcessSignal, ProcessSpawnRequest, RunningProcess, SessionToken, SpawnSink,
    WorkspaceAuthoritySnapshot, WorkspaceSupervisor, WorkspaceSupervisorConfig,
    WorkspaceSupervisorHandle,
};

#[derive(Debug)]
struct Spawned {
    request: ProcessSpawnRequest,
    events: mpsc::Sender<ProcessEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcessObservation {
    Stdin(JobId, Bytes),
    StdinClosed(JobId),
    Signal(JobId, ProcessSignal),
}

struct FakeSpawner {
    spawned: mpsc::UnboundedSender<Spawned>,
    process_observations: mpsc::UnboundedSender<ProcessObservation>,
    fail_next: bool,
    backpressure: bool,
    order: mpsc::UnboundedSender<OrderObservation>,
    devenv_requests: mpsc::UnboundedSender<PathBuf>,
    devenv_outputs: VecDeque<DevenvCommandOutput>,
}

#[async_trait]
impl SpawnSink for FakeSpawner {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        if self.fail_next {
            self.fail_next = false;
            return Err(CowshedError::environment_missing(
                "injected spawn failure",
                "repair the executable",
            ));
        }
        self.order
            .send(OrderObservation::Spawn(request.job_id))
            .expect("order observer");
        self.spawned
            .send(Spawned {
                request: request.clone(),
                events,
            })
            .expect("spawn observer");
        Ok(Box::new(FakeProcess {
            job_id: request.job_id,
            pid: 10_000 + u32::try_from(request.job_id.get()).unwrap(),
            observations: self.process_observations.clone(),
            backpressure: self.backpressure,
            writes: 0,
        }))
    }

    async fn print_devenv_env(
        &mut self,
        devenv_dir: &std::path::Path,
    ) -> Result<DevenvCommandOutput> {
        self.devenv_requests
            .send(devenv_dir.to_owned())
            .expect("devenv request observer");
        self.devenv_outputs.pop_front().ok_or_else(|| {
            CowshedError::internal("test did not provide a devenv print-dev-env output")
        })
    }
}

struct FakeProcess {
    job_id: JobId,
    pid: u32,
    observations: mpsc::UnboundedSender<ProcessObservation>,
    backpressure: bool,
    writes: usize,
}

impl RunningProcess for FakeProcess {
    fn pid(&self) -> u32 {
        self.pid
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.writes += 1;
        if self.backpressure && self.writes == 2 {
            return Ok(false);
        }
        self.observations
            .send(ProcessObservation::Stdin(self.job_id, bytes))
            .expect("process observer");
        Ok(true)
    }

    fn close_stdin(&mut self) -> Result<()> {
        self.observations
            .send(ProcessObservation::StdinClosed(self.job_id))
            .expect("process observer");
        Ok(())
    }

    fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
        self.observations
            .send(ProcessObservation::Signal(self.job_id, signal))
            .expect("process observer");
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ArtifactObservation {
    Admit(JobId),
    Write(JobId, OutputStream, Bytes),
    Seal(JobId, JobState),
    Barrier(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OrderObservation {
    ArtifactAdmit(JobId),
    ArtifactSeal(JobId),
    ArtifactBarrier(u64),
    Commitment(&'static str),
    Spawn(JobId),
}

struct FakeArtifactSink {
    next: JobId,
    next_barrier: u64,
    quota: u64,
    jobs: BTreeMap<JobId, FakeArtifactJob>,
    observations: mpsc::UnboundedSender<ArtifactObservation>,
    order: mpsc::UnboundedSender<OrderObservation>,
}

impl ArtifactSink for FakeArtifactSink {
    fn next_job_id(&self) -> Result<JobId> {
        Ok(self.next)
    }

    fn admit(
        &mut self,
        expected_job_id: JobId,
        _grant_revision: u64,
        argv: &[CommandArg],
    ) -> Result<()> {
        assert!(!argv.is_empty());
        assert_eq!(expected_job_id, self.next);
        self.observations
            .send(ArtifactObservation::Admit(expected_job_id))
            .expect("artifact observer");
        self.order
            .send(OrderObservation::ArtifactAdmit(expected_job_id))
            .expect("order observer");
        self.next = JobId::new(self.next.get() + 1).unwrap();
        let replaced = self.jobs.insert(
            expected_job_id,
            FakeArtifactJob {
                id: expected_job_id,
                quota: self.quota,
                accepted: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
                observations: self.observations.clone(),
                crossing: None,
                order: self.order.clone(),
            },
        );
        assert!(replaced.is_none());
        Ok(())
    }

    fn prepare_background(&mut self, job_id: JobId) -> Result<()> {
        assert!(self.jobs.contains_key(&job_id));
        Ok(())
    }

    fn write(
        &mut self,
        job_id: JobId,
        stream: OutputStream,
        bytes: &[u8],
    ) -> Result<ArtifactWrite> {
        self.jobs
            .get_mut(&job_id)
            .expect("live fake artifact job")
            .write(stream, bytes)
    }

    fn seal(
        &mut self,
        job_id: JobId,
        state: JobState,
        _stdout_copy: Option<OutputPublication>,
        _stderr_copy: Option<OutputPublication>,
    ) -> Result<ArtifactSeal> {
        self.jobs
            .remove(&job_id)
            .expect("live fake artifact job")
            .seal(state)
    }

    fn checkpoint(&mut self) -> Result<CheckpointBarrier> {
        let barrier_id = self.next_barrier;
        self.next_barrier += 1;
        self.observations
            .send(ArtifactObservation::Barrier(barrier_id))
            .expect("artifact observer");
        self.order
            .send(OrderObservation::ArtifactBarrier(barrier_id))
            .expect("order observer");
        Ok(CheckpointBarrier {
            checkpoint_id: String::new(),
            barrier_id,
            manifest_batch_sha256: Sha256Digest::compute(&barrier_id.to_be_bytes()),
        })
    }
}

struct FakeArtifactJob {
    id: JobId,
    quota: u64,
    accepted: u64,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    observations: mpsc::UnboundedSender<ArtifactObservation>,
    crossing: Option<OutputLimitInfo>,
    order: mpsc::UnboundedSender<OrderObservation>,
}

impl FakeArtifactJob {
    fn write(&mut self, stream: OutputStream, bytes: &[u8]) -> Result<ArtifactWrite> {
        let remaining = self.quota.saturating_sub(self.accepted);
        let accepted = usize::try_from(remaining.min(u64::try_from(bytes.len()).unwrap())).unwrap();
        let admitted = &bytes[..accepted];
        match stream {
            OutputStream::Stdout => self.stdout.extend_from_slice(admitted),
            OutputStream::Stderr => self.stderr.extend_from_slice(admitted),
        }
        self.accepted += u64::try_from(accepted).unwrap();
        self.observations
            .send(ArtifactObservation::Write(
                self.id,
                stream,
                Bytes::copy_from_slice(admitted),
            ))
            .expect("artifact observer");
        let output_limit = (accepted < bytes.len()).then(|| OutputLimitInfo {
            limit_bytes: self.quota,
            crossing_bytes: self.accepted + u64::try_from(bytes.len() - accepted).unwrap(),
        });
        if output_limit.is_some() {
            self.crossing = output_limit.clone();
        }
        Ok(ArtifactWrite {
            accepted_bytes: accepted,
            output_limit,
        })
    }

    fn seal(self, state: JobState) -> Result<ArtifactSeal> {
        self.observations
            .send(ArtifactObservation::Seal(self.id, state))
            .expect("artifact observer");
        self.order
            .send(OrderObservation::ArtifactSeal(self.id))
            .expect("order observer");
        Ok(ArtifactSeal {
            stdout: stream(self.stdout),
            stderr: stream(self.stderr),
            terminal_batch_sha256: Sha256Digest::compute(&self.id.get().to_be_bytes()),
            output_limit: self.crossing,
        })
    }
}

struct FakeCommitments {
    next_order: u64,
    observations: mpsc::UnboundedSender<ControllerCommitment>,
    order: mpsc::UnboundedSender<OrderObservation>,
}

#[async_trait]
impl CommitmentSink for FakeCommitments {
    async fn record(&mut self, draft: CommitmentDraft) -> Result<()> {
        let order = self.next_order;
        let commitment = draft.into_commitment(order);
        assert_eq!(commitment.version(), CONTROLLER_COMMITMENT_VERSION);
        assert_eq!(commitment.order(), order);
        self.next_order += 1;
        self.order
            .send(OrderObservation::Commitment(match &commitment {
                ControllerCommitment::WorkspaceIntroduced(_) => "workspace-introduced",
                ControllerCommitment::WorkspaceRetired(_) => "workspace-retired",
                ControllerCommitment::Admission(_) => "admission",
                ControllerCommitment::Terminal(_) => "terminal",
                ControllerCommitment::Checkpoint(_) => "checkpoint",
                ControllerCommitment::Fork(_) => "fork",
                ControllerCommitment::Restore(_) => "restore",
            }))
            .expect("order observer");
        self.observations
            .send(commitment)
            .expect("commitment observer");
        Ok(())
    }
}

struct Harness {
    handle: WorkspaceSupervisorHandle,
    spawned: mpsc::UnboundedReceiver<Spawned>,
    process: mpsc::UnboundedReceiver<ProcessObservation>,
    artifacts: mpsc::UnboundedReceiver<ArtifactObservation>,
    commitments: mpsc::UnboundedReceiver<ControllerCommitment>,
    order: mpsc::UnboundedReceiver<OrderObservation>,
    devenv_requests: mpsc::UnboundedReceiver<PathBuf>,
}

fn authority() -> WorkspaceAuthoritySnapshot {
    WorkspaceAuthoritySnapshot {
        repo_id: RepoId::parse("acme/widget").unwrap(),
        workspace: WorkspaceName::new("main").unwrap(),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
        grant_revision: 7,
        lifecycle_revision: 11,
    }
}

fn config() -> WorkspaceSupervisorConfig {
    // Under TMPDIR and process-unique: a fixed shared path races concurrent test binaries and,
    // on a shared runner, inherits a directory another user owns and this process cannot write.
    let workspace_root = std::env::temp_dir().join(format!(
        "cowshed-supervisor-test-workspace-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&workspace_root).expect("workspace root");
    WorkspaceSupervisorConfig {
        authority: authority(),
        workspace_root: workspace_root.clone(),
        default_cwd: Some(WorkspacePath::new("packages/app").unwrap()),
        sandbox: SandboxConfig {
            home: PathBuf::from("/Users/tester"),
            mount_root: workspace_root.parent().expect("temp root").to_path_buf(),
            workspace_mount: workspace_root,
            exec_temp_dir: PathBuf::from("/tmp/cowshed-exec"),
            port_block: PortBlock::new(49_152, 16).unwrap(),
            mode: cowshed_core::sandbox::RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            git_worktree_repository: None,
        },
        artifacts: ArtifactConfig {
            combined_output_quota_bytes: 1024,
            ..ArtifactConfig::default()
        },
        term_grace: Duration::from_millis(10),
        actor_capacity: 8,
        event_capacity: 8,
    }
}

fn harness(start_id: u64, quota: u64, fail_next: bool, backpressure: bool) -> Harness {
    harness_with_config(config(), start_id, quota, fail_next, backpressure)
}

fn harness_with_config(
    supervisor_config: WorkspaceSupervisorConfig,
    start_id: u64,
    quota: u64,
    fail_next: bool,
    backpressure: bool,
) -> Harness {
    harness_with_devenv_outputs(
        supervisor_config,
        start_id,
        quota,
        fail_next,
        backpressure,
        VecDeque::new(),
    )
}

fn harness_with_devenv_outputs(
    supervisor_config: WorkspaceSupervisorConfig,
    start_id: u64,
    quota: u64,
    fail_next: bool,
    backpressure: bool,
    devenv_outputs: VecDeque<DevenvCommandOutput>,
) -> Harness {
    let (spawn_tx, spawned) = mpsc::unbounded_channel();
    let (process_tx, process) = mpsc::unbounded_channel();
    let (artifact_tx, artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, commitments) = mpsc::unbounded_channel();
    let (order_tx, order) = mpsc::unbounded_channel();
    let (devenv_tx, devenv_requests) = mpsc::unbounded_channel();
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
        Box::new(FakeSpawner {
            spawned: spawn_tx,
            process_observations: process_tx,
            fail_next,
            backpressure,
            order: order_tx.clone(),
            devenv_requests: devenv_tx,
            devenv_outputs,
        }),
        Box::new(FakeArtifactSink {
            next: JobId::new(start_id).unwrap(),
            next_barrier: 1,
            quota,
            jobs: BTreeMap::new(),
            observations: artifact_tx,
            order: order_tx.clone(),
        }),
        Box::new(FakeCommitments {
            next_order: 1,
            observations: commitment_tx,
            order: order_tx,
        }),
    )
    .unwrap();
    Harness {
        handle,
        spawned,
        process,
        artifacts,
        commitments,
        order,
        devenv_requests,
    }
}

/// A harness whose artifact sink is the production [`ArtifactStoreSink`] over the config's
/// workspace root, so barrier state persists across supervisors exactly as it does across
/// `cowshed checkpoint` processes.
fn real_store_harness(supervisor_config: WorkspaceSupervisorConfig) -> Harness {
    let (spawn_tx, spawned) = mpsc::unbounded_channel();
    let (process_tx, process) = mpsc::unbounded_channel();
    let (_artifact_tx, artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, commitments) = mpsc::unbounded_channel();
    let (order_tx, order) = mpsc::unbounded_channel();
    let (devenv_tx, devenv_requests) = mpsc::unbounded_channel();
    let store = ArtifactStoreSink::open(
        supervisor_config.workspace_root.clone(),
        &supervisor_config.authority,
        supervisor_config.artifacts.clone(),
    )
    .expect("open artifact store");
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
        Box::new(FakeSpawner {
            spawned: spawn_tx,
            process_observations: process_tx,
            fail_next: false,
            backpressure: false,
            order: order_tx.clone(),
            devenv_requests: devenv_tx,
            devenv_outputs: VecDeque::new(),
        }),
        Box::new(store),
        Box::new(FakeCommitments {
            next_order: 1,
            observations: commitment_tx,
            order: order_tx,
        }),
    )
    .unwrap();
    Harness {
        handle,
        spawned,
        process,
        artifacts,
        commitments,
        order,
        devenv_requests,
    }
}

fn request(stdin: StdinSource) -> ExecRequest {
    ExecRequest {
        argv: vec!["printf".into(), "payload".into()],
        cwd: Some(WorkspacePath::new("packages/app").unwrap()),
        mode: RunSandboxMode::ReadWrite,
        env: BTreeMap::from([("LANG".into(), "C".into())])
            .into_iter()
            .collect(),
        trace: None,
        stdin,
        stdout_copy: None,
        stderr_copy: None,
    }
}

fn isolated_config(label: &str) -> (WorkspaceSupervisorConfig, PathBuf) {
    let root = std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!(
            "cowshed-supervisor-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
    let workspace_root = root.join("workspace");
    std::fs::create_dir_all(&workspace_root).expect("isolated workspace");
    let mut supervisor_config = config();
    supervisor_config.workspace_root = workspace_root.clone();
    supervisor_config.sandbox.workspace_mount = workspace_root;
    supervisor_config.sandbox.home = root.join("home");
    supervisor_config.sandbox.exec_temp_dir = root.join("tmp");
    (supervisor_config, root)
}

fn printed_devenv_output(variables: serde_json::Value) -> DevenvCommandOutput {
    DevenvCommandOutput {
        status: 0,
        stdout: serde_json::to_vec(&serde_json::json!({ "variables": variables })).unwrap(),
        stderr: Vec::new(),
    }
}

fn write_devenv_snapshot(devenv_dir: &std::path::Path, vars: serde_json::Value) {
    let path = devenv_dir.join(".devenv/cowshed-env.json");
    std::fs::create_dir_all(path.parent().unwrap()).expect("snapshot directory");
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&serde_json::json!({ "vars": vars })).unwrap(),
    )
    .expect("snapshot");
}

fn stream(bytes: Vec<u8>) -> StreamInfo {
    let digest = Sha256Digest::compute(&bytes);
    StreamInfo {
        bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: digest,
        summary: OutputSummary {
            version: 1,
            text: String::from_utf8_lossy(&bytes).into_owned(),
            truncated: false,
        },
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::Inline {
                data: cowshed_core::api::BinaryData::new(bytes).unwrap(),
            },
        },
    }
}

async fn complete(spawned: &Spawned, stdout: &[u8], stderr: &[u8], exit: ProcessExit) {
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: spawned.request.job_id,
            stream: OutputStream::Stdout,
            bytes: Bytes::copy_from_slice(stdout),
        })
        .await
        .unwrap();
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: spawned.request.job_id,
            stream: OutputStream::Stderr,
            bytes: Bytes::copy_from_slice(stderr),
        })
        .await
        .unwrap();
    spawned
        .events
        .send(ProcessEvent::Exited {
            job_id: spawned.request.job_id,
            exit,
        })
        .await
        .unwrap();
    for stream in [OutputStream::Stdout, OutputStream::Stderr] {
        spawned
            .events
            .send(ProcessEvent::OutputEof {
                job_id: spawned.request.job_id,
                stream,
            })
            .await
            .unwrap();
    }
}

async fn open_named(handle: &WorkspaceSupervisorHandle, name: &str) -> SessionToken {
    handle.open_session(Some(name.into())).await.unwrap()
}

#[tokio::test]
async fn stale_devenv_snapshot_refreshes_once_and_merges_before_spawn() {
    let (supervisor_config, root) = isolated_config("devenv-stale");
    let mount = supervisor_config.workspace_root.clone();
    let devenv_dir = mount.join("tooling/devenv");
    std::fs::create_dir_all(&devenv_dir).expect("devenv dir");
    std::fs::write(mount.join("devenv.nix"), "{}").expect("root devenv");
    std::fs::write(
        mount.join(".cowshed.toml"),
        "[devenv]\ndir = \"tooling/devenv\"\n",
    )
    .expect("config");
    std::fs::write(devenv_dir.join("devenv.nix"), "{ version = 1; }").expect("devenv.nix");
    write_devenv_snapshot(
        &devenv_dir,
        serde_json::json!({
            "ONLY_DEVENV": "old",
            "PATH": "/old/untrusted/bin"
        }),
    );
    std::thread::sleep(Duration::from_millis(20));
    std::fs::write(devenv_dir.join("devenv.nix"), "{ version = 2; }").expect("changed devenv.nix");

    let output = printed_devenv_output(serde_json::json!({
        "ONLY_DEVENV": { "type": "exported", "value": "updated" },
        "CONFLICT": { "type": "exported", "value": "snapshot" },
        "PATH": { "type": "exported", "value": "/new/untrusted/bin" },
        "NOT_EXPORTED": { "type": "var", "value": "private" }
    }));
    let mut h = harness_with_devenv_outputs(
        supervisor_config,
        1,
        1024,
        false,
        false,
        VecDeque::from([output]),
    );
    let mut exec = request(StdinSource::Empty);
    exec.env.insert("CONFLICT".into(), "controller".into());
    let job = h.handle.exec(None, exec).await.unwrap();
    assert_eq!(h.devenv_requests.recv().await.unwrap(), devenv_dir);
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(
        spawned.request.devenv_dir.as_deref(),
        Some(devenv_dir.as_path())
    );
    assert_eq!(
        spawned.request.env.get("ONLY_DEVENV").map(String::as_str),
        Some("updated")
    );
    assert_eq!(
        spawned.request.env.get("CONFLICT").map(String::as_str),
        Some("controller")
    );
    assert!(!spawned.request.env.contains_key("PATH"));
    assert!(!spawned.request.env.contains_key("NOT_EXPORTED"));
    let snapshot: serde_json::Value = serde_json::from_slice(
        &std::fs::read(devenv_dir.join(".devenv/cowshed-env.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(snapshot["vars"]["ONLY_DEVENV"], "updated");
    assert_eq!(snapshot["vars"]["PATH"], "/new/untrusted/bin");
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    h.handle.wait(job).await.unwrap();

    let mut exec = request(StdinSource::Empty);
    exec.env.insert("CONFLICT".into(), "controller".into());
    let second = h.handle.exec(None, exec).await.unwrap();
    let second_spawn = h.spawned.recv().await.unwrap();
    assert_eq!(
        second_spawn
            .request
            .env
            .get("ONLY_DEVENV")
            .map(String::as_str),
        Some("updated")
    );
    assert!(matches!(
        h.devenv_requests.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    complete(&second_spawn, b"", b"", ProcessExit::Exited(0)).await;
    h.handle.wait(second).await.unwrap();

    drop(h);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn fresh_devenv_snapshot_spawns_without_source_stats_or_devenv_invocation() {
    let (supervisor_config, root) = isolated_config("devenv-fresh");
    let mount = supervisor_config.workspace_root.clone();
    let devenv_dir = mount.join("tooling/devenv");
    std::fs::create_dir_all(&devenv_dir).expect("devenv dir");
    std::fs::write(
        mount.join(".cowshed.toml"),
        "[devenv]\ndir = \"tooling/devenv\"\n",
    )
    .expect("config");
    std::fs::write(devenv_dir.join("devenv.nix"), "{}").expect("devenv.nix");
    std::thread::sleep(Duration::from_millis(20));
    write_devenv_snapshot(&devenv_dir, serde_json::json!({ "FROM_SNAPSHOT": "ready" }));

    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(
        spawned.request.env.get("FROM_SNAPSHOT").map(String::as_str),
        Some("ready")
    );
    assert!(matches!(
        h.devenv_requests.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    h.handle.wait(job).await.unwrap();

    drop(h);
    std::fs::remove_dir_all(root).ok();
}

#[tokio::test]
async fn configured_missing_devenv_and_failed_refresh_are_fail_closed() {
    let (missing_config, missing_root) = isolated_config("devenv-missing");
    let missing_mount = missing_config.workspace_root.clone();
    std::fs::write(
        missing_mount.join(".cowshed.toml"),
        "[devenv]\ndir = \"tooling/devenv\"\n",
    )
    .expect("config");
    let mut missing = harness_with_config(missing_config, 1, 1024, false, false);
    let error = missing
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert!(error.message.contains("tooling/devenv"));
    assert!(error.message.contains("devenv.nix"));
    assert!(matches!(
        missing.devenv_requests.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        missing.spawned.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    drop(missing);
    std::fs::remove_dir_all(missing_root).ok();

    let (failed_config, failed_root) = isolated_config("devenv-failed");
    let failed_mount = failed_config.workspace_root.clone();
    std::fs::write(failed_mount.join("devenv.nix"), "{}").expect("devenv.nix");
    let mut failed = harness_with_devenv_outputs(
        failed_config,
        1,
        1024,
        false,
        false,
        VecDeque::from([DevenvCommandOutput {
            status: 42,
            stdout: Vec::new(),
            stderr: b"evaluation exploded".to_vec(),
        }]),
    );
    let error = failed
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert!(error.message.contains("evaluation exploded"));
    assert_eq!(failed.devenv_requests.recv().await.unwrap(), failed_mount);
    assert!(matches!(
        failed.spawned.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        failed.artifacts.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    drop(failed);
    std::fs::remove_dir_all(failed_root).ok();
}

#[tokio::test]
async fn non_utf8_argv_reaches_spawn_and_job_info_without_loss() {
    let mut h = harness(1, 1024, false, false);
    let raw = vec![0xff, b'x', 0x80];
    let mut exec = request(StdinSource::Empty);
    exec.argv = vec![
        CommandArg::from(OsString::from_vec(raw.clone())),
        CommandArg::from("--flag"),
    ];
    let job = h.handle.exec(None, exec).await.unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(spawned.request.argv[0].as_os_str().as_bytes(), raw);
    let info = h.handle.info(job).await.unwrap();
    assert_eq!(info.argv[0].as_os_str().as_bytes(), raw);
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    h.handle.wait(job).await.unwrap();
}

#[tokio::test]
async fn unsafe_argv_rejects_before_artifact_commitment_or_spawn_effects() {
    let mut h = harness(1, 1024, false, false);
    for argument in [
        OsString::from_vec(vec![b'x', 0]),
        OsString::from_vec(vec![b'x'; MAX_COMMAND_ARG_BYTES + 1]),
    ] {
        let mut exec = request(StdinSource::Empty);
        exec.argv = vec![CommandArg::from(argument)];
        let error = h.handle.exec(None, exec).await.unwrap_err();
        assert_eq!(error.code, ErrorCode::Usage);
        assert!(matches!(
            h.spawned.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            h.artifacts.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            h.commitments.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            h.order.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn monotonic_ids_and_simultaneous_completions_are_serialized() {
    let mut h = harness(41, 1024, false, false);
    let first = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let second = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    assert_eq!((first.get(), second.get()), (41, 42));
    for job in [first, second] {
        assert_eq!(
            h.order.recv().await.unwrap(),
            OrderObservation::ArtifactAdmit(job)
        );
        assert_eq!(
            h.order.recv().await.unwrap(),
            OrderObservation::Commitment("admission")
        );
        assert_eq!(h.order.recv().await.unwrap(), OrderObservation::Spawn(job));
    }
    let first_spawn = h.spawned.recv().await.unwrap();
    let second_spawn = h.spawned.recv().await.unwrap();

    let a = complete(&first_spawn, b"one", b"", ProcessExit::Exited(0));
    let b = complete(&second_spawn, b"two", b"", ProcessExit::Exited(0));
    tokio::join!(a, b);

    let (first_info, second_info) = tokio::join!(h.handle.wait(first), h.handle.wait(second));
    assert_eq!(first_info.unwrap().state, JobState::Exited);
    assert_eq!(second_info.unwrap().state, JobState::Exited);
    let listed = h.handle.list().await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|job| job.job_id.get())
            .collect::<Vec<_>>(),
        [41, 42]
    );
}

#[tokio::test]
async fn exact_authority_and_session_identity_are_fenced() {
    let mut h = harness(1, 1024, false, false);
    let old = h.handle.clone();
    let session = open_named(&h.handle, "build").await;
    let same = open_named(&h.handle, "build").await;
    assert_eq!(session.identity(), same.identity());
    let advanced = h
        .handle
        .advance_authority(8, 12, config().sandbox)
        .await
        .unwrap();
    let stale = old.list().await.unwrap_err();
    assert_eq!(stale.code, ErrorCode::Conflict);
    let stale_session = advanced
        .exec(Some(&session), request(StdinSource::Empty))
        .await
        .unwrap_err();
    assert_eq!(stale_session.code, ErrorCode::Conflict);

    let current = open_named(&advanced, "build").await;
    advanced.close_session(current.clone()).await.unwrap();
    let reopened = open_named(&advanced, "build").await;
    assert_ne!(current.identity(), reopened.identity());
    assert_eq!(
        advanced.session_snapshot(&current).await.unwrap_err().code,
        ErrorCode::Conflict
    );
    assert!(h.spawned.try_recv().is_err());
}

#[tokio::test]
async fn disconnect_does_not_cancel_background_process() {
    let mut h = harness(1, 1024, false, false);
    let session = open_named(&h.handle, "daemon").await;
    let job = h
        .handle
        .exec_background(Some(&session), request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let survivor = h.handle.clone();
    drop(h.handle);
    complete(&spawned, b"alive", b"", ProcessExit::Exited(0)).await;
    let info = survivor.wait(job).await.unwrap();
    assert_eq!(info.state, JobState::Exited);
    assert_eq!(info.stdout.bytes, 5);
}

#[tokio::test]
async fn opaque_non_utf8_output_round_trips_through_logs_and_artifacts() {
    let mut h = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let opaque = [0xff, 0x00, 0x80, b'x'];
    complete(&spawned, &opaque, b"", ProcessExit::Exited(0)).await;
    let info = h.handle.wait(job).await.unwrap();
    assert_eq!(info.stdout.bytes, u64::try_from(opaque.len()).unwrap());
    let log = h
        .handle
        .log_read(job, OutputStream::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!(log.bytes.as_ref(), opaque);
    assert!(log.eof);
    assert!(matches!(
        h.artifacts.recv().await.unwrap(),
        ArtifactObservation::Admit(id) if id == job
    ));
    assert!(matches!(
        h.artifacts.recv().await.unwrap(),
        ArtifactObservation::Write(id, OutputStream::Stdout, bytes)
            if id == job && bytes.as_ref() == opaque
    ));
}

#[tokio::test]
async fn stdin_write_observes_bounded_backpressure() {
    let mut h = harness(1, 1024, false, true);
    let (stream_writer, stream_reader) = tokio::io::duplex(8);
    let job = h
        .handle
        .exec(None, request(StdinSource::Stream(Box::pin(stream_reader))))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();

    h.handle
        .stdin_write(job, Bytes::from_static(b"first"))
        .await
        .unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"first"))
    );

    let handle = h.handle.clone();
    let second =
        tokio::spawn(async move { handle.stdin_write(job, Bytes::from_static(b"second")).await });
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    spawned
        .events
        .send(ProcessEvent::StdinReady { job_id: job })
        .await
        .unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"second"))
    );

    drop(stream_writer);
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    h.handle.wait(job).await.unwrap();
}

#[tokio::test]
async fn output_limit_terms_kills_and_drains_before_terminal_commitment() {
    let mut h = harness(1, 4, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: OutputStream::Stdout,
            bytes: Bytes::from_static(b"abcdef"),
        })
        .await
        .unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(job, ProcessSignal::Term)
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(job, ProcessSignal::Kill)
    );
    spawned
        .events
        .send(ProcessEvent::Exited {
            job_id: job,
            exit: ProcessExit::Signaled {
                signal: 9,
                core_dumped: false,
            },
        })
        .await
        .unwrap();
    for stream in [OutputStream::Stdout, OutputStream::Stderr] {
        spawned
            .events
            .send(ProcessEvent::OutputEof {
                job_id: job,
                stream,
            })
            .await
            .unwrap();
    }
    let info = h.handle.wait(job).await.unwrap();
    assert_eq!(info.state, JobState::OutputLimit);
    assert_eq!(info.stdout.bytes, 4);
    assert_eq!(info.output_limit.unwrap().limit_bytes, 4);
}

#[tokio::test]
async fn spawn_failure_is_typed_terminal_with_one_terminal_commitment() {
    let mut h = harness(7, 1024, true, false);
    let error = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    let listed = h.handle.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].job_id.get(), 7);
    assert_eq!(listed[0].state, JobState::Failed);
    let admission = h.commitments.recv().await.unwrap();
    let terminal = h.commitments.recv().await.unwrap();
    assert!(matches!(admission, ControllerCommitment::Admission(_)));
    assert!(matches!(terminal, ControllerCommitment::Terminal(_)));
    assert!(h.commitments.try_recv().is_err());
}

#[tokio::test]
async fn named_session_preserves_cwd_env_and_background_membership() {
    let mut h = harness(1, 1024, false, false);
    let session = open_named(&h.handle, "dev").await;
    let mut first = request(StdinSource::Empty);
    first.cwd = Some(WorkspacePath::new("packages/worker").unwrap());
    first.env.insert("MODE".into(), "watch".into());
    let job = h
        .handle
        .exec_background(Some(&session), first)
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let snapshot = h.handle.session_snapshot(&session).await.unwrap();
    assert_eq!(
        snapshot.cwd.as_ref().unwrap().as_path(),
        std::path::Path::new("packages/worker")
    );
    assert_eq!(snapshot.env["MODE"], "watch");
    assert!(snapshot.background_jobs.contains(&job));
    assert_eq!(spawned.request.env["MODE"], "watch");
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    h.handle.wait(job).await.unwrap();
    assert!(
        h.handle
            .session_snapshot(&session)
            .await
            .unwrap()
            .background_jobs
            .is_empty()
    );
}

#[tokio::test]
async fn checkpoint_barrier_orders_artifact_digest_before_commitment() {
    let mut h = harness(1, 1024, false, false);
    let barrier = h
        .handle
        .checkpoint_barrier("checkpoint-1".into())
        .await
        .unwrap();
    assert_eq!(
        barrier.manifest_batch_sha256,
        Sha256Digest::compute(&1_u64.to_be_bytes())
    );
    assert_eq!(
        h.artifacts.recv().await.unwrap(),
        ArtifactObservation::Barrier(1)
    );
    assert_eq!(
        h.order.recv().await.unwrap(),
        OrderObservation::ArtifactBarrier(1)
    );
    assert_eq!(
        h.order.recv().await.unwrap(),
        OrderObservation::Commitment("checkpoint")
    );
    let commitment = h.commitments.recv().await.unwrap();
    let ControllerCommitment::Checkpoint(checkpoint) = commitment else {
        panic!("checkpoint commitment")
    };
    assert_eq!(checkpoint.barrier_id, 1);
    assert_eq!(
        checkpoint.manifest_batch_sha256,
        barrier.manifest_batch_sha256
    );
    // The sink owns allocation: the next barrier over the same sink is simply the next id.
    let next = h
        .handle
        .checkpoint_barrier("checkpoint-2".into())
        .await
        .unwrap();
    assert_eq!(next.barrier_id, 2);
}

/// One `cowshed checkpoint` per process: each invocation starts a fresh supervisor over the same
/// durable artifact store. The barrier sequence is owned by the store, so a later supervisor must
/// continue where the previous one stopped — and a checkpoint that fails after its barrier must
/// not wedge the sequence for every checkpoint that follows.
#[tokio::test]
async fn a_fresh_supervisor_continues_the_durable_barrier_sequence() {
    let (supervisor_config, _root) = isolated_config("barrier-durability");

    let first = real_store_harness(supervisor_config.clone())
        .handle
        .checkpoint_barrier("checkpoint-1".into())
        .await
        .expect("first barrier over an empty store");
    assert_eq!(first.barrier_id, 1);

    // A second process over the same workspace. The durable store already holds barrier 1;
    // no session-local counter may contradict it.
    let second = real_store_harness(supervisor_config)
        .handle
        .checkpoint_barrier("checkpoint-2".into())
        .await
        .expect("a fresh supervisor must continue the durable barrier sequence");
    assert_eq!(second.barrier_id, 2);
}

#[tokio::test]
async fn retire_waits_for_process_tree_stop_and_terminal_persistence() {
    let mut h = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let retire = tokio::spawn(async move { handle.retire().await });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(job, ProcessSignal::Term)
    );
    assert!(!retire.is_finished());
    spawned
        .events
        .send(ProcessEvent::Exited {
            job_id: job,
            exit: ProcessExit::Signaled {
                signal: 15,
                core_dumped: false,
            },
        })
        .await
        .unwrap();
    for stream in [OutputStream::Stdout, OutputStream::Stderr] {
        spawned
            .events
            .send(ProcessEvent::OutputEof {
                job_id: job,
                stream,
            })
            .await
            .unwrap();
    }
    retire.await.unwrap().unwrap();
    assert_eq!(h.handle.info(job).await.unwrap().state, JobState::Killed);
    let mut terminal_count = 0;
    while let Ok(commitment) = h.commitments.try_recv() {
        if matches!(commitment, ControllerCommitment::Terminal(_)) {
            terminal_count += 1;
        }
    }
    assert_eq!(terminal_count, 1);
    assert_eq!(
        h.handle
            .exec(None, request(StdinSource::Empty))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
}

#[tokio::test]
async fn kill_acknowledges_only_after_terminal_artifact_and_commitment() {
    let mut h = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    for expected in [
        OrderObservation::ArtifactAdmit(job),
        OrderObservation::Commitment("admission"),
        OrderObservation::Spawn(job),
    ] {
        assert_eq!(h.order.recv().await.unwrap(), expected);
    }
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );

    let handle = h.handle.clone();
    let kill = tokio::spawn(async move { handle.kill(job).await });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(job, ProcessSignal::Term)
    );
    assert!(!kill.is_finished());
    spawned
        .events
        .send(ProcessEvent::Exited {
            job_id: job,
            exit: ProcessExit::Signaled {
                signal: 15,
                core_dumped: false,
            },
        })
        .await
        .unwrap();
    for stream in [OutputStream::Stdout, OutputStream::Stderr] {
        spawned
            .events
            .send(ProcessEvent::OutputEof {
                job_id: job,
                stream,
            })
            .await
            .unwrap();
    }
    kill.await.unwrap().unwrap();
    assert_eq!(
        h.order.recv().await.unwrap(),
        OrderObservation::ArtifactSeal(job)
    );
    assert_eq!(
        h.order.recv().await.unwrap(),
        OrderObservation::Commitment("terminal")
    );
    assert_eq!(h.handle.info(job).await.unwrap().state, JobState::Killed);
}

#[tokio::test]
async fn log_follow_and_attach_wait_for_exact_next_bytes() {
    let mut h = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let follow =
        tokio::spawn(async move { handle.log_read(job, OutputStream::Stdout, 0, true).await });
    tokio::task::yield_now().await;
    assert!(!follow.is_finished());
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: OutputStream::Stdout,
            bytes: Bytes::from_static(b"first"),
        })
        .await
        .unwrap();
    let first = follow.await.unwrap().unwrap();
    assert_eq!(first.bytes, Bytes::from_static(b"first"));
    assert_eq!(first.next_offset, 5);
    assert!(!first.eof);

    let handle = h.handle.clone();
    let attach =
        tokio::spawn(async move { handle.attach_read(job, OutputStream::Stdout, 5).await });
    tokio::task::yield_now().await;
    assert!(!attach.is_finished());
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: OutputStream::Stdout,
            bytes: Bytes::from_static(b"-second"),
        })
        .await
        .unwrap();
    let second = attach.await.unwrap().unwrap();
    assert_eq!(second.bytes, Bytes::from_static(b"-second"));
    assert_eq!(second.next_offset, 12);

    spawned
        .events
        .send(ProcessEvent::Exited {
            job_id: job,
            exit: ProcessExit::Exited(0),
        })
        .await
        .unwrap();
    for stream in [OutputStream::Stdout, OutputStream::Stderr] {
        spawned
            .events
            .send(ProcessEvent::OutputEof {
                job_id: job,
                stream,
            })
            .await
            .unwrap();
    }
    h.handle.wait(job).await.unwrap();
    let eof = h
        .handle
        .attach_read(job, OutputStream::Stdout, 12)
        .await
        .unwrap();
    assert!(eof.bytes.is_empty());
    assert!(eof.eof);
}

#[tokio::test]
async fn quiesce_rejects_admission_and_waits_for_existing_terminal_commitment() {
    let mut h = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let quiesce = tokio::spawn(async move { handle.quiesce().await });
    tokio::task::yield_now().await;
    assert!(!quiesce.is_finished());
    assert_eq!(
        h.handle
            .exec(None, request(StdinSource::Empty))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    quiesce.await.unwrap().unwrap();
    assert_eq!(h.handle.info(job).await.unwrap().state, JobState::Exited);
}

#[tokio::test]
async fn none_cwd_is_preserved_as_workspace_root_without_a_sentinel() {
    let mut supervisor_config = config();
    supervisor_config.default_cwd = None;
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let mut exec = request(StdinSource::Empty);
    exec.cwd = None;
    let job = h.handle.exec(None, exec).await.unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert!(spawned.request.cwd.as_os_str().is_empty());
    assert_eq!(h.handle.info(job).await.unwrap().cwd, None);
    complete(&spawned, b"", b"", ProcessExit::Exited(0)).await;
    assert_eq!(h.handle.wait(job).await.unwrap().cwd, None);
}
