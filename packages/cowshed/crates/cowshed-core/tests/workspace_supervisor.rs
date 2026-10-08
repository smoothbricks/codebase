use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_core::api::{
    AdmissionKey, CONTROLLER_COMMITMENT_VERSION, CommandArg, ControllerCommitment, ExecCommand,
    ExecRequest, ExitStatus, JobFailure, JobId, JobJournalCursor, JobState, JobStreamWatermark,
    JobTailBytes, JobTailLimits, MAX_COMMAND_ARG_BYTES, OutputLimitInfo, OutputPublication,
    OutputStorage, OutputSummary, ProtectedOutput, PublicationPolicy, RunSandboxMode, Sha256Digest,
    StdinKind, StdinSource, StreamInfo, WorkspacePath,
};
use cowshed_core::error::{AdmissionField, AdmissionRefusal, CowshedError, ErrorCode, Result};
use cowshed_core::fork_lock::Spawn as _;
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::{OwnedRepoIds, RepoId};
use cowshed_core::runtime::job_groups::Birth;
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::job_artifact::{
    AdmissionLookup, ArtifactConfig, ArtifactStore, FIRST_KEYED_LAYOUT, JobAdmission, JobEnding,
    StreamKind,
};
use tokio::sync::mpsc;

use cowshed_core::runtime::supervisor::{
    ArtifactSeal, ArtifactSink, ArtifactStoreSink, ArtifactWrite, CheckpointBarrier,
    CommitmentDraft, CommitmentSink, Labelled, OwnedProcess, ProcessEvent, ProcessSignal,
    ProcessSpawnRequest, RunningProcess, SessionToken, SpawnCommand, SpawnSink, VolumeLabeller,
    VolumeLabels, WorkspaceAuthoritySnapshot, WorkspaceSupervisor, WorkspaceSupervisorConfig,
    WorkspaceSupervisorHandle,
};

#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

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
                events: events.clone(),
            })
            .expect("spawn observer");
        let pid = 10_000 + u32::try_from(request.job_id.get()).unwrap();
        Ok(Box::new(FakeProcess {
            job_id: request.job_id,
            // Not a process: its pid names nothing this test owns, so no group is identified.
            process: OwnedProcess {
                birth: Birth::Unobserved {
                    pid,
                    reason: "a fake process leads no group".into(),
                },
                spawned: Instant::now(),
                host: cowshed_core::host_load::read_host_load(),
            },
            observations: self.process_observations.clone(),
            events,
            backpressure: self.backpressure,
            writes: 0,
        }))
    }
}

struct FakeProcess {
    job_id: JobId,
    process: OwnedProcess,
    observations: mpsc::UnboundedSender<ProcessObservation>,
    /// Where an unbackpressured fake reports each write it took as written to the child.
    events: mpsc::Sender<ProcessEvent>,
    /// A backpressured fake refuses its second write: the lane is still busy with the write
    /// before. It reports no write written; the test sends each `StdinReady` itself. The actor
    /// closes only once those writes are delivered, when the modeled lane is free.
    backpressure: bool,
    writes: usize,
}

impl RunningProcess for FakeProcess {
    fn process(&self) -> Option<&OwnedProcess> {
        Some(&self.process)
    }

    fn try_write_stdin(&mut self, bytes: Bytes) -> Result<bool> {
        self.writes += 1;
        if self.backpressure && self.writes == 2 {
            return Ok(false);
        }
        self.observations
            .send(ProcessObservation::Stdin(self.job_id, bytes))
            .expect("process observer");
        if !self.backpressure {
            let events = self.events.clone();
            let job_id = self.job_id;
            tokio::spawn(async move { events.send(ProcessEvent::StdinReady { job_id }).await });
        }
        Ok(true)
    }

    fn close_stdin(&mut self) -> bool {
        self.observations
            .send(ProcessObservation::StdinClosed(self.job_id))
            .expect("process observer");
        true
    }

    // A fake has no pipe to close; the supervisor's own stdin state says it ended.
    fn end_stdin(&mut self) {}

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
    Write(JobId, StreamKind, Bytes),
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

/// `sealed_stdout` lets a test commit a stdout stream that differs from the bytes the job wrote,
/// which is the only way to observe from outside whether a terminal log read is answered from the
/// store's committed copy or from a second copy the actor kept.
struct FakeArtifactSink {
    sealed_stdout: Option<Vec<u8>>,
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
        command: &cowshed_core::api::ExecCommand,
        admission: Option<JobAdmission>,
    ) -> Result<()> {
        assert!(command.validate().is_ok());
        assert_eq!(admission, None, "this sink keeps no admission keys");
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
                sealed_stdout: self.sealed_stdout.clone(),
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

    /// Keyed admission is the production store's ([`real_store_harness`]).
    fn admitted(&self, _key: &AdmissionKey) -> AdmissionLookup<'_> {
        AdmissionLookup::Absent
    }

    fn prepare_background(&mut self, job_id: JobId) -> Result<()> {
        assert!(self.jobs.contains_key(&job_id));
        Ok(())
    }

    fn write(&mut self, job_id: JobId, stream: StreamKind, bytes: &[u8]) -> Result<ArtifactWrite> {
        self.jobs
            .get_mut(&job_id)
            .expect("live fake artifact job")
            .write(stream, bytes)
    }

    fn seal(
        &mut self,
        job_id: JobId,
        ending: JobEnding,
        _stdout_copy: Option<OutputPublication>,
        _stderr_copy: Option<OutputPublication>,
    ) -> Result<ArtifactSeal> {
        self.jobs
            .remove(&job_id)
            .expect("live fake artifact job")
            .seal(ending.state)
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

    /// This sink keeps no terminal records: it hands each seal back and forgets the job. The
    /// record a fresh supervisor answers from is the production store's
    /// ([`a_fresh_supervisor_answers_for_a_job_its_predecessor_sealed`]).
    fn sealed(&self, _job_id: JobId) -> Option<cowshed_core::api::SealedJob> {
        None
    }
}

struct FakeArtifactJob {
    id: JobId,
    sealed_stdout: Option<Vec<u8>>,
    quota: u64,
    accepted: u64,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    observations: mpsc::UnboundedSender<ArtifactObservation>,
    crossing: Option<OutputLimitInfo>,
    order: mpsc::UnboundedSender<OrderObservation>,
}

impl FakeArtifactJob {
    fn write(&mut self, stream: StreamKind, bytes: &[u8]) -> Result<ArtifactWrite> {
        let remaining = self.quota.saturating_sub(self.accepted);
        let accepted = usize::try_from(remaining.min(u64::try_from(bytes.len()).unwrap())).unwrap();
        let admitted = &bytes[..accepted];
        match stream {
            StreamKind::Stdout => self.stdout.extend_from_slice(admitted),
            StreamKind::Stderr => self.stderr.extend_from_slice(admitted),
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
            stdout: stream(self.sealed_stdout.unwrap_or(self.stdout)),
            stderr: stream(self.stderr),
            terminal_batch_sha256: Sha256Digest::compute(&self.id.get().to_be_bytes()),
            output_limit: self.crossing,
            publication_failure: None,
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
                ControllerCommitment::LandAdoption(_) => "land-adoption",
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

/// The supervisor config of the workspace at `root/workspace`, mounted under `root`. A pure
/// function of `root`, so a test advancing authority re-derives the very same sandbox.
fn config(root: &TempRoot) -> WorkspaceSupervisorConfig {
    let workspace_root = root.join("workspace");
    WorkspaceSupervisorConfig {
        authority: authority(),
        owned_repo_ids: OwnedRepoIds::sole(authority().repo_id),
        workspace_root: workspace_root.clone(),
        default_cwd: Some(WorkspacePath::new("packages/app").unwrap()),
        sandbox: SandboxConfig {
            home: PathBuf::from("/Users/tester"),
            mount_root: root.to_path_buf(),
            workspace_mount: workspace_root,
            exec_temp_dir: PathBuf::from("/tmp/cowshed-exec"),
            port_block: PortBlock::new(49_136, 16).unwrap(),
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
        artifacts: ArtifactConfig {
            combined_output_quota_bytes: 1024,
            ..ArtifactConfig::default()
        },
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

/// A test's private root holding the workspace `config` places in it, removed when the test
/// ends on any path. Each test binds it before any supervisor over it, so it outlives them all.
fn workspace_root(label: &str) -> TempRoot {
    let root = TempRoot::new(&format!("cowshed-supervisor-{label}"));
    std::fs::create_dir(root.join("workspace")).expect("workspace root");
    root
}

fn harness(start_id: u64, quota: u64, fail_next: bool, backpressure: bool) -> (Harness, TempRoot) {
    let root = workspace_root("test-workspace");
    let h = harness_with_config(config(&root), start_id, quota, fail_next, backpressure);
    (h, root)
}

fn harness_with_config(
    supervisor_config: WorkspaceSupervisorConfig,
    start_id: u64,
    quota: u64,
    fail_next: bool,
    backpressure: bool,
) -> Harness {
    harness_with_artifacts(
        supervisor_config,
        start_id,
        quota,
        fail_next,
        backpressure,
        None,
    )
}

/// A harness whose artifact sink commits a stdout stream that is not what the job wrote.
fn harness_with_sealed_stdout(sealed_stdout: Vec<u8>) -> (Harness, TempRoot) {
    let root = workspace_root("test-workspace");
    let h = harness_with_artifacts(config(&root), 1, 1024, false, false, Some(sealed_stdout));
    (h, root)
}

fn harness_with_artifacts(
    supervisor_config: WorkspaceSupervisorConfig,
    start_id: u64,
    quota: u64,
    fail_next: bool,
    backpressure: bool,
    sealed_stdout: Option<Vec<u8>>,
) -> Harness {
    let (spawn_tx, spawned) = mpsc::unbounded_channel();
    let (process_tx, process) = mpsc::unbounded_channel();
    let (artifact_tx, artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, commitments) = mpsc::unbounded_channel();
    let (order_tx, order) = mpsc::unbounded_channel();
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
        Box::new(FakeSpawner {
            spawned: spawn_tx,
            process_observations: process_tx,
            fail_next,
            backpressure,
            order: order_tx.clone(),
        }),
        Box::new(FakeArtifactSink {
            sealed_stdout,
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
    let store = ArtifactStoreSink::open(
        supervisor_config.workspace_root.clone(),
        &supervisor_config.owned_repo_ids,
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
    }
}

fn request(stdin: StdinSource) -> ExecRequest {
    ExecRequest {
        command: cowshed_core::api::ExecCommand::Argv(vec!["printf".into(), "payload".into()]),
        cwd: Some(WorkspacePath::new("packages/app").unwrap()),
        mode: RunSandboxMode::ReadWrite,
        env: BTreeMap::from([("LANG".into(), "C".into())])
            .into_iter()
            .collect(),
        trace: None,
        stdin,
        stdout_copy: None,
        stderr_copy: None,
        admission_key: None,
    }
}

fn isolated_config(label: &str) -> (WorkspaceSupervisorConfig, TempRoot) {
    let root = workspace_root(label);
    let mut supervisor_config = config(&root);
    supervisor_config.sandbox.home = root.join("home");
    supervisor_config.sandbox.exec_temp_dir = root.join("tmp");
    (supervisor_config, root)
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

async fn complete(spawned: &Spawned, stdout: &[u8], stderr: &[u8], exit: ExitStatus) {
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: spawned.request.job_id,
            stream: StreamKind::Stdout,
            bytes: Bytes::copy_from_slice(stdout),
        })
        .await
        .unwrap();
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: spawned.request.job_id,
            stream: StreamKind::Stderr,
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
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
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

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "requires an unsandboxed macOS host-controller process"]
async fn host_controller_exec_mode_enforces_each_request_without_widening_the_ceiling() {
    for ceiling in [
        cowshed_core::sandbox::RunSandboxMode::ReadWrite,
        cowshed_core::sandbox::RunSandboxMode::ReadOnly,
    ] {
        let (mut supervisor_config, root) = isolated_config("exec-mode");
        supervisor_config.sandbox.mode = ceiling;
        supervisor_config.default_cwd = None;
        // A configured temp tree may sit beneath a denied store. Its narrow carve-back
        // must survive that deny without granting writes to the workspace.
        let denied_store = root.join("private-store");
        supervisor_config.sandbox.exec_temp_dir = denied_store.join("tmp");
        supervisor_config
            .sandbox
            .additional_denies
            .push(denied_store);
        let mount = supervisor_config.workspace_root.clone();
        std::fs::create_dir_all(&supervisor_config.sandbox.home).unwrap();
        std::fs::create_dir_all(&supervisor_config.sandbox.exec_temp_dir).unwrap();
        std::fs::create_dir_all(mount.join(".cowshed/bin")).unwrap();
        std::fs::write(
            mount.join(cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH),
            cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32]).encode(),
        )
        .unwrap();
        std::fs::write(mount.join("readable"), b"readable\n").unwrap();
        let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
        for (index, mode) in [
            RunSandboxMode::ReadOnly,
            RunSandboxMode::ReadWrite,
            RunSandboxMode::ReadOnly,
        ]
        .into_iter()
        .enumerate()
        {
            let target = format!("write-{index}");
            let mut exec = request(StdinSource::Empty);
            exec.mode = mode;
            exec.cwd = None;
            exec.command = ExecCommand::Argv(
                [
                    "/bin/sh",
                    "-c",
                    "printf state > \"$XDG_DATA_HOME/probe\" && printf state > \"$HOME/probe\" && mkdir -p \"$XDG_RUNTIME_DIR/test\" && /bin/cat readable && printf written > \"$1\"",
                    "probe",
                    &target,
                ]
                .into_iter()
                .map(CommandArg::from)
                .collect(),
            );
            let job = h.handle.exec(None, None, exec).await.unwrap();
            let spawned = h.spawned.recv().await.unwrap();
            // Run the admitted request through the production spawn checks and the
            // kernel. A profile-only probe misses the supervisor/child role boundary.
            let (events, mut received) = mpsc::channel(16);
            let mut process = cowshed_core::runtime::supervisor::SystemSpawnSink::default()
                .spawn(spawned.request, events)
                .await
                .expect("spawn admitted job");
            assert!(process.close_stdin(), "a fresh lane takes the EOF");
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let mut eof_count = 0;
            let mut exit = None;
            while exit.is_none() || eof_count != 2 {
                let event = received.recv().await.expect("complete process events");
                match &event {
                    ProcessEvent::Output { stream, bytes, .. } => match stream {
                        StreamKind::Stdout => stdout.extend_from_slice(bytes),
                        StreamKind::Stderr => stderr.extend_from_slice(bytes),
                    },
                    ProcessEvent::OutputEof { .. } => eof_count += 1,
                    ProcessEvent::Exited { exit: status, .. } => exit = Some(status.clone()),
                    ProcessEvent::WaitFailed { error, .. } => panic!("wait failed: {error}"),
                    _ => {}
                }
                spawned.events.send(event).await.unwrap();
            }
            h.handle.wait(job).await.unwrap();
            assert_eq!(stdout, b"readable\n");
            let writable = mode == RunSandboxMode::ReadWrite
                && ceiling == cowshed_core::sandbox::RunSandboxMode::ReadWrite;
            assert_eq!(
                exit == Some(ExitStatus::Exited { code: 0 }),
                writable,
                "request {mode:?} under ceiling {ceiling:?}: {}",
                String::from_utf8_lossy(&stderr)
            );
            assert_eq!(mount.join(target).exists(), writable);
        }
    }
}

#[tokio::test]
async fn graceful_requested_cancellation_keeps_its_actual_exit_serializable() {
    let (mut h, _root) = harness(1, 1024, false, false);
    for code in [0, 143] {
        let job = h
            .handle
            .exec(None, None, request(StdinSource::Empty))
            .await
            .unwrap();
        let spawned = h.spawned.recv().await.unwrap();
        assert_eq!(
            h.process.recv().await.unwrap(),
            ProcessObservation::StdinClosed(job)
        );
        let handle = h.handle.clone();
        let cancelled = tokio::spawn(async move { handle.kill(job).await });
        assert_eq!(
            h.process.recv().await.unwrap(),
            ProcessObservation::Signal(job, ProcessSignal::Term)
        );
        complete(&spawned, b"", b"", ExitStatus::Exited { code }).await;
        cancelled.await.unwrap().unwrap();
        let info = h.handle.info(job).await.unwrap();
        assert_eq!(info.state, JobState::Killed);
        assert_eq!(info.exit, Some(ExitStatus::Exited { code }));
        let encoded = serde_json::to_value(&info)
            .expect("a cancelled child may catch SIGTERM and exit normally");
        let decoded: cowshed_core::api::JobInfo = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.state, JobState::Killed);
        assert_eq!(decoded.exit, Some(ExitStatus::Exited { code }));
    }
}

#[tokio::test]
async fn non_utf8_argv_reaches_spawn_and_job_info_without_loss() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let raw = vec![0xff, b'x', 0x80];
    let mut exec = request(StdinSource::Empty);
    exec.command = ExecCommand::Argv(vec![
        CommandArg::from(OsString::from_vec(raw.clone())),
        CommandArg::from("--flag"),
    ]);
    let job = h.handle.exec(None, None, exec).await.unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let SpawnCommand::Argv(spawned_argv) = &spawned.request.command else {
        panic!("an argv job spawns an argv");
    };
    assert_eq!(spawned_argv[0].as_os_str().as_bytes(), raw);
    let info = h.handle.info(job).await.unwrap();
    assert_eq!(
        info.command.argv().expect("an argv job")[0]
            .as_os_str()
            .as_bytes(),
        raw
    );
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
}

#[tokio::test]
async fn unsafe_argv_rejects_before_artifact_commitment_or_spawn_effects() {
    let (mut h, _root) = harness(1, 1024, false, false);
    for argument in [
        OsString::from_vec(vec![b'x', 0]),
        OsString::from_vec(vec![b'x'; MAX_COMMAND_ARG_BYTES + 1]),
    ] {
        let mut exec = request(StdinSource::Empty);
        exec.command = ExecCommand::Argv(vec![CommandArg::from(argument)]);
        let error = h.handle.exec(None, None, exec).await.unwrap_err();
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

/// A job runs with the build volume the controller resolved when it was admitted
/// (16_build_volumes.md, "Process lifetime across a swap"): after an adoption moves the
/// checkout's link, the next job gets the adopted volume while a job admitted before keeps the
/// profile it was spawned with, and nothing is relaunched.
#[tokio::test]
async fn each_job_runs_with_the_build_volume_it_was_admitted_with() {
    let root = workspace_root("build-volume");
    use cowshed_core::build_volume::{BuildVolumeId, BuildVolumeLayout};
    use cowshed_core::repository::ProjectPaths;

    let mut supervisor_config = config(&root);
    let project = ProjectPaths::with_mount_root(
        root.join("store"),
        &supervisor_config.sandbox.mount_root,
        &supervisor_config.authority.repo_id,
    )
    .unwrap();
    let layout = BuildVolumeLayout::new(&project).unwrap();
    let before_id = BuildVolumeId::parse(&"a".repeat(32)).unwrap();
    let adopted_id = BuildVolumeId::parse(&"b".repeat(32)).unwrap();
    std::fs::create_dir_all(layout.images()).unwrap();
    for id in [&before_id, &adopted_id] {
        std::fs::File::create(layout.image(id)).unwrap();
    }
    let before = layout.mount(&before_id);
    let adopted = layout.mount(&adopted_id);
    supervisor_config.build_volume_layout = Some(layout.clone());
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let first = h
        .handle
        .exec(None, Some(before.clone()), request(StdinSource::Empty))
        .await
        .unwrap();
    let first_spawn = h.spawned.recv().await.unwrap();
    let second = h
        .handle
        .exec(None, Some(adopted.clone()), request(StdinSource::Empty))
        .await
        .unwrap();
    let second_spawn = h.spawned.recv().await.unwrap();
    assert!(
        layout.held(&before_id).unwrap(),
        "the first job retains its admitted volume"
    );
    assert!(
        layout.held(&adopted_id).unwrap(),
        "the later job retains the adopted volume"
    );
    let granted = |spawn: &Spawned, mode| {
        spawn
            .request
            .policy
            .child(mode)
            .0
            .build_volume_mount
            .clone()
    };
    for mode in [RunSandboxMode::ReadWrite, RunSandboxMode::ReadOnly] {
        assert_eq!(granted(&first_spawn, mode), Some(before.clone()));
        assert_eq!(granted(&second_spawn, mode), Some(adopted.clone()));
    }
    let (_, first_profile) = first_spawn.request.policy.child(RunSandboxMode::ReadWrite);
    assert!(first_profile.contains(before.to_str().unwrap()));
    assert!(!first_profile.contains(adopted.to_str().unwrap()));
    let (_, second_profile) = second_spawn.request.policy.child(RunSandboxMode::ReadWrite);
    assert!(second_profile.contains(adopted.to_str().unwrap()));
    assert!(!second_profile.contains(before.to_str().unwrap()));
    // A checkout that links no volume grants none.
    let third = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let third_spawn = h.spawned.recv().await.unwrap();
    assert_eq!(granted(&third_spawn, RunSandboxMode::ReadWrite), None);
    let completions = [
        (first, &first_spawn),
        (second, &second_spawn),
        (third, &third_spawn),
    ];
    for (job, spawn) in completions {
        complete(spawn, b"", b"", ExitStatus::Exited { code: 0 }).await;
        h.handle.wait(job).await.unwrap();
    }
    assert!(
        !layout.held(&before_id).unwrap(),
        "the concluded job drops its old-volume hold"
    );
    assert!(
        !layout.held(&adopted_id).unwrap(),
        "the concluded job drops its new-volume hold"
    );
}

#[tokio::test]
async fn monotonic_ids_and_simultaneous_completions_are_serialized() {
    let (mut h, _root) = harness(41, 1024, false, false);
    let first = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let second = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
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

    let a = complete(&first_spawn, b"one", b"", ExitStatus::Exited { code: 0 });
    let b = complete(&second_spawn, b"two", b"", ExitStatus::Exited { code: 0 });
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
    let (mut h, root) = harness(1, 1024, false, false);
    let old = h.handle.clone();
    let session = open_named(&h.handle, "build").await;
    let same = open_named(&h.handle, "build").await;
    assert_eq!(session.identity(), same.identity());
    let advanced = h
        .handle
        .advance_authority(8, 12, config(&root).sandbox)
        .await
        .unwrap();
    let stale = old.list().await.unwrap_err();
    assert_eq!(stale.code, ErrorCode::Conflict);
    let stale_session = advanced
        .exec(Some(&session), None, request(StdinSource::Empty))
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
    let (mut h, _root) = harness(1, 1024, false, false);
    let session = open_named(&h.handle, "daemon").await;
    let job = h
        .handle
        .exec_background(Some(&session), None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let survivor = h.handle.clone();
    drop(h.handle);
    complete(&spawned, b"alive", b"", ExitStatus::Exited { code: 0 }).await;
    let info = survivor.wait(job).await.unwrap();
    assert_eq!(info.state, JobState::Exited);
    assert_eq!(info.stdout.bytes, 5);
}

#[tokio::test]
async fn opaque_non_utf8_output_round_trips_through_logs_and_artifacts() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let opaque = [0xff, 0x00, 0x80, b'x'];
    complete(&spawned, &opaque, b"", ExitStatus::Exited { code: 0 }).await;
    let info = h.handle.wait(job).await.unwrap();
    assert_eq!(info.stdout.bytes, u64::try_from(opaque.len()).unwrap());
    let log = h
        .handle
        .log_read(job, StreamKind::Stdout, 0, false)
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
        ArtifactObservation::Write(id, StreamKind::Stdout, bytes)
            if id == job && bytes.as_ref() == opaque
    ));
}

/// A write is answered once the pump reports it written to the child, and the job's stdin cursor
/// counts only such bytes: a write the lane took but the pump has not written is neither answered
/// nor counted, and one behind it waits in the queue until the lane has room.
#[tokio::test]
async fn stdin_write_observes_bounded_backpressure_and_counts_only_delivered_bytes() {
    let (mut h, _root) = harness(1, 1024, false, true);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Open))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let delivered = |handle: WorkspaceSupervisorHandle| async move {
        handle.info(job).await.unwrap().stdin.bytes
    };

    let handle = h.handle.clone();
    let first = tokio::spawn(async move {
        handle
            .stdin_write(job, 0, Bytes::from_static(b"first"))
            .await
    });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"first"))
    );
    assert_eq!(
        delivered(h.handle.clone()).await,
        0,
        "the lane took the write; nothing reached the child"
    );
    assert!(!first.is_finished());

    // Sent on its first poll, ahead of the status read behind it: the write is queued behind the
    // busy lane before the pump reports anything.
    let second = h.handle.stdin_write(job, 5, Bytes::from_static(b"second"));
    tokio::pin!(second);
    let sent = std::future::poll_fn(|context| {
        std::task::Poll::Ready(second.as_mut().poll(context).is_pending())
    })
    .await;
    assert!(sent, "the write waits for the lane");
    assert_eq!(delivered(h.handle.clone()).await, 0);
    spawned
        .events
        .send(ProcessEvent::StdinReady { job_id: job })
        .await
        .unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"second"))
    );
    assert_eq!(delivered(h.handle.clone()).await, 5);
    spawned
        .events
        .send(ProcessEvent::StdinReady { job_id: job })
        .await
        .unwrap();
    second.await.unwrap();
    assert_eq!(delivered(h.handle.clone()).await, 11);

    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
}

#[tokio::test]
async fn output_limit_terms_kills_and_drains_before_terminal_commitment() {
    let (mut h, _root) = harness(1, 4, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: StreamKind::Stdout,
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
            exit: ExitStatus::Signaled {
                signal: 9,
                core_dumped: false,
            },
        })
        .await
        .unwrap();
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
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
    let (mut h, _root) = harness(7, 1024, true, false);
    let error = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
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
    let (mut h, _root) = harness(1, 1024, false, false);
    let session = open_named(&h.handle, "dev").await;
    let mut first = request(StdinSource::Empty);
    first.cwd = Some(WorkspacePath::new("packages/worker").unwrap());
    first.env.insert("MODE".into(), "watch".into());
    let job = h
        .handle
        .exec_background(Some(&session), None, first)
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
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
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

/// A gateway-held registry credential is pointless if the workspace also gets the bytes.
///
/// The withheld names come from host state, so this is the one place a caller's ambient token
/// can be dropped for every job — including the second exec in a long-lived named session,
/// which is why the session's own remembered environment is checked too.
#[tokio::test]
async fn a_registered_credential_env_name_never_reaches_a_child() {
    let root = workspace_root("credential-env");
    let mut h = harness_with_config(
        WorkspaceSupervisorConfig {
            credential_env_names: std::collections::BTreeSet::from([
                "REGISTRY_READ_TOKEN".to_owned()
            ]),
            ..config(&root)
        },
        1,
        1024,
        false,
        false,
    );
    let session = open_named(&h.handle, "dev").await;
    let mut first = request(StdinSource::Empty);
    first
        .env
        .insert("REGISTRY_READ_TOKEN".into(), "ambient-secret".into());
    first.env.insert("MODE".into(), "watch".into());
    let job = h
        .handle
        .exec_background(Some(&session), None, first)
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert!(
        !spawned.request.env.contains_key("REGISTRY_READ_TOKEN"),
        "the child must not receive a token the gateway holds"
    );
    assert_eq!(spawned.request.env["MODE"], "watch");
    let snapshot = h.handle.session_snapshot(&session).await.unwrap();
    assert!(
        !snapshot.env.contains_key("REGISTRY_READ_TOKEN"),
        "the session must not remember it for the next exec either"
    );
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
}

#[tokio::test]
async fn checkpoint_barrier_orders_artifact_digest_before_commitment() {
    let (mut h, _root) = harness(1, 1024, false, false);
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

/// A job an earlier supervisor of the workspace incarnation ran and sealed is answered by the next
/// one from the durable records — its terminal facts, and its output from any offset — although
/// only the supervisor that ran a job answers its status. A client whose supervisor retired (a
/// drained supervisor of another build retires the moment its last job ends) still reaches the job
/// by its number, and reads on from the bytes it already holds.
#[tokio::test]
async fn a_fresh_supervisor_answers_for_a_job_its_predecessor_sealed() {
    let (supervisor_config, _root) = isolated_config("sealed-predecessor");
    let mut first = real_store_harness(supervisor_config.clone());
    let job = first
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = first.spawned.recv().await.unwrap();
    complete(
        &spawned,
        b"hello world\n",
        b"oops",
        ExitStatus::Exited { code: 3 },
    )
    .await;
    first.handle.wait(job).await.unwrap();
    first.handle.quiesce().await.unwrap();
    first.handle.retire().await.unwrap();
    drop(first);

    let second = real_store_harness(supervisor_config);
    let (remote, _path) = served(&second.handle).await;
    assert_eq!(
        remote.info(job).await.unwrap_err().code,
        ErrorCode::NotFound,
        "status answers only the supervisor's own jobs"
    );
    let sealed = remote.sealed(job).await.unwrap();
    assert_eq!(
        (
            sealed.job_id,
            sealed.state,
            sealed.exit,
            sealed.stdout.bytes,
            sealed.stderr.bytes
        ),
        (
            job,
            JobState::Exited,
            Some(ExitStatus::Exited { code: 3 }),
            12,
            4
        )
    );
    let tail = remote
        .log_read(job, StreamKind::Stdout, 6, false)
        .await
        .unwrap();
    assert_eq!(
        (tail.bytes.as_ref(), tail.next_offset, tail.eof),
        (&b"world\n"[..], 12, true)
    );
    let unknown = JobId::new(job.get() + 1).unwrap();
    assert_eq!(
        remote.sealed(unknown).await.unwrap_err().code,
        ErrorCode::NotFound,
        "a job nothing sealed has no terminal record to answer from"
    );
}

/// A copy the job asked for is a separate act from sealing it. When the store refuses the copy --
/// here a destination inside the protected `.cowshed` tree -- the job's terminal record and
/// commitment still stand and it stops running, while every waiter and killer, early or late, is
/// told the copy's own refusal.
#[tokio::test]
async fn a_refused_output_copy_fails_its_callers_after_the_job_ends_truthfully() {
    let (supervisor_config, _root) = isolated_config("refused-copy");
    let destination = supervisor_config.workspace_root.join(".cowshed/leak");
    let mut h = real_store_harness(supervisor_config);
    let job = h
        .handle
        .exec(
            None,
            None,
            ExecRequest {
                stdout_copy: Some(OutputPublication {
                    path: WorkspacePath::new(".cowshed/leak").unwrap(),
                    policy: PublicationPolicy::CreateNew,
                }),
                ..request(StdinSource::Empty)
            },
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let early = tokio::spawn(async move { handle.wait(job).await });
    tokio::task::yield_now().await;
    // Commands are served in order: once this answers, the early wait is queued on a running job.
    h.handle.info(job).await.unwrap();
    complete(&spawned, b"payload", b"", ExitStatus::Exited { code: 0 }).await;

    let refused = early
        .await
        .unwrap()
        .expect_err("the refused copy reaches the waiter queued before the job ended");
    assert_eq!(refused.code, ErrorCode::Integrity);
    assert_eq!(h.handle.wait(job).await.unwrap_err(), refused);
    assert_eq!(h.handle.kill(job).await.unwrap_err(), refused);

    let info = h.handle.info(job).await.unwrap();
    assert_eq!(
        (info.state, info.exit, info.stdout.bytes),
        (JobState::Exited, Some(ExitStatus::Exited { code: 0 }), 7)
    );
    let sealed = h.handle.sealed(job).await.unwrap();
    assert_eq!((sealed.state, sealed.stdout.bytes), (JobState::Exited, 7));
    assert!(!destination.exists());
    let terminals = std::iter::from_fn(|| h.commitments.try_recv().ok())
        .filter(|commitment| matches!(commitment, ControllerCommitment::Terminal(_)))
        .count();
    assert_eq!(terminals, 1);
    h.handle.quiesce().await.unwrap();
    h.handle.retire().await.unwrap();
}

/// A job whose terminal record the store refuses has still ended. Its waiters, early and late, and
/// its killers are answered with the refusal instead of queueing forever, its output stays readable
/// and retirement completes. The durable record stays unterminated, so the workspace's next
/// supervisor seals it failed as lost rather than as a success nothing recorded.
#[tokio::test]
async fn a_refused_terminal_record_answers_every_caller_and_stays_unterminated() {
    use std::os::unix::fs::PermissionsExt;

    let (supervisor_config, _root) = isolated_config("refused-seal");
    let job_root = supervisor_config.workspace_root.join(".cowshed/job");
    let store = (
        supervisor_config.workspace_root.clone(),
        supervisor_config.owned_repo_ids.clone(),
        supervisor_config.authority.workspace_incarnation.clone(),
        supervisor_config.artifacts.clone(),
    );
    let mut h = real_store_harness(supervisor_config);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let early = tokio::spawn(async move { handle.wait(job).await });
    tokio::task::yield_now().await;
    h.handle.info(job).await.unwrap();
    // The store appends records only under a private job directory.
    std::fs::set_permissions(&job_root, std::fs::Permissions::from_mode(0o755)).unwrap();
    complete(&spawned, b"payload", b"", ExitStatus::Exited { code: 0 }).await;

    let refused = early
        .await
        .unwrap()
        .expect_err("the store's refusal reaches the waiter queued before the job ended");
    assert_eq!(refused.code, ErrorCode::Integrity);
    assert_eq!(h.handle.wait(job).await.unwrap_err(), refused);
    assert_eq!(h.handle.kill(job).await.unwrap_err(), refused);
    // Nothing durable says how the job ended, so its status is the refusal rather than a
    // projection of streams no record backs.
    assert_eq!(h.handle.info(job).await.unwrap_err(), refused);
    assert_eq!(h.handle.list().await.unwrap_err(), refused);
    assert_eq!(
        h.handle.sealed(job).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    let output = h
        .handle
        .log_read(job, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!((output.bytes.as_ref(), output.eof), (&b"payload"[..], true));
    assert!(
        !std::iter::from_fn(|| h.commitments.try_recv().ok())
            .any(|commitment| matches!(commitment, ControllerCommitment::Terminal(_)))
    );
    h.handle.quiesce().await.unwrap();
    h.handle.retire().await.unwrap();
    drop(h);

    std::fs::set_permissions(&job_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (workspace_root, owned, incarnation, artifacts) = store;
    let lost = ArtifactStore::open(workspace_root, owned, incarnation, artifacts)
        .unwrap()
        .seal_unterminated()
        .unwrap();
    assert_eq!(
        lost.iter()
            .map(|(record, _)| (record.job_id, record.state, record.failure))
            .collect::<Vec<_>>(),
        vec![(job, JobState::Failed, Some(JobFailure::SupervisorLost))]
    );
}

/// A commitment sink whose publisher is gone by the time a job ends.
struct GoneTerminalPublisher {
    refusal: CowshedError,
}

#[async_trait]
impl CommitmentSink for GoneTerminalPublisher {
    async fn record(&mut self, draft: CommitmentDraft) -> Result<()> {
        match draft {
            CommitmentDraft::Terminal { .. } => Err(self.refusal.clone()),
            _ => Ok(()),
        }
    }
}

/// When the terminal record is durable but its audit commitment is refused, the job's status is
/// the record's truth -- its state, exit and sealed streams, read back from the store -- while
/// every waiter and killer, early or late, is told the commitment refusal.
#[tokio::test]
async fn a_refused_terminal_commitment_keeps_the_sealed_truth_and_fails_its_callers() {
    let (supervisor_config, _root) = isolated_config("refused-commitment");
    let refusal = CowshedError::environment_missing(
        "the commitment publisher is gone",
        "reattach the workspace",
    );
    let store = ArtifactStoreSink::open(
        supervisor_config.workspace_root.clone(),
        &supervisor_config.owned_repo_ids,
        &supervisor_config.authority,
        supervisor_config.artifacts.clone(),
    )
    .unwrap();
    let (spawn_tx, mut spawned) = mpsc::unbounded_channel();
    let (process_tx, _process) = mpsc::unbounded_channel();
    let (order_tx, _order) = mpsc::unbounded_channel();
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
        Box::new(FakeSpawner {
            spawned: spawn_tx,
            process_observations: process_tx,
            fail_next: false,
            backpressure: false,
            order: order_tx,
        }),
        Box::new(store),
        Box::new(GoneTerminalPublisher {
            refusal: refusal.clone(),
        }),
    )
    .unwrap();
    let job = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = spawned.recv().await.unwrap();
    let waiter = handle.clone();
    let early = tokio::spawn(async move { waiter.wait(job).await });
    tokio::task::yield_now().await;
    handle.info(job).await.unwrap();
    complete(&spawned, b"payload", b"", ExitStatus::Exited { code: 0 }).await;

    assert_eq!(early.await.unwrap().unwrap_err(), refusal);
    assert_eq!(handle.wait(job).await.unwrap_err(), refusal);
    assert_eq!(handle.kill(job).await.unwrap_err(), refusal);
    let info = handle.info(job).await.unwrap();
    let sealed = handle.sealed(job).await.unwrap();
    assert_eq!(
        (info.state, info.exit, info.stdout.sha256),
        (
            JobState::Exited,
            Some(ExitStatus::Exited { code: 0 }),
            Sha256Digest::compute(b"payload")
        )
    );
    assert_eq!(
        (sealed.state, sealed.stdout),
        (JobState::Exited, info.stdout)
    );
    let output = handle
        .log_read(job, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!((output.bytes.as_ref(), output.eof), (&b"payload"[..], true));
    handle.quiesce().await.unwrap();
    handle.retire().await.unwrap();
}

/// Spawns nothing: hands each job a process group the test owns, observed as its parent observes
/// it, and then ends and reaps that group's leader before the supervisor learns of the job -- the
/// race a real wait task can win. After that only the parent's observation can name the leader.
struct ReapedGroupSpawner {
    groups: std::collections::VecDeque<std::process::Child>,
    births: mpsc::UnboundedSender<Birth>,
    spawned: mpsc::UnboundedSender<Spawned>,
    process_observations: mpsc::UnboundedSender<ProcessObservation>,
}

#[async_trait]
impl SpawnSink for ReapedGroupSpawner {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        let mut group = self.groups.pop_front().expect("one owned group per job");
        let birth = Birth::of(group.id());
        let pgid = i32::try_from(group.id()).unwrap();
        // SAFETY: the unreaped test child leads this group.
        assert_eq!(unsafe { libc::killpg(pgid, libc::SIGKILL) }, 0);
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(group.wait().unwrap().signal(), Some(libc::SIGKILL));
        self.spawned
            .send(Spawned {
                request: request.clone(),
                events: events.clone(),
            })
            .expect("spawn observer");
        self.births.send(birth.clone()).expect("birth observer");
        Ok(Box::new(FakeProcess {
            job_id: request.job_id,
            process: OwnedProcess {
                birth,
                spawned: Instant::now(),
                host: cowshed_core::host_load::read_host_load(),
            },
            observations: self.process_observations.clone(),
            events,
            backpressure: false,
            writes: 0,
        }))
    }
}

fn owned_group() -> std::process::Child {
    use std::os::unix::process::CommandExt as _;
    std::process::Command::new("/bin/sh")
        .args(["-c", "sleep 300 & wait"])
        .stdin(std::process::Stdio::null())
        .process_group(0)
        .spawn_locked()
        .expect("a test-owned process group")
}

/// `(job id, leader birth)` for every group the ledger at `path` names.
fn ledger_leaders(path: &std::path::Path) -> Vec<(u64, Option<u64>)> {
    let ledger: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    ledger["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|group| {
            (
                group["jobId"].as_u64().unwrap(),
                group["leaderBirth"].as_u64(),
            )
        })
        .collect()
}

/// A job's group enters the ledger with its leader as the job's parent observed it, even when the
/// leader was reaped before the supervisor learned of the job, and every later rewrite carries
/// that identity. Observing the pid at either point instead could find no leader at all, and once
/// the pid is reused, whatever stranger then holds it.
#[tokio::test]
async fn the_ledger_names_each_group_by_its_parent_s_observation() {
    let (mut supervisor_config, root) = isolated_config("ledger-births");
    let ledger = root.join("supervisor.groups");
    supervisor_config.group_ledger = Some(ledger.clone());
    let (birth_tx, mut births) = mpsc::unbounded_channel();
    let (spawn_tx, mut spawns) = mpsc::unbounded_channel();
    let (process_tx, _process) = mpsc::unbounded_channel();
    let (artifact_tx, _artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, _commitments) = mpsc::unbounded_channel();
    let (order_tx, _order) = mpsc::unbounded_channel();
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
        Box::new(ReapedGroupSpawner {
            groups: [owned_group(), owned_group()].into(),
            births: birth_tx,
            spawned: spawn_tx,
            process_observations: process_tx,
        }),
        Box::new(FakeArtifactSink {
            sealed_stdout: None,
            next: JobId::new(1).unwrap(),
            next_barrier: 1,
            quota: 1024,
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
    let birth = |birth: Birth| {
        let leader = birth.leader().expect("the parent identified the leader");
        Some(leader.birth())
    };

    handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let first = birth(births.recv().await.unwrap());
    assert_eq!(ledger_leaders(&ledger), vec![(1, first)]);
    handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let second = birth(births.recv().await.unwrap());
    assert_eq!(ledger_leaders(&ledger), vec![(1, first), (2, second)]);
    for id in [JobId::new(1).unwrap(), JobId::new(2).unwrap()] {
        let spawned = spawns.recv().await.unwrap();
        complete(
            &spawned,
            b"",
            b"",
            ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false,
            },
        )
        .await;
        handle.wait(id).await.unwrap();
    }
    handle.retire().await.unwrap();
}

/// A job whose parent could not identify its group leader still runs and reports its pid, but its
/// group never enters the ledger: nothing may later end a group no one identified.
#[tokio::test]
async fn an_unidentified_leader_never_enters_the_ledger() {
    let (mut supervisor_config, root) = isolated_config("ledger-unobserved");
    let ledger = root.join("supervisor.groups");
    supervisor_config.group_ledger = Some(ledger.clone());
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    assert_eq!(h.handle.info(job).await.unwrap().pid, Some(10_001));
    assert_eq!(ledger_leaders(&ledger), Vec::new());
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    h.handle.retire().await.unwrap();
}

/// The groups a lost predecessor left unresolved neither stop the next supervisor from serving
/// nor fall out of its ledger: each rewrite carries them while processes hold their ids, and drops
/// one only once nothing does. Never signalled throughout.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_supervisor_serves_and_carries_the_groups_its_predecessor_left_unresolved() {
    use std::io::BufRead as _;
    use std::os::unix::process::CommandExt as _;

    let (mut supervisor_config, root) = isolated_config("ledger-inherited");
    let ledger = root.join("supervisor.groups");
    // A group whose leader exited and was reaped while a descendant holds the id.
    let mut leaderless = std::process::Command::new("/bin/sh")
        .args(["-c", "(trap '' TERM; echo READY; exec sleep 300) & wait"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .process_group(0)
        .spawn_locked()
        .unwrap();
    let pgid = i32::try_from(leaderless.id()).unwrap();
    let mut ready = String::new();
    std::io::BufReader::new(leaderless.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    leaderless.kill().unwrap();
    leaderless.wait().unwrap();
    std::fs::write(
        &ledger,
        format!(r#"{{"supervisor":1,"groups":[{{"jobId":9,"pgid":{pgid},"leaderStart":null}}]}}"#),
    )
    .unwrap();
    let inherited = cowshed_core::runtime::job_groups::take_lost(&ledger, Duration::ZERO).unwrap();
    assert_eq!(inherited.len(), 1);
    supervisor_config.group_ledger = Some(ledger.clone());
    supervisor_config.inherited_groups = inherited;
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let carried = |ledger: &std::path::Path| -> Vec<u64> {
        let ledger: serde_json::Value =
            serde_json::from_slice(&std::fs::read(ledger).unwrap()).unwrap();
        ledger["unresolved"]
            .as_array()
            .map(|groups| {
                groups
                    .iter()
                    .map(|group| group["group"]["jobId"].as_u64().unwrap())
                    .collect()
            })
            .unwrap_or_default()
    };

    let first = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    assert_eq!(carried(&ledger), vec![9]);
    // SAFETY: signal 0 only checks that the group exists.
    assert_eq!(
        unsafe { libc::killpg(pgid, 0) },
        0,
        "the inherited group was signalled"
    );
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(first).await.unwrap();

    // SAFETY: this test made the group; its id still names the test's own descendant.
    assert_eq!(unsafe { libc::killpg(pgid, libc::SIGKILL) }, 0);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !cowshed_core::runtime::job_groups::unresolved(&ledger)
        .unwrap()
        .is_empty()
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    let second = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    assert_eq!(carried(&ledger), Vec::<u64>::new());
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(second).await.unwrap();
    h.handle.retire().await.unwrap();
}

#[tokio::test]
async fn retire_waits_for_process_tree_stop_and_terminal_persistence() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
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
            exit: ExitStatus::Signaled {
                signal: 15,
                core_dumped: false,
            },
        })
        .await
        .unwrap();
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
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
            .exec(None, None, request(StdinSource::Empty))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
}

#[tokio::test]
async fn kill_acknowledges_only_after_terminal_artifact_and_commitment() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
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
            exit: ExitStatus::Signaled {
                signal: 15,
                core_dumped: false,
            },
        })
        .await
        .unwrap();
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
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

/// A job whose leader exited while descendants hold its output open is still running, and a kill
/// still signals its group: the process stays held until the job concludes. The job ends Killed,
/// with the leader's own exit, once its output ends.
#[tokio::test]
async fn a_kill_after_the_leader_exits_reaches_the_group_still_holding_its_output() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    spawned
        .events
        .send(ProcessEvent::Exited {
            job_id: job,
            exit: ExitStatus::Exited { code: 3 },
        })
        .await
        .unwrap();
    assert_eq!(h.handle.info(job).await.unwrap().state, JobState::Running);

    let handle = h.handle.clone();
    let kill = tokio::spawn(async move { handle.kill(job).await });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(job, ProcessSignal::Term)
    );
    assert!(!kill.is_finished());
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
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
    let info = h.handle.info(job).await.unwrap();
    assert_eq!(
        (info.state, info.exit),
        (JobState::Killed, Some(ExitStatus::Exited { code: 3 }))
    );
}

#[tokio::test]
async fn log_follow_and_attach_wait_for_exact_next_bytes() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let follow =
        tokio::spawn(async move { handle.log_read(job, StreamKind::Stdout, 0, true).await });
    tokio::task::yield_now().await;
    assert!(!follow.is_finished());
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: StreamKind::Stdout,
            bytes: Bytes::from_static(b"first"),
        })
        .await
        .unwrap();
    let first = follow.await.unwrap().unwrap();
    assert_eq!(first.bytes, Bytes::from_static(b"first"));
    assert_eq!(first.next_offset, 5);
    assert!(!first.eof);

    let handle = h.handle.clone();
    let attach = tokio::spawn(async move { handle.attach_read(job, StreamKind::Stdout, 5).await });
    tokio::task::yield_now().await;
    assert!(!attach.is_finished());
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: StreamKind::Stdout,
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
            exit: ExitStatus::Exited { code: 0 },
        })
        .await
        .unwrap();
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
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
        .attach_read(job, StreamKind::Stdout, 12)
        .await
        .unwrap();
    assert!(eof.bytes.is_empty());
    assert!(eof.eof);
}

#[tokio::test]
async fn quiesce_does_not_wait_for_background_volume_labels() {
    struct BlockedLabeller {
        entered: mpsc::UnboundedSender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl VolumeLabeller for BlockedLabeller {
        fn ensure_label(&self, _mount: &Path, _label: &str) -> std::io::Result<Labelled> {
            self.entered.send(()).map_err(std::io::Error::other)?;
            self.release
                .lock()
                .expect("unpoisoned labeller gate")
                .recv()
                .map_err(std::io::Error::other)?;
            Ok(Labelled::Renamed)
        }
    }
    let root = workspace_root("volume-label-quiesce");
    let (entered, mut renaming) = mpsc::unbounded_channel();
    let (release, gate) = std::sync::mpsc::channel();
    let mut supervisor = config(&root);
    supervisor.volume_labels = Some(VolumeLabels {
        workspace: "[cowshed] acme · widget — main".into(),
        build: "[cowshed] acme · widget — build main".into(),
        labeller: std::sync::Arc::new(BlockedLabeller {
            entered,
            release: std::sync::Mutex::new(gate),
        }),
    });
    let h = harness_with_config(supervisor, 1, 1024, false, false);
    renaming
        .recv()
        .await
        .expect("the background rename entered its gate");
    // The rename is still blocked: quiescence must answer without Disk Arbitration latency.
    h.handle.quiesce().await.unwrap();
    release.send(()).expect("release the rename");
}

#[tokio::test]
async fn a_build_volume_label_request_cannot_name_a_host_mount_outside_its_layout() {
    use cowshed_core::build_volume::{BuildVolumeLayout, link};
    use cowshed_core::repository::ProjectPaths;

    struct RecordingLabeller(mpsc::UnboundedSender<PathBuf>);
    impl VolumeLabeller for RecordingLabeller {
        fn ensure_label(&self, mount: &Path, _label: &str) -> std::io::Result<Labelled> {
            self.0
                .send(mount.to_owned())
                .map_err(std::io::Error::other)?;
            Ok(Labelled::Renamed)
        }
    }
    let root = workspace_root("volume-label-authority");
    let mut supervisor = config(&root);
    let project = ProjectPaths::with_mount_root(
        root.join("store"),
        &supervisor.sandbox.mount_root,
        &supervisor.authority.repo_id,
    )
    .unwrap();
    supervisor.build_volume_layout = Some(BuildVolumeLayout::new(&project).unwrap());
    let checkout = supervisor.workspace_root.clone();
    let host_mount = root.join("host-mount");
    std::fs::create_dir_all(&host_mount).unwrap();
    link::point(&checkout, &host_mount).unwrap();
    let (named, mut labels) = mpsc::unbounded_channel();
    supervisor.volume_labels = Some(VolumeLabels {
        workspace: "[cowshed] acme · widget — main".into(),
        build: "[cowshed] acme · widget — build main".into(),
        labeller: std::sync::Arc::new(RecordingLabeller(named)),
    });
    let h = harness_with_config(supervisor, 1, 1024, false, false);
    assert_eq!(
        labels.recv().await.unwrap(),
        checkout,
        "the initial workspace label runs"
    );
    let refused = h
        .handle
        .name_build_volume(Some(host_mount))
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Integrity);
    assert!(
        labels.try_recv().is_err(),
        "even a matching checkout link cannot authorize naming a host volume"
    );
    h.handle.quiesce().await.unwrap();
}

#[tokio::test]
async fn quiesce_rejects_admission_and_waits_for_existing_terminal_commitment() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let quiesce = tokio::spawn(async move { handle.quiesce().await });
    tokio::task::yield_now().await;
    assert!(!quiesce.is_finished());
    assert_eq!(
        h.handle
            .exec(None, None, request(StdinSource::Empty))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    quiesce.await.unwrap().unwrap();
    assert_eq!(h.handle.info(job).await.unwrap().state, JobState::Exited);
}

#[tokio::test]
async fn idle_quiesce_refuses_busy_without_closing_admission_or_waiting() {
    let (mut h, _root) = harness(2, 1024, false, false);
    let first = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let first_spawned = h.spawned.recv().await.unwrap();
    let refused = h.handle.quiesce_if_idle().await.unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    let second = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .expect("busy refusal must leave admission open");
    let second_spawned = h.spawned.recv().await.unwrap();
    complete(&first_spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    complete(&second_spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(first).await.unwrap();
    h.handle.wait(second).await.unwrap();
    h.handle.quiesce_if_idle().await.unwrap();
    assert_eq!(
        h.handle
            .exec(None, None, request(StdinSource::Empty))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    h.handle.retire().await.unwrap();
}

#[tokio::test]
async fn none_cwd_is_preserved_as_workspace_root_without_a_sentinel() {
    let root = workspace_root("none-cwd");
    let mut supervisor_config = config(&root);
    supervisor_config.default_cwd = None;
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let mut exec = request(StdinSource::Empty);
    exec.cwd = None;
    let job = h.handle.exec(None, None, exec).await.unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert!(spawned.request.cwd.as_os_str().is_empty());
    assert_eq!(h.handle.info(job).await.unwrap().cwd, None);
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(h.handle.wait(job).await.unwrap().cwd, None);
}

/// The lifecycle half of the same defect: `wait(2)` failing must not produce a terminal job that
/// claims a signal death. The job seals as `Failed` with no exit status, the group is killed, and
/// everyone awaiting the job is handed the integrity error instead of a false success.
#[tokio::test]
async fn a_wait_failure_fails_the_job_without_inventing_an_exit_status() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();

    let handle = h.handle.clone();
    let waiter = tokio::spawn(async move { handle.wait(job).await });

    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: StreamKind::Stdout,
            bytes: Bytes::from_static(b"partial"),
        })
        .await
        .unwrap();
    spawned
        .events
        .send(ProcessEvent::WaitFailed {
            job_id: job,
            error: CowshedError::integrity(
                "cannot wait for the sandbox process: injected",
                "cowshed doctor --json",
            ),
        })
        .await
        .unwrap();

    // No `OutputEof` is sent: an unreaped child can hold its pipes open forever, so the job must
    // still reach a terminal record.
    let error = waiter
        .await
        .unwrap()
        .expect_err("an unobserved termination must not resolve as a completed job");
    assert_eq!(error.code, ErrorCode::Integrity);

    // The sealed record is honest: failed, with no fabricated exit.
    let info = h.handle.info(job).await.unwrap();
    assert_eq!(info.state, JobState::Failed);
    assert_eq!(info.exit, None);
    assert_eq!(
        h.artifacts.recv().await.unwrap(),
        ArtifactObservation::Admit(job)
    );
    assert_eq!(
        h.artifacts.recv().await.unwrap(),
        ArtifactObservation::Write(job, StreamKind::Stdout, Bytes::from_static(b"partial"))
    );
    assert_eq!(
        h.artifacts.recv().await.unwrap(),
        ArtifactObservation::Seal(job, JobState::Failed)
    );

    // Killing the group is what stops an unobservable child from outliving its record. Stdin
    // bookkeeping for the empty pump also lands on this lane, so scan rather than pin an index.
    let mut killed = false;
    while let Ok(observation) = h.process.try_recv() {
        killed |= observation == ProcessObservation::Signal(job, ProcessSignal::Kill);
    }
    assert!(
        killed,
        "an unreaped child's process group must be killed, not left running behind a sealed record"
    );

    // A retire must not hang on a job whose child was never reaped.
    h.handle.retire().await.unwrap();
}

/// After seal there must be exactly one copy of a job's output, and it must be the store's.
///
/// The fake sink seals a stream that deliberately differs from the bytes the job wrote. A
/// terminal `log_read` that answers from the actor's retained deque returns the written bytes;
/// one that answers from the sealed artifact returns the committed bytes. The retained deque is
/// the second copy this test exists to forbid -- it is also unbounded growth, since the store's
/// per-job output quota is a gigabyte and no terminal record was ever released.
#[tokio::test]
async fn a_terminal_log_read_answers_from_the_sealed_artifact_not_a_retained_copy() {
    let (mut h, _root) = harness_with_sealed_stdout(b"sealed".to_vec());
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"written", b"", ExitStatus::Exited { code: 0 }).await;
    let info = h.handle.wait(job).await.unwrap();
    assert_eq!(info.stdout.bytes, 6);

    let chunk = h
        .handle
        .log_read(job, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!(chunk.bytes, Bytes::from_static(b"sealed"));
    assert_eq!(chunk.next_offset, 6);
    assert!(chunk.eof);

    // Paging from an offset walks the sealed stream, and reading at the end is a clean eof.
    let tail = h
        .handle
        .log_read(job, StreamKind::Stdout, 2, false)
        .await
        .unwrap();
    assert_eq!(tail.bytes, Bytes::from_static(b"aled"));
    assert_eq!(tail.next_offset, 6);
    assert!(tail.eof);

    let end = h
        .handle
        .log_read(job, StreamKind::Stdout, 6, false)
        .await
        .unwrap();
    assert!(end.bytes.is_empty());
    assert!(end.eof);

    // An offset past the committed length is a refusal, not a short read.
    assert_eq!(
        h.handle
            .log_read(job, StreamKind::Stdout, 7, false)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
}

/// A supervisor torn down with its hosting runtime, while a job still runs, was dropped rather
/// than stopped. No later controller knows that job, so it must end with the supervisor instead
/// of running on as an orphan nothing can observe or cancel — asked first, so a job that stops
/// its own children on SIGTERM gets to, and killed after the grace.
#[test]
fn a_dropped_supervisor_ends_the_jobs_it_still_runs() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (mut h, job, spawned, _root) = runtime.block_on(async {
        let (mut h, root) = harness(1, 1024, false, false);
        let job = h
            .handle
            .exec_background(None, None, request(StdinSource::Empty))
            .await
            .unwrap();
        let spawned = h.spawned.recv().await.unwrap();
        (h, job, spawned, root)
    });
    let signals = |observed: Vec<ProcessObservation>| {
        observed
            .into_iter()
            .filter(|observation| matches!(observation, ProcessObservation::Signal(..)))
            .collect::<Vec<_>>()
    };
    let before: Vec<_> = std::iter::from_fn(|| h.process.try_recv().ok()).collect();
    assert_eq!(signals(before), [], "nothing was signalled while it ran");
    drop(runtime);
    let after: Vec<_> = std::iter::from_fn(|| h.process.try_recv().ok()).collect();
    assert_eq!(
        signals(after),
        [
            ProcessObservation::Signal(job, ProcessSignal::Term),
            ProcessObservation::Signal(job, ProcessSignal::Kill),
        ],
        "the running job of a dropped supervisor"
    );
    drop(spawned);
}

/// `handle`'s supervisor served on a fresh socket, and a handle that reaches it only through
/// that socket.
async fn served(handle: &WorkspaceSupervisorHandle) -> (WorkspaceSupervisorHandle, PathBuf) {
    use cowshed_core::runtime::supervisor_socket;
    // Short: a Unix socket path is bounded at 104 bytes on macOS.
    let path = PathBuf::from("/tmp").join(format!(
        "cowshed-sock-{}/s",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    ));
    let listener = supervisor_socket::bind(&path).await.expect("bind");
    tokio::spawn(supervisor_socket::serve(
        listener,
        handle.clone(),
        None,
        None,
    ));
    let remote = supervisor_socket::connect(path.clone(), handle.snapshot().clone());
    (remote, path)
}

#[tokio::test]
async fn a_served_supervisor_runs_a_job_exactly_as_the_in_process_one_does() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let (remote, path) = served(&h.handle).await;
    assert_eq!(
        cowshed_core::runtime::supervisor_socket::hello(&path)
            .await
            .unwrap()
            .authority,
        authority(),
        "the socket names the authority it serves"
    );
    let session = remote.open_session(Some("build".into())).await.unwrap();
    let stdin = Bytes::from_static(&[0xfe, 0x00, b'i', 0x80]);
    let job = remote
        .exec(
            Some(&session),
            None,
            request(StdinSource::Inline(stdin.clone())),
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(spawned.request.job_id, job);
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, stdin)
    );
    let opaque = [0xff, 0x00, 0x80, b'x'];
    complete(
        &spawned,
        &opaque,
        b"err",
        ExitStatus::Signaled {
            signal: 9,
            core_dumped: true,
        },
    )
    .await;
    let info = remote.wait(job).await.unwrap();
    assert_eq!(
        info.exit,
        Some(ExitStatus::Signaled {
            signal: 9,
            core_dumped: true
        })
    );
    assert_eq!(info, h.handle.info(job).await.unwrap());
    let log = remote
        .log_read(job, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!((log.bytes.as_ref(), log.eof), (&opaque[..], true));
    assert_eq!(remote.list().await.unwrap(), vec![info]);
    assert_eq!(
        remote.session_snapshot(&session).await.unwrap(),
        h.handle.session_snapshot(&session).await.unwrap()
    );
}

#[tokio::test]
async fn a_served_supervisor_answers_a_tail_as_the_in_process_one_does() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(
        &spawned,
        b"one\ntwo\n",
        b"err",
        ExitStatus::Exited { code: 0 },
    )
    .await;
    remote.wait(job).await.unwrap();
    let limits = JobTailLimits {
        bytes_per_stream: JobTailBytes::new(64).unwrap(),
        lines_per_stream: std::num::NonZeroU32::new(1).unwrap(),
    };
    let tail = remote.tail(job, None, limits).await.unwrap();
    assert_eq!(tail, h.handle.tail(job, None, limits).await.unwrap());
    assert_eq!(
        (tail.stdout.as_bytes(), tail.stderr.as_bytes()),
        (&b"two\n"[..], &b"err"[..])
    );
    assert_eq!(
        tail.next,
        JobJournalCursor {
            stdout: 8,
            stderr: 3
        }
    );
    let past = JobJournalCursor {
        stdout: 9,
        stderr: 0,
    };
    let error = remote.tail(job, Some(past), limits).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::Usage, "{error:?}");
}

#[tokio::test]
async fn a_streamed_stdin_reaches_a_served_job_whole_and_in_order() {
    use tokio::io::AsyncWriteExt as _;
    let (mut h, _root) = harness(1, 1024, false, false);
    let (remote, _path) = served(&h.handle).await;
    let (mut writer, reader) = tokio::io::duplex(1024);
    let job = remote
        .exec(None, None, request(StdinSource::Stream(Box::pin(reader))))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let sent: Vec<u8> = (0..200_000_u32).map(|index| (index % 251) as u8).collect();
    let payload = sent.clone();
    tokio::spawn(async move {
        writer.write_all(&payload).await.unwrap();
    });
    let mut received = Vec::new();
    loop {
        match h.process.recv().await.unwrap() {
            ProcessObservation::Stdin(id, bytes) => {
                assert_eq!(id, job);
                received.extend_from_slice(&bytes);
            }
            ProcessObservation::StdinClosed(id) => {
                assert_eq!(id, job);
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(received, sent);
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(remote.wait(job).await.unwrap().state, JobState::Exited);
}

fn keyed(key: &str, stdin: StdinSource) -> ExecRequest {
    ExecRequest {
        admission_key: Some(AdmissionKey::new(key).unwrap()),
        ..request(stdin)
    }
}

/// Open remains a distinct wire source with no reader behind it. Its durable stream-kind
/// identity replays the same job; a repeat that supplies a reader receives StdinBound and
/// drops that reader without a single poll, so it can never replace the attachment's input.
#[tokio::test]
async fn a_keyed_open_repeat_reaches_the_same_job_and_never_binds_a_reader() {
    use std::io;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::sync::oneshot;

    struct Unread {
        polls: Arc<AtomicUsize>,
        dropped: Option<oneshot::Sender<()>>,
    }
    impl AsyncRead for Unread {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Err(io::Error::other(
                "a refused reader must never be polled",
            )))
        }
    }
    impl Drop for Unread {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    let (supervisor_config, _root) = isolated_config("keyed-open");
    let mut h = real_store_harness(supervisor_config);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, keyed("op-open", StdinSource::Open))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(
        remote
            .exec(None, None, keyed("op-open", StdinSource::Open))
            .await
            .unwrap(),
        job
    );
    assert_eq!(
        remote
            .job_by_key(AdmissionKey::new("op-open").unwrap())
            .await
            .unwrap(),
        Some(job)
    );
    let before = remote.info(job).await.unwrap().stdin;
    assert_eq!(
        (before.kind, before.bytes, before.complete),
        (StdinKind::Stream, 0, false)
    );
    assert!(h.spawned.try_recv().is_err(), "Open replay spawned nothing");
    assert_eq!(
        h.process.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "Open admission and replay bind no reader and send no EOF"
    );

    let polls = Arc::new(AtomicUsize::new(0));
    let (dropped, reader_dropped) = oneshot::channel();
    let refused = remote
        .exec(
            None,
            None,
            keyed(
                "op-open",
                StdinSource::Stream(Box::pin(Unread {
                    polls: Arc::clone(&polls),
                    dropped: Some(dropped),
                })),
            ),
        )
        .await
        .unwrap_err();
    reader_dropped
        .await
        .expect("the refused reader was dropped");
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(refused.code, ErrorCode::Usage);
    assert_eq!(
        refused.admission_source(),
        Some(&AdmissionRefusal::StdinBound { job_id: job })
    );
    assert!(
        h.spawned.try_recv().is_err(),
        "a second reader spawned nothing"
    );

    let input = Bytes::from_static(b"line\n");
    remote.stdin_write(job, 0, input.clone()).await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, input)
    );
    remote.stdin_close(job).await.unwrap();
    remote.stdin_close(job).await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    assert_eq!(h.process.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    complete(&spawned, b"line\n", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(job).await.unwrap();
    assert_eq!(
        remote
            .exec(None, None, keyed("op-open", StdinSource::Open))
            .await
            .unwrap(),
        job,
        "a terminal Open replay is still the original job"
    );
    assert!(h.spawned.try_recv().is_err());
}

/// 07_api "Keyed admission": a key's first exec binds its stdin stream to the job it admits. A
/// repeat under the key -- the caller lost the reply and exec'd again -- is answered with that job
/// by number in a usage refusal, and its own stream reaches nothing: the job reads the first
/// stream's bytes exactly once, and nothing runs a second time.
#[tokio::test]
async fn a_keyed_stream_repeat_names_its_job_and_binds_no_second_stream() {
    use tokio::io::AsyncWriteExt as _;
    let (supervisor_config, _root) = isolated_config("keyed-stream");
    let mut h = real_store_harness(supervisor_config);
    let (remote, _path) = served(&h.handle).await;
    let (mut writer, reader) = tokio::io::duplex(64);
    let job = remote
        .exec(
            None,
            None,
            keyed("op-1", StdinSource::Stream(Box::pin(reader))),
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();

    let refused = remote
        .exec(
            None,
            None,
            keyed(
                "op-1",
                StdinSource::Stream(Box::pin(std::io::Cursor::new(b"second".to_vec()))),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Usage);
    assert_eq!(
        refused.message,
        format!("stdin already bound to job {}; attach to write", job.get())
    );
    assert_eq!(
        refused.admission_source(),
        Some(&AdmissionRefusal::StdinBound { job_id: job })
    );

    writer.write_all(b"first").await.unwrap();
    drop(writer);
    let mut received = Vec::new();
    loop {
        match h.process.recv().await.unwrap() {
            ProcessObservation::Stdin(id, bytes) => {
                assert_eq!(id, job);
                received.extend_from_slice(&bytes);
            }
            ProcessObservation::StdinClosed(id) => {
                assert_eq!(id, job);
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(received, b"first");
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(remote.wait(job).await.unwrap().state, JobState::Exited);
    assert!(h.spawned.try_recv().is_err(), "the repeat spawned nothing");
    assert!(
        h.process.try_recv().is_err(),
        "no byte of the second stream reached the job"
    );
}

#[tokio::test]
async fn a_keyed_repeat_compares_each_authored_field_and_ignores_withheld_env() {
    let (mut supervisor_config, _root) = isolated_config("keyed-fields");
    supervisor_config
        .credential_env_names
        .insert("TEST_SECRET".to_owned());
    let mut h = real_store_harness(supervisor_config);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, keyed("op-fields", StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"once\n", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(job).await.unwrap();
    let mut same = keyed("op-fields", StdinSource::Empty);
    same.env
        .insert("TEST_SECRET".to_owned(), "not an admitted field".to_owned());
    assert_eq!(remote.exec(None, None, same).await.unwrap(), job);

    let named = remote
        .open_session(Some("different".to_owned()))
        .await
        .unwrap();
    for field in [
        AdmissionField::Command,
        AdmissionField::Cwd,
        AdmissionField::Mode,
        AdmissionField::Env,
        AdmissionField::Stdin,
        AdmissionField::Session,
        AdmissionField::StdoutCopy,
        AdmissionField::StderrCopy,
    ] {
        let mut changed = keyed("op-fields", StdinSource::Empty);
        let mut session = None;
        match field {
            AdmissionField::Command => {
                changed.command = ExecCommand::Argv(vec!["other".into()]);
            }
            AdmissionField::Cwd => changed.cwd = None,
            AdmissionField::Mode => changed.mode = RunSandboxMode::ReadOnly,
            AdmissionField::Env => {
                changed
                    .env
                    .insert("LANG".to_owned(), "different".to_owned());
            }
            AdmissionField::Stdin => {
                changed.stdin = StdinSource::Inline(Bytes::from_static(b"input"));
            }
            AdmissionField::Session => session = Some(&named),
            AdmissionField::StdoutCopy => {
                changed.stdout_copy = Some(OutputPublication {
                    path: WorkspacePath::new("stdout-copy").unwrap(),
                    policy: PublicationPolicy::CreateNew,
                });
            }
            AdmissionField::StderrCopy => {
                changed.stderr_copy = Some(OutputPublication {
                    path: WorkspacePath::new("stderr-copy").unwrap(),
                    policy: PublicationPolicy::CreateNew,
                });
            }
        }
        let refused = remote.exec(session, None, changed).await.unwrap_err();
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert_eq!(
            refused.admission_source(),
            Some(&AdmissionRefusal::KeyConflict {
                job_id: job,
                fields: vec![field],
            }),
            "the changed field is {field:?}"
        );
    }
    assert!(
        h.spawned.try_recv().is_err(),
        "every repeat spawned nothing"
    );
}

#[tokio::test]
async fn an_absent_key_is_none_only_for_the_current_supervisor_authority() {
    use cowshed_core::runtime::supervisor_socket;
    let (supervisor_config, _root) = isolated_config("keyed-absent");
    let mut h = real_store_harness(supervisor_config);
    let (remote, path) = served(&h.handle).await;
    let key = AdmissionKey::new("op-absent").unwrap();
    assert_eq!(h.handle.job_by_key(key.clone()).await.unwrap(), None);
    assert_eq!(remote.job_by_key(key.clone()).await.unwrap(), None);

    let current = h.handle.snapshot();
    let mut stale_incarnation = current.clone();
    stale_incarnation.workspace_incarnation =
        WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c81").unwrap();
    let mut stale_grant = current.clone();
    stale_grant.grant_revision += 1;
    let mut stale_lifecycle = current.clone();
    stale_lifecycle.lifecycle_revision += 1;
    let expected = CowshedError::conflict(
        "workspace supervisor authority is stale",
        "reattach the workspace and retry with its current incarnation and revisions",
    );
    for authority in [stale_incarnation, stale_grant, stale_lifecycle] {
        let stale = supervisor_socket::connect(path.clone(), authority);
        assert_eq!(stale.job_by_key(key.clone()).await.unwrap_err(), expected);
    }
    assert_eq!(
        remote.info(JobId::new(1).unwrap()).await.unwrap_err().code,
        ErrorCode::NotFound,
        "a missing job remains an error, not a missing admission key"
    );
    let unreachable = path.with_file_name("unavailable");
    let unavailable = supervisor_socket::connect(unreachable.clone(), current.clone());
    let error = unavailable.job_by_key(key).await.unwrap_err();
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert!(error.message.contains(&unreachable.display().to_string()));
    assert_eq!(
        error.hint,
        "retry; cowshed restarts the workspace supervisor"
    );
    assert!(h.spawned.try_recv().is_err(), "lookup never spawned a job");
}

/// The reply is the killpoint: a caller sends a keyed exec over the supervisor socket and hangs up
/// without reading a byte back, so the job's number reaches no one, and the supervisor that ran
/// the job is gone before anyone asks again. The next supervisor answers the key from the records
/// alone -- the same request gets the same job, `jobByKey` reaches it, and the spawn sink saw one
/// run -- while a changed request under the key is a conflict naming what changed.
#[tokio::test]
async fn a_keyed_exec_whose_reply_was_lost_answers_its_one_job_from_the_next_supervisor() {
    use tokio::io::AsyncWriteExt as _;
    let (supervisor_config, _root) = isolated_config("keyed-lost-reply");
    let payload = || StdinSource::Inline(Bytes::from_static(b"payload"));
    let mut first = real_store_harness(supervisor_config.clone());
    let (_remote, path) = served(&first.handle).await;
    let authority = first.handle.current_authority().await.unwrap();
    let sent = request(StdinSource::Empty);
    let ExecCommand::Argv(argv) = &sent.command else {
        unreachable!("request() runs an argv")
    };
    let call = serde_json::to_vec(&serde_json::json!({
        "call": {
            "authority": {
                "repoId": authority.repo_id,
                "workspace": authority.workspace,
                "workspaceIncarnation": authority.workspace_incarnation,
                "grantRevision": authority.grant_revision,
                "lifecycleRevision": authority.lifecycle_revision,
            },
            "call": {
                "call": "exec",
                "session": null,
                "buildVolume": null,
                "background": false,
                "request": {
                    "argv": argv,
                    "script": null,
                    "cwd": sent.cwd,
                    "mode": sent.mode,
                    "env": sent.env,
                    "trace": null,
                    "stdin": { "kind": "inline" },
                    "stdoutCopy": null,
                    "stderrCopy": null,
                    "admissionKey": "op-1",
                },
            },
            "bytes": b"payload".len(),
        }
    }))
    .unwrap();
    let mut caller = tokio::net::UnixStream::connect(&path).await.unwrap();
    for frame in [&call[..], &b"payload"[..]] {
        caller
            .write_all(&u32::try_from(frame.len()).unwrap().to_be_bytes())
            .await
            .unwrap();
        caller.write_all(frame).await.unwrap();
    }
    let spawned = first.spawned.recv().await.unwrap();
    // Nothing was read: whatever the supervisor answered dies with the caller's socket.
    drop(caller);
    let job = spawned.request.job_id;
    complete(&spawned, b"ran\n", b"", ExitStatus::Exited { code: 0 }).await;
    first.handle.wait(job).await.unwrap();
    first.handle.quiesce().await.unwrap();
    first.handle.retire().await.unwrap();
    drop(first);

    let mut second = real_store_harness(supervisor_config);
    let (remote, _path) = served(&second.handle).await;
    let key = AdmissionKey::new("op-1").unwrap();
    assert_eq!(remote.job_by_key(key.clone()).await.unwrap(), Some(job));
    assert_eq!(
        remote
            .exec(None, None, keyed("op-1", payload()))
            .await
            .unwrap(),
        job
    );

    let changed_argv = ExecRequest {
        command: ExecCommand::Argv(vec!["printf".into(), "other".into()]),
        ..keyed("op-1", payload())
    };
    let conflict = remote.exec(None, None, changed_argv).await.unwrap_err();
    assert_eq!(conflict.code, ErrorCode::Conflict);
    assert_eq!(
        conflict.admission_source(),
        Some(&AdmissionRefusal::KeyConflict {
            job_id: job,
            fields: vec![AdmissionField::Command],
        })
    );
    let changed_bytes = remote
        .exec(
            None,
            None,
            keyed("op-1", StdinSource::Inline(Bytes::from_static(b"payloaD"))),
        )
        .await
        .unwrap_err();
    assert_eq!(
        changed_bytes.admission_source(),
        Some(&AdmissionRefusal::KeyConflict {
            job_id: job,
            fields: vec![AdmissionField::Stdin],
        })
    );
    assert_eq!(
        remote
            .job_by_key(AdmissionKey::new("op-2").unwrap())
            .await
            .unwrap(),
        None
    );
    assert!(
        second.spawned.try_recv().is_err(),
        "the job ran once: nothing spawned again"
    );
}

/// A job store set aside in a keyed layout for this incarnation may hold any key: a keyed exec
/// is refused with that reason, never spawned, and `jobByKey` says the same, never absence.
#[tokio::test]
async fn a_key_a_set_aside_store_may_hold_is_refused_and_never_spawned() {
    use std::os::unix::fs::DirBuilderExt as _;
    let (supervisor_config, _root) = isolated_config("keyed-unprovable");
    let set_aside = supervisor_config
        .workspace_root
        .join(".cowshed/job/set-aside")
        .join(format!("layout-{FIRST_KEYED_LAYOUT}"));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&set_aside)
        .unwrap();
    let mut h = real_store_harness(supervisor_config);
    let unprovable = AdmissionRefusal::Unprovable {
        set_aside: set_aside.clone(),
    };
    let refused = h
        .handle
        .exec(None, None, keyed("op-1", StdinSource::Empty))
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert_eq!(refused.admission_source(), Some(&unprovable));
    let lookup = h
        .handle
        .job_by_key(AdmissionKey::new("op-1").unwrap())
        .await
        .unwrap_err();
    assert_eq!(lookup.admission_source(), Some(&unprovable));
    assert!(h.spawned.try_recv().is_err(), "nothing ran under the key");
    let (remote, _path) = served(&h.handle).await;
    assert_eq!(
        remote
            .job_by_key(AdmissionKey::new("op-1").unwrap())
            .await
            .unwrap_err(),
        lookup,
        "the supervisor socket preserves the complete unprovable error"
    );
}

#[test]
fn a_corrupt_or_unreadable_admission_history_refuses_the_supervisor_store() {
    use cowshed_core::storage::job_artifact::ArtifactError;
    use std::os::unix::fs::DirBuilderExt as _;

    for unreadable in [false, true] {
        let (config, _root) = isolated_config("keyed-refused-store");
        let set_aside = config
            .workspace_root
            .join(".cowshed/job/set-aside")
            .join(format!("layout-{FIRST_KEYED_LAYOUT}"));
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&set_aside)
            .unwrap();
        let marker = set_aside.join("incarnation");
        if unreadable {
            std::fs::create_dir(&marker).unwrap();
        } else {
            std::fs::write(&marker, b"not-an-incarnation").unwrap();
        }
        let source = ArtifactStore::open(
            &config.workspace_root,
            config.owned_repo_ids.clone(),
            config.authority.workspace_incarnation.clone(),
            config.artifacts.clone(),
        )
        .err()
        .expect("the store cannot prove admission authority");
        if unreadable {
            assert!(matches!(&source, ArtifactError::Io { path, .. } if path == &marker));
        } else {
            assert!(matches!(&source, ArtifactError::Integrity { .. }));
        }
        let error = ArtifactStoreSink::open(
            config.workspace_root,
            &config.owned_repo_ids,
            &config.authority,
            config.artifacts,
        )
        .err()
        .expect("a supervisor cannot serve lookup from the refused store");
        assert_eq!(
            error,
            CowshedError::integrity(source.to_string(), "cowshed doctor --json"),
            "the original store failure survives the supervisor boundary"
        );
    }
}

/// A job admitted with open stdin keeps it open for its attachment, over the socket the
/// controller reaches its supervisor through: a written line reaches the job and its echo is
/// readable before any EOF; ending stdin twice delivers one EOF; a later write is a conflict;
/// and neither ends the job, which exits 0 on its own.
#[tokio::test]
async fn an_open_stdin_answers_before_one_eof_and_never_ends_the_job() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, request(StdinSource::Open))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let admitted = remote.info(job).await.unwrap().stdin;
    assert_eq!(
        (admitted.kind, admitted.complete),
        (StdinKind::Stream, false)
    );

    let line = Bytes::from_static(b"line\n");
    remote.stdin_write(job, 0, line.clone()).await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, line.clone()),
        "the line reached the job before anything ended its stdin"
    );
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id: job,
            stream: StreamKind::Stdout,
            bytes: line.clone(),
        })
        .await
        .unwrap();
    let echoed = remote
        .log_read(job, StreamKind::Stdout, 0, true)
        .await
        .unwrap();
    assert_eq!((echoed.bytes, echoed.eof), (line, false));

    remote.stdin_close(job).await.unwrap();
    remote.stdin_close(job).await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    let late = remote
        .stdin_write(job, 5, Bytes::from_static(b"late\n"))
        .await
        .unwrap_err();
    assert_eq!(late.code, ErrorCode::Conflict, "{late:?}");
    assert_eq!(
        late.stdin_source(),
        Some(cowshed_core::StdinRefusal::Ended { cursor: 5 })
    );
    // Both closes and the refused write were answered after the actor acted on them, so a
    // second EOF or a late byte would already be queued here.
    assert_eq!(
        h.process.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "one EOF and nothing after it reached the job"
    );
    let running = remote.info(job).await.unwrap();
    assert_eq!(running.state, JobState::Running, "EOF did not end the job");
    assert_eq!((running.stdin.bytes, running.stdin.complete), (5, true));

    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let ended = remote.wait(job).await.unwrap();
    assert_eq!(
        (ended.state, ended.exit),
        (JobState::Exited, Some(ExitStatus::Exited { code: 0 }))
    );
}

/// A close that finds the job's last write undelivered waits for it, refuses every later write
/// at once, and sends its one EOF only after the pump reports the preceding write delivered.
#[tokio::test]
async fn a_close_behind_a_busy_lane_waits_for_it_and_refuses_later_writes() {
    let (mut h, _root) = harness(1, 1024, false, true);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Open))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let last = tokio::spawn(async move {
        handle
            .stdin_write(job, 0, Bytes::from_static(b"last\n"))
            .await
    });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"last\n"))
    );

    // The close reaches the actor before the write: each call is sent on its first poll.
    let close = h.handle.stdin_close(job);
    tokio::pin!(close);
    let late = tokio::select! {
        biased;
        closed = &mut close => panic!("the close answered while the lane was busy: {closed:?}"),
        late = h.handle.stdin_write(job, 5, Bytes::from_static(b"late\n")) => late,
    };
    assert_eq!(late.unwrap_err().code, ErrorCode::Conflict);
    assert_eq!(
        h.process.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "no EOF while the lane still holds the write"
    );

    spawned
        .events
        .send(ProcessEvent::StdinReady { job_id: job })
        .await
        .unwrap();
    last.await.unwrap().unwrap();
    close.await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let ended = h.handle.wait(job).await.unwrap();
    assert_eq!(
        (ended.exit, ended.stdin.bytes, ended.stdin.complete),
        (Some(ExitStatus::Exited { code: 0 }), 5, true)
    );
}

/// A writer that repeats bytes the job already holds -- the answer to its write was lost -- is
/// answered without anything reaching the job twice, but only when the bytes are the ones the
/// job received: other bytes over the same range are a typed conflict naming the cursor, and so
/// is a write that neither continues the input nor lies within it.
#[tokio::test]
async fn a_repeated_write_is_answered_once_and_only_with_the_bytes_delivered() {
    use cowshed_core::StdinRefusal;
    let (mut h, _root) = harness(1, 1024, false, false);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, request(StdinSource::Open))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    remote
        .stdin_write(job, 0, Bytes::from_static(b"abc"))
        .await
        .unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"abc"))
    );

    for (offset, bytes) in [(0, &b"abc"[..]), (1, &b"bc"[..]), (3, &b""[..])] {
        remote
            .stdin_write(job, offset, Bytes::from_static(bytes))
            .await
            .unwrap_or_else(|error| panic!("repeat at {offset}: {error:?}"));
    }
    let refusal = |offset, bytes: &'static [u8]| {
        let remote = remote.clone();
        async move {
            let error = remote
                .stdin_write(job, offset, Bytes::from_static(bytes))
                .await
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::Conflict, "{error:?}");
            error.stdin_source()
        }
    };
    assert_eq!(
        refusal(0, b"abX").await,
        Some(StdinRefusal::ReplayMismatch {
            offset: 0,
            cursor: 3
        })
    );
    assert_eq!(
        refusal(2, b"cde").await,
        Some(StdinRefusal::Discontinuous {
            offset: 2,
            cursor: 3,
            admitted: 3
        })
    );
    assert_eq!(
        refusal(5, b"z").await,
        Some(StdinRefusal::Discontinuous {
            offset: 5,
            cursor: 3,
            admitted: 3
        })
    );
    // Every call above was answered after the actor acted on it: a repeated byte would be here.
    assert_eq!(
        h.process.try_recv(),
        Err(mpsc::error::TryRecvError::Empty),
        "nothing reached the job twice"
    );
    assert_eq!(remote.info(job).await.unwrap().stdin.bytes, 3);

    remote
        .stdin_write(job, 3, Bytes::from_static(b"def"))
        .await
        .unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"def"))
    );
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(remote.wait(job).await.unwrap().stdin.bytes, 6);
}

/// A write the pipe refused partway leaves how much of it reached the job unknown: the write in
/// flight is refused with the cursor before it, and so is every later write and close, however
/// exactly they repeat it. The job itself runs on.
#[tokio::test]
async fn a_partly_delivered_write_refuses_every_later_write_with_the_last_known_cursor() {
    use cowshed_core::StdinRefusal;
    let (mut h, _root) = harness(1, 1024, false, true);
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Open))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let delivered = tokio::spawn(async move {
        handle
            .stdin_write(job, 0, Bytes::from_static(b"kept"))
            .await
    });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"kept"))
    );
    spawned
        .events
        .send(ProcessEvent::StdinReady { job_id: job })
        .await
        .unwrap();
    delivered.await.unwrap().unwrap();

    // The fake's lane refuses its second write, so it waits in the queue -- sent on its first
    // poll, ahead of the status read behind it -- and the lane takes it as its third.
    let queued = h.handle.stdin_write(job, 4, Bytes::from_static(b"lost"));
    tokio::pin!(queued);
    let sent = std::future::poll_fn(|context| {
        std::task::Poll::Ready(queued.as_mut().poll(context).is_pending())
    })
    .await;
    assert!(sent, "the write waits for the lane");
    assert_eq!(h.handle.info(job).await.unwrap().stdin.bytes, 4);
    spawned
        .events
        .send(ProcessEvent::StdinReady { job_id: job })
        .await
        .unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Stdin(job, Bytes::from_static(b"lost"))
    );
    spawned
        .events
        .send(ProcessEvent::StdinBroken {
            job_id: job,
            error: "Broken pipe (os error 32)".into(),
        })
        .await
        .unwrap();
    let unknown = Some(StdinRefusal::DeliveryUnknown { cursor: 4 });
    let lost = queued.await.unwrap_err();
    assert_eq!(lost.stdin_source(), unknown, "{lost:?}");
    assert!(lost.message.contains("Broken pipe"), "{}", lost.message);
    for repeat in [
        h.handle
            .stdin_write(job, 4, Bytes::from_static(b"lost"))
            .await,
        h.handle
            .stdin_write(job, 0, Bytes::from_static(b"kept"))
            .await,
        h.handle.stdin_close(job).await,
    ] {
        assert_eq!(repeat.unwrap_err().stdin_source(), unknown);
    }
    let info = h.handle.info(job).await.unwrap();
    assert_eq!(
        (info.state, info.stdin.bytes, info.stdin.complete),
        (JobState::Running, 4, false)
    );
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(h.handle.wait(job).await.unwrap().state, JobState::Exited);
}

#[tokio::test]
async fn a_served_supervisor_refuses_a_caller_that_holds_another_authority() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let (_remote, path) = served(&h.handle).await;
    let stale = cowshed_core::runtime::supervisor_socket::connect(
        path,
        WorkspaceAuthoritySnapshot {
            grant_revision: authority().grant_revision + 1,
            ..authority()
        },
    );
    let refused = stale
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert!(
        h.spawned.try_recv().is_err(),
        "nothing ran for the stale caller"
    );
}

#[tokio::test]
async fn a_pending_wait_holds_up_no_other_call_to_a_served_supervisor() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let waiting = {
        let remote = remote.clone();
        tokio::spawn(async move { remote.wait(job).await })
    };
    let follow = {
        let remote = remote.clone();
        tokio::spawn(async move { remote.log_read(job, StreamKind::Stdout, 0, true).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        remote.info(job).await.unwrap().state,
        JobState::Running,
        "a call made while a wait and a follow are pending is answered"
    );
    assert!(!waiting.is_finished() && !follow.is_finished());
    complete(&spawned, b"late", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(follow.await.unwrap().unwrap().bytes.as_ref(), b"late");
    assert_eq!(waiting.await.unwrap().unwrap().state, JobState::Exited);
}

/// A supervisor of another build is refused by name, whether it names that build or, having
/// been built before builds were named, none.
#[tokio::test]
async fn a_supervisor_of_another_build_is_refused_by_name() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    for (answer, named) in [
        (
            &br#"{"ok":{"value":{"build":"another build","shape":"future"},"bytes":0}}"#[..],
            "is cowshed build another build",
        ),
        (
            &br#"{"ok":{"value":{"protocol":2,"pid":1},"bytes":0}}"#[..],
            "is cowshed build (unnamed)",
        ),
    ] {
        let path = PathBuf::from("/tmp").join(format!(
            "cowshed-sock-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut request = vec![0_u8; u32::from_be_bytes(length) as usize];
            stream.read_exact(&mut request).await.unwrap();
            stream
                .write_all(&u32::try_from(answer.len()).unwrap().to_be_bytes())
                .await
                .unwrap();
            stream.write_all(answer).await.unwrap();
        });
        let refused = cowshed_core::runtime::supervisor_socket::hello(&path)
            .await
            .unwrap_err();
        std::fs::remove_file(&path).ok();
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert!(refused.message.contains(named), "{}", refused.message);
    }
}

/// Starts `supervisor` on the workspace socket in-process, standing in for the supervisor
/// process, and hands the manager a real child to watch.
struct InProcessSpawner {
    store_root: PathBuf,
    supervisor: Option<WorkspaceSupervisorHandle>,
    child: &'static [&'static str],
    spawned: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl cowshed_core::runtime::supervisor_manager::SupervisorSpawner for InProcessSpawner {
    fn spawn(
        &self,
        _project_root: &std::path::Path,
        workspace: &WorkspaceName,
        _report: std::io::PipeWriter,
    ) -> std::io::Result<tokio::process::Child> {
        use cowshed_core::runtime::supervisor_socket;
        self.spawned
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(supervisor) = self.supervisor.clone() {
            let path = supervisor_socket::socket_path(
                &self.store_root,
                &supervisor.snapshot().repo_id,
                workspace,
            );
            tokio::spawn(async move {
                // A real supervisor takes a moment to open its project before it serves.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let listener = supervisor_socket::bind(&path).await.expect("bind");
                supervisor_socket::serve(listener, supervisor, None, None).await
            });
        }
        tokio::process::Command::new(self.child[0])
            .args(&self.child[1..])
            .kill_on_drop(true)
            .spawn_locked()
    }
}

fn manager_store() -> PathBuf {
    PathBuf::from("/tmp").join(format!(
        "cowshed-mgr-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    ))
}

/// A daemon whose startup pass has finished, so the manager serves ensures.
fn healed() -> std::sync::Arc<cowshed_core::StartupHealState> {
    std::sync::Arc::new(cowshed_core::StartupHealState::healed())
}

#[tokio::test]
async fn the_manager_starts_one_supervisor_for_concurrent_ensures() {
    use cowshed_core::runtime::supervisor_manager::SupervisorManager;
    let (h, _root) = harness(1, 1024, false, false);
    let store = manager_store();
    let spawned = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let manager = SupervisorManager::new(
        &store,
        Box::new(InProcessSpawner {
            store_root: store.clone(),
            supervisor: Some(h.handle.clone()),
            child: &["sleep", "30"],
            spawned: std::sync::Arc::clone(&spawned),
        }),
        healed(),
    );
    let project = PathBuf::from("/nonexistent/project");
    let needed = authority();
    let (first, second) = tokio::join!(
        manager.ensure(&project, &needed),
        manager.ensure(&project, &needed)
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.socket, second.socket);
    assert_eq!(first.authority, authority());
    assert_eq!(
        spawned.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "two controllers asking at once share one supervisor"
    );
    let again = manager.ensure(&project, &authority()).await.unwrap();
    assert_eq!(again.pid, first.pid);
    assert_eq!(spawned.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_manager_reports_a_supervisor_that_exits_before_serving() {
    use cowshed_core::runtime::supervisor_manager::SupervisorManager;
    let store = manager_store();
    let manager = SupervisorManager::new(
        &store,
        Box::new(InProcessSpawner {
            store_root: store.clone(),
            supervisor: None,
            child: &["sh", "-c", "exit 3"],
            spawned: std::sync::Arc::default(),
        }),
        healed(),
    );
    let refused = manager
        .ensure(&PathBuf::from("/nonexistent/project"), &authority())
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::EnvironmentMissing);
    assert!(
        refused.message.contains("exited") && refused.message.contains('3'),
        "{}",
        refused.message
    );
}

/// A supervisor that cannot start says why on the report pipe the manager handed it, and the
/// command that asked for the workspace fails with that reason and its code, not a pointer to
/// the daemon's log.
#[tokio::test]
async fn a_supervisor_that_cannot_start_fails_the_ensure_with_its_own_reason() {
    use cowshed_core::runtime::supervisor_manager::{
        ProgramSpawner, START_REPORT_FD_ENV, SupervisorManager,
    };
    let reason = CowshedError::conflict(
        "the record at byte 123080 is in a newer record layout",
        "run the cowshed that wrote these records",
    );
    let json = serde_json::to_string(&reason).unwrap();
    assert!(!json.contains('\''));
    let script = format!("printf '%s' '{json}' >&\"${START_REPORT_FD_ENV}\"; exit 4");
    let store = manager_store();
    let manager = SupervisorManager::new(
        &store,
        Box::new(ProgramSpawner::new(
            "/bin/sh",
            vec!["-c".into(), script.into(), "sh".into()],
        )),
        healed(),
    );
    let refused = manager
        .ensure(&PathBuf::from("/nonexistent/project"), &authority())
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict, "{}", refused.message);
    assert!(
        refused.message.contains("could not start") && refused.message.ends_with(&reason.message),
        "{}",
        refused.message
    );
    assert_eq!(refused.hint, reason.hint);
}

#[tokio::test]
async fn the_manager_moves_a_running_supervisor_to_a_newer_grant_revision() {
    use cowshed_core::runtime::{supervisor_manager::SupervisorManager, supervisor_socket};
    let (mut h, root) = harness(1, 1024, false, false);
    let store = manager_store();
    let path = supervisor_socket::socket_path(&store, &authority().repo_id, &authority().workspace);
    let listener = supervisor_socket::bind(&path).await.unwrap();
    let (advances, mut requests) = mpsc::channel(1);
    tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        Some(advances),
        None,
    ));
    let newer = WorkspaceAuthoritySnapshot {
        grant_revision: authority().grant_revision + 1,
        ..authority()
    };
    let actor = h.handle.clone();
    let sandbox = config(&root).sandbox;
    let answering = tokio::spawn(async move {
        let reply: tokio::sync::oneshot::Sender<Result<WorkspaceAuthoritySnapshot>> =
            requests.recv().await.expect("an advance request");
        let advanced = actor
            .advance_authority(
                authority().grant_revision + 1,
                authority().lifecycle_revision,
                sandbox,
            )
            .await
            .map(|advanced| advanced.snapshot().clone());
        reply.send(advanced).unwrap();
    });
    let spawned = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let manager = SupervisorManager::new(
        &store,
        Box::new(InProcessSpawner {
            store_root: store.clone(),
            supervisor: None,
            child: &["sleep", "30"],
            spawned: std::sync::Arc::clone(&spawned),
        }),
        healed(),
    );
    let ensured = manager
        .ensure(&PathBuf::from("/nonexistent/project"), &newer)
        .await
        .unwrap();
    answering.await.unwrap();
    assert_eq!(ensured.authority, newer);
    assert_eq!(
        spawned.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no second supervisor"
    );
    let remote = supervisor_socket::connect(ensured.socket, ensured.authority);
    let admitted = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(admitted).await.unwrap();
    remote.retire().await.unwrap();
}

/// A grant change can land between a controller's read of the grants and its ensure — another
/// process's gateway reconcile moving the workspace's port block, a `grant`, a project-wide
/// grant — and the supervisor started or advanced since serves the newer revision. The
/// controller holding the older read is answered by that supervisor, under the authority it
/// reports, which every call then names: never refused, never served under grants older than
/// the ones it read, and never given a second allocator.
#[tokio::test]
async fn an_ensure_from_a_stale_grant_read_is_answered_by_the_newer_supervisor() {
    use cowshed_core::runtime::{supervisor_manager::SupervisorManager, supervisor_socket};
    let (mut h, root) = harness(1, 1024, false, false);
    let store = manager_store();
    // The controller reads the grants...
    let read = authority();
    // ...then the grant change lands and the workspace's supervisor serves its revision.
    let served = h
        .handle
        .advance_authority(
            read.grant_revision + 1,
            read.lifecycle_revision,
            config(&root).sandbox,
        )
        .await
        .unwrap();
    let path = supervisor_socket::socket_path(&store, &read.repo_id, &read.workspace);
    let listener = supervisor_socket::bind(&path).await.unwrap();
    tokio::spawn(supervisor_socket::serve(
        listener,
        served.clone(),
        None,
        None,
    ));
    let spawned = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let manager = SupervisorManager::new(
        &store,
        Box::new(InProcessSpawner {
            store_root: store.clone(),
            supervisor: None,
            child: &["sleep", "30"],
            spawned: std::sync::Arc::clone(&spawned),
        }),
        healed(),
    );

    let ensured = manager
        .ensure(&PathBuf::from("/nonexistent/project"), &read)
        .await
        .unwrap();
    assert_eq!(&ensured.authority, served.snapshot());
    assert_eq!(
        spawned.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no second supervisor"
    );
    let remote = supervisor_socket::connect(ensured.socket, ensured.authority);
    let admitted = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = h.spawned.recv().await.unwrap();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(
        remote.wait(admitted).await.unwrap().grant_revision,
        read.grant_revision + 1,
        "the job runs under the newer grants"
    );
    remote.retire().await.unwrap();
}

/// Revisions of one incarnation only grow, so the newer revision is the only one a stale read
/// may be answered under: a supervisor of another incarnation is still refused by name.
#[tokio::test]
async fn an_ensure_is_refused_by_a_supervisor_of_another_incarnation() {
    use cowshed_core::runtime::{supervisor_manager::SupervisorManager, supervisor_socket};
    let (h, _root) = harness(1, 1024, false, false);
    let store = manager_store();
    let path = supervisor_socket::socket_path(&store, &authority().repo_id, &authority().workspace);
    let listener = supervisor_socket::bind(&path).await.unwrap();
    tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        None,
        None,
    ));
    let manager = SupervisorManager::new(
        &store,
        Box::new(InProcessSpawner {
            store_root: store.clone(),
            supervisor: None,
            child: &["sleep", "30"],
            spawned: std::sync::Arc::default(),
        }),
        healed(),
    );
    let successor = WorkspaceAuthoritySnapshot {
        workspace_incarnation: WorkspaceIncarnation::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap(),
        grant_revision: authority().grant_revision + 1,
        ..authority()
    };
    let refused = manager
        .ensure(&PathBuf::from("/nonexistent/project"), &successor)
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict, "{}", refused.message);
    h.handle.retire().await.unwrap();
}

#[tokio::test]
async fn a_drained_supervisor_finishes_its_jobs_admits_none_and_stops_serving() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let path = PathBuf::from("/tmp").join(format!(
        "cowshed-sock-{}/s",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    ));
    let listener = supervisor_socket::bind(&path).await.unwrap();
    let server = tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        None,
        None,
    ));
    let remote = supervisor_socket::connect(path.clone(), authority());
    let job = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();

    assert_eq!(
        supervisor_socket::drain(&path).await.unwrap().pid(),
        std::process::id(),
        "drain answers at once with the serving pid"
    );
    let refused = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match remote.exec(None, None, request(StdinSource::Empty)).await {
                Err(error) => return error,
                // Admitted before the drain reached the actor: let it end and try again.
                Ok(admitted) => {
                    let spawned = h.spawned.recv().await.unwrap();
                    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
                    remote.wait(admitted).await.unwrap();
                }
            }
        }
    })
    .await
    .expect("a draining supervisor refuses new work");
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert!(!server.is_finished(), "the running job holds it serving");

    // A wait already in flight is answered even as the supervisor stops serving.
    let waiting = {
        let remote = remote.clone();
        tokio::spawn(async move { remote.wait(job).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    complete(&spawned, b"done", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(waiting.await.unwrap().unwrap().state, JobState::Exited);
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("it stops serving once its jobs ended")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_controller_reads_a_served_supervisor_s_commitments_by_cursor_until_it_acknowledges() {
    use cowshed_core::runtime::commitment_feed::{CommitmentFeed, FeedingSink};
    use cowshed_core::runtime::supervisor_socket;
    let root = workspace_root("commitment-feed");
    let (spawn_tx, mut spawned) = mpsc::unbounded_channel();
    let (process_tx, _process) = mpsc::unbounded_channel();
    let (artifact_tx, _artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, _commitments) = mpsc::unbounded_channel();
    let (order_tx, _order) = mpsc::unbounded_channel();
    let feed = CommitmentFeed::default();
    let handle = WorkspaceSupervisor::start_with_sinks(
        config(&root),
        Box::new(FakeSpawner {
            spawned: spawn_tx,
            process_observations: process_tx,
            fail_next: false,
            backpressure: false,
            order: order_tx.clone(),
        }),
        Box::new(FakeArtifactSink {
            sealed_stdout: None,
            next: JobId::new(1).unwrap(),
            next_barrier: 1,
            quota: 1024,
            jobs: BTreeMap::new(),
            observations: artifact_tx,
            order: order_tx.clone(),
        }),
        Box::new(FeedingSink {
            inner: FakeCommitments {
                next_order: 1,
                observations: commitment_tx,
                order: order_tx,
            },
            feed: feed.clone(),
        }),
    )
    .unwrap();
    let path = PathBuf::from("/tmp").join(format!(
        "cowshed-sock-{}/s",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    ));
    let listener = supervisor_socket::bind(&path).await.unwrap();
    tokio::spawn(supervisor_socket::serve(
        listener,
        handle.clone(),
        None,
        Some(feed),
    ));

    let first = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    complete(
        &spawned.recv().await.unwrap(),
        b"",
        b"",
        ExitStatus::Exited { code: 0 },
    )
    .await;
    handle.wait(first).await.unwrap();

    let page = supervisor_socket::commitments(&path, 0).await.unwrap();
    let kinds: Vec<(u64, &'static str)> = page
        .entries
        .iter()
        .map(|entry| {
            (
                entry.cursor,
                match entry.draft {
                    CommitmentDraft::Admission { .. } => "admission",
                    CommitmentDraft::Terminal { .. } => "terminal",
                    _ => "other",
                },
            )
        })
        .collect();
    assert_eq!(kinds, vec![(1, "admission"), (2, "terminal")]);
    assert_eq!(page.lost_through, None);

    supervisor_socket::acknowledge_commitments(&path, 2)
        .await
        .unwrap();
    let second = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    complete(
        &spawned.recv().await.unwrap(),
        b"",
        b"",
        ExitStatus::Exited { code: 0 },
    )
    .await;
    handle.wait(second).await.unwrap();
    let cursors: Vec<u64> = supervisor_socket::commitments(&path, 0)
        .await
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.cursor)
        .collect();
    assert_eq!(cursors, vec![3, 4], "acknowledged commitments are gone");
}

#[tokio::test]
async fn a_job_s_durable_record_keeps_its_exit_and_duration() {
    let (supervisor_config, _root) = isolated_config("job-record");
    let records = supervisor_config
        .workspace_root
        .join(".cowshed/job/records.arrow");
    let mut h = real_store_harness(supervisor_config);
    let job_id = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"built\n", b"", ExitStatus::Exited { code: 0 }).await;
    let info = h.handle.wait(job_id).await.unwrap();

    let terminal = cowshed_core::storage::job_artifact::recover_records(&records)
        .unwrap()
        .frames
        .into_iter()
        .find_map(|frame| match frame.record {
            cowshed_core::storage::job_artifact::ProtectedRecord::Job(record)
                if record.job_id == job_id && record.state == JobState::Exited =>
            {
                Some(record)
            }
            _ => None,
        })
        .expect("the job's terminal record");
    assert_eq!(terminal.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(terminal.duration_ms, info.duration_ms);
    assert!(terminal.duration_ms.is_some());
}

/// A warm-shell job's spawn sink: the job owns no process when it is admitted; its processes
/// arrive as `Activating`/`Started` events the test sends.
struct WarmSpawner {
    spawned: mpsc::UnboundedSender<Spawned>,
}

struct WarmProcess;

impl RunningProcess for WarmProcess {
    fn process(&self) -> Option<&OwnedProcess> {
        None
    }

    fn try_write_stdin(&mut self, _bytes: Bytes) -> Result<bool> {
        Ok(true)
    }

    fn close_stdin(&mut self) -> bool {
        true
    }

    fn end_stdin(&mut self) {}

    fn signal_process_tree(&mut self, _signal: ProcessSignal) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SpawnSink for WarmSpawner {
    async fn spawn(
        &mut self,
        request: ProcessSpawnRequest,
        events: mpsc::Sender<ProcessEvent>,
    ) -> Result<Box<dyn RunningProcess>> {
        self.spawned
            .send(Spawned { request, events })
            .expect("spawn observer");
        Ok(Box::new(WarmProcess))
    }
}

fn warm_harness(supervisor_config: WorkspaceSupervisorConfig) -> Harness {
    let (spawn_tx, spawned) = mpsc::unbounded_channel();
    let (_process_tx, process) = mpsc::unbounded_channel();
    let (_artifact_tx, artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, commitments) = mpsc::unbounded_channel();
    let (order_tx, order) = mpsc::unbounded_channel();
    let store = ArtifactStoreSink::open(
        supervisor_config.workspace_root.clone(),
        &supervisor_config.owned_repo_ids,
        &supervisor_config.authority,
        supervisor_config.artifacts.clone(),
    )
    .expect("open artifact store");
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
        Box::new(WarmSpawner { spawned: spawn_tx }),
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
    }
}

/// A group this test owns whose one process is its leader, held unreaped until [`end_group`].
/// Resolve `sleep` through the toolchain PATH; NixOS has no `/bin/sleep`.
fn lone_group() -> std::process::Child {
    use std::os::unix::process::CommandExt as _;
    std::process::Command::new("sleep")
        .arg("300")
        .stdin(std::process::Stdio::null())
        .process_group(0)
        .spawn_locked()
        .expect("a test-owned process group")
}

/// Kill the group `leader` leads and wait for the leader's exit without reaping it: like a job's
/// parent, the test holds it until the job concluded ([`reap`]).
fn end_group(leader: &std::process::Child) {
    let pgid = i32::try_from(leader.id()).unwrap();
    // SAFETY: the unreaped test child leads this group.
    assert_eq!(unsafe { libc::killpg(pgid, libc::SIGKILL) }, 0);
    exited_unreaped(leader);
}

/// Wait for `child` to exit without reaping it.
fn exited_unreaped(child: &std::process::Child) {
    // SAFETY: an all-zero siginfo is a valid value of the plain C struct.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waiting for our own child without reaping it.
    let waited = unsafe {
        libc::waitid(
            libc::P_PID,
            child.id(),
            &mut info,
            libc::WEXITED | libc::WNOWAIT,
        )
    };
    assert_eq!(waited, 0, "waitid: {}", std::io::Error::last_os_error());
}

/// Reap a leader once nothing reads it any more: its job concluded, or its interval closed.
fn reap(mut leader: std::process::Child) {
    leader.wait().unwrap();
}

/// `leader` as the job's owned process, identified while its parent holds it unreaped.
fn owned(leader: &std::process::Child, spawned: Instant) -> OwnedProcess {
    OwnedProcess {
        birth: Birth::of(leader.id()),
        spawned,
        host: cowshed_core::host_load::read_host_load(),
    }
}

/// Deliver `event`, then one stdout byte, and return once the supervisor served that byte at
/// `offset`: the actor handles a job's events in order, so `event` has been handled too.
async fn deliver(
    handle: &WorkspaceSupervisorHandle,
    spawned: &Spawned,
    event: ProcessEvent,
    offset: u64,
) {
    let job_id = spawned.request.job_id;
    spawned.events.send(event).await.unwrap();
    spawned
        .events
        .send(ProcessEvent::Output {
            job_id,
            stream: StreamKind::Stdout,
            bytes: Bytes::from_static(b"."),
        })
        .await
        .unwrap();
    let chunk = handle
        .log_read(job_id, StreamKind::Stdout, offset, true)
        .await
        .unwrap();
    assert_eq!(chunk.bytes.as_ref(), b".");
}

/// A sample's wall time is exactly the time since `spawn` when it was taken: read between
/// `before` and `after`, it lies between their distances from the spawn.
fn assert_wall(
    sample: &cowshed_core::api::JobResourceSample,
    spawn: Instant,
    before: Instant,
    after: Instant,
) {
    let low = u64::try_from(before.duration_since(spawn).as_micros()).unwrap();
    let high = u64::try_from(after.duration_since(spawn).as_micros()).unwrap();
    assert!(
        (low..=high).contains(&sample.wall_us.get()),
        "wallUs {} is not the time since the job's first spawn, between {low} and {high}",
        sample.wall_us.get()
    );
    assert_eq!(sample.wall_ms.get(), sample.wall_us.get() / 1_000);
}

#[tokio::test]
async fn a_job_is_sampled_from_its_first_owned_process_until_its_sealed_terminal() {
    let (supervisor_config, _root) = isolated_config("resources");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();

    // Admitted, no process of its own yet: nothing to sample, and the read says so.
    assert_eq!(handle.info(job_id).await.unwrap().resources, None);
    let unsampled = handle.resources(job_id).await.unwrap_err();
    assert_eq!(unsampled.code, ErrorCode::Conflict, "{}", unsampled.message);

    // A cold host spawned for the job five seconds ago is its first process: its leader, its
    // wall baseline, and its group's one member.
    let host = lone_group();
    let activation = Instant::now().checked_sub(Duration::from_secs(5)).unwrap();
    deliver(
        &handle,
        &job,
        ProcessEvent::Activating {
            job_id,
            process: owned(&host, activation),
        },
        0,
    )
    .await;
    let before = Instant::now();
    let activating = handle.resources(job_id).await.unwrap();
    let after = Instant::now();
    assert_eq!(
        (
            activating.job_id,
            activating.leader_pid,
            activating.members.clone()
        ),
        (job_id, host.id(), vec![host.id()])
    );
    assert_wall(&activating, activation, before, after);
    let before = Instant::now();
    let still_activating = handle.resources(job_id).await.unwrap();
    let after = Instant::now();
    assert_wall(&still_activating, activation, before, after);
    assert_eq!(
        handle.info(job_id).await.unwrap().resources,
        Some(still_activating),
        "status carries the latest sample"
    );

    // The activation ended: its parent read what it cost while it still held it. The command
    // starts now: it leads the job, whose wall still counts from the activation, and its group
    // is the job's.
    #[cfg(target_os = "macos")]
    job.events
        .send(ProcessEvent::ActivationEnded {
            job_id,
            usage: cowshed_core::runtime::job_accounting::read_leader(&Birth::of(host.id())),
        })
        .await
        .unwrap();
    let command = lone_group();
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
        },
        1,
    )
    .await;
    end_group(&host);
    reap(host);
    let before = Instant::now();
    let running = handle.resources(job_id).await.unwrap();
    let after = Instant::now();
    assert_eq!(
        (running.leader_pid, running.members.clone()),
        (command.id(), vec![command.id()])
    );
    assert_wall(&running, activation, before, after);

    end_group(&command);

    let before = Instant::now();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let ended = handle.wait(job_id).await.unwrap();
    let after = Instant::now();
    let terminal = ended.resources.clone().expect("a terminal sample");
    assert_eq!(
        terminal.accounting.is_some(),
        cfg!(target_os = "macos"),
        "macOS states the job's totals; Linux has no complete source yet"
    );
    assert_eq!(
        (terminal.leader_pid, terminal.members.clone()),
        (command.id(), Vec::new()),
        "the leader is named after its group emptied"
    );
    assert_wall(&terminal, activation, before, after);
    assert_eq!(
        handle.resources(job_id).await.unwrap(),
        terminal,
        "an ended job's sample is frozen"
    );
    assert_eq!(
        handle.sealed(job_id).await.unwrap().resources,
        Some(terminal),
        "the sealed sample is the last one"
    );
    reap(command);
}

/// Through a served supervisor, a progress subscription opened before the job owns a process
/// sends nothing until it does, then its samples, then the terminal one its sealed record keeps.
#[tokio::test]
async fn a_served_supervisor_s_progress_starts_at_ownership_and_ends_at_the_sealed_sample() {
    let (supervisor_config, _root) = isolated_config("progress-served");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let (remote, _path) = served(&handle).await;
    let job_id = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let every = cowshed_core::api::SampleInterval::new(20).unwrap();
    let mut progress = remote.progress(job_id, every).await.unwrap();

    let command = lone_group();
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
        },
        0,
    )
    .await;
    for _ in 0..2 {
        let sample = progress.next().await.unwrap().unwrap();
        assert_eq!(
            (sample.job_id, sample.leader_pid, sample.members.clone()),
            (job_id, command.id(), vec![command.id()])
        );
    }

    end_group(&command);
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(job_id).await.unwrap();
    let terminal = remote.resources(job_id).await.unwrap();
    let sealed = remote.sealed(job_id).await.unwrap().resources.unwrap();
    assert_eq!(terminal, sealed, "resources and the sealed sample agree");
    let mut after = Vec::new();
    while let Some(sample) = progress.next().await {
        after.push(sample.unwrap());
    }
    assert_eq!(
        after.last(),
        Some(&sealed),
        "the last sample is the sealed one"
    );
    assert_eq!(
        after.iter().filter(|sample| **sample == sealed).count(),
        1,
        "the terminal sample is sent once: {after:?}"
    );
    reap(command);
}

/// What each [`Holder`] keeps resident.
const HELD: u64 = 128 << 20;

/// A process this test started in the job's group: it holds [`HELD`] bytes of its own, every
/// page written, from the line it writes on stdout until its stdin -- a pipe this test holds --
/// closes.
struct Holder(std::process::Child);

impl Holder {
    /// Start a holder in the group `leader` leads and return once its memory is resident.
    fn hold(leader: &std::process::Child) -> Self {
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;

        // One buffer, filled 1 MiB at a time with random bytes: no page of it is one the kernel
        // can share or compress away, and no large transient copy -- whose freed pages Darwin's
        // allocator may leave resident -- inflates what the holder holds.
        let script = format!(
            "import os, sys\nheld = bytearray({HELD})\nfor at in range(0, {HELD}, 1 << 20):\n    held[at:at + (1 << 20)] = os.urandom(1 << 20)\nprint('held', flush=True)\nsys.stdin.read()\n"
        );
        let mut child = std::process::Command::new("python3")
            .args(["-c", &script])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(i32::try_from(leader.id()).unwrap())
            .spawn_locked()
            .expect("python3, which the development shell provides");
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert_eq!(line, "held\n", "the holder wrote every page it holds");
        Self(child)
    }

    /// Close its stdin, and reap it once it exits: it holds nothing any more.
    fn release(mut self) {
        drop(self.0.stdin.take());
        assert!(self.0.wait().unwrap().success());
    }
}

/// A job's resident memory is what its group holds together at a sample, and its peak the
/// largest such sum read: two processes holding memory at once raise it, two holding the same
/// amount one after the other do not, and the sealed sample keeps it.
#[tokio::test]
async fn a_job_s_rss_is_its_group_s_simultaneous_sum_and_its_peak_the_largest_read() {
    let (supervisor_config, _root) = isolated_config("resources-rss");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let leader = lone_group();
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&leader, Instant::now()),
        },
        0,
    )
    .await;

    let (first, second) = (Holder::hold(&leader), Holder::hold(&leader));
    let both = handle.resources(job_id).await.unwrap();
    let mut members = both.members.clone();
    members.sort_unstable();
    let mut expected = vec![leader.id(), first.0.id(), second.0.id()];
    expected.sort_unstable();
    assert_eq!(members, expected, "the leader and both holders");
    assert!(
        both.rss_bytes.get() >= 2 * HELD,
        "two holders hold {} bytes together, not {}",
        2 * HELD,
        both.rss_bytes.get()
    );
    assert_eq!(
        both.rss_peak_bytes, both.rss_bytes,
        "the largest sum read so far"
    );
    first.release();
    second.release();

    for _ in 0..2 {
        let holder = Holder::hold(&leader);
        let one = handle.resources(job_id).await.unwrap();
        assert!(
            (HELD..2 * HELD).contains(&one.rss_bytes.get()),
            "one holder holds {HELD} bytes, not {}",
            one.rss_bytes.get()
        );
        assert_eq!(
            one.rss_peak_bytes, both.rss_peak_bytes,
            "a smaller sum leaves the peak where it was"
        );
        assert!(
            one.rss_peak_bytes.get() < 4 * HELD,
            "the peak is no sum of every holder's own peak"
        );
        holder.release();
    }

    end_group(&leader);
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let terminal = handle
        .wait(job_id)
        .await
        .unwrap()
        .resources
        .expect("a terminal sample");
    reap(leader);
    assert_eq!(
        (
            terminal.members.clone(),
            terminal.rss_bytes.get(),
            terminal.rss_peak_bytes
        ),
        (Vec::new(), 0, both.rss_peak_bytes),
        "an emptied group holds nothing, and the job's peak stays"
    );
    assert_eq!(
        handle.sealed(job_id).await.unwrap().resources,
        Some(terminal),
        "the sealed sample keeps the peak"
    );
}

#[tokio::test]
async fn a_job_that_ends_before_any_process_of_its_own_has_no_sample() {
    let (supervisor_config, _root) = isolated_config("resources-unspawned");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    job.events
        .send(ProcessEvent::LaunchFailed {
            job_id,
            error: CowshedError::environment_missing("no host could start", "repair the host"),
        })
        .await
        .unwrap();
    for stream in [StreamKind::Stdout, StreamKind::Stderr] {
        job.events
            .send(ProcessEvent::OutputEof { job_id, stream })
            .await
            .unwrap();
    }
    let ended = handle.wait(job_id).await.unwrap();
    assert_eq!((ended.state, ended.resources), (JobState::Failed, None));
    assert_eq!(handle.sealed(job_id).await.unwrap().resources, None);
    let unsampled = handle.resources(job_id).await.unwrap_err();
    assert_eq!(unsampled.code, ErrorCode::Conflict, "{}", unsampled.message);
}

/// A watermark as the bare numbers its wire projects.
fn watermark(stream: &JobStreamWatermark) -> (u64, u64) {
    (stream.bytes.get(), stream.lines.get())
}

/// A stream counts every chunk the supervisor admits, whatever line the chunk splits: `a`,
/// `\nb`, `\n` and `c` are five bytes in three lines, the last one unterminated. The count is
/// the stream's read cursor, live and terminal, and the sealed record keeps it.
#[tokio::test]
async fn a_stream_counts_the_bytes_and_lines_of_every_admitted_chunk() {
    let (supervisor_config, _root) = isolated_config("resources-lines");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let command = lone_group();
    job.events
        .send(ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
        })
        .await
        .unwrap();
    // Each chunk is admitted before the next is sent: a read at the stream's end waits for the
    // chunk, and the supervisor serves it from where the chunk before it ended.
    let mut cursor = 0;
    for chunk in [&b"a"[..], b"\nb", b"\n", b"c"] {
        job.events
            .send(ProcessEvent::Output {
                job_id,
                stream: StreamKind::Stdout,
                bytes: Bytes::from_static(chunk),
            })
            .await
            .unwrap();
        let admitted = handle
            .log_read(job_id, StreamKind::Stdout, cursor, true)
            .await
            .unwrap();
        assert_eq!(admitted.bytes.as_ref(), chunk);
        cursor = admitted.next_offset;
    }
    assert_eq!(cursor, 5);

    let running = handle.resources(job_id).await.unwrap();
    assert_eq!(
        (watermark(&running.stdout), watermark(&running.stderr)),
        ((5, 3), (0, 0))
    );
    assert_eq!(
        running.stdout.bytes.get(),
        cursor,
        "bytes is the next read cursor"
    );

    end_group(&command);
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let terminal = handle
        .wait(job_id)
        .await
        .unwrap()
        .resources
        .expect("a terminal sample");
    reap(command);
    assert_eq!(
        (watermark(&terminal.stdout), watermark(&terminal.stderr)),
        ((5, 3), (0, 0))
    );
    let sealed = handle.sealed(job_id).await.unwrap();
    assert_eq!(sealed.stdout.bytes, terminal.stdout.bytes.get());
    assert_eq!(
        sealed.resources,
        Some(terminal),
        "the sealed sample is the last one"
    );
}

/// 2 MiB of newlines on stderr spill from the inline buffer to the job's protected file midway;
/// every one of them is a line, before and after, and the record a later supervisor reads from
/// the store carries the very terminal sample the job ended with.
#[tokio::test]
async fn a_streams_counts_survive_its_promotion_to_a_protected_file() {
    const NEWLINES: usize = 2 * 1024 * 1024;
    let (mut supervisor_config, _root) = isolated_config("resources-promotion");
    // Room for the stream: the harness's quota would end the job long before its last line.
    supervisor_config.artifacts.combined_output_quota_bytes = 2 * u64::try_from(NEWLINES).unwrap();
    let inline_cap = supervisor_config.artifacts.inline_cap_bytes;
    assert!(
        NEWLINES > inline_cap && NEWLINES.is_multiple_of(inline_cap),
        "the stream outgrows the inline cap in whole chunks"
    );
    let mut harness = warm_harness(supervisor_config.clone());
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let command = lone_group();
    job.events
        .send(ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
        })
        .await
        .unwrap();
    // The first chunk fills the inline buffer exactly; the second promotes the stream. Each is
    // admitted before the next is sent: a read at the stream's end waits for it.
    let chunk = Bytes::from(vec![b'\n'; inline_cap]);
    let cap = u64::try_from(inline_cap).unwrap();
    let total = u64::try_from(NEWLINES).unwrap();
    for start in (0..total).step_by(inline_cap) {
        job.events
            .send(ProcessEvent::Output {
                job_id,
                stream: StreamKind::Stderr,
                bytes: chunk.clone(),
            })
            .await
            .unwrap();
        let admitted = handle
            .log_read(job_id, StreamKind::Stderr, start, true)
            .await
            .unwrap();
        assert!(
            admitted.next_offset > start,
            "the chunk at {start} is served"
        );
        if start == 0 {
            let inline = handle.resources(job_id).await.unwrap();
            assert_eq!(
                watermark(&inline.stderr),
                (cap, cap),
                "before the promotion"
            );
        }
    }
    let running = handle.resources(job_id).await.unwrap();
    assert_eq!(
        (watermark(&running.stdout), watermark(&running.stderr)),
        ((0, 0), (total, total)),
        "after the promotion"
    );

    end_group(&command);
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let terminal = handle
        .wait(job_id)
        .await
        .unwrap()
        .resources
        .expect("a terminal sample");
    reap(command);
    assert_eq!(watermark(&terminal.stderr), (total, total));
    let sealed = handle.sealed(job_id).await.unwrap();
    assert!(
        matches!(
            &sealed.stderr.storage,
            OutputStorage::Captured {
                artifact: ProtectedOutput::File { .. }
            }
        ),
        "stderr was promoted to its protected file: {:?}",
        sealed.stderr.storage
    );
    assert_eq!(sealed.stderr.bytes, total);
    assert_eq!(sealed.resources.as_ref(), Some(&terminal));
    handle.quiesce().await.unwrap();
    handle.retire().await.unwrap();
    drop(harness);

    let successor = warm_harness(supervisor_config);
    assert_eq!(
        successor.handle.sealed(job_id).await.unwrap().resources,
        Some(terminal),
        "the stored record decodes to the terminal sample"
    );
}

/// The CPU a burner spends on itself, measured by itself, before it reports `burned <us>` and
/// exits.
#[cfg(target_os = "macos")]
const BURN_SECONDS: f64 = 0.5;

/// What a job's leaders and the burners they reaped cost beyond what the burners measured:
/// a Python interpreter's start per burner and `sh`'s own work. Far less than one more burner.
#[cfg(target_os = "macos")]
const SLACK_US: u64 = 300_000;

/// A command line that burns [`BURN_SECONDS`] of its own CPU, then reports it.
#[cfg(target_os = "macos")]
fn burner() -> String {
    format!(
        "python3 -c 'import resource\n\
         def used():\n    u = resource.getrusage(resource.RUSAGE_SELF)\n    return u.ru_utime + u.ru_stime\n\
         x = 0\n\
         while used() < {BURN_SECONDS}:\n    for _ in range(10000):\n        x += 1\n\
         print(\"burned\", int(used() * 1e6), flush=True)\n'"
    )
}

/// `script` run by `/bin/sh` leading a group of its own: the test is its parent, holds it
/// unreaped, feeds its stdin and reads its stdout.
#[cfg(target_os = "macos")]
struct Scripted {
    child: std::process::Child,
    input: std::process::ChildStdin,
    lines: std::io::Lines<std::io::BufReader<std::process::ChildStdout>>,
}

#[cfg(target_os = "macos")]
impl Scripted {
    fn start(script: &str) -> Self {
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;

        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", script])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("sh, and python3, which the development shell provides");
        let input = child.stdin.take().unwrap();
        let lines = std::io::BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            input,
            lines,
        }
    }

    /// The next line, which must be `expected`.
    fn expect(&mut self, expected: &str) {
        assert_eq!(self.lines.next().unwrap().unwrap(), expected);
    }

    /// The CPU microseconds the next burner to report measured itself spending.
    fn burned(&mut self) -> u64 {
        let line = self.lines.next().unwrap().unwrap();
        line.strip_prefix("burned ")
            .unwrap_or_else(|| panic!("a burner's report, not {line:?}"))
            .parse()
            .unwrap()
    }

    /// One line on its stdin.
    fn send(&mut self, line: &str) {
        use std::io::Write as _;
        writeln!(self.input, "{line}").unwrap();
    }

    /// Let it exit, and wait without reaping it: the test holds it as a job's parent does.
    fn exit(mut self) -> std::process::Child {
        self.send("end");
        exited_unreaped(&self.child);
        self.child
    }
}

/// A sample's total CPU from its accounting source, whose storage bytes macOS cannot state:
/// they are unavailable, never zero.
#[cfg(target_os = "macos")]
fn accounted_us(sample: &cowshed_core::api::JobResourceSample) -> u64 {
    match sample.accounting {
        Some(cowshed_core::api::JobAccounting::MacOsRusageChildren { cpu, io: None }) => {
            cpu.user_us.get() + cpu.sys_us.get()
        }
        ref other => panic!("the leader/children rusage source with no byte totals: {other:?}"),
    }
}

/// Two children burn CPU at once and are reaped before any sample: the group the sample reads is
/// the leader alone, whose own CPU holds none of theirs. The job's totals hold both, once, live
/// and in the sealed sample.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_job_s_cpu_keeps_the_descendants_that_ended_between_samples() {
    let (supervisor_config, _root) = isolated_config("accounting-reaped");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let mut leader = Scripted::start(&format!(
        "{burner} &\n{burner} &\nwait\necho reaped\nread line\n",
        burner = burner()
    ));
    let pid = leader.child.id();
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&leader.child, Instant::now()),
        },
        0,
    )
    .await;
    let burned = leader.burned() + leader.burned();
    leader.expect("reaped");

    let running = handle.resources(job_id).await.unwrap();
    assert_eq!(
        running.members,
        vec![pid],
        "both children ended before this sample"
    );
    let accounted = accounted_us(&running);
    assert!(
        (burned..burned + SLACK_US).contains(&accounted),
        "the job's {accounted} us hold the {burned} us its reaped children burned, once"
    );
    let own = cowshed_core::runtime::job_accounting::read_leader(&Birth::of(pid))
        .unwrap()
        .own;
    let live = own.user_us.get() + own.sys_us.get();
    assert!(
        live * 10 < burned,
        "the live members' own CPU ({live} us) misses what the reaped children cost"
    );

    let leader = leader.exit();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let terminal = handle
        .wait(job_id)
        .await
        .unwrap()
        .resources
        .expect("a terminal sample");
    let sealed = accounted_us(&terminal);
    assert!(
        (accounted..burned + SLACK_US).contains(&sealed),
        "the terminal {sealed} us keep the running {accounted} us and add no child again"
    );
    assert_eq!(
        handle.sealed(job_id).await.unwrap().resources,
        Some(terminal),
        "the sealed sample keeps the totals"
    );
    reap(leader);
}

/// A cold host's activation counts up to its end and the command from its start, each once:
/// what the host burns after its activation ended -- serving the command, then other jobs -- is
/// not the job's.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn an_activation_and_its_command_each_count_once() {
    let (supervisor_config, _root) = isolated_config("accounting-activation");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let burner = burner();
    let mut host = Scripted::start(&format!(
        "{burner}\necho activated\nread go\n{burner}\necho served\nread line\n"
    ));
    deliver(
        &handle,
        &job,
        ProcessEvent::Activating {
            job_id,
            process: owned(&host.child, Instant::now()),
        },
        0,
    )
    .await;
    let activation = host.burned();
    host.expect("activated");
    job.events
        .send(ProcessEvent::ActivationEnded {
            job_id,
            usage: cowshed_core::runtime::job_accounting::read_leader(&Birth::of(host.child.id())),
        })
        .await
        .unwrap();
    // The host works on after its activation ended.
    host.send("go");
    let served = host.burned();
    host.expect("served");

    let mut command = Scripted::start(&format!("{burner}\necho ran\nread line\n"));
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&command.child, Instant::now()),
        },
        1,
    )
    .await;
    let ran = command.burned();
    command.expect("ran");
    reap(host.exit());

    let both = activation + ran;
    assert!(
        served > SLACK_US,
        "the host's later {served} us would show if charged"
    );
    let running = accounted_us(&handle.resources(job_id).await.unwrap());
    assert!(
        (both..both + SLACK_US).contains(&running),
        "the job's {running} us are its activation's {activation} us and its command's {ran} us, \
         once each, without the host's later {served} us"
    );

    let command = command.exit();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let terminal = handle
        .wait(job_id)
        .await
        .unwrap()
        .resources
        .expect("a terminal sample");
    let sealed = accounted_us(&terminal);
    assert!(
        (running..both + SLACK_US).contains(&sealed),
        "the terminal {sealed} us keep both intervals once"
    );
    reap(command);
}

/// Workspace `name`'s supervisor config, in an Nx project no daemon serves.
fn nx_project(name: &str) -> (WorkspaceSupervisorConfig, TempRoot) {
    let (mut supervisor_config, root) = isolated_config(&format!("nx-daemon-{name}"));
    supervisor_config.authority.workspace = WorkspaceName::new(name).unwrap();
    supervisor_config.default_cwd = None;
    std::fs::write(supervisor_config.workspace_root.join("nx.json"), "{}").unwrap();
    supervisor_config
        .sandbox
        .configure_capabilities()
        .expect("detect capabilities");
    (supervisor_config, root)
}

/// The argv that starts the daemon of the Nx project at `project`: the project's own `nx`.
fn nx_daemon_start(project: &Path) -> Vec<OsString> {
    vec![
        project.join("node_modules/.bin/nx").into_os_string(),
        OsString::from("daemon"),
        OsString::from("--start"),
    ]
}

/// Many of a keeper's probes, on a paused clock that skips the wait.
const NX_PROBES: Duration = Duration::from_secs(60);

/// A shed's code is unsigned and Nx's daemon runs it, so the daemon a host client connects to is
/// one the shed's supervisor keeps alive inside the sandbox: a read-write background job of the
/// project's own `nx`, started with the supervisor, never a second while one runs, and again
/// once the daemon it started is gone.
#[tokio::test(start_paused = true)]
async fn a_shed_keeps_its_nx_daemon_inside_the_sandbox() {
    let (supervisor_config, _root) = nx_project("raven");
    let project = supervisor_config.workspace_root.clone();
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);

    let first = h.spawned.recv().await.unwrap();
    let SpawnCommand::Argv(argv) = &first.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(argv, &nx_daemon_start(&project));
    assert_eq!(first.request.mode, RunSandboxMode::ReadWrite);
    assert!(
        first.request.cwd.as_os_str().is_empty(),
        "run from the project root"
    );
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "no second start while the first runs"
    );

    // The start leaves a live daemon: its record names a running process whose socket accepts.
    // Short, under /tmp: a Unix socket path is bounded at 104 bytes on macOS.
    let sockets = PathBuf::from("/tmp").join(format!(
        "cs-nxk-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    ));
    std::fs::create_dir_all(&sockets).unwrap();
    let socket = sockets.join("d.sock");
    let _daemon = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let record = project.join(".nx/workspace-data/d/server-process.json");
    std::fs::create_dir_all(record.parent().unwrap()).unwrap();
    let live = serde_json::json!({
        "processId": std::process::id(),
        "socketPath": socket,
        "nxVersion": "23.2.1",
    })
    .to_string();
    std::fs::write(&record, &live).unwrap();
    complete(&first, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(first.request.job_id).await.unwrap();
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "a live daemon is not started again"
    );

    // The daemon exits and takes its record with it: the next probe starts another.
    std::fs::remove_file(&record).unwrap();
    let second = tokio::time::timeout(NX_PROBES, h.spawned.recv())
        .await
        .expect("the dead daemon is started again")
        .unwrap();
    let SpawnCommand::Argv(argv) = &second.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(argv, &nx_daemon_start(&project));
    assert_eq!(second.request.mode, RunSandboxMode::ReadWrite);
    assert_ne!(second.request.job_id, first.request.job_id);

    std::fs::write(&record, &live).unwrap();
    complete(&second, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(second.request.job_id).await.unwrap();
    std::fs::remove_dir_all(sockets).unwrap();
}

/// How long a shed must go unused before its keeper stops the daemon it keeps.
const NX_IDLE: Duration = Duration::from_secs(5 * 60);

/// The daemon a start job leaves, as Nx leaves one: a running process its record names, whose
/// socket accepts. The process stands in for the daemon and is `sleep`, which a `SIGTERM` ends.
/// The socket is served, as Nx's daemon serves its own: a listener nobody accepts from refuses
/// a connection once its backlog holds the keeper's probes, and the keeper would find the daemon
/// gone.
struct LiveDaemon {
    process: std::process::Child,
    sockets: PathBuf,
    socket: PathBuf,
    serving: Option<std::thread::JoinHandle<()>>,
    closing: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl LiveDaemon {
    fn start(project: &Path) -> Self {
        use std::sync::atomic::{AtomicBool, Ordering};
        // Short, under /tmp: a Unix socket path is bounded at 104 bytes on macOS.
        let sockets = PathBuf::from("/tmp").join(format!(
            "cs-nxi-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(&sockets).unwrap();
        let socket = sockets.join("d.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let closing = std::sync::Arc::new(AtomicBool::new(false));
        let serving = {
            let closing = closing.clone();
            std::thread::spawn(move || {
                for connection in listener.incoming() {
                    if closing.load(Ordering::SeqCst) {
                        break;
                    }
                    drop(connection);
                }
            })
        };
        let process = std::process::Command::new("sleep")
            .arg("3600")
            .spawn_locked()
            .unwrap();
        let record = project.join(".nx/workspace-data/d/server-process.json");
        std::fs::create_dir_all(record.parent().unwrap()).unwrap();
        std::fs::write(
            &record,
            serde_json::json!({
                "processId": process.id(),
                "socketPath": socket,
                "nxVersion": "23.2.1",
            })
            .to_string(),
        )
        .unwrap();
        Self {
            process,
            sockets,
            socket,
            serving: Some(serving),
            closing,
        }
    }

    /// Whether the stand-in still runs: stopping it is what the keeper does to an idle daemon.
    fn running(&mut self) -> bool {
        self.process.try_wait().unwrap().is_none()
    }
}

impl Drop for LiveDaemon {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        // The serving thread is blocked in `accept`: one connection wakes it to see the flag.
        self.closing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::os::unix::net::UnixStream::connect(&self.socket);
        if let Some(serving) = self.serving.take() {
            let _ = serving.join();
        }
        let _ = std::fs::remove_dir_all(&self.sockets);
    }
}

/// A shed with a live daemon its start job left, and the harness over it.
async fn shed_with_live_nx_daemon(name: &str) -> (Harness, LiveDaemon, PathBuf, TempRoot) {
    let (supervisor_config, root) = nx_project(name);
    let project = supervisor_config.workspace_root.clone();
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let start = h.spawned.recv().await.unwrap();
    let daemon = LiveDaemon::start(&project);
    complete(&start, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(start.request.job_id).await.unwrap();
    (h, daemon, project, root)
}

/// A daemon costs its host a Node process, its plugin workers and a file watcher that recomputes
/// the project graph on every change, and Nx's own three-hour idle stop never comes in a tree
/// that keeps changing. So a shed's keeper stops the daemon it keeps once the shed has gone
/// unused, starts none while the shed stays unused, and starts one again for the next job.
#[tokio::test(start_paused = true)]
async fn a_shed_stops_its_idle_nx_daemon_and_starts_another_only_for_a_job() {
    let (mut h, mut daemon, project, _root) = shed_with_live_nx_daemon("wren").await;

    tokio::time::sleep(NX_IDLE - Duration::from_secs(30)).await;
    assert!(daemon.running(), "a daemon is kept for the whole interval");
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!daemon.running(), "the idle daemon is stopped");
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "an idle shed starts no daemon"
    );

    let job = h
        .handle
        .exec_background(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let ran = h.spawned.recv().await.unwrap();
    assert_eq!(ran.request.job_id, job);
    let restart = tokio::time::timeout(NX_PROBES, h.spawned.recv())
        .await
        .expect("a shed at work wants its daemon again")
        .unwrap();
    let SpawnCommand::Argv(argv) = &restart.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(argv, &nx_daemon_start(&project));
    assert_eq!(restart.request.mode, RunSandboxMode::ReadWrite);

    complete(&ran, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    complete(&restart, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(restart.request.job_id).await.unwrap();
}

/// A job in the shed is use for as long as it runs: its own Nx talks to the daemon, which stays
/// until the shed has been unused for the interval after the job's end.
#[tokio::test(start_paused = true)]
async fn a_shed_keeps_its_nx_daemon_while_a_job_runs() {
    let (mut h, mut daemon, _project, _root) = shed_with_live_nx_daemon("lark").await;

    let job = h
        .handle
        .exec_background(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let ran = h.spawned.recv().await.unwrap();
    tokio::time::sleep(NX_IDLE * 2).await;
    assert!(daemon.running(), "a running job keeps the daemon");

    complete(&ran, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    tokio::time::sleep(NX_IDLE - Duration::from_secs(30)).await;
    assert!(daemon.running(), "the interval starts when the job ends");
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!daemon.running(), "the daemon stops once the shed is idle");
}

/// A host shell's Nx run is no job of the supervisor's, but it holds a task database open while
/// its tasks run, so a shed whose gate runs from a host shell is not idle.
#[tokio::test(start_paused = true)]
async fn a_shed_keeps_its_nx_daemon_while_a_client_holds_a_task_database() {
    let (_h, mut daemon, project, _root) = shed_with_live_nx_daemon("pika").await;

    let database = project.join(".nx/workspace-data/task-v3.db");
    std::fs::write(&database, b"").unwrap();
    let client = std::fs::File::open(&database).unwrap();
    tokio::time::sleep(NX_IDLE * 2).await;
    assert!(daemon.running(), "a client at work keeps the daemon");

    // The last look that found the client was up to a look interval before it ended.
    drop(client);
    tokio::time::sleep(NX_IDLE - Duration::from_secs(60)).await;
    assert!(daemon.running(), "the interval starts when the client ends");
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(!daemon.running(), "the daemon stops once the shed is idle");
}

/// A supervisor whose keeper still holds a daemon is not idle, however little else it holds, so
/// the supervisor never retires over a daemon nobody then watches.
#[cfg(target_os = "macos")]
#[tokio::test(start_paused = true)]
async fn a_shed_supervisor_is_idle_only_once_its_nx_daemon_is_gone() {
    let (h, mut daemon, _project, _root) = shed_with_live_nx_daemon("tern").await;

    assert!(
        !h.handle.idle().await.unwrap(),
        "a live daemon is something to come back for"
    );
    tokio::time::sleep(NX_IDLE + Duration::from_secs(60)).await;
    assert!(!daemon.running());
    assert!(
        h.handle.idle().await.unwrap(),
        "no job, no session and no daemon: nothing is left"
    );
}

/// A keeper resolves the checkout's current pointer even when no user job has been
/// admitted since a land. Restarting the daemon must not reuse the previous job's grant.
#[tokio::test(start_paused = true)]
async fn nx_daemon_restart_follows_a_build_volume_swap_without_an_exec() {
    use cowshed_core::build_volume::{
        BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, link,
    };
    use cowshed_core::capabilities::{BuildStatePath, RelPath};
    use cowshed_core::repository::ProjectPaths;

    let (mut supervisor_config, root) = nx_project("raven");
    let checkout = supervisor_config.workspace_root.clone();
    supervisor_config.sandbox.mount_root = root.join("mounts");
    std::fs::create_dir_all(&supervisor_config.sandbox.mount_root).unwrap();
    let project = ProjectPaths::with_mount_root(
        root.join("store"),
        &supervisor_config.sandbox.mount_root,
        &supervisor_config.authority.repo_id,
    )
    .unwrap();
    let layout = BuildVolumeLayout::new(&project).unwrap();
    let before_id = BuildVolumeId::parse(&"a".repeat(32)).unwrap();
    let adopted_id = BuildVolumeId::parse(&"b".repeat(32)).unwrap();
    let before = layout.mount(&before_id);
    let adopted = layout.mount(&adopted_id);
    std::fs::create_dir_all(layout.images()).unwrap();
    for id in [&before_id, &adopted_id] {
        std::fs::File::create(layout.image(id)).unwrap();
        let mount = layout.mount(id);
        std::fs::create_dir_all(mount.join("nx/cache")).unwrap();
        std::fs::create_dir_all(mount.join("nx/workspace-data")).unwrap();
        layout
            .write_record(
                id,
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Linked {
                        checkout: supervisor_config.authority.workspace.clone(),
                    },
                ),
            )
            .unwrap();
    }
    link::point(&checkout, &before).unwrap();
    let paths = [
        BuildStatePath {
            checkout: RelPath::new(".nx/cache").unwrap(),
            volume: RelPath::new("nx/cache").unwrap(),
        },
        BuildStatePath {
            checkout: RelPath::new(".nx/workspace-data").unwrap(),
            volume: RelPath::new("nx/workspace-data").unwrap(),
        },
    ];
    link::link_paths(&checkout, &before, &paths).unwrap();
    supervisor_config.sandbox.build_volume_mount = Some(before.clone());
    supervisor_config.build_volume_layout = Some(layout.clone());
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);

    let first = h.spawned.recv().await.unwrap();
    let SpawnCommand::Argv(first_argv) = &first.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(first_argv, &nx_daemon_start(&checkout));
    // The first keeper job is still active. Pivot before completing it, with no exec
    // between the pivot and the keeper's next start.
    link::point(&checkout, &adopted).unwrap();
    layout
        .write_record(
            &before_id,
            &BuildVolumeRecord::new(None, BuildVolumeRole::Unlinked),
        )
        .unwrap();
    complete(&first, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(first.request.job_id).await.unwrap();
    let second = tokio::time::timeout(NX_PROBES, h.spawned.recv())
        .await
        .expect("the keeper restarts the daemon after the pivot")
        .unwrap();
    let SpawnCommand::Argv(second_argv) = &second.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(second_argv, &nx_daemon_start(&checkout));
    assert_eq!(second.request.mode, RunSandboxMode::ReadWrite);
    assert_ne!(second.request.job_id, first.request.job_id);
    for mode in [RunSandboxMode::ReadWrite, RunSandboxMode::ReadOnly] {
        let (first_config, first_profile) = first.request.policy.child(mode);
        let (second_config, second_profile) = second.request.policy.child(mode);
        assert_eq!(first_config.build_volume_mount.as_ref(), Some(&before));
        assert_eq!(second_config.build_volume_mount.as_ref(), Some(&adopted));
        assert!(first_profile.contains(before.to_str().unwrap()));
        assert!(!first_profile.contains(adopted.to_str().unwrap()));
        assert!(second_profile.contains(adopted.to_str().unwrap()));
        assert!(!second_profile.contains(before.to_str().unwrap()));
    }
    complete(&second, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(second.request.job_id).await.unwrap();
    h.handle.retire().await.unwrap();
}

/// Main is the operator's own checkout and its Nx daemon the host's: its supervisor starts none.
#[tokio::test(start_paused = true)]
async fn main_leaves_its_nx_daemon_to_the_host() {
    let (supervisor_config, _root) = nx_project("main");
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "main's supervisor started a job of its own"
    );
}

/// One row of a sealed job-span segment, decoded from the columns a reader of lmao's trace
/// schema sees.
#[derive(Debug, Eq, PartialEq)]
struct JobSpanRow {
    trace_id: String,
    thread_id: u64,
    span_id: u32,
    parent_span_id: Option<u32>,
    entry_type: String,
    message: String,
    repo_id: String,
    workspace_incarnation: String,
    job_id: u64,
    grant_revision: u64,
    w3c_span_id: u64,
    job_state: Option<String>,
}

/// Every job-span segment under `telemetry`, in the order its writer sealed them, as one row
/// each.
fn job_spans(telemetry: &Path) -> Vec<(String, JobSpanRow)> {
    use arrow_array::Array as _;
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{UInt8Type, UInt32Type, UInt64Type};

    let mut segments = Vec::new();
    for partition in std::fs::read_dir(telemetry).expect("list telemetry root") {
        for entry in std::fs::read_dir(partition.expect("partition").path()).expect("list date") {
            let path = entry.expect("segment").path();
            let name = path.file_name().unwrap().to_str().unwrap().to_owned();
            if name.starts_with("job-") && name.ends_with(".arrow") {
                segments.push((name, path));
            }
        }
    }
    segments.sort();
    segments
        .into_iter()
        .map(|(name, path)| {
            let mut reader = arrow_ipc::reader::StreamReader::try_new(
                std::fs::File::open(&path).expect("open job span segment"),
                None,
            )
            .expect("Arrow stream");
            let batch = reader.next().expect("one batch").expect("valid batch");
            assert!(reader.next().is_none(), "{name} holds one batch");
            assert_eq!(batch.num_rows(), 1, "{name} holds one row");
            let column = |name: &str| batch.column_by_name(name).expect(name).clone();
            let text = |name: &str| {
                let column = column(name);
                let dictionary = column.as_dictionary_opt::<UInt32Type>();
                match dictionary {
                    Some(dictionary) => {
                        let key = usize::try_from(dictionary.keys().value(0)).unwrap();
                        dictionary.values().as_string::<i32>().value(key).to_owned()
                    }
                    None => column.as_string::<i32>().value(0).to_owned(),
                }
            };
            let number = |name: &str| column(name).as_primitive::<UInt64Type>().value(0);
            let entry_types = column("entry_type");
            let entry_types = entry_types.as_dictionary::<UInt8Type>();
            let parent = column("parent_span_id");
            let parent = parent.as_primitive::<UInt32Type>();
            let state = column("job_state");
            let state = state.as_string::<i32>();
            let row = JobSpanRow {
                trace_id: text("trace_id"),
                thread_id: number("thread_id"),
                span_id: column("span_id").as_primitive::<UInt32Type>().value(0),
                parent_span_id: parent.is_valid(0).then(|| parent.value(0)),
                entry_type: entry_types
                    .values()
                    .as_string::<i32>()
                    .value(usize::from(entry_types.keys().value(0)))
                    .to_owned(),
                message: text("message"),
                repo_id: text("repo_id"),
                workspace_incarnation: text("workspace_incarnation"),
                job_id: number("job_id"),
                grant_revision: number("grant_revision"),
                w3c_span_id: number("w3c_span_id"),
                job_state: state.is_valid(0).then(|| state.value(0).to_owned()),
            };
            (name, row)
        })
        .collect()
}

/// The two rows of `job`'s span: its start, then its end as `entry_type` in `state`.
fn expected_span(
    job: &cowshed_core::api::JobInfo,
    entry_type: &str,
    state: &str,
) -> [JobSpanRow; 2] {
    let row = |entry_type: &str, job_state: Option<&str>| JobSpanRow {
        trace_id: job.trace.trace_id.as_str().to_owned(),
        thread_id: cowshed_core::runtime::job_spans::job_span_thread(
            &job.repo_id,
            &job.workspace_incarnation,
            job.job_id,
        ),
        span_id: 1,
        parent_span_id: None,
        entry_type: entry_type.to_owned(),
        message: "cowshed.job".to_owned(),
        repo_id: job.repo_id.as_str().to_owned(),
        workspace_incarnation: job.workspace_incarnation.as_str().to_owned(),
        job_id: job.job_id.get(),
        grant_revision: job.grant_revision,
        w3c_span_id: u64::from_str_radix(job.trace.span_id.as_str(), 16).unwrap(),
        job_state: job_state.map(str::to_owned),
    };
    [row("span-start", None), row(entry_type, Some(state))]
}

/// Asserts the segments under `telemetry` are exactly `spans`, two per job in sealing order,
/// each named `job-<order>-<writer>.arrow` by one writer counting from 1.
fn assert_job_spans(telemetry: &Path, spans: &[[JobSpanRow; 2]]) {
    let sealed = job_spans(telemetry);
    let names = sealed
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names.len(),
        spans.len() * 2,
        "two sealed segments per job: {names:?}"
    );
    let writer = &names[0]["job-00000000000000000001-".len()..];
    for (index, name) in names.iter().enumerate() {
        assert_eq!(*name, format!("job-{:020}-{writer}", index + 1));
    }
    let rows = sealed.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    let expected = spans.iter().flatten().collect::<Vec<_>>();
    assert_eq!(rows.iter().collect::<Vec<_>>(), expected);
}

fn adopted_trace() -> cowshed_core::api::TraceContext {
    cowshed_core::api::TraceContext {
        trace_id: cowshed_core::api::TraceId::new("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
        span_id: cowshed_core::api::SpanId::new("00f067aa0ba902b7").unwrap(),
    }
}

/// A job's span is sealed at admission and at its terminal state under the trace context the
/// controller handed the supervisor, or the one the supervisor minted: one span, ended ok only
/// by a clean zero exit.
#[tokio::test]
async fn an_exec_and_its_terminal_seal_one_job_span_under_the_job_s_trace_context() {
    let root = workspace_root("job-span");
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    let mut h = harness_with_config(
        WorkspaceSupervisorConfig {
            telemetry_root: Some(telemetry.clone()),
            ..config(&root)
        },
        1,
        1024,
        false,
        false,
    );

    let mut adopted = request(StdinSource::Empty);
    adopted.trace = Some(adopted_trace());
    let job = h.handle.exec(None, None, adopted).await.unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let clean = h.handle.wait(job).await.unwrap();
    assert_eq!(clean.trace, adopted_trace());
    let health = h.handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
    assert_job_spans(&telemetry, &[expected_span(&clean, "span-ok", "exited")]);

    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 1 }).await;
    let failed = h.handle.wait(job).await.unwrap();
    assert_ne!(
        failed.trace,
        adopted_trace(),
        "the supervisor minted this job's trace"
    );
    let health = h.handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (4, 0), "{health:?}");
    assert_job_spans(
        &telemetry,
        &[
            expected_span(&clean, "span-ok", "exited"),
            expected_span(&failed, "span-err", "exited"),
        ],
    );
}

/// A span row the writer cannot seal is counted in the supervisor's trace health; the job it
/// describes ends as it would have anyway.
#[tokio::test]
async fn a_refused_job_span_is_counted_and_never_fails_its_job() {
    let root = workspace_root("job-span-refused");
    let mut h = harness_with_config(
        WorkspaceSupervisorConfig {
            telemetry_root: Some(root.join("absent-telemetry")),
            ..config(&root)
        },
        1,
        1024,
        false,
        false,
    );
    let job = h
        .handle
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let info = h.handle.wait(job).await.unwrap();
    assert_eq!(info.state, JobState::Exited);
    let health = h.handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (0, 2), "{health:?}");
    assert!(
        health
            .last_failure
            .as_deref()
            .is_some_and(|failure| failure.contains("creating telemetry partition")),
        "{health:?}"
    );
}

/// Two workspaces' first jobs under one adopted trace context share the trace id and the
/// workspace-local job id, yet seal distinct lmao spans: the thread follows the durable key
/// `(repo_id, workspace_incarnation, job_id)`, never `job_id` alone.
#[tokio::test]
async fn jobs_sharing_an_adopted_trace_and_a_job_id_across_workspaces_are_distinct_spans() {
    let root = workspace_root("job-span-two-workspaces");
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    let second = WorkspaceAuthoritySnapshot {
        workspace_incarnation: WorkspaceIncarnation::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap(),
        ..authority()
    };
    let mut ended = Vec::new();
    for (name, authority) in [("first", authority()), ("second", second)] {
        let workspace = workspace_root(&format!("job-span-workspace-{name}"));
        let mut h = harness_with_config(
            WorkspaceSupervisorConfig {
                owned_repo_ids: OwnedRepoIds::sole(authority.repo_id.clone()),
                authority,
                telemetry_root: Some(telemetry.clone()),
                ..config(&workspace)
            },
            1,
            1024,
            false,
            false,
        );
        let mut adopted = request(StdinSource::Empty);
        adopted.trace = Some(adopted_trace());
        let job = h.handle.exec(None, None, adopted).await.unwrap();
        let spawned = h.spawned.recv().await.unwrap();
        complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
        ended.push(h.handle.wait(job).await.unwrap());
        let health = h.handle.trace_health().await.unwrap();
        assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
    }
    assert_eq!(
        ended[0].job_id, ended[1].job_id,
        "both are their workspace's first job"
    );
    assert_eq!(
        ended[0].trace, ended[1].trace,
        "both adopted one trace context"
    );
    let addresses = job_spans(&telemetry)
        .into_iter()
        .map(|(_, row)| {
            (
                row.workspace_incarnation,
                (row.trace_id, row.thread_id, row.span_id),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        addresses.len(),
        2,
        "one address per job, both rows of a job on it: {addresses:?}"
    );
    let distinct = addresses
        .iter()
        .map(|(_, address)| address)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        distinct.len(),
        2,
        "the two jobs' spans collide: {addresses:?}"
    );
    let sealed = job_spans(&telemetry);
    for job in &ended {
        let rows = sealed
            .iter()
            .filter(|(_, row)| row.workspace_incarnation == job.workspace_incarnation.as_str())
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        let expected = expected_span(job, "span-ok", "exited");
        assert_eq!(rows, expected.iter().collect::<Vec<_>>());
    }
}

/// A handle that reaches the supervisor only through its socket reads the same trace health.
#[tokio::test]
async fn a_served_supervisor_answers_its_trace_health() {
    let root = workspace_root("job-span-served");
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    let mut h = harness_with_config(
        WorkspaceSupervisorConfig {
            telemetry_root: Some(telemetry.clone()),
            ..config(&root)
        },
        1,
        1024,
        false,
        false,
    );
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(None, None, request(StdinSource::Empty))
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(job).await.unwrap();
    let health = remote.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
    assert_eq!(health, h.handle.trace_health().await.unwrap());

    let (silent, _root) = harness(1, 1024, false, false);
    let (remote, _path) = served(&silent.handle).await;
    let refused = remote.trace_health().await.unwrap_err();
    assert_eq!(
        refused.code,
        ErrorCode::EnvironmentMissing,
        "{}",
        refused.message
    );
}

/// The production supervisor's own spawn path, real `true` and `false` under seatbelt: each
/// exec and its terminal leave one sealed job span carrying the trace context it was given.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_real_exec_seals_one_job_span_through_the_production_supervisor() {
    let (mut supervisor_config, root) = isolated_config("job-span-exec");
    supervisor_config.default_cwd = None;
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    supervisor_config.telemetry_root = Some(telemetry.clone());
    let mount = supervisor_config.workspace_root.clone();
    std::fs::create_dir_all(&supervisor_config.sandbox.home).unwrap();
    std::fs::create_dir_all(&supervisor_config.sandbox.exec_temp_dir).unwrap();
    std::fs::create_dir_all(mount.join(".cowshed/bin")).unwrap();
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        // A supervisor start publishes `.cowshed/env` from the image's private token.
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(mount.join(cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH))
            .unwrap()
            .write_all(
                cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32])
                    .encode()
                    .as_bytes(),
            )
            .unwrap();
    }
    let commitments = cowshed_core::runtime::supervisor::CommitmentPublisher::start(
        Box::new(cowshed_core::storage::audit::NullAuditSink),
        8,
    )
    .unwrap();
    let handle = WorkspaceSupervisor::start(supervisor_config, commitments).unwrap();

    let run = |argv: &str, trace: Option<cowshed_core::api::TraceContext>| ExecRequest {
        command: ExecCommand::Argv(vec![CommandArg::from(argv)]),
        cwd: None,
        trace,
        ..request(StdinSource::Empty)
    };
    let job = handle
        .exec(None, None, run("/usr/bin/true", Some(adopted_trace())))
        .await
        .unwrap();
    let clean = handle.wait(job).await.unwrap();
    assert_eq!(clean.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(clean.trace, adopted_trace());
    let health = handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
    assert_job_spans(&telemetry, &[expected_span(&clean, "span-ok", "exited")]);

    let job = handle
        .exec(None, None, run("/usr/bin/false", None))
        .await
        .unwrap();
    let failed = handle.wait(job).await.unwrap();
    assert_eq!(failed.exit, Some(ExitStatus::Exited { code: 1 }));
    let health = handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (4, 0), "{health:?}");
    assert_job_spans(
        &telemetry,
        &[
            expected_span(&clean, "span-ok", "exited"),
            expected_span(&failed, "span-err", "exited"),
        ],
    );
    handle.retire().await.unwrap();
}
