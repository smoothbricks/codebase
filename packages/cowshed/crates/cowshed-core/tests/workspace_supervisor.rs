#[cfg(target_os = "macos")]
use cowshed_os::test_backstop::CommandBackstop as _;
use cowshed_os::test_backstop::{
    AsyncUnixStreamBackstop as _, ChildBackstop as _, UnixStreamBackstop as _,
};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_core::api::{
    AdmissionKey, CONTROLLER_COMMITMENT_VERSION, CommandArg, ControllerCommitment, ExecClass,
    ExecCommand, ExecRequest, ExecScope, ExitStatus, JobFailure, JobId, JobJournalCursor, JobState,
    JobTailBytes, JobTailLimits, MAX_COMMAND_ARG_BYTES, OutputLimitInfo, OutputPublication,
    OutputStorage, OutputSummary, ProtectedOutput, PublicationPolicy, RunSandboxMode, Sha256Digest,
    StdinKind, StdinSource, StreamInfo, WorkspacePath,
};
#[cfg(target_os = "macos")]
use cowshed_core::api::{JobLeafAttribution, ProcessCoverage, ProcessCoverageGap};
use cowshed_core::error::{AdmissionField, AdmissionRefusal, CowshedError, ErrorCode, Result};
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::{OwnedRepoIds, RepoId};
use cowshed_core::runtime::job_groups::Birth;
use cowshed_core::runtime::supervisor_socket::AdmissionSource;
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::apfs::Labelled;
use cowshed_core::storage::job_artifact::{
    AdmissionLookup, ArtifactConfig, ArtifactStore, FIRST_KEYED_LAYOUT, JobAdmission,
    JobArtifactRecord, JobEnding, JobScheduling, SealedJobReader, StreamKind,
};
use cowshed_os::fork_lock::Spawn as _;
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use tokio::sync::mpsc;

use cowshed_core::runtime::supervisor::{
    ArtifactIndex, ArtifactSeal, ArtifactSink, ArtifactStoreSink, ArtifactWrite, CommitmentDraft,
    CommitmentSink, NxDaemonRetirement, OwnedProcess, ProcessEvent, ProcessSignal,
    ProcessSpawnRequest, RunningProcess, SessionToken, SpawnCommand, SpawnSink, VolumeLabeller,
    VolumeLabels, WorkspaceAuthoritySnapshot, WorkspaceSupervisor, WorkspaceSupervisorConfig,
    WorkspaceSupervisorHandle,
};

#[path = "support/calm_pressure.rs"]
mod calm_pressure;
#[path = "support/other_build_supervisor.rs"]
mod other_build_supervisor;
#[path = "support/publication.rs"]
mod publication;
#[path = "support/temp_parent.rs"]
mod temp_parent;
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

    fn nice(&self) -> Option<i32> {
        None
    }

    #[cfg(target_os = "macos")]
    fn observations(&self) -> Option<&cowshed_core::runtime::process_monitor::ProcessMonitor> {
        None
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OrderObservation {
    ArtifactAdmit(JobId),
    ArtifactSeal(JobId),
    Commitment(&'static str),
    Spawn(JobId),
}

/// `sealed_stdout` lets a test commit a stdout stream that differs from the bytes the job wrote,
/// which is the only way to observe from outside whether a terminal log read is answered from the
/// store's committed copy or from a second copy the actor kept.
struct FakeArtifactSink {
    sealed_stdout: Option<Vec<u8>>,
    next: JobId,
    quota: u64,
    jobs: BTreeMap<JobId, FakeArtifactJob>,
    terminals: BTreeMap<JobId, SealedJobReader>,
    authority: WorkspaceAuthoritySnapshot,
    observations: mpsc::UnboundedSender<ArtifactObservation>,
    order: mpsc::UnboundedSender<OrderObservation>,
}

impl ArtifactIndex for FakeArtifactSink {
    fn next_job_id(&self) -> Result<JobId> {
        Ok(self.next)
    }

    /// Keyed admission is the production store's ([`real_store_harness`]).
    fn admitted(&self, _key: &AdmissionKey) -> AdmissionLookup<'_> {
        AdmissionLookup::Absent
    }
}

impl ArtifactSink for FakeArtifactSink {
    fn admit(
        &mut self,
        expected_job_id: JobId,
        grant_revision: u64,
        command: &cowshed_core::api::ExecCommand,
        admission: Option<JobAdmission>,
        scheduling: JobScheduling,
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
                record: JobArtifactRecord {
                    repo_id: self.authority.repo_id.clone(),
                    workspace_incarnation: self.authority.workspace_incarnation.clone(),
                    job_id: expected_job_id,
                    sequence: expected_job_id.get(),
                    state: JobState::Queued,
                    grant_revision,
                    command: command.clone(),
                    output_limit: None,
                    stdout: stream(Vec::new()),
                    stderr: stream(Vec::new()),
                    failure: None,
                    exit: None,
                    duration_ms: None,
                    resources: None,
                    admission,
                    scheduling,
                    nice: None,
                    containment: None,
                    outlived: Vec::new(),
                    outlived_incomplete: false,
                },
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
        let (sealed, terminal) = self
            .jobs
            .remove(&job_id)
            .expect("live fake artifact job")
            .seal(ending)?;
        assert!(
            self.terminals.insert(job_id, terminal).is_none(),
            "one terminal entry per job"
        );
        Ok(sealed)
    }

    /// Facts and reads use this same terminal entry; durable predecessor recovery stays on the
    /// production store, not a separate stream map in this boundary fixture.
    fn sealed(&mut self, job_id: JobId) -> Result<Option<SealedJobReader>> {
        Ok(self.terminals.get(&job_id).cloned())
    }
}

struct FakeArtifactJob {
    record: JobArtifactRecord,
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
                self.record.job_id,
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

    fn seal(self, ending: JobEnding) -> Result<(ArtifactSeal, SealedJobReader)> {
        let id = self.record.job_id;
        self.observations
            .send(ArtifactObservation::Seal(id, ending.state))
            .expect("artifact observer");
        self.order
            .send(OrderObservation::ArtifactSeal(id))
            .expect("order observer");
        let JobEnding {
            state,
            exit,
            duration_ms,
            resources,
            failure,
            nice,
            containment,
            outlived,
            outlived_incomplete,
        } = ending;
        let record = JobArtifactRecord {
            state,
            exit,
            duration_ms,
            resources,
            failure,
            nice,
            containment,
            outlived,
            outlived_incomplete,
            stdout: stream(self.sealed_stdout.unwrap_or(self.stdout)),
            stderr: stream(self.stderr),
            output_limit: self.crossing,
            ..self.record
        };
        let sealed = ArtifactSeal {
            stdout: record.stdout.clone(),
            stderr: record.stderr.clone(),
            terminal_batch_sha256: Sha256Digest::compute(&id.get().to_be_bytes()),
            output_limit: record.output_limit.clone(),
            publication_failure: None,
        };
        let terminal = SealedJobReader::inline(record)
            .expect("fixture produced a valid inline terminal record");
        Ok((sealed, terminal))
    }
}

struct FakeCommitments {
    next_order: u64,
    observations: mpsc::UnboundedSender<ControllerCommitment>,
    order: mpsc::UnboundedSender<OrderObservation>,
}

/// Commitments recorded as [`FakeCommitments`] records them, a job's terminal one only after
/// `delay`: a controller slow to publish a job's end, while the job is not yet concluded.
struct SlowTerminal {
    inner: FakeCommitments,
    delay: tokio::sync::watch::Receiver<Duration>,
}

#[async_trait]
impl CommitmentSink for SlowTerminal {
    async fn record(&mut self, draft: CommitmentDraft) -> Result<()> {
        let delay = *self.delay.borrow();
        if !delay.is_zero() && matches!(draft, CommitmentDraft::Terminal { .. }) {
            tokio::time::sleep(delay).await;
        }
        self.inner.record(draft).await
    }
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
                ControllerCommitment::Fork(_) => "fork",
                ControllerCommitment::LandAdoption(_) => "land-adoption",
                ControllerCommitment::LandOutcome(_) => "land-outcome",
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
        census: census(),
        authority: authority(),
        owned_repo_ids: OwnedRepoIds::sole(authority().repo_id),
        workspace_root: workspace_root.clone(),
        default_cwd: Some(WorkspacePath::new("packages/app").unwrap()),
        sandbox: SandboxConfig {
            home: PathBuf::from("/Users/tester"),
            mount_root: root.join("mounts"),
            workspace_mount: workspace_root,
            exec_temp_dir: PathBuf::from("/tmp/cowshed-exec"),
            store_root: root.join("store"),
            port_block: PortBlock::new(49_136, 16).unwrap(),
            retained_port_blocks: Vec::new(),
            mode: cowshed_core::sandbox::RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            cpu_pool_directory: None,
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            main_mount: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            program_directories: Vec::new(),
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
        group_ledger_lease: None,
        membership_socket: None,
        telemetry_root: None,
        inherited_groups: Vec::new(),
        volume_labels: None,
        workspace_volume: None,
        job_sampling: cowshed_core::runtime::supervisor::JobSamplingPolicy::default(),
        build_volume_released: None,
        memory_pressure: calm_pressure::calm(),
        nx_daemon_idle: cowshed_core::runtime::supervisor::NxDaemonIdle::DEFAULT,
    }
}

fn gate(request: ExecRequest) -> ExecRequest {
    ExecRequest {
        class: ExecClass::Gate,
        ..request
    }
}

/// A test's private root holding the workspace `config` places in it, removed when the test
/// ends on any path. Each test binds it before any supervisor over it, so it outlives them all.
fn workspace_root(parent: &Path, label: &str) -> TempRoot {
    // Capability containment compares real directories against this workspace root. Socket fixtures
    // own separate short aliases and never pass through this filesystem-only constructor.
    let parent = std::fs::canonicalize(parent).expect("canonical granted workspace fixture parent");
    let root = TempRoot::new_in(&parent, &format!("cowshed-supervisor-{label}"));
    // A minted workspace holds `.cowshed`, where its supervisor publishes the host environment.
    std::fs::create_dir_all(root.join("workspace/.cowshed")).expect("workspace root");
    root
}

fn harness(start_id: u64, quota: u64, fail_next: bool, backpressure: bool) -> (Harness, TempRoot) {
    let root = workspace_root(&temp_parent::scratch_parent(), "test-workspace");
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
        tokio::sync::watch::channel(Duration::ZERO).1,
    )
}

/// A harness whose artifact sink commits a stdout stream that is not what the job wrote.
fn harness_with_sealed_stdout(sealed_stdout: Vec<u8>) -> (Harness, TempRoot) {
    let root = workspace_root(&temp_parent::scratch_parent(), "test-workspace");
    let h = harness_with_artifacts(
        config(&root),
        1,
        1024,
        false,
        false,
        Some(sealed_stdout),
        tokio::sync::watch::channel(Duration::ZERO).1,
    );
    (h, root)
}

fn harness_with_artifacts(
    supervisor_config: WorkspaceSupervisorConfig,
    start_id: u64,
    quota: u64,
    fail_next: bool,
    backpressure: bool,
    sealed_stdout: Option<Vec<u8>>,
    terminal_delay: tokio::sync::watch::Receiver<Duration>,
) -> Harness {
    let (spawn_tx, spawned) = mpsc::unbounded_channel();
    let (process_tx, process) = mpsc::unbounded_channel();
    let (artifact_tx, artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, commitments) = mpsc::unbounded_channel();
    let (order_tx, order) = mpsc::unbounded_channel();
    let artifact_authority = supervisor_config.authority.clone();
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
            quota,
            jobs: BTreeMap::new(),
            terminals: BTreeMap::new(),
            authority: artifact_authority,
            observations: artifact_tx,
            order: order_tx.clone(),
        }),
        Box::new(SlowTerminal {
            inner: FakeCommitments {
                next_order: 1,
                observations: commitment_tx,
                order: order_tx,
            },
            delay: terminal_delay,
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
/// workspace root, so records and admissions persist across supervisors exactly as they do
/// across `cowshed` processes.
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
        census(),
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
        class: ExecClass::Background,
        want: None,
        scope: ExecScope::Workspace,
    }
}

fn isolated_config(parent: &Path, label: &str) -> (WorkspaceSupervisorConfig, TempRoot) {
    let root = workspace_root(parent, label);
    let mut supervisor_config = config(&root);
    supervisor_config.sandbox.home = root.join("home");
    supervisor_config.sandbox.exec_temp_dir = root.join("tmp");
    // The store the launcher serves is the fixture's own, and exists: a grant makes every spawn
    // read its Git fetch routes there (04_sandbox.md, "Exec pipeline" step 4).
    std::fs::create_dir_all(&supervisor_config.sandbox.store_root).expect("fixture store");
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
        let (mut supervisor_config, root) =
            isolated_config(&temp_parent::canonical_parent(), "exec-mode");
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
            let job = h
                .handle
                .exec(None, None, exec, AdmissionSource::Worker)
                .await
                .unwrap();
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

/// 04_sandbox.md, "Read policy" and "Store reads": an admitted Nix program can load
/// other store entries without a Nix project or a separate grant for each dependency.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_admitted_nix_bun_loads_icu_without_a_nix_project() {
    let directories = std::env::split_paths(&std::env::var_os("PATH").expect("host tool PATH"))
        .collect::<Vec<_>>();
    let bun = cowshed_core::capabilities::bootstrap_program("bun", &directories)
        .unwrap()
        .expect("run the host-controller lane in the repository's Nix/devenv shell");
    let package = bun.parent().unwrap().parent().unwrap();
    assert_eq!(
        package.parent(),
        Some(Path::new("/nix/store")),
        "the regression requires the repository's Nix Bun, not a standalone Bun: {}",
        bun.display()
    );
    let libraries = std::process::Command::new("/usr/bin/otool")
        .arg("-L")
        .arg(&bun)
        .output_bounded()
        .unwrap();
    assert!(libraries.status.success(), "{libraries:?}");
    assert!(
        String::from_utf8_lossy(&libraries.stdout)
            .lines()
            .any(|line| line.trim().starts_with("/nix/store/") && line.contains("/lib/libicu")),
        "the admitted Bun must actually load a separate Nix ICU library: {libraries:?}"
    );

    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::canonical_parent(), "nix-bun-icu");
    supervisor_config.default_cwd = None;
    supervisor_config.artifacts.combined_output_quota_bytes = 64 * 1024;
    // Admit only Bun's package. No capability marker or tool link can contribute
    // a store-wide grant and mask a missing baseline metadata rule.
    supervisor_config
        .sandbox
        .grants
        .read
        .push(package.to_path_buf());
    let mount = &supervisor_config.workspace_root;
    std::fs::create_dir_all(&supervisor_config.sandbox.home).unwrap();
    std::fs::create_dir_all(mount.join(".cowshed/bin")).unwrap();
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
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
    let job = handle.exec(None, None, ExecRequest {
        command: ExecCommand::Argv(vec![
            CommandArg::from(bun.into_os_string()),
            "--config=/dev/null".into(),
            "--no-env-file".into(),
            "--no-install".into(),
            "--eval".into(),
            "console.log(new Intl.DateTimeFormat('en-GB', {year:'numeric', month:'2-digit', day:'2-digit', timeZone:'UTC'}).format(new Date('2026-10-12T00:00:00Z')))".into(),
        ]),
        cwd: None,
        ..request(StdinSource::Empty)
    }, AdmissionSource::Worker)
        .await
        .unwrap();
    let info = handle.wait(job).await.unwrap();
    let stderr = handle
        .log_read(job, StreamKind::Stderr, 0, false)
        .await
        .unwrap();
    let stdout = handle
        .log_read(job, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
    assert_eq!(
        info.exit,
        Some(ExitStatus::Exited { code: 0 }),
        "admitted Bun must load ICU under the production child sandbox: {}",
        String::from_utf8_lossy(&stderr.bytes)
    );
    assert_eq!(stdout.bytes.as_ref(), b"12/10/2026\n");
    assert!(stderr.bytes.is_empty(), "{stderr:?}");
    drop(root);
}

#[tokio::test]
async fn graceful_requested_cancellation_keeps_its_actual_exit_serializable() {
    let (mut h, _root) = harness(1, 1024, false, false);
    for code in [0, 143] {
        let job = h
            .handle
            .exec(
                None,
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker,
            )
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
    let job = h
        .handle
        .exec(None, None, exec, AdmissionSource::Worker)
        .await
        .unwrap();
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
        let error = h
            .handle
            .exec(None, None, exec, AdmissionSource::Worker)
            .await
            .unwrap_err();
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
    let root = workspace_root(&temp_parent::scratch_parent(), "build-volume");
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
    std::fs::create_dir_all(layout.mounts()).unwrap();
    for id in [&before_id, &adopted_id] {
        std::fs::File::create(layout.image(id)).unwrap();
    }
    let before = layout.mount(&before_id);
    let adopted = layout.mount(&adopted_id);
    supervisor_config.build_volume_layout = Some(layout.clone());
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let first = h
        .handle
        .exec(
            None,
            Some(before.clone()),
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let first_spawn = h.spawned.recv().await.unwrap();
    let second = h
        .handle
        .exec(
            None,
            Some(adopted.clone()),
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    let binding = |volume: &Path| OsString::from(format!("BUILD_VOLUME={}", volume.display()));
    let (_, first_profile) = first_spawn.request.policy.child(RunSandboxMode::ReadWrite);
    let (_, second_profile) = second_spawn.request.policy.child(RunSandboxMode::ReadWrite);
    assert_eq!(
        first_profile.text(),
        second_profile.text(),
        "a pivot binds the new volume to the same profile"
    );
    let first_args = first_profile.launch().unwrap().args();
    let second_args = second_profile.launch().unwrap().args();
    assert!(first_args.contains(&binding(&before)));
    assert!(!first_args.contains(&binding(&adopted)));
    assert!(second_args.contains(&binding(&adopted)));
    assert!(!second_args.contains(&binding(&before)));
    // A checkout that links no volume grants none.
    let third = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let second = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    let mut invalid = config(&root).sandbox;
    invalid.home = PathBuf::from("relative/home");
    let refused = match h.handle.advance_authority(8, 12, invalid).await {
        Ok(_) => panic!("a non-absolute HOME must refuse profile rendering"),
        Err(error) => error,
    };
    assert!(
        refused.message.contains("path is not absolute"),
        "{}",
        refused.message
    );
    h.handle.session_snapshot(&session).await.unwrap();
    let unchanged = h
        .handle
        .advance_authority(
            authority().grant_revision,
            authority().lifecycle_revision,
            config(&root).sandbox,
        )
        .await
        .unwrap();
    let same = open_named(&unchanged, "build").await;
    assert_eq!(session.identity(), same.identity());
    unchanged.session_snapshot(&session).await.unwrap();
    let advanced = h
        .handle
        .advance_authority(8, 12, config(&root).sandbox)
        .await
        .unwrap();
    let stale = old.list().await.unwrap_err();
    assert_eq!(stale.code, ErrorCode::Conflict);
    let stale_session = advanced
        .exec(
            Some(&session),
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            Some(&session),
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    h.handle.detach(job).await.unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let survivor = h.handle.clone();
    drop(h.handle);
    complete(&spawned, b"alive", b"", ExitStatus::Exited { code: 0 }).await;
    let info = survivor.wait(job).await.unwrap();
    assert_eq!(info.state, JobState::Exited);
    assert_eq!(info.stdout.bytes, 5);
}

/// Admission through the supervisor on a driven pressure feed (05_gateway.md "Admission"): under
/// Warning a background start spawns nothing while a gate asked after it spawns at once; the
/// reported drop spawns the background job, whose stderr names its wait.
#[tokio::test]
async fn a_background_start_waits_for_the_reported_drop_and_a_gate_does_not() {
    use cowshed_core::host_pressure::{Reading, Source};
    use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure};
    let level = |level| Reading::Known(MemoryPressure::Darwin { level });
    let (pressure, reports) = tokio::sync::watch::channel(level(DarwinPressureLevel::Warning));
    let root = workspace_root(&temp_parent::scratch_parent(), "test-workspace");
    let mut h = harness_with_config(
        WorkspaceSupervisorConfig {
            memory_pressure: Source::Driven(reports),
            ..config(&root)
        },
        1,
        1024,
        false,
        false,
    );
    let held = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let gated = h
        .handle
        .exec(
            None,
            None,
            gate(request(StdinSource::Empty)),
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let first = h.spawned.recv().await.unwrap();
    assert_eq!(
        first.request.job_id, gated,
        "the gate is not held behind it"
    );
    assert!(
        h.spawned.try_recv().is_err(),
        "the background start is held"
    );

    pressure.send_replace(level(DarwinPressureLevel::Normal));
    let second = h.spawned.recv().await.unwrap();
    assert_eq!(second.request.job_id, held);
    complete(&second, b"", b"", ExitStatus::Exited { code: 0 }).await;
    complete(&first, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(h.handle.wait(held).await.unwrap().state, JobState::Exited);
    let stderr = h
        .handle
        .log_read(held, StreamKind::Stderr, 0, false)
        .await
        .unwrap();
    let stderr = String::from_utf8_lossy(&stderr.bytes);
    assert!(
        stderr.starts_with("cowshed: jobserver wait start queue=memory pressure=")
            && stderr.contains("cowshed: jobserver wait done elapsed=")
            && stderr.ends_with(" status=ok\n"),
        "{stderr}"
    );
}

/// A checkout gains a capability while its supervisor runs -- its dev shell gains mbx's Cargo
/// shim, an `nx.json` arrives -- and the next exec adopts it: the job's sandbox carries it, and the
/// host shell's `.cowshed/env` says the same, so a host shell and the land's host gate build in the
/// environment the jobs do.
#[tokio::test]
async fn a_capability_gained_while_the_supervisor_runs_reaches_the_host_environment() {
    let (mut h, root) = harness(1, 1024, false, false);
    let workspace = root.join("workspace");
    let env = workspace.join(".cowshed/env");
    assert!(
        std::fs::read_to_string(&env).map_or(true, |text| !text.contains("NX_MAX_CACHE_SIZE")),
        "no Nx cache size before nx.json"
    );
    std::fs::write(workspace.join("nx.json"), "{}").unwrap();
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    let published = std::fs::read_to_string(&env).unwrap();
    assert!(published.contains("NX_MAX_CACHE_SIZE"), "{published}");
}

#[tokio::test]
async fn opaque_non_utf8_output_round_trips_through_logs_and_artifacts() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Open),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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

/// A spawn that fails comes after the exec answered at admission: the job ends `failed` with the
/// failure's wrapper exit code, the typed failure `unlaunched` (its command never ran: an absent
/// program, a held child the observer refused, any spawn error but a nice placement) and one
/// terminal commitment, and its stderr says why.
#[tokio::test]
async fn spawn_failure_is_typed_terminal_with_one_terminal_commitment() {
    let (mut h, _root) = harness(7, 1024, true, false);
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .expect("answered at admission, before the spawn");
    assert_eq!(job.get(), 7);
    let ended = h.handle.wait(job).await.unwrap();
    assert_eq!(
        (ended.state, ended.exit),
        (
            JobState::Failed,
            Some(ExitStatus::Exited {
                code: ErrorCode::EnvironmentMissing
                    .exec_wrapper_exit_code()
                    .into()
            })
        )
    );
    let stderr = h
        .handle
        .log_read(job, StreamKind::Stderr, 0, false)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&stderr.bytes).starts_with("cowshed: "),
        "the spawn failure is said on the job's stderr"
    );
    assert_eq!(ended.failure, Some(JobFailure::Unlaunched));
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
        .exec(Some(&session), None, first, AdmissionSource::Worker)
        .await
        .unwrap();
    h.handle.detach(job).await.unwrap();
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
    let root = workspace_root(&temp_parent::scratch_parent(), "credential-env");
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
        .exec(Some(&session), None, first, AdmissionSource::Worker)
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

/// A job an earlier supervisor of the workspace incarnation ran and sealed is answered by the next
/// one from the durable records — its terminal facts, and its output from any offset — although
/// only the supervisor that ran a job answers its status. A client whose supervisor retired (a
/// drained supervisor of another build retires once its jobs' ends were read, or after 30 idle
/// minutes) still reaches the job by its number, and reads on from the bytes it already holds.
#[tokio::test]
async fn a_fresh_supervisor_answers_for_a_job_its_predecessor_sealed() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "sealed-predecessor");
    let mut first = real_store_harness(supervisor_config.clone());
    let job = first
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    first
        .handle
        .retire(NxDaemonRetirement::Recorded)
        .await
        .unwrap();
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "refused-copy");
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
            AdmissionSource::Worker,
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
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// A job whose terminal record the store refuses has still ended. Its waiters, early and late, and
/// its killers are answered with the refusal instead of queueing forever, its output stays readable
/// and retirement completes. The durable record stays unterminated, so the workspace's next
/// supervisor seals it failed as lost rather than as a success nothing recorded.
#[tokio::test]
async fn a_refused_terminal_record_answers_every_caller_and_stays_unterminated() {
    use std::os::unix::fs::PermissionsExt;

    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "refused-seal");
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
    drop(h);

    std::fs::set_permissions(&job_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (workspace_root, owned, incarnation, artifacts) = store;
    let lost = ArtifactStore::open(workspace_root, owned, incarnation, artifacts, census())
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "refused-commitment");
    let refusal = CowshedError::environment_missing(
        "the commitment publisher is gone",
        "reattach the workspace",
    );
    let store = ArtifactStoreSink::open(
        supervisor_config.workspace_root.clone(),
        &supervisor_config.owned_repo_ids,
        &supervisor_config.authority,
        supervisor_config.artifacts.clone(),
        census(),
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
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
        assert_eq!(group.wait_bounded().unwrap().signal(), Some(libc::SIGKILL));
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
        .args(["-c", "exec 3<&0; read _ <&3 & wait"])
        .stdin(std::process::Stdio::piped())
        .process_group(0)
        .spawn_locked()
        .expect("a test-owned process group")
}

/// `(job id, leader start)` for every group the ledger at `path` names.
#[cfg(target_os = "macos")]
fn ledger_leaders(path: &std::path::Path) -> Vec<(u64, Option<u64>)> {
    let ledger: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    ledger_group_leaders(&ledger)
}

/// The writer, not the actor's next turn, determines when a cut reaches the filesystem.
async fn published_ledger_leaders(path: &Path, count: usize) -> Vec<(u64, Option<u64>)> {
    let ledger = publication::read_when(path.to_path_buf(), move |value| {
        value["groups"]
            .as_array()
            .is_some_and(|groups| groups.len() == count)
    })
    .await;
    ledger_group_leaders(&ledger)
}

fn ledger_group_leaders(ledger: &serde_json::Value) -> Vec<(u64, Option<u64>)> {
    ledger["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|group| {
            (
                group["jobId"].as_u64().unwrap(),
                group["leaderStart"].as_u64(),
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
    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::scratch_parent(), "ledger-births");
    let ledger = root.join("supervisor.groups");
    supervisor_config.group_ledger = Some(ledger.clone());
    let (birth_tx, mut births) = mpsc::unbounded_channel();
    let (spawn_tx, mut spawns) = mpsc::unbounded_channel();
    let (process_tx, _process) = mpsc::unbounded_channel();
    let (artifact_tx, _artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, _commitments) = mpsc::unbounded_channel();
    let (order_tx, _order) = mpsc::unbounded_channel();
    let artifact_authority = supervisor_config.authority.clone();
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
            quota: 1024,
            jobs: BTreeMap::new(),
            terminals: BTreeMap::new(),
            authority: artifact_authority,
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
        Some(leader.start())
    };

    handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let first = birth(births.recv().await.unwrap());
    assert_eq!(published_ledger_leaders(&ledger, 1).await, vec![(1, first)]);
    handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let second = birth(births.recv().await.unwrap());
    assert_eq!(
        published_ledger_leaders(&ledger, 2).await,
        vec![(1, first), (2, second)]
    );
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
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// A job whose parent could not identify its group leader still runs and reports its pid, but its
/// group never enters the ledger: nothing may later end a group no one identified.
#[tokio::test]
async fn an_unidentified_leader_never_enters_the_ledger() {
    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::scratch_parent(), "ledger-unobserved");
    let ledger = root.join("supervisor.groups");
    supervisor_config.group_ledger = Some(ledger.clone());
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    assert_eq!(h.handle.info(job).await.unwrap().pid, Some(10_001));
    assert_eq!(published_ledger_leaders(&ledger, 0).await, Vec::new());
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// The groups a lost predecessor left unresolved neither stop the next supervisor from serving
/// nor fall out of its ledger: each rewrite carries them while processes hold their ids, and drops
/// one only once nothing does. Never signalled throughout.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_supervisor_serves_and_carries_the_groups_its_predecessor_left_unresolved() {
    use std::io::BufRead as _;
    use std::os::unix::process::CommandExt as _;

    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::scratch_parent(), "ledger-inherited");
    let ledger = root.join("supervisor.groups");
    // A group whose leader exited and was reaped while a descendant holds the id.
    let mut leaderless = std::process::Command::new("/bin/sh")
        .args([
            "-c",
            "exec 3<&0; (trap '' TERM; echo READY; read _ <&3) & wait",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .process_group(0)
        .spawn_locked()
        .unwrap();
    let pgid = i32::try_from(leaderless.id()).unwrap();
    let mut ready = String::new();
    std::io::BufReader::new(leaderless.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    // The descendant reads this pipe: reaping the leader must not close it.
    let held = leaderless.stdin.take();
    leaderless.kill().unwrap();
    leaderless.wait_bounded().unwrap();
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    publication::read_when(ledger.clone(), |value| {
        value["unresolved"]
            .as_array()
            .is_some_and(|groups| !groups.is_empty())
    })
    .await;
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
    drop(held);
    while !cowshed_core::runtime::job_groups::unresolved(&ledger)
        .unwrap()
        .is_empty()
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    let second = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    publication::read_when(ledger.clone(), |value| {
        value["unresolved"].as_array().is_none_or(Vec::is_empty)
    })
    .await;
    assert_eq!(carried(&ledger), Vec::<u64>::new());
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(second).await.unwrap();
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

#[tokio::test]
async fn retire_waits_for_process_tree_stop_and_terminal_persistence() {
    let (mut h, _root) = harness(1, 1024, false, false);
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let retire = tokio::spawn(async move { handle.retire(NxDaemonRetirement::Recorded).await });
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
            .exec(
                None,
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker
            )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        fn ensure_label(&self, _image: &Path, _label: &str) -> std::io::Result<Labelled> {
            self.entered.send(()).map_err(std::io::Error::other)?;
            self.release
                .lock()
                .expect("unpoisoned labeller gate")
                .recv()
                .map_err(std::io::Error::other)?;
            Ok(Labelled::Renamed)
        }
    }
    let root = workspace_root(&temp_parent::scratch_parent(), "volume-label-quiesce");
    let (entered, mut renaming) = mpsc::unbounded_channel();
    let (release, gate) = std::sync::mpsc::channel();
    let mut supervisor = config(&root);
    let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
    supervisor.volume_labels = Some(VolumeLabels {
        workspace_image: root.join("store").join("main.dmg"),
        workspace: namespace.volume_label(format_args!("acme · widget — main")),
        build: namespace.volume_label(format_args!("acme · widget — build main")),
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
        fn ensure_label(&self, image: &Path, _label: &str) -> std::io::Result<Labelled> {
            self.0
                .send(image.to_owned())
                .map_err(std::io::Error::other)?;
            Ok(Labelled::Renamed)
        }
    }
    let root = workspace_root(&temp_parent::scratch_parent(), "volume-label-authority");
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
    let image = root.join("store").join("main.dmg");
    let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
    supervisor.volume_labels = Some(VolumeLabels {
        workspace_image: image.clone(),
        workspace: namespace.volume_label(format_args!("acme · widget — main")),
        build: namespace.volume_label(format_args!("acme · widget — build main")),
        labeller: std::sync::Arc::new(RecordingLabeller(named)),
    });
    let h = harness_with_config(supervisor, 1, 1024, false, false);
    assert_eq!(
        labels.recv().await.unwrap(),
        image,
        "the initial workspace label names the workspace's image, never its checkout path"
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    let handle = h.handle.clone();
    let quiesce = tokio::spawn(async move { handle.quiesce().await });
    tokio::task::yield_now().await;
    assert!(!quiesce.is_finished());
    assert_eq!(
        h.handle
            .exec(
                None,
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker
            )
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
async fn retire_if_idle_refuses_busy_without_closing_admission_or_waiting() {
    let (mut h, _root) = harness(2, 1024, false, false);
    let first = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let first_spawned = h.spawned.recv().await.unwrap();
    let refused = h
        .handle
        .retire_if_idle(NxDaemonRetirement::Recorded)
        .await
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    let second = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .expect("busy refusal must leave admission open");
    let second_spawned = h.spawned.recv().await.unwrap();
    complete(&first_spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    complete(&second_spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(first).await.unwrap();
    h.handle.wait(second).await.unwrap();
    h.handle
        .retire_if_idle(NxDaemonRetirement::Recorded)
        .await
        .unwrap();
    assert_eq!(
        h.handle
            .exec(
                None,
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

#[tokio::test]
async fn none_cwd_is_preserved_as_workspace_root_without_a_sentinel() {
    let root = workspace_root(&temp_parent::scratch_parent(), "none-cwd");
    let mut supervisor_config = config(&root);
    supervisor_config.default_cwd = None;
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let mut exec = request(StdinSource::Empty);
    exec.cwd = None;
    let job = h
        .handle
        .exec(None, None, exec, AdmissionSource::Worker)
        .await
        .unwrap();
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"written", b"", ExitStatus::Exited { code: 0 }).await;
    let info = h.handle.wait(job).await.unwrap();
    assert_eq!(info.stdout.bytes, 6);
    let terminal = h
        .handle
        .sealed(job)
        .await
        .expect("the canonical fake terminal is present");
    assert_eq!(terminal.stdout, info.stdout);
    assert_eq!(terminal.stderr, info.stderr);

    let chunk = h
        .handle
        .log_read(job, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!(chunk.bytes, Bytes::from_static(b"sealed"));
    assert_eq!(chunk.next_offset, 6);
    assert!(chunk.eof);

    // A page starts at its offset; reading at the committed end is a clean eof.
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
            .exec(
                None,
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker,
            )
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

/// A Unix socket path in a private directory of its own, short because a socket path is bounded
/// at 104 bytes on macOS. The directory goes with its owner on every exit path, panics included,
/// so no test leaves an entry in `/tmp`.
struct SocketPath {
    path: PathBuf,
    _directory: TempRoot,
}

impl SocketPath {
    fn new() -> Self {
        let directory = TempRoot::new_in(&temp_parent::scratch_parent(), "cowshed-sock");
        Self {
            path: directory.join("s"),
            _directory: directory,
        }
    }
}

impl std::ops::Deref for SocketPath {
    type Target = PathBuf;

    fn deref(&self) -> &PathBuf {
        &self.path
    }
}

impl AsRef<Path> for SocketPath {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}
/// `handle`'s supervisor served on a fresh socket, and a handle that reaches it only through
/// that socket.
async fn served(handle: &WorkspaceSupervisorHandle) -> (WorkspaceSupervisorHandle, SocketPath) {
    use cowshed_core::runtime::supervisor_socket;
    let path = SocketPath::new();
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
            AdmissionSource::Worker,
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Stream(Box::pin(reader))),
            AdmissionSource::Worker,
        )
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

    let (supervisor_config, _root) = isolated_config(&temp_parent::scratch_parent(), "keyed-open");
    let mut h = real_store_harness(supervisor_config);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(
            None,
            None,
            keyed("op-open", StdinSource::Open),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(
        remote
            .exec(
                None,
                None,
                keyed("op-open", StdinSource::Open),
                AdmissionSource::Worker
            )
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
            AdmissionSource::Worker,
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
            .exec(
                None,
                None,
                keyed("op-open", StdinSource::Open),
                AdmissionSource::Worker
            )
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "keyed-stream");
    let mut h = real_store_harness(supervisor_config);
    let (remote, _path) = served(&h.handle).await;
    let (mut writer, reader) = tokio::io::duplex(64);
    let job = remote
        .exec(
            None,
            None,
            keyed("op-1", StdinSource::Stream(Box::pin(reader))),
            AdmissionSource::Worker,
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
            AdmissionSource::Worker,
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
    let (mut supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "keyed-fields");
    supervisor_config
        .credential_env_names
        .insert("TEST_SECRET".to_owned());
    let mut h = real_store_harness(supervisor_config);
    let (remote, _path) = served(&h.handle).await;
    let job = remote
        .exec(
            None,
            None,
            keyed("op-fields", StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"once\n", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(job).await.unwrap();
    let mut same = keyed("op-fields", StdinSource::Empty);
    same.env
        .insert("TEST_SECRET".to_owned(), "not an admitted field".to_owned());
    assert_eq!(
        remote
            .exec(None, None, same, AdmissionSource::Worker)
            .await
            .unwrap(),
        job
    );

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
        AdmissionField::Want,
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
            AdmissionField::Want => changed.want = NonZeroUsize::new(4),
            AdmissionField::Class | AdmissionField::Scope => {
                unreachable!("a class and its scope change together, below")
            }
        }
        let refused = remote
            .exec(session, None, changed, AdmissionSource::Worker)
            .await
            .unwrap_err();
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
    // The gate class and the host-gate scope change together, and only a coordinator may ask for
    // either.
    for (changed, fields) in [
        (
            gate(keyed("op-fields", StdinSource::Empty)),
            vec![AdmissionField::Class],
        ),
        (
            ExecRequest {
                scope: ExecScope::HostGate,
                ..gate(keyed("op-fields", StdinSource::Empty))
            },
            vec![AdmissionField::Scope, AdmissionField::Class],
        ),
    ] {
        let refused = remote
            .exec(None, None, changed, AdmissionSource::Coordinator)
            .await
            .unwrap_err();
        assert_eq!(
            refused.admission_source(),
            Some(&AdmissionRefusal::KeyConflict {
                job_id: job,
                fields
            })
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "keyed-absent");
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "keyed-lost-reply");
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
                                "source": AdmissionSource::Worker,
                "request": {
                    "argv": argv,
                    "script": null,
                    "cwd": sent.cwd,
                    "mode": sent.mode,
                    "env": sent.env,
                                        "class": sent.class,
                    "want": sent.want,
                    "scope": sent.scope,
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
    let mut caller = tokio::net::UnixStream::connect_bounded(&path)
        .await
        .unwrap();
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
    first
        .handle
        .retire(NxDaemonRetirement::Recorded)
        .await
        .unwrap();
    drop(first);

    let mut second = real_store_harness(supervisor_config);
    let (remote, _path) = served(&second.handle).await;
    let key = AdmissionKey::new("op-1").unwrap();
    assert_eq!(remote.job_by_key(key.clone()).await.unwrap(), Some(job));
    assert_eq!(
        remote
            .exec(
                None,
                None,
                keyed("op-1", payload()),
                AdmissionSource::Worker
            )
            .await
            .unwrap(),
        job
    );

    let changed_argv = ExecRequest {
        command: ExecCommand::Argv(vec!["printf".into(), "other".into()]),
        ..keyed("op-1", payload())
    };
    let conflict = remote
        .exec(None, None, changed_argv, AdmissionSource::Worker)
        .await
        .unwrap_err();
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
            AdmissionSource::Worker,
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "keyed-unprovable");
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
        .exec(
            None,
            None,
            keyed("op-1", StdinSource::Empty),
            AdmissionSource::Worker,
        )
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

/// One real-store lifecycle distinguishes authoritative absence from unread numeric history and
/// preserves readable captured content. Every served read retains the same structured refusal.
/// A job this supervisor holds unsealed answers NotFound, never that refusal. A sealed one
/// still answers under the unread history.
#[tokio::test]
async fn sealed_reads_preserve_unread_history_without_hiding_readable_jobs() {
    use std::os::unix::fs::DirBuilderExt as _;
    let (config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "sealed-history-authority");
    let mut empty = real_store_harness(config.clone());
    let (remote, _socket) = served(&empty.handle).await;
    let missing = JobId::new(42).unwrap();
    let limits = JobTailLimits {
        bytes_per_stream: JobTailBytes::new(64).unwrap(),
        lines_per_stream: std::num::NonZeroU32::new(8).unwrap(),
    };
    for error in [
        remote.sealed(missing).await.unwrap_err(),
        remote
            .log_read(missing, StreamKind::Stdout, 0, false)
            .await
            .unwrap_err(),
        remote.tail(missing, None, limits).await.unwrap_err(),
    ] {
        assert_eq!(
            error.code,
            ErrorCode::NotFound,
            "a genuinely empty protected store proves absence"
        );
        assert_eq!(error.admission_source(), None);
    }
    assert!(empty.spawned.try_recv().is_err());
    empty
        .handle
        .retire(NxDaemonRetirement::Recorded)
        .await
        .unwrap();
    drop(empty);

    let mut store = ArtifactStoreSink::open(
        config.workspace_root.clone(),
        &config.owned_repo_ids,
        &config.authority,
        config.artifacts.clone(),
        census(),
    )
    .unwrap();
    let readable = store.next_job_id().unwrap();
    store
        .admit(
            readable,
            config.authority.grant_revision,
            &request(StdinSource::Empty).command,
            None,
            JobScheduling::new(
                ExecScope::Workspace,
                ExecClass::Background,
                NonZeroUsize::MIN,
            ),
        )
        .unwrap();
    store
        .write(readable, StreamKind::Stdout, b"real captured bytes\n")
        .unwrap();
    store
        .seal(readable, JobState::Failed.into(), None, None)
        .unwrap();
    drop(store);

    // Numeric ids predate keyed layouts, and an opening marker cannot prove the contents of
    // retained lineage. No archived payload is projected or interpreted in this fixture.
    let set_aside = config
        .workspace_root
        .join(".cowshed/job/set-aside")
        .join(format!("layout-{}", FIRST_KEYED_LAYOUT - 1));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&set_aside)
        .unwrap();
    std::fs::write(
        set_aside.join("incarnation"),
        "1198f2c0b7e34dc795f17b238b331c80",
    )
    .unwrap();
    let mut resumed = real_store_harness(config);
    let (remote, _socket) = served(&resumed.handle).await;
    let refusal = AdmissionRefusal::Unprovable { set_aside };
    let direct = resumed.handle.sealed(missing).await.unwrap_err();
    assert_eq!(direct.code, ErrorCode::Conflict);
    assert_eq!(direct.admission_source(), Some(&refusal));
    for error in [
        remote.sealed(missing).await.unwrap_err(),
        remote
            .log_read(missing, StreamKind::Stdout, 0, false)
            .await
            .unwrap_err(),
        remote.tail(missing, None, limits).await.unwrap_err(),
    ] {
        assert_eq!(
            error, direct,
            "each socket read preserves the complete same history refusal"
        );
    }
    assert_eq!(
        remote
            .job_by_key(AdmissionKey::new("unknown-key").unwrap())
            .await
            .unwrap(),
        None,
        "pre-key history never changes key lookup's narrower contract"
    );
    let sealed = remote.sealed(readable).await.unwrap();
    assert_eq!(sealed.job_id, readable);
    let log = remote
        .log_read(readable, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    assert_eq!(log.bytes.as_ref(), b"real captured bytes\n");
    let tail = remote.tail(readable, None, limits).await.unwrap();
    assert_eq!(tail.stdout.as_bytes(), b"real captured bytes\n");
    assert!(
        resumed.spawned.try_recv().is_err(),
        "absence, refusal and restored reads start no process"
    );

    // One deliberate admission under the same unread history. Until it seals, the supervisor's
    // own state answers: NotFound with no admission refusal, on the direct and socket paths alike.
    let admitted = resumed
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = resumed.spawned.recv().await.unwrap();
    assert_eq!(spawned.request.job_id, admitted);
    let held = [
        resumed.handle.sealed(admitted).await.unwrap_err(),
        remote.sealed(admitted).await.unwrap_err(),
    ];
    for error in &held {
        assert_eq!(
            error.code,
            ErrorCode::NotFound,
            "a held unsealed job has no terminal record yet"
        );
        assert_eq!(error.admission_source(), None);
    }
    assert_eq!(held[0], held[1], "the socket preserves the same answer");
    complete(&spawned, b"admitted\n", b"", ExitStatus::Exited { code: 0 }).await;
    remote.wait(admitted).await.unwrap();
    for answer in [
        resumed.handle.sealed(admitted).await.unwrap(),
        remote.sealed(admitted).await.unwrap(),
    ] {
        assert_eq!(
            answer.job_id, admitted,
            "a sealed job answers despite unread history"
        );
    }
    assert!(
        resumed.spawned.try_recv().is_err(),
        "the one admission spawned once; no read replayed it"
    );
}

#[test]
fn a_corrupt_or_unreadable_admission_history_refuses_the_supervisor_store() {
    use cowshed_core::storage::job_artifact::ArtifactError;
    use std::os::unix::fs::DirBuilderExt as _;

    for unreadable in [false, true] {
        let (config, _root) =
            isolated_config(&temp_parent::scratch_parent(), "keyed-refused-store");
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
            census(),
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
            census(),
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
        .exec(
            None,
            None,
            request(StdinSource::Open),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Open),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Open),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Open),
            AdmissionSource::Worker,
        )
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
        path.to_path_buf(),
        WorkspaceAuthoritySnapshot {
            grant_revision: authority().grant_revision + 1,
            ..authority()
        },
    );
    let refused = stale
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        let path = SocketPath::new();
        let other = other_build_supervisor::OtherBuild::serve(&path, answer);
        let refused = cowshed_core::runtime::supervisor_socket::hello(&path)
            .await
            .unwrap_err();
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert!(refused.message.contains(named), "{}", refused.message);
        assert_eq!(other.hellos(), 1);
    }
}

/// Starts `supervisor` on the workspace socket in-process, standing in for the supervisor
/// process, and hands the manager a real child to watch.
struct InProcessSpawner {
    store_root: PathBuf,
    supervisor: Option<WorkspaceSupervisorHandle>,
    child: &'static [&'static str],
    spawned: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// The child's stdin. The spawner holds the write end, so a child reading it runs until the
    /// manager drops its spawner.
    stdin: (std::io::PipeReader, std::io::PipeWriter),
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
            .stdin(self.stdin.0.try_clone()?)
            .kill_on_drop(true)
            .spawn_locked()
    }
}

/// The manager's own store, short enough for the supervisor sockets under its `run` directory and
/// removed with its owner on every exit path.
fn manager_store() -> TempRoot {
    TempRoot::new_in(&temp_parent::scratch_parent(), "cowshed-mgr")
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
        &*store,
        Box::new(InProcessSpawner {
            store_root: store.to_path_buf(),
            supervisor: Some(h.handle.clone()),
            child: &["sh", "-c", "read _"],
            stdin: std::io::pipe().unwrap(),
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
        &*store,
        Box::new(InProcessSpawner {
            store_root: store.to_path_buf(),
            supervisor: None,
            child: &["sh", "-c", "exit 3"],
            stdin: std::io::pipe().unwrap(),
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
        &*store,
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
    let (mut h, root) = harness(2, 1024, false, false);
    let session = open_named(&h.handle, "build").await;
    let running = h
        .handle
        .exec(
            Some(&session),
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let running_spawn = h.spawned.recv().await.unwrap();
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
        &*store,
        Box::new(InProcessSpawner {
            store_root: store.to_path_buf(),
            supervisor: None,
            child: &["sh", "-c", "read _"],
            stdin: std::io::pipe().unwrap(),
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
    assert_eq!(
        remote
            .exec(
                Some(&session),
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict,
        "the session opened under the old grants cannot cross revisions"
    );
    let current_session = open_named(&remote, "build").await;
    assert_ne!(current_session.identity(), session.identity());
    let admitted = remote
        .exec(
            Some(&current_session),
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(
        remote.wait(admitted).await.unwrap().grant_revision,
        newer.grant_revision,
        "new commands use the advanced grant revision while the old job still runs"
    );
    assert_eq!(
        remote
            .list()
            .await
            .unwrap()
            .iter()
            .find(|job| job.job_id == running)
            .unwrap()
            .state,
        JobState::Running
    );
    complete(&running_spawn, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(
        remote.wait(running).await.unwrap().grant_revision,
        authority().grant_revision,
        "the running job retains its original enforced revision"
    );
    remote.close_session(current_session).await.unwrap();
    remote.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// The served supervisor for a running ordinary background job, as every foreground `cowshed
/// exec` reaches it whatever its class: the job, its spawn, and the socket a hold names.
async fn held_job_fixture(h: &mut Harness) -> (JobId, Spawned, SocketPath) {
    use cowshed_core::runtime::supervisor_socket;
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(job)
    );
    let store = manager_store();
    let path = SocketPath {
        path: supervisor_socket::socket_path(&store, &authority().repo_id, &authority().workspace),
        _directory: store,
    };
    let listener = supervisor_socket::bind(&path).await.unwrap();
    tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        None,
        None,
    ));
    (job, spawned, path)
}

/// A job whose requester is gone is killed (05_gateway.md, "A job lives with its requester"):
/// the holder's connection closing while the job runs — its process ending, `SIGKILL` included —
/// signals the job's group, and the job ends Killed.
#[tokio::test]
async fn a_held_job_is_killed_when_its_holder_goes_away() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let (job, spawned, path) = held_job_fixture(&mut h).await;
    let hold = supervisor_socket::hold(&path, &authority().workspace_incarnation, job)
        .await
        .unwrap();
    drop(hold);
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(job, ProcessSignal::Term)
    );
    complete(
        &spawned,
        b"",
        b"",
        ExitStatus::Signaled {
            signal: 15,
            core_dumped: false,
        },
    )
    .await;
    assert_eq!(h.handle.wait(job).await.unwrap().state, JobState::Killed);
}

/// A released hold leaves the job running, as `--timeout` leaves a gate it stopped waiting for;
/// a job that ended answers its hold, and closing that hold afterwards kills nothing.
#[tokio::test]
async fn a_released_or_answered_hold_kills_nothing() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let (job, spawned, path) = held_job_fixture(&mut h).await;
    let incarnation = authority().workspace_incarnation;
    supervisor_socket::hold(&path, &incarnation, job)
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
    let answered = supervisor_socket::hold(&path, &incarnation, job)
        .await
        .unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(h.handle.wait(job).await.unwrap().state, JobState::Exited);
    drop(answered);
    assert!(
        h.process.try_recv().is_err(),
        "neither the released hold nor the answered one signalled the job"
    );
    assert_eq!(h.handle.info(job).await.unwrap().state, JobState::Exited);
}

/// A hold names the incarnation its job belongs to; a supervisor serving another one refuses it
/// before anything is held.
#[tokio::test]
async fn a_hold_refuses_another_incarnation() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let (job, _spawned, path) = held_job_fixture(&mut h).await;
    let other = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c81").unwrap();
    assert_eq!(
        supervisor_socket::hold(&path, &other, job)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict
    );
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
        &*store,
        Box::new(InProcessSpawner {
            store_root: store.to_path_buf(),
            supervisor: None,
            child: &["sh", "-c", "read _"],
            stdin: std::io::pipe().unwrap(),
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = h.spawned.recv().await.unwrap();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(
        remote.wait(admitted).await.unwrap().grant_revision,
        read.grant_revision + 1,
        "the job runs under the newer grants"
    );
    remote.retire(NxDaemonRetirement::Recorded).await.unwrap();
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
        &*store,
        Box::new(InProcessSpawner {
            store_root: store.to_path_buf(),
            supervisor: None,
            child: &["sh", "-c", "read _"],
            stdin: std::io::pipe().unwrap(),
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
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

#[tokio::test]
async fn a_drained_supervisor_finishes_its_jobs_admits_none_and_stops_serving() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let path = SocketPath::new();
    let listener = supervisor_socket::bind(&path).await.unwrap();
    let server = tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        None,
        None,
    ));
    let remote = supervisor_socket::connect(path.clone(), authority());
    let job = remote
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();

    assert_eq!(
        supervisor_socket::drain(&path).await.unwrap().pid(),
        std::process::id(),
        "drain answers at once with the serving pid"
    );
    let refused = async {
        loop {
            match remote
                .exec(
                    None,
                    None,
                    request(StdinSource::Empty),
                    AdmissionSource::Worker,
                )
                .await
            {
                Err(error) => return error,
                // Admitted before the drain reached the actor: let it end and try again.
                Ok(admitted) => {
                    let spawned = h.spawned.recv().await.unwrap();
                    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
                    remote.wait(admitted).await.unwrap();
                }
            }
        }
    }
    .await;
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
    server
        .await
        .expect("it stops serving once its jobs ended")
        .unwrap();
}

/// Measured 2026-10-20: an exec client of the replaced build came back for its job's end after the
/// job had ended, found its drained supervisor retired, and the new build's daemon refused it, so
/// work that succeeded reported failure. A drained supervisor now serves the end to that client:
/// it keeps serving after its last job ends, and retires once the end has been read.
#[tokio::test]
async fn a_drained_supervisor_serves_a_job_s_end_to_a_client_that_comes_back_for_it() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let path = SocketPath::new();
    let listener = supervisor_socket::bind(&path).await.unwrap();
    let server = tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        None,
        None,
    ));
    let remote = supervisor_socket::connect(path.clone(), authority());
    let job = remote
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    supervisor_socket::drain(&path).await.unwrap();

    // The job ends while no client waits on it.
    complete(&spawned, b"done", b"", ExitStatus::Exited { code: 0 }).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !server.is_finished(),
        "the job's end is unread, so the drained supervisor still serves"
    );
    supervisor_socket::hello(&path)
        .await
        .expect("its client still reaches it");

    assert_eq!(
        remote.wait(job).await.expect("the end").state,
        JobState::Exited
    );
    server
        .await
        .expect("it retires once the end was read")
        .unwrap();
}

/// Measured 2026-10-21: a supervisor whose 17 jobs had all ended refused every exec as
/// "quiescing or retired" for hours. A caller in another process had quiesced it and gone before
/// it asked the retirement, which was a second request; nothing returns a supervisor to running,
/// and its idle retirement never fired, since the Nx keeper probes only while it runs. A caller
/// in another process now asks one request, retire after the jobs: if it goes away while it
/// waits, the supervisor still retires once the jobs end, and the next command starts another.
/// No remote request closes admission without retiring.
#[tokio::test]
async fn a_caller_that_goes_away_while_waiting_never_leaves_the_supervisor_refusing_work() {
    use cowshed_core::runtime::supervisor_socket;
    let (mut h, _root) = harness(1, 1024, false, false);
    let path = SocketPath::new();
    let listener = supervisor_socket::bind(&path).await.unwrap();
    let server = tokio::spawn(supervisor_socket::serve(
        listener,
        h.handle.clone(),
        None,
        None,
    ));
    let remote = supervisor_socket::connect(path.clone(), authority());
    let job = remote
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();

    assert!(
        remote.quiesce().await.is_err(),
        "a remote caller cannot close admission without retiring"
    );
    let stopping = tokio::spawn({
        let remote = remote.clone();
        async move { remote.retire_after_jobs(NxDaemonRetirement::Recorded).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        remote
            .exec(
                None,
                None,
                request(StdinSource::Empty),
                AdmissionSource::Worker,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::Conflict,
        "admission is closed while the retirement waits"
    );
    // The caller goes away mid-wait: a detach killed by its caller's timeout.
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());

    let _ = job;
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    server
        .await
        .expect("the supervisor retires once its job ends, its caller gone")
        .unwrap();
}

#[tokio::test]
async fn a_controller_reads_a_served_supervisor_s_commitments_by_cursor_until_it_acknowledges() {
    use cowshed_core::runtime::commitment_feed::{CommitmentFeed, FeedingSink};
    use cowshed_core::runtime::supervisor_socket;
    let root = workspace_root(&temp_parent::scratch_parent(), "commitment-feed");
    let (spawn_tx, mut spawned) = mpsc::unbounded_channel();
    let (process_tx, _process) = mpsc::unbounded_channel();
    let (artifact_tx, _artifacts) = mpsc::unbounded_channel();
    let (commitment_tx, _commitments) = mpsc::unbounded_channel();
    let (order_tx, _order) = mpsc::unbounded_channel();
    let feed = CommitmentFeed::default();
    let supervisor_config = config(&root);
    let artifact_authority = supervisor_config.authority.clone();
    let handle = WorkspaceSupervisor::start_with_sinks(
        supervisor_config,
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
            quota: 1024,
            jobs: BTreeMap::new(),
            terminals: BTreeMap::new(),
            authority: artifact_authority,
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
    let path = SocketPath::new();
    let listener = supervisor_socket::bind(&path).await.unwrap();
    tokio::spawn(supervisor_socket::serve(
        listener,
        handle.clone(),
        None,
        Some(feed),
    ));

    let first = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    let (supervisor_config, _root) = isolated_config(&temp_parent::scratch_parent(), "job-record");
    let records = supervisor_config
        .workspace_root
        .join(".cowshed/job/records.arrow");
    let mut h = real_store_harness(supervisor_config);
    let job_id = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"built\n", b"", ExitStatus::Exited { code: 0 }).await;
    let info = h.handle.wait(job_id).await.unwrap();

    let terminal = cowshed_core::storage::job_artifact::recover_records(&census(), &records)
        .unwrap()
        .frames
        .into_iter()
        .map(|frame| frame.record)
        .find(|record| record.job_id == job_id && record.state == JobState::Exited)
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

    fn nice(&self) -> Option<i32> {
        None
    }

    #[cfg(target_os = "macos")]
    fn observations(&self) -> Option<&cowshed_core::runtime::process_monitor::ProcessMonitor> {
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
        census(),
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
/// It reads its stdin, a pipe the returned child holds, so it runs until [`end_group`] or the
/// child's drop. Resolve `cat` through the toolchain PATH; NixOS has no `/bin/cat`.
fn lone_group() -> std::process::Child {
    use std::os::unix::process::CommandExt as _;
    std::process::Command::new("cat")
        .stdin(std::process::Stdio::piped())
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
    cowshed_os::test_backstop::waitid(libc::P_PID, child.id(), libc::WEXITED | libc::WNOWAIT)
        .expect("waitid");
}

/// Reap a leader once nothing reads it any more: its job concluded, or its interval closed.
fn reap(mut leader: std::process::Child) {
    leader.wait_bounded().unwrap();
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
#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_job_is_sampled_from_its_first_owned_process_until_its_sealed_terminal() {
    let (supervisor_config, _root) = isolated_config(&temp_parent::scratch_parent(), "resources");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
            nice: Some(10),
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
    assert!(
        matches!(
            terminal.accounting,
            cowshed_core::api::JobAccounting::MacOsRusageChildren { io: None, .. }
        ),
        "macOS states the job's totals from its leaders' rusage: {:?}",
        terminal.accounting
    );
    assert!(terminal.consistent(), "{terminal:?}");
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
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_served_supervisor_s_progress_starts_at_ownership_and_ends_at_the_sealed_sample() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "progress-served");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let (remote, _path) = served(&handle).await;
    let job_id = remote
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
            nice: Some(10),
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
#[cfg(target_os = "macos")]
const HELD: u64 = 128 << 20;

/// A process this test started in the job's group: it holds [`HELD`] bytes of its own, every
/// page written, from the line it writes on stdout until its stdin -- a pipe this test holds --
/// closes.
#[cfg(target_os = "macos")]
struct Holder(std::process::Child);

#[cfg(target_os = "macos")]
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
        assert!(self.0.wait_bounded().unwrap().success());
    }
}

/// A job's resident memory is what its group holds together at a sample, and its peak the
/// largest such sum read: two processes holding memory at once raise it, two holding the same
/// amount one after the other do not, and the sealed sample keeps it.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_job_s_rss_is_its_group_s_simultaneous_sum_and_its_peak_the_largest_read() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "resources-rss");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
            nice: Some(10),
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
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "resources-unspawned");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    // The host never launched the command: typed, whatever the host process owned meanwhile.
    assert_eq!(ended.failure, Some(JobFailure::Unlaunched));
    let sealed = handle.sealed(job_id).await.unwrap();
    assert_eq!(
        (sealed.resources, sealed.failure),
        (None, Some(JobFailure::Unlaunched))
    );
    let unsampled = handle.resources(job_id).await.unwrap_err();
    assert_eq!(unsampled.code, ErrorCode::Conflict, "{}", unsampled.message);
}

/// A watermark as the bare numbers its wire projects.
#[cfg(target_os = "macos")]
fn watermark(stream: &cowshed_core::api::JobStreamWatermark) -> (u64, u64) {
    (stream.bytes.get(), stream.lines.get())
}

/// A stream counts every chunk the supervisor admits, whatever line the chunk splits: `a`,
/// `\nb`, `\n` and `c` are five bytes in three lines, the last one unterminated. The count is
/// the stream's read cursor, live and terminal, and the sealed record keeps it.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_stream_counts_the_bytes_and_lines_of_every_admitted_chunk() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "resources-lines");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let command = lone_group();
    job.events
        .send(ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
            nice: Some(10),
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
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_streams_counts_survive_its_promotion_to_a_protected_file() {
    const NEWLINES: usize = 2 * 1024 * 1024;
    let (mut supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "resources-promotion");
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let command = lone_group();
    job.events
        .send(ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
            nice: Some(10),
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
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
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

/// What `wait4` reports as the test reaps a scripted leader: its own CPU and every child it
/// reaped, with theirs. That is the kernel's own total for the whole tree, read after every
/// sample it bounds; the job source reads the same counters while the tree lives, so no sample
/// may exceed it, whatever an interpreter's start or `sh`'s own work cost on this host.
#[cfg(target_os = "macos")]
fn reaped_cpu_us(leader: std::process::Child) -> u64 {
    let pid = libc::pid_t::try_from(leader.id()).unwrap();
    // Reaping the test's own exited child, which nothing else waits for.
    let (reaped, _, usage) = cowshed_os::test_backstop::wait4(pid, 0).expect("wait4");
    assert_eq!(reaped, pid);
    let micros = |time: libc::timeval| {
        u64::try_from(time.tv_sec).unwrap() * 1_000_000 + u64::try_from(time.tv_usec).unwrap()
    };
    micros(usage.ru_utime) + micros(usage.ru_stime)
}

/// The units between the two readings, not the host: `wait4` truncates its user and system
/// times to whole microseconds and the job source converts its own and its reaped children's
/// user and system nanoseconds field by field, so the two may differ by under a microsecond for
/// each of those four fields.
#[cfg(target_os = "macos")]
const WAIT4_TRUNCATION_US: u64 = 4;

/// A command line that burns [`BURN_SECONDS`] of its own CPU, then reports it.
#[cfg(target_os = "macos")]
fn burner() -> String {
    format!(
        "python3 -c 'import os, resource\n\
         def used():\n    u = resource.getrusage(resource.RUSAGE_SELF)\n    return u.ru_utime + u.ru_stime\n\
         x = 0\n\
         while used() < {BURN_SECONDS}:\n    for _ in range(10000):\n        x += 1\n\
         os.write(1, (\"burned %d\\n\" % int(used() * 1e6)).encode(\"ascii\"))\n'"
    )
}

/// `script` run by `/bin/sh` leading a group of its own: the test is its parent, holds it
/// unreaped, feeds its stdin and reads its stdout. A test that panics first ends the group, so
/// no process of it outlives the test.
#[cfg(target_os = "macos")]
struct Scripted {
    child: std::process::Child,
    input: std::process::ChildStdin,
    lines: std::io::Lines<std::io::BufReader<std::process::ChildStdout>>,
    _group: EndedOnPanic,
}

/// The process group a panicking test kills as it unwinds.
#[cfg(target_os = "macos")]
struct EndedOnPanic(libc::pid_t);

#[cfg(target_os = "macos")]
impl Drop for EndedOnPanic {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        // SAFETY: the group is the test's own scripted child's, which it holds unreaped.
        if unsafe { libc::killpg(self.0, libc::SIGKILL) } != 0 {
            cowshed_os::eprintln!(
                "the panicking test could not end its scripted group {}: {}",
                self.0,
                std::io::Error::last_os_error()
            );
        }
    }
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
        let group = EndedOnPanic(libc::pid_t::try_from(child.id()).unwrap());
        Self {
            child,
            input,
            lines,
            _group: group,
        }
    }

    /// The next line, which must be `expected`.
    fn expect(&mut self, expected: &str) {
        assert_eq!(self.lines.next().unwrap().unwrap(), expected);
    }

    /// The CPU microseconds the next burner to report measured itself spending.
    fn burned(&mut self) -> u64 {
        self.reported("burned")
    }

    /// The CPU microseconds of the next line, which must report them as `what <us>`.
    fn reported(&mut self, what: &str) -> u64 {
        let line = self.lines.next().unwrap().unwrap();
        line.strip_prefix(what)
            .and_then(|rest| rest.strip_prefix(' '))
            .unwrap_or_else(|| panic!("a {what} report, not {line:?}"))
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
        cowshed_core::api::JobAccounting::MacOsRusageChildren { cpu, io: None, .. } => {
            cpu.user_us.get() + cpu.sys_us.get()
        }
        ref other => panic!("the leader/children rusage source with no byte totals: {other:?}"),
    }
}

/// The CPU share `read` may state over the window the supervisor boundary `opened` opened: the
/// CPU the job spent between the two samples over the wall time between them, as 07_api.md
/// defines `cpuPct`. Each sample truncates its own wall time to a whole microsecond, so the
/// window the supervisor measured is their difference or one microsecond shorter.
#[cfg(target_os = "macos")]
fn window_shares(
    opened: &cowshed_core::api::JobResourceSample,
    read: &cowshed_core::api::JobResourceSample,
) -> [f64; 2] {
    let micros = |value: u64| f64::from(u32::try_from(value).expect("a test's microseconds"));
    let spent = micros(accounted_us(read) - accounted_us(opened));
    let wall = read.wall_us.get() - opened.wall_us.get();
    [wall, wall - 1].map(|wall| spent * 100.0 / micros(wall))
}

/// The job's latest sample, without taking one: a supervisor boundary's, unless a read since
/// replaced it.
#[cfg(target_os = "macos")]
async fn latest(
    handle: &WorkspaceSupervisorHandle,
    job_id: JobId,
) -> cowshed_core::api::JobResourceSample {
    handle
        .info(job_id)
        .await
        .unwrap()
        .resources
        .expect("a job that owns a process has a sample")
}

/// Two children burn CPU at once and are reaped before any sample: the group the sample reads is
/// the leader alone, whose own CPU holds none of theirs. The job's totals hold both, once, live
/// and in the sealed sample.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_job_s_cpu_keeps_the_descendants_that_ended_between_samples() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "accounting-reaped");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
            nice: Some(10),
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
        accounted >= burned,
        "the job's {accounted} us hold the {burned} us its reaped children burned"
    );
    let own = cowshed_core::runtime::job_accounting::read_leader(&Birth::of(pid))
        .unwrap()
        .own;
    // Had the leader been charged its reaped children's CPU, its own would hold all `burned`:
    // each burner stops at a fixed CPU it measured itself, while `sh`'s own cost is the host's.
    let live = own.user_us.get() + own.sys_us.get();
    assert!(
        live < burned,
        "the live members' own CPU ({live} us) misses the {burned} us the reaped children cost"
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
        sealed >= accounted,
        "the terminal {sealed} us keep the running {accounted} us"
    );
    assert_eq!(
        handle.sealed(job_id).await.unwrap().resources,
        Some(terminal),
        "the sealed sample keeps the totals"
    );
    let total = reaped_cpu_us(leader);
    assert!(
        sealed <= total + WAIT4_TRUNCATION_US,
        "the terminal {sealed} us add no child again: the kernel reaped the tree at {total} us"
    );
}

/// A cold host's activation counts up to its end and the command from its start, each once:
/// what the host burns after its activation ended -- serving the command, then other jobs -- is
/// not the job's.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn an_activation_and_its_command_each_count_once() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "accounting-activation");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    let ended = cowshed_core::runtime::job_accounting::read_leader(&Birth::of(host.child.id()));
    let ended_us = {
        let usage = ended
            .as_ref()
            .expect("the host's usage as its activation ended");
        usage.own.user_us.get()
            + usage.own.sys_us.get()
            + usage.children.user_us.get()
            + usage.children.sys_us.get()
    };
    job.events
        .send(ProcessEvent::ActivationEnded {
            job_id,
            usage: ended,
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
            nice: Some(10),
        },
        1,
    )
    .await;
    let ran = command.burned();
    command.expect("ran");
    reap(host.exit());

    let both = activation + ran;
    let running = accounted_us(&handle.resources(job_id).await.unwrap());
    assert!(
        running >= both,
        "the job's {running} us hold its activation's {activation} us and its command's {ran} us"
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
        sealed >= running,
        "the terminal {sealed} us keep the running {running} us"
    );
    let command_us = reaped_cpu_us(command);
    assert!(
        sealed <= ended_us + command_us + WAIT4_TRUNCATION_US,
        "the job's {sealed} us are its activation's {ended_us} us as it ended and its command's \
         {command_us} us, once each, without the host's later {served} us"
    );
}

/// A command line that reports `spinning <pid>` as it starts and holds until a line arrives on
/// its stdin. Woken, it reports `woke <us>`, the CPU it had spent before its burn, then spins for
/// `seconds` more of its own CPU in the very process it runs in, reports `burned <us>` once done,
/// and waits for another line.
#[cfg(target_os = "macos")]
fn spinner(seconds: f64) -> String {
    format!(
        "python3 -c 'import os, resource, sys\n\
         def used():\n    u = resource.getrusage(resource.RUSAGE_SELF)\n    return u.ru_utime + u.ru_stime\n\
         print(\"spinning\", os.getpid(), flush=True)\n\
         sys.stdin.readline()\n\
         woke = used()\n\
         print(\"woke\", int(woke * 1e6), flush=True)\n\
         x = 0\n\
         while used() < woke + {seconds}:\n    for _ in range(10000):\n        x += 1\n\
         print(\"burned\", int(used() * 1e6), flush=True)\n\
         sys.stdin.readline()\n'"
    )
}

/// A command line that spins in the very process it runs in, reporting `spinning <pid>` as it
/// starts. Once it has spent `pause` seconds of its own CPU it reports `paused <us>` and holds,
/// live, until a line arrives on its stdin; then it spins on to `seconds` of its own CPU,
/// reports `burned <us>` and exits.
#[cfg(target_os = "macos")]
fn pausing_spinner(pause: f64, seconds: f64) -> String {
    format!(
        "python3 -c 'import os, resource, sys\n\
         def used():\n    u = resource.getrusage(resource.RUSAGE_SELF)\n    return u.ru_utime + u.ru_stime\n\
         def spin(until):\n    x = 0\n    while used() < until:\n        for _ in range(10000):\n            x += 1\n\
         print(\"spinning\", os.getpid(), flush=True)\n\
         spin({pause})\n\
         print(\"paused\", int(used() * 1e6), flush=True)\n\
         sys.stdin.readline()\n\
         spin({seconds})\n\
         print(\"burned\", int(used() * 1e6), flush=True)\n'"
    )
}

/// The pid a spinner reported as it started.
#[cfg(target_os = "macos")]
fn spinning(leader: &mut Scripted) -> u32 {
    let line = leader.lines.next().unwrap().unwrap();
    line.strip_prefix("spinning ")
        .unwrap_or_else(|| panic!("a spinner's start, not {line:?}"))
        .parse()
        .unwrap()
}

/// A job whose leader burns CPU reads that CPU's share over the supervisor's window -- opened by
/// its own boundary, not by a read -- and names that leader its leaf from the processes it
/// observed. Once the leader idles, one tick of the sampling clock opens a window after the burn,
/// while the accounted CPU keeps what it burned.
///
/// The share is compared with the job's own CPU delta and wall time between the boundary and the
/// read, never with a presumed share of a loaded host (08_testing.md, "Job CPU sampling"): the
/// paused Tokio clock moves the supervisor's boundaries only when the test advances it, while
/// rusage and monotonic wall time stay real. The test reads the spinner's lines blocking the
/// runtime the supervisor's actor runs on, so no boundary falls between its own steps.
#[cfg(target_os = "macos")]
#[tokio::test(start_paused = true)]
async fn a_busy_job_reads_its_measured_cpu_share_and_its_busy_leader_as_leaf() {
    let period = Duration::from_millis(400);
    let (mut supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "cpu-busy");
    supervisor_config.job_sampling.cpu_sample_period = period;
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let mut leader = Scripted::start(&format!("exec {}", spinner(BURN_SECONDS)));
    let pid = spinning(&mut leader);
    assert_eq!(pid, leader.child.id(), "the spinner is the leader itself");
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&leader.child, Instant::now()),
            nice: Some(10),
        },
        0,
    )
    .await;
    // No read has been taken: the latest sample is the boundary that opened the window. The
    // leader was blocked on its stdin from before that boundary until the line below wakes it,
    // so the CPU it reports on waking bounds what the boundary can have accounted, whatever an
    // interpreter's start cost on this host.
    let opened = latest(&handle, job_id).await;
    leader.send("burn");
    let woke = leader.reported("woke");
    assert!(
        accounted_us(&opened) <= woke,
        "the window opened at the leader's start, before its burn from {woke} us: {opened:?}"
    );
    let burned = leader.burned();
    let busy = handle.resources(job_id).await.unwrap();
    assert!(busy.consistent(), "{busy:?}");
    let measured_share = |sample: &cowshed_core::api::JobResourceSample| {
        let spent = u32::try_from(accounted_us(sample) - accounted_us(&opened))
            .expect("fixture CPU fits u32 microseconds");
        let elapsed = u32::try_from(sample.wall_us.get() - opened.wall_us.get())
            .expect("fixture interval fits u32 microseconds");
        assert!(spent > 0 && elapsed > 0, "a real observed CPU interval");
        let expected = 100.0 * f64::from(spent) / f64::from(elapsed);
        assert!(
            (sample.cpu_pct.get() - expected).abs() < 0.001,
            "reported {} %, measured {spent} us CPU / {elapsed} us wall = {expected} %",
            sample.cpu_pct.get()
        );
    };
    measured_share(&busy);
    assert!(
        window_shares(&opened, &busy).contains(&busy.cpu_pct.get()),
        "the share is the window's CPU over its wall time: {:?} of {opened:?} and {busy:?}",
        window_shares(&opened, &busy)
    );
    let busy_us = accounted_us(&busy);
    assert!(
        busy_us >= burned,
        "the job's {busy_us} us hold the {burned} us its leader burned"
    );
    let read_again = handle.resources(job_id).await.unwrap();
    measured_share(&read_again);
    let JobLeafAttribution::Attributed {
        leaf,
        observed_cpu_permille,
    } = &busy.leaf
    else {
        panic!("the leader's own CPU is the job's: {:?}", busy.leaf);
    };
    assert_eq!(leaf.pid, pid);
    assert!(leaf.program.contains("python"), "{}", leaf.program);
    assert!(
        *observed_cpu_permille >= 900,
        "the observed leader covers the job's CPU: {observed_cpu_permille}‰"
    );
    assert_eq!(
        busy.accounting.reconciliation().coverage,
        ProcessCoverage::Gap {
            reason: ProcessCoverageGap::UncountedFork { pid }
        },
        "a polled census never claims the whole tree"
    );

    let again = handle.resources(job_id).await.unwrap();
    assert!(
        window_shares(&opened, &again).contains(&again.cpu_pct.get()),
        "a second read measures from the same window: {:?} of {opened:?} and {again:?}",
        window_shares(&opened, &again)
    );

    // Idle now, waiting for its line: one tick of the clock closes the burn into its window. The
    // paused clock moves only once both tasks wait on it, to each deadline in turn: the actor
    // takes the tick alone, a millisecond (the timer's resolution) before the test wakes.
    tokio::time::sleep(period + Duration::from_millis(1)).await;
    let ticked = latest(&handle, job_id).await;
    assert!(
        ticked.wall_us > again.wall_us,
        "the clock's tick took a boundary after the reads: {ticked:?}"
    );
    assert!(
        accounted_us(&ticked) >= burned,
        "the burn lies before the window the tick opened: {ticked:?}"
    );
    let idle = handle.resources(job_id).await.unwrap();
    assert!(
        window_shares(&ticked, &idle).contains(&idle.cpu_pct.get()),
        "the idle share is the new window's: {:?} of {ticked:?} and {idle:?}",
        window_shares(&ticked, &idle)
    );
    let idle_us = accounted_us(&idle);
    assert!(
        idle_us >= burned,
        "the job's {idle_us} us hold the {burned} us its leader burned"
    );

    let leader = leader.exit();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let terminal = handle
        .wait(job_id)
        .await
        .unwrap()
        .resources
        .expect("a terminal sample");
    assert!(terminal.consistent(), "{terminal:?}");
    assert_eq!(
        handle.sealed(job_id).await.unwrap().resources,
        Some(terminal),
        "the sealed sample keeps the measured share and leaf"
    );
    let total = reaped_cpu_us(leader);
    assert!(
        busy_us.max(idle_us) <= total + WAIT4_TRUNCATION_US,
        "the job's {busy_us} and {idle_us} us are its leader's alone: the kernel reaped it at \
         {total} us"
    );
}

/// When a forked child does the work, the leaf is that child: the observed process with the most
/// own CPU, although the macOS source holds none of a live child's CPU yet. The reconciliation
/// states that difference, signed. Once the child is reaped the source holds all of it while the
/// census holds only what it read before the child left, so no leaf is named.
///
/// The test waits on the child's own CPU, never on the clock: a loaded host gives a spinning
/// child any share of a core in a given wall time. The child holds, live and unreaped, once it
/// has spent a second of its own CPU. The test reads its lines blocking the runtime the
/// supervisor's actor runs on, so no sampling boundary reads the child between the test's own
/// reads: the census holds the child at its pause, half of what it spends.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_busy_child_is_the_leaf_until_the_census_no_longer_covers_the_job() {
    let (mut supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "cpu-child");
    supervisor_config.job_sampling.cpu_sample_period = Duration::from_millis(400);
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    // `sh` gives a background command `/dev/null` for its stdin unless told otherwise: the
    // spinner reads the leader's own, through which the test releases it.
    let mut leader = Scripted::start(&format!(
        "exec 3<&0\n{} <&3 &\nwait\necho reaped\nread line\n",
        pausing_spinner(1.0, 2.0)
    ));
    let pid = leader.child.id();
    let child = spinning(&mut leader);
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&leader.child, Instant::now()),
            nice: Some(10),
        },
        0,
    )
    .await;
    let paused = leader.reported("paused");
    let busy = handle.resources(job_id).await.unwrap();
    assert!(busy.consistent(), "{busy:?}");
    assert!(busy.members.contains(&child), "{:?}", busy.members);
    let JobLeafAttribution::Attributed { leaf, .. } = &busy.leaf else {
        panic!("the busy child is observed: {:?}", busy.leaf);
    };
    assert_eq!(
        leaf.pid, child,
        "the leaf is the child, not its waiting leader {pid}"
    );
    let reconciliation = busy.accounting.reconciliation();
    assert!(
        reconciliation.cpu_us.get() < -300_000,
        "the source lacks the live child's {paused} us of CPU, stated signed: \
         {reconciliation:?}"
    );
    assert_eq!(
        reconciliation.coverage,
        ProcessCoverage::Gap {
            reason: ProcessCoverageGap::UnobservedBirth { pid: child }
        }
    );

    leader.send("spin on");
    let burned = leader.burned();
    leader.expect("reaped");
    let reaped = handle.resources(job_id).await.unwrap();
    let accounted = accounted_us(&reaped);
    assert!(
        accounted >= burned,
        "reaped, the child's {burned} us are in the source's {accounted} us"
    );
    assert!(
        matches!(
            reaped.leaf,
            JobLeafAttribution::Unattributed {
                required_permille: 900,
                ..
            }
        ),
        "the census read the child only part-way, so it covers too little: {:?}",
        reaped.leaf
    );

    let leader = leader.exit();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    handle.wait(job_id).await.unwrap();
    let total = reaped_cpu_us(leader);
    assert!(
        accounted <= total + WAIT4_TRUNCATION_US,
        "the source's {accounted} us hold the child once: the kernel reaped the tree at {total} us"
    );
}

/// Linux has no job accounting source until a job runs in its own cgroup: a sample of an owned
/// job is that typed operational failure, never a sample with invented totals, and the job's
/// end keeps no sample.
#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn a_linux_job_s_sample_names_its_missing_accounting_source() {
    let (supervisor_config, _root) =
        isolated_config(&temp_parent::scratch_parent(), "linux-unaccounted");
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let command = lone_group();
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
            nice: None,
        },
        0,
    )
    .await;
    let unaccounted = handle.resources(job_id).await.unwrap_err();
    assert_eq!(
        unaccounted.code,
        ErrorCode::EnvironmentMissing,
        "{}",
        unaccounted.message
    );
    assert_eq!(handle.info(job_id).await.unwrap().resources, None);
    end_group(&command);
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    assert_eq!(handle.wait(job_id).await.unwrap().resources, None);
    reap(command);
}

/// Workspace `name`'s supervisor config, in an Nx project no daemon serves. No shell activates
/// it, so its jobs' PATH is the bootstrap PATH, and the `node` there is the workspace's own shim
/// ([`fixture_node`]), as a shed's keeper finds it (04_sandbox.md).
fn nx_project(name: &str) -> (WorkspaceSupervisorConfig, TempRoot) {
    let (supervisor_config, root) =
        isolated_config(&temp_parent::scratch_parent(), &format!("nx-daemon-{name}"));
    nx_project_with_config(name, supervisor_config, root)
}

fn nx_project_with_config(
    name: &str,
    mut supervisor_config: WorkspaceSupervisorConfig,
    root: TempRoot,
) -> (WorkspaceSupervisorConfig, TempRoot) {
    supervisor_config.authority.workspace = WorkspaceName::new(name).unwrap();
    supervisor_config.default_cwd = None;
    // These tests sleep through the keeper's idle bound on a paused clock, which steps through
    // every sampling tick on the way: at the default 1 s period that is hundreds of real
    // process-table walks, whose cost grows with the host's process count (one ran past a land
    // gate's 180 s progress bound). Sampling at the probe cadence keeps the steps few.
    supervisor_config.job_sampling.cpu_sample_period = NX_PROBES;
    std::fs::write(supervisor_config.workspace_root.join("nx.json"), "{}").unwrap();
    let node = fixture_node(&supervisor_config.workspace_root);
    std::fs::create_dir_all(node.parent().unwrap()).unwrap();
    std::fs::write(&node, b"#!/bin/sh\n").unwrap();
    let executable = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
    std::fs::set_permissions(&node, executable).unwrap();
    supervisor_config
        .sandbox
        .configure_capabilities()
        .expect("detect capabilities");
    (supervisor_config, root)
}

/// The `node` on the bootstrap PATH of the workspace at `project`: its `.cowshed/bin` shim.
fn fixture_node(project: &Path) -> PathBuf {
    project.join(".cowshed/bin/node")
}

/// The argv that starts the daemon of the Nx project at `project`: the project's own `nx`, run
/// by the `node` its keeper found on the job's PATH, kept, so the daemon leaves its restart to
/// the keeper.
fn nx_daemon_start(project: &Path) -> Vec<OsString> {
    vec![
        fixture_node(project).into_os_string(),
        project
            .join("node_modules/nx/dist/src/daemon/server/start.js")
            .into_os_string(),
        OsString::from("--kept"),
    ]
}

/// A held ledger publication must not park the actor, but retirement must wait for that cut.
#[tokio::test(start_paused = true)]
async fn a_stalled_group_ledger_write_leaves_the_supervisor_actor_responsive() {
    use std::future::Future as _;
    use std::io::Read as _;

    let root = workspace_root(&temp_parent::scratch_parent(), "group-ledger-write");
    let ledger = root.join("supervisor.groups");
    let temporary = ledger.with_extension(format!("groups.{}", std::process::id()));
    let name = std::ffi::CString::new(temporary.as_os_str().as_bytes()).unwrap();
    // SAFETY: the NUL-terminated path is private to this test and outlives the call.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let mut supervisor_config = config(&root);
    supervisor_config.group_ledger = Some(ledger.clone());
    let expected_authority = supervisor_config.authority.clone();
    let h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let retirement = h.handle.retire(NxDaemonRetirement::Recorded);
    tokio::pin!(retirement);
    assert!(
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(retirement.as_mut().poll(context).is_pending())
        })
        .await
    );

    // No reader exists, so the real recorder cannot open its temporary FIFO. Advance the
    // injected clock through four heartbeat turns and beyond the unchanged 15 s bound.
    for advance in [
        Duration::ZERO,
        Duration::from_secs(5),
        Duration::from_secs(10),
        Duration::from_secs(5),
    ] {
        tokio::time::advance(advance).await;
        assert_eq!(
            h.handle.current_authority().await.unwrap(),
            expected_authority
        );
        assert!(
            std::future::poll_fn(|context| {
                std::task::Poll::Ready(retirement.as_mut().poll(context).is_pending())
            })
            .await,
            "retirement cannot acknowledge an unpublished final cut"
        );
    }

    // Opening the reader releases exactly that filesystem fault, not an actor-side mock.
    let reading = tokio::task::spawn_blocking(move || {
        let mut bytes = Vec::new();
        std::fs::File::open(temporary)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        bytes
    });
    retirement.await.unwrap();
    let publication: serde_json::Value = serde_json::from_slice(&reading.await.unwrap()).unwrap();
    assert!(publication["groups"].as_array().unwrap().is_empty());
    assert!(
        ledger.exists(),
        "the final publication precedes retirement success"
    );
}

/// A held record read must not park the actor that answers the heartbeat round trip.
#[tokio::test(start_paused = true)]
async fn a_stalled_nx_record_read_leaves_the_supervisor_actor_responsive() {
    use std::io::Write as _;

    let parent = std::fs::canonicalize(temp_parent::scratch_parent()).unwrap();
    let root = TempRoot::new_in(&parent, "nx-record-read");
    std::fs::create_dir(root.join("workspace")).unwrap();
    let mut supervisor_config = config(&root);
    supervisor_config.sandbox.home = root.join("home");
    supervisor_config.sandbox.exec_temp_dir = root.join("tmp");
    std::fs::create_dir_all(&supervisor_config.sandbox.store_root).unwrap();
    let (supervisor_config, _root) = nx_project_with_config("record-read", supervisor_config, root);
    let record = supervisor_config
        .workspace_root
        .join(".nx/workspace-data/d/server-process.json");
    let mut daemon = LiveDaemon::start(&supervisor_config.workspace_root);
    let record_bytes = std::fs::read(&record).unwrap();
    let expected_authority = supervisor_config.authority.clone();
    let h = harness_with_config(supervisor_config, 1, 1024, false, false);
    h.handle.nx_daemon_keeper().await.unwrap();
    std::fs::remove_file(&record).unwrap();
    let name = std::ffi::CString::new(record.as_os_str().as_bytes()).unwrap();
    // SAFETY: the NUL-terminated path belongs to this test and outlives the call.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let writer_path = record.clone();
    // Opening the writer proves the real off-actor reader opened this FIFO. Holding it
    // without bytes keeps that same read pending, with no sleep or elapsed-time oracle.
    let opening = tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .write(true)
            .open(writer_path)
            .unwrap()
    });
    tokio::time::advance(cowshed_core::runtime::supervisor::NxDaemonIdle::DEFAULT.get()).await;
    let mut writer = opening.await.unwrap();
    assert_eq!(
        h.handle.current_authority().await.unwrap(),
        expected_authority
    );
    assert!(
        h.spawned.is_empty(),
        "no result was invented for the held read"
    );
    // The census is acknowledged while the read is held. Its completion must leave the
    // already-live, now-idle daemon alone until new use explicitly resumes maintenance.
    h.handle.stop_nx_keeper().await.unwrap();
    writer.write_all(&record_bytes).unwrap();
    std::fs::remove_file(&record).unwrap();
    std::fs::write(&record, &record_bytes).unwrap();
    drop(writer);
    let state = h.handle.nx_daemon_keeper().await.unwrap().unwrap();
    assert!(matches!(
        state.held,
        Some(cowshed_core::runtime::NxKeeperHold::Stopped)
    ));
    assert!(
        daemon.running(),
        "a completed live probe must not stop a census's daemon"
    );
    h.handle
        .retire(NxDaemonRetirement::ControllerOwned)
        .await
        .unwrap();
}

/// Many of a keeper's probes, on a paused clock that skips the wait.
const NX_PROBES: Duration = Duration::from_secs(60);

/// A shed's code is unsigned and Nx's daemon runs it, so the daemon a host client connects to is
/// one the shed's supervisor keeps alive inside the sandbox: a read-write resident job of the
/// project's own `nx`, started once the shed is in use, never a second while one runs, and again
/// once the daemon it started is gone.
#[tokio::test(start_paused = true)]
async fn a_shed_keeps_its_nx_daemon_inside_the_sandbox() {
    let (supervisor_config, _root) = nx_project("raven");
    let project = supervisor_config.workspace_root.clone();
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let _in_use = open_named(&h.handle, "build").await;

    let first = h.spawned.recv().await.unwrap();
    let SpawnCommand::Argv(argv) = &first.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(argv, &nx_daemon_start(&project));
    assert_eq!(first.request.mode, RunSandboxMode::ReadWrite);
    let started = h.handle.info(first.request.job_id).await.unwrap();
    assert_eq!(started.class, ExecClass::Resident);
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
    // Short, under the scratch parent: a Unix socket path is bounded at 104 bytes on macOS.
    let sockets = TempRoot::new_in(&temp_parent::scratch_parent(), "cs-nxk");
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
}

/// A foreground keeper is an ordinary owned job for draining, not developer activity. Even before
/// its socket is ready, quiescing an otherwise idle workspace stops that exact job and waits for it.
/// A named session puts the shed in use so the keeper starts, and is closed before the quiesce, so
/// the keeper's own job is all the ledger holds.
#[tokio::test(start_paused = true)]
async fn a_foreground_nx_keeper_does_not_count_as_user_work_and_quiescence_drains_it() {
    let (config, _root) = nx_project("foreground");
    let mut h = harness_with_config(config, 1, 1024, false, false);
    let in_use = open_named(&h.handle, "build").await;
    let keeper = h.spawned.recv().await.expect("the foreground keeper job");
    h.handle.close_session(in_use).await.unwrap();
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::StdinClosed(keeper.request.job_id),
        "empty stdin closes before lifecycle signalling"
    );
    let handle = h.handle.clone();
    let quiesce =
        tokio::spawn(async move { handle.retire_if_idle(NxDaemonRetirement::Recorded).await });
    assert_eq!(
        h.process.recv().await.unwrap(),
        ProcessObservation::Signal(keeper.request.job_id, ProcessSignal::Term)
    );
    assert!(
        !quiesce.is_finished(),
        "quiescence waits for the real keeper conclusion"
    );
    complete(&keeper, b"", b"", ExitStatus::Exited { code: 0 }).await;
    quiesce
        .await
        .unwrap()
        .expect("maintenance is not user activity and must not refuse idle quiescence");
    assert!(
        h.spawned.try_recv().is_err(),
        "quiescence admits no replacement keeper"
    );
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// A named session held open without a job, and the foreground maintenance job, are not continuous
/// developer use: reopening the same handle, a keyed repeat of an admitted exec and reading actor
/// state do not revive an expired interval; only a new named session's opening does.
#[tokio::test(start_paused = true)]
async fn idle_named_handles_and_maintenance_do_not_keep_the_nx_daemon_resident() {
    let (config, _root) = nx_project("idle-handles");
    let mut h = real_store_harness(config);
    let session = open_named(&h.handle, "build").await;
    let maintenance = h
        .spawned
        .recv()
        .await
        .expect("session opening starts maintenance");
    let job = h
        .handle
        .exec(
            Some(&session),
            None,
            keyed("idle-handles-command", StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let command = h.spawned.recv().await.expect("a real session command");
    assert_eq!(command.request.job_id, job);
    complete(&command, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    pass_idle(&h, NX_IDLE + NX_PROBES).await;
    let _ = h.handle.list().await.unwrap();
    let _ = h.handle.nx_daemon_keeper().await.unwrap();
    let same = open_named(&h.handle, "build").await;
    assert_eq!(session, same, "an existing session is not a new opening");
    assert_eq!(
        h.handle
            .exec(
                Some(&session),
                None,
                keyed("idle-handles-command", StdinSource::Empty),
                AdmissionSource::Worker,
            )
            .await
            .unwrap(),
        job,
        "a keyed replay is not a real admission"
    );
    complete(&maintenance, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(maintenance.request.job_id).await.unwrap();
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "idle handles, keyed replays, reads and maintenance admission/end admit no successor"
    );
    h.handle.close_session(session).await.unwrap();
    let _new = open_named(&h.handle, "build").await;
    let restart = h
        .spawned
        .recv()
        .await
        .expect("a new session opening is one use");
    // Retirement stops the keeper's start as it stops every admitted job, and the start ends only
    // then. Ended before, it would be a start that left no daemon live, which the keeper follows
    // with another at its next probe (04_sandbox.md, the kept daemon). That probe finishes on the
    // blocking pool, in real time, so whether it is judged before the retirement is a race, and a
    // successor nothing ends would hold the retirement forever.
    let handle = h.handle.clone();
    let retiring = tokio::spawn(async move { handle.retire(NxDaemonRetirement::Recorded).await });
    let stopped = loop {
        if let ProcessObservation::Signal(job, signal) = h.process.recv().await.unwrap() {
            break (job, signal);
        }
    };
    assert_eq!(stopped, (restart.request.job_id, ProcessSignal::Term));
    complete(
        &restart,
        b"",
        b"",
        ExitStatus::Signaled {
            signal: 15,
            core_dumped: false,
        },
    )
    .await;
    retiring.await.unwrap().unwrap();
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
        // Short, under the scratch parent: a Unix socket path is bounded at 104 bytes on macOS.
        let sockets = temp_parent::scratch_parent().join(format!(
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
        let process = std::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
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
        let _ = self.process.wait_bounded();
        // The serving thread is blocked in `accept`: one connection wakes it to see the flag.
        self.closing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::os::unix::net::UnixStream::connect_bounded(&self.socket);
        if let Some(serving) = self.serving.take() {
            let _ = serving.join();
        }
        let _ = std::fs::remove_dir_all(&self.sockets);
    }
}

/// Land hands record cleanup to the controller without surrendering the supervisor's own jobs.
/// The Recorded arms keep the ordinary retirement behavior as a real, fixture-owned control.
#[tokio::test]
async fn nx_retirement_policy_preserves_foreign_nominees_but_retires_owned_jobs_locally_and_over_socket()
 {
    use std::os::unix::process::ExitStatusExt as _;

    use cowshed_core::runtime::{job_groups, supervisor_socket};

    // A real one-process group, held by its actual parent until the supervisor concludes its job.
    // No sandbox is needed to prove this actor/socket policy boundary, and no output pipe is open.
    struct GroupSpawner {
        reaped: mpsc::UnboundedSender<std::process::ExitStatus>,
    }

    struct HeldGroup {
        child: std::process::Child,
        process: OwnedProcess,
        job: JobId,
        events: mpsc::Sender<ProcessEvent>,
        reaped: mpsc::UnboundedSender<std::process::ExitStatus>,
        reported: bool,
    }

    #[async_trait]
    impl SpawnSink for GroupSpawner {
        async fn spawn(
            &mut self,
            request: ProcessSpawnRequest,
            events: mpsc::Sender<ProcessEvent>,
        ) -> Result<Box<dyn RunningProcess>> {
            let SpawnCommand::Argv(argv) = &request.command else {
                panic!("the fixture admits only its real cat group");
            };
            assert_eq!(argv, &[OsString::from("cat")]);
            let child = lone_group();
            let process = owned(&child, Instant::now());
            assert!(
                process.birth.leader().is_some(),
                "the parent identified its live child"
            );
            Ok(Box::new(HeldGroup {
                child,
                process,
                job: request.job_id,
                events,
                reaped: self.reaped.clone(),
                reported: false,
            }))
        }
    }

    impl RunningProcess for HeldGroup {
        fn process(&self) -> Option<&OwnedProcess> {
            Some(&self.process)
        }

        fn nice(&self) -> Option<i32> {
            None
        }

        #[cfg(target_os = "macos")]
        fn observations(&self) -> Option<&cowshed_core::runtime::process_monitor::ProcessMonitor> {
            None
        }

        fn try_write_stdin(&mut self, _bytes: Bytes) -> Result<bool> {
            panic!("the fixture admits only empty stdin");
        }

        fn close_stdin(&mut self) -> bool {
            // lone_group's stdin is a pipe its child handle holds, so the group runs until it is
            // signalled: an empty stdin's close has no write end of the supervisor's to close.
            true
        }

        fn end_stdin(&mut self) {}

        fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
            if self.reported {
                return Ok(());
            }
            let pid = i32::try_from(self.child.id()).unwrap();
            job_groups::signal_unreaped_group(
                pid,
                match signal {
                    ProcessSignal::Term => libc::SIGTERM,
                    ProcessSignal::Kill => libc::SIGKILL,
                },
            )
            .expect("signal the fixture's held job group");
            let exit = job_groups::await_exit_unreaped(pid).expect("observe the real job exit");
            self.events
                .try_send(ProcessEvent::Exited {
                    job_id: self.job,
                    exit,
                })
                .expect("publish the observed exit");
            for stream in [StreamKind::Stdout, StreamKind::Stderr] {
                self.events
                    .try_send(ProcessEvent::OutputEof {
                        job_id: self.job,
                        stream,
                    })
                    .expect("the fixture has no open output pipe");
            }
            self.reported = true;
            Ok(())
        }
    }

    impl Drop for HeldGroup {
        fn drop(&mut self) {
            if !self.reported {
                job_groups::signal_unreaped_group(
                    i32::try_from(self.child.id()).unwrap(),
                    libc::SIGKILL,
                )
                .expect("clean up the fixture's held group");
            }
            let exit = self
                .child
                .wait_bounded()
                .expect("reap the concluded job's leader");
            if let Err(error) = self.reaped.send(exit) {
                cowshed_os::eprintln!("the job's exit observer is gone: {error}");
            }
        }
    }

    for (name, over_socket) in [("retire-local", false), ("retire-socket", true)] {
        for policy in [
            NxDaemonRetirement::ControllerOwned,
            NxDaemonRetirement::Recorded,
        ] {
            let (mut config, root) = nx_project(name);
            let sockets =
                over_socket.then(|| TempRoot::new_in(&temp_parent::scratch_parent(), "cs-retire"));
            let mut daemon = LiveDaemon::start(&config.workspace_root);
            assert!(daemon.running(), "the foreign nominee starts alive");
            std::os::unix::net::UnixStream::connect_bounded(&daemon.socket)
                .expect("the valid daemon record names a live socket");
            let ledger = root.join("supervisor.groups");
            config.group_ledger = Some(ledger.clone());
            let store = ArtifactStoreSink::open(
                config.workspace_root.clone(),
                &config.owned_repo_ids,
                &config.authority,
                config.artifacts.clone(),
                census(),
            )
            .expect("open the real job artifact store");
            let commitments = cowshed_core::runtime::supervisor::CommitmentPublisher::start(
                Box::new(cowshed_core::storage::audit::NullAuditSink),
                8,
            )
            .unwrap();
            let (reaped, mut exits) = mpsc::unbounded_channel();
            let handle = WorkspaceSupervisor::start_with_sinks(
                config,
                Box::new(GroupSpawner { reaped }),
                Box::new(store),
                Box::new(commitments),
            )
            .unwrap();
            let (caller, serving) = if let Some(sockets) = &sockets {
                let path = sockets.join("s");
                let bound = supervisor_socket::bind(&path).await.unwrap();
                let server =
                    tokio::spawn(supervisor_socket::serve(bound, handle.clone(), None, None));
                (
                    supervisor_socket::connect(path.clone(), handle.snapshot().clone()),
                    Some((path, server)),
                )
            } else {
                (handle.clone(), None)
            };
            let job = caller
                .exec(
                    None,
                    None,
                    ExecRequest {
                        command: ExecCommand::Argv(vec!["cat".into()]),
                        cwd: None,
                        ..request(StdinSource::Empty)
                    },
                    AdmissionSource::Worker,
                )
                .await
                .unwrap();
            let leaders = published_ledger_leaders(&ledger, 1).await;
            assert_eq!(leaders.len(), 1, "only the admitted job enters the ledger");
            assert_eq!(leaders[0].0, job.get());
            assert!(
                leaders[0].1.is_some(),
                "the job has an observed group identity"
            );
            assert_eq!(handle.info(job).await.unwrap().state, JobState::Running);
            assert!(
                daemon.running(),
                "admitting a job leaves the foreign nominee alone"
            );

            caller.retire(policy).await.unwrap();

            assert_eq!(
                exits
                    .try_recv()
                    .expect("retirement reaps its actual owned job")
                    .signal(),
                Some(libc::SIGTERM),
                "{name} {policy:?} retires the real owned group with TERM"
            );
            let ended = handle.wait(job).await.unwrap();
            assert_eq!(ended.state, JobState::Killed);
            assert_eq!(
                ended.exit,
                Some(ExitStatus::Signaled {
                    signal: libc::SIGTERM,
                    core_dumped: false
                })
            );
            assert_eq!(
                handle
                    .exec(
                        None,
                        None,
                        request(StdinSource::Empty),
                        AdmissionSource::Worker,
                    )
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::Conflict,
                "{name} {policy:?} closes job admission"
            );
            match policy {
                NxDaemonRetirement::ControllerOwned => {
                    assert!(
                        daemon.running(),
                        "{name}: controller-owned retirement must not signal a record nominee"
                    );
                    std::os::unix::net::UnixStream::connect_bounded(&daemon.socket)
                        .expect("the surviving nominee's recorded socket still accepts");
                }
                NxDaemonRetirement::Recorded => {
                    assert_eq!(
                        daemon.process.wait_bounded().unwrap().signal(),
                        Some(libc::SIGTERM),
                        "{name}: ordinary record cleanup still stops the fixture-owned nominee"
                    );
                }
            }
            if let Some((path, server)) = serving {
                let replacement = supervisor_socket::bind(&path)
                    .await
                    .expect("retirement acknowledgement releases the real socket lease");
                drop(replacement);
                server.await.unwrap().unwrap();
            }
        }
    }
}

/// A shed with a live daemon its start job left, and the harness over it. A named session puts
/// the shed in use for the start and is closed once the daemon is up, so the shed is unused from
/// then on.
/// Pass `total` of paused test time as the keeper sees it: in steps of at most one probe interval,
/// each advanced explicitly and then answered by the keeper. A sleep auto-advances only while no
/// blocking work is in flight; the keeper probes and stops daemons on the blocking pool, and while
/// that work runs a paused clock parks in real time until its next timer. Under host load a virtual
/// interval of minutes became minutes of wall clock that way, and a land gate's 180 s progress
/// bound expired in it (the stack: the time driver parked in kevent with a timeout).
async fn pass_idle(h: &Harness, total: Duration) {
    let mut left = total;
    while !left.is_zero() {
        let step = left.min(NX_PROBES);
        tokio::time::advance(step).await;
        h.handle.nx_daemon_keeper().await.unwrap();
        left -= step;
    }
}

/// Whether `daemon` still runs once the keeper's probe in flight has answered. The keeper stops a
/// daemon on the blocking pool, in real time, and paused test time auto-advances while the actor
/// awaits it, so a virtual sleep alone can end before the stop does: the probe's own answer is the
/// signal that it finished.
async fn running_after_probe(h: &Harness, daemon: &mut LiveDaemon) -> bool {
    h.handle.nx_daemon_keeper().await.unwrap();
    daemon.running()
}

async fn shed_with_live_nx_daemon(name: &str) -> (Harness, LiveDaemon, PathBuf, TempRoot) {
    shed_with_live_nx_daemon_publishing_after(name, Duration::ZERO).await
}

/// [`shed_with_live_nx_daemon`], whose controller delays each subsequent terminal publication
/// by `terminal_delay`, armed only once the keeper start has concluded.
async fn shed_with_live_nx_daemon_publishing_after(
    name: &str,
    terminal_delay: Duration,
) -> (Harness, LiveDaemon, PathBuf, TempRoot) {
    let (supervisor_config, root) = nx_project(name);
    let project = supervisor_config.workspace_root.clone();
    let (delay, terminal_delays) = tokio::sync::watch::channel(Duration::ZERO);
    let mut h = harness_with_artifacts(
        supervisor_config,
        1,
        1024,
        false,
        false,
        None,
        terminal_delays,
    );
    let in_use = open_named(&h.handle, "build").await;
    let start = h.spawned.recv().await.unwrap();
    let mut daemon = LiveDaemon::start(&project);
    assert!(
        running_after_probe(&h, &mut daemon).await,
        "the fixture daemon is observed live before its start concludes"
    );
    complete(&start, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(start.request.job_id).await.unwrap();
    h.handle.close_session(in_use).await.unwrap();
    assert!(
        running_after_probe(&h, &mut daemon).await,
        "maintenance setup consumes no developer idle interval"
    );
    delay.send_replace(terminal_delay);
    (h, daemon, project, root)
}

/// A supervisor's own start is no use (04_sandbox.md, the kept daemon): one that runs no job and
/// holds no session starts no daemon, so a supervisor started for anything else, such as a failed
/// `rm` restoring its workspace, launches nothing on the mount. The first session is use, and the
/// daemon follows at the next probe.
#[tokio::test(start_paused = true)]
async fn a_shed_supervisor_nothing_uses_starts_no_nx_daemon() {
    let (supervisor_config, _root) = nx_project("finch");
    let project = supervisor_config.workspace_root.clone();
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "a supervisor nothing uses started a job"
    );
    let _in_use = open_named(&h.handle, "build").await;
    let start = tokio::time::timeout(NX_PROBES, h.spawned.recv())
        .await
        .expect("a shed in use wants its daemon")
        .unwrap();
    let SpawnCommand::Argv(argv) = &start.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(argv, &nx_daemon_start(&project));
    complete(&start, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(start.request.job_id).await.unwrap();
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
    assert!(
        !running_after_probe(&h, &mut daemon).await,
        "the idle daemon is stopped"
    );
    assert!(
        tokio::time::timeout(NX_PROBES, h.spawned.recv())
            .await
            .is_err(),
        "an idle shed starts no daemon"
    );

    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
/// until the shed has been unused for the interval after the job's end. The job's end itself
/// starts the interval, not the last probe that saw the job run: this one ends 3 s after a probe,
/// a probe being due every 5 s from its admission.
#[tokio::test(start_paused = true)]
async fn a_shed_keeps_its_nx_daemon_while_a_job_runs() {
    let (mut h, mut daemon, _project, _root) = shed_with_live_nx_daemon("lark").await;

    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let ran = h.spawned.recv().await.unwrap();
    tokio::time::sleep(NX_IDLE * 2 + Duration::from_secs(3)).await;
    assert!(daemon.running(), "a running job keeps the daemon");

    complete(&ran, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    tokio::time::sleep(NX_IDLE - Duration::from_secs(1)).await;
    assert!(daemon.running(), "the interval starts when the job ends");
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        !running_after_probe(&h, &mut daemon).await,
        "the daemon stops once the shed is idle"
    );
}

/// A job's end starts the interval once the job has concluded, not where its conclusion began:
/// a controller that takes a whole interval to publish the end leaves the shed its daemon for a
/// whole interval after that, the actor running no probe while it waits.
#[tokio::test(start_paused = true)]
async fn a_jobs_idle_interval_starts_once_its_end_has_concluded() {
    let (mut h, mut daemon, _project, _root) =
        shed_with_live_nx_daemon_publishing_after("tern", NX_IDLE).await;

    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let ran = h.spawned.recv().await.unwrap();
    complete(&ran, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
    tokio::time::sleep(NX_IDLE - Duration::from_secs(10)).await;
    assert!(
        daemon.running(),
        "the interval starts once the job has concluded"
    );
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert!(
        !running_after_probe(&h, &mut daemon).await,
        "the daemon stops once the shed is idle"
    );
}

/// The supervisor's ledger alone says whether a shed is in use (04_sandbox.md, "The Nx daemon
/// stays on"). A host client holding a task database open is no job of the shed's, so the daemon
/// stops an interval after it came up all the same. Throughout, no probe begins a process-table
/// walk, so nothing a probe does can wait on another process's kernel state, and the supervisor
/// answers at once, before and after. A one-shot census, the guard of a destructive operation,
/// still walks the table and finds the client.
#[cfg(target_os = "macos")]
#[tokio::test(start_paused = true)]
async fn a_shed_s_ledger_alone_says_it_is_in_use_and_its_supervisor_answers_throughout() {
    let (mut h, mut daemon, project, _root) = shed_with_live_nx_daemon("pika").await;
    let database = project.join(".nx/workspace-data/task-v3.db");
    std::fs::write(&database, b"").unwrap();
    let client = std::fs::File::open(&database).unwrap();
    let walks = cowshed_core::build_volume::nx::process_table_walks();

    tokio::time::sleep(NX_IDLE - Duration::from_secs(30)).await;
    let asked = Instant::now();
    h.handle.current_authority().await.unwrap();
    let answered = asked.elapsed();
    assert!(daemon.running(), "a daemon is kept for the whole interval");
    tokio::time::sleep(Duration::from_secs(60)).await;
    let walked = cowshed_core::build_volume::nx::process_table_walks() - walks;
    assert!(
        !running_after_probe(&h, &mut daemon).await,
        "a client the ledger does not hold keeps no daemon"
    );
    assert_eq!(
        walked, 0,
        "the supervisor began {walked} process-table walks over the interval"
    );
    let holders = cowshed_core::build_volume::nx::holders(&census(), &database).unwrap();
    assert!(
        cowshed_core::build_volume::nx::process_table_walks() - walks > 0,
        "a one-shot census walks the process table"
    );
    let own = i32::try_from(std::process::id()).unwrap();
    assert!(
        holders.iter().any(|holder| holder.pid == own),
        "a one-shot census still finds the client: {holders:?}"
    );
    drop(client);

    let asked = Instant::now();
    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let admitted = asked.elapsed();
    let ran = h.spawned.recv().await.unwrap();
    assert_eq!(ran.request.job_id, job);
    assert!(
        answered < Duration::from_secs(1) && admitted < Duration::from_secs(1),
        "the supervisor answered in {answered:?} and admitted in {admitted:?}"
    );
    complete(&ran, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(job).await.unwrap();
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
    assert!(!running_after_probe(&h, &mut daemon).await);
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
    use cowshed_core::capabilities::BuildStatePath;
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
        BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
        BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
    ];
    link::link_paths(&checkout, &before, &paths).unwrap();
    supervisor_config.sandbox.build_volume_mount = Some(before.clone());
    supervisor_config.build_volume_layout = Some(layout.clone());
    // The controller's release at each job end, awaited before the job is told ended: this test
    // records what it is told, and refuses the release of the adopted volume, as a release whose
    // unlink fails would.
    let (released_tx, released) = std::sync::mpsc::channel();
    let refused = adopted_id.clone();
    supervisor_config.build_volume_released = Some(
        cowshed_core::runtime::supervisor::BuildVolumeReleased::new(move |id| {
            let refuse = id == refused;
            // A receiver gone at teardown has nothing left to learn.
            let _ = released_tx.send(id.clone());
            async move {
                if refuse {
                    return Err(cowshed_core::CowshedError::internal(format!(
                        "release of build volume {id} failed"
                    )));
                }
                Ok(())
            }
        }),
    );
    let owner = supervisor_config.authority.workspace.clone();
    let incarnation = supervisor_config.authority.workspace_incarnation.clone();
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let _in_use = open_named(&h.handle, "build").await;

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
            &BuildVolumeRecord::new(None, BuildVolumeRole::Superseded { owner, incarnation }),
        )
        .unwrap();
    assert!(
        released.try_recv().is_err(),
        "nothing is told while the keeper job still holds the superseded volume"
    );
    complete(&first, b"", b"", ExitStatus::Exited { code: 0 }).await;
    h.handle.wait(first.request.job_id).await.unwrap();
    assert_eq!(
        released.try_recv(),
        Ok(before_id.clone()),
        "the job's end awaited the controller's release of the volume it held, so a superseded \
         one is released before the job is told ended, not at a later collection"
    );
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
        assert_eq!(first_profile.text(), second_profile.text());
        let binding = |volume: &Path| OsString::from(format!("BUILD_VOLUME={}", volume.display()));
        assert!(
            first_profile
                .launch()
                .unwrap()
                .args()
                .contains(&binding(&before))
        );
        assert!(
            second_profile
                .launch()
                .unwrap()
                .args()
                .contains(&binding(&adopted))
        );
    }
    complete(&second, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let failed =
        h.handle.wait(second.request.job_id).await.expect_err(
            "a release that fails is the job's terminal failure, never a quiet success",
        );
    assert!(failed.message.contains(adopted_id.as_str()), "{failed:?}");
    h.handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
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

/// A shed whose jobs can reach no `node` cannot start its daemon: its keeper says why at its
/// first probe, to anyone who asks it, and admits no start job -- never one per probe that
/// `sandbox-exec` fails with exit 71. It looks again at each probe, so the `node` that appears
/// is started at the next one, by its absolute path.
#[tokio::test(start_paused = true)]
async fn a_shed_with_no_node_refuses_its_nx_daemon_with_its_enoent_named_until_one_appears() {
    use cowshed_core::runtime::NxKeeperHold;

    let (supervisor_config, _root) = nx_project("nodeless");
    let project = supervisor_config.workspace_root.clone();
    let node = fixture_node(&project);
    std::fs::remove_file(&node).unwrap();
    let mut h = harness_with_config(supervisor_config, 1, 1024, false, false);
    let _in_use = open_named(&h.handle, "build").await;
    // The probe that found the supervisor unused at its start wanted no daemon; the next one,
    // finding the session, wants one and refuses its start.
    tokio::time::sleep(Duration::from_secs(10)).await;
    let refused = |keeper: Option<cowshed_core::runtime::NxKeeperState>| {
        let keeper = keeper.expect("a shed keeps a daemon");
        assert_eq!(keeper.starting, None);
        match keeper.held {
            Some(NxKeeperHold::Refused { why }) => why,
            held => panic!("the keeper refused the start, not {held:?}"),
        }
    };
    let why = refused(h.handle.nx_daemon_keeper().await.unwrap());
    assert!(
        why.contains("no executable `node` on its PATH")
            && why.contains(&std::io::Error::from_raw_os_error(libc::ENOENT).to_string()),
        "{why}"
    );
    assert_eq!(
        refused(h.handle.nx_daemon_keeper().await.unwrap()),
        why,
        "the next probe refuses the same way"
    );
    assert!(
        h.spawned.try_recv().is_err(),
        "the refused keeper admitted a start job"
    );

    std::fs::write(&node, b"#!/bin/sh\n").unwrap();
    let executable = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
    std::fs::set_permissions(&node, executable).unwrap();
    let start = h.spawned.recv().await.unwrap();
    let SpawnCommand::Argv(argv) = &start.request.command else {
        panic!("the daemon start is an argv");
    };
    assert_eq!(argv, &nx_daemon_start(&project));
    complete(&start, b"", b"", ExitStatus::Exited { code: 1 }).await;
    h.handle.wait(start.request.job_id).await.unwrap();
}

/// The unattributed-usage cells of a sealed job-span row: null on every row but that one.
#[derive(Debug, Default, Eq, PartialEq)]
struct Unattributed {
    cpu_us: Option<i64>,
    io_read_bytes: Option<i64>,
    io_write_bytes: Option<i64>,
    source: Option<String>,
    coverage: Option<String>,
    gap_pid: Option<u32>,
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
    unattributed: Unattributed,
}

/// Every job-span segment under `telemetry`, in the order its writer sealed them: its name and its
/// one batch of one row.
fn job_span_batches(telemetry: &Path) -> Vec<(String, arrow_array::RecordBatch)> {
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
            (name, batch)
        })
        .collect()
}

/// Every job-span segment under `telemetry`, in the order its writer sealed them, as one row
/// each.
fn job_spans(telemetry: &Path) -> Vec<(String, JobSpanRow)> {
    use arrow_array::Array as _;
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type};

    job_span_batches(telemetry)
        .into_iter()
        .map(|(name, batch)| {
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
            let signed = |name: &str| {
                let column = column(name);
                let column = column.as_primitive::<Int64Type>();
                column.is_valid(0).then(|| column.value(0))
            };
            let enum_text = |name: &str| {
                let column = column(name);
                let dictionary = column.as_dictionary::<UInt16Type>();
                dictionary.is_valid(0).then(|| {
                    let key = usize::from(dictionary.keys().value(0));
                    dictionary.values().as_string::<i32>().value(key).to_owned()
                })
            };
            let gap_pid = column("unattributed_gap_pid");
            let gap_pid = gap_pid.as_primitive::<UInt32Type>();
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
                unattributed: Unattributed {
                    cpu_us: signed("unattributed_cpu_us"),
                    io_read_bytes: signed("unattributed_io_read_bytes"),
                    io_write_bytes: signed("unattributed_io_write_bytes"),
                    source: enum_text("unattributed_source"),
                    coverage: enum_text("unattributed_coverage"),
                    gap_pid: gap_pid.is_valid(0).then(|| gap_pid.value(0)),
                },
            };
            (name, row)
        })
        .collect()
}

/// The unattributed-usage cells 13_telemetry.md says `accounting`'s reconciliation states, written
/// here from the specification rather than read from the writer's projection: the source and the
/// coverage by their wire kinds, the differences signed and unclamped, a comparison the source
/// could not make null. None for a complete tree that agrees with the totals in every comparison
/// its source could make.
fn stated_unattributed(accounting: &cowshed_core::api::JobAccounting) -> Option<Unattributed> {
    use cowshed_core::api::{JobAccounting, ProcessCoverage, ProcessCoverageGap};

    let (source, reconciliation) = match accounting {
        JobAccounting::LinuxCgroupV2 { reconciliation, .. } => ("linuxCgroupV2", reconciliation),
        JobAccounting::MacOsRusageChildren { reconciliation, .. } => {
            ("macOsRusageChildren", reconciliation)
        }
    };
    let (coverage, gap_pid) = match reconciliation.coverage {
        ProcessCoverage::Complete => ("complete", None),
        ProcessCoverage::Gap { reason } => match reason {
            ProcessCoverageGap::EventsLost => ("eventsLost", None),
            ProcessCoverageGap::UnobservedBirth { pid } => ("unobservedBirth", Some(pid)),
            ProcessCoverageGap::UnobservedExit { pid } => ("unobservedExit", Some(pid)),
            ProcessCoverageGap::UncountedFork { pid } => ("uncountedFork", Some(pid)),
            ProcessCoverageGap::UnreadImage { pid } => ("unreadImage", Some(pid)),
            ProcessCoverageGap::UnreadFinalUsage { pid } => ("unreadFinalUsage", Some(pid)),
            ProcessCoverageGap::UnreadExitStatus { pid } => ("unreadExitStatus", Some(pid)),
        },
    };
    let cells = Unattributed {
        cpu_us: Some(reconciliation.cpu_us.get()),
        io_read_bytes: reconciliation.io_read_bytes.map(|bytes| bytes.get()),
        io_write_bytes: reconciliation.io_write_bytes.map(|bytes| bytes.get()),
        source: Some(source.to_owned()),
        coverage: Some(coverage.to_owned()),
        gap_pid,
    };
    let agrees = coverage == "complete"
        && cells.cpu_us == Some(0)
        && cells.io_read_bytes.is_none_or(|bytes| bytes == 0)
        && cells.io_write_bytes.is_none_or(|bytes| bytes == 0);
    (!agrees).then_some(cells)
}

/// The rows of `job`'s span: its start, the usage its terminal sample states as unattributed if
/// it states any, then its end as `entry_type` in `state`.
fn expected_span(
    job: &cowshed_core::api::JobInfo,
    entry_type: &str,
    state: &str,
) -> Vec<JobSpanRow> {
    let row = |entry_type: &str, message: &str, job_state: Option<&str>| JobSpanRow {
        trace_id: job.trace.trace_id.as_str().to_owned(),
        thread_id: cowshed_core::runtime::job_spans::job_span_thread(
            &job.repo_id,
            &job.workspace_incarnation,
            job.job_id,
        ),
        span_id: 1,
        parent_span_id: None,
        entry_type: entry_type.to_owned(),
        message: message.to_owned(),
        repo_id: job.repo_id.as_str().to_owned(),
        workspace_incarnation: job.workspace_incarnation.as_str().to_owned(),
        job_id: job.job_id.get(),
        grant_revision: job.grant_revision,
        w3c_span_id: u64::from_str_radix(job.trace.span_id.as_str(), 16).unwrap(),
        job_state: job_state.map(str::to_owned),
        unattributed: Unattributed::default(),
    };
    let unattributed = job
        .resources
        .as_ref()
        .and_then(|sample| stated_unattributed(&sample.accounting))
        .map(|unattributed| JobSpanRow {
            unattributed,
            ..row("info", "job.unattributed", None)
        });
    [
        Some(row("span-start", "cowshed.job", None)),
        unattributed,
        Some(row(entry_type, "cowshed.job", Some(state))),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Asserts the segments under `telemetry` are exactly `spans`, each job's rows in sealing order,
/// each named `job-<order>-<writer>.arrow` by one writer counting from 1.
fn assert_job_spans(telemetry: &Path, spans: &[Vec<JobSpanRow>]) {
    let sealed = job_spans(telemetry);
    let names = sealed
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names.len(),
        spans.iter().map(Vec::len).sum::<usize>(),
        "one sealed segment per row of each job's span: {names:?}"
    );
    let writer = &names[0]["job-00000000000000000001-".len()..];
    let mut previous = 0;
    for name in &names {
        let order: u64 = name["job-".len().."job-".len() + 20].parse().unwrap();
        assert!(
            order > previous,
            "one shared writer's ordered job rows: {names:?}"
        );
        assert_eq!(&name["job-00000000000000000001-".len()..], writer);
        previous = order;
    }
    let rows = sealed.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    let expected = spans.iter().flatten().collect::<Vec<_>>();
    assert_eq!(rows.iter().collect::<Vec<_>>(), expected);
}

#[cfg(target_os = "macos")]
fn sealed_trace_count(telemetry: &Path) -> u64 {
    std::fs::read_dir(telemetry)
        .expect("telemetry partitions")
        .flat_map(|day| std::fs::read_dir(day.expect("partition").path()).expect("trace segments"))
        .filter(|entry| {
            entry
                .as_ref()
                .expect("trace segment")
                .path()
                .extension()
                .is_some_and(|extension| extension == "arrow")
        })
        .count()
        .try_into()
        .expect("the fixture's segment count fits u64")
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
    let root = workspace_root(&temp_parent::scratch_parent(), "job-span");
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
    let job = h
        .handle
        .exec(None, None, adopted, AdmissionSource::Worker)
        .await
        .unwrap();
    let spawned = h.spawned.recv().await.unwrap();
    complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let clean = h.handle.wait(job).await.unwrap();
    assert_eq!(clean.trace, adopted_trace());
    let health = h.handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
    assert_job_spans(&telemetry, &[expected_span(&clean, "span-ok", "exited")]);

    let job = h
        .handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    let root = workspace_root(&temp_parent::scratch_parent(), "job-span-refused");
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
    let root = workspace_root(&temp_parent::scratch_parent(), "job-span-two-workspaces");
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    let second = WorkspaceAuthoritySnapshot {
        workspace_incarnation: WorkspaceIncarnation::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap(),
        ..authority()
    };
    let mut ended = Vec::new();
    for (name, authority) in [("first", authority()), ("second", second)] {
        let workspace = workspace_root(
            &temp_parent::scratch_parent(),
            &format!("job-span-workspace-{name}"),
        );
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
        let job = h
            .handle
            .exec(None, None, adopted, AdmissionSource::Worker)
            .await
            .unwrap();
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
    let root = workspace_root(&temp_parent::scratch_parent(), "job-span-served");
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
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
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
/// exec and its terminal leave one sealed job span carrying the trace context it was given: its
/// start, the usage its terminal sample states as unattributed -- a polled census never covers
/// the whole tree -- and its end.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_real_exec_seals_one_job_span_through_the_production_supervisor() {
    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::canonical_parent(), "job-span-exec");
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
        .exec(
            None,
            None,
            run("/usr/bin/true", Some(adopted_trace())),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let clean = handle.wait(job).await.unwrap();
    assert_eq!(clean.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(clean.trace, adopted_trace());
    assert!(
        clean.resources.is_some(),
        "a real exec owns a process: {clean:?}"
    );
    let health = handle.trace_health().await.unwrap();
    assert_eq!(health.failed, 0, "{health:?}");
    assert_eq!(health.recorded, sealed_trace_count(&telemetry));
    assert_job_spans(&telemetry, &[expected_span(&clean, "span-ok", "exited")]);

    let job = handle
        .exec(
            None,
            None,
            run("/usr/bin/false", None),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let failed = handle.wait(job).await.unwrap();
    assert_eq!(failed.exit, Some(ExitStatus::Exited { code: 1 }));
    assert!(
        failed.resources.is_some(),
        "a real exec owns a process: {failed:?}"
    );
    let health = handle.trace_health().await.unwrap();
    assert_eq!(health.failed, 0, "{health:?}");
    assert_eq!(health.recorded, sealed_trace_count(&telemetry));
    assert_job_spans(
        &telemetry,
        &[
            expected_span(&clean, "span-ok", "exited"),
            expected_span(&failed, "span-err", "exited"),
        ],
    );
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// Real background jobs under seatbelt, with the production artifact, commitment and span
/// writers. CPU admission is immediate so scheduler queueing is not mistaken for setup cost.
/// Tool counts describe the initial capability ceiling, not the live bootstrap links: per-cwd
/// detection replaces those contributions. The fsio probe independently varies actual link count.
/// Run explicitly on the host with COWSHED_TIMING=1 and --test-threads=1; never part of a gate.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "manual host-controller performance measurement"]
async fn per_job_fixed_cost_measurement() {
    use cowshed_core::capabilities::BootstrapProgram;
    use cowshed_core::runtime::supervisor::CommitmentPublisher;
    use cowshed_core::storage::audit::ArrowAuditSink;
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    const TOOLS: [&str; 32] = [
        "git",
        "node",
        "direnv",
        "devenv",
        "nix",
        "nix-store",
        "nix-shell",
        "nix-env",
        "cargo",
        "rustc",
        "rustdoc",
        "rustup",
        "go",
        "gofmt",
        "python",
        "uv",
        "java",
        "javac",
        "gradle",
        "mvn",
        "cc",
        "c++",
        "clang",
        "clang++",
        "cmake",
        "make",
        "ninja",
        "sccache",
        "ruby",
        "bundle",
        "php",
        "composer",
    ];
    for count in [1, 8, 32] {
        let (mut config, root) = isolated_config(&temp_parent::canonical_parent(), "fixed-cost");
        config.default_cwd = None;
        config.group_ledger = Some(root.join("groups.json"));
        config.telemetry_root = Some(root.join("telemetry"));
        config.sandbox.capabilities.contribution.bootstrap_programs = TOOLS[..count]
            .iter()
            .map(|name| BootstrapProgram {
                name,
                target: PathBuf::from("/usr/bin/true"),
            })
            .collect();
        let mount = &config.workspace_root;
        std::fs::create_dir_all(&config.sandbox.home).unwrap();
        std::fs::create_dir_all(&config.sandbox.exec_temp_dir).unwrap();
        std::fs::create_dir_all(mount.join(".cowshed/bin")).unwrap();
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
        let commitments = CommitmentPublisher::start(
            Box::new(ArrowAuditSink::open(root.join("telemetry")).unwrap()),
            8,
        )
        .unwrap();
        let handle = WorkspaceSupervisor::start(config, commitments).unwrap();
        let mut samples = Vec::new();
        for iteration in 0..25 {
            let started = Instant::now();
            let job = handle
                .exec(
                    None,
                    None,
                    ExecRequest {
                        command: ExecCommand::Argv(vec!["/usr/bin/true".into()]),
                        cwd: None,
                        ..request(StdinSource::Empty)
                    },
                    AdmissionSource::Worker,
                )
                .await
                .unwrap();
            let ended = handle.wait(job).await.unwrap();
            assert_eq!(ended.exit, Some(ExitStatus::Exited { code: 0 }));
            assert_eq!(ended.state, JobState::Exited);
            assert!(ended.resources.is_some(), "{ended:?}");
            if iteration != 0 {
                samples.push(started.elapsed().as_secs_f64());
            }
        }
        samples.sort_by(f64::total_cmp);
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        println!(
            "fixed-cost initial_tools={count} jobs={} mean_ms={:.3} median_ms={:.3} seconds_per_1000={:.3}",
            samples.len(),
            mean * 1e3,
            samples[samples.len() / 2] * 1e3,
            mean * 1e3,
        );
        assert_eq!(handle.trace_health().await.unwrap().failed, 0);
        handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
    }
}

/// A worker asking for a gate, or for the host boundary, is refused with the typed
/// class-not-permitted error before anything exists: no record, no spawn.
#[tokio::test]
async fn a_worker_gate_is_refused_before_any_record_request_or_spawn() {
    let (mut h, _root) = harness(1, 1024, false, false);
    for scope in [ExecScope::Workspace, ExecScope::HostGate] {
        let refused = h
            .handle
            .exec(
                None,
                None,
                ExecRequest {
                    scope,
                    ..gate(request(StdinSource::Empty))
                },
                AdmissionSource::Worker,
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code, ErrorCode::SandboxDenied, "{scope:?}");
        assert!(refused.cpu_admission_source().is_some(), "{scope:?}");
    }
    assert!(h.artifacts.try_recv().is_err(), "no job record");
    assert!(h.spawned.try_recv().is_err(), "no spawn");
}

/// Public plaintext is a record projection, not an execution policy. Both scopes preserve the
/// authored custom environment but bind it through a digest after their supervisor restarts.
#[tokio::test]
async fn a_host_gate_records_only_public_environment_without_restricting_execution() {
    const PRIVATE: &str = "operator-private-value-must-never-enter-admission";
    for scope in [ExecScope::Workspace, ExecScope::HostGate] {
        let (mut config, _root) =
            isolated_config(&temp_parent::scratch_parent(), "record-only-environment");
        config.credential_env_names.insert("TEST_SECRET".to_owned());
        let records = config.workspace_root.join(".cowshed/job/records.arrow");
        let authored = || {
            let mut ask = keyed("record-projection", StdinSource::Empty);
            ask.scope = scope;
            if scope == ExecScope::HostGate {
                ask = gate(ask);
            }
            for name in ["PROJECT_CUSTOM", "NPM_TOKEN", "NODE_OPTIONS", "NX_DAEMON"] {
                ask.env.insert(name.to_owned(), PRIVATE.to_owned());
            }
            ask.env
                .insert("TEST_SECRET".to_owned(), "withheld-value".to_owned());
            ask
        };
        let source = if scope == ExecScope::HostGate {
            AdmissionSource::Coordinator
        } else {
            AdmissionSource::Worker
        };
        let mut first = real_store_harness(config.clone());
        let job = first
            .handle
            .exec(None, None, authored(), source)
            .await
            .expect("custom environment names never refuse a request");
        let spawned = first.spawned.recv().await.unwrap();
        assert_eq!(spawned.request.env["PROJECT_CUSTOM"], PRIVATE);
        assert!(
            !spawned.request.env.contains_key("TEST_SECRET"),
            "existing credentials remain withheld"
        );
        complete(&spawned, b"", b"", ExitStatus::Exited { code: 0 }).await;
        first.handle.wait(job).await.unwrap();
        let history =
            cowshed_core::storage::job_artifact::recover_records(&census(), &records).unwrap();
        assert!(!history.frames.is_empty());
        for frame in history.frames {
            let record = frame.record;
            let admitted = record.admission.expect("keyed admission is durable");
            assert_eq!(
                admitted.env,
                BTreeMap::from([("LANG".to_owned(), "C".to_owned())])
            );
            assert_ne!(admitted.env_sha256, Sha256Digest::compute(&[]));
        }
        let bytes = std::fs::read(&records).unwrap();
        assert!(
            !bytes
                .windows(PRIVATE.len())
                .any(|window| window == PRIVATE.as_bytes())
        );
        first.handle.quiesce().await.unwrap();
        first
            .handle
            .retire(NxDaemonRetirement::Recorded)
            .await
            .unwrap();
        drop(first);
        let mut second = real_store_harness(config);
        assert_eq!(
            second
                .handle
                .exec(None, None, authored(), source)
                .await
                .unwrap(),
            job
        );
        let mut withheld = authored();
        withheld.env.insert(
            "TEST_SECRET".to_owned(),
            "another-withheld-value".to_owned(),
        );
        assert_eq!(
            second
                .handle
                .exec(None, None, withheld, source)
                .await
                .unwrap(),
            job
        );
        let mut changed = authored();
        changed.env.insert(
            "PROJECT_CUSTOM".to_owned(),
            "different-private-value".to_owned(),
        );
        let refused = second
            .handle
            .exec(None, None, changed, source)
            .await
            .unwrap_err();
        assert_eq!(
            refused.admission_source(),
            Some(&AdmissionRefusal::KeyConflict {
                job_id: job,
                fields: vec![AdmissionField::Env],
            })
        );
        assert!(
            second.spawned.try_recv().is_err(),
            "neither repeats nor conflicts run another job"
        );
    }
}

/// A job's span states the host at its two boundaries as the supervisor found it: the start row as
/// admission read it, the end row as the job's terminal resource sample holds it. A supervisor
/// configured with no volume to stat states no volume change: those cells are null, never zero.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_job_span_states_the_admission_host_and_the_terminal_sample_s_host() {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{Int64Type, UInt16Type, UInt32Type};
    use cowshed_core::api::{VolumeUnavailable, VolumeUsage};
    use cowshed_core::runtime::job_span_facts::HostColumns;
    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::scratch_parent(), "job-span-facts");
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    supervisor_config.telemetry_root = Some(telemetry.clone());
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let before = cowshed_core::host_load::read_host_load().expect("a host before admission");
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let after = cowshed_core::host_load::read_host_load().expect("a host after admission");
    let job = spawned.recv().await.unwrap();
    let command = lone_group();
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&command, Instant::now()),
            nice: {
                // SAFETY: the fixture still owns this live leader.
                let observed = unsafe { libc::getpriority(libc::PRIO_PROCESS, command.id()) };
                assert_ne!(observed, -1, "fixture leader priority is observable");
                Some(observed)
            },
        },
        0,
    )
    .await;
    end_group(&command);
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let ended = handle.wait(job_id).await.unwrap();
    reap(command);
    let terminal = ended.resources.expect("a terminal sample");
    assert!(
        matches!(
            terminal.volumes.workspace,
            VolumeUsage::Unavailable {
                reason: VolumeUnavailable::Unconfigured
            }
        ) && terminal.volumes.build.is_none(),
        "the supervisor was configured with no volume: {:?}",
        terminal.volumes
    );
    let health = handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (3, 0), "{health:?}");
    let facts = |batch: &arrow_array::RecordBatch| {
        let cell = |name: &str| batch.column_by_name(name).expect(name).clone();
        let milli = cell("load1_milli");
        let cores = cell("cores");
        let workspace = cell("disk_ws_delta_bytes");
        let build = cell("disk_build_delta_bytes");
        (
            milli.as_primitive::<UInt32Type>().iter().next().flatten(),
            cores.as_primitive::<UInt16Type>().iter().next().flatten(),
            workspace
                .as_primitive::<Int64Type>()
                .iter()
                .next()
                .flatten(),
            build.as_primitive::<Int64Type>().iter().next().flatten(),
        )
    };
    let batches = job_span_batches(&telemetry);
    assert_eq!(
        batches.len(),
        3,
        "a start row, the unattributed usage its terminal sample states -- a polled census never covers the whole tree -- and an end row"
    );
    // The start row's host lies between independent reads taken around admission.
    let milli = |host| HostColumns::of(host).expect("a host in range");
    let (low, high) = (
        milli(before).load1_milli.min(milli(after).load1_milli),
        milli(before).load1_milli.max(milli(after).load1_milli),
    );
    let (start_load, start_cores, start_workspace, start_build) = facts(&batches[0].1);
    assert!(
        start_load.is_some_and(|load| (low..=high).contains(&load)),
        "start load {start_load:?} outside {low}..={high}"
    );
    assert_eq!(start_cores, Some(before.cores.get()));
    assert_eq!((start_workspace, start_build), (None, None));
    // The unattributed-usage row states no host or volume fact of its own.
    assert_eq!(facts(&batches[1].1), (None, None, None, None));
    // The end row is the terminal sample's host, and no volume change it did not state.
    let host = milli(terminal.host);
    assert_eq!(
        facts(&batches[2].1),
        (Some(host.load1_milli), Some(host.cores), None, None)
    );
}

/// What the job's totals hold and no observed process counted -- children that forked, burned CPU
/// and were reaped between two samples -- is stated whole by the job's span: one unattributed
/// row, just before its end, whose cells are the terminal sample's own reconciliation. The macOS
/// source has no storage bytes to compare, so those cells are null, never zero.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn cpu_the_observed_processes_never_counted_is_the_job_span_s_unattributed_row() {
    let (mut supervisor_config, root) =
        isolated_config(&temp_parent::scratch_parent(), "unattributed-row");
    let telemetry = root.join("telemetry");
    std::fs::create_dir(&telemetry).unwrap();
    supervisor_config.telemetry_root = Some(telemetry.clone());
    let mut harness = warm_harness(supervisor_config);
    let (handle, spawned) = (harness.handle.clone(), &mut harness.spawned);
    let job_id = handle
        .exec(
            None,
            None,
            request(StdinSource::Empty),
            AdmissionSource::Worker,
        )
        .await
        .unwrap();
    let job = spawned.recv().await.unwrap();
    let mut leader = Scripted::start(&format!(
        "{burner} &\n{burner} &\nwait\necho reaped\nread line\n",
        burner = burner()
    ));
    deliver(
        &handle,
        &job,
        ProcessEvent::Started {
            job_id,
            process: owned(&leader.child, Instant::now()),
            nice: {
                // SAFETY: the fixture still owns this live leader.
                let observed = unsafe { libc::getpriority(libc::PRIO_PROCESS, leader.child.id()) };
                assert_ne!(observed, -1, "fixture leader priority is observable");
                Some(observed)
            },
        },
        0,
    )
    .await;
    let burned = leader.burned() + leader.burned();
    leader.expect("reaped");
    let leader = leader.exit();
    complete(&job, b"", b"", ExitStatus::Exited { code: 0 }).await;
    let ended = handle.wait(job_id).await.unwrap();
    let terminal = ended.resources.clone().expect("a terminal sample");
    let reconciliation = *terminal.accounting.reconciliation();
    assert!(
        reconciliation.cpu_us.get() > i64::try_from(burned / 2).unwrap(),
        "the reaped children's {burned} us are in the job's totals and in no observed process: {reconciliation:?}"
    );
    assert!(
        matches!(reconciliation.coverage, ProcessCoverage::Gap { .. }),
        "a polled census never claims the whole tree: {reconciliation:?}"
    );
    let health = handle.trace_health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (3, 0), "{health:?}");
    assert_job_spans(&telemetry, &[expected_span(&ended, "span-ok", "exited")]);
    let rows = job_spans(&telemetry);
    let stated = &rows[1].1;
    assert_eq!(
        (stated.entry_type.as_str(), stated.message.as_str()),
        ("info", "job.unattributed")
    );
    assert_eq!(
        stated.unattributed.cpu_us,
        Some(reconciliation.cpu_us.get())
    );
    assert_eq!(
        (
            stated.unattributed.io_read_bytes,
            stated.unattributed.io_write_bytes
        ),
        (None, None),
        "the source counts no storage bytes: they are unavailable, not zero"
    );
    assert_eq!(
        stated.unattributed.source.as_deref(),
        Some("macOsRusageChildren")
    );
    assert_ne!(
        stated.unattributed.coverage.as_deref(),
        Some("complete"),
        "the span states the gap the sample states"
    );
    assert!(
        stated.unattributed.gap_pid.is_some(),
        "every gap a macOS census names has the process it is about: {:?}",
        stated.unattributed
    );
    reap(leader);
}

/// A real native supervisor, including its protected credentials and ownership ledger.
#[cfg(target_os = "macos")]
fn native_process_fixture(
    name: &str,
    membership: Option<&std::path::Path>,
) -> (WorkspaceSupervisorHandle, PathBuf, TempRoot) {
    let (mut config, root) = isolated_config(&temp_parent::canonical_parent(), name);
    config.default_cwd = None;
    config.term_grace = Duration::from_secs(1);
    std::fs::create_dir_all(&config.sandbox.home).unwrap();
    std::fs::create_dir_all(config.workspace_root.join(".cowshed/bin")).unwrap();
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(
                config
                    .workspace_root
                    .join(cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH),
            )
            .unwrap()
            .write_all(
                cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32])
                    .encode()
                    .as_bytes(),
            )
            .unwrap();
    }
    let ledger = root.join("supervisor.groups");
    config.membership_socket =
        membership.map(cowshed_core::runtime::supervisor_socket::membership_socket_path);
    config.group_ledger = Some(ledger.clone());
    let commitments = cowshed_core::runtime::supervisor::CommitmentPublisher::start(
        Box::new(cowshed_core::storage::audit::NullAuditSink),
        8,
    )
    .unwrap();
    let handle = WorkspaceSupervisor::start(config, commitments).unwrap();
    (handle, ledger, root)
}

/// The command exits while its child is blocked on a fixture-owned socket. The child may keep
/// its inherited output open; the socket, not a late PID kill, releases it even on panic.
///
/// The child also passes this test a copy of its stdout, so the job cannot seal until the test
/// drops it: the draining window the test asserts in is the test's, not the retirement grace's.
/// Retirement sends the child TERM and, a grace later, KILL; a child the host does not schedule
/// within that grace never answers the TERM, so the answer is optional and the child's end -- its
/// socket closing -- is the event the test waits for. In a land gate the test, then requiring
/// the answer, read `early eof`: the child had ended at the KILL without answering. `starved`
/// makes that order certain: the child blocks SIGTERM, so the TERM stays pending and the KILL ends
/// it unanswered. Stopping the child instead is no starvation: once the leader exits, the kernel
/// sends SIGHUP and SIGCONT to a stopped member of the group that exit orphans, and the child dies
/// before retirement signals it.
#[cfg(target_os = "macos")]
async fn terminal_descendant_reproduction(
    detach: bool,
    cancel: bool,
    hold_output: bool,
    starved: bool,
) {
    use cowshed_core::runtime::job_groups::{Process, group_has_live_members};
    use cowshed_core::runtime::process_events::{Darwin, ProcessReads as _};
    use std::io::Read as _;

    let (handle, ledger, root) = native_process_fixture("terminal-descendant", None);
    let socket = root.join("child.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let script = r#"
import os, signal, socket, struct, sys
if os.fork() == 0:
    signal.signal(signal.SIGTERM, lambda *_: peer.sendall(b"t"))
    if sys.argv[4] == "starve":
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
    if sys.argv[2] == "detach":
        os.setsid()
    peer = socket.socket(socket.AF_UNIX)
    peer.connect(sys.argv[1])
    socket.send_fds(peer, [struct.pack("!IIII", os.getpid(), os.getppid(), os.getpgrp(), os.getsid(0))], [1])
    for fd in ((0,) if sys.argv[3] == "hold" else (0, 1, 2)):
        os.close(fd)
    peer.recv(1)
    os._exit(0)
os.read(0, 1)
os._exit(0)
"#;
    let job = handle
        .exec(
            None,
            None,
            ExecRequest {
                command: ExecCommand::Argv(vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    r#"exec python3 -c "$1" "$2" "$3" "$4" "$5""#.into(),
                    "descendant-fixture".into(),
                    script.into(),
                    CommandArg::from(socket.into_os_string()),
                    if detach { "detach" } else { "group" }.into(),
                    if hold_output { "hold" } else { "close" }.into(),
                    if starved { "starve" } else { "run" }.into(),
                ]),
                cwd: None,
                scope: ExecScope::HostGate,
                env: [(
                    "PATH".into(),
                    std::env::var("PATH").expect("development shell supplies python3"),
                )]
                .into_iter()
                .collect(),
                ..gate(request(StdinSource::Open))
            },
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let (child_socket, _) = listener.accept().await.unwrap();
    let child_socket = child_socket.into_std().unwrap();
    child_socket.set_nonblocking(false).unwrap();
    let (child_socket, report, held_stdout) = tokio::task::spawn_blocking(move || {
        let (report, stdout) = report_with_descriptor(&child_socket);
        (child_socket, report, stdout)
    })
    .await
    .unwrap();
    let [child, parent, group, session] = [0, 4, 8, 12]
        .map(|offset| u32::from_be_bytes(report[offset..offset + 4].try_into().unwrap()));
    let child_record = Darwin.record(child).unwrap().unwrap();
    let leader = Darwin.record(parent).unwrap().unwrap();
    assert_eq!(child_record.ppid, leader.pid);
    assert_eq!(child_record.parent_unique_id, leader.unique_id);
    assert!(Darwin.children(parent).unwrap().contains(&child));
    let mut chain = vec![child_record];
    while chain.last().unwrap().pid != std::process::id() {
        let next = Darwin.record(chain.last().unwrap().ppid).unwrap().unwrap();
        assert_eq!(chain.last().unwrap().parent_unique_id, next.unique_id);
        chain.push(next);
    }
    assert_eq!(group, if detach { child } else { parent });
    if detach {
        assert_eq!(session, child, "setsid makes a new session and group");
    }
    let child_process = Process::observe(i32::try_from(child).unwrap())
        .unwrap()
        .unwrap();
    let exit_watch = child_process.watch_exit().unwrap();
    // Hold the parent until the runtime has attached the child's kernel life. This proves
    // retirement of observed detached members, not completeness of Darwin's fork notifications.
    let mut membership = handle
        .process_events(job, cowshed_core::api::SampleInterval::new(1000).unwrap())
        .await
        .unwrap();
    // Runtime attaches the live child's kernel identity before parent exit.
    let child_index = async {
        loop {
            match membership.next().await.unwrap().unwrap() {
                cowshed_core::api::process::JobProcessEvent::Born { index, process }
                    if process.pid == child =>
                {
                    break index;
                }
                _ => {}
            }
        }
    }
    .await;
    let held = handle.info(job).await.unwrap();
    assert_eq!(
        held.state,
        JobState::Running,
        "a blocked live job is not a leak"
    );
    assert_eq!(published_ledger_leaders(&ledger, 1).await.len(), 1);
    let finishing_handle = handle.clone();
    let finishing = tokio::spawn(async move {
        if cancel {
            finishing_handle.kill(job).await?;
        } else {
            finishing_handle
                .stdin_write(job, 0, Bytes::from_static(b"x"))
                .await?;
        }
        finishing_handle.wait(job).await
    });
    // Retirement ends the owned descendant, its socket closing with it. It answers the TERM it is
    // sent first only if the host schedules it within the grace, before the KILL.
    let (child_socket, answered) = tokio::task::spawn_blocking(move || {
        let mut answered = Vec::new();
        (&child_socket).read_to_end(&mut answered).unwrap();
        (child_socket, answered)
    })
    .await
    .unwrap();
    assert!(
        answered.is_empty() || (!starved && answered == b"t"),
        "{answered:?}"
    );
    let draining = handle.info(job).await.unwrap();
    assert_eq!(draining.state, JobState::Draining);
    assert_eq!(draining.exit, None);
    assert_eq!(
        published_ledger_leaders(&ledger, 1).await.len(),
        1,
        "draining retains its claim"
    );
    assert!(
        !finishing.is_finished(),
        "sealing waits for the stdout this test holds"
    );
    drop(held_stdout);
    let ended = finishing.await.unwrap().unwrap();
    assert_eq!(
        ended.state,
        if cancel {
            JobState::Killed
        } else {
            JobState::Exited
        }
    );
    assert_eq!(
        ended.exit,
        Some(if cancel {
            ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false,
            }
        } else {
            ExitStatus::Exited { code: 0 }
        })
    );
    let surviving = Darwin
        .record(child)
        .unwrap()
        .filter(|now| now.unique_id == child_record.unique_id);
    let group_alive = group_has_live_members(i32::try_from(group).unwrap()).unwrap();
    cowshed_os::eprintln!(
        "terminal-descendant detach={detach} cancel={cancel} job={} state={:?} leader={parent} child={child} \
         group={group} session={session} group_alive={group_alive} surviving={surviving:?} \
         parent_chain={chain:?} ledger={:?}",
        job.get(),
        ended.state,
        ledger_leaders(&ledger)
    );
    // Close the fixture capability and join its pre-registered exit before the red assertion.
    drop(child_socket);
    tokio::task::spawn_blocking(move || exit_watch.wait())
        .await
        .unwrap()
        .unwrap();
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
    let config = config(&root);
    let mut reopened = ArtifactStore::open(
        &config.workspace_root,
        config.owned_repo_ids,
        config.authority.workspace_incarnation,
        config.artifacts,
        census(),
    )
    .unwrap();
    let durable = reopened.sealed(job).unwrap().unwrap();
    let durable = durable.record();
    assert_eq!(durable.containment, ended.containment);
    assert_eq!(durable.outlived, ended.outlived);
    assert_eq!(durable.outlived_incomplete, ended.outlived_incomplete);
    assert!(
        surviving.is_none() && !group_alive,
        "terminal job left an owned descendant alive: detach={detach}, child={child}, group={group}"
    );
    assert!(
        matches!(
            ended.containment,
            Some(cowshed_core::api::process::JobContainment::Observed { .. })
        ),
        "Darwin may prove observed membership ended, never kernel containment"
    );
    let outlived = ended
        .outlived
        .iter()
        .find(|member| member.pid == child)
        .expect("the process actually ended must have a durable retirement receipt");
    assert_eq!(outlived.index, Some(child_index));
    assert_eq!(outlived.signal, libc::SIGKILL);
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_terminal_reaps_a_background_descendant_with_closed_output() {
    terminal_descendant_reproduction(false, false, false, false).await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_terminal_reaps_a_descendant_that_started_a_new_session() {
    terminal_descendant_reproduction(true, false, false, false).await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_cancellation_reaps_a_background_descendant_with_closed_output() {
    terminal_descendant_reproduction(false, true, false, false).await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_cancellation_reaps_a_descendant_that_started_a_new_session() {
    terminal_descendant_reproduction(true, true, false, false).await;
}

/// The land gate's order: the host does not run the fixture child again before retirement's KILL.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_terminal_reaps_a_descendant_the_host_does_not_run_within_the_grace() {
    terminal_descendant_reproduction(false, false, false, true).await;
}

/// The fixture child's 16-byte report and the one descriptor it passes with it (`SCM_RIGHTS`).
#[cfg(target_os = "macos")]
fn report_with_descriptor(
    socket: &std::os::unix::net::UnixStream,
) -> ([u8; 16], std::os::fd::OwnedFd) {
    use std::io::Read as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    let mut report = [0_u8; 16];
    // Aligned for `cmsghdr` and larger than CMSG_SPACE of one descriptor.
    let mut space = [0_u64; 8];
    let mut part = libc::iovec {
        iov_base: report.as_mut_ptr().cast(),
        iov_len: report.len(),
    };
    // SAFETY: an all-zero msghdr is a valid empty header; the fields set next point at live
    // buffers of the lengths given.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &mut part;
    header.msg_iovlen = 1;
    header.msg_control = space.as_mut_ptr().cast();
    header.msg_controllen = libc::socklen_t::try_from(std::mem::size_of_val(&space)).unwrap();
    // SAFETY: `header` describes writable storage that outlives the call.
    let read = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut header, 0) };
    let read = usize::try_from(read)
        .unwrap_or_else(|_| panic!("recvmsg: {}", std::io::Error::last_os_error()));
    assert_ne!(
        read, 0,
        "the fixture child closed its socket before reporting"
    );
    // SAFETY: `header` was filled by recvmsg; CMSG_FIRSTHDR reads only its control fields.
    let message = unsafe { libc::CMSG_FIRSTHDR(&header) };
    assert!(
        header.msg_flags & libc::MSG_CTRUNC == 0
            && !message.is_null()
            // SAFETY: a non-null CMSG_FIRSTHDR points into `space`.
            && unsafe {
                (*message).cmsg_level == libc::SOL_SOCKET
                    && (*message).cmsg_type == libc::SCM_RIGHTS
                    && (*message).cmsg_len
                        == libc::CMSG_LEN(
                            libc::c_uint::try_from(std::mem::size_of::<libc::c_int>()).unwrap(),
                        )
            },
        "the fixture child passes its stdout, and only it, with its report"
    );
    // SAFETY: an SCM_RIGHTS message carries descriptors, possibly unaligned after its header.
    let descriptor =
        unsafe { std::ptr::read_unaligned(libc::CMSG_DATA(message).cast::<libc::c_int>()) };
    // SAFETY: the kernel installed this descriptor for this process; nothing else owns it.
    let descriptor = unsafe { std::os::fd::OwnedFd::from_raw_fd(descriptor) };
    // Darwin's recvmsg has no MSG_CMSG_CLOEXEC; a process this one spawns must not hold the stdout.
    // SAFETY: F_SETFD takes a plain integer and touches no memory of ours.
    assert_ne!(
        unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) },
        -1,
        "{}",
        std::io::Error::last_os_error()
    );
    let mut rest = socket;
    rest.read_exact(&mut report[read..]).unwrap();
    (report, descriptor)
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_terminal_reaps_a_detached_descendant_still_holding_output_open() {
    terminal_descendant_reproduction(true, false, true, false).await;
}

/// Client detachment leaves the declared Resident under the supervisor's one ownership ledger.
/// An unrelated job's conclusion may not turn that Resident into a cleanup target.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_declared_resident_survives_unrelated_terminal_and_refused_start() {
    use cowshed_core::runtime::job_groups::Process;
    use cowshed_core::runtime::process_events::{Darwin, ProcessReads as _};

    let (handle, ledger, _root) = native_process_fixture("resident-membership", None);
    let resident = handle
        .exec(
            None,
            None,
            ExecRequest {
                command: ExecCommand::Argv(
                    ["/bin/sh", "-c", "printf ready; read line"]
                        .into_iter()
                        .map(CommandArg::from)
                        .collect(),
                ),
                cwd: None,
                scope: ExecScope::Workspace,
                class: ExecClass::Resident,
                ..request(StdinSource::Open)
            },
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let ready = handle
        .log_read(resident, StreamKind::Stdout, 0, true)
        .await
        .unwrap();
    assert_eq!(ready.bytes, Bytes::from_static(b"ready"));
    let held = handle.info(resident).await.unwrap();
    assert_eq!(held.class, ExecClass::Resident);
    let pid = held.pid.unwrap();
    let life = Darwin.record(pid).unwrap().unwrap();
    let exit = Process::observe(i32::try_from(pid).unwrap())
        .unwrap()
        .unwrap()
        .watch_exit()
        .unwrap();
    handle.detach(resident).await.unwrap();
    let before = Darwin.children(std::process::id()).unwrap();

    for (executable, state) in [
        ("/usr/bin/true", JobState::Exited),
        (
            "/definitely-missing-cowshed-test-executable",
            JobState::Failed,
        ),
    ] {
        let job = handle
            .exec(
                None,
                None,
                ExecRequest {
                    command: ExecCommand::Argv(vec![executable.into()]),
                    cwd: None,
                    scope: ExecScope::HostGate,
                    ..gate(request(StdinSource::Empty))
                },
                AdmissionSource::Coordinator,
            )
            .await
            .unwrap();
        let ended = handle.wait(job).await.unwrap();
        assert_eq!(
            ended.state,
            state,
            "{ended:?}; stderr: {}",
            String::from_utf8_lossy(
                &handle
                    .log_read(job, StreamKind::Stderr, 0, false)
                    .await
                    .unwrap()
                    .bytes
            )
        );
        if state == JobState::Failed {
            assert_eq!(ended.pid, None);
            assert_eq!(ended.containment, None);
            assert!(ended.outlived.is_empty());
            assert!(!ended.outlived_incomplete);
        }
        assert_eq!(
            handle.info(resident).await.unwrap().state,
            JobState::Running
        );
        assert_eq!(
            Darwin.record(pid).unwrap().unwrap().unique_id,
            life.unique_id
        );
        assert_eq!(
            published_ledger_leaders(&ledger, 1)
                .await
                .into_iter()
                .map(|(job, _)| job)
                .collect::<Vec<_>>(),
            vec![resident.get()],
            "only the declared resident's claim remains"
        );
    }
    assert_eq!(
        Darwin.children(std::process::id()).unwrap(),
        before,
        "neither terminal nor refused start leaves a new direct child"
    );
    handle.kill(resident).await.unwrap();
    tokio::task::spawn_blocking(move || exit.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(published_ledger_leaders(&ledger, 0).await.is_empty());
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_native_runtime_drop_retires_observed_detached_membership() {
    use cowshed_core::runtime::job_groups::{Process, group_has_live_members};
    use cowshed_core::runtime::process_events::{Darwin, ProcessReads as _};
    use tokio::io::AsyncReadExt as _;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (handle, ledger, _root, peer, life, exit) = runtime.block_on(async {
        let (handle, ledger, root) = native_process_fixture("runtime-drop-membership", None);
        let socket = root.join("child.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let script = r#"
import os, signal, socket, struct, sys
if os.fork() == 0:
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    peer = socket.socket(socket.AF_UNIX)
    peer.connect(sys.argv[1])
    peer.sendall(struct.pack("!II", os.getpid(), os.getppid()))
    peer.recv(1)
    os._exit(0)
os.read(0, 1)
os._exit(0)
"#;
        let job = handle
            .exec(
                None,
                None,
                ExecRequest {
                    command: ExecCommand::Argv(vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        "exec python3 -c \"$1\" \"$2\"".into(),
                        "drop-fixture".into(),
                        script.into(),
                        CommandArg::from(socket.into_os_string()),
                    ]),
                    cwd: None,
                    scope: ExecScope::HostGate,
                    env: [("PATH".into(), std::env::var("PATH").unwrap())]
                        .into_iter()
                        .collect(),
                    ..gate(request(StdinSource::Open))
                },
                AdmissionSource::Coordinator,
            )
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut report = [0_u8; 8];
        peer.read_exact(&mut report).await.unwrap();
        let child = u32::from_be_bytes(report[..4].try_into().unwrap());
        let parent = u32::from_be_bytes(report[4..].try_into().unwrap());
        let life = Darwin.record(child).unwrap().unwrap();
        let parent_life = Darwin.record(parent).unwrap().unwrap();
        assert_eq!(
            (life.ppid, life.parent_unique_id),
            (parent, parent_life.unique_id)
        );
        assert_eq!(handle.info(job).await.unwrap().pid, Some(parent));
        let exit = Process::observe(i32::try_from(child).unwrap())
            .unwrap()
            .unwrap()
            .watch_exit()
            .unwrap();
        let mut observed = handle
            .process_events(job, cowshed_core::api::SampleInterval::new(1000).unwrap())
            .await
            .unwrap();
        loop {
            if let cowshed_core::api::process::JobProcessEvent::Born { process, .. } =
                observed.next().await.unwrap().unwrap()
                && process.pid == child
            {
                break;
            }
        }
        (handle, ledger, root, peer, life, exit)
    });
    drop(runtime);
    let joined = exit.within(Duration::from_secs(5)).unwrap();
    let live = Darwin
        .record(life.pid)
        .unwrap()
        .is_some_and(|now| now.unique_id == life.unique_id);
    let group_live = group_has_live_members(i32::try_from(life.pid).unwrap()).unwrap();
    drop(peer);
    if !joined {
        exit.wait().unwrap();
    }
    drop(handle);
    assert!(
        joined && !live && !group_live,
        "runtime drop left a retained setsid life running"
    );
    assert!(
        ledger_leaders(&ledger).is_empty(),
        "exit proof closes the ledger claim"
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_membership_endpoint_is_job_bound_and_never_admits_coordinator_authority() {
    use cowshed_core::api::process::{
        JobSubtreeAdmission, JobSubtreeRefusal, JobSubtreeRetirement,
    };
    use cowshed_core::runtime::job_groups::{Process, group_has_live_members};
    use cowshed_core::runtime::process_events::{Darwin, ProcessReads as _};
    use cowshed_core::runtime::supervisor_socket;
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

    let sockets = TempRoot::new_in(&temp_parent::canonical_parent(), "cs-members");
    let socket = sockets.join("s.sock");
    let (handle, ledger, root) = native_process_fixture("subtree-endpoint", Some(&socket));
    let bound = supervisor_socket::bind(&socket).await.unwrap();
    let serving = tokio::spawn(supervisor_socket::serve(bound, handle.clone(), None, None));
    supervisor_socket::hello(&socket).await.unwrap();
    let control_path = root.join("control.sock");
    let children_path = root.join("children.sock");
    let controls = tokio::net::UnixListener::bind(&control_path).unwrap();
    let children = tokio::net::UnixListener::bind(&children_path).unwrap();

    let script = r#"
import json, os, signal, socket, struct, sys, threading
endpoint = os.environ['COWSHED_SUPERVISOR_SOCKET']
def exact(s, n):
    result = b''
    while len(result) < n:
        part = s.recv(n - len(result))
        if not part: raise RuntimeError('membership response ended early')
        result += part
    return result
def rpc(request):
    with socket.socket(socket.AF_UNIX) as s:
        s.connect(endpoint)
        data = json.dumps(request).encode()
        s.sendall(struct.pack('!I', len(data)) + data)
        return json.loads(exact(s, struct.unpack('!I', exact(s, 4))[0]))
if sys.argv[1] == 'foreign':
    print(json.dumps(rpc({'retireDescendants': {'handle': json.loads(sys.argv[2]), 'graceMs': 20}})['ok']['value']))
    sys.exit(0)

def ps_argv():
    return ['/bin/ps', '-p', str(os.getpid()), '-ww', '-o', 'pid=' + 'h' * 131072]
def run_ps():
    out_read, out_write = os.pipe()
    pid = os.fork()

    if pid == 0:
        os.close(out_read)
        os.dup2(out_write, 1)
        argv = ps_argv()
        os.execv(argv[0], argv)
    os.close(out_write)
    with os.fdopen(out_read, 'rb') as out:
        data = out.read()
    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
    return {'pid': pid, 'code': 0, 'bytes': len(data)}
control = socket.socket(socket.AF_UNIX)
control.connect(sys.argv[1])
root_read, root_write = os.pipe()
go_read, go_write = os.pipe()
root = os.fork()
if root == 0:
    control.close()
    os.close(root_read)
    os.close(go_write)
    admitted = rpc('subtreeHandle')['ok']['value']
    assert admitted['kind'] == 'admitted', admitted
    child = os.fork()
    if child == 0:
        os.close(root_write)
        os.close(go_read)
        os.setsid()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        peer = socket.socket(socket.AF_UNIX)
        peer.connect(sys.argv[2])
        peer.sendall(struct.pack('!IIII', os.getpid(), os.getppid(), os.getpgrp(), os.getsid(0)))
        peer.recv(1)
        os._exit(0)
    with os.fdopen(root_write, 'w') as out:
        out.write(json.dumps({'admission': admitted, 'root': os.getpid(), 'child': child}) + '\n')
    os.read(go_read, 1)
    os._exit(0)
os.close(root_write)
os.close(go_read)
with os.fdopen(root_read) as inp:
    report = json.loads(inp.readline())
sib_cmd_read, sib_cmd_write = os.pipe()
sib_rep_read, sib_rep_write = os.pipe()
sibling = os.fork()
if sibling == 0:
    control.close()
    os.close(go_write)
    os.close(sib_cmd_write)
    os.close(sib_rep_read)
    held = None
    root2 = None
    def reply(value):
        os.write(sib_rep_write, (json.dumps(value) + '\n').encode())

    try:
        for raw in os.fdopen(sib_cmd_read, 'rb'):
            command = json.loads(raw)
            if command == 'ps_hold':
                churn = [run_ps() for _ in range(3)]
                out_read, out_write = os.pipe()
                ps = os.fork()
                if ps == 0:
                    os.close(out_read)
                    os.dup2(out_write, 1)
                    argv = ps_argv()
                    os.execv(argv[0], argv)
                os.close(out_write)
                stream = os.fdopen(out_read, 'rb')
                first = stream.read(1)
                assert first, 'ps produced no output'
                os.kill(ps, signal.SIGSTOP)
                stopped, status = os.waitpid(ps, os.WUNTRACED)
                assert stopped == ps and os.WIFSTOPPED(status), status
                held = (ps, stream, len(first))
                reply({'ps': ps, 'first_bytes': len(first), 'churn': churn})
            elif command == 'ps_release':
                ps, stream, first_bytes = held
                held = None
                os.kill(ps, signal.SIGCONT)
                rest = stream.read()
                stream.close()
                _, status = os.waitpid(ps, 0)
                assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
                reply({'ps_done': ps, 'code': 0, 'bytes': first_bytes + len(rest)})
            elif command == 'ps_churn':
                reply({'churned': [run_ps() for _ in range(4)]})
            elif command == 'root':
                r2_read, r2_write = os.pipe()
                go2_read, go2_write = os.pipe()
                root2_pid = os.fork()
                if root2_pid == 0:
                    os.close(r2_read)
                    os.close(go2_write)
                    admitted_reply = rpc('subtreeHandle')
                    value = admitted_reply.get('ok', {}).get('value')
                    if not value or value.get('kind') != 'admitted':
                        with os.fdopen(r2_write, 'w') as out:
                            out.write(json.dumps({'admission_error': admitted_reply}) + '\n')
                        os._exit(0)
                    child = os.fork()
                    if child == 0:
                        os.close(r2_write)
                        os.close(go2_read)
                        os.setsid()
                        signal.signal(signal.SIGTERM, signal.SIG_IGN)
                        peer = socket.socket(socket.AF_UNIX)
                        peer.connect(sys.argv[2])
                        peer.sendall(struct.pack('!IIII', os.getpid(), os.getppid(), os.getpgrp(), os.getsid(0)))
                        peer.recv(1)
                        os._exit(0)
                    with os.fdopen(r2_write, 'w') as out:
                        out.write(json.dumps({'admission': value, 'root': os.getpid(), 'child': child}) + '\n')
                    os.read(go2_read, 1)
                    os._exit(0)
                os.close(r2_write)
                os.close(go2_read)
                with os.fdopen(r2_read) as inp:
                    root2_report = json.loads(inp.readline())
                if 'admission_error' in root2_report:
                    os.waitpid(root2_pid, 0)
                    root2 = (root2_pid, None, root2_report)
                else:
                    root2 = (root2_pid, go2_write, root2_report)
                reply({'root2': root2_report})
            elif command == 'release_root':
                root2_pid, go2_write, root2_report = root2
                if go2_write is not None:
                    os.write(go2_write, b'x')
                    os.close(go2_write)
                    os.waitpid(root2_pid, 0)
                reply({'released_root2': True})
            elif command == 'exit':
                break
            else:
                raise RuntimeError(command)
    finally:
        if held is not None:
            ps, stream, first_bytes = held
            held = None
            os.kill(ps, signal.SIGCONT)
            stream.read()
            stream.close()
            os.waitpid(ps, 0)
    os._exit(0)
os.close(sib_cmd_read)
os.close(sib_rep_write)
sib_commands = os.fdopen(sib_cmd_write, 'w')
sib_replies = os.fdopen(sib_rep_read)
def sibling_rpc(command):
    sib_commands.write(json.dumps(command) + '\n')
    sib_commands.flush()
    return json.loads(sib_replies.readline())
control.sendall((json.dumps({**report, 'sibling': sibling}) + '\n').encode())
lines = control.makefile('rb')

root2_report = None
sibling_reaped = False
for line in lines:
    command = json.loads(line)
    if command == 'release':
        sibling_reply = sibling_rpc('release_root')
        assert sibling_reply == {'released_root2': True}, sibling_reply
        os.write(go_write, b'x')
        os.close(go_write)
        os.waitpid(root, 0)
        reply = {'released': True}
    elif command == 'ps_hold':
        reply = sibling_rpc('ps_hold')
    elif command == 'admit_root2':
        reply = sibling_rpc('root')
        root2_report = reply['root2']
    elif command == 'ps_release':
        reply = sibling_rpc('ps_release')
    elif command == 'ps_churn':
        reply = sibling_rpc('ps_churn')
    elif command == 'retire_both':
        handles = [('first', report['admission']['handle']), ('second', root2_report['admission']['handle'])]
        barrier = threading.Barrier(len(handles))
        results = {}
        def spend(name, handle):
            barrier.wait()
            results[name] = rpc({'retireDescendants': {'handle': handle, 'graceMs': 20}})
        threads = [threading.Thread(target=spend, args=(name, handle)) for name, handle in handles]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        reply = {'retire_both': [results['first'], results['second']]}
    elif command == 'privileged':
        reply = rpc('hello')
    elif command == 'retire':
        reply = rpc({'retireDescendants': {'handle': report['admission']['handle'], 'graceMs': 20}})['ok']['value']

    elif command == 'exit':
        sib_commands.write(json.dumps('exit') + '\n')
        sib_commands.flush()
        os.waitpid(sibling, 0)
        sibling_reaped = True
        break
    else:
        raise RuntimeError(command)

    control.sendall((json.dumps(reply) + '\n').encode())
if not sibling_reaped:
    sib_commands.write(json.dumps('exit') + '\n')
    sib_commands.flush()
    os.waitpid(sibling, 0)
"#;
    let request_for = |arguments: Vec<CommandArg>| ExecRequest {
        command: ExecCommand::Argv(arguments),
        cwd: None,
        scope: ExecScope::HostGate,
        env: [
            ("PATH".into(), std::env::var("PATH").unwrap()),
            (
                "COWSHED_SUPERVISOR_SOCKET".into(),
                "/not-the-owned-endpoint".into(),
            ),
        ]
        .into_iter()
        .collect(),
        ..gate(request(StdinSource::Empty))
    };
    let job = handle
        .exec(
            None,
            None,
            request_for(vec![
                "/bin/sh".into(),
                "-c".into(),
                r#"exec python3 -c "$1" "$2" "$3""#.into(),
                "membership-fixture".into(),
                script.into(),
                CommandArg::from(control_path.into_os_string()),
                CommandArg::from(children_path.into_os_string()),
            ]),
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let (control, _) = controls.accept().await.unwrap();
    let (reader, mut writer) = control.into_split();
    let mut lines = tokio::io::BufReader::new(reader).lines();
    let report: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    let admitted: JobSubtreeAdmission =
        serde_json::from_value(report["admission"].clone()).unwrap();
    let JobSubtreeAdmission::Admitted { handle: capability } = admitted else {
        panic!("admission refused")
    };

    let root_pid = u32::try_from(report["root"].as_u64().unwrap()).unwrap();
    let child_pid = u32::try_from(report["child"].as_u64().unwrap()).unwrap();
    let sibling_pid = u32::try_from(report["sibling"].as_u64().unwrap()).unwrap();
    let (mut child_peer, _) = children.accept().await.unwrap();
    let mut child_report = [0_u8; 16];
    child_peer.read_exact(&mut child_report).await.unwrap();
    let field = |at| u32::from_be_bytes(child_report[at..at + 4].try_into().unwrap());
    assert_eq!(
        [field(0), field(4), field(8), field(12)],
        [child_pid, root_pid, child_pid, child_pid]
    );
    let root_life = Darwin.record(root_pid).unwrap().unwrap();
    let child_life = Darwin.record(child_pid).unwrap().unwrap();
    assert_eq!(child_life.parent_unique_id, root_life.unique_id);
    let child = Process::observe(i32::try_from(child_pid).unwrap())
        .unwrap()
        .unwrap();
    let exit = child.watch_exit().unwrap();
    let mut events = handle
        .process_events(job, cowshed_core::api::SampleInterval::new(1000).unwrap())
        .await
        .unwrap();
    async {
        loop {
            if matches!(events.next().await.unwrap().unwrap(), cowshed_core::api::process::JobProcessEvent::Born { process, .. } if process.pid == child_pid) { break; }
        }
    }
    .await;

    // The same user and exact capability do not authorize a neighbouring admitted job.
    let foreign = handle
        .exec(
            None,
            None,
            request_for(vec![
                "/bin/sh".into(),
                "-c".into(),
                r#"exec python3 -c "$1" foreign "$2""#.into(),
                "foreign-fixture".into(),
                script.into(),
                serde_json::to_string(&capability).unwrap().into(),
            ]),
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let foreign_end = handle.wait(foreign).await.unwrap();
    assert_eq!(foreign_end.exit, Some(ExitStatus::Exited { code: 0 }));
    let foreign_out = handle
        .log_read(foreign, StreamKind::Stdout, 0, false)
        .await
        .unwrap();
    let refused: JobSubtreeRetirement = serde_json::from_slice(&foreign_out.bytes).unwrap();
    assert_eq!(
        refused,
        JobSubtreeRetirement::Refused {
            reason: JobSubtreeRefusal::NotMember
        }
    );

    writer.write_all(b"\"privileged\"\n").await.unwrap();
    let denied: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert!(
        denied["err"]["message"].as_str().unwrap().contains("hello"),
        "{denied}"
    );

    writer.write_all(b"\"ps_hold\"\n").await.unwrap();
    let ps_report: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert!(
        ps_report["churn"]
            .as_array()
            .unwrap()
            .iter()
            .all(|run| run["code"] == 0),
        "every churned ps exited successfully: {ps_report}"
    );
    let ps_pid = u32::try_from(ps_report["ps"].as_u64().unwrap()).unwrap();

    writer.write_all(b"\"admit_root2\"\n").await.unwrap();
    let root2_reply: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    let root2_report = &root2_reply["root2"];
    if let Some(error) = root2_report.get("admission_error") {
        writer.write_all(b"\"ps_release\"\n").await.unwrap();
        let released_ps: serde_json::Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        panic!(
            "second admission failed while setuid ps was held: {error}; held ps then released and reaped by its parent: {released_ps}"
        );
    }
    let admitted2: JobSubtreeAdmission =
        serde_json::from_value(root2_report["admission"].clone()).unwrap();
    let JobSubtreeAdmission::Admitted {
        handle: _capability2,
    } = admitted2
    else {
        panic!("second admission refused")
    };
    let root2_pid = u32::try_from(root2_report["root"].as_u64().unwrap()).unwrap();
    let child2_pid = u32::try_from(root2_report["child"].as_u64().unwrap()).unwrap();
    let (mut child2_peer, _) = children.accept().await.unwrap();
    let mut child2_report = [0_u8; 16];
    child2_peer.read_exact(&mut child2_report).await.unwrap();
    let field2 = |at| u32::from_be_bytes(child2_report[at..at + 4].try_into().unwrap());
    assert_eq!(
        [field2(0), field2(4), field2(8), field2(12)],
        [child2_pid, root2_pid, child2_pid, child2_pid]
    );
    let sibling_life = Darwin.record(sibling_pid).unwrap().unwrap();
    let root2_life = Darwin.record(root2_pid).unwrap().unwrap();
    let child2_life = Darwin.record(child2_pid).unwrap().unwrap();
    assert_eq!(root2_life.parent_unique_id, sibling_life.unique_id);
    assert_eq!(child2_life.parent_unique_id, root2_life.unique_id);
    let child2 = Process::observe(i32::try_from(child2_pid).unwrap())
        .unwrap()
        .unwrap();
    let exit2 = child2.watch_exit().unwrap();

    let mut held_failure: Option<String> = None;
    let mut retained_observation_errors: Vec<String> = Vec::new();
    async {
        loop {
            match events.next().await {
                Some(Ok(cowshed_core::api::process::JobProcessEvent::Born { process, .. }))
                    if process.pid == child2_pid =>
                {
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => retained_observation_errors.push(error.to_string()),
                None => {
                    if held_failure.is_none() {
                        held_failure = Some(
                            "process event stream ended before the second detached child was born"
                                .to_owned(),
                        );
                    }
                    break;
                }
            }
        }
    }
    .await;
    if !retained_observation_errors.is_empty() {
        cowshed_os::eprintln!(
            "process observation errors retained: {}",
            retained_observation_errors.join("; ")
        );
    }

    // The retained sibling's held ps is genuinely in its setuid-root interval: public
    // identity inspection reads its exact life, while the legacy combined flavor-18
    // read is still denied with its native EPERM preserved, never read as absence.
    // A failure here is reported only after the sibling releases and reaps its ps.
    let ps_record = Darwin.record(ps_pid);
    // SAFETY: geteuid takes no pointers and only reads the caller's credential.
    let caller_uid = unsafe { libc::geteuid() };
    let oracle_proved = matches!(&ps_record, Ok(Some(ps_life)) if ps_life.ppid == sibling_pid
        && ps_life.parent_unique_id == sibling_life.unique_id
        && ps_life.uid == 0
        && ps_life.uid != caller_uid);
    if !oracle_proved && held_failure.is_none() {
        held_failure = Some(format!(
            "held setuid ps identity read did not prove its exact root life: {ps_record:?}"
        ));
    }
    #[repr(C)]
    #[allow(dead_code)]
    struct LegacyUniqueId {
        _uuid: [u8; 16],
        _unique_id: u64,
        _parent_unique_id: u64,
        _id_version: i32,
        _reserved: [u8; 20],
    }
    #[repr(C)]
    #[allow(dead_code)]
    struct LegacyBsdWithUniqueId {
        _bsd: libc::proc_bsdinfo,
        _unique: LegacyUniqueId,
    }
    let mut legacy = std::mem::MaybeUninit::<LegacyBsdWithUniqueId>::zeroed();
    let legacy_size = libc::c_int::try_from(std::mem::size_of::<LegacyBsdWithUniqueId>()).unwrap();
    // SAFETY: legacy is writable storage of exactly legacy_size bytes.
    let legacy_written = unsafe {
        libc::proc_pidinfo(
            i32::try_from(ps_pid).unwrap(),
            18,
            0,
            legacy.as_mut_ptr().cast(),
            legacy_size,
        )
    };
    let legacy_error = std::io::Error::last_os_error();
    if !(legacy_written != legacy_size && legacy_error.raw_os_error() == Some(libc::EPERM))
        && held_failure.is_none()
    {
        held_failure = Some(format!(
            "legacy flavor-18 read of held setuid ps must preserve EPERM, got {legacy_written} bytes and {legacy_error}"
        ));
    }

    writer.write_all(b"\"release\"\n").await.unwrap();
    assert_eq!(
        lines.next_line().await.unwrap().unwrap(),
        "{\"released\": true}"
    );

    // An unrelated live process can use the same checkout without granting retirement authority.
    struct Held(std::process::Child);
    impl Drop for Held {
        fn drop(&mut self) {
            drop(self.0.stdin.take());
            if let Err(error) = self.0.wait_bounded() {
                cowshed_os::eprintln!("fixture child wait: {error}");
            }
        }
    }
    let mut outsider = Held(
        std::process::Command::new("/bin/sh")
            .args(["-c", "printf r; read line"])
            .current_dir(root.join("workspace"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap(),
    );
    let mut ready = [0_u8; 1];
    std::io::Read::read_exact(outsider.0.stdout.as_mut().unwrap(), &mut ready).unwrap();
    let outsider_life = Darwin.record(outsider.0.id()).unwrap().unwrap();

    // Two retirement requests for two admitted subtrees of this one job are in flight
    // together (the fixture starts both endpoint calls behind one barrier) while the
    // retained sibling's setuid ps is still held live.

    writer.write_all(b"\"retire_both\"\n").await.unwrap();
    let both: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    let retirement_of = |reply: &serde_json::Value| -> JobSubtreeRetirement {
        let value = reply["ok"]["value"].clone();
        assert!(
            !value.is_null(),
            "simultaneous retirement request failed: {reply}"
        );
        serde_json::from_value(value).unwrap()
    };

    // Only now may the sibling continue its own ps: it drains the blocked pipe and
    // its exit status is asserted, then it churns more real ps children to completion.
    // Every failure observed while the ps was held is reported after this release, so
    // no assertion above can strand the stopped ps with its parent.
    writer.write_all(b"\"ps_release\"\n").await.unwrap();
    let ps_done: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(ps_done["ps_done"].as_u64().unwrap(), u64::from(ps_pid));
    assert_eq!(ps_done["code"], 0, "held ps exited successfully: {ps_done}");
    assert!(
        ps_done["bytes"].as_u64().unwrap() > 131_072,
        "held ps header alone exceeds every pipe capacity, so it could not have exited early: {ps_done}"
    );
    writer.write_all(b"\"ps_churn\"\n").await.unwrap();
    let churned: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert!(
        churned["churned"]
            .as_array()
            .is_some_and(|runs| { runs.len() == 4 && runs.iter().all(|run| run["code"] == 0) }),
        "retained sibling kept churning ps after both retirements: {churned}"
    );

    assert!(
        held_failure.is_none(),
        "held setuid ps identity proof failed: {}",
        held_failure.unwrap_or_default()
    );
    let first = retirement_of(&both["retire_both"][0]);
    let second = retirement_of(&both["retire_both"][1]);
    let JobSubtreeRetirement::Retired {
        retirement: first_retirement,
    } = first
    else {
        panic!("first simultaneous retirement refused: {first:?}")
    };
    let JobSubtreeRetirement::Retired {
        retirement: second_retirement,
    } = second
    else {
        panic!("second simultaneous retirement refused: {second:?}")
    };

    for (retirement, retired_child_pid, retired_exit) in [
        (&first_retirement, child_pid, &exit),
        (&second_retirement, child2_pid, &exit2),
    ] {
        assert!(
            retirement.outlived.iter().any(|member| {
                member.pid == retired_child_pid && member.signal == libc::SIGKILL
            }),
            "detached child {retired_child_pid} was killed and joined: {retirement:?}"
        );
        assert!(
            !retirement
                .outlived
                .iter()
                .any(|member| { [outsider_life.pid, sibling_pid, ps_pid].contains(&member.pid) }),
            "retirement expanded beyond its subtree: {retirement:?}"
        );
        assert!(Darwin.record(retired_child_pid).unwrap().is_none());
        assert!(!group_has_live_members(i32::try_from(retired_child_pid).unwrap()).unwrap());
        assert!(retired_exit.within(Duration::from_secs(2)).unwrap());
    }
    assert_eq!(
        Darwin.record(outsider.0.id()).unwrap().unwrap().unique_id,
        outsider_life.unique_id
    );
    assert_eq!(
        Darwin.record(sibling_pid).unwrap().unwrap().unique_id,
        sibling_life.unique_id,
        "the retained sibling survived both retirements"
    );
    assert!(
        Darwin.record(ps_pid).unwrap().is_none(),
        "the sibling reaped its released ps"
    );
    assert_eq!(handle.info(job).await.unwrap().state, JobState::Running);
    writer.write_all(b"\"retire_both\"\n").await.unwrap();
    let spent_both: serde_json::Value =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(
        retirement_of(&spent_both["retire_both"][0]),
        JobSubtreeRetirement::Refused {
            reason: JobSubtreeRefusal::SpentHandle
        }
    );
    assert_eq!(
        retirement_of(&spent_both["retire_both"][1]),
        JobSubtreeRetirement::Refused {
            reason: JobSubtreeRefusal::SpentHandle
        }
    );
    writer.write_all(b"\"exit\"\n").await.unwrap();
    drop(writer);
    let ended = handle.wait(job).await.unwrap();

    assert!(
        ended.outlived.iter().any(|member| member.pid == child_pid)
            && ended.outlived.iter().any(|member| member.pid == child2_pid)
    );
    assert!(published_ledger_leaders(&ledger, 0).await.is_empty());

    drop(child_peer);
    drop(child2_peer);
    drop(outsider);
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
    serving.abort();
    assert!(serving.await.unwrap_err().is_cancelled());
}

/// This process's allocator footprint, threads and kqueues: a native fixture's supervisor runs
/// in the test's own process, so these are what the supervisor retains.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug)]
struct Footprint {
    heap_bytes: usize,
    heap_blocks: u32,
    resident_bytes: u64,
    threads: i32,
    kqueues: usize,
}

#[cfg(target_os = "macos")]
fn footprint() -> Footprint {
    let pid = i32::try_from(std::process::id()).unwrap();
    // SAFETY: plain-data out-parameters, written whole by the calls below.
    let (mut heap, mut task): (libc::malloc_statistics_t, libc::proc_taskinfo) =
        unsafe { (std::mem::zeroed(), std::mem::zeroed()) };
    // SAFETY: a null zone sums every zone; `heap` is a valid out-pointer.
    unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), &raw mut heap) };
    let size = i32::try_from(std::mem::size_of::<libc::proc_taskinfo>()).unwrap();
    // SAFETY: `task` is a valid out-pointer of `size` bytes.
    let read =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTASKINFO, 0, (&raw mut task).cast(), size) };
    assert_eq!(read, size, "{}", std::io::Error::last_os_error());
    // SAFETY: `proc_fdinfo` is plain data; the zeroed entries are overwritten or unread.
    let mut descriptors: Vec<libc::proc_fdinfo> = vec![unsafe { std::mem::zeroed() }; 4096];
    let capacity =
        i32::try_from(descriptors.len() * std::mem::size_of::<libc::proc_fdinfo>()).unwrap();
    // SAFETY: `descriptors` holds `capacity` writable bytes.
    let listed = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDLISTFDS,
            0,
            descriptors.as_mut_ptr().cast(),
            capacity,
        )
    };
    assert!(listed > 0, "{}", std::io::Error::last_os_error());
    let listed = usize::try_from(listed).unwrap() / std::mem::size_of::<libc::proc_fdinfo>();
    assert!(listed < descriptors.len(), "every descriptor was listed");
    Footprint {
        heap_bytes: heap.size_in_use,
        heap_blocks: heap.blocks_in_use,
        resident_bytes: task.pti_resident_size,
        threads: task.pti_threadnum,
        kqueues: descriptors[..listed]
            .iter()
            .filter(|descriptor| descriptor.proc_fdtype == libc::PROX_FDTYPE_KQUEUE.cast_unsigned())
            .count(),
    }
}

/// A sealed job keeps nothing of its process observation (07_api.md, "Process-tree
/// observations"): no writer thread, no kqueue, and no argv of any life it ran; its tree and
/// events answer a conflict naming its sealed record. Each ended job once kept its writer, two
/// kqueue descriptors and several copies of every argv until the supervisor retired: a
/// workspace that built for an hour held a gigabyte.
#[cfg(target_os = "macos")]
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_sealed_job_keeps_no_process_observation() {
    const JOBS: usize = 8;
    // The lives each job runs, as the script below counts them, and each one's argv bytes.
    const LIVES: usize = 32;
    const ARGV_BYTES: usize = 64 * 128;
    let (handle, _ledger, _root) = native_process_fixture("concluded-observation", None);
    // Each of 32 lives runs an 8 KiB argv and forks a child that shares it until its own exec,
    // as a compiler driver and the tools it starts do.
    let argv: Vec<CommandArg> = (0..64)
        .map(|index| CommandArg::from(format!("{index:0>128}")))
        .collect();
    let run = || async {
        let mut command: Vec<CommandArg> = vec![
            "/bin/bash".into(),
            "-c".into(),
            r#"i=0; while [ $i -lt 32 ]; do /bin/bash -c ':; /bin/sleep 0.01' bash "$@"; i=$((i+1)); done"#
                .into(),
            "bash".into(),
        ];
        command.extend(argv.iter().cloned());
        let job = handle
            .exec(
                None,
                None,
                ExecRequest {
                    command: ExecCommand::Argv(command),
                    cwd: None,
                    scope: ExecScope::HostGate,
                    env: [("PATH".into(), "/usr/bin:/bin".into())]
                        .into_iter()
                        .collect(),
                    ..gate(request(StdinSource::Empty))
                },
                AdmissionSource::Coordinator,
            )
            .await
            .unwrap();
        let ended = handle.wait(job).await.unwrap();
        assert_eq!(ended.exit, Some(ExitStatus::Exited { code: 0 }));
        job
    };
    // The first job warms the supervisor's own allocations; a read answered after its seal
    // orders the measurement after its release.
    let warm = run().await;
    handle.processes(warm).await.unwrap_err();
    let before = footprint();
    for _ in 0..JOBS {
        let job = run().await;
        // Answered by the actor once the job's seal released its observation.
        let released = handle.processes(job).await.unwrap_err();
        assert_eq!(released.code, ErrorCode::Conflict, "{released:?}");
        assert!(released.message.contains("is sealed"), "{released:?}");
    }
    let after = footprint();
    let retained = after.heap_bytes.saturating_sub(before.heap_bytes);
    cowshed_os::eprintln!(
        "{JOBS} sealed jobs: heap {} -> {} bytes ({retained} retained, {} blocks), \
         resident {} -> {}, threads {} -> {}, kqueues {} -> {}",
        before.heap_bytes,
        after.heap_bytes,
        i64::from(after.heap_blocks) - i64::from(before.heap_blocks),
        before.resident_bytes,
        after.resident_bytes,
        before.threads,
        after.threads,
        before.kqueues,
        after.kqueues,
    );
    assert!(
        after.kqueues <= before.kqueues,
        "a sealed job keeps no kqueue: {} -> {}",
        before.kqueues,
        after.kqueues
    );
    // A sealed job's record holds its command, once per store and record, and no life's argv:
    // measured at under a tenth of one argv per life it ran, bounded at a quarter. One copy per
    // life is four times the bound; the retained observation held five per life.
    assert!(
        retained < JOBS * LIVES * ARGV_BYTES / 4,
        "{JOBS} sealed jobs of {LIVES} lives retain {retained} heap bytes"
    );
    handle.retire(NxDaemonRetirement::Recorded).await.unwrap();
}

/// This test process's census, as a fixture store's census reaches it, over a store that holds no
/// lease.
fn census() -> cowshed_core::census::CensusScope {
    let namespace = cowshed_os::namespace::TestNamespace::of(std::process::id(), 0)
        .expect("this process's namespace");
    cowshed_core::census::CensusScope::new(
        cowshed_os::namespace::HostNamespace::Test(namespace),
        std::sync::Arc::from(std::path::Path::new("/var/empty")),
    )
}
