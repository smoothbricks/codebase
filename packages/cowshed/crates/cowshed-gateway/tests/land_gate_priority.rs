//! End-to-end land-gate priority tests against the real `Gateway` and real `WorkspaceSupervisor`
//! instances over `<store>/gateway.sock` (05_gateway.md "A land's gate takes tokens back").

#[path = "../../cowshed-core/tests/support/calm_pressure.rs"]
mod calm_pressure;
#[path = "../../cowshed-core/tests/support/sealed_segments.rs"]
mod sealed_segments;
#[path = "../../cowshed-core/tests/support/temp_parent.rs"]
mod temp_parent;
#[path = "../../cowshed-core/tests/support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cowshed_core::api::{
    CommandArg, ExecClass, ExecCommand, ExecRequest, ExecScope, ExitStatus, JobId, JobLander,
    RunSandboxMode, StdinSource,
};
use cowshed_core::gate_watch::{GateWatchOpen, GateWatchState};
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::{OwnedRepoIds, RepoId};
use cowshed_core::runtime::supervisor::{
    CommitmentPublisher, NxDaemonRetirement, WorkspaceAuthoritySnapshot, WorkspaceSupervisor,
    WorkspaceSupervisorConfig, WorkspaceSupervisorHandle,
};
use cowshed_core::runtime::supervisor_socket::AdmissionSource;
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::job_artifact::ArtifactConfig;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_os::deadline;
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};

struct NoCredentials;

#[async_trait]
impl CredentialProvider for NoCredentials {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(None)
    }
}

struct NoConnector;

#[async_trait]
impl UpstreamConnector for NoConnector {
    async fn health(&self, _target: &cowshed_gateway::CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Unknown
    }

    async fn connect(
        &self,
        _target: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        Err(ConnectError::NoAddresses)
    }
}

struct DiscardAudit;

#[async_trait]
impl AuditSink for DiscardAudit {
    async fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Ok(())
    }
}

async fn within_silence<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    match tokio::time::timeout(deadline::HEARTBEAT_SILENCE, fut).await {
        Ok(value) => value,
        Err(_) => panic!(
            "{what} did not happen within the silence bound ({:?})",
            deadline::HEARTBEAT_SILENCE
        ),
    }
}

async fn start_gateway(store_root: &Path) -> (Gateway, PathBuf) {
    let cache = store_root.join("cache");
    std::fs::create_dir_all(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    std::fs::create_dir_all(store_root.join("run")).expect("run dir");
    let socket = store_root.join(cowshed_core::storage::bootstrap::GATEWAY_SOCKET_NAME);
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            listener_handoff: Some(cowshed_gateway::handoff_path(&socket)),
            mirror_cache: MirrorCacheConfig::new(cache),
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start real gateway");
    (gateway, socket)
}

fn start_supervisor(
    store_root: &Path,
    name: &str,
    incarnation_hex: &str,
) -> (WorkspaceSupervisorHandle, PathBuf, PathBuf) {
    let ws_root = store_root.join(format!("ws-{name}"));
    let home = store_root.join(format!("home-{name}"));
    let telemetry = store_root.join(format!("telemetry-{name}"));
    let ledger = store_root.join("run").join(format!("{name}.groups"));
    std::fs::create_dir_all(ws_root.join(".cowshed/bin")).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&telemetry).unwrap();
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(ws_root.join(cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH))
            .unwrap()
            .write_all(
                cowshed_gateway_types::WorkspaceToken::from_bytes([7; 32])
                    .encode()
                    .as_bytes(),
            )
            .unwrap();
    }
    let repo_id = RepoId::parse("acme/widget").unwrap();
    let sandbox = SandboxConfig {
        home,
        mount_root: store_root.join("mounts"),
        workspace_mount: ws_root.clone(),
        exec_temp_dir: store_root.join(format!("tmp-{name}")),
        store_root: store_root.to_path_buf(),
        port_block: Some(PortBlock::new(49_136, 16).unwrap()),
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
    };
    let namespace = TestNamespace::of(std::process::id(), 0).expect("test namespace");
    let census = cowshed_core::census::CensusScope::new(
        HostNamespace::Test(namespace),
        Arc::from(Path::new("/var/empty")),
    );
    let config = WorkspaceSupervisorConfig {
        authority: WorkspaceAuthoritySnapshot {
            repo_id: repo_id.clone(),
            workspace: WorkspaceName::new(name).unwrap(),
            workspace_incarnation: WorkspaceIncarnation::new(incarnation_hex).unwrap(),
            grant_revision: 1,
            lifecycle_revision: 1,
        },
        owned_repo_ids: OwnedRepoIds::sole(repo_id),
        workspace_root: ws_root,
        census,
        default_cwd: None,
        sandbox,
        build_volume_layout: None,
        artifacts: ArtifactConfig::default(),
        term_grace: Duration::from_secs(1),
        actor_capacity: 64,
        event_capacity: 64,
        credential_env_names: Default::default(),
        shell_host: None,
        shell_pool: Default::default(),
        group_ledger: Some(ledger.clone()),
        group_ledger_lease: None,
        membership_socket: None,
        telemetry_root: Some(telemetry.clone()),
        inherited_groups: Vec::new(),
        volume_labels: None,
        workspace_volume: None,
        job_sampling: Default::default(),
        build_volume_released: None,
        memory_pressure: calm_pressure::calm(),
        nx_daemon_idle: cowshed_core::runtime::supervisor::NxDaemonIdle::DEFAULT,
    };
    let commitments =
        CommitmentPublisher::start(Box::new(cowshed_core::storage::audit::NullAuditSink), 8)
            .unwrap();
    let handle = WorkspaceSupervisor::start(config, commitments).unwrap();
    handle.gateway_hello();
    (handle, telemetry, ledger)
}

fn token_rows(telemetry: &Path, job_id: JobId) -> Vec<(String, Option<u32>)> {
    use arrow_array::Array as _;
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{UInt32Type, UInt64Type};

    let sealed = sealed_segments::read_sealed(telemetry).expect("read telemetry");
    let mut rows = Vec::new();
    for segment in sealed.segments {
        if !segment.name().starts_with("job-") {
            continue;
        }
        for batch in segment.batches {
            let job_ids = batch
                .column_by_name("job_id")
                .expect("job_id")
                .as_primitive::<UInt64Type>();
            let messages = batch
                .column_by_name("message")
                .expect("message")
                .as_dictionary::<UInt32Type>();
            let tokens = batch.column_by_name("tokens");
            for row in 0..batch.num_rows() {
                if job_ids.value(row) != job_id.get() {
                    continue;
                }
                let message = messages
                    .values()
                    .as_string::<i32>()
                    .value(usize::try_from(messages.keys().value(row)).unwrap())
                    .to_owned();
                let count = tokens.and_then(|col| {
                    let values = col.as_primitive::<UInt32Type>();
                    values.is_valid(row).then(|| values.value(row))
                });
                rows.push((message, count));
            }
        }
    }
    rows
}

/// Real `Gateway` + two real `WorkspaceSupervisor`s: a land's gate admitted on supervisor 1 drains
/// the jobserver fifo of a background job running on supervisor 2, and restores all tokens when
/// supervisor 1's land's gate ends.
#[tokio::test]
async fn real_gateway_coordinates_land_gate_token_withdrawal_across_supervisors() {
    let root = TempRoot::new_in(&temp_parent::canonical_parent(), "csgp");
    std::fs::set_permissions(&*root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store_root = root.to_path_buf();
    let (gateway, _socket) = start_gateway(&store_root).await;
    let (sup_gate, _gate_tel, _gate_ledger) =
        start_supervisor(&store_root, "lander", "0198f2c0b7e34dc795f17b238b331c81");
    let (sup_bg, bg_tel, _bg_ledger) =
        start_supervisor(&store_root, "worker", "0198f2c0b7e34dc795f17b238b331c82");

    // Start background jobserver client on supervisor 2 (`want = 4` -> 3 token bytes).
    let bg_sock = store_root.join("bg.sock");
    let bg_listener = UnixListener::bind(&bg_sock).unwrap();
    let client_py = r#"
import fcntl, os, select, socket, struct, sys

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sys.argv[1])
f_in = sock.makefile('r', encoding='utf-8')
f_out = sock.makefile('w', encoding='utf-8')
flags = os.environ.get('CARGO_MAKEFLAGS', '')
marker = '--jobserver-auth=fifo:'
fifo = flags[flags.index(marker) + len(marker):].split()[0]
fd = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)
if sys.platform == 'darwin':
    kq = select.kqueue()
    kq.control([select.kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD)], 0, 0)
    def avail():
        evs = kq.control([], 1, 0)
        return evs[0].data if evs else 0
else:
    def avail():
        buf = fcntl.ioctl(fd, 0x541b, struct.pack('I', 0))
        return struct.unpack('I', buf)[0]
f_out.write('ready\n')
f_out.flush()
for line in f_in:
    parts = line.strip().split()
    if not parts:
        continue
    if parts[0] == 'avail':
        f_out.write(f'{avail()}\n')
        f_out.flush()
    elif parts[0] == 'wait':
        n = int(parts[1])
        os.set_blocking(fd, True)
        got = b''
        while len(got) < n:
            got += os.read(fd, n - len(got))
        os.set_blocking(fd, False)
        os.write(fd, got)
        f_out.write(f'restored {len(got)}\n')
        f_out.flush()
    elif parts[0] == 'exit':
        f_out.write('bye\n')
        f_out.flush()
        break
"#;
    let bg_job = sup_bg
        .exec(
            None,
            None,
            ExecRequest {
                command: ExecCommand::Argv(vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    r#"exec python3 -c "$1" "$2""#.into(),
                    "bg-client".into(),
                    client_py.into(),
                    CommandArg::from(bg_sock.into_os_string()),
                ]),
                cwd: None,
                mode: RunSandboxMode::ReadWrite,
                scope: ExecScope::HostGate,
                env: [(
                    "PATH".into(),
                    std::env::var("PATH").expect("PATH supplies python3"),
                )]
                .into_iter()
                .collect(),
                stdin: StdinSource::Empty,
                stdout_copy: None,
                stderr_copy: None,
                record: cowshed_core::api::JobRecord::default(),
                trace: None,
                admission_key: None,
                class: ExecClass::Background,
                lander: None,
                want: Some(NonZeroUsize::new(4).unwrap()),
                hold: false,
            },
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let (bg_stream, _) = within_silence("bg client connect", bg_listener.accept())
        .await
        .unwrap();
    let (bg_r, mut bg_w) = bg_stream.into_split();
    let mut bg_r = BufReader::new(bg_r);
    let mut line = String::new();
    within_silence("bg ready", bg_r.read_line(&mut line))
        .await
        .unwrap();
    assert_eq!(line.trim(), "ready");

    // Start land's gate job on supervisor 1.
    let gate_sock = store_root.join("gate.sock");
    let gate_listener = UnixListener::bind(&gate_sock).unwrap();
    let gate_py = r#"
import socket, sys
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(sys.argv[1])
s.sendall(b"ready\n")
s.recv(16)
"#;
    let gate_job = sup_gate
        .exec(
            None,
            None,
            ExecRequest {
                command: ExecCommand::Argv(vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    r#"exec python3 -c "$1" "$2""#.into(),
                    "land-gate".into(),
                    gate_py.into(),
                    CommandArg::from(gate_sock.into_os_string()),
                ]),
                cwd: None,
                mode: RunSandboxMode::ReadWrite,
                scope: ExecScope::HostGate,
                env: [(
                    "PATH".into(),
                    std::env::var("PATH").expect("PATH supplies python3"),
                )]
                .into_iter()
                .collect(),
                stdin: StdinSource::Empty,
                stdout_copy: None,
                stderr_copy: None,
                record: cowshed_core::api::JobRecord::default(),
                trace: None,
                admission_key: None,
                class: ExecClass::Gate,
                lander: Some(JobLander {
                    pid: std::process::id(),
                    start: 1,
                }),
                want: None,
                hold: false,
            },
            AdmissionSource::Coordinator,
        )
        .await
        .unwrap();
    let (gate_stream, _) = within_silence("gate connect", gate_listener.accept())
        .await
        .unwrap();
    let (gate_r, mut gate_w) = gate_stream.into_split();
    let mut gate_r = BufReader::new(gate_r);
    line.clear();
    within_silence("gate ready", gate_r.read_line(&mut line))
        .await
        .unwrap();
    assert_eq!(line.trim(), "ready");

    // Supervisor 2 must drain all 3 token bytes via the real gateway's broadcast, observed through
    // its published liveness watch (never by polling telemetry rows).
    let mut bg_watch = sup_bg.watch_liveness().expect("watch sup_bg liveness");
    within_silence("sup_bg drains 3 tokens", async {
        loop {
            if bg_watch
                .borrow_and_update()
                .job_gate_priority(bg_job)
                .is_some_and(|state| state.tokens_withdrawn >= 3)
            {
                return;
            }
            bg_watch.changed().await.expect("sup_bg liveness closed");
        }
    })
    .await;
    bg_w.write_all(b"avail\n").await.unwrap();
    line.clear();
    within_silence("bg avail", bg_r.read_line(&mut line))
        .await
        .unwrap();
    assert_eq!(line.trim(), "0");

    // End the land's gate job on supervisor 1; supervisor 2 restores all 3 token bytes.
    gate_w.write_all(b"go\n").await.unwrap();
    gate_w.shutdown().await.unwrap();
    let ended = within_silence("gate exit", sup_gate.wait(gate_job))
        .await
        .unwrap();
    assert_eq!(ended.exit, Some(ExitStatus::Exited { code: 0 }));

    bg_w.write_all(b"wait 3\n").await.unwrap();
    line.clear();
    within_silence("bg restored 3", bg_r.read_line(&mut line))
        .await
        .unwrap();
    assert_eq!(line.trim(), "restored 3");

    bg_w.write_all(b"exit\n").await.unwrap();
    line.clear();
    within_silence("bg bye", bg_r.read_line(&mut line))
        .await
        .unwrap();
    let bg_ended = within_silence("bg exit", sup_bg.wait(bg_job))
        .await
        .unwrap();
    assert_eq!(bg_ended.exit, Some(ExitStatus::Exited { code: 0 }));
    let _ = sup_bg.trace_health().await.unwrap();
    let rows = token_rows(&bg_tel, bg_job);
    assert!(
        rows.iter()
            .any(|(msg, count)| msg == "job.tokens.restored" && *count == Some(3)),
        "expected job.tokens.restored(3): {rows:?}"
    );

    sup_gate.retire(NxDaemonRetirement::Recorded).await.unwrap();
    sup_bg.retire(NxDaemonRetirement::Recorded).await.unwrap();
    gateway.drain().await.unwrap();
}

/// Real `Gateway`: when a watching supervisor's `gate-watch` connection hits EOF, the gateway
/// clears `PRIO_DARWIN_BG` on every recorded life in that supervisor's `.groups` ledger under
/// `<store>/run/` immediately, without waiting for a successor supervisor to start.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn real_gateway_clears_darwinbg_from_ledger_on_watch_eof() {
    use cowshed_core::runtime::job_groups::GroupLeader;
    use cowshed_core::runtime::process_events::{Darwin, ProcessReads as _};
    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;
    use std::io::BufRead as _;
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};

    let root = TempRoot::new_in(&temp_parent::canonical_parent(), "csgpeof");
    std::fs::set_permissions(&*root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store_root = root.to_path_buf();
    let (gateway, socket) = start_gateway(&store_root).await;

    let mut child = Command::new("/bin/sh")
        .args(["-c", "printf 'ready\\n'; read _"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .process_group(0)
        .spawn_locked()
        .expect("spawn child");
    let pid = child.id();
    let mut ready = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(ready.trim(), "ready");

    let leader = GroupLeader::observe(pid).unwrap();
    let record = Darwin.record(pid).unwrap().unwrap();
    let boot_session = cowshed_apfs::attach_budget::boot().unwrap().session;
    // SAFETY: `pid` is our live child.
    let set = unsafe { libc::setpriority(libc::PRIO_DARWIN_PROCESS, pid, libc::PRIO_DARWIN_BG) };
    assert_eq!(set, 0, "set PRIO_DARWIN_BG");
    assert_eq!(
        procinfo::thread_max_priorities(pid.cast_signed()).unwrap(),
        Some(vec![4])
    );

    let ledger = store_root.join("run").join("dead-sup.groups");
    let payload = serde_json::json!({
        "supervisor": 1,
        "groups": [{
            "jobId": 1,
            "pgid": leader.pgid(),
            "leaderStart": leader.start(),
            "members": {
                "bootSession": boot_session,
                "lives": [{
                    "pid": pid,
                    "birth": record.unique_id,
                }]
            }
        }],
        "darwinbg": {
            "bootSession": boot_session,
            "lives": [{
                "pid": pid,
                "birth": record.unique_id,
            }]
        }
    });
    std::fs::write(&ledger, serde_json::to_vec(&payload).unwrap()).unwrap();

    // Open a second observer `gate-watch` stream so we observe the exact `active: false`
    // transition that fires after the dead supervisor's EOF handler clears `PRIO_DARWIN_BG`.
    use cowshed_os::test_backstop::AsyncUnixStreamBackstop as _;
    let observer = UnixStream::connect_bounded(&socket).await.unwrap();
    let (obs_reader, mut obs_writer) = observer.into_split();
    let mut obs_open = serde_json::to_vec(&GateWatchOpen::new(None)).unwrap();
    obs_open.push(b'\n');
    obs_writer.write_all(&obs_open).await.unwrap();
    let mut obs_lines = BufReader::new(obs_reader).lines();
    let obs_first = within_silence("observer initial gate-watch state", obs_lines.next_line())
        .await
        .expect("read observer initial state")
        .expect("gateway closed observer gate-watch");
    let obs_state: GateWatchState = serde_json::from_str(&obs_first).unwrap();
    assert!(obs_state.ok && !obs_state.active, "{obs_state:?}");

    // Connect `gate-watch` for the dead supervisor, register a gate job so EOF produces a
    // deterministic `active: false` event on `observer`, then close (EOF).
    let stream = UnixStream::connect_bounded(&socket).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut open = serde_json::to_vec(&GateWatchOpen::new(Some(ledger))).unwrap();
    open.push(b'\n');
    writer.write_all(&open).await.unwrap();
    let mut lines = BufReader::new(reader).lines();
    let first = within_silence("initial gate-watch state", lines.next_line())
        .await
        .expect("read initial state")
        .expect("gateway closed gate-watch without answering");
    let state: GateWatchState = serde_json::from_str(&first)
        .unwrap_or_else(|error| panic!("gateway answered non-GateWatchState {first:?}: {error}"));
    assert!(state.ok, "{state:?}");
    let mut start =
        serde_json::to_vec(&cowshed_core::gate_watch::GateWatchEvent::GateStart { job_id: 1 })
            .unwrap();
    start.push(b'\n');
    writer.write_all(&start).await.unwrap();
    let obs_on = within_silence("observer active=true", obs_lines.next_line())
        .await
        .expect("read observer active=true")
        .expect("gateway closed observer");
    let obs_on: GateWatchState = serde_json::from_str(&obs_on).unwrap();
    assert!(obs_on.ok && obs_on.active, "{obs_on:?}");
    drop(writer);
    drop(lines);

    // Wait on the observer's `active: false` event, emitted after the gateway's EOF handler
    // finishes clearing `PRIO_DARWIN_BG` on the dead supervisor's ledger.
    let obs_off = within_silence("observer active=false after EOF", obs_lines.next_line())
        .await
        .expect("read observer active=false")
        .expect("gateway closed observer");
    let obs_off: GateWatchState = serde_json::from_str(&obs_off).unwrap();
    assert!(obs_off.ok && !obs_off.active, "{obs_off:?}");
    drop(obs_writer);
    drop(obs_lines);

    let priorities = procinfo::thread_max_priorities(pid.cast_signed())
        .unwrap()
        .expect("child still running");
    assert!(
        !priorities.is_empty() && priorities.iter().all(|&p| p == 63),
        "expected PRIO_DARWIN_BG cleared (63) after gate-watch EOF, got {priorities:?}"
    );

    drop(child.stdin.take());
    let _ = child.wait_bounded();
    gateway.drain().await.unwrap();
}
