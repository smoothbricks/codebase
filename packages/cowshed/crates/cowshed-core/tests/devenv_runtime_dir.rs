//! Real shell activation through SystemSpawnSink and the executed-child Seatbelt profile.
//! Minimal .envrc fixtures exercise exports, executable lookup, private authority, and the
//! runtime/temp directories without Nix evaluation or a stand-in development environment.
//! Kernel-profile probes run in the host-controller lane: an enclosing workspace
//! sandbox cannot grant the fixture supervisor its independently declared authority.

#![cfg(target_os = "macos")]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use cowshed_core::api::{ExitStatus, JobId};
use cowshed_core::fork_lock::Run as _;
use cowshed_core::metadata::{
    GrantSet, MACOS_PORT_MIN, PortBlock, WorkspaceIncarnation, WorkspaceName,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor::{
    ProcessEvent, ProcessSignal, ProcessSpawnRequest, RunningProcess, SandboxPolicy, SpawnSink,
    SystemSpawnSink, WorkspaceAuthoritySnapshot,
};
use cowshed_core::sandbox::{
    RunSandboxMode, SandboxConfig, SandboxGrants, WorkspaceSandbox, sandbox_runtime_dir,
    sandbox_runtime_link, workspace_sandbox,
};
use cowshed_core::storage::job_artifact::StreamKind;
use cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH;
use cowshed_core::workspace_environment::{PORT_BASE_ENV, PORT_BLOCK_SIZE_ENV};
use cowshed_gateway_types::WorkspaceToken;

use tokio::sync::mpsc;

/// `sun_path` on macOS is 104 bytes; devenv keeps its default runtime base short for exactly
/// this reason (`resolve_runtime_dir`, devenv-core `paths.rs`). A base that leaves no room for
/// the `devenv-<7 hex>` component plus a socket name is a base devenv cannot use.
const SUN_PATH_BYTES: usize = 104;
const LONGEST_RUNTIME_SUFFIX: &str = "/devenv-1234567/x.sock";
/// `/Users/<user>/Dev/.cowshed/<owner>/<repo>/<workspace>` at the lengths this host actually
/// has (`/Users/danny/Dev/.cowshed/example-org/minigraf/minigraf-query-deps` is 66 bytes).
const PRODUCTION_MOUNT_BYTES: usize = 66;

/// Shell-entry probes run under the same profile as the command, not in the supervisor.
const RUNTIME_ENVRC: &str = r#"runtime_ok=false
tmp_denied=false
tmpdir_denied=false
if [ -n "$XDG_RUNTIME_DIR" ] && mkdir -m 700 "$XDG_RUNTIME_DIR/devenv-1234567"; then
  runtime_ok=true
fi
if mkdir -p "/private/tmp/devenv-1234567-$$"; then
  rmdir "/private/tmp/devenv-1234567-$$"
else
  tmp_denied=true
fi
if [ -n "$TMPDIR" ] && mkdir -p "$TMPDIR/devenv-1234567"; then
  rmdir "$TMPDIR/devenv-1234567"
else
  tmpdir_denied=true
fi
export COWSHED_RUNTIME_OK="$runtime_ok"
export COWSHED_TMP_DENIED="$tmp_denied"
export COWSHED_TMPDIR_DENIED="$tmpdir_denied"
"#;

fn scratch(label: &str) -> PathBuf {
    let alias = std::env::temp_dir().join(format!(
        "cowshed-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&alias).expect("scratch root");
    // Seatbelt matches resolved paths; `/var/folders` is a symlink into `/private/var`.
    std::fs::canonicalize(&alias).expect("canonical scratch root")
}

/// The host HOME is never under the mount root on a host, and a detector's HOME probes are
/// refused under a hard deny, so the fixture's mount root is a directory of its own beside it.
fn workspace(root: &Path, port_base: u16) -> SandboxConfig {
    let mount_root = root.join("mounts");
    let mount = mount_root.join("workspace");
    let private = mount.join(".cowshed");
    std::fs::create_dir_all(private.join("bin")).expect("private bin");
    std::fs::write(
        mount.join(WORKSPACE_TOKEN_PATH),
        WorkspaceToken::from_bytes([7; 32]).encode(),
    )
    .expect("workspace token");
    let home = root.join("home");
    let exec_temp_dir = root.join("tmp");
    std::fs::create_dir_all(&home).expect("home");
    std::fs::create_dir_all(&exec_temp_dir).expect("exec temp dir");
    SandboxConfig {
        home,
        mount_root,
        workspace_mount: mount,
        exec_temp_dir,
        port_block: PortBlock::new(port_base, 16).expect("port block"),
        retained_port_blocks: Vec::new(),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        shed_links: Vec::new(),
        git_worktree_repository: None,
        build_volume_mount: None,
        repository_caches: Vec::new(),
        capabilities: Default::default(),
    }
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_shell_activation_owns_a_runtime_directory_the_profile_lets_it_write() {
    let root = scratch("devenv-runtime");
    let sandbox = workspace(&root, 40_960);
    install_real_tool(&sandbox, "direnv");
    std::fs::write(sandbox.workspace_mount.join(".envrc"), RUNTIME_ENVRC)
        .expect("runtime shell-entry probes");
    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        &sandbox.workspace_mount,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            r#"printf '%s\0' "$XDG_RUNTIME_DIR" "$COWSHED_RUNTIME_OK" "$COWSHED_TMP_DENIED" "$COWSHED_TMPDIR_DENIED" "$TMPDIR""#.into(),
        ],
    )
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "shell activation exits 0 inside the sandbox; stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
    let printed: BTreeMap<&str, &str> = [
        "XDG_RUNTIME_DIR",
        "COWSHED_RUNTIME_OK",
        "COWSHED_TMP_DENIED",
        "COWSHED_TMPDIR_DENIED",
        "TMPDIR",
    ]
    .into_iter()
    .zip(
        std::str::from_utf8(&stdout)
            .expect("runtime values are UTF-8")
            .split_terminator('\0'),
    )
    .collect();

    // The regression condition: the profile still denies `/tmp`, so the only runtime base the
    // evaluation can use is the one the sandbox hands it.
    assert_eq!(
        printed["COWSHED_TMP_DENIED"], "true",
        "the executed-child profile must keep denying /tmp"
    );

    // `TMPDIR` is the exec temp dir and it IS writable: its grant follows every deny that
    // covers it, so `mktemp` in a child works. It is still not the runtime base - devenv
    // ignores TMPDIR for that by design, and the socket path length budget is the workspace
    // runtime dir's reason to exist - so the evaluation must be handed the runtime dir
    // explicitly, which the assertion below checks.
    assert_eq!(
        printed["COWSHED_TMPDIR_DENIED"], "false",
        "TMPDIR ({}) is the sandbox's own scratch and must be writable",
        printed["TMPDIR"]
    );

    // The child sees the short `/tmp/cs-<mount digest>` link - the `sun_path` budget - and it
    // resolves onto the shed's own runtime directory, which is what the profile grants.
    let runtime_link = PathBuf::from(printed["XDG_RUNTIME_DIR"]);
    assert_eq!(runtime_link, sandbox_runtime_link(&sandbox));
    let runtime_base =
        std::fs::read_link(&runtime_link).expect("the runtime link exists on the host");
    assert_eq!(
        runtime_base,
        sandbox_runtime_dir(&sandbox),
        "the link resolves to the workspace's own runtime directory"
    );
    assert!(
        runtime_base.is_absolute() && runtime_base.is_dir(),
        "the runtime base must exist before the child runs: {}",
        runtime_base.display()
    );
    assert!(
        runtime_base.starts_with(&sandbox.workspace_mount),
        "a shed carries its own runtime base inside its mount, never the host's /tmp: {}",
        runtime_base.display()
    );
    // Write-allowed by the profile, proven by the child rather than by reading the profile: the
    // shell-entry probe created devenv's `devenv-<hash>`-shaped subdirectory there, mode 0700.
    assert_eq!(
        printed["COWSHED_RUNTIME_OK"],
        "true",
        "the child must be able to create its runtime directory under {}",
        runtime_base.display()
    );
    let probe = runtime_base.join("devenv-1234567");
    assert!(
        probe.is_dir(),
        "the probe directory lands on the host side too"
    );
    assert_eq!(
        std::fs::metadata(&probe)
            .expect("probe metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    // Short enough for the unix-domain sockets devenv puts under it. The scratch mount here is
    // arbitrarily long, so the budget is checked on what the runtime base ADDS to the mount: a
    // production mount (`~/Dev/.cowshed/<owner>/<repo>/<workspace>`) is about 65 bytes, and the
    // suffix plus devenv's own `devenv-<7 hex>/<socket>` must fit in what remains.
    let added = runtime_base
        .strip_prefix(&sandbox.workspace_mount)
        .expect("inside the mount")
        .as_os_str()
        .len()
        + 1;
    let longest = PRODUCTION_MOUNT_BYTES + added + LONGEST_RUNTIME_SUFFIX.len();
    assert!(
        longest < SUN_PATH_BYTES,
        "runtime base adds {added} bytes to the mount; a {PRODUCTION_MOUNT_BYTES}-byte mount then leaves a {longest}-byte socket path, over the {SUN_PATH_BYTES}-byte sun_path limit"
    );

    std::fs::remove_dir_all(&root).expect("remove test workspace");
}

fn install_real_tool(sandbox: &SandboxConfig, name: &str) {
    let installed = std::env::split_paths(&std::env::var_os("PATH").expect("host PATH"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("required runtime tool `{name}` is not on PATH"));
    let installed = std::fs::canonicalize(installed).expect("resolve runtime tool");
    std::os::unix::fs::symlink(
        installed,
        sandbox.workspace_mount.join(".cowshed/bin").join(name),
    )
    .expect("make the installed runtime tool available in the sandbox");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_nx_runtime_directory_supports_real_unix_socket_roundtrips() {
    for mode in [RunSandboxMode::ReadWrite, RunSandboxMode::ReadOnly] {
        let root = scratch("nx-socket");
        let mut sandbox = workspace(&root, 41_056);
        sandbox.mode = mode;
        // Nx's socket namespace is the Nx capability's: the fixture is an Nx project.
        std::fs::write(sandbox.workspace_mount.join("nx.json"), "{}").expect("nx.json");
        sandbox
            .configure_capabilities()
            .expect("detect capabilities");
        install_real_tool(&sandbox, "node");
        let script = r#"
const net = require('node:net');
const fs = require('node:fs');
const directory = process.env.NX_SOCKET_DIR;
fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
fs.closeSync(fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW));
const path = require('node:path').join(directory, 'p12345-3-plugin.sock');
const server = net.createServer(socket => socket.end('nx-private-socket'));
server.listen(path, () => {
    const client = net.createConnection(path);
    client.on('data', data => process.stdout.write(data));
    client.on('end', () => server.close());
});
"#;
        let (exit, stdout, stderr) = run_in_sandbox(
            &sandbox,
            &sandbox.workspace_mount,
            vec!["node".into(), "-e".into(), script.into()],
        )
        .await;
        assert_eq!(
            exit,
            ExitStatus::Exited { code: 0 },
            "{mode:?} private Unix socket roundtrip failed: {}",
            String::from_utf8_lossy(&stderr)
        );
        assert_eq!(stdout, b"nx-private-socket");
        std::fs::remove_dir_all(root).expect("remove test workspace");
    }
}

/// A socket path must fit `sun_path`; the scratch roots under TMPDIR do not leave room for one.
fn short_scratch(label: &str) -> PathBuf {
    let root = PathBuf::from(format!(
        "/private/tmp/cowshed-{label}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    std::fs::create_dir_all(&root).expect("short scratch root");
    root
}

/// `roundtrip <path>` listens on a Unix socket and connects to it; `connect <path>` connects to an
/// existing one. Either prints what happened, so a refusal is an answer rather than a crash.
const UNIX_SOCKET_PROBE: &str = r#"
const net = require('node:net');
const [op, path] = process.argv.slice(1);
if (op === 'roundtrip') {
    const server = net.createServer(socket => socket.end('roundtrip'));
    server.on('error', error => process.stdout.write(`bind:${error.code}`));
    server.listen(path, () => {
        const client = net.createConnection(path);
        client.on('error', error => { process.stdout.write(`connect:${error.code}`); server.close(); });
        client.on('data', data => process.stdout.write(data));
        client.on('end', () => server.close());
    });
} else {
    const client = net.createConnection(path);
    client.on('connect', () => { process.stdout.write('connected'); client.destroy(); });
    client.on('error', error => process.stdout.write(`connect:${error.code}`));
}
"#;

async fn unix_socket_probe(sandbox: &SandboxConfig, op: &str, path: &Path) -> String {
    let (exit, stdout, stderr) = run_in_sandbox(
        sandbox,
        &sandbox.workspace_mount,
        vec![
            "node".into(),
            "-e".into(),
            UNIX_SOCKET_PROBE.into(),
            op.into(),
            path.as_os_str().to_owned(),
        ],
    )
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "{}",
        String::from_utf8_lossy(&stderr)
    );
    String::from_utf8(stdout).expect("probe output is UTF-8")
}

/// A workspace's own processes rendezvous over Unix sockets anywhere in its own tree — a test's
/// socket in its temp dir, a tool's under the checkout — but never reach a socket outside it, and
/// a read-only job never reaches the sockets under the mount, where read-write jobs' daemons
/// listen. A write grant is not a socket grant, and a directory whose name merely extends the
/// temp dir's is outside the tree.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_unix_sockets_are_admitted_in_the_workspace_tree_only() {
    let root = short_scratch("sockets");
    let outside = short_scratch("sockets-host");
    let host_socket = outside.join("host.sock");
    let _host_listener =
        std::os::unix::net::UnixListener::bind(&host_socket).expect("host listener");
    let mut sandbox = workspace(&root, 41_104);
    sandbox.exec_temp_dir = outside.join("tmp");
    let granted = outside.join("tmp-granted");
    for directory in [&sandbox.exec_temp_dir, &granted] {
        std::fs::create_dir(directory).expect("directory beside the workspace");
    }
    sandbox.grants.write.push(granted.clone());
    install_real_tool(&sandbox, "node");
    let writer_socket = sandbox.workspace_mount.join("writer.sock");
    let _writer_listener =
        std::os::unix::net::UnixListener::bind(&writer_socket).expect("read-write job's listener");

    assert_eq!(
        unix_socket_probe(
            &sandbox,
            "roundtrip",
            &sandbox.workspace_mount.join("tool.sock")
        )
        .await,
        "roundtrip",
        "a read-write job's socket under the checkout"
    );
    let temp_socket = sandbox.exec_temp_dir.join("test.sock");
    assert_eq!(
        unix_socket_probe(&sandbox, "roundtrip", &temp_socket).await,
        "roundtrip",
        "a read-write job's socket in its temp dir"
    );
    assert_eq!(
        unix_socket_probe(&sandbox, "connect", &host_socket).await,
        "connect:EPERM",
        "a socket outside the workspace tree"
    );
    assert_eq!(
        unix_socket_probe(&sandbox, "roundtrip", &granted.join("granted.sock")).await,
        "bind:EPERM",
        "a socket in a write-granted directory beside the temp dir"
    );

    sandbox.mode = RunSandboxMode::ReadOnly;
    assert_eq!(
        unix_socket_probe(&sandbox, "roundtrip", &temp_socket).await,
        "roundtrip",
        "a read-only job's socket in its temp dir"
    );
    assert_eq!(
        unix_socket_probe(&sandbox, "connect", &writer_socket).await,
        "connect:EPERM",
        "a read-only job reaching a read-write job's socket under the mount"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
    std::fs::remove_dir_all(outside).expect("remove host socket directory");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_private_environment_symlinks_cannot_redirect_host_preparation() {
    for mode in [RunSandboxMode::ReadOnly, RunSandboxMode::ReadWrite] {
        let root = scratch("environment-symlink");
        let mut sandbox = workspace(&root, 41_072);
        sandbox.mode = mode;
        let outside = root.join("denied");
        std::fs::create_dir(&outside).expect("outside sentinel directory");
        sandbox.additional_denies.push(outside.clone());
        let (exit, _, stderr) = run_in_sandbox(
            &sandbox,
            &sandbox.workspace_mount,
            vec![
                "/bin/sh".into(),
                "-c".into(),
                "mv \"$XDG_STATE_HOME\" \"$XDG_STATE_HOME.saved\" && ln -s \"$1\" \"$XDG_STATE_HOME\""
                    .into(),
                "plant".into(),
                outside.as_os_str().to_owned(),
            ],
        )
        .await;
        assert_eq!(exit, ExitStatus::Exited { code: 0 }, "{stderr:?}");
        let request = spawn_request(
            &sandbox,
            &sandbox.workspace_mount,
            vec!["/usr/bin/true".into()],
        );
        let (events, _receiver) = mpsc::channel(16);
        let result = SystemSpawnSink::default().spawn(request, events).await;
        assert!(
            !outside.join("nix").exists(),
            "a sandboxed child redirected the controller's next link creation"
        );
        let Err(error) = result else {
            panic!("a symlinked private state root must refuse before spawning");
        };
        assert_eq!(error.code, cowshed_core::ErrorCode::Integrity);
        std::fs::remove_dir_all(root).expect("remove test workspace");
    }
}

/// Every child gets a private `XDG_STATE_HOME` beside its other XDG roots. Nix's state under it
/// links to the host's own `~/.local/state/nix` only for a Nix project, as its cache under
/// `XDG_CACHE_HOME` links to `~/.cache/nix`; a project without the Nix convention gets no link
/// there.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_children_get_a_private_state_home_sharing_nix_state() {
    for nix_project in [false, true] {
        let root = scratch("state-home");
        let sandbox = workspace(&root, 41_088);
        let mount = sandbox.workspace_mount.clone();
        if nix_project {
            std::fs::write(mount.join("flake.nix"), "{ outputs = _: { }; }\n").expect("flake");
        }
        let (exit, stdout, stderr) = run_in_sandbox(
            &sandbox,
            &mount,
            vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf '%s\\n' \"$XDG_STATE_HOME\" && if [ -L \"$XDG_STATE_HOME/nix\" ]; then /usr/bin/readlink \"$XDG_STATE_HOME/nix\"; fi"
                    .into(),
            ],
        )
        .await;
        assert_eq!(
            exit,
            ExitStatus::Exited { code: 0 },
            "{}",
            String::from_utf8_lossy(&stderr)
        );
        let mut expected = format!("{}\n", mount.join(".cowshed/state").display());
        if nix_project {
            expected.push_str(&format!(
                "{}\n",
                sandbox.home.join(".local/state/nix").display()
            ));
        }
        assert_eq!(
            String::from_utf8_lossy(&stdout),
            expected,
            "nix project: {nix_project}"
        );
        std::fs::remove_dir_all(root).expect("remove test workspace");
    }
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_proxy_aware_client_reaches_an_allocated_loopback_service() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let root = scratch("loopback-http");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("local HTTP listener");
    let port = listener.local_addr().expect("listener address").port();
    // The block containing the listener's port: blocks are aligned to their own size.
    let sandbox = workspace(&root, port - port % 16);
    let origin = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("local HTTP connection");
        let mut request = [0u8; 1024];
        let mut used = 0;
        while !request[..used].ends_with(b"\r\n\r\n") {
            assert!(used < request.len(), "request exceeds fixture bound");
            let received = socket.read(&mut request[used..]).await.expect("request");
            assert_ne!(received, 0, "request closed before complete headers");
            used += received;
        }
        assert!(request[..used].starts_with(b"GET /local "));
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\nworkspace-local")
            .await
            .expect("local response");
    });
    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        &sandbox.workspace_mount,
        vec![
            "/usr/bin/curl".into(),
            "--fail".into(),
            "--silent".into(),
            "--show-error".into(),
            "--max-time".into(),
            "3".into(),
            format!("http://127.0.0.1:{port}/local").into(),
        ],
    )
    .await;
    origin.abort();
    let served = origin.await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "allocated loopback HTTP failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    served.expect("local service handled the request");
    assert_eq!(stdout, b"workspace-local");
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_proxy_bypass_does_not_admit_unallocated_loopback_ports() {
    let root = scratch("loopback-denial");
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("unallocated HTTP listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let port = listener.local_addr().expect("listener address").port();
    let base = if (40_000..40_016).contains(&port) {
        41_024
    } else {
        40_000
    };
    let sandbox = workspace(&root, base);
    let (exit, _, _) = run_in_sandbox(
        &sandbox,
        &sandbox.workspace_mount,
        vec![
            "/usr/bin/curl".into(),
            "--fail".into(),
            "--silent".into(),
            "--show-error".into(),
            "--max-time".into(),
            "3".into(),
            format!("http://127.0.0.1:{port}/forbidden").into(),
        ],
    )
    .await;
    assert_ne!(exit, ExitStatus::Exited { code: 0 });
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "the sandbox admitted an unallocated loopback connection"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

/// Turns this test binary into a TCP probe inside a workspace sandbox: `listen <port>,…` holds
/// the block's listener pairs and the named retained-block ports until stdin ends;
/// `serve` holds one listener on a block port and one on port 0 until stdin ends;
/// `connect <port>,…` reports what each connect met.
const TCP_PROBE: &str = "COWSHED_TCP_PROBE";
const TCP_PROBE_TEST: &str = "host_controller_tcp_probe_runs_as_a_sandboxed_child";
/// The prefix of every line the probe reports; the test harness's own output has none.
const TCP_PROBE_LINE: &str = "tcp-probe ";
const TCP_PROBE_IO: Duration = Duration::from_secs(3);
/// The size every workspace's block used to be capped at, and a block grown past it.
const CAPPED_BLOCK_SIZE: u16 = 64;
const GROWN_BLOCK_SIZE: u16 = 128;
/// 80 service ports held at once: more than a capped block has at all.
const LISTENER_PAIRS: u16 = 40;

/// Pair `i` is a block's `i`th service port from the bottom and from the top (the base is the
/// gateway's), so every pair straddles the capped block's boundary and the block's last port is
/// held. The controller lays the pairs out from the block it built, the probe from the base and
/// size its environment names, so the two agree only if the environment carries the grown block.
fn listener_pairs(base: u16, size: u16) -> Vec<[u16; 2]> {
    (0..LISTENER_PAIRS)
        .map(|pair| [base + 1 + pair, base + size - 1 - pair])
        .collect()
}

/// The sandboxed half of the grown-block regression: this test binary, executed inside a
/// workspace sandbox with its probe named in the environment. Run without one, it has nothing
/// to answer.
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_tcp_probe_runs_as_a_sandboxed_child() {
    let Ok(probe) = std::env::var(TCP_PROBE) else {
        return;
    };
    let port = |name: &str| -> u16 {
        let value = std::env::var(name).unwrap_or_else(|error| panic!("{name}: {error}"));
        value
            .parse()
            .unwrap_or_else(|error| panic!("{name}={value}: {error}"))
    };
    let base = port(PORT_BASE_ENV);
    if let Some(retained) = probe.strip_prefix("listen ") {
        hold_listener_pairs(base, port(PORT_BLOCK_SIZE_ENV), parse_ports(retained));
    } else if probe == "serve" {
        serve_on_the_block_and_on_port_zero(base, port(PORT_BLOCK_SIZE_ENV));
    } else if let Some(targets) = probe.strip_prefix("connect ") {
        connect_beyond_the_block(base, parse_ports(targets));
    } else {
        panic!("unknown TCP probe `{probe}`");
    }
}

fn format_ports(ports: &[u16]) -> String {
    ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_ports(ports: &str) -> Vec<u16> {
    ports
        .split(',')
        .map(|port| {
            port.parse()
                .unwrap_or_else(|error| panic!("probe port `{port}`: {error}"))
        })
        .collect()
}

/// Binds every pair's ports and the named ports of a retained block, connects to each while all
/// of them stay bound, then holds them until the controller closes stdin. A connection waiting
/// on any listener afterwards is one the sandbox let through from outside this process.
fn hold_listener_pairs(base: u16, size: u16, retained: Vec<u16>) {
    use std::io::Read;

    let listeners: Vec<(u16, TcpListener)> = listener_pairs(base, size)
        .into_iter()
        .flatten()
        .chain(retained)
        .map(
            |port| match TcpListener::bind((Ipv4Addr::LOCALHOST, port)) {
                Ok(listener) => (port, listener),
                Err(error) => panic!("bind 127.0.0.1:{port} inside the sandbox: {error}"),
            },
        )
        .collect();
    for (port, listener) in &listeners {
        roundtrip(listener, *port);
        println!("{TCP_PROBE_LINE}{port} roundtrip");
    }
    println!("{TCP_PROBE_LINE}ready");
    // The controller's stdin pipe is the hold: it ends when the controller closes it, kills this
    // process group, or exits.
    std::io::stdin()
        .read_to_end(&mut Vec::new())
        .expect("hold until the controller closes stdin");
    for (port, listener) in &listeners {
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        match listener.accept() {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Ok((_, peer)) => println!("{TCP_PROBE_LINE}{port} reached by {peer}"),
            Err(error) => panic!("accept on {port}: {error}"),
        }
    }
}

/// Roundtrips one connection to the block's first service port, then reports what connecting
/// to each target met: `denied` is the profile refusing the connect inside this process.
fn connect_beyond_the_block(base: u16, targets: Vec<u16>) {
    let own = base + 1;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, own))
        .unwrap_or_else(|error| panic!("bind 127.0.0.1:{own} inside the sandbox: {error}"));
    roundtrip(&listener, own);
    println!("{TCP_PROBE_LINE}{own} roundtrip");
    for port in targets {
        match TcpStream::connect_timeout(
            &SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            TCP_PROBE_IO,
        ) {
            Ok(_) => println!("{TCP_PROBE_LINE}{port} connected"),
            Err(error) if error.kind() == ErrorKind::PermissionDenied => {
                println!("{TCP_PROBE_LINE}{port} denied");
            }
            Err(error) => println!("{TCP_PROBE_LINE}{port} {error}"),
        }
    }
}

/// Binds one listener by the block allocation rule (04_sandbox.md), highest port first and on to
/// the next one down only past `EADDRINUSE`, and one on port 0. Reports both, holds them until the
/// controller closes stdin, then reports which of them a connection reached.
fn serve_on_the_block_and_on_port_zero(base: u16, size: u16) {
    use std::io::Read;

    let block = (base + 1..base + size)
        .rev()
        .find_map(
            |port| match TcpListener::bind((Ipv4Addr::LOCALHOST, port)) {
                Ok(listener) => Some(listener),
                Err(error) if error.kind() == ErrorKind::AddrInUse => None,
                Err(error) => panic!("bind 127.0.0.1:{port} inside the sandbox: {error}"),
            },
        )
        .expect("a free service port in the block");
    let ephemeral = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap_or_else(|error| panic!("bind 127.0.0.1:0 inside the sandbox: {error}"));
    let listeners = [("block", block), ("ephemeral", ephemeral)].map(|(kind, listener)| {
        let port = listener.local_addr().expect("listener address").port();
        println!("{TCP_PROBE_LINE}{kind} {port}");
        (port, listener)
    });
    println!("{TCP_PROBE_LINE}ready");
    std::io::stdin()
        .read_to_end(&mut Vec::new())
        .expect("hold until the controller closes stdin");
    for (port, listener) in &listeners {
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        match listener.accept() {
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Ok(_) => println!("{TCP_PROBE_LINE}{port} reached"),
            Err(error) => panic!("accept on {port}: {error}"),
        }
    }
}

/// One request and reply over a connection this process makes to `listener`. The client closes
/// first, so the `TIME_WAIT` lands on its ephemeral port and the listener's port is free again
/// as soon as the listener goes.
fn roundtrip(listener: &TcpListener, port: u16) {
    use std::io::{Read, Write};

    let mut client =
        TcpStream::connect_timeout(&SocketAddr::from((Ipv4Addr::LOCALHOST, port)), TCP_PROBE_IO)
            .unwrap_or_else(|error| panic!("connect to 127.0.0.1:{port}: {error}"));
    let (mut accepted, peer) = listener
        .accept()
        .unwrap_or_else(|error| panic!("accept on {port}: {error}"));
    assert_eq!(
        peer,
        client.local_addr().expect("client address"),
        "port {port} accepted a connection that is not the probe's"
    );
    for stream in [&client, &accepted] {
        stream
            .set_read_timeout(Some(TCP_PROBE_IO))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(TCP_PROBE_IO))
            .expect("write timeout");
    }
    let request = port.to_be_bytes();
    client.write_all(&request).expect("request");
    let mut received = [0_u8; 2];
    accepted.read_exact(&mut received).expect("receive request");
    assert_eq!(received, request, "request on {port}");
    accepted.write_all(&received).expect("reply");
    let mut reply = [0_u8; 2];
    client.read_exact(&mut reply).expect("receive reply");
    assert_eq!(reply, request, "reply on {port}");
    drop(client);
    assert_eq!(
        accepted.read(&mut [0_u8; 1]).expect("client close"),
        0,
        "the client on {port} closed after the reply"
    );
}

/// The grown block a relocated workspace now holds, the 64-port block it was relocated from and
/// still retains, and a sibling workspace's block between them: adjacent to both, so the grown
/// block's last port and the retained block's base border it.
struct ProbeBlocks {
    own: PortBlock,
    sibling: PortBlock,
    retained: PortBlock,
    /// A kernel claim shared across parallel runs and TMPDIR namespaces. The probe's
    /// service listeners never use the current gateway port.
    _claim: TcpListener,
}

impl ProbeBlocks {
    /// The retained block's first and last port: the old gateway port and its last service port.
    fn retained_ports(&self) -> [u16; 2] {
        let ports = self.retained.ports().expect("retained block");
        [*ports.start(), *ports.end()]
    }
}

/// Blocks below the macOS allocator range, where live workspaces hold theirs, whose probed ports
/// are free on this host.
fn free_probe_blocks() -> ProbeBlocks {
    (MACOS_PORT_MIN / 2..MACOS_PORT_MIN)
        .step_by(usize::from(4 * GROWN_BLOCK_SIZE))
        .find_map(|base| {
            let claim = TcpListener::bind((Ipv4Addr::LOCALHOST, base)).ok()?;
            let blocks = ProbeBlocks {
                own: PortBlock::new(base, GROWN_BLOCK_SIZE).expect("aligned grown block"),
                sibling: PortBlock::new(base + GROWN_BLOCK_SIZE, GROWN_BLOCK_SIZE)
                    .expect("aligned sibling block"),
                retained: PortBlock::new(base + 2 * GROWN_BLOCK_SIZE, CAPPED_BLOCK_SIZE)
                    .expect("aligned retained block"),
                _claim: claim,
            };
            listener_pairs(base, GROWN_BLOCK_SIZE)
                .into_iter()
                .flatten()
                .chain(blocks.retained_ports())
                .chain([blocks.sibling.base() + 1])
                .map(|port| TcpListener::bind((Ipv4Addr::LOCALHOST, port)))
                .collect::<Result<Vec<_>, _>>()
                .is_ok()
                .then_some(blocks)
        })
        .expect("free probe blocks below the macOS allocator range")
}

/// Every port is bound by someone else: a listener this host process cannot take.
fn assert_ports_held(ports: &[u16], moment: &str) {
    for port in ports {
        match TcpListener::bind((Ipv4Addr::LOCALHOST, *port)) {
            Err(error) if error.kind() == ErrorKind::AddrInUse => {}
            Ok(_) => panic!("127.0.0.1:{port} was free {moment}: the holder is not listening"),
            Err(error) => panic!("probe 127.0.0.1:{port} {moment}: {error}"),
        }
    }
}

fn probe_lines(stdout: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter(|line| line.starts_with(TCP_PROBE_LINE))
        .map(str::to_owned)
        .collect()
}

/// A workspace holding `block` and `retained` whose children may also read, and so execute,
/// this test binary.
fn probe_workspace(
    root: &Path,
    block: PortBlock,
    retained: Vec<PortBlock>,
    probe: &Path,
) -> SandboxConfig {
    let mut sandbox = workspace(root, block.base());
    sandbox.port_block = block;
    sandbox.retained_port_blocks = retained;
    sandbox.grants.read.push(probe.to_path_buf());
    sandbox
}

fn tcp_probe_request(
    sandbox: &SandboxConfig,
    probe: &Path,
    operation: String,
) -> ProcessSpawnRequest {
    let mut request = spawn_request(
        sandbox,
        &sandbox.workspace_mount,
        vec![
            probe.into(),
            "--exact".into(),
            TCP_PROBE_TEST.into(),
            "--ignored".into(),
            "--nocapture".into(),
            // Terse output prints nothing as a test starts, so every probe line starts a line.
            "--quiet".into(),
        ],
    );
    request.env.insert(TCP_PROBE.to_owned(), operation);
    request
}

/// A workspace relocated to a grown block holds 40 listener pairs at once: 80 service ports,
/// more than a capped block has, one of every pair past its boundary and one the block's last
/// port. It still holds the 64-port block it was relocated from, so it also listens on that
/// block's first and last port. The executed child binds them all inside its sandbox, the pairs
/// from the base and size its environment names, and roundtrips a connection to every one. A
/// sibling workspace's sandbox holding the block between the two meanwhile reaches its own port
/// and is denied every one of them, while each stays bound and none sees a connection.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_grown_port_block_holds_listener_pairs_no_sibling_reaches() {
    let blocks = free_probe_blocks();
    let ProbeBlocks {
        own,
        sibling,
        retained,
        ..
    } = blocks;
    for (left, right) in [(own, sibling), (sibling, retained), (own, retained)] {
        assert!(!left.overlaps(right), "{left} and {right} are disjoint");
    }
    let pairs: Vec<u16> = listener_pairs(own.base(), own.size())
        .into_iter()
        .flatten()
        .collect();
    let service = own.ports().expect("grown block");
    assert_eq!(
        pairs
            .iter()
            .filter(|port| service.contains(*port) && **port != own.base())
            .collect::<BTreeSet<_>>()
            .len(),
        usize::from(2 * LISTENER_PAIRS),
        "every listener holds its own service port of the grown block"
    );
    assert_eq!(
        pairs
            .iter()
            .filter(|port| **port - own.base() >= CAPPED_BLOCK_SIZE)
            .count(),
        usize::from(LISTENER_PAIRS),
        "one port of every pair lies past the capped block"
    );
    assert!(pairs.contains(service.end()));
    let retained_ports = blocks.retained_ports();
    let ports: Vec<u16> = pairs.iter().copied().chain(retained_ports).collect();

    let probe = std::fs::canonicalize(std::env::current_exe().expect("test binary"))
        .expect("canonical test binary");
    let own_root = scratch("grown-ports");
    let sibling_root = scratch("grown-ports-sibling");
    let holder = probe_workspace(&own_root, own, vec![retained], &probe);
    let neighbour = probe_workspace(&sibling_root, sibling, Vec::new(), &probe);

    let mut held = SandboxedChild::spawn(tcp_probe_request(
        &holder,
        &probe,
        format!("listen {}", format_ports(&retained_ports)),
    ))
    .await;
    held.until_probe_line(&format!("{TCP_PROBE_LINE}ready"))
        .await;
    assert_ports_held(&ports, "once the holder was ready");
    let (exit, stdout, stderr) = SandboxedChild::run(tcp_probe_request(
        &neighbour,
        &probe,
        format!("connect {}", format_ports(&ports)),
    ))
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "sibling probe: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(
        probe_lines(&stdout),
        std::iter::once(format!("{TCP_PROBE_LINE}{} roundtrip", sibling.base() + 1))
            .chain(
                ports
                    .iter()
                    .map(|port| format!("{TCP_PROBE_LINE}{port} denied"))
            )
            .collect::<Vec<_>>(),
        "the sibling reaches its own block and no listener of the grown or retained one"
    );
    assert_ports_held(&ports, "after the sibling's connects");

    held.close_stdin();
    let (exit, stdout, stderr) = held.wait().await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "holding probe: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(
        probe_lines(&stdout),
        ports
            .iter()
            .map(|port| format!("{TCP_PROBE_LINE}{port} roundtrip"))
            .chain([format!("{TCP_PROBE_LINE}ready")])
            .collect::<Vec<_>>(),
        "every listener roundtrips inside the sandbox and no sibling connect reaches one"
    );
    for port in &ports {
        TcpListener::bind((Ipv4Addr::LOCALHOST, *port)).unwrap_or_else(|error| {
            panic!("127.0.0.1:{port} stayed bound after the holder exited: {error}")
        });
    }
    std::fs::remove_dir_all(own_root).expect("remove grown workspace");
    std::fs::remove_dir_all(sibling_root).expect("remove sibling workspace");
}

/// A job binds the listeners its own processes connect to in its block. A second process of the
/// same workspace reaches a listener that took a block port by the allocation rule, and is denied
/// a listener the same server bound on port 0: the kernel picked that port from the host's
/// ephemeral range, and the connect allowlist holds only the block's literal ports.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_same_workspace_process_reaches_block_listeners_and_not_port_zero_ones() {
    let blocks = free_probe_blocks();
    let own = blocks.own;
    let probe = std::fs::canonicalize(std::env::current_exe().expect("test binary"))
        .expect("canonical test binary");
    let root = scratch("own-listeners");
    let workspace = probe_workspace(&root, own, Vec::new(), &probe);

    let mut server =
        SandboxedChild::spawn(tcp_probe_request(&workspace, &probe, "serve".to_owned())).await;
    server
        .until_probe_line(&format!("{TCP_PROBE_LINE}ready"))
        .await;
    let reported = |kind: &str| -> u16 {
        let prefix = format!("{TCP_PROBE_LINE}{kind} ");
        let lines = probe_lines(&server.stdout);
        let line = lines
            .iter()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("the server reports its {kind} listener: {lines:?}"));
        line.parse()
            .unwrap_or_else(|error| panic!("{kind} port `{line}`: {error}"))
    };
    let block = reported("block");
    let ephemeral = reported("ephemeral");
    assert_eq!(
        block,
        *own.ports().expect("own block").end(),
        "the rule's first candidate, the block's highest port, was free"
    );
    assert!(
        !own.ports().expect("own block").contains(&ephemeral),
        "port 0 took {ephemeral}, inside the block {own}"
    );

    let (exit, stdout, stderr) = SandboxedChild::run(tcp_probe_request(
        &workspace,
        &probe,
        format!("connect {block},{ephemeral}"),
    ))
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "connecting probe: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(
        probe_lines(&stdout),
        [
            format!("{TCP_PROBE_LINE}{} roundtrip", own.base() + 1),
            format!("{TCP_PROBE_LINE}{block} connected"),
            format!("{TCP_PROBE_LINE}{ephemeral} denied"),
        ],
        "a process of the same workspace reaches the block listener and is denied the port-0 one"
    );

    server.close_stdin();
    let (exit, stdout, stderr) = server.wait().await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "serving probe: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(
        probe_lines(&stdout),
        [
            format!("{TCP_PROBE_LINE}block {block}"),
            format!("{TCP_PROBE_LINE}ephemeral {ephemeral}"),
            format!("{TCP_PROBE_LINE}ready"),
            format!("{TCP_PROBE_LINE}{block} reached"),
        ],
        "only the block listener saw a connection"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

fn spawn_request(sandbox: &SandboxConfig, cwd: &Path, argv: Vec<OsString>) -> ProcessSpawnRequest {
    ProcessSpawnRequest {
        authority: WorkspaceAuthoritySnapshot {
            repo_id: RepoId::parse("acme/widget").expect("repo id"),
            workspace: WorkspaceName::new("main").expect("workspace name"),
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("incarnation"),
            grant_revision: 1,
            lifecycle_revision: 1,
        },
        job_id: JobId::new(1).expect("job id"),
        command: cowshed_core::runtime::supervisor::SpawnCommand::Argv(argv),
        cwd: cwd.to_path_buf(),
        env: BTreeMap::new(),
        policy: SandboxPolicy::render(sandbox.clone()).expect("sandbox policy"),
        mode: cowshed_core::api::RunSandboxMode::ReadWrite,
    }
}

/// A child running under the real sandbox, its output collected as it arrives. Dropping one that
/// has not exited kills its process group, so a failed assertion leaves nothing behind.
struct SandboxedChild {
    process: Box<dyn RunningProcess>,
    events: mpsc::Receiver<ProcessEvent>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    exit: Option<ExitStatus>,
    eof: usize,
}

impl SandboxedChild {
    async fn spawn(request: ProcessSpawnRequest) -> Self {
        let (events, receiver) = mpsc::channel(16);
        let process = SystemSpawnSink::default()
            .spawn(request, events)
            .await
            .expect("spawn through the real sandbox");
        Self {
            process,
            events: receiver,
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit: None,
            eof: 0,
        }
    }

    async fn run(request: ProcessSpawnRequest) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        let mut child = Self::spawn(request).await;
        child.close_stdin();
        child.wait().await
    }

    fn close_stdin(&mut self) {
        assert!(self.process.close_stdin(), "a fresh lane takes the EOF");
    }

    fn finished(&self) -> bool {
        self.exit.is_some() && self.eof == 2
    }

    /// Advances on the child's next event. The deadline only guards a hang, and names what is
    /// still open when it fires.
    async fn step(&mut self) {
        let Ok(event) = tokio::time::timeout(Duration::from_secs(30), self.events.recv()).await
        else {
            panic!(
                "sandboxed child hung: exit {:?}, {} of 2 output streams ended\nstdout: {}\nstderr: {}",
                self.exit,
                self.eof,
                String::from_utf8_lossy(&self.stdout),
                String::from_utf8_lossy(&self.stderr)
            );
        };
        let event = event.expect("process event channel remains open until exit and EOF");
        match event {
            ProcessEvent::Output { stream, bytes, .. } => match stream {
                StreamKind::Stdout => self.stdout.extend_from_slice(&bytes),
                StreamKind::Stderr => self.stderr.extend_from_slice(&bytes),
            },
            ProcessEvent::OutputEof { .. } => self.eof += 1,
            ProcessEvent::Exited { exit, .. } => self.exit = Some(exit),
            ProcessEvent::WaitFailed { error, .. } => panic!("child wait failed: {error}"),
            _ => {}
        }
    }

    /// Waits, while the child keeps running, until it reports `line`.
    async fn until_probe_line(&mut self, line: &str) {
        while !probe_lines(&self.stdout).iter().any(|seen| seen == line) {
            assert!(
                !self.finished(),
                "the child ended before reporting `{line}`: {}",
                String::from_utf8_lossy(&self.stderr)
            );
            self.step().await;
        }
    }

    async fn wait(mut self) -> (ExitStatus, Vec<u8>, Vec<u8>) {
        while !self.finished() {
            self.step().await;
        }
        (
            self.exit.clone().expect("observed child exit"),
            std::mem::take(&mut self.stdout),
            std::mem::take(&mut self.stderr),
        )
    }
}

impl Drop for SandboxedChild {
    fn drop(&mut self) {
        if self.exit.is_none()
            && let Err(error) = self.process.signal_process_tree(ProcessSignal::Kill)
        {
            eprintln!("cannot kill the sandboxed child: {error}");
        }
    }
}

async fn run_in_sandbox(
    sandbox: &SandboxConfig,
    cwd: &Path,
    argv: Vec<OsString>,
) -> (ExitStatus, Vec<u8>, Vec<u8>) {
    SandboxedChild::run(spawn_request(sandbox, cwd, argv)).await
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_shell_activation_preserves_environment_path_cwd_argv_and_private_home() {
    let root = scratch("shell-activation");
    let sandbox = workspace(&root, 40_976);
    install_real_tool(&sandbox, "direnv");
    let mount = &sandbox.workspace_mount;
    let cwd = mount.join("nested cwd ' $;");
    std::fs::create_dir_all(&cwd).expect("requested cwd");
    std::fs::create_dir_all(mount.join("tools")).expect("workspace tools");
    let tool = mount.join("tools/activation-probe");
    std::fs::write(&tool, "#!/bin/sh\nprintf '%s\\0' \"$SHELL_ACTIVATION_SDK\" \"$PWD\" \"$HOME\" \"$XDG_CONFIG_HOME\" \"$@\"\n")
        .expect("workspace tool");
    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755))
        .expect("executable workspace tool");
    std::fs::write(
        mount.join(".envrc"),
        "export SHELL_ACTIVATION_SDK='workspace SDK from enterShell'\nPATH_add \"$PWD/tools\"\n",
    )
    .expect("real shell activation");
    std::fs::create_dir_all(sandbox.home.join(".ssh")).expect("host SSH directory");
    let host_secret = sandbox.home.join(".ssh/id_ed25519");
    std::fs::write(&host_secret, "host-only credential").expect("host credential fixture");
    let host_direnv = sandbox.home.join(".config/direnv");
    std::fs::create_dir_all(&host_direnv).expect("host direnv config");
    std::fs::write(
        host_direnv.join("direnvrc"),
        "export HOST_DIRENV_SECRET=leaked\n",
    )
    .expect("host direnv startup fixture");
    let literal = "space ' \" $HOME $(touch injected) ; *\nsecond line";
    let script = r#"set -eu
test -z "${HOST_DIRENV_SECRET-}"
host_secret=$1
shift
activation-probe "$@"
if /bin/cat "$host_secret"; then
  printf 'host SSH credential was readable\n' >&2
  exit 91
fi
"#;
    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        &cwd,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            script.into(),
            "activation check".into(),
            host_secret.into_os_string(),
            literal.into(),
            "".into(),
            "--literal-option".into(),
        ],
    )
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "activation must run before the command; stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
    let expected = [
        "workspace SDK from enterShell".to_owned(),
        cwd.to_str().expect("cwd UTF-8").to_owned(),
        mount
            .join(".cowshed/home")
            .to_str()
            .expect("home UTF-8")
            .to_owned(),
        mount
            .join(".cowshed/config")
            .to_str()
            .expect("config UTF-8")
            .to_owned(),
        literal.to_owned(),
        String::new(),
        "--literal-option".to_owned(),
    ]
    .join("\0")
        + "\0";
    assert_eq!(stdout, expected.as_bytes());
    assert!(
        !cwd.join("injected").exists(),
        "argv must never be shell-interpolated"
    );
    assert!(
        !host_direnv.join("allow").exists(),
        "workspace approval must not write the host direnv trust store"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_shell_activation_failure_prevents_command_execution() {
    let root = scratch("shell-activation-failure");
    let sandbox = workspace(&root, 40_992);
    install_real_tool(&sandbox, "direnv");
    let mount = &sandbox.workspace_mount;
    std::fs::write(
        mount.join(".envrc"),
        "printf entered > activation-entered\nexit 42\n",
    )
    .expect("failing shell activation");
    let (exit, _, stderr) = run_in_sandbox(
        &sandbox,
        mount,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf ran > command-ran".into(),
        ],
    )
    .await;
    assert_eq!(
        std::fs::read(mount.join("activation-entered")).expect("activation actually ran"),
        b"entered"
    );
    assert_ne!(
        exit,
        ExitStatus::Exited { code: 0 },
        "failed activation must return a failed job; stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert!(
        !mount.join("command-ran").exists(),
        "a valid command must not run after failed activation"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_shell_activation_selects_nearest_workspace_envrc() {
    let root = scratch("shell-activation-nearest");
    let sandbox = workspace(&root, 41_008);
    install_real_tool(&sandbox, "direnv");
    let mount = &sandbox.workspace_mount;
    let nested = mount.join("nested project");
    let cwd = nested.join("working directory");
    std::fs::create_dir_all(&cwd).expect("nested cwd");
    std::fs::write(
        mount.join(".envrc"),
        "printf wrong > wrong-envrc-ran\nexport SHELL_ACTIVATION_SDK=wrong\n",
    )
    .expect("outer envrc");
    std::fs::write(
        nested.join(".envrc"),
        "export SHELL_ACTIVATION_SDK=nearest\n",
    )
    .expect("nearest envrc");
    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        &cwd,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf '%s' \"$SHELL_ACTIVATION_SDK\"".into(),
        ],
    )
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "nearest activation succeeds: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(stdout, b"nearest");
    assert!(
        !mount.join("wrong-envrc-ran").exists(),
        "a farther envrc must not run before or instead of the nearest one"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

/// A checkout whose toolchain comes only from its dev environment: the bootstrap PATH a job
/// starts from holds a different `cargo` that cannot read the project, and the checkout's
/// `.envrc` puts the one its jobs build with first. Discovery names the target directory only
/// if it asks Cargo after that activation, as a job of the workspace.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_cargo_discovery_asks_the_toolchain_the_dev_environment_provides() {
    use cowshed_core::capabilities::{BuildStatePath, JobCargo, discover_build_state};
    let root = scratch("cargo-discovery-toolchain");
    let sandbox = workspace(&root, 41_120);
    install_real_tool(&sandbox, "direnv");
    let mount = sandbox.workspace_mount.clone();
    let executable = |path: &Path, script: &str| {
        std::fs::write(path, script).expect("fixture executable");
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("executable fixture");
    };
    executable(
        &mount.join(".cowshed/bin/cargo"),
        "#!/bin/sh\necho 'the bootstrap cargo cannot read this project' >&2\nexit 101\n",
    );
    let real_cargo = std::env::split_paths(&std::env::var_os("PATH").expect("host PATH"))
        .map(|directory| directory.join("cargo"))
        .find(|candidate| candidate.is_file())
        .expect("the gate's own cargo is on PATH");
    std::fs::create_dir_all(mount.join("toolchain")).expect("dev environment toolchain");
    std::os::unix::fs::symlink(
        std::fs::canonicalize(real_cargo).expect("resolve the gate's cargo"),
        mount.join("toolchain/cargo"),
    )
    .expect("dev environment cargo");
    std::fs::write(mount.join(".envrc"), "PATH_add \"$PWD/toolchain\"\n").expect("envrc");
    std::fs::write(
        mount.join("Cargo.toml"),
        "[package]\nname = \"probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .expect("manifest");
    std::fs::create_dir_all(mount.join("src")).expect("sources");
    std::fs::write(mount.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").expect("library");
    for args in [&["init", "--quiet"][..], &["add", "Cargo.toml"]] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(&mount)
            .output_locked()
            .expect("git");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let runtime = tokio::runtime::Handle::current();
    let discovery = tokio::task::spawn_blocking(move || {
        let mut cargo = JobCargo::new(&sandbox, runtime);
        sandbox.with_detection_context(&sandbox.workspace_mount, |context| {
            discover_build_state(context, &mut cargo)
        })
    })
    .await
    .expect("discovery task")
    .expect("discovery");
    assert!(
        discovery.findings.is_empty(),
        "{}",
        discovery
            .findings
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        discovery.paths,
        [BuildStatePath::new("target", "target").expect("target path")]
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_shell_activation_does_not_authorize_or_load_an_envrc_outside_the_workspace()
 {
    let root = scratch("shell-activation-boundary");
    let sandbox = workspace(&root, 41_024);
    install_real_tool(&sandbox, "direnv");
    std::fs::write(
        root.join(".envrc"),
        "printf escaped > workspace/ancestor-activated\nexport SHELL_ACTIVATION_SDK=outside\n",
    )
    .expect("unrelated ancestor envrc");
    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        &sandbox.workspace_mount,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf '%s' \"${SHELL_ACTIVATION_SDK-unactivated}\"".into(),
        ],
    )
    .await;
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "an envrc outside the workspace is not an activation prerequisite: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(stdout, b"unactivated");
    assert!(
        !sandbox.workspace_mount.join("ancestor-activated").exists(),
        "ancestor shell code must never run"
    );
    assert!(
        !sandbox
            .workspace_mount
            .join(".cowshed/config/direnv/allow")
            .exists(),
        "an unrelated ancestor envrc must not be approved"
    );
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_native_file_watching_observes_allowed_updates_without_private_paths() {
    let root = scratch("native-file-watching");
    let mut sandbox = workspace(&root, 41_072);
    install_real_tool(&sandbox, "node");
    let modules = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR")
            .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs"),
    )
    .join("../../../../node_modules")
    .canonicalize()
    .expect("Nx test dependencies");
    sandbox.grants.read.push(modules.clone());
    // Bun may link the Nx package and its declared platform dependency into its host cache,
    // which only a Bun project's sandbox reads. Grant each resolved package, not the whole host
    // cache or user home: Nx's with the `node_modules` beside it, whose links name its
    // dependencies, and the native binding's.
    let nx = modules
        .join("nx")
        .canonicalize()
        .expect("canonical Nx package");
    sandbox
        .grants
        .read
        .push(nx.parent().expect("Nx's node_modules").to_path_buf());
    let resolved = std::process::Command::new("node")
        .args([
            "-p",
            "require.resolve(`@nx/nx-${process.platform}-${process.arch}`, { paths: [process.argv[1]] })",
        ])
        .arg(&nx).output_locked()
        .expect("resolve the native watcher dependency");
    assert!(
        resolved.status.success(),
        "{}",
        String::from_utf8_lossy(&resolved.stderr)
    );
    let binding = PathBuf::from(
        String::from_utf8(resolved.stdout)
            .expect("native binding path UTF-8")
            .trim(),
    );
    sandbox.grants.read.push(
        binding
            .parent()
            .expect("native binding package")
            .canonicalize()
            .expect("canonical native binding package"),
    );
    let mount = &sandbox.workspace_mount;
    let private = mount.join("private");
    std::fs::create_dir(&private).expect("private fixture directory");
    std::fs::write(private.join("secret.txt"), b"synthetic-before").expect("private fixture");
    std::fs::write(mount.join("own.txt"), b"before").expect("allowed fixture");
    sandbox.additional_denies.push(private.clone());
    let ready = mount.join("watch-ready");
    let script = mount.join("watch.cjs");
    std::fs::write(
        &script,
        r#"
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { Watcher } = require(process.argv[2]);
assert.throws(
  () => fs.readFileSync('private/secret.txt'),
  error => error.code === 'EPERM' || error.code === 'EACCES',
);
let sawAllowedUpdate = false;
let failure = null;
function collect(events) {
  for (const event of events) {
    if (event.path.startsWith('private/')) failure = new Error('private path exposed by watcher');
    if (event.path === 'own.txt' && fs.readFileSync('own.txt', 'utf8') === 'after') {
      sawAllowedUpdate = true;
    }
  }
}
const watcher = new Watcher(process.cwd(), [], false);
watcher.watch((error, events) => {
  if (error) failure = new Error(error);
  else collect(events);
});
fs.writeFileSync('watch-ready', '');
let polls = 0;
const timer = setInterval(async () => {
  collect(watcher.forceFlushPending());
  if (++polls === 30) {
    clearInterval(timer);
    await watcher.stop();
    assert.equal(failure, null);
    assert.ok(sawAllowedUpdate, 'the allowed source change must produce a notification');
  }
}, 100);
"#,
    )
    .expect("watcher probe");
    let publish = async {
        if tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !ready.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .is_err()
        {
            return false;
        }
        std::fs::write(mount.join("own.txt"), b"after").expect("publish allowed change");
        std::fs::write(private.join("secret.txt"), b"synthetic-after")
            .expect("publish private negative control");
        true
    };
    let ((exit, _, stderr), started) = tokio::join!(
        run_in_sandbox(
            &sandbox,
            mount,
            vec![
                "node".into(),
                script.into_os_string(),
                modules.join("nx/dist/src/native").into_os_string(),
            ],
        ),
        publish,
    );
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "watcher must report allowed changes without exposing denied paths: {}",
        String::from_utf8_lossy(&stderr),
    );
    assert!(started, "watcher must start before publishing changes");
    std::fs::remove_dir_all(root).expect("remove watcher fixture");
}

/// A workspace sandbox built by the production builder, [`workspace_sandbox`], for a mount the
/// fixture prepares the way `cowshed new` leaves one: private bin and token in place. `deny` is
/// the workspace's granted read+write deny, `repository_deny` main's `[sandbox] deny`.
#[expect(
    clippy::too_many_arguments,
    reason = "one fixture input per builder input"
)]
fn production_workspace(
    root: &Path,
    home: &Path,
    mount_root: &Path,
    main: &Path,
    mount: PathBuf,
    port_base: u16,
    deny: Vec<PathBuf>,
    repository_deny: &[PathBuf],
) -> SandboxConfig {
    std::fs::create_dir_all(mount.join(".cowshed/bin")).expect("private bin");
    std::fs::write(
        mount.join(WORKSPACE_TOKEN_PATH),
        WorkspaceToken::from_bytes([7; 32]).encode(),
    )
    .expect("workspace token");
    let exec_temp_dir = root.join(format!("tmp-{port_base}"));
    std::fs::create_dir_all(&exec_temp_dir).expect("exec temp dir");
    workspace_sandbox(WorkspaceSandbox {
        home,
        mount_root,
        project_root: &root.join("project"),
        main_mount: main,
        telemetry_root: &root.join("telemetry"),
        grants: &GrantSet {
            port_block: Some(PortBlock::new(port_base, 16).expect("port block")),
            deny,
            ..GrantSet::default()
        },
        repository_deny,
        repository_caches: &[],
        git_worktree_repository: None,
        build_volume_mount: None,
        workspace_mount: mount,
        exec_temp_dir,
    })
    .expect("production workspace sandbox")
}

/// What `cat` of each path printed, or why it was refused.
async fn read_outcomes(sandbox: &SandboxConfig, paths: &[&Path]) -> Vec<(PathBuf, String)> {
    let mut outcomes = Vec::new();
    for path in paths {
        let (exit, stdout, stderr) = run_in_sandbox(
            sandbox,
            &sandbox.workspace_mount,
            vec!["/bin/cat".into(), path.as_os_str().to_owned()],
        )
        .await;
        let outcome = match exit {
            ExitStatus::Exited { code: 0 } => {
                format!("read {:?}", String::from_utf8_lossy(&stdout))
            }
            _ => String::from_utf8_lossy(&stderr).trim().to_owned(),
        };
        outcomes.push((path.to_path_buf(), outcome));
    }
    outcomes
}

/// The read leak, closed: a workspace job could `cat` main's checkout — mounted at the
/// operator's own path, outside the mount root the sibling deny covers, `.cowshed/token`
/// included — and anything under HOME, because the profile's broad `file-read-data` reached
/// both. Main's checkout is now denied by name like a sibling and HOME reads deny by default.
/// Main itself still reads its own checkout. The mount root sits under HOME as on a host; main
/// sits outside HOME, so its deny is proven on its own rather than by the HOME deny.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_workspace_reads_neither_main_nor_home_outside_its_allowlist() {
    let root = scratch("read-policy");
    let home = root.join("home");
    let mount_root = home.join("Dev/.cowshed");
    let main = root.join("checkouts/widget");
    let raven = mount_root.join("acme/widget/raven");
    let agent_db = home.join(".omp/agent/agent.db");
    let application_support = home.join("Library/Application Support/Fixture/state.json");
    let main_untracked = main.join("untracked.txt");
    for (path, contents) in [
        (&agent_db, "agent-db-sentinel"),
        (&application_support, "application-support-sentinel"),
        (&main_untracked, "main-untracked-sentinel"),
    ] {
        std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture dir");
        std::fs::write(path, contents).expect("fixture file");
    }
    let main_sandbox = production_workspace(
        &root,
        &home,
        &mount_root,
        &main,
        main.clone(),
        42_496,
        Vec::new(),
        &[],
    );
    let sandbox = production_workspace(
        &root,
        &home,
        &mount_root,
        &main,
        raven.clone(),
        42_512,
        Vec::new(),
        &[],
    );
    let own = raven.join("own.txt");
    std::fs::write(&own, "own-sentinel").expect("own fixture");
    let main_token = main.join(WORKSPACE_TOKEN_PATH);

    let leaks: Vec<_> = read_outcomes(
        &sandbox,
        &[
            &main_token,
            &main_untracked,
            &agent_db,
            &application_support,
        ],
    )
    .await
    .into_iter()
    .filter(|(_, outcome)| !outcome.ends_with("Operation not permitted"))
    .collect();
    let own_read = read_outcomes(&sandbox, &[&own]).await;
    let main_reads_itself = read_outcomes(&main_sandbox, &[&main_untracked]).await;
    std::fs::remove_dir_all(&root).expect("remove read-policy fixture");

    assert!(
        leaks.is_empty(),
        "a workspace job read outside its allowlist: {leaks:#?}"
    );
    assert_eq!(
        own_read[0].1, "read \"own-sentinel\"",
        "own mount stays readable"
    );
    assert_eq!(
        main_reads_itself[0].1, "read \"main-untracked-sentinel\"",
        "main's own jobs keep reading main's checkout"
    );
}

/// The HOME read deny leaves a real toolchain working, against the host's real HOME: shell
/// activation loads the workspace's own `.envrc-local`, cargo builds and runs a crate (through
/// the host's shared cargo home and sccache when it has them), and bun runs a module graph. The
/// workspace is a direnv and cargo project, so its HOME reads are exactly what those detectors
/// contribute; a tool that needed anything under HOME beyond them would fail here.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_real_build_runs_under_the_home_read_deny() {
    fn host_tool_bin(name: &str) -> PathBuf {
        let installed = std::env::split_paths(&std::env::var_os("PATH").expect("host PATH"))
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
            .unwrap_or_else(|| panic!("required build tool `{name}` is not on PATH"));
        std::fs::canonicalize(installed)
            .expect("resolve build tool")
            .parent()
            .expect("tool directory")
            .to_path_buf()
    }
    let home = std::fs::canonicalize(std::env::var_os("HOME").expect("host HOME"))
        .expect("canonical host HOME");
    let root = scratch("read-policy-build");
    let main = root.join("main");
    std::fs::create_dir_all(&main).expect("main checkout");
    let mount_root = root.join("mounts");
    let mut sandbox = production_workspace(
        &root,
        &home,
        &mount_root,
        &main,
        mount_root.join("acme/widget/raven"),
        42_528,
        Vec::new(),
        &[],
    );
    install_real_tool(&sandbox, "direnv");
    let mount = &sandbox.workspace_mount;
    std::fs::write(
        mount.join(".envrc"),
        "source_env_if_exists \"$PWD/.envrc-local\"\n",
    )
    .expect("workspace envrc");
    std::fs::write(
        mount.join(".envrc-local"),
        "export COWSHED_LOCAL_OVERRIDE=workspace\n",
    )
    .expect("workspace envrc-local");
    std::fs::write(
        mount.join("Cargo.toml"),
        "[package]\nname = \"probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
    )
    .expect("probe manifest");
    std::fs::create_dir_all(mount.join("src")).expect("probe crate");
    std::fs::write(
        mount.join("src/main.rs"),
        "fn main() { println!(\"cargo-built\"); }\n",
    )
    .expect("probe source");
    std::fs::write(
        mount.join("lib.ts"),
        "export const greeting: string = 'bun-ran';\n",
    )
    .expect("bun module");
    std::fs::write(
        mount.join("main.ts"),
        "import { greeting } from './lib.ts';\nconsole.log(greeting);\n",
    )
    .expect("bun entry");
    sandbox
        .configure_capabilities()
        .expect("detect the direnv and cargo capabilities");
    let mount = &sandbox.workspace_mount;
    // What the supervisor resolves through HOME before any shell exists — bootstrap tools
    // through the user's Nix profile links, `RUSTC_WRAPPER` through the sccache GC root — must
    // resolve under the same deny, to what the unsandboxed host resolves. A host without one
    // of them has nothing to resolve there.
    let resolved: Vec<(PathBuf, PathBuf)> = [
        home.join(".nix-profile"),
        home.join(".local/state/nix/profile"),
        cowshed_core::capabilities::sccache::gc_root(&home),
    ]
    .into_iter()
    .filter_map(|link| {
        std::fs::canonicalize(&link)
            .ok()
            .map(|target| (link, target))
    })
    .collect();
    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        mount,
        [
            "/bin/sh".into(),
            "-c".into(),
            r#"set -e; PATH="$1:$2:$PATH"; export PATH; shift 2
printf '%s\n' "$COWSHED_LOCAL_OVERRIDE"
cargo build --offline --quiet && ./target/debug/probe
bun run main.ts
for link in "$@"; do /bin/realpath "$link"; done"#
                .into(),
            "build".into(),
            host_tool_bin("cargo").into_os_string(),
            host_tool_bin("bun").into_os_string(),
        ]
        .into_iter()
        .chain(resolved.iter().map(|(link, _)| link.as_os_str().to_owned()))
        .collect(),
    )
    .await;
    std::fs::remove_dir_all(&root).expect("remove build fixture");
    let mut expected = "workspace\ncargo-built\nbun-ran\n".to_owned();
    for (_, target) in &resolved {
        expected.push_str(&format!("{}\n", target.display()));
    }
    assert_eq!(
        (exit, String::from_utf8_lossy(&stdout).into_owned()),
        (ExitStatus::Exited { code: 0 }, expected),
        "stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
}

/// A workspace-relative deny hides a path inside the job's own mount: reads and writes beneath
/// it fail, and the job can neither rename the path away nor replace it. The deny arrives both
/// ways it can — a `cowshed grant --deny` and main's `[sandbox] deny` — and the rest of the
/// mount stays readable.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_workspace_relative_deny_hides_reads_and_writes() {
    let root = scratch("relative-deny");
    let home = root.join("home");
    let mount_root = home.join("Dev/.cowshed");
    let main = root.join("checkouts/widget");
    let raven = mount_root.join("acme/widget/raven");
    let sandbox = production_workspace(
        &root,
        &home,
        &mount_root,
        &main,
        raven.clone(),
        42_544,
        vec![PathBuf::from(".runtime")],
        &[PathBuf::from("secrets/vault")],
    );
    let granted = raven.join(".runtime/token");
    let declared = raven.join("secrets/vault/key");
    let open = raven.join("secrets/open.txt");
    for (path, contents) in [
        (&granted, "runtime-sentinel"),
        (&declared, "vault-sentinel"),
        (&open, "open-sentinel"),
    ] {
        std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture dir");
        std::fs::write(path, contents).expect("fixture file");
    }

    let reads = read_outcomes(&sandbox, &[&granted, &declared, &open]).await;
    // Each attempt runs on its own and names itself only if it succeeded.
    let (write_exit, succeeded, _) = run_in_sandbox(
        &sandbox,
        &raven,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            r#"(echo planted > .runtime/planted) 2>/dev/null && echo write
mv .runtime moved 2>/dev/null && echo rename-granted
mv secrets/vault vault-moved 2>/dev/null && echo rename-declared
exit 0"#
                .into(),
        ],
    )
    .await;
    let planted = raven.join(".runtime/planted").exists();
    let runtime_kept = granted.is_file();
    let vault_kept = declared.is_file();
    std::fs::remove_dir_all(&root).expect("remove relative-deny fixture");

    assert_eq!(
        reads
            .iter()
            .map(|(_, outcome)| outcome.ends_with("Operation not permitted"))
            .collect::<Vec<_>>(),
        [true, true, false],
        "{reads:#?}"
    );
    assert_eq!(reads[2].1, "read \"open-sentinel\"");
    assert_eq!(write_exit, ExitStatus::Exited { code: 0 });
    assert_eq!(
        String::from_utf8_lossy(&succeeded),
        "",
        "no write, create or rename may succeed beneath a denied path"
    );
    assert!(!planted, "nothing may be created beneath a denied path");
    assert!(
        runtime_kept && vault_kept,
        "denied paths stay where they are"
    );
}

/// A repository's `.envrc` may run `bun` before any shell it evaluates puts bun on PATH — a
/// managed shell's own bootstrap script does. A Bun project's sandbox therefore reaches the
/// host's bun by name from its first activation step, while the host's install directory stays
/// beneath the HOME read deny.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_bun_project_runs_bun_before_its_envrc_evaluates() {
    let root = scratch("bun-before-envrc");
    let home = root.join("home");
    let mount_root = home.join("Dev/.cowshed");
    let main = root.join("checkouts/widget");
    let raven = mount_root.join("acme/widget/raven");
    let bun = home.join(".bun/bin/bun");
    std::fs::create_dir_all(bun.parent().expect("bun bin")).expect("bun install");
    std::fs::write(&bun, "#!/bin/sh\nprintf 'fixture-bun %s' \"$*\"\n").expect("bun program");
    std::fs::set_permissions(&bun, std::fs::Permissions::from_mode(0o755)).expect("executable");
    std::fs::create_dir_all(&main).expect("main checkout");
    std::fs::create_dir_all(raven.join(".cowshed/bin")).expect("private bin");
    for (file, contents) in [
        ("package.json", "{}\n"),
        ("bun.lock", "{}\n"),
        (".envrc", "export BUN_BEFORE_SHELL=\"$(bun --version)\"\n"),
    ] {
        std::fs::write(raven.join(file), contents).expect("bun project fixture");
    }
    let direnv = std::env::split_paths(&std::env::var_os("PATH").expect("host PATH"))
        .map(|directory| directory.join("direnv"))
        .find(|candidate| candidate.is_file())
        .expect("required runtime tool `direnv` is not on PATH");
    std::os::unix::fs::symlink(
        std::fs::canonicalize(direnv).expect("resolve direnv"),
        raven.join(".cowshed/bin/direnv"),
    )
    .expect("direnv in the workspace bin");
    let sandbox = production_workspace(
        &root,
        &home,
        &mount_root,
        &main,
        raven.clone(),
        42_560,
        Vec::new(),
        &[],
    );

    let (exit, stdout, stderr) = run_in_sandbox(
        &sandbox,
        &raven,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf '%s' \"$BUN_BEFORE_SHELL\"".into(),
        ],
    )
    .await;
    std::fs::remove_dir_all(&root).expect("remove bun-before-envrc fixture");
    assert_eq!(
        exit,
        ExitStatus::Exited { code: 0 },
        "activation runs bun: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(String::from_utf8_lossy(&stdout), "fixture-bun --version");
}

/// Nested modules still need the shared read-at-build caches (15_capabilities.md), which stay at
/// the tools' own defaults in the host HOME (03_caches.md). Each operation runs independently so
/// a denied Go cache does not conceal Cargo or Bun failures.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_nested_go_cargo_fetch_and_bun_install_write_shared_caches() {
    let root = scratch("shared-tool-caches");
    let mut sandbox = workspace(&root, 42_592);
    std::fs::create_dir_all(sandbox.home.join(".cargo")).unwrap();
    for name in cowshed_core::capabilities::cargo::STATE_FILES {
        std::fs::write(sandbox.home.join(".cargo").join(name), "").unwrap();
    }
    for tool in ["go", "cargo", "rustc", "bun", "git"] {
        install_real_tool(&sandbox, tool);
    }
    let mount = &sandbox.workspace_mount;
    let module = mount.join("packages/go-probe");
    std::fs::create_dir_all(&module).expect("nested module");
    std::fs::write(
        module.join("go.mod"),
        "module example.com/cache-probe\n\ngo 1.24\n",
    )
    .expect("Go manifest");
    std::fs::write(
        module.join("probe.go"),
        "package probe\nfunc Value() int { return 7 }\n",
    )
    .expect("Go source");
    for args in [
        vec!["init", "--quiet"],
        vec!["add", "packages/go-probe/go.mod"],
    ] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(mount)
            .output_locked()
            .expect("track nested module");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let dependency = mount.join("fetch-dependency");
    std::fs::create_dir_all(dependency.join("src")).expect("git dependency");
    std::fs::write(
        dependency.join("Cargo.toml"),
        "[package]\nname = \"fetch-probe-dependency\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        dependency.join("src/lib.rs"),
        "pub fn value() -> u8 { 7 }\n",
    )
    .unwrap();
    for args in [
        vec!["init", "--quiet"],
        vec!["add", "Cargo.toml", "src/lib.rs"],
        vec![
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.com",
            "commit",
            "--quiet",
            "-m",
            "dependency",
        ],
    ] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(&dependency)
            .output_locked()
            .expect("prepare git dependency");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    std::fs::write(mount.join("Cargo.toml"), format!(
        "[package]\nname = \"fetch-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[dependencies]\nfetch-probe-dependency = {{ git = \"file://{}\" }}\n", dependency.display())).unwrap();
    std::fs::create_dir_all(mount.join("src")).unwrap();
    std::fs::write(
        mount.join("src/lib.rs"),
        "pub fn value() -> u8 { fetch_probe_dependency::value() }\n",
    )
    .unwrap();
    let package = mount.join("package");
    std::fs::create_dir_all(&package).unwrap();
    std::fs::write(
        package.join("package.json"),
        r#"{"name":"install-probe-dependency","version":"1.0.0","main":"index.js"}"#,
    )
    .unwrap();
    std::fs::write(package.join("index.js"), "module.exports = 7;\n").unwrap();
    let packed = std::process::Command::new("/usr/bin/tar")
        .args(["-czf", "dependency.tgz", "package"])
        .current_dir(mount)
        .output_locked()
        .expect("pack Bun dependency");
    assert!(
        packed.status.success(),
        "{}",
        String::from_utf8_lossy(&packed.stderr)
    );
    std::fs::write(
        mount.join("package.json"),
        r#"{"private":true,"dependencies":{"install-probe-dependency":"file:./dependency.tgz"}}"#,
    )
    .unwrap();
    // Let Bun name its real lockfile before detection, without warming the shared cache
    // the sandboxed install must use.
    let locked = std::process::Command::new("bun")
        .args([
            "install",
            "--lockfile-only",
            "--ignore-scripts",
            "--no-progress",
        ])
        .current_dir(mount)
        .env("HOME", &sandbox.home)
        .env("BUN_INSTALL_CACHE_DIR", root.join("lockfile-cache"))
        .output_locked()
        .expect("resolve Bun lockfile");
    assert!(
        locked.status.success(),
        "Bun lockfile: {}",
        String::from_utf8_lossy(&locked.stderr)
    );
    sandbox
        .configure_capabilities()
        .expect("detect tool conventions");
    // The supervisor creates every shared cache before a child runs: a child granted writes
    // inside one cannot create its parent.
    for cache in &sandbox.capabilities.contribution.shared_caches {
        std::fs::create_dir_all(&cache.path).expect("shared cache directory");
    }
    let mut outcomes = Vec::new();
    for (tool, script) in [
        (
            "go",
            r#"set -eu
cache="$1/Library/Caches/go-build"
printf probe > "$cache/cowshed-probe-$$"
rm "$cache/cowshed-probe-$$"
test "$GOCACHE" = "$cache"
test "$GOMODCACHE" = "$1/go/pkg/mod"
cd packages/go-probe
go build ./...
"#,
        ),
        (
            "cargo",
            r#"set -eu
test "$CARGO_HOME" = "$1/.cargo"
cargo fetch
"#,
        ),
        (
            "bun",
            r#"set -eu
test "$BUN_INSTALL_CACHE_DIR" = "$1/.bun/install/cache"
mkdir "$BUN_INSTALL_CACHE_DIR/cowshed-probe-$$"
rmdir "$BUN_INSTALL_CACHE_DIR/cowshed-probe-$$"
bun install --ignore-scripts --no-progress
bun -e 'if (require("install-probe-dependency") !== 7) process.exit(1)'
"#,
        ),
    ] {
        let result = run_in_sandbox(
            &sandbox,
            &sandbox.workspace_mount,
            vec![
                "/bin/sh".into(),
                "-c".into(),
                script.into(),
                tool.into(),
                sandbox.home.as_os_str().to_owned(),
            ],
        )
        .await;
        eprintln!(
            "{tool} cache operation: {:?}\nstdout: {}\nstderr: {}",
            result.0,
            String::from_utf8_lossy(&result.1),
            String::from_utf8_lossy(&result.2)
        );
        outcomes.push((tool, result));
    }
    std::fs::remove_dir_all(&root).expect("remove cache fixture");
    for (tool, (exit, stdout, stderr)) in outcomes {
        assert_eq!(
            exit,
            ExitStatus::Exited { code: 0 },
            "{tool} must write its shared cache from the sandbox\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
    }
}
