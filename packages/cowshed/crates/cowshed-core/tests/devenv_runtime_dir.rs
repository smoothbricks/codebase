//! Real shell activation through SystemSpawnSink and the executed-child Seatbelt profile.
//! Minimal .envrc fixtures exercise exports, executable lookup, private authority, and the
//! runtime/temp directories without Nix evaluation or a stand-in development environment.

#![cfg(target_os = "macos")]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use cowshed_core::api::{ExitStatus, JobId};
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor::{
    ProcessEvent, ProcessSpawnRequest, SpawnSink, SystemSpawnSink, WorkspaceAuthoritySnapshot,
    sandbox_runtime_dir, sandbox_runtime_link,
};
use cowshed_core::sandbox::{
    RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, seatbelt_profile,
};
use cowshed_core::storage::job_artifact::StreamKind;
use cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH;
use cowshed_gateway_types::WorkspaceToken;

use tokio::sync::mpsc;
/// `sun_path` on macOS is 104 bytes; devenv keeps its default runtime base short for exactly
/// this reason (`resolve_runtime_dir`, devenv-core `paths.rs`). A base that leaves no room for
/// the `devenv-<7 hex>` component plus a socket name is a base devenv cannot use.
const SUN_PATH_BYTES: usize = 104;
const LONGEST_RUNTIME_SUFFIX: &str = "/devenv-1234567/x.sock";
/// `/Users/<user>/Dev/.cowshed/<owner>/<repo>/<workspace>` at the lengths this host actually
/// has (`/Users/danny/Dev/.cowshed/axe-scale/minigraf/minigraf-query-deps` is 66 bytes).
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

fn workspace(root: &Path, port_base: u16) -> SandboxConfig {
    let mount = root.join("workspace");
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
        mount_root: root.to_path_buf(),
        workspace_mount: mount,
        exec_temp_dir,
        port_block: PortBlock::new(port_base, 16).expect("port block"),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        shed_links: Vec::new(),
        git_worktree_repository: None,
    }
}

#[tokio::test]
async fn shell_activation_owns_a_runtime_directory_the_profile_lets_it_write() {
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

    // The child sees the short `/tmp/cs-<port>` link - the `sun_path` budget - and it resolves
    // onto the shed's own runtime directory, which is what the profile grants.
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
        .expect("required runtime tool is installed on PATH");
    let installed = std::fs::canonicalize(installed).expect("resolve runtime tool");
    std::os::unix::fs::symlink(
        installed,
        sandbox.workspace_mount.join(".cowshed/bin").join(name),
    )
    .expect("make the installed runtime tool available in the sandbox");
}

#[tokio::test]
async fn nx_runtime_directory_supports_real_unix_socket_roundtrips() {
    let root = scratch("nx-socket");
    let sandbox = workspace(&root, 41_056);
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
        "private Unix socket roundtrip failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert_eq!(stdout, b"nx-private-socket");
    std::fs::remove_dir_all(root).expect("remove test workspace");
}

#[tokio::test]
async fn proxy_aware_client_reaches_an_allocated_loopback_service() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let root = scratch("loopback-http");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("local HTTP listener");
    let port = listener.local_addr().expect("listener address").port();
    let sandbox = workspace(&root, port.checked_sub(15).expect("ephemeral port"));
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
async fn proxy_bypass_does_not_admit_unallocated_loopback_ports() {
    let root = scratch("loopback-denial");
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("unallocated HTTP listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let port = listener.local_addr().expect("listener address").port();
    let base = if (40_000..40_016).contains(&port) {
        41_000
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
        argv,
        cwd: cwd.to_path_buf(),
        env: BTreeMap::new(),
        devenv_dir: None,
        trusted_supervisor_profile: seatbelt_profile(
            sandbox,
            SandboxProfileRole::TrustedSupervisor,
        )
        .expect("supervisor profile"),
        executed_child_profile: seatbelt_profile(sandbox, SandboxProfileRole::ExecutedChild)
            .expect("child profile"),
        sandbox: sandbox.clone(),
    }
}

async fn run_in_sandbox(
    sandbox: &SandboxConfig,
    cwd: &Path,
    argv: Vec<OsString>,
) -> (ExitStatus, Vec<u8>, Vec<u8>) {
    let request = spawn_request(sandbox, cwd, argv);
    let (events, mut receiver) = mpsc::channel(16);
    let mut process = SystemSpawnSink
        .spawn(request, events)
        .await
        .expect("spawn through the real sandbox");
    process.close_stdin().expect("close unused stdin");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit = None;
    let mut eof = 0;
    while exit.is_none() || eof < 2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(30), receiver.recv())
            .await
            .expect("sandboxed command terminates")
            .expect("process event channel remains open until exit and EOF");
        match event {
            ProcessEvent::Output { stream, bytes, .. } => match stream {
                StreamKind::Stdout => stdout.extend_from_slice(&bytes),
                StreamKind::Stderr => stderr.extend_from_slice(&bytes),
            },
            ProcessEvent::OutputEof { .. } => eof += 1,
            ProcessEvent::Exited { exit: status, .. } => exit = Some(status),
            ProcessEvent::WaitFailed { error, .. } => panic!("child wait failed: {error}"),
            _ => {}
        }
    }
    (exit.expect("observed child exit"), stdout, stderr)
}

#[tokio::test]
async fn shell_activation_preserves_environment_path_cwd_argv_and_private_home() {
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
async fn shell_activation_failure_prevents_command_execution() {
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
async fn shell_activation_selects_nearest_workspace_envrc() {
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

#[tokio::test]
async fn shell_activation_does_not_authorize_or_load_an_envrc_outside_the_workspace() {
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
async fn configured_missing_devenv_fails_before_command_execution() {
    let root = scratch("shell-activation-missing");
    let sandbox = workspace(&root, 41_040);
    std::fs::write(
        sandbox.workspace_mount.join(".cowshed.toml"),
        "[devenv]\ndir = \"tooling/devenv\"\n",
    )
    .expect("configured missing devenv");
    let request = spawn_request(
        &sandbox,
        &sandbox.workspace_mount,
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf ran > command-ran".into(),
        ],
    );
    let (events, _) = mpsc::channel(16);
    let error = match SystemSpawnSink.spawn(request, events).await {
        Ok(mut process) => {
            process
                .signal_process_tree(cowshed_core::runtime::supervisor::ProcessSignal::Kill)
                .expect("stop incorrectly admitted command");
            panic!("missing configured devenv must reject the spawn");
        }
        Err(error) => error,
    };
    assert_eq!(
        error.code,
        cowshed_core::error::ErrorCode::EnvironmentMissing
    );
    assert!(!sandbox.workspace_mount.join("command-ran").exists());
    std::fs::remove_dir_all(root).expect("remove test workspace");
}
