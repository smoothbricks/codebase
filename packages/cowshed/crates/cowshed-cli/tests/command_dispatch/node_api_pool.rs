//! What an N-API embedder's exec reaches (07_api.md, "CLI and API exec parity"): the workspace
//! supervisor's warm shell pool, as the CLI's exec does (11_shell.md, "One spare and a SIEVE
//! cache").

use super::*;
use cowshed_core::api::server::{ConnectionAuthority, serve_controller_connection};
use cowshed_core::api::{ExitStatus, JobInfo};
use cowshed_core::runtime::shell_pool::PoolCounts;
use cowshed_core::runtime::{ProjectRuntime, supervisor_socket};

#[path = "../../../cowshed-core/tests/support/napi_client.rs"]
mod napi_client;

/// Two Node-API embedders, each on a coordinator connection of its own to the project's
/// controller, run `/usr/bin/true` in main through `worker.exec`, one after the other. The second
/// is answered by a warm host, and main's supervisor's own pool counts (`shellPool`, 11_shell.md,
/// "One spare and a SIEVE cache") say so, not the clock: between the two execs idle plus spare
/// grew by exactly one and no acquire activated a host of its own. An exec that bypassed the pool
/// grows neither; one that found it cold grows `activated`.
///
/// The controller is the project's router served on a socketpair by
/// `serve_controller_connection`, as `cowshed controller` serves it, over a directory store whose
/// supervisors run in this process with the test's `cowshed` as their registered shell host, as
/// the `cowshed` binary registers itself. A directory store's workspace is no mount, so the
/// resident path (06_cli.md, "Resident workspaces") declines it and the controller admits the
/// exec itself; the pool it reaches is the same supervisor's.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_node_api_exec_reaches_the_warm_pool() {
    cowshed_core::runtime::shell_host::register(
        cowshed_core::runtime::shell_host::ShellHostProgram::with_arguments(
            cowshed(),
            vec![cowshed_core::runtime::shell_host::SHELL_HOST_ARGUMENT.into()],
        )
        .expect("the cowshed binary as host program"),
    );
    // Only a workspace with an `.envrc` runs its commands in warm hosts.
    let mut fixture = Fixture::directory();
    fs::write(
        fixture.checkout.join(".envrc"),
        "export FROM_ENVRC=loaded\n",
    )
    .expect("main's shell");
    git(&fixture.checkout, &["add", ".envrc"]);
    git(&fixture.checkout, &["commit", "-q", "-m", "main's shell"]);
    install_in_fixture_profile(&fixture, "direnv", &path_program("direnv"));
    serve_project(&mut fixture, &[])
        .await
        .shutdown()
        .await
        .expect("stop the CLI bridge so the embedders' controller is the only one");
    let main = WorkspaceName::main();
    let runtime = ProjectRuntime::open_directory_existing_at(
        &fixture.checkout,
        RecoveryScope::Workspaces([main.clone()].into()),
        fixture.storage.clone(),
        cowshed_cli::gateway_service::runtime_control,
    )
    .await
    .expect("reopen the project runtime");

    let first = embedder_exec(&runtime, &fixture.checkout).await;
    let warmed = pool_counts(&runtime, &main).await;
    let second = embedder_exec(&runtime, &fixture.checkout).await;
    let after = pool_counts(&runtime, &main).await;
    for ended in [&first, &second] {
        assert_eq!(
            ended.exit,
            Some(ExitStatus::Exited { code: 0 }),
            "{ended:?}"
        );
    }
    assert_eq!(
        after.idle + after.spare,
        warmed.idle + warmed.spare + 1,
        "the second exec took an idle host or the spare: {warmed:?} then {after:?}"
    );
    assert_eq!(
        after.activated, warmed.activated,
        "the second exec activated no host of its own: {warmed:?} then {after:?}"
    );
    runtime.shutdown().await.expect("stop the runtime");
    fixture.stop_gateway().await;
}

/// One embedder: Node, on a new coordinator connection to `runtime`'s controller, admits
/// `/usr/bin/true` in main through `worker.exec` and answers the job once it ended.
async fn embedder_exec(runtime: &ProjectRuntime, checkout: &Path) -> JobInfo {
    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let connection = tokio::spawn(serve_controller_connection(
        server.into(),
        ConnectionAuthority::Coordinator {
            repo_id: runtime.descriptor().repo_id.clone(),
        },
        runtime.router(),
    ));
    let ended = napi_client::answer(
        client.into(),
        checkout,
        "const worker = await coordinator.worker('main');\n\
         const job = await worker.exec({ argv: ['/usr/bin/true'] });\n\
         return await job.wait();",
    )
    .await;
    connection
        .await
        .expect("the connection task")
        .expect("the controller ends the connection its embedder closed");
    ended
}

/// How `workspace`'s supervisor answered its pool acquires so far, read over its socket.
async fn pool_counts(runtime: &ProjectRuntime, workspace: &WorkspaceName) -> PoolCounts {
    let descriptor = runtime.descriptor();
    let socket =
        supervisor_socket::socket_path(descriptor.storage.store(), &descriptor.repo_id, workspace);
    let hello = supervisor_socket::hello(&socket)
        .await
        .expect("the workspace's supervisor answers");
    supervisor_socket::connect(socket, hello.authority)
        .shell_pool()
        .await
        .expect("the workspace's supervisor runs warm shells")
}

//#region smoo/cowshed!n/embedder-awaits-other-build
/// Names the socket [`serves_as_a_supervisor_of_another_build`] binds; unset, it does nothing.
const OTHER_BUILD_SOCKET: &str = "COWSHED_TEST_OTHER_BUILD_SOCKET";

/// Not a test: the process a test starts to serve a workspace's socket as a supervisor of another
/// build that drains. It binds `OTHER_BUILD_SOCKET`, says `ready` on stdout, then answers a
/// hello with a build that is no build's and its pid, and a drain with its pid, and names each request
/// on stdout, until it is killed.
#[test]
#[ignore = "a helper process the other-build tests start"]
fn serves_as_a_supervisor_of_another_build() {
    use std::io::{Read as _, Write as _};
    let Some(socket) = std::env::var_os(OTHER_BUILD_SOCKET) else {
        return;
    };
    let listener = std::os::unix::net::UnixListener::bind(socket).expect("bind");
    let mut stdout = std::io::stdout();
    writeln!(stdout, "ready").expect("say ready");
    for stream in listener.incoming() {
        let mut stream = stream.expect("accept");
        let mut length = [0_u8; 4];
        // A readiness probe connects and sends nothing.
        if stream.read_exact(&mut length).is_err() {
            continue;
        }
        let mut request = vec![0_u8; usize::try_from(u32::from_be_bytes(length)).expect("fits")];
        stream.read_exact(&mut request).expect("request");
        let named = serde_json::from_slice::<serde_json::Value>(&request)
            .ok()
            .and_then(|request| request.as_str().map(str::to_owned))
            .unwrap_or_else(|| "other".to_owned());
        let pid = std::process::id();
        let value = match named.as_str() {
            "drain" => serde_json::json!(pid),
            _ => serde_json::json!({"build": "another build", "pid": pid}),
        };
        let answer = serde_json::to_vec(&serde_json::json!({"ok": {"value": value, "bytes": 0}}))
            .expect("answer");
        let framed = u32::try_from(answer.len()).expect("fits").to_be_bytes();
        stream.write_all(&framed).expect("answer length");
        stream.write_all(&answer).expect("answer");
        writeln!(stdout, "{named}").expect("name the request");
    }
}

/// `/usr/bin/true` in main through `worker.exec`, on the coordinator connection whose client end
/// is `client`, answered once it ended.
async fn exec_true(client: OwnedFd, checkout: &Path) -> cowshed_core::Result<JobInfo> {
    let (cowshed, token) = cowshed_core::Cowshed::connect(client).await?;
    let project = cowshed.open(checkout).await?;
    let coordinator = cowshed.coordinator(&project, token)?;
    let worker = coordinator.worker("main").await?;
    let job = worker
        .exec(cowshed_core::api::ExecRequest {
            command: cowshed_core::api::dto::ExecCommand::Argv(vec![
                cowshed_core::api::CommandArg::from("/usr/bin/true"),
            ]),
            cwd: None,
            mode: cowshed_core::api::RunSandboxMode::ReadWrite,
            env: std::collections::HashMap::new(),
            trace: None,
            stdin: cowshed_core::api::StdinSource::Empty,
            stdout_copy: None,
            stderr_copy: None,
            admission_key: None,
            class: cowshed_core::api::ExecClass::Background,
            want: None,
            hold: false,
            scope: cowshed_core::api::ExecScope::Workspace,
            record: cowshed_core::api::JobRecord::Cowshed,
        })
        .await?;
    job.wait().await
}

/// 11_shell.md "Draining a supervisor of another build": after an install, a workspace's
/// supervisor of the replaced build drains. A CLI verb that meets it is refused at once, naming its
/// pid, so its operator chooses; an embedder's `cowshed controller`, which nobody stands behind,
/// waits for it to retire as a land step does, and its exec then runs on a supervisor of this
/// build. The drained supervisor here is a process of its own whose exit ends the wait.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_embedder_waits_out_a_supervisor_of_another_build_a_verb_is_refused_by()
{
    use cowshed_os::test_backstop::AsyncChildBackstop as _;
    use tokio::io::AsyncBufReadExt as _;
    let mut fixture = Fixture::directory();
    serve_project(&mut fixture, &[])
        .await
        .shutdown()
        .await
        .expect("stop the CLI bridge so the test's controllers are the only ones");
    let main = WorkspaceName::main();
    let scope = || RecoveryScope::Workspaces([main.clone()].into());
    let verb = ProjectRuntime::open_directory_existing_at(
        &fixture.checkout,
        scope(),
        fixture.storage.clone(),
        cowshed_cli::gateway_service::runtime_control,
    )
    .await
    .expect("a CLI verb's runtime");
    let descriptor = verb.descriptor();
    let socket =
        supervisor_socket::socket_path(descriptor.storage.store(), &descriptor.repo_id, &main);
    let mut other = tokio::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "macos::node_api_pool::serves_as_a_supervisor_of_another_build",
            "--ignored",
            "--nocapture",
        ])
        .env(OTHER_BUILD_SOCKET, &socket)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the supervisor of another build");
    let pid = other.id().expect("it runs");
    let mut said = tokio::io::BufReader::new(other.stdout.take().expect("its stdout")).lines();
    // libtest prints its own header lines first; the helper's own word is the signal.
    while said
        .next_line()
        .await
        .expect("its stdout")
        .expect("it serves")
        != "ready"
    {}

    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    tokio::spawn(serve_controller_connection(
        server.into(),
        ConnectionAuthority::Coordinator {
            repo_id: verb.descriptor().repo_id.clone(),
        },
        verb.router(),
    ));
    let refused = exec_true(client.into(), &fixture.checkout)
        .await
        .expect_err("a CLI verb is refused by a supervisor of another build");
    assert_eq!(refused.code, ErrorCode::Conflict, "{refused:?}");
    assert!(
        refused.message.contains(&format!("pid {pid}")),
        "the refusal names the draining supervisor: {refused:?}"
    );
    verb.shutdown().await.expect("stop the verb's runtime");

    let embedder = ProjectRuntime::open_directory_existing_at(
        &fixture.checkout,
        scope(),
        fixture.storage.clone(),
        cowshed_cli::gateway_service::runtime_control,
    )
    .await
    .expect("an embedder's runtime");
    // Served as `cowshed controller` serves its embedder: the relay marks the runtime.
    let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let served = tokio::spawn(controller_service::serve_runtime(
        embedder,
        GatewayStoreBackend::Directory,
        server.into(),
    ));
    let checkout = fixture.checkout.clone();
    let exec = tokio::spawn(async move { exec_true(client.into(), &checkout).await });
    tokio::pin!(exec);
    let drained = async {
        while said
            .next_line()
            .await
            .expect("its stdout")
            .expect("it serves")
            != "drain"
        {}
    };
    tokio::select! {
        () = drained => {}
        answered = &mut exec => panic!(
            "the embedder's exec was answered before it asked the supervisor of another build to \
             drain: {:?}",
            answered.expect("the exec task")
        ),
    }
    assert!(
        !exec.is_finished(),
        "the embedder waits while the supervisor of another build runs"
    );
    other
        .kill_bounded()
        .await
        .expect("the drained supervisor exits");
    let ended = exec.await.expect("the exec task");
    assert_eq!(
        ended
            .expect("the exec runs on a supervisor of this build")
            .exit,
        Some(ExitStatus::Exited { code: 0 })
    );
    served
        .await
        .expect("the controller task")
        .expect("the controller ends once its embedder closed its end");
    fixture.stop_gateway().await;
}
//#endregion smoo/cowshed!n/embedder-awaits-other-build
