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
        ),
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
