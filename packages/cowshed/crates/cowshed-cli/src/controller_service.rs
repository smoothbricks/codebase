//! `cowshed controller`: one coordinator controller connection, served on this process's standard
//! input for the embedding process that holds the socket's other end.
//!
//! A process that links cowshed as a library is another cowshed build, and the daemon's manager
//! starts workspace supervisors only for its own build (11_shell.md "Protocol"). Such a process
//! therefore runs its controller as this verb of the host's own `cowshed`: the project opens in
//! this process, exactly as any verb opens it, and the embedder speaks the controller protocol to
//! it over the inherited socket, as `Cowshed::connect` and the N-API `coordinatorEndpoint` do.
//!
//! Gateway reconciliation belongs to Core work admission after route and live-incarnation
//! fences (05_gateway.md "Control plane"). This relay forwards both worker and coordinator
//! execution unchanged; it performs no preflight effects before Core authorizes the request.

use std::fs::File;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;
use std::path::Path;

use cowshed_core::api::server::{
    ConnectionAuthority, RouterCommand, RouterHandle, serve_controller_connection,
};
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::{CowshedError, Result};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// How many calls wait between the connection and the relay; the connection itself holds at most
/// 64 open, so this is never the bound a caller meets.
const RELAY_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).expect("nonzero");

/// Take the controller socket off standard input. Checked before anything else is read, so a
/// terminal or a pipe on stdin is refused before the project is touched.
///
/// The socket keeps a close-on-exec descriptor of its own, and standard input becomes
/// `/dev/null`: every child this process starts inherits stdin, and one that read the socket would
/// consume controller frames.
pub fn take_inherited_socket() -> Result<OwnedFd> {
    let descriptor = std::io::stdin()
        .as_fd()
        .try_clone_to_owned()
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot duplicate standard input: {error}"),
                "check the per-process file descriptor limit",
            )
        })?;
    let stdin = File::from(descriptor);
    let is_socket = stdin
        .metadata()
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot inspect standard input: {error}"),
                "pass one end of a socketpair as this command's standard input",
            )
        })?
        .file_type()
        .is_socket();
    if !is_socket {
        return Err(CowshedError::usage(
            "cowshed controller serves a controller connection on standard input, which is not a socket",
            "an embedding process passes one end of a Unix socketpair as this command's standard input and keeps the other",
        ));
    }
    let null = File::open("/dev/null").map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot open /dev/null to take the controller socket off standard input: {error}"
            ),
            "check the per-process file descriptor limit",
        )
    })?;
    // SAFETY: both descriptors are open for the duration of the call, and `dup2` only replaces
    // descriptor 0, which nothing in this process holds as an owned value: the socket is kept
    // through `stdin`, a separate descriptor.
    if unsafe { libc::dup2(null.as_raw_fd(), libc::STDIN_FILENO) } < 0 {
        return Err(CowshedError::environment_missing(
            format!(
                "cannot replace standard input with /dev/null: {}",
                std::io::Error::last_os_error()
            ),
            "check the per-process file descriptor limit",
        ));
    }
    Ok(OwnedFd::from(stdin))
}

/// Serve one coordinator connection for the project at `project_root` on `socket` until its peer
/// closes it, then shut the project down.
pub async fn serve(project_root: &Path, socket: OwnedFd) -> Result<()> {
    // Only this controller's selected recovery runs during open; workspace requests are fenced
    // by the Core actor before its job path reconciles any gateway state.
    let runtime = ProjectRuntime::open_existing(
        project_root,
        RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
        crate::gateway_service::runtime_control,
    )
    .await?;
    let authority = ConnectionAuthority::Coordinator {
        repo_id: runtime.descriptor().repo_id.clone(),
    };
    let (router, calls) = RouterHandle::channel(RELAY_CAPACITY);
    let relay = tokio::spawn(relay(calls, runtime.router()));
    let served = serve_controller_connection(socket, authority, router).await;
    let relayed = relay
        .await
        .map_err(|error| CowshedError::internal(format!("the controller relay failed: {error}")));
    let shutdown = runtime.shutdown().await;
    served.and(relayed).and(shutdown)
}

/// Forward calls independently so a job wait never holds the calls behind it. Core owns
/// admission and gateway preparation for both decoded execution routes.
async fn relay(mut calls: mpsc::Receiver<RouterCommand>, router: RouterHandle) {
    let mut open = JoinSet::new();
    loop {
        tokio::select! {
            call = calls.recv() => match call {
                Some(call) => {
                    open.spawn(answer(call, router.clone()));
                }
                None => return,
            },
            Some(ended) = open.join_next() => {
                if let Err(error) = ended {
                    eprintln!("cowshed: a controller call ended without an answer: {error}");
                }
            }
        }
    }
}

async fn answer(call: RouterCommand, router: RouterHandle) {
    let (request, reply) = call.into_parts();
    let (authority, operation, upload, steps) = request.into_parts();
    let response = router.route(authority, operation, upload, steps).await;
    // A peer that has gone away no longer wants the answer; its connection already dropped it.
    let _ = reply.send(response);
}
