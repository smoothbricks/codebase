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
//! execution unchanged; it performs no preflight effects before Core authorizes the request. Its one
//! effect of its own comes after a retire, which it serves like any other call: it reconciles the
//! project's gateway sessions through the function the CLI verbs run, before it answers
//! (06_cli.md, `cowshed controller`).

use std::fs::File;
use std::num::NonZeroUsize;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::Arc;

use crate::gateway_service::{self, PortSlotAssigner};
use async_trait::async_trait;
use cowshed_core::api::EmptyResult;
use cowshed_core::api::operations::{CoordinatorAssignSlot, OperationRequest, SlotRequest};
use cowshed_core::api::server::{
    ConnectionAuthority, RouterCommand, RouterHandle, serve_controller_connection,
};
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::{CowshedError, GatewayStoreBackend, Result, ValidatedHostStorage};
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
    serve_runtime(runtime, GatewayStoreBackend::Native, socket).await
}

/// Serve one coordinator connection on `socket` over an opened `runtime` of the `backend` store
/// until its peer closes it, then shut the project down.
pub async fn serve_runtime(
    runtime: ProjectRuntime,
    backend: GatewayStoreBackend,
    socket: OwnedFd,
) -> Result<()> {
    let authority = ConnectionAuthority::Coordinator {
        repo_id: runtime.descriptor().repo_id.clone(),
    };
    let gateway = Arc::new(ProjectGateway {
        repo_id: runtime.descriptor().repo_id.clone(),
        storage: runtime.descriptor().storage.clone(),
        backend,
        slots: RouterSlots {
            router: runtime.router(),
            repo_id: runtime.descriptor().repo_id.clone(),
        },
    });
    let (router, calls) = RouterHandle::relaying(RELAY_CAPACITY, &runtime.router());
    let relay = tokio::spawn(relay(calls, runtime.router(), gateway));
    let served = serve_controller_connection(socket, authority, router).await;
    let relayed = relay
        .await
        .map_err(|error| CowshedError::internal(format!("the controller relay failed: {error}")));
    let shutdown = runtime.shutdown().await;
    served.and(relayed).and(shutdown)
}

/// The gateway sessions of the project this controller serves, reconciled the way every CLI verb
/// that changes the project reconciles them (06_cli.md, `cowshed controller`).
struct ProjectGateway {
    repo_id: RepoId,
    storage: ValidatedHostStorage,
    backend: GatewayStoreBackend,
    slots: RouterSlots,
}

impl ProjectGateway {
    async fn reconcile(&self) -> Result<()> {
        gateway_service::reconcile_project_for_backend(
            &self.repo_id,
            &self.storage,
            &self.slots,
            self.backend,
        )
        .await
        .map(|_| ())
    }
}

/// Port-slot assignment through the served runtime's own router, as the CLI's embedded
/// coordinator asks it: a reconcile that moves a workspace off an occupied port block publishes
/// the replacement grant through the project's actor.
struct RouterSlots {
    router: RouterHandle,
    repo_id: RepoId,
}

#[async_trait]
impl PortSlotAssigner for RouterSlots {
    async fn assign_port_slot(&self, workspace: &str, slot: u32) -> Result<()> {
        let workspace = WorkspaceName::new(workspace)
            .map_err(|error| CowshedError::usage(error.to_string(), "name a workspace"))?;
        self.router
            .call::<CoordinatorAssignSlot>(
                ConnectionAuthority::Coordinator {
                    repo_id: self.repo_id.clone(),
                },
                SlotRequest {
                    repo_id: self.repo_id.clone(),
                    workspace,
                    slot,
                },
            )
            .await
            .map(|EmptyResult {}| ())
    }
}

/// Whether `operation` retires workspaces when it succeeds: a destroy, and a land that retires its
/// unit (and the peers it folds). A preflight retires nothing.
fn retires_a_workspace(operation: &OperationRequest) -> bool {
    match operation {
        OperationRequest::CoordinatorDestroy(_) => true,
        OperationRequest::CoordinatorLand(land) => land.options.retire && !land.options.preflight,
        _ => false,
    }
}

/// Forward calls independently so a job wait never holds the calls behind it. Core owns
/// admission and gateway preparation for both decoded execution routes.
async fn relay(
    mut calls: mpsc::Receiver<RouterCommand>,
    router: RouterHandle,
    gateway: Arc<ProjectGateway>,
) {
    let mut open = JoinSet::new();
    loop {
        tokio::select! {
            call = calls.recv() => match call {
                Some(call) => {
                    open.spawn(answer(call, router.clone(), Arc::clone(&gateway)));
                }
                None => return,
            },
            Some(ended) = open.join_next() => {
                if let Err(error) = ended {
                    cowshed_os::eprintln!("cowshed: a controller call ended without an answer: {error}");
                }
            }
        }
    }
}

async fn answer(call: RouterCommand, router: RouterHandle, gateway: Arc<ProjectGateway>) {
    let (request, reply) = call.into_parts();
    let (authority, operation, upload, steps) = request.into_parts();
    let retires = retires_a_workspace(&operation);
    let response = router.route(authority, operation, upload, steps).await;
    // The retire is done: a gateway that refuses the reconcile is said, and the retire answers as
    // done, since the project's next reconcile removes the session it left.
    if retires
        && response.is_ok()
        && let Err(error) = gateway.reconcile().await
    {
        cowshed_os::eprintln!(
            "cowshed: the gateway's view of {} is stale after a retire: {error}",
            gateway.repo_id
        );
    }
    // A peer that has gone away no longer wants the answer; its connection already dropped it.
    let _ = reply.send(response);
}
