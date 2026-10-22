//! `cowshed host`: one host connection, served on this process's standard input for the
//! embedding process that holds the socket's other end (07_api.md, "Host operations").
//!
//! The host's own verbs have no project controller to serve them, so they are served here, by the
//! host's own build, as `cowshed controller` serves a project's. Every operation calls the
//! function its CLI verb calls and answers with what that function computes; only rendering is
//! the verb's alone. Guidance a verb says along the way goes to this process's stderr, which is
//! the embedding process's own; stdout is never written.

use std::num::NonZeroUsize;
use std::os::fd::OwnedFd;

use cowshed_core::api::EmptyResult;
use cowshed_core::api::dto::{BinaryData, CommandArg, DiskCommandTool};
use cowshed_core::api::operations::{
    self, CredentialSecretSource, Operation, OperationRequest, encode_result,
};
use cowshed_core::api::server::{
    ConnectionAuthority, EventSource, HostRole, RouterCommand, RouterHandle, RouterRequest,
    RouterResponse, serve_controller_connection,
};
use cowshed_core::{CowshedError, Result};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::forget::ForgetBackend as _;
use crate::output::Output;

/// How many calls wait between the connection and the relay; the connection itself holds at most
/// 64 open, so this is never the bound a caller meets.
const RELAY_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).expect("nonzero");

/// Serve one host connection of `role` on `socket` until its peer closes it. The connection
/// refuses every method outside the role's scope before it reaches the router.
pub async fn serve(socket: OwnedFd, role: HostRole) -> Result<()> {
    let (router, calls) = RouterHandle::channel(RELAY_CAPACITY);
    let relay = tokio::spawn(relay(calls));
    let served = serve_controller_connection(socket, ConnectionAuthority::Host(role), router).await;
    // The connection's router handle is gone once it returns, so the relay drains what it holds
    // and ends.
    relay
        .await
        .map_err(|error| CowshedError::internal(format!("the host relay task failed: {error}")))?;
    served
}

/// Answer each call on a task of its own, so a slow verb never holds the answers behind it.
async fn relay(mut calls: mpsc::Receiver<RouterCommand>) {
    let mut answers = JoinSet::new();
    loop {
        tokio::select! {
            call = calls.recv() => match call {
                Some(call) => {
                    answers.spawn(async move {
                        let (request, reply) = call.into_parts();
                        // A caller that stopped waiting has no answer to read; the work it asked
                        // for is done.
                        let _ = reply.send(answer(request).await);
                    });
                }
                // The peer closed: the calls it left in flight still run to their end, so a verb
                // with side effects is never cut off midway.
                None => break,
            },
            Some(ended) = answers.join_next() => reaped(ended),
        }
    }
    while let Some(ended) = answers.join_next().await {
        reaped(ended);
    }
}

fn reaped(ended: std::result::Result<(), tokio::task::JoinError>) {
    if let Err(error) = ended {
        cowshed_os::eprintln!("cowshed: a host call ended without an answer: {error}");
    }
}

fn respond<O: Operation>(result: &O::Result) -> Result<RouterResponse> {
    encode_result::<O>(result).map(RouterResponse::json)
}

async fn answer(request: RouterRequest) -> Result<RouterResponse> {
    let (_, operation, upload, _) = request.into_parts();
    use OperationRequest as Op;
    match operation {
        Op::HostListProjects(request) => respond::<operations::HostListProjects>(
            &crate::runtime::list_projects(request.landing).await?,
        ),
        Op::HostAttachAll(request) => {
            let mounts = crate::runtime::attach_store_wide(request.browse).await?;
            report_stale(&mounts.stale)?;
            respond::<operations::HostAttachAll>(&mounts.result)
        }
        Op::HostDetachAll(_) => {
            let mounts = crate::runtime::detach_store_wide().await?;
            report_stale(&mounts.stale)?;
            respond::<operations::HostDetachAll>(&EmptyResult {})
        }
        Op::HostClearAttachBudget(_) => {
            crate::runtime::clear_attach_budget()?;
            respond::<operations::HostClearAttachBudget>(&EmptyResult {})
        }
        Op::HostForgetAbandoned(request) => {
            let backend =
                crate::forget::NativeForgetBackend::new(crate::runtime::host_storage().await?);
            respond::<operations::HostForgetAbandoned>(
                &backend.forget_project(&request.repo_id).await?,
            )
        }
        Op::HostMountMain(request) => {
            let backend = crate::mount_main::NativeMainMountBackend::new(
                crate::runtime::host_storage().await?,
            );
            respond::<operations::HostMountMain>(
                &crate::mount_main::mount_main(&backend, &request.repo_id).await?,
            )
        }
        Op::HostGatewayStatus(_) => respond::<operations::HostGatewayStatus>(
            &crate::gateway_service::service_status().await?,
        ),
        Op::HostCredentialStatus(request) => {
            let root = crate::runtime::discover_project_root(&request.project).await?;
            respond::<operations::HostCredentialStatus>(
                &crate::credential_service::status(&root).await?,
            )
        }
        Op::HostCredentialRemove(request) => {
            let root = crate::runtime::discover_project_root(&request.project).await?;
            respond::<operations::HostCredentialRemove>(
                &crate::credential_service::remove_origin(&root, &request.origin).await?,
            )
        }
        Op::HostIdentityAdd(request) => {
            let root = crate::runtime::discover_project_root(&request.project).await?;
            respond::<operations::HostIdentityAdd>(
                &crate::identity_service::add(&root, &request.remote).await?,
            )
        }
        Op::HostSkillInstall(request) => respond::<operations::HostSkillInstall>(
            &crate::skill::install_skill(request.project.as_deref(), &request.harnesses)?,
        ),
        Op::HostBuildState(request) => respond::<operations::HostBuildState>(
            &crate::build_state::listing(crate::build_state::read_at(&request.project)?),
        ),
        Op::HostClearNxStore(request) => {
            let root = crate::runtime::discover_project_root(&request.project).await?;
            let (repo_id, store) = crate::credential_service::project(&root).await?;
            let cleared = crate::gateway_service::ControlSocket::at(store.join("gateway.sock"))?
                .clear_nx_cache(&repo_id)
                .await?;
            respond::<operations::HostClearNxStore>(&cleared)
        }
        Op::HostGatewayStart(_) => {
            // Guidance the verb says while the gateway comes up goes to this process's stderr.
            let mut output = Output::new(std::io::sink(), std::io::stderr(), false);
            respond::<operations::HostGatewayStart>(
                &crate::gateway_service::start(&mut output).await?,
            )
        }
        Op::HostGatewayStop(request) => {
            crate::gateway_service::stop(request.purge).await?;
            respond::<operations::HostGatewayStop>(&EmptyResult {})
        }
        Op::HostLockHolders(request) => {
            respond::<operations::HostLockHolders>(&crate::lock::holders(&request.path)?)
        }
        Op::HostTreeHolders(request) => {
            respond::<operations::HostTreeHolders>(&crate::tree_holders::holders(&request.path)?)
        }
        Op::HostRetireDescendants(request) => respond::<operations::HostRetireDescendants>(
            &crate::subtree::retire_handle(
                request.handle,
                std::time::Duration::from_millis(request.grace_ms),
            )
            .await?,
        ),
        Op::HostCredentialAdd(request) => {
            let (secret_env, secret_command, secret_stdin) = match request.secret {
                CredentialSecretSource::Env { name, command } => (Some(name), command, false),
                CredentialSecretSource::Command { argv } => (None, Some(argv), false),
                CredentialSecretSource::Upload => (None, None, true),
            };
            if upload.is_some() && !secret_stdin {
                return Err(CowshedError::usage(
                    "host.credentialAdd carried an upload frame, but its secret is not the upload",
                    "send the secret as the upload frame only with secret { kind: \"upload\" }",
                ));
            }
            let root = crate::runtime::discover_project_root(&request.project).await?;
            let args = crate::args::CredentialAddArgs {
                origin: request.origin,
                path_prefixes: request.path_prefixes,
                methods: match request.methods.is_empty() {
                    true => crate::args::DEFAULT_CREDENTIAL_METHODS
                        .iter()
                        .map(|method| (*method).to_owned())
                        .collect(),
                    false => request.methods,
                },
                secret_env,
                secret_command,
                secret_stdin,
            };
            let uploaded = upload.as_deref().unwrap_or_default();
            respond::<operations::HostCredentialAdd>(
                &crate::credential_service::add_origin(
                    &root,
                    &args,
                    crate::credential_service::StdinSecret::Uploaded(uploaded),
                )
                .await?,
            )
        }
        Op::HostDiskCommand(request) => {
            let args = crate::args::DiskCommandArgs {
                tool: match request.tool {
                    DiskCommandTool::Diskutil => cowshed_apfs::DiskTool::Diskutil,
                    DiskCommandTool::Hdiutil => cowshed_apfs::DiskTool::Hdiutil,
                },
                deadline: request
                    .deadline_ms
                    .map_or(cowshed_apfs::DISK_CHILD_DEADLINE, |deadline| {
                        std::time::Duration::from_millis(deadline.get())
                    }),
                args: request
                    .argv
                    .into_iter()
                    .map(CommandArg::into_os_string)
                    .collect(),
            };
            let input = request
                .stdin
                .map(BinaryData::into_bytes)
                .unwrap_or_default();
            let ran = crate::disk_command::run(&args, crate::disk_command::ToolInput::Bytes(input))
                .await?;
            Ok(RouterResponse::events(Box::new(DiskEvents(
                crate::disk_command::OutputEvents::new(ran),
            ))))
        }
        Op::SccacheStart(request) => {
            let capacity = request
                .capacity
                .as_deref()
                .map(crate::capabilities::sccache::service::parse_capacity)
                .transpose()?;
            respond::<operations::SccacheStart>(
                &crate::capabilities::sccache::service::start_service(capacity).await?,
            )
        }
        Op::SccacheStop(_) => {
            crate::capabilities::sccache::service::stop_service()?;
            respond::<operations::SccacheStop>(&EmptyResult {})
        }
        Op::SccacheStatusRead(_) => respond::<operations::SccacheStatusRead>(
            &crate::capabilities::sccache::service::service_status().await?,
        ),
        other => Err(CowshedError::internal(format!(
            "{} reached the host router past the connection's scope check",
            other.method()
        ))),
    }
}

/// A finished disk tool's output, then its end, each event made on its demand.
struct DiskEvents(crate::disk_command::OutputEvents);

#[async_trait::async_trait]
impl EventSource for DiskEvents {
    async fn next(&mut self) -> Option<Result<serde_json::Value>> {
        let event = self.0.next_event()?;
        Some(event.and_then(|event| encode_result::<operations::HostDiskCommand>(&event)))
    }
}

/// A store-wide mount that changed the store but whose gateway view could not follow says so on
/// stderr, exactly as the verb does: the work completed, so it is no failure, and no silence.
fn report_stale(stale: &[(cowshed_core::repository::RepoId, CowshedError)]) -> Result<()> {
    let mut output = Output::new(std::io::sink(), std::io::stderr(), false);
    crate::runtime::report_stale_gateway_views(&mut output, stale)
}
