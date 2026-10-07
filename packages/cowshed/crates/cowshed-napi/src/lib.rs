//! Node-API bindings for the capability-safe cowshed client surface.
//!
//! Every controller operation reaches JavaScript through an adapter `cowshed-api-gen` emits from
//! cowshed-core's operation table into `operations.generated.rs`: one method per operation, on
//! the handle its namespace names, taking the request fields that handle does not bind as one
//! JSON object. What is written here by hand is what no operation declares — the inherited
//! endpoint, each handle's identity getters — and the error and promise plumbing the generated
//! adapters share.

use std::{
    future::Future,
    io,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
};

use bytes::Bytes;
use cowshed_core::{
    Coordinator as CoreCoordinator, Cowshed, CowshedError, JobHandle as CoreJobHandle,
    Project as CoreProject, WorkspaceHandle as CoreWorkspaceHandle,
    WorkspaceRef as CoreWorkspaceRef,
    api::{
        call::{self, Arguments, NamesJob, Serves},
        operations::{LogsChunk, Operation, WorkerView, WorkspaceView},
    },
};
use napi::{
    Env, JsObject,
    bindgen_prelude::{Buffer, ToNapiValue},
};
use napi_derive::napi;
use serde::Serialize;

#[path = "operations.generated.rs"]
mod operations;

const CONSUMED_FD: i32 = -1;

/// Retains the complete canonical error until its generated native projection settles the call.
struct AddonFailure(CowshedError);

impl AddonFailure {
    fn usage(message: impl Into<String>, hint: impl Into<String>) -> Self {
        CowshedError::usage(message, hint).into()
    }

    fn conflict(message: impl Into<String>, hint: impl Into<String>) -> Self {
        CowshedError::conflict(message, hint).into()
    }

    fn internal(message: impl Into<String>) -> Self {
        CowshedError::internal(message).into()
    }
}

impl From<CowshedError> for AddonFailure {
    fn from(error: CowshedError) -> Self {
        Self(error)
    }
}

type AddonResult<T> = std::result::Result<T, AddonFailure>;

/// Projects all canonical error details; failure to build that object remains an explicit
/// native environment error, never a silently hintless or causeless operational refusal.
fn to_napi_error(env: Env, failure: AddonFailure) -> napi::Error {
    match operations::cowshed_error(env, failure.0) {
        Ok(error) | Err(error) => error,
    }
}

fn spawn_promise<T, F>(env: Env, future: F) -> napi::Result<JsObject>
where
    T: ToNapiValue + Send + 'static,
    F: Future<Output = AddonResult<T>> + Send + 'static,
{
    let (deferred, promise) = env.create_deferred()?;
    napi::tokio::spawn(async move {
        let result = future.await;
        deferred.resolve(move |env| result.map_err(|failure| to_napi_error(env, failure)));
    });
    Ok(promise)
}

fn canonical_json<T: Serialize>(kind: &'static str, value: &T) -> AddonResult<String> {
    serde_json::to_string(value)
        .map_err(|error| AddonFailure::internal(format!("failed to serialize {kind}: {error}")))
}

/// A call's caller fields: one JSON object, never a number JavaScript has already rounded. JSON
/// text crosses the boundary because napi's own value conversion turns an integer above
/// `u32::MAX` into a float, which no `u64` request field accepts.
fn arguments(method: &'static str, json: &str) -> AddonResult<Arguments> {
    serde_json::from_str(json).map_err(|error| {
        AddonFailure::usage(
            format!("{method} arguments are not one JSON object: {error}"),
            "pass the operation's request fields as one JSON object",
        )
    })
}

/// An upload's raw-byte frame, when the caller sent one.
fn frame(buffer: Option<Buffer>) -> Option<Bytes> {
    buffer.map(|buffer| Bytes::from(Vec::<u8>::from(buffer)))
}

/// A download's answer: the chunk's metadata as JSON, and its bytes.
#[napi(object)]
pub struct Download {
    pub json: String,
    pub bytes: Buffer,
}

// The adapters `operations.generated.rs` emits, one shape per kind of declared answer. Each runs
// the call on the addon's runtime and settles the promise JavaScript holds.

/// A JSON-lane operation whose result crosses as its canonical JSON.
fn json_call<O, H>(env: Env, handle: Arc<H>, json: String) -> napi::Result<JsObject>
where
    O: Operation,
    H: Serves<O> + Send + Sync + 'static,
{
    spawn_promise(env, async move {
        let result = call::call::<O, H>(&*handle, arguments(O::METHOD, &json)?).await?;
        canonical_json(O::METHOD, &result)
    })
}

/// An upload-lane operation, with the caller's bytes as its raw-byte frame when it sent any.
fn upload_call<O, H>(
    env: Env,
    handle: Arc<H>,
    json: String,
    bytes: Option<Buffer>,
) -> napi::Result<JsObject>
where
    O: Operation,
    H: Serves<O> + Send + Sync + 'static,
{
    let bytes = frame(bytes);
    spawn_promise(env, async move {
        let result =
            call::call_upload::<O, H>(&*handle, arguments(O::METHOD, &json)?, bytes).await?;
        canonical_json(O::METHOD, &result)
    })
}

/// A download-lane operation: the chunk's metadata and the bytes it describes.
fn download_call<O, H>(env: Env, handle: Arc<H>, json: String) -> napi::Result<JsObject>
where
    O: Operation<Result = LogsChunk>,
    H: Serves<O> + Send + Sync + 'static,
{
    spawn_promise(env, async move {
        let (chunk, bytes) =
            call::call_download::<O, H>(&*handle, arguments(O::METHOD, &json)?).await?;
        Ok(Download {
            json: canonical_json(O::METHOD, &chunk)?,
            bytes: Buffer::from(Vec::from(bytes)),
        })
    })
}

/// An operation whose result is one workspace, as a reference to it.
fn workspace_call<O, H>(env: Env, handle: Arc<H>, json: String) -> napi::Result<JsObject>
where
    O: Operation<Result = WorkspaceView>,
    H: Serves<O> + Send + Sync + 'static,
{
    spawn_promise(env, async move {
        let workspace =
            call::call_workspace::<O, H>(&*handle, arguments(O::METHOD, &json)?).await?;
        Ok(WorkspaceRef {
            inner: Arc::new(workspace),
        })
    })
}

/// The operation that mints a worker capability, as that capability.
fn worker_call<O>(
    env: Env,
    coordinator: Arc<CoreCoordinator>,
    json: String,
) -> napi::Result<JsObject>
where
    O: Operation<Result = WorkerView>,
    CoreCoordinator: Serves<O>,
{
    spawn_promise(env, async move {
        let worker = coordinator
            .call_worker::<O>(arguments(O::METHOD, &json)?)
            .await?;
        Ok(WorkspaceHandle {
            inner: Arc::new(worker),
        })
    })
}

/// A JSON-lane operation of a workspace whose result names one of its jobs, as that job.
fn job_call<O>(
    env: Env,
    workspace: Arc<CoreWorkspaceHandle>,
    json: String,
) -> napi::Result<JsObject>
where
    O: Operation<Result: NamesJob>,
    CoreWorkspaceHandle: Serves<O>,
{
    spawn_promise(env, async move {
        let job = workspace
            .call_job::<O>(arguments(O::METHOD, &json)?)
            .await?;
        Ok(JobHandle {
            inner: Arc::new(job),
        })
    })
}

/// [`job_call`] for an upload-lane operation.
fn job_upload_call<O>(
    env: Env,
    workspace: Arc<CoreWorkspaceHandle>,
    json: String,
    bytes: Option<Buffer>,
) -> napi::Result<JsObject>
where
    O: Operation<Result: NamesJob>,
    CoreWorkspaceHandle: Serves<O>,
{
    let bytes = frame(bytes);
    spawn_promise(env, async move {
        let job = workspace
            .call_job_upload::<O>(arguments(O::METHOD, &json)?, bytes)
            .await?;
        Ok(JobHandle {
            inner: Arc::new(job),
        })
    })
}

fn set_cloexec(descriptor: &OwnedFd) -> io::Result<()> {
    let fd = descriptor.as_raw_fd();
    // SAFETY: `fd` is owned and live for the duration of both fcntl calls.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    if flags & libc::FD_CLOEXEC != 0 {
        return Ok(());
    }
    // SAFETY: `F_SETFD` consumes an integer flags argument and does not take ownership of `fd`.
    let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// An affine inherited controller descriptor. It can be consumed exactly once.
#[napi]
pub struct CoordinatorEndpoint {
    fd: AtomicI32,
}

impl CoordinatorEndpoint {
    fn take(&self) -> AddonResult<OwnedFd> {
        let fd = self.fd.swap(CONSUMED_FD, Ordering::AcqRel);
        if fd == CONSUMED_FD {
            return Err(AddonFailure::conflict(
                "coordinator endpoint has already been consumed",
                "create a new endpoint from a fresh inherited controller descriptor",
            ));
        }

        // SAFETY: the successful atomic swap transfers the endpoint's sole ownership here.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

impl Drop for CoordinatorEndpoint {
    fn drop(&mut self) {
        let fd = self.fd.swap(CONSUMED_FD, Ordering::AcqRel);
        if fd != CONSUMED_FD {
            // SAFETY: this endpoint still owns the unconsumed descriptor after the swap.
            drop(unsafe { OwnedFd::from_raw_fd(fd) });
        }
    }
}

#[napi(js_name = "coordinatorEndpoint")]
pub fn coordinator_endpoint(env: Env, fd: i32) -> napi::Result<CoordinatorEndpoint> {
    if fd <= libc::STDERR_FILENO {
        return Err(to_napi_error(
            env,
            AddonFailure::usage(
                format!("invalid inherited coordinator descriptor {fd}"),
                "pass an open inherited controller descriptor",
            ),
        ));
    }

    // SAFETY: a successful call transfers this inherited descriptor to the endpoint.
    let descriptor = unsafe { OwnedFd::from_raw_fd(fd) };
    set_cloexec(&descriptor).map_err(|error| {
        to_napi_error(
            env,
            AddonFailure::usage(
                format!("failed to configure inherited coordinator descriptor: {error}"),
                "pass an open inherited controller descriptor",
            ),
        )
    })?;

    Ok(CoordinatorEndpoint {
        fd: AtomicI32::new(descriptor.into_raw_fd()),
    })
}

#[napi(js_name = "openProject")]
pub fn open_project(
    env: Env,
    endpoint: &CoordinatorEndpoint,
    path: String,
) -> napi::Result<JsObject> {
    let descriptor = endpoint.take();
    spawn_promise(env, async move {
        let descriptor = descriptor?;
        let (cowshed, coordinator_token) = Cowshed::connect(descriptor)
            .await
            .map_err(AddonFailure::from)?;
        let project = cowshed.open(path).await.map_err(AddonFailure::from)?;
        drop(coordinator_token);
        Ok(Project {
            inner: Arc::new(project),
        })
    })
}

/// Coordinator authority retained for the wrapper lifetime.
///
/// Unlike `Project`, this owns the authenticated coordinator channel, so dropping the JavaScript
/// wrapper cleanly releases the authority obtained from the inherited endpoint.
#[napi(js_name = "connectCoordinator")]
pub fn connect_coordinator(
    env: Env,
    endpoint: &CoordinatorEndpoint,
    path: String,
) -> napi::Result<JsObject> {
    let descriptor = endpoint.take();
    spawn_promise(env, async move {
        let descriptor = descriptor?;
        let (cowshed, token) = Cowshed::connect(descriptor)
            .await
            .map_err(AddonFailure::from)?;
        let project = cowshed.open(path).await.map_err(AddonFailure::from)?;
        let coordinator = cowshed
            .coordinator(&project, token)
            .map_err(AddonFailure::from)?;
        Ok(Coordinator {
            inner: Arc::new(coordinator),
        })
    })
}

fn utf8_path(env: Env, path: &std::path::Path, what: &str) -> napi::Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        to_napi_error(
            env,
            AddonFailure::internal(format!("controller returned a non-UTF-8 {what}")),
        )
    })
}

/// Sole project mutation and cross-workspace authority.
#[napi]
pub struct Coordinator {
    inner: Arc<CoreCoordinator>,
}

/// Discovery-only project identity.
#[napi]
pub struct Project {
    inner: Arc<CoreProject>,
}

#[napi]
impl Project {
    #[napi(getter, js_name = "repoId")]
    pub fn repo_id(&self) -> String {
        self.inner.repo_id().to_string()
    }

    #[napi(getter, js_name = "gitRoot")]
    pub fn git_root(&self, env: Env) -> napi::Result<String> {
        utf8_path(env, self.inner.git_root(), "Git root")
    }
}

/// One workspace incarnation, as the call that resolved it saw it.
#[napi]
pub struct WorkspaceRef {
    inner: Arc<CoreWorkspaceRef>,
}

#[napi]
impl WorkspaceRef {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter, js_name = "mountPath")]
    pub fn mount_path(&self, env: Env) -> napi::Result<String> {
        utf8_path(env, self.inner.mount_path(), "workspace mount path")
    }

    /// This reference as a land or rebase target, pinned to the incarnation it was resolved at.
    #[napi(getter, js_name = "targetJson")]
    pub fn target_json(&self, env: Env) -> napi::Result<String> {
        canonical_json("workspace target", &self.inner.target())
            .map_err(|failure| to_napi_error(env, failure))
    }
}

/// Non-escalating capability for exactly one workspace incarnation.
#[napi]
pub struct WorkspaceHandle {
    inner: Arc<CoreWorkspaceHandle>,
}

#[napi]
impl WorkspaceHandle {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter, js_name = "mountPath")]
    pub fn mount_path(&self, env: Env) -> napi::Result<String> {
        utf8_path(env, self.inner.mount_path(), "workspace mount path")
    }
}

/// One job of one workspace incarnation.
#[napi]
pub struct JobHandle {
    inner: Arc<CoreJobHandle>,
}

#[napi]
impl JobHandle {
    /// A job id is at most `MAX_JOB_ID`, a safe integer, so JavaScript's number holds it exactly.
    #[napi(getter)]
    pub fn id(&self, env: Env) -> napi::Result<i64> {
        i64::try_from(self.inner.id().get()).map_err(|_| {
            to_napi_error(
                env,
                AddonFailure::internal("a job id exceeded the safe integer range"),
            )
        })
    }
}

#[cfg(test)]
mod wire_contract;

#[cfg(test)]
mod parity_tests {
    use std::collections::BTreeSet;

    use cowshed_cli::args::{COMMANDS, Command, parse_args};
    use cowshed_core::api::operations::{OPERATIONS, Scope};

    /// The controller operations a CLI verb reaches through the addon, or `None` when the verb
    /// has none: host management runs the packaged binary through the `cli.ts` trampoline, and
    /// the addon deliberately does not link the CLI to offer a second in-process copy of it.
    /// Every declared operation outside the controller's own is an addon method generated from
    /// the operation table, so naming the operation names the export.
    ///
    /// Adding a `Command` variant breaks this match. Adding an arm without adding it to `SAMPLES`
    /// breaks `every_cli_command_maps_to_its_named_operations`.
    fn napi_operations(command: &Command) -> Option<&'static [&'static str]> {
        match command {
            Command::Adopt(_) => Some(&["coordinator.adopt"]),
            Command::New(_) => Some(&["coordinator.create"]),
            Command::Fork(_) => Some(&["coordinator.fork"]),
            Command::Move(_) => Some(&["coordinator.rename", "coordinator.moveCheckout"]),
            Command::Checkpoint(_) => Some(&["worker.checkpoint"]),
            Command::Restore(_) => Some(&["coordinator.restore"]),
            Command::List(_) => Some(&["project.list"]),
            Command::Path(_) => Some(&["project.workspace", "workspace.info"]),
            Command::Exec(_) => Some(&["worker.exec"]),
            Command::Grant(_) => Some(&["coordinator.grant", "workspace.grants"]),
            Command::Remove(_) => Some(&["coordinator.destroy"]),
            Command::Attach(_) => Some(&["workspace.attach"]),
            Command::Detach(_) => Some(&["coordinator.detach"]),
            Command::Resize(_) => Some(&["coordinator.resize"]),
            Command::Defrag(_) => Some(&["coordinator.defragment"]),
            Command::Reseed(_) => Some(&["coordinator.reseed"]),
            Command::Gc(_) => Some(&["coordinator.gc"]),
            Command::Push(_) => Some(&["worker.push"]),
            Command::Rebase(_) => Some(&["coordinator.rebase"]),
            Command::Land(_) => Some(&["coordinator.land"]),
            Command::Doctor(_) => Some(&["coordinator.doctor"]),
            // The verb serves the endpoint `coordinatorEndpoint` wraps; the addon is its peer, not a
            // second copy of it.
            Command::Controller
            | Command::Gateway(_)
            | Command::Credential(_)
            | Command::Identity(_)
            | Command::Sccache(_)
            | Command::Skill(_)
            | Command::Setup(_)
            | Command::Mount(_)
            | Command::Rekey(_)
            | Command::Version
            // A read-only lint helper over the checkout's recorded volume state; no controller call.
            | Command::BuildState
            | Command::Help(_) => None,
        }
    }

    /// One representative argv per `Command` arm, paired with the operations it must map to.
    const SAMPLES: &[(&[&str], Option<&[&str]>)] = &[
        (&["adopt"], Some(&["coordinator.adopt"])),
        (&["new", "parity"], Some(&["coordinator.create"])),
        (&["fork", "main", "parity"], Some(&["coordinator.fork"])),
        (
            &["mv", "parity", "renamed"],
            Some(&["coordinator.rename", "coordinator.moveCheckout"]),
        ),
        (&["checkpoint", "parity"], Some(&["worker.checkpoint"])),
        (
            &["restore", "parity", "saved"],
            Some(&["coordinator.restore"]),
        ),
        (&["ls"], Some(&["project.list"])),
        (
            &["path", "parity"],
            Some(&["project.workspace", "workspace.info"]),
        ),
        (&["exec", "parity", "--", "true"], Some(&["worker.exec"])),
        (
            &["grant", "parity"],
            Some(&["coordinator.grant", "workspace.grants"]),
        ),
        (&["rm", "parity"], Some(&["coordinator.destroy"])),
        (&["attach", "parity"], Some(&["workspace.attach"])),
        (&["detach", "parity"], Some(&["coordinator.detach"])),
        (&["resize", "parity", "200g"], Some(&["coordinator.resize"])),
        (&["defrag", "parity"], Some(&["coordinator.defragment"])),
        (&["reseed", "parity"], Some(&["coordinator.reseed"])),
        (&["gc"], Some(&["coordinator.gc"])),
        (&["push", "parity"], Some(&["worker.push"])),
        (&["rebase", "parity"], Some(&["coordinator.rebase"])),
        (&["land", "parity"], Some(&["coordinator.land"])),
        (&["doctor"], Some(&["coordinator.doctor"])),
        (&["gateway", "status"], None),
        (&["controller"], None),
        (&["credential", "status"], None),
        (&["identity", "add", "forge"], None),
        (&["sccache", "status"], None),
        (&["skill", "install"], None),
        (&["mount", "main", "--repo-id", "acme/widget"], None),
        (&["rekey", "parity"], None),
        (&["help"], None),
        (&["setup"], None),
        (&["build-state"], None),
        (&["--version"], None),
    ];

    #[test]
    fn every_cli_command_maps_to_its_named_operations() {
        for (argv, expected) in SAMPLES {
            let parsed =
                parse_args(argv.iter().copied()).expect("representative CLI command parses");
            assert_eq!(
                napi_operations(&parsed.command),
                *expected,
                "CLI command {argv:?} does not map to the operations the parity table names"
            );
        }
    }

    /// Every operation the table names is declared and projected onto the addon: a renamed or
    /// controller-internal operation is red here, not a verb JavaScript silently lost.
    #[test]
    fn every_named_operation_is_a_projected_declaration() {
        let projected: BTreeSet<&str> = OPERATIONS
            .iter()
            .filter(|operation| operation.scope != Scope::Internal)
            .map(|operation| operation.method)
            .collect();
        for (argv, operations) in SAMPLES {
            for method in operations.iter().copied().flatten() {
                assert!(
                    projected.contains(method),
                    "{argv:?} names {method}, which the addon does not project"
                );
            }
        }
    }

    /// Every verb the CLI dispatches must appear in the parity table.
    ///
    /// `COMMANDS` is the command map `cowshed --help` prints and the list the parser is generated
    /// from, so this is red the moment a verb is added without naming the operations it
    /// corresponds to.
    #[test]
    fn every_cli_verb_has_a_parity_sample() {
        let dispatched: BTreeSet<&str> = COMMANDS.iter().map(|spec| spec.name).collect();
        let sampled: BTreeSet<&str> = SAMPLES
            .iter()
            .filter_map(|(argv, _)| argv.first().copied())
            .collect();

        assert_eq!(
            dispatched.difference(&sampled).copied().collect::<Vec<_>>(),
            Vec::<&str>::new(),
            "CLI verbs the parity table does not sample"
        );
        // `help` and `--version` resolve to a `Command` without being entries in the command map,
        // so they are the only tokens allowed to be sampled without being dispatched verbs.
        assert_eq!(
            sampled.difference(&dispatched).copied().collect::<Vec<_>>(),
            vec!["--version", "help"],
            "the parity table samples a token the CLI does not dispatch"
        );
    }

    /// No two verbs may claim one operation: that would mean the table has stopped describing the
    /// seam it is named after.
    #[test]
    fn no_two_verbs_claim_one_operation() {
        let mut claimed: Vec<&str> = SAMPLES
            .iter()
            .filter_map(|(_, operations)| *operations)
            .flatten()
            .copied()
            .collect();
        let distinct: BTreeSet<&str> = claimed.iter().copied().collect();
        claimed.sort_unstable();

        assert_eq!(claimed.len(), distinct.len(), "duplicate operation claim");
    }
}

#[cfg(test)]
mod host_stable_path_tests {
    use std::path::Path;

    /// The launcher is a shell script and cannot ask where launchd installed the binary, so
    /// `bin/cowshed` restates the path under `$HOME`. This test is the check that it still names
    /// `HostStableExecutable`'s path: changing either side without the other is red.
    #[test]
    fn launcher_host_stable_path_matches_launchd() {
        let launcher = include_str!("../../../bin/cowshed");
        assert!(
            launcher.contains(r"${HOME:-}/Library/Application\ Support/dev.cowshed/bin/cowshed"),
            "bin/cowshed must name the launchd host-stable binary"
        );

        let executable = cowshed_cli::launchd::HostStableExecutable::new(
            Path::new("/Users/test"),
            cowshed_cli::launchd::COWSHED_BINARY_NAME,
        )
        .expect("a lexical home path is accepted");
        assert_eq!(
            executable.path(),
            Path::new("/Users/test/Library/Application Support/dev.cowshed/bin/cowshed")
        );
    }
}

#[cfg(test)]
mod error_code_ssot {
    use cowshed_core::ErrorCode;

    const TYPES_TS: &str = include_str!("../../../src/api.generated.ts");

    /// Core's kebab-case spellings, in enum-declaration order. Adding a variant breaks
    /// `ErrorCode::as_str`; adding it there without this list leaves the TypeScript union
    /// unverified, so keep the two lists in this test the same length as the enum.
    fn rust_spellings() -> Vec<&'static str> {
        [
            ErrorCode::Internal,
            ErrorCode::Usage,
            ErrorCode::NotFound,
            ErrorCode::Conflict,
            ErrorCode::EnvironmentMissing,
            ErrorCode::SandboxDenied,
            ErrorCode::Integrity,
        ]
        .into_iter()
        .map(ErrorCode::as_str)
        .collect()
    }

    fn ts_spellings() -> Vec<&'static str> {
        let start = TYPES_TS
            .find("export type ErrorCode =")
            .expect("api.generated.ts exports ErrorCode");
        let end = start
            + TYPES_TS[start..]
                .find(';')
                .expect("ErrorCode union is semicolon-terminated");
        TYPES_TS[start..end]
            .strip_prefix("export type ErrorCode =")
            .expect("the generated taxonomy declaration")
            .split('|')
            .filter(|member| !member.trim().is_empty())
            .map(|member| {
                member
                    .trim()
                    .strip_prefix('\'')
                    .and_then(|value| value.strip_suffix('\''))
                    .expect("each generated taxonomy member is a quoted string")
            })
            .collect()
    }

    /// The JSON wire corpus covers DTOs; ErrorCode rides on the napi Error object instead, so
    /// this checks the generated union names every `as_str` spelling and nothing else.
    #[test]
    fn types_ts_error_code_union_matches_core_taxonomy() {
        assert_eq!(ts_spellings(), rust_spellings());
    }
}
