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
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
};

use bytes::Bytes;
use cowshed_core::{
    Coordinator as CoreCoordinator, Cowshed, CowshedError, EventStream, Host as CoreHost,
    JobAttachment as CoreJobAttachment, JobHandle as CoreJobHandle, JobStdin as CoreJobStdin,
    Project as CoreProject, SccacheHost as CoreSccacheHost, WorkspaceHandle as CoreWorkspaceHandle,
    WorkspaceRef as CoreWorkspaceRef,
    api::{
        call::{self, Arguments, NamesJob, Serves},
        operations::{AdoptView, LogsChunk, Operation, StreamOperation, WorkerView, WorkspaceView},
        server::HostRole,
    },
};
use napi::{
    Env, JsObject,
    bindgen_prelude::{Buffer, ToNapiValue},
    tokio::{
        self,
        sync::{Mutex, watch},
    },
};
use napi_derive::napi;
use serde::{Serialize, de::DeserializeOwned};

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

/// Decodes a handle-local optional API record using its canonical serde declaration.
fn optional_argument<T: DeserializeOwned>(
    method: &'static str,
    json: Option<&str>,
) -> AddonResult<Option<T>> {
    json.map(|json| {
        serde_json::from_str(json).map_err(|error| {
            AddonFailure::usage(
                format!("{method} argument is invalid: {error}"),
                "pass the declared API record",
            )
        })
    })
    .transpose()
}

/// Snapshot mutable JavaScript bytes before handing them to another runtime thread.
/// The TS wrapper passes only a Buffer view: this is the one required ownership copy.
fn owned_bytes(buffer: Buffer) -> Bytes {
    Bytes::from(Vec::<u8>::from(buffer))
}

/// An upload's raw-byte frame, when the caller sent one.
fn frame(buffer: Option<Buffer>) -> Option<Bytes> {
    buffer.map(owned_bytes)
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

/// The operation that adopts main, as main's reference and what a re-adoption kept.
fn adopted_call<O>(
    env: Env,
    coordinator: Arc<CoreCoordinator>,
    json: String,
) -> napi::Result<JsObject>
where
    O: Operation<Result = AdoptView>,
    CoreCoordinator: Serves<O>,
{
    spawn_promise(env, async move {
        let adopted = coordinator
            .call_adopt::<O>(arguments(O::METHOD, &json)?)
            .await?;
        Ok(Adopted {
            workspace: Arc::new(adopted.workspace),
            re_adoption: adopted
                .re_adoption
                .map(|re_adoption| canonical_json("re-adoption", &re_adoption))
                .transpose()?,
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

/// A JSON-lane operation whose optional result names a job, or no job, without masking errors.
fn optional_job_call<O, T>(
    env: Env,
    workspace: Arc<CoreWorkspaceHandle>,
    json: String,
) -> napi::Result<JsObject>
where
    O: Operation<Result = Option<T>>,
    T: NamesJob,
    CoreWorkspaceHandle: Serves<O>,
{
    spawn_promise(env, async move {
        let job = workspace
            .call_optional_job::<O, T>(arguments(O::METHOD, &json)?)
            .await?;
        Ok(job.map(|job| JobHandle {
            inner: Arc::new(job),
        }))
    })
}

/// A stream-lane operation, as the events its caller demands one at a time.
fn stream_call<O, H>(env: Env, handle: Arc<H>, json: String) -> napi::Result<JsObject>
where
    O: StreamOperation,
    H: Serves<O> + Send + Sync + 'static,
{
    spawn_promise(env, async move {
        let events = call::call_stream::<O, H>(&*handle, arguments(O::METHOD, &json)?).await?;
        Ok(Events {
            open: Arc::new(Mutex::new(Some(Box::new(events)))),
            closed: watch::Sender::new(false),
        })
    })
}

/// The next event of one open stream-lane call, as its canonical JSON: [`EventStream`] with its
/// operation erased, so one JavaScript class reads every stream operation's events.
trait JsonEvents: Send {
    /// Demands the next event: `None` after the call's end, an error as its last item.
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<AddonResult<String>>> + Send + '_>>;
}

impl<O: StreamOperation> JsonEvents for EventStream<O> {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<AddonResult<String>>> + Send + '_>> {
        Box::pin(async move {
            let event = EventStream::next(self).await?;
            Some(
                event
                    .map_err(AddonFailure::from)
                    .and_then(|event| canonical_json(O::METHOD, &event)),
            )
        })
    }
}

/// One stream-lane call's events. `next` sends one demand and resolves to the event that
/// answers it, or to `null` once the call has ended; `close` ends the call — the subscription,
/// never what it observes — at once, even while a `next` waits for its event, which then
/// resolves to `null`. A stream that ended or failed is dropped at once, so its close sends
/// nothing; one JavaScript collects while it is still open is closed as it is dropped.
#[napi]
pub struct Events {
    /// The call while it is open. A demand holds the lock until its answer, so a second waits
    /// its turn instead of sending a demand while one is unanswered.
    open: Arc<Mutex<Option<Box<dyn JsonEvents>>>>,
    /// Set once by `close`: a demand waiting for its event gives it up and drops the call,
    /// which sends its close, and no later demand is sent.
    closed: watch::Sender<bool>,
}

#[napi]
impl Events {
    #[napi]
    pub fn next(&self, env: Env) -> napi::Result<JsObject> {
        let open = Arc::clone(&self.open);
        let mut closed = self.closed.subscribe();
        spawn_promise(env, async move {
            let mut open = open.lock().await;
            let step = match open.as_mut() {
                None => return Ok(None),
                Some(_) if *closed.borrow() => None,
                Some(events) => tokio::select! {
                    biased;
                    _ = closed.wait_for(|closed| *closed) => None,
                    step = events.next() => Some(step),
                },
            };
            match step {
                Some(Some(Ok(event))) => Ok(Some(event)),
                Some(Some(Err(failure))) => {
                    *open = None;
                    Err(failure)
                }
                // Ended, or closed while this demand waited: dropping a call still open sends
                // its close.
                Some(None) | None => {
                    *open = None;
                    Ok(None)
                }
            }
        })
    }

    #[napi]
    pub fn close(&self, env: Env) -> napi::Result<JsObject> {
        let open = Arc::clone(&self.open);
        self.closed.send_replace(true);
        spawn_promise(env, async move {
            // A waiting demand releases the lock as soon as it sees the close, having dropped
            // the call; otherwise the call is dropped here.
            open.lock().await.take();
            Ok(())
        })
    }
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

/// The host's own operations over one `cowshed host` connection (07_api.md, "Host operations").
#[napi]
pub struct Host {
    inner: Arc<CoreHost>,
    process: HostProcess,
}

#[napi]
impl Host {
    /// End the connection and wait for `cowshed host` to exit: it reads end of stream and ends.
    /// Every call still pending, and every call after this, rejects. Every close, concurrent ones
    /// included, waits for the same ending and answers with it.
    #[napi]
    pub fn close(&self, env: Env) -> napi::Result<JsObject> {
        let host = Arc::clone(&self.inner);
        let process = self.process.clone();
        spawn_promise(env, async move {
            host.close().await.map_err(AddonFailure::from)?;
            process.ended().await
        })
    }
}

/// The sccache daemon's control over one `cowshed sccache host` connection (07_api.md, "Host
/// operations"), a connection and a process of its own.
#[napi]
pub struct SccacheHost {
    inner: Arc<CoreSccacheHost>,
    process: HostProcess,
}

#[napi]
impl SccacheHost {
    /// End the connection and wait for `cowshed sccache host` to exit, as `Host.close` does.
    #[napi]
    pub fn close(&self, env: Env) -> napi::Result<JsObject> {
        let host = Arc::clone(&self.inner);
        let process = self.process.clone();
        spawn_promise(env, async move {
            host.close().await.map_err(AddonFailure::from)?;
            process.ended().await
        })
    }
}

/// Starts `launcher host` — the package's `bin/cowshed`, which picks the binary every verb runs
/// (06_cli.md, "Launcher") — on one end of a fresh socketpair, with this process's stdout and
/// stderr, and connects to it on the other. The child's stdin is the socket and nothing else;
/// closing the connection ends it, and a thread of its own reaps it.
#[napi(js_name = "openHost")]
pub fn open_host(env: Env, launcher: String) -> napi::Result<JsObject> {
    let started = HostProcess::start(&launcher, HostRole::Host);
    spawn_promise(env, async move {
        let (socket, process) = started?;
        let host = CoreHost::connect(socket)
            .await
            .map_err(AddonFailure::from)?;
        Ok(Host {
            inner: Arc::new(host),
            process,
        })
    })
}

/// Starts `launcher sccache host` as `openHost` starts `launcher host`: the launcher routes the
/// `sccache` verb to the installed build, so the daemon this controls is the one `cowshed sccache`
/// controls.
#[napi(js_name = "openSccacheHost")]
pub fn open_sccache_host(env: Env, launcher: String) -> napi::Result<JsObject> {
    let started = HostProcess::start(&launcher, HostRole::Sccache);
    spawn_promise(env, async move {
        let (socket, process) = started?;
        let host = CoreSccacheHost::connect(socket)
            .await
            .map_err(AddonFailure::from)?;
        Ok(SccacheHost {
            inner: Arc::new(host),
            process,
        })
    })
}

type HostReaped = tokio::sync::oneshot::Receiver<io::Result<std::process::ExitStatus>>;

/// How a host connection's process ended, once the thread that reaps it has said so.
enum HostExit {
    Pending(HostReaped),
    Ended(Result<std::process::ExitStatus, String>),
}

/// A host connection's process: its verb, and its ending, awaited under the lock by the first
/// close and kept for every close after it.
#[derive(Clone)]
struct HostProcess {
    verb: &'static str,
    exited: Arc<Mutex<HostExit>>,
}

impl HostProcess {
    fn start(launcher: &str, role: HostRole) -> AddonResult<(OwnedFd, Self)> {
        use cowshed_os::fork_lock::Spawn as _;
        let (verb, named): (&[&str], &'static str) = match role {
            HostRole::Host => (&["host"], "cowshed host"),
            HostRole::Sccache => (&["sccache", "host"], "cowshed sccache host"),
        };
        // Darwin has no SOCK_CLOEXEC: made under the fork lock, no spawn of this process inherits
        // the pair between the socketpair and the close-on-exec that follows it
        // (cowshed_os::fork_lock).
        let (ours, theirs) = cowshed_os::fork_lock::create(std::os::unix::net::UnixStream::pair)
            .map_err(|error| {
                AddonFailure::internal(format!(
                    "cannot make the {named} connection's socketpair: {error}"
                ))
            })?;
        let mut child = std::process::Command::new(launcher)
            .args(verb)
            .stdin(std::process::Stdio::from(OwnedFd::from(theirs)))
            .spawn_locked()
            .map_err(|error| {
                AddonFailure::usage(
                    format!("cannot start {launcher} {}: {error}", verb.join(" ")),
                    "build or install the package's cowshed binary (06_cli.md, Launcher)",
                )
            })?;
        let (reaped, exited) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            // A handle dropped without close has no one waiting for how its process ended, so an
            // ending that is not a clean exit is said here instead of lost.
            match reaped.send(child.wait()) {
                Ok(()) => {}
                Err(Ok(status)) if status.success() => {}
                Err(Ok(status)) => cowshed_os::eprintln!("cowshed: {named} ended with {status}"),
                Err(Err(error)) => {
                    cowshed_os::eprintln!(
                        "cowshed: the {named} process could not be reaped: {error}"
                    );
                }
            }
        });
        let process = Self {
            verb: named,
            exited: Arc::new(Mutex::new(HostExit::Pending(exited))),
        };
        Ok((OwnedFd::from(ours), process))
    }

    /// Wait for the process to end, after its connection closed. Held across the wait, so a
    /// concurrent close waits for the same ending and answers with it.
    async fn ended(&self) -> AddonResult<()> {
        let mut state = self.exited.lock().await;
        if let HostExit::Pending(reaped) = &mut *state {
            let ended = match reaped.await {
                Ok(Ok(status)) => Ok(status),
                Ok(Err(error)) => Err(format!(
                    "the {} process could not be reaped: {error}",
                    self.verb
                )),
                Err(_) => Err(format!("the {} reaper ended without an answer", self.verb)),
            };
            *state = HostExit::Ended(ended);
        }
        match &*state {
            HostExit::Ended(Ok(status)) if status.success() => Ok(()),
            HostExit::Ended(Ok(status)) => Err(AddonFailure::internal(format!(
                "{} ended with {status}",
                self.verb
            ))),
            HostExit::Ended(Err(message)) => Err(AddonFailure::internal(message.clone())),
            HostExit::Pending(_) => Err(AddonFailure::internal(format!(
                "the {} ending was not recorded",
                self.verb
            ))),
        }
    }
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

/// What an adopt made: main's reference, and what a re-adoption kept.
#[napi]
pub struct Adopted {
    workspace: Arc<CoreWorkspaceRef>,
    re_adoption: Option<String>,
}

#[napi]
impl Adopted {
    #[napi(getter)]
    pub fn workspace(&self) -> WorkspaceRef {
        WorkspaceRef {
            inner: Arc::clone(&self.workspace),
        }
    }

    /// What the re-adoption kept, as canonical JSON, or null when this adopt replaced nothing.
    #[napi(getter, js_name = "reAdoption")]
    pub fn re_adoption(&self) -> Option<String> {
        self.re_adoption.clone()
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

    /// The workspace incarnation this capability was minted for: the fence a coordinator names when it admits a job in it.
    #[napi(getter, js_name = "workspaceIncarnation")]
    pub fn workspace_incarnation(&self) -> String {
        self.inner.info().workspace_incarnation.as_str().to_owned()
    }
}

/// One job of one workspace incarnation.
#[napi]
pub struct JobHandle {
    inner: Arc<CoreJobHandle>,
}

/// A stdin view on the same durable job, retaining core's sole delivered-byte cursor.
/// Output remains available through JobHandle.logs; no second output projection is minted.
#[napi]
pub struct JobAttachment {
    inner: Arc<CoreJobHandle>,
    stdin: Arc<CoreJobStdin>,
}

fn attachment(job: Arc<CoreJobHandle>, attachment: CoreJobAttachment) -> JobAttachment {
    let (stdin, stdout, stderr) = attachment.into_parts();
    drop(stdout);
    drop(stderr);
    JobAttachment {
        inner: job,
        stdin: Arc::new(stdin),
    }
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
mod stdin_failure_tests {
    use super::*;
    use cowshed_core::StdinRefusal;

    #[test]
    fn every_canonical_stdin_refusal_survives_the_common_bridge() {
        for stdin in [
            StdinRefusal::Ended { cursor: 7 },
            StdinRefusal::Discontinuous {
                offset: 9,
                cursor: 7,
                admitted: 8,
            },
            StdinRefusal::ReplayMismatch {
                offset: 0,
                cursor: 7,
            },
            StdinRefusal::ReplayUnprovable {
                offset: 0,
                cursor: 7,
                retained_from: 4,
            },
            StdinRefusal::DeliveryUnknown { cursor: 7 },
        ] {
            let error = CowshedError::stdin_refusal(stdin, "opaque refusal", "inspect the input");
            assert!(error.retry_source().is_none());
            let error = AddonFailure::from(error);
            assert_eq!(error.0.code.as_str(), "conflict");
            assert_eq!(error.0.message, "opaque refusal");
            assert_eq!(error.0.hint, "inspect the input");
            assert_eq!(error.0.stdin_source(), Some(stdin));
        }
    }
}

#[cfg(test)]
mod wire_contract;

#[cfg(test)]
mod parity_tests {
    //! Option-level CLI/N-API parity is api-check's (`cowshed_api_gen::cli_parity`, 07_api.md
    //! "CLI and N-API option parity"): it reads the CLI grammar from `args.rs` source, so it can
    //! run without building the CLI. This is the check that what it reads is the tree clap itself
    //! builds, so no verb or argument reaches the parser through a shape the reader misses.

    use std::collections::BTreeSet;

    use cowshed_cli::args::{COMMANDS, cli_command};

    /// Every leaf verb and every argument, keyed as the parity table keys them: `*` for the
    /// options every verb accepts, an empty argument for a leaf verb itself.
    fn clap_words(command: &clap::Command, path: &str, words: &mut BTreeSet<(String, String)>) {
        let scope = if path.is_empty() { "*" } else { path };
        for argument in command.get_arguments() {
            words.insert((scope.to_owned(), argument.get_id().to_string()));
        }
        let mut subcommands = command.get_subcommands().peekable();
        if subcommands.peek().is_none() && !path.is_empty() {
            words.insert((path.to_owned(), String::new()));
        }
        for subcommand in subcommands {
            let child = if path.is_empty() {
                subcommand.get_name().to_owned()
            } else {
                format!("{path} {}", subcommand.get_name())
            };
            clap_words(subcommand, &child, words);
        }
    }

    #[test]
    fn the_parity_reader_sees_exactly_the_clap_tree() {
        let mut built = BTreeSet::new();
        clap_words(&cli_command(), "", &mut built);
        let read: BTreeSet<(String, String)> =
            cowshed_api_gen::cli_parity::read_cli(include_str!("../../cowshed-cli/src/args.rs"))
                .expect("the parity reader reads args.rs")
                .into_iter()
                .map(|word| (word.command, word.argument))
                .collect();
        assert_eq!(
            built.difference(&read).collect::<Vec<_>>(),
            Vec::<&(String, String)>::new(),
            "clap parses words the parity reader does not see"
        );
        assert_eq!(
            read.difference(&built).collect::<Vec<_>>(),
            Vec::<&(String, String)>::new(),
            "the parity reader sees words clap does not parse"
        );
    }

    /// The command map `cowshed --help` prints is the clap tree's verbs, so a verb parity covers
    /// is a verb the help names.
    #[test]
    fn the_command_map_names_every_clap_verb() {
        let command = cli_command();
        let verbs: BTreeSet<&str> = command
            .get_subcommands()
            .map(clap::Command::get_name)
            .collect();
        let mapped: BTreeSet<&str> = COMMANDS.iter().map(|spec| spec.name).collect();
        assert_eq!(verbs, mapped);
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
