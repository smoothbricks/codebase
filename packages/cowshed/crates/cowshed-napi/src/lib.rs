//! Node-API bindings for the capability-safe cowshed client surface.

use std::{
    collections::HashMap,
    future::Future,
    io,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, Ordering},
    },
};

use cowshed_core::{
    Coordinator as CoreCoordinator, Cowshed, CowshedError, JobAttachment as CoreJobAttachment,
    JobHandle as CoreJobHandle, JobStream, Project as CoreProject, Session as CoreSession,
    WorkspaceHandle as CoreWorkspaceHandle, WorkspaceRef as CoreWorkspaceRef,
    api::{
        AdoptOptions, AttachOptions, CheckpointOptions, CreateOptions, ExecRequest, GcOptions,
        GrantDelta, JobId, LandOptions, MAX_JOB_ID, OutputPublication, PushOptions, RebaseOptions,
        RemoveOptions, RunSandboxMode, StdinSource, TraceContext, WorkspacePath,
    },
};
use napi::{
    Env, JsError, JsObject,
    bindgen_prelude::{Buffer, ToNapiValue},
};
use napi_derive::napi;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const CONSUMED_FD: i32 = -1;

struct AddonFailure {
    code: &'static str,
    message: String,
    hint: String,
}

// Constructors delegate to `CowshedError` so the addon can never invent a code spelling or a
// default hint that disagrees with core's taxonomy; this type exists only as the flattened form
// `to_napi_error` needs.
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
        Self {
            code: error.code.as_str(),
            message: error.message,
            hint: error.hint,
        }
    }
}

type AddonResult<T> = std::result::Result<T, AddonFailure>;

/// Hands JavaScript a `CowshedError`-shaped rejection: `code` from core's taxonomy, `message`
/// unmodified, and `hint` as a real property on the JS `Error`.
///
/// The hint used to be appended to `message` behind a `\nnext: ` delimiter that `src/index.ts`
/// split back off, which made one wire delimiter a literal in two languages and turned any
/// message containing that sequence into a mis-parsed hint. A property has no delimiter to agree
/// on, and `index.ts` reading `error.hint` directly means an error without one is no longer
/// dressed up with an invented hint.
fn to_napi_error(env: Env, failure: AddonFailure) -> napi::Error {
    let AddonFailure {
        code,
        message,
        hint,
    } = failure;
    if let Ok(hinted) = hinted_error(env, code, &message, &hint) {
        return hinted;
    }
    // Setting the property is the only fallible step, and only the environment can refuse it. The
    // code and message still have to reach JavaScript when it does; `index.ts` then declines to
    // recognise a hintless error as ours rather than inventing a hint for it.
    napi::Error::from(JsError::from(napi::Error::new(code, message)).into_unknown(env))
}

fn hinted_error(
    env: Env,
    code: &'static str,
    message: &str,
    hint: &str,
) -> napi::Result<napi::Error> {
    let mut error = JsError::from(napi::Error::new(code, message.to_owned()))
        .into_unknown(env)
        .coerce_to_object()?;
    error.set_named_property("hint", hint)?;
    Ok(napi::Error::from(error.into_unknown()))
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

fn parse_json<T: DeserializeOwned>(kind: &'static str, value: &str) -> AddonResult<T> {
    serde_json::from_str(value).map_err(|error| {
        AddonFailure::usage(
            format!("invalid {kind} JSON: {error}"),
            format!("pass a valid {kind} object"),
        )
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NapiExecRequest {
    argv: Vec<String>,
    #[serde(default)]
    cwd: Option<WorkspacePath>,
    #[serde(default)]
    mode: RunSandboxMode,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    trace: Option<TraceContext>,
    #[serde(default)]
    stdin: Option<String>,
    #[serde(default)]
    stdin_workspace_path: Option<WorkspacePath>,
    #[serde(default)]
    stdout_copy: Option<OutputPublication>,
    #[serde(default)]
    stderr_copy: Option<OutputPublication>,
}

impl TryFrom<NapiExecRequest> for ExecRequest {
    type Error = AddonFailure;

    fn try_from(request: NapiExecRequest) -> AddonResult<Self> {
        let stdin = match (request.stdin, request.stdin_workspace_path) {
            (Some(_), Some(_)) => {
                return Err(AddonFailure::usage(
                    "exec request cannot provide both stdin and stdinWorkspacePath",
                    "provide inline stdin or a workspace-relative stdinWorkspacePath",
                ));
            }
            (Some(stdin), None) => StdinSource::Inline(stdin.into_bytes().into()),
            (None, Some(path)) => StdinSource::WorkspaceFile(path),
            (None, None) => StdinSource::Empty,
        };
        Ok(Self {
            argv: request.argv.into_iter().map(Into::into).collect(),
            cwd: request.cwd,
            mode: request.mode,
            env: request.env,
            trace: request.trace,
            stdin,
            stdout_copy: request.stdout_copy,
            stderr_copy: request.stderr_copy,
        })
    }
}

async fn read_all_logs(mut logs: cowshed_core::RawByteStream) -> AddonResult<Buffer> {
    let mut output = Vec::new();
    while let Some(chunk) = logs.next().await {
        let chunk = chunk.map_err(AddonFailure::from)?;
        output.extend_from_slice(&chunk);
    }
    Ok(Buffer::from(output))
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
        Ok(Project { inner: project })
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

#[napi]
pub struct Coordinator {
    inner: Arc<CoreCoordinator>,
}

#[napi]
impl Coordinator {
    #[napi]
    pub fn adopt(&self, env: Env, options_json: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<AdoptOptions>("adopt options", &options_json)?;
            let workspace = coordinator
                .adopt(options)
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }

    #[napi]
    pub fn create(&self, env: Env, name: String, options_json: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<CreateOptions>("create options", &options_json)?;
            let workspace = coordinator
                .create(&name, options)
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }

    #[napi]
    pub fn fork(&self, env: Env, source: String, destination: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let workspace = coordinator
                .fork(&source, &destination)
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }
    #[napi]
    pub fn rename(&self, env: Env, source: String, destination: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let workspace = coordinator
                .rename(&source, &destination)
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }

    #[napi(js_name = "moveCheckout")]
    pub fn move_checkout(&self, env: Env, destination: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let workspace = coordinator
                .move_checkout(std::path::Path::new(&destination))
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }

    #[napi]
    pub fn grant(&self, env: Env, workspace: String, delta_json: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let delta = parse_json::<GrantDelta>("grant delta", &delta_json)?;
            let grants = coordinator
                .grant(&workspace, delta)
                .await
                .map_err(AddonFailure::from)?;
            canonical_json("grant set", &grants)
        })
    }

    #[napi]
    pub fn revoke(
        &self,
        env: Env,
        workspace: String,
        delta_json: String,
    ) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let delta = parse_json::<GrantDelta>("grant delta", &delta_json)?;
            let grants = coordinator
                .revoke(&workspace, delta)
                .await
                .map_err(AddonFailure::from)?;
            canonical_json("grant set", &grants)
        })
    }

    #[napi]
    pub fn rebase(
        &self,
        env: Env,
        workspace: String,
        options_json: String,
    ) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<RebaseOptions>("rebase options", &options_json)?;
            Ok(coordinator
                .rebase(&workspace, options)
                .await
                .map_err(AddonFailure::from)?
                .as_str()
                .to_owned())
        })
    }

    #[napi]
    pub fn land(
        &self,
        env: Env,
        workspace: String,
        options_json: String,
    ) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<LandOptions>("land options", &options_json)?;
            let report = coordinator
                .land(&workspace, options)
                .await
                .map_err(AddonFailure::from)?;
            canonical_json("land report", &report)
        })
    }

    #[napi]
    pub fn restore(&self, env: Env, workspace: String, label: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            coordinator
                .restore(&workspace, &label)
                .await
                .map_err(AddonFailure::from)
        })
    }

    #[napi]
    pub fn detach(&self, env: Env, workspace: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            coordinator
                .detach(&workspace)
                .await
                .map_err(AddonFailure::from)
                .map(|_| ())
        })
    }
    #[napi]
    pub fn resize(&self, env: Env, workspace: String, capacity: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let result = coordinator
                .resize(&workspace, &capacity)
                .await
                .map_err(AddonFailure::from)?;
            canonical_json("resize result", &result)
        })
    }

    #[napi]
    pub fn remove(
        &self,
        env: Env,
        workspace: String,
        options_json: String,
    ) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<RemoveOptions>("remove options", &options_json)?;
            let report = coordinator
                .destroy(&workspace, options)
                .await
                .map_err(AddonFailure::from)?;
            canonical_json("remove report", &report)
        })
    }

    #[napi]
    pub fn gc(&self, env: Env, options_json: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<GcOptions>("GC options", &options_json)?;
            let report = coordinator.gc(options).await.map_err(AddonFailure::from)?;
            canonical_json("GC report", &report)
        })
    }
    #[napi]
    pub fn doctor(&self, env: Env) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let report = coordinator.doctor().await.map_err(AddonFailure::from)?;
            canonical_json("doctor report", &report)
        })
    }

    #[napi]
    pub fn worker(&self, env: Env, workspace: String) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let worker = coordinator
                .worker(&workspace)
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceHandle {
                inner: Arc::new(worker),
            })
        })
    }
}

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
        self.inner
            .mount_path()
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                to_napi_error(
                    env,
                    AddonFailure::internal("controller returned a non-UTF-8 workspace mount path"),
                )
            })
    }

    #[napi]
    pub fn exec(&self, env: Env, request_json: String) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let request = parse_json::<NapiExecRequest>("exec request", &request_json)?;
            let job = worker
                .exec(request.try_into()?)
                .await
                .map_err(AddonFailure::from)?;
            Ok(JobHandle {
                inner: Arc::new(job),
            })
        })
    }

    #[napi]
    pub fn shell(&self, env: Env, session: Option<String>) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let session = worker
                .shell(session.as_deref())
                .await
                .map_err(AddonFailure::from)?;
            Ok(Session {
                inner: Arc::new(session),
            })
        })
    }

    #[napi(js_name = "listJobs")]
    pub fn list_jobs(&self, env: Env) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let jobs = worker.list_jobs().await.map_err(AddonFailure::from)?;
            canonical_json("job list", &jobs)
        })
    }

    #[napi]
    pub fn job(&self, env: Env, id: f64) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            // JS `number` is not a `u64`, so the finite/integral gate is the addon's job; the
            // bound is not, and is read from core rather than respelled as a shift. `JobId::new`
            // is the authority on the range and rejects anything this misses.
            if !id.is_finite() || id.fract() != 0.0 || id < 1.0 || id > MAX_JOB_ID as f64 {
                return Err(AddonFailure::usage(
                    format!("invalid job id {id}"),
                    "pass a positive safe integer job id",
                ));
            }
            let id = JobId::new(id as u64).map_err(|error| {
                AddonFailure::usage(error.to_string(), "pass a valid positive job id")
            })?;
            let job = worker.job(id).await.map_err(AddonFailure::from)?;
            Ok(JobHandle {
                inner: Arc::new(job),
            })
        })
    }
    #[napi]
    pub fn checkpoint(&self, env: Env, options_json: String) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<CheckpointOptions>("checkpoint options", &options_json)?;
            worker.checkpoint(options).await.map_err(AddonFailure::from)
        })
    }

    #[napi]
    pub fn push(&self, env: Env, options_json: String) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<PushOptions>("push options", &options_json)?;
            let report = worker.push(options).await.map_err(AddonFailure::from)?;
            canonical_json("push report", &report)
        })
    }

    #[napi(js_name = "grantsJson")]
    pub fn grants_json(&self, env: Env) -> napi::Result<JsObject> {
        let worker = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let grants = worker.grants().await.map_err(AddonFailure::from)?;
            canonical_json("workspace grants", &grants)
        })
    }
}

/// A shell session retains worker authority and can launch jobs through that named session.
#[napi]
pub struct Session {
    inner: Arc<CoreSession>,
}

#[napi]
impl Session {
    #[napi(getter, js_name = "isNamed")]
    pub fn is_named(&self) -> bool {
        self.inner.is_named()
    }

    #[napi]
    pub fn exec(&self, env: Env, request_json: String) -> napi::Result<JsObject> {
        let session = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let request = parse_json::<NapiExecRequest>("exec request", &request_json)?;
            let job = session
                .run(request.try_into()?)
                .await
                .map_err(AddonFailure::from)?;
            Ok(JobHandle {
                inner: Arc::new(job),
            })
        })
    }
}

#[napi]
pub struct JobHandle {
    inner: Arc<CoreJobHandle>,
}

#[napi]
impl JobHandle {
    #[napi(getter)]
    pub fn id(&self) -> f64 {
        self.inner.id().get() as f64
    }

    #[napi(js_name = "statusJson")]
    pub fn status_json(&self, env: Env) -> napi::Result<JsObject> {
        let job = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let status = job.status().await.map_err(AddonFailure::from)?;
            canonical_json("job status", &status)
        })
    }

    /// Returns a buffered stream. `follow` remains asynchronous and resolves when the stream closes.
    #[napi(js_name = "readLogs")]
    pub fn read_logs(&self, env: Env, stream: String, follow: bool) -> napi::Result<JsObject> {
        let job = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let stream = match stream.as_str() {
                "stdout" => JobStream::Stdout,
                "stderr" => JobStream::Stderr,
                _ => {
                    return Err(AddonFailure::usage(
                        format!("invalid job log stream {stream:?}"),
                        "use stdout or stderr",
                    ));
                }
            };
            read_all_logs(job.logs(stream, follow).await.map_err(AddonFailure::from)?).await
        })
    }

    #[napi]
    pub fn attach(&self, env: Env) -> napi::Result<JsObject> {
        let job = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let attachment = job.attach().await.map_err(AddonFailure::from)?;
            Ok(JobAttachment {
                inner: Arc::new(Mutex::new(Some(attachment))),
            })
        })
    }

    #[napi]
    pub fn detach(&self, env: Env) -> napi::Result<JsObject> {
        let job = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            job.detach().await.map_err(AddonFailure::from)
        })
    }

    #[napi]
    pub fn wait(&self, env: Env) -> napi::Result<JsObject> {
        let job = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let status = job.wait().await.map_err(AddonFailure::from)?;
            canonical_json("job status", &status)
        })
    }

    #[napi]
    pub fn kill(&self, env: Env) -> napi::Result<JsObject> {
        let job = Arc::clone(&self.inner);
        spawn_promise(
            env,
            async move { job.kill().await.map_err(AddonFailure::from) },
        )
    }
}

/// Active job attachment. Dropping it releases local stream receivers; call `detach` to notify
/// the controller explicitly that the attached session is finished.
#[napi]
pub struct JobAttachment {
    inner: Arc<Mutex<Option<CoreJobAttachment>>>,
}

#[napi]
impl JobAttachment {
    #[napi]
    pub fn detach(&self, env: Env) -> napi::Result<JsObject> {
        let attachment = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let attachment = attachment
                .lock()
                .map_err(|_| {
                    AddonFailure::from(CowshedError::new(
                        cowshed_core::ErrorCode::Internal,
                        "job attachment mutex was poisoned",
                        "restart the coordinator process",
                    ))
                })?
                .take()
                .ok_or_else(|| {
                    AddonFailure::conflict(
                        "job attachment has already been detached",
                        "create a new job attachment before detaching again",
                    )
                })?;
            attachment.detach().await.map_err(AddonFailure::from)
        })
    }
}

#[napi]
pub struct Project {
    inner: CoreProject,
}

#[napi]
impl Project {
    #[napi(getter, js_name = "repoId")]
    pub fn repo_id(&self) -> String {
        self.inner.repo_id().to_string()
    }

    #[napi(getter, js_name = "gitRoot")]
    pub fn git_root(&self, env: Env) -> napi::Result<String> {
        self.inner
            .git_root()
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                to_napi_error(
                    env,
                    AddonFailure::internal("controller returned a non-UTF-8 Git root"),
                )
            })
    }

    #[napi]
    pub fn main(&self, env: Env) -> napi::Result<JsObject> {
        let project = self.inner.clone();
        spawn_promise(env, async move {
            let workspace = project.main().await.map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }

    #[napi]
    pub fn workspace(&self, env: Env, name: String) -> napi::Result<JsObject> {
        let project = self.inner.clone();
        spawn_promise(env, async move {
            let workspace = project.workspace(&name).await.map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }
    #[napi(js_name = "workspaceAt")]
    pub fn workspace_at(&self, env: Env, path: String) -> napi::Result<JsObject> {
        let project = self.inner.clone();
        spawn_promise(env, async move {
            let workspace = project
                .workspace_at(std::path::Path::new(&path))
                .await
                .map_err(AddonFailure::from)?;
            Ok(WorkspaceRef { inner: workspace })
        })
    }

    #[napi]
    pub fn path(&self, env: Env, name: String, no_attach: bool) -> napi::Result<JsObject> {
        let project = self.inner.clone();
        spawn_promise(env, async move {
            let workspace = project.workspace(&name).await.map_err(AddonFailure::from)?;
            if !no_attach {
                workspace
                    .attach(AttachOptions::default())
                    .await
                    .map_err(AddonFailure::from)?;
            }
            canonical_json(
                "workspace info",
                &workspace.refresh_info().await.map_err(AddonFailure::from)?,
            )
        })
    }

    #[napi(js_name = "listWorkspaces")]
    pub fn list_workspaces(&self, env: Env) -> napi::Result<JsObject> {
        let project = self.inner.clone();
        spawn_promise(env, async move {
            let infos = project
                .list()
                .await
                .map_err(AddonFailure::from)?
                .into_iter()
                .map(CoreWorkspaceRef::into_info)
                .collect::<Vec<_>>();
            canonical_json("workspace list", &infos)
        })
    }
}

#[napi]
pub struct WorkspaceRef {
    inner: CoreWorkspaceRef,
}

#[napi]
impl WorkspaceRef {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.inner.name().to_string()
    }

    #[napi(getter, js_name = "mountPath")]
    pub fn mount_path(&self, env: Env) -> napi::Result<String> {
        self.inner
            .mount_path()
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                to_napi_error(
                    env,
                    AddonFailure::internal("controller returned a non-UTF-8 workspace mount path"),
                )
            })
    }

    #[napi(js_name = "infoJson")]
    pub fn info_json(&self, env: Env) -> napi::Result<JsObject> {
        let workspace = self.inner.clone();
        spawn_promise(env, async move {
            let info = workspace.refresh_info().await.map_err(AddonFailure::from)?;
            canonical_json("workspace info", &info)
        })
    }

    #[napi]
    pub fn attach(&self, env: Env, options_json: Option<String>) -> napi::Result<JsObject> {
        let workspace = self.inner.clone();
        spawn_promise(env, async move {
            let options =
                serde_json::from_str::<AttachOptions>(options_json.as_deref().unwrap_or("{}"))
                    .map_err(|error| {
                        AddonFailure::usage(
                            format!("invalid workspace attach options JSON: {error}"),
                            "pass attach options JSON such as {\"browse\":false}",
                        )
                    })?;
            workspace.attach(options).await.map_err(AddonFailure::from)
        })
    }

    #[napi(js_name = "grantsJson")]
    pub fn grants_json(&self, env: Env) -> napi::Result<JsObject> {
        let workspace = self.inner.clone();
        spawn_promise(env, async move {
            let grants = workspace
                .refresh_grants()
                .await
                .map_err(AddonFailure::from)?;
            canonical_json("workspace grants", &grants)
        })
    }
}

#[cfg(test)]
mod wire_contract;

#[cfg(test)]
mod parity_tests {
    use std::collections::BTreeSet;

    use cowshed_cli::args::{COMMANDS, Command, parse_args};
    use cowshed_core::api::StdinSource;

    /// Compile-visible seam over core's `StdinSource`: every variant must name the JS wire field
    /// that produces it, or state that it deliberately has none (`NapiExecRequest`'s `TryFrom`
    /// maps the other direction and therefore cannot be exhaustive over core). Adding a core
    /// variant breaks this match instead of silently becoming a mode the wire cannot express.
    fn wire_stdin_spelling(source: &StdinSource) -> &'static str {
        match source {
            StdinSource::Empty => "omit stdin and stdinWorkspacePath",
            StdinSource::Inline(_) => "stdin",
            StdinSource::WorkspaceFile(_) => "stdinWorkspacePath",
            StdinSource::Stream(_) => "(no wire spelling: process stdin is CLI-only)",
        }
    }

    #[test]
    fn every_core_stdin_variant_has_a_wire_verdict() {
        // All four, not just the two that were asserted before: an unasserted arm means the JS
        // field name it pins can be renamed without a single test going red, which is exactly
        // what `stdinWorkspacePath` needs protecting from.
        assert_eq!(
            wire_stdin_spelling(&StdinSource::Empty),
            "omit stdin and stdinWorkspacePath"
        );
        assert_eq!(
            wire_stdin_spelling(&StdinSource::Inline(Vec::new().into())),
            "stdin"
        );
        assert_eq!(
            wire_stdin_spelling(&StdinSource::WorkspaceFile(
                cowshed_core::api::WorkspacePath::new("fixtures/input.txt")
                    .expect("a relative fixture path")
            )),
            "stdinWorkspacePath"
        );
        assert_eq!(
            wire_stdin_spelling(&StdinSource::Stream(Box::pin(&b""[..]))),
            "(no wire spelling: process stdin is CLI-only)"
        );
    }

    /// The capability export a CLI verb corresponds to, or `None` when the verb has no export at
    /// all: host management runs the packaged binary through the `cli.ts` trampoline, and the
    /// addon deliberately does not link the CLI to offer a second in-process copy of it.
    ///
    /// Adding a `Command` variant breaks this match. Adding an arm without adding it to `SAMPLES`
    /// breaks `every_napi_export_is_exercised_by_a_sample`.
    fn napi_export(command: &Command) -> Option<&'static str> {
        match command {
            Command::Adopt(_) => Some("Coordinator.adopt"),
            Command::New(_) => Some("Coordinator.create"),
            Command::Fork(_) => Some("Coordinator.fork"),
            Command::Move(_) => Some("Coordinator.rename|Coordinator.moveCheckout"),
            Command::Checkpoint(_) => Some("WorkspaceHandle.checkpoint"),
            Command::Restore(_) => Some("Coordinator.restore"),
            Command::List(_) => Some("Project.listWorkspaces"),
            Command::Path(_) => Some("Project.path"),
            Command::Exec(_) => Some("WorkspaceHandle.exec"),
            Command::Remove(_) => Some("Coordinator.remove"),
            Command::Attach(_) => Some("WorkspaceRef.attach"),
            Command::Detach(_) => Some("Coordinator.detach"),
            Command::Resize(_) => Some("Coordinator.resize"),
            Command::Gc(_) => Some("Coordinator.gc"),
            Command::Push(_) => Some("WorkspaceHandle.push"),
            Command::Rebase(_) => Some("Coordinator.rebase"),
            Command::Land(_) => Some("Coordinator.land"),
            Command::Doctor(_) => Some("Coordinator.doctor"),
            Command::Gateway(_)
            | Command::Sccache(_)
            | Command::Skill(_)
            | Command::Setup(_)
            | Command::Version
            | Command::Help(_) => None,
        }
    }

    /// One representative argv per `Command` arm, paired with the export it must map to. The
    /// pairing is the assertion: the previous `!is_empty()` check passed for every arm that
    /// returned any literal at all, so no rename and no re-pointing of a verb could fail it.
    const SAMPLES: &[(&[&str], Option<&str>)] = &[
        (&["adopt"], Some("Coordinator.adopt")),
        (&["new", "parity"], Some("Coordinator.create")),
        (&["fork", "main", "parity"], Some("Coordinator.fork")),
        (
            &["mv", "parity", "renamed"],
            Some("Coordinator.rename|Coordinator.moveCheckout"),
        ),
        (
            &["checkpoint", "parity"],
            Some("WorkspaceHandle.checkpoint"),
        ),
        (&["restore", "parity", "saved"], Some("Coordinator.restore")),
        (&["ls"], Some("Project.listWorkspaces")),
        (&["path", "parity"], Some("Project.path")),
        (
            &["exec", "parity", "--", "true"],
            Some("WorkspaceHandle.exec"),
        ),
        (&["rm", "parity"], Some("Coordinator.remove")),
        (&["attach", "parity"], Some("WorkspaceRef.attach")),
        (&["detach", "parity"], Some("Coordinator.detach")),
        (&["resize", "parity", "200g"], Some("Coordinator.resize")),
        (&["gc"], Some("Coordinator.gc")),
        (&["push", "parity"], Some("WorkspaceHandle.push")),
        (&["rebase", "parity"], Some("Coordinator.rebase")),
        (&["land", "parity"], Some("Coordinator.land")),
        (&["doctor"], Some("Coordinator.doctor")),
        (&["gateway", "status"], None),
        (&["sccache", "status"], None),
        (&["skill", "install"], None),
        (&["help"], None),
        (&["setup"], None),
        (&["--version"], None),
    ];

    #[test]
    fn every_cli_command_maps_to_its_named_napi_export() {
        for (argv, expected) in SAMPLES {
            let parsed =
                parse_args(argv.iter().copied()).expect("representative CLI command parses");
            assert_eq!(
                napi_export(&parsed.command),
                *expected,
                "CLI command {argv:?} does not map to the export the parity table names"
            );
        }
    }

    /// Every verb the CLI dispatches must appear in the parity table.
    ///
    /// `COMMANDS` is the command map `cowshed --help` prints and the list the parser is generated
    /// from, so this is red the moment a verb is added without naming the capability export it
    /// corresponds to — which is the hole the old `!napi_export(..).is_empty()` left wide open.
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

    /// No two verbs may claim one export: that would mean the table has stopped describing the
    /// seam it is named after.
    #[test]
    fn no_two_verbs_claim_one_napi_export() {
        let mut claimed: Vec<&str> = SAMPLES.iter().filter_map(|(_, export)| *export).collect();
        let distinct: BTreeSet<&str> = claimed.iter().copied().collect();
        claimed.sort_unstable();

        assert_eq!(claimed.len(), distinct.len(), "duplicate N-API export claim");
    }
}
