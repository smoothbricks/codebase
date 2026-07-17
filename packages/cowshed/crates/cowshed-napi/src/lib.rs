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
        AdoptOptions, AttachOptions, CreateOptions, ExecRequest, GcOptions, GrantDelta, JobId,
        LandOptions, OutputPublication, PushOptions, RebaseOptions, RemoveOptions, RunSandboxMode,
        StdinSource, TraceContext, WorkspacePath,
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

impl AddonFailure {
    fn usage(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            code: "usage",
            message: message.into(),
            hint: hint.into(),
        }
    }

    fn conflict(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            code: "conflict",
            message: message.into(),
            hint: hint.into(),
        }
    }

    fn internal(message: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            code: "internal",
            message: message.into(),
            hint: hint.into(),
        }
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

fn to_napi_error(env: Env, failure: AddonFailure) -> napi::Error {
    let AddonFailure {
        code,
        message,
        hint,
    } = failure;
    let reason = format!("{message}\nnext: {hint}");
    let error = JsError::from(napi::Error::new(code, reason)).into_unknown(env);
    napi::Error::from(error)
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
    serde_json::to_string(value).map_err(|error| {
        AddonFailure::internal(
            format!("failed to serialize {kind}: {error}"),
            "cowshed doctor --json",
        )
    })
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
    mode: NapiRunSandboxMode,
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

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
enum NapiRunSandboxMode {
    #[default]
    ReadWrite,
    ReadOnly,
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
            mode: match request.mode {
                NapiRunSandboxMode::ReadWrite => RunSandboxMode::ReadWrite,
                NapiRunSandboxMode::ReadOnly => RunSandboxMode::ReadOnly,
            },
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
    pub fn destroy(
        &self,
        env: Env,
        workspace: String,
        options_json: String,
    ) -> napi::Result<JsObject> {
        let coordinator = Arc::clone(&self.inner);
        spawn_promise(env, async move {
            let options = parse_json::<RemoveOptions>("remove options", &options_json)?;
            coordinator
                .destroy(&workspace, options)
                .await
                .map_err(AddonFailure::from)
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
                    AddonFailure::internal(
                        "controller returned a non-UTF-8 workspace mount path",
                        "cowshed doctor --json",
                    ),
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
            if !id.is_finite() || id.fract() != 0.0 || id < 1.0 || id > ((1_u64 << 53) - 1) as f64 {
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
                    AddonFailure::internal(
                        "job attachment mutex was poisoned",
                        "restart the coordinator process",
                    )
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
                    AddonFailure::internal(
                        "controller returned a non-UTF-8 Git root",
                        "cowshed doctor --json",
                    ),
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
                    AddonFailure::internal(
                        "controller returned a non-UTF-8 workspace mount path",
                        "cowshed doctor --json",
                    ),
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

    #[napi(js_name = "ensureJson")]
    pub fn ensure_json(&self, env: Env) -> napi::Result<JsObject> {
        let workspace = self.inner.clone();
        spawn_promise(env, async move {
            let report = workspace.ensure().await.map_err(AddonFailure::from)?;
            canonical_json("workspace ensure report", &report)
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
