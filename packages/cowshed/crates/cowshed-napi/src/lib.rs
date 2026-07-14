//! Node-API bindings for the capability-safe cowshed client surface.

use std::{
    future::Future,
    io,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
    sync::atomic::{AtomicI32, Ordering},
};

use cowshed_core::{
    Cowshed, CowshedError, Project as CoreProject, WorkspaceRef as CoreWorkspaceRef,
    api::AttachOptions,
};
use napi::{Env, JsError, JsObject, bindgen_prelude::ToNapiValue};
use napi_derive::napi;
use serde::Serialize;

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
