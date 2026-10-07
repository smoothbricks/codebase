//! Calling a declared operation through a handle, with the caller naming only the request fields
//! the handle does not bind.
//!
//! A handle's authority is the operations it may call and the request fields it supplies itself:
//! a [`Coordinator`] its repository, a [`WorkspaceRef`] its workspace, a [`WorkspaceHandle`] its
//! workspace incarnation, a [`JobHandle`] its job as well. A reference still binds the
//! incarnation it was resolved at: a request that requires one carries it, while one that leaves
//! it optional is sent without it, so a stale reference reads by name and no caller chooses an
//! incarnation. What a handle may call and what it binds are one fact per operation, [`Serves`],
//! which `cowshed-api-gen` generates from the operation table and nothing else can implement: a
//! handle whose authority does not admit an operation cannot name it, and the fields it binds are
//! the declaration's, never the caller's choice. [`call`] refuses a caller field that names a
//! bound one, then builds the declared request from the handle's own validated fields and the
//! caller's, decoded as exactly the request's remaining fields — the generated construction, never
//! a second field list, decides what the call accepts.

use super::capability::{
    ControllerRuntime, Coordinator, EventStream, JobHandle, Project, WorkspaceHandle, WorkspaceRef,
    invoke, invoke_download, invoke_stream, invoke_upload,
};
use super::dto::{JobId, JobInfo, WorkspaceIncarnation};
use super::operations::{
    Lane, LogsChunk, Operation, Scope, StreamOperation, WorkerView, WorkspaceView,
};
use crate::error::{CowshedError, Result};
use crate::metadata::WorkspaceName;
use crate::repository::RepoId;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;

/// A caller's fields of an operation's request: every field but those its handle binds.
pub type Arguments = serde_json::Map<String, Value>;

/// A handle that supplies part of each request from its own authority.
pub trait Binder: sealed::Sealed {
    /// The request fields the handle supplies, borrowed from the authority it was minted with.
    #[doc(hidden)]
    type Fields<'a>
    where
        Self: 'a;

    #[doc(hidden)]
    fn binding(&self) -> Binding<'_, Self::Fields<'_>>;
}

/// A handle whose authority admits `O`, the request fields of `O` it binds, and `O`'s request
/// built from them. Generated from the operation table; sealed through [`Binder`] and
/// [`Operation`], so no other crate can grant a handle an operation.
pub trait Serves<O: Operation>: Binder {
    /// The bound fields as the wire spells them, those supplied as `None` included: a caller naming
    /// one is refused.
    #[doc(hidden)]
    const BOUND: &'static [&'static str];

    /// `O`'s request: the bound fields cloned from `authority`, except an optional one outside the
    /// handle's fence, which is `None`; the rest decoded from `arguments`.
    #[doc(hidden)]
    fn request(authority: &Self::Fields<'_>, arguments: Arguments) -> Result<O::Request>;
}

mod sealed {
    pub trait Sealed {}
}

/// What a handle binds: the controller connection it calls through, and its authority's fields.
#[doc(hidden)]
pub struct Binding<'a, F> {
    pub(super) runtime: &'a Arc<dyn ControllerRuntime>,
    pub(super) authority: F,
}

/// The fields a [`Project`] or [`Coordinator`] binds: its repository.
#[doc(hidden)]
pub struct RepoFields<'a> {
    pub(super) repo_id: &'a RepoId,
}

/// The fields a [`WorkspaceRef`] or [`WorkspaceHandle`] binds: one workspace incarnation. A
/// reference fences only the workspace; a handle fences the whole incarnation.
#[doc(hidden)]
pub struct WorkspaceFields<'a> {
    pub(super) repo_id: &'a RepoId,
    pub(super) workspace: &'a WorkspaceName,
    pub(super) workspace_incarnation: &'a WorkspaceIncarnation,
}

/// The fields a [`JobHandle`] binds: its workspace incarnation and its job.
#[doc(hidden)]
pub struct JobFields<'a> {
    pub(super) repo_id: &'a RepoId,
    pub(super) workspace: &'a WorkspaceName,
    pub(super) workspace_incarnation: &'a WorkspaceIncarnation,
    pub(super) job_id: &'a JobId,
}

impl sealed::Sealed for Project {}
impl sealed::Sealed for WorkspaceRef {}
impl sealed::Sealed for Coordinator {}
impl sealed::Sealed for WorkspaceHandle {}
impl sealed::Sealed for JobHandle {}

/// A bound field's value, owned for the request: one clone of what the handle validated, whatever
/// the field's type, so the generated construction need not know which fields are `Copy`.
pub(super) fn owned<T: Clone>(value: &T) -> T {
    value.clone()
}

/// Decodes the caller's fields of `O`'s request as `C`, the generated record of exactly those
/// fields.
pub(super) fn decode<O: Operation, C: DeserializeOwned>(arguments: Arguments) -> Result<C> {
    C::deserialize(Value::Object(arguments)).map_err(|error| {
        CowshedError::usage(
            format!("invalid {} arguments: {error}", O::METHOD),
            "pass the operation's declared request fields",
        )
    })
}

/// The connection `handle` calls through, and `O`'s request: the fields `H` binds for it, taken
/// from the handle, and the caller's `arguments`.
fn bind<O: Operation, H: Serves<O>>(
    handle: &H,
    arguments: Arguments,
) -> Result<(&Arc<dyn ControllerRuntime>, O::Request)> {
    if let Some(field) = H::BOUND
        .iter()
        .find(|field| arguments.contains_key(**field))
    {
        return Err(CowshedError::usage(
            format!(
                "{} arguments name {field}, which the handle supplies",
                O::METHOD
            ),
            "omit the fields the handle binds; call through the handle that holds them",
        ));
    }
    let binding = handle.binding();
    let request = H::request(&binding.authority, arguments)?;
    Ok((binding.runtime, request))
}

/// Calls a JSON-lane operation through `handle`.
pub async fn call<O: Operation, H: Serves<O>>(
    handle: &H,
    arguments: Arguments,
) -> Result<O::Result> {
    const { assert!(matches!(O::LANE, Lane::Json) && !matches!(O::SCOPE, Scope::Internal)) };
    let (runtime, request) = bind::<O, H>(handle, arguments)?;
    invoke::<O>(&**runtime, &request).await
}

/// Calls an upload-lane operation through `handle`, with `frame` as its raw-byte frame when there
/// is one.
pub async fn call_upload<O: Operation, H: Serves<O>>(
    handle: &H,
    arguments: Arguments,
    frame: Option<Bytes>,
) -> Result<O::Result> {
    const { assert!(matches!(O::LANE, Lane::Upload) && !matches!(O::SCOPE, Scope::Internal)) };
    let (runtime, request) = bind::<O, H>(handle, arguments)?;
    match frame {
        Some(bytes) => invoke_upload::<O>(&**runtime, &request, bytes).await,
        None => invoke::<O>(&**runtime, &request).await,
    }
}

/// Calls a download-lane operation through `handle`: the chunk's metadata and its bytes, which
/// start at the offset the request declares.
pub async fn call_download<O: Operation<Result = LogsChunk>, H: Serves<O>>(
    handle: &H,
    arguments: Arguments,
) -> Result<(LogsChunk, Bytes)> {
    const { assert!(matches!(O::LANE, Lane::Download) && !matches!(O::SCOPE, Scope::Internal)) };
    let (runtime, request) = bind::<O, H>(handle, arguments)?;
    invoke_download::<O>(&**runtime, request).await
}

/// Opens a call of a stream-lane operation through `handle`: its events, each sent for one
/// demand. Dropping the stream before its end closes the call.
pub async fn call_stream<O: StreamOperation, H: Serves<O>>(
    handle: &H,
    arguments: Arguments,
) -> Result<EventStream<O>> {
    const { assert!(matches!(O::LANE, Lane::Stream) && !matches!(O::SCOPE, Scope::Internal)) };
    let (runtime, request) = bind::<O, H>(handle, arguments)?;
    invoke_stream::<O>(&**runtime, &request).await
}

/// Calls a JSON-lane operation whose result is one workspace, as a reference to it.
pub async fn call_workspace<O: Operation<Result = WorkspaceView>, H: Serves<O>>(
    handle: &H,
    arguments: Arguments,
) -> Result<WorkspaceRef> {
    let runtime = Arc::clone(handle.binding().runtime);
    let view = call::<O, H>(handle, arguments).await?;
    Ok(WorkspaceRef::from_view(view, runtime))
}

impl Coordinator {
    /// Calls the operation that mints a worker capability, as that capability.
    pub async fn call_worker<O: Operation<Result = WorkerView>>(
        &self,
        arguments: Arguments,
    ) -> Result<WorkspaceHandle>
    where
        Self: Serves<O>,
    {
        let WorkerView(view) = call::<O, Self>(self, arguments).await?;
        Ok(self.worker_handle(view))
    }
}

/// A result that names exactly one job of the workspace it was answered for.
pub trait NamesJob {
    fn job_id(&self) -> JobId;
}

impl NamesJob for JobId {
    fn job_id(&self) -> JobId {
        *self
    }
}

impl NamesJob for JobInfo {
    fn job_id(&self) -> JobId {
        self.job_id
    }
}

impl WorkspaceHandle {
    /// Calls a JSON-lane operation of this workspace whose result names one of its jobs, as a
    /// handle to that job.
    pub async fn call_job<O: Operation<Result: NamesJob>>(
        &self,
        arguments: Arguments,
    ) -> Result<JobHandle>
    where
        Self: Serves<O>,
    {
        let result = call::<O, Self>(self, arguments).await?;
        Ok(self.job_handle(result.job_id()))
    }

    /// [`Self::call_job`] for an upload-lane operation, with `frame` as its raw-byte frame when
    /// there is one.
    pub async fn call_job_upload<O: Operation<Result: NamesJob>>(
        &self,
        arguments: Arguments,
        frame: Option<Bytes>,
    ) -> Result<JobHandle>
    where
        Self: Serves<O>,
    {
        let result = call_upload::<O, Self>(self, arguments, frame).await?;
        Ok(self.job_handle(result.job_id()))
    }
}
