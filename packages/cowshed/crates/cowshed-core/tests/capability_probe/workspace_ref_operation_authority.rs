//! A `WorkspaceRef` uploads for a worker exec.
//! must name: Serves<cowshed_core::api::operations::WorkerExec>` is not implemented for `WorkspaceRef`
use cowshed_core::WorkspaceRef;
use cowshed_core::api::call::{Arguments, call_upload};
use cowshed_core::api::operations::WorkerExec;
pub async fn deny(value: &WorkspaceRef) {
    let _ = call_upload::<WorkerExec, _>(value, Arguments::new(), None).await;
}
