//! A `WorkspaceHandle` calls a coordinator operation and a workspace-attach operation.
//! must name: Serves<cowshed_core::api::operations::CoordinatorDestroy>` is not implemented for `WorkspaceHandle`
//! must name: Serves<cowshed_core::api::operations::WorkspaceAttach>` is not implemented for `WorkspaceHandle`
use cowshed_core::WorkspaceHandle;
use cowshed_core::api::call::{Arguments, call};
use cowshed_core::api::operations::{CoordinatorDestroy, WorkspaceAttach};
pub async fn deny(value: &WorkspaceHandle) {
    let _ = call::<CoordinatorDestroy, _>(value, Arguments::new()).await;
    let _ = call::<WorkspaceAttach, _>(value, Arguments::new()).await;
}
