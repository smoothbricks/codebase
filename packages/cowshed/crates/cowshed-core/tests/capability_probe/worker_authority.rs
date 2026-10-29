//! A `WorkspaceHandle` calls the coordinator's verbs and names another workspace.
//! must name: grant
//! must name: revoke
//! must name: destroy
//! must name: rebase
//! must name: land
//! must name: gc
//! must name: repo_mirror
//! must name: detach
//! must name: workspace
use cowshed_core::WorkspaceHandle;
fn deny(value: &WorkspaceHandle) {
    value.grant();
    value.revoke();
    value.destroy();
    value.rebase();
    value.land();
    value.gc();
    value.repo_mirror();
    value.detach();
    value.workspace("other");
}
