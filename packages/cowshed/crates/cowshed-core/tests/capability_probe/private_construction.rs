//! The authority types are built by struct literal.
//! must name: Cowshed
//! must name: Project
//! must name: WorkspaceHandle
//! must name: CoordinatorToken
//! must name: private
use cowshed_core::{CoordinatorToken, Cowshed, Project, WorkspaceHandle};
fn deny() {
    let _ = Cowshed {};
    let _ = Project {};
    let _ = WorkspaceHandle {};
    let _ = CoordinatorToken {};
}
