//! `cowshed __workspace-supervisor <project-root> <workspace>`: one workspace's supervisor as
//! a process of its own, serving the workspace's socket until a client retires it.
//!
//! Not a user verb and not in the command map: the process that owns a workspace's supervisor
//! starts it. It opens the project exactly as any verb does, mounts the workspace if it has to,
//! and then serves nothing but that supervisor.

use std::ffi::OsString;
use std::path::PathBuf;

use cowshed_core::metadata::WorkspaceName;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::{CowshedError, Result};

/// The argument that selects this verb.
pub const VERB: &str = "__workspace-supervisor";

/// Serve the workspace `arguments` name; the process's exit code.
pub async fn run(arguments: &[OsString]) -> i32 {
    match serve(arguments).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("cowshed: {}", error.message);
            eprintln!("hint: {}", error.hint);
            i32::from(error.code.exit_code())
        }
    }
}

async fn serve(arguments: &[OsString]) -> Result<()> {
    let [project, workspace] = arguments else {
        return Err(CowshedError::usage(
            format!("{VERB} takes a project root and a workspace name"),
            "the cowshed daemon starts workspace supervisors; run commands with `cowshed exec`",
        ));
    };
    let workspace = workspace
        .to_str()
        .ok_or_else(|| CowshedError::usage("the workspace name is not UTF-8", "name a workspace"))
        .and_then(|name| {
            WorkspaceName::new(name)
                .map_err(|error| CowshedError::usage(error.to_string(), "name a workspace"))
        })?;
    let runtime = ProjectRuntime::open_existing(
        PathBuf::from(project),
        RecoveryScope::Workspaces(std::collections::BTreeSet::from([workspace.clone()])),
    )
    .await?;
    let served = runtime.serve_supervisor(&workspace).await;
    let shutdown = runtime.shutdown().await;
    served.and(shutdown)
}
