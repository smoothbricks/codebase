//! `cowshed __workspace-supervisor <project-root> <workspace>`: one workspace's supervisor as
//! a process of its own, serving the workspace's socket until a client retires it.
//!
//! Not a user verb and not in the command map: the process that owns a workspace's supervisor
//! starts it. It reads the project as a verb naming the workspace does but finishes nothing, so it
//! waits for no image lock ([`RecoveryScope::Supervisor`]). It mounts the workspace when it has
//! to and the workspace's lock is free, serves one that already stands mounted while a live
//! operation holds that lock, and then serves nothing but that supervisor.

use std::ffi::OsString;
use std::path::PathBuf;

use cowshed_core::metadata::WorkspaceName;
use cowshed_core::runtime::supervisor_manager::StartReport;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::{CowshedError, Result};

/// The argument that selects this verb.
pub const VERB: &str = "__workspace-supervisor";

/// Serve the workspace `arguments` name; the process's exit code. An error also goes to the
/// manager that started this process, which answers the command that asked for the workspace
/// with it.
pub async fn run(arguments: &[OsString]) -> i32 {
    // This thread runs the whole runtime: a log write the container stalls must not stall it.
    crate::gateway_service::detach_stderr("workspace supervisor");
    let report = StartReport::inherited();
    match serve(arguments).await {
        Ok(()) => 0,
        Err(error) => {
            cowshed_os::eprintln!("cowshed: {}", error.message);
            cowshed_os::eprintln!("hint: {}", error.hint);
            match report.send(&error) {
                // Nobody reads a report once the supervisor has served: the log has it.
                Err(sent) if sent.kind() != std::io::ErrorKind::BrokenPipe => {
                    cowshed_os::eprintln!(
                        "cowshed: could not report this to the cowshed daemon: {sent}"
                    );
                }
                _ => {}
            }
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
        RecoveryScope::Supervisor(workspace.clone()),
        crate::gateway_service::runtime_control,
    )
    .await?;
    let served = runtime.serve_supervisor(&workspace).await;
    let shutdown = runtime.shutdown().await;
    served.and(shutdown)
}
