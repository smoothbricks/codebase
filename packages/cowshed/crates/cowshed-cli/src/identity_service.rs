//! Binding another remote of a project's main checkout as a repository identity.
//!
//! A dependency can name one repository by several URLs. The routes that fetch those URLs from the
//! local clone come only from the project's binding (`workspace_git_fetch`), never from checkout
//! configuration a workspace could write, so widening the binding is an operator's host command:
//! one configured remote at a time, refused when another project already binds the URL.

use std::path::Path;

use cowshed_core::api::IdentityReport;
use cowshed_core::runtime::project::{ProjectRuntime, RecoveryScope, bind_remote_identity};
use cowshed_core::{CowshedError, Result};

use crate::args::IdentityCommand;
use crate::output::Output;

pub async fn dispatch<W: std::io::Write + Send, E: std::io::Write + Send>(
    command: IdentityCommand,
    project_root: &Path,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    // The binding belongs to the project, not to a workspace: the open finishes only `main`'s
    // unfinished lifecycle work.
    let runtime =
        ProjectRuntime::open_existing(project_root, RecoveryScope::Workspaces(Default::default()))
            .await?;
    let outcome = match command {
        IdentityCommand::Add { remote } => {
            bind_remote_identity(runtime.descriptor(), &remote).await
        }
    };
    runtime.shutdown().await?;
    report(outcome?, json, output)
}

fn report<W: std::io::Write + Send, E: std::io::Write + Send>(
    report: IdentityReport,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32> {
    if json {
        output.success(report).map_err(write_error)?;
        return Ok(0);
    }
    let named = &report.identity;
    let url = named.remote_url.as_deref().unwrap_or_default();
    let remote = named.remote_name.as_deref().unwrap_or_default();
    output
        .announce(&if report.added {
            format!(
                "bound remote {remote} ({url}) as {} of {}",
                named.repo_id, report.repo_id
            )
        } else {
            format!(
                "remote {remote} ({url}) is already bound as {} of {}",
                named.repo_id, report.repo_id
            )
        })
        .map_err(write_error)?;
    for identity in &report.identities {
        let role = if identity.primary { "primary" } else { "bound" };
        let line = match (&identity.remote_name, &identity.remote_url) {
            (Some(name), Some(url)) => format!("{}  {role}  {name}  {url}", identity.repo_id),
            _ => format!("{}  {role}", identity.repo_id),
        };
        output.bare_line(line.as_bytes()).map_err(write_error)?;
    }
    output
        .note(
            "workspaces that can read this checkout fetch these URLs from it from their next exec",
        )
        .map_err(write_error)?;
    Ok(0)
}

fn write_error(error: std::io::Error) -> CowshedError {
    CowshedError::internal(format!("failed to write command result: {error}"))
}
