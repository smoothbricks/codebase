//! `cowshed forget <owner/repo> --abandon`: retire one project's registration from store records
//! (06_cli.md, "Project retirement").
//!
//! The retirement itself — the durable authorization record, the fence on new opens, the
//! supervisor drain, the unforced image detach and the rename that preserves the whole project
//! tree — is `cowshed_core::project_retirement`'s. This module is the CLI's shape around it:
//! which storage it resolves, what it tells the operator before and after, and what it prints.
//!
//! The project is named by its repository identity alone and the storage is the host's existing,
//! already-validated store, never a setup and never a remount. There is no checkout to be missing:
//! a vanished or corrupt checkout is the case this verb exists for, so nothing here asks for one.

use async_trait::async_trait;
use cowshed_core::project_retirement::ProjectRetirementReport;
use cowshed_core::{CowshedError, Result, ValidatedHostStorage};
use cowshed_types::repository::RepoId;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;

use crate::output::Output;
use crate::runtime::DispatchExit;

/// The retirement behind [`dispatch_forget`], so tests can prove the verb's contract without a
/// host store.
#[async_trait]
pub trait ForgetBackend: Send + Sync {
    /// Retire `repo_id` on the operator's explicit abandonment, or finish the retirement already
    /// recorded for it.
    async fn forget_project(&self, repo_id: &RepoId) -> Result<ProjectRetirementReport>;
}

/// The production backend: core's record-driven retirement over the host's existing storage.
pub struct NativeForgetBackend {
    storage: ValidatedHostStorage,
}

impl NativeForgetBackend {
    pub fn new(storage: ValidatedHostStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl ForgetBackend for NativeForgetBackend {
    async fn forget_project(&self, repo_id: &RepoId) -> Result<ProjectRetirementReport> {
        cowshed_core::project_retirement::native::forget_project(
            self.storage.clone(),
            repo_id.clone(),
        )
        .await
    }
}

/// The sentence that says a project was retired, for the operator's terminal and the gateway's
/// startup log alike.
///
/// It says "preserved", never "deleted": the whole project tree stands unchanged at the archive,
/// and `images` counts the backing images that tree carries, every one of them detached.
#[must_use]
pub fn retired(report: &ProjectRetirementReport) -> String {
    let noun = if report.images == 1 {
        "image"
    } else {
        "images"
    };
    format!(
        "retired {}: {} {noun} detached; its whole project tree is preserved at {}",
        report.repo_id,
        report.images,
        report.archive.display()
    )
}

/// Retire `repo_id` and report where its tree was preserved.
///
/// The authorization is the caller's: the parser refuses a `forget` without `--abandon`, so
/// reaching this function is the operator's explicit request, and nothing here adds to it or
/// stands in for it — no checkout, marker or pid is consulted. stdout is the archive path, or with
/// `--json` the ordinary envelope around the typed report; everything else is guidance on stderr.
pub async fn dispatch_forget<B, W, E>(
    backend: &B,
    repo_id: &RepoId,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    B: ForgetBackend,
    W: Write,
    E: Write,
{
    // Said before the first side effect: what `--abandon` confirmed, and what is about to happen.
    output
        .guidance(&format!(
            "retiring {repo_id} on your explicit --abandon: new opens are refused, running \
             supervisors drained, each backing image detached without force, then its whole \
             project tree preserved in an archive"
        ))
        .map_err(announce_error)?;
    let report = backend.forget_project(repo_id).await?;
    let sentence = retired(&report);
    let wrote = |error: std::io::Error| report_error(repo_id, error);
    if json {
        output.success(report).map_err(wrote)?;
    } else {
        output
            .bare_line(report.archive.as_os_str().as_bytes())
            .map_err(wrote)?;
    }
    output.guidance(&sentence).map_err(wrote)?;
    output.hint("cowshed ls --all").map_err(wrote)?;
    Ok(DispatchExit { code: 0 })
}

/// A failed write before the retirement started: nothing has changed, and the message says so by
/// being about the guidance rather than about a result.
fn announce_error(error: std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("could not write forget's guidance: {error}"),
        "check that the output consumer is still connected",
    )
}

/// A failed write after the retirement finished. The retirement is durable by now, so the refusal
/// says it happened instead of implying nothing did.
fn report_error(repo_id: &RepoId, error: std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("{repo_id} was retired, but the result could not be written: {error}"),
        "check that the output consumer is still connected",
    )
}
