//! Record-driven retirement drains and archives disk images, which only the macOS image
//! substrate provides: on Linux both entry points refuse, and the record stays for its host.

use crate::project_retirement::{ProjectRetirementReport, RetirementIntent};
use crate::repository::RepoId;
use crate::storage::bootstrap::ValidatedHostStorage;
use crate::{CowshedError, Result};

pub async fn forget_project(
    _storage: ValidatedHostStorage,
    _repo_id: RepoId,
) -> Result<ProjectRetirementReport> {
    Err(CowshedError::environment_missing(
        "record-driven project retirement requires the native macOS image substrate",
        "retain the project registration and run this operation on its macOS host",
    ))
}

pub(super) async fn execute(
    _storage: ValidatedHostStorage,
    _intent: RetirementIntent,
) -> Result<ProjectRetirementReport> {
    Err(CowshedError::environment_missing(
        "recorded image retirement requires its native macOS host",
        "retain the explicit retirement record and resume it on that host",
    ))
}
