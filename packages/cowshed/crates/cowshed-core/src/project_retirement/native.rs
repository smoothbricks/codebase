//! Explicit abandonment of a recorded project, independent of its checkout and volatile owners.
//! The record fences admission; supervisor/socket and file leases prove quiescence. Nothing here
//! guesses whether a PID is stale, follows a checkout path, or deletes preserved image/Git bytes.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub use platform::forget_project;

/// Forward recovery only for existing positive authorizations. It never turns a missing record,
/// stale PID, vanished checkout or interrupted fixture into permission to abandon a project.
pub async fn resume_retirements(
    storage: crate::storage::bootstrap::ValidatedHostStorage,
) -> crate::Result<
    Vec<(
        crate::repository::RepoId,
        crate::Result<super::ProjectRetirementReport>,
    )>,
> {
    let store = storage.store().to_owned();
    let intents = crate::storage::lifecycle::dispatch_blocking(move || {
        super::outstanding_retirements(&store)
    })
    .await
    .map_err(|error| {
        crate::CowshedError::internal(format!("reading authorized project retirements: {error}"))
    })??;
    let mut outcomes = Vec::with_capacity(intents.len());
    for intent in intents {
        let repo_id = intent.repo_id.clone();
        let outcome = platform::execute(storage.clone(), intent).await;
        outcomes.push((repo_id, outcome));
    }
    Ok(outcomes)
}

/// [`super::reclaim_unbound_leases`] off the async runtime: the leases it removed.
pub async fn reclaim_unbound_leases(
    storage: crate::storage::bootstrap::ValidatedHostStorage,
) -> crate::Result<Vec<std::path::PathBuf>> {
    let store = storage.store().to_owned();
    crate::storage::lifecycle::dispatch_blocking(move || super::reclaim_unbound_leases(&store))
        .await
        .map_err(|error| {
            crate::CowshedError::internal(format!("reclaiming unbound project leases: {error}"))
        })?
}
