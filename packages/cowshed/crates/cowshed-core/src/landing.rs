//! Whether a workspace's commits are already in the branch that outlives it.
//!
//! One module, two consumers that must never disagree: `ls` reports this so a human can triage, and
//! `rm` gates destruction on it. A listing that says "landed" where the gate would say "unlanded"
//! trains users to reach for `--abandon`; the reverse destroys work. So both read the same
//! measurement, taken the same way.
//!
//! Two properties are load-bearing:
//!
//! * **The target is read live.** The tip comes from the project's own main workspace repository,
//!   never from a `refs/remotes/*` cache inside the workspace. Those caches are clone-time
//!   snapshots; workspaces have been observed frozen hundreds of commits behind, each at a
//!   different commit, which produced confident and wrong verdicts in *both* directions.
//! * **Failure is a value, and it is never "landed".** Every way this can come up short is a real
//!   state of a real project — main detached, its mount no longer a repository, no such branch — so
//!   each one becomes [`LandingCommits::Indeterminate`] carrying the reason. Consumers treat that
//!   exactly as they treat unlanded work.

use std::path::{Path, PathBuf};

use crate::api::dto::{GitOid, LandingCommits, WorkspaceLanding};
use crate::git::GitRepository;

/// The branch a workspace's work has to reach, resolved live, with the object store that holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandingTarget {
    branch: String,
    tip: GitOid,
    objects: PathBuf,
}

impl LandingTarget {
    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub const fn tip(&self) -> &GitOid {
        &self.tip
    }
}

/// Resolve `branch` from main's own repository, or say why it could not be resolved.
///
/// `main_mount` is main's *canonical mount*, which is where the project ledger says main's
/// repository is. That is the whole point: it routes around the workspace-side `main` remote, whose
/// URL is a clone-time artifact and has been observed pointing at a directory that is no longer a
/// repository at all.
///
/// The error is a sentence, not a code, because its only consumers are a human reading `ls` and a
/// refusal message explaining why a removal will not proceed. Both need the reason, neither needs
/// to branch on it.
pub async fn resolve_target(
    main_mount: &Path,
    branch: &str,
) -> std::result::Result<LandingTarget, String> {
    let main = GitRepository::from_root(main_mount);
    let objects = main.object_directory().await.map_err(|error| {
        format!(
            "main's repository at {} could not be read: {}",
            main_mount.display(),
            error.message
        )
    })?;
    let tip = main
        .branch_tip(branch)
        .await
        .map_err(|error| format!("main's {branch} could not be read: {}", error.message))?
        .ok_or_else(|| format!("main's repository has no {branch} branch"))?;
    let tip = GitOid::new(tip).map_err(|error| format!("main's {branch} tip is unusable: {error}"))?;
    Ok(LandingTarget {
        branch: branch.to_owned(),
        tip,
        objects,
    })
}

/// Measure one workspace tree against `target`.
///
/// Total by construction: there is no error return, because every failure has a truthful
/// representation as [`LandingCommits::Indeterminate`] and no failure may be allowed to read as
/// landed. Folding errors inward is safe precisely because indeterminate is a refusal everywhere it
/// is consumed — it can never authorize a deletion, only decline to.
///
/// `head` is a revision in the workspace's own repository; pass `HEAD` unless a caller has already
/// fenced a specific oid and needs that exact commit judged.
pub async fn measure(
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    head: &str,
) -> WorkspaceLanding {
    let workspace = GitRepository::from_root(workspace_mount);
    let dirty_files = workspace.dirty_file_count().await.ok();
    WorkspaceLanding {
        dirty_files,
        commits: measure_commits(target, workspace_mount, head).await,
    }
}

/// The commit half of [`measure`], separated so the gate can take it without a working-tree read it
/// already performed through its own fence.
pub async fn measure_commits(
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    head: &str,
) -> LandingCommits {
    let target = match target {
        Ok(target) => target,
        Err(reason) => {
            return LandingCommits::Indeterminate {
                reason: reason.clone(),
            };
        }
    };
    match counted(target, workspace_mount, head).await {
        Ok(commits) => commits,
        Err(reason) => LandingCommits::Indeterminate { reason },
    }
}

async fn counted(
    target: &LandingTarget,
    workspace_mount: &Path,
    head: &str,
) -> std::result::Result<LandingCommits, String> {
    let workspace = GitRepository::from_root(workspace_mount)
        .with_alternate_objects(&target.objects)
        .map_err(|error| error.message)?;
    let tip = target.tip.as_str();
    // Strict on both endpoints: if the workspace cannot see main's tip even with main's object
    // store attached, the comparison has not been made and must not be reported as if it had.
    let ahead = workspace
        .commits_ahead_of(tip, head)
        .await
        .map_err(|error| error.message)?;
    let behind = workspace
        .commits_ahead_of(head, tip)
        .await
        .map_err(|error| error.message)?;
    let landed = workspace
        .patch_equivalent_count(tip, head)
        .await
        .map_err(|error| error.message)?;
    // `landed` counts a subset of the same range `ahead` counts, so this cannot underflow unless
    // git's own two answers disagree. If they ever do, say so rather than wrap into a huge
    // `unlanded` or a zero one — the second would authorize a deletion.
    let unlanded = ahead.checked_sub(landed).ok_or_else(|| {
        format!(
            "git reported {landed} patch-equivalent commits in a range of {ahead}: refusing to \
             guess which is right"
        )
    })?;
    Ok(LandingCommits::Measured {
        target_branch: target.branch.clone(),
        target_head: target.tip.clone(),
        unlanded,
        landed,
        behind,
    })
}
