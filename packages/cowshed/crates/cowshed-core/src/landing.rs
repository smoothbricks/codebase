//! Whether a workspace's commits are already in the branch that outlives it.
//!
//! One module, two consumers that must never disagree: `ls` reports this so a human can triage, and
//! `rm` gates destruction on it. A listing that says "landed" where the gate would say "unlanded"
//! trains users to reach for `--abandon`; the reverse destroys work. So both read the same
//! measurement, taken the same way.
//!
//! Two properties are load-bearing:
//!
//! * **The target is read live.** The tip comes from the target workspace's own repository —
//!   main's, or the lane base a unit landed into — never from a `refs/remotes/*` cache inside the
//!   workspace. Those caches are clone-time snapshots; workspaces have been observed frozen
//!   hundreds of commits behind, each at a different commit, which produced confident and wrong
//!   verdicts in *both* directions.
//! * **Failure is a value, and it is never "landed".** Every way this can come up short is a real
//!   state of a real project — the target detached, its mount no longer a repository, no such
//!   branch — so each one becomes [`LandingCommits::Indeterminate`] carrying the reason.
//!   Consumers treat that exactly as they treat unlanded work.

use std::path::{Path, PathBuf};

use crate::api::dto::{BranchLanding, BranchLandings, GitOid, LandingCommits, WorkspaceLanding};
use crate::git::GitRepository;

/// The branch a workspace's work has to reach, resolved live, with the object store that holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandingTarget {
    branch: String,
    tip: GitOid,
    objects: PathBuf,
    /// The target's canonical mount, whose refs say which workspace branches it preserves.
    repository: PathBuf,
}

impl LandingTarget {
    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub const fn tip(&self) -> &GitOid {
        &self.tip
    }
}

/// Resolve `branch` from the target's own repository, or say why it could not be resolved.
///
/// `target_mount` is the canonical mount of the workspace the work has to reach — main's, or the
/// lane base a unit landed into — which is where the project ledger says that repository is. That
/// is the whole point: it routes around the workspace-side `main` remote, whose URL is a
/// clone-time artifact and has been observed pointing at a directory that is no longer a
/// repository at all.
///
/// The error is a sentence, not a code, because its only consumers are a human reading `ls` and a
/// refusal message explaining why a removal will not proceed. Both need the reason, neither needs
/// to branch on it.
pub async fn resolve_target(
    store_root: &std::sync::Arc<Path>,
    target_mount: &Path,
    branch: &str,
) -> std::result::Result<LandingTarget, String> {
    let target = GitRepository::from_root(store_root, target_mount);
    let objects = target.object_directory().await.map_err(|error| {
        format!(
            "the repository at {} could not be read: {}",
            target_mount.display(),
            error.message
        )
    })?;
    let tip = target
        .branch_tip(branch)
        .await
        .map_err(|error| {
            format!(
                "{branch} could not be read at {}: {}",
                target_mount.display(),
                error.message
            )
        })?
        .ok_or_else(|| {
            format!(
                "the repository at {} has no {branch} branch",
                target_mount.display()
            )
        })?;
    Ok(LandingTarget {
        branch: branch.to_owned(),
        tip,
        objects,
        repository: target_mount.to_path_buf(),
    })
}

/// Measure one workspace tree against `target`: its head, and every other local branch.
///
/// Total by construction: there is no error return, because every failure has a truthful
/// representation as [`LandingCommits::Indeterminate`] and no failure may be allowed to read as
/// landed. Folding errors inward is safe precisely because indeterminate is a refusal everywhere it
/// is consumed — it can never authorize a deletion, only decline to.
///
/// `head` is a revision in the workspace's own repository; pass `HEAD` unless a caller has already
/// fenced a specific oid and needs that exact commit judged. `workspace` names the workspace's
/// `push` refs in the target repository.
pub async fn measure(
    store_root: &std::sync::Arc<Path>,
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    workspace: &str,
    head: &str,
) -> WorkspaceLanding {
    let repository = GitRepository::from_root(store_root, workspace_mount);
    let dirty_files = repository.dirty_file_count().await.ok();
    WorkspaceLanding {
        dirty_files,
        commits: measure_commits(store_root, target, workspace_mount, head).await,
        branches: measure_branches(store_root, target, workspace_mount, workspace).await,
    }
}

/// Judge every local branch of the workspace except the checked-out one, which the head
/// measurement covers.
///
/// A branch passes when its tip equals or is an ancestor of the target repository's branch of the
/// same name or of `refs/cowshed/<workspace>/heads/<branch>`, where `cowshed push` preserves it -
/// destroying the workspace's copy then loses nothing - or when the target branch holds its
/// commits by ancestry or patch equivalence. Every other branch is named with its measurement. A
/// tip equal to the target's same-named tip is decided without a process, which is what keeps a
/// workspace carrying its source's branches cheap to list.
pub async fn measure_branches(
    store_root: &std::sync::Arc<Path>,
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    workspace: &str,
) -> BranchLandings {
    match judged_branches(store_root, target, workspace_mount, workspace).await {
        Ok(unlanded) => BranchLandings::Measured { unlanded },
        Err(reason) => BranchLandings::Indeterminate { reason },
    }
}

async fn judged_branches(
    store_root: &std::sync::Arc<Path>,
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    workspace: &str,
) -> std::result::Result<Vec<BranchLanding>, String> {
    let local = GitRepository::from_root(store_root, workspace_mount);
    let checked_out = local
        .current_branch()
        .await
        .map_err(|error| error.message)?
        .map(|branch| format!("refs/heads/{branch}").into_bytes());
    let branches = local
        .ref_tips(&["refs/heads"])
        .await
        .map_err(|error| error.message)?;
    let mut candidates = branches
        .into_iter()
        .filter(|(name, _)| Some(name) != checked_out.as_ref())
        .peekable();
    if candidates.peek().is_none() {
        return Ok(Vec::new());
    }
    let target = target.as_ref().map_err(Clone::clone)?;
    let preserving_namespace = format!("refs/cowshed/{workspace}/heads");
    let target_repository = GitRepository::from_root(store_root, &target.repository);
    let preserving = target_repository
        .ref_tips(&["refs/heads", preserving_namespace.as_str()])
        .await
        .map_err(|error| error.message)?;
    let mut unlanded = Vec::new();
    for (name, tip) in candidates {
        let short = name.strip_prefix(b"refs/heads/").unwrap_or(&name);
        let preserving_refs = [
            name.clone(),
            [preserving_namespace.as_bytes(), b"/".as_slice(), short].concat(),
        ];
        let mut preserved = false;
        for preserving_ref in &preserving_refs {
            let Some(preserving_tip) = preserving.get(preserving_ref) else {
                continue;
            };
            if *preserving_tip == tip
                || target_repository
                    .commit_is_ancestor(
                        &String::from_utf8_lossy(&tip),
                        &String::from_utf8_lossy(preserving_tip),
                    )
                    .await
                    .map_err(|error| error.message)?
            {
                preserved = true;
                break;
            }
        }
        if preserved {
            continue;
        }
        let tip = GitOid::new(String::from_utf8_lossy(&tip).into_owned()).map_err(|error| {
            format!("git listed a branch tip that is not an object id: {error}")
        })?;
        let commits = counted(store_root, target, workspace_mount, tip.as_str())
            .await
            .unwrap_or_else(|reason| LandingCommits::Indeterminate { reason });
        if !commits.fully_landed() {
            unlanded.push(BranchLanding {
                branch: String::from_utf8_lossy(short).into_owned(),
                tip,
                commits,
            });
        }
    }
    Ok(unlanded)
}

/// The commit half of [`measure`], separated so the gate can take it without a working-tree read it
/// already performed through its own fence.
pub async fn measure_commits(
    store_root: &std::sync::Arc<Path>,
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
    match counted(store_root, target, workspace_mount, head).await {
        Ok(commits) => commits,
        Err(reason) => LandingCommits::Indeterminate { reason },
    }
}

async fn counted(
    store_root: &std::sync::Arc<Path>,
    target: &LandingTarget,
    workspace_mount: &Path,
    head: &str,
) -> std::result::Result<LandingCommits, String> {
    let workspace = GitRepository::from_root(store_root, workspace_mount)
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
        .commits_already_held(tip, head)
        .await
        .map_err(|error| error.message)?;
    // `landed` counts a subset of the same range `ahead` counts, so this cannot underflow unless
    // git's own answers disagree with each other. If they ever do, say so rather than wrap into a
    // huge `unlanded` or a zero one — the second would authorize a deletion.
    let unlanded = ahead.checked_sub(landed).ok_or_else(|| {
        format!(
            "git reported {landed} already-held commits in a range of {ahead}: refusing to guess \
             which is right"
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
