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
//! * **Landed is content, not commit identity.** Work reaches a target by cherry-pick, squash,
//!   rewrite, or a pick whose conflict a lander resolved; the measurement names each file whose
//!   change the target still lacks, and why, so a refusal says what would be lost.
//!
//! One reading here is not a verdict: [`published_holding`] measures the target repository's own
//! tracking ref for its published branch, the same way, so a refusal can say when that ref already
//! holds every commit of the workspace head that the live target does not. It is read only to
//! explain a refusal and decides nothing — `rm`'s gate is the live target alone
//! (`specs/cowshed/02_workspaces.md`, `rm` step 1).

use std::path::{Path, PathBuf};

use crate::api::dto::{
    AbandonedRefs, AbandonedTip, BranchLanding, BranchLandings, GitOid, LandingCommits,
    PreservedRef, UnlandedPaths, WorkspaceLanding,
};
use crate::git::{ContentNotHeld, GitRepository};
use crate::metadata::WorkspaceName;

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

/// What the target repository's published branch holds of a workspace head the live target refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedHolding {
    /// The tracking ref as the target repository names it: `<remote>/<branch>`.
    pub reference: String,
    pub tip: GitOid,
    /// Commits the live target has that the published tip does not.
    pub target_ahead: u64,
    /// Commits the published tip has that the live target does not.
    pub target_behind: u64,
}

/// Whether `remote`'s tracking ref for `branch`, in the target's own repository, holds every
/// commit `head` carries — counted exactly as [`measure_commits`] counts against the live target —
/// and if so how far the live tip `target_tip` has drifted from it.
///
/// It measures the head alone, never the workspace's other local branches: it explains the head's
/// count in a refusal and nothing else. Diagnostic only, and it never authorizes anything: a
/// refusal reads it to say "reconcile the target" instead of reading as missing work, and no gate
/// consults it. Nothing is fetched, so this is what the target repository last fetched. Every
/// answer short of "holds all of them" — no such ref, an unreadable repository, a git
/// disagreement, unheld work — is `None`, which claims nothing: the refusal then says exactly what
/// the gate measured and no more.
pub async fn published_holding(
    store_root: &std::sync::Arc<Path>,
    target_mount: &Path,
    remote: &str,
    branch: &str,
    target_tip: &GitOid,
    workspace_mount: &Path,
    head: &str,
) -> Option<PublishedHolding> {
    let target = GitRepository::from_root(store_root, target_mount);
    let published = LandingTarget {
        branch: format!("{remote}/{branch}"),
        tip: target.remote_tracking_tip(remote, branch).await.ok()??,
        objects: target.object_directory().await.ok()?,
        repository: target_mount.to_path_buf(),
    };
    if !counted(store_root, &published, workspace_mount, head)
        .await
        .ok()?
        .fully_landed()
    {
        return None;
    }
    let target_ahead = target
        .commits_ahead_of(published.tip.as_str(), target_tip.as_str())
        .await
        .ok()?;
    let target_behind = target
        .commits_ahead_of(target_tip.as_str(), published.tip.as_str())
        .await
        .ok()?;
    Some(PublishedHolding {
        reference: published.branch,
        tip: published.tip,
        target_ahead,
        target_behind,
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
/// content as it would a head's. Every other branch is named with its measurement. A tip equal to
/// the target's same-named tip is decided without a process, which is what keeps a workspace
/// carrying its source's branches cheap to list.
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

/// Every abandonment the target's repository keeps under `refs/cowshed/<ws>/abandoned/<head>/`,
/// each tip measured against the target exactly as `rm` measures a branch: ancestry, then content
/// (02_workspaces.md, `rm` step 1). One definition of landed serves both, so a tip that reached
/// main by cherry-pick or a conflict-resolved pick reads as landed here as it would at `rm`.
///
/// The second list names every ref under an `abandoned/` namespace that is not one an abandonment
/// writes (`HEAD` or `heads/<branch>` under a workspace name and a full head): those are never
/// measured, so nothing may retire them. A listing that fails is the error, never an empty answer.
pub async fn abandoned_refs(
    store_root: &std::sync::Arc<Path>,
    target: &LandingTarget,
) -> std::result::Result<(Vec<AbandonedRefs>, Vec<String>), String> {
    let refs = GitRepository::from_root(store_root, &target.repository)
        .ref_tips(&["refs/cowshed"])
        .await
        .map_err(|error| error.message)?;
    let mut namespaces: std::collections::BTreeMap<(WorkspaceName, GitOid), Vec<PreservedRef>> =
        std::collections::BTreeMap::new();
    let mut unreadable = Vec::new();
    for (name, tip) in refs {
        // A name rendered lossily is not the ref's name, and deleting by it would fail every sweep.
        let name = match String::from_utf8(name) {
            Ok(name) => name,
            Err(error) => {
                let name = String::from_utf8_lossy(error.as_bytes()).into_owned();
                if name.starts_with("refs/cowshed/") && name.split('/').nth(3) == Some("abandoned")
                {
                    unreadable.push(name);
                }
                continue;
            }
        };
        let Some(rest) = name.strip_prefix("refs/cowshed/") else {
            continue;
        };
        let mut parts = rest.splitn(4, '/');
        let (Some(workspace), Some("abandoned")) = (parts.next(), parts.next()) else {
            continue;
        };
        let parsed = (|| {
            let workspace = WorkspaceName::new(workspace).ok()?;
            let head = GitOid::new(parts.next()?.to_owned()).ok()?;
            let kept_as = parts.next()?;
            let short = if kept_as == "HEAD" {
                String::from("HEAD")
            } else {
                kept_as
                    .strip_prefix("heads/")
                    .filter(|branch| !branch.is_empty())?
                    .to_owned()
            };
            let tip = GitOid::new(String::from_utf8_lossy(&tip).into_owned()).ok()?;
            Some((workspace, head, short, tip))
        })();
        match parsed {
            Some((workspace, head, short, tip)) => {
                namespaces
                    .entry((workspace, head))
                    .or_default()
                    .push(PreservedRef {
                        name: short,
                        tip,
                        reference: name,
                    });
            }
            None => unreadable.push(name),
        }
    }
    let mut measured = Vec::with_capacity(namespaces.len());
    for ((workspace, head), kept) in namespaces {
        let mut tips = Vec::with_capacity(kept.len());
        for kept in kept {
            let commits = counted(store_root, target, &target.repository, kept.tip.as_str())
                .await
                .unwrap_or_else(|reason| LandingCommits::Indeterminate { reason });
            tips.push(AbandonedTip { kept, commits });
        }
        measured.push(AbandonedRefs {
            workspace,
            head,
            tips,
        });
    }
    Ok((measured, unreadable))
}

/// The two questions of `rm` step 1 (02_workspaces.md), in order: a head the target contains is
/// landed, cheaply and exactly; otherwise every file the head changed since its merge base with
/// the target must carry that change in the target ([`GitRepository::content_not_held`]), however
/// the content got there.
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
    let unheld = if ahead == 0 {
        ContentNotHeld::default()
    } else {
        workspace
            .content_not_held(tip, head)
            .await
            .map_err(|error| error.message)?
    };
    // The unheld commits are a subset of the same range `ahead` counts, so this cannot underflow
    // unless git's own answers disagree with each other. If they ever do, say so rather than wrap
    // into a huge `landed`.
    let landed = ahead.checked_sub(unheld.commits).ok_or_else(|| {
        format!(
            "git reported {} commits changing unheld files in a range of {ahead}: refusing to \
             guess which is right",
            unheld.commits
        )
    })?;
    Ok(LandingCommits::Measured {
        target_branch: target.branch.clone(),
        target_head: target.tip.clone(),
        unlanded: unheld.commits,
        landed,
        behind,
        unlanded_paths: UnlandedPaths::Named {
            paths: unheld.paths,
        },
    })
}
