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
//! * **Landed is the merge changing nothing, content or patch identity.** Work reaches a target
//!   by cherry-pick, squash, rewrite, or a pick whose conflict a lander resolved. A head whose
//!   merge into the target reproduces the target's tree is landed, in one `git merge-tree` that
//!   also follows renames; otherwise a commit is landed when the target's content carries its
//!   change or the target holds a patch-equivalent of it. The measurement names each file whose
//!   change the target still lacks, and why, and a refusal names the commits held by neither
//!   proof, so it says what would be lost.
//! * **A land's folds are measured here, decided by [`fold_step`].** The sessions a land carried
//!   into its target are retired by the land (02_workspaces.md, `land` step 5, "Folds"); which
//!   ones, and why one is kept, is a pure function of what [`fold_facts`] read.
//!
//! One reading here is not a verdict: [`published_holding`] measures the target repository's own
//! tracking ref for its published branch, the same way, so a refusal can say when that ref already
//! holds every commit of the workspace head that the live target does not. It is read only to
//! explain a refusal and decides nothing — `rm`'s gate is the live target alone
//! (`specs/cowshed/02_workspaces.md`, `rm` step 1).

use std::path::{Path, PathBuf};

use crate::api::dto::{
    BranchLanding, BranchLandings, FoldKept, GitOid, LandingCommits, UnlandedPaths, WorkspaceInfo,
    WorkspaceLanding, WorkspaceState,
};
use crate::error::ErrorCode;
use crate::git::{CommitSubject, ContentNotHeld, GitRepository};
use cowshed_types::metadata::{WorkspaceName, WorkspaceRole};

/// The branch a workspace's work has to reach, resolved live, with the object store that holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandingTarget {
    branch: String,
    tip: GitOid,
    objects: PathBuf,
    /// The target's canonical mount, whose branches say which workspace branches it preserves.
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
    let divergence = target
        .divergence(published.tip.as_str(), target_tip.as_str())
        .await
        .ok()?;
    Some(PublishedHolding {
        reference: published.branch,
        tip: published.tip,
        target_ahead: divergence.ahead,
        target_behind: divergence.behind,
    })
}

/// The commits `head` carries that `target_tip` holds neither by content nor by patch identity,
/// oldest first: exactly those [`measure_commits`] counts as unlanded against that tip.
///
/// Read only to name them in a refusal the gate has already made, at the tip its count was taken
/// at, so the names explain that count; it decides nothing. A failure is the reason, which the
/// refusal states in place of the names.
pub async fn unmatched_commits(
    store_root: &std::sync::Arc<Path>,
    target_mount: &Path,
    target_tip: &GitOid,
    workspace_mount: &Path,
    head: &str,
) -> std::result::Result<Vec<CommitSubject>, String> {
    let objects = GitRepository::from_root(store_root, target_mount)
        .object_directory()
        .await
        .map_err(|error| error.message)?;
    let workspace = GitRepository::from_root(store_root, workspace_mount)
        .with_alternate_objects(&objects)
        .map_err(|error| error.message)?;
    workspace
        .content_not_held(target_tip.as_str(), head)
        .await
        .map(|unheld| unheld.commits)
        .map_err(|error| error.message)
}

/// Measure one workspace tree against `target`: its head, and every other local branch.
///
/// Total by construction: there is no error return, because every failure has a truthful
/// representation as [`LandingCommits::Indeterminate`] and no failure may be allowed to read as
/// landed. Folding errors inward is safe precisely because indeterminate is a refusal everywhere it
/// is consumed — it can never authorize a deletion, only decline to.
///
/// `head` is a revision in the workspace's own repository; pass `HEAD` unless a caller has already
/// fenced a specific oid and needs that exact commit judged. `push_repository` is main's
/// repository, where `cowshed push` preserves the workspace's branches, and `workspace` names
/// them there.
pub async fn measure(
    store_root: &std::sync::Arc<Path>,
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    push_repository: &Path,
    workspace: &str,
    head: &str,
) -> WorkspaceLanding {
    let repository = GitRepository::from_root(store_root, workspace_mount);
    let dirty_files = repository.dirty_file_count().await.ok();
    WorkspaceLanding {
        dirty_files,
        commits: measure_commits(store_root, target, workspace_mount, head).await,
        branches: measure_branches(
            store_root,
            target,
            workspace_mount,
            push_repository,
            workspace,
        )
        .await,
    }
}

/// Judge every local branch of the workspace except the checked-out one, which the head
/// measurement covers.
///
/// A branch passes when its tip equals or is an ancestor of the target repository's branch of the
/// same name or of `refs/cowshed/<workspace>/heads/<branch>` in `push_repository` - main's
/// repository, the one place `cowshed push` writes that ref (02_workspaces.md, `push` and `rm`
/// step 1), whichever target the workspace lands into - so destroying the workspace's copy loses
/// nothing; or when the target branch holds each commit by content or patch identity, as it
/// would a head's. Every other branch is named with its measurement. A tip equal to the target's
/// same-named tip is decided without a process, which is what keeps a workspace carrying its
/// source's branches cheap to list.
pub async fn measure_branches(
    store_root: &std::sync::Arc<Path>,
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    push_repository: &Path,
    workspace: &str,
) -> BranchLandings {
    match judged_branches(
        store_root,
        target,
        workspace_mount,
        push_repository,
        workspace,
    )
    .await
    {
        Ok(unlanded) => BranchLandings::Measured { unlanded },
        Err(reason) => BranchLandings::Indeterminate { reason },
    }
}

async fn judged_branches(
    store_root: &std::sync::Arc<Path>,
    target: &std::result::Result<LandingTarget, String>,
    workspace_mount: &Path,
    push_repository: &Path,
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
    let target_repository = GitRepository::from_root(store_root, &target.repository);
    let target_branches = target_repository
        .ref_tips(&["refs/heads"])
        .await
        .map_err(|error| error.message)?;
    let push_namespace = format!("refs/cowshed/{workspace}/heads");
    let pushed = GitRepository::from_root(store_root, push_repository);
    let push_refs = pushed
        .ref_tips(&[push_namespace.as_str()])
        .await
        .map_err(|error| error.message)?;
    let mut unlanded = Vec::new();
    for (name, tip) in candidates {
        let short = name.strip_prefix(b"refs/heads/").unwrap_or(&name);
        let push_ref = [push_namespace.as_bytes(), b"/".as_slice(), short].concat();
        if holds(&target_repository, target_branches.get(&name), &tip).await?
            || holds(&pushed, push_refs.get(&push_ref), &tip).await?
        {
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

/// Whether `preserving`, a ref tip read from `repository`, keeps `tip`: it is that commit or a
/// descendant of it there. No ref keeps nothing.
async fn holds(
    repository: &GitRepository,
    preserving: Option<&Vec<u8>>,
    tip: &[u8],
) -> std::result::Result<bool, String> {
    let Some(preserving) = preserving else {
        return Ok(false);
    };
    if preserving.as_slice() == tip {
        return Ok(true);
    }
    repository
        .commit_is_ancestor(
            &String::from_utf8_lossy(tip),
            &String::from_utf8_lossy(preserving),
        )
        .await
        .map_err(|error| error.message)
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

/// What merging a workspace head into its target produced (`git merge-tree --write-tree`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MergedTree {
    /// A clean merge, and the tree it wrote.
    Clean(GitOid),
    /// The merge conflicts: the head changes something the target changed differently.
    Conflicted,
}

/// Whether merging a head into the target changes nothing: the merge is clean and its tree is the
/// target's own. Then every change the head carries is already in the target, however it got
/// there (02_workspaces.md, `rm` step 1).
pub fn merge_changes_nothing(target_tree: &GitOid, merged: &MergedTree) -> bool {
    matches!(merged, MergedTree::Clean(tree) if tree == target_tree)
}

/// What a land's fold step read of one session (02_workspaces.md, `land` step 5, "Folds").
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FoldFacts {
    /// Whether its own change since its merge base with the target's previous head touches a path
    /// the landed range changes, or why that could not be read.
    pub touched: std::result::Result<bool, String>,
    /// Whether the landed tip holds its head and every other local branch, as `rm` measures them.
    pub landed: bool,
    /// Its uncommitted changes (tracked, and untracked no ignore rule covers), or why they could
    /// not be counted.
    pub dirty_files: std::result::Result<u64, String>,
}

/// What the land does with a session it folded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FoldStep {
    /// Retire it as `rm` would, sparing a live owner.
    Retire,
    /// Keep it, saying why.
    Keep(FoldKept),
}

/// The fold decision: `None` for a session that is not the land's (its own change touches nothing
/// the land changed, or the target does not hold it), which the land neither touches nor names.
/// A folded session is retired when its tree is clean and kept, with why, when it is dirty or its
/// tree cannot be read. A session whose own change cannot be read is kept and named rather than
/// passed over in silence.
pub fn fold_step(facts: &FoldFacts) -> Option<FoldStep> {
    match &facts.touched {
        Err(reason) => {
            return Some(FoldStep::Keep(FoldKept::Refused {
                code: ErrorCode::EnvironmentMissing,
                message: format!("its own change could not be read: {reason}"),
            }));
        }
        Ok(false) => return None,
        Ok(true) if !facts.landed => return None,
        Ok(true) => {}
    }
    Some(match &facts.dirty_files {
        Ok(0) => FoldStep::Retire,
        Ok(files) => FoldStep::Keep(FoldKept::Dirty { files: *files }),
        Err(reason) => FoldStep::Keep(FoldKept::Refused {
            code: ErrorCode::EnvironmentMissing,
            message: format!("its working tree could not be read: {reason}"),
        }),
    })
}

/// The paths `previous..landed` changes in the target's repository: what a land carried, against
/// which every session's own change is matched (02_workspaces.md, `land` step 5, "Folds").
pub async fn landed_paths(
    store_root: &std::sync::Arc<Path>,
    target: &LandingTarget,
    previous: &GitOid,
    landed: &GitOid,
) -> std::result::Result<std::collections::BTreeSet<Vec<u8>>, String> {
    GitRepository::from_root(store_root, &target.repository)
        .changed_paths(&format!("{previous}..{landed}"))
        .await
        .map_err(|error| error.message)
}

/// Read what [`fold_step`] decides from for the session at `workspace_mount`: whether its own
/// change since its merge base with `previous` touches `landed_paths`, and only then whether the
/// target holds it, measured as `rm` measures it ([`measure`]), and how dirty its tree is.
pub async fn fold_facts(
    store_root: &std::sync::Arc<Path>,
    target: &LandingTarget,
    landed_paths: &std::collections::BTreeSet<Vec<u8>>,
    previous: &GitOid,
    workspace_mount: &Path,
    push_repository: &Path,
    workspace: &str,
) -> FoldFacts {
    let own = match GitRepository::from_root(store_root, workspace_mount)
        .with_alternate_objects(&target.objects)
    {
        Ok(repository) => repository
            .changed_paths(&format!("{previous}...HEAD"))
            .await
            .map_err(|error| error.message),
        Err(error) => Err(error.message),
    };
    let touched = own.map(|own| !own.is_disjoint(landed_paths));
    if touched != Ok(true) {
        return FoldFacts {
            touched,
            landed: false,
            dirty_files: Ok(0),
        };
    }
    let landing = measure(
        store_root,
        &Ok(target.clone()),
        workspace_mount,
        push_repository,
        workspace,
        "HEAD",
    )
    .await;
    FoldFacts {
        touched,
        landed: landing.fully_landed(),
        dirty_files: landing
            .dirty_files
            .ok_or_else(|| String::from("git status could not be read")),
    }
}
/// The three questions of `rm` step 1 (02_workspaces.md), in order: a head the target contains is
/// landed, cheaply and exactly; otherwise a head whose merge into the target changes nothing is
/// landed, in one process; otherwise each of its commits must be held by the target's content or
/// by a patch-equivalent commit of the target ([`GitRepository::content_not_held`]), however the
/// work got there.
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
    let crate::git::Divergence { ahead, behind } = workspace
        .divergence(tip, head)
        .await
        .map_err(|error| error.message)?;
    let unheld = if ahead == 0
        || merge_changes_nothing(
            &workspace
                .tree_of(tip)
                .await
                .map_err(|error| error.message)?,
            &workspace
                .merged_tree(tip, head)
                .await
                .map_err(|error| error.message)?,
        ) {
        ContentNotHeld::default()
    } else {
        workspace
            .content_not_held(tip, head)
            .await
            .map_err(|error| error.message)?
    };
    let unlanded = u64::try_from(unheld.commits.len()).expect("a commit count fits in u64");
    // The unheld commits are a subset of the same range `ahead` counts, so this cannot underflow
    // unless git's own answers disagree with each other. If they ever do, say so rather than wrap
    // into a huge `landed`.
    let landed = ahead.checked_sub(unlanded).ok_or_else(|| {
        format!(
            "git reported {unlanded} commits changing unheld files in a range of {ahead}: \
             refusing to guess which is right"
        )
    })?;
    Ok(LandingCommits::Measured {
        target_branch: target.branch.clone(),
        target_head: target.tip.clone(),
        unlanded,
        landed,
        behind,
        unlanded_paths: UnlandedPaths::Named {
            paths: unheld.paths,
        },
    })
}

/// The branch `ls` measures against, which is the same constant `land` merges into and `rm` gates
/// on. Sharing it is the point: a listing that reported against a different branch than the gate
/// enforces would be a listing that lies about what `rm` will do.
const LISTING_TARGET_BRANCH: &str = crate::runtime::project::DEFAULT_LANDING_BRANCH;

/// Measure every session workspace of one project's listing against the project's own main.
///
/// Main is left unmeasured because main *is* the target: comparing it to itself answers nothing,
/// and a zero-unlanded row for main would make it look retirable to anything reading the column.
/// A detached session, a project without main and a measurement that never completed each carry
/// an indeterminate landing naming why, never an absent one.
///
/// One task per workspace, because the measurement is process-bound rather than CPU-bound and the
/// serial version costs the sum of every workspace's history walk — which on a project whose main
/// has been rewritten is the slowest thing a listing could be asked to do.
///
/// Its Git runs for `store_root`, the store the measuring process serves: the controller's own
/// (a test fixture's in a test), or the host's for `ls --all`. Controller Git writes its sandbox
/// profile there and nowhere else (04_sandbox.md, "Exec pipeline" step 4).
pub async fn annotate_listing<T: AsMut<WorkspaceInfo>>(
    store_root: &std::sync::Arc<Path>,
    rows: &mut [T],
) {
    let Some(main) = rows
        .iter_mut()
        .map(AsMut::as_mut)
        .find(|workspace| workspace.role == WorkspaceRole::Main)
        .map(|main| main.mount.clone())
    else {
        let reason = String::from(
            "this project records no main workspace, so there is no branch to compare against",
        );
        for row in rows.iter_mut() {
            row.as_mut().landing = Some(indeterminate(reason.clone()));
        }
        return;
    };
    let target =
        std::sync::Arc::new(resolve_target(store_root, &main, LISTING_TARGET_BRANCH).await);
    let main: std::sync::Arc<Path> = std::sync::Arc::from(main);
    let mut tasks = tokio::task::JoinSet::new();
    for (index, row) in rows.iter_mut().enumerate() {
        let workspace = row.as_mut();
        if workspace.role == WorkspaceRole::Main {
            continue;
        }
        if workspace.state == WorkspaceState::Detached {
            workspace.landing = Some(indeterminate(String::from(
                "workspace is detached, so its repository is not mounted to be read",
            )));
            continue;
        }
        let target = std::sync::Arc::clone(&target);
        let main = std::sync::Arc::clone(&main);
        let store_root = std::sync::Arc::clone(store_root);
        let mount = workspace.mount.clone();
        let name = workspace.workspace.as_str().to_owned();
        tasks.spawn(async move {
            (
                index,
                measure(&store_root, &target, &mount, &main, &name, "HEAD").await,
            )
        });
    }
    while let Some(finished) = tasks.join_next().await {
        match finished {
            Ok((index, landing)) => rows[index].as_mut().landing = Some(landing),
            // A panicked measurement is still an unanswered question, and the row it belongs to is
            // no longer identifiable — so every row left unmeasured says so below.
            Err(_) => continue,
        }
    }
    for row in rows.iter_mut() {
        let workspace = row.as_mut();
        if workspace.role != WorkspaceRole::Main && workspace.landing.is_none() {
            workspace.landing = Some(indeterminate(String::from(
                "the measurement did not complete",
            )));
        }
    }
}

fn indeterminate(reason: String) -> WorkspaceLanding {
    WorkspaceLanding {
        dirty_files: None,
        branches: BranchLandings::Indeterminate {
            reason: reason.clone(),
        },
        commits: LandingCommits::Indeterminate { reason },
    }
}

/// A listing row `retain_fully_landed` dropped because its landing could not be answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Withheld {
    pub workspace: WorkspaceName,
    pub reason: String,
}

/// Keep only the workspaces a landed listing may name, and report which of the rest went
/// unanswered.
///
/// A landed listing is meant to be piped into `rm`, so the bar for staying in it is the
/// measurement `rm` gates on saying nothing is unlanded - its head and every other local branch -
/// never an absent measurement, and never main. Rows dropped for having real unlanded work need no
/// explanation; a row dropped because the question could not be answered does, or the silence
/// reads as "nothing here".
pub fn retain_fully_landed<T: AsRef<WorkspaceInfo>>(rows: &mut Vec<T>) -> Vec<Withheld> {
    let mut withheld = Vec::new();
    rows.retain(|row| {
        let workspace = row.as_ref();
        if workspace.role == WorkspaceRole::Main {
            return false;
        }
        let Some(landing) = workspace.landing.as_ref() else {
            withheld.push(Withheld {
                workspace: workspace.workspace.clone(),
                reason: String::from("no landing measurement was taken"),
            });
            return false;
        };
        let unanswered = match (&landing.commits, &landing.branches) {
            (LandingCommits::Indeterminate { reason }, _)
            | (_, BranchLandings::Indeterminate { reason }) => Some(reason),
            _ => None,
        };
        if let Some(reason) = unanswered {
            withheld.push(Withheld {
                workspace: workspace.workspace.clone(),
                reason: reason.clone(),
            });
            return false;
        }
        landing.fully_landed()
    });
    withheld
}

#[cfg(test)]
mod tests {
    use super::{FoldFacts, FoldStep, MergedTree, fold_step, merge_changes_nothing};
    use crate::api::dto::{FoldKept, GitOid};
    use crate::error::ErrorCode;

    fn oid(byte: char) -> GitOid {
        GitOid::new(byte.to_string().repeat(40)).expect("an object id")
    }

    #[test]
    fn only_a_clean_merge_to_the_targets_own_tree_changes_nothing() {
        let target = oid('a');
        for (merged, changes_nothing) in [
            (MergedTree::Clean(oid('a')), true),
            (MergedTree::Clean(oid('b')), false),
            (MergedTree::Conflicted, false),
        ] {
            assert_eq!(
                merge_changes_nothing(&target, &merged),
                changes_nothing,
                "{merged:?}"
            );
        }
    }

    #[test]
    fn a_fold_is_a_touched_landed_session_retired_clean_and_kept_dirty_or_unread() {
        let facts = |touched, landed, dirty_files| FoldFacts {
            touched,
            landed,
            dirty_files,
        };
        for (facts, step) in [
            (facts(Ok(false), true, Ok(0)), None),
            (facts(Ok(true), false, Ok(0)), None),
            (facts(Ok(false), false, Ok(3)), None),
            (facts(Ok(true), true, Ok(0)), Some(FoldStep::Retire)),
            (
                facts(Ok(true), true, Ok(2)),
                Some(FoldStep::Keep(FoldKept::Dirty { files: 2 })),
            ),
            (
                facts(Ok(true), true, Err(String::from("unreadable"))),
                Some(FoldStep::Keep(FoldKept::Refused {
                    code: ErrorCode::EnvironmentMissing,
                    message: String::from("its working tree could not be read: unreadable"),
                })),
            ),
            (
                facts(Err(String::from("unreadable")), false, Ok(0)),
                Some(FoldStep::Keep(FoldKept::Refused {
                    code: ErrorCode::EnvironmentMissing,
                    message: String::from("its own change could not be read: unreadable"),
                })),
            ),
        ] {
            assert_eq!(fold_step(&facts), step, "{facts:?}");
        }
    }
}
