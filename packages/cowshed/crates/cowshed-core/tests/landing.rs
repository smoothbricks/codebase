//! Landed detection, against real git repositories.
//!
//! Nothing here is mocked. The classification exists to authorize deleting a workspace's object
//! store, so the only fixture worth testing is one git itself produced: every case below builds
//! actual commits and asks the real implementation what it sees.
//!
//! The shape of the fixture is the shape of production. `parent` plays the project's main workspace
//! — the repository the ledger points at, whose branch tip is the live target — and each workspace
//! is a *separate clone*, so it genuinely does not hold main's later objects. That is what makes the
//! alternates path load-bearing rather than decorative, and
//! [`alternate_object_store_is_what_makes_the_comparison_possible`] pins it.

use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::{LandingCommits, WorkspaceLanding};
use cowshed_core::landing::{measure, resolve_target};

const GIT: &str = "/usr/bin/git";
const TARGET: &str = "main";

/// A parent repository plus clones of it, deleted with the test.
struct Fixture(PathBuf);

impl Fixture {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-landing-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("fixture root");
        let fixture = Self(root);
        git(
            fixture.parent_unchecked(),
            ["init", "-q", "-b", TARGET, "."],
        );
        fixture.write_parent("base.txt", "base\n");
        git(fixture.parent_unchecked(), ["add", "-A"]);
        commit(fixture.parent_unchecked(), "base");
        fixture
    }

    /// The parent directory before it is known to be a repository, for `init` and for the case that
    /// deliberately leaves it as something other than a repository.
    fn parent_unchecked(&self) -> PathBuf {
        let path = self.0.join("parent");
        fs::create_dir_all(&path).expect("parent directory");
        path
    }

    fn parent(&self) -> PathBuf {
        self.parent_unchecked()
    }

    fn write_parent(&self, name: &str, contents: &str) {
        fs::write(self.parent().join(name), contents).expect("write parent file");
    }

    /// A fresh clone of the parent, checked out on its own branch.
    fn clone_workspace(&self, name: &str) -> PathBuf {
        let mount = self.0.join(name);
        git(
            &self.0,
            [
                OsStr::new("clone"),
                OsStr::new("-q"),
                self.parent().as_os_str(),
                mount.as_os_str(),
            ],
        );
        git(&mount, ["checkout", "-q", "-b", name]);
        mount
    }

    /// Land `file`'s workspace content in the parent as a brand new commit — a squash-merge or a
    /// history rewrite, as far as the workspace can tell. The patch matches; the oid does not.
    fn land_content_separately(&self, file: &str, contents: &str, message: &str) {
        self.write_parent(file, contents);
        git(self.parent(), ["add", "-A"]);
        commit(self.parent(), message);
    }

    fn parent_commit(&self, file: &str, contents: &str, message: &str) {
        self.land_content_separately(file, contents, message);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn git<I, S>(root: impl AsRef<Path>, args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(GIT)
        .arg("-C")
        .arg(root.as_ref())
        .args(args)
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git failed in {}: {}",
        root.as_ref().display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("git output is UTF-8")
}

/// Commit with a fixed timestamp so two commits carrying the same patch differ only where the test
/// intends them to: patch-id ignores the commit header, and pinning it keeps that fact honest
/// rather than accidental.
fn commit(root: impl AsRef<Path>, message: &str) {
    let output = Command::new(GIT)
        .arg("-C")
        .arg(root.as_ref())
        .args(["commit", "-q", "-m", message])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git commit");
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn workspace_commit(mount: &Path, file: &str, contents: &str, message: &str) {
    fs::write(mount.join(file), contents).expect("write workspace file");
    git(mount, ["add", "-A"]);
    commit(mount, message);
}

async fn measured(fixture: &Fixture, mount: &Path) -> WorkspaceLanding {
    let target = resolve_target(&fixture.parent(), TARGET).await;
    measure(&target, mount, "HEAD").await
}

/// The counts, or a panic naming the reason the measurement was declined.
fn counts(landing: &WorkspaceLanding) -> (u64, u64, u64) {
    match &landing.commits {
        LandingCommits::Measured {
            unlanded,
            landed,
            behind,
            ..
        } => (*unlanded, *landed, *behind),
        LandingCommits::Indeterminate { reason } => {
            panic!("expected a measurement, got indeterminate: {reason}")
        }
    }
}

fn reason(landing: &WorkspaceLanding) -> &str {
    match &landing.commits {
        LandingCommits::Indeterminate { reason } => reason,
        LandingCommits::Measured { unlanded, .. } => {
            panic!("expected indeterminate, got a measurement with {unlanded} unlanded")
        }
    }
}

/// Nothing ahead of the target is the trivially landed case, and it stays landed as the target moves
/// on without the workspace.
#[tokio::test]
async fn a_workspace_with_no_commits_of_its_own_is_landed_and_counts_how_far_behind_it_is() {
    let fixture = Fixture::new("zero-commits");
    let mount = fixture.clone_workspace("quiet");

    assert_eq!(counts(&measured(&fixture, &mount).await), (0, 0, 0));

    fixture.parent_commit("upstream.txt", "one\n", "upstream one");
    fixture.parent_commit("upstream2.txt", "two\n", "upstream two");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 0, 2));
    assert!(landing.commits.fully_landed());
}

/// Strict ancestry is still landed: the target contains the workspace head outright, so the range
/// `target..HEAD` is empty and there is nothing left to prove.
#[tokio::test]
async fn a_workspace_whose_head_the_target_contains_is_landed_by_ancestry() {
    let fixture = Fixture::new("ancestry");
    let mount = fixture.clone_workspace("raven");
    workspace_commit(&mount, "raven.txt", "raven\n", "raven work");
    let head = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();

    // The parent takes the workspace's commit itself, so main's tip *is* the workspace's head.
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            mount.as_os_str(),
            OsStr::new("raven"),
        ],
    );
    git(fixture.parent(), ["merge", "-q", "--ff-only", "FETCH_HEAD"]);
    assert_eq!(
        git(fixture.parent(), ["rev-parse", "refs/heads/main"]).trim(),
        head
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 0, 0));
    assert!(landing.commits.fully_landed());
}

/// The case the whole feature exists for: the content is upstream, the commit is not an ancestor of
/// anything, and strict ancestry therefore calls a safe removal unsafe.
#[tokio::test]
async fn content_that_reached_the_target_by_squash_or_rewrite_is_landed_without_being_an_ancestor()
{
    let fixture = Fixture::new("patch-id-only");
    let mount = fixture.clone_workspace("squashed-feature");
    workspace_commit(&mount, "feature.txt", "feature\n", "the feature");
    let head = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();

    fixture.land_content_separately(
        "feature.txt",
        "feature\n",
        "the feature, landed differently",
    );
    let tip = git(fixture.parent(), ["rev-parse", "refs/heads/main"])
        .trim()
        .to_owned();
    assert_ne!(
        head, tip,
        "the fixture must not accidentally build an ancestor"
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(
        counts(&landing),
        (0, 1, 1),
        "one commit ahead, its patch already upstream, and one upstream commit not held here"
    );
    assert!(landing.commits.fully_landed());

    // And ancestry genuinely disagrees, which is exactly why the gate needed this. Asked in the
    // workspace with main's object store attached, because that is the only place both oids
    // resolve at once — itself a demonstration of why the production path attaches it.
    let ancestry = Command::new(GIT)
        .arg("-C")
        .arg(&mount)
        .args(["merge-base", "--is-ancestor", &head, &tip])
        .env(
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            fixture.parent().join(".git/objects"),
        )
        .output()
        .expect("run merge-base");
    assert_eq!(
        ancestry.status.code(),
        Some(1),
        "the workspace head must not be an ancestor of the target"
    );
}

/// Per-commit, not per-branch: a workspace part of whose work landed is not a landed workspace.
#[tokio::test]
async fn a_partially_landed_workspace_reports_only_the_landed_commits_as_landed() {
    let fixture = Fixture::new("partial");
    let mount = fixture.clone_workspace("partly-shipped");
    workspace_commit(&mount, "shipped.txt", "shipped\n", "shipped work");
    workspace_commit(&mount, "pending.txt", "pending\n", "pending work");
    workspace_commit(&mount, "later.txt", "later\n", "later work");

    fixture.land_content_separately("shipped.txt", "shipped\n", "shipped work, upstream");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (2, 1, 1));
    assert!(!landing.commits.fully_landed());
}

#[tokio::test]
async fn a_workspace_whose_work_is_nowhere_upstream_is_wholly_unlanded() {
    let fixture = Fixture::new("unlanded");
    let mount = fixture.clone_workspace("clean-fix");
    workspace_commit(&mount, "one.txt", "one\n", "one");
    workspace_commit(&mount, "two.txt", "two\n", "two");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (2, 0, 0));
    assert!(!landing.commits.fully_landed());
}

/// An *evil merge* — one whose combined diff is non-empty — authored a conflict resolution that
/// exists in neither parent, so retiring the workspace destroys it. `git cherry` omits every merge
/// from its output, so a check built on cherry alone cannot see this work at all: measured on a real
/// workspace, three of its four merges carried 6, 5 and 2 files that neither parent had.
#[tokio::test]
async fn an_evil_merge_counts_as_unlanded_because_its_resolution_exists_in_no_parent() {
    let fixture = Fixture::new("evil-merge");
    let mount = fixture.clone_workspace("merge-heavy");
    workspace_commit(&mount, "guard.txt", "guard\n", "guard work");
    let guard = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    git(&mount, ["checkout", "-q", "-b", "side", "HEAD~1"]);
    workspace_commit(&mount, "side.txt", "side\n", "side work");
    let side = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    git(&mount, ["checkout", "-q", "merge-heavy"]);

    // The parents touch disjoint files, so the merge itself resolves cleanly — and then authors a
    // file neither parent has. That is an evil merge in its purest form: the only copy of
    // `evil.txt` in existence is the merge commit.
    git(&mount, ["merge", "-q", "--no-ff", "--no-commit", "side"]);
    fs::write(mount.join("evil.txt"), "authored by the merge\n").expect("evil merge content");
    git(&mount, ["add", "-A"]);
    commit(&mount, "merge side");

    // Both parents' patches reach the target verbatim, by cherry-pick rather than re-authoring, so
    // patch identity really does match and patch-id alone would call the whole branch landed.
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            mount.as_os_str(),
            OsStr::new("merge-heavy"),
            OsStr::new("side"),
        ],
    );
    git(fixture.parent(), ["cherry-pick", &guard]);
    git(fixture.parent(), ["cherry-pick", &side]);

    let landing = measured(&fixture, &mount).await;
    let (unlanded, landed, _) = counts(&landing);
    assert_eq!(
        (unlanded, landed),
        (1, 2),
        "both patches are held; only the merge's own content is unaccounted for"
    );
    assert!(
        !landing.commits.fully_landed(),
        "a merge whose resolution exists only here must block a no-flag removal"
    );
}

/// A merge with an empty combined diff authored nothing: it is topology, and retiring the workspace
/// loses no content. Counting it as unheld would mean a branch of already-landed commits joined by
/// clean merges could never be reported landed — exactly the false positive the landed filter exists
/// to remove.
#[tokio::test]
async fn a_clean_merge_does_not_block_a_workspace_whose_commits_are_all_held() {
    let fixture = Fixture::new("clean-merge");
    let mount = fixture.clone_workspace("topic-branch");
    workspace_commit(&mount, "guard.txt", "guard\n", "guard work");
    git(&mount, ["checkout", "-q", "-b", "side"]);
    workspace_commit(&mount, "side.txt", "side\n", "side work");
    git(&mount, ["checkout", "-q", "topic-branch"]);
    // Disjoint files, so the merge resolves itself and contributes nothing.
    git(
        &mount,
        ["merge", "-q", "--no-ff", "side", "-m", "merge side"],
    );

    fixture.land_content_separately("guard.txt", "guard\n", "guard work upstream");
    fixture.land_content_separately("side.txt", "side\n", "side work upstream");

    let landing = measured(&fixture, &mount).await;
    let (unlanded, landed, _) = counts(&landing);
    assert_eq!(
        (unlanded, landed),
        (0, 3),
        "two held patches plus a merge that authored nothing"
    );
    assert!(
        landing.commits.fully_landed(),
        "structural merge scaffolding must not force --abandon"
    );
}

/// An empty commit has no patch-id either. Git reports it unmatched even when the target holds an
/// equally empty commit, and that is the answer this check wants: unprovable, therefore unlanded.
#[tokio::test]
async fn an_empty_commit_in_the_range_counts_as_unlanded() {
    let fixture = Fixture::new("empty-commit");
    let mount = fixture.clone_workspace("marker");
    let output = Command::new(GIT)
        .arg("-C")
        .arg(&mount)
        .args(["commit", "-q", "--allow-empty", "-m", "a marker commit"])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git commit");
    assert!(output.status.success(), "empty commit");

    // The target gets an empty commit too, so a naive "same patch" rule would match them.
    let upstream = Command::new(GIT)
        .arg("-C")
        .arg(fixture.parent())
        .args(["commit", "-q", "--allow-empty", "-m", "an upstream marker"])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git commit");
    assert!(upstream.status.success(), "upstream empty commit");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (1, 0, 1));
    assert!(!landing.commits.fully_landed());
}

/// A rebase that resolved a conflict produced content that exists nowhere upstream, and the patch-id
/// rule must notice. This is the false-positive that would matter most: it touches only files the
/// target already changed, so a file-level or branch-level heuristic would wave it through.
#[tokio::test]
async fn a_conflict_resolution_is_unlanded_even_though_it_touches_only_landed_files() {
    let fixture = Fixture::new("conflict");
    let mount = fixture.clone_workspace("conflict-resolver");
    workspace_commit(&mount, "base.txt", "workspace\n", "workspace edit");
    fixture.land_content_separately("base.txt", "upstream\n", "upstream edit");

    // Replay onto the upstream edit and resolve to something neither side had.
    git(
        &mount,
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            fixture.parent().as_os_str(),
            OsStr::new("main"),
        ],
    );
    let rebase = Command::new(GIT)
        .arg("-C")
        .arg(&mount)
        .args(["rebase", "FETCH_HEAD"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git rebase");
    assert!(
        !rebase.status.success(),
        "the fixture depends on a conflict"
    );
    fs::write(mount.join("base.txt"), "upstream and workspace\n").expect("resolve conflict");
    git(&mount, ["add", "-A"]);
    let resolved = Command::new(GIT)
        .arg("-C")
        .arg(&mount)
        .args(["-c", "core.editor=true", "rebase", "--continue"])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git rebase --continue");
    assert!(
        resolved.status.success(),
        "rebase --continue: {}",
        String::from_utf8_lossy(&resolved.stderr)
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(
        counts(&landing),
        (1, 0, 0),
        "the resolution is content the target has never held"
    );
    assert!(!landing.commits.fully_landed());
}

/// The dirty count is a separate fact from the landing verdict, and it stays a count of *changes*:
/// a rename is one change carried in two NUL-separated fields, not two changes.
#[tokio::test]
async fn a_dirty_tree_is_counted_by_change_and_does_not_disturb_the_landing_verdict() {
    let fixture = Fixture::new("dirty");
    let mount = fixture.clone_workspace("dirty-tree");
    workspace_commit(&mount, "kept.txt", "kept\n", "kept work");
    fixture.land_content_separately("kept.txt", "kept\n", "kept work upstream");

    assert_eq!(measured(&fixture, &mount).await.dirty_files, Some(0));

    git(&mount, ["mv", "kept.txt", "moved.txt"]);
    fs::write(mount.join("untracked.txt"), "new\n").expect("write untracked file");
    fs::write(mount.join("base.txt"), "modified\n").expect("modify tracked file");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(
        landing.dirty_files,
        Some(3),
        "one rename, one modification, one untracked path"
    );
    assert_eq!(
        counts(&landing),
        (0, 1, 1),
        "uncommitted work is reported, not conflated with the commit verdict"
    );
    assert!(landing.commits.fully_landed());
}

/// A target branch that does not exist is not an empty target: it is an unanswered question.
#[tokio::test]
async fn a_target_branch_that_does_not_exist_is_indeterminate_rather_than_landed() {
    let fixture = Fixture::new("no-branch");
    let mount = fixture.clone_workspace("topic-branch");
    workspace_commit(&mount, "wave.txt", "wave\n", "wave work");

    let target = resolve_target(&fixture.parent(), "release").await;
    let landing = measure(&target, &mount, "HEAD").await;
    assert!(
        reason(&landing).contains("no release branch"),
        "reason must name the missing branch: {}",
        reason(&landing)
    );
    assert!(
        !landing.commits.fully_landed(),
        "an unanswered question must never read as landed"
    );
    assert_eq!(
        landing.dirty_files,
        Some(0),
        "the tree is still readable and is still reported"
    );
}

/// The observed production failure: the path the workspace's `main` remote points at is not a
/// repository — an unmounted mountpoint holding a stray directory. Every workspace in that project
/// resolved its target off a stale clone-time cache instead, each frozen at a different commit, and
/// produced confident verdicts that were wrong in both directions. Resolving from main's own
/// repository turns that class of bug into a refusal that names itself.
#[tokio::test]
async fn a_main_mount_that_is_not_a_repository_is_indeterminate_and_says_so() {
    let fixture = Fixture::new("dead-mount");
    let mount = fixture.clone_workspace("orphaned-mount");
    workspace_commit(&mount, "wall.txt", "wall\n", "wall work");

    let stub = fixture.0.join("unmounted-stub");
    fs::create_dir_all(stub.join(".devenv")).expect("mountpoint stub");

    let target = resolve_target(&stub, TARGET).await;
    let landing = measure(&target, &mount, "HEAD").await;
    let reason = reason(&landing);
    assert!(
        reason.contains("could not be read") && reason.contains("unmounted-stub"),
        "reason must name what could not be read: {reason}"
    );
    assert!(!landing.commits.fully_landed());
}

/// Without main's object store attached the workspace cannot see the target commit at all, so the
/// comparison is only possible because it is attached. Pinned because the alternates line looks
/// removable and is not: delete it and every measurement silently becomes a refusal.
#[tokio::test]
async fn alternate_object_store_is_what_makes_the_comparison_possible() {
    let fixture = Fixture::new("alternates");
    let mount = fixture.clone_workspace("needs-alternates");
    workspace_commit(&mount, "parity.txt", "parity\n", "parity work");
    fixture.land_content_separately("parity.txt", "parity\n", "parity work upstream");
    let tip = git(fixture.parent(), ["rev-parse", "refs/heads/main"])
        .trim()
        .to_owned();

    let visible = Command::new(GIT)
        .arg("-C")
        .arg(&mount)
        .args(["cat-file", "-e", &tip])
        .output()
        .expect("run cat-file");
    assert!(
        !visible.status.success(),
        "the fixture must model a workspace that does not hold the target commit"
    );

    assert_eq!(counts(&measured(&fixture, &mount).await), (0, 1, 1));
}

/// Observed in production: a workspace's registered branch is a label recorded when it was minted,
/// and its owner can `git switch` away from it at any time. One workspace's registered ref was a
/// strict ancestor of main while its HEAD sat on a different branch carrying two unlanded commits,
/// so anything classifying by label would have called it landed.
///
/// The subject of the measurement is HEAD, because HEAD is what the checkout actually is. Pinned
/// here so no later refactor can quietly substitute the recorded name for it.
#[tokio::test]
async fn classification_follows_head_and_not_a_registered_branch_that_has_drifted_from_it() {
    let fixture = Fixture::new("label-drift");
    let mount = fixture.clone_workspace("drifted-label");

    // The registered label: one commit, landed upstream by content, left behind on that branch.
    workspace_commit(&mount, "verify.txt", "verify\n", "verify work");
    fixture.land_content_separately("verify.txt", "verify\n", "verify work upstream");
    let registered = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();

    // HEAD moves to a different branch carrying work that is nowhere upstream.
    git(&mount, ["checkout", "-q", "-b", "shadow-verify"]);
    workspace_commit(&mount, "shadow.txt", "shadow\n", "shadow work");
    workspace_commit(&mount, "shadow2.txt", "shadow again\n", "more shadow work");

    // The label really is landed, so a check reading it would wave this workspace through.
    let by_label = measure(
        &resolve_target(&fixture.parent(), TARGET).await,
        &mount,
        &registered,
    )
    .await;
    assert_eq!(counts(&by_label), (0, 1, 1));
    assert!(by_label.commits.fully_landed());

    // What the implementation actually measures.
    let landing = measured(&fixture, &mount).await;
    assert_eq!(
        counts(&landing),
        (2, 1, 1),
        "the two commits only HEAD carries must be counted"
    );
    assert!(
        !landing.commits.fully_landed(),
        "a workspace whose HEAD holds unlanded work must never be reported landed"
    );
}

/// The defect this whole module routes around, tested from the outside.
///
/// A workspace's `refs/remotes/main/main` is a clone-time snapshot. Nothing refreshes it, so it
/// freezes at whatever main's tip was when the workspace was minted — on this host, seven
/// workspaces each frozen at a different commit, some hundreds behind. A check that resolves its
/// target through that ref is confidently wrong in *both* directions: it under-reports how far
/// behind a workspace is, and it calls work unlanded that the real main has held for months.
///
/// So the property is not "the cache is preferred less"; it is that the cache has no influence at
/// all. Both halves are asserted, because a check that read the cache would fail exactly one of
/// them and pass the other.
#[tokio::test]
async fn a_stale_cached_remote_ref_has_no_influence_on_the_verdict() {
    let fixture = Fixture::new("stale-remote-cache");
    let mount = fixture.clone_workspace("stale-cache");
    let clone_time_tip = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();

    // Half one: work that is nowhere upstream, against a main that has moved on. Freeze the cache
    // at clone time and point the remote itself at a directory that stopped being a repository,
    // which is the exact state a moved checkout leaves behind.
    workspace_commit(&mount, "formats.txt", "formats\n", "format work");
    git(
        &mount,
        ["update-ref", "refs/remotes/main/main", &clone_time_tip],
    );
    git(
        &mount,
        [
            "remote",
            "add",
            "main",
            "/Users/nobody/.cowshed/mnt/gone/main",
        ],
    );
    for index in 0..5 {
        fixture.parent_commit(
            &format!("upstream-{index}.txt"),
            "upstream\n",
            &format!("upstream {index}"),
        );
    }
    let real_tip = git(fixture.parent(), ["rev-parse", "HEAD"])
        .trim()
        .to_owned();
    assert_ne!(
        real_tip, clone_time_tip,
        "the fixture must actually diverge"
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(
        counts(&landing),
        (1, 0, 5),
        "the target is main's live tip, so the workspace is five behind and not zero"
    );
    match &landing.commits {
        LandingCommits::Measured { target_head, .. } => {
            assert_eq!(
                target_head.as_str(),
                real_tip,
                "the reported target head must be main's live tip, never the cached ref"
            );
        }
        LandingCommits::Indeterminate { reason } => panic!("expected a measurement: {reason}"),
    }

    // Half two, the destructive direction: the workspace's content is now in the real main, while
    // the frozen cache still predates it. A check reading the cache calls this unlanded and demands
    // `--abandon`; a check reading main calls it landed, which it is.
    fixture.land_content_separately("formats.txt", "formats\n", "format work upstream");
    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 1, 6));
    assert!(
        landing.commits.fully_landed(),
        "content the live main holds is landed however stale the cached ref is"
    );

    // And the cache is still exactly where it was, so the verdicts above were not the result of
    // something quietly refreshing it.
    assert_eq!(
        git(&mount, ["rev-parse", "refs/remotes/main/main"])
            .trim()
            .to_owned(),
        clone_time_tip,
        "nothing may fetch: a refreshed cache would hide the property under test"
    );
}
