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

use cowshed_os::test_backstop::CommandBackstop as _;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_core::api::{
    BranchLandings, GitOid, LandingCommits, PushRef, UnlandedChange, UnlandedPaths,
    WorkspaceIncarnation, WorkspaceInfo, WorkspaceLanding, WorkspaceName, WorkspaceRole,
    WorkspaceState,
};
use cowshed_core::landing::{
    annotate_listing, fold_facts, fold_step, measure, resolve_target, retain_fully_landed,
    unmatched_commits,
};
use cowshed_core::repository::RepoId;

#[path = "support/temp_parent.rs"]
mod temp_parent;
#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::{FixtureStore, TempRoot};

const TARGET: &str = "main";

/// A parent repository plus clones of it, deleted with the test. Under `/tmp`, not TMPDIR: the
/// measurement runs controller git sandboxed, and the sandbox never grants a workspace's TMPDIR,
/// which is inside the protected store. The second field is the fixture's own store, where that
/// sandboxed controller Git writes its profiles.
struct Fixture(TempRoot, FixtureStore);

impl Fixture {
    fn new(label: &str) -> Self {
        let fixture = Self(
            TempRoot::new_in(
                &temp_parent::canonical_parent(),
                &format!("cowshed-landing-{label}"),
            ),
            FixtureStore::new(),
        );
        git(
            fixture.parent_unchecked(),
            ["init", "-q", "-b", TARGET, "."],
        );
        fixture.write_parent("base.txt", "base\n");
        git(fixture.parent_unchecked(), ["add", "-A"]);
        commit(fixture.parent_unchecked(), "base");
        fixture
    }

    fn store(&self) -> &std::sync::Arc<Path> {
        self.1.shared()
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
            &*self.0,
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

fn git<I, S>(root: impl AsRef<Path>, args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new("git")
        .arg("-C")
        .arg(root.as_ref())
        .args(args)
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_bounded()
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
    let output = Command::new("git")
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
        .output_bounded()
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
    let target = resolve_target(fixture.store(), &fixture.parent(), TARGET).await;
    measure(
        fixture.store(),
        &target,
        mount,
        &fixture.parent(),
        "topic",
        "HEAD",
    )
    .await
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
    let ancestry = Command::new("git")
        .arg("-C")
        .arg(&mount)
        .args(["merge-base", "--is-ancestor", &head, &tip])
        .env(
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            fixture.parent().join(".git/objects"),
        )
        .output_bounded()
        .expect("run merge-base");
    assert_eq!(
        ancestry.status.code(),
        Some(1),
        "the workspace head must not be an ancestor of the target"
    );
}

/// A file the branch renamed and edited reached the target in a squash beside another unit's work,
/// and the target then edited further down. No commit's patch matches, and per file the renamed
/// path is new on the branch and differs in the target, so neither per-commit proof holds it.
/// Merging the branch into the target follows the rename on both sides and changes nothing, which is
/// what makes it landed (02_workspaces.md, `rm` step 1).
#[tokio::test]
async fn a_renamed_file_the_target_took_and_edited_past_is_landed_by_the_whole_tree_merge() {
    let fixture = Fixture::new("renamed");
    let lines: Vec<String> = (1..=20).map(|line| format!("line {line}\n")).collect();
    fixture.parent_commit("story.txt", &lines.concat(), "story");
    let mount = fixture.clone_workspace("renamer");
    git(&mount, ["mv", "story.txt", "tale.txt"]);
    let mut retold = lines.clone();
    retold[2] = String::from("line 3, retold\n");
    workspace_commit(
        &mount,
        "tale.txt",
        &retold.concat(),
        "rename the story, retell line 3",
    );

    git(fixture.parent(), ["mv", "story.txt", "tale.txt"]);
    fixture.write_parent("other.txt", "another unit\n");
    fixture.land_content_separately(
        "tale.txt",
        &retold.concat(),
        "squash the rename with another unit",
    );
    let mut revised = retold.clone();
    revised[18] = String::from("line 19, revised\n");
    fixture.parent_commit("tale.txt", &revised.concat(), "revise line 19");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 1, 2), "{landing:?}");
    assert!(landing.commits.fully_landed());
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
    let mount = fixture.clone_workspace("local-only");
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

/// An empty commit changes no file, so it carries no content the target could lack: landed. Its
/// message is all it holds, and a message is not work `rm` keeps a workspace alive for.
#[tokio::test]
async fn an_empty_commit_carries_no_content_and_is_landed() {
    let fixture = Fixture::new("empty-commit");
    let mount = fixture.clone_workspace("marker");
    let output = Command::new("git")
        .arg("-C")
        .arg(&mount)
        .args(["commit", "-q", "--allow-empty", "-m", "a marker commit"])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_bounded()
        .expect("run git commit");
    assert!(output.status.success(), "empty commit");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 1, 0));
    assert!(landing.commits.fully_landed());
}

/// The files a measurement names, each with why the target does not hold its change.
fn unlanded_paths(landing: &WorkspaceLanding) -> Vec<(&str, &UnlandedChange)> {
    match &landing.commits {
        LandingCommits::Measured {
            unlanded_paths: UnlandedPaths::Named { paths },
            ..
        } => paths
            .iter()
            .map(|unlanded| (unlanded.path.as_str(), &unlanded.change))
            .collect(),
        LandingCommits::Measured {
            unlanded_paths: UnlandedPaths::PredatesPaths,
            ..
        } => panic!("a live measurement names its files"),
        LandingCommits::Indeterminate { reason } => {
            panic!("expected a measurement, got indeterminate: {reason}")
        }
    }
}

/// Pick the workspace commit `oid` into the parent, where it conflicts, and resolve `file` to
/// `resolution`.
fn pick_resolving(fixture: &Fixture, mount: &Path, oid: &str, file: &str, resolution: &str) {
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            mount.as_os_str(),
            OsStr::new("HEAD"),
        ],
    );
    let pick = Command::new("git")
        .arg("-C")
        .arg(fixture.parent())
        .args(["cherry-pick", oid])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_bounded()
        .expect("run git cherry-pick");
    assert!(!pick.status.success(), "the fixture depends on a conflict");
    fixture.write_parent(file, resolution);
    git(fixture.parent(), ["add", "-A"]);
    git(
        fixture.parent(),
        ["-c", "core.editor=true", "cherry-pick", "--continue"],
    );
}

/// The pick a lander lands by hand. The target edited the line beside the workspace's, so the
/// pick conflicted and was resolved keeping both; the target then edited the same file again. No
/// target commit is the workspace's by ancestry or patch identity, and the file differs from the
/// workspace's byte for byte, yet the target holds every change the workspace made: landed.
#[tokio::test]
async fn a_conflict_resolved_pick_the_target_moved_past_is_landed_by_content() {
    let fixture = Fixture::new("resolved-pick");
    fixture.parent_commit("gate.txt", "alpha\nbeta\ngamma\ndelta\nepsilon\n", "gate");
    let mount = fixture.clone_workspace("gate-coverage");
    workspace_commit(
        &mount,
        "gate.txt",
        "alpha\nbeta\ngamma shed\ndelta\nepsilon\n",
        "cover the gate",
    );
    let shed = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    fixture.parent_commit(
        "gate.txt",
        "alpha\nbeta main\ngamma\ndelta\nepsilon\n",
        "beside it",
    );
    pick_resolving(
        &fixture,
        &mount,
        &shed,
        "gate.txt",
        "alpha\nbeta main\ngamma shed\ndelta\nepsilon\n",
    );
    fixture.parent_commit(
        "gate.txt",
        "alpha main\nbeta main\ngamma shed\ndelta\nepsilon main\n",
        "moved on",
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 1, 3));
    assert!(landing.commits.fully_landed(), "{landing:?}");
}

/// The same pick, but the lander's resolution dropped the workspace's line for one of its own:
/// the content differs, and the measurement names the file and why.
#[tokio::test]
async fn a_pick_resolved_without_the_workspace_line_names_the_file_as_a_conflict() {
    let fixture = Fixture::new("dropped-pick");
    fixture.parent_commit("gate.txt", "alpha\nbeta\ngamma\n", "gate");
    let mount = fixture.clone_workspace("gate-coverage");
    workspace_commit(
        &mount,
        "gate.txt",
        "alpha\nbeta shed\ngamma\n",
        "cover the gate",
    );
    workspace_commit(&mount, "kept.txt", "kept\n", "kept work");
    let shed = git(&mount, ["rev-parse", "HEAD~1"]).trim().to_owned();
    let kept = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    fixture.parent_commit("gate.txt", "alpha\nbeta main\ngamma\n", "same line");
    pick_resolving(
        &fixture,
        &mount,
        &shed,
        "gate.txt",
        "alpha\nbeta main and more\ngamma\n",
    );
    git(fixture.parent(), ["cherry-pick", &kept]);

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (1, 1, 3));
    assert_eq!(
        unlanded_paths(&landing),
        [("gate.txt", &UnlandedChange::Conflict)],
        "kept.txt is held; only the file whose change the target lacks is named"
    );
    assert!(!landing.commits.fully_landed());
}

/// The commits the target holds neither by content nor by patch identity, as `rm`'s refusal names
/// them: oid and subject, oldest first.
async fn unmatched(fixture: &Fixture, mount: &Path) -> Vec<(String, String)> {
    let target = resolve_target(fixture.store(), &fixture.parent(), TARGET)
        .await
        .expect("main resolves");
    unmatched_commits(
        fixture.store(),
        &fixture.parent(),
        target.tip(),
        mount,
        "HEAD",
    )
    .await
    .expect("the unmatched commits are named")
    .into_iter()
    .map(|commit| (commit.oid.to_string(), commit.subject))
    .collect()
}

/// Pick the workspace commit `oid` into the parent cleanly.
fn pick(fixture: &Fixture, mount: &Path, oid: &str) {
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            mount.as_os_str(),
            OsStr::new("HEAD"),
        ],
    );
    git(fixture.parent(), ["cherry-pick", oid]);
}

/// The landing batch sheds were refused for: main took the workspace's commit by
/// cherry-pick, then edited the very line it changed. Main's content no longer carries the
/// workspace's line, so the content proof alone would name `gate.txt`; main holds a
/// patch-equivalent commit, so the work is landed and `rm` needs no flag.
#[tokio::test]
async fn a_picked_commit_main_then_edited_past_is_landed_by_patch_identity() {
    let fixture = Fixture::new("picked-then-edited");
    fixture.parent_commit("gate.txt", "alpha\nbeta\ngamma\n", "gate");
    let mount = fixture.clone_workspace("gate-coverage");
    workspace_commit(
        &mount,
        "gate.txt",
        "alpha\nbeta shed\ngamma\n",
        "cover the gate",
    );
    let shed = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    pick(&fixture, &mount, &shed);
    fixture.parent_commit(
        "gate.txt",
        "alpha\nbeta main\ngamma\n",
        "main rewrites the line",
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (0, 1, 2), "{landing:?}");
    assert!(landing.commits.fully_landed(), "{landing:?}");
    assert_eq!(unmatched(&fixture, &mount).await, []);
}

/// One commit picked and edited past, one never taken: only the commit held by neither proof is
/// unlanded, and the file only the picked commit changed is not named.
#[tokio::test]
async fn an_unpicked_commit_beside_a_picked_one_is_named_and_the_picked_file_is_not() {
    let fixture = Fixture::new("picked-and-not");
    fixture.parent_commit("gate.txt", "alpha\nbeta\ngamma\n", "gate");
    let mount = fixture.clone_workspace("gate-coverage");
    workspace_commit(
        &mount,
        "gate.txt",
        "alpha\nbeta shed\ngamma\n",
        "cover the gate",
    );
    let shed = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    workspace_commit(&mount, "notes.txt", "never picked\n", "shed notes");
    let notes = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    pick(&fixture, &mount, &shed);
    fixture.parent_commit(
        "gate.txt",
        "alpha\nbeta main\ngamma\n",
        "main rewrites the line",
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (1, 1, 2), "{landing:?}");
    assert_eq!(
        unlanded_paths(&landing),
        [("notes.txt", &UnlandedChange::Added)]
    );
    assert_eq!(
        unmatched(&fixture, &mount).await,
        [(notes, String::from("shed notes"))]
    );
}

/// A squash of two commits matches neither one's patch, so once main edits past the squashed
/// lines no proof holds them: both commits are unlanded and named. Patch identity admits a commit
/// main holds verbatim, never a squash or a resolution it cannot see in any single patch.
#[tokio::test]
async fn a_squash_main_then_edited_past_is_held_by_neither_proof_and_names_each_commit() {
    let fixture = Fixture::new("squashed-then-edited");
    fixture.parent_commit("gate.txt", "alpha\nbeta\ngamma\n", "gate");
    let mount = fixture.clone_workspace("gate-coverage");
    workspace_commit(
        &mount,
        "gate.txt",
        "alpha\nbeta shed\ngamma\n",
        "cover beta",
    );
    let first = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    workspace_commit(
        &mount,
        "gate.txt",
        "alpha\nbeta shed\ngamma shed\n",
        "cover gamma",
    );
    let second = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    fixture.land_content_separately(
        "gate.txt",
        "alpha\nbeta shed\ngamma shed\n",
        "cover the gate, squashed",
    );
    fixture.parent_commit(
        "gate.txt",
        "alpha\nbeta main\ngamma shed\n",
        "main rewrites beta",
    );

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (2, 0, 2), "{landing:?}");
    assert_eq!(
        unlanded_paths(&landing),
        [("gate.txt", &UnlandedChange::Conflict)]
    );
    assert_eq!(
        unmatched(&fixture, &mount).await,
        [
            (first, String::from("cover beta")),
            (second, String::from("cover gamma")),
        ]
    );
}

/// Every way a file can differ reads as itself: an edit the target never took, a file the
/// workspace added, one it deleted, one the target deleted under it, and a mode change.
#[tokio::test]
async fn each_unheld_file_is_named_with_why_the_target_lacks_its_change() {
    let fixture = Fixture::new("reasons");
    fixture.parent_commit("edited.txt", "edited\n", "edited");
    fixture.parent_commit("doomed.txt", "doomed\n", "doomed");
    fixture.parent_commit("orphaned.txt", "orphaned\n", "orphaned");
    fixture.parent_commit("tool.sh", "echo\n", "tool");
    let mount = fixture.clone_workspace("reasons");
    fs::write(mount.join("edited.txt"), "edited here\n").expect("edit");
    fs::write(mount.join("added.txt"), "added\n").expect("add");
    fs::remove_file(mount.join("doomed.txt")).expect("delete");
    fs::write(mount.join("orphaned.txt"), "changed here\n").expect("change");
    git(&mount, ["add", "-A"]);
    git(&mount, ["update-index", "--chmod=+x", "tool.sh"]);
    commit(&mount, "every kind of change");
    fs::remove_file(fixture.parent().join("orphaned.txt")).expect("delete upstream");
    git(fixture.parent(), ["add", "-A"]);
    commit(fixture.parent(), "drop orphaned");

    let landing = measured(&fixture, &mount).await;
    assert_eq!(counts(&landing), (1, 0, 1));
    assert_eq!(
        unlanded_paths(&landing),
        [
            ("added.txt", &UnlandedChange::Added),
            ("doomed.txt", &UnlandedChange::Deleted),
            ("edited.txt", &UnlandedChange::Missing),
            ("orphaned.txt", &UnlandedChange::DeletedInTarget),
            ("tool.sh", &UnlandedChange::Mode),
        ]
    );
}

/// A rebase that resolved a conflict produced content that exists nowhere upstream, and the
/// content rule must notice. This is the false-positive that would matter most: it touches only
/// files the target already changed, so a file-level or branch-level heuristic would wave it
/// through.
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
    let rebase = Command::new("git")
        .arg("-C")
        .arg(&mount)
        .args(["rebase", "FETCH_HEAD"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_bounded()
        .expect("run git rebase");
    assert!(
        !rebase.status.success(),
        "the fixture depends on a conflict"
    );
    fs::write(mount.join("base.txt"), "upstream and workspace\n").expect("resolve conflict");
    git(&mount, ["add", "-A"]);
    let resolved = Command::new("git")
        .arg("-C")
        .arg(&mount)
        .args(["-c", "core.editor=true", "rebase", "--continue"])
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output_bounded()
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

    let target = resolve_target(fixture.store(), &fixture.parent(), "release").await;
    let landing = measure(
        fixture.store(),
        &target,
        &mount,
        &fixture.parent(),
        "topic",
        "HEAD",
    )
    .await;
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

    let target = resolve_target(fixture.store(), &stub, TARGET).await;
    let landing = measure(fixture.store(), &target, &mount, &stub, "topic", "HEAD").await;
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

    let visible = Command::new("git")
        .arg("-C")
        .arg(&mount)
        .args(["cat-file", "-e", &tip])
        .output_bounded()
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
        fixture.store(),
        &resolve_target(fixture.store(), &fixture.parent(), TARGET).await,
        &mount,
        &fixture.parent(),
        "topic",
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

/// Every local branch besides HEAD dies with the workspace image, so each one is measured too. A
/// branch preserved - by the target repository's own branch of that name, or by the
/// `refs/cowshed/<ws>/heads/<branch>` ref `cowshed push` writes in main's repository - or whose
/// patches the target holds is never named; one carrying work found nowhere upstream is, with its
/// tip.
#[tokio::test]
async fn other_local_branches_are_measured_and_only_unpreserved_work_is_named() {
    let fixture = Fixture::new("branches");
    let mount = fixture.clone_workspace("topic");

    git(&mount, ["checkout", "-q", "-b", "parked"]);
    workspace_commit(&mount, "parked.txt", "parked\n", "parked work");
    let parked = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();

    git(&mount, ["checkout", "-q", "-b", "pushed", "topic"]);
    workspace_commit(&mount, "pushed.txt", "pushed\n", "pushed work");
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            mount.as_os_str(),
            OsStr::new("refs/heads/pushed:refs/cowshed/topic/heads/pushed"),
        ],
    );

    git(&mount, ["checkout", "-q", "-b", "squashed", "topic"]);
    workspace_commit(&mount, "squashed.txt", "squashed\n", "squashed work");
    fixture.land_content_separately("squashed.txt", "squashed\n", "squashed upstream");

    // `main` came with the clone and is now an ancestor of the parent's `main`.
    git(&mount, ["checkout", "-q", "topic"]);
    let landing = measured(&fixture, &mount).await;
    assert!(
        landing.commits.fully_landed(),
        "the head itself carries nothing"
    );
    let BranchLandings::Measured { unlanded } = &landing.branches else {
        panic!("the branches were not judged: {:?}", landing.branches);
    };
    assert_eq!(
        unlanded
            .iter()
            .map(|branch| (branch.branch.as_str(), branch.tip.as_str()))
            .collect::<Vec<_>>(),
        [("parked", parked.as_str())],
        "only the branch whose work exists nowhere upstream is named"
    );
    assert!(
        matches!(
            unlanded[0].commits,
            LandingCommits::Measured { unlanded: 1, .. }
        ),
        "{:?}",
        unlanded[0].commits
    );
    assert!(
        !landing.fully_landed(),
        "a landed head beside an unlanded branch is not a landed workspace"
    );
}

/// `cowshed push` preserves the branch the workspace has checked out, and the ref it writes keeps
/// the head as it keeps any branch (02_workspaces.md, `rm` step 1): a head that is the ref's commit
/// or an ancestor of it retires without `--abandon` though the target lacks its commits, and the
/// counts still say what the target lacks. A head past its ref, a ref some other workspace pushed, a
/// detached head, and a head the target already holds each read no ref as keeping.
#[tokio::test]
async fn a_head_the_push_ref_keeps_is_landed_for_removal_and_one_past_it_is_not() {
    let fixture = Fixture::new("head-push-ref");
    let mount = fixture.clone_workspace("topic");
    workspace_commit(&mount, "topic.txt", "topic\n", "topic work");
    let pushed = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    let reference = "refs/cowshed/topic/heads/topic";
    let push = || {
        git(
            fixture.parent(),
            [
                OsStr::new("fetch"),
                OsStr::new("-q"),
                mount.as_os_str(),
                OsStr::new(&format!("+refs/heads/topic:{reference}")),
            ],
        )
    };
    let kept_by = |landing: &WorkspaceLanding, tip: &str| {
        assert_eq!(
            landing.commits.preserved_by(),
            Some(&PushRef {
                reference: reference.to_owned(),
                tip: GitOid::new(tip.to_owned()).expect("an object id"),
            }),
            "{landing:?}"
        );
    };

    let unpushed = measured(&fixture, &mount).await;
    assert_eq!(counts(&unpushed), (1, 0, 0));
    assert_eq!(unpushed.commits.preserved_by(), None, "no push, no ref");
    assert!(!unpushed.fully_landed());

    push();
    let landing = measured(&fixture, &mount).await;
    kept_by(&landing, &pushed);
    assert_eq!(
        counts(&landing),
        (1, 0, 0),
        "the counts are still what the target holds"
    );
    assert!(
        landing.fully_landed(),
        "the ref keeps the head: {landing:?}"
    );
    assert!(
        !landing.commits.held_by_target(),
        "kept is not held: {landing:?}"
    );

    workspace_commit(&mount, "later.txt", "later\n", "later work");
    let ahead = measured(&fixture, &mount).await;
    assert_eq!(counts(&ahead), (2, 0, 0));
    assert_eq!(
        ahead.commits.preserved_by(),
        None,
        "the head is past its ref"
    );
    assert!(!ahead.fully_landed());

    push();
    let newest = git(&mount, ["rev-parse", "HEAD"]).trim().to_owned();
    git(&mount, ["reset", "-q", "--hard", "HEAD~1"]);
    let moved_back = measured(&fixture, &mount).await;
    kept_by(&moved_back, &newest);
    assert!(
        moved_back.fully_landed(),
        "a head moved back from its ref is an ancestor of it"
    );

    let elsewhere = measure(
        fixture.store(),
        &resolve_target(fixture.store(), &fixture.parent(), TARGET).await,
        &mount,
        &fixture.parent(),
        "another",
        "HEAD",
    )
    .await;
    assert_eq!(
        elsewhere.commits.preserved_by(),
        None,
        "a ref pushed under another workspace's name is not this workspace's"
    );

    git(&mount, ["checkout", "-q", "--detach"]);
    let detached = measured(&fixture, &mount).await;
    assert_eq!(
        detached.commits.preserved_by(),
        None,
        "a detached head is on no branch"
    );
    assert!(!detached.fully_landed());

    git(&mount, ["checkout", "-q", "topic"]);
    fixture.land_content_separately("topic.txt", "topic\n", "topic work upstream");
    let landed = measured(&fixture, &mount).await;
    assert!(landed.commits.held_by_target(), "{landed:?}");
    assert_eq!(
        landed.commits.preserved_by(),
        None,
        "a head the target holds asks no ref anything"
    );
}

/// A fold is work the target carries (02_workspaces.md, `land` step 5, "Folds"): a head only the
/// push ref keeps is retirable by `rm` and still no fold, so a land never retires, or names, a
/// session whose work it did not take.
#[tokio::test]
async fn a_head_only_the_push_ref_keeps_is_retirable_and_not_a_fold() {
    let fixture = Fixture::new("fold-push-ref");
    let mount = fixture.clone_workspace("topic");
    let previous =
        GitOid::new(git(&mount, ["rev-parse", "HEAD"]).trim().to_owned()).expect("the base commit");
    workspace_commit(&mount, "story.txt", "topic's story\n", "topic work");
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            mount.as_os_str(),
            OsStr::new("+refs/heads/topic:refs/cowshed/topic/heads/topic"),
        ],
    );
    fixture.land_content_separately("story.txt", "the land's story\n", "the land's own change");

    let landing = measured(&fixture, &mount).await;
    assert!(landing.fully_landed(), "rm retires it: {landing:?}");
    assert!(!landing.commits.held_by_target());

    let target = resolve_target(fixture.store(), &fixture.parent(), TARGET)
        .await
        .expect("main's target");
    let facts = fold_facts(
        fixture.store(),
        &target,
        &std::collections::BTreeSet::from([b"story.txt".to_vec()]),
        &previous,
        &mount,
        &fixture.parent(),
        "topic",
    )
    .await;
    assert_eq!(facts.touched, Ok(true), "the land changed the same path");
    assert!(!facts.landed, "{facts:?}");
    assert_eq!(
        fold_step(&facts),
        None,
        "the land neither retires nor names it"
    );
}

/// One row of a project listing: the fixture's workspace `name`, attached at `mount`.
fn listing_row(name: &str, role: WorkspaceRole, mount: PathBuf) -> WorkspaceInfo {
    WorkspaceInfo {
        repo_id: RepoId::parse("fixture/listing").expect("fixture repo id"),
        workspace: WorkspaceName::new(name).expect("fixture workspace name"),
        workspace_incarnation: WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef")
            .expect("fixture incarnation"),
        role,
        mount,
        state: WorkspaceState::Attached,
        branch: None,
        base_commit: None,
        created_at: None,
        checkpoints: Vec::new(),
        snapshot_stale: false,
        landing: None,
    }
}

/// A listing measures with the controller Git of the store it is given, a fixture's own here,
/// and on macOS that Git writes its Seatbelt profile into that store (04_sandbox.md, "Exec
/// pipeline" step 4). Publish run 39822015838's hosted macOS runner has no host store: a listing that ran its Git
/// for the host's withheld every attached session as unreadable, "cannot write controller git's
/// sandbox profile into /private/cowshed/store/run/profiles".
#[tokio::test]
async fn a_listing_measures_with_the_controller_git_of_the_store_it_is_given() {
    let fixture = Fixture::new("listing-store");
    let quiet = fixture.clone_workspace("quiet");
    let row = listing_row;
    let mut rows = vec![
        row("main", WorkspaceRole::Main, fixture.parent()),
        row("quiet", WorkspaceRole::Workspace, quiet),
    ];

    annotate_listing(fixture.store(), &mut rows).await;

    assert!(
        rows[0].landing.is_none(),
        "main is the target, never measured"
    );
    let landing = rows[1].landing.as_ref().expect("the session was measured");
    assert_eq!(counts(landing), (0, 0, 0));
    // Only macOS runs controller Git under a Seatbelt profile file; elsewhere Git runs unsandboxed
    // and writes none.
    #[cfg(target_os = "macos")]
    {
        let profiles = fs::read_dir(cowshed_core::exec::profile_directory(fixture.store()))
            .expect("the given store holds the controller Git's profile directory")
            .map(|entry| entry.expect("profile entry").path())
            .filter(|path| path.extension() == Some(OsStr::new("sb")))
            .count();
        assert!(
            profiles > 0,
            "the listing's controller Git wrote its profile into the given store"
        );
    }
}

/// `ls --landed` and `rm` read one measurement (02_workspaces.md, `rm` step 1): a shed whose head
/// the push ref keeps is in the landed listing, which `rm` retires with no flag, and one whose head
/// nothing keeps is not, though both carry commits main lacks.
#[tokio::test]
async fn the_landed_listing_keeps_a_shed_whose_head_the_push_ref_keeps() {
    let fixture = Fixture::new("listing-push-ref");
    let pushed = fixture.clone_workspace("pushed");
    workspace_commit(&pushed, "pushed.txt", "pushed\n", "pushed work");
    git(
        fixture.parent(),
        [
            OsStr::new("fetch"),
            OsStr::new("-q"),
            pushed.as_os_str(),
            OsStr::new("+refs/heads/pushed:refs/cowshed/pushed/heads/pushed"),
        ],
    );
    let unpushed = fixture.clone_workspace("unpushed");
    workspace_commit(&unpushed, "unpushed.txt", "unpushed\n", "unpushed work");
    let mut rows = vec![
        listing_row("main", WorkspaceRole::Main, fixture.parent()),
        listing_row("pushed", WorkspaceRole::Workspace, pushed),
        listing_row("unpushed", WorkspaceRole::Workspace, unpushed),
    ];

    annotate_listing(fixture.store(), &mut rows).await;
    let withheld = retain_fully_landed(&mut rows);

    assert_eq!(
        rows.iter()
            .map(|row| row.workspace.as_str())
            .collect::<Vec<_>>(),
        ["pushed"]
    );
    assert!(withheld.is_empty(), "every row was answered: {withheld:?}");
    let landing = rows[0].landing.as_ref().expect("the shed was measured");
    assert_eq!(
        counts(landing),
        (1, 0, 0),
        "the listing still counts what main lacks"
    );
}
