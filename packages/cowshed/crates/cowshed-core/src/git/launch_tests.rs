//! Controller Git launched under its confinement, on every platform: the policy
//! [`super::confined`] decides, rendered by the platform's enforcer and started through the
//! platform's launcher, in real repositories (04_sandbox.md, "Controller Git is separately
//! confined"; "Linux enforcement", "Controller Git"). What a test asserts is what Git did or was
//! refused under the kernel, never the profile's text.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};

use crate::git::GitRepository;
use crate::git::tests::{Repository, git, repository};
use crate::sandbox::tests::fixture_store as store;

#[test]
fn fetching_main_outside_the_workspace_tree_needs_an_explicit_read_root() {
    let store = store();
    let main = repository();
    let parent = main.join("units");
    fs::create_dir_all(&parent).expect("units");
    let workspace = parent.join("raven");
    let cloned = Command::new("git")
        .args(["clone", "-q"])
        .arg(&main)
        .arg(&workspace)
        .status_bounded()
        .expect("clone workspace");
    assert!(cloned.success());
    let main = main.canonicalize().expect("canonical main");
    let workspace = workspace.canonicalize().expect("canonical workspace");
    let source = main.to_str().expect("UTF-8 fixture");
    let denied = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect("narrow Git")
        .args(["fetch", "--no-tags", source])
        .output_bounded()
        .expect("sandboxed fetch");
    assert!(
        !denied.status.success(),
        "a repository URL cannot grant its own read"
    );
    let admitted = crate::git::sandboxed_git_fetch_command(store.shared(), &workspace, &main)
        .expect("trusted read grant")
        .args(["fetch", "--no-tags", source])
        .output_bounded()
        .expect("sandboxed fetch");
    assert!(
        admitted.status.success(),
        "trusted source fetch: {}",
        String::from_utf8_lossy(&admitted.stderr)
    );
}

/// Main's config is a linked worktree's config, and a file it includes by a path relative to
/// main's `.git` is in main's working tree, which the worktree's sandboxed Git cannot
/// otherwise read: every controller verb on a git-worktree workspace failed at its first Git
/// command with "unable to access …/.git/../tooling/workspace.gitconfig".
#[test]
fn a_linked_worktree_reads_the_files_its_common_config_includes() {
    let store = store();
    let main = repository();
    fs::create_dir_all(main.join("tooling")).expect("tooling");
    fs::write(
        main.join("tooling/workspace.gitconfig"),
        "[core]\n\tabbrev = 12\n",
    )
    .expect("included config");
    git(
        &main,
        &["config", "include.path", "../tooling/workspace.gitconfig"],
    );
    let workspace = main.with_extension("linked");
    let added = Command::new("git")
        .arg("-C")
        .arg(&*main)
        .args(["worktree", "add", "-q", "--detach"])
        .arg(&workspace)
        .status_bounded()
        .expect("add linked worktree");
    assert!(added.success());
    let workspace = workspace.canonicalize().expect("canonical workspace");
    let output = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect("narrow Git")
        .args(["config", "core.abbrev"])
        .output_bounded()
        .expect("sandboxed Git");
    assert!(
        output.status.success(),
        "the include is readable: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"12\n");
    let head = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect("narrow Git")
        .args(["rev-parse", "HEAD"])
        .output_bounded()
        .expect("sandboxed Git");
    assert!(
        head.status.success(),
        "HEAD reads: {}",
        String::from_utf8_lossy(&head.stderr)
    );
}

/// A git-worktree workspace whose main config includes each of `includes`, and main.
fn linked_worktree(includes: &[&str]) -> (Repository, PathBuf) {
    let main = repository();
    for include in includes {
        git(&main, &["config", "--add", "include.path", include]);
    }
    let workspace = main.with_extension("linked");
    let added = Command::new("git")
        .arg("-C")
        .arg(&*main)
        .args(["worktree", "add", "-q", "--detach"])
        .arg(&workspace)
        .status_bounded()
        .expect("add linked worktree");
    assert!(added.success());
    let workspace = workspace.canonicalize().expect("canonical workspace");
    (main, workspace)
}

fn sandboxed_config(store_root: &Path, workspace: &Path, key: &str) -> String {
    let output = crate::git::sandboxed_git_command_at(store_root, workspace)
        .expect("narrow Git")
        .args(["config", key])
        .output_bounded()
        .expect("sandboxed Git");
    assert!(
        output.status.success(),
        "{key}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("UTF-8 value")
}

/// `~/` is the HOME the sandboxed Git runs with, inside the workspace; a conditional include
/// inside main's checkout is followed and its own relative include is granted too.
#[test]
fn linked_worktree_includes_resolve_home_conditions_and_nesting() {
    let store = store();
    let (main, workspace) = linked_worktree(&["~/home.gitconfig"]);
    git(
        &main,
        &[
            "config",
            "includeIf.gitdir:/.path",
            "../tooling/outer.gitconfig",
        ],
    );
    fs::create_dir_all(main.join("tooling")).expect("tooling");
    fs::write(
        main.join("tooling/outer.gitconfig"),
        "[include]\n\tpath = inner.gitconfig\n",
    )
    .expect("outer");
    fs::write(
        main.join("tooling/inner.gitconfig"),
        "[core]\n\tabbrev = 11\n",
    )
    .expect("inner");
    fs::create_dir_all(workspace.join(".cowshed/home")).expect("git home");
    fs::write(
        workspace.join(".cowshed/home/home.gitconfig"),
        "[user]\n\tname = Home Include\n",
    )
    .expect("home include");
    assert_eq!(
        sandboxed_config(store.shared(), &workspace, "core.abbrev"),
        "11\n"
    );
    assert_eq!(
        sandboxed_config(store.shared(), &workspace, "user.name"),
        "Home Include\n"
    );
}

/// An include outside main's checkout stays denied: the controller refuses it by name rather
/// than granting it, and never opens a device or a FIFO a job named.
#[test]
fn linked_worktree_includes_outside_main_or_not_regular_are_refused() {
    let store = store();
    let (main, workspace) = linked_worktree(&[]);
    // Written by file, from outside the repository: Git opening main would read the include.
    let config = main.join(".git/config");
    let include = |target: &str| {
        let status = Command::new("git")
            .args(["config", "--file"])
            .arg(&config)
            .args(["include.path", target])
            .current_dir("/")
            .status_bounded()
            .expect("git config --file");
        assert!(status.success());
    };
    let outside = main.with_extension("outside.gitconfig");
    fs::write(&outside, "[core]\n\tabbrev = 13\n").expect("outside include");
    for target in [outside.to_str().expect("UTF-8 path"), "/dev/zero"] {
        include(target);
        let refused = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
            .expect_err("an include outside main is refused");
        assert!(
            refused.message.contains("outside main's checkout"),
            "{}",
            refused.message
        );
    }
    let fifo = main.join("tooling.fifo");
    let made = Command::new("mkfifo")
        .arg(&fifo)
        .status_bounded()
        .expect("mkfifo");
    assert!(made.success());
    include("../tooling.fifo");
    let refused = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect_err("a FIFO include is refused without blocking");
    assert!(
        refused.message.contains("is not a regular file"),
        "{}",
        refused.message
    );
}

/// The config a workspace's jobs write cannot run a filter under the controller's read of
/// main's checkout: the same `git add` that runs the clean filter unconfined passes the file
/// through unchanged under the controller's Git.
#[test]
fn linked_worktree_controller_git_runs_no_repository_filter() {
    let store = store();
    let (main, workspace) = linked_worktree(&[]);
    git(
        &main,
        &[
            "config",
            "filter.marker.clean",
            "sh -c 'echo ran >> .filter-ran; cat'",
        ],
    );
    fs::write(workspace.join(".gitattributes"), "README filter=marker\n").expect("attributes");
    fs::write(workspace.join("README"), "changed\n").expect("change README");
    let added = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect("narrow Git")
        .args(["add", "README"])
        .output_bounded()
        .expect("sandboxed add");
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    assert!(
        !workspace.join(".filter-ran").exists(),
        "the controller's Git ran the repository's clean filter"
    );
    // New content, so the unconfined add cleans the file rather than trusting the index.
    fs::write(workspace.join("README"), "changed again\n").expect("change README again");
    git(&workspace, &["add", "README"]);
    assert!(
        workspace.join(".filter-ran").exists(),
        "unconfined Git runs the filter, so the fixture would have shown it"
    );
}

/// A git-worktree workspace whose main config includes a file of main's checkout, so its
/// controller Git holds that read, with a forged signed commit in main and an executable in
/// the workspace that records it ran: what a job could leave behind for `rm`'s `git log` or
/// a land's `git merge` to run.
fn worktree_with_a_planted_program() -> (Repository, PathBuf, String, PathBuf) {
    use cowshed_os::fork_lock::Spawn as _;
    let (main, workspace) = linked_worktree(&["../tooling/workspace.gitconfig"]);
    fs::create_dir_all(main.join("tooling")).expect("tooling");
    fs::write(
        main.join("tooling/workspace.gitconfig"),
        "[core]\n\tabbrev = 12\n",
    )
    .expect("included config");
    let marker = workspace.join(".program-ran");
    let program = workspace.join("planted-gpg");
    fs::write(
        &program,
        format!("#!/bin/sh\necho ran >> '{}'\nexit 1\n", marker.display()),
    )
    .expect("planted program");
    fs::set_permissions(
        &program,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .expect("executable");
    let rev = |spec: &str| {
        let output = Command::new("git")
            .arg("-C")
            .arg(&*main)
            .args(["rev-parse", spec])
            .output_bounded()
            .expect("rev-parse");
        String::from_utf8(output.stdout)
            .expect("oid")
            .trim()
            .to_owned()
    };
    let body = format!(
        "tree {}\nparent {}\nauthor Forger <f@example.invalid> 0 +0000\ncommitter Forger <f@example.invalid> 0 +0000\ngpgsig -----BEGIN PGP SIGNATURE-----\n \n forged\n -----END PGP SIGNATURE-----\n\nforged\n",
        rev("HEAD^{tree}"),
        rev("HEAD")
    );
    let mut hash = Command::new("git")
        .arg("-C")
        .arg(&*main)
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn_locked()
        .expect("hash-object");
    std::io::Write::write_all(&mut hash.stdin.take().expect("stdin"), body.as_bytes())
        .expect("write commit");
    let output = hash.wait_with_output_bounded().expect("hash-object output");
    assert!(output.status.success());
    let forged = String::from_utf8(output.stdout)
        .expect("oid")
        .trim()
        .to_owned();
    git(
        &main,
        &["config", "gpg.program", program.to_str().expect("UTF-8")],
    );
    (main, workspace, forged, marker)
}

/// `log.showSignature` makes `git log` run `gpg.program` on a signed commit. The controller's
/// Git for a workspace holding main-tree reads cannot run it: only Git's own executables run.
#[test]
fn linked_worktree_controller_git_runs_no_gpg_program_for_log() {
    let store = store();
    let (main, workspace, forged, marker) = worktree_with_a_planted_program();
    git(&main, &["config", "log.showSignature", "true"]);
    let _ = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect("narrow Git")
        // Git writes the signature to a temporary file before it runs gpg.program; a TMPDIR
        // the sandbox can write isolates the exec rule as the only thing that stops it.
        .env("TMPDIR", &workspace)
        .args(["log", "-1", "--pretty=format:%H", &forged])
        .output_bounded()
        .expect("sandboxed log");
    assert!(!marker.exists(), "the controller's Git ran gpg.program");
    let _ = Command::new("git")
        .arg("-C")
        .arg(&workspace)
        .args(["log", "-1", "--pretty=format:%H", &forged])
        .output_bounded()
        .expect("unconfined log");
    assert!(
        marker.exists(),
        "unconfined Git runs it, so the fixture would have shown it"
    );
}

/// `merge.verifySignatures` makes `git merge` run `gpg.program` on the commit it would take.
#[test]
fn linked_worktree_controller_git_runs_no_gpg_program_for_merge() {
    let store = store();
    let (main, workspace, forged, marker) = worktree_with_a_planted_program();
    git(&main, &["config", "merge.verifySignatures", "true"]);
    let merged = crate::git::sandboxed_git_command_at(store.shared(), &workspace)
        .expect("narrow Git")
        .env("TMPDIR", &workspace)
        .args(["merge", "--ff-only", &forged])
        .output_bounded()
        .expect("sandboxed merge");
    assert!(
        !merged.status.success(),
        "an unverifiable commit is not merged"
    );
    assert!(!marker.exists(), "the controller's Git ran gpg.program");
}

/// `rm`'s unlanded-work proof and `ls --landing` read a linked worktree through another
/// repository's object store (`with_alternate_objects`). That is a read, not a fetch: no shell
/// runs, so it works under the Git-only profile that main-tree include grants impose.
#[tokio::test]
async fn an_alternate_object_read_works_under_main_tree_include_grants() {
    let store = store();
    let (main, workspace) = linked_worktree(&["../tooling/workspace.gitconfig"]);
    fs::create_dir_all(main.join("tooling")).expect("tooling");
    fs::write(
        main.join("tooling/workspace.gitconfig"),
        "[core]\n\tabbrev = 12\n",
    )
    .expect("included config");
    let other = repository();
    let objects = other.join(".git/objects").canonicalize().expect("objects");
    let held = GitRepository::from_root(store.shared(), &workspace)
        .with_alternate_objects(objects)
        .expect("alternate objects")
        .content_not_held("HEAD", "HEAD")
        .await
        .expect("an alternate-object read is not a fetch");
    assert_eq!(held, crate::git::ContentNotHeld::default());
}

/// A driver added after the command was built — by a job of this or a sibling workspace,
/// between construction and spawn — is named by no override, and still cannot run.
#[test]
fn a_driver_added_after_construction_cannot_run() {
    let store = store();
    let (main, workspace, _, marker) = worktree_with_a_planted_program();
    let mut command =
        crate::git::sandboxed_git_command_at(store.shared(), &workspace).expect("narrow Git");
    git(
        &main,
        &[
            "config",
            "filter.late.clean",
            workspace.join("planted-gpg").to_str().expect("UTF-8"),
        ],
    );
    fs::write(workspace.join(".gitattributes"), "README filter=late\n").expect("attributes");
    fs::write(workspace.join("README"), "changed\n").expect("change README");
    let _ = command
        .args(["add", "README"])
        .output_bounded()
        .expect("sandboxed add");
    assert!(
        !marker.exists(),
        "a late driver ran under the controller's Git"
    );
}

/// Controller Git run for a fixture writes its sandbox profile into the fixture's own store,
/// never the host's (04_sandbox.md, "Exec pipeline" step 4). A runner without a host store,
/// like CI, otherwise refuses every controller Git command for a store nobody asked it to
/// serve.
#[tokio::test]
async fn controller_git_writes_its_profile_into_the_store_it_serves() {
    let store = store();
    let root = repository();
    let head = GitRepository::from_root(store.shared(), &root)
        .head_oid()
        .await
        .expect("controller Git runs for the fixture's store");
    assert_eq!(head.as_str().len(), 40);
    let profiles: Vec<PathBuf> = fs::read_dir(crate::exec::profile_directory(store.shared()))
        .expect("the served store holds the profile directory")
        .map(|entry| entry.expect("profile entry").path())
        .filter(|path| path.extension() == Some(std::ffi::OsStr::new("sb")))
        .collect();
    assert_eq!(
        profiles.len(),
        1,
        "one profile for one plain checkout: {profiles:?}"
    );
}

/// Repeated `git` spawns through the process-wide fork-lock recorder use a single resolved
/// absolute program path instead of a bare `"git"` that walks `PATH` on every spawn
/// (`fork_lock.rs`, `11_shell.md`).
#[test]
fn repeated_git_spawns_record_absolute_program_paths() {
    let root = repository();
    let recorded = cowshed_os::fork_lock::spawn_count::ProcessSpawns::start();
    for _ in 0..2 {
        let output = crate::git::git_command_at(&root)
            .args(["rev-parse", "HEAD"])
            .output_bounded()
            .expect("run git rev-parse");
        assert!(
            output.status.success(),
            "git rev-parse failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let programs = recorded.programs();
    assert!(
        programs.len() >= 2,
        "expected at least two recorded git spawns, got {programs:?}"
    );
    for program in &programs {
        assert!(
            Path::new(program).is_absolute(),
            "repeated git spawn used bare program name instead of resolved absolute path: {programs:?}"
        );
    }
    assert!(
        programs.windows(2).all(|pair| pair[0] == pair[1]),
        "repeated git spawns resolved differently: {programs:?}"
    );
    let sandboxed = crate::git::sandboxed_git();
    assert!(
        sandboxed.program.is_absolute(),
        "sandboxed_git program is not an absolute path: {:?}",
        sandboxed.program
    );
}
