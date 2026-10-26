//! `cowshed doc-commit`, run as the binary an agent runs, on a real git repository whose main
//! `.cowshed.toml` declares `[land]` and `[doc]` (02_workspaces.md "`cowshed doc-commit`"): an
//! edit is applied and committed in one hold of the ref turn, a path that is not documentation or
//! that main held uncommitted is refused with main's tree put back, and a doc commit waits in line
//! behind the turn's holder.
#![cfg(unix)]

use std::fs;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};

use cowshed_core::host_dirs::support_directory;
use cowshed_core::land_turn::{land_turn_root, observe};

const TURN: &str = "docrepo";
const POLL: Duration = Duration::from_millis(20);

fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A fresh HOME and a main checkout beside it: a git repository with one commit holding
/// `README.md`, `src/lib.rs` and the `.cowshed.toml` of `config`.
struct Repo {
    root: PathBuf,
}

impl Repo {
    fn new(doc: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("cowshed-doc-commit-{}-{n}", std::process::id()));
        fs::create_dir_all(root.join("home")).expect("create the test HOME");
        fs::create_dir_all(root.join("bin")).expect("create the test PATH directory");
        let root = fs::canonicalize(&root).expect("canonicalize the test root");
        let repo = Self { root };
        let main = repo.main();
        fs::create_dir_all(main.join("src")).expect("create main");
        git(&main, &["init", "-q", "-b", "main"]);
        git(&main, &["config", "user.email", "doc@example.invalid"]);
        git(&main, &["config", "user.name", "Doc Commit"]);
        fs::write(main.join("README.md"), "# readme\n").expect("write README.md");
        fs::write(main.join("src/lib.rs"), "fn lib() {}\n").expect("write src/lib.rs");
        fs::write(
            main.join(".cowshed.toml"),
            format!("[land]\nturn = \"{TURN}\"\ngate = [\"true\"]\n\n[doc]\n{doc}"),
        )
        .expect("write .cowshed.toml");
        git(&main, &["add", "-A"]);
        git(&main, &["commit", "-q", "-m", "initial"]);
        repo
    }

    fn main(&self) -> PathBuf {
        self.root.join("main")
    }

    fn home(&self) -> PathBuf {
        self.root.join("home")
    }

    /// The ref turn's queue directory under this HOME.
    fn ref_turn(&self) -> PathBuf {
        land_turn_root(&support_directory(&self.home())).join(format!("{TURN}.ref"))
    }

    /// A file in the test's own directory.
    fn file(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.root.join(name);
        fs::write(&path, contents).expect("write a test input");
        path
    }

    /// An executable `icm` on the test's PATH that runs `script` as the program would.
    fn icm(&self, script: &str) {
        let path = self.root.join("bin/icm");
        fs::write(&path, format!("#!/bin/sh\n{script}\n")).expect("write the fake icm");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("make icm executable");
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(cowshed());
        let path = format!(
            "{}:{}",
            self.root.join("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        command
            .env("HOME", self.home())
            .env("PATH", path)
            .current_dir(self.main())
            .args(args)
            .stdin(Stdio::null());
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args)
            .output_bounded()
            .expect("run cowshed doc-commit")
    }

    fn head(&self) -> String {
        stdout(&self.main(), &["rev-parse", "HEAD"])
    }

    fn status(&self) -> String {
        stdout(
            &self.main(),
            &["status", "--porcelain", "--untracked-files=all"],
        )
    }
}

impl Drop for Repo {
    fn drop(&mut self) {
        let removed = fs::remove_dir_all(&self.root);
        if !thread::panicking() {
            removed.expect("remove the test root");
        }
    }
}

fn git(directory: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .status_bounded()
        .expect("run git");
    assert!(
        status.success(),
        "git {args:?} failed in {}",
        directory.display()
    );
}

fn stdout(directory: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .output_bounded()
        .expect("run git");
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8(output.stdout)
        .expect("git prints UTF-8")
        .trim()
        .to_owned()
}

fn said(output: &Output) -> String {
    format!(
        "status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

const NOTES_PATCH: &str = "diff --git a/docs/notes.md b/docs/notes.md\nnew file mode 100644\n--- /dev/null\n+++ b/docs/notes.md\n@@ -0,0 +1 @@\n+notes\n";
const README_PATCH: &str = "diff --git a/README.md b/README.md\n--- a/README.md\n+++ b/README.md\n@@ -1 +1,2 @@\n # readme\n+more\n";
const CODE_PATCH: &str = "diff --git a/README.md b/README.md\n--- a/README.md\n+++ b/README.md\n@@ -1 +1,2 @@\n # readme\n+more\ndiff --git a/src/lib.rs b/src/lib.rs\n--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1,2 @@\n fn lib() {}\n+fn more() {}\n";

/// A Markdown patch is committed as exactly that commit, the ref turn's log records the hold, and
/// main's tree is clean after it.
#[test]
fn a_markdown_patch_commits_exactly_its_paths_under_the_ref_turn() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let before = repo.head();
    let patch = repo.file("notes.diff", NOTES_PATCH);
    let output = repo.run(&[
        "--json",
        "doc-commit",
        "-m",
        "docs: add notes",
        "--patch",
        patch.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{}", said(&output));
    let envelope: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("the report is JSON");
    let head = repo.head();
    assert_eq!(
        envelope["result"]["commit"],
        head.as_str(),
        "{}",
        said(&output)
    );
    assert_eq!(
        envelope["result"]["paths"],
        serde_json::json!(["docs/notes.md"])
    );
    assert_eq!(stdout(&repo.main(), &["rev-parse", "HEAD~1"]), before);
    assert_eq!(
        stdout(
            &repo.main(),
            &["show", "--name-only", "--format=%s", "HEAD"]
        ),
        "docs: add notes\n\ndocs/notes.md"
    );
    assert_eq!(repo.status(), "", "main's tree is clean after the commit");
    let log = fs::read_to_string(repo.ref_turn().join("turns.log")).expect("the turn's log");
    let events: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).expect("a turn log line"))
        .collect();
    assert_eq!(
        events
            .iter()
            .map(|event| event["event"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["queued", "taken", "finished"],
        "{log}"
    );
    assert_eq!(events[2]["ticket"]["workspace"], "doc-commit");
    assert_eq!(events[2]["outcome"], format!("committed {head}"));
}

/// A patch that also writes code is refused before main's tree is touched.
#[test]
fn a_patch_that_writes_code_is_refused_and_main_is_untouched() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let before = repo.head();
    let patch = repo.file("code.diff", CODE_PATCH);
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs: sneak",
        "--patch",
        patch.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(4), "{}", said(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("src/lib.rs"),
        "the refusal names the code path: {stderr}"
    );
    assert!(
        stderr.contains("cowshed land"),
        "the hint names the land route: {stderr}"
    );
    assert_eq!(repo.head(), before);
    assert_eq!(repo.status(), "");
}

/// A path main holds uncommitted is someone else's edit: refused, and its bytes kept.
#[test]
fn a_patch_to_a_path_main_holds_uncommitted_is_refused_and_that_edit_kept() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let before = repo.head();
    fs::write(repo.main().join("README.md"), "# readme\nsomeone else's\n").expect("dirty README");
    let patch = repo.file("readme.diff", README_PATCH);
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs: more",
        "--patch",
        patch.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(4), "{}", said(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("uncommitted edits at README.md"),
        "{}",
        said(&output)
    );
    assert_eq!(repo.head(), before);
    assert_eq!(
        fs::read_to_string(repo.main().join("README.md")).unwrap(),
        "# readme\nsomeone else's\n",
        "the held edit is kept byte for byte"
    );
}

/// An ICM program's paths are what it wrote: one that also writes code is refused and every path
/// it wrote is put back, a held path to its own bytes.
#[test]
fn an_icm_program_that_writes_code_is_refused_and_put_back() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let before = repo.head();
    fs::write(repo.main().join("held.md"), "held\n").expect("an untracked held path");
    repo.icm(
        "root=$4\nprintf 'more\\n' >> \"$root/README.md\"\nprintf 'x\\n' >> \"$root/src/lib.rs\"\nprintf 'new\\n' > \"$root/new.md\"\nprintf 'clobbered\\n' > \"$root/held.md\"\necho '{\"state\":\"OK\"}'",
    );
    let program = repo.file("edit.ts", "// the fake icm ignores its program\n");
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs: icm",
        "--icm",
        program.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(4), "{}", said(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("src/lib.rs"),
        "{}",
        said(&output)
    );
    assert_eq!(repo.head(), before);
    assert_eq!(
        repo.status(),
        "?? held.md",
        "only the held path is left, as it was"
    );
    assert_eq!(
        fs::read_to_string(repo.main().join("held.md")).unwrap(),
        "held\n"
    );
}

/// A held path is put back as the entry it was, never through it: a held deletion the program
/// recreated is removed again, and a held symlink it retargeted points where it did, with neither
/// file it named rewritten.
#[test]
fn a_refused_icm_program_puts_back_a_held_deletion_and_a_held_symlink() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    fs::write(repo.main().join("a.md"), "a\n").expect("write a.md");
    fs::write(repo.main().join("b.md"), "b\n").expect("write b.md");
    git(&repo.main(), &["add", "a.md", "b.md"]);
    git(&repo.main(), &["commit", "-q", "-m", "a and b"]);
    let before = repo.head();
    fs::remove_file(repo.main().join("README.md")).expect("a held deletion");
    std::os::unix::fs::symlink("a.md", repo.main().join("link.md")).expect("a held symlink");
    repo.icm(
        "root=$4\nprintf 'back\\n' > \"$root/README.md\"\nln -sfn b.md \"$root/link.md\"\necho '{\"state\":\"OK\"}'",
    );
    let program = repo.file("edit.ts", "// the fake icm ignores its program\n");
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs: held",
        "--icm",
        program.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(4), "{}", said(&output));
    assert_eq!(repo.head(), before);
    assert!(
        !repo.main().join("README.md").exists(),
        "the held deletion is put back absent: {}",
        said(&output)
    );
    assert_eq!(
        fs::read_link(repo.main().join("link.md")).expect("link.md is a symlink again"),
        PathBuf::from("a.md")
    );
    assert_eq!(fs::read_to_string(repo.main().join("a.md")).unwrap(), "a\n");
    assert_eq!(fs::read_to_string(repo.main().join("b.md")).unwrap(), "b\n");
    assert_eq!(repo.status(), "D README.md\n?? link.md");
}

/// An ICM program that writes only documentation commits what it wrote, and the declared check
/// runs on those paths and may rewrite them before the commit.
#[test]
fn an_icm_program_commits_what_it_wrote_after_the_declared_check() {
    let repo = Repo::new(
        "paths = [\"*.md\"]\ncheck = [\"sh\", \"-c\", \"for p; do printf 'checked\\\\n' >> \\\"$p\\\"; done\", \"check\"]\n",
    );
    repo.icm("printf 'more\\n' >> \"$4/README.md\"\necho '{\"state\":\"OK\"}'");
    let program = repo.file("edit.ts", "// the fake icm ignores its program\n");
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs: icm",
        "--icm",
        program.to_str().unwrap(),
    ]);
    assert!(output.status.success(), "{}", said(&output));
    assert_eq!(
        stdout(&repo.main(), &["show", "HEAD:README.md"]),
        "# readme\nmore\nchecked"
    );
    assert_eq!(repo.status(), "");
}

/// A doc commit asks for the ref turn in line: while another writer holds it, the doc commit
/// says who and waits, and it commits once the holder ends.
#[test]
fn a_doc_commit_waits_in_line_behind_the_ref_turn_holder() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let release = repo.root.join("release");
    let mut holder: Child = repo
        .command(&[
            "land-turn",
            &format!("{TURN}.ref"),
            "--workspace",
            "holder",
            "--",
            "sh",
            "-c",
            &format!("while [ ! -e {} ]; do sleep 0.02; done", release.display()),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the ref turn's holder");
    while observe(&repo.ref_turn())
        .map(|tickets| tickets.len())
        .unwrap_or(0)
        != 1
    {
        thread::sleep(POLL);
    }
    let patch = repo.file("notes.diff", NOTES_PATCH);
    let mut doc = repo
        .command(&[
            "doc-commit",
            "-m",
            "docs: waited",
            "--patch",
            patch.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the doc commit");
    let mut stderr = BufReader::new(doc.stderr.take().unwrap());
    let mut line = String::new();
    stderr
        .read_line(&mut line)
        .expect("the doc commit reports its wait");
    assert!(line.contains("holder"), "the wait names the holder: {line}");
    let before = repo.head();
    assert_eq!(
        observe(&repo.ref_turn()).unwrap().len(),
        2,
        "the doc commit waits behind the holder"
    );
    fs::File::create(&release)
        .and_then(|mut file| file.write_all(b"go"))
        .expect("release the holder");
    assert!(holder.wait_bounded().expect("the holder ends").success());
    let output = doc.wait_with_output_bounded().expect("the doc commit ends");
    assert!(output.status.success(), "{}", said(&output));
    assert_ne!(repo.head(), before);
    assert_eq!(
        stdout(&repo.main(), &["log", "-1", "--format=%s"]),
        "docs: waited"
    );
}

/// A doc lane needs `[land]`, whose turn names the ref turn: a `[doc]` alone is refused.
#[test]
fn a_repository_without_a_declared_doc_lane_is_refused() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    fs::write(repo.main().join(".cowshed.toml"), "").expect("declare nothing");
    git(&repo.main(), &["commit", "-q", "-am", "declare nothing"]);
    let patch = repo.file("notes.diff", NOTES_PATCH);
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs",
        "--patch",
        patch.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(5), "{}", said(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("declares no doc lane"),
        "{}",
        said(&output)
    );
}
