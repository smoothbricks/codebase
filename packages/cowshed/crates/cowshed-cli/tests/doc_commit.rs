//! `cowshed doc-commit`, run as the binary an agent runs, on a real git repository whose main
//! `.cowshed.toml` declares `[land]` and `[doc]` (02_workspaces.md "`cowshed doc-commit`"): an
//! edit is applied and committed in one hold of the ref turn, a path that is not documentation or
//! that main held uncommitted is refused with main's tree put back, and a doc commit waits in line
//! behind the turn's holder. The ref turn is the gateway's: each test runs its own.
#![cfg(unix)]

use std::fs;
use std::io::{BufRead as _, BufReader};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::thread;

use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};

use cowshed_core::api::dto::LandTurnQueue;

#[path = "../../cowshed-core/tests/support/temp_parent.rs"]
mod temp_parent;
#[path = "../../cowshed-core/tests/support/temp_root.rs"]
mod temp_root;
#[path = "support/test_gateway.rs"]
mod test_gateway;

use test_gateway::TestGateway;

const TURN: &str = "docrepo";

fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A fresh HOME and a main checkout beside it: a git repository with one commit holding
/// `README.md`, `src/lib.rs` and the `.cowshed.toml` of `config`, and the gateway whose ref turn
/// its doc commits take.
struct Repo {
    root: PathBuf,
    gateway: TestGateway,
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
        let repo = Self {
            root,
            gateway: TestGateway::start(),
        };
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

    /// How many tickets the gateway holds in the ref turn `<TURN>.ref`.
    fn ref_tickets(&self) -> usize {
        let queue = format!("{TURN}.ref");
        self.gateway
            .land_turns(u64::MAX)
            .into_iter()
            .find(|turns| turns.repo.as_str() == queue)
            .map_or(0, |turns| turns.tickets.len())
    }

    /// Every land-turn queue the gateway lists, the probe's ticket left out, read while a probe
    /// holds the ref turn `<TURN>.ref`: the gateway gives the probe the turn only once it has
    /// processed the departure of every ticket asked before it, which reaches it after their
    /// processes exit.
    fn settled_turns(&self) -> Vec<LandTurnQueue> {
        let (mut probe, _said) = self.hold_ref_turn("probe");
        let mut turns = self.gateway.land_turns(0);
        for queue in &mut turns {
            queue.tickets.retain(|ticket| ticket.workspace != "probe");
        }
        turns.retain(|queue| !queue.tickets.is_empty() || !queue.abandoned.is_empty());
        assert!(probe.wait_bounded().expect("the probe ends").success());
        turns
    }

    /// `cowshed land-turn <TURN>.ref` as `workspace`, answered once it holds the turn: its command
    /// says so on stdout as it starts and holds the turn until its stdin closes. The receiver of
    /// the rest of its stdout goes with it, so nothing it prints later meets a closed pipe.
    fn hold_ref_turn(&self, workspace: &str) -> (Child, mpsc::Receiver<String>) {
        let mut holder = self
            .command(&[
                "land-turn",
                &format!("{TURN}.ref"),
                "--gateway-socket",
                self.gateway.socket.to_str().unwrap(),
                "--workspace",
                workspace,
                "--",
                "sh",
                "-c",
                "echo held && exec cat >/dev/null",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the ref turn's holder");
        let said = lines(holder.stdout.take().expect("stdout is piped"));
        let held = said
            .recv_timeout(cowshed_os::test_backstop::NO_PROGRESS)
            .expect("the holder's command starts before the no-progress backstop");
        assert_eq!(held, "held", "the holder's command says it holds the turn");
        (holder, said)
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

    /// `cowshed <args>`, a doc-commit with its verb and any global flag before it, on this test's
    /// gateway.
    fn doc_commit(&self, args: &[&str]) -> Command {
        let mut command = self.command(args);
        command.arg("--gateway-socket").arg(&self.gateway.socket);
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.doc_commit(args)
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

    /// Install a git that counts the binary's own git runs, then becomes the real git by
    /// exec, so its pid, stdin and exit are the real one's. It stands in this test HOME's
    /// installed binary directory, first on the bootstrap PATH a doc commit's pure
    /// environment resolves git through, so every git the production runner spawns in
    /// that environment is one line of the log and nothing behind git is reproduced.
    fn counting_git(&self) {
        let real = std::env::split_paths(&std::env::var_os("PATH").expect("the test PATH"))
            .map(|directory| directory.join("git"))
            .find(|candidate| {
                candidate.metadata().is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            })
            .expect("git on the test PATH");
        let directory = cowshed_core::host_dirs::installed_binary_directory(&self.home());
        fs::create_dir_all(&directory).expect("create the installed binary directory");
        let path = directory.join("git");
        fs::write(
            &path,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"{}\"\nexec \"{}\" \"$@\"\n",
                self.root.join("git-calls.log").display(),
                real.display()
            ),
        )
        .expect("write the counting git");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("make the counting git executable");
    }

    /// How many of the counted git runs named the given verb (ls-files, ls-tree, ...).
    fn git_calls(&self, verb: &str) -> usize {
        let Ok(calls) = fs::read_to_string(self.root.join("git-calls.log")) else {
            return 0;
        };
        calls
            .lines()
            .filter(|line| line.split_whitespace().any(|word| word == verb))
            .count()
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

fn git_fails(directory: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .status_bounded()
        .expect("run git");
    assert!(
        !status.success(),
        "git {args:?} succeeded in {}",
        directory.display()
    );
}

fn git_bytes(directory: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(args)
        .output_bounded()
        .expect("run git");
    assert!(output.status.success(), "git {args:?}: {output:?}");
    output.stdout
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

/// A Markdown patch is committed as exactly that commit, the hold ends its ref turn rather than
/// abandoning it, and main's tree is clean after it.
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
    assert_eq!(
        repo.settled_turns(),
        [],
        "the doc commit ended its ref turn with its outcome"
    );
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

/// The lines `stream` carries, read to its end on a thread of its own, so the test waits for each
/// within the no-progress backstop (`recv_timeout`).
fn lines(stream: impl std::io::Read + Send + 'static) -> mpsc::Receiver<String> {
    let (lines, heard) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    heard
}
/// A doc commit asks for the ref turn in line: while another writer holds it, the doc commit
/// says who and waits, and it commits once the holder ends.
#[test]
fn a_doc_commit_waits_in_line_behind_the_ref_turn_holder() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let (mut holder, _holder_said) = repo.hold_ref_turn("holder");
    let patch = repo.file("notes.diff", NOTES_PATCH);
    let mut doc = repo
        .doc_commit(&[
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
    // The doc commit's stderr, line by line, so its wait is read within a bound.
    let heard = lines(doc.stderr.take().expect("stderr is piped"));
    let line = heard
        .recv_timeout(cowshed_os::test_backstop::NO_PROGRESS)
        .expect("the doc commit reports its wait before the no-progress backstop");
    assert!(line.contains("holder"), "the wait names the holder: {line}");
    let before = repo.head();
    assert_eq!(
        repo.ref_tickets(),
        2,
        "the doc commit waits behind the holder"
    );
    // The holder's command ends once its stdin closes, and with it the holder's turn.
    drop(holder.stdin.take());
    assert!(holder.wait_bounded().expect("the holder ends").success());
    let output = doc.wait_with_output_bounded().expect("the doc commit ends");
    assert!(
        output.status.success(),
        "{}\nstderr: {:?}",
        said(&output),
        heard.try_iter().collect::<Vec<_>>()
    );
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

/// Keeping the paths main holds uncommitted reads the index once, however many it holds,
/// and not at all when it holds none: one counted ls-files for 1, 8 and 64 held paths,
/// none for zero, with the edit itself committing exactly as before.
#[test]
fn keeping_held_paths_reads_the_index_once_however_many_are_held() {
    for held in [0usize, 1, 8, 64] {
        let repo = Repo::new("paths = [\"*.md\"]\n");
        for index in 0..held {
            fs::write(repo.main().join(format!("held-{index}.md")), "held\n").expect("a held path");
        }
        repo.counting_git();
        repo.icm("printf 'more\\n' >> \"$4/README.md\"\necho '{\"state\":\"OK\"}'");
        let program = repo.file("edit.ts", "// the fake icm ignores its program\n");
        let output = repo.run(&[
            "doc-commit",
            "-m",
            "docs: icm",
            "--icm",
            program.to_str().unwrap(),
        ]);
        assert!(output.status.success(), "held={held}: {}", said(&output));
        assert_eq!(
            repo.git_calls("ls-files"),
            usize::from(held > 0),
            "held={held}: one index read, counted at the git the hold ran"
        );
        assert_eq!(
            stdout(&repo.main(), &["show", "HEAD:README.md"]),
            "# readme\nmore",
            "held={held}"
        );
    }
}

/// Putting a refused edit back runs a constant number of git batches however many paths
/// it restores: one HEAD classification, two index batches for the held paths (their
/// current stages removed, their kept stages replayed), one restore of the paths HEAD
/// has and one index removal of the paths it lacks -- the same counts for 1, 8 and 64
/// paths of each kind, and no index work at all when none are held.
#[test]
fn putting_back_runs_a_constant_number_of_git_batches_however_many_paths_it_restores() {
    for held in [0usize, 1, 8, 64] {
        let repo = Repo::new("paths = [\"*.md\"]\n");
        let main = repo.main();
        for index in 0..held {
            fs::write(main.join(format!("held-{index}.md")), "held base\n").expect("held base");
            fs::write(main.join(format!("tracked-{index}.md")), "tracked\n").expect("tracked");
        }
        if held > 0 {
            git(&main, &["add", "-A"]);
            git(&main, &["commit", "-q", "-m", "held and tracked"]);
        }
        let before = repo.head();
        for index in 0..held {
            fs::write(main.join(format!("held-{index}.md")), "held edit\n").expect("held edit");
        }
        repo.counting_git();
        let mut script = String::from("root=$4\n");
        for index in 0..held {
            script.push_str(&format!(
                "printf 'clobbered\\n' > \"$root/held-{index}.md\"\n"
            ));
            script.push_str(&format!(
                "printf 'more\\n' >> \"$root/tracked-{index}.md\"\n"
            ));
            script.push_str(&format!("printf 'new\\n' > \"$root/new-{index}.md\"\n"));
        }
        script.push_str(
            "printf 'more\\n' >> \"$root/README.md\"\nprintf 'new\\n' > \"$root/created.md\"\nprintf 'x\\n' >> \"$root/src/lib.rs\"\necho '{\"state\":\"OK\"}'",
        );
        repo.icm(&script);
        let program = repo.file("edit.ts", "// the fake icm ignores its program\n");
        let output = repo.run(&[
            "doc-commit",
            "-m",
            "docs: icm",
            "--icm",
            program.to_str().unwrap(),
        ]);
        assert_eq!(
            output.status.code(),
            Some(4),
            "held={held}: {}",
            said(&output)
        );
        assert_eq!(repo.head(), before, "held={held}");
        assert_eq!(
            repo.git_calls("ls-files"),
            usize::from(held > 0),
            "held={held}"
        );
        assert_eq!(repo.git_calls("ls-tree"), 1, "held={held}");
        assert_eq!(
            repo.git_calls("update-index"),
            if held > 0 { 2 } else { 0 },
            "held={held}"
        );
        assert_eq!(repo.git_calls("restore"), 1, "held={held}");
        assert_eq!(repo.git_calls("rm"), 1, "held={held}");
        for index in 0..held {
            assert_eq!(
                fs::read_to_string(main.join(format!("held-{index}.md"))).unwrap(),
                "held edit\n",
                "held={held} index={index}"
            );
            assert_eq!(
                fs::read_to_string(main.join(format!("tracked-{index}.md"))).unwrap(),
                "tracked\n",
                "held={held} index={index}"
            );
            assert!(
                !main.join(format!("new-{index}.md")).exists(),
                "held={held} index={index}"
            );
        }
        assert_eq!(
            fs::read_to_string(main.join("README.md")).unwrap(),
            "# readme\n"
        );
        assert_eq!(
            fs::read_to_string(main.join("src/lib.rs")).unwrap(),
            "fn lib() {}\n"
        );
        assert!(!main.join("created.md").exists());
        assert_eq!(repo.status().lines().count(), held, "held={held}");
    }
}

/// A held path is put back with the very index stages it was kept with -- a merge
/// conflict's three stages byte for byte, its conflict-marked entry bytes, and another
/// held path's mode -- while the refused edit's tracked paths return to HEAD and its
/// created path goes.
#[test]
fn a_refused_icm_program_puts_back_held_conflict_stages_bytes_and_mode() {
    let repo = Repo::new("paths = [\"*.md\"]\n");
    let main = repo.main();
    fs::write(main.join("conflict.md"), "base\n").expect("write conflict.md");
    fs::write(main.join("mode.md"), "mode\n").expect("write mode.md");
    git(&main, &["add", "conflict.md", "mode.md"]);
    git(&main, &["commit", "-q", "-m", "conflict and mode"]);
    git(&main, &["checkout", "-q", "-b", "side"]);
    fs::write(main.join("conflict.md"), "side\n").expect("side of the conflict");
    git(&main, &["commit", "-q", "-am", "side"]);
    git(&main, &["checkout", "-q", "main"]);
    fs::write(main.join("conflict.md"), "main\n").expect("main of the conflict");
    git(&main, &["commit", "-q", "-am", "main side"]);
    git_fails(&main, &["merge", "-q", "side"]);
    let before = repo.head();
    let conflict_index = git_bytes(&main, &["ls-files", "-s", "-z", "--", "conflict.md"]);
    assert_eq!(
        conflict_index
            .split(|&byte| byte == 0)
            .filter(|record| !record.is_empty())
            .count(),
        3,
        "the conflict is held at three stages"
    );
    let conflict_entry = fs::read(main.join("conflict.md")).expect("the conflicted entry");
    fs::write(main.join("mode.md"), "held mode\n").expect("a held mode edit");
    fs::set_permissions(main.join("mode.md"), fs::Permissions::from_mode(0o755))
        .expect("a held mode");
    let mode_index = git_bytes(&main, &["ls-files", "-s", "-z", "--", "mode.md"]);
    repo.icm(
        "root=$4\nprintf 'clobbered\\n' > \"$root/conflict.md\"\nprintf 'clobbered\\n' > \"$root/mode.md\"\nchmod 644 \"$root/mode.md\"\nprintf 'more\\n' >> \"$root/README.md\"\nprintf 'new\\n' > \"$root/new.md\"\nprintf 'x\\n' >> \"$root/src/lib.rs\"\necho '{\"state\":\"OK\"}'",
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
    assert_eq!(
        git_bytes(&main, &["ls-files", "-s", "-z", "--", "conflict.md"]),
        conflict_index,
        "every conflict stage is back byte for byte"
    );
    assert_eq!(
        fs::read(main.join("conflict.md")).unwrap(),
        conflict_entry,
        "the conflicted entry is back byte for byte"
    );
    assert_eq!(
        git_bytes(&main, &["ls-files", "-s", "-z", "--", "mode.md"]),
        mode_index
    );
    assert_eq!(
        fs::read_to_string(main.join("mode.md")).unwrap(),
        "held mode\n"
    );
    assert_eq!(
        fs::metadata(main.join("mode.md"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755,
        "the held mode is back"
    );
    assert_eq!(
        fs::read_to_string(main.join("README.md")).unwrap(),
        "# readme\n"
    );
    assert_eq!(
        fs::read_to_string(main.join("src/lib.rs")).unwrap(),
        "fn lib() {}\n"
    );
    assert!(!main.join("new.md").exists());
    assert_eq!(repo.status(), "UU conflict.md\n M mode.md");
}

/// A patch's rename is judged on both its sides, and a check that then refuses puts both
/// back: the side HEAD has restored, the side it created removed.
#[test]
fn a_rename_whose_check_refuses_puts_back_both_sides() {
    let repo = Repo::new("paths = [\"*.md\"]\ncheck = [\"sh\", \"-c\", \"exit 1\", \"check\"]\n");
    let main = repo.main();
    fs::write(main.join("old.md"), "old\n").expect("write old.md");
    git(&main, &["add", "old.md"]);
    git(&main, &["commit", "-q", "-m", "old"]);
    let before = repo.head();
    let patch = repo.file(
        "rename.diff",
        "diff --git a/old.md b/renamed.md\nsimilarity index 100%\nrename from old.md\nrename to renamed.md\n",
    );
    let output = repo.run(&[
        "doc-commit",
        "-m",
        "docs: rename",
        "--patch",
        patch.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(4), "{}", said(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("declared doc check"),
        "{}",
        said(&output)
    );
    assert_eq!(repo.head(), before);
    assert_eq!(fs::read_to_string(main.join("old.md")).unwrap(), "old\n");
    assert!(!main.join("renamed.md").exists());
    assert_eq!(repo.status(), "");
}

/// A HEAD that moved under the hold means the edit landed before the stop: the doc
/// commit still refuses -- its own commit then has nothing left to commit -- but puts
/// nothing back, so the landed edit stays exactly as HEAD has it.
#[test]
fn a_head_that_moved_under_the_hold_is_not_put_back() {
    let repo = Repo::new(
        "paths = [\"*.md\"]\ncheck = [\"sh\", \"-c\", \"git add README.md && git commit -q -m checked\", \"check\"]\n",
    );
    let before = repo.head();
    repo.icm("printf 'more\\n' >> \"$4/README.md\"\necho '{\"state\":\"OK\"}'");
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
        said(&output).contains("nothing to put back"),
        "{}",
        said(&output)
    );
    assert_ne!(repo.head(), before);
    assert_eq!(
        stdout(&repo.main(), &["show", "HEAD:README.md"]),
        "# readme\nmore",
        "the landed edit stays committed"
    );
    assert_eq!(repo.status(), "");
}
