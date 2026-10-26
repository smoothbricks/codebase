//! Contract tests for `cowshed forget <owner/repo> --abandon` (06_cli.md, "Project retirement").
//!
//! Everything asserted here is behaviour a caller can observe: the grammar (a required explicit
//! `--abandon`, a validated repository identity, no `--project`), the verb needing no project at
//! all, so a project whose checkout is gone is retired the same way as any other, and the report
//! the verb prints. The retirement itself is core's and proved there: the backend in this file is
//! a fake, because nothing here may touch a host store, and no test runs a valid `forget` against
//! a host. The binary tests below run refusals only, which never get past the parser.

use async_trait::async_trait;
use cowshed_cli::args::{Command, ForgetArgs, ProjectDiscovery, parse_args};
use cowshed_cli::forget::{ForgetBackend, dispatch_forget, retired};
use cowshed_cli::help;
use cowshed_cli::output::Output;
use cowshed_core::api::JsonEnvelope;
use cowshed_core::project_retirement::ProjectRetirementReport;
use cowshed_core::repository::RepoId;
use cowshed_core::{CowshedError, ErrorCode, Result};
use cowshed_os::test_backstop::CommandBackstop as _;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::Command as Process;
use std::sync::Mutex;

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("fixture repo")
}

fn archive() -> PathBuf {
    PathBuf::from("/private/cowshed/store/.retired-projects/0198f2c0-5b7a-7000-8000-000000000001")
}

fn report(images: usize) -> ProjectRetirementReport {
    ProjectRetirementReport {
        repo_id: repo(),
        archive: archive(),
        images,
    }
}

fn output(quiet: bool) -> Output<Vec<u8>, Vec<u8>> {
    Output::new(Vec::new(), Vec::new(), quiet)
}

/// The retirement behind the dispatch: records what it was asked and answers once.
struct FakeBackend {
    calls: Mutex<Vec<String>>,
    answer: Mutex<Option<Result<ProjectRetirementReport>>>,
}

impl FakeBackend {
    fn answering(answer: Result<ProjectRetirementReport>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            answer: Mutex::new(Some(answer)),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }
}

#[async_trait]
impl ForgetBackend for FakeBackend {
    async fn forget_project(&self, repo_id: &RepoId) -> Result<ProjectRetirementReport> {
        self.calls.lock().expect("calls").push(repo_id.to_string());
        self.answer
            .lock()
            .expect("answer")
            .take()
            .expect("a test asks the backend once")
    }
}

/// A consumer that has gone away: every write fails.
struct Disconnected;

impl Write for Disconnected {
    fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "reader left"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn forget_args(argv: &[&str]) -> ForgetArgs {
    match parse_args(argv.iter().copied())
        .unwrap_or_else(|error| panic!("{argv:?} must parse: {error}"))
        .command
    {
        Command::Forget(args) => args,
        other => panic!("expected forget, got {other:?}"),
    }
}

#[test]
fn forget_parses_only_with_an_explicit_abandon() {
    for argv in [
        &["forget", "acme/widget", "--abandon"][..],
        &["forget", "--abandon", "acme/widget"],
        &["--json", "forget", "acme/widget", "--abandon"],
        &["forget", "acme/widget", "--abandon", "--quiet"],
    ] {
        assert_eq!(
            forget_args(argv),
            ForgetArgs { repo_id: repo() },
            "{argv:?}"
        );
    }

    let cli = parse_args(["--json", "-q", "forget", "acme/widget", "--abandon"]).unwrap();
    assert!(cli.global.json);
    assert!(cli.global.quiet);
    assert_eq!(cli.global.project, None);
}

#[test]
fn a_forget_without_abandon_is_refused_with_the_completed_command_line() {
    for argv in [
        &["forget", "acme/widget"][..],
        &["--json", "forget", "acme/widget"],
        &["forget", "acme/widget", "--quiet"],
    ] {
        let error = parse_args(argv.iter().copied()).unwrap_err();
        assert_eq!(error.exit_code(), 2, "{argv:?}");
        assert!(
            error.message.contains("--abandon") && error.message.contains("acme/widget"),
            "{argv:?}: {}",
            error.message
        );
        // The refusal is the confirmation prompt: it names the one command that confirms it.
        assert_eq!(
            error.hint, "cowshed forget acme/widget --abandon",
            "{argv:?}"
        );
        // A hinted command is one the parser accepts (06_cli.md rule 4), and accepting this one
        // is the confirmation.
        let hinted: Vec<&str> = error.hint.split_whitespace().skip(1).collect();
        assert_eq!(
            forget_args(&hinted),
            ForgetArgs { repo_id: repo() },
            "{argv:?}"
        );
    }
}

#[test]
fn a_missing_or_malformed_repository_is_refused_before_anything_else() {
    let spec = help::command_named("forget").expect("forget has a help page");
    assert_eq!(spec.hint(), "cowshed forget <owner/repo> --abandon");

    let missing = parse_args(["forget", "--abandon"]).unwrap_err();
    assert_eq!(missing.message, "forget requires an owner/repo");
    assert_eq!(missing.message, spec.missing);
    assert_eq!(missing.hint, spec.hint());
    assert_eq!(parse_args(["forget"]).unwrap_err().message, spec.missing);

    for bad in [
        "",
        "acme",
        "acme/",
        "/widget",
        "a/b/c",
        "../widget",
        "acme/..",
    ] {
        let error = parse_args(["forget", bad, "--abandon"]).unwrap_err();
        assert_eq!(error.exit_code(), 2, "{bad:?}");
        assert!(
            error.message.contains("invalid owner/repo"),
            "{bad:?}: {}",
            error.message
        );
        assert_eq!(error.hint, spec.hint(), "{bad:?}");
    }

    // The identity is validated first, so a hint that completes the command line is only ever
    // built from an identity known to be valid.
    let both = parse_args(["forget", "not-a-repo"]).unwrap_err();
    assert!(
        both.message.contains("invalid owner/repo"),
        "{}",
        both.message
    );
    assert_eq!(both.hint, spec.hint());

    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let opaque = OsString::from_vec(vec![b'a', 0xff, b'/', b'b']);
        let error = parse_args([
            OsString::from("forget"),
            opaque,
            OsString::from("--abandon"),
        ])
        .unwrap_err();
        assert_eq!(error.message, "forget requires a UTF-8 owner/repo");
    }
}

#[test]
fn forget_refuses_a_project_and_every_other_argument() {
    for argv in [
        &[
            "forget",
            "acme/widget",
            "--abandon",
            "--project",
            "/checkout",
        ][..],
        &[
            "--project",
            "/checkout",
            "forget",
            "acme/widget",
            "--abandon",
        ],
        &["forget", "--project", "/vanished/checkout", "acme/widget"],
    ] {
        let error = parse_args(argv.iter().copied()).unwrap_err();
        assert_eq!(error.exit_code(), 2, "{argv:?}");
        assert!(
            error.message.contains("--project"),
            "{argv:?}: {}",
            error.message
        );
    }

    let extra = parse_args(["forget", "acme/widget", "other/repo", "--abandon"]).unwrap_err();
    assert!(
        extra.message.contains("exactly one owner/repo") && extra.message.contains("other/repo"),
        "{}",
        extra.message
    );
    let unknown = parse_args(["forget", "acme/widget", "--abandon", "--force"]).unwrap_err();
    assert_eq!(unknown.message, "unknown flag `--force`");
    assert!(parse_args(["forget", "acme/widget", "--abandon", "--abandon"]).is_err());
}

/// The project a `forget` retires is the one whose checkout is gone, so the verb names no
/// project: discovery is never asked for one, and a checkout cannot be supplied to be consulted.
#[test]
fn forget_needs_no_project_so_an_unavailable_checkout_is_irrelevant() {
    let cli = parse_args(["forget", "acme/widget", "--abandon"]).expect("forget parses");
    assert_eq!(cli.command.project_discovery(), ProjectDiscovery::NotUsed);
    assert_eq!(cli.global.project, None);
}

#[test]
fn a_mistyped_forget_is_corrected_against_the_command_list() {
    let error = parse_args(["forgot", "acme/widget", "--abandon"]).unwrap_err();
    assert_eq!(error.exit_code(), 2);
    assert_eq!(
        error.message,
        "unknown command `forgot`; did you mean: forget"
    );
}

#[test]
fn forget_help_documents_the_authorization_the_refusals_and_the_preserved_tree() {
    let spec = help::command_named("forget").expect("forget has a help page");
    for argv in [
        &["help", "forget"][..],
        &["forget", "--help"],
        &["forget", "acme/widget", "--help"],
    ] {
        assert_eq!(
            parse_args(argv.iter().copied()).unwrap().command,
            Command::Help(Some(spec)),
            "{argv:?}"
        );
    }

    let page = spec.page().split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        page.starts_with("usage: cowshed forget <owner/repo> --abandon"),
        "{page}"
    );
    for fact in [
        "its checkout is gone",
        "no `--project`",
        "`--abandon` is required",
        "never infers it from a missing checkout",
        "a dead pid",
        "a path prefix",
        "nothing retires a project automatically",
        "`.retirements/<owner>/<repo>.json`",
        "fenced, drained, detached, archived",
        "without force",
        "`.retired-projects/<operation-id>`",
        "nothing is deleted or rewritten",
        "stdout is the archive path",
        "refuses that step with exit 4 and nothing is forced",
        "resumes from its recorded phase",
        "already exists is left intact",
        "never begins one",
    ] {
        assert!(page.contains(fact), "missing {fact}: {page}");
    }

    // The command map lists it, grammar and summary, with the explicit flag in the grammar.
    assert!(help::command_map().lines().any(|line| {
        line.trim_start()
            .starts_with("forget <owner/repo> --abandon")
            && line.contains(spec.summary)
    }));
}

#[tokio::test]
async fn dispatch_prints_the_archive_path_and_says_what_was_preserved() {
    let backend = FakeBackend::answering(Ok(report(2)));
    let mut out = output(false);

    let exit = dispatch_forget(&backend, &repo(), false, &mut out)
        .await
        .expect("dispatch");

    assert_eq!(exit.code, 0);
    assert_eq!(backend.calls(), ["acme/widget"]);
    let (stdout, stderr) = out.into_inner();
    assert_eq!(stdout, format!("{}\n", archive().display()).as_bytes());
    let stderr = String::from_utf8(stderr).expect("stderr");
    let announced = stderr
        .find("cowshed: retiring acme/widget on your explicit --abandon")
        .expect("what --abandon confirmed is said first");
    let reported = stderr
        .find("cowshed: retired acme/widget: 2 images detached")
        .expect("the repository and the detached images are reported");
    assert!(announced < reported, "{stderr}");
    assert!(
        stderr.contains(&format!(
            "its whole project tree is preserved at {}",
            archive().display()
        )),
        "{stderr}"
    );
    assert!(stderr.ends_with("next: cowshed ls --all\n"), "{stderr}");
    // The next hint names a verb the parser has.
    let hinted = stderr
        .lines()
        .last()
        .and_then(|line| line.strip_prefix("next: cowshed "))
        .expect("the last line is a next hint");
    let Command::List(list) = parse_args(hinted.split_whitespace())
        .expect("the hinted command parses")
        .command
    else {
        panic!("the hint is a listing");
    };
    assert!(list.all);
    // A retirement moves the tree whole: nothing in its output claims bytes were destroyed.
    for word in ["delet", "freed", "reclaim", "removed"] {
        assert!(!stderr.contains(word), "{word} in {stderr}");
    }
}

#[tokio::test]
async fn dispatch_counts_the_detached_images_in_the_right_number() {
    for (images, said) in [
        (0, "0 images detached"),
        (1, "1 image detached"),
        (3, "3 images detached"),
    ] {
        let backend = FakeBackend::answering(Ok(report(images)));
        let mut out = output(false);
        dispatch_forget(&backend, &repo(), false, &mut out)
            .await
            .expect("dispatch");
        let (_, stderr) = out.into_inner();
        let stderr = String::from_utf8(stderr).expect("stderr");
        assert!(stderr.contains(said), "{said} in {stderr}");
        assert!(retired(&report(images)).contains(said));
    }
}

#[tokio::test]
async fn dispatch_json_reports_the_typed_report_through_the_ordinary_envelope() {
    let backend = FakeBackend::answering(Ok(report(2)));
    let mut out = output(false);

    dispatch_forget(&backend, &repo(), true, &mut out)
        .await
        .expect("dispatch");

    let (stdout, stderr) = out.into_inner();
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let envelope: JsonEnvelope<ProjectRetirementReport> =
        serde_json::from_slice(&stdout).expect("the named report envelope");
    assert_eq!(envelope, JsonEnvelope::success(report(2)));
    let value: serde_json::Value = serde_json::from_slice(&stdout).expect("json");
    assert_eq!(value.as_object().expect("object").len(), 2);
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"]["repoId"], "acme/widget");
    assert_eq!(
        value["result"]["archive"],
        archive().to_str().expect("utf-8")
    );
    assert_eq!(value["result"]["images"], 2);
    assert_eq!(value["result"].as_object().expect("object").len(), 3);
    // The guidance is still said, on stderr.
    let stderr = String::from_utf8(stderr).expect("stderr");
    assert!(
        stderr.contains("retired acme/widget: 2 images detached"),
        "{stderr}"
    );
}

#[tokio::test]
async fn quiet_drops_the_guidance_but_keeps_the_answer_and_the_next_hint() {
    let backend = FakeBackend::answering(Ok(report(1)));
    let mut out = output(true);

    dispatch_forget(&backend, &repo(), false, &mut out)
        .await
        .expect("dispatch");

    let (stdout, stderr) = out.into_inner();
    assert_eq!(stdout, format!("{}\n", archive().display()).as_bytes());
    assert_eq!(stderr, b"next: cowshed ls --all\n");
}

#[tokio::test]
async fn a_refusal_from_core_reaches_the_caller_untouched() {
    let refusal = CowshedError::conflict(
        "workspace raven has not finished draining after 30 seconds; retirement remains fenced",
        "let its owned jobs finish, then retry: cowshed forget acme/widget --abandon",
    );
    let backend = FakeBackend::answering(Err(refusal.clone()));
    let mut out = output(false);

    let error = dispatch_forget(&backend, &repo(), false, &mut out)
        .await
        .unwrap_err();

    assert_eq!(error, refusal);
    assert_eq!(error.code, ErrorCode::Conflict);
    let (stdout, stderr) = out.into_inner();
    assert!(
        stdout.is_empty(),
        "no report for a retirement that did not finish"
    );
    let stderr = String::from_utf8(stderr).expect("stderr");
    assert!(stderr.contains("retiring acme/widget"), "{stderr}");
    assert!(!stderr.contains("retired acme/widget"), "{stderr}");
    assert!(
        !stderr.contains("next:"),
        "the caller prints the refusal's own hint: {stderr}"
    );
}

#[tokio::test]
async fn a_guidance_that_cannot_be_written_stops_before_any_retirement() {
    let backend = FakeBackend::answering(Ok(report(2)));
    let mut out = Output::new(Vec::new(), Disconnected, false);

    let error = dispatch_forget(&backend, &repo(), false, &mut out)
        .await
        .unwrap_err();

    assert!(
        error.message.contains("could not write forget's guidance"),
        "{}",
        error.message
    );
    assert!(backend.calls().is_empty(), "nothing was retired");
}

#[tokio::test]
async fn a_result_that_cannot_be_written_still_says_the_project_was_retired() {
    for json in [false, true] {
        let backend = FakeBackend::answering(Ok(report(2)));
        let mut out = Output::new(Disconnected, Vec::new(), false);

        let error = dispatch_forget(&backend, &repo(), json, &mut out)
            .await
            .unwrap_err();

        assert_eq!(backend.calls(), ["acme/widget"], "json={json}");
        assert!(
            error
                .message
                .starts_with("acme/widget was retired, but the result could not be written"),
            "json={json}: {}",
            error.message
        );
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    }
}

/// The `cowshed` binary under test, read when the test runs (see `output_contracts.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

#[test]
fn the_binary_refuses_a_forget_without_abandon_and_names_the_command_that_confirms_it() {
    let output = Process::new(cowshed())
        .args(["forget", "acme/widget"])
        .output_bounded()
        .expect("run cowshed forget");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("stderr");
    assert!(stderr.contains("--abandon"), "{stderr}");
    assert!(
        stderr.contains("next: cowshed forget acme/widget --abandon"),
        "{stderr}"
    );
}

#[test]
fn the_binary_refuses_a_malformed_repository_and_a_project_as_typed_usage_errors() {
    for argv in [
        &["--json", "forget", "not-a-repo", "--abandon"][..],
        &[
            "--json",
            "forget",
            "acme/widget",
            "--abandon",
            "--project",
            "/checkout",
        ],
    ] {
        let output = Process::new(cowshed())
            .args(argv)
            .output_bounded()
            .expect("run cowshed forget");

        assert_eq!(output.status.code(), Some(2), "{argv:?}");
        assert!(output.stderr.is_empty(), "{argv:?}");
        let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
        assert_eq!(envelope["ok"], false, "{argv:?}");
        assert_eq!(envelope["error"]["code"], "usage", "{argv:?}");
    }
}
