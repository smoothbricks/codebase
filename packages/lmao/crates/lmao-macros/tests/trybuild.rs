//! Schema-DSL mistakes are pinned with trybuild. Those diagnostics are ours,
//! and the span is the test file, so the snapshot does not depend on how a
//! dependency was compiled.
//!
//! `provenance_required` is rustc's E0599, not a macro diagnostic. `span!` is
//! the only supported way to open a span: it expands to doc-hidden `__span`
//! with compile-time attribution (`lmao-macros/src/lib.rs`, the Rust equivalent
//! of `specs/lmao/01o_typescript_transformer.md` §1 Line Number Injection).
//! Calling `TraceContext::span` without that argument must keep failing E0599
//! at the call site, with the similar-method help naming `__span` in
//! `context.rs`.
//!
//! trybuild rewrites an absolute workspace path to
//! `$WORKSPACE/.../context.rs` and keeps the expanded signature. cowshed's
//! sccache compiles that dependency under `--remap-path-prefix=<dir>=`, so
//! rustc emits `src/context.rs:line:col` and cannot reload the file to expand
//! the signature. trybuild never rewrites that relative path, so one snapshot
//! cannot match both. Normalize the help location and compare that.

use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};

const SPAN_HELP: &str =
    "help: there is a method `__span` with a similar name, but with different arguments";

const PROVENANCE_CASE: &str = "tests/compile_fail/provenance_required.rs";

#[test]
fn compile_fail_cases() {
    let t = trybuild::TestCases::new();
    // Not `*.rs`: that glob also matches provenance_required.rs, whose
    // cross-crate help path is checked separately so both path renderings pass.
    t.compile_fail("tests/compile_fail/empty_enum.rs");
    t.compile_fail("tests/compile_fail/empty_schema.rs");
    t.compile_fail("tests/compile_fail/unknown_kind.rs");
    t.pass("tests/pass/*.rs");
}

#[test]
fn compile_fail_provenance_required() {
    let preferred = preferred_compile_fail(PROVENANCE_CASE);
    let normalized = normalize_provenance_stderr(&preferred)
        .unwrap_or_else(|err| panic!("{err}\n--- compiler output ---\n{preferred}"));
    let expected = include_str!("compile_fail/provenance_required.stderr");
    assert_eq!(
        normalized, expected,
        "normalized diagnostic diverged from the contract"
    );
}

#[test]
fn provenance_help_path_normalizes_across_remap() {
    let expected = include_str!("compile_fail/provenance_required.stderr");
    let primary = "\
error[E0599]: no method named `span` found for reference `&TraceContext` in the current scope
 --> tests/compile_fail/provenance_required.rs:4:19
  |
4 |     let _ = trace.span(TextInput::Static(\"missing\"), None, 8, |_| Ok::<_, ()>(()));
  |                   ^^^^
  |
";
    let sandbox = format!("{primary}{SPAN_HELP}\n --> src/context.rs:76:4\n");
    let host = format!(
        "{primary}{SPAN_HELP}\n --> $WORKSPACE/packages/lmao/crates/lmao-core/src/context.rs\n  |\n  | /     pub fn __span<T, E>(\n  | |         &self,\n... |\n  | |___________________________________^\n"
    );
    let absolute = format!(
        "{primary}{SPAN_HELP}\n --> /checkout/packages/lmao/crates/lmao-core/src/context.rs:76:4\n"
    );
    assert_eq!(normalize_provenance_stderr(&sandbox).unwrap(), expected);
    assert_eq!(normalize_provenance_stderr(&host).unwrap(), expected);
    assert_eq!(normalize_provenance_stderr(&absolute).unwrap(), expected);

    let wrong = sandbox.replace("src/context.rs:76:4", "src/other.rs:1:1");
    let err = normalize_provenance_stderr(&wrong).unwrap_err();
    assert!(err.contains("other.rs"), "{err}");
}

/// Collapse host (`$WORKSPACE/.../context.rs` plus the expanded signature) and
/// sandbox (`src/context.rs:line:col`, no signature) renderings of the same
/// `__span` help. A signature, when rustc could load the file, must still name
/// `__span`; a help that points somewhere else is a different refusal.
fn normalize_provenance_stderr(stderr: &str) -> Result<String, String> {
    let lines: Vec<&str> = stderr.lines().collect();
    let help_at = lines
        .iter()
        .position(|line| *line == SPAN_HELP)
        .ok_or_else(|| format!("missing `{SPAN_HELP}`"))?;
    let loc_at = lines[help_at + 1..]
        .iter()
        .position(|line| line.trim_start().starts_with("--> "))
        .map(|offset| help_at + 1 + offset)
        .ok_or_else(|| "missing location for the __span help".to_owned())?;

    let loc = lines[loc_at].trim_start();
    let path = loc
        .strip_prefix("--> ")
        .ok_or_else(|| format!("help location is not an arrow: {}", lines[loc_at]))?;
    let path = strip_rustc_position(path.trim());
    if !points_at_context_rs(path) {
        return Err(format!(
            "__span help points at {path}, not context.rs — the refusal target changed"
        ));
    }

    let shown: Vec<&str> = lines[loc_at + 1..]
        .iter()
        .copied()
        .filter(|line| !line.is_empty())
        .collect();
    if !shown.is_empty() {
        if !shown.iter().copied().all(is_span_display) {
            return Err(format!(
                "unexpected diagnostic after the __span help:\n{}",
                shown.join("\n")
            ));
        }
        if !shown.iter().any(|line| line.contains("__span")) {
            return Err(format!(
                "expanded help span does not name __span:\n{}",
                shown.join("\n")
            ));
        }
    }

    let mut out = lines[..=help_at].join("\n");
    let indent = lines[loc_at].len() - loc.len();
    out.push('\n');
    for _ in 0..indent {
        out.push(' ');
    }
    out.push_str("--> context.rs\n");
    Ok(out)
}

fn points_at_context_rs(path: &str) -> bool {
    let path = path.replace('\\', "/");
    path == "context.rs" || path.ends_with("/context.rs")
}

fn is_span_display(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with('|')
        || trimmed.starts_with('.')
        || trimmed.chars().next().is_some_and(|c| c.is_ascii_digit())
}

fn strip_rustc_position(path: &str) -> &str {
    let bytes = path.as_bytes();
    let mut end = bytes.len();
    for _ in 0..2 {
        let digits = bytes[..end]
            .iter()
            .rev()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digits == 0 || end == digits || bytes[end - digits - 1] != b':' {
            break;
        }
        end -= digits + 1;
    }
    &path[..end]
}

/// trybuild has no normalization hook. Hiding the sibling `.stderr` makes it
/// write its preferred variation to `wip/` (then panic, so a missing snapshot
/// is not a pass). That preferred text is what a blind `TRYBUILD=overwrite`
/// would commit, which is the environment-specific rendering we normalize.
fn preferred_compile_fail(case: &str) -> String {
    let case_path = PathBuf::from(case);
    let stderr_path = case_path.with_extension("stderr");
    // Same directory as the snapshot: cowshed's TMPDIR is another volume, and
    // rename across that boundary is EXDEV.
    let mut hold = stderr_path.clone().into_os_string();
    hold.push(".hold");
    let hold = PathBuf::from(hold);
    fs::rename(&stderr_path, &hold).unwrap_or_else(|err| {
        panic!(
            "hide {} so trybuild records a preferred variation: {err}",
            stderr_path.display()
        )
    });
    let _restore = RestoreFile {
        from: hold,
        to: stderr_path.clone(),
    };
    remove_wip_artifact(&case_path);
    let overwrite = std::env::var_os("TRYBUILD").is_some_and(|value| value == "overwrite");
    let panicked = panic::catch_unwind(AssertUnwindSafe(|| {
        let t = trybuild::TestCases::new();
        t.compile_fail(case);
    }));

    // `panic!` with format args on this nightly does not downcast as String
    // or &str. The wip file is the signal trybuild actually wrote a preferred
    // variation; a panic without that file is a real failure.
    if let Some(preferred) = read_wip_stderr(&case_path) {
        remove_wip_artifact(&case_path);
        return preferred;
    }
    match panicked {
        Ok(()) if overwrite => fs::read_to_string(&stderr_path).unwrap_or_else(|err| {
            panic!(
                "TRYBUILD=overwrite passed without writing {}: {err}",
                stderr_path.display()
            )
        }),
        Ok(()) => panic!("trybuild accepted {case} with no stderr snapshot"),
        Err(payload) => panic::resume_unwind(payload),
    }
}

fn read_wip_stderr(case: &Path) -> Option<String> {
    let name = case.with_extension("stderr");
    let file_name = name.file_name()?;
    let mut candidates = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("wip").join(file_name));
    }
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("wip")
            .join(file_name),
    );
    candidates
        .into_iter()
        .find_map(|path| fs::read_to_string(path).ok())
}

fn remove_wip_artifact(case: &Path) {
    let Some(file_name) = case.with_extension("stderr").file_name().map(PathBuf::from) else {
        return;
    };
    let mut dirs = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        dirs.push(cwd.join("wip"));
    }
    dirs.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("wip"));
    for dir in dirs {
        let _ = fs::remove_file(dir.join(&file_name));
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        let left: Vec<_> = entries.filter_map(Result::ok).collect();
        if left.len() == 1 && left[0].file_name() == ".gitignore" {
            let _ = fs::remove_file(left[0].path());
            let _ = fs::remove_dir(dir);
        }
    }
}

struct RestoreFile {
    from: PathBuf,
    to: PathBuf,
}

impl Drop for RestoreFile {
    fn drop(&mut self) {
        if self.from.exists() {
            let _ = fs::rename(&self.from, &self.to);
        }
    }
}
