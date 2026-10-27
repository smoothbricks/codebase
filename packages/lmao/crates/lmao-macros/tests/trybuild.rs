//! Schema-DSL mistakes are pinned with trybuild. Those diagnostics are ours,
//! and the span is the test file, so the snapshot does not depend on how a
//! dependency was compiled.
//!
//! `provenance_required` is rustc's E0599, not a macro diagnostic. `span!`
//! (`lmao-macros/src/lib.rs` 499–550) is the only supported opener: it expands
//! to doc-hidden `__span` with compile-time attribution, the Rust equivalent of
//! `specs/lmao/01o_typescript_transformer.md` §1 Line Number Injection. Calling
//! `TraceContext::span` without that argument must keep failing E0599 at
//! `tests/compile_fail/provenance_required.rs:4:19`, with the similar-method
//! help naming `__span` in `context.rs`.
//!
//! trybuild rewrites an absolute workspace path to
//! `$WORKSPACE/.../context.rs` and keeps the expanded signature. cowshed's
//! sccache compiles that dependency under `--remap-path-prefix=<dir>=`, so
//! rustc emits `src/context.rs:line:col` and cannot reload the file to expand
//! the signature. trybuild never rewrites that relative path, so one snapshot
//! cannot match both. Normalize the help location and compare that.

use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::time::{Duration, SystemTime};

const SPAN_HELP: &str =
    "help: there is a method `__span` with a similar name, but with different arguments";

const PROVENANCE_CASE: &str = "tests/compile_fail/provenance_required.rs";

/// One test owns trybuild's project directory (`target/tests/trybuild/lmao-macros`), which every
/// trybuild run regenerates. nextest runs each test in its own process, so two trybuild tests in this
/// binary raced on it: one built the other's generated project, and the provenance probe "compiled".
/// The cases run first, when `t` drops, then the probe, one writer at a time.
#[test]
fn compile_fail_cases() {
    {
        let t = trybuild::TestCases::new();
        // Not `*.rs`: that glob also matches provenance_required.rs, whose
        // cross-crate help path is checked separately so both path renderings pass.
        t.compile_fail("tests/compile_fail/empty_enum.rs");
        t.compile_fail("tests/compile_fail/empty_schema.rs");
        t.compile_fail("tests/compile_fail/unknown_kind.rs");
        t.pass("tests/pass/*.rs");
    }
    compile_fail_provenance_required();
}

fn compile_fail_provenance_required() {
    let (preferred, probe_name) = preferred_compile_fail(PROVENANCE_CASE);
    let bound = bind_probe_callsite(&preferred, &probe_name)
        .unwrap_or_else(|err| panic!("{err}\n--- compiler output ---\n{preferred}"));
    let normalized = normalize_provenance_stderr(&bound)
        .unwrap_or_else(|err| panic!("{err}\n--- compiler output ---\n{bound}"));
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

#[test]
fn provenance_probe_callsite_stays_the_tracked_file() {
    let probe = "provenance_required_9_9.rs";
    let preferred = format!(
        "error[E0599]: no method named `span` found for reference `&TraceContext` in the current scope\n --> wip/provenance-probe-9-9/{probe}:4:19\n  |\n{CALLSITE_SOURCE}\n  |                   ^^^^\n  |\n{SPAN_HELP}\n --> src/context.rs:76:4\n"
    );
    let bound = bind_probe_callsite(&preferred, probe).unwrap();
    assert_eq!(
        normalize_provenance_stderr(&bound).unwrap(),
        include_str!("compile_fail/provenance_required.stderr")
    );
    assert!(bind_probe_callsite(&preferred.replace(":4:19", ":5:1"), probe).is_err());
    assert!(bind_probe_callsite(&preferred.replace(probe, "other.rs"), probe).is_err());
    assert!(bind_probe_callsite(&preferred.replace("trace.span", "trace.other"), probe).is_err());
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

const CALLSITE_SOURCE: &str =
    "4 |     let _ = trace.span(TextInput::Static(\"missing\"), None, 8, |_| Ok::<_, ()>(()));";

/// The probe is a copy, so rustc names that file. Require the same
/// callsite (line 4, column 19, the direct `span` invocation) and rewrite the
/// path to the tracked fixture before comparing.
fn bind_probe_callsite(stderr: &str, probe_name: &str) -> Result<String, String> {
    let mut lines: Vec<String> = stderr.lines().map(str::to_owned).collect();
    let arrow = lines
        .iter()
        .position(|line| line.trim_start().starts_with("--> "))
        .ok_or_else(|| "missing callsite arrow".to_owned())?;
    let loc = lines[arrow].trim_start().to_owned();
    let path = loc
        .strip_prefix("--> ")
        .ok_or_else(|| format!("callsite is not an arrow: {}", lines[arrow]))?
        .trim();
    if !path.ends_with(":4:19") {
        return Err(format!("callsite position is not 4:19: {path}"));
    }
    let file_name = Path::new(strip_rustc_position(path))
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    if file_name != probe_name {
        return Err(format!(
            "callsite file is {file_name}, not the probe {probe_name}"
        ));
    }
    if !lines.iter().any(|line| line == CALLSITE_SOURCE) {
        return Err("callsite source line is not the direct span invocation".to_owned());
    }
    let indent = lines[arrow].len() - loc.len();
    let mut rewritten = " ".repeat(indent);
    rewritten.push_str("--> tests/compile_fail/provenance_required.rs:4:19");
    lines[arrow] = rewritten;
    let mut out = lines.join("\n");
    out.push('\n');
    Ok(out)
}

/// Compile a unique copy that has no sibling `.stderr`. trybuild writes
/// its preferred variation to `wip/<that name>.stderr` and panics so a missing
/// snapshot is not a pass. The tracked snapshot is never opened for write.
fn preferred_compile_fail(case: &str) -> (String, String) {
    match capture_preferred(case) {
        Ok(captured) => captured,
        Err(err) => panic!("{err}"),
    }
}

fn capture_preferred(case: &str) -> Result<(String, String), String> {
    let cwd = std::env::current_dir().map_err(|err| format!("cwd: {err}"))?;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|err| format!("clock: {err}"))?
        .as_nanos();
    let probe_name = format!("provenance_required_{}_{nanos}.rs", std::process::id());
    // trybuild keeps rustc's line:column only for the input file it was handed,
    // matched as given relative to this crate. A probe anywhere else under the
    // workspace, as under a TMPDIR inside the checkout, becomes `$WORKSPACE/...`
    // with its position erased. trybuild's own `wip/` ignores itself.
    let probe_dir =
        Path::new("wip").join(format!("provenance-probe-{}-{nanos}", std::process::id()));
    let probe = probe_dir.join(&probe_name);
    let dir = cwd.join(&probe_dir);
    fs::create_dir_all(cwd.join("wip")).map_err(|err| format!("create wip: {err}"))?;
    fs::create_dir(&dir).map_err(|err| format!("create {}: {err}", dir.display()))?;
    let rs = dir.join(&probe_name);
    if let Err(err) = fs::copy(case, &rs) {
        let cleanup = remove_owned_dir(&dir);
        let mut msg = format!("copy {case} -> {}: {err}", rs.display());
        if let Err(cleanup) = cleanup {
            msg.push_str(&format!("; {cleanup}"));
        }
        return Err(msg);
    }
    if rs.with_extension("stderr").exists() {
        let cleanup = remove_owned_dir(&dir);
        let mut msg = format!("probe {} has an expected stderr", rs.display());
        if let Err(cleanup) = cleanup {
            msg.push_str(&format!("; {cleanup}"));
        }
        return Err(msg);
    }

    let wip_name = Path::new(&probe_name).with_extension("stderr");
    let wip = cwd.join("wip").join(&wip_name);
    if wip.exists() {
        let cleanup = remove_owned_dir(&dir);
        let mut msg = format!("refusing pre-existing wip {}", wip.display());
        if let Err(cleanup) = cleanup {
            msg.push_str(&format!("; {cleanup}"));
        }
        return Err(msg);
    }
    let started = SystemTime::now();
    let _panicked = panic::catch_unwind(AssertUnwindSafe(|| {
        let t = trybuild::TestCases::new();
        t.compile_fail(&probe);
    }));

    let read = read_fresh_wip(&wip, started);
    let cleanup_dir = remove_owned_dir(&dir);
    let cleanup_wip = remove_fresh_wip(&wip);
    let cleanup = match (cleanup_dir, cleanup_wip) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(dir_err), Ok(())) => Err(dir_err),
        (Ok(()), Err(wip_err)) => Err(wip_err),
        (Err(dir_err), Err(wip_err)) => Err(format!("{dir_err}; {wip_err}")),
    };
    match (read, cleanup) {
        (Ok(text), Ok(())) => Ok((text, probe_name)),
        (Ok(_), Err(err)) => Err(format!("diagnostic captured but cleanup failed: {err}")),
        (Err(err), Ok(())) => Err(err),
        (Err(err), Err(cleanup)) => Err(format!("{err}; cleanup also failed: {cleanup}")),
    }
}

fn read_fresh_wip(path: &Path, started: SystemTime) -> Result<String, String> {
    let meta = fs::metadata(path)
        .map_err(|err| format!("wip {} was not written by this run: {err}", path.display()))?;
    let modified = meta
        .modified()
        .map_err(|err| format!("mtime {}: {err}", path.display()))?;
    if modified + Duration::from_secs(2) < started {
        return Err(format!(
            "wip {} predates this run; refusing a stale file",
            path.display()
        ));
    }
    fs::read_to_string(path).map_err(|err| format!("read {}: {err}", path.display()))
}

fn remove_fresh_wip(path: &Path) -> Result<(), String> {
    remove_file_if_present(path)?;
    let Some(dir) = path.parent() else {
        return Ok(());
    };
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(format!("read {}: {err}", dir.display())),
    };
    let mut left = Vec::new();
    for entry in entries {
        left.push(entry.map_err(|err| format!("read {}: {err}", dir.display()))?);
    }
    if left.len() == 1 && left[0].file_name() == ".gitignore" {
        remove_file_if_present(&left[0].path())?;
        fs::remove_dir(dir).map_err(|err| format!("remove {}: {err}", dir.display()))?;
    }
    Ok(())
}

fn remove_owned_dir(dir: &Path) -> Result<(), String> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(dir).map_err(|err| format!("read {}: {err}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("read {}: {err}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|err| format!("stat {}: {err}", path.display()))?;
        if !file_type.is_file() {
            return Err(format!("refusing to remove non-file {}", path.display()));
        }
        fs::remove_file(&path).map_err(|err| format!("remove {}: {err}", path.display()))?;
    }
    fs::remove_dir(dir).map_err(|err| format!("remove {}: {err}", dir.display()))
}

fn remove_file_if_present(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("remove {}: {err}", path.display())),
    }
}
