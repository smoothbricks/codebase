//! Records, in a release build, the commit it was made from (`src/build_record.rs`).
//!
//! A debug build records nothing: it is never installed (`refuse_unsupervisable_build`), and
//! leaving it out keeps every `cargo test` build from rerunning this script with each commit.
//! A release build made where git cannot answer — a source archive, a nix build — records
//! nothing either, and a build that records nothing is never taken for a newer one.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    let release = std::env::var("PROFILE").as_deref() == Ok("release");
    let (git_directory, commit, commit_time) = if release {
        let manifest_directory = PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"),
        );
        let git_directory = git(
            &manifest_directory,
            &["rev-parse", "--path-format=relative", "--git-dir"],
        );
        let commit = git(&manifest_directory, &["rev-parse", "HEAD"]);
        let commit_time = commit
            .as_ref()
            .and_then(|_| git(&manifest_directory, &["log", "-1", "--format=%ct", "HEAD"]));
        (git_directory, commit, commit_time)
    } else {
        (None, None, None)
    };
    for directive in directives(
        release,
        git_directory.as_deref(),
        commit.as_deref(),
        commit_time.as_deref(),
    ) {
        println!("{directive}");
    }
}

/// The Cargo directives for one build, from the profile and what git answered: the watch
/// set a warm clone is rerun by, and the commit a release build records. A debug build
/// watches only this script. A release build with a git directory also watches both Git
/// HEAD clocks — `logs/HEAD` grows with every commit, checkout and rebase, which the
/// symbolic `HEAD` file does not — and records its commit only when git named one with a
/// readable commit time.
pub(crate) fn directives(
    release: bool,
    git_directory: Option<&str>,
    commit: Option<&str>,
    commit_time: Option<&str>,
) -> Vec<String> {
    let mut directives = vec!["cargo:rerun-if-changed=build.rs".to_owned()];
    if !release {
        return directives;
    }
    if let Some(git_directory) = git_directory {
        directives.push(format!("cargo:rerun-if-changed={git_directory}/HEAD"));
        directives.push(format!("cargo:rerun-if-changed={git_directory}/logs/HEAD"));
    }
    if let (Some(commit), Some(commit_time)) = (
        commit,
        commit_time.filter(|time| time.parse::<u64>().is_ok()),
    ) {
        directives.push(format!("cargo:rustc-env=COWSHED_BUILD_COMMIT={commit}"));
        directives.push(format!(
            "cargo:rustc-env=COWSHED_BUILD_COMMIT_TIME={commit_time}"
        ));
    }
    directives
}

/// What `git <arguments>` prints in `directory`, trimmed; `None` when git is absent or refuses.
// WHY unbounded: the build script's git reads this checkout's own repository (its HEAD and the
// commit's time) and waits on no other process; the build that runs this script ends with it.
#[allow(clippy::disallowed_methods)]
fn git(directory: &PathBuf, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(directory)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}
