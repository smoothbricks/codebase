//! Records, in a release build, the commit it was made from (`src/build_record.rs`).
//!
//! A debug build records nothing: it is never installed (`refuse_unsupervisable_build`), and
//! leaving it out keeps every `cargo test` build from rerunning this script with each commit.
//! A release build made where git cannot answer — a source archive, a nix build — records
//! nothing either, and a build that records nothing is never taken for a newer one.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("PROFILE").as_deref() != Ok("release") {
        return;
    }
    let manifest_directory = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"),
    );
    // `logs/HEAD` grows with every commit, checkout and rebase, which the symbolic `HEAD` file
    // does not: a release build is remade when the commit it records changes.
    if let Some(git_directory) = git(&manifest_directory, &["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_directory}/HEAD");
        println!("cargo:rerun-if-changed={git_directory}/logs/HEAD");
    }
    let Some(commit) = git(&manifest_directory, &["rev-parse", "HEAD"]) else {
        return;
    };
    let Some(commit_time) = git(&manifest_directory, &["log", "-1", "--format=%ct", "HEAD"])
        .filter(|time| time.parse::<u64>().is_ok())
    else {
        return;
    };
    println!("cargo:rustc-env=COWSHED_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=COWSHED_BUILD_COMMIT_TIME={commit_time}");
}

/// What `git <arguments>` prints in `directory`, trimmed; `None` when git is absent or refuses.
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
