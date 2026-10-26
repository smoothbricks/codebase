//! Release provenance follows the checkout that owns a cloned warm Cargo target.
//! Governing contract: specs/cowshed/05_gateway.md, "Build records and downgrades";
//! specs/cowshed/16_build_volumes.md, "Hash inputs are the same in every checkout".

use cowshed_os::test_backstop::CommandBackstop as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
#[path = "../../cowshed-core/tests/support/temp_root.rs"]
mod temp_root;

/// The owner script itself, included so its directive decision is tested at the function
/// that makes it; its `main` and git shell are exercised by the warm-clone build below.
#[allow(dead_code)]
mod build_script {
    include!("../build.rs");
}

fn output(command: &mut Command) -> String {
    let result = command.output_bounded().expect("run the real fixture tool");
    assert!(
        result.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).expect("fixture tool emits UTF-8")
}

fn git(root: &Path, arguments: &[&str]) -> String {
    output(Command::new("git").current_dir(root).args(arguments))
}

fn cargo(root: &Path, arguments: &[&str]) {
    output(
        Command::new("cargo")
            .current_dir(root)
            .env_remove("CARGO_TARGET_DIR")
            .args(arguments),
    );
}

fn stamp(root: &Path) -> String {
    output(&mut Command::new(root.join("target/release/stamp-probe")))
}

#[test]
fn release_provenance_follows_a_warm_clone_not_its_donor() {
    let scratch = temp_root::TempRoot::new_in(&std::env::temp_dir(), "cowshed-build-provenance");
    let donor = scratch.join("donor");
    fs::create_dir_all(donor.join("src")).unwrap();
    fs::write(
        donor.join("Cargo.toml"),
        "[package]\nname = \"stamp-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .unwrap();
    let owner = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    fs::copy(owner.join("build.rs"), donor.join("build.rs")).unwrap();
    fs::write(donor.join(".gitignore"), "target/\n").unwrap();
    fs::write(
        donor.join("src/main.rs"),
        "fn main() { println!(\"{} {}\", env!(\"COWSHED_BUILD_COMMIT\"), env!(\"COWSHED_BUILD_COMMIT_TIME\")); }\n",
    )
    .unwrap();
    git(&donor, &["init", "-q", "-b", "main"]);
    git(&donor, &["config", "user.name", "Provenance fixture"]);
    git(&donor, &["config", "user.email", "fixture@example.invalid"]);
    cargo(&donor, &["generate-lockfile", "--offline"]);
    git(&donor, &["add", "."]);
    git(&donor, &["commit", "-q", "-m", "donor"]);
    cargo(&donor, &["build", "--release", "--offline", "--frozen"]);
    let first = format!(
        "{} {}\n",
        git(&donor, &["rev-parse", "HEAD"]).trim(),
        git(&donor, &["log", "-1", "--format=%ct", "HEAD"]).trim()
    );
    assert_eq!(stamp(&donor), first);

    // Preserve Cargo's warm artifacts and file times, as a source-image clone does.
    let clone = scratch.join("clone");
    output(
        Command::new("cp")
            .args(["-p", "-R"])
            .arg(&donor)
            .arg(&clone),
    );
    fs::write(
        clone.join("clone-owned"),
        "this commit belongs only to the clone\n",
    )
    .unwrap();
    git(&clone, &["add", "clone-owned"]);
    git(&clone, &["commit", "-q", "-m", "clone"]);
    let expected = format!(
        "{} {}\n",
        git(&clone, &["rev-parse", "HEAD"]).trim(),
        git(&clone, &["log", "-1", "--format=%ct", "HEAD"]).trim()
    );
    assert_ne!(expected, first);
    assert_eq!(
        stamp(&clone),
        first,
        "the clone starts with the donor's warm artifact"
    );
    cargo(&clone, &["build", "--release", "--offline", "--frozen"]);
    assert_eq!(
        stamp(&clone),
        expected,
        "release provenance must follow the clone's own HEAD"
    );
    assert_eq!(stamp(&donor), first, "the donor artifact is untouched");
}

#[test]
fn build_directives_name_the_watch_set_and_the_recorded_commit() {
    let directives = |release: bool,
                      git_directory: Option<&str>,
                      commit: Option<&str>,
                      commit_time: Option<&str>| {
        build_script::directives(release, git_directory, commit, commit_time).join("\n")
    };
    assert_eq!(
        directives(false, Some(".git"), Some("abc"), Some("123")),
        "cargo:rerun-if-changed=build.rs",
        "debug builds record no provenance"
    );
    assert_eq!(
        directives(true, None, None, None),
        "cargo:rerun-if-changed=build.rs",
        "an unversioned archive records no provenance"
    );
    assert_eq!(
        directives(true, Some(".git"), Some("abc"), Some("not-a-time")),
        "cargo:rerun-if-changed=build.rs\ncargo:rerun-if-changed=.git/HEAD\ncargo:rerun-if-changed=.git/logs/HEAD",
        "an unreadable commit time records no provenance, and the watch set is build.rs and both Git HEAD clocks"
    );
    assert_eq!(
        directives(true, Some(".git"), Some("abc"), Some("123")),
        "cargo:rerun-if-changed=build.rs\ncargo:rerun-if-changed=.git/HEAD\ncargo:rerun-if-changed=.git/logs/HEAD\ncargo:rustc-env=COWSHED_BUILD_COMMIT=abc\ncargo:rustc-env=COWSHED_BUILD_COMMIT_TIME=123",
        "a release build records the commit git named"
    );
}
