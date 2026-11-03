//! Release provenance follows the checkout that owns a cloned warm Cargo target.
//! Governing contract: specs/cowshed/05_gateway.md, "Build records and downgrades";
//! specs/cowshed/16_build_volumes.md, "Hash inputs are the same in every checkout".

use cowshed_os::test_backstop::CommandBackstop as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-provenance-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            cowshed_os::eprintln!(
                "release provenance fixture cleanup {} failed: {error}",
                self.0.display()
            );
        }
    }
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
    let scratch = Scratch::new();
    let donor = scratch.0.join("donor");
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

    // Execute the actual owner script to inspect its Cargo directives, not Cargo's private file layout.
    let script = scratch.0.join("owner-build-script");
    output(
        Command::new("rustc")
            .current_dir(&donor)
            .arg("build.rs")
            .arg("-o")
            .arg(&script),
    );
    let metadata = output(
        Command::new(&script)
            .current_dir(&donor)
            .env("PROFILE", "release")
            .env("CARGO_MANIFEST_DIR", &donor),
    );

    let debug = output(
        Command::new(&script)
            .current_dir(&donor)
            .env("PROFILE", "debug")
            .env("CARGO_MANIFEST_DIR", &donor),
    );
    assert_eq!(
        debug, "cargo:rerun-if-changed=build.rs\n",
        "debug builds record no provenance"
    );
    let archive = scratch.0.join("archive");
    fs::create_dir(&archive).unwrap();
    let unversioned = output(
        Command::new(&script)
            .current_dir(&archive)
            .env("PROFILE", "release")
            .env("CARGO_MANIFEST_DIR", &archive),
    );
    assert_eq!(
        unversioned, "cargo:rerun-if-changed=build.rs\n",
        "an unversioned archive records no provenance"
    );
    let watched: Vec<_> = metadata
        .lines()
        .filter_map(|line| line.strip_prefix("cargo:rerun-if-changed="))
        .collect();
    assert_eq!(watched.len(), 3, "build.rs and both Git HEAD clocks");
    for path in watched {
        assert!(
            Path::new(path).is_relative(),
            "warm rerun metadata must be checkout-relative: {path}"
        );
        assert!(
            donor.join(path).try_exists().unwrap(),
            "the declared watch path exists: {path}"
        );
    }

    // Preserve Cargo's warm artifacts and file times, as a source-image clone does.
    let clone = scratch.0.join("clone");
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

/// A release build records the commit HEAD names, so a cached one may be replayed only for that
/// commit. Keyed on its sources alone, the release CLI built at one commit was replayed for every
/// later commit that left them unchanged, recording a commit the installed build already
/// contained, and setup refused it as a downgrade. Every platform CLI target therefore keys on
/// HEAD, read the way every runtime input is.
#[test]
fn every_release_cli_cache_key_names_the_commit_it_records() {
    // Resolved at run time, as the release-target test does: a cached test archive replayed into
    // another checkout must read that checkout's package.json.
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    let package: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(Path::new(&manifest).join("../../package.json"))
            .expect("cowshed package.json"),
    )
    .expect("package.json is JSON");
    let head = serde_json::json!({
        "runtime": "sh node_modules/@smoothbricks/nx-plugin/runtime-input.sh git rev-parse HEAD"
    });
    let targets = package["nx"]["targets"]
        .as_object()
        .expect("cowshed declares Nx targets");
    let releases = targets
        .iter()
        .filter(|(_, target)| {
            target["configurations"]["production"].is_object()
                && target.to_string().contains("cowshed-cli")
        })
        .collect::<Vec<_>>();
    assert!(
        releases.len() >= 4,
        "every platform's CLI target builds cowshed-cli in production: {releases:?}"
    );
    for (name, target) in releases {
        let inputs = target["inputs"]
            .as_array()
            .unwrap_or_else(|| panic!("{name} declares its inputs"));
        assert!(
            inputs.contains(&head),
            "{name}'s production build records HEAD, so its cache key must hold it: {inputs:?}"
        );
    }
}
