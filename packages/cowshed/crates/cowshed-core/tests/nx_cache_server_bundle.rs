//! The nx-cache-server ships as one bundle that Bun runs from PATH (the owner's ruling, 2026-10-18),
//! and the manifest guard reads the repository at runtime rather than at compile time.

use cowshed_os::test_backstop::CommandBackstop as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};

#[path = "support/temp_parent.rs"]
mod temp_parent;
#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

/// A fixture workspace under the scratch parent. Its child is owned by this guard from the moment
/// it is created, so a failed check below removes it as the panic unwinds.
fn fixture() -> TempRoot {
    TempRoot::new_in(&temp_parent::scratch_parent(), "cowshed-nx-bundle")
}

fn repository_root() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo or nextest supplies CARGO_MANIFEST_DIR");
    std::fs::canonicalize(Path::new(&manifest).join("../../../.."))
        .expect("repository root from the crate manifest")
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

const COWSHED_NX_BUNDLE_ROOT_PROBE: &str = "COWSHED_NX_BUNDLE_ROOT_PROBE";

/// The repository root is resolved from the manifest directory at runtime, never baked in at
/// compile time: a child test process whose `CARGO_MANIFEST_DIR` names another fixture resolves
/// that fixture. A private file carries the exact native path bytes independently of libtest's
/// stdout formatting. The child gets its environment on its own `Command`, so no test mutates
/// the shared environment that parallel tests read.
#[test]
fn repository_root_resolves_the_manifest_at_runtime_not_at_compile_time() {
    if let Some(probe) = std::env::var_os(COWSHED_NX_BUNDLE_ROOT_PROBE) {
        std::fs::write(probe, repository_root().as_os_str().as_encoded_bytes())
            .expect("write the resolved root probe");
        return;
    }
    let fixture = fixture();
    let manifest = fixture.join("packages/cowshed/crates/cowshed-core");
    std::fs::create_dir_all(&manifest).expect("fixture manifest directory");
    let expected = std::fs::canonicalize(&*fixture).expect("canonical fixture root");
    let probe = fixture.join("resolved-root");
    let output = Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            "--exact",
            "repository_root_resolves_the_manifest_at_runtime_not_at_compile_time",
            "--nocapture",
        ])
        .env("CARGO_MANIFEST_DIR", &manifest)
        .env(COWSHED_NX_BUNDLE_ROOT_PROBE, &probe)
        .output_bounded()
        .expect("run the root probe child");
    assert!(output.status.success(), "{}", text(&output));
    let resolved = std::fs::read(&probe)
        .unwrap_or_else(|error| panic!("read root probe {}: {error}", probe.display()));
    assert_eq!(resolved.as_slice(), expected.as_os_str().as_encoded_bytes());
}

fn read_manifest(path: &Path) -> Value {
    let bytes =
        std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// The server ships as one platform-independent bundle that Bun runs from `PATH` (the owner's
/// ruling, 2026-10-18): the nx-cache-server package publishes `dist/server` and no `dist/bin`.
/// That no cowshed CLI build depends on, hashes, or copies an nx-cache-server artifact is
/// proven where the `cli-*` targets are now generated from `napi.targets`, in the Nx plugin's
/// inference tests; the targets no longer exist in this manifest to inventory here.
#[test]
fn the_server_ships_its_bundle_and_no_platform_binaries() {
    let root = repository_root();
    let server = read_manifest(&root.join("packages/nx-cache-server/package.json"));
    let files = server["files"]
        .as_array()
        .expect("nx-cache-server declares its published files");
    assert!(files.contains(&json!("dist/server")), "{files:?}");
    assert!(!files.contains(&json!("dist/bin")), "{files:?}");
    assert_eq!(
        server["nx"]["targets"]["bundle"]["outputs"],
        json!(["{projectRoot}/dist/server/nx-cache-server.js"])
    );
}
