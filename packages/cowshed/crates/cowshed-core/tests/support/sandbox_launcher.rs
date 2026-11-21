//! The sandbox launcher of a test process on Linux (04_sandbox.md, "Linux enforcement", "The
//! launcher"; 08_testing.md, "Fixtures").
//!
//! A test binary links `cowshed-core` and is not the cowshed binary, so it launches nothing
//! sandboxed until it names the `cowshed-sandbox-exec` built with it, as any such program does.
//! A fixture that starts a sandboxed command calls [`name`] first. The launcher is the real one,
//! built from the sources under test: a missing one fails the test and says how to build it, and
//! one built from other sources refuses every launch by its build (exit 65).

use std::path::{Path, PathBuf};

/// `cowshed_core::sandbox::SANDBOX_EXEC_NAME`, spelled here because this file is compiled into
/// `cowshed-core`'s own unit tests and into its dependants' alike.
const NAME: &str = "cowshed-sandbox-exec";

/// Name this process's launcher through `name_launcher`, which is
/// `cowshed_core::sandbox::name_launcher` as the including crate spells it. Naming is idempotent,
/// so every fixture of a process may call this.
pub(crate) fn name<E: std::fmt::Display>(name_launcher: fn(&Path) -> Result<(), E>) {
    let executable = executable();
    if let Err(error) = name_launcher(&executable) {
        panic!(
            "cannot name the sandbox launcher {}: {error}",
            executable.display()
        );
    }
}

/// A native Nx lane has the declared artifact of `cowshed:sandbox-exec-bin`, which its test
/// targets depend on. A direct Cargo run uses the one Cargo wrote where its binaries land, a
/// directory above this test executable.
fn executable() -> PathBuf {
    let current = std::env::current_exe().expect("this test executable's path");
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);
    let lane = std::env::var_os("BOUNDED_EXEC_FIXTURE_DIR").is_some();
    match locate(&current, manifest.as_deref(), lane, |candidate| {
        candidate.is_file()
    }) {
        Ok(executable) => executable,
        Err(searched) => panic!(
            "no {NAME} to launch sandboxed commands with: looked for {}; build it with `nx run \
             cowshed:sandbox-exec-bin`, or for a direct Cargo run `cargo build -p cowshed-core \
             --bin {NAME}`",
            searched
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The launcher of the test executable `current`: in a lane, the Nx artifact of the package
/// `manifest` (a crate's directory, two below the package) belongs to; otherwise the nearest one
/// in a directory above `current`. `Err` carries everywhere it looked.
fn locate(
    current: &Path,
    manifest: Option<&Path>,
    lane: bool,
    is_file: impl Fn(&Path) -> bool,
) -> Result<PathBuf, Vec<PathBuf>> {
    let candidates: Vec<PathBuf> = match (lane, manifest) {
        (true, Some(manifest)) => vec![manifest.join("../../.cache/sandbox-exec").join(NAME)],
        _ => current
            .ancestors()
            .skip(1)
            .map(|directory| directory.join(NAME))
            .collect(),
    };
    match candidates.iter().find(|candidate| is_file(candidate)) {
        Some(found) => Ok(found.clone()),
        None => Err(candidates),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cargo writes a test executable below the directory its binaries land in, at a depth that
    /// depends on its layout, so the launcher is the nearest one above it. A lane uses the
    /// artifact its target declares and nothing a stale target directory holds.
    #[test]
    fn the_launcher_is_the_lanes_artifact_or_the_nearest_one_above_the_test_executable() {
        let built = Path::new("/w/target/debug/cowshed-sandbox-exec");
        let artifact =
            Path::new("/w/packages/cowshed/crates/cowshed-core/../../.cache/sandbox-exec")
                .join(NAME);
        let manifest = Path::new("/w/packages/cowshed/crates/cowshed-core");
        for (current, lane, present, expected) in [
            (
                "/w/target/debug/build/cowshed-core/0123/out/cowshed_core-0123",
                false,
                vec![built],
                Some(built),
            ),
            (
                "/w/target/debug/deps/cowshed_core-0123",
                false,
                vec![built],
                Some(built),
            ),
            (
                "/w/target/debug/deps/cowshed_core-0123",
                false,
                vec![built, Path::new("/w/cowshed-sandbox-exec")],
                Some(built),
            ),
            (
                "/w/target/debug/deps/cowshed_core-0123",
                true,
                vec![built, artifact.as_path()],
                Some(artifact.as_path()),
            ),
            (
                "/w/target/debug/deps/cowshed_core-0123",
                true,
                vec![built],
                None,
            ),
            (
                "/w/target/debug/deps/cowshed_core-0123",
                false,
                vec![],
                None,
            ),
        ] {
            let found = locate(Path::new(current), Some(manifest), lane, |candidate| {
                present.contains(&candidate)
            });
            assert_eq!(found.as_deref().ok(), expected, "{current} lane={lane}");
            if let Err(searched) = found {
                assert!(!searched.is_empty(), "a refusal names where it looked");
            }
        }
        assert_eq!(
            locate(
                Path::new("/w/target/debug/deps/cowshed_core-0123"),
                None,
                true,
                |candidate| candidate == built
            ),
            Ok(built.to_owned()),
            "a lane that names no manifest is a direct run"
        );
    }
}
