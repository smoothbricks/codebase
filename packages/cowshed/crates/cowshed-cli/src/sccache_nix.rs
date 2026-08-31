//! The patched sccache, built from cowshed's own flake and pinned by a nix GC root.
//!
//! `nix/sccache` beside this package is a standalone flake: a pinned nixpkgs plus the two patches
//! that let one cache serve workspaces at different mount paths and stop N clones compiling one
//! crate. `cowshed setup --sccache` builds it, and launchd runs the result *straight out of the
//! store* — nothing is copied to a host-stable path.
//!
//! Two facts hold this together, and both are load-bearing:
//!
//! 1. **The out-link is the GC root.** `nix build --out-link <path>` registers `<path>` as an
//!    indirect root under `/nix/var/nix/gcroots/auto`. Without it the freshly built store path is
//!    garbage the next `nix store gc` may delete — and it would delete it out from under a loaded
//!    LaunchAgent, leaving launchd exec'ing a path that no longer exists. The root is what turns
//!    the plist's program path into a durable promise.
//! 2. **The plist names the resolved store path, not the out-link.** A store path carries its own
//!    content hash, so a different build is a different plist, and rewriting a plist is what boots
//!    the old server out. Naming the symlink instead would let a later build repoint it and leave
//!    launchd starting a different binary at the next restart, under a definition nobody changed.
//!
//! What that buys is one supervised server, and the thing being contended is the SOCKET, not a
//! version string. A unix bind unlinks the path first (sccache 0.17.0, server.rs:510-514), so any
//! client that auto-starts a server takes the LaunchAgent's socket over from it;
//! `connect_or_start_server` (commands.rs:310-348) does that on ConnectionRefused, TimedOut, or
//! NotFound. It never does it because of a version disagreement — `ServerInfo.version`
//! (server.rs:2147, 2203-2206) is only reported, a mismatch appears merely as a hint on a bincode
//! failure in `request_stats`, and Shutdown is reachable only from the explicit StopServer verb.
//! The `-cowshed` version suffix is therefore provenance and nothing more: it tells an operator
//! which build is answering. The property that keeps clients off each other's servers is that
//! exactly one server is supervised at a path they all reach.
//!
//! sccache is opt-in because not every cowshed user writes Rust: a default `setup` neither builds
//! this flake nor requires nix. A host that asks for it and has no nix gets a named prerequisite,
//! not a crash and not a silent success.

use crate::gateway_service::launchd_error;
use crate::launchd::{
    APPLICATION_SUPPORT, LIBRARY_DIRECTORY, SCCACHE_BINARY_NAME, STABLE_BINARY_DIRECTORY,
    STABLE_SUPPORT_DIRECTORY, StoreBackedProgram,
};
use cowshed_core::api::{Finding, FindingSeverity};
use cowshed_core::{CowshedError, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The flake directory, relative to the package root that ships it.
const FLAKE_DIRECTORY: [&str; 2] = ["nix", "sccache"];

/// The file that identifies [`FLAKE_DIRECTORY`] as the real thing rather than an empty namesake.
const FLAKE_MANIFEST: &str = "flake.nix";

/// The flake output `setup` installs. Named rather than defaulted so the error a typo produces
/// names the attribute nix could not find.
const FLAKE_ATTRIBUTE: &str = "sccache";

/// Where the GC root lives: `~/Library/Application Support/dev.cowshed/nix/sccache`.
///
/// Under the same support directory as the plists and the host-stable binaries. Whatever launchd
/// can read an agent definition from, `nix store gc` can read a root from — and keeping cowshed's
/// only nix root in cowshed's own directory is what makes `setup --uninstall` able to release it.
const GC_ROOT_DIRECTORY: &str = "nix";

/// The `nix` this host would run. Resolved through `PATH` on purpose: nix's own installer puts
/// `/nix/var/nix/profiles/default/bin` on it, and a host that has nix elsewhere is a host whose
/// `PATH` is the only authority on where.
const NIX_EXECUTABLE: &str = "nix";

/// The doctor/setup finding code for a host that asked for sccache and cannot build it.
const NIX_MISSING_CODE: &str = "sccache-nix-missing";

/// The doctor/setup finding code for a build that ran and failed.
const BUILD_FAILED_CODE: &str = "sccache-build-failed";

/// The doctor/setup finding code for a host the flake cannot build for at all.
const UNSUPPORTED_SYSTEM_CODE: &str = "sccache-unsupported-system";

/// The systems `nix/sccache/flake.nix` declares.
///
/// Checked here, before nix is invoked, so an Intel mac gets one cowshed sentence instead of a
/// nixpkgs evaluation trace. `x86_64-darwin` is absent from both this list and the flake because
/// nixpkgs 26.11 dropped it; the flake comment carries the release note.
const SUPPORTED_SYSTEMS: [&str; 3] = ["aarch64-darwin", "aarch64-linux", "x86_64-linux"];

/// This host's nix system double.
///
/// Derived from the compile target rather than from `nix eval`: the binary asking the question was
/// built for exactly one system, and `uname` would answer for the kernel rather than for the
/// package set. An architecture or OS cowshed itself does not build for cannot reach here.
fn host_system() -> &'static str {
    match (std::env::consts::ARCH, std::env::consts::OS) {
        ("aarch64", "macos") => "aarch64-darwin",
        ("x86_64", "macos") => "x86_64-darwin",
        ("aarch64", "linux") => "aarch64-linux",
        ("x86_64", "linux") => "x86_64-linux",
        (arch, os) => {
            // cowshed builds for exactly four targets (package.json `napi.targets`), so this is
            // unreachable from any shipped binary. Said out loud rather than guessed at: inventing
            // a system double would hand nix an attribute path that does not exist.
            debug_assert!(false, "unmapped host system {arch}-{os}");
            "unknown"
        }
    }
}

/// The doctor finding code for an installed agent whose program is no longer pinned or no longer
/// there.
const UNPINNED_CODE: &str = "sccache-unpinned";

pub fn gc_root(home: &Path) -> PathBuf {
    home.join(LIBRARY_DIRECTORY)
        .join(APPLICATION_SUPPORT)
        .join(STABLE_SUPPORT_DIRECTORY)
        .join(GC_ROOT_DIRECTORY)
        .join(SCCACHE_BINARY_NAME)
}

/// What building and pinning the flake did, as a value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildOutcome {
    /// Built and rooted. The program is inside the store path the root pins.
    Installed(StoreBackedProgram),
    /// Nothing was installed, and this is why.
    Refused(BuildRefusal),
}

/// Why a host that asked for sccache did not get it.
///
/// A missing `nix` is not an error: `setup`'s subject is host storage, and the caller owes the
/// reader a named prerequisite rather than an aborted transaction. A build that *ran* and failed is
/// also a value, carrying nix's own stderr — the only thing that can explain a nix failure. Both
/// are separate from [`BuildOutcome::Installed`] so a refusal cannot carry a program and a success
/// cannot carry a reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildRefusal {
    /// The flake declares no package for this host's system, so there is nothing to build. Named
    /// before nix runs, or the operator reads a nixpkgs release note instead of a cowshed sentence.
    UnsupportedSystem { system: &'static str },
    /// `nix` is not on `PATH`.
    NixMissing,
    /// `nix build` ran and failed. `stderr` is nix's, verbatim and untruncated.
    Failed { status: Option<i32>, stderr: String },
}

impl BuildRefusal {
    /// The prose `setup` prints. Both arms name the flake, because the point of the line is that a
    /// reader can go run the build by hand. No default branch.
    pub fn phrase(&self, flake: &Path) -> String {
        match self {
            Self::UnsupportedSystem { system } => format!(
                "sccache: {} builds for {}, not {system}; no sccache was installed",
                flake.display(),
                SUPPORTED_SYSTEMS.join(", ")
            ),
            Self::NixMissing => format!(
                "sccache: nix is not on PATH, so {} could not be built; no sccache was installed",
                flake.display()
            ),
            Self::Failed { status, stderr } => format!(
                "sccache: nix build of {} failed ({}); no sccache was installed\n{}",
                flake.display(),
                exit_phrase(*status),
                stderr.trim_end()
            ),
        }
    }

    /// The same fact as a finding, so `doctor` reports the prerequisite the operator is missing
    /// until they install it. `Error` severity on both arms: the host does not have the sccache it
    /// was told to install, and calling that a warning would be the comfortable answer.
    pub fn finding(&self, flake: &Path) -> Finding {
        match self {
            Self::UnsupportedSystem { system } => Finding {
                code: UNSUPPORTED_SYSTEM_CODE.into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "sccache was requested but {} declares no package for {system} (it builds for {})",
                    flake.display(),
                    SUPPORTED_SYSTEMS.join(", ")
                ),
                hint: "install sccache by hand on this system; cowshed's flake cannot build for it"
                    .into(),
                path: Some(flake.to_path_buf()),
            },
            Self::NixMissing => Finding {
                code: NIX_MISSING_CODE.into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "sccache was requested but nix is not on PATH, so {} cannot be built",
                    flake.display()
                ),
                hint: "install nix (https://nixos.org/download) and rerun cowshed setup --sccache"
                    .into(),
                path: Some(flake.to_path_buf()),
            },
            Self::Failed { status, .. } => Finding {
                code: BUILD_FAILED_CODE.into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "nix build of {} failed ({})",
                    flake.display(),
                    exit_phrase(*status)
                ),
                hint: format!(
                    "run nix build {}#{FLAKE_ATTRIBUTE} to see the failure in full",
                    flake.display()
                ),
                path: Some(flake.to_path_buf()),
            },
        }
    }
}

/// A process outcome in words. `None` is a signal, which has no exit code at all — reporting it as
/// "exit 0" or "exit -1" would be an invented number.
fn exit_phrase(status: Option<i32>) -> String {
    match status {
        Some(code) => format!("exit {code}"),
        None => "killed by a signal".to_owned(),
    }
}

/// Build the flake and register its GC root, then name the program launchd will run.
///
/// `--out-link <gc-root>` is not a convenience: the out-link *is* the root (see the module
/// header). `--no-link` here would return a store path nothing holds, and the first `nix store gc`
/// on the host would delete the binary the LaunchAgent is configured to exec.
///
/// The program is read back through the link rather than parsed out of nix's stdout, so what the
/// plist names and what the root pins are the same lookup — [`StoreBackedProgram`] then derives the
/// program from the rooted store path, which is what makes them provably the same artifact.
pub fn build(home: &Path, flake: &Path) -> Result<BuildOutcome> {
    // Before the directory is created and before nix is spawned: a host the flake cannot build for
    // gets one sentence, not a nixpkgs evaluation trace and not an empty GC root directory.
    let system = host_system();
    if !SUPPORTED_SYSTEMS.contains(&system) {
        return Ok(BuildOutcome::Refused(BuildRefusal::UnsupportedSystem {
            system,
        }));
    }
    let root = gc_root(home);
    let parent = root
        .parent()
        .expect("the gc root is always derived with a parent");
    std::fs::create_dir_all(parent).map_err(|error| {
        CowshedError::internal(format!("could not create {}: {error}", parent.display()))
    })?;

    let output = match Command::new(NIX_EXECUTABLE)
        .arg("build")
        .arg(format!("{}#{FLAKE_ATTRIBUTE}", flake.display()))
        .arg("--out-link")
        .arg(&root)
        .output()
    {
        Ok(output) => output,
        // Only "there is no such program" is a missing prerequisite. Anything else — a permission
        // denial, a broken PATH entry — is a real failure and keeps its own words.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BuildOutcome::Refused(BuildRefusal::NixMissing));
        }
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not run {NIX_EXECUTABLE} build: {error}"
            )));
        }
    };
    if !output.status.success() {
        return Ok(BuildOutcome::Refused(BuildRefusal::Failed {
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }));
    }
    Ok(BuildOutcome::Installed(rooted_program(home, &root)?))
}

/// The program the GC root pins, or why the root does not pin one.
///
/// Read from the link every time rather than remembered: the root is the host's only record of
/// which build is installed, and a record that disagreed with the link would be the stale pointer
/// this whole design exists to make impossible.
pub fn rooted_program(home: &Path, root: &Path) -> Result<StoreBackedProgram> {
    let target = std::fs::read_link(root).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "could not read the sccache nix GC root {}: {error}",
                root.display()
            ),
            "cowshed setup --sccache",
        )
    })?;
    StoreBackedProgram::new(home, root, &target, SCCACHE_BINARY_NAME).map_err(launchd_error)
}

/// The flake directory this build ships.
///
/// Found by walking up from the running executable rather than from the working directory: the
/// flake travels inside the npm package, and `setup` is run from wherever the operator happens to
/// stand. Both shapes the package takes are reached by the same walk — `dist/bin/<platform>/cowshed`
/// in a published package and `target/<profile>/cowshed` in a checkout are both under the package
/// root that carries `nix/sccache`.
pub fn flake_directory() -> Result<PathBuf> {
    let executable = std::env::current_exe().map_err(|error| {
        CowshedError::environment_missing(
            format!("could not identify the cowshed executable: {error}"),
            "reinstall cowshed",
        )
    })?;
    let executable = std::fs::canonicalize(&executable).map_err(|error| {
        CowshedError::environment_missing(
            format!("could not resolve {}: {error}", executable.display()),
            "reinstall cowshed",
        )
    })?;
    flake_directory_from(&executable)
}

/// `search_from` is passed rather than read so the walk is testable, and so the error can name
/// every directory that was actually looked at instead of a guess about where the package is.
pub fn flake_directory_from(search_from: &Path) -> Result<PathBuf> {
    let mut searched = Vec::new();
    for ancestor in search_from.ancestors().skip(1) {
        let candidate = FLAKE_DIRECTORY
            .iter()
            .fold(ancestor.to_path_buf(), |path, part| path.join(part));
        if candidate.join(FLAKE_MANIFEST).is_file() {
            return Ok(candidate);
        }
        searched.push(candidate);
    }
    Err(CowshedError::environment_missing(
        format!(
            "could not find the sccache flake ({}/{FLAKE_MANIFEST}) above {}; looked in {}",
            FLAKE_DIRECTORY.join("/"),
            search_from.display(),
            searched
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "reinstall cowshed; the npm package ships nix/sccache and this install is missing it",
    ))
}

/// Whether the installed sccache agent still has a program to run, and a root keeping it.
///
/// Deleting the out-link and running `nix store gc` silently removes the binary launchd is
/// configured to exec: the agent keeps its plist, keeps its label, and fails only at the next
/// restart, which may be a reboot away. That is a state a host has to be told about while it is
/// still cheap to fix, so `doctor` asks the question every run.
///
/// Nothing is reported when no agent is installed. sccache is opt-in, and a host that never asked
/// for it is not a host with a problem.
pub fn pinning_findings(home: &Path, plist: &Path) -> Vec<Finding> {
    if !plist.is_file() {
        return Vec::new();
    }
    let root = gc_root(home);
    let unpinned = |message: String, hint: &str| {
        vec![Finding {
            code: UNPINNED_CODE.into(),
            severity: FindingSeverity::Error,
            message,
            hint: hint.to_owned(),
            path: Some(root.clone()),
        }]
    };
    let target = match std::fs::read_link(&root) {
        Ok(target) => target,
        // No root at all: either nothing built it, or someone deleted the link. Both leave the
        // store path this agent runs eligible for collection.
        Err(error) => {
            return unpinned(
                format!(
                    "the {} agent is installed but cowshed holds no nix GC root at {} ({error}); nix store gc may delete the binary launchd runs",
                    crate::launchd::SCCACHE_LABEL,
                    root.display()
                ),
                "cowshed setup --sccache",
            );
        }
    };
    let program = target
        .join(STABLE_BINARY_DIRECTORY)
        .join(SCCACHE_BINARY_NAME);
    if !program.is_file() {
        return unpinned(
            format!(
                "the sccache nix GC root {} names {}, which is not there; the store path was collected",
                root.display(),
                program.display()
            ),
            "cowshed setup --sccache",
        );
    }
    // The root pins a real program — but the question is whether it pins *this agent's* program.
    // The plist is the definition launchd loaded, so the program it names is read from the file
    // rather than assumed to be whatever the root currently points at.
    match std::fs::read(plist) {
        Ok(bytes) => {
            let text = String::from_utf8_lossy(&bytes);
            let named = program.to_string_lossy();
            if text.contains(named.as_ref()) {
                Vec::new()
            } else {
                unpinned(
                    format!(
                        "the {} plist does not name {}, the program the nix GC root pins; launchd is running some other build",
                        plist.display(),
                        program.display()
                    ),
                    "cowshed setup --sccache",
                )
            }
        }
        Err(error) => unpinned(
            format!(
                "could not read {} to check which sccache it runs: {error}",
                plist.display()
            ),
            "cowshed doctor --json",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-sccache-nix-{}-{name}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("scratch root");
        root
    }

    /// The two layouts the package actually ships in reach the same flake, and nothing else does.
    #[test]
    fn the_flake_is_found_from_both_shipped_binary_layouts() {
        let package = scratch("layouts");
        let flake = package.join("nix").join("sccache");
        std::fs::create_dir_all(&flake).expect("flake directory");
        std::fs::write(flake.join(FLAKE_MANIFEST), b"{}").expect("flake.nix");

        for relative in [
            ["dist", "bin", "darwin-arm64", "cowshed"].as_slice(),
            ["target", "release", "cowshed"].as_slice(),
        ] {
            let executable = relative
                .iter()
                .fold(package.clone(), |path, part| path.join(part));
            assert_eq!(
                flake_directory_from(&executable).expect("flake found"),
                flake,
                "{} should reach the shipped flake",
                executable.display()
            );
        }

        // An empty `nix/sccache` is not the flake: the manifest is what identifies it, so a
        // half-copied package fails loudly instead of handing nix a directory it cannot evaluate.
        std::fs::remove_file(flake.join(FLAKE_MANIFEST)).expect("remove manifest");
        let error = flake_directory_from(&package.join("dist").join("cowshed"))
            .expect_err("a directory without flake.nix is not the flake");
        assert!(
            error.message.contains(&flake.display().to_string()),
            "the error must name every directory searched; got {}",
            error.message
        );

        let _ = std::fs::remove_dir_all(&package);
    }

    /// The whole point of the doctor check: a deleted out-link is reported while it is still cheap
    /// to fix, and an agent nobody installed is not a finding.
    #[test]
    fn a_missing_gc_root_under_an_installed_agent_is_an_error_finding() {
        let home = scratch("pinning");
        let plist = home.join("dev.cowshed.sccache.plist");

        assert!(
            pinning_findings(&home, &plist).is_empty(),
            "sccache is opt-in: no agent, no finding"
        );

        std::fs::write(&plist, b"<plist/>").expect("plist");
        let findings = pinning_findings(&home, &plist);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, UNPINNED_CODE);
        assert_eq!(findings[0].severity, FindingSeverity::Error);
        assert!(
            findings[0].message.contains("nix store gc"),
            "the finding must say what is about to happen; got {}",
            findings[0].message
        );
        assert_eq!(findings[0].hint, "cowshed setup --sccache");

        // A root pointing at a collected store path is the same class of problem, said with the
        // path that went missing.
        let collected = home.join("collected-store-path");
        let root = gc_root(&home);
        std::fs::create_dir_all(root.parent().expect("root parent")).expect("root parent");
        std::os::unix::fs::symlink(&collected, &root).expect("symlink the gc root");
        let findings = pinning_findings(&home, &plist);
        assert_eq!(findings.len(), 1);
        assert!(
            findings[0].message.contains("was collected"),
            "got {}",
            findings[0].message
        );

        // A root pinning a real program the plist does not name is launchd running some other
        // build — the exact drift the store-path-in-the-plist rule exists to expose.
        let installed = collected
            .join(STABLE_BINARY_DIRECTORY)
            .join(SCCACHE_BINARY_NAME);
        std::fs::create_dir_all(installed.parent().expect("store bin")).expect("store bin");
        std::fs::write(&installed, b"#!/bin/sh\n").expect("program");
        let findings = pinning_findings(&home, &plist);
        assert_eq!(findings.len(), 1);
        assert!(
            findings[0].message.contains("some other build"),
            "got {}",
            findings[0].message
        );

        // And a plist that does name it is healthy.
        std::fs::write(&plist, format!("<plist>{}</plist>", installed.display())).expect("plist");
        assert!(pinning_findings(&home, &plist).is_empty());

        let _ = std::fs::remove_dir_all(&home);
    }
}
