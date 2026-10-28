//! The patched sccache an earlier `cowshed setup --sccache` built and pinned by a nix GC root.
//!
//! Setup installs no sccache now: cowshed's Cargo compiler cache is mbx (03_caches.md, "mbx";
//! `crate::capabilities::mbx`). What an earlier setup left is read here: the program the GC root
//! pins, which the host's sccache agent runs straight out of the store, and whether that agent
//! still has a program and a root keeping it.
//!
//! The plist names the resolved store path, not the out-link, so the agent cannot start a build
//! other than the one its definition names; the out-link is what keeps that path from `nix store
//! gc`.

use crate::gateway_service::launchd_error;
use crate::launchd::{
    LaunchAgentTarget, SCCACHE_BINARY_NAME, STABLE_BINARY_DIRECTORY, StoreBackedProgram,
};
use cowshed_core::api::{Finding, FindingSeverity};
use cowshed_core::capabilities::sccache::gc_root;
use cowshed_core::{CowshedError, Result};
use std::path::Path;

/// The doctor finding code for an installed agent whose program is no longer pinned or no longer
/// there.
const UNPINNED_CODE: &str = "sccache-unpinned";

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
            "cowshed sccache stop",
        )
    })?;
    StoreBackedProgram::new(home, root, &target, SCCACHE_BINARY_NAME).map_err(launchd_error)
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
///
/// `agent` is the agent's control target: its plist is the definition launchd loaded, and its
/// label is how a finding names it.
pub fn pinning_findings(home: &Path, agent: &LaunchAgentTarget) -> Vec<Finding> {
    let plist = agent.plist_path();
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
                    agent.label(),
                    root.display()
                ),
                "cowshed sccache stop",
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
            "cowshed sccache stop",
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
                    "cowshed sccache stop",
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
    use std::path::PathBuf;

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

    /// The whole point of the doctor check: a deleted out-link is reported while it is still cheap
    /// to fix, and an agent nobody installed is not a finding.
    #[test]
    fn a_missing_gc_root_under_an_installed_agent_is_an_error_finding() {
        let home = scratch("pinning");
        let agent = LaunchAgentTarget::new(&home, "dev.cowshed.sccache").expect("the agent");
        let plist = agent.plist_path().to_path_buf();
        std::fs::create_dir_all(agent.launch_agents_directory()).expect("LaunchAgents");

        assert!(
            pinning_findings(&home, &agent).is_empty(),
            "sccache is opt-in: no agent, no finding"
        );

        std::fs::write(&plist, b"<plist/>").expect("plist");
        let findings = pinning_findings(&home, &agent);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].code, UNPINNED_CODE);
        assert_eq!(findings[0].severity, FindingSeverity::Error);
        assert!(
            findings[0].message.contains("nix store gc"),
            "the finding must say what is about to happen; got {}",
            findings[0].message
        );
        assert_eq!(findings[0].hint, "cowshed sccache stop");

        // A root pointing at a collected store path is the same class of problem, said with the
        // path that went missing.
        let collected = home.join("collected-store-path");
        let root = gc_root(&home);
        std::fs::create_dir_all(root.parent().expect("root parent")).expect("root parent");
        std::os::unix::fs::symlink(&collected, &root).expect("symlink the gc root");
        let findings = pinning_findings(&home, &agent);
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
        let findings = pinning_findings(&home, &agent);
        assert_eq!(findings.len(), 1);
        assert!(
            findings[0].message.contains("some other build"),
            "got {}",
            findings[0].message
        );

        // And a plist that does name it is healthy.
        std::fs::write(&plist, format!("<plist>{}</plist>", installed.display())).expect("plist");
        assert!(pinning_findings(&home, &agent).is_empty());

        let _ = std::fs::remove_dir_all(&home);
    }
}
