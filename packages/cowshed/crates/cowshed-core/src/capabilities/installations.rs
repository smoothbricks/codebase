//! Read authority required by conventionally installed programs.

use super::{CapabilityContribution, CapabilityGrant, GrantAccess, GrantScope};
use crate::Result;
use std::io;
use std::path::{Path, PathBuf};

const NIX_STORE: &str = "/nix/store";

pub(super) fn contribute_reads(contribution: &mut CapabilityContribution, executable: &Path) {
    if executable.starts_with(NIX_STORE) {
        // Store packages may load dependencies from other immutable entries. This installation
        // convention does not grant the daemon, client-cache writes, or the operator's profile.
        contribution.grants.push(CapabilityGrant {
            path: NIX_STORE.into(),
            scope: GrantScope::Subtree,
            access: GrantAccess::Read,
        });
    }
}

/// A sandboxed command runs a program by name through the links in `directories` — the mode's
/// private `tools/bin` and the workspace's `.cowshed/bin`. A link into the Nix store needs the
/// store's other immutable entries to load, with or without a Nix project, so it gets the same
/// read-only store grant a store-resolved bootstrap program does. A host with no store gets none.
///
/// Links are read, never resolved: preparation and the shim directory link resolved targets.
pub(super) fn contribute_linked_program_reads(
    contribution: &mut CapabilityContribution,
    directories: &[PathBuf],
) -> Result<()> {
    let Some(target) = store_linked_program(directories)? else {
        return Ok(());
    };
    match std::fs::symlink_metadata(NIX_STORE) {
        Ok(metadata) if metadata.is_dir() => contribute_reads(contribution, &target),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        // The sandboxed supervisor repeating detection cannot see a store its own profile does
        // not grant: that profile is the ceiling every child narrows, so no child could read
        // the store either, and the contribution stays the one that profile was rendered from.
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {}
        Err(error) => return Err(super::detection_error(Path::new(NIX_STORE), error)),
    }
    Ok(())
}

/// The first program link in `directories` whose target is in the Nix store.
fn store_linked_program(directories: &[PathBuf]) -> Result<Option<PathBuf>> {
    for directory in directories {
        let entries = match std::fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(super::detection_error(directory, error)),
        };
        for entry in entries {
            let entry = entry.map_err(|error| super::detection_error(directory, error))?;
            let link = entry.path();
            match std::fs::read_link(&link) {
                Ok(target) if target.starts_with(NIX_STORE) => return Ok(Some(target)),
                Ok(_) => {}
                // Not a link: a workspace's own script, which names nothing in the store.
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(super::detection_error(&link, error)),
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::Fixture;
    use super::*;
    use std::collections::BTreeMap;

    fn store_read() -> CapabilityGrant {
        CapabilityGrant {
            path: NIX_STORE.into(),
            scope: GrantScope::Subtree,
            access: GrantAccess::Read,
        }
    }

    /// A shim or tool link into the store reads the store without any Nix convention, on a host
    /// that has one; a workspace's own script beside it reads nothing.
    #[test]
    fn a_program_linked_into_the_store_reads_the_store_without_a_nix_project() {
        let fixture = Fixture::new();
        for directory in [".cowshed/bin", ".cowshed/tools/bin"] {
            let bin = fixture.root.join(directory);
            std::fs::create_dir_all(&bin).unwrap();
            std::fs::write(bin.join("script"), "#!/bin/sh\n").unwrap();
        }
        let detect = || super::super::detect(&fixture.context(), &BTreeMap::new()).unwrap();
        assert!(!detect().contribution.grants.contains(&store_read()));

        std::os::unix::fs::symlink(
            "/nix/store/00000000000000000000000000000000-node/bin/node",
            fixture.root.join(".cowshed/bin/node"),
        )
        .unwrap();
        let detected = detect();
        assert!(detected.active.is_empty(), "{:?}", detected.active);
        assert_eq!(
            detected.contribution.grants.contains(&store_read()),
            Path::new(NIX_STORE).is_dir()
        );
    }

    #[test]
    fn a_program_linked_outside_the_store_reads_nothing() {
        let fixture = Fixture::new();
        let tools = fixture.root.join(".cowshed/tools/bin");
        std::fs::create_dir_all(&tools).unwrap();
        std::os::unix::fs::symlink("/usr/bin/env", tools.join("env")).unwrap();
        let detected = super::super::detect(&fixture.context(), &BTreeMap::new()).unwrap();
        assert!(detected.contribution.grants.is_empty());
    }
}
