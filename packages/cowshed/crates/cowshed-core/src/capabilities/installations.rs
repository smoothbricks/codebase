//! Read authority required by conventionally installed bootstrap programs.

use super::{CapabilityContribution, CapabilityGrant, GrantAccess, GrantScope};
use std::path::Path;

pub(super) fn contribute_reads(contribution: &mut CapabilityContribution, executable: &Path) {
    if executable.starts_with("/nix/store") {
        // Store packages may load dependencies from other immutable entries. This installation
        // convention does not grant the daemon, client-cache writes, or the operator's profile.
        contribution.grants.push(CapabilityGrant {
            path: "/nix/store".into(),
            scope: GrantScope::Subtree,
            access: GrantAccess::Read,
        });
    }
}
