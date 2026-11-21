//! Linux signs nothing: no first-launch assessment reads a signature, so the gateway host is an
//! unsigned copy of the build and no keychain, `security` or `codesign` is ever asked.

use std::path::Path;

use cowshed_core::CowshedError;

use super::SigningIdentity;

/// No identity: the gateway host is installed unsigned and never verified.
pub fn gateway_host_identity(_home: &Path) -> Result<Option<SigningIdentity>, CowshedError> {
    Ok(None)
}
