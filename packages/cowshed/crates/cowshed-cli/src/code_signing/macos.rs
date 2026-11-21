//! macOS signs the gateway host: a Developer Tools entry is keyed to its designated requirement.

use std::path::Path;

use cowshed_core::CowshedError;

use super::{Keychain, SigningIdentity, ensure_identity, prepare};

/// This user's cowshed signing identity, made in cowshed's own keychain in `home` when it holds
/// none ([`prepare`], [`ensure_identity`]).
pub fn gateway_host_identity(home: &Path) -> Result<Option<SigningIdentity>, CowshedError> {
    let keychain = Keychain::cowshed(home);
    prepare(&keychain)?;
    ensure_identity(&keychain).map(Some)
}
