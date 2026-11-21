//! macOS serves each session on the base of its workspace's TCP port block.
//!
//! The block's grammar is the session's own to state
//! ([`cowshed_gateway_types::WorkspaceEndpoint::validate_macos_port_block`], which every
//! session's validation applies on this platform), so the daemon adds nothing: it has no data
//! socket root to configure, prepare or hold a session to.

use std::io;
use std::path::{Path, PathBuf};

use cowshed_gateway_types::{ConfigError, WorkspaceSession};

use crate::config::GatewayConfig;

/// A macOS gateway serves no data socket.
pub fn data_socket_root(_store: &Path) -> Option<PathBuf> {
    None
}

/// Nothing exists before a TCP listener binds.
pub(crate) fn prepare(_config: &GatewayConfig) -> io::Result<()> {
    Ok(())
}

pub(crate) fn validate_config(_config: &GatewayConfig) -> Result<(), ConfigError> {
    Ok(())
}

pub(crate) fn validate_session(
    _config: &GatewayConfig,
    _session: &WorkspaceSession,
) -> Result<(), ConfigError> {
    Ok(())
}
