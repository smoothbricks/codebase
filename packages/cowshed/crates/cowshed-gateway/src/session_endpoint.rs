//! Where the gateway serves a workspace's session: each platform's endpoint topology.
//!
//! A session's endpoint selects its workspace before any token is compared (05_gateway.md,
//! "Placement and identity"). macOS serves each session on the base of the workspace's TCP port
//! block (`macos.rs`); Linux serves it on a Unix socket in the store's private data socket root
//! (`linux.rs`). Each file answers the same questions behind the same signatures: which data
//! socket root a store's gateway is configured with, what a start prepares, and what a
//! configuration and a session must satisfy. The daemon's configuration and its actor ask them
//! and never branch on the platform.
//!
//! Binding itself is not here: [`cowshed_gateway_types::WorkspaceEndpoint`] is data, and the
//! actor binds whichever variant a validated session carries.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// The data socket root a gateway serving `store` is configured with
/// ([`crate::GatewayConfig::data_socket_root`]): `None` where sessions have no data socket.
pub use platform::data_socket_root;
pub(crate) use platform::{prepare, validate_config, validate_session};
