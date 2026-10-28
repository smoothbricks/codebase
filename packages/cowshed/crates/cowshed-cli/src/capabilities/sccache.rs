//! The host compiler cache an earlier `cowshed setup --sccache` installed, which a cargo project's
//! sandbox reaches while the host keeps it: its pinned build, its LaunchAgent and its client config.

pub mod client_config;
pub mod nix;
pub mod service;
