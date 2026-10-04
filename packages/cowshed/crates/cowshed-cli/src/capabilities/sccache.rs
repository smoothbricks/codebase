//! The optional host compiler cache a cargo project's sandbox reaches when the host installed one
//! (`cowshed setup --sccache`): its pinned build, its LaunchAgent and its client configuration.

pub mod client_config;
pub mod nix;
pub mod service;
