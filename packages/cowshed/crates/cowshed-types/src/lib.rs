//! Canonical cowshed value declarations, starting with repository identity and binding.
//!
//! `cowshed-core`, the CLI, the gateway and the addon consume these values directly.
//! The crate performs no ambient effects; its one in-workspace dependency is
//! `cowshed-gateway-types`, which owns repository-id character validation. Shells
//! construct these values where they live.

pub mod capabilities;
pub mod clock;
pub mod config;
pub mod image;
pub mod land_turn;
pub mod lifecycle;
pub mod metadata;
pub mod paths;
pub mod repository;
pub mod supervisor_socket;
pub mod trace_enum;
pub mod vnodes;
pub mod wait;
