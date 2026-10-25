//! The first launch of a fresh executable, as the gateway's `first-launch` control op answers it
//! (specs/cowshed/05_gateway.md, "Control plane" and "The gateway host").
//!
//! The first launch of a fresh executable can pay macOS's first-launch malware scan; the second
//! launch of the same bytes is the baseline. The pair says whether a job under the gateway pays
//! that scan. The launches must run from the daemon's own process, so they have the responsible
//! process every job has: this crate carries the probe and the answer and links nothing that
//! launches.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The gateway control op that measures a fresh executable's first launch (05_gateway.md,
/// "Control plane").
pub const FIRST_LAUNCH_OP: &str = "first-launch";

/// One fresh executable's first and second launch, each timed from spawn to exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FirstLaunch {
    pub first_micros: u64,
    pub second_micros: u64,
}

/// What measures a first launch for the daemon: a fresh executable, launched twice from the
/// daemon's own process, so the launches have the daemon's responsible process, as every job does.
pub trait FirstLaunchProbe: fmt::Debug + Send + Sync {
    fn measure(&self) -> Result<FirstLaunch, String>;
}
