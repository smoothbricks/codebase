//! Canonical workspace and store path vocabulary.
//!
//! Suffixes, published-file paths and pure directory derivations named by more
//! than one layer. Shells join these onto mounts and store roots where they
//! live; nothing here touches the filesystem.

use std::path::{Path, PathBuf};

use crate::metadata::WorkspaceIncarnation;

/// Where the supervisor stages the host executable inside the workspace. Every sandbox
/// profile denies writes beneath it, like the other controller-published files, so no job can
/// replace the program every later command's host runs as.
pub const SHELL_HOST_DIRECTORY: &str = ".cowshed/shell-host";

/// Suffix of the per-workspace land lease beside the project's session images.
pub const LAND_LEASE_SUFFIX: &str = ".land.lock";

/// Suffix of the per-workspace lease file beside a project's session images.
pub const INTENT_LEASE_SUFFIX: &str = ".intent.lock";

pub const WORKSPACE_ENVIRONMENT_PATH: &str = ".cowshed/env";

pub const WORKSPACE_GIT_FETCH_CONFIG_PATH: &str = ".cowshed/git-fetch.inc";

/// Private identity-only global Git config for one workspace, never shared .git state.
pub const WORKSPACE_GIT_IDENTITY_CONFIG_PATH: &str = ".cowshed/git-identity.inc";

/// The branch a workspace's commits are expected to reach.
///
/// One constant for two questions that must never disagree: where `land` merges by default, and
/// which branch `rm` requires to contain a workspace's head before destroying its object store.
/// If they diverged, `land` would satisfy a check `rm` does not make.
pub const DEFAULT_LANDING_BRANCH: &str = "main";

/// The workspace's pool directory, `<store>/run/cpu/<incarnation as base64url>`: the one
/// directory its sandbox opens for FIFOs, where its jobs' jobserver fifos live. Sibling
/// workspaces' directories sit beside it, and no sandbox can list `run/cpu`.
pub fn cpu_pool_directory(store: &Path, incarnation: &WorkspaceIncarnation) -> PathBuf {
    use base64::Engine as _;
    // A validated incarnation is 32 lowercase hex digits: 16 bytes.
    let digit = |byte: u8| {
        if byte <= b'9' {
            byte - b'0'
        } else {
            byte - b'a' + 10
        }
    };
    let mut identity = [0_u8; 16];
    for (slot, pair) in identity
        .iter_mut()
        .zip(incarnation.as_str().as_bytes().as_chunks::<2>().0)
    {
        *slot = digit(pair[0]) * 16 + digit(pair[1]);
    }
    let mut encoded = [0_u8; 22];
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode_slice(identity, &mut encoded)
        .expect("22 bytes hold a complete base64url 128-bit incarnation");
    let name = std::str::from_utf8(&encoded).expect("base64url is ASCII");
    store.join("run/cpu").join(name)
}

/// Where the profile files of `store_root`'s launches live: beside its supervisors' sockets in
/// its `run` directory.
pub fn profile_directory(store_root: &Path) -> PathBuf {
    store_root.join("run").join("profiles")
}
