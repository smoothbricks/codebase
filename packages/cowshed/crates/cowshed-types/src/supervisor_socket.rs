//! Supervisor socket vocabulary: build identity, socket naming and the admission source.
//!
//! The socket shell that binds, serves and calls stays with the runtime; only the names and
//! values every layer must agree on live here.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::metadata::WorkspaceName;
use crate::repository::RepoId;

/// The identity of a cowshed build: the Mach-O `LC_UUID` the linker derives from the image's
/// contents on macOS, the SHA-256 of the executable elsewhere. Two binaries share one only when
/// they are the same build, so a supervisor started by another build — whatever changed, and
/// whether or not anybody remembered to say so — is refused by name at `hello` and drained by the
/// manager of a newly started daemon (11_shell.md "Draining a supervisor of another build").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BuildId(String);

impl BuildId {
    /// The build another process named on the wire.
    pub fn named(build: &str) -> Self {
        Self(build.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BuildId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}
/// Hex digits of the workspace digest that names a supervisor's socket ([`socket_path`]).
const SOCKET_DIGEST_HEX: usize = 32;

/// Where the supervisor of `workspace` in `repo_id` listens: the store's `run` directory, under
/// a digest of the pair. Owner, repository and workspace names together can exceed the 104
/// bytes a Unix socket path may hold on macOS; the digest keeps every path at 64.
pub fn socket_path(store_root: &Path, repo_id: &RepoId, workspace: &WorkspaceName) -> PathBuf {
    let mut identity = Vec::with_capacity(repo_id.as_str().len() + workspace.as_str().len() + 1);
    identity.extend_from_slice(repo_id.as_str().as_bytes());
    // NUL is in neither name, so no two pairs share an identity.
    identity.push(0);
    identity.extend_from_slice(workspace.as_str().as_bytes());
    let digest: String = sha2::Sha256::digest(&identity)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    store_root
        .join("run")
        .join(format!("{}.sock", &digest[..SOCKET_DIGEST_HEX]))
}

/// Whether `name`, an entry of the store's `run` directory, is a workspace supervisor's socket:
/// exactly the name [`socket_path`] gives one, its digest's first 32 lowercase hex digits and then
/// `.sock`. Nothing else there is one: not the manager's `manager.sock`, not a supervisor's
/// `<digest>.members.sock` membership endpoint ([`membership_socket_path`]), its `.groups`
/// ledger or `.sock.lock` lease, nor a `.bind` listener still being published.
pub fn is_socket_name(name: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt as _;
    name.as_bytes()
        .strip_suffix(b".sock")
        .is_some_and(|digest| {
            digest.len() == SOCKET_DIGEST_HEX
                && digest
                    .iter()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
}
/// Only the membership endpoint is exported to admitted children, never coordinator authority.
pub const MEMBERSHIP_SOCKET_ENV: &str = "COWSHED_SUPERVISOR_SOCKET";

pub fn membership_socket_path(supervisor: &Path) -> PathBuf {
    supervisor.with_extension("members.sock")
}
/// The controller route that admitted an exec. The route is the authority for the gate class and
/// the host-gate scope (05_gateway.md "Classes", 07_api.md "Capability split"): no request
/// field, environment value or caller claim selects it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AdmissionSource {
    /// worker.exec: a workspace's own capability, which admits background, interactive and
    /// resident work in its workspace sandbox only.
    Worker,
    /// coordinator.exec and the controller's own execs: the trusted coordinator, which may also
    /// admit the gate class and the host-gate scope.
    Coordinator,
}
