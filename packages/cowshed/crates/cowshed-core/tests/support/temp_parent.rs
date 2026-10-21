//! Fixture parents. Each names a directory the running test is already granted; neither adds a
//! grant (08_testing.md, "Fixtures").

use std::path::PathBuf;

/// The short parent for a fixture whose paths are used as spelled, Unix sockets included:
/// `COWSHED_RUNTIME_LINK`, the workspace's short runtime link, when it is set, else `/tmp`.
/// The link is the one a workspace's own sockets live under (04_sandbox.md), and the sandbox
/// admits binding below it by that literal path. It is never canonicalized: that expands it to
/// `<mount>/.cowshed/run`, past `sockaddr_un.sun_path`'s 104 bytes for a socket a few levels
/// down. The system temporary directory is not such a parent: in a workspace it is the sandbox's
/// `TMPDIR` under the cowshed store, and on a macOS runner `/var/folders/<..>/T/`, both too long
/// for a socket below a fixture. A link that is set but is not an absolute directory fails
/// loudly; it never falls through to `/tmp`.
#[allow(dead_code)]
pub(crate) fn scratch_parent() -> PathBuf {
    let path = match std::env::var_os("COWSHED_RUNTIME_LINK") {
        Some(value) => PathBuf::from(value),
        None => PathBuf::from("/tmp"),
    };
    assert!(
        path.is_absolute(),
        "fixture parent {} must be absolute",
        path.display()
    );
    assert!(
        path.is_dir(),
        "fixture parent {} must be a directory",
        path.display()
    );
    path
}

/// Canonical parent for independent sandbox fixtures, not a grant to inherited store paths.
#[allow(dead_code)]
pub(crate) fn canonical_parent() -> PathBuf {
    std::fs::canonicalize("/tmp").unwrap_or_else(|error| panic!("canonical /tmp: {error}"))
}
