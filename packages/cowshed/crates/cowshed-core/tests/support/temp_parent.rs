//! Canonical parent for independent sandbox fixtures, not a grant to inherited store paths.

pub(crate) fn canonical_parent() -> std::path::PathBuf {
    std::fs::canonicalize("/tmp").unwrap_or_else(|error| panic!("canonical /tmp: {error}"))
}
