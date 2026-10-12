//! Rust span provenance is the explicit build revision, independent of source paths.

#[test]
fn source_provenance_is_the_explicit_build_revision() {
    let revision = std::env::var("LMAO_GIT_REVISION")
        .ok()
        .filter(|revision| !revision.is_empty());
    assert_eq!(lmao_core::build_revision(), revision.as_deref());
}
