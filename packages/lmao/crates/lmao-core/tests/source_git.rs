//! The build-time source → commit map that span provenance reads.

/// Guards the empty-map failure: git answered a revision, yet every per-file lookup came back
/// empty. Skipped where the build had no git at all (e.g. a source archive), where the map is
/// legitimately empty.
#[test]
fn resolves_committed_sources_when_git_answered_a_revision() {
    if !env!("LMAO_GIT_REVISION").is_empty() {
        assert!(lmao_core::source_git_sha("src/lib.rs").is_some());
    }
}
