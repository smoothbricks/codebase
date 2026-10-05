//! The `.codegraph/` marker owns one checkout's entire per-tree index (spec 16).
//! The index is cloned/adopted with build state, not shared between tree writers.

use super::{
    BuildStatePath, CapabilityContribution, CapabilityId, DetectionContext, DetectionScope,
    Detector, MarkerKind,
};
use crate::{CowshedError, Result};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Codegraph,
    marker_kind: MarkerKind::Directory,
    scope: DetectionScope::Project,
    all: &[".codegraph"],
    any: &[],
    host_cache_homes: &[],
    reached_from: None,
    contribute,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let relative = context
        .project_root
        .strip_prefix(context.workspace_root)
        .map_err(|_| {
            CowshedError::integrity(
                "indexer project is outside the checkout",
                "keep the capability directory contained",
            )
        })?;
    let checkout = relative.join(".codegraph");
    let volume = relative.join("codegraph");
    Ok(CapabilityContribution {
        build_state: vec![BuildStatePath::from_paths(&checkout, &volume)?],
        ..CapabilityContribution::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::test_support::Fixture;

    #[test]
    fn the_directory_marker_contributes_the_whole_index_and_nothing_else() {
        let fixture = Fixture::new();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
        std::fs::create_dir(fixture.root.join(".codegraph")).unwrap();
        let detected = DETECTOR.detect(&fixture.context()).unwrap().unwrap();
        assert_eq!(
            detected.build_state,
            [BuildStatePath::new(".codegraph", "codegraph").unwrap()]
        );
        assert!(
            detected.env.is_empty()
                && detected.grants.is_empty()
                && detected.cache_mounts.is_empty()
        );
        std::fs::remove_dir(fixture.root.join(".codegraph")).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }

    #[test]
    fn the_fixed_build_link_remains_a_marker_but_external_links_are_refused() {
        let fixture = Fixture::new();
        let marker = fixture.root.join(".codegraph");
        std::os::unix::fs::symlink(".cowshed/build/codegraph", &marker).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_some());
        std::fs::remove_file(&marker).unwrap();
        std::os::unix::fs::symlink("/etc", &marker).unwrap();
        assert!(DETECTOR.detect(&fixture.context()).is_err());
    }
}
