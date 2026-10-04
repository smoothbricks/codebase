use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, add_bootstrap,
    host_program_directories,
};
use crate::Result;

pub static NPM_HOME: SharedToolHome = SharedToolHome {
    variable: Some("NPM_CONFIG_CACHE"),
    home: ".npm",
    layout: SharedLayout::Whole("npm"),
    linked_from_checkouts: false,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Npm,
    scope: super::DetectionScope::Project,
    all: &["package.json"],
    any: &["package-lock.json", "npm-shrinkwrap.json"],
    contribute,
    host_cache_homes: &[&NPM_HOME],
    reached_from: None,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = super::javascript::contribute(context, &NPM_HOME)?;
    let directories = host_program_directories(context);
    add_bootstrap(&mut contribution, context, "npm", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{Fixture, assert_switch};
    use super::*;
    #[test]
    fn convention_0_enables_npm() {
        assert_switch(&DETECTOR, &["package.json", "package-lock.json"]);
    }
    #[test]
    fn convention_1_enables_npm() {
        assert_switch(&DETECTOR, &["package.json", "npm-shrinkwrap.json"]);
    }
    #[test]
    fn a_manifest_alone_does_not_enable_npm() {
        let fixture = Fixture::new();
        fixture.files(&["package.json"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
    #[test]
    fn a_lock_without_a_manifest_does_not_enable_npm() {
        let fixture = Fixture::new();
        fixture.files(&["package-lock.json"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
}
