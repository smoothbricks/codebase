use super::cache::{SharedLayout, SharedToolHome};
use crate::Result;
use super::{CapabilityContribution, CapabilityId, DetectionContext, Detector, add_bootstrap, host_program_directories};

pub static BUN_HOME: SharedToolHome = SharedToolHome {
    variable: Some("BUN_INSTALL_CACHE_DIR"), home: ".bun/install/cache", layout: SharedLayout::Whole("bun/install/cache"), linked_from_checkouts: true,
};

pub const DETECTOR: Detector = Detector { id: CapabilityId::Bun, scope: super::DetectionScope::Project, all: &["package.json"], any: &["bun.lock", "bun.lockb"], contribute, host_cache_homes: &[&BUN_HOME] };

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = super::javascript::contribute(context, &BUN_HOME)?;
    let mut directories = host_program_directories(context);
    directories.insert(0, context.home.join(".bun/bin"));
    add_bootstrap(&mut contribution, "bun", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_support::{Fixture, assert_switch};
    #[test] fn convention_0_enables_bun() { assert_switch(&DETECTOR, &["package.json", "bun.lock"]); }
    #[test] fn convention_1_enables_bun() { assert_switch(&DETECTOR, &["package.json", "bun.lockb"]); }
    #[test] fn a_manifest_alone_does_not_enable_bun() {
        let fixture = Fixture::new(); fixture.files(&["package.json"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
    #[test] fn a_lock_without_a_manifest_does_not_enable_bun() {
        let fixture = Fixture::new(); fixture.files(&["bun.lock"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
}
