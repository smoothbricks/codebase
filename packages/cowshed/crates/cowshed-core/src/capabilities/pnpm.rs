use super::cache::{SharedLayout, SharedToolHome};
use super::{CapabilityContribution, DetectionContext, Detector, add_bootstrap};
use crate::Result;
use cowshed_types::capabilities::CapabilityId;

#[cfg(target_os = "macos")]
const PNPM_HOME_PATH: &str = "Library/pnpm/store";
#[cfg(not(target_os = "macos"))]
const PNPM_HOME_PATH: &str = ".local/share/pnpm/store";
pub static PNPM_HOME: SharedToolHome = SharedToolHome {
    variable: Some("PNPM_CONFIG_STORE_DIR"),
    home: PNPM_HOME_PATH,
    layout: SharedLayout::Whole,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Pnpm,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::Project,
    all: &["package.json"],
    any: &["pnpm-lock.yaml"],
    contribute,
    reached_from: None,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = super::javascript::contribute(context, &PNPM_HOME)?;
    add_bootstrap(
        &mut contribution,
        context,
        "pnpm",
        context.program_directories,
    )?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{Fixture, assert_switch};
    use super::*;
    #[test]
    fn convention_0_enables_pnpm() {
        assert_switch(&DETECTOR, &["package.json", "pnpm-lock.yaml"]);
    }
    #[test]
    fn a_manifest_alone_does_not_enable_pnpm() {
        let fixture = Fixture::new();
        fixture.files(&["package.json"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
    #[test]
    fn a_lock_without_a_manifest_does_not_enable_pnpm() {
        let fixture = Fixture::new();
        fixture.files(&["pnpm-lock.yaml"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
}
