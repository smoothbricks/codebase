use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, add_bootstrap,
    add_shared_tool_home, host_program_directories,
};
use crate::Result;

pub static ZIG_HOME: SharedToolHome = SharedToolHome {
    variable: Some("ZIG_GLOBAL_CACHE_DIR"),
    home: ".cache/zig",
    layout: SharedLayout::Whole,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Zig,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::Project,
    all: &["build.zig"],
    any: &[],
    contribute,
    reached_from: None,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    add_shared_tool_home(&mut contribution, context.home, &ZIG_HOME);
    add_bootstrap(
        &mut contribution,
        context,
        "zig",
        &host_program_directories(context),
    )?;
    Ok(contribution)
}
#[cfg(test)]
mod tests {
    use super::super::test_support::assert_switch;
    use super::*;
    #[test]
    fn build_zig_enables_zig() {
        assert_switch(&DETECTOR, &["build.zig"]);
    }
}
