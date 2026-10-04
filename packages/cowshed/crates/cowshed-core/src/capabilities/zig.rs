use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, add_bootstrap,
    host_program_directories, shared_tool_contribution,
};
use crate::Result;

pub static ZIG_HOME: SharedToolHome = SharedToolHome {
    variable: Some("ZIG_GLOBAL_CACHE_DIR"),
    home: ".cache/zig",
    layout: SharedLayout::Whole("zig"),
    linked_from_checkouts: false,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Zig,
    scope: super::DetectionScope::Project,
    all: &["build.zig"],
    any: &[],
    contribute,
    host_cache_homes: &[&ZIG_HOME],
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = shared_tool_contribution(context, &ZIG_HOME)?;
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
