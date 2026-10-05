use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, EnvAction, add_bootstrap,
    host_program_directories, shared_tool_contribution,
};
use crate::Result;

pub static UV_HOME: SharedToolHome = SharedToolHome {
    variable: Some("UV_CACHE_DIR"),
    home: ".cache/uv",
    layout: SharedLayout::Whole("uv"),
    linked_from_checkouts: false,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Uv,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::Project,
    all: &[],
    any: &["pyproject.toml", "uv.lock"],
    contribute,
    host_cache_homes: &[&UV_HOME],
    reached_from: None,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = shared_tool_contribution(context, &UV_HOME)?;
    if context.trust_bundle.is_some() {
        contribution
            .env
            .insert("UV_SYSTEM_CERTS", EnvAction::Default("true".into()));
    }
    let mut directories = host_program_directories(context);
    directories.insert(0, context.home.join(".local/bin"));
    add_bootstrap(&mut contribution, context, "uv", &directories)?;
    Ok(contribution)
}
#[cfg(test)]
mod tests {
    use super::super::test_support::assert_switch;
    use super::*;
    #[test]
    fn pyproject_enables_uv() {
        assert_switch(&DETECTOR, &["pyproject.toml"]);
    }
    #[test]
    fn uv_lock_enables_uv() {
        assert_switch(&DETECTOR, &["uv.lock"]);
    }
}
