use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CacheMount, CapabilityContribution, CapabilityId, DetectionContext, Detector, EnvAction,
    add_bootstrap, host_program_directories,
};
use crate::Result;

pub static GRADLE_HOME: SharedToolHome = SharedToolHome {
    variable: None,
    home: ".gradle",
    layout: SharedLayout::Split {
        links: &[("caches", "gradle/caches")],
        state_files: &[],
    },
    linked_from_checkouts: false,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Gradle,
    scope: super::DetectionScope::Project,
    all: &[],
    any: &[
        "settings.gradle",
        "settings.gradle.kts",
        "build.gradle",
        "build.gradle.kts",
    ],
    contribute,
    host_cache_homes: &[&GRADLE_HOME],
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    // Share only the caches; user configuration and credentials never enter the tool home.
    contribution.env.insert(
        "GRADLE_USER_HOME",
        EnvAction::Own(
            context
                .environment_root
                .join("cache/gradle")
                .into_os_string(),
        ),
    );
    if context.caches_root.is_dir() {
        contribution.cache_mounts.push(CacheMount {
            source: context.caches_root.join("gradle/caches"),
            private_target: Some(context.environment_root.join("cache/gradle/caches")),
        });
    }
    let directories = host_program_directories(context);
    add_bootstrap(&mut contribution, context, "gradle", &directories)?;
    add_bootstrap(&mut contribution, context, "java", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::assert_switch;
    use super::*;
    #[test]
    fn gradle_settings_enable_gradle() {
        assert_switch(&DETECTOR, &["settings.gradle.kts"]);
    }
}
