use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, EnvAction, add_bootstrap,
    add_shared_tool_home, host_program_directories,
};
use crate::Result;

/// Gradle's own `~/.gradle`: only `caches` is shared. The daemon, wrapper distributions, native
/// libraries, JDKs and `gradle.properties` beside it stay the host's.
pub static GRADLE_HOME: SharedToolHome = SharedToolHome {
    variable: None,
    home: ".gradle",
    layout: SharedLayout::Split {
        caches: &["caches"],
        state_files: &[],
    },
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Gradle,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::Project,
    all: &[],
    any: &[
        "settings.gradle",
        "settings.gradle.kts",
        "build.gradle",
        "build.gradle.kts",
    ],
    contribute,
    reached_from: None,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = CapabilityContribution::default();
    // A private user home whose `caches` links to the host's: user configuration and credentials
    // never enter the tool home a sandbox runs with.
    let private = context.environment_root.join("cache/gradle");
    contribution.env.insert(
        "GRADLE_USER_HOME",
        EnvAction::Own(private.clone().into_os_string()),
    );
    add_shared_tool_home(&mut contribution, context.home, &GRADLE_HOME);
    for cache in &mut contribution.shared_caches {
        cache.private_link = cache.path.file_name().map(|name| private.join(name));
    }
    let directories = host_program_directories(context);
    add_bootstrap(&mut contribution, context, "gradle", &directories)?;
    add_bootstrap(&mut contribution, context, "java", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{Fixture, assert_switch};
    use super::*;
    use crate::capabilities::{CapabilityGrant, GrantAccess, GrantScope, SharedCache};

    #[test]
    fn gradle_settings_enable_gradle() {
        assert_switch(&DETECTOR, &["settings.gradle.kts"]);
    }

    /// The private `GRADLE_USER_HOME`'s `caches` links to the host's `~/.gradle/caches`, which is
    /// shared read-write; `~/.gradle` itself is a literal read and nothing else in it is granted.
    #[test]
    fn gradle_shares_only_the_host_caches_through_its_private_home() {
        let fixture = Fixture::new();
        fixture.files(&["settings.gradle"]);
        let contribution = DETECTOR
            .detect(&fixture.context())
            .unwrap()
            .expect("gradle detected");
        let gradle = fixture.home.join(".gradle");
        assert_eq!(
            contribution.shared_caches,
            vec![SharedCache {
                path: gradle.join("caches"),
                private_link: Some(fixture.environment.join("cache/gradle/caches")),
            }]
        );
        assert_eq!(
            contribution.env.get("GRADLE_USER_HOME"),
            Some(&EnvAction::Own(
                fixture.environment.join("cache/gradle").into()
            ))
        );
        let home_grants = contribution
            .grants
            .iter()
            .filter(|grant| grant.path.starts_with(&gradle))
            .collect::<Vec<_>>();
        assert_eq!(
            home_grants,
            [&CapabilityGrant {
                path: gradle,
                scope: GrantScope::Literal,
                access: GrantAccess::Read,
            }]
        );
    }
}
