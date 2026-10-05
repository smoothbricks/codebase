use super::cache::{SharedLayout, SharedToolHome};
use super::{
    CapabilityContribution, CapabilityId, DetectionContext, Detector, add_bootstrap,
    host_program_directories,
};
use crate::Result;

pub static BUN_HOME: SharedToolHome = SharedToolHome {
    variable: Some("BUN_INSTALL_CACHE_DIR"),
    home: ".bun/install/cache",
    layout: SharedLayout::Whole("bun/install/cache"),
    linked_from_checkouts: true,
};

pub const DETECTOR: Detector = Detector {
    id: CapabilityId::Bun,
    marker_kind: super::MarkerKind::File,
    scope: super::DetectionScope::Project,
    all: &["package.json"],
    any: &["bun.lock", "bun.lockb"],
    contribute,
    host_cache_homes: &[&BUN_HOME],
    reached_from: None,
};

fn contribute(context: &DetectionContext<'_>) -> Result<CapabilityContribution> {
    let mut contribution = super::javascript::contribute(context, &BUN_HOME)?;
    let mut directories = host_program_directories(context);
    directories.insert(0, context.home.join(".bun/bin"));
    add_bootstrap(&mut contribution, context, "bun", &directories)?;
    Ok(contribution)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{Fixture, assert_switch};
    use super::*;
    #[test]
    fn convention_0_enables_bun() {
        assert_switch(&DETECTOR, &["package.json", "bun.lock"]);
    }
    #[test]
    fn convention_1_enables_bun() {
        assert_switch(&DETECTOR, &["package.json", "bun.lockb"]);
    }
    #[test]
    fn a_manifest_alone_does_not_enable_bun() {
        let fixture = Fixture::new();
        fixture.files(&["package.json"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }
    #[test]
    fn a_lock_without_a_manifest_does_not_enable_bun() {
        let fixture = Fixture::new();
        fixture.files(&["bun.lock"]);
        assert!(DETECTOR.detect(&fixture.context()).unwrap().is_none());
    }

    /// A repository's `.envrc` may run `bun` before any shell it evaluates puts bun on PATH, so
    /// a Bun project bootstraps the host's own bun as the `bun` command, granted as the literal
    /// program it probed beneath HOME and the file it resolves to.
    #[test]
    fn a_bun_project_bootstraps_the_host_bun_before_its_shell_evaluates() {
        use super::super::{BootstrapProgram, CapabilityGrant, GrantAccess, GrantScope};
        use std::os::unix::fs::PermissionsExt as _;
        let fixture = Fixture::new();
        fixture.files(&["package.json", "bun.lock"]);
        let installed = fixture.home.join(".bun/bin/bun");
        std::fs::create_dir_all(installed.parent().unwrap()).unwrap();
        std::fs::write(&installed, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&installed, std::fs::Permissions::from_mode(0o755)).unwrap();

        let contribution = DETECTOR
            .detect(&fixture.context())
            .unwrap()
            .expect("bun project");
        // `node` is bootstrapped too, from whatever the host itself installs.
        assert_eq!(
            contribution
                .bootstrap_programs
                .iter()
                .find(|program| program.name == "bun"),
            Some(&BootstrapProgram {
                name: "bun",
                target: installed.clone(),
            })
        );
        assert!(contribution.grants.contains(&CapabilityGrant {
            path: installed,
            scope: GrantScope::Literal,
            access: GrantAccess::Read,
        }));
    }

    /// Bun's isolated linker writes its cache's path into every `node_modules/.bun` link, so a
    /// shed's `bun install` names the host's own cache path, the one main's links name: the
    /// shared cache once host setup has relocated it, and before that the host's still-private
    /// cache, read-only — never a store under the shed's private HOME or `XDG_CACHE_HOME`.
    #[test]
    fn a_shed_installs_through_the_host_cache_path_that_resolves_to_the_shared_cache() {
        use super::super::{CapabilityGrant, EnvAction, GrantAccess, GrantScope};
        let fixture = Fixture::new();
        fixture.files(&["package.json", "bun.lock"]);
        let link = BUN_HOME.links(&fixture.home, &fixture.caches).remove(0);
        let host = EnvAction::Own(link.host.clone().into_os_string());
        std::fs::create_dir_all(&link.host).unwrap();

        let unrelocated = super::super::detect_for_workspace(&fixture.context())
            .unwrap()
            .contribution;
        assert_eq!(
            unrelocated.env.get("BUN_INSTALL_CACHE_DIR"),
            Some(&host),
            "an unrelocated host cache is still the one path main's links name"
        );
        assert!(unrelocated.grants.contains(&CapabilityGrant {
            path: link.host.clone(),
            scope: GrantScope::Subtree,
            access: GrantAccess::Read,
        }));

        std::fs::remove_dir(&link.host).unwrap();
        std::fs::create_dir_all(&link.shared).unwrap();
        std::os::unix::fs::symlink(&link.shared, &link.host).unwrap();
        let relocated = super::super::detect_for_workspace(&fixture.context())
            .unwrap()
            .contribution;
        let Some(EnvAction::Own(cache)) = relocated.env.get("BUN_INSTALL_CACHE_DIR") else {
            panic!("a shed's bun is pointed at its install cache: {relocated:?}");
        };
        assert_eq!(cache.as_os_str(), link.host.as_os_str());
        assert_eq!(
            std::fs::canonicalize(cache).unwrap(),
            std::fs::canonicalize(&link.shared).unwrap(),
            "the shed's install cache resolves to the shared cache"
        );
        assert!(
            relocated
                .cache_mounts
                .iter()
                .any(|mount| mount.source == link.shared && mount.private_target.is_none()),
            "the shared cache is the shed's writable install cache"
        );
    }
}
