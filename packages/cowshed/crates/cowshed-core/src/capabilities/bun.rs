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
}
