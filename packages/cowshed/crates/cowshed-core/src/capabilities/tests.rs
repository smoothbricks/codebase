use super::test_support::Fixture;
use super::*;

#[test]
fn a_plain_repository_has_no_tool_authority() {
    let fixture = Fixture::new();
    assert_eq!(
        detect(&fixture.context(), &BTreeMap::new()).unwrap(),
        DetectedCapabilities::default()
    );
}

#[test]
fn an_override_cannot_enable_an_absent_convention() {
    let fixture = Fixture::new();
    let overrides = BTreeMap::from([(CapabilityId::Nx, CapabilityOverride {
        disabled: false, directory: Some(PathBuf::from("frontend")),
    })]);
    assert!(detect(&fixture.context(), &overrides).unwrap().active.is_empty());
    fixture.files(&["nx.json"]);
    assert!(detect(&fixture.context(), &overrides).unwrap().active.is_empty());
    fixture.files(&["frontend/nx.json"]);
    let detected = detect(&fixture.context(), &overrides).unwrap();
    assert_eq!(detected.active, [CapabilityId::Nx]);
    assert_eq!(detected.contribution.env.get("NX_WORKSPACE_ROOT_PATH"),
        Some(&EnvAction::Own(fixture.root.join("frontend").into_os_string())));
    assert!(detected.contribution.daemon_isolation.discard_at_mint.contains(&PathBuf::from("frontend/.nx/workspace-data/d")));
}

#[test]
fn a_disabled_capability_does_not_inspect_its_convention() {
    let fixture = Fixture::new();
    std::os::unix::fs::symlink("/etc/hosts", fixture.root.join("nx.json")).unwrap();
    let overrides = BTreeMap::from([(CapabilityId::Nx, CapabilityOverride { disabled: true, directory: None })]);
    assert!(detect(&fixture.context(), &overrides).unwrap().active.is_empty());
    assert!(detect(&fixture.context(), &BTreeMap::new()).is_err());
}

#[test]
fn conventions_and_override_directories_cannot_escape_the_workspace() {
    let fixture = Fixture::new();
    std::os::unix::fs::symlink("/etc/hosts", fixture.root.join("nx.json")).unwrap();
    assert!(detect(&fixture.context(), &BTreeMap::new()).is_err());
    fs::remove_file(fixture.root.join("nx.json")).unwrap();
    std::os::unix::fs::symlink("/etc", fixture.root.join("outside")).unwrap();
    let overrides = BTreeMap::from([(CapabilityId::Nx, CapabilityOverride { disabled: false, directory: Some(PathBuf::from("outside")) })]);
    assert!(detect(&fixture.context(), &overrides).is_err());
    for invalid in ["", "/tmp", "../outside", "a/../b", "./a", "a//b", "a/./b", "a/", "a\0b"] {
        assert!(validate_override_directory(invalid).is_err(), "{invalid:?}");
    }
    assert_eq!(validate_override_directory("tools/project").unwrap(), Path::new("tools/project"));
}

#[test]
fn a_convention_must_be_a_regular_file_and_missing_is_not_an_error() {
    let fixture = Fixture::new();
    assert!(!convention_file(&fixture.root, &fixture.root.join("nx.json")).unwrap());
    fs::create_dir(fixture.root.join("nx.json")).unwrap();
    assert!(convention_file(&fixture.root, &fixture.root.join("nx.json")).is_err());
}

fn merge_environment(
    first: (&'static str, EnvAction),
    second: (&'static str, EnvAction),
) -> Result<CapabilityContribution> {
    let mut output = CapabilityContribution::default();
    let mut owners = BTreeMap::new();
    for (id, (name, action)) in [(CapabilityId::Cargo, first), (CapabilityId::Go, second)] {
        merge(
            &mut output,
            &mut owners,
            id,
            CapabilityContribution {
                env: BTreeMap::from([(name, action)]),
                ..Default::default()
            },
        )?;
    }
    Ok(output)
}

#[test]
fn environment_conflicts_name_both_providers_and_equal_values_coalesce() {
    let error = merge_environment(
        ("TOOL_CACHE", EnvAction::Own("one".into())),
        ("TOOL_CACHE", EnvAction::Own("two".into())),
    )
    .unwrap_err()
    .to_string();
    for word in ["cargo", "go", "TOOL_CACHE"] {
        assert!(error.contains(word), "{error}");
    }
    let output = merge_environment(
        ("TOOL_CACHE", EnvAction::Unset),
        ("TOOL_CACHE", EnvAction::Unset),
    )
    .unwrap();
    assert_eq!(output.env.len(), 1);
    assert!(
        merge_environment(
            ("HOME", EnvAction::Own("host".into())),
            ("HOME", EnvAction::Unset)
        )
        .is_err()
    );
}

#[test]
fn conflicting_executables_and_cache_targets_fail_without_order_dependence() {
    for reverse in [false, true] {
        let sources = if reverse {
            ["two", "one"]
        } else {
            ["one", "two"]
        };
        for executable in [false, true] {
            let mut output = CapabilityContribution::default();
            let mut owners = BTreeMap::new();
            for (index, source) in sources.into_iter().enumerate() {
                let mut contribution = CapabilityContribution::default();
                if executable {
                    contribution.bootstrap_programs.push(BootstrapProgram {
                        name: "tool",
                        target: PathBuf::from(source),
                    });
                } else {
                    contribution.cache_mounts.push(CacheMount {
                        source: PathBuf::from(source),
                        private_target: Some(PathBuf::from("cache/tool")),
                    });
                }
                let result = merge(
                    &mut output,
                    &mut owners,
                    if index == 0 {
                        CapabilityId::Cargo
                    } else {
                        CapabilityId::Go
                    },
                    contribution,
                );
                if index == 0 {
                    result.unwrap();
                } else {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("cargo") && error.contains("go"), "{error}");
                }
            }
        }
    }
}

#[test]
fn normalization_coalesces_grants_without_broadening_literals() {
    let grant = |path: &str, scope, access| CapabilityGrant {
        path: path.into(),
        scope,
        access,
    };
    let mut contribution = CapabilityContribution {
        grants: vec![
            grant("/cache", GrantScope::Subtree, GrantAccess::ReadWrite),
            grant("/cache/child", GrantScope::Literal, GrantAccess::Read),
            grant("/cache", GrantScope::Subtree, GrantAccess::ReadWrite),
            grant("/other", GrantScope::Literal, GrantAccess::ReadWrite),
            grant("/other", GrantScope::Subtree, GrantAccess::Read),
        ],
        ..Default::default()
    };
    normalize(&mut contribution);
    assert_eq!(contribution.grants.len(), 3);
    assert!(
        contribution
            .grants
            .contains(&grant("/other", GrantScope::Subtree, GrantAccess::Read))
    );
}

#[test]
fn shared_caches_are_not_granted_before_host_links_are_provisioned() {
    static HOME: SharedToolHome = SharedToolHome {
        variable: Some("TEST_CACHE"),
        home: ".test-cache",
        linked_from_checkouts: false,
        layout: SharedLayout::Whole("test-cache"),
    };
    let fixture = Fixture::new();
    let unavailable = fixture.root.join("not-provisioned");
    let context = DetectionContext {
        caches_root: &unavailable,
        ..fixture.context()
    };
    let private = shared_tool_contribution(&context, &HOME).unwrap();
    assert_eq!(private.env.get("TEST_CACHE"), Some(&EnvAction::Unset));
    assert!(private.cache_mounts.is_empty() && private.grants.is_empty());
    let shared = HOME.links(&fixture.home, &fixture.caches).remove(0);
    fs::create_dir_all(&shared.shared).unwrap();
    std::os::unix::fs::symlink(&shared.shared, &shared.host).unwrap();
    let public = shared_tool_contribution(&fixture.context(), &HOME).unwrap();
    assert_eq!(
        public.env.get("TEST_CACHE"),
        Some(&EnvAction::Own(shared.host.into_os_string()))
    );
    assert_eq!(public.cache_mounts.len(), 1);
    assert_eq!(public.grants.len(), 1);
}
