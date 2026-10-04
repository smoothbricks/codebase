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
    let overrides = BTreeMap::from([(
        CapabilityId::Nx,
        CapabilityOverride {
            disabled: false,
            directory: Some(PathBuf::from("frontend")),
        },
    )]);
    assert!(
        detect(&fixture.context(), &overrides)
            .unwrap()
            .active
            .is_empty()
    );
    fixture.files(&["nx.json"]);
    assert!(
        detect(&fixture.context(), &overrides)
            .unwrap()
            .active
            .is_empty()
    );
    fixture.files(&["frontend/nx.json"]);
    let detected = detect(&fixture.context(), &overrides).unwrap();
    assert_eq!(detected.active, [CapabilityId::Nx]);
    assert_eq!(
        detected.contribution.env.get("NX_WORKSPACE_ROOT_PATH"),
        Some(&EnvAction::Own(
            fixture.root.join("frontend").into_os_string()
        ))
    );
    assert!(
        detected
            .contribution
            .daemon_isolation
            .discard_at_mint
            .contains(&PathBuf::from("frontend/.nx/workspace-data/d"))
    );
}

#[test]
fn a_disabled_capability_does_not_inspect_its_convention() {
    let fixture = Fixture::new();
    std::os::unix::fs::symlink("/etc/hosts", fixture.root.join("nx.json")).unwrap();
    let overrides = BTreeMap::from([(
        CapabilityId::Nx,
        CapabilityOverride {
            disabled: true,
            directory: None,
        },
    )]);
    assert!(
        detect(&fixture.context(), &overrides)
            .unwrap()
            .active
            .is_empty()
    );
    assert!(detect(&fixture.context(), &BTreeMap::new()).is_err());
}

#[test]
fn conventions_and_override_directories_cannot_escape_the_workspace() {
    let fixture = Fixture::new();
    std::os::unix::fs::symlink("/etc/hosts", fixture.root.join("nx.json")).unwrap();
    assert!(detect(&fixture.context(), &BTreeMap::new()).is_err());
    fs::remove_file(fixture.root.join("nx.json")).unwrap();
    std::os::unix::fs::symlink("/etc", fixture.root.join("outside")).unwrap();
    let overrides = BTreeMap::from([(
        CapabilityId::Nx,
        CapabilityOverride {
            disabled: false,
            directory: Some(PathBuf::from("outside")),
        },
    )]);
    assert!(detect(&fixture.context(), &overrides).is_err());
    for invalid in [
        "",
        "/tmp",
        "../outside",
        "a/../b",
        "./a",
        "a//b",
        "a/./b",
        "a/",
        "a\0b",
    ] {
        assert!(validate_override_directory(invalid).is_err(), "{invalid:?}");
    }
    assert_eq!(
        validate_override_directory("tools/project").unwrap(),
        Path::new("tools/project")
    );
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
                    contribution.shared_caches.push(SharedCache {
                        path: PathBuf::from(source),
                        private_link: Some(PathBuf::from("cache/tool")),
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
fn overlapping_build_state_contributions_are_refused_and_identical_ones_coalesce() {
    let path = |checkout, volume| BuildStatePath::new(checkout, volume).unwrap();
    for conflicting in [
        path("target/debug", "other"),
        path("other", "target/debug"),
        path("target", "different"),
        path("different", "target"),
    ] {
        for reverse in [false, true] {
            let mut paths = vec![path("target", "target"), conflicting.clone()];
            if reverse {
                paths.reverse();
            }
            let mut output = CapabilityContribution::default();
            let first = paths.remove(0);
            merge_build_state(&mut output.build_state, vec![first]).unwrap();
            assert!(merge_build_state(&mut output.build_state, paths).is_err());
        }
    }
    let mut output = CapabilityContribution::default();
    merge_build_state(
        &mut output.build_state,
        vec![path("target", "target"), path("target", "target")],
    )
    .unwrap();
    normalize(&mut output);
    assert_eq!(output.build_state, vec![path("target", "target")]);
}

/// A shared tool home names the host's own directories: the cache directories as read-write
/// shared caches, a split root as a literal read beside its state files, and the variable as the
/// host path a caller's own value cannot replace.
#[test]
fn a_shared_tool_home_is_carved_back_at_the_host_path() {
    static WHOLE: SharedToolHome = SharedToolHome {
        variable: Some("TEST_CACHE"),
        home: ".test-cache",
        layout: SharedLayout::Whole,
    };
    static SPLIT: SharedToolHome = SharedToolHome {
        variable: None,
        home: ".tool",
        layout: SharedLayout::Split {
            caches: &["cache"],
            state_files: &[".lock"],
        },
    };
    let fixture = Fixture::new();
    let mut contribution = CapabilityContribution::default();
    add_shared_tool_home(&mut contribution, &fixture.home, &WHOLE);
    add_shared_tool_home(&mut contribution, &fixture.home, &SPLIT);
    let whole = fixture.home.join(".test-cache");
    let split = fixture.home.join(".tool");
    assert_eq!(
        contribution.env,
        BTreeMap::from([("TEST_CACHE", EnvAction::Own(whole.clone().into()))])
    );
    assert_eq!(
        contribution.shared_caches,
        vec![
            SharedCache {
                path: whole,
                private_link: None,
            },
            SharedCache {
                path: split.join("cache"),
                private_link: None,
            },
        ]
    );
    assert_eq!(
        contribution.grants,
        vec![
            CapabilityGrant {
                path: split.clone(),
                scope: GrantScope::Literal,
                access: GrantAccess::Read,
            },
            CapabilityGrant {
                path: split.join(".lock"),
                scope: GrantScope::Literal,
                access: GrantAccess::ReadWrite,
            },
        ]
    );
}

/// `[caches] home` entries from main's configuration are shared where the host keeps them and
/// linked from the private HOME, so `$HOME/<path>` reaches the same bytes in every sandbox.
#[test]
fn repository_caches_are_shared_and_linked_from_the_private_home() {
    let fixture = Fixture::new();
    let declared = [PathBuf::from(".cache/ttsc")];
    let context = DetectionContext {
        repository_caches: &declared,
        ..fixture.context()
    };
    let detected = detect(&context, &BTreeMap::new()).unwrap();
    assert!(detected.active.is_empty());
    assert_eq!(
        detected.contribution.shared_caches,
        vec![SharedCache {
            path: fixture.home.join(".cache/ttsc"),
            private_link: Some(fixture.environment.join("home/.cache/ttsc")),
        }]
    );
}
