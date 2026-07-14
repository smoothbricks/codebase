use std::path::{Path, PathBuf};

use cowshed_cli::launchd::{
    ExistingPlist, GATEWAY_LABEL, InstallState, LaunchAgentSpec, LaunchdError, Mutation,
    PRIVATE_DIRECTORY_MODE, PRIVATE_PLIST_MODE, ServiceLifecycle, plan_install, plan_remove,
};

const HOME: &str = "/Users/cowshed-test";
const EXECUTABLE: &str = "/nix/store/abc-cowshed/bin/cowshed";

fn gateway() -> LaunchAgentSpec {
    LaunchAgentSpec::gateway(Path::new(HOME), Path::new(EXECUTABLE)).unwrap()
}

#[test]
fn gateway_definition_has_exact_paths_argv_lifecycle_and_plist_bytes() {
    let spec = gateway();

    assert_eq!(spec.label(), GATEWAY_LABEL);
    assert_eq!(spec.executable(), Path::new(EXECUTABLE));
    assert_eq!(spec.arguments(), ["gateway", "run"]);
    assert_eq!(spec.lifecycle(), ServiceLifecycle::KeepAlive);
    assert_eq!(
        spec.plist_path(),
        Path::new("/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.gateway.plist")
    );
    assert_eq!(
        spec.launch_agents_directory(),
        Path::new("/Users/cowshed-test/Library/LaunchAgents")
    );
    assert_eq!(
        spec.standard_error_path(),
        Path::new("/Users/cowshed-test/.cowshed/telemetry/daemon-stderr.log")
    );
    assert_eq!(
        spec.program_arguments().collect::<Vec<_>>(),
        vec![EXECUTABLE, "gateway", "run"]
    );

    let expected = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
        "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
        "<plist version=\"1.0\">\n",
        "<dict>\n",
        "  <key>Label</key>\n",
        "  <string>dev.cowshed.gateway</string>\n",
        "  <key>ProgramArguments</key>\n",
        "  <array>\n",
        "    <string>/nix/store/abc-cowshed/bin/cowshed</string>\n",
        "    <string>gateway</string>\n",
        "    <string>run</string>\n",
        "  </array>\n",
        "  <key>RunAtLoad</key>\n",
        "  <true/>\n",
        "  <key>KeepAlive</key>\n",
        "  <true/>\n",
        "  <key>ProcessType</key>\n",
        "  <string>Background</string>\n",
        "  <key>StandardErrorPath</key>\n",
        "  <string>/Users/cowshed-test/.cowshed/telemetry/daemon-stderr.log</string>\n",
        "</dict>\n",
        "</plist>\n",
    );
    assert_eq!(spec.plist_bytes(), expected.as_bytes());
}

#[test]
fn generic_run_at_load_definition_is_immutable_and_escapes_plist_strings() {
    let spec = LaunchAgentSpec::new_user(
        Path::new("/Users/a&b"),
        "dev.cowshed.future",
        Path::new("/Applications/Cowshed & Tools/cowshed"),
        vec!["future".into(), "a<b".into()],
        ServiceLifecycle::RunAtLoad,
    )
    .unwrap();

    let plist = String::from_utf8(spec.plist_bytes()).unwrap();
    assert!(plist.contains("<string>/Applications/Cowshed &amp; Tools/cowshed</string>"));
    assert!(plist.contains("<string>a&lt;b</string>"));
    assert!(plist.contains("<key>KeepAlive</key>\n  <false/>"));
}

#[test]
fn new_install_plan_is_restrictive_and_atomically_replaces_the_plist() {
    let spec = gateway();
    let plan = plan_install(&spec, InstallState::default());
    let desired = spec.plist_bytes();

    assert_eq!(
        plan.operations(),
        [
            Mutation::EnsureDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::CreateExclusiveTemporaryFile {
                directory: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
                name_prefix: ".dev.cowshed.gateway.plist.".into(),
                bytes: desired,
                mode: PRIVATE_PLIST_MODE,
            },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile {
                destination: PathBuf::from(
                    "/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.gateway.plist"
                ),
            },
            Mutation::SyncDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
            },
        ]
    );
}

#[test]
fn current_install_is_a_noop_but_bad_permissions_are_repaired() {
    let spec = gateway();
    let desired = spec.plist_bytes();
    let current = InstallState {
        launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
        plist: Some(ExistingPlist {
            bytes: &desired,
            mode: PRIVATE_PLIST_MODE,
        }),
    };
    assert!(plan_install(&spec, current).is_noop());

    let wrong_plist_mode = InstallState {
        plist: Some(ExistingPlist {
            bytes: &desired,
            mode: 0o644,
        }),
        ..current
    };
    assert!(matches!(
        plan_install(&spec, wrong_plist_mode).operations(),
        [
            Mutation::CreateExclusiveTemporaryFile {
                mode: PRIVATE_PLIST_MODE,
                ..
            },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile { .. },
            Mutation::SyncDirectory { .. }
        ]
    ));

    let wrong_directory_mode = InstallState {
        launch_agents_directory_mode: Some(0o755),
        ..current
    };
    assert_eq!(
        plan_install(&spec, wrong_directory_mode).operations(),
        [
            Mutation::SetPermissions {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::SyncDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
            },
        ]
    );
}

#[test]
fn update_and_remove_plans_are_deterministic_and_filesystem_only() {
    let spec = gateway();
    let state = InstallState {
        launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
        plist: Some(ExistingPlist {
            bytes: b"stale plist",
            mode: PRIVATE_PLIST_MODE,
        }),
    };
    let first = plan_install(&spec, state);
    let second = plan_install(&spec, state);
    assert_eq!(first, second);
    assert!(matches!(
        first.operations(),
        [
            Mutation::CreateExclusiveTemporaryFile { .. },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile { .. },
            Mutation::SyncDirectory { .. }
        ]
    ));

    assert!(plan_remove(&spec, false).is_noop());
    assert_eq!(
        plan_remove(&spec, true).operations(),
        [
            Mutation::RemoveFile {
                path: PathBuf::from(
                    "/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.gateway.plist"
                ),
            },
            Mutation::SyncDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
            },
        ]
    );
}

#[test]
fn rejects_noncanonical_paths_empty_or_unsafe_inputs_and_provisioning() {
    let cases = [
        LaunchAgentSpec::gateway(Path::new("Users/me"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new("/Users/me/../other"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new("/Users/me/"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new(HOME), Path::new("bin/cowshed")),
        LaunchAgentSpec::gateway(Path::new(HOME), Path::new("/opt/./cowshed")),
        LaunchAgentSpec::gateway(Path::new("/"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new(HOME), Path::new("/")),
    ];
    for result in cases {
        assert!(matches!(result, Err(LaunchdError::InvalidPath { .. })));
    }

    for label in ["", ".dev.cowshed", "dev..cowshed", "dev/cowshed"] {
        assert_eq!(
            LaunchAgentSpec::new_user(
                Path::new(HOME),
                label,
                Path::new(EXECUTABLE),
                vec!["run".into()],
                ServiceLifecycle::RunAtLoad,
            ),
            Err(LaunchdError::InvalidLabel)
        );
    }

    assert!(matches!(
        LaunchAgentSpec::new_user(
            Path::new(HOME),
            "dev.cowshed.empty",
            Path::new(EXECUTABLE),
            Vec::new(),
            ServiceLifecycle::RunAtLoad,
        ),
        Err(LaunchdError::InvalidArgument { .. })
    ));
    assert!(matches!(
        LaunchAgentSpec::new_user(
            Path::new(HOME),
            "dev.cowshed.empty",
            Path::new(EXECUTABLE),
            vec!["run".into(), String::new()],
            ServiceLifecycle::RunAtLoad,
        ),
        Err(LaunchdError::InvalidArgument { index: 1, .. })
    ));
    assert_eq!(
        LaunchAgentSpec::new_user(
            Path::new(HOME),
            "dev.cowshed.provision",
            Path::new(EXECUTABLE),
            vec!["adopt".into()],
            ServiceLifecycle::KeepAlive,
        ),
        Err(LaunchdError::PrivilegedProvisioning)
    );
}
