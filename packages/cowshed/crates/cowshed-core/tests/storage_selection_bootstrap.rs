use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::ThreadId;

use async_trait::async_trait;
use cowshed_core::storage::bootstrap::*;
use cowshed_core::storage::fstab::FstabPin;
use proptest::prelude::*;

fn apfs_evidence() -> StatFsEvidence {
    StatFsEvidence::Apfs {
        mount_source: PathBuf::from("/dev/disk3s1"),
        container: Some("disk3".to_owned()),
    }
}

fn explicit_zfs(pool: &str) -> SubstrateConfig {
    parse_cowshed_config(&format!("[substrate]\nkind = \"zfs\"\npool = \"{pool}\"\n"))
        .unwrap()
        .substrate()
        .unwrap()
        .clone()
}

fn absent_apfs_storage() -> BootstrapEvidence {
    BootstrapEvidence::Apfs {
        store: ExistingStorage::Absent,
        caches: ExistingStorage::Absent,
    }
}

fn absent_zfs_storage() -> BootstrapEvidence {
    BootstrapEvidence::Zfs {
        root: ExistingStorage::Absent,
        store: ExistingStorage::Absent,
        caches: ExistingStorage::Absent,
        projects: Box::new(ExistingStorage::Absent),
    }
}

fn mounted_apfs_storage() -> BootstrapEvidence {
    BootstrapEvidence::Apfs {
        store: ExistingStorage::mounted_valid("disk8s7"),
        caches: ExistingStorage::mounted_valid("disk8s8"),
    }
}

fn mounted_zfs_storage(pool: &str) -> BootstrapEvidence {
    BootstrapEvidence::Zfs {
        root: ExistingStorage::mounted_valid(format!("{pool}/cowshed")),
        store: ExistingStorage::mounted_valid(format!("{pool}/cowshed/store")),
        caches: ExistingStorage::mounted_valid(format!("{pool}/cowshed/caches")),
        projects: Box::new(ExistingStorage::existing_unmounted(
            format!("{pool}/cowshed/projects"),
            VolumeMarker::new(VolumeRole::Projects, SubstrateKind::Zfs),
        )),
    }
}

fn selected_zfs(pool: &str) -> SelectedSubstrate {
    select_substrate(
        StatFsEvidence::Other {
            fs_type: "ext4".to_owned(),
        },
        Some(&explicit_zfs(pool)),
    )
    .unwrap()
}

fn mutates_host(operation: &HostOperation) -> bool {
    matches!(
        operation,
        HostOperation::EnsureDirectory(_)
            | HostOperation::ReclaimMountpoint(_)
            | HostOperation::MountApfsVolume { .. }
            | HostOperation::ProvisionApfsVolumes { .. }
            | HostOperation::RunCommand(_)
            | HostOperation::WriteMarkerAtomic { .. }
    )
}

fn command_line(operation: &HostOperation) -> Option<String> {
    match operation {
        HostOperation::RunCommand(command) => Some(
            std::iter::once(command.program().to_owned())
                .chain(command.args().iter().cloned())
                .collect::<Vec<_>>()
                .join(" "),
        ),
        HostOperation::MountApfsVolume { mountpoint, volume } => {
            let VolumeRef::ExistingExact(identifier) = volume;
            Some(format!(
                "/usr/sbin/diskutil mount -nobrowse -mountPoint {} {identifier}",
                mountpoint.display()
            ))
        }
        _ => None,
    }
}

#[test]
fn cowshed_config_parser_accepts_only_complete_known_sections() {
    let config = parse_cowshed_config(
        "# project settings\n[substrate] # deliberate override\nkind = \"zfs\"\npool = \"tank\" # no scan\n",
    )
    .unwrap();
    assert_eq!(config.substrate().unwrap().pool(), "tank");

    assert_eq!(
        parse_cowshed_config("[repository]\nname = \"widget\"\n").unwrap_err(),
        ConfigError::UnknownSection("repository".to_owned())
    );
    // Cowshed has no devenv backend: a project activates devenv through its own `.envrc`.
    assert_eq!(
        parse_cowshed_config("[devenv]\ndir = \"tooling/devenv\"\n").unwrap_err(),
        ConfigError::UnknownSection("devenv".to_owned())
    );
    let invalid = [
        (
            "[substrate]\npool = \"tank\"\n",
            "missing [substrate] key \"kind\"",
        ),
        (
            "[substrate]\nkind = \"zfs\"\n",
            "missing [substrate] key \"pool\"",
        ),
        (
            "[substrate]\nkind = \"apfs\"\npool = \"tank\"\n",
            "unsupported substrate kind",
        ),
        (
            "[substrate]\nkind = \"zfs\"\npool = \"tank/child\"\n",
            "invalid ZFS pool",
        ),
        (
            "[substrate]\nkind = \"zfs\"\npool = \"tank\"\nextra = \"x\"\n",
            "unknown [substrate] key",
        ),
        (
            "[substrate]\nkind = \"zfs\"\nkind = \"zfs\"\npool = \"tank\"\n",
            "duplicated",
        ),
        (
            "[substrate]\nkind = \"zfs\"\npool = tank\n",
            "quoted string",
        ),
        (
            "[substrate]\nkind = \"zfs\"\npool = \"tank\"\n[substrate]\nkind = \"zfs\"\npool = \"tank\"\n",
            "duplicated",
        ),
    ];
    for (source, message) in invalid {
        let error = parse_cowshed_config(source).unwrap_err();
        assert!(error.to_string().contains(message), "{source:?}: {error}");
    }
    assert_eq!(
        parse_cowshed_config("[substrate]\nmalformed\n").unwrap_err(),
        ConfigError::MalformedLine { line: 2 }
    );
    for source in [
        "[substrate]\nkind = \"zfs\npool = \"tank\"\n",
        "[substrate]\nkind = zfs\"\npool = \"tank\"\n",
    ] {
        assert!(matches!(
            parse_cowshed_config(source),
            Err(ConfigError::ExpectedQuotedString {
                section: "substrate",
                line: 2
            })
        ));
    }
    assert_eq!(
        parse_cowshed_config("[substrate]\nkind = \"z#fs\" # outside comment\npool = \"tank\"\n")
            .unwrap_err(),
        ConfigError::UnsupportedKind("z#fs".to_owned())
    );
    assert_eq!(
        parse_cowshed_config(
            r##"[substrate]
kind = "z\"#fs" # outside comment
pool = "tank"
"##,
        )
        .unwrap_err(),
        ConfigError::UnsupportedKind("z\"#fs".to_owned())
    );
    for pool in ["1tank", "-tank"] {
        assert!(matches!(
            parse_cowshed_config(&format!("[substrate]\nkind = \"zfs\"\npool = \"{pool}\"\n")),
            Err(ConfigError::InvalidPool(PoolNameError::InvalidPool))
        ));
    }
}

/// `land` runs nothing in its target, so `.cowshed.toml` has no `[land]` section: a file that
/// still declares one is refused, never read as if its step ran.
#[test]
fn a_land_section_is_refused() {
    let error = parse_cowshed_config("[land]\nwarm = [\"tooling/warm-main\"]\n").unwrap_err();
    assert_eq!(error, ConfigError::UnknownSection("land".to_owned()));
    assert_eq!(error.to_string(), "unknown configuration section [land]");
}

/// `[sandbox] deny` is a one-line array of workspace-relative paths, normalized, sorted and
/// deduplicated; anything that could name a path outside the workspace is refused.
#[test]
fn sandbox_deny_is_an_array_of_workspace_relative_paths() {
    let config = parse_cowshed_config(
        "# operator-owned denies\n[sandbox]\n# runtime secrets\ndeny = [\".runtime\", \"a/./b\", \".runtime\"] # trailing\n",
    )
    .unwrap();
    assert_eq!(
        config.sandbox_deny(),
        [PathBuf::from(".runtime"), PathBuf::from("a/b")]
    );
    assert!(
        parse_cowshed_config("[substrate]\nkind = \"zfs\"\npool = \"tank\"\n")
            .unwrap()
            .sandbox_deny()
            .is_empty()
    );
    assert!(
        parse_cowshed_config("[sandbox]\ndeny = []\n")
            .unwrap()
            .sandbox_deny()
            .is_empty()
    );

    let invalid = [
        ("[sandbox]\n", "missing [sandbox] key \"deny\""),
        ("[sandbox]\ndeny = \".runtime\"\n", "array of quoted paths"),
        ("[sandbox]\ndeny = [1]\n", "array of quoted paths"),
        ("[sandbox]\ndeny = [\"\"]\n", "workspace-relative"),
        ("[sandbox]\ndeny = [\"/etc\"]\n", "workspace-relative"),
        ("[sandbox]\ndeny = [\"../main\"]\n", "workspace-relative"),
        ("[sandbox]\ndeny = [\"a/../../b\"]\n", "workspace-relative"),
        ("[sandbox]\ndeny = [\"a\"]\ndeny = [\"b\"]\n", "duplicated"),
        ("[sandbox]\nread = [\"a\"]\n", "unknown [sandbox] key"),
    ];
    for (source, message) in invalid {
        let error = parse_cowshed_config(source).unwrap_err();
        assert!(error.to_string().contains(message), "{source:?}: {error}");
    }
}

/// `[build] capacity` sets the capacity of a build volume created from nothing; without it a
/// project gets 100 GiB. The value is an image capacity, so it refuses what an image refuses.
#[test]
fn build_capacity_defaults_to_100_gibibytes_and_is_an_image_capacity() {
    use cowshed_core::metadata::ImageCapacity;
    assert_eq!(
        parse_cowshed_config("").unwrap().build_capacity(),
        ImageCapacity::from_gibibytes(100)
    );
    assert_eq!(
        parse_cowshed_config("[build]\ncapacity = \"250g\" # cap on build-cache growth\n")
            .unwrap()
            .build_capacity(),
        ImageCapacity::from_gibibytes(250)
    );
    let invalid = [
        ("[build]\n", "the [build] section sets nothing"),
        ("[build]\ncapacity = 100\n", "must be a quoted string"),
        (
            "[build]\ncapacity = \"lots\"\n",
            "[build] capacity at line 2",
        ),
        ("[build]\ncapacity = \"1k\"\n", "[build] capacity at line 2"),
        (
            "[build]\ncapacity = \"1g\"\ncapacity = \"2g\"\n",
            "duplicated",
        ),
        ("[build]\ncapacity = \"1g\"\n[build]\n", "duplicated"),
        ("[build]\nsize = \"1g\"\n", "unknown [build] key"),
    ];
    for (source, message) in invalid {
        let error = parse_cowshed_config(source).unwrap_err();
        assert!(error.to_string().contains(message), "{source:?}: {error}");
    }
}

/// `[caches] home` reuses `[sandbox] deny`'s parsing: a one-line array of HOME-relative paths of
/// plain components, normalized, sorted and deduplicated; an empty, absolute or escaping entry is
/// refused, and so is a section without its key.
#[test]
fn caches_home_is_an_array_of_home_relative_paths() {
    let config = parse_cowshed_config(
        "[caches]\n# the TypeScript plugin cache\nhome = [\".cache/ttsc\", \"Library/Caches/./tool\", \".cache/ttsc\"]\n",
    )
    .unwrap();
    assert_eq!(
        config.caches_home(),
        [
            PathBuf::from(".cache/ttsc"),
            PathBuf::from("Library/Caches/tool")
        ]
    );
    assert!(config.sandbox_deny().is_empty());
    assert!(
        parse_cowshed_config("[sandbox]\ndeny = [\".runtime\"]\n")
            .unwrap()
            .caches_home()
            .is_empty()
    );
    assert!(
        parse_cowshed_config("[caches]\nhome = []\n")
            .unwrap()
            .caches_home()
            .is_empty()
    );

    let invalid = [
        ("[caches]\n", "missing [caches] key \"home\""),
        (
            "[caches]\nhome = \".cache/ttsc\"\n",
            "[caches] home at line 2 must be an array of quoted paths",
        ),
        ("[caches]\nhome = [7]\n", "array of quoted paths"),
        ("[caches]\nhome = [\"\"]\n", "HOME-relative"),
        ("[caches]\nhome = [\"/etc\"]\n", "HOME-relative"),
        ("[caches]\nhome = [\"..\"]\n", "HOME-relative"),
        ("[caches]\nhome = [\"../other-user\"]\n", "HOME-relative"),
        (
            "[caches]\nhome = [\".cache/../../escape\"]\n",
            "HOME-relative",
        ),
        ("[caches]\nhome = [\"a\"]\nhome = [\"b\"]\n", "duplicated"),
        ("[caches]\n[caches]\n", "duplicated"),
        ("[caches]\nshared = [\"a\"]\n", "unknown [caches] key"),
    ];
    for (source, message) in invalid {
        let error = parse_cowshed_config(source).unwrap_err();
        assert!(error.to_string().contains(message), "{source:?}: {error}");
    }
}

/// `[build] state` declares build state no capability detects: checkout-relative, normalized,
/// never Git's or cowshed's own, and never one path inside another. The volume keeps each under
/// its own `declared/` namespace.
#[test]
fn build_state_declares_disjoint_checkout_relative_paths() {
    use cowshed_core::capabilities::{BuildStatePath, DeclaredState};
    assert_eq!(parse_cowshed_config("").unwrap().build_state(), []);
    let config = parse_cowshed_config(
        "[build]\nstate = [\"vendor/upstream\", \"tools/.pin\", \"packages/*/.cache/lmao\", \"vendor/upstream\"] # rebuildable\n",
    )
    .unwrap();
    assert_eq!(
        config.build_state(),
        [
            DeclaredState::Path(BuildStatePath::declared("tools/.pin").unwrap()),
            DeclaredState::Path(BuildStatePath::declared("vendor/upstream").unwrap()),
            DeclaredState::Pattern {
                selector: "packages/*".into(),
                state: ".cache/lmao".into(),
            },
        ]
    );
    assert_eq!(
        BuildStatePath::declared("tools/.pin")
            .unwrap()
            .volume
            .as_path(),
        Path::new("declared/tools/.pin")
    );
    let invalid = [
        ("[build]\nstate = \"a\"\n", "must be an array of quoted"),
        (
            "[build]\nstate = [\"/abs\"]\n",
            "normalized checkout-relative",
        ),
        (
            "[build]\nstate = [\"a/../b\"]\n",
            "normalized checkout-relative",
        ),
        (
            "[build]\nstate = [\"sub/.git\"]\n",
            "must not name Git metadata",
        ),
        (
            "[build]\nstate = [\".cowshed/x\"]\n",
            "must not be inside cowshed's own",
        ),
        (
            "[build]\nstate = [\"a\", \"a/b\"]\n",
            "declares a/b inside a; declare only the outer path",
        ),
        ("[build]\nstate = []\nstate = []\n", "duplicated"),
        (
            "[build]\nstate = [\"packages/*\"]\n",
            "must end in a literal name",
        ),
        (
            "[build]\nstate = [\"packages/*/.cache\", \"packages/*/.cache/lmao\"]\n",
            "declares packages/*/.cache/lmao inside packages/*/.cache",
        ),
    ];
    for (source, message) in invalid {
        let error = parse_cowshed_config(source).unwrap_err();
        assert!(error.to_string().contains(message), "{source:?}: {error}");
    }
}

/// Main's `.cowshed.toml` is the only source of `[sandbox] deny` and `[caches] home`: an absent
/// file declares nothing, and an invalid one refuses rather than run without the deny.
#[test]
fn main_cowshed_config_reads_main_and_refuses_an_invalid_file() {
    let main = std::env::temp_dir().join(format!(
        "cowshed-main-config-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&main).unwrap();
    let absent = main_cowshed_config(&main).unwrap();
    std::fs::write(
        main.join(".cowshed.toml"),
        "[sandbox]\n# runtime secrets live here\ndeny = [\".runtime\"]\n[caches]\nhome = [\".cache/ttsc\"]\n",
    )
    .unwrap();
    let declared = main_cowshed_config(&main).unwrap();
    std::fs::write(main.join(".cowshed.toml"), "[caches]\nhome = [\"../x\"]\n").unwrap();
    let invalid = main_cowshed_config(&main).expect_err("escaping cache refused");
    std::fs::remove_dir_all(&main).unwrap();

    assert!(absent.sandbox_deny().is_empty() && absent.caches_home().is_empty());
    assert_eq!(declared.sandbox_deny(), [PathBuf::from(".runtime")]);
    assert_eq!(declared.caches_home(), [PathBuf::from(".cache/ttsc")]);
    assert_eq!(invalid.code.as_str(), "usage");
}
#[test]
fn capability_overrides_are_strict_convention_overrides() {
    use cowshed_core::capabilities::CapabilityId;
    for id in CapabilityId::ALL {
        let config = parse_cowshed_config(&format!(
            "[{}]\ndisabled = true\ndirectory = \"tools/project\"\n",
            id.section_name(),
        ))
        .unwrap();
        let override_ = config.capabilities().get(&id).unwrap();
        assert!(override_.disabled);
        assert_eq!(
            override_.directory.as_deref(),
            Some(Path::new("tools/project"))
        );
    }
    let config = parse_cowshed_config("[capabilities.nx]\n").unwrap();
    let override_ = config.capabilities().get(&CapabilityId::Nx).unwrap();
    assert!(!override_.disabled);
    assert!(override_.directory.is_none());
    for source in [
        "[capabilities.missing]\n",
        "[capabilities.nx]\nenabled = true\n",
        "[capabilities.nx]\ndisabled = \"true\"\n",
        "[capabilities.nx]\ndisabled = 1\n",
        "[capabilities.nx]\ndisabled = false\ndisabled = true\n",
        "[capabilities.nx]\ndirectory = \"tools\"\ndirectory = \"other\"\n",
        "[capabilities.nx]\n[capabilities.nx]\n",
        "[capabilities.nx]\ndirectory = \"../outside\"\n",
        "[capabilities.nx]\ndirectory = \"/outside\"\n",
        "[capabilities.nx]\ndirectory = \"\"\n",
        "[capabilities.nx]\ndirectory = \"a//b\"\n",
    ] {
        assert!(parse_cowshed_config(source).is_err(), "{source:?}");
    }
}

#[test]
fn selection_matrix_uses_evidence_without_guessing() {
    let apfs = select_substrate(apfs_evidence(), None).unwrap();
    assert!(matches!(
        apfs,
        SelectedSubstrate::Apfs { ref container, .. } if container == "disk3"
    ));
    assert!(matches!(
        apfs.evidence(),
        [SelectionEvidence::ApfsStatFs { mount_source, container }]
            if mount_source == Path::new("/dev/disk3s1") && container == "disk3"
    ));

    assert!(matches!(
        select_substrate(
            StatFsEvidence::Apfs {
                mount_source: PathBuf::from("/dev/disk3s1"),
                container: None,
            },
            None,
        ),
        Err(SelectionError::MissingApfsContainer)
    ));
    for container in ["", "   "] {
        assert_eq!(
            select_substrate(
                StatFsEvidence::Apfs {
                    mount_source: PathBuf::from("/dev/disk3s1"),
                    container: Some(container.to_owned()),
                },
                None,
            )
            .unwrap_err(),
            SelectionError::MissingApfsContainer
        );
    }

    let delegated = DelegatedZfsDataset::new("tank/home", true).unwrap();
    let zfs = select_substrate(
        StatFsEvidence::Zfs {
            containing_dataset: Some(delegated),
        },
        None,
    )
    .unwrap();
    assert!(matches!(
        zfs,
        SelectedSubstrate::Zfs { ref pool, .. } if pool == "tank"
    ));
    assert!(matches!(
        zfs.evidence(),
        [SelectionEvidence::DelegatedContainingDataset { dataset, pool }]
            if dataset == "tank/home" && pool == "tank"
    ));

    for insufficient in [
        StatFsEvidence::Zfs {
            containing_dataset: None,
        },
        StatFsEvidence::Zfs {
            containing_dataset: Some(DelegatedZfsDataset::new("tank/home", false).unwrap()),
        },
        StatFsEvidence::Other {
            fs_type: "ext4".to_owned(),
        },
    ] {
        assert_eq!(
            select_substrate(insufficient, None).unwrap_err(),
            SelectionError::ExplicitZfsRequired
        );
    }
}

#[test]
fn explicit_zfs_is_exact_and_conflicting_evidence_is_ambiguous() {
    let configured = explicit_zfs("work");
    let selected = select_substrate(
        StatFsEvidence::Other {
            fs_type: "ext4".to_owned(),
        },
        Some(&configured),
    )
    .unwrap();
    assert!(matches!(
        selected,
        SelectedSubstrate::Zfs { ref pool, .. } if pool == "work"
    ));
    assert_eq!(
        selected.evidence(),
        [SelectionEvidence::ExplicitConfig {
            pool: "work".to_owned()
        }]
    );

    let same_pool = select_substrate(
        StatFsEvidence::Zfs {
            containing_dataset: Some(DelegatedZfsDataset::new("work/home", true).unwrap()),
        },
        Some(&configured),
    )
    .unwrap();
    assert_eq!(same_pool.evidence().len(), 2);

    assert_eq!(
        select_substrate(
            StatFsEvidence::Zfs {
                containing_dataset: Some(DelegatedZfsDataset::new("other/home", true).unwrap()),
            },
            Some(&configured),
        )
        .unwrap_err(),
        SelectionError::AmbiguousPools {
            configured: "work".to_owned(),
            containing: "other".to_owned(),
        }
    );
}

/// A fresh APFS host gets the store alone: the caches volume is retired and never created.
#[test]
fn apfs_plan_has_exact_roots_commands_markers_and_store_first_order() {
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    let plan = plan_bootstrap(selected, Path::new("/Users/alice"), absent_apfs_storage()).unwrap();
    assert_eq!(plan.home(), Path::new("/Users/alice"));
    assert_eq!(plan.roots().store(), Path::new("/private/cowshed/store"));

    assert!(matches!(
        &plan.operations()[0],
        HostOperation::GuardMountpoint { path, role: VolumeRole::Store, substrate: SubstrateKind::Apfs }
            if path == Path::new("/private/cowshed/store")
    ));
    let HostOperation::ProvisionApfsVolumes { container, volumes } = &plan.operations()[1] else {
        panic!("absent APFS storage must use one provisioning batch");
    };
    assert_eq!(plan.operations().len(), 2);
    assert_eq!(container, "disk3");
    assert_eq!(volumes.len(), 1);
    assert_eq!(volumes[0].name(), APFS_STORE_VOLUME);
    assert_eq!(volumes[0].mountpoint(), Path::new("/private/cowshed/store"));
    assert_eq!(volumes[0].role(), VolumeRole::Store);
    assert!(matches!(volumes[0].kind(), ApfsProvisionKind::Create));
}

#[test]
fn zfs_plan_has_exact_fixed_sibling_hierarchy_and_store_first_order() {
    let configured = explicit_zfs("tank");
    let selected = select_substrate(
        StatFsEvidence::Other {
            fs_type: "ext4".to_owned(),
        },
        Some(&configured),
    )
    .unwrap();
    let plan = plan_bootstrap(selected, Path::new("/home/alice"), absent_zfs_storage()).unwrap();

    assert!(matches!(
        &plan.operations()[0],
        HostOperation::VerifyZfsDelegation { pool, required_root }
            if pool == "tank" && required_root == "tank/cowshed"
    ));
    let commands: Vec<_> = plan.operations().iter().filter_map(command_line).collect();
    assert_eq!(
        commands,
        [
            "/usr/sbin/zfs create -o mountpoint=none tank/cowshed",
            "/usr/sbin/zfs create -o mountpoint=/private/cowshed/store tank/cowshed/store",
            "/usr/sbin/zfs set org.cowshed:version=1 org.cowshed:role=store tank/cowshed/store",
            "/usr/sbin/zfs create -o mountpoint=none tank/cowshed/projects",
            "/usr/sbin/zfs set org.cowshed:version=1 org.cowshed:role=projects tank/cowshed/projects",
        ]
    );
    assert!(commands.iter().all(|command| !command.contains("zpool")));
    // The caches dataset is retired: a fresh pool never gets one.
    assert!(commands.iter().all(|command| !command.contains("caches")));

    let store_marker = plan
        .operations()
        .iter()
        .position(|operation| matches!(operation, HostOperation::WriteMarkerAtomic { marker, .. } if marker.role() == VolumeRole::Store))
        .unwrap();
    let projects_create = plan
        .operations()
        .iter()
        .position(|operation| {
            command_line(operation).is_some_and(|command| {
                command == "/usr/sbin/zfs create -o mountpoint=none tank/cowshed/projects"
            })
        })
        .unwrap();
    assert!(store_marker < projects_create);
}
#[test]
fn create_or_heal_uses_only_exact_existing_storage_evidence() {
    let apfs = select_substrate(apfs_evidence(), None).unwrap();
    let first = plan_bootstrap(
        apfs.clone(),
        Path::new("/Users/alice"),
        absent_apfs_storage(),
    )
    .unwrap();
    let batches: Vec<_> = first
        .operations()
        .iter()
        .filter_map(|operation| match operation {
            HostOperation::ProvisionApfsVolumes { volumes, .. } => Some(volumes),
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 1);

    let repeated = plan_bootstrap(
        apfs.clone(),
        Path::new("/Users/alice"),
        mounted_apfs_storage(),
    )
    .unwrap();
    assert!(
        repeated
            .operations()
            .iter()
            .all(|operation| !mutates_host(operation))
    );
    assert_eq!(repeated.operations().len(), 2);

    let healed = plan_bootstrap(
        apfs,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::existing_unmounted(
                "disk9s11",
                VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs),
            ),
            caches: ExistingStorage::existing_unmounted(
                "disk9s12",
                VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Apfs),
            ),
        },
    )
    .unwrap();
    let healed_commands: Vec<_> = healed
        .operations()
        .iter()
        .filter_map(command_line)
        .collect();
    assert_eq!(
        healed_commands,
        [
            "/usr/sbin/diskutil mount -nobrowse -mountPoint /private/cowshed/store disk9s11",
            "/usr/sbin/diskutil mount -nobrowse -mountPoint /private/cowshed/caches disk9s12",
        ]
    );
    assert!(
        healed_commands
            .iter()
            .all(|command| !command.contains("addVolume"))
    );

    let zfs = selected_zfs("tank");
    let repeated = plan_bootstrap(
        zfs.clone(),
        Path::new("/home/alice"),
        mounted_zfs_storage("tank"),
    )
    .unwrap();
    assert!(
        repeated
            .operations()
            .iter()
            .all(|operation| !mutates_host(operation))
    );
    assert!(matches!(
        repeated.operations(),
        [
            HostOperation::VerifyZfsDelegation { .. },
            HostOperation::GuardMountpoint {
                role: VolumeRole::Store,
                ..
            },
            HostOperation::GuardMountpoint {
                role: VolumeRole::Caches,
                ..
            }
        ]
    ));

    let healed = plan_bootstrap(
        zfs,
        Path::new("/home/alice"),
        BootstrapEvidence::Zfs {
            root: ExistingStorage::mounted_valid("tank/cowshed"),
            store: ExistingStorage::existing_unmounted(
                "tank/cowshed/store",
                VolumeMarker::new(VolumeRole::Store, SubstrateKind::Zfs),
            ),
            caches: ExistingStorage::existing_unmounted(
                "tank/cowshed/caches",
                VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Zfs),
            ),
            projects: Box::new(ExistingStorage::existing_unmounted(
                "tank/cowshed/projects",
                VolumeMarker::new(VolumeRole::Projects, SubstrateKind::Zfs),
            )),
        },
    )
    .unwrap();
    let healed_commands: Vec<_> = healed
        .operations()
        .iter()
        .filter_map(command_line)
        .collect();
    assert_eq!(
        healed_commands,
        [
            "/usr/sbin/zfs set mountpoint=/private/cowshed/store tank/cowshed/store",
            "/usr/sbin/zfs mount tank/cowshed/store",
            "/usr/sbin/zfs set mountpoint=/private/cowshed/caches tank/cowshed/caches",
            "/usr/sbin/zfs mount tank/cowshed/caches",
        ]
    );
    assert!(
        healed_commands
            .iter()
            .all(|command| !command.contains(" create "))
    );
    let first_mutation = healed.operations().iter().position(mutates_host).unwrap();
    assert!(
        healed.operations()[..first_mutation]
            .iter()
            .all(|operation| {
                matches!(
                    operation,
                    HostOperation::VerifyZfsDelegation { .. }
                        | HostOperation::GuardMountpoint { .. }
                )
            })
    );
    assert_eq!(
        healed.operations()[..first_mutation]
            .iter()
            .filter(|operation| matches!(operation, HostOperation::GuardMountpoint { .. }))
            .count(),
        1
    );
}

#[test]
fn bootstrap_evidence_must_match_the_selected_exact_storage() {
    let apfs = select_substrate(apfs_evidence(), None).unwrap();
    assert!(matches!(
        plan_bootstrap(apfs, Path::new("/Users/alice"), absent_zfs_storage()),
        Err(PlanError::EvidenceSubstrateMismatch {
            selected: SubstrateKind::Apfs,
            evidence: SubstrateKind::Zfs,
        })
    ));

    let zfs = selected_zfs("tank");
    assert!(matches!(
        plan_bootstrap(zfs.clone(), Path::new("/home/alice"), absent_apfs_storage()),
        Err(PlanError::EvidenceSubstrateMismatch {
            selected: SubstrateKind::Zfs,
            evidence: SubstrateKind::Apfs,
        })
    ));
    assert!(matches!(
        plan_bootstrap(
            zfs,
            Path::new("/home/alice"),
            BootstrapEvidence::Zfs {
                root: ExistingStorage::mounted_valid("other/cowshed"),
                store: ExistingStorage::Absent,
                caches: ExistingStorage::Absent,
                projects: Box::new(ExistingStorage::Absent),
            }
        ),
        Err(PlanError::UnexpectedStorageIdentifier { expected, actual })
            if expected == "tank/cowshed" && actual == "other/cowshed"
    ));
}
#[test]
fn unmounted_existing_storage_requires_a_current_exact_marker() {
    let apfs = select_substrate(apfs_evidence(), None).unwrap();
    let zfs = selected_zfs("tank");
    for invalid in [
        ExistingMarkerEvidence::Missing,
        ExistingMarkerEvidence::Invalid,
        ExistingMarkerEvidence::UnsupportedVersion(2),
        ExistingMarkerEvidence::Valid(VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Apfs)),
    ] {
        assert!(matches!(
            plan_bootstrap(
                apfs.clone(),
                Path::new("/Users/alice"),
                BootstrapEvidence::Apfs {
                    store: ExistingStorage::ExistingUnmounted {
                        exact_identifier: "disk9s11".to_owned(),
                        marker: invalid,
                    },
                    caches: ExistingStorage::Absent,
                },
            ),
            Err(PlanError::InvalidExistingStorageMarker {
                expected_role: VolumeRole::Store,
                expected_substrate: SubstrateKind::Apfs,
                ..
            })
        ));
    }

    for invalid in [
        ExistingMarkerEvidence::Missing,
        ExistingMarkerEvidence::Invalid,
        ExistingMarkerEvidence::UnsupportedVersion(2),
        ExistingMarkerEvidence::Valid(VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Zfs)),
    ] {
        assert!(matches!(
            plan_bootstrap(
                zfs.clone(),
                Path::new("/home/alice"),
                BootstrapEvidence::Zfs {
                    root: ExistingStorage::mounted_valid("tank/cowshed"),
                    store: ExistingStorage::ExistingUnmounted {
                        exact_identifier: "tank/cowshed/store".to_owned(),
                        marker: invalid,
                    },
                    caches: ExistingStorage::Absent,
                    projects: Box::new(ExistingStorage::Absent),
                },
            ),
            Err(PlanError::InvalidExistingStorageMarker {
                expected_role: VolumeRole::Store,
                expected_substrate: SubstrateKind::Zfs,
                ..
            })
        ));
    }
}

#[test]
fn sibling_volume_roots_plan_independently_but_zfs_children_still_require_their_root() {
    let apfs = select_substrate(apfs_evidence(), None).unwrap();
    for (store, caches) in [
        (
            ExistingStorage::Absent,
            ExistingStorage::mounted_valid("disk9s12"),
        ),
        (
            ExistingStorage::existing_unmounted(
                "disk9s11",
                VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs),
            ),
            ExistingStorage::mounted_valid("disk9s12"),
        ),
    ] {
        plan_bootstrap(
            apfs.clone(),
            Path::new("/Users/alice"),
            BootstrapEvidence::Apfs { store, caches },
        )
        .expect("sibling APFS volume roots do not impose a nested mount topology");
    }

    assert!(matches!(
        plan_bootstrap(
            selected_zfs("tank"),
            Path::new("/home/alice"),
            BootstrapEvidence::Zfs {
                root: ExistingStorage::Absent,
                store: ExistingStorage::mounted_valid("tank/cowshed/store"),
                caches: ExistingStorage::Absent,
                projects: Box::new(ExistingStorage::Absent),
            },
        ),
        Err(PlanError::ImpossibleStorageTopology(_))
    ));
}

fn role_strategy() -> impl Strategy<Value = VolumeRole> {
    prop_oneof![
        Just(VolumeRole::Store),
        Just(VolumeRole::Caches),
        Just(VolumeRole::Projects),
    ]
}

fn substrate_strategy() -> impl Strategy<Value = SubstrateKind> {
    prop_oneof![Just(SubstrateKind::Apfs), Just(SubstrateKind::Zfs)]
}

proptest! {
    #[test]
    fn markers_round_trip_for_every_role_and_substrate(
        role in role_strategy(),
        substrate in substrate_strategy(),
    ) {
        let marker = VolumeMarker::new(role, substrate);
        let bytes = marker.to_json().unwrap();
        prop_assert_eq!(VolumeMarker::from_json(&bytes).unwrap(), marker);
        prop_assert!(require_mounted_marker(Some(&bytes), role, substrate).is_ok());
    }

    #[test]
    fn every_unsupported_marker_version_is_rejected(
        version in any::<u32>().prop_filter("version one is supported", |version| *version != 1),
        role in role_strategy(),
        substrate in substrate_strategy(),
    ) {
        let role = match role {
            VolumeRole::Store => "store",
            VolumeRole::Caches => "caches",
            VolumeRole::Projects => "projects",
        };
        let substrate = match substrate {
            SubstrateKind::Apfs => "apfs",
            SubstrateKind::Zfs => "zfs",
        };
        let bytes = format!(
            r#"{{"version":{version},"role":"{role}","substrate":"{substrate}"}}"#
        );
        prop_assert!(matches!(
            VolumeMarker::from_json(bytes.as_bytes()),
            Err(MarkerError::UnsupportedVersion(actual)) if actual == version
        ));
    }

    #[test]
    fn successful_apfs_bootstrap_converges_to_a_mutation_free_repeat(
        store_state in 0_u8..3,
        caches_state in 0_u8..3,
    ) {
        fn state(value: u8, identifier: &str, role: VolumeRole) -> ExistingStorage {
            match value {
                0 => ExistingStorage::Absent,
                1 => ExistingStorage::existing_unmounted(
                    identifier,
                    VolumeMarker::new(role, SubstrateKind::Apfs),
                ),
                2 => ExistingStorage::mounted_valid(identifier),
                _ => unreachable!(),
            }
        }
        prop_assume!(store_state != 0 || caches_state == 0);
        prop_assume!(store_state == 2 || caches_state != 2);

        let selected = select_substrate(apfs_evidence(), None).unwrap();
        let first = plan_bootstrap(
            selected.clone(),
            Path::new("/Users/alice"),
            BootstrapEvidence::Apfs {
                store: state(store_state, "disk9s11", VolumeRole::Store),
                caches: state(caches_state, "disk9s12", VolumeRole::Caches),
            },
        )
        .unwrap();
        let provisioned: Vec<_> = first
            .operations()
            .iter()
            .filter_map(|operation| match operation {
                HostOperation::ProvisionApfsVolumes { volumes, .. } => Some(volumes),
                _ => None,
            })
            .flatten()
            .collect();
        // Only the store is ever created; a retired caches volume is at most mounted again.
        prop_assert_eq!(
            provisioned
                .iter()
                .filter(|volume| matches!(volume.kind(), ApfsProvisionKind::Create))
                .count(),
            usize::from(store_state == 0)
        );
        let commands: Vec<_> = first.operations().iter().filter_map(command_line).collect();
        prop_assert_eq!(
            commands
                .iter()
                .filter(|command| command.contains("/usr/sbin/diskutil mount "))
                .count(),
            usize::from(store_state == 1) + usize::from(caches_state == 1)
        );

        let repeated = plan_bootstrap(
            selected,
            Path::new("/Users/alice"),
            mounted_apfs_storage(),
        )
        .unwrap();
        prop_assert!(repeated.operations().iter().all(|operation| !mutates_host(operation)));
    }
}

#[test]
fn plans_reject_noncanonical_home_paths() {
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    assert!(matches!(
        plan_bootstrap(
            selected.clone(),
            Path::new("relative/home"),
            absent_apfs_storage()
        ),
        Err(PlanError::NonCanonicalHome(_))
    ));
    assert!(matches!(
        plan_bootstrap(
            selected,
            Path::new("/Users/alice/../bob"),
            absent_apfs_storage()
        ),
        Err(PlanError::NonCanonicalHome(_))
    ));
}

#[test]
fn marker_round_trip_and_missing_or_wrong_markers_refuse_access() {
    let marker = VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs);
    let bytes = marker.to_json().unwrap();
    assert_eq!(VolumeMarker::from_json(&bytes).unwrap(), marker);
    assert!(matches!(
        require_mounted_marker(None, VolumeRole::Store, SubstrateKind::Apfs),
        Err(MountGuardError::MissingMarker)
    ));
    assert!(matches!(
        require_mounted_marker(Some(&bytes), VolumeRole::Caches, SubstrateKind::Apfs),
        Err(MountGuardError::WrongMarker { .. })
    ));
    let future = br#"{"version":2,"role":"store","substrate":"apfs"}"#;
    assert!(matches!(
        VolumeMarker::from_json(future),
        Err(MarkerError::UnsupportedVersion(2))
    ));
}

struct SpyHost {
    effects: AtomicUsize,
    mutations: AtomicUsize,
    inside_lane: AtomicBool,
    observed_off_caller: AtomicBool,
    caller: ThreadId,
    mountpoint: MountpointState,
    creates: AtomicUsize,
}

impl SpyHost {
    fn new(mountpoint: MountpointState) -> Self {
        Self {
            effects: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
            inside_lane: AtomicBool::new(false),
            observed_off_caller: AtomicBool::new(false),
            caller: std::thread::current().id(),
            mountpoint,
            creates: AtomicUsize::new(0),
        }
    }

    fn record(&self, mutation: bool) {
        assert!(
            self.inside_lane.load(Ordering::SeqCst),
            "host effect escaped blocking lane"
        );
        self.effects.fetch_add(1, Ordering::SeqCst);
        if mutation {
            self.mutations.fetch_add(1, Ordering::SeqCst);
        }
        if std::thread::current().id() != self.caller {
            self.observed_off_caller.store(true, Ordering::SeqCst);
        }
    }
}

impl BootstrapHost for SpyHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        self.record(false);
        Ok(())
    }

    fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
        self.record(false);
        Ok(self.mountpoint.clone())
    }

    fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
        self.record(true);
        Ok(())
    }

    fn reclaim_mountpoint(&self, _path: &Path) -> Result<(), HostError> {
        self.record(true);
        Ok(())
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        self.record(true);
        let stdout = if command
            .args()
            .starts_with(&["apfs".to_owned(), "addVolume".to_owned()])
        {
            let slice = self.creates.fetch_add(1, Ordering::SeqCst) + 8;
            format!("Created new APFS Volume disk3s{slice}\n").into_bytes()
        } else if command
            .args()
            .starts_with(&["info".to_owned(), "-plist".to_owned()])
        {
            let identifier = &command.args()[2];
            let name = if identifier == "disk3s8" {
                "cowshed.store"
            } else {
                "cowshed.caches"
            };
            format!(
                "<?xml version=\"1.0\"?><plist version=\"1.0\"><dict>\
                 <key>DeviceIdentifier</key><string>{identifier}</string>\
                 <key>APFSContainerReference</key><string>disk3</string>\
                 <key>VolumeName</key><string>{name}</string>\
                 <key>FilesystemType</key><string>apfs</string>\
                 <key>MountPoint</key><string></string>\
                 <key>APFSSnapshot</key><false/>\
                 </dict></plist>"
            )
            .into_bytes()
        } else {
            Vec::new()
        };
        Ok(HostCommandOutput::success(stdout))
    }

    fn provision_apfs_volumes(
        &self,
        _container: &str,
        _volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        self.record(true);
        Ok(())
    }

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        self.record(true);
        Ok(())
    }

    fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
        self.record(true);
        Ok(())
    }
}
struct TransitionHost {
    post_store: MountpointState,
    caches: MountpointState,
    store_inspections: AtomicUsize,
    sequence: AtomicUsize,
    store_publication: AtomicUsize,
    caches_guard: AtomicUsize,
    caches_mutations: AtomicUsize,
    relabels: AtomicUsize,
}

impl TransitionHost {
    fn new(post_store: MountpointState, caches: MountpointState) -> Self {
        Self {
            post_store,
            caches,
            store_inspections: AtomicUsize::new(0),
            sequence: AtomicUsize::new(0),
            store_publication: AtomicUsize::new(0),
            caches_guard: AtomicUsize::new(0),
            caches_mutations: AtomicUsize::new(0),
            relabels: AtomicUsize::new(0),
        }
    }

    fn next_sequence(&self) -> usize {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }
}

impl BootstrapHost for TransitionHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        self.next_sequence();
        Ok(())
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        let sequence = self.next_sequence();
        if path.ends_with("caches") {
            self.caches_guard.store(sequence, Ordering::SeqCst);
            return Ok(self.caches.clone());
        }
        let inspection = self.store_inspections.fetch_add(1, Ordering::SeqCst);
        if inspection == 0 {
            Ok(MountpointState::EmptyDirectory)
        } else {
            Ok(self.post_store.clone())
        }
    }

    fn create_dir_all(&self, path: &Path) -> Result<(), HostError> {
        self.next_sequence();
        if path.ends_with("caches") {
            self.caches_mutations.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError> {
        self.next_sequence();
        if path.ends_with("caches") {
            self.caches_mutations.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        let sequence = self.next_sequence();
        let line = command.args().join(" ");
        if line.contains("disk9s12") || line.contains("cowshed/caches") {
            self.caches_mutations.fetch_add(1, Ordering::SeqCst);
        } else if line.contains("disk9s11") || line.contains("cowshed/store") {
            self.store_publication.store(sequence, Ordering::SeqCst);
        }
        if line.contains("org.cowshed:") {
            self.relabels.fetch_add(1, Ordering::SeqCst);
        }
        Ok(HostCommandOutput::default())
    }

    fn provision_apfs_volumes(
        &self,
        _container: &str,
        _volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        self.next_sequence();
        Ok(())
    }

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        self.next_sequence();
        self.relabels.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn pin_volumes_in_fstab(&self, _pins: &[FstabPin]) -> Result<(), HostError> {
        self.next_sequence();
        Ok(())
    }
}

struct InlineLane;

#[async_trait]
impl BlockingLane for InlineLane {
    async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError> {
        job()
    }
}

fn existing_unmounted_plan(zfs: bool, caches: ExistingStorage) -> BootstrapPlan {
    if zfs {
        plan_bootstrap(
            selected_zfs("tank"),
            Path::new("/home/alice"),
            BootstrapEvidence::Zfs {
                root: ExistingStorage::mounted_valid("tank/cowshed"),
                store: ExistingStorage::existing_unmounted(
                    "tank/cowshed/store",
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Zfs),
                ),
                caches,
                projects: Box::new(ExistingStorage::Absent),
            },
        )
        .unwrap()
    } else {
        plan_bootstrap(
            select_substrate(apfs_evidence(), None).unwrap(),
            Path::new("/Users/alice"),
            BootstrapEvidence::Apfs {
                store: ExistingStorage::existing_unmounted(
                    "disk9s11",
                    VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs),
                ),
                caches,
            },
        )
        .unwrap()
    }
}

#[tokio::test]
async fn nested_caches_guard_observes_the_published_store_before_cache_mutation() {
    for zfs in [false, true] {
        let substrate = if zfs {
            SubstrateKind::Zfs
        } else {
            SubstrateKind::Apfs
        };
        let caches = ExistingStorage::existing_unmounted(
            if zfs {
                "tank/cowshed/caches"
            } else {
                "disk9s12"
            },
            VolumeMarker::new(VolumeRole::Caches, substrate),
        );
        let plan = existing_unmounted_plan(zfs, caches);
        let store_marker = VolumeMarker::new(VolumeRole::Store, substrate)
            .to_json()
            .unwrap();
        let host = Arc::new(TransitionHost::new(
            MountpointState::Mounted {
                marker: Some(store_marker),
            },
            MountpointState::NonEmptyDirectoryWithoutMount,
        ));

        let error = execute_bootstrap(&plan, Arc::clone(&host), &InlineLane)
            .await
            .unwrap_err();
        assert!(matches!(error, BootstrapExecutionError::MaskedData(_)));
        let publication = host.store_publication.load(Ordering::SeqCst);
        let caches_guard = host.caches_guard.load(Ordering::SeqCst);
        assert!(publication > 0 && publication < caches_guard);
        assert_eq!(host.store_inspections.load(Ordering::SeqCst), 2);
        assert_eq!(host.caches_mutations.load(Ordering::SeqCst), 0);
        assert_eq!(host.relabels.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn post_mount_marker_mismatch_refuses_without_relabeling_existing_storage() {
    for zfs in [false, true] {
        let substrate = if zfs {
            SubstrateKind::Zfs
        } else {
            SubstrateKind::Apfs
        };
        let future = format!(
            r#"{{"version":2,"role":"store","substrate":"{}"}}"#,
            if zfs { "zfs" } else { "apfs" }
        )
        .into_bytes();
        let wrong = VolumeMarker::new(VolumeRole::Caches, substrate)
            .to_json()
            .unwrap();
        for marker in [None, Some(future), Some(wrong)] {
            let plan = existing_unmounted_plan(zfs, ExistingStorage::Absent);
            let host = Arc::new(TransitionHost::new(
                MountpointState::Mounted { marker },
                MountpointState::Missing,
            ));

            let error = execute_bootstrap(&plan, Arc::clone(&host), &InlineLane)
                .await
                .unwrap_err();
            assert!(matches!(error, BootstrapExecutionError::MountGuard { .. }));
            assert_eq!(host.store_inspections.load(Ordering::SeqCst), 2);
            assert_eq!(host.caches_guard.load(Ordering::SeqCst), 0);
            assert_eq!(host.relabels.load(Ordering::SeqCst), 0);
        }
    }
}

struct AssertingLane {
    dispatches: AtomicUsize,
    host: Arc<SpyHost>,
}

#[async_trait]
impl BlockingLane for AssertingLane {
    async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        assert!(!self.host.inside_lane.swap(true, Ordering::SeqCst));
        let result = job();
        self.host.inside_lane.store(false, Ordering::SeqCst);
        result
    }
}

#[test]
fn pure_planning_has_no_host_effects() {
    let host = SpyHost::new(MountpointState::EmptyDirectory);
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    let first = plan_bootstrap(
        selected.clone(),
        Path::new("/Users/alice"),
        absent_apfs_storage(),
    )
    .unwrap();
    let second =
        plan_bootstrap(selected, Path::new("/Users/alice"), absent_apfs_storage()).unwrap();
    assert_eq!(first, second);
    assert_eq!(host.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn every_host_effect_crosses_the_injected_blocking_lane() {
    let host = Arc::new(SpyHost::new(MountpointState::EmptyDirectory));
    let lane = AssertingLane {
        dispatches: AtomicUsize::new(0),
        host: Arc::clone(&host),
    };
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    let plan = plan_bootstrap(selected, Path::new("/Users/alice"), absent_apfs_storage()).unwrap();

    execute_bootstrap(&plan, Arc::clone(&host), &lane)
        .await
        .unwrap();
    assert_eq!(
        lane.dispatches.load(Ordering::SeqCst),
        plan.operations().len()
    );
    assert_eq!(host.effects.load(Ordering::SeqCst), plan.operations().len());
}

#[tokio::test]
async fn marker_version_and_masking_refuse_before_any_mutation() {
    let unsupported = br#"{"version":2,"role":"store","substrate":"apfs"}"#;
    for state in [
        MountpointState::Mounted { marker: None },
        MountpointState::Mounted {
            marker: Some(unsupported.to_vec()),
        },
        MountpointState::NonEmptyDirectoryWithoutMount,
    ] {
        for zfs in [false, true] {
            let host = Arc::new(SpyHost::new(state.clone()));
            let lane = AssertingLane {
                dispatches: AtomicUsize::new(0),
                host: Arc::clone(&host),
            };
            let (selected, home, evidence, observations_before_guard) = if zfs {
                (
                    selected_zfs("tank"),
                    Path::new("/home/alice"),
                    absent_zfs_storage(),
                    2,
                )
            } else {
                (
                    select_substrate(apfs_evidence(), None).unwrap(),
                    Path::new("/Users/alice"),
                    absent_apfs_storage(),
                    1,
                )
            };
            let plan = plan_bootstrap(selected, home, evidence).unwrap();
            let error = execute_bootstrap(&plan, Arc::clone(&host), &lane)
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                BootstrapExecutionError::MountGuard { .. } | BootstrapExecutionError::MaskedData(_)
            ));
            assert_eq!(
                lane.dispatches.load(Ordering::SeqCst),
                observations_before_guard
            );
            assert_eq!(
                host.effects.load(Ordering::SeqCst),
                observations_before_guard
            );
            assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_lane_moves_platform_work_off_the_async_worker() {
    let host = Arc::new(SpyHost::new(MountpointState::EmptyDirectory));
    host.inside_lane.store(true, Ordering::SeqCst);
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    let plan = plan_bootstrap(selected, Path::new("/Users/alice"), absent_apfs_storage()).unwrap();

    execute_bootstrap(&plan, Arc::clone(&host), &TokioBlockingLane)
        .await
        .unwrap();
    assert_eq!(host.effects.load(Ordering::SeqCst), plan.operations().len());
    assert!(host.observed_off_caller.load(Ordering::SeqCst));
}
