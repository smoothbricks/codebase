use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use async_trait::async_trait;
use cowshed_core::storage::bootstrap::*;

fn apfs_evidence() -> StatFsEvidence {
    StatFsEvidence::Apfs {
        mount_source: PathBuf::from("/dev/disk3s1"),
        container: Some("disk3".to_owned()),
    }
}

fn explicit_zfs(pool: &str) -> SubstrateConfig {
    parse_substrate_config(&format!("[substrate]\nkind = \"zfs\"\npool = \"{pool}\"\n"))
        .unwrap()
        .unwrap()
}

fn command_line(operation: &HostOperation) -> Option<String> {
    let HostOperation::RunCommand(command) = operation else {
        return None;
    };
    Some(
        std::iter::once(command.program().to_owned())
            .chain(command.args().iter().cloned())
            .collect::<Vec<_>>()
            .join(" "),
    )
}

#[test]
fn substrate_config_parser_is_narrow_and_strict() {
    assert_eq!(
        parse_substrate_config("[repository]\nname = broken syntax\n").unwrap(),
        None
    );
    assert_eq!(
        parse_substrate_config(
            "# project settings\n[other]\nvalue = 7\n[substrate] # deliberate override\nkind = \"zfs\"\npool = \"tank\" # no scan\n"
        )
        .unwrap()
        .unwrap()
        .pool(),
        "tank"
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
        let error = parse_substrate_config(source).unwrap_err();
        assert!(error.to_string().contains(message), "{source:?}: {error}");
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

#[test]
fn apfs_plan_has_exact_roots_commands_markers_and_store_first_order() {
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    let plan = plan_bootstrap(selected, Path::new("/Users/alice")).unwrap();
    assert_eq!(plan.roots().home(), Path::new("/Users/alice"));
    assert_eq!(plan.roots().store(), Path::new("/Users/alice/.cowshed"));
    assert_eq!(
        plan.roots().caches(),
        Path::new("/Users/alice/.cowshed/caches")
    );

    let commands: Vec<_> = plan.operations().iter().filter_map(command_line).collect();
    assert_eq!(
        commands,
        [
            "/usr/sbin/diskutil apfs addVolume disk3 APFS cowshed.store -nomount",
            "/usr/sbin/diskutil mount -nobrowse -mountPoint /Users/alice/.cowshed cowshed.store",
            "/usr/sbin/diskutil apfs addVolume disk3 APFS cowshed.caches -nomount",
            "/usr/sbin/diskutil mount -nobrowse -mountPoint /Users/alice/.cowshed/caches cowshed.caches",
        ]
    );

    assert!(matches!(
        &plan.operations()[0],
        HostOperation::GuardMountpoint { path, role: VolumeRole::Store, substrate: SubstrateKind::Apfs }
            if path == Path::new("/Users/alice/.cowshed")
    ));
    let store_marker = plan
        .operations()
        .iter()
        .position(|operation| matches!(operation, HostOperation::WriteMarkerAtomic { marker, .. } if marker.role() == VolumeRole::Store))
        .unwrap();
    let cache_guard = plan
        .operations()
        .iter()
        .position(|operation| {
            matches!(
                operation,
                HostOperation::GuardMountpoint {
                    role: VolumeRole::Caches,
                    ..
                }
            )
        })
        .unwrap();
    assert!(
        store_marker < cache_guard,
        "store must be mounted and marked before nested caches"
    );
    assert!(matches!(
        &plan.operations()[store_marker],
        HostOperation::WriteMarkerAtomic { path, marker }
            if path == Path::new("/Users/alice/.cowshed/.cowshed-volume.json")
                && marker.substrate() == SubstrateKind::Apfs
    ));
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
    let plan = plan_bootstrap(selected, Path::new("/home/alice")).unwrap();

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
            "/usr/sbin/zfs create -o mountpoint=/home/alice/.cowshed tank/cowshed/store",
            "/usr/sbin/zfs set org.cowshed:version=1 org.cowshed:role=store tank/cowshed/store",
            "/usr/sbin/zfs create -o mountpoint=/home/alice/.cowshed/caches tank/cowshed/caches",
            "/usr/sbin/zfs set org.cowshed:version=1 org.cowshed:role=caches tank/cowshed/caches",
            "/usr/sbin/zfs create -o mountpoint=none tank/cowshed/projects",
            "/usr/sbin/zfs set org.cowshed:version=1 org.cowshed:role=projects tank/cowshed/projects",
        ]
    );
    assert!(commands.iter().all(|command| !command.contains("zpool")));

    let store_marker = plan
        .operations()
        .iter()
        .position(|operation| matches!(operation, HostOperation::WriteMarkerAtomic { marker, .. } if marker.role() == VolumeRole::Store))
        .unwrap();
    let cache_create = plan
        .operations()
        .iter()
        .position(|operation| {
            command_line(operation).is_some_and(|command| command.ends_with("tank/cowshed/caches"))
        })
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
    assert!(store_marker < cache_create && cache_create < projects_create);
}

#[test]
fn plans_reject_noncanonical_home_paths() {
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    assert!(matches!(
        plan_bootstrap(selected.clone(), Path::new("relative/home")),
        Err(PlanError::NonCanonicalHome(_))
    ));
    assert!(matches!(
        plan_bootstrap(selected, Path::new("/Users/alice/../bob")),
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
    inside_lane: AtomicBool,
    mountpoint: Mutex<MountpointState>,
    threads: Mutex<Vec<ThreadId>>,
}

impl SpyHost {
    fn new(mountpoint: MountpointState) -> Self {
        Self {
            effects: AtomicUsize::new(0),
            inside_lane: AtomicBool::new(false),
            mountpoint: Mutex::new(mountpoint),
            threads: Mutex::new(Vec::new()),
        }
    }

    fn record(&self) {
        assert!(
            self.inside_lane.load(Ordering::SeqCst),
            "host effect escaped blocking lane"
        );
        self.effects.fetch_add(1, Ordering::SeqCst);
        self.threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
    }
}

impl BootstrapHost for SpyHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        self.record();
        Ok(())
    }

    fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
        self.record();
        Ok(self.mountpoint.lock().unwrap().clone())
    }

    fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
        self.record();
        Ok(())
    }

    fn run_command(&self, _command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        self.record();
        Ok(HostCommandOutput {
            success: true,
            ..HostCommandOutput::default()
        })
    }

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        self.record();
        Ok(())
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
    let first = plan_bootstrap(selected.clone(), Path::new("/Users/alice")).unwrap();
    let second = plan_bootstrap(selected, Path::new("/Users/alice")).unwrap();
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
    let plan = plan_bootstrap(selected, Path::new("/Users/alice")).unwrap();

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
async fn execution_refuses_markerless_mounts_before_first_mutation() {
    for state in [
        MountpointState::Mounted { marker: None },
        MountpointState::NonEmptyDirectoryWithoutMount,
    ] {
        let host = Arc::new(SpyHost::new(state));
        let lane = AssertingLane {
            dispatches: AtomicUsize::new(0),
            host: Arc::clone(&host),
        };
        let selected = select_substrate(apfs_evidence(), None).unwrap();
        let plan = plan_bootstrap(selected, Path::new("/Users/alice")).unwrap();
        let error = execute_bootstrap(&plan, Arc::clone(&host), &lane)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapExecutionError::MountGuard { .. } | BootstrapExecutionError::MaskedData(_)
        ));
        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(host.effects.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tokio_lane_moves_platform_work_off_the_async_worker() {
    let caller = std::thread::current().id();
    let host = Arc::new(SpyHost::new(MountpointState::EmptyDirectory));
    host.inside_lane.store(true, Ordering::SeqCst);
    let selected = select_substrate(apfs_evidence(), None).unwrap();
    let plan = plan_bootstrap(selected, Path::new("/Users/alice")).unwrap();

    execute_bootstrap(&plan, Arc::clone(&host), &TokioBlockingLane)
        .await
        .unwrap();
    let threads = host.threads.lock().unwrap();
    assert_eq!(threads.len(), plan.operations().len());
    assert!(threads.iter().all(|thread| *thread != caller));
}
