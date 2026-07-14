use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};

use async_trait::async_trait;
use cowshed_core::storage::bootstrap::native::{
    NativeBootstrapError, NativeBootstrapMode, execute_native_bootstrap_plan,
};
use cowshed_core::storage::bootstrap::*;

struct InlineLane;

#[async_trait]
impl BlockingLane for InlineLane {
    async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError> {
        job()
    }
}

#[derive(Default)]
struct CountingLane {
    dispatches: AtomicUsize,
}

#[async_trait]
impl BlockingLane for CountingLane {
    async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        job()
    }
}

#[derive(Default)]
struct ValidationHost {
    inspections: AtomicUsize,
    mutations: AtomicUsize,
}

impl BootstrapHost for ValidationHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        unreachable!("APFS test plan has no ZFS operation")
    }

    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError> {
        self.inspections.fetch_add(1, Ordering::SeqCst);
        let role = if path.ends_with("caches") {
            VolumeRole::Caches
        } else {
            VolumeRole::Store
        };
        Ok(MountpointState::Mounted {
            marker: Some(
                VolumeMarker::new(role, SubstrateKind::Apfs)
                    .to_json()
                    .unwrap(),
            ),
        })
    }

    fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn run_command(&self, _command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(HostCommandOutput::default())
    }

    fn provision_apfs_volumes(
        &self,
        _container: &str,
        _volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

enum CommandBehavior {
    CreatedIdentifiers { info_container: &'static str },
}

struct RecordingHost {
    commands: Sender<HostCommand>,
    provisions: Sender<(String, Vec<ApfsVolumeProvision>)>,
    behavior: CommandBehavior,
    creations: AtomicUsize,
}

impl BootstrapHost for RecordingHost {
    fn verify_zfs_delegation(&self, _pool: &str, _required_root: &str) -> Result<(), HostError> {
        unreachable!("APFS test plan has no ZFS operation")
    }

    fn inspect_mountpoint(&self, _path: &Path) -> Result<MountpointState, HostError> {
        Ok(MountpointState::EmptyDirectory)
    }

    fn create_dir_all(&self, _path: &Path) -> Result<(), HostError> {
        Ok(())
    }

    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError> {
        self.commands.send(command.clone()).unwrap();
        match &self.behavior {
            CommandBehavior::CreatedIdentifiers { .. }
                if command
                    .args()
                    .starts_with(&["apfs".to_owned(), "addVolume".to_owned()]) =>
            {
                let slice = self.creations.fetch_add(1, Ordering::SeqCst) + 8;
                Ok(HostCommandOutput {
                    success: true,
                    stdout: format!("Created new APFS Volume disk3s{slice}\n").into_bytes(),
                    stderr: Vec::new(),
                })
            }
            CommandBehavior::CreatedIdentifiers { info_container }
                if command
                    .args()
                    .starts_with(&["info".to_owned(), "-plist".to_owned()]) =>
            {
                let identifier = &command.args()[2];
                let name = if identifier == "disk3s8" {
                    "cowshed.store"
                } else {
                    "cowshed.caches"
                };
                Ok(HostCommandOutput {
                    success: true,
                    stdout: info_plist(identifier, info_container, name),
                    stderr: Vec::new(),
                })
            }
            CommandBehavior::CreatedIdentifiers { .. } => Ok(HostCommandOutput {
                success: true,
                ..HostCommandOutput::default()
            }),
        }
    }

    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError> {
        self.provisions
            .send((container.to_owned(), volumes.to_vec()))
            .unwrap();
        Ok(())
    }

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        Ok(())
    }
}

fn absent_apfs_plan() -> BootstrapPlan {
    let selected = select_substrate(
        StatFsEvidence::Apfs {
            mount_source: "/dev/disk3s5".into(),
            container: Some("disk3".to_owned()),
        },
        None,
    )
    .unwrap();
    plan_bootstrap(
        selected,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::Absent,
            caches: ExistingStorage::Absent,
        },
    )
    .unwrap()
}

fn mounted_apfs_plan() -> BootstrapPlan {
    let selected = select_substrate(
        StatFsEvidence::Apfs {
            mount_source: "/dev/disk3s5".into(),
            container: Some("disk3".to_owned()),
        },
        None,
    )
    .unwrap();
    plan_bootstrap(
        selected,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::mounted_valid("disk3s8"),
            caches: ExistingStorage::mounted_valid("disk3s9"),
        },
    )
    .unwrap()
}

fn interrupted_caches_plan() -> BootstrapPlan {
    let selected = select_substrate(
        StatFsEvidence::Apfs {
            mount_source: "/dev/disk3s5".into(),
            container: Some("disk3".to_owned()),
        },
        None,
    )
    .unwrap();
    plan_bootstrap(
        selected,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::mounted_valid("disk3s8"),
            caches: ExistingStorage::mounted_incomplete("disk3s9"),
        },
    )
    .unwrap()
}

fn info_plist(identifier: &str, container: &str, name: &str) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><plist version=\"1.0\"><dict>\
         <key>DeviceIdentifier</key><string>{identifier}</string>\
         <key>APFSContainerReference</key><string>{container}</string>\
         <key>VolumeName</key><string>{name}</string>\
         <key>FilesystemType</key><string>apfs</string>\
         <key>MountPoint</key><string></string>\
         <key>APFSSnapshot</key><false/>\
         </dict></plist>"
    )
    .into_bytes()
}

#[tokio::test]
async fn absent_volumes_collapse_into_one_explicit_provisioning_batch() {
    let (command_sender, command_receiver) = mpsc::channel();
    let (provision_sender, provision_receiver) = mpsc::channel();
    let host = Arc::new(RecordingHost {
        commands: command_sender,
        provisions: provision_sender,
        behavior: CommandBehavior::CreatedIdentifiers {
            info_container: "disk3",
        },
        creations: AtomicUsize::new(0),
    });

    execute_native_bootstrap_plan(
        &absent_apfs_plan(),
        NativeBootstrapMode::Provision,
        host,
        &InlineLane,
    )
    .await
    .unwrap();

    assert_eq!(command_receiver.try_iter().count(), 0);
    let batches: Vec<_> = provision_receiver.try_iter().collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0, "disk3");
    assert_eq!(batches[0].1.len(), 2);
    assert_eq!(batches[0].1[0].name(), "cowshed.store");
    assert_eq!(
        batches[0].1[0].mountpoint(),
        Path::new("/Users/alice/.cowshed")
    );
    assert!(matches!(batches[0].1[0].kind(), ApfsProvisionKind::Create));
    assert_eq!(batches[0].1[1].name(), "cowshed.caches");
    assert_eq!(
        batches[0].1[1].mountpoint(),
        Path::new("/Users/alice/.cowshed/caches")
    );
    assert!(matches!(batches[0].1[1].kind(), ApfsProvisionKind::Create));
}

#[tokio::test]
async fn existing_only_missing_volumes_rejects_before_dispatch_with_adopt_hint() {
    let host = Arc::new(ValidationHost::default());
    let lane = CountingLane::default();
    let error = execute_native_bootstrap_plan(
        &absent_apfs_plan(),
        NativeBootstrapMode::ExistingOnly,
        Arc::clone(&host),
        &lane,
    )
    .await
    .unwrap_err();

    match error {
        NativeBootstrapError::StorageSetupRequired { actions, hint } => {
            assert_eq!(hint, "next: cowshed adopt");
            assert_eq!(
                actions,
                ["provision APFS volumes cowshed.store, cowshed.caches"]
            );
        }
        error => panic!("unexpected error: {error}"),
    }
    assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
    assert_eq!(host.inspections.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn markerless_exact_mount_recovery_is_provision_only() {
    let plan = interrupted_caches_plan();
    assert!(matches!(
        plan.operations(),
        [
            HostOperation::GuardMountpoint {
                role: VolumeRole::Store,
                ..
            },
            HostOperation::ProvisionApfsVolumes { container, volumes }
        ] if container == "disk3"
            && matches!(
                volumes.as_slice(),
                [volume]
                    if volume.name() == APFS_CACHES_VOLUME
                        && matches!(
                            volume.kind(),
                            ApfsProvisionKind::RepairMounted { exact_identifier }
                                if exact_identifier == "disk3s9"
                        )
            )
    ));

    let existing_host = Arc::new(ValidationHost::default());
    let existing_lane = CountingLane::default();
    assert!(matches!(
        execute_native_bootstrap_plan(
            &plan,
            NativeBootstrapMode::ExistingOnly,
            Arc::clone(&existing_host),
            &existing_lane,
        )
        .await,
        Err(NativeBootstrapError::StorageSetupRequired { .. })
    ));
    assert_eq!(existing_lane.dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(existing_host.mutations.load(Ordering::SeqCst), 0);
    assert_eq!(existing_host.inspections.load(Ordering::SeqCst), 0);

    let provision_host = Arc::new(ValidationHost::default());
    let provision_lane = CountingLane::default();
    execute_native_bootstrap_plan(
        &plan,
        NativeBootstrapMode::Provision,
        Arc::clone(&provision_host),
        &provision_lane,
    )
    .await
    .unwrap();
    assert_eq!(provision_lane.dispatches.load(Ordering::SeqCst), 2);
    assert_eq!(provision_host.inspections.load(Ordering::SeqCst), 1);
    assert_eq!(provision_host.mutations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn detached_exact_volume_requires_explicit_provisioning_without_prompt() {
    let selected = select_substrate(
        StatFsEvidence::Apfs {
            mount_source: "/dev/disk3s5".into(),
            container: Some("disk3".to_owned()),
        },
        None,
    )
    .unwrap();
    let plan = plan_bootstrap(
        selected,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::mounted_valid("disk3s8"),
            caches: ExistingStorage::detached_incomplete("disk3s9"),
        },
    )
    .unwrap();
    assert!(matches!(
        plan.operations(),
        [
            HostOperation::GuardMountpoint {
                role: VolumeRole::Store,
                ..
            },
            HostOperation::GuardMountpoint {
                role: VolumeRole::Caches,
                ..
            },
            HostOperation::ProvisionApfsVolumes { volumes, .. }
        ] if matches!(
            volumes.as_slice(),
            [volume]
                if matches!(
                    volume.kind(),
                    ApfsProvisionKind::RecoverDetached { exact_identifier }
                        if exact_identifier == "disk3s9"
                )
        )
    ));

    let host = Arc::new(ValidationHost::default());
    let lane = CountingLane::default();
    let error = execute_native_bootstrap_plan(
        &plan,
        NativeBootstrapMode::ExistingOnly,
        Arc::clone(&host),
        &lane,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        NativeBootstrapError::StorageSetupRequired {
            hint: "next: cowshed adopt",
            ..
        }
    ));
    assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(host.inspections.load(Ordering::SeqCst), 0);
    assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mismounted_exact_volume_is_rejected_before_existing_only_dispatch() {
    let selected = select_substrate(
        StatFsEvidence::Apfs {
            mount_source: "/dev/disk3s5".into(),
            container: Some("disk3".to_owned()),
        },
        None,
    )
    .unwrap();
    let plan = plan_bootstrap(
        selected,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::mounted_valid("disk3s8"),
            caches: ExistingStorage::mis_mounted_incomplete("disk3s9", "/Volumes/cowshed-wrong"),
        },
    )
    .unwrap();
    assert!(plan.operations().iter().any(|operation| matches!(
        operation,
        HostOperation::ProvisionApfsVolumes { volumes, .. }
            if matches!(
                volumes.as_slice(),
                [volume]
                    if matches!(
                        volume.kind(),
                        ApfsProvisionKind::RepairMisMounted {
                            exact_identifier,
                            current_mountpoint,
                        } if exact_identifier == "disk3s9"
                            && current_mountpoint == Path::new("/Volumes/cowshed-wrong")
                    )
            )
    )));

    let host = Arc::new(ValidationHost::default());
    let lane = CountingLane::default();
    let error = execute_native_bootstrap_plan(
        &plan,
        NativeBootstrapMode::ExistingOnly,
        Arc::clone(&host),
        &lane,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        NativeBootstrapError::StorageSetupRequired {
            hint: "next: cowshed adopt",
            ..
        }
    ));
    assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(host.inspections.load(Ordering::SeqCst), 0);
    assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
}

#[test]
fn canonical_mount_flag_repair_does_not_require_a_prepublished_marker() {
    let selected = select_substrate(
        StatFsEvidence::Apfs {
            mount_source: "/dev/disk3s5".into(),
            container: Some("disk3".to_owned()),
        },
        None,
    )
    .unwrap();
    let plan = plan_bootstrap(
        selected,
        Path::new("/Users/alice"),
        BootstrapEvidence::Apfs {
            store: ExistingStorage::mounted_valid("disk3s8"),
            caches: ExistingStorage::mis_mounted_incomplete(
                "disk3s9",
                "/Users/alice/.cowshed/caches",
            ),
        },
    )
    .unwrap();
    assert!(matches!(
        plan.operations(),
        [
            HostOperation::GuardMountpoint {
                role: VolumeRole::Store,
                ..
            },
            HostOperation::ProvisionApfsVolumes { volumes, .. }
        ] if matches!(
            volumes.as_slice(),
            [volume]
                if matches!(
                    volume.kind(),
                    ApfsProvisionKind::RepairMisMounted {
                        exact_identifier,
                        current_mountpoint,
                    } if exact_identifier == "disk3s9"
                        && current_mountpoint == Path::new("/Users/alice/.cowshed/caches")
                )
        )
    ));
}

#[tokio::test]
async fn already_correct_volumes_validate_in_both_modes_without_mutation() {
    for mode in [
        NativeBootstrapMode::Provision,
        NativeBootstrapMode::ExistingOnly,
    ] {
        let host = Arc::new(ValidationHost::default());
        let lane = CountingLane::default();
        execute_native_bootstrap_plan(&mounted_apfs_plan(), mode, Arc::clone(&host), &lane)
            .await
            .unwrap();

        assert_eq!(lane.dispatches.load(Ordering::SeqCst), 2);
        assert_eq!(host.inspections.load(Ordering::SeqCst), 2);
        assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
    }
}
