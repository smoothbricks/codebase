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

    fn write_file_atomic(&self, _path: &Path, _contents: &[u8]) -> Result<(), HostError> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

enum CommandBehavior {
    CreatedIdentifiers { info_container: &'static str },
    Fixed(HostCommandOutput),
}

struct RecordingHost {
    commands: Sender<HostCommand>,
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
            CommandBehavior::Fixed(output) => Ok(output.clone()),
        }
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

fn command_argv(command: &HostCommand) -> Vec<String> {
    std::iter::once(command.program().to_owned())
        .chain(command.args().iter().cloned())
        .collect()
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
async fn created_volume_identifiers_flow_to_exact_store_then_caches_mounts() {
    let (sender, receiver) = mpsc::channel();
    let host = Arc::new(RecordingHost {
        commands: sender,
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

    let commands: Vec<_> = receiver
        .try_iter()
        .map(|command| command_argv(&command))
        .collect();
    assert_eq!(
        commands,
        [
            vec![
                "/usr/sbin/diskutil",
                "apfs",
                "addVolume",
                "disk3",
                "APFS",
                "cowshed.store",
                "-nomount",
            ],
            vec!["/usr/sbin/diskutil", "info", "-plist", "disk3s8"],
            vec![
                "/usr/sbin/diskutil",
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/Users/alice/.cowshed",
                "disk3s8",
            ],
            vec![
                "/usr/sbin/diskutil",
                "apfs",
                "addVolume",
                "disk3",
                "APFS",
                "cowshed.caches",
                "-nomount",
            ],
            vec!["/usr/sbin/diskutil", "info", "-plist", "disk3s9"],
            vec![
                "/usr/sbin/diskutil",
                "mount",
                "-nobrowse",
                "-mountPoint",
                "/Users/alice/.cowshed/caches",
                "disk3s9",
            ],
        ]
        .map(|argv| argv.into_iter().map(str::to_owned).collect::<Vec<_>>())
    );
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
            assert!(
                actions
                    .iter()
                    .any(|action| action.contains("cowshed.store"))
            );
            assert!(
                actions
                    .iter()
                    .any(|action| action.contains("cowshed.caches"))
            );
            assert!(
                actions
                    .iter()
                    .any(|action| action == "mount APFS volume at /Users/alice/.cowshed")
            );
            assert!(
                actions.iter().any(|action| {
                    action == "mount APFS volume at /Users/alice/.cowshed/caches"
                })
            );
        }
        error => panic!("unexpected error: {error}"),
    }
    assert_eq!(lane.dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(host.mutations.load(Ordering::SeqCst), 0);
    assert_eq!(host.inspections.load(Ordering::SeqCst), 0);
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

#[tokio::test]
async fn malformed_created_identifier_output_fails_before_any_mount() {
    for stdout in [
        b"Finished APFS operation\n".to_vec(),
        b"Created new APFS Volume disk3s8\nCreated new APFS Volume disk3s9\n".to_vec(),
        b"Created new APFS Volume disk3sx\n".to_vec(),
    ] {
        let (sender, receiver) = mpsc::channel();
        let host = Arc::new(RecordingHost {
            commands: sender,
            behavior: CommandBehavior::Fixed(HostCommandOutput {
                success: true,
                stdout,
                stderr: Vec::new(),
            }),
            creations: AtomicUsize::new(0),
        });

        let error = execute_bootstrap(&absent_apfs_plan(), host, &InlineLane)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            BootstrapExecutionError::CreatedVolumeOutput(_)
        ));
        let commands: Vec<_> = receiver.try_iter().collect();
        assert_eq!(commands.len(), 1);
        assert!(
            commands[0]
                .args()
                .starts_with(&["apfs".to_owned(), "addVolume".to_owned()])
        );
    }
}

#[tokio::test]
async fn wrong_container_info_attestation_fails_before_mount() {
    let (sender, receiver) = mpsc::channel();
    let host = Arc::new(RecordingHost {
        commands: sender,
        behavior: CommandBehavior::CreatedIdentifiers {
            info_container: "disk4",
        },
        creations: AtomicUsize::new(0),
    });

    let error = execute_bootstrap(&absent_apfs_plan(), host, &InlineLane)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BootstrapExecutionError::CreatedVolumeAttestation(_)
    ));
    let commands: Vec<_> = receiver.try_iter().collect();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[1].args(), ["info", "-plist", "disk3s8"]);
    assert!(
        commands
            .iter()
            .all(|command| !command.args().iter().any(|arg| arg == "mount"))
    );
}

#[tokio::test]
async fn command_failure_preserves_stderr_and_never_attempts_a_mount() {
    let (sender, receiver) = mpsc::channel();
    let host = Arc::new(RecordingHost {
        commands: sender,
        behavior: CommandBehavior::Fixed(HostCommandOutput {
            success: false,
            stdout: Vec::new(),
            stderr: b"diskutil: exact failure; $(not a shell)\n".to_vec(),
        }),
        creations: AtomicUsize::new(0),
    });

    let error = execute_bootstrap(&absent_apfs_plan(), host, &InlineLane)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BootstrapExecutionError::CommandFailed { stderr, .. }
            if stderr == "diskutil: exact failure; $(not a shell)\n"
    ));
    assert_eq!(receiver.try_iter().count(), 1);
}
