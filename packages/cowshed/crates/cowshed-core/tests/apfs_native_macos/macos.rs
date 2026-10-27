use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
use std::fs::{FileTimes, OpenOptions};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};

use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::mounts::KernelMountSnapshot;
use cowshed_apfs::store_lock::LockMode;
use cowshed_apfs::template::{blank_template, blank_template_path, blank_template_staged_stem};
use cowshed_apfs::{
    ApfsBackend, ApfsError, AttachedDiskImage, AttachedImage, CommandOutput, CommandRequest,
    CommandRunError, CommandRunner, DetachIntent, DiskImageSource, ImageCapture, MacOsApfsBackend,
    MountAccess, SystemCommandRunner,
};
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, GrantSet, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, Platform, PortBlock,
    PublicationState, SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceName,
    WorkspaceRole, sidecar_path,
};
use cowshed_core::repository::{OwnedRepoIds, RepoId};
use cowshed_core::storage::apfs::native::{
    KernelMountSource, MacOsApfsExecutionHost, RecoveryMarkerSource, RestoreFailpoint,
    SystemKernelMountSource,
};
use cowshed_core::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, FoundMount, Labelled,
    MarkerExpectation, MetadataPolicy,
};
use cowshed_core::storage::deletion_log::DELETION_LOG_FILE;
use cowshed_core::storage::lifecycle::{
    LifecycleFact, LifecycleWorkspace, MountJoin, MountRefusalReason, MountState,
    OperationIdentity, Pin, RetiredRef, Revision, StorageGcOptions, StorageGcReason,
    derive_workspaces,
};
use cowshed_core::storage::{CheckpointLabel, StorageLayout, StorageLayoutError};
use cowshed_core::workspace_credentials::mint_workspace_credentials;
use cowshed_os::fork_lock::{Fenced, Spawn as _};
use cowshed_os::namespace::{HostNamespace, TestNamespace};

#[path = "../../../cowshed-apfs/tests/support/blank_image.rs"]
mod blank_image;
#[path = "../support/fsevents.rs"]
mod fsevents;
#[path = "../../../cowshed-apfs/tests/support/scratch_apfs.rs"]
mod scratch_apfs;
#[path = "../../../cowshed-apfs/tests/support/scratch_namespace.rs"]
mod scratch_namespace;

use scratch_apfs::ScratchRoot;

/// `diskutil image attach --nobrowse --noMount --plist` for a RAW image: the whole device
/// carries no content hint, and the synthesized container and its one volume follow it. The
/// attach answers with the volume and the container it hangs from.
const ATTACH_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
<dict><key>content-hint</key><string></string><key>dev-entry</key><string>/dev/disk9</string></dict>
<dict><key>content-hint</key><string>Apple_APFS_Container</string><key>dev-entry</key><string>/dev/disk10</string></dict>
<dict><key>content-hint</key><string>Apple_APFS_Volume</string><key>dev-entry</key><string>/dev/disk10s1</string><key>filesystem-type</key><string>apfs</string></dict>
</array></dict></plist>"#;

/// `diskutil info -plist <device>` for a device in the fixture's single container: names the
/// volume, for rename checks.
fn device_info_plist(device: &str, volume_name: &str) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\"?><plist><dict><key>DeviceIdentifier</key><string>{device}</string><key>VolumeName</key><string>{volume_name}</string><key>APFSContainerReference</key><string>disk10</string></dict></plist>"
    )
    .into_bytes()
}

/// The kernel's disk-image inventory in these fixtures: every image the fake attached and has
/// not detached, holding the fixture's devices at one gibibyte. Every device detach re-reads it
/// first to prove the device still belongs to that image, so the fake must remember what it
/// attached.
#[derive(Clone, Default)]
struct FakeInventory(Arc<Mutex<Vec<String>>>);

impl FakeInventory {
    fn respond(&self, request: &CommandRequest) -> CommandOutput {
        let args: Vec<_> = request
            .args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        let mut attached = self.0.lock().expect("fake inventory");
        let stdout = if request.program == Path::new("/usr/bin/hdiutil")
            && args.first().is_some_and(|argument| argument == "attach")
        {
            attached.push(args.last().expect("attached image").clone());
            ATTACH_PLIST.as_bytes().to_vec()
        } else {
            if request.program == Path::new("/usr/bin/hdiutil")
                && args.first().is_some_and(|argument| argument == "detach")
            {
                assert_eq!(args.last().map(String::as_str), Some("/dev/disk9"));
                // Every fixture attachment holds these same devices; detaching releases them.
                attached.clear();
            }
            Vec::new()
        };
        CommandOutput::success(stdout)
    }

    fn images(&self) -> Vec<AttachedDiskImage> {
        self.0
            .lock()
            .expect("fake inventory")
            .iter()
            .map(|image| {
                AttachedDiskImage::file(
                    image,
                    Some(ImageCapacity::from_gibibytes(1)),
                    [
                        ("/dev/disk9", ""),
                        ("/dev/disk10", "EF57347C-0000-11AA-AA11-00306543ECAC"),
                        ("/dev/disk10s1", "41504653-0000-11AA-AA11-00306543ECAC"),
                    ],
                )
            })
            .collect()
    }
}

#[derive(Clone)]
struct RecordingRunner {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<CommandRequest>>>,
    volume_name: Arc<Mutex<String>>,
    inventory: FakeInventory,
    /// The fake kernel's mounts, by mountpoint, as a held mountpoint answers them. A path not
    /// listed is read from the real kernel, which answers the volume holding that directory.
    mounted: Arc<Mutex<std::collections::BTreeMap<PathBuf, String>>>,
    /// Every owned volume root named, as (volume device, name), in order.
    named: Arc<Mutex<Vec<(String, String)>>>,
    unmounted: Arc<
        Mutex<
            Vec<(
                String,
                cowshed_apfs::mounts::FilesystemId,
                cowshed_apfs::mounts::UnmountMode,
            )>,
        >,
    >,
}

impl Default for RecordingRunner {
    fn default() -> Self {
        Self {
            calls: Arc::default(),
            requests: Arc::default(),
            volume_name: Arc::new(Mutex::new("cowshed.acme--widget.main".to_owned())),
            inventory: FakeInventory::default(),
            mounted: Arc::default(),
            named: Arc::default(),
            unmounted: Arc::default(),
        }
    }
}

impl RecordingRunner {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<CommandRequest> {
        self.requests.lock().expect("requests").clone()
    }

    /// The fake kernel unmounts what was mounted at `mount_point`.
    fn unmount(&self, mount_point: &Path) {
        self.mounted.lock().expect("mounts").remove(mount_point);
    }
}

impl CommandRunner for RecordingRunner {
    fn run_guarded<E>(
        &self,
        request: &cowshed_apfs::CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<cowshed_apfs::CommandOutput>, E>
    where
        E: From<cowshed_apfs::CommandRunError> + std::fmt::Display,
    {
        if check()? {
            self.run(request, "guarded disk command")
                .map(|run| Some(run.output))
                .map_err(E::from)
        } else {
            Ok(None)
        }
    }
    fn run(
        &self,
        request: &CommandRequest,
        _operation: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        if request.program == Path::new("/sbin/mount_apfs") {
            assert_eq!(request.args.len(), 4, "mount_apfs argv: {request:?}");
            assert_eq!(request.args[0], "-o");
            assert_eq!(request.args[2], "/dev/disk10s1");
            assert!(
                !self.inventory.images().is_empty(),
                "mount only an attached volume"
            );
            let mount_point = PathBuf::from(&request.args[3]);
            assert!(mount_point.is_dir(), "the requested mount root must exist");
            self.mounted
                .lock()
                .expect("mounts")
                .insert(mount_point, request.args[2].to_string_lossy().into_owned());
            return Ok(cowshed_apfs::CommandRun {
                output: CommandOutput::success([]),
                work_elapsed: None,
            });
        }
        if request.program == Path::new("/usr/bin/hdiutil")
            && request
                .args
                .first()
                .is_some_and(|argument| argument == "detach")
        {
            let expected = "/dev/disk9";
            assert_eq!(
                request.args.last().and_then(|arg| arg.to_str()),
                Some(expected)
            );
            self.mounted
                .lock()
                .expect("mounts")
                .retain(|_, device| device != "/dev/disk10s1");
        }
        let is_info = request.program == Path::new("/usr/sbin/diskutil")
            && request
                .args
                .first()
                .is_some_and(|argument| argument == "info");
        if is_info {
            let device = request
                .args
                .last()
                .expect("device")
                .to_string_lossy()
                .trim_start_matches("/dev/")
                .to_owned();
            let volume_name = self.volume_name.lock().expect("volume name").clone();
            Ok(cowshed_apfs::CommandRun {
                output: CommandOutput::success(device_info_plist(&device, &volume_name)),
                work_elapsed: None,
            })
        } else {
            Ok(cowshed_apfs::CommandRun {
                output: self.inventory.respond(request),
                work_elapsed: None,
            })
        }
    }
    fn image_lease(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        Ok(None)
    }
    fn pin_raw_device(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        Ok(None)
    }
    fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
        Ok(self.inventory.images())
    }
    fn grow_image(&self, image: &Path, _: ImageCapacity) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("the recording runner grows no image: {}", image.display()),
        ))
    }
    fn volume_mount(&self, volume_device: &str) -> std::io::Result<Option<KernelMountSnapshot>> {
        Ok(self
            .mounted
            .lock()
            .expect("mounts")
            .iter()
            .find(|(_, device)| device.as_str() == volume_device)
            .map(|(path, device)| KernelMountSnapshot::new(1, path, device, true, true)))
    }
    fn unmount_guarded<E>(
        &self,
        mode: cowshed_apfs::mounts::UnmountMode,
        prove: impl FnOnce() -> Result<Option<cowshed_apfs::OwnedVolumeRoot>, E>,
    ) -> Result<cowshed_apfs::mounts::UnmountEffect, E>
    where
        E: From<cowshed_apfs::mounts::NativeUnmountError> + std::fmt::Display,
    {
        let Some(root) = prove()? else {
            return Ok(cowshed_apfs::mounts::UnmountEffect::NoEffect);
        };
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.unmounted.lock().expect("native effects").push((
            root.volume_device().to_owned(),
            root.filesystem_id(),
            mode,
        ));
        self.mounted
            .lock()
            .expect("mounts")
            .retain(|_, device| device != root.volume_device());
        Ok(cowshed_apfs::mounts::UnmountEffect::Unmounted {
            post_fd: cowshed_apfs::mounts::PostUnmountFd::Dead(std::io::Error::from_raw_os_error(
                libc::ENOTSUP,
            )),
        })
    }
    fn hold_mount(&self, mount_point: &Path) -> std::io::Result<cowshed_apfs::mounts::HeldMount> {
        match self.mounted.lock().expect("mounts").get(mount_point) {
            Some(device) => Ok(cowshed_apfs::mounts::HeldMount::stated(
                device.clone(),
                true,
                cowshed_apfs::mounts::FilesystemId::new([7, 26]),
            )),
            None => cowshed_apfs::mounts::hold_mount(mount_point),
        }
    }
    fn name_volume(
        &self,
        root: &cowshed_apfs::OwnedVolumeRoot,
        volume_name: &str,
    ) -> std::io::Result<()> {
        self.named
            .lock()
            .expect("named")
            .push((root.volume_device().to_owned(), volume_name.to_owned()));
        *self.volume_name.lock().expect("volume name") = volume_name.to_owned();
        Ok(())
    }
}
/// Fails the first kernel disk-image inventory read, then reports nothing attached.
///
/// This is the one double in the GC tests, and it stays one for a concrete reason: a live host's
/// I/O Registry cannot be made to refuse a matching snapshot on demand, so an unreadable
/// inventory is reachable only by answering as the runner would.
#[derive(Default)]
struct UnreadableFirstInventoryRunner(AtomicUsize);

impl CommandRunner for UnreadableFirstInventoryRunner {
    fn run_guarded<E>(
        &self,
        request: &cowshed_apfs::CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<cowshed_apfs::CommandOutput>, E>
    where
        E: From<cowshed_apfs::CommandRunError> + std::fmt::Display,
    {
        if check()? {
            self.run(request, "guarded disk command")
                .map(|run| Some(run.output))
                .map_err(E::from)
        } else {
            Ok(None)
        }
    }
    fn run(
        &self,
        _: &CommandRequest,
        _: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
        Ok(cowshed_apfs::CommandRun {
            output: CommandOutput::success([]),
            work_elapsed: None,
        })
    }
    fn image_lease(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        Ok(None)
    }
    fn pin_raw_device(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        Ok(None)
    }
    fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(std::io::Error::other(
                "injected I/O Registry snapshot failure",
            ));
        }
        Ok(Vec::new())
    }
    fn grow_image(&self, image: &Path, _: ImageCapacity) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "the GC inventory double grows no image: {}",
                image.display()
            ),
        ))
    }
}

#[derive(Clone, Copy)]
struct ByteRecoveryMarkers;

impl RecoveryMarkerSource for ByteRecoveryMarkers {
    fn incarnation(&self, image: &Path) -> Result<String, ApfsStorageError> {
        match std::fs::read(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?
            .as_slice()
        {
            b"old generation" => Ok("00000000000000000000000000000001".to_owned()),
            b"new generation" => Ok("00000000000000000000000000000002".to_owned()),
            _ => Err(ApfsStorageError::Host(format!(
                "unexpected recovery image bytes: {}",
                image.display()
            ))),
        }
    }
}

struct Fixture {
    root: PathBuf,
    namespace: TestNamespace,
}

impl Fixture {
    fn new(test: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "cowshed-apfs-boundary-{test}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).expect("fixture root");
        Self {
            root,
            namespace: TestNamespace::mint().expect("a test namespace"),
        }
    }

    fn host_namespace(&self) -> HostNamespace {
        HostNamespace::Test(self.namespace)
    }

    fn config(&self) -> ApfsSubstrateConfig {
        ApfsSubstrateConfig::new(self.host_namespace(), &self.root, self.root.join("mount"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let result = match std::fs::symlink_metadata(&self.root) {
            Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(&self.root),
            Ok(_) => std::fs::remove_file(&self.root),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            cowshed_os::eprintln!("remove APFS fixture {}: {error}", self.root.display());
        }
    }
}

#[test]
fn fixture_cleanup_removes_a_store_root_replaced_by_a_file() {
    let fixture = Fixture::new("cleanup-file-root");
    let root = fixture.root.clone();
    std::fs::remove_dir(&root).expect("replace fixture directory");
    std::fs::write(&root, b"not a directory").expect("file store root");
    drop(fixture);
    assert!(
        !root.exists(),
        "a corrupt store fixture must not survive its test"
    );
}

/// A store on real APFS images: a scratch root under the real-APFS scratch parent
/// (`/private/var/folders/cowshed/<uid>/t`) laid out like [`Fixture`]'s,
/// driven by the production host (real `hdiutil`/`diskutil`/`mount_apfs`, the live kernel mount
/// table). Its attachments are detached and its tree removed when the test ends however it
/// ends, and by the next run's sweep when it could not.
pub(super) struct RealFixture {
    scratch: ScratchRoot,
}

impl RealFixture {
    pub(super) fn new(test: &str) -> Self {
        Self {
            scratch: ScratchRoot::new(test).expect("scratch root"),
        }
    }

    pub(super) fn root(&self) -> &Path {
        self.scratch.path()
    }

    /// The namespace this fixture's store, and every name it mints, derives from.
    pub(super) fn host_namespace(&self) -> HostNamespace {
        HostNamespace::Test(self.scratch.namespace())
    }

    fn config(&self) -> ApfsSubstrateConfig {
        ApfsSubstrateConfig::new(
            self.host_namespace(),
            self.root(),
            self.root().join("mount"),
        )
    }

    /// A fresh production host. Each one starts with an empty mount registry, so a second one is
    /// what a restarted controller sees: whatever is mounted, it learns from the kernel alone.
    fn host(&self) -> MacOsApfsExecutionHost<SystemCommandRunner> {
        MacOsApfsExecutionHost::new(
            SystemCommandRunner::new(self.host_namespace()),
            self.config(),
        )
        .expect("native APFS host")
    }

    /// Main mounts at the adopted checkout itself.
    fn main_mount(&self) -> PathBuf {
        self.config().checkout_path
    }

    fn layout(&self) -> StorageLayout {
        StorageLayout::new(self.root(), &repo()).expect("layout")
    }

    /// A real one-volume RAW image at `image` (which must end `.dmg`) and nothing beside it,
    /// handed back detached: every verb that grows or replaces it next needs an image nothing
    /// holds. It is a clone of the run's blank image, not a mint of its own.
    pub(super) fn blank_image(&self, image: &Path) {
        blank_image::blank_image(self.scratch.namespace(), image);
    }

    /// [`Self::blank_image`] published as [`workspace`]'s: its sidecar and CA key beside it.
    fn published_image(&self, image: &Path) {
        self.blank_image(image);
        metadata().write_for_image(image).expect("sidecar");
        let signing_key =
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("fixture P-256 private key");
        write_ca_key(image, signing_key.serialize_pem().as_bytes());
    }

    /// Attach `image` and mount its volume at `mount_point` with exactly `options` (as
    /// `mount_apfs -o` takes them), the way an earlier process left it: the hosts under test
    /// learn of the mount from the kernel alone.
    fn mount_left_behind(&self, image: &Path, mount_point: &Path, options: &str) -> AttachedImage {
        let attachment = self
            .host()
            .backend()
            .attach_verified(image)
            .expect("attach a real image");
        self.remount(attachment.volume_device(), mount_point, options);
        attachment
    }

    /// Mount the attached `volume_device` at `mount_point` with exactly `options`.
    pub(super) fn remount(&self, volume_device: &str, mount_point: &Path, options: &str) {
        std::fs::create_dir_all(mount_point).expect("mount point");
        let mounted = SystemCommandRunner::new(self.host_namespace())
            .run(
                &CommandRequest::new(
                    "/sbin/mount_apfs",
                    [
                        "-o".as_ref(),
                        options.as_ref(),
                        volume_device.as_ref(),
                        mount_point.as_os_str(),
                    ],
                ),
                "fixture mount",
            )
            .expect("run mount_apfs")
            .output;
        assert!(
            mounted.succeeded(),
            "mount_apfs -o {options} {volume_device} {}: {}",
            mount_point.display(),
            String::from_utf8_lossy(&mounted.stderr)
        );
    }

    /// [`workspace`]'s in-image marker on the volume mounted at `mount_point`.
    fn plant_marker(&self, mount_point: &Path, workspace: &LifecycleWorkspace) {
        self.host()
            .write_marker(mount_point, workspace, None, &identity_at(self.root()))
            .expect("mount marker");
    }
}

/// The system runner of a helper that no fixture hands its namespace to: a namespace of its own.
fn helper_system() -> SystemCommandRunner {
    SystemCommandRunner::new(HostNamespace::Test(
        TestNamespace::mint().expect("a test namespace"),
    ))
}

fn unmount(mount: &Path) {
    let unmounted = helper_system()
        .run(
            &CommandRequest::new(
                "/usr/sbin/diskutil",
                ["unmount".as_ref(), mount.as_os_str()],
            ),
            "fixture unmount",
        )
        .expect("Disk Arbitration unmount")
        .output;
    assert!(
        unmounted.succeeded(),
        "diskutil unmount {}: {}",
        mount.display(),
        String::from_utf8_lossy(&unmounted.stderr)
    );
}

/// The mount ids of proven joins; a refused join fails the assertion with its refusal.
fn proven_mount_ids(joins: Vec<MountJoin>) -> Vec<u64> {
    joins
        .into_iter()
        .map(|join| join.named().expect("a proven mount join").mount_id)
        .collect()
}

/// What the kernel mounts at `mount_point` right now, if anything.
pub(super) fn kernel_mount_at(mount_point: &Path) -> Option<KernelMountSnapshot> {
    SystemKernelMountSource
        .mounts()
        .expect("getfsstat")
        .into_iter()
        .find(|mount| mount.mount_point == mount_point)
}

/// Every attachment the kernel's I/O Registry holds for `image` right now. `hdiutil info` is no
/// witness here: it omits attached images while another image attaches or detaches.
fn kernel_attachments(image: &Path) -> Vec<AttachedDiskImage> {
    helper_system()
        .attached_disk_images()
        .expect("kernel disk-image inventory")
        .into_iter()
        .filter(|attached| matches!(&attached.source, DiskImageSource::File(path) if path == image))
        .collect()
}

/// Whether the kernel holds `image` attached right now.
pub(super) fn attached(image: &Path) -> bool {
    !kernel_attachments(image).is_empty()
}

/// The kernel's filesystem attributes for a real mount.
fn filesystem_stat(mount_point: &Path) -> libc::statfs {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(mount_point.as_os_str().as_bytes()).expect("C path");
    // SAFETY: statfs writes only into the zeroed struct it is handed; the path is NUL-terminated.
    let mut stat = unsafe { std::mem::zeroed::<libc::statfs>() };
    assert_eq!(
        unsafe { libc::statfs(path.as_ptr(), &mut stat) },
        0,
        "statfs {}: {}",
        mount_point.display(),
        std::io::Error::last_os_error()
    );
    stat
}

fn filesystem_bytes(mount_point: &Path) -> u64 {
    let stat = filesystem_stat(mount_point);
    stat.f_blocks * u64::from(stat.f_bsize)
}

/// A published source image, attached once and mounted read-write and hidden at the main
/// mountpoint with its marker, credentials and payload, registered as [`workspace`]'s mount.
/// Returns the host, the image, the mountpoint and the volume's device, which a later remount
/// of the same attachment names.
fn live_capture_source(
    fixture: &RealFixture,
) -> (
    MacOsApfsExecutionHost<SystemCommandRunner>,
    PathBuf,
    PathBuf,
    String,
) {
    let source = fixture
        .layout()
        .main_image()
        .expect("source image")
        .image()
        .to_owned();
    fixture.published_image(&source);
    let mount = fixture.main_mount();
    let host = fixture.host();
    let attachment = host
        .backend()
        .attach_verified(&source)
        .expect("attach source");
    host.backend()
        .mount(&attachment, &mount, MountAccess::ReadWrite, false)
        .expect("initialize source");
    fixture.plant_marker(&mount, &workspace());
    mint_credentials(&workspace(), &mount, &ca_key_path(&source)).expect("source credentials");
    std::fs::write(mount.join("sealed-payload"), b"complete source bytes").expect("source payload");
    let volume_device = attachment.volume_device().to_owned();
    host.retain_mounted(&workspace(), attachment)
        .expect("registered source");
    (host, source, mount, volume_device)
}

/// A live source's identity as the kernel holds it: its mount (id, device and flags) and its
/// disk-image attachment. A live capture leaves all of it exactly as it found it.
#[derive(Debug, PartialEq, Eq)]
struct LiveSourceState {
    mount_id: u64,
    source_device: String,
    flags: u32,
    attachments: Vec<AttachedDiskImage>,
}

fn live_source_state(source: &Path, mount: &Path) -> LiveSourceState {
    let mounted = kernel_mount_at(mount).expect("the source is mounted");
    LiveSourceState {
        mount_id: mounted.mount_id,
        source_device: mounted.source_device,
        flags: filesystem_stat(mount).f_flags,
        attachments: kernel_attachments(source),
    }
}

/// Attach and verify the captured `image`, mount it read-only at `mount`, and check that it
/// carries the source's payload.
fn assert_capture_carries_source(
    host: &MacOsApfsExecutionHost<SystemCommandRunner>,
    image: &Path,
    mount: &Path,
) {
    let cloned = host
        .backend()
        .attach_verified(image)
        .expect("verify captured image");
    host.backend()
        .mount(&cloned, mount, MountAccess::ReadOnly, false)
        .expect("mount capture");
    assert_eq!(
        std::fs::read(mount.join("sealed-payload")).unwrap(),
        b"complete source bytes"
    );
}

/// The live rule (01a_apfs.md, "Live source snapshots"): unheld RW/RO captures and destination
/// refusals leave the source's mount, device, flags and attachment unchanged. A held RW capture
/// also succeeds without changing those facts. RW volume writes are flushed; RO has none, so the
/// RW capture's payload is the one read back through a mount of its own. One source attachment
/// serves both sources: the read-only browsable source is a remount of the same volume.
#[test]
fn real_apfs_live_capture_leaves_its_source_unchanged_on_success_and_refusal() {
    let fixture = RealFixture::new("live-capture");
    let (host, source, mount, volume_device) = live_capture_source(&fixture);
    for (label, access) in [
        ("rw-hidden", MountAccess::ReadWrite),
        ("ro-browse", MountAccess::ReadOnly),
    ] {
        if access == MountAccess::ReadOnly {
            unmount(&mount);
            fixture.remount(&volume_device, &mount, "rdonly,owners,noatime");
        }
        let before = live_source_state(&source, &mount);
        let destination = fixture.root().join(format!("{label}.dmg"));
        host.clone_image(&source, Some(&mount), &destination)
            .expect("live capture");
        assert_eq!(
            live_source_state(&source, &mount),
            before,
            "{label}: a live capture leaves the source's mount, device, flags and attachment as they were"
        );
        assert!(
            !attached(&destination),
            "{label}: captured image stays detached"
        );
        assert_eq!(
            std::fs::read(mount.join("sealed-payload")).unwrap(),
            b"complete source bytes"
        );

        // A refused capture never deletes a pre-existing target, and touches nothing else.
        let occupied = fixture.root().join(format!("{label}-occupied.dmg"));
        std::fs::write(&occupied, b"preserve destination").unwrap();
        let error = host
            .clone_image(&source, Some(&mount), &occupied)
            .expect_err("occupied target");
        assert!(error.to_string().contains("already exists"), "{error}");
        assert_eq!(std::fs::read(&occupied).unwrap(), b"preserve destination");
        assert_eq!(
            live_source_state(&source, &mount),
            before,
            "{label}: a refused capture leaves the source as it was"
        );

        if access == MountAccess::ReadWrite {
            let held = std::fs::File::open(mount.join("sealed-payload"))
                .expect("hold source during live capture");
            let held_destination = fixture.root().join("snapshot-held.dmg");
            host.clone_image(&source, Some(&mount), &held_destination)
                .expect("a live capture is not refused by a holder");
            assert_eq!(
                live_source_state(&source, &mount),
                before,
                "a held live capture leaves the source's mount, device, flags and attachment as they were"
            );
            assert!(!attached(&held_destination), "held capture stays detached");
            drop(held);
            assert_capture_carries_source(&host, &destination, &fixture.root().join(label));
        }
    }
    host.detach_mounted(&workspace(), DetachIntent::WhenIdle)
        .expect("source registry mapping");
    assert!(kernel_mount_at(&mount).is_none());
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("repo")
}

/// Mark `layout`'s owner as adoption's provisioning does, so the store-wide recovery pass reads
/// its project: it reads only marked owners (01_storage.md, "Directory layout").
fn adopt_owner(layout: &StorageLayout) {
    let owner = layout.project().project_root.parent().expect("owner");
    std::fs::create_dir_all(owner).expect("owner directory");
    cowshed_core::storage::bootstrap::write_owner_marker(owner).expect("owner marker");
}

fn execute_gc<R>(
    host: &MacOsApfsExecutionHost<R>,
    config: &ApfsSubstrateConfig,
) -> Result<cowshed_core::storage::lifecycle::StorageGcReport, ApfsStorageError>
where
    R: CommandRunner + Send + Sync + 'static,
{
    let plan = host.preview_gc(config, &repo(), StorageGcOptions::default())?;
    host.execute_gc(config, plan)
}

fn metadata() -> DetachedWorkspaceMetadata {
    DetachedWorkspaceMetadata {
        version: SIDECAR_VERSION,
        repo_id: repo(),
        workspace: WorkspaceName::new("main").expect("main"),
        workspace_incarnation: WorkspaceIncarnation::new("00000000000000000000000000000001")
            .expect("incarnation"),
        platform: Platform::Macos,
        publication_state: PublicationState::Active,
        updated_at: "2026-07-13T00:00:00Z".to_owned(),
        grants: GrantSet::closed_baseline(Some(
            PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("port block"),
        ))
        .expect("grants"),
        info_snapshot: WorkspaceInfoSnapshot {
            project_root: PathBuf::from("/project"),
            role: WorkspaceRole::Main,
            base_commit: "0123456789abcdef".to_owned(),
            branch: Some("main".to_owned()),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            forked_from: None,
            captured_at: "2026-07-13T00:00:00Z".to_owned(),
            stale: false,
            git_worktree: false,
        },
    }
}
fn set_metadata_workspace(metadata: &mut DetachedWorkspaceMetadata, workspace: WorkspaceName) {
    let role = if workspace.is_main() {
        WorkspaceRole::Main
    } else {
        WorkspaceRole::Workspace
    };
    metadata.workspace = workspace;
    metadata.info_snapshot.role = role;
}

fn write_session_metadata(image: &Path, workspace: &str, incarnation: &str) {
    let mut detached = metadata();
    set_metadata_workspace(
        &mut detached,
        WorkspaceName::session(workspace).expect("session workspace"),
    );
    detached.workspace_incarnation =
        WorkspaceIncarnation::new(incarnation).expect("workspace incarnation");
    detached
        .write_for_image(image)
        .expect("session detached metadata");
}

fn session_workspace(workspace: &str, incarnation: &str) -> LifecycleWorkspace {
    LifecycleWorkspace::new(
        repo(),
        WorkspaceName::session(workspace).expect("session workspace"),
        WorkspaceIncarnation::new(incarnation).expect("workspace incarnation"),
        Revision::new(0),
        Revision::new(0),
        WorkspaceRole::Workspace,
    )
    .expect("lifecycle workspace")
}

fn native_host(
    fixture: &Fixture,
    runner: RecordingRunner,
) -> MacOsApfsExecutionHost<RecordingRunner> {
    MacOsApfsExecutionHost::with_recovery_sources(
        runner,
        fixture.config(),
        SystemKernelMountSource,
        ByteRecoveryMarkers,
    )
    .expect("native APFS host")
}

fn native_host_at(
    namespace: HostNamespace,
    root: &Path,
) -> MacOsApfsExecutionHost<RecordingRunner> {
    MacOsApfsExecutionHost::with_recovery_sources(
        RecordingRunner::default(),
        ApfsSubstrateConfig::new(namespace, root, root.join("mount")),
        SystemKernelMountSource,
        ByteRecoveryMarkers,
    )
    .expect("native APFS host")
}

fn identity(fixture: &Fixture) -> OperationIdentity {
    identity_at(&fixture.root)
}

fn identity_at(root: &Path) -> OperationIdentity {
    OperationIdentity {
        project_root: root.join("project"),
        base_commit: "0123456789abcdef".to_owned(),
        created_at: "2026-07-13T00:00:00Z".to_owned(),
        branch: Some("main".to_owned()),
        forked_from: None,
        created_trace: "trace-apfs-boundary".to_owned(),
        git_worktree: false,
        grants: GrantSet::closed_baseline(Some(
            PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("port block"),
        ))
        .expect("grants"),
    }
}

fn set_modified(path: &Path, seconds_since_epoch: u64) {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open image for timestamp");
    file.set_times(
        FileTimes::new()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds_since_epoch)),
    )
    .expect("set image timestamp");
}

fn make_old(path: &Path) {
    set_modified(path, 946_684_800);
}

fn make_future(path: &Path) {
    set_modified(path, 4_102_444_800);
}

fn checkpoint_fact_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".checkpoint.json");
    PathBuf::from(path)
}

fn restore_recovery_fact_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".restore.json");
    PathBuf::from(path)
}

fn ca_key_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".ca.key");
    PathBuf::from(path)
}

fn write_ca_key(image: &Path, contents: &[u8]) {
    let path = ca_key_path(image);
    std::fs::write(&path, contents).expect("CA key");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("CA key mode");
}

fn create_image(path: &Path) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("image parent");
    std::fs::write(path, b"fixture").expect("image");
    metadata().write_for_image(path).expect("sidecar");
    let signing_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("fixture P-256 private key");
    write_ca_key(path, signing_key.serialize_pem().as_bytes());
}

fn workspace() -> LifecycleWorkspace {
    LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        WorkspaceIncarnation::new("00000000000000000000000000000001").expect("incarnation"),
        Revision::new(1),
        Revision::new(1),
        WorkspaceRole::Main,
    )
    .expect("workspace")
}

fn mint_credentials(
    workspace: &LifecycleWorkspace,
    image_mount: &Path,
    private_key_path: &Path,
) -> Result<(), cowshed_core::workspace_credentials::WorkspaceCredentialError> {
    mint_workspace_credentials(
        workspace,
        image_mount,
        Platform::Macos,
        Some(PortBlock::new(40_960, 16).expect("port block")),
        private_key_path,
    )
}

#[test]
fn a_clone_into_a_path_that_is_not_a_dmg_image_fails_before_any_command() {
    let fixture = Fixture::new("preflight");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let source = layout.main_image().expect("source");
    create_image(source.image());
    let bad_destination = layout.project().sessions.join("wrong.sparseimage");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());

    let clone_error = host
        .clone_image(source.image(), None, &bad_destination)
        .expect_err("a destination that is not .dmg");
    assert!(
        clone_error
            .to_string()
            .contains("every workspace image is .dmg"),
        "{clone_error}"
    );
    assert!(!bad_destination.exists());
    assert_eq!(runner.calls(), 0);
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_restore_swap_keeps_old_metadata_until_verified_publication_and_rolls_back_generations()
 {
    let fixture = Fixture::new("restore-swap");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.dmg");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.dmg");
    create_image(canonical.image());
    std::fs::write(canonical.image(), b"old generation").expect("old image");
    create_image(&staged);
    std::fs::write(&staged, b"new generation").expect("new image");
    let mut next_metadata = metadata();
    next_metadata.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("next incarnation");
    next_metadata
        .write_for_image(&staged)
        .expect("next metadata");
    let host = native_host(&fixture, RecordingRunner::default());

    host.restore_swap(&staged, canonical.image(), &undo)
        .expect("restore swap");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"new generation"
    );
    assert_eq!(std::fs::read(&undo).expect("undo"), b"old generation");
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("pre-publication metadata")
            .workspace_incarnation,
        metadata().workspace_incarnation
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&undo)
            .expect("undo metadata")
            .workspace_incarnation,
        metadata().workspace_incarnation
    );

    host.rollback_restore(canonical.image(), &undo, &staged)
        .expect("rollback");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"old generation"
    );
    assert!(!staged.exists());
    assert!(!sidecar_path(&staged).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("restored metadata")
            .workspace_incarnation,
        metadata().workspace_incarnation
    );
}

#[test]
fn stats_count_only_images_and_gc_preserves_recorded_recovery_trash() {
    let fixture = Fixture::new("stats-gc");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    create_image(canonical.image());
    let checkpoints = &layout.project().checkpoints.join("main");
    let first = checkpoints.join("one.dmg");
    let second = checkpoints.join("two.dmg");
    let pre_restore = checkpoints.join("pre-restore-00000000000000000000000000000002.dmg");
    create_image(&first);
    create_image(&second);
    create_image(&pre_restore);
    std::fs::write(checkpoints.join("not-an-image.txt"), b"ignored").expect("noise");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    host.publish_checkpoint_fact(
        &first,
        &CheckpointLabel::new("one").expect("label"),
        Revision::new(2),
        Pin::Pinned,
    )
    .expect("pinned fact");
    host.publish_checkpoint_fact(
        &second,
        &CheckpointLabel::new("two").expect("label"),
        Revision::new(3),
        Pin::Automatic,
    )
    .expect("automatic fact");

    let stats = host.stats(&workspace(), canonical.image()).expect("stats");
    assert_eq!(stats.logical_bytes, b"fixture".len() as u64);
    assert_eq!(stats.checkpoint_count, 3);
    let first_bytes = std::fs::metadata(&first)
        .expect("first metadata")
        .blocks()
        .saturating_mul(512);
    let second_bytes = std::fs::metadata(&second)
        .expect("second metadata")
        .blocks()
        .saturating_mul(512);
    let pre_restore_bytes = std::fs::metadata(&pre_restore)
        .expect("pre-restore metadata")
        .blocks()
        .saturating_mul(512);
    assert_eq!(
        stats.checkpoint_bytes,
        first_bytes + second_bytes + pre_restore_bytes
    );
    assert_eq!(stats.pinned_checkpoint_bytes, first_bytes);

    let active_name = WorkspaceName::session("active").expect("workspace");
    let active = layout.session_image(&active_name).expect("active");
    create_image(active.image());
    let mut active_metadata = metadata();
    set_metadata_workspace(&mut active_metadata, active_name);
    active_metadata
        .write_for_image(active.image())
        .expect("active metadata");
    let trash = layout
        .project()
        .sessions
        .join(".trash/retired-00000000000000000000000000000001.dmg");
    create_image(&trash);
    let mut retired_metadata = metadata();
    set_metadata_workspace(
        &mut retired_metadata,
        WorkspaceName::session("retired").expect("retired workspace"),
    );
    retired_metadata
        .write_for_image(&trash)
        .expect("retired metadata");
    let cache_image = fixture.root.join("caches/acme/sessions/cache.dmg");
    create_image(&cache_image);

    let report = execute_gc(&host, &fixture.config()).expect("gc");
    // Recovery trash is evidence, not a candidate; the two checkpoints retain their facts.
    assert_eq!(report.examined, 2);
    assert_eq!(report.reclaimed, 0);
    assert!(trash.exists());
    assert!(sidecar_path(&trash).exists());
    assert!(active.image().exists());
    assert!(
        cache_image.exists(),
        "GC must not descend into the caches volume"
    );
    assert_eq!(
        runner.calls(),
        0,
        "GC of detached images runs no host command"
    );
}

#[test]
fn gc_sweeps_around_an_unpublished_clone_without_deleting_it() {
    // A `new` killed after its sidecar went down leaves the clone PendingFence under its
    // canonical name. That is lifecycle state — finished by its intent, or retired by the
    // project once nothing is creating it — never a detached image, and one such residue must
    // not fail the reclaim of everything else in the project.
    let fixture = Fixture::new("pending-clone-gc");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let pending_name = WorkspaceName::session("pending").expect("workspace");
    let pending = layout.session_image(&pending_name).expect("pending");
    create_image(pending.image());
    let mut pending_metadata = metadata();
    set_metadata_workspace(&mut pending_metadata, pending_name);
    pending_metadata.publication_state = PublicationState::PendingFence;
    pending_metadata
        .write_for_image(pending.image())
        .expect("pending metadata");
    let trash = layout
        .project()
        .sessions
        .join(".trash/retired-00000000000000000000000000000001.dmg");
    create_image(&trash);
    let mut retired_metadata = metadata();
    set_metadata_workspace(
        &mut retired_metadata,
        WorkspaceName::session("retired").expect("retired workspace"),
    );
    retired_metadata
        .write_for_image(&trash)
        .expect("retired metadata");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());

    let report = execute_gc(&host, &fixture.config()).expect("an unpublished clone cannot fail gc");
    assert_eq!(
        report.reclaimed, 0,
        "neither pending nor recovery work is garbage"
    );
    assert!(trash.exists());
    assert!(
        pending.image().exists(),
        "gc never deletes an unpublished clone"
    );
    assert!(sidecar_path(pending.image()).exists());
    assert_eq!(
        runner.calls(),
        0,
        "gc of an unpublished clone runs no host command"
    );
}

/// The abandon bundles `rm --abandon` keeps in the trash are the only copy of the commits that
/// removal gave up, so a sweep reclaims them only when it is asked to: without the option they
/// are not candidates and survive a run, with it they are named at their real allocated bytes
/// and reclaimed, and a bundle that appeared after the preview makes the plan stale instead of
/// being swept by a plan that never named it.
#[test]
fn gc_reclaims_abandon_bundles_only_under_the_abandoned_sweep() {
    let fixture = Fixture::new("abandon-bundles");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let project = layout.project().project_root.clone();
    let trash = layout.project().sessions.join(".trash");
    std::fs::create_dir_all(&trash).expect("trash");
    let bundle = trash.join("raven-0123456789abcdef0123456789abcdef01234567.bundle");
    std::fs::write(&bundle, vec![b'x'; 512 * 1024]).expect("bundle");
    // The name `git bundle create` stages under is not a bundle: hidden, `.partial`, and not the
    // terminal name a removal renames into place.
    let partial = trash.join(".raven-0123456789abcdef0123456789abcdef01234567.bundle.0f.partial");
    std::fs::write(&partial, b"half-written").expect("staged bundle");
    let host = native_host(&fixture, RecordingRunner::default());
    let config = fixture.config();

    let default_plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("default preview");
    assert!(
        !default_plan
            .candidates()
            .iter()
            .any(|candidate| matches!(candidate.reason(), StorageGcReason::AbandonedBundle)),
        "a plain sweep never names an abandon bundle: {default_plan:?}"
    );
    let report = host.execute_gc(&config, default_plan).expect("plain sweep");
    assert_eq!(report.released_bytes, 0, "a plain sweep frees nothing here");
    assert!(
        bundle.exists(),
        "a plain gc leaves the abandoned commits fetchable"
    );

    let plan = host
        .preview_gc(
            &config,
            &repo(),
            StorageGcOptions {
                abandoned_bundles: true,
            },
        )
        .expect("abandoned preview");
    let candidate = plan
        .candidates()
        .iter()
        .find(|candidate| candidate.reason() == StorageGcReason::AbandonedBundle)
        .expect("the bundle is a candidate");
    assert_eq!(candidate.path(), bundle.as_path());
    assert_eq!(
        candidate.bytes(),
        512 * 1024,
        "the candidate reports the bundle's allocated bytes"
    );

    // A bundle another removal wrote after the preview is a change to the store the plan names,
    // so the unchanged-plan rule refuses rather than sweeping around it.
    let appeared = trash.join("heron-fedcba9876543210fedcba9876543210fedcba98.bundle");
    std::fs::write(&appeared, b"another abandonment").expect("late bundle");
    let error = host
        .execute_gc(&config, plan)
        .expect_err("a bundle that appeared makes the plan stale");
    assert!(matches!(error, ApfsStorageError::GcPlanStale));
    assert!(
        bundle.exists() && appeared.exists(),
        "a stale plan reclaims nothing"
    );
    std::fs::remove_file(&appeared).expect("retract the late bundle");

    let plan = host
        .preview_gc(
            &config,
            &repo(),
            StorageGcOptions {
                abandoned_bundles: true,
            },
        )
        .expect("abandoned preview");
    let expected = plan
        .candidates()
        .iter()
        .find(|candidate| candidate.reason() == StorageGcReason::AbandonedBundle)
        .expect("the bundle is a candidate")
        .bytes();
    let report = host.execute_gc(&config, plan).expect("abandoned sweep");
    assert_eq!(report.reclaimed, 1, "the bundle is the only candidate");
    assert_eq!(
        report.released_bytes, expected,
        "the report names the bytes the bundle held"
    );
    assert!(!bundle.exists(), "the abandoned commits are reclaimed");
    assert!(
        partial.exists() && !appeared.exists(),
        "only the bundle the plan named was reclaimed"
    );
    let log = std::fs::read_to_string(project.join(DELETION_LOG_FILE)).expect("deletion log");
    assert!(
        log.contains("remove-abandoned-bundle")
            && log.contains(bundle.to_str().expect("bundle path")),
        "the reclaim is journaled like every other deletion: {log}"
    );
}

/// A store written when images could also be SPARSE: `main.sparseimage` and a session's
/// `x.sparseimage`, each beside a sidecar that names `"imageFormat":"sparse"`. Every image is
/// `<name>.dmg` now, so store-wide scans never read those files: recovery, listing and GC
/// neither fail on them nor report them, and leave them exactly as they were.
#[test]
fn sparse_images_and_their_sidecars_are_invisible_to_recovery_listing_and_gc() {
    let fixture = Fixture::new("sparse-leftovers");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let main = layout.main_image().expect("main");
    create_image(main.image());
    let sparse_sidecar = |workspace: WorkspaceName| {
        let mut detached = metadata();
        set_metadata_workspace(&mut detached, workspace);
        let mut json = serde_json::to_value(&detached).expect("sidecar JSON");
        json.as_object_mut()
            .expect("sidecar object")
            .insert("imageFormat".to_owned(), serde_json::json!("sparse"));
        serde_json::to_vec_pretty(&json).expect("sidecar bytes")
    };
    let project = &layout.project().project_root;
    let main_sparse = project.join("main.sparseimage");
    let session_sparse = layout.project().sessions.join("x.sparseimage");
    let leftovers = [
        (main_sparse.clone(), sparse_sidecar(WorkspaceName::main())),
        (
            session_sparse.clone(),
            sparse_sidecar(WorkspaceName::session("x").expect("session")),
        ),
    ];
    for (image, sidecar) in &leftovers {
        std::fs::create_dir_all(image.parent().expect("parent")).expect("parent");
        std::fs::write(image, b"sparse bundle").expect("sparse image");
        std::fs::write(sidecar_path(image), sidecar).expect("sparse sidecar");
    }
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let config = fixture.config();

    host.recover_pending(&config, &[])
        .expect("recovery does not read a SPARSE sidecar");
    assert!(
        host.pending_publications(&repo())
            .expect("pending publications")
            .is_empty()
    );
    let facts = host
        .list(&repo())
        .expect("listing does not read a SPARSE sidecar");
    assert_eq!(facts.len(), 1, "only main.dmg is a workspace");
    assert!(facts[0].workspace.name().is_main());
    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("GC does not read a SPARSE sidecar");
    assert!(
        plan.candidates().is_empty(),
        "a SPARSE file is no GC candidate: {:?}",
        plan.candidates()
            .iter()
            .map(|candidate| candidate.path().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(plan.examined(), 0);

    for (image, sidecar) in &leftovers {
        assert_eq!(std::fs::read(image).expect("image"), b"sparse bundle");
        assert_eq!(
            &std::fs::read(sidecar_path(image)).expect("sidecar"),
            sidecar
        );
    }
    assert_eq!(runner.calls(), 0, "nothing is attached to look at them");
}

#[test]
fn listing_skips_sidecarless_session_images_that_gc_names_and_reclaims() {
    let fixture = Fixture::new("orphan-session-images");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    std::fs::create_dir_all(&layout.project().sessions).expect("sessions directory");
    let main = layout.main_image().expect("main");
    create_image(main.image());

    let orphan_raw = layout.project().sessions.join("orphan-raw.dmg");
    std::fs::write(&orphan_raw, b"fixture").expect("sidecarless RAW image");
    let orphan_sparseimage = layout
        .project()
        .sessions
        .join("orphan-sparseimage.sparseimage");
    std::fs::write(&orphan_sparseimage, b"non-RAW file").expect("non-RAW sparseimage");

    let published_name = WorkspaceName::session("published").expect("workspace");
    let published = layout
        .session_image(&published_name)
        .expect("published image");
    create_image(published.image());
    let mut published_metadata = metadata();
    set_metadata_workspace(&mut published_metadata, published_name);
    published_metadata
        .write_for_image(published.image())
        .expect("published sidecar");

    let host = native_host(&fixture, RecordingRunner::default());
    let config = fixture.config();
    let listed = host.list(&repo()).expect("sidecarless images are skipped");
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|fact| fact.workspace.name().is_main()));
    assert!(
        listed
            .iter()
            .any(|fact| fact.workspace.name().as_str() == "published")
    );

    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("orphan plan");
    assert!(
        plan.candidates().iter().any(|candidate| {
            candidate.path() == orphan_raw.as_path()
                && candidate.reason() == StorageGcReason::OrphanSessionImage
        }),
        "doctor-facing GC candidates name the sidecarless RAW image"
    );
    assert!(
        plan.candidates()
            .iter()
            .all(|candidate| candidate.path() != orphan_sparseimage.as_path()),
        "a non-RAW file is never a RAW orphan"
    );

    let report = host
        .execute_gc(&config, plan)
        .expect("reclaim sidecarless images");
    assert_eq!(report.reclaimed, 1);
    assert!(report.deferred.is_empty());
    assert!(!orphan_raw.exists());
    assert_eq!(std::fs::read(&orphan_sparseimage).unwrap(), b"non-RAW file");
    assert!(published.image().exists());
    assert!(sidecar_path(published.image()).exists());
}

#[test]
fn malformed_session_sidecar_is_not_treated_as_a_missing_sidecar_orphan() {
    let fixture = Fixture::new("malformed-session-sidecar");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.project().sessions.join("corrupt.dmg");
    create_image(&image);
    std::fs::write(sidecar_path(&image), b"{ malformed").expect("malformed sidecar");
    let host = native_host(&fixture, RecordingRunner::default());

    assert!(
        host.list(&repo()).is_err(),
        "a present malformed grants sidecar keeps its integrity refusal"
    );
    let plan = host
        .preview_gc(&fixture.config(), &repo(), StorageGcOptions::default())
        .expect("the malformed image is not an orphan candidate");
    assert!(
        !plan
            .candidates()
            .iter()
            .any(|candidate| candidate.path() == image.as_path())
    );
    assert!(image.exists());
    assert!(sidecar_path(&image).exists());
}

/// A session image the kernel still mounts or still has attached is deferred with the reason,
/// never deleted, and the sweep carries on to the candidates after it. Both holds are real: one
/// image's volume is mounted at its workspace mount point, another is attached without a mount.
#[test]
fn real_apfs_gc_reports_item_local_deferrals_and_reclaims_later_session_images() {
    let fixture = RealFixture::new("gc-session-deferrals");
    let layout = fixture.layout();
    std::fs::create_dir_all(&layout.project().sessions).expect("sessions directory");
    let mounted_image = layout.project().sessions.join("a-mounted.dmg");
    fixture.blank_image(&mounted_image);
    let later_image = layout.project().sessions.join("z-reclaim.dmg");
    std::fs::write(&later_image, b"fixture").expect("sidecarless later image");

    let locked_name = WorkspaceName::session("b-locked").expect("workspace");
    let locked_paths = layout
        .session_image(&locked_name)
        .expect("locked image paths");
    std::fs::write(locked_paths.image(), b"fixture").expect("sidecarless locked image");
    let lock_file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(locked_paths.lock())
        .expect("workspace lifecycle lock");
    lock_file
        .try_lock()
        .expect("hold the workspace lifecycle lock");

    use std::os::unix::fs::symlink;
    let symlink_image = layout.project().sessions.join("y-symlink.sparseimage");
    let symlink_target = fixture.root().join("external.sparseimage");
    std::fs::write(&symlink_target, b"outside image").expect("external image");
    symlink(&symlink_target, &symlink_image).expect("non-RAW image symlink");
    let attached_image = layout.project().sessions.join("c-attached.dmg");
    fixture.blank_image(&attached_image);
    fixture
        .host()
        .backend()
        .attach_verified(&attached_image)
        .expect("attach without mounting");

    let mount_point = layout.project().mount_root.join("a-mounted");
    fixture.mount_left_behind(&mounted_image, &mount_point, "nobrowse,owners");

    // The stray is judged against the project's repository; with none to judge against, the
    // deferral names the failed judgement instead of the file.
    let checkout = fixture.config().checkout_path;
    std::fs::create_dir_all(&checkout).expect("project checkout");
    let status = Command::new("git")
        .args(["init", "-q"])
        .arg(&checkout)
        .stdout(Stdio::null())
        .spawn_locked()
        .expect("git init")
        .wait_bounded()
        .expect("wait for git init");
    assert!(status.success(), "git init the project checkout");
    let orphan_mountpoint = layout.project().mount_root.join("stray");
    std::fs::create_dir_all(&orphan_mountpoint).expect("orphan mountpoint");
    let kept_work = orphan_mountpoint.join("unsaved.rs");
    std::fs::write(&kept_work, b"unsaved work").expect("visible user work");

    let config = fixture.config();
    let host = fixture.host();
    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("GC plan");
    assert!(plan.candidates().iter().any(|candidate| {
        candidate.path() == mounted_image.as_path()
            && candidate.reason() == StorageGcReason::OrphanSessionImage
    }));
    assert!(plan.candidates().iter().any(|candidate| {
        candidate.path() == later_image.as_path()
            && candidate.reason() == StorageGcReason::OrphanSessionImage
    }));
    assert!(plan.candidates().iter().any(|candidate| {
        candidate.path() == attached_image.as_path()
            && candidate.reason() == StorageGcReason::OrphanSessionImage
    }));
    assert!(
        !plan
            .candidates()
            .iter()
            .any(|candidate| candidate.path() == locked_paths.image()),
        "an orphan image under its workspace lock is not a candidate"
    );
    assert!(
        !plan
            .candidates()
            .iter()
            .any(|candidate| candidate.path() == symlink_image.as_path()),
        "a symlink is never an orphan image candidate"
    );

    let report = host.execute_gc(&config, plan).expect("deferred GC sweep");
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.deferred.len(), 3);
    assert_eq!(report.retained_active, 1);
    assert!(report.deferred.iter().any(|candidate| {
        candidate.path == mounted_image && candidate.diagnostic.contains("mounted")
    }));
    assert!(report.deferred.iter().any(|candidate| {
        candidate.path == attached_image && candidate.diagnostic.contains("attached")
    }));
    assert!(report.deferred.iter().any(|candidate| {
        candidate.path == orphan_mountpoint && candidate.diagnostic.contains("unsaved.rs")
    }));
    assert!(mounted_image.exists(), "mounted image is never deleted");
    assert!(
        kernel_mount_at(&mount_point).is_some(),
        "the deferral leaves the volume mounted"
    );
    assert!(
        attached(&attached_image),
        "the deferral leaves the image attached"
    );
    assert!(
        !later_image.exists(),
        "a deferred item does not stop later reclamation"
    );
    assert!(kept_work.exists(), "visible mountpoint work is preserved");
    assert!(
        attached_image.exists(),
        "an attached image is never deleted"
    );
    assert!(
        locked_paths.image().exists(),
        "an active lock protects its image"
    );
    assert!(
        std::fs::symlink_metadata(&symlink_image)
            .expect("symlink remains")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(&symlink_target).expect("external image remains"),
        b"outside image"
    );
}

#[test]
fn gc_defers_an_unreadable_inventory_and_reclaims_later_candidates() {
    let fixture = Fixture::new("gc-inventory-unreadable");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    std::fs::create_dir_all(&layout.project().sessions).expect("sessions directory");
    let deferred_image = layout.project().sessions.join("a-deferred.dmg");
    std::fs::write(&deferred_image, b"inventory unreadable").expect("deferred image");
    let later_image = layout.project().sessions.join("z-reclaim.dmg");
    std::fs::write(&later_image, b"later candidate").expect("later image");
    let config = fixture.config();
    let host = MacOsApfsExecutionHost::with_mount_source(
        UnreadableFirstInventoryRunner::default(),
        config.clone(),
        SystemKernelMountSource,
    )
    .expect("native APFS host");
    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("GC plan");

    let report = host
        .execute_gc(&config, plan)
        .expect("item-local inventory failure");
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.deferred.len(), 1);
    assert_eq!(report.deferred[0].path, deferred_image);
    assert!(
        report.deferred[0]
            .diagnostic
            .contains("injected I/O Registry snapshot failure"),
        "{}",
        report.deferred[0].diagnostic
    );
    assert!(deferred_image.exists());
    assert!(!later_image.exists());
}

#[test]
fn gc_stale_orphan_image_identity_preserves_same_size_replacement() {
    let fixture = Fixture::new("gc-orphan-identity");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    std::fs::create_dir_all(&layout.project().sessions).expect("sessions directory");
    let image = layout.project().sessions.join("orphan.dmg");
    let later = layout.project().sessions.join("later.dmg");
    std::fs::write(&image, b"fixture").expect("sidecarless orphan image");
    std::fs::write(&later, b"fixture").expect("later orphan image");
    let config = fixture.config();
    let host = native_host(&fixture, RecordingRunner::default());
    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("GC plan");
    let original = std::fs::metadata(&image).expect("original metadata");

    let replacement = layout.project().sessions.join(".replacement");
    std::fs::write(&replacement, b"replace").expect("same-sized replacement");
    let replacement_metadata = {
        let file = OpenOptions::new()
            .write(true)
            .open(&replacement)
            .expect("replacement file");
        file.set_times(
            FileTimes::new().set_modified(original.modified().expect("original timestamp")),
        )
        .expect("preserve replacement timestamp");
        std::fs::metadata(&replacement).expect("replacement metadata")
    };
    assert_eq!(replacement_metadata.len(), original.len());
    assert_eq!(replacement_metadata.blocks(), original.blocks());
    assert_eq!(
        replacement_metadata.modified().unwrap(),
        original.modified().unwrap()
    );
    assert_ne!(replacement_metadata.ino(), original.ino());
    std::fs::rename(&replacement, &image).expect("replace orphan image");

    let error = host
        .execute_gc(&config, plan)
        .expect_err("a same-path replacement makes the GC plan stale");
    assert!(matches!(error, ApfsStorageError::GcPlanStale));
    assert_eq!(
        std::fs::metadata(&image)
            .expect("replacement remains")
            .ino(),
        replacement_metadata.ino()
    );
    assert_eq!(
        std::fs::read(&image).expect("replacement bytes"),
        b"replace"
    );
    assert!(
        later.exists(),
        "a stale identity is rejected before mutation"
    );
}

#[test]
fn marker_validation_checks_every_detached_identity_dimension() {
    let fixture = Fixture::new("marker");
    let host = native_host(&fixture, RecordingRunner::default());
    let workspace = workspace();
    let mount = fixture.root.join("mounted");
    host.write_marker(&mount, &workspace, None, &identity(&fixture))
        .expect("write marker");
    let private_key = fixture.root.join("marker.ca.key");
    mint_credentials(&workspace, &mount, &private_key).expect("workspace credentials");
    let expected = MarkerExpectation {
        repos: OwnedRepoIds::sole(workspace.repo().clone()),
        workspace: workspace.name().clone(),
        incarnation: workspace.incarnation().clone(),
    };
    host.validate_marker(&mount, &expected)
        .expect("matching marker");

    let mismatches = [
        MarkerExpectation {
            repos: OwnedRepoIds::sole(RepoId::parse("other/widget").expect("repo")),
            ..expected.clone()
        },
        MarkerExpectation {
            workspace: WorkspaceName::session("other").expect("workspace"),
            ..expected.clone()
        },
        MarkerExpectation {
            incarnation: WorkspaceIncarnation::new("00000000000000000000000000000009")
                .expect("incarnation"),
            ..expected
        },
    ];
    for mismatch in mismatches {
        assert!(matches!(
            host.validate_marker(&mount, &mismatch),
            Err(ApfsStorageError::MarkerMismatch(_))
        ));
    }
}

/// `write_marker` stamps the owned-link revision a marker's history earns: the current revision for
/// a fresh image and for a fork, a source's own revision for a same-workspace copy. Each case
/// writes into a fresh destination with a new incarnation, so no case takes the idempotent branch.
#[test]
fn write_marker_owned_links_revision_follows_the_copy_that_produced_the_marker() {
    use cowshed_core::metadata::{MARKER_VERSION, OWNED_LINKS_REVISION, WorkspaceMarker};

    let fixture = Fixture::new("owned-links-revision");
    let host = native_host(&fixture, RecordingRunner::default());
    let main = workspace();
    let topic = session_workspace("topic", "00000000000000000000000000000002");
    let fork_source = WorkspaceName::main();

    // A marker a copy carried in, as the copied tree's own: `revision` is what the source recorded.
    let copied = |mount: &Path, name: &str, incarnation: &str, revision: u8| {
        std::fs::create_dir_all(mount.join(".cowshed")).expect("copied marker directory");
        let legacy = WorkspaceMarker {
            version: MARKER_VERSION,
            repo_id: repo(),
            project_root: fixture.root.join("project"),
            workspace: WorkspaceName::new(name).expect("workspace"),
            workspace_incarnation: WorkspaceIncarnation::new(incarnation).expect("incarnation"),
            role: if name == "main" {
                WorkspaceRole::Main
            } else {
                WorkspaceRole::Workspace
            },
            base_commit: "0123456789abcdef".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            forked_from: None,
            created_trace: "trace-apfs-boundary".to_owned(),
            lineage: None,
            owned_links_revision: revision,
        };
        cowshed_core::metadata::write_json(&mount.join(".cowshed/workspace.json"), &legacy)
            .expect("copied marker");
    };
    let read_revision = |mount: &Path| {
        WorkspaceMarker::read_from(&mount.join(".cowshed/workspace.json"))
            .expect("marker")
            .owned_links_revision
    };

    // (1) A fresh image with no copied marker starts at the current revision.
    let fresh = fixture.root.join("fresh");
    host.write_marker(&fresh, &main, None, &identity(&fixture))
        .expect("fresh marker");
    assert_eq!(read_revision(&fresh), OWNED_LINKS_REVISION, "fresh image");

    // (2) A fork whose clone carried a legacy source-main marker starts at the current revision: a
    // fork is a new mint, whatever its source's marker says.
    let forked = fixture.root.join("forked");
    copied(&forked, "main", "00000000000000000000000000000001", 0);
    let fork_identity = OperationIdentity {
        forked_from: Some(fork_source.clone()),
        ..identity(&fixture)
    };
    host.write_marker(&forked, &topic, Some(&fork_source), &fork_identity)
        .expect("fork marker");
    assert_eq!(
        read_revision(&forked),
        OWNED_LINKS_REVISION,
        "fork of a legacy source"
    );

    // (3) A same-workspace copy with a new incarnation and no fork keeps the source's legacy 0.
    let legacy_copy = fixture.root.join("legacy-copy");
    copied(&legacy_copy, "main", "00000000000000000000000000000001", 0);
    host.write_marker(
        &legacy_copy,
        &main_with_incarnation("00000000000000000000000000000003"),
        None,
        &identity(&fixture),
    )
    .expect("legacy restore copy");
    assert_eq!(
        read_revision(&legacy_copy),
        0,
        "a legacy copy still migrates once"
    );

    // (4) A same-workspace copy of a marker already at the current revision keeps it.
    let current_copy = fixture.root.join("current-copy");
    copied(
        &current_copy,
        "main",
        "00000000000000000000000000000001",
        OWNED_LINKS_REVISION,
    );
    host.write_marker(
        &current_copy,
        &main_with_incarnation("00000000000000000000000000000004"),
        None,
        &identity(&fixture),
    )
    .expect("current restore copy");
    assert_eq!(
        read_revision(&current_copy),
        OWNED_LINKS_REVISION,
        "a current copy stays current"
    );
}

/// The main workspace with a different incarnation: a copy's new timeline, never its source's.
fn main_with_incarnation(incarnation: &str) -> LifecycleWorkspace {
    LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        WorkspaceIncarnation::new(incarnation).expect("incarnation"),
        Revision::new(1),
        Revision::new(1),
        WorkspaceRole::Main,
    )
    .expect("workspace")
}

#[test]
fn retirement_moves_image_and_sidecar_atomically_and_reclaim_is_idempotent() {
    let fixture = Fixture::new("retire");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    create_image(canonical.image());
    let record = std::fs::read(sidecar_path(canonical.image())).expect("canonical record");
    let key = std::fs::read(ca_key_path(canonical.image())).expect("canonical key");
    let trash = layout
        .project()
        .sessions
        .join(".trash/main-00000000000000000000000000000001.dmg");
    let host = native_host(&fixture, RecordingRunner::default());

    host.retire_image(canonical.image(), &trash)
        .expect("retire");
    assert!(!canonical.image().exists());
    assert!(!sidecar_path(canonical.image()).exists());
    assert!(!ca_key_path(canonical.image()).exists());
    assert!(trash.exists());
    assert_eq!(
        std::fs::read(sidecar_path(&trash)).expect("trash record"),
        record
    );
    assert_eq!(std::fs::read(ca_key_path(&trash)).expect("trash key"), key);

    host.reclaim_image(&trash).expect("first reclaim");
    host.reclaim_image(&trash).expect("idempotent reclaim");
    assert!(!trash.exists());
    assert!(!sidecar_path(&trash).exists());
    assert!(!ca_key_path(&trash).exists());
}

#[test]
fn restore_swap_rejects_an_undo_sidecar_without_touching_generations() {
    let fixture = Fixture::new("undo-sidecar-conflict");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.dmg");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.dmg");
    create_image(canonical.image());
    std::fs::write(canonical.image(), b"old").expect("old");
    create_image(&staged);
    std::fs::write(&staged, b"new").expect("new");
    std::fs::create_dir_all(undo.parent().expect("undo parent")).expect("undo parent");
    std::fs::write(sidecar_path(&undo), b"occupied").expect("undo sidecar");
    let host = native_host(&fixture, RecordingRunner::default());

    host.restore_swap(&staged, canonical.image(), &undo)
        .expect_err("undo sidecar is a conflict");

    assert_eq!(std::fs::read(canonical.image()).expect("canonical"), b"old");
    assert_eq!(std::fs::read(&staged).expect("staged"), b"new");
    assert!(!undo.exists());
}

#[test]
fn metadata_publication_writes_the_requested_identity_and_revision() {
    let fixture = Fixture::new("metadata-publication");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image().expect("image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("parent");
    std::fs::write(image.image(), b"image").expect("image");
    let host = native_host(&fixture, RecordingRunner::default());
    let workspace = workspace();

    host.publish_metadata(
        image.image(),
        &workspace,
        Revision::new(17),
        MetadataPolicy::Fresh,
        Some(&identity(&fixture)),
        None,
    )
    .expect("publish metadata");

    let published =
        DetachedWorkspaceMetadata::read_for_image(image.image()).expect("published metadata");
    assert_eq!(published.repo_id, *workspace.repo());
    assert_eq!(published.workspace, *workspace.name());
    assert_eq!(published.workspace_incarnation, *workspace.incarnation());
    assert_eq!(published.grants.revision, 17);
    assert_eq!(
        published.info_snapshot,
        cowshed_core::metadata::WorkspaceInfoSnapshot {
            project_root: fixture.root.join("project"),
            role: WorkspaceRole::Main,
            base_commit: "0123456789abcdef".to_owned(),
            branch: Some("main".to_owned()),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            forked_from: None,
            captured_at: "2026-07-13T00:00:00Z".to_owned(),
            stale: false,
            git_worktree: false,
        }
    );
}

#[test]
fn fresh_session_metadata_persists_branch_fork_and_original_project_root() {
    let fixture = Fixture::new("session-info-snapshot");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let workspace = session_workspace("raven", "00000000000000000000000000000002");
    let image = layout
        .session_image(workspace.name())
        .expect("session image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("parent");
    std::fs::write(image.image(), b"image").expect("image");
    let host = native_host(&fixture, RecordingRunner::default());
    let mut operation = identity(&fixture);
    operation.branch = Some("cowshed/raven".to_owned());
    operation.forked_from = Some(WorkspaceName::new("main").expect("main"));

    host.publish_metadata(
        image.image(),
        &workspace,
        Revision::new(3),
        MetadataPolicy::Fresh,
        Some(&operation),
        None,
    )
    .expect("publish session metadata");

    let published =
        DetachedWorkspaceMetadata::read_for_image(image.image()).expect("published metadata");
    let info = published.info_snapshot;
    assert_eq!(info.project_root, fixture.root.join("project"));
    assert_eq!(info.role, WorkspaceRole::Workspace);
    assert_eq!(info.branch.as_deref(), Some("cowshed/raven"));
    assert_eq!(
        info.forked_from.as_ref().map(WorkspaceName::as_str),
        Some("main")
    );
}

#[test]
fn preserved_metadata_keeps_origin_facts_and_refreshes_capture_time() {
    let fixture = Fixture::new("preserved-info-snapshot");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let source = layout.main_image().expect("source");
    let destination = layout
        .checkpoint_image(
            &WorkspaceName::new("main").expect("main"),
            &cowshed_core::storage::CheckpointLabel::new("saved").expect("label"),
        )
        .expect("destination");
    create_image(source.image());
    create_image(destination.image());
    let host = native_host(&fixture, RecordingRunner::default());
    let workspace = workspace();
    let original = identity(&fixture);
    host.publish_metadata(
        source.image(),
        &workspace,
        Revision::new(1),
        MetadataPolicy::Fresh,
        Some(&original),
        None,
    )
    .expect("fresh source");
    let mut refresh = identity(&fixture);
    refresh.created_at = "2026-07-14T01:02:03Z".to_owned();
    refresh.branch = Some("wrong-new-branch".to_owned());
    host.publish_metadata(
        destination.image(),
        &workspace,
        Revision::new(2),
        MetadataPolicy::Preserve,
        Some(&refresh),
        Some(source.image()),
    )
    .expect("preserve metadata");

    let published =
        DetachedWorkspaceMetadata::read_for_image(destination.image()).expect("preserved metadata");
    let info = published.info_snapshot;
    assert_eq!(info.project_root, fixture.root.join("project"));
    assert_eq!(info.created_at, "2026-07-13T00:00:00Z");
    assert_eq!(info.branch.as_deref(), Some("main"));
    assert_eq!(info.captured_at, "2026-07-14T01:02:03Z");
    assert!(!info.stale);
}

#[test]
fn canonical_identity_mismatches_are_rejected_one_dimension_at_a_time() {
    for (test, mutate) in [("repo", 0_u8), ("workspace", 1_u8)] {
        let fixture = Fixture::new(test);
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image().expect("canonical");
        create_image(canonical.image());
        let mut mismatched = metadata();
        if mutate == 0 {
            mismatched.repo_id = RepoId::parse("other/widget").expect("other repo");
        } else {
            set_metadata_workspace(
                &mut mismatched,
                WorkspaceName::session("other").expect("other workspace"),
            );
        }
        mismatched
            .write_for_image(canonical.image())
            .expect("mismatched metadata");
        let host = native_host(&fixture, RecordingRunner::default());

        assert!(
            host.list(&repo())
                .expect_err("identity mismatch")
                .to_string()
                .contains("detached metadata identity mismatch")
        );
    }
}

#[test]
fn checkpoint_observation_reads_authoritative_detached_metadata() {
    let fixture = Fixture::new("checkpoint-observe");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let label = cowshed_core::storage::CheckpointLabel::new("ready").expect("label");
    let checkpoint = layout
        .checkpoint_image(&WorkspaceName::new("main").expect("main"), &label)
        .expect("checkpoint");
    create_image(checkpoint.image());
    let host = native_host(&fixture, RecordingRunner::default());

    let observed = host
        .observe(&[LifecycleFact::Checkpoint {
            repo: repo(),
            workspace: WorkspaceName::new("main").expect("main"),
            label: label.clone(),
            revision: Revision::new(0),
        }])
        .expect("observe checkpoint");

    assert!(matches!(
        observed.as_slice(),
        [cowshed_core::storage::lifecycle::LifecycleFact::Checkpoint {
            repo: observed_repo,
            workspace,
            label: observed_label,
            revision,
        }] if observed_repo == &repo()
            && workspace.is_main()
            && observed_label == &label
            && *revision == Revision::new(0)
    ));
}

#[test]
fn checkpoint_enumeration_reads_regular_image_files() {
    let fixture = Fixture::new("checkpoint-regular-files");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let main = WorkspaceName::new("main").expect("main");
    let host = native_host(&fixture, RecordingRunner::default());

    for (name, revision, pin) in [("first", 11, Pin::Pinned), ("second", 12, Pin::Automatic)] {
        let label = CheckpointLabel::new(name).expect("label");
        let image = layout
            .checkpoint_image(&main, &label)
            .expect("checkpoint")
            .image()
            .to_owned();
        create_image(&image);
        host.publish_checkpoint_fact(&image, &label, Revision::new(revision), pin)
            .expect("checkpoint fact");
    }

    let facts = host.checkpoints(&repo()).expect("checkpoint facts");
    let observed = facts
        .iter()
        .map(|fact| (fact.label.as_str(), fact.revision, fact.pin))
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            ("first", Revision::new(11), Pin::Pinned),
            ("second", Revision::new(12), Pin::Automatic),
        ]
    );
    assert!(
        facts
            .iter()
            .all(|fact| fact.repo == repo() && fact.workspace == main)
    );
}

#[test]
fn checkpoint_gc_reclaims_only_expired_automatic_regular_files_and_sidecars() {
    let fixture = Fixture::new("checkpoint-retention");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let main = WorkspaceName::new("main").expect("main");
    let host = native_host(&fixture, RecordingRunner::default());
    let create_checkpoint = |name: &str, revision: u64, pin: Pin| {
        let label = CheckpointLabel::new(name).expect("label");
        let image = layout
            .checkpoint_image(&main, &label)
            .expect("checkpoint")
            .image()
            .to_owned();
        create_image(&image);
        host.publish_checkpoint_fact(&image, &label, Revision::new(revision), pin)
            .expect("checkpoint fact");
        image
    };

    let mut newest_five = Vec::new();
    for index in 0..5 {
        let image = create_checkpoint(&format!("future-{index}"), index + 1, Pin::Automatic);
        make_future(&image);
        newest_five.push(image);
    }
    let young = create_checkpoint("young", 6, Pin::Automatic);
    let expired = create_checkpoint("expired", 7, Pin::Automatic);
    make_old(&expired);
    let pinned = create_checkpoint("pinned-old", 8, Pin::Pinned);
    make_old(&pinned);
    drop(host);

    let restarted = native_host(&fixture, RecordingRunner::default());
    assert_eq!(
        restarted.checkpoints(&repo()).expect("restart facts").len(),
        8
    );

    let plan = restarted
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("GC preview");
    let repeated = restarted
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("repeated preview");
    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(
        plan.candidates()[0].identity(),
        repeated.candidates()[0].identity(),
        "unchanged candidates have stable identities"
    );
    let candidate = &plan.candidates()[0];
    assert_eq!(candidate.path(), expired);
    assert_eq!(candidate.reason(), StorageGcReason::ExpiredCheckpoint);
    let expected_bytes = [
        expired.clone(),
        sidecar_path(&expired),
        ca_key_path(&expired),
        checkpoint_fact_path(&expired),
    ]
    .into_iter()
    .map(|path| {
        std::fs::metadata(path)
            .expect("candidate component")
            .blocks()
            .saturating_mul(512)
    })
    .sum::<u64>();
    assert_eq!(candidate.bytes(), expected_bytes);
    assert!(
        plan.candidates()
            .iter()
            .all(|candidate| candidate.path() != pinned),
        "pinned checkpoints are never candidates"
    );
    assert!(expired.exists(), "preview must not mutate");

    restarted
        .publish_checkpoint_fact(
            &expired,
            &CheckpointLabel::new("expired").expect("label"),
            Revision::new(7),
            Pin::Pinned,
        )
        .expect("pin after preview");
    let stale = restarted
        .execute_gc(&config, plan)
        .expect_err("pin change makes plan stale");
    assert!(matches!(stale, ApfsStorageError::GcPlanStale));
    assert!(expired.exists(), "stale execution must not mutate");
    restarted
        .publish_checkpoint_fact(
            &expired,
            &CheckpointLabel::new("expired").expect("label"),
            Revision::new(7),
            Pin::Automatic,
        )
        .expect("restore automatic pin");
    let plan = restarted
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("fresh preview");
    let report = restarted.execute_gc(&config, plan).expect("checkpoint gc");
    assert_eq!(report.examined, 8);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.retained_pinned, 1);
    assert_eq!(report.retained_recent, 6);
    assert!(newest_five.iter().all(|image| image.exists()));
    assert!(young.exists(), "young automatic checkpoint must survive");
    assert!(pinned.exists(), "old pinned checkpoint must survive");
    assert!(
        !expired.exists(),
        "expired automatic checkpoint is reclaimed"
    );
    assert!(
        !sidecar_path(&expired).exists(),
        "grants sidecar is reclaimed"
    );
    assert!(
        !checkpoint_fact_path(&expired).exists(),
        "checkpoint fact sidecar is reclaimed"
    );
    let surviving = restarted.checkpoints(&repo()).expect("surviving facts");
    assert_eq!(surviving.len(), 7);
    assert!(
        surviving
            .iter()
            .all(|fact| fact.label.as_str() != "expired")
    );
}

#[test]
fn retired_reclaim_excludes_workspace_immediately_and_removes_every_restore_artifact() {
    const FIRST: &str = "00000000000000000000000000000001";
    const SECOND: &str = "00000000000000000000000000000002";
    const CURRENT: &str = "00000000000000000000000000000003";
    let fixture = Fixture::new("retired-complete-reclaim");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let main = layout.main_image().expect("main");
    create_image(main.image());
    let name = WorkspaceName::session("retired").expect("retired workspace");
    let canonical = layout.session_image(&name).expect("session image");
    create_image(canonical.image());
    write_session_metadata(canonical.image(), "retired", CURRENT);

    let label = CheckpointLabel::new("pinned").expect("checkpoint label");
    let checkpoint = layout
        .checkpoint_image(&name, &label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "retired", FIRST);
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(3), Pin::Pinned)
        .expect("pinned checkpoint fact");

    let checkpoint_directory = layout.project().checkpoints.join("retired");
    let first_undo = checkpoint_directory.join(format!("pre-restore-{SECOND}.dmg"));
    create_image(&first_undo);
    write_session_metadata(&first_undo, "retired", FIRST);
    let second_undo = checkpoint_directory.join(format!("pre-restore-{CURRENT}.dmg"));
    create_image(&second_undo);
    write_session_metadata(&second_undo, "retired", SECOND);
    let mountpoint = layout.workspace_mount(&name).expect("workspace mountpoint");
    std::fs::create_dir_all(&mountpoint).expect("empty mountpoint");

    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/retired-{CURRENT}.dmg"));
    host.retire_image(canonical.image(), &trash)
        .expect("logical retirement");
    drop(host);

    let restarted = native_host(&fixture, RecordingRunner::default());
    let derived = derive_workspaces(
        restarted.list(&repo()).expect("published facts"),
        restarted.mounts(&repo()).expect("kernel mounts"),
        restarted.checkpoints(&repo()).expect("checkpoint facts"),
    )
    .expect("retired checkpoints do not republish the workspace");
    assert_eq!(derived.len(), 1);
    assert!(derived[0].workspace.name().is_main());

    let retired = RetiredRef::new(session_workspace("retired", CURRENT), Revision::new(1));
    restarted
        .reclaim_retired(&config, &retired)
        .expect("background reclaim");
    restarted
        .reclaim_retired(&config, &retired)
        .expect("idempotent repeated reclaim");

    for image in [&trash, &checkpoint, &first_undo, &second_undo] {
        assert!(!image.exists(), "{} image remains", image.display());
        assert!(
            !sidecar_path(image).exists(),
            "{} grants remain",
            image.display()
        );
        assert!(
            !ca_key_path(image).exists(),
            "{} CA key remains",
            image.display()
        );
        assert!(
            !checkpoint_fact_path(image).exists(),
            "{} checkpoint fact remains",
            image.display()
        );
    }
    assert!(!checkpoint_directory.exists());
    assert!(!mountpoint.exists());
}

/// A retired workspace's checkpoint images are hundreds of gigabytes of sparse blocks, and `cowshed
/// doctor` previews their collection. The preview, and the identical one the reclaim repeats under
/// the locks, once hashed every image in full: 1.5 TB of reads and 23 CPU-minutes for one doctor.
/// The images here are 100 GiB and mode 000, so any open of one to read it fails the preview.
#[test]
fn gc_preview_and_reclaim_of_a_retired_workspace_never_read_its_checkpoint_images() {
    const FIRST: &str = "00000000000000000000000000000001";
    const SECOND: &str = "00000000000000000000000000000002";
    const CURRENT: &str = "00000000000000000000000000000003";
    const HUNDRED_GIB: u64 = 100 << 30;
    let fixture = Fixture::new("retired-sparse-checkpoints");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    create_image(layout.main_image().expect("main").image());
    let name = WorkspaceName::session("retired").expect("retired workspace");
    let canonical = layout.session_image(&name).expect("session image");
    create_image(canonical.image());
    write_session_metadata(canonical.image(), "retired", CURRENT);

    let label = CheckpointLabel::new("pinned").expect("checkpoint label");
    let checkpoint = layout
        .checkpoint_image(&name, &label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "retired", FIRST);
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(3), Pin::Pinned)
        .expect("pinned checkpoint fact");
    let undo = layout
        .project()
        .checkpoints
        .join("retired")
        .join(format!("pre-restore-{SECOND}.dmg"));
    create_image(&undo);
    write_session_metadata(&undo, "retired", FIRST);
    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/retired-{CURRENT}.dmg"));
    host.retire_image(canonical.image(), &trash)
        .expect("logical retirement");

    for image in [&checkpoint, &undo] {
        std::fs::OpenOptions::new()
            .write(true)
            .open(image)
            .and_then(|file| file.set_len(HUNDRED_GIB))
            .expect("sparse length");
        std::fs::set_permissions(image, std::fs::Permissions::from_mode(0o000))
            .expect("unreadable image");
        assert!(
            std::fs::File::open(image).is_err(),
            "this test needs a process that mode 000 binds, not root"
        );
    }

    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("a preview over sparse checkpoint images reads none of them");
    assert!(
        plan.candidates().is_empty(),
        "recovery images are not generic GC candidates"
    );
    host.execute_gc(&config, plan)
        .expect("plain gc retains recovery");
    assert!(trash.exists());
    let retired = RetiredRef::new(session_workspace("retired", CURRENT), Revision::new(1));
    host.reclaim_retired(&config, &retired)
        .expect("explicit scoped reclamation reads no image bytes");
    for image in [&trash, &checkpoint, &undo] {
        assert!(!image.exists(), "{} image remains", image.display());
    }
}

#[test]
fn retired_pending_clone_reclaims_without_touching_reused_name() {
    const RETIRED: &str = "00000000000000000000000000000004";
    const LIVE: &str = "00000000000000000000000000000005";
    let fixture = Fixture::new("retired-pending-reclaim");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let name = WorkspaceName::session("reused").expect("session workspace");
    let canonical = layout.session_image(&name).expect("session image");
    create_image(canonical.image());
    write_session_metadata(canonical.image(), "reused", RETIRED);
    let mut pending = DetachedWorkspaceMetadata::read_for_image(canonical.image())
        .expect("pending clone metadata");
    pending.publication_state = PublicationState::PendingFence;
    pending
        .write_for_image(canonical.image())
        .expect("record pending clone");

    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/reused-{RETIRED}.dmg"));
    host.retire_image(canonical.image(), &trash)
        .expect("retire unpublished clone");
    create_image(canonical.image());
    write_session_metadata(canonical.image(), "reused", LIVE);

    let retired = RetiredRef::new(session_workspace("reused", RETIRED), Revision::new(1));
    host.reclaim_retired(&config, &retired)
        .expect("reclaim pending clone by exact retired identity");
    assert!(!trash.exists(), "retired clone image was reclaimed");
    assert!(
        !sidecar_path(&trash).exists(),
        "retired clone sidecar was reclaimed"
    );
    assert!(
        canonical.image().exists(),
        "new incarnation retains its image"
    );
    let current = DetachedWorkspaceMetadata::read_for_image(canonical.image())
        .expect("new incarnation retains its sidecar");
    assert_eq!(current.workspace_incarnation.as_str(), LIVE);
}

/// Trash entries are keyed `<name>-<incarnation>`, but `checkpoints/<name>`, the temp dir and the
/// mountpoint are keyed on the name alone. Re-minting a retired name hands the new lifetime those
/// same paths, so a live workspace has to withhold them from a stranded retirement of its name
/// without withholding that retirement's own per-incarnation artifacts — which are garbage either
/// way.
#[test]
fn gc_collects_each_stranded_retirement_while_a_live_workspace_reuses_the_name() {
    const FIRST: &str = "1836ace7b0cd4b41b3ca31a9eb1d131a";
    const SECOND: &str = "a5e833cf3c73476184ef904ef11846d1";
    const LIVE: &str = "00000000000000000000000000000009";
    let fixture = Fixture::new("retired-name-reuse-live");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let main = layout.main_image().expect("main");
    create_image(main.image());

    let name = WorkspaceName::session("lockfree-doctrine").expect("session workspace");
    let live = layout.session_image(&name).expect("session image");
    create_image(live.image());
    write_session_metadata(live.image(), "lockfree-doctrine", LIVE);

    let label = CheckpointLabel::new("pinned").expect("checkpoint label");
    let checkpoint = layout
        .checkpoint_image(&name, &label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "lockfree-doctrine", LIVE);
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(3), Pin::Pinned)
        .expect("pinned checkpoint fact");
    let mountpoint = layout.workspace_mount(&name).expect("workspace mountpoint");
    std::fs::create_dir_all(&mountpoint).expect("live mountpoint");
    let exec_temp = layout.exec_temp_dir(&name).expect("temp dir");
    std::fs::create_dir_all(&exec_temp).expect("live temp dir");
    std::fs::write(exec_temp.join("scratch"), b"live").expect("live scratch");

    let stranded = [FIRST, SECOND].map(|incarnation| {
        let path = layout
            .project()
            .sessions
            .join(format!(".trash/lockfree-doctrine-{incarnation}.dmg"));
        create_image(&path);
        write_session_metadata(&path, "lockfree-doctrine", incarnation);
        path
    });

    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("a reused name does not block its stranded retirements");
    let retired = plan
        .candidates()
        .iter()
        .filter(|candidate| candidate.reason() == StorageGcReason::RetiredWorkspace)
        .map(|candidate| candidate.path().to_owned())
        .collect::<Vec<_>>();
    assert!(
        retired.is_empty(),
        "name reuse does not make recovery images garbage"
    );
    host.execute_gc(&config, plan)
        .expect("plain gc preserves recovery");
    for incarnation in [FIRST, SECOND] {
        host.reclaim_retired(
            &config,
            &RetiredRef::new(
                session_workspace("lockfree-doctrine", incarnation),
                Revision::new(1),
            ),
        )
        .expect("explicitly reclaim the selected old lifetime");
    }

    for image in &stranded {
        assert!(!image.exists(), "{} image remains", image.display());
        assert!(
            !sidecar_path(image).exists(),
            "{} grants remain",
            image.display()
        );
        assert!(
            !ca_key_path(image).exists(),
            "{} CA key remains",
            image.display()
        );
    }
    assert!(live.image().exists(), "live workspace image was collected");
    assert!(
        sidecar_path(live.image()).exists(),
        "live workspace grants were collected"
    );
    assert!(
        checkpoint.exists(),
        "live lifetime checkpoint was collected"
    );
    assert!(
        checkpoint_fact_path(&checkpoint).exists(),
        "live lifetime checkpoint fact was collected"
    );
    assert!(
        mountpoint.exists(),
        "live workspace mountpoint was collected"
    );
    assert_eq!(
        std::fs::read(exec_temp.join("scratch")).expect("live temp dir kept"),
        b"live"
    );
}

/// A retired entry whose grants sidecar was deleted by hand once failed every GC preview, and so
/// every doctor. Missing authority must neither block unrelated work nor authorize deletion
/// of a potentially only-copy recovery image.
#[test]
fn gc_preserves_recovery_images_even_when_their_sidecar_is_missing() {
    const RECORDED: &str = "1836ace7b0cd4b41b3ca31a9eb1d131a";
    const UNRECORDED: &str = "a5e833cf3c73476184ef904ef11846d1";
    let fixture = Fixture::new("retired-sidecar-gone");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    create_image(layout.main_image().expect("main").image());
    let trash = layout.project().sessions.join(".trash");
    let recorded = trash.join(format!("cpg-review-{RECORDED}.dmg"));
    create_image(&recorded);
    write_session_metadata(&recorded, "cpg-review", RECORDED);
    let unrecorded = trash.join(format!("cpg-review3-{UNRECORDED}.dmg"));
    create_image(&unrecorded);
    std::fs::remove_file(sidecar_path(&unrecorded)).expect("delete the sidecar by hand");

    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("a sidecarless retired image does not fail the preview");
    let retired = plan
        .candidates()
        .iter()
        .filter(|candidate| candidate.reason() == StorageGcReason::RetiredWorkspace)
        .map(|candidate| candidate.path().to_owned())
        .collect::<Vec<_>>();
    assert!(retired.is_empty());
    host.execute_gc(&config, plan)
        .expect("gc preserves both recovery payloads");
    assert!(recorded.exists());
    assert!(sidecar_path(&recorded).exists());
    assert!(
        unrecorded.exists(),
        "missing metadata is not release authority"
    );
    assert!(!sidecar_path(&unrecorded).exists());
    assert!(ca_key_path(&unrecorded).exists());
}

/// Retiring one name twice leaves two independently collectable entries. The paths keyed on the
/// name alone belong to exactly one of them, so they are collected once rather than claimed twice
/// or abandoned along with the entry that did not own them.
#[test]
fn gc_collects_every_stranded_retirement_of_one_name_and_its_shared_paths_once() {
    const FIRST: &str = "1836ace7b0cd4b41b3ca31a9eb1d131a";
    const SECOND: &str = "a5e833cf3c73476184ef904ef11846d1";
    let fixture = Fixture::new("retired-name-reuse-stranded");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let main = layout.main_image().expect("main");
    create_image(main.image());

    let name = WorkspaceName::session("lockfree-doctrine").expect("session workspace");
    let label = CheckpointLabel::new("keep").expect("checkpoint label");
    let checkpoint = layout
        .checkpoint_image(&name, &label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "lockfree-doctrine", SECOND);
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(2), Pin::Pinned)
        .expect("pinned checkpoint fact");
    let checkpoint_directory = layout.project().checkpoints.join("lockfree-doctrine");
    let mountpoint = layout.workspace_mount(&name).expect("workspace mountpoint");
    std::fs::create_dir_all(&mountpoint).expect("empty mountpoint");
    let exec_temp = layout.exec_temp_dir(&name).expect("temp dir");
    std::fs::create_dir_all(exec_temp.join("nested")).expect("temp dir");
    std::fs::write(exec_temp.join("nested/scratch"), b"left behind").expect("scratch");

    let stranded = [FIRST, SECOND].map(|incarnation| {
        let path = layout
            .project()
            .sessions
            .join(format!(".trash/lockfree-doctrine-{incarnation}.dmg"));
        create_image(&path);
        write_session_metadata(&path, "lockfree-doctrine", incarnation);
        path
    });

    let report = execute_gc(&host, &config).expect("recovery entries remain");
    assert_eq!(report.reclaimed, 0);
    for incarnation in [FIRST, SECOND] {
        host.reclaim_retired(
            &config,
            &RetiredRef::new(
                session_workspace("lockfree-doctrine", incarnation),
                Revision::new(1),
            ),
        )
        .expect("explicitly reclaim the selected lifetime");
    }
    for image in &stranded {
        assert!(!image.exists(), "{} image remains", image.display());
        assert!(
            !sidecar_path(image).exists(),
            "{} grants remain",
            image.display()
        );
        assert!(
            !ca_key_path(image).exists(),
            "{} CA key remains",
            image.display()
        );
    }
    assert!(!checkpoint.exists(), "shared checkpoint image remains");
    assert!(
        !checkpoint_fact_path(&checkpoint).exists(),
        "shared checkpoint fact remains"
    );
    assert!(
        !checkpoint_directory.exists(),
        "shared checkpoint directory remains"
    );
    assert!(!mountpoint.exists(), "shared mountpoint remains");
    assert!(!exec_temp.exists(), "shared temp dir remains");
}

#[test]
fn retired_gc_revalidates_stale_plans_and_resumes_from_sidecar_authority() {
    const CURRENT: &str = "00000000000000000000000000000003";
    let fixture = Fixture::new("retired-gc-resume");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let name = WorkspaceName::session("retired").expect("retired workspace");
    let canonical = layout.session_image(&name).expect("session image");
    create_image(canonical.image());
    write_session_metadata(canonical.image(), "retired", CURRENT);
    let checkpoint_label = CheckpointLabel::new("keep").expect("checkpoint label");
    let checkpoint = layout
        .checkpoint_image(&name, &checkpoint_label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "retired", CURRENT);
    host.publish_checkpoint_fact(
        &checkpoint,
        &checkpoint_label,
        Revision::new(2),
        Pin::Pinned,
    )
    .expect("pinned fact");
    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/retired-{CURRENT}.dmg"));
    host.retire_image(canonical.image(), &trash)
        .expect("logical retirement");

    std::fs::remove_file(&trash).expect("physical release completed before metadata cleanup");
    let stale_plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("retired GC preview");
    assert_eq!(stale_plan.candidates().len(), 1);
    assert_eq!(
        stale_plan.candidates()[0].reason(),
        StorageGcReason::RetiredWorkspace
    );
    assert_eq!(stale_plan.retained_pinned(), 0);
    host.publish_checkpoint_fact(
        &checkpoint,
        &checkpoint_label,
        Revision::new(2),
        Pin::Automatic,
    )
    .expect("change checkpoint after preview");
    let stale = host
        .execute_gc(&config, stale_plan)
        .expect_err("changed retired artifact makes plan stale");
    assert!(matches!(stale, ApfsStorageError::GcPlanStale));
    assert!(
        sidecar_path(&trash).exists(),
        "stale plan mutated retirement authority"
    );
    assert!(checkpoint.exists(), "stale plan mutated checkpoint");

    host.publish_checkpoint_fact(
        &checkpoint,
        &checkpoint_label,
        Revision::new(2),
        Pin::Pinned,
    )
    .expect("restore pinned checkpoint");
    std::fs::remove_file(&checkpoint)
        .expect("simulate checkpoint image deletion before sidecar cleanup");
    drop(host);

    let restarted = native_host(&fixture, RecordingRunner::default());
    let plan = restarted
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("sidecar resumes retired cleanup");
    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(plan.retained_pinned(), 0);
    let report = restarted
        .execute_gc(&config, plan)
        .expect("resume interrupted retirement GC");
    assert_eq!(report.reclaimed, 1);
    assert!(!checkpoint.exists());
    assert!(!sidecar_path(&checkpoint).exists());
    assert!(!checkpoint_fact_path(&checkpoint).exists());
    assert!(!sidecar_path(&trash).exists());
    assert!(!ca_key_path(&trash).exists());
}

#[test]
fn malformed_foreign_orphan_checkpoint_is_rejected_without_cleanup_authority() {
    let fixture = Fixture::new("malformed-orphan-checkpoint");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let host = native_host(&fixture, RecordingRunner::default());
    let name = WorkspaceName::session("orphan").expect("orphan workspace");
    let label = CheckpointLabel::new("foreign").expect("checkpoint label");
    let checkpoint = layout
        .checkpoint_image(&name, &label)
        .expect("checkpoint")
        .image()
        .to_owned();
    create_image(&checkpoint);
    write_session_metadata(&checkpoint, "orphan", "00000000000000000000000000000001");
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(1), Pin::Pinned)
        .expect("checkpoint fact");
    let fact_path = checkpoint_fact_path(&checkpoint);
    let mut fact: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&fact_path).expect("read fact")).expect("JSON fact");
    fact["repoId"] = serde_json::Value::String("foreign/project".to_owned());
    std::fs::write(
        &fact_path,
        serde_json::to_vec_pretty(&fact).expect("serialize fact"),
    )
    .expect("write foreign fact");

    assert!(host.checkpoints(&repo()).is_err());
    assert!(
        host.preview_gc(&config, &repo(), StorageGcOptions::default())
            .is_err()
    );
    assert!(checkpoint.exists(), "malformed orphan was deleted");
    assert!(sidecar_path(&checkpoint).exists());
    assert!(checkpoint_fact_path(&checkpoint).exists());
}

#[test]
fn missing_and_invalid_gc_namespaces_have_distinct_behavior() {
    let fixture = Fixture::new("gc-missing");
    let project = fixture.root.join("acme/widget");
    std::fs::create_dir_all(&project).expect("project");
    let host = native_host(&fixture, RecordingRunner::default());
    assert_eq!(
        execute_gc(&host, &fixture.config()).expect("missing namespaces"),
        Default::default()
    );

    std::fs::create_dir_all(project.join("sessions")).expect("sessions");
    assert_eq!(
        execute_gc(&host, &fixture.config()).expect("missing trash"),
        Default::default()
    );
    std::fs::write(project.join("sessions/.trash"), b"not a directory").expect("trash file");
    assert!(execute_gc(&host, &fixture.config()).is_err());

    std::fs::remove_file(project.join("sessions/.trash")).expect("remove trash file");
    std::fs::remove_dir(project.join("sessions")).expect("remove sessions");
    std::fs::write(project.join("sessions"), b"not a directory").expect("sessions file");
    assert!(execute_gc(&host, &fixture.config()).is_err());
}

#[test]
fn stats_distinguish_a_missing_checkpoint_directory_from_an_invalid_one() {
    let fixture = Fixture::new("stats-missing");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    create_image(canonical.image());
    let host = native_host(&fixture, RecordingRunner::default());
    assert_eq!(
        host.stats(&workspace(), canonical.image())
            .expect("missing checkpoint directory")
            .checkpoint_count,
        0
    );

    let checkpoint_path = layout.project().checkpoints.join("main");
    std::fs::create_dir_all(checkpoint_path.parent().expect("parent")).expect("parent");
    std::fs::write(&checkpoint_path, b"not a directory").expect("checkpoint file");
    assert!(host.stats(&workspace(), canonical.image()).is_err());
}

#[test]
fn sidecar_removal_does_not_hide_non_file_errors() {
    let fixture = Fixture::new("sidecar-error");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image().expect("image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("parent");
    std::fs::write(image.image(), b"image").expect("image");
    std::fs::create_dir_all(sidecar_path(image.image())).expect("sidecar directory");
    let host = native_host(&fixture, RecordingRunner::default());

    assert!(host.reclaim_image(image.image()).is_err());
}

/// A `WhenIdle` detach that a holder dissents leaves the workspace exactly as it found it: still
/// mounted, and still the actor's to detach once the holder lets go. The holder is real — a file
/// open on the volume — so the refusal is the native unmount's resource-busy result. The separate
/// real held-file release test proves escalation after the unchanged grace; this test proves that
/// `WhenIdle` leaves the volume and its contents to their owner.
#[test]
fn real_apfs_dissented_when_idle_detach_leaves_the_mount_to_the_actor() {
    let fixture = RealFixture::new("detach-dissent");
    let image = fixture.layout().main_image().expect("image");
    fixture.published_image(image.image());
    let host = fixture.host();
    let workspace = workspace();
    let mount = fixture.main_mount();
    std::fs::create_dir_all(&mount).expect("mount point");
    let attachment = host.attach_verified(image.image()).expect("attachment");
    host.mount(&attachment, &mount, MountAccess::ReadWrite, false)
        .expect("mount");
    host.retain_mounted(&workspace, attachment).expect("retain");
    let holder = std::fs::File::create(mount.join("held")).expect("hold the volume");

    host.detach_mounted(&workspace, DetachIntent::WhenIdle)
        .expect_err("a dissented WhenIdle detach must fail");
    assert!(
        kernel_mount_at(&mount).is_some(),
        "WhenIdle never forces: the volume stays mounted"
    );

    drop(holder);
    host.detach_mounted(&workspace, DetachIntent::Release)
        .expect("the workspace is still the actor's to detach");
    assert!(kernel_mount_at(&mount).is_none());
    assert!(!attached(image.image()));
}

#[test]
fn real_apfs_release_detaches_a_volume_held_open_after_its_grace() {
    let fixture = RealFixture::new("native-release-held-file");
    let image = fixture.layout().main_image().expect("image");
    fixture.published_image(image.image());
    let host = fixture.host();
    let workspace = workspace();
    let mount = fixture.main_mount();
    std::fs::create_dir_all(&mount).expect("mount point");
    let attachment = host.attach_verified(image.image()).expect("attachment");
    host.mount(&attachment, &mount, MountAccess::ReadWrite, false)
        .expect("mount");
    host.retain_mounted(&workspace, attachment).expect("retain");
    let mut holder = std::fs::File::create(mount.join("held")).expect("hold the volume");

    host.detach_mounted(&workspace, DetachIntent::Release)
        .expect("release waits its grace, then forces the owned volume");
    assert!(kernel_mount_at(&mount).is_none());
    assert!(!attached(image.image()));
    use std::io::Write as _;
    assert!(
        holder.write_all(b"after forced unmount").is_err(),
        "a held file cannot write into a released filesystem"
    );
}

/// On real APFS: a refused create (`slotted`, never attached) and an attached volume mounted
/// nowhere are typed skips, the empty mountpoint is refused as no store image, and nothing the
/// host owns changes name. The mounted workspace is renamed through its device, under the raw pin
/// the rename takes, and the kernel then reports the label.
/// Before mounting, detached capture refuses the attached image without publishing a copy.
/// Once mounted, a holder can dissent to WhenIdle without surrendering the actor's attachment;
/// with the holder still open, Release waits its grace, then forces the owned volume off: the
/// mount and attachment are gone and the held file can no longer write.
#[test]
fn real_apfs_rename_names_the_pinned_device_and_skips_what_is_not_mounted() {
    let fixture = RealFixture::new("native-rename-pinned");
    let layout = fixture.layout();
    let image = layout.main_image().expect("image");
    fixture.published_image(image.image());
    let host = fixture.host();
    let host_data = cowshed_apfs::volume_name(Path::new("/System/Volumes/Data"))
        .expect("the host's Data volume name");
    let label_name = fixture
        .host_namespace()
        .volume_label(format_args!("fixture · intents — main"));
    let label = label_name.as_str();

    let slotted = layout
        .canonical_image(&WorkspaceName::new("slotted").expect("workspace name"))
        .expect("slotted image path");
    let refused_mount = fixture.root().join("mount").join("slotted");
    std::fs::create_dir_all(&refused_mount).expect("an empty mountpoint");
    let holding_volume = cowshed_apfs::volume_name(&refused_mount).expect("the volume holding it");
    assert_eq!(
        host.label_image_volume(slotted.image(), label)
            .expect("a refused create is a typed skip"),
        Labelled::NotAttached
    );
    assert!(
        host.label_image_volume(&refused_mount, label).is_err(),
        "a mountpoint is no image under the store"
    );
    assert_eq!(
        cowshed_apfs::volume_name(&refused_mount).expect("the volume holding it"),
        holding_volume,
        "the volume holding the empty mountpoint keeps its name"
    );

    let mount = fixture.main_mount();
    std::fs::create_dir_all(&mount).expect("mount point");
    let attachment = host.attach_verified(image.image()).expect("attachment");
    let destination = fixture.root().join("destination.dmg");
    assert!(
        attached(image.image()),
        "the kernel has the source image attached"
    );
    let cloned = host
        .backend()
        .prepare_clone(image.image(), &destination, ImageCapture::Detached)
        .and_then(|admitted| admitted.clone_image());
    assert!(
        cloned.is_err(),
        "an attached image must never be captured detached: {cloned:?}"
    );
    assert!(!destination.exists(), "no unsealed image may be published");
    assert!(
        attached(image.image()),
        "refusal leaves the source attachment alone"
    );
    assert_eq!(
        host.label_image_volume(image.image(), label)
            .expect("an attached, unmounted volume is a typed skip"),
        Labelled::NotMounted
    );
    host.mount(&attachment, &mount, MountAccess::ReadWrite, false)
        .expect("mount");
    host.retain_mounted(&workspace(), attachment)
        .expect("retain");

    assert_eq!(
        host.label_image_volume(image.image(), label)
            .expect("rename under the device's raw pin"),
        Labelled::Renamed
    );
    assert_eq!(
        cowshed_apfs::volume_name(&mount).expect("the renamed volume's name"),
        label
    );
    assert_eq!(
        host.label_image_volume(image.image(), label)
            .expect("an already-named volume"),
        Labelled::Already
    );
    assert_eq!(
        cowshed_apfs::volume_name(Path::new("/System/Volumes/Data"))
            .expect("the host's Data volume name"),
        host_data,
        "the host's Data volume keeps its name"
    );

    let mut holder = std::fs::File::create(mount.join("held")).expect("hold the volume");
    host.detach_mounted(&workspace(), DetachIntent::WhenIdle)
        .expect_err("a dissented WhenIdle detach must fail");
    assert!(
        kernel_mount_at(&mount).is_some(),
        "WhenIdle never forces: the volume stays mounted"
    );
    host.detach_mounted(&workspace(), DetachIntent::Release)
        .expect("release waits its grace, then forces the owned volume");
    assert!(kernel_mount_at(&mount).is_none());
    assert!(!attached(image.image()));
    use std::io::Write as _;
    assert!(
        holder.write_all(b"after forced unmount").is_err(),
        "a held file cannot write into a released filesystem"
    );
}

/// Kernel mounts a test states outright, so a rename's target is proven from the fake inventory
/// and these mounts alone.
struct FixedMounts(Vec<KernelMountSnapshot>);

impl KernelMountSource for FixedMounts {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
        Ok(self.0.clone())
    }
}

/// A host over `runner` that sees `mounts` and nothing else mounted; a held mountpoint of
/// `runner` answers the same mounts.
fn host_seeing(
    fixture: &Fixture,
    runner: RecordingRunner,
    mounts: Vec<KernelMountSnapshot>,
) -> MacOsApfsExecutionHost<RecordingRunner> {
    runner.mounted.lock().expect("mounts").extend(
        mounts
            .iter()
            .map(|mount| (mount.mount_point.clone(), mount.source_device.clone())),
    );
    MacOsApfsExecutionHost::with_mount_source(runner, fixture.config(), FixedMounts(mounts))
        .expect("native APFS host")
}

/// The session image a workspace `slotted` of this fixture's store would be.
fn slotted_image(fixture: &Fixture) -> PathBuf {
    StorageLayout::new(&fixture.root, &repo())
        .expect("layout")
        .canonical_image(&WorkspaceName::new("slotted").expect("workspace name"))
        .expect("image path")
        .image()
        .to_owned()
}

/// `image`, held attached by the fake kernel inventory.
fn attach(runner: &RecordingRunner, image: &Path) {
    runner
        .inventory
        .0
        .lock()
        .expect("fake inventory")
        .push(image.to_string_lossy().into_owned());
}

/// Every volume the host named, as (volume device, name). No disk tool names a volume.
fn renames(runner: &RecordingRunner) -> Vec<(String, String)> {
    runner.named.lock().expect("named").clone()
}

/// The label a test's slotted volume is named, in the fixture's namespace.
fn slotted_label(fixture: &Fixture) -> String {
    fixture
        .host_namespace()
        .volume_label(format_args!("fixture · intents — slotted"))
}

#[test]
fn native_volume_rename_names_the_image_volume_through_its_held_root() {
    let fixture = Fixture::new("rename-volume");
    let runner = RecordingRunner::default();
    let image = slotted_image(&fixture);
    attach(&runner, &image);
    let mount = fixture.root.join("mount").join("slotted");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let host = host_seeing(
        &fixture,
        runner.clone(),
        vec![KernelMountSnapshot::new(
            7,
            &mount,
            "/dev/disk10s1",
            true,
            true,
        )],
    );

    assert_eq!(
        host.label_image_volume(&image, &slotted_label(&fixture))
            .expect("rename"),
        Labelled::Renamed
    );

    assert_eq!(
        renames(&runner),
        [(
            "/dev/disk10s1".to_owned(),
            slotted_label(&fixture).to_owned()
        )]
    );
    assert!(
        runner.requests().iter().all(|request| request
            .args
            .first()
            .is_none_or(|verb| verb != "renameVolume" && verb != "rename")),
        "no disk tool renames: {:?}",
        runner.requests()
    );
}

/// The volume a label found mounted is unmounted before its rename runs: the mount table read
/// still lists it, but its mountpoint is now the bare directory beneath, on the host's own Data
/// volume. Held under the image's lease, the mountpoint answers the host's device, the real
/// kernel's answer for that directory, so it is no owned root and nothing is named.
#[test]
fn a_volume_unmounted_before_its_rename_is_refused_before_any_rename() {
    let fixture = Fixture::new("rename-unmounted-since");
    let runner = RecordingRunner::default();
    let image = slotted_image(&fixture);
    attach(&runner, &image);
    let mount = fixture.root.join("mount").join("slotted");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let host = host_seeing(
        &fixture,
        runner.clone(),
        vec![KernelMountSnapshot::new(
            7,
            &mount,
            "/dev/disk10s1",
            true,
            true,
        )],
    );
    runner.unmount(&mount);
    let beneath = cowshed_apfs::mounts::hold_mount(&mount).expect("the directory beneath");

    let refused = host
        .label_image_volume(&image, &slotted_label(&fixture))
        .expect_err("a volume unmounted since is no rename target");

    assert!(
        matches!(
            &refused,
            ApfsStorageError::Apfs(ApfsError::RenameTargetNotMounted {
                volume_device,
                found,
                ..
            }) if volume_device == "/dev/disk10s1" && found == beneath.source_device()
        ),
        "{refused}"
    );
    assert!(
        renames(&runner).is_empty(),
        "renamed {:?}",
        renames(&runner)
    );
}

/// A workspace whose create was refused, or that is not mounted, has only a directory where it
/// would mount, and that directory lies on whatever volume holds the store: here the host's own.
/// Its image names nothing, and its mountpoint is no rename target at all.
#[test]
fn an_unmounted_workspace_names_no_volume_through_its_mount_path() {
    let fixture = Fixture::new("rename-unmounted");
    let runner = RecordingRunner::default();
    let mount = fixture.root.join("mount").join("slotted");
    std::fs::create_dir_all(&mount).expect("an empty mountpoint on the host volume");
    // The volume that holds that directory, as the kernel lists it: the host's own.
    let host = host_seeing(
        &fixture,
        runner.clone(),
        vec![KernelMountSnapshot::new(
            1,
            "/System/Volumes/Data",
            "/dev/disk3s5",
            true,
            true,
        )],
    );

    assert_eq!(
        host.label_image_volume(&slotted_image(&fixture), &slotted_label(&fixture))
            .expect("a refused create is a typed skip"),
        Labelled::NotAttached
    );
    assert!(
        host.label_image_volume(&mount, &slotted_label(&fixture))
            .is_err(),
        "a mountpoint is no image under the store"
    );
    assert!(
        runner.requests().is_empty(),
        "a rename reached the volume holding {}: {:?}",
        mount.display(),
        runner.requests()
    );
}

/// Attached but mounted nowhere: APFS renames only mounted volumes, so nothing is named.
#[test]
fn an_attached_image_mounted_nowhere_names_no_volume() {
    let fixture = Fixture::new("rename-unmounted-attachment");
    let runner = RecordingRunner::default();
    let image = slotted_image(&fixture);
    attach(&runner, &image);
    let host = host_seeing(&fixture, runner.clone(), Vec::new());

    assert_eq!(
        host.label_image_volume(&image, &slotted_label(&fixture))
            .expect("typed skip"),
        Labelled::NotMounted
    );
    assert!(runner.requests().is_empty(), "ran {:?}", runner.requests());
}

/// An image outside the store is never a rename target, even one the kernel holds and mounts:
/// the host's disks, and any image cowshed did not make, are not cowshed's to name.
#[test]
fn an_image_outside_the_store_is_refused_as_a_rename_target() {
    let fixture = Fixture::new("rename-foreign-image");
    let runner = RecordingRunner::default();
    let foreign =
        std::env::temp_dir().join(format!("cowshed-foreign-{}.dmg", uuid::Uuid::new_v4()));
    attach(&runner, &foreign);
    let host = host_seeing(
        &fixture,
        runner.clone(),
        vec![KernelMountSnapshot::new(
            7,
            fixture.root.join("mounted"),
            "/dev/disk10s1",
            true,
            true,
        )],
    );

    let refused = host
        .label_image_volume(&foreign, &slotted_label(&fixture))
        .unwrap_err();

    assert!(
        matches!(
            refused,
            ApfsStorageError::Layout(StorageLayoutError::EscapesStoreRoot)
        ),
        "{refused}"
    );
    assert!(runner.requests().is_empty(), "ran {:?}", runner.requests());
}

/// An image the kernel holds twice (a dead process's attach that landed after its file was
/// replaced) is ambiguous: no rename picks one of its devices.
#[test]
fn an_image_held_twice_is_refused_as_ambiguous_and_never_renamed() {
    let fixture = Fixture::new("rename-held-twice");
    let runner = RecordingRunner::default();
    let image = slotted_image(&fixture);
    attach(&runner, &image);
    attach(&runner, &image);
    let mount = fixture.root.join("mount").join("slotted");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let host = host_seeing(
        &fixture,
        runner.clone(),
        vec![KernelMountSnapshot::new(
            7,
            &mount,
            "/dev/disk10s1",
            true,
            true,
        )],
    );

    let refused = host
        .label_image_volume(&image, &slotted_label(&fixture))
        .expect_err("an image held twice names no volume");

    assert!(
        matches!(
            refused,
            ApfsStorageError::Apfs(ApfsError::InvalidAttachmentInventory(_))
        ),
        "{refused}"
    );
    assert!(
        renames(&runner).is_empty(),
        "renamed {:?}",
        renames(&runner)
    );
}

/// A kernel holding one image attached unformatted, its whole device alone with no APFS volume,
/// recording any command run against it.
#[derive(Clone)]
struct UnformattedAttachment {
    image: PathBuf,
    requests: Arc<Mutex<Vec<CommandRequest>>>,
}

impl CommandRunner for UnformattedAttachment {
    fn run_guarded<E>(
        &self,
        request: &cowshed_apfs::CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<cowshed_apfs::CommandOutput>, E>
    where
        E: From<cowshed_apfs::CommandRunError> + std::fmt::Display,
    {
        if check()? {
            self.run(request, "guarded disk command")
                .map(|run| Some(run.output))
                .map_err(E::from)
        } else {
            Ok(None)
        }
    }
    fn run(
        &self,
        request: &CommandRequest,
        _operation: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        Ok(cowshed_apfs::CommandRun {
            output: CommandOutput::success([]),
            work_elapsed: None,
        })
    }
    fn image_lease(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        Ok(None)
    }
    fn pin_raw_device(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        Ok(None)
    }
    fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
        Ok(vec![AttachedDiskImage::file(
            &self.image,
            Some(ImageCapacity::from_gibibytes(1)),
            [("/dev/disk9", "")],
        )])
    }
    fn grow_image(&self, image: &Path, _: ImageCapacity) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("the unformatted double grows no image: {}", image.display()),
        ))
    }
}

/// An image the kernel holds unformatted has no volume to name, yet it is held: an inventory
/// fault, refused, never reported as `NotAttached`.
#[test]
fn an_image_held_unformatted_is_refused_rather_than_reported_unattached() {
    let fixture = Fixture::new("rename-unformatted");
    let image = slotted_image(&fixture);
    let runner = UnformattedAttachment {
        image: image.clone(),
        requests: Arc::default(),
    };
    let host = MacOsApfsExecutionHost::with_mount_source(
        runner.clone(),
        fixture.config(),
        FixedMounts(Vec::new()),
    )
    .expect("native APFS host");

    let refused = host
        .label_image_volume(&image, &slotted_label(&fixture))
        .expect_err("an unformatted attachment is no skip");

    assert!(
        matches!(
            refused,
            ApfsStorageError::Apfs(ApfsError::InvalidAttachmentInventory(_))
        ),
        "{refused}"
    );
    assert!(
        runner.requests.lock().expect("requests").is_empty(),
        "ran {:?}",
        runner.requests.lock().expect("requests")
    );
}

/// The host's own rename refuses an attachment of an image outside the store, even one the
/// kernel holds and that attached verified: only cowshed's images are cowshed's to name.
#[test]
fn the_host_rename_refuses_an_attachment_of_an_image_outside_the_store() {
    let fixture = Fixture::new("rename-foreign-attachment");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let foreign =
        std::env::temp_dir().join(format!("cowshed-foreign-{}.dmg", uuid::Uuid::new_v4()));
    let attachment = host
        .backend()
        .attach_verified(&foreign)
        .expect("a verified attachment of a foreign image");

    let refused = ApfsExecutionHost::rename_volume(
        &host,
        &attachment,
        &fixture.root,
        &slotted_label(&fixture),
    )
    .unwrap_err();

    assert!(
        matches!(
            refused,
            ApfsStorageError::Layout(StorageLayoutError::EscapesStoreRoot)
        ),
        "{refused}"
    );
    assert!(
        renames(&runner).is_empty(),
        "renamed {:?}",
        renames(&runner)
    );
}

#[test]
fn gc_distinguishes_a_missing_store_from_a_non_directory_store() {
    let fixture = Fixture::new("gc-store");
    let missing_root = fixture.root.join("missing-store");
    let config = ApfsSubstrateConfig::new(
        fixture.host_namespace(),
        &missing_root,
        fixture.root.join("mount"),
    );
    let host =
        MacOsApfsExecutionHost::new(RecordingRunner::default(), config.clone()).expect("host");
    assert_eq!(
        execute_gc(&host, &config).expect("missing store"),
        Default::default()
    );

    std::fs::write(&missing_root, b"not a directory").expect("store file");
    assert!(execute_gc(&host, &config).is_err());
}

#[test]
fn session_identity_mismatches_are_rejected_one_dimension_at_a_time() {
    for (test, mutate) in [("session-repo", 0_u8), ("session-name", 1_u8)] {
        let fixture = Fixture::new(test);
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let session_name = WorkspaceName::session("raven").expect("session");
        let image = layout.session_image(&session_name).expect("session image");
        create_image(image.image());
        let mut mismatched = metadata();
        set_metadata_workspace(&mut mismatched, session_name);
        if mutate == 0 {
            mismatched.repo_id = RepoId::parse("other/widget").expect("other repo");
        } else {
            set_metadata_workspace(
                &mut mismatched,
                WorkspaceName::session("other").expect("other workspace"),
            );
        }
        mismatched
            .write_for_image(image.image())
            .expect("mismatched sidecar");
        let host = native_host(&fixture, RecordingRunner::default());

        assert!(
            host.list(&repo())
                .expect_err("session identity mismatch")
                .to_string()
                .contains("detached metadata identity mismatch")
        );
    }
}

#[test]
fn published_listing_distinguishes_missing_sessions_from_an_invalid_sessions_path() {
    let fixture = Fixture::new("invalid-sessions-list");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    create_image(canonical.image());
    let host = native_host(&fixture, RecordingRunner::default());
    assert_eq!(host.list(&repo()).expect("missing sessions").len(), 1);

    std::fs::write(&layout.project().sessions, b"not a directory").expect("sessions file");
    assert!(host.list(&repo()).is_err());
}

#[test]
fn real_apfs_unmount_verified_twice_preserves_the_attached_image() {
    let fixture = RealFixture::new("unmount-twice");
    let image = fixture.root().join("owned.dmg");
    let mount = fixture.root().join("mounted");
    fixture.blank_image(&image);
    let backend = MacOsApfsBackend::new(SystemCommandRunner::new(fixture.host_namespace()));
    let (attachment, _) = backend
        .attach_mounted(&image, &mount, MountAccess::ReadWrite, false)
        .expect("mount owned image");
    assert_eq!(
        kernel_mount_at(&mount).expect("owned mount").source_device,
        attachment.volume_device()
    );
    for _ in 0..2 {
        assert_eq!(
            backend
                .unmount_within_grace(&attachment)
                .expect("idempotent exact-volume unmount"),
            cowshed_apfs::Unmounted::Unforced
        );
        assert!(
            kernel_mount_at(&mount).is_none(),
            "the owned namespace must be released"
        );
        let still = backend
            .existing_attachment(&image)
            .expect("current owned mapping")
            .expect("namespace unmount must not detach the image");
        assert_eq!(
            (still.whole_device(), still.volume_device()),
            (attachment.whole_device(), attachment.volume_device())
        );
    }
    backend
        .detach(&attachment, DetachIntent::Release)
        .expect("release owned image");
    assert!(!attached(&image));
}

/// Independent exact-whole image-driver detach attempts release after cowshed has checked its
/// identity and before fsck opens diskN. A pathname check alone loses that race;
/// the raw volume descriptor must keep the kernel from recycling diskN even for a forced eject.
/// Unexpected refusals retain command timing and current ownership observations without changing
/// the Busy oracle or adding a pin that could itself prevent release.
struct EjectAtFsck {
    system: SystemCommandRunner,
    owned: PathBuf,
    foreign: PathBuf,
    ejected: AtomicUsize,
    release_attempts: AtomicUsize,
}

impl EjectAtFsck {
    fn try_recycle(&self, volume: &str) -> Result<(), CommandRunError> {
        let system = self.system;
        let owned = cowshed_apfs::MacOsApfsBackend::new(self.system)
            .existing_attachment(&self.owned)
            .expect("exact current pinned image mapping")
            .expect("the recorded pinned fixture image is attached");
        let volume_device = format!("/dev/{volume}");
        assert_eq!(
            owned.volume_device(),
            volume_device,
            "the pinned operation must name the recorded fixture image's exact APFS volume"
        );
        let whole = owned.whole_device();
        self.release_attempts.fetch_add(1, Ordering::SeqCst);
        let mounted_before = SystemKernelMountSource
            .mounts()
            .expect("kernel mount inventory before the pinned operation")
            .into_iter()
            .filter(|mount| mount.source_device == volume_device)
            .collect::<Vec<_>>();
        assert!(
            mounted_before.is_empty(),
            "the raw-pin release hook must run before the owned volume is mounted"
        );
        let eject_request = CommandRequest::new("/usr/bin/hdiutil", ["detach", "-force", whole]);
        cowshed_os::eprintln!(
            "raw-pin attack start wall={:?} pid={} image={} whole={whole} \
         volume={volume_device} kernel_mounts={mounted_before:?} command={eject_request:?}",
            SystemTime::now(),
            std::process::id(),
            self.owned.display(),
        );
        let started = Instant::now();
        let eject = system
            .run(&eject_request, "fixture forced detach")
            .map(|run| run.output);
        cowshed_os::eprintln!(
            "raw-pin attack end wall={:?} elapsed={:?} image={} whole={whole} \
         volume={volume_device} result={eject:?}",
            SystemTime::now(),
            started.elapsed(),
            self.owned.display(),
        );
        let eject = eject?;
        if eject.succeeded() {
            self.ejected.store(1, Ordering::SeqCst);
            let attached = system
                .run(
                    &foreign_kernel_attach(&self.foreign, true),
                    "fixture foreign attach",
                )?
                .output;
            assert!(
                attached.succeeded(),
                "foreign image attach failed: {attached:?}"
            );
            assert!(
                String::from_utf8_lossy(&attached.stdout)
                    .contains(&format!("<string>/dev/{volume}</string>")),
                "recycled volume did not reuse {volume}: {}",
                String::from_utf8_lossy(&attached.stdout)
            );
            cowshed_os::eprintln!(
                "foreign mounted on recycled {volume} before the pinned operation"
            );
        } else {
            let refusal = ApfsError::CommandFailed {
                operation: "independent forced detach of pinned image",
                request: eject_request,
                output: eject,
            };
            if !cowshed_apfs::detach_was_dissented(&refusal) {
                // Observe before unwinding drops the production pin and cleanup releases the image.
                // Failed observations stay visible beside the original command failure.
                let registry_after = system.attached_disk_images().map(|images| {
                    images
                        .into_iter()
                        .filter(|image| {
                            matches!(&image.source, DiskImageSource::File(path)
                                if path == &self.owned || path == &self.foreign)
                                || image.media.iter().any(|media| {
                                    media.device == whole || media.device == volume_device
                                })
                        })
                        .collect::<Vec<_>>()
                });
                let mounts_after = SystemKernelMountSource.mounts().map(|mounts| {
                    mounts
                        .into_iter()
                        .filter(|mount| mount.source_device == volume_device)
                        .collect::<Vec<_>>()
                });
                cowshed_os::eprintln!(
                    "raw-pin unexpected refusal wall={:?} image={} whole={whole} \
                 volume={volume_device} registry_after={registry_after:?} \
                 kernel_mounts_after={mounts_after:?}; {refusal}",
                    SystemTime::now(),
                    self.owned.display(),
                );
            }
            assert!(
                cowshed_apfs::detach_was_dissented(&refusal),
                "the raw pin must produce an image-driver Busy refusal: {refusal}"
            );
        }
        Ok(())
    }
}

impl CommandRunner for EjectAtFsck {
    fn run_guarded<E>(
        &self,
        request: &CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<CommandOutput>, E>
    where
        E: From<CommandRunError> + std::fmt::Display,
    {
        self.system.run_guarded(request, check)
    }
    fn run(
        &self,
        request: &CommandRequest,
        _operation: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
        let volume = if request.program == Path::new("/sbin/fsck_apfs") {
            request
                .args
                .last()
                .and_then(|raw| raw.to_str())
                .and_then(|raw| raw.strip_prefix("/dev/r"))
        } else if request.program == Path::new("/sbin/mount_apfs") {
            request
                .args
                .get(2)
                .and_then(|raw| raw.to_str())
                .and_then(|raw| raw.strip_prefix("/dev/"))
        } else {
            None
        };
        if let Some(volume) = volume {
            cowshed_os::eprintln!("raw-pin attack hook request={request:?}");
            self.try_recycle(volume)?;
        }
        self.system.run(request, _operation)
    }

    fn image_lease(&self, identity: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        self.system.image_lease(identity)
    }

    fn pin_raw_device(&self, device: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        let pin = self.system.pin_raw_device(device)?;
        cowshed_os::eprintln!(
            "raw-pin opened wall={:?} image={} raw={} descriptor={pin:?} rdev={:?}",
            SystemTime::now(),
            self.owned.display(),
            device.display(),
            pin.as_ref()
                .map(|pin| pin.metadata().map(|metadata| metadata.rdev())),
        );
        Ok(pin)
    }

    fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
        self.system.attached_disk_images()
    }

    fn attachments_of(&self, image: &Path) -> std::io::Result<Vec<AttachedDiskImage>> {
        self.system.attachments_of(image)
    }

    fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> std::io::Result<()> {
        self.system.grow_image(image, capacity)
    }
}

/// The raw pin is held from cowshed's identity check through fsck and the mount that follows. An
/// independent forced release is refused in each window: the injected releases fire inside fsck and
/// mount (`EjectAtFsck`), and an independent hdiutil detach fires between verification and mount.
/// Every attack starts from the kernel-proven unmounted state. A foreign volume is never attached,
/// whichever window the release tries.
#[test]
fn real_apfs_raw_pin_prevents_foreign_eject_during_fsck() {
    let fixture = RealFixture::new("pin-vs-foreign-eject");
    let owned = fixture.root().join("owned.dmg");
    let foreign = fixture.root().join("foreign.dmg");
    fixture.blank_image(&owned);
    fixture.blank_image(&foreign);
    let runner = EjectAtFsck {
        system: SystemCommandRunner::new(fixture.host_namespace()),
        owned: owned.clone(),
        foreign: foreign.clone(),
        ejected: AtomicUsize::new(0),
        release_attempts: AtomicUsize::new(0),
    };
    let backend = cowshed_apfs::MacOsApfsBackend::new(runner);
    let attachment = backend
        .attach_verified(&owned)
        .expect("owned image verified");
    assert_eq!(
        backend.runner().ejected.load(Ordering::SeqCst),
        0,
        "independent image-driver detach released an image while its raw volume was pinned"
    );
    assert_eq!(backend.runner().release_attempts.load(Ordering::SeqCst), 1);
    assert!(
        SystemKernelMountSource
            .mounts()
            .expect("kernel mount inventory between verification and mount")
            .iter()
            .all(|mount| mount.source_device != attachment.volume_device()),
        "verified attachment must remain unmounted before the independent forced detach"
    );
    let image_driver_request = CommandRequest::new(
        "/usr/bin/hdiutil",
        ["detach", "-force", attachment.whole_device()],
    );
    let image_driver_ejected = SystemCommandRunner::new(fixture.host_namespace())
        .run(&image_driver_request, "fixture image-driver detach")
        .expect("external image-driver forced detach")
        .output;
    if image_driver_ejected.succeeded() {
        let attached_foreign = SystemCommandRunner::new(fixture.host_namespace())
            .run(
                &foreign_kernel_attach(&foreign, true),
                "fixture foreign attach",
            )
            .expect("external foreign attachment")
            .output;
        assert!(
            attached_foreign.succeeded(),
            "foreign RAW mount: {attached_foreign:?}"
        );
        let volume = attachment.volume_device().trim_start_matches("/dev/");
        cowshed_os::eprintln!(
            "foreign volume reused {volume}: {}",
            String::from_utf8_lossy(&attached_foreign.stdout)
                .contains(&format!("<string>/dev/{volume}</string>"))
        );
    }
    let refusal = ApfsError::CommandFailed {
        operation: "independent forced detach between verification and mount",
        request: image_driver_request,
        output: image_driver_ejected,
    };
    assert!(
        cowshed_apfs::detach_was_dissented(&refusal),
        "verified image must refuse exact-whole forced detach as Busy while pinned: {refusal}"
    );
    let mount = fixture.root().join("owned-mount");
    backend
        .mount(&attachment, &mount, MountAccess::ReadWrite, false)
        .expect("pin also holds while mounting");
    assert_eq!(
        backend.runner().ejected.load(Ordering::SeqCst),
        0,
        "independent image-driver detach released an image while its raw volume was pinned during mount"
    );
    assert_eq!(backend.runner().release_attempts.load(Ordering::SeqCst), 2);
    backend
        .detach(&attachment, DetachIntent::Release)
        .expect("detach owned image");
    assert!(!attached(&owned));
    assert!(!attached(&foreign), "the foreign image was never attached");
}

/// Another process's attach of `image`, outside every cowshed image lease and check: the kernel
/// RAW attach cowshed itself runs, mounted by the tool when `mount` says so, never a DiskImages2
/// attach.
fn foreign_kernel_attach(image: &Path, mount: bool) -> CommandRequest {
    let mut args: Vec<std::ffi::OsString> = vec!["attach".into(), "-kernel".into()];
    if !mount {
        args.push("-nomount".into());
    }
    args.extend(
        [
            "-noverify",
            "-nobrowse",
            "-imagekey",
            "diskimage-class=CRawDiskImage",
            "-plist",
        ]
        .map(std::ffi::OsString::from),
    );
    args.push(image.as_os_str().to_owned());
    CommandRequest::new("/usr/bin/hdiutil", args)
}

/// Attach `image` the way another process does, outside every cowshed image lease and check.
fn attach_behind_cowshed(image: &Path) {
    let attached = helper_system()
        .run(
            &foreign_kernel_attach(image, false),
            "foreign kernel attach",
        )
        .expect("foreign kernel attach")
        .output;
    assert!(
        attached.succeeded(),
        "attach {}: {}",
        image.display(),
        String::from_utf8_lossy(&attached.stderr)
    );
}

/// What a killed template minter left behind: `image` attached, its file then removed while the
/// attachment lives on and keeps the path, a new image put at the same path, and that one
/// attached too. The kernel then holds one path twice.
fn hold_twice(fixture: &RealFixture, image: &Path) {
    fixture.blank_image(image);
    attach_behind_cowshed(image);
    std::fs::remove_file(image).expect("remove the attached image's file");
    fixture.blank_image(image);
    attach_behind_cowshed(image);
    assert_eq!(kernel_attachments(image).len(), 2, "the path is held twice");
}

/// The minter's own leftovers, as the incident left them: the single staging name older minters
/// reused, held twice, and a staging image of this minter's kind whose file is gone while its
/// attachment stays. A path the kernel holds twice wedged every real-APFS run on the host: each
/// lookup refused it as `matching image has multiple inventory entries`, the sweep included.
/// Cowshed never adds a third attachment to such a path. The next mint releases every attachment
/// of both leftovers through `release_image`, the release every image detach ends in,
/// removes their files, and mints the template, which it leaves detached under its name.
#[test]
fn real_apfs_a_template_mint_releases_every_leftover_staging_attachment() {
    let fixture = RealFixture::new("template-leftovers");
    let store = fixture.root().join("store");
    let capacity = blank_image::CAPACITY;
    let template = blank_template_path(&store, capacity);
    let reused = template.with_file_name(format!(
        "{}-minting.dmg",
        template
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("a template stem")
    ));
    hold_twice(&fixture, &reused);
    let refused = MacOsApfsBackend::new(SystemCommandRunner::new(fixture.host_namespace()))
        .attach_verified(&reused)
        .expect_err("a held path is never attached again");
    assert!(
        matches!(&refused, ApfsError::InvalidAttachmentInventory(message)
            if message.contains("is already attached as")),
        "{refused}"
    );
    assert_eq!(kernel_attachments(&reused).len(), 2);
    let orphaned = blank_template_staged_stem(&store, capacity).with_extension("dmg");
    fixture.blank_image(&orphaned);
    attach_behind_cowshed(&orphaned);
    std::fs::remove_file(&orphaned).expect("remove the attached image's file");
    assert!(attached(&orphaned));

    let minted = blank_template(
        &MacOsApfsBackend::new(SystemCommandRunner::new(fixture.host_namespace())),
        &store,
        &fixture.host_namespace().label_prefix(),
        capacity,
        &cowshed_core::held_lock::ImageLockTaker {
            holding: &cowshed_core::api::held_lock::LockHolding::store(
                cowshed_core::api::held_lock::LockOperation::MintTemplate,
            ),
        },
    )
    .expect("mint over the leftovers");

    assert_eq!(minted, template);
    assert!(template.is_file());
    assert!(!attached(&template));
    for leftover in [&reused, &orphaned] {
        assert!(
            !attached(leftover),
            "{}: {:?}",
            leftover.display(),
            kernel_attachments(leftover)
        );
        assert!(!leftover.exists(), "{}", leftover.display());
    }
    let staging = std::fs::read_dir(template.parent().expect("a template directory"))
        .expect("list templates")
        .map(|entry| entry.expect("template entry").path())
        .filter(|path| path.to_string_lossy().contains("-minting"))
        .collect::<Vec<_>>();
    assert_eq!(staging, Vec::<PathBuf>::new());
}

/// Release the owner's real attachment before cowshed can open its raw volume, then attach a
/// foreign image, so the owner's recorded volume spelling names the foreign volume. The owner must
/// refuse without another attach or fsck.
///
/// The kernel does not promise to hand the freed BSD unit to the next attach: on a shared host
/// another attach can take it first, or the foreign image can land on a lower free unit. So the
/// reuse is made exact here rather than hoped for: once the owner is gone, opening the owner's
/// recorded raw volume opens the foreign replacement's real raw volume, a live descriptor on
/// foreign media, which is what a recycled spelling hands the pin. Everything else stays the
/// kernel's: the attachments, the inventory the identity proof reads, the mount table.
struct EjectAfterAttach {
    system: SystemCommandRunner,
    owned: PathBuf,
    foreign: PathBuf,
    ejected: AtomicUsize,
    fscks: AtomicUsize,
    /// The owner's recorded raw volume and the foreign replacement's, once the owner is ejected.
    reused: Mutex<Option<(PathBuf, PathBuf)>>,
    /// How many times the owner's recorded raw volume was opened onto the foreign volume.
    pinned_foreign: AtomicUsize,
}

/// The raw device node of a `/dev/diskN[sM]` block device.
fn raw_device(device: &str) -> PathBuf {
    PathBuf::from(device.replacen("/dev/disk", "/dev/rdisk", 1))
}

impl CommandRunner for EjectAfterAttach {
    fn run_guarded<E>(
        &self,
        request: &CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<CommandOutput>, E>
    where
        E: From<CommandRunError> + std::fmt::Display,
    {
        self.system.run_guarded(request, check)
    }
    fn run(
        &self,
        request: &CommandRequest,
        _operation: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
        let system = self.system;
        let output = system.run(request, _operation)?;
        if request.program == Path::new("/sbin/fsck_apfs") {
            self.fscks.fetch_add(1, Ordering::SeqCst);
        }
        if request.program == Path::new("/usr/bin/hdiutil")
            && request.args.first().is_some_and(|arg| arg == "attach")
            && request
                .args
                .last()
                .is_some_and(|arg| arg == self.owned.as_os_str())
            && self.ejected.fetch_add(1, Ordering::SeqCst) == 0
        {
            assert!(output.output.succeeded(), "owner attach failed: {output:?}");
            let owned = cowshed_apfs::MacOsApfsBackend::new(self.system)
                .existing_attachment(&self.owned)
                .expect("exact current owner mapping")
                .expect("the recorded fixture image is attached");
            let whole = owned.whole_device();
            let ejected = system
                .run(
                    &CommandRequest::new("/usr/bin/hdiutil", ["detach", "-force", whole]),
                    "fixture detach",
                )?
                .output;
            assert!(
                ejected.succeeded(),
                "unheld exact whole must detach: {ejected:?}"
            );
            let attached = system
                .run(
                    &foreign_kernel_attach(&self.foreign, true),
                    "fixture foreign attach",
                )?
                .output;
            assert!(attached.succeeded(), "foreign mount failed: {attached:?}");
            let replacement = cowshed_apfs::MacOsApfsBackend::new(self.system)
                .existing_attachment(&self.foreign)
                .expect("exact current replacement mapping")
                .expect("the recorded foreign image is attached");
            *self.reused.lock().expect("reused spelling") = Some((
                raw_device(owned.volume_device()),
                raw_device(replacement.volume_device()),
            ));
        }
        Ok(output)
    }

    fn image_lease(&self, identity: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        self.system.image_lease(identity)
    }

    fn pin_raw_device(&self, device: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        match &*self.reused.lock().expect("reused spelling") {
            Some((recorded, foreign)) if device == recorded => {
                self.pinned_foreign.fetch_add(1, Ordering::SeqCst);
                self.system.pin_raw_device(foreign)
            }
            _ => self.system.pin_raw_device(device),
        }
    }

    fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
        self.system.attached_disk_images()
    }

    fn attachments_of(&self, image: &Path) -> std::io::Result<Vec<AttachedDiskImage>> {
        self.system.attachments_of(image)
    }

    fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> std::io::Result<()> {
        self.system.grow_image(image, capacity)
    }
}

#[test]
fn real_apfs_lost_attachment_refuses_without_verifying_foreign_volume() {
    let fixture = RealFixture::new("refuse-after-reuse");
    let owned = fixture.root().join("owned.dmg");
    let foreign = fixture.root().join("foreign.dmg");
    fixture.blank_image(&owned);
    fixture.blank_image(&foreign);
    let backend = cowshed_apfs::MacOsApfsBackend::new(EjectAfterAttach {
        system: SystemCommandRunner::new(fixture.host_namespace()),
        owned: owned.clone(),
        foreign: foreign.clone(),
        ejected: AtomicUsize::new(0),
        fscks: AtomicUsize::new(0),
        reused: Mutex::new(None),
        pinned_foreign: AtomicUsize::new(0),
    });
    let refusal = backend
        .attach_verified(&owned)
        .expect_err("a lost owner attachment cannot authorize another attach or a foreign fsck");
    assert!(
        matches!(&refusal, ApfsError::InvalidAttachmentInventory(detail) if detail.contains("refusing a reused device")),
        "lost attachment must retain its exact-image ownership refusal: {refusal}"
    );
    assert_eq!(backend.runner().ejected.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend.runner().pinned_foreign.load(Ordering::SeqCst),
        1,
        "the pin must have opened the foreign volume the reused spelling names"
    );
    assert_eq!(backend.runner().fscks.load(Ordering::SeqCst), 0);
    assert!(
        !attached(&owned),
        "the refused attempt must not reattach the owner"
    );
    let replacement =
        cowshed_apfs::MacOsApfsBackend::new(SystemCommandRunner::new(fixture.host_namespace()))
            .existing_attachment(&foreign)
            .expect("exact current foreign mapping")
            .expect("the foreign attachment must survive the owner's refusal");
    assert!(
        SystemKernelMountSource
            .mounts()
            .expect("kernel mount inventory")
            .iter()
            .any(|mount| mount.source_device == replacement.volume_device()),
        "the owner's refusal must leave the foreign mount intact"
    );
}

/// A mount an earlier controller left is a kernel fact a restarted host reads back, identified by
/// the in-image marker rather than the volume label (here renamed by hand between the two), and
/// a `WhenIdle` detach of it reports the holder's dissent rather than forcing, and the restarted
/// host detaches it by its mountpoint after the holder lets go.
#[test]
fn real_apfs_kernel_mount_facts_survive_host_restart_and_detach_by_mountpoint() {
    let fixture = RealFixture::new("kernel-restart");
    let canonical = fixture.layout().main_image().expect("canonical");
    fixture.published_image(canonical.image());
    let mount = fixture.main_mount();
    fixture.mount_left_behind(canonical.image(), &mount, "nobrowse,owners");
    fixture.plant_marker(&mount, &workspace());
    let kernel_id = kernel_mount_at(&mount).expect("mounted").mount_id;

    let first = fixture.host();
    assert_eq!(
        proven_mount_ids(first.mounts(&repo()).expect("first facts"))[0],
        kernel_id
    );
    drop(first);

    let restarted = fixture.host();
    assert_eq!(
        restarted
            .label_image_volume(canonical.image(), "hand-renamed")
            .expect("rename the volume behind cowshed's back"),
        Labelled::Renamed
    );
    assert_eq!(
        proven_mount_ids(restarted.mounts(&repo()).expect("restart facts"))[0],
        kernel_id,
        "mount identity is read from the in-image marker, never from the volume label"
    );
    let holder = std::fs::File::create(mount.join("held")).expect("hold the volume");

    let error = restarted
        .detach_mounted(&workspace(), DetachIntent::WhenIdle)
        .expect_err("a dissented WhenIdle detach must not escalate");
    assert!(
        matches!(
            &error,
            ApfsStorageError::Apfs(ApfsError::UnmountFailed {
                source: cowshed_apfs::mounts::NativeUnmountError::Busy(_),
                ..
            })
        ),
        "unexpected refusal: {error}"
    );
    assert!(
        kernel_mount_at(&mount).is_some(),
        "the dissented volume is left mounted"
    );

    drop(holder);

    restarted
        .detach_mounted(&workspace(), DetachIntent::Release)
        .expect("restart-safe detach");
    assert!(kernel_mount_at(&mount).is_none());
    assert!(!attached(canonical.image()));
}

#[test]
fn real_apfs_canonical_path_with_an_unrelated_volume_fails_closed_without_detaching() {
    let fixture = RealFixture::new("wrong-source");
    let canonical = fixture.layout().main_image().expect("canonical");
    fixture.published_image(canonical.image());
    // An impostor that is itself a well-formed cowshed volume — a different project's, on an
    // image of its own — proves the check reads identity out of the marker rather than merely
    // noticing a missing file.
    let impostor_image = fixture.root().join("impostor/main.dmg");
    fixture.blank_image(&impostor_image);
    let mount = fixture.main_mount();
    fixture.mount_left_behind(&impostor_image, &mount, "nobrowse,owners");
    let foreign = LifecycleWorkspace::new(
        RepoId::parse("other/widget").expect("repo"),
        WorkspaceName::new("main").expect("main"),
        WorkspaceIncarnation::new("00000000000000000000000000000009").expect("incarnation"),
        Revision::new(1),
        Revision::new(1),
        WorkspaceRole::Main,
    )
    .expect("foreign workspace");
    fixture.plant_marker(&mount, &foreign);
    let impostor = kernel_mount_at(&mount).expect("impostor mounted");
    let host = fixture.host();

    // The impostor is main's finding, not a failed enumeration: every other workspace of the
    // project still enumerates, and the read that names main refuses.
    let joins = host
        .mounts(&repo())
        .expect("an impostor mount is a per-workspace finding");
    let [MountJoin::Refused { refusal, .. }] = joins.as_slice() else {
        panic!("the impostor is refused as main's join: {joins:?}");
    };
    assert_eq!(refusal.workspace.name(), workspace().name());
    assert_eq!(refusal.mount_point, mount);
    assert_eq!(refusal.mounted_from, impostor.source_device);
    assert!(
        matches!(
            &refusal.reason,
            MountRefusalReason::MarkerNamesAnother { repo, .. } if repo == foreign.repo()
        ),
        "the refusal names the marker it read: {refusal}"
    );
    assert!(refusal.to_string().contains("mount identity mismatch"));
    let error = host
        .workspace_snapshot(&repo(), workspace().name())
        .expect_err("the read that names main refuses its impostor");
    assert!(error.to_string().contains("mount identity mismatch"));
    let error = host
        .heal_mount(&workspace(), &mount)
        .expect_err("impostor must not be healed destructively");
    assert!(error.to_string().contains("cannot use unrelated mount"));
    let error = host
        .detach_mounted(&workspace(), DetachIntent::Release)
        .expect_err("restart-safe detach must reject an impostor source");
    assert!(
        error
            .to_string()
            .contains("refusing to detach unrelated mount")
    );
    assert_eq!(
        kernel_mount_at(&mount).map(|mount| mount.source_device),
        Some(impostor.source_device),
        "an impostor mount is rejected from its marker alone and never detached"
    );
}

/// The direct-mount handoff has one job the symlink handoff also has: the user's path is never
/// absent and never half-built. It gets there with the same `RENAME_SWAP`, exchanging the original
/// tree for an empty mountpoint without creating repository shell hooks.
#[test]
fn direct_mount_handoff_swaps_the_checkout_for_an_empty_mountpoint() {
    let fixture = Fixture::new("direct-vacate");
    let checkout = fixture.root.join("mount");
    let pre_cowshed = fixture.root.join("mount.pre-cowshed");
    std::fs::create_dir_all(&checkout).expect("checkout");
    std::fs::write(checkout.join("README"), b"the user's tree").expect("user file");
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner::new(fixture.host_namespace()),
        fixture.config(),
    )
    .expect("host");

    host.vacate_adopted_checkout(&checkout, &pre_cowshed)
        .expect("vacate");

    // The checkout path is now cowshed's empty mountpoint...
    assert!(checkout.is_dir());
    assert_eq!(
        std::fs::read_dir(&checkout).expect("mountpoint").count(),
        0,
        "cowshed creates no shell hook underneath the mount"
    );
    // ...and the user's tree is retained beside it, whole.
    assert_eq!(
        std::fs::read(pre_cowshed.join("README")).expect("retained tree"),
        b"the user's tree"
    );
    assert!(
        !fixture.root.join("mount.cowshed-mountpoint").exists(),
        "the staging name is consumed by the swap"
    );

    // Re-running is a no-op: the swap is already done and nothing is left to retain.
    host.vacate_adopted_checkout(&checkout, &pre_cowshed)
        .expect("idempotent vacate");
    assert_eq!(
        std::fs::read(pre_cowshed.join("README")).expect("retained tree"),
        b"the user's tree"
    );
}

#[test]
fn direct_mount_handoff_refuses_a_checkout_it_did_not_take_over() {
    let fixture = Fixture::new("direct-vacate-refuse");
    let checkout = fixture.root.join("mount");
    let pre_cowshed = fixture.root.join("mount.pre-cowshed");
    std::fs::create_dir_all(&checkout).expect("checkout");
    std::fs::create_dir_all(&pre_cowshed).expect("collision");
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner::new(fixture.host_namespace()),
        fixture.config(),
    )
    .expect("host");

    // A retained tree already at the destination means a previous adoption left state behind.
    host.vacate_adopted_checkout(&checkout, &pre_cowshed)
        .expect_err("pre-cowshed collision must be refused before the user's tree is touched");
    assert!(
        !checkout.join(".envrc").exists(),
        "a refused handoff must leave the checkout exactly as it was"
    );
}

fn interrupted_restore_image_publication(
    fixture: &Fixture,
    failpoint: RestoreFailpoint,
) -> (
    MacOsApfsExecutionHost<RecordingRunner>,
    PathBuf,
    PathBuf,
    PathBuf,
    DetachedWorkspaceMetadata,
) {
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical").image().to_owned();
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.dmg");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.dmg");
    create_image(&canonical);
    std::fs::write(&canonical, b"old generation").expect("old image");
    create_image(&staged);
    std::fs::write(&staged, b"new generation").expect("new image");
    let mut replacement = metadata();
    replacement.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("next incarnation");
    replacement
        .write_for_image(&staged)
        .expect("replacement metadata");
    let host = native_host(fixture, RecordingRunner::default());
    host.set_restore_failpoint(failpoint);
    host.restore_swap(&staged, &canonical, &undo)
        .expect_err("injected image publication crash");
    (host, canonical, staged, undo, replacement)
}

#[test]
fn recovery_quarantines_a_canonical_image_missing_its_ca_companion() {
    let missing = Fixture::new("recovery-missing-ca");
    let missing_layout = StorageLayout::new(&missing.root, &repo()).expect("layout");
    adopt_owner(&missing_layout);
    let missing_canonical = missing_layout.main_image().expect("canonical");
    create_image(missing_canonical.image());
    std::fs::remove_file(ca_key_path(missing_canonical.image())).expect("remove CA key");
    // A missing CA companion beside a published image is a per-workspace integrity failure:
    // the pass completes, the sidecar quarantines with a tombstone, and the image stays.
    native_host(&missing, RecordingRunner::default())
        .recover_pending(&missing.config(), &[])
        .expect("a keyless canonical quarantines instead of failing the pass");
    assert!(
        missing_canonical.image().exists(),
        "the image stays in place"
    );
    assert!(
        !sidecar_path(missing_canonical.image()).exists(),
        "the keyless grants sidecar leaves the canonical location"
    );
    let missing_project = missing_layout.project().project_root.clone();
    let mut missing_entries: Vec<PathBuf> = std::fs::read_dir(missing_project.join("quarantine"))
        .expect("quarantine root")
        .map(|entry| entry.expect("quarantine entry").path())
        .collect();
    missing_entries.sort();
    assert_eq!(missing_entries.len(), 1, "one workspace quarantined");
    let missing_wire: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(missing_entries[0].join("tombstone.json")).expect("tombstone"),
    )
    .expect("tombstone json");
    assert_eq!(
        missing_wire["workspace"],
        serde_json::Value::String("main".to_owned())
    );
    assert_eq!(
        missing_wire["reason"],
        serde_json::Value::String("missing-ca-companion".to_owned())
    );
    assert_eq!(
        missing_wire["image"],
        serde_json::Value::String(missing_canonical.image().display().to_string())
    );
}

/// A sibling whose payload is provably gone must not fail recovery for the rest of the store.
///
/// `recover_pending` runs at project open (`runtime/project.rs`), before any verb dispatches, so
/// refusing here failed `ls`, `rm`, and even the `cowshed doctor --json` the refusal itself
/// pointed at — for every *other* workspace, none of which was inconsistent. Reproduced on a real
/// host by parking one session image aside and leaving its sidecar and CA key: `cowshed ls` exited
/// 7 immediately.
#[test]
fn an_imageless_sibling_publication_is_reconciled_instead_of_failing_every_other_workspace() {
    let fixture = Fixture::new("orphan-publication-sibling");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);

    let live = WorkspaceName::new("session-live").expect("live session");
    let live_canonical = layout.session_image(&live).expect("live canonical");
    create_image(live_canonical.image());
    let mut live_metadata = metadata();
    set_metadata_workspace(&mut live_metadata, live.clone());
    live_metadata
        .write_for_image(live_canonical.image())
        .expect("live metadata");

    let dangling = WorkspaceName::new("session-dangling").expect("dangling session");
    let dangling_canonical = layout.session_image(&dangling).expect("dangling canonical");
    create_image(dangling_canonical.image());
    let mut dangling_metadata = metadata();
    set_metadata_workspace(&mut dangling_metadata, dangling.clone());
    dangling_metadata
        .write_for_image(dangling_canonical.image())
        .expect("dangling metadata");
    // Sidecar and CA companion published, image gone: an interrupted publication or an
    // interrupted retirement, indistinguishable and identical in consequence.
    std::fs::remove_file(dangling_canonical.image()).expect("lose the dangling payload");
    assert!(sidecar_path(dangling_canonical.image()).exists());
    assert!(ca_key_path(dangling_canonical.image()).exists());

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("a sibling whose image is provably gone must not fail project recovery");

    assert!(
        !sidecar_path(dangling_canonical.image()).exists(),
        "the orphaned grants sidecar is reconciled away"
    );
    assert!(
        !ca_key_path(dangling_canonical.image()).exists(),
        "the orphaned CA key goes with it rather than being stranded as an unreachable private key"
    );
    let listed = host
        .list(&repo())
        .expect("recovery leaves the project enumerable")
        .into_iter()
        .map(|fact| fact.workspace.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        listed,
        vec![live],
        "the live sibling survives and stays enumerable, so its removal is unblocked"
    );
    assert_eq!(
        std::fs::read(live_canonical.image()).expect("live image"),
        b"fixture",
        "reconciling one workspace's metadata must not touch another's payload"
    );
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated recovery is idempotent");
}

/// The crash the deleting directory exists for (02_workspaces.md, `rm` step 5): the release
/// renamed the clean source image to `sessions/.deleting/<ws>-<incarnation>.dmg` and died with its
/// grants sidecar and CA key still canonical, and the background deletion made its first bounded cut
/// and stopped. The next open's recovery reconciles the record the rename left imageless without
/// touching the entry, the workspace stays gone, and the gateway's next pass frees what the cut
/// left.
#[test]
fn a_workspace_release_is_reconciled_by_recovery_and_finished_by_the_next_deleter_pass() {
    use cowshed_core::storage::apfs::release::{DELETING_DIRECTORY, Opened, Release, StepExtents};
    use std::os::unix::fs::FileExt;

    let fixture = Fixture::new("release-journal-recovery");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let name = WorkspaceName::new("session-released").expect("name");
    let canonical = layout.session_image(&name).expect("canonical");
    create_image(canonical.image());
    let mut released = metadata();
    set_metadata_workspace(&mut released, name.clone());
    released
        .write_for_image(canonical.image())
        .expect("released metadata");

    // A payload of many physical runs: written, cloned, and every other page rewritten while
    // the clone shares it, so one cut of one run leaves the rest.
    const PAGE: u64 = 16 * 1024;
    let image = OpenOptions::new()
        .write(true)
        .read(true)
        .open(canonical.image())
        .expect("open payload");
    image
        .write_all_at(&vec![0x5a; 4 * 1024 * 1024], 0)
        .expect("payload");
    image.sync_all().expect("flush payload");
    let holder = canonical.image().with_extension("holder");
    assert!(
        Command::new("/bin/cp")
            .arg("-c")
            .arg(canonical.image())
            .arg(&holder)
            .status_bounded()
            .expect("cp -c")
            .success()
    );
    let mut page = vec![0_u8; usize::try_from(PAGE).expect("page")];
    for offset in (0..4 * 1024 * 1024).step_by(usize::try_from(2 * PAGE).expect("stride")) {
        image.read_exact_at(&mut page, offset).expect("read page");
        image.write_all_at(&page, offset).expect("rewrite page");
    }
    image.sync_all().expect("flush fragments");
    drop(image);
    std::fs::remove_file(&holder).expect("drop the clone");

    // The release's commit, the deleter's first cut, and the stop.
    let entry = canonical
        .image()
        .parent()
        .expect("sessions")
        .join(DELETING_DIRECTORY)
        .join(format!(
            "{}-{}.dmg",
            name.as_str(),
            released.workspace_incarnation
        ));
    std::fs::create_dir_all(entry.parent().expect("journal")).expect("journal");
    std::fs::rename(canonical.image(), &entry).expect("commit");
    let length = std::fs::metadata(&entry).expect("entry").len();
    let Opened::Release(mut release) =
        Release::open(&entry, StepExtents::new(std::num::NonZeroU64::MIN))
            .expect("open the release")
    else {
        panic!("the committed entry opens for the deleter");
    };
    let cut = release.step().expect("cut").expect("a cut is left");
    assert!(
        cut > 0 && cut < length,
        "one run cut, the rest left: {cut} of {length}"
    );
    drop(release);

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("recovery reconciles the committed release");
    assert_eq!(
        std::fs::metadata(&entry).expect("the entry stays").len(),
        cut,
        "recovery frees nothing: the background deletion does"
    );
    assert!(
        !sidecar_path(canonical.image()).exists() && !ca_key_path(canonical.image()).exists(),
        "the record goes once its image is gone"
    );
    assert!(
        !canonical.image().exists(),
        "nothing brings the workspace back"
    );
    assert!(
        host.list(&repo())
            .expect("the project stays enumerable")
            .iter()
            .all(|fact| fact.workspace.name() != &name)
    );
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated recovery is idempotent");
    let listing = cowshed_core::storage::apfs::released_sessions(&fixture.root);
    assert_eq!(listing.entries, std::slice::from_ref(&entry), "{listing:?}");
    assert!(listing.left.is_empty(), "{listing:?}");
    assert!(matches!(
        cowshed_core::storage::apfs::release::free_committed(&entry).expect("the cleanup's free"),
        cowshed_core::storage::apfs::release::Released::Freed { .. }
    ));
    assert!(
        !entry.exists(),
        "the cleanup frees and unlinks the entry the stop left"
    );
}

/// A session create that died between its grants sidecar and its CA key leaves a staged image with
/// a sidecar and no companion (`prepare_clone_stage`: `publish_metadata` lands before attach, mount
/// and `mint_workspace_credentials`). Nothing names that image for publication, so recovery has
/// nothing to complete; refusing instead failed every verb of the project, including the gc that
/// reclaims it. Reproduced on a real host: one interrupted `cowshed new` made `ls`, `land` and
/// `doctor` all exit with "staged session publication image is missing its CA companion".
#[test]
fn a_keyless_staged_session_image_is_left_to_gc_instead_of_failing_every_verb() {
    let fixture = Fixture::new("staging-keyless-session");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);

    let live = WorkspaceName::new("session-live").expect("live session");
    let live_canonical = layout.session_image(&live).expect("live canonical");
    create_image(live_canonical.image());
    let mut live_metadata = metadata();
    set_metadata_workspace(&mut live_metadata, live.clone());
    live_metadata
        .write_for_image(live_canonical.image())
        .expect("live metadata");

    let dead = WorkspaceName::new("session-dead").expect("dead session");
    let staged = layout
        .project()
        .project_root
        .join(".staging/session-dead-00000000000000000000000000000002.dmg");
    create_image(&staged);
    let mut dead_metadata = metadata();
    set_metadata_workspace(&mut dead_metadata, dead.clone());
    dead_metadata.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("incarnation");
    dead_metadata
        .write_for_image(&staged)
        .expect("staged metadata");
    std::fs::remove_file(ca_key_path(&staged)).expect("die before the CA key is minted");
    assert!(sidecar_path(&staged).exists());
    assert!(!ca_key_path(&staged).exists());

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("a keyless staged session image must not fail project recovery");
    assert!(
        staged.exists(),
        "recovery does not decide the orphan's fate"
    );
    assert!(sidecar_path(&staged).exists());
    let listed = host
        .list(&repo())
        .expect("recovery leaves the project enumerable")
        .into_iter()
        .map(|fact| fact.workspace.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(listed, vec![live], "the live sibling stays enumerable");

    let plan = host
        .preview_gc(&fixture.config(), &repo(), StorageGcOptions::default())
        .expect("plan");
    let orphans = plan
        .candidates()
        .iter()
        .filter(|candidate| candidate.reason() == StorageGcReason::OrphanStagingImage)
        .map(|candidate| candidate.path().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        orphans,
        vec![staged.clone()],
        "the orphan is the sweep's, not recovery's"
    );
    host.execute_gc(&fixture.config(), plan)
        .expect("staging gc");
    assert!(!staged.exists(), "the keyless staged image is reclaimed");
    assert!(!sidecar_path(&staged).exists());
    assert_eq!(
        std::fs::read(live_canonical.image()).expect("live image"),
        b"fixture",
        "the sweep must not touch the live sibling's payload"
    );
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated recovery is idempotent");
}

/// The boundary of the reconciliation above: an in-flight restore is the one reading of a missing
/// canonical image that is not "the payload is gone", so it keeps its evidence and still refuses.
#[test]
fn an_imageless_publication_with_a_restore_in_flight_keeps_its_evidence_and_still_refuses() {
    let fixture = Fixture::new("orphan-publication-restore-in-flight");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.dmg");
    create_image(canonical.image());
    create_image(&undo);
    std::fs::write(&undo, b"old generation").expect("undo payload");
    std::fs::remove_file(canonical.image()).expect("lose the canonical image");
    std::fs::write(restore_recovery_fact_path(canonical.image()), b"{}")
        .expect("restore recovery fact");

    let error = native_host(&fixture, RecordingRunner::default())
        .recover_pending(&fixture.config(), &[])
        .expect_err("an in-flight restore is ambiguous and must still fail closed");
    match error {
        ApfsStorageError::MarkerMismatch(message) => {
            assert!(message.contains("restore recovery has no canonical image"));
            assert!(message.contains(&canonical.image().display().to_string()));
        }
        other => panic!("expected typed restore integrity error, got {other:?}"),
    }
    assert!(
        sidecar_path(canonical.image()).exists(),
        "reconciliation must not consume the evidence the restore pass reads"
    );
    assert!(ca_key_path(canonical.image()).exists());
    assert_eq!(
        std::fs::read(&undo).expect("undo"),
        b"old generation",
        "the retained undo generation is the payload and must survive"
    );
}

/// Staging is judged by ownership, not by shape: an image with its grants sidecar and one
/// without are both garbage once nothing runs and nothing publishes them, while a staged image a
/// crashed sidecar-first publication still names is recovery's evidence and stays.
#[test]
fn gc_reclaims_unowned_staging_pairs_and_keeps_a_recoverable_publication() {
    let fixture = Fixture::new("staging-orphan");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let staging = layout.project().project_root.join(".staging");
    let orphan = staging.join("session-a-00000000000000000000000000000002.dmg");
    std::fs::create_dir_all(&staging).expect("staging directory");
    std::fs::write(&orphan, b"crashed before metadata").expect("orphan staged image");
    let abandoned = staging.join("session-b-00000000000000000000000000000003.dmg");
    create_image(&abandoned);
    let recoverable = staging.join("main-00000000000000000000000000000001.dmg");
    std::fs::write(&recoverable, b"published generation").expect("recoverable staged image");
    let canonical = layout.main_image().expect("canonical");
    metadata()
        .write_for_image(canonical.image())
        .expect("canonical sidecar of a sidecar-first publication whose rename never ran");

    let host = native_host(&fixture, RecordingRunner::default());
    let plan = host
        .preview_gc(&fixture.config(), &repo(), StorageGcOptions::default())
        .expect("plan");
    assert_eq!(
        plan.retained_active(),
        1,
        "the recoverable publication is owned"
    );
    let report = host
        .execute_gc(&fixture.config(), plan)
        .expect("staging gc");

    assert_eq!(report.examined, 3);
    assert_eq!(report.reclaimed, 2);
    assert_eq!(report.retained_active, 1);
    assert!(!orphan.exists(), "sidecarless crash image is reclaimed");
    assert!(
        !abandoned.exists(),
        "an abandoned image is reclaimed with its sidecar"
    );
    assert!(!sidecar_path(&abandoned).exists());
    assert!(!ca_key_path(&abandoned).exists());
    assert!(
        recoverable.exists(),
        "a staged image a canonical sidecar names is evidence"
    );
    assert!(sidecar_path(canonical.image()).exists());
}

/// A mountpoint under the mount root's staging directory with no image behind it is the debris
/// of an interrupted operation; one whose volume the kernel still holds is detached first, and
/// one an operation still owns (its lifecycle lock is held) is retained and counted.
#[test]
fn real_apfs_gc_retires_orphaned_staging_mountpoints_and_detaches_the_ones_still_attached() {
    let fixture = RealFixture::new("staging-mounts");
    let layout = fixture.layout();
    let staging_mounts = layout.project().mount_root.join(".staging");
    let empty = staging_mounts.join("session-a-00000000000000000000000000000002");
    let held = staging_mounts.join("session-b-00000000000000000000000000000003");
    let owned = staging_mounts.join("session-c-00000000000000000000000000000004");
    for directory in [&empty, &held, &owned] {
        std::fs::create_dir_all(directory).expect("staging mountpoint");
    }
    std::fs::write(staging_mounts.join("not-a-staging-name"), b"ignored").expect("noise");
    let staged = layout
        .project()
        .project_root
        .join(".staging/session-b-00000000000000000000000000000003.dmg");
    fixture.published_image(&staged);
    fixture.mount_left_behind(&staged, &held, "nobrowse,owners");
    let owned_lock = layout
        .session_image(&WorkspaceName::session("session-c").expect("session-c"))
        .expect("session-c image")
        .lock()
        .to_owned();

    let host = fixture.host();
    let owner = fixture.host();
    let guard = owner
        .lock_images(
            std::slice::from_ref(&owned_lock),
            LockMode::Wait,
            &other_operation(),
        )
        .expect("owner lock")
        .expect("blocking owner lock");

    let plan = host
        .preview_gc(&fixture.config(), &repo(), StorageGcOptions::default())
        .expect("plan");
    let reasons: Vec<_> = plan
        .candidates()
        .iter()
        .map(|candidate| (candidate.path().to_owned(), candidate.reason()))
        .collect();
    assert!(reasons.contains(&(empty.clone(), StorageGcReason::OrphanStagingMount)));
    assert!(reasons.contains(&(staged.clone(), StorageGcReason::OrphanStagingImage)));
    assert!(
        !reasons.iter().any(|(path, _)| path == &owned),
        "a mountpoint an operation still owns is not a candidate"
    );
    assert_eq!(plan.retained_active(), 1);
    let report = host
        .execute_gc(&fixture.config(), plan)
        .expect("staging mount gc");
    drop(guard);

    assert_eq!(report.reclaimed, 2);
    assert!(!empty.exists(), "the empty mountpoint is removed");
    assert!(
        kernel_mount_at(&held).is_none(),
        "the kernel-held volume is detached before its mountpoint goes"
    );
    assert!(
        !held.exists(),
        "the held mountpoint is removed after detach"
    );
    assert!(
        !attached(&staged),
        "nothing still holds the reclaimed image"
    );
    assert!(!staged.exists(), "the attached image is reclaimed");
    assert!(owned.exists(), "the owned mountpoint is retained");
}

#[cfg(unix)]
#[test]
fn gc_does_not_follow_symlinked_owner_repository_staging_or_image_paths() {
    let fixture = Fixture::new("gc-containment");
    let external = Fixture::new("gc-external-targets");
    let orphan_name = "main-00000000000000000000000000000002.dmg";
    let write_orphan = |path: &Path, contents: &[u8]| {
        std::fs::create_dir_all(path.parent().expect("external parent")).expect("external parent");
        std::fs::write(path, contents).expect("external staged image");
    };

    let owner_target = external
        .root
        .join("owner-target/repository/.staging")
        .join(orphan_name);
    write_orphan(&owner_target, b"owner target");
    std::os::unix::fs::symlink(
        external.root.join("owner-target"),
        fixture.root.join("linked-owner"),
    )
    .expect("owner symlink");

    let repository_target = external
        .root
        .join("repository-target/.staging")
        .join(orphan_name);
    write_orphan(&repository_target, b"repository target");
    std::fs::create_dir_all(fixture.root.join("real-owner")).expect("real owner");
    std::os::unix::fs::symlink(
        external.root.join("repository-target"),
        fixture.root.join("real-owner/linked-repository"),
    )
    .expect("repository symlink");

    let staging_target = external.root.join("staging-target").join(orphan_name);
    write_orphan(&staging_target, b"staging target");
    let staging_link = fixture.root.join("staging-owner/repository/.staging");
    std::fs::create_dir_all(staging_link.parent().expect("staging project"))
        .expect("staging project");
    std::os::unix::fs::symlink(external.root.join("staging-target"), &staging_link)
        .expect("staging symlink");

    let image_target = external.root.join("image-target.dmg");
    write_orphan(&image_target, b"image target");
    let real_staging = fixture.root.join("image-owner/repository/.staging");
    std::fs::create_dir_all(&real_staging).expect("real staging");
    let image_link = real_staging.join(orphan_name);
    std::os::unix::fs::symlink(&image_target, &image_link).expect("image symlink");
    let invalid_name = real_staging.join("manual-backup.dmg");
    std::fs::write(&invalid_name, b"not a transaction").expect("manual staging file");

    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let escaped_label = CheckpointLabel::new("escaped").expect("escaped label");
    let escaped_checkpoint = layout
        .checkpoint_image(&WorkspaceName::new("main").expect("main"), &escaped_label)
        .expect("escaped checkpoint")
        .image()
        .to_owned();
    std::fs::create_dir_all(escaped_checkpoint.parent().expect("checkpoint directory"))
        .expect("checkpoint directory");
    std::os::unix::fs::symlink(&image_target, &escaped_checkpoint).expect("checkpoint symlink");
    metadata()
        .write_for_image(&escaped_checkpoint)
        .expect("escaped checkpoint metadata");
    let host = native_host(&fixture, RecordingRunner::default());
    let error = host
        .publish_checkpoint_fact(
            &escaped_checkpoint,
            &escaped_label,
            Revision::new(99),
            Pin::Automatic,
        )
        .expect_err("symlinked checkpoint publication must fail closed");
    assert!(matches!(
        error,
        ApfsStorageError::Layout(StorageLayoutError::SymlinkComponent(path))
            if path == escaped_checkpoint
    ));
    assert!(
        host.checkpoints(&repo())
            .expect("checkpoint enumeration")
            .is_empty()
    );

    let report = execute_gc(&host, &fixture.config()).expect("contained gc");

    assert_eq!(report, Default::default());
    for (path, contents) in [
        (&owner_target, b"owner target".as_slice()),
        (&repository_target, b"repository target".as_slice()),
        (&staging_target, b"staging target".as_slice()),
        (&image_target, b"image target".as_slice()),
    ] {
        assert_eq!(std::fs::read(path).expect("external target"), contents);
    }
    assert!(
        std::fs::symlink_metadata(&escaped_checkpoint)
            .expect("checkpoint link")
            .file_type()
            .is_symlink()
    );
    assert!(
        std::fs::symlink_metadata(&image_link)
            .expect("image link")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(&invalid_name).expect("invalid staging name"),
        b"not a transaction"
    );
}

#[test]
fn checkpoint_pin_facts_survive_restart_and_gc_prunes_only_old_automatic_excess() {
    let fixture = Fixture::new("checkpoint-retention");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let main = WorkspaceName::new("main").expect("main");
    let host = native_host(&fixture, RecordingRunner::default());
    let create_checkpoint = |name: &str, revision: u64, pin: Pin| {
        let label = CheckpointLabel::new(name).expect("label");
        let image = layout
            .checkpoint_image(&main, &label)
            .expect("checkpoint")
            .image()
            .to_owned();
        create_image(&image);
        host.publish_checkpoint_fact(&image, &label, Revision::new(revision), pin)
            .expect("checkpoint fact");
        image
    };

    let mut newest_five = Vec::new();
    for index in 0..5 {
        let image = create_checkpoint(&format!("future-{index}"), index + 1, Pin::Automatic);
        make_future(&image);
        newest_five.push(image);
    }
    let young = create_checkpoint("young", 6, Pin::Automatic);
    let expired = create_checkpoint("expired", 7, Pin::Automatic);
    make_old(&expired);
    let pinned = create_checkpoint("pinned-old", 8, Pin::Pinned);
    make_old(&pinned);
    drop(host);

    let restarted = native_host(&fixture, RecordingRunner::default());
    let facts = restarted.checkpoints(&repo()).expect("restart facts");
    assert_eq!(facts.len(), 8);
    assert!(facts.iter().any(|fact| {
        fact.label.as_str() == "young"
            && fact.revision == Revision::new(6)
            && fact.pin == Pin::Automatic
    }));
    assert!(facts.iter().any(|fact| {
        fact.label.as_str() == "pinned-old"
            && fact.revision == Revision::new(8)
            && fact.pin == Pin::Pinned
    }));

    let report = execute_gc(&restarted, &config).expect("gc");
    assert_eq!(report.examined, 8);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.retained_pinned, 1);
    assert_eq!(report.retained_recent, 6);
    assert!(newest_five.iter().all(|image| image.exists()));
    assert!(
        young.exists(),
        "young checkpoint must survive beyond newest five"
    );
    assert!(pinned.exists(), "old pinned checkpoint must survive");
    assert!(
        !expired.exists(),
        "old automatic checkpoint beyond five is pruned"
    );
    assert!(!sidecar_path(&expired).exists(), "grants sidecar is pruned");
    assert!(
        !checkpoint_fact_path(&expired).exists(),
        "checkpoint fact sidecar is pruned"
    );

    let surviving = restarted.checkpoints(&repo()).expect("surviving facts");
    assert_eq!(surviving.len(), 7);
    assert_eq!(
        surviving
            .iter()
            .filter(|fact| fact.pin == Pin::Pinned)
            .count(),
        1
    );
}

#[test]
fn recovery_ignores_unscoped_and_non_internal_restore_lookalikes() {
    let fixture = Fixture::new("restore-lookalikes");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical");
    create_image(canonical.image());
    std::fs::write(canonical.image(), b"live generation").expect("canonical");

    for root in [
        fixture.root.join("caches"),
        fixture.root.join("mnt"),
        fixture.root.join("telemetry"),
    ] {
        std::fs::create_dir_all(&root).expect("lookalike root");
        std::fs::write(
            root.join("pre-restore-00000000000000000000000000000002.dmg.grants.json"),
            b"not metadata",
        )
        .expect("lookalike");
    }
    let invalid_checkpoints = [
        "pre-restore-user.dmg",
        "pre-restore-0000000000000000000000000000000.dmg",
        "pre-restore-gggggggggggggggggggggggggggggggg.dmg",
    ]
    .map(|name| layout.project().checkpoints.join("main").join(name));
    for checkpoint in &invalid_checkpoints {
        create_image(checkpoint);
        std::fs::write(checkpoint, b"user checkpoint").expect("checkpoint");
    }

    let session = WorkspaceName::new("session-a").expect("session");
    let session_canonical = layout.session_image(&session).expect("session canonical");
    create_image(session_canonical.image());
    std::fs::write(session_canonical.image(), b"live session").expect("session");
    let mut session_metadata = metadata();
    set_metadata_workspace(&mut session_metadata, session.clone());
    session_metadata
        .write_for_image(session_canonical.image())
        .expect("session metadata");
    let mismatched_undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.dmg");
    create_image(&mismatched_undo);
    std::fs::write(&mismatched_undo, b"unrelated undo").expect("undo");
    session_metadata
        .write_for_image(&mismatched_undo)
        .expect("mismatched undo metadata");

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("ignore lookalikes");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"live generation"
    );
    assert!(
        invalid_checkpoints
            .iter()
            .all(|checkpoint| checkpoint.exists())
    );
    assert_eq!(
        std::fs::read(session_canonical.image()).expect("session"),
        b"live session"
    );
    assert!(mismatched_undo.exists());
}

#[test]
fn recovery_rejects_a_non_directory_store_root_and_ignores_file_children() {
    let file_root = Fixture::new("recovery-file-root");
    let file_config = file_root.config();
    std::fs::remove_dir_all(&file_config.store_root).expect("remove store directory");
    std::fs::write(&file_config.store_root, b"not a directory").expect("file store root");
    assert!(
        native_host(&file_root, RecordingRunner::default())
            .recover_pending(&file_config, &[])
            .is_err(),
        "a non-directory store root is corruption, not an empty store"
    );

    let child_file = Fixture::new("recovery-file-child");
    let child_config = child_file.config();
    std::fs::create_dir_all(&child_config.store_root).expect("store root");
    std::fs::write(child_config.store_root.join("not-a-project"), b"file").expect("child file");
    native_host(&child_file, RecordingRunner::default())
        .recover_pending(&child_config, &[])
        .expect("non-directory children are not project roots");
}

#[test]
fn recovery_never_publishes_metadata_without_its_staged_image() {
    let fixture = Fixture::new("sidecar-without-image");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.dmg");
    std::fs::create_dir_all(staged.parent().expect("staging parent")).expect("staging");
    let mut next = metadata();
    next.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("incarnation");
    next.write_for_image(&staged)
        .expect("orphan staging metadata");

    native_host(&fixture, RecordingRunner::default())
        .recover_pending(&fixture.config(), &[])
        .expect("recovery");
    assert!(sidecar_path(&staged).exists());
    assert!(!sidecar_path(canonical.image()).exists());
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_recovery_rolls_forward_published_metadata_before_undo_rename() {
    let fixture = Fixture::new("published-before-undo");
    let (host, canonical, staged, undo, replacement) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterImageSwap);
    replacement
        .write_for_image(&canonical)
        .expect("simulate committed metadata");

    drop(host);
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    host.recover_pending(&fixture.config(), &[])
        .expect("roll forward");

    assert_eq!(
        std::fs::read(&canonical).expect("canonical"),
        b"new generation"
    );
    assert_eq!(std::fs::read(&undo).expect("undo"), b"old generation");
    assert!(!staged.exists());
    assert!(
        runner.requests().is_empty(),
        "roll-forward rename must preserve the retained undo without deleting an absent staging image"
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&canonical)
            .expect("canonical metadata")
            .workspace_incarnation,
        replacement.workspace_incarnation
    );
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_restore_recovery_rejects_published_new_image_with_old_ca_key() {
    let fixture = Fixture::new("published-new-image-old-ca");
    let (host, canonical, staged, undo, replacement) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterRestoreImageSwap);
    replacement
        .write_for_image(&canonical)
        .expect("simulate published replacement metadata");
    drop(host);

    let error = native_host(&fixture, RecordingRunner::default())
        .recover_pending(&fixture.config(), &[])
        .expect_err("old CA key must not authenticate replacement image");
    match error {
        ApfsStorageError::MarkerMismatch(message) => {
            assert!(message.contains(&canonical.display().to_string()));
            assert!(message.contains(&ca_key_path(&canonical).display().to_string()));
            assert!(message.contains(&ca_key_path(&undo).display().to_string()));
        }
        other => panic!("expected typed restore integrity error, got {other:?}"),
    }
    assert!(canonical.exists());
    assert!(staged.exists());
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_recovery_rolls_back_when_published_metadata_is_missing() {
    let fixture = Fixture::new("published-metadata-missing");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.dmg");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.dmg");
    let source_checkpoint = layout
        .project()
        .checkpoints
        .join("main/recovery-source.dmg");
    create_image(&source_checkpoint);
    write_ca_key(&source_checkpoint, b"source-ca-key");
    create_image(canonical.image());
    std::fs::write(canonical.image(), b"old generation").expect("old image");
    create_image(&staged);
    std::fs::write(&staged, b"new generation").expect("new image");
    let next_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("next incarnation");
    let mut next_metadata = metadata();
    next_metadata.workspace_incarnation = next_incarnation.clone();
    next_metadata
        .write_for_image(&staged)
        .expect("next metadata");
    let replacement = LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        next_incarnation,
        Revision::new(2),
        Revision::new(1),
        WorkspaceRole::Main,
    )
    .expect("replacement");
    let host = native_host(&fixture, RecordingRunner::default());
    host.restore_swap(&staged, canonical.image(), &undo)
        .expect("image publication");
    host.set_restore_failpoint(RestoreFailpoint::AfterMetadataPublish);
    host.publish_restored_metadata(
        &staged,
        canonical.image(),
        &replacement,
        replacement.revision(),
        &source_checkpoint,
        &metadata().workspace_incarnation,
    )
    .expect_err("metadata failpoint");
    std::fs::remove_file(sidecar_path(canonical.image())).expect("remove published metadata");

    drop(host);
    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("missing publication evidence rolls back");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"old generation"
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("restored metadata")
            .workspace_incarnation,
        metadata().workspace_incarnation
    );
}
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_recovery_accepts_an_already_rolled_back_canonical_layout() {
    let fixture = Fixture::new("already-rolled-back");
    let (host, canonical, staged, _, _) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterImageSwap);
    let temporary = canonical.with_extension("swap");
    std::fs::rename(&canonical, &temporary).expect("stage new canonical");
    std::fs::rename(&staged, &canonical).expect("restore old canonical");
    std::fs::rename(&temporary, &staged).expect("restage replacement");

    drop(host);
    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("recognize old canonical");

    assert_eq!(
        std::fs::read(&canonical).expect("canonical"),
        b"old generation"
    );
    assert!(!staged.exists());
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_recovery_restores_missing_prepublication_canonical_metadata() {
    let fixture = Fixture::new("missing-canonical-metadata");
    let (host, canonical, staged, undo, _) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterUndoRename);
    std::fs::remove_file(sidecar_path(&canonical)).expect("remove interrupted sidecar");

    drop(host);
    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("restore metadata from undo");

    assert_eq!(
        std::fs::read(&canonical).expect("canonical"),
        b"old generation"
    );
    assert!(!staged.exists());
    assert!(!undo.exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&canonical)
            .expect("canonical metadata")
            .workspace_incarnation,
        metadata().workspace_incarnation
    );
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_restored_publication_preserves_authoritative_workspace_topology() {
    let fixture = Fixture::new("authoritative-restored-publication");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    let destination =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("destination");
    let staged = layout
        .project()
        .project_root
        .join(format!(".staging/main-{destination}.dmg"));
    let undo = layout
        .project()
        .checkpoints
        .join(format!("main/pre-restore-{destination}.dmg"));
    let source = layout.project().checkpoints.join("main/baseline.dmg");
    create_image(canonical.image());
    create_image(&source);
    create_image(&staged);
    let mut staged_metadata = metadata();
    staged_metadata.workspace_incarnation = destination.clone();
    staged_metadata
        .write_for_image(&staged)
        .expect("destination metadata");
    let authoritative = LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        destination.clone(),
        Revision::new(2),
        Revision::new(47),
        WorkspaceRole::Main,
    )
    .expect("authoritative replacement");
    let replaced = metadata().workspace_incarnation;
    let host = native_host(&fixture, RecordingRunner::default());
    host.restore_swap(&staged, canonical.image(), &undo)
        .expect("restore swap");

    let fact = host
        .publish_restored_metadata(
            &staged,
            canonical.image(),
            &authoritative,
            authoritative.revision(),
            &source,
            &replaced,
        )
        .expect("publish restored metadata");

    assert_eq!(fact.workspace, authoritative);
    assert_eq!(fact.source_checkpoint, "baseline");
    assert_eq!(fact.source_incarnation, replaced);
    assert_eq!(fact.replaced_incarnation, replaced);
    assert_eq!(fact.destination_incarnation, destination);
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_stateless_restore_recovery_converges_each_publication_boundary() {
    for failpoint in [
        RestoreFailpoint::AfterUndoSidecar,
        RestoreFailpoint::AfterRestoreImageSwap,
        RestoreFailpoint::AfterImageSwap,
        RestoreFailpoint::AfterRestoreUndoImageRename,
        RestoreFailpoint::AfterUndoRename,
        RestoreFailpoint::AfterRestoreCanonicalParentFsync,
        RestoreFailpoint::AfterRestoreUndoParentFsync,
        RestoreFailpoint::AfterMetadataPublish,
        RestoreFailpoint::AfterMetadataFsync,
        RestoreFailpoint::AfterStagedMetadataRemoval,
        RestoreFailpoint::AfterRestoreMetadataParentFsync,
    ] {
        let fixture = Fixture::new(&format!("stateless-restore-{failpoint:?}"));
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        adopt_owner(&layout);
        let canonical = layout.main_image().expect("canonical");
        let staged = layout
            .project()
            .project_root
            .join(".staging/main-00000000000000000000000000000002.dmg");
        let undo = layout
            .project()
            .checkpoints
            .join("main/pre-restore-00000000000000000000000000000002.dmg");
        let source_checkpoint = layout
            .project()
            .checkpoints
            .join("main/recovery-source.dmg");
        create_image(&source_checkpoint);
        write_ca_key(&source_checkpoint, b"source-ca-key");
        create_image(canonical.image());
        std::fs::write(canonical.image(), b"old generation").expect("old image");
        write_ca_key(canonical.image(), b"old-ca-key");
        create_image(&staged);
        std::fs::write(&staged, b"new generation").expect("new image");
        write_ca_key(&staged, b"new-ca-key");
        let next_incarnation = WorkspaceIncarnation::new("00000000000000000000000000000002")
            .expect("next incarnation");
        let mut next_metadata = metadata();
        next_metadata.workspace_incarnation = next_incarnation.clone();
        next_metadata
            .write_for_image(&staged)
            .expect("next metadata");
        let replacement = LifecycleWorkspace::new(
            repo(),
            WorkspaceName::new("main").expect("main"),
            next_incarnation.clone(),
            Revision::new(2),
            Revision::new(1),
            WorkspaceRole::Main,
        )
        .expect("replacement");
        let old_credential_mount = fixture.root.join("old-credential-mount");
        let new_credential_mount = fixture.root.join("new-credential-mount");
        std::fs::create_dir_all(&old_credential_mount).expect("old credential mount");
        std::fs::create_dir_all(&new_credential_mount).expect("new credential mount");
        mint_credentials(
            &workspace(),
            &old_credential_mount,
            &ca_key_path(canonical.image()),
        )
        .expect("old credentials");
        mint_credentials(&replacement, &new_credential_mount, &ca_key_path(&staged))
            .expect("replacement credentials");
        let old_ca_key = std::fs::read(ca_key_path(canonical.image())).expect("old CA key");
        let new_ca_key = std::fs::read(ca_key_path(&staged)).expect("new CA key");
        let host = native_host(&fixture, RecordingRunner::default());
        host.set_restore_failpoint(failpoint);
        let image_phase = matches!(
            failpoint,
            RestoreFailpoint::AfterUndoSidecar
                | RestoreFailpoint::AfterRestoreImageSwap
                | RestoreFailpoint::AfterImageSwap
                | RestoreFailpoint::AfterRestoreUndoImageRename
                | RestoreFailpoint::AfterUndoRename
                | RestoreFailpoint::AfterRestoreCanonicalParentFsync
                | RestoreFailpoint::AfterRestoreUndoParentFsync
        );
        if image_phase {
            host.restore_swap(&staged, canonical.image(), &undo)
                .expect_err("injected image publication crash");
        } else {
            host.restore_swap(&staged, canonical.image(), &undo)
                .expect("image publication");
            host.publish_restored_metadata(
                &staged,
                canonical.image(),
                &replacement,
                replacement.revision(),
                &source_checkpoint,
                &metadata().workspace_incarnation,
            )
            .expect_err("injected metadata publication crash");
        }

        drop(host);
        let host = native_host(&fixture, RecordingRunner::default());
        host.recover_pending(&fixture.config(), &[])
            .expect("restart recovery");
        host.recover_pending(&fixture.config(), &[])
            .expect("repeated restart recovery is idempotent");
        let pending = host.pending_publications(&repo()).expect("pending facts");
        let metadata_was_published = !pending.is_empty();
        if metadata_was_published {
            assert_eq!(pending.len(), 1, "{failpoint:?}");
            assert_eq!(
                pending[0].source_incarnation,
                metadata().workspace_incarnation
            );
            assert_eq!(pending[0].source_checkpoint, "recovery-source");
            assert_eq!(pending[0].destination_incarnation, next_incarnation);
            host.activate_restored_metadata(&pending[0].image)
                .expect("idempotent forward activation");
            assert!(
                host.pending_publications(&repo())
                    .expect("activated pending facts")
                    .is_empty()
            );
        } else {
            assert!(pending.is_empty(), "{failpoint:?}");
        }
        assert!(
            !restore_recovery_fact_path(canonical.image()).exists(),
            "{failpoint:?}"
        );
        assert_eq!(
            std::fs::read(canonical.image()).expect("canonical bytes"),
            if metadata_was_published {
                b"new generation".as_slice()
            } else {
                b"old generation".as_slice()
            },
            "{failpoint:?}"
        );
        assert_eq!(
            std::fs::read(ca_key_path(canonical.image())).expect("canonical CA key"),
            if metadata_was_published {
                new_ca_key.as_slice()
            } else {
                old_ca_key.as_slice()
            },
            "{failpoint:?}"
        );
        if metadata_was_published {
            assert_eq!(std::fs::read(&undo).expect("undo image"), b"old generation");
            assert_eq!(
                std::fs::read(ca_key_path(&undo)).expect("undo CA key"),
                old_ca_key
            );
        } else {
            assert!(!undo.exists(), "{failpoint:?}");
            assert!(!ca_key_path(&undo).exists(), "{failpoint:?}");
        }
        assert!(!staged.exists(), "{failpoint:?}");
        assert!(!sidecar_path(&staged).exists(), "{failpoint:?}");
        assert!(!ca_key_path(&staged).exists(), "{failpoint:?}");
        let canonical_metadata =
            DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("metadata");
        assert_eq!(
            canonical_metadata.workspace_incarnation,
            if metadata_was_published {
                next_incarnation
            } else {
                metadata().workspace_incarnation
            },
            "{failpoint:?}"
        );
    }
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_restore_recovery_selects_current_undo_generation_among_older_history() {
    let fixture = Fixture::new("restore-current-undo-generation");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical");
    let first_destination =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("first destination");
    let second_destination =
        WorkspaceIncarnation::new("00000000000000000000000000000003").expect("second destination");
    let original_incarnation = metadata().workspace_incarnation;
    let checkpoint_directory = layout.project().checkpoints.join("main");
    let source_checkpoint = checkpoint_directory.join("second-restore.dmg");
    create_image(&source_checkpoint);
    write_ca_key(&source_checkpoint, b"checkpoint-source-ca-key");
    let older_undo = checkpoint_directory.join(format!("pre-restore-{first_destination}.dmg"));
    let current_undo = checkpoint_directory.join(format!("pre-restore-{second_destination}.dmg"));

    create_image(&current_undo);
    std::fs::write(&current_undo, b"first restored generation").expect("current undo image");
    let mut current_undo_metadata = metadata();
    current_undo_metadata.workspace_incarnation = first_destination.clone();
    current_undo_metadata
        .write_for_image(&current_undo)
        .expect("current undo metadata");

    create_image(&older_undo);
    std::fs::write(&older_undo, b"original generation").expect("older undo image");
    assert!(
        older_undo.file_name() < current_undo.file_name(),
        "sorted recovery enumeration must encounter the older restore first"
    );

    create_image(canonical.image());
    std::fs::write(canonical.image(), b"second restored generation").expect("canonical image");
    let mut pending_metadata = metadata();
    pending_metadata.workspace_incarnation = second_destination.clone();
    pending_metadata.publication_state = PublicationState::PendingFence;
    pending_metadata
        .write_for_image(canonical.image())
        .expect("pending canonical metadata");
    let recovery_fact = serde_json::json!({
        "version": 2,
        "repoId": repo(),
        "workspace": "main",
        "sourceCheckpoint": "second-restore",
        "sourceIncarnation": original_incarnation.clone(),
        "replacedIncarnation": first_destination.clone(),
        "destinationIncarnation": second_destination.clone(),
    });
    std::fs::write(
        restore_recovery_fact_path(canonical.image()),
        serde_json::to_vec(&recovery_fact).expect("restore recovery fact JSON"),
    )
    .expect("restore recovery fact");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(restore_recovery_fact_path(canonical.image()))
                .expect("read restore recovery fact"),
        )
        .expect("decode restore recovery fact"),
        recovery_fact,
        "restore recovery fact has an exact v2 schema"
    );

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("first startup recovery");
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated startup recovery");

    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical bytes"),
        b"second restored generation"
    );
    assert_eq!(
        std::fs::read(&current_undo).expect("current undo bytes"),
        b"first restored generation"
    );
    assert_eq!(
        std::fs::read(&older_undo).expect("older undo bytes"),
        b"original generation"
    );
    assert!(sidecar_path(&older_undo).exists());
    assert!(ca_key_path(&older_undo).exists());
    let pending = host.pending_publications(&repo()).expect("pending restore");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].source_incarnation, original_incarnation);
    assert_eq!(
        pending[0].replaced_incarnation,
        current_undo_metadata.workspace_incarnation
    );
    assert_eq!(pending[0].destination_incarnation, second_destination);
    assert_eq!(pending[0].source_checkpoint, "second-restore");
    let invalid_facts = [
        (
            "source checkpoint",
            serde_json::json!({
                "version": 2,
                "repoId": repo(),
                "workspace": "main",
                "sourceCheckpoint": "missing-source",
                "sourceIncarnation": original_incarnation.clone(),
                "replacedIncarnation": first_destination.clone(),
                "destinationIncarnation": second_destination.clone(),
            }),
        ),
        (
            "source incarnation",
            serde_json::json!({
                "version": 2,
                "repoId": repo(),
                "workspace": "main",
                "sourceCheckpoint": "second-restore",
                "sourceIncarnation": first_destination.clone(),
                "replacedIncarnation": first_destination.clone(),
                "destinationIncarnation": second_destination.clone(),
            }),
        ),
        (
            "replaced incarnation",
            serde_json::json!({
                "version": 2,
                "repoId": repo(),
                "workspace": "main",
                "sourceCheckpoint": "second-restore",
                "sourceIncarnation": original_incarnation.clone(),
                "replacedIncarnation": original_incarnation.clone(),
                "destinationIncarnation": second_destination.clone(),
            }),
        ),
        (
            "destination incarnation",
            serde_json::json!({
                "version": 2,
                "repoId": repo(),
                "workspace": "main",
                "sourceCheckpoint": "second-restore",
                "sourceIncarnation": original_incarnation.clone(),
                "replacedIncarnation": first_destination.clone(),
                "destinationIncarnation": first_destination.clone(),
            }),
        ),
        (
            "unknown field",
            serde_json::json!({
                "version": 2,
                "repoId": repo(),
                "workspace": "main",
                "sourceCheckpoint": "second-restore",
                "sourceIncarnation": original_incarnation.clone(),
                "replacedIncarnation": first_destination.clone(),
                "destinationIncarnation": second_destination.clone(),
                "runtimeJournal": true,
            }),
        ),
    ];
    for (field, invalid_fact) in invalid_facts {
        std::fs::write(
            restore_recovery_fact_path(canonical.image()),
            serde_json::to_vec(&invalid_fact).expect("invalid fact JSON"),
        )
        .expect("invalid restore fact");
        assert!(
            host.pending_publications(&repo()).is_err(),
            "forged or unknown {field} must be rejected"
        );
    }
    std::fs::write(
        restore_recovery_fact_path(canonical.image()),
        serde_json::to_vec(&recovery_fact).expect("valid fact JSON"),
    )
    .expect("restore valid recovery fact");

    host.activate_restored_metadata(canonical.image())
        .expect("activate recovered restore");
    std::fs::write(
        restore_recovery_fact_path(canonical.image()),
        serde_json::to_vec(&recovery_fact).expect("lingering fact JSON"),
    )
    .expect("simulate crash before activation fact cleanup");
    host.activate_restored_metadata(canonical.image())
        .expect("clean lingering activation fact");
    host.activate_restored_metadata(canonical.image())
        .expect("repeated activation is idempotent");
    host.recover_pending(&fixture.config(), &[])
        .expect("post-activation recovery");
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated post-activation recovery");
    assert!(
        host.pending_publications(&repo())
            .expect("active facts")
            .is_empty()
    );
    assert!(!restore_recovery_fact_path(canonical.image()).exists());
    assert!(
        older_undo.exists(),
        "older undo remains a retention candidate"
    );
    assert!(
        current_undo.exists(),
        "current undo remains a retention candidate"
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("activated metadata")
            .workspace_incarnation,
        WorkspaceIncarnation::new("00000000000000000000000000000003").expect("second destination")
    );
    pending_metadata
        .write_for_image(canonical.image())
        .expect("restore pending metadata for forged fact");
    let forged_fact = serde_json::json!({
        "version": 2,
        "repoId": repo(),
        "workspace": "main",
        "sourceCheckpoint": "second-restore",
        "sourceIncarnation": original_incarnation.clone(),
        "replacedIncarnation": original_incarnation.clone(),
        "destinationIncarnation": second_destination,
    });
    std::fs::write(
        restore_recovery_fact_path(canonical.image()),
        serde_json::to_vec(&forged_fact).expect("forged fact JSON"),
    )
    .expect("forged restore fact");
    let error = host
        .recover_pending(&fixture.config(), &[])
        .expect_err("forged replaced incarnation must be rejected");
    assert!(matches!(error, ApfsStorageError::MarkerMismatch(_)));
    let legacy_v1_fact = serde_json::json!({
        "version": 1,
        "repoId": repo(),
        "workspace": "main",
        "sourceCheckpoint": "second-restore",
        "sourceIncarnation": original_incarnation.clone(),
        "destinationIncarnation": WorkspaceIncarnation::new(
            "00000000000000000000000000000003"
        )
        .expect("second destination"),
    });
    std::fs::write(
        restore_recovery_fact_path(canonical.image()),
        serde_json::to_vec(&legacy_v1_fact).expect("legacy fact JSON"),
    )
    .expect("legacy restore fact");
    assert!(
        host.pending_publications(&repo()).is_err(),
        "v1 facts without replacedIncarnation require clean restore recreation"
    );
    assert_ne!(
        original_incarnation,
        current_undo_metadata.workspace_incarnation
    );
}

#[test]
fn retired_workspace_restart_ignores_multiple_retained_restore_generations() {
    let fixture = Fixture::new("retired-repeated-restore-history");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let workspace_name = WorkspaceName::session("raven").expect("workspace");
    let canonical = layout.session_image(&workspace_name).expect("canonical");
    let original = WorkspaceIncarnation::new("00000000000000000000000000000001").expect("original");
    let first_destination =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("first destination");
    let second_destination =
        WorkspaceIncarnation::new("00000000000000000000000000000003").expect("second destination");
    let checkpoint_directory = layout.project().checkpoints.join("raven");
    let first_undo = checkpoint_directory.join(format!("pre-restore-{first_destination}.dmg"));
    let second_undo = checkpoint_directory.join(format!("pre-restore-{second_destination}.dmg"));
    let write_generation = |image: &Path, incarnation: &WorkspaceIncarnation| {
        create_image(image);
        let mut detached = metadata();
        set_metadata_workspace(&mut detached, workspace_name.clone());
        detached.workspace_incarnation = incarnation.clone();
        detached
            .write_for_image(image)
            .expect("generation metadata");
    };
    write_generation(&first_undo, &original);
    write_generation(&second_undo, &first_destination);
    write_generation(canonical.image(), &second_destination);
    let trash = layout
        .project()
        .sessions
        .join(".trash")
        .join(format!("raven-{second_destination}.dmg"));
    std::fs::create_dir_all(trash.parent().expect("trash parent")).expect("trash directory");
    let host = native_host(&fixture, RecordingRunner::default());
    host.retire_image(canonical.image(), &trash)
        .expect("retire restored workspace");
    drop(host);

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("retained undo history is not pending recovery");
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated restart remains converged");

    assert!(!canonical.image().exists());
    assert!(!restore_recovery_fact_path(canonical.image()).exists());
    for retained in [&first_undo, &second_undo] {
        assert!(retained.exists());
        assert!(sidecar_path(retained).exists());
        assert!(ca_key_path(retained).exists());
    }
    assert!(trash.exists());
    assert!(sidecar_path(&trash).exists());
    assert!(ca_key_path(&trash).exists());
}

#[test]
fn restore_recovery_rejects_fact_without_canonical_image() {
    let fixture = Fixture::new("restore-fact-without-canonical");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical");
    let destination =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("destination");
    let undo = layout
        .project()
        .checkpoints
        .join(format!("main/pre-restore-{destination}.dmg"));
    create_image(&undo);
    let fact_path = restore_recovery_fact_path(canonical.image());
    std::fs::write(
        &fact_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "repoId": repo(),
            "workspace": "main",
            "sourceCheckpoint": "baseline",
            "sourceIncarnation": metadata().workspace_incarnation,
            "replacedIncarnation": metadata().workspace_incarnation,
            "destinationIncarnation": destination,
        }))
        .expect("fact JSON"),
    )
    .expect("orphan fact");

    let error = native_host(&fixture, RecordingRunner::default())
        .recover_pending(&fixture.config(), &[])
        .expect_err("fact without canonical must fail closed");
    let ApfsStorageError::MarkerMismatch(message) = error else {
        panic!("expected restore integrity error, got {error:?}");
    };
    assert!(message.contains(&canonical.image().display().to_string()));
    assert!(message.contains(&fact_path.display().to_string()));
    assert!(undo.exists());
}

#[test]
fn system_mount_source_observes_the_live_root_mount() {
    let mounts = SystemKernelMountSource.mounts().expect("getfsstat");
    assert!(!mounts.is_empty());
    let root = mounts
        .iter()
        .find(|mount| mount.mount_point == Path::new("/"))
        .expect("root mount");
    assert!(root.source_device.starts_with("/dev/disk"));
    assert!(mounts.iter().all(|mount| mount.mount_id != 0));
}

/// One real volume remounted under every combination of the two flags the check reads, each read
/// back from the kernel: browsing is the user's choice, honoring ownership is not. The ownership-free
/// combination is the wrong-flags mount the host heals by its mountpoint, so the heal is proven here
/// on this same volume and marker, once the verification attachment has released its raw pin.
#[test]
fn real_apfs_kernel_mount_flag_truth_table_allows_browse_but_requires_owners() {
    // The pool's one scenario image, attached once for the whole lane run: this test only
    // mounts its already-attached volume under each flag combination and reads the kernel.
    let pool = blank_image::pool_root();
    let layout = StorageLayout::new(&pool, &repo()).expect("pool layout");
    let canonical = layout.main_image().expect("canonical");
    let namespace = TestNamespace::mint().expect("a test namespace");
    let shared = blank_image::shared_image_at(namespace, "scenario-main", canonical.image());
    metadata()
        .write_for_image(canonical.image())
        .expect("sidecar");
    let signing_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("fixture P-256 private key");
    write_ca_key(canonical.image(), signing_key.serialize_pem().as_bytes());
    let mount = pool.join("mount");
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner::new(HostNamespace::Test(namespace)),
        ApfsSubstrateConfig::new(HostNamespace::Test(namespace), &pool, &mount),
    )
    .expect("native APFS host");
    let mounted = shared.mount_at(&mount, "nobrowse,owners");
    host.write_marker(&mount, &workspace(), None, &identity_at(&pool))
        .expect("mount marker");
    drop(mounted);
    for (options, expected_valid) in [
        ("nobrowse,owners", true),
        ("nobrowse,noowners", false),
        ("owners", true),
    ] {
        let mounted = shared.mount_at(&mount, options);
        let joins = host
            .mounts(&repo())
            .expect("flags are a per-workspace finding");
        assert_eq!(
            matches!(joins.as_slice(), [MountJoin::Proven(_)]),
            expected_valid,
            "{options}: {joins:?}"
        );
        drop(mounted);
    }

    let _mounted = shared.mount_at(&mount, "noowners");
    let joins = host
        .mounts(&repo())
        .expect("flags are a per-workspace finding");
    let [MountJoin::Refused { refusal, .. }] = joins.as_slice() else {
        panic!("wrong flags refuse main's join: {joins:?}");
    };
    assert!(matches!(
        refusal.reason,
        MountRefusalReason::NonCanonicalFlags(_)
    ));
    let wrong_flags = refusal.to_string();
    assert!(
        wrong_flags.contains("non-canonical flags"),
        "noowners: {wrong_flags}"
    );
    host.heal_mount(&workspace(), &mount)
        .expect("heal by mountpoint");
    assert!(
        kernel_mount_at(&mount).is_none(),
        "healing a wrongly flagged mount detaches it for a canonical remount"
    );
    assert!(!attached(canonical.image()));
    assert!(host.mounts(&repo()).expect("healed facts").is_empty());
}

/// fseventsd's persistent event log on real volumes (01a_apfs.md, "The event log"). A fresh
/// volume is opted out at its first read-write mount. A volume carrying a log a root fseventsd
/// created — what a browsable mount leaves, and what every clone of such a volume inherits — has
/// it replaced by the marker at its next mount, through one mount that ignores ownership; the
/// mount after that runs without a log and retires nothing. fseventsd can carry the old log
/// across the immediate remount (measured: three runs in four), so only the marker is asserted
/// there. Every mount, the retiring one included, ends honoring ownership and without access
/// times. Live delivery is not asserted: on this host fseventsd loses a just-mounted volume's
/// events whether it is logged or not (01a_apfs.md, "The event log", records the measurement).
#[test]
fn real_apfs_mount_opts_volumes_out_of_the_event_log_and_retires_an_inherited_one() {
    let fixture = RealFixture::new("event-log");
    let host = fixture.host();
    let backend = host.backend();
    let assert_opted_out = |mount: &Path, label: &str| {
        let marker = std::fs::symlink_metadata(mount.join(".fseventsd/no_log"))
            .unwrap_or_else(|error| panic!("{label}: the opt-out marker: {error}"));
        assert!(
            marker.file_type().is_file() && marker.len() == 0,
            "{label}: the opt-out marker is an empty regular file: {marker:?}"
        );
    };
    let assert_canonical_flags = |mount: &Path, label: &str| {
        let path = std::ffi::CString::new(mount.as_os_str().as_encoded_bytes()).expect("C path");
        // SAFETY: `path` is NUL-terminated and `statfs` writes only the zeroed buffer.
        let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::statfs(path.as_ptr(), &mut stat) },
            0,
            "statfs"
        );
        assert_eq!(
            stat.f_flags & libc::MNT_IGNORE_OWNERSHIP as u32,
            0,
            "{label}: honors ownership"
        );
        assert_ne!(
            stat.f_flags & libc::MNT_NOATIME as u32,
            0,
            "{label}: no access times"
        );
    };
    use fsevents::Transition::{Mounted, Unmounted};
    let mut mounts = fsevents::Watch::start(fixture.root());

    let fresh_image = fixture.root().join("fresh.dmg");
    let fresh = fixture.root().join("fresh");
    fixture.blank_image(&fresh_image);
    let attachment = backend.attach_verified(&fresh_image).expect("attach fresh");
    backend
        .mount(&attachment, &fresh, MountAccess::ReadWrite, false)
        .expect("mount fresh");
    mounts.wait_for(&fresh, Mounted);
    assert_canonical_flags(&fresh, "fresh");
    assert_opted_out(&fresh, "fresh");
    assert!(!fsevents::keeps_event_log(&fresh));

    let logged_image = fixture.root().join("logged.dmg");
    let logged = fixture.root().join("logged");
    fixture.blank_image(&logged_image);
    let attachment = fixture.mount_left_behind(&logged_image, &logged, "owners");
    mounts.wait_for(&logged, Mounted);
    // fseventsd opens a browsable volume's log once a stream asks about the volume.
    drop(fsevents::Watch::start(&logged));
    assert!(
        fsevents::keeps_event_log(&logged),
        "a browsable mount is logged"
    );
    assert_eq!(
        std::fs::symlink_metadata(logged.join(".fseventsd/no_log"))
            .expect_err("root's log")
            .kind(),
        std::io::ErrorKind::PermissionDenied,
        "the log is root's"
    );
    unmount(&logged);
    mounts.wait_for(&logged, Unmounted);

    // The retiring mount mounts twice more — once ignoring ownership, once as asked — and the
    // retired one once.
    for mount in ["retiring", "retired"] {
        backend
            .mount(&attachment, &logged, MountAccess::ReadWrite, false)
            .unwrap_or_else(|error| panic!("{mount} mount: {error}"));
        mounts.wait_for(&logged, Mounted);
        assert_canonical_flags(&logged, mount);
        // Readable under ownership: the next mount finds the volume opted out, with nothing to
        // retire.
        assert_opted_out(&logged, mount);
        let kept = std::fs::symlink_metadata(logged.join(".fseventsd")).expect("event log");
        assert_eq!(
            (kept.uid(), kept.mode() & 0o777),
            (0, 0o711),
            "{mount}: root's log is kept, its records unlisted"
        );
        if mount == "retired" {
            assert!(!fsevents::keeps_event_log(&logged), "{mount}: no log");
        }
        unmount(&logged);
        mounts.wait_for(&logged, Unmounted);
    }
}

/// Real root-owned event-log debris, without sudo or an unmount-write race: move the kernel's
/// log within one image while ownership is ignored, then honor ownership again. Its containing
/// directory is an ordinary bare mountpoint on that filesystem, not a mounted volume.
#[test]
fn real_apfs_gc_retains_root_held_event_debris_without_blocking_retirement() {
    use fsevents::Transition::{Mounted, Unmounted};

    const INCARNATION: &str = "00000000000000000000000000000003";
    let fixture = RealFixture::new("root-held-event-debris");
    let backing_image = fixture.root().join("bare-host.dmg");
    let backing = fixture.root().join("mnt");
    fixture.blank_image(&backing_image);
    let mut mounts = fsevents::Watch::start(fixture.root());
    let attachment = fixture.mount_left_behind(&backing_image, &backing, "owners");
    mounts.wait_for(&backing, Mounted);
    drop(fsevents::Watch::start(&backing));
    assert!(
        fsevents::keeps_event_log(&backing),
        "the kernel created its log"
    );
    unmount(&backing);
    mounts.wait_for(&backing, Unmounted);
    fixture.remount(attachment.volume_device(), &backing, "nobrowse,noowners");
    mounts.wait_for(&backing, Mounted);

    let config = fixture.config();
    let layout = fixture.layout();
    let name = WorkspaceName::session("retired").expect("workspace");
    let bare = layout.workspace_mount(&name).expect("bare mountpoint");
    std::fs::create_dir_all(&bare).expect("user-owned bare mountpoint");
    std::fs::rename(backing.join(".fseventsd"), bare.join(".fseventsd"))
        .expect("retain the kernel's ownership within the same filesystem");
    // No logger flush may leave privileged debris on the real host during fixture teardown.
    std::fs::create_dir(backing.join(".fseventsd")).expect("user event-log directory");
    std::fs::write(backing.join(".fseventsd/no_log"), b"").expect("opt fixture volume out");
    unmount(&backing);
    mounts.wait_for(&backing, Unmounted);
    fixture.remount(attachment.volume_device(), &backing, "nobrowse,owners");
    mounts.wait_for(&backing, Mounted);
    assert!(!fsevents::keeps_event_log(&backing));
    let log = std::fs::symlink_metadata(bare.join(".fseventsd")).expect("root-owned log");
    assert_eq!((log.uid(), log.mode() & 0o777), (0, 0o700));
    assert_eq!(
        std::fs::read_dir(bare.join(".fseventsd"))
            .expect_err("the log is unreadable without root")
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert!(
        kernel_mount_at(&bare).is_none(),
        "only its parent filesystem is mounted"
    );

    let canonical = layout.session_image(&name).expect("retired image");
    fixture.published_image(canonical.image());
    write_session_metadata(canonical.image(), "retired", INCARNATION);
    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/retired-{INCARNATION}.dmg"));
    let host = fixture.host();
    host.retire_image(canonical.image(), &trash)
        .expect("retire image");
    std::fs::remove_file(&trash)
        .expect("physical release completed before scoped metadata cleanup");
    for other in [".late-data", "work.rs"] {
        let work = bare.join(other);
        std::fs::write(&work, b"unique late work").expect("late work");
        let error = host
            .preview_gc(&config, &repo(), StorageGcOptions::default())
            .expect_err("mixed entries are not root-held-only");
        assert!(
            error.to_string().contains("mixed with other entries"),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&work).expect("preserved work"),
            b"unique late work"
        );
        assert!(bare.exists(), "mixed debris is never renamed");
        std::fs::remove_file(work).expect("remove this test's late work");
    }
    let foreign_image = fixture.root().join("foreign.dmg");
    fixture.blank_image(&foreign_image);
    let foreign = host
        .backend()
        .attach_verified(&foreign_image)
        .expect("foreign volume");
    host.backend()
        .mount(&foreign, &bare, MountAccess::ReadWrite, false)
        .expect("mount foreign volume");
    mounts.wait_for(&bare, Mounted);
    let error = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect_err("a mounted filesystem is never retained aside");
    assert!(error.to_string().contains("mounted filesystem"), "{error}");
    assert!(
        kernel_mount_at(&bare).is_some(),
        "the mounted volume was not detached or renamed"
    );
    unmount(&bare);
    mounts.wait_for(&bare, Unmounted);
    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("root-held event debris must not block a read-only GC preview");
    assert!(bare.exists(), "preview must not rename the bare mountpoint");
    assert_eq!(
        plan.root_held_mountpoints(),
        &[cowshed_core::storage::lifecycle::RootHeldMountpoint::Pending(bare.clone())]
    );
    host.backend()
        .mount(&foreign, &bare, MountAccess::ReadWrite, false)
        .expect("mount after preview");
    mounts.wait_for(&bare, Mounted);
    let error = host
        .execute_gc(&config, plan)
        .expect_err("a mount after preview invalidates collection");
    assert!(error.to_string().contains("mounted filesystem"), "{error}");
    assert!(
        sidecar_path(&trash).exists(),
        "retirement authority remains after the mount changed"
    );
    assert!(
        kernel_mount_at(&bare).is_some(),
        "no mounted filesystem is renamed"
    );
    unmount(&bare);
    mounts.wait_for(&bare, Unmounted);
    let plan = host
        .preview_gc(&config, &repo(), StorageGcOptions::default())
        .expect("fresh unmounted plan");
    let report = host
        .execute_gc(&config, plan)
        .expect("collect retired image");
    assert_eq!(report.reclaimed, 1);
    assert!(report.deferred.is_empty(), "{report:?}");
    assert!(
        !sidecar_path(&trash).exists(),
        "completed physical release no longer needs its retirement record"
    );
    assert!(
        !bare.exists(),
        "the name is free after its mountpoint is renamed aside"
    );
    let [cowshed_core::storage::lifecycle::RootHeldMountpoint::Retained(retained)] =
        report.root_held_mountpoints.as_slice()
    else {
        panic!("one retained event-log mountpoint: {report:?}");
    };
    assert_eq!(
        retained.parent(),
        bare.parent(),
        "the rename is within one held parent"
    );
    assert!(
        retained
            .file_name()
            .unwrap()
            .as_encoded_bytes()
            .starts_with(b".root-held-")
    );
    let retained_log =
        std::fs::symlink_metadata(retained.join(".fseventsd")).expect("retained root log");
    assert_eq!(
        (retained_log.uid(), retained_log.mode() & 0o777),
        (0, 0o700)
    );
    assert_eq!(
        retained_log.ino(),
        log.ino(),
        "the log was renamed with its parent, never copied"
    );
    let late = retained.join("later-work.rs");
    std::fs::write(&late, b"preserve work added after retention")
        .expect("late retained-parent writer");
    let again = execute_gc(&host, &config).expect("idempotent retained-debris collection");
    assert_eq!(again.reclaimed, 0);
    assert!(again.deferred.is_empty(), "{again:?}");
    assert_eq!(again.root_held_mountpoints, report.root_held_mountpoints);
    assert_eq!(
        std::fs::read(late).expect("retained late work"),
        b"preserve work added after retention"
    );
}

/// A retired workspace's TMPDIR is its sandbox's scratch, and a test run inside it may mount a
/// volume there and die holding it. Reclamation never reaches through that mount: removing the
/// tree would empty the volume — its in-image marker included — and then stop at the busy
/// mountpoint anyway. The mount is a path a sandboxed child chose, so it is named and never
/// detached, and the retirement stays until a pass finds nothing mounted beneath it.
#[test]
fn real_apfs_retired_temp_dir_is_never_reclaimed_through_a_mounted_volume() {
    const INCARNATION: &str = "00000000000000000000000000000005";
    let fixture = RealFixture::new("retired-temp-mount");
    let layout = fixture.layout();
    let name = WorkspaceName::session("retired").expect("workspace");
    let canonical = layout.session_image(&name).expect("retired image");
    fixture.published_image(canonical.image());
    write_session_metadata(canonical.image(), "retired", INCARNATION);
    let trash = layout
        .project()
        .sessions
        .join(format!(".trash/retired-{INCARNATION}.dmg"));
    let host = fixture.host();
    host.retire_image(canonical.image(), &trash)
        .expect("retire image");

    std::fs::remove_file(&trash)
        .expect("physical release completed before scoped metadata cleanup");
    let exec_temp = layout.exec_temp_dir(&name).expect("temp dir");
    std::fs::create_dir_all(&exec_temp).expect("temp dir");
    std::fs::write(exec_temp.join("scratch"), b"own scratch").expect("own scratch");
    let held = exec_temp.join("smoke/project");
    let foreign_image = fixture.root().join("foreign.dmg");
    fixture.blank_image(&foreign_image);
    fixture.mount_left_behind(&foreign_image, &held, "nobrowse,owners");
    std::fs::write(held.join("data"), b"another volume's data").expect("foreign data");

    let report = execute_gc(&host, &fixture.config()).expect("gc defers what it cannot reclaim");
    assert_eq!(report.reclaimed, 0);
    let [deferred] = report.deferred.as_slice() else {
        panic!("the retirement is deferred, naming the mount: {report:?}");
    };
    assert_eq!(deferred.path, trash);
    assert!(
        deferred.diagnostic.contains("carries a mounted filesystem")
            && deferred
                .diagnostic
                .contains(held.to_str().expect("UTF-8 mount path")),
        "{}",
        deferred.diagnostic
    );
    assert_eq!(
        std::fs::read(held.join("data")).expect("the mounted volume keeps its files"),
        b"another volume's data"
    );
    assert!(
        kernel_mount_at(&held).is_some(),
        "the mount is never detached"
    );
    assert!(
        sidecar_path(&trash).exists(),
        "the retirement still authorises the next pass"
    );

    unmount(&held);
    let report = execute_gc(&host, &fixture.config())
        .expect("reclaim once nothing is mounted beneath the temp dir");
    assert_eq!(report.reclaimed, 1);
    assert!(!trash.exists(), "the retired image is reclaimed");
    assert!(!exec_temp.exists(), "the temp dir is reclaimed with it");
}

#[test]
fn flock_child_helper() {
    use std::io::Write as _;
    let Ok(root) = std::env::var("COWSHED_FLOCK_HELPER_ROOT") else {
        return;
    };
    let lock =
        PathBuf::from(std::env::var_os("COWSHED_FLOCK_HELPER_LOCK").expect("helper lock path"));
    // Its own namespace suffices: this helper only takes image locks, which derive no names.
    let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
    let host = native_host_at(namespace, Path::new(&root));
    let _guard = host
        .lock_images(&[lock], LockMode::Wait, &other_operation())
        .expect("helper lock")
        .expect("blocking helper lock");
    // On stderr, which libtest neither captures nor frames: the lock is held from here.
    std::io::stderr()
        .write_all(b"held\n")
        .expect("say the lock is held");
    // Held in the parent's custody: its stdin closing -- the parent letting go, or the
    // parent's own death closing the pipe -- ends this helper and releases the lock.
    std::io::copy(&mut std::io::stdin(), &mut std::io::sink()).expect("wait for the release");
    if std::env::var_os("COWSHED_FLOCK_HELPER_CRASH").is_some() {
        std::process::abort();
    }
}

#[test]
fn independent_hosts_and_processes_serialize_and_crash_releases_the_lock() {
    let fixture = Fixture::new("flock-process");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let lock = layout.main_image().expect("main").lock().to_owned();
    let first = native_host_at(fixture.host_namespace(), &fixture.root);
    let second = native_host_at(fixture.host_namespace(), &fixture.root);
    let guard = first
        .lock_images(
            std::slice::from_ref(&lock),
            LockMode::Wait,
            &other_operation(),
        )
        .expect("first lock")
        .expect("blocking first lock");
    assert!(
        second
            .lock_images(
                std::slice::from_ref(&lock),
                LockMode::Try,
                &other_operation()
            )
            .expect("second try lock")
            .is_none(),
        "independent hosts must contend through the kernel"
    );
    drop(guard);
    drop(
        second
            .lock_images(
                std::slice::from_ref(&lock),
                LockMode::Try,
                &other_operation(),
            )
            .expect("second lock after release")
            .expect("released lock must be available"),
    );

    for crash in [false, true] {
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .arg("--exact")
            .arg("macos::flock_child_helper")
            .arg("--nocapture")
            .env("COWSHED_FLOCK_HELPER_ROOT", &fixture.root)
            .env("COWSHED_FLOCK_HELPER_LOCK", &lock)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if crash {
            command.env("COWSHED_FLOCK_HELPER_CRASH", "1");
        }
        let mut child = command.spawn_locked().expect("spawn lock helper");
        let mut said = std::io::BufReader::new(child.stderr.take().expect("the helper's stderr"));
        {
            use std::io::BufRead as _;
            let mut transcript = String::new();
            loop {
                let start = transcript.len();
                let read = said
                    .read_line(&mut transcript)
                    .expect("the helper's stderr");
                assert_ne!(
                    read,
                    0,
                    "the lock helper ended before it held {}; it said: {transcript}",
                    lock.display()
                );
                if transcript[start..].trim_end() == "held" {
                    break;
                }
            }
        }
        assert!(
            first
                .lock_images(
                    std::slice::from_ref(&lock),
                    LockMode::Try,
                    &other_operation()
                )
                .expect("parent try while child owns")
                .is_none(),
            "another process must own the lock"
        );
        drop(child.stdin.take());
        std::io::copy(&mut said, &mut std::io::sink()).expect("the helper's last output");
        let status = child.wait_bounded().expect("wait for helper");
        assert_eq!(status.success(), !crash);
        drop(
            first
                .lock_images(
                    std::slice::from_ref(&lock),
                    LockMode::Try,
                    &other_operation(),
                )
                .expect("lock after child exit")
                .expect("process exit must release flock"),
        );
    }
}

#[test]
fn gc_skips_staging_owned_by_an_active_lifecycle_lock() {
    let fixture = Fixture::new("gc-active-staging");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.dmg");
    std::fs::create_dir_all(staged.parent().expect("staging directory"))
        .expect("staging directory");
    std::fs::write(&staged, b"sidecarless staged orphan").expect("staged orphan");
    let owner = native_host_at(fixture.host_namespace(), &fixture.root);
    let collector = native_host_at(fixture.host_namespace(), &fixture.root);
    let guard = owner
        .lock_images(
            &[canonical.lock().to_owned()],
            LockMode::Wait,
            &other_operation(),
        )
        .expect("owner lock")
        .expect("blocking owner lock");

    // Owned staging is retained and reported, not a stale plan: the sweep reads the held lock
    // as the operation's ownership rather than tripping over it at execution.
    let report = execute_gc(&collector, &fixture.config()).expect("owned staging is retained");
    assert_eq!(report.examined, 1);
    assert_eq!(report.reclaimed, 0);
    assert_eq!(report.retained_active, 1);
    assert!(staged.exists());
    assert!(!sidecar_path(&staged).exists());

    drop(guard);
    let report = execute_gc(&collector, &fixture.config()).expect("released gc");
    assert_eq!(report.examined, 1);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.retained_active, 0);
    assert!(!staged.exists());
    assert!(!sidecar_path(&staged).exists());
}

#[test]
fn lock_and_command_targets_reject_intermediate_symlink_ancestors_without_effects() {
    let fixture = Fixture::new("symlink-ancestor");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image().expect("canonical");
    let attacker = fixture.root.join("attacker");
    std::fs::create_dir(&attacker).expect("attacker directory");
    std::os::unix::fs::symlink(&attacker, fixture.root.join("acme")).expect("owner symlink");

    assert!(
        host.lock_images(
            &[canonical.lock().to_owned()],
            LockMode::Try,
            &other_operation()
        )
        .is_err(),
        "dirfd traversal must reject a symlinked owner"
    );
    assert!(
        host.create_attached(
            ImageCapacity::from_gibibytes(1),
            canonical.image(),
            &fixture.root.join("mounted-main"),
        )
        .is_err(),
        "command target validation must reject the same ancestor"
    );
    assert!(runner.requests().is_empty(), "no APFS command may spawn");
    assert!(
        std::fs::read_dir(&attacker)
            .expect("attacker directory")
            .next()
            .is_none(),
        "no lock or image may be created through the symlink"
    );
}

/// Main as an interrupted adoption leaves it: its canonical image and CA companion behind a
/// `PendingFence` sidecar recording revision 1, and the user's checkout still the original tree.
fn pending_adopt_fixture(fixture: &Fixture) -> (PathBuf, DetachedWorkspaceMetadata) {
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);
    let canonical = layout.main_image().expect("canonical").image().to_owned();
    create_image(&canonical);
    std::fs::write(&canonical, b"partially copied main").expect("pending payload");
    let mut pending = metadata();
    pending.publication_state = PublicationState::PendingFence;
    pending.grants.revision = 1;
    pending.info_snapshot.project_root = config.checkout_path.clone();
    pending
        .write_for_image(&canonical)
        .expect("pending sidecar");
    std::fs::create_dir_all(&config.checkout_path).expect("source checkout");
    std::fs::write(config.checkout_path.join("tracked"), b"original source").expect("source bytes");
    (canonical, pending)
}

#[test]
fn recovery_and_gc_leave_an_unpublished_main_to_its_adoption_and_the_checkout_alone() {
    let fixture = Fixture::new("pending-adopt-recovery");
    let config = fixture.config();
    let (canonical, pending) = pending_adopt_fixture(&fixture);
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", config.checkout_path.display()));
    let host = native_host(&fixture, RecordingRunner::default());

    for _ in 0..2 {
        host.recover_pending(&config, &[])
            .expect("recovery leaves the pending main to its intent");
        execute_gc(&host, &config).expect("gc never deletes a PendingFence image");
    }

    assert_eq!(
        std::fs::read(&canonical).expect("pending payload"),
        b"partially copied main"
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&canonical).expect("pending sidecar"),
        pending
    );
    assert!(ca_key_path(&canonical).exists());
    assert!(
        host.list(&repo()).expect("published facts").is_empty(),
        "an unpublished main is invisible to enumeration"
    );
    let adoption = host
        .pending_adoption(&config, &repo())
        .expect("pending adoption")
        .expect("the interrupted adoption is found");
    assert_eq!(adoption.image, canonical);
    assert_eq!(adoption.incarnation, pending.workspace_incarnation);
    assert_eq!(adoption.info, pending.info_snapshot);
    assert_eq!(
        std::fs::read(config.checkout_path.join("tracked")).expect("untouched source"),
        b"original source"
    );
    assert!(!pre_cowshed.exists());
    assert!(
        matches!(
            host.attach_verified(&canonical),
            Err(ApfsStorageError::PendingPublication(path)) if path == canonical
        ),
        "no verb may mount an unpublished main"
    );
}

#[test]
fn recovery_removes_a_pending_main_sidecar_whose_payload_never_appeared() {
    let fixture = Fixture::new("pending-adopt-sidecar-only");
    let config = fixture.config();
    let (canonical, _) = pending_adopt_fixture(&fixture);
    std::fs::remove_file(&canonical).expect("crash before the payload existed");

    native_host(&fixture, RecordingRunner::default())
        .recover_pending(&config, &[])
        .expect("sidecar-only fence is removed");

    assert!(!sidecar_path(&canonical).exists());
    assert!(!ca_key_path(&canonical).exists());
    assert_eq!(
        native_host(&fixture, RecordingRunner::default())
            .pending_adoption(&config, &repo())
            .expect("no adoption left"),
        None
    );
    assert_eq!(
        std::fs::read(config.checkout_path.join("tracked")).expect("untouched source"),
        b"original source"
    );
}

#[test]
fn pending_adoption_refuses_a_published_main() {
    let fixture = Fixture::new("pending-adopt-published");
    let canonical = StorageLayout::new(&fixture.root, &repo())
        .expect("layout")
        .main_image()
        .expect("canonical");
    create_image(canonical.image());

    assert!(matches!(
        native_host(&fixture, RecordingRunner::default())
            .pending_adoption(&fixture.config(), &repo()),
        Err(ApfsStorageError::InvalidPlan("main is already published"))
    ));
}

#[test]
fn gc_retires_a_crash_left_adopt_staging_mountpoint_and_keeps_the_pending_main() {
    let fixture = Fixture::new("pending-adopt-staging-mount");
    let config = fixture.config();
    let (canonical, pending) = pending_adopt_fixture(&fixture);
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let staging_mount = layout
        .project()
        .mount_root
        .join(".staging")
        .join(format!("main-{}", pending.workspace_incarnation));
    std::fs::create_dir_all(&staging_mount).expect("crash-left staging mountpoint");
    let host = native_host(&fixture, RecordingRunner::default());

    execute_gc(&host, &config).expect("gc retires the empty staging mountpoint");

    assert!(!staging_mount.exists());
    assert!(canonical.exists());
    assert!(sidecar_path(&canonical).exists());
}

/// A real main image mounted canonically at the checkout, carrying its marker, its minted
/// credentials, and a payload a rewrite or resize must carry across — mounted by an earlier
/// process, so the host under test learns of it from the kernel alone.
fn mounted_main(fixture: &RealFixture) -> (PathBuf, PathBuf) {
    let image = fixture
        .layout()
        .main_image()
        .expect("image")
        .image()
        .to_owned();
    fixture.published_image(&image);
    let mount = fixture.main_mount();
    fixture.mount_left_behind(&image, &mount, "nobrowse,owners");
    fixture.plant_marker(&mount, &workspace());
    mint_credentials(&workspace(), &mount, &fixture.root().join("main.ca.key"))
        .expect("workspace credentials");
    std::fs::write(mount.join("payload"), b"carried across").expect("payload");
    (image, mount)
}

/// After a verb that hands the workspace back mounted: it is on its own mount point with the
/// canonical flags, its payload intact, and the attachment is the host's to detach.
fn assert_back_on_its_mount<R: CommandRunner + Send + Sync + 'static>(
    host: &MacOsApfsExecutionHost<R>,
    image: &Path,
    mount: &Path,
) {
    assert!(
        kernel_mount_at(mount).is_some(),
        "the workspace returns to its own mount point"
    );
    assert_eq!(
        proven_mount_ids(host.mounts(&repo()).expect("canonical mount facts")).len(),
        1,
        "the remount carries the canonical flags and the workspace's marker"
    );
    assert_eq!(
        std::fs::read(mount.join("payload")).expect("payload"),
        b"carried across"
    );
    host.detach_mounted(&workspace(), DetachIntent::Release)
        .expect("the attachment is owned by the mount registry");
    assert!(kernel_mount_at(mount).is_none());
    assert!(!attached(image));
}

/// Pure (08_testing.md, "Unit tests": no mounts): a refused grow reads the image's length and runs no disk command.
#[test]
fn resize_refuses_a_capacity_that_does_not_grow_before_touching_the_image() {
    let fixture = Fixture::new("resize-shrink");
    let image = StorageLayout::new(&fixture.root, &repo())
        .expect("layout")
        .main_image()
        .expect("image");
    create_image(image.image());
    std::fs::OpenOptions::new()
        .write(true)
        .open(image.image())
        .expect("fixture image")
        .set_len(ImageCapacity::from_gibibytes(1).bytes())
        .expect("sparse RAW capacity");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let mount = fixture.config().checkout_path;

    for requested in [
        ImageCapacity::parse("512m").expect("half a gibibyte"),
        ImageCapacity::from_gibibytes(1),
    ] {
        let error = host
            .resize(&workspace(), image.image(), &mount, requested)
            .expect_err("resize only ever grows");

        assert!(
            matches!(
                error,
                ApfsStorageError::CapacityNotGrowing { current, requested: asked }
                    if current == ImageCapacity::from_gibibytes(1) && asked == requested
            ),
            "unexpected refusal: {error}"
        );
        assert_eq!(
            host.backend()
                .image_capacity(image.image())
                .expect("image capacity"),
            ImageCapacity::from_gibibytes(1),
            "a refused resize must not have grown anything"
        );
    }

    let requests = runner.requests();
    assert!(
        requests.is_empty(),
        "a refused resize runs no disk command: {requests:?}"
    );
}

/// A holder first refuses both resize and defragment without changing the image or its
/// attachment. Once released, the same mounted workspace grows and returns with its payload.
/// The refused-grow, defragment and failed-rewrite phases run on this same mounted workspace
/// in turn, and the detached workspace they leave then grows again and is handed back detached:
/// one fixture lifecycle proves the whole resize/defrag scenario (08_testing.md, "Host limit"),
/// instead of an attach per phase. Both resizes grow the container through the same verified
/// attachment; the mounted one reads the grown filesystem through its remount.
#[test]
fn real_apfs_resize_and_defrag_scenario_puts_a_mounted_workspace_back_on_its_mount_substrate_lifecycle()
 {
    let fixture = RealFixture::new("resize-mounted");
    let (image, mount) = mounted_main(&fixture);
    let host = fixture.host();
    let holder = std::fs::File::open(mount.join("payload")).expect("hold the volume");
    let before = kernel_attachments(&image);
    assert!(
        matches!(
            before.as_slice(),
            [only] if only.capacity == Some(ImageCapacity::from_gibibytes(1))
        ),
        "one attachment at one gibibyte before the resize: {before:?}"
    );

    let error = host
        .resize(
            &workspace(),
            &image,
            &mount,
            ImageCapacity::from_gibibytes(2),
        )
        .expect_err("a busy volume refuses the resize rather than being torn out");
    // The kernel's unmount dissent is the refusal, not an attachment the host failed to identify.
    assert!(
        matches!(
            &error,
            ApfsStorageError::Apfs(ApfsError::UnmountFailed {
                source: cowshed_apfs::mounts::NativeUnmountError::Busy(_),
                ..
            })
        ),
        "unexpected refusal: {error}"
    );
    assert!(
        kernel_mount_at(&mount).is_some(),
        "the workspace stays mounted"
    );
    assert_eq!(
        kernel_attachments(&image),
        before,
        "a workspace that would not detach keeps its one attachment, devices and capacity"
    );
    let defrag_host = fixture.host();
    let inode_before = std::fs::metadata(&image).expect("image").ino();
    let error = defrag_host
        .defragment(&workspace(), &image, &mount)
        .expect_err("a busy volume refuses the rewrite rather than being torn out");
    assert!(
        matches!(
            &error,
            ApfsStorageError::Apfs(ApfsError::UnmountFailed {
                source: cowshed_apfs::mounts::NativeUnmountError::Busy(_),
                ..
            })
        ),
        "unexpected refusal: {error}"
    );
    assert_eq!(
        std::fs::metadata(&image).expect("image").ino(),
        inode_before,
        "a workspace that would not detach keeps its image"
    );
    assert!(
        !std::fs::exists(cowshed_core::storage::apfs::extents::rewrite_sibling(
            &image
        ))
        .expect("stat sibling"),
        "a refused rewrite never starts a copy"
    );
    assert!(
        kernel_mount_at(&mount).is_some(),
        "the workspace stays mounted"
    );
    drop(holder);
    // A grow the image file refuses (user-immutable during the grow) leaves the image ungrown
    // and the workspace back on its mount, on this same fixture.
    let refused_host = MacOsApfsExecutionHost::with_mount_source(
        ImmutableDuringResize {
            system: SystemCommandRunner::new(fixture.host_namespace()),
            image: image.clone(),
            resized: AtomicUsize::new(0),
        },
        fixture.config(),
        SystemKernelMountSource,
    )
    .expect("native APFS host");
    let error = refused_host
        .resize(
            &workspace(),
            &image,
            &mount,
            ImageCapacity::from_gibibytes(2),
        )
        .expect_err("an image nothing may write refuses to grow");
    assert!(
        matches!(
            &error,
            ApfsStorageError::Apfs(ApfsError::FileOperation { operation, source, .. })
                if *operation == "grow RAW image"
                    && source.kind() == std::io::ErrorKind::PermissionDenied
        ),
        "unexpected failure: {error}"
    );
    assert_eq!(
        refused_host
            .backend()
            .runner()
            .resized
            .load(Ordering::SeqCst),
        1,
        "the refused grow really ran against the immutable file"
    );
    let after = kernel_attachments(&image);
    assert!(
        matches!(
            after.as_slice(),
            [only] if only.capacity == Some(ImageCapacity::from_gibibytes(1))
        ),
        "a refused resize grows nothing and leaves one fresh attachment: {after:?}"
    );
    assert!(
        kernel_mount_at(&mount).is_some(),
        "the workspace is back on its mount"
    );
    assert_eq!(
        std::fs::read(mount.join("payload")).expect("payload"),
        b"carried across"
    );

    let outcome = host
        .resize(
            &workspace(),
            &image,
            &mount,
            ImageCapacity::from_gibibytes(2),
        )
        .expect("resize a mounted workspace");
    assert_eq!(outcome.capacity, ImageCapacity::from_gibibytes(2));
    assert!(
        filesystem_bytes(&mount) > ImageCapacity::from_gibibytes(1).bytes(),
        "the remounted volume spans the grown container"
    );
    assert!(
        kernel_mount_at(&mount).is_some(),
        "the workspace is back on its mount"
    );
    assert_eq!(
        std::fs::read(mount.join("payload")).expect("payload"),
        b"carried across"
    );

    // Defragmenting the same mounted workspace replaces its image and puts it back.
    let before = std::fs::metadata(&image).expect("image");
    let outcome = host
        .defragment(&workspace(), &image, &mount)
        .expect("defragment a mounted workspace");
    let after = std::fs::metadata(&image).expect("image");
    assert_ne!(after.ino(), before.ino(), "the image is a new file");
    assert_eq!(after.len(), before.len());
    assert_eq!(after.permissions().mode(), before.permissions().mode());
    assert!(
        outcome.bytes > 0 && outcome.bytes <= before.len(),
        "only the image's data is copied: {} of {} bytes",
        outcome.bytes,
        before.len()
    );
    assert!(
        kernel_mount_at(&mount).is_some(),
        "the workspace is back on its mount"
    );

    // A rewrite that cannot clear its copy fails before a byte is copied, image untouched.
    let before = std::fs::metadata(&image).expect("image").ino();
    let sibling = cowshed_core::storage::apfs::extents::rewrite_sibling(&image);
    std::fs::create_dir(&sibling).expect("obstruct the rewrite");
    std::fs::write(sibling.join("occupant"), b"x").expect("occupy the obstruction");
    let error = host
        .defragment(&workspace(), &image, &mount)
        .expect_err("an obstructed rewrite fails");
    assert!(
        matches!(
            &error,
            ApfsStorageError::Io { operation, path, .. }
                if *operation == "remove an interrupted rewrite's copy" && *path == sibling
        ),
        "unexpected failure: {error}"
    );
    assert_eq!(std::fs::metadata(&image).expect("image").ino(), before);
    std::fs::remove_dir_all(&sibling).expect("clear the obstruction");
    assert_back_on_its_mount(&host, &image, &mount);

    // The workspace is detached now; a detached workspace is resized detached.
    let outcome = host
        .resize(
            &workspace(),
            &image,
            &mount,
            ImageCapacity::from_gibibytes(3),
        )
        .expect("resize a detached workspace");
    assert_eq!(outcome.previous, ImageCapacity::from_gibibytes(2));
    assert_eq!(outcome.capacity, ImageCapacity::from_gibibytes(3));
    assert!(
        !attached(&image),
        "a detached workspace is handed back detached"
    );
    assert!(kernel_mount_at(&mount).is_none());
    assert_eq!(
        host.backend()
            .image_capacity(&image)
            .expect("image capacity"),
        ImageCapacity::from_gibibytes(3)
    );
}

/// An image the kernel still holds with nothing mounted at the workspace's mount point — what an
/// image release that misread its inventory leaves behind — is refused before `diskutil image
/// resize` runs. diskutil would answer only `Resource busy`, and that attachment is not the
/// workspace mount's to release, so nothing is detached either.
#[test]
fn resize_refuses_an_image_still_attached_with_nothing_mounted_before_touching_it() {
    let fixture = Fixture::new("resize-attached-unmounted");
    let image = StorageLayout::new(&fixture.root, &repo())
        .expect("layout")
        .main_image()
        .expect("image");
    create_image(image.image());
    let runner = RecordingRunner::default();
    runner
        .inventory
        .0
        .lock()
        .expect("fake inventory")
        .push(image.image().to_str().expect("UTF-8 image path").to_owned());
    let host = native_host(&fixture, runner.clone());
    let mount_point = fixture.config().checkout_path;

    let error = host
        .resize(
            &workspace(),
            image.image(),
            &mount_point,
            ImageCapacity::from_gibibytes(2),
        )
        .expect_err("an attached image nothing mounts is refused");

    assert!(
        matches!(
            &error,
            ApfsStorageError::Host(message)
                if message.contains(&image.image().display().to_string())
                    && message.contains("hdiutil detach /dev/disk9")
                    && message.contains("confirm its backing image is still this exact image")
        ),
        "unexpected refusal: {error}"
    );
    let requests = runner.requests();
    assert!(
        requests.is_empty(),
        "a refused resize runs no disk command: {requests:?}"
    );
    assert_eq!(
        std::fs::read(image.image()).expect("image"),
        b"fixture",
        "a refused resize leaves the image as it found it"
    );
}

/// Lets the image's header growth really run, against an image file nothing may write: the
/// user-immutable flag goes on immediately before the grow and comes off as soon as it returns.
/// The open really refuses, and the image is really unchanged.
struct ImmutableDuringResize {
    system: SystemCommandRunner,
    image: PathBuf,
    resized: AtomicUsize,
}

fn set_file_flags(path: &Path, flags: u32) {
    use std::os::unix::ffi::OsStrExt;
    let raw = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("C path");
    // SAFETY: chflags reads only the NUL-terminated path, which outlives the call.
    assert_eq!(
        unsafe { libc::chflags(raw.as_ptr(), flags) },
        0,
        "chflags {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

impl CommandRunner for ImmutableDuringResize {
    fn run_guarded<E>(
        &self,
        request: &CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<CommandOutput>, E>
    where
        E: From<CommandRunError> + std::fmt::Display,
    {
        self.system.run_guarded(request, check)
    }
    fn run(
        &self,
        request: &CommandRequest,
        _operation: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
        self.system.run(request, _operation)
    }

    fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> std::io::Result<()> {
        use std::os::macos::fs::MetadataExt as _;
        assert_eq!(image, self.image, "only the fixture image grows");
        let flags = std::fs::metadata(&self.image).expect("image").st_flags();
        set_file_flags(&self.image, flags | libc::UF_IMMUTABLE);
        let grown = self.system.grow_image(image, capacity);
        set_file_flags(&self.image, flags);
        self.resized.fetch_add(1, Ordering::SeqCst);
        grown
    }

    fn image_lease(&self, identity: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        self.system.image_lease(identity)
    }

    fn pin_raw_device(&self, device: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
        self.system.pin_raw_device(device)
    }

    fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
        self.system.attached_disk_images()
    }

    fn attachments_of(&self, image: &Path) -> std::io::Result<Vec<AttachedDiskImage>> {
        self.system.attachments_of(image)
    }
}

/// SliceA quarantine: a published canonical image whose grants sidecar exists but whose CA
/// companion is missing must not abort the store-wide pass. Recovery quarantines the sidecar
/// (+ companion if present) into `<project>/quarantine/<ws>-<unix_ts>/` with a tombstone,
/// leaves the image in place, and completes the pass for the healthy sibling.
#[test]
fn quarantine_keyless_canonical_sidecar_leaves_image_and_spares_healthy_sibling() {
    let fixture = Fixture::new("quarantine-keyless-canonical");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    adopt_owner(&layout);

    let live = WorkspaceName::new("session-live").expect("live session");
    let live_canonical = layout.session_image(&live).expect("live canonical");
    create_image(live_canonical.image());
    let mut live_metadata = metadata();
    set_metadata_workspace(&mut live_metadata, live.clone());
    live_metadata
        .write_for_image(live_canonical.image())
        .expect("live metadata");

    let keyless = WorkspaceName::new("session-keyless").expect("keyless session");
    let keyless_canonical = layout.session_image(&keyless).expect("keyless canonical");
    create_image(keyless_canonical.image());
    let mut keyless_metadata = metadata();
    set_metadata_workspace(&mut keyless_metadata, keyless.clone());
    keyless_metadata
        .write_for_image(keyless_canonical.image())
        .expect("keyless metadata");
    std::fs::remove_file(ca_key_path(keyless_canonical.image())).expect("remove CA key");
    assert!(sidecar_path(keyless_canonical.image()).exists());
    assert!(!ca_key_path(keyless_canonical.image()).exists());

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("a keyless canonical must quarantine, not abort the pass");

    assert!(
        keyless_canonical.image().exists(),
        "the keyless image stays in place"
    );
    assert!(
        !sidecar_path(keyless_canonical.image()).exists(),
        "the keyless grants sidecar is quarantined away from the canonical location"
    );
    let project = layout.project().project_root.clone();
    let quarantine_root = project.join("quarantine");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&quarantine_root)
        .expect("quarantine root")
        .map(|entry| entry.expect("quarantine entry").path())
        .collect();
    entries.sort();
    assert_eq!(entries.len(), 1, "one workspace quarantined");
    let dir = &entries[0];
    assert!(
        dir.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("session-keyless-")),
        "quarantine dir names the workspace: {}",
        dir.display()
    );
    let tombstone = dir.join("tombstone.json");
    assert!(tombstone.exists(), "quarantine writes a tombstone");
    let wire: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&tombstone).expect("tombstone"))
            .expect("tombstone json");
    assert_eq!(wire["version"], serde_json::Value::Number(1.into()));
    assert_eq!(
        wire["repoId"],
        serde_json::Value::String("acme/widget".to_owned())
    );
    assert_eq!(
        wire["workspace"],
        serde_json::Value::String("session-keyless".to_owned())
    );
    assert_eq!(
        wire["incarnation"],
        serde_json::Value::String("00000000000000000000000000000001".to_owned())
    );
    assert!(
        wire["revision"].as_u64().is_some(),
        "tombstone records revision: {wire}"
    );
    assert_eq!(
        wire["reason"],
        serde_json::Value::String("missing-ca-companion".to_owned())
    );
    assert_eq!(
        wire["image"],
        serde_json::Value::String(keyless_canonical.image().display().to_string())
    );
    assert_eq!(
        wire["companion"],
        serde_json::Value::String(ca_key_path(keyless_canonical.image()).display().to_string())
    );
    assert!(
        wire["quarantinedAt"]
            .as_str()
            .is_some_and(|at| at.len() == 20 && at.contains('T') && at.ends_with('Z')),
        "tombstone records an RFC3339 UTC second: {wire}"
    );
    assert_eq!(
        wire["sidecar"],
        serde_json::Value::String(
            dir.join(
                sidecar_path(keyless_canonical.image())
                    .file_name()
                    .expect("sidecar name")
            )
            .display()
            .to_string()
        )
    );
    let sidecar_name = sidecar_path(keyless_canonical.image())
        .file_name()
        .expect("sidecar name")
        .to_owned();
    assert!(
        dir.join(&sidecar_name).exists(),
        "the quarantined sidecar lands beside the tombstone"
    );
    assert_eq!(
        std::fs::read(live_canonical.image()).expect("live image"),
        b"fixture",
        "the healthy sibling payload is untouched"
    );
    assert!(sidecar_path(live_canonical.image()).exists());
    assert!(ca_key_path(live_canonical.image()).exists());
    host.recover_pending(&fixture.config(), &[])
        .expect("repeated recovery is idempotent");
}

/// The production brick: a crashed create leaves a payload with a PendingFence sidecar that
/// enumeration deliberately hides, while the CAS re-read used to report it as Exists — every
/// plan saw Absent, every re-read saw Exists, and controller startup exhausted its retries
/// forever. Observation must agree with enumeration so the planned verb reaches apply, where
/// the intent's resume owns the residue.
#[test]
fn absent_observation_agrees_with_enumeration_on_pending_fence_residue() {
    let fixture = Fixture::new("observe-pending-fence");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let name = WorkspaceName::session("residue").expect("session");
    let canonical = layout.session_image(&name).expect("session image");
    create_image(canonical.image());
    let mut pending = metadata();
    set_metadata_workspace(&mut pending, name.clone());
    pending.publication_state = PublicationState::PendingFence;
    pending
        .write_for_image(canonical.image())
        .expect("pending sidecar");

    let host = native_host(&fixture, RecordingRunner::default());
    let expected = LifecycleFact::Absent {
        repo: repo(),
        name: name.clone(),
        topology_revision: Revision::new(2),
    };
    assert_eq!(
        host.observe(std::slice::from_ref(&expected))
            .expect("observe fenced residue"),
        vec![expected.clone()],
        "a fenced residue observes as Absent, like enumeration reports it"
    );

    // Control: a genuinely live workspace still observes as Exists, so real contention
    // keeps failing closed at revalidation instead of being paved over.
    let mut live = pending.clone();
    live.publication_state = PublicationState::Active;
    live.write_for_image(canonical.image())
        .expect("live sidecar");
    assert!(
        matches!(
            host.observe(std::slice::from_ref(&expected))
                .expect("observe live workspace")
                .as_slice(),
            [LifecycleFact::Exists { .. }]
        ),
        "an active image under the same name still observes as Exists"
    );
}

/// Resume must survive a main that moved since the crash: base commit is provenance recorded
/// on the residue, not identity, and workspaces forked from an older main are routine.
/// Every structural leg (repo, slot, role, format, generation, checkout, branch, ancestry,
/// worktree mode) still refuses on drift.
#[test]
fn resumable_clone_tolerates_a_main_that_moved_since_the_crash() {
    let fixture = Fixture::new("resume-moved-base");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let name = WorkspaceName::session("residue").expect("session");
    let canonical = layout.session_image(&name).expect("session image");
    create_image(canonical.image());
    let incarnation =
        WorkspaceIncarnation::new("d4d16c0f739d441fb38aed67cf010891").expect("incarnation");
    let mut pending = metadata();
    set_metadata_workspace(&mut pending, name.clone());
    pending.workspace_incarnation = incarnation.clone();
    pending.publication_state = PublicationState::PendingFence;
    pending.grants.revision = 3;
    let info = &mut pending.info_snapshot;
    info.base_commit = "oldbase".to_owned();
    info.branch = Some("cowshed/residue".to_owned());
    pending
        .write_for_image(canonical.image())
        .expect("pending sidecar");

    let source = LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        WorkspaceIncarnation::new("00000000000000000000000000000001").expect("incarnation"),
        Revision::new(2),
        Revision::new(2),
        WorkspaceRole::Main,
    )
    .expect("source workspace");
    // The replayed identity matches the residue structurally but carries main's current
    // base, which moved on after the crash.
    let mut requested = identity(&fixture);
    requested.project_root = PathBuf::from("/project");
    requested.base_commit = "newbase".to_owned();
    requested.branch = Some("cowshed/residue".to_owned());

    let host = native_host(&fixture, RecordingRunner::default());
    let resumed = host
        .resumable_clone(
            &fixture.config(),
            &source,
            &name,
            Revision::new(2),
            &requested,
        )
        .expect("resume check")
        .expect("a moved base must still resume");
    assert_eq!(resumed.workspace.name(), &name);
    assert_eq!(resumed.workspace.incarnation(), &incarnation);
    assert_eq!(
        resumed.identity.base_commit, "oldbase",
        "the resumed record keeps the residue's own base as provenance"
    );

    // Control: structural drift still fails closed.
    let mut drifted = requested.clone();
    drifted.branch = Some("cowshed/other".to_owned());
    assert!(
        host.resumable_clone(
            &fixture.config(),
            &source,
            &name,
            Revision::new(2),
            &drifted,
        )
        .is_err(),
        "a residue for another branch must not resume into this operation"
    );
}

#[test]
fn recovering_an_unmounted_attachment_mounts_its_pinned_volume_without_reattaching() {
    let fixture = Fixture::new("recover-pinned-mount");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image().expect("image");
    create_image(image.image());
    let runner = RecordingRunner::default();
    attach(&runner, image.image());
    let host = native_host(&fixture, runner.clone());
    let mount = fixture.root.join("recovered-mount");

    let resumed = host
        .attach_and_mount_resumable(image.image(), &mount, &workspace())
        .expect("reuse the existing attachment");
    assert_eq!(
        resumed.found,
        FoundMount::Attached,
        "an unmounted attachment is found attached, so a refusal unmounts only"
    );
    let attachment = resumed.attachment;

    assert_eq!(attachment.volume_device(), "/dev/disk10s1");
    assert_eq!(
        runner.requests(),
        [CommandRequest::new(
            "/sbin/mount_apfs",
            [
                "-o".as_ref(),
                "nobrowse,owners,noatime".as_ref(),
                attachment.volume_device().as_ref(),
                mount.as_os_str(),
            ],
        )],
        "recovery mounts only the pinned volume, without another attach"
    );
    let held = runner.hold_mount(&mount).expect("requested mount root");
    assert!(held.is_root());
    assert_eq!(held.source_device(), attachment.volume_device());
}

mod build_mount_races {
    use super::*;
    use cowshed_core::build_volume::{BuildVolumeId, BuildVolumeLayout};
    use cowshed_os::process::ProcessStatus;
    use parking_lot::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    #[derive(Clone, Default)]
    struct Mounts {
        rows: Arc<Mutex<Vec<KernelMountSnapshot>>>,
        unreadable: Arc<AtomicBool>,
    }

    impl KernelMountSource for Mounts {
        fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
            if self.unreadable.load(Ordering::SeqCst) {
                return Err(ApfsStorageError::Host(
                    "injected unreadable mount table".into(),
                ));
            }
            Ok(self.rows.lock().clone())
        }
    }

    #[derive(Clone, Copy)]
    enum Observation {
        Expected,
        Elsewhere,
        Foreign,
        Absent,
        Unreadable,
    }

    #[derive(Clone, Copy)]
    enum Completion {
        Mount,
        InProgress(Observation),
        OtherFailure,
    }

    struct LeaseGate {
        waiting: mpsc::SyncSender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }

    struct Runner {
        system: SystemCommandRunner,
        recorded: RecordingRunner,
        mounts: Mounts,
        completion: Completion,
        lease_calls: AtomicUsize,
        gate: Option<LeaseGate>,
    }

    fn already_in_progress() -> CommandOutput {
        CommandOutput {
            status: ProcessStatus::Exit(75),
            stdout: Vec::new(),
            stderr: b"mount_apfs: Operation already in progress\n".to_vec(),
        }
    }

    impl CommandRunner for Runner {
        fn run_guarded<E>(
            &self,
            request: &CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<CommandOutput>, E>
        where
            E: From<CommandRunError> + std::fmt::Display,
        {
            if check()? {
                self.run(request, "guarded disk command")
                    .map(|run| Some(run.output))
                    .map_err(E::from)
            } else {
                Ok(None)
            }
        }

        fn run(
            &self,
            request: &CommandRequest,
            _operation: &'static str,
        ) -> Result<cowshed_apfs::CommandRun, CommandRunError> {
            let output = self.recorded.run(request, _operation)?;
            if request.program != Path::new("/sbin/mount_apfs") {
                return Ok(output);
            }
            let point = PathBuf::from(request.args.last().unwrap());
            let mut rows = self.mounts.rows.lock();
            let observation = match self.completion {
                Completion::Mount if !rows.is_empty() => {
                    return Ok(cowshed_apfs::CommandRun {
                        output: already_in_progress(),
                        work_elapsed: None,
                    });
                }
                Completion::Mount | Completion::OtherFailure => Observation::Expected,
                Completion::InProgress(observation) => observation,
            };
            let row = match observation {
                Observation::Expected => Some((point, "/dev/disk10s1")),
                Observation::Elsewhere => {
                    Some((point.with_extension("elsewhere"), "/dev/disk10s1"))
                }
                Observation::Foreign => Some((point, "/dev/disk99s1")),
                Observation::Absent => None,
                Observation::Unreadable => {
                    self.mounts.unreadable.store(true, Ordering::SeqCst);
                    None
                }
            };
            *rows = row
                .into_iter()
                .map(|(point, device)| KernelMountSnapshot::new(1, point, device, true, true))
                .collect();
            Ok(cowshed_apfs::CommandRun {
                work_elapsed: output.work_elapsed,
                output: match self.completion {
                    Completion::Mount => output.output,
                    Completion::InProgress(_) => already_in_progress(),
                    Completion::OtherFailure => CommandOutput {
                        status: ProcessStatus::Exit(1),
                        stdout: Vec::new(),
                        stderr: b"injected unrelated mount failure\n".to_vec(),
                    },
                },
            })
        }

        fn image_lease(&self, identity: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
            if self.lease_calls.fetch_add(1, Ordering::SeqCst) == 1
                && let Some(gate) = &self.gate
            {
                gate.waiting.send(()).unwrap();
                gate.resume.lock().recv().unwrap();
            }
            self.system.image_lease(identity)
        }
        fn pin_raw_device(&self, _: &Path) -> std::io::Result<Option<Fenced<std::fs::File>>> {
            Ok(None)
        }
        fn attached_disk_images(&self) -> std::io::Result<Vec<AttachedDiskImage>> {
            self.recorded.attached_disk_images()
        }
        fn attachments_of(&self, image: &Path) -> std::io::Result<Vec<AttachedDiskImage>> {
            self.recorded.attachments_of(image)
        }
        fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> std::io::Result<()> {
            self.recorded.grow_image(image, capacity)
        }
    }

    fn runner(
        recorded: &RecordingRunner,
        mounts: &Mounts,
        completion: Completion,
        system: SystemCommandRunner,
    ) -> Runner {
        Runner {
            system,
            recorded: recorded.clone(),
            mounts: mounts.clone(),
            completion,
            lease_calls: AtomicUsize::new(0),
            gate: None,
        }
    }

    fn build(fixture: &Fixture, recorded: &RecordingRunner) -> (BuildVolumeLayout, BuildVolumeId) {
        let storage = StorageLayout::new(&fixture.root, &repo()).unwrap();
        let layout = BuildVolumeLayout::new(storage.project()).unwrap();
        let id = BuildVolumeId::mint();
        let image = layout.image(&id);
        std::fs::create_dir_all(image.parent().unwrap()).unwrap();
        std::fs::write(&image, b"ordinary file, never a real attached image").unwrap();
        attach(recorded, &image);
        (layout, id)
    }

    fn mount_commands(recorded: &RecordingRunner) -> usize {
        recorded
            .requests()
            .iter()
            .filter(|request| request.program == Path::new("/sbin/mount_apfs"))
            .count()
    }

    #[test]
    fn two_mounters_reobserve_after_the_second_image_lease() {
        let fixture = Fixture::new("two-build-mounters");
        let recorded = RecordingRunner::default();
        let mounts = Mounts::default();
        let (layout, id) = build(&fixture, &recorded);
        let (waiting, waited) = mpsc::sync_channel(1);
        let (resume, resumed) = mpsc::sync_channel(1);
        let mut loser = runner(
            &recorded,
            &mounts,
            Completion::Mount,
            SystemCommandRunner::new(fixture.host_namespace()),
        );
        loser.gate = Some(LeaseGate {
            waiting,
            resume: Mutex::new(resumed),
        });
        std::thread::scope(|scope| {
            let config = fixture.config();
            let loser_mounts = mounts.clone();
            let layout = &layout;
            let id = &id;
            let loser = scope.spawn(move || {
                MacOsApfsExecutionHost::with_mount_source(loser, config, loser_mounts)
                    .unwrap()
                    .mount_build_volume(layout, id)
            });
            waited.recv().expect("loser reached its second image lease");
            let winner = MacOsApfsExecutionHost::with_mount_source(
                runner(
                    &recorded,
                    &mounts,
                    Completion::Mount,
                    SystemCommandRunner::new(fixture.host_namespace()),
                ),
                fixture.config(),
                mounts.clone(),
            )
            .unwrap()
            .mount_build_volume(layout, id);
            resume.send(()).unwrap();
            let loser = loser.join().unwrap();
            assert_eq!(winner.expect("winner mounts").0, layout.mount(id));
            assert_eq!(
                loser
                    .expect("loser adopts the winner under the reacquired lease")
                    .0,
                layout.mount(id)
            );
        });
        assert_eq!(
            mount_commands(&recorded),
            1,
            "no second mount and no blind retry"
        );
    }

    #[test]
    fn an_in_progress_mount_is_adopted_only_after_its_expected_mount_is_observed() {
        let fixture = Fixture::new("in-progress-build-mount");
        let recorded = RecordingRunner::default();
        let mounts = Mounts::default();
        let (layout, id) = build(&fixture, &recorded);
        let host = MacOsApfsExecutionHost::with_mount_source(
            runner(
                &recorded,
                &mounts,
                Completion::InProgress(Observation::Expected),
                SystemCommandRunner::new(fixture.host_namespace()),
            ),
            fixture.config(),
            mounts,
        )
        .unwrap();
        assert_eq!(
            host.mount_build_volume(&layout, &id)
                .expect("adopt observed mount")
                .0,
            layout.mount(&id)
        );
        assert_eq!(mount_commands(&recorded), 1);
    }

    #[test]
    fn an_in_progress_mount_without_the_expected_identity_is_refused_without_retry() {
        for observation in [
            Observation::Elsewhere,
            Observation::Foreign,
            Observation::Absent,
            Observation::Unreadable,
        ] {
            let fixture = Fixture::new("refused-build-mount");
            let recorded = RecordingRunner::default();
            let mounts = Mounts::default();
            let (layout, id) = build(&fixture, &recorded);
            let host = MacOsApfsExecutionHost::with_mount_source(
                runner(
                    &recorded,
                    &mounts,
                    Completion::InProgress(observation),
                    SystemCommandRunner::new(fixture.host_namespace()),
                ),
                fixture.config(),
                mounts,
            )
            .unwrap();
            let error = host
                .mount_build_volume(&layout, &id)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(layout.image(&id).to_str().unwrap()),
                "{error}"
            );
            assert!(error.contains("/dev/disk10s1"), "{error}");
            assert!(
                error.contains(layout.mount(&id).to_str().unwrap()),
                "{error}"
            );
            assert!(error.contains("Operation already in progress"), "{error}");
            assert_eq!(mount_commands(&recorded), 1);
        }
    }

    #[test]
    fn an_unrelated_mount_failure_is_not_adopted_even_if_the_mount_appears() {
        let fixture = Fixture::new("other-build-mount-failure");
        let recorded = RecordingRunner::default();
        let mounts = Mounts::default();
        let (layout, id) = build(&fixture, &recorded);
        let host = MacOsApfsExecutionHost::with_mount_source(
            runner(
                &recorded,
                &mounts,
                Completion::OtherFailure,
                SystemCommandRunner::new(fixture.host_namespace()),
            ),
            fixture.config(),
            mounts,
        )
        .unwrap();
        let error = host
            .mount_build_volume(&layout, &id)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("injected unrelated mount failure"),
            "{error}"
        );
        assert_eq!(mount_commands(&recorded), 1);
    }
}

#[path = "workspace_isolation.rs"]
mod workspace_isolation;

/// The request these tests take an image lock for when they stand in for another lifecycle
/// operation holding it: a store's gc, which names no one workspace.
fn other_operation() -> cowshed_core::api::held_lock::LockHolding {
    cowshed_core::api::held_lock::LockHolding::store(
        cowshed_core::api::held_lock::LockOperation::Gc,
    )
}
