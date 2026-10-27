use super::*;
use cowshed_os::fork_lock::Spawn as _;
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use cowshed_os::test_backstop::ChildBackstop as _;
use scratch_apfs::ScratchRoot;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// The runner of a fixture's disk tools: they lease as the fixture's test namespace, never as
/// production.
fn runner(namespace: TestNamespace) -> SystemCommandRunner {
    SystemCommandRunner::new(HostNamespace::Test(namespace))
}

fn capacity(value: &str) -> ImageCapacity {
    ImageCapacity::parse(value).expect("test capacities are well formed")
}

struct FixtureRetainedInput<'a>(&'a Path);

impl RetainedInputs for FixtureRetainedInput<'_> {
    fn names(&self, path: &Path) -> bool {
        self.0 == path
    }
}

fn assert_lease_excludes_another_descriptor(held: &fs::File) {
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let mut path = [0u8; libc::PATH_MAX as usize];
    // SAFETY: F_GETPATH writes at most PATH_MAX bytes to the live descriptor's path buffer.
    assert_eq!(
        unsafe { libc::fcntl(held.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) },
        0
    );
    let path = std::ffi::CStr::from_bytes_until_nul(&path).expect("the kernel terminates the path");
    let probe =
        fs::File::open(std::ffi::OsStr::from_bytes(path.to_bytes())).expect("reopen held lease");
    assert!(matches!(
        probe.try_lock(),
        Err(fs::TryLockError::WouldBlock)
    ));
}
#[cfg(target_os = "macos")]
struct RealImageCleanup<'a> {
    backend: &'a MacOsApfsBackend<SystemCommandRunner>,
    image: PathBuf,
    attachment: Option<AttachedImage>,
    armed: bool,
}

#[cfg(target_os = "macos")]
impl<'a> RealImageCleanup<'a> {
    fn new(backend: &'a MacOsApfsBackend<SystemCommandRunner>, image: PathBuf) -> Self {
        Self {
            backend,
            image,
            attachment: None,
            armed: true,
        }
    }

    fn track(&mut self, attachment: AttachedImage) -> &AttachedImage {
        assert!(
            self.attachment.is_none(),
            "only one real image attachment may be tracked"
        );
        self.attachment.insert(attachment)
    }

    fn release_attachment(&self) -> Result<(), ApfsError> {
        // The scenario may already have detached its returned handle. Only the image's
        // current owned mapping authorizes cleanup, including an unreturned attachment.
        self.backend
            .release_image(&self.image, DetachIntent::Release)
    }

    fn delete_released_backing_file(&self) -> Result<(), ApfsError> {
        use std::os::unix::fs::OpenOptionsExt;

        // Registered-media absence is not proof that a departed helper released its file lock.
        // Keep the exclusive descriptor across unlink, so an attach cannot enter that gap.
        let _unlocked = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXLOCK | libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(&self.image)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(ApfsError::FileOperation {
                    operation: "prove released fixture backing file unlocked",
                    path: self.image.clone(),
                    source,
                });
            }
        };
        self.backend.delete_image(&self.image)
    }

    fn finish(mut self) -> Result<(), ApfsError> {
        // Exactly one cleanup attempt: a failure retains its media and original error.
        self.armed = false;
        self.release_attachment()?;
        self.delete_released_backing_file()
    }
}

#[cfg(target_os = "macos")]
impl Drop for RealImageCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            let released = self
                .release_attachment()
                .and_then(|()| self.delete_released_backing_file());
            if let Err(error) = released {
                cowshed_os::eprintln!(
                    "cowshed: retained real APFS fixture {}: {error}",
                    self.image.display()
                );
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn finish_real_image_test(result: Result<(), ApfsError>, cleanup: RealImageCleanup<'_>) {
    match (result, cleanup.finish()) {
        (Ok(()), Ok(())) => {}
        (Err(error), Ok(())) => panic!("real APFS scenario failed: {error}"),
        (Ok(()), Err(error)) => panic!("real APFS cleanup failed: {error}"),
        (Err(primary), Err(cleanup)) => {
            panic!("real APFS scenario failed: {primary}; cleanup failed: {cleanup}")
        }
    }
}
/// A missing attachment handle and empty registered mapping do not release a helper's file
/// lock. The fixture preserves that refusal; an unlocked backing file is removable without an
/// image attach, and the same cleanup accepts a file already absent.
#[cfg(target_os = "macos")]
#[test]
fn fixture_cleanup_proves_backing_file_unlock_before_unlink() {
    use std::os::unix::fs::OpenOptionsExt;

    let root = ScratchRoot::new_owned("cleanup-lock").expect("owned scratch root");
    let image = root.path().join("unreturned.dmg");
    fs::write(&image, b"fixture backing file").expect("create fixture backing file");
    let helper = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_SHLOCK | libc::O_NONBLOCK)
        .open(&image)
        .expect("hold the helper's image-file lock");
    let backend = MacOsApfsBackend::new(runner(root.namespace()));
    let refused = RealImageCleanup::new(&backend, image.clone()).finish();
    assert!(matches!(
        refused,
        Err(ApfsError::FileOperation { operation: "prove released fixture backing file unlocked", source, .. })
            if source.kind() == io::ErrorKind::WouldBlock
    ));
    assert_eq!(
        fs::read(&image).expect("retained backing file"),
        b"fixture backing file"
    );
    drop(helper);
    RealImageCleanup::new(&backend, image.clone())
        .finish()
        .expect("exact-image absence and exclusive open permit unlink");
    assert!(!image.exists());
    RealImageCleanup::new(&backend, image)
        .finish()
        .expect("an absent backing file needs no unlink");
}

/// A scratch root is the directory of the test namespace it mints, in the per-user root beside `t`
/// (01_storage.md, "Host namespace"): 0700 and the user's, named for the namespace and so for the
/// pid that owns it. `t` holds only what every test run shares, never a root.
#[cfg(target_os = "macos")]
#[test]
fn a_scratch_root_is_its_test_namespaces_private_directory_beside_t() {
    use std::os::unix::fs::MetadataExt as _;

    let root = ScratchRoot::new_owned("namespace-directory").expect("owned scratch root");
    // SAFETY: `geteuid` reads this process's credentials; it takes no pointers and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let namespace = root.namespace();
    assert_eq!(
        namespace.owner_pid(),
        std::process::id(),
        "this process minted it"
    );
    assert_eq!(
        root.path(),
        HostNamespace::Test(namespace)
            .macos_user_root(euid)
            .as_path(),
        "the root is the directory of its namespace"
    );
    assert_eq!(
        root.path().parent(),
        Some(HostNamespace::Production.macos_user_root(euid).as_path()),
        "it is an entry of the per-user root, beside t, mnt and ram<revision>"
    );
    assert!(
        !root
            .path()
            .starts_with(crate::scratch_apfs::SCRATCH_PARENT.as_path()),
        "no root is made in the directory every test run shares"
    );
    let metadata = fs::symlink_metadata(root.path()).expect("the root's metadata");
    assert!(metadata.is_dir());
    assert_eq!(metadata.uid(), euid);
    assert_eq!(metadata.mode() & 0o777, 0o700);
}

/// The system runner, except that naming a volume first force-unmounts it, as another process
/// may (`hdiutil detach -force` past a grace, a fixture teardown after its supervisor was
/// stopped): the window between proving a volume's root and naming it.
#[cfg(target_os = "macos")]
struct ForcedUnmountBeforeNaming(SystemCommandRunner);

#[cfg(target_os = "macos")]
impl CommandRunner for ForcedUnmountBeforeNaming {
    fn run(
        &self,
        request: &CommandRequest,
        operation: &'static str,
    ) -> Result<cowshed_apfs::CommandRun, cowshed_apfs::CommandRunError> {
        self.0.run(request, operation)
    }
    fn run_guarded<E>(
        &self,
        request: &CommandRequest,
        check: impl FnOnce() -> Result<bool, E>,
    ) -> Result<Option<cowshed_apfs::CommandOutput>, E>
    where
        E: From<cowshed_apfs::CommandRunError> + std::fmt::Display,
    {
        self.0.run_guarded(request, check)
    }
    fn image_lease(
        &self,
        identity: &Path,
    ) -> io::Result<Option<cowshed_os::fork_lock::Fenced<fs::File>>> {
        self.0.image_lease(identity)
    }
    fn pin_raw_device(
        &self,
        device: &Path,
    ) -> io::Result<Option<cowshed_os::fork_lock::Fenced<fs::File>>> {
        self.0.pin_raw_device(device)
    }
    fn attached_disk_images(&self) -> io::Result<Vec<cowshed_apfs::AttachedDiskImage>> {
        self.0.attached_disk_images()
    }
    fn attachments_of(&self, image: &Path) -> io::Result<Vec<cowshed_apfs::AttachedDiskImage>> {
        self.0.attachments_of(image)
    }
    fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
        self.0.grow_image(image, capacity)
    }
    fn name_volume(&self, root: &cowshed_apfs::OwnedVolumeRoot, name: &str) -> io::Result<()> {
        use std::os::unix::ffi::OsStrExt as _;
        let mount_point = std::ffi::CString::new(root.mount_point().as_os_str().as_bytes())
            .expect("a mountpoint without NUL");
        // SAFETY: `mount_point` is NUL-terminated and outlives the call.
        if unsafe { libc::unmount(mount_point.as_ptr(), libc::MNT_FORCE) } != 0 {
            return Err(io::Error::last_os_error());
        }
        root.set_name(name)
    }
}

/// The kernel's name for the volume holding `path`.
#[cfg(target_os = "macos")]
fn name_at(path: &Path) -> String {
    cowshed_apfs::volume_name(path)
        .unwrap_or_else(|error| panic!("read the volume name at {}: {error}", path.display()))
        .to_string_lossy()
        .into_owned()
}

/// A volume is named only through its own held mount root. Two real images stand in for the
/// host: the outer image's volume holds the inner's mountpoint, so every wrong target a rename
/// could fall back to — the bare directory a departed volume leaves, the volume beneath — is
/// the outer throwaway volume, never the host's Data volume.
///
/// The store image's rename (`rename_volume`) names its own owned volume, refuses the outer
/// volume's path as the inner's root, and, with the inner volume force-unmounted between the
/// proof and the name, fails and leaves the outer volume's name as it was. Disk Arbitration's
/// rename by device took the outer volume's name in that window in 16 of 20 measured runs.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_a_rename_names_only_its_own_held_volume_root() {
    let root = ScratchRoot::new("owned-rename").expect("scratch root");
    let label = HostNamespace::Test(root.namespace())
        .volume_label(format_args!("fixture · dispatch — main"));
    let system = runner(root.namespace());
    let backend = MacOsApfsBackend::new(system);
    let forcing = MacOsApfsBackend::new(ForcedUnmountBeforeNaming(system));
    let outer_image = root.path().join("outer").with_extension(IMAGE_EXTENSION);
    let inner_image = root.path().join("inner").with_extension(IMAGE_EXTENSION);
    let outer_mount = root.path().join("outer-volume");
    let inner_mount = outer_mount.join("inner-volume");
    let _paths = TestPaths(vec![outer_mount.clone()]);
    let mut outer_cleanup = RealImageCleanup::new(&backend, outer_image.clone());
    let mut inner_cleanup = RealImageCleanup::new(&backend, inner_image.clone());
    let result = (|| -> Result<(), ApfsError> {
        crate::blank_image::blank_image(root.namespace(), &outer_image);
        crate::blank_image::blank_image(root.namespace(), &inner_image);
        let outer = outer_cleanup.track(
            backend
                .attach_mounted(&outer_image, &outer_mount, MountAccess::ReadWrite, false)?
                .0,
        );
        let inner = inner_cleanup.track(
            backend
                .attach_mounted(&inner_image, &inner_mount, MountAccess::ReadWrite, false)?
                .0,
        );
        let outer_name = name_at(&outer_mount);
        assert_ne!(outer_name, label);

        backend.rename_volume(inner, &inner_mount, &label)?;
        assert_eq!(
            name_at(&inner_mount),
            label,
            "the store rename names its volume"
        );

        let refused = backend
            .rename_volume(inner, &outer_mount, &label)
            .expect_err("the outer volume's root is not the inner's");
        assert!(
            matches!(
                &refused,
                ApfsError::RenameTargetNotMounted { found, .. } if found == outer.volume_device()
            ),
            "{refused}"
        );
        assert_eq!(name_at(&outer_mount), outer_name);

        let failed = forcing
            .rename_volume(inner, &inner_mount, "MUST-NOT-LAND")
            .expect_err("a volume force-unmounted since its proof is not named");
        assert!(
            matches!(&failed, ApfsError::RenameFailed { volume_device, .. }
                if volume_device == inner.volume_device()),
            "{failed}"
        );
        assert_eq!(
            name_at(&outer_mount),
            outer_name,
            "the rename fell back to the volume beneath"
        );
        assert_eq!(
            name_at(&inner_mount),
            outer_name,
            "the departed mountpoint is a bare directory of the outer volume"
        );

        backend.mount(inner, &inner_mount, MountAccess::ReadWrite, false)?;
        assert_eq!(name_at(&inner_mount), label, "the failed name never landed");
        Ok(())
    })();
    finish_real_image_test(result, inner_cleanup);
    finish_real_image_test(Ok(()), outer_cleanup);
}

/// A question about one image is the kernel matching that image's `DiskImageURL` to the file URL
/// cowshed spells for its path, so the two spellings must agree for every path a store holds:
/// a store's owner and repository components percent-encode, and a file URL escapes a literal
/// `%` as it does a space or a `#`. The answer is exactly the whole inventory's entries for the
/// image, and a path the kernel does not hold has none.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_one_image_is_matched_by_the_url_its_driver_registered() {
    let root = ScratchRoot::new("image-url").expect("scratch root");
    let system = runner(root.namespace());
    let backend = MacOsApfsBackend::new(system);
    let image = root.path().join("a b%2F#c").with_extension(IMAGE_EXTENSION);
    let absent = root.path().join("a b%2F#d").with_extension(IMAGE_EXTENSION);
    let mount = root.path().join("volume");
    let _paths = TestPaths(vec![mount.clone()]);
    let mut cleanup = RealImageCleanup::new(&backend, image.clone());
    let result = (|| -> Result<(), ApfsError> {
        crate::blank_image::blank_image(root.namespace(), &image);
        let attached = cleanup.track(
            backend
                .attach_mounted(&image, &mount, MountAccess::ReadWrite, false)?
                .0,
        );
        let whole: Vec<_> = system
            .attached_disk_images()
            .map_err(ApfsError::KernelInventory)?
            .into_iter()
            .filter(|held| held.source == DiskImageSource::File(image.clone()))
            .collect();
        assert_eq!(whole.len(), 1, "the image is attached once: {whole:?}");
        assert_eq!(
            system
                .attachments_of(&image)
                .map_err(ApfsError::KernelInventory)?,
            whole
        );
        assert!(
            system
                .attachments_of(&absent)
                .map_err(ApfsError::KernelInventory)?
                .is_empty()
        );
        let recovered = backend
            .recovered_image_attachment(&image)?
            .expect("the attached image is recovered");
        assert_eq!(recovered.whole_device(), attached.whole_device());
        Ok(())
    })();
    finish_real_image_test(result, cleanup);
}

#[cfg(target_os = "macos")]
fn lease_test_image(staged_stem: PathBuf, volume_name: &str) -> CreateImageRequest {
    CreateImageRequest {
        staged_stem,
        capacity: capacity("64m"),
        volume_name: volume_name.into(),
        // SAFETY: `getuid` reads this process's credentials; it takes no pointers and
        // cannot fail.
        owner_uid: unsafe { libc::getuid() },
        // SAFETY: `getgid` reads this process's credentials; it takes no pointers and
        // cannot fail.
        owner_gid: unsafe { libc::getgid() },
    }
}

/// Plain filesystem paths a real-APFS test creates beside its images: alias links and mount
/// directories. Declared before the image cleanups, so on every exit — success, `?` or
/// unwind — it drops after them, once scoped threads have joined and devices are released.
/// A removal failure fails a passing test; during an unwind it is reported beside the
/// original failure instead of replacing it.
#[cfg(target_os = "macos")]
struct TestPaths(Vec<PathBuf>);

#[cfg(target_os = "macos")]
impl Drop for TestPaths {
    fn drop(&mut self) {
        let mut retained = Vec::new();
        for path in &self.0 {
            let removed = match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_dir() => fs::remove_dir(path),
                Ok(_) => fs::remove_file(path),
                Err(error) => Err(error),
            };
            match removed {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => retained.push(format!("{}: {error}", path.display())),
            }
        }
        if retained.is_empty() {
            return;
        }
        let retained = retained.join("; ");
        if std::thread::panicking() {
            cowshed_os::eprintln!(
                "cowshed: retained test paths after the failure above: {retained}"
            );
        } else {
            panic!("remove test paths: {retained}");
        }
    }
}

/// The device namespace is shared, but leases are per image: every cooperating step of one
/// image waits for that image's lease, and nothing of another image does.
///
/// Holding image A's production lease, the test drives image B through a complete real
/// lifecycle on another thread — blank create, attach, format and detach; attach and fsck;
/// mount; native unmount; detach — while a real detach of A, started first, waits. Under a
/// host-wide lease B could not begin; without a per-image lease A's detach would not wait
/// for the release. Aliases of A's file (a symlinked parent, a hard link) take other leases
/// yet gain nothing: the inventory never maps them, so they cannot release A's device, and
/// growth through either finds A's attachment by the file's device and inode, so neither can
/// grow A's file — checked without spending an attach of its own. Every wait is an event; the
/// only deadline is the hang bound, and it names the step still open.
///
/// A's attachment is also the image-growth witness, so growth spends no attach of its own: an
/// in-kernel attachment takes no lock on its file, so growth refuses it by the registry
/// (01a_apfs.md, "Images"); growing A while attached is refused outright and nothing is
/// written, and a detached capture of A is refused the same way; once A is detached, the grow
/// lands and A's capacity reports it.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_image_leases_overlap_across_images_and_exclude_within_one() {
    use std::sync::mpsc;

    enum Progress {
        Step(&'static str),
        Done,
    }

    let root = ScratchRoot::new("leases").expect("scratch root");
    let system = runner(root.namespace());
    let backend = MacOsApfsBackend::new(system);
    let image_a = root
        .path()
        .join("lease-held")
        .with_extension(IMAGE_EXTENSION);
    let stem_b = root.path().join("lease-free");
    let image_b = stem_b.with_extension(IMAGE_EXTENSION);
    let mount_b = root.path().join("lease-free-mount");
    let alias_parent = root.path().join("lease-alias-parent");
    let hard_link = root
        .path()
        .join("lease-hard-link")
        .with_extension(IMAGE_EXTENSION);
    let _paths = TestPaths(vec![
        alias_parent.clone(),
        hard_link.clone(),
        mount_b.clone(),
    ]);
    let order = std::sync::Mutex::new(Vec::new());
    let mut cleanup_a = RealImageCleanup::new(&backend, image_a.clone());
    let result = (|| -> Result<(), ApfsError> {
        crate::blank_image::blank_image(root.namespace(), &image_a);
        let attachment = cleanup_a.track(backend.attach_verified(&image_a)?);
        assert!(attachment.whole_device().starts_with("/dev/disk"));
        assert!(attachment.volume_device().starts_with("/dev/disk"));
        // Before A's lease is held below, since resizing takes it.
        let refused = system
            .grow_image(&image_a, capacity("2g"))
            .expect_err("growth refuses an attached image");
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock, "{refused}");
        assert!(matches!(
            backend.resize_image(&image_a, capacity("2g")),
            Err(ApfsError::FileOperation { operation: "grow RAW image", source, .. })
                if source.kind() == io::ErrorKind::WouldBlock
        ));
        // A detached capture refuses the attached source too, and writes no clone.
        let capture = root
            .path()
            .join("lease-held-capture")
            .with_extension(IMAGE_EXTENSION);
        assert!(matches!(
            backend.prepare_clone(&image_a, &capture, ImageCapture::Detached).and_then(|admitted| admitted.clone_image()),
            Err(cowshed_apfs::CloneFileError::SourceAttached { source }) if source == image_a
        ));
        assert!(
            !capture.exists(),
            "a refused capture wrote {}",
            capture.display()
        );

        std::os::unix::fs::symlink(image_a.parent().expect("temp parent"), &alias_parent)
            .expect("symlinked-parent alias");
        fs::hard_link(&image_a, &hard_link).expect("hard-link alias");
        let symlinked = alias_parent.join(image_a.file_name().expect("image name"));

        let held = system
            .image_lease(&image_a)
            .expect("take the image's production lease")
            .expect("production takes a real lease");
        assert_lease_excludes_another_descriptor(&held);

        // Each alias takes its own lease, so these run while A's is held; none may take A's
        // file or disturb A's attachment. No alias is attached: its inventory key is its own
        // path, so an attach would reach `hdiutil attach` and spend a host attach. The
        // inventory shows no alias owns a device, and growth finds A's attachment of the file.
        for alias in [&symlinked, &hard_link] {
            assert!(
                backend.recovered_image_attachment(alias)?.is_none(),
                "{} has an inventory mapping of its own",
                alias.display()
            );
            let refused = system
                .grow_image(alias, capacity("2g"))
                .expect_err("A's attachment backs the file every alias names");
            assert_eq!(
                refused.kind(),
                io::ErrorKind::WouldBlock,
                "{}: {refused}",
                alias.display()
            );
            // Releases only the alias's own (empty) inventory, never A's device.
            backend.release_image(alias, DetachIntent::Release)?;
            match backend.recovered_image_attachment(&image_a)? {
                Some(RecoveredImageAttachment::Apfs(still)) => assert_eq!(
                    (still.whole_device(), still.volume_device()),
                    (attachment.whole_device(), attachment.volume_device())
                ),
                _ => panic!("{} took A's attachment away", alias.display()),
            }
        }

        let (detach_started, detach_waiting) = mpsc::channel();
        let (progress, steps) = mpsc::channel();
        std::thread::scope(|scope| -> Result<(), ApfsError> {
            let (backend, order, mount_b) = (&backend, &order, &mount_b);
            let detach_a = scope.spawn(move || {
                detach_started.send(()).expect("the test awaits A's detach");
                let detached = backend.detach(attachment, DetachIntent::Release);
                order.lock().expect("order").push("A detached");
                detached
            });
            detach_waiting
                .recv_timeout(DISK_CHILD_DEADLINE)
                .expect("A's detach started");
            scope.spawn(move || {
                // A send fails only after the test panicked; the scope reports that panic.
                let report = |event: Progress| {
                    if let Progress::Step(step) = &event {
                        // On stderr too, so a harness kill still shows the open step.
                        cowshed_os::eprintln!("cowshed: lease test image B step `{step}` started");
                    }
                    let _ = progress.send(event);
                };
                let mut cleanup = RealImageCleanup::new(backend, image_b);
                let result = (|| -> Result<(), ApfsError> {
                    report(Progress::Step("create"));
                    let created = backend
                        .create_staged_image(&lease_test_image(stem_b, "cowshed-lease-free"))?;
                    report(Progress::Step("attach-mount"));
                    let attachment = cleanup.track(
                        backend
                            .attach_mounted(&created, mount_b, MountAccess::ReadWrite, false)?
                            .0,
                    );
                    fs::write(mount_b.join("written-beside-a-held-lease"), b"independent")
                        .expect("write into B's volume");
                    report(Progress::Step("unmount"));
                    backend.unmount_verified(attachment, DetachIntent::Release)?;
                    report(Progress::Step("detach"));
                    // Exercise cleanup recovery when a caller received no attachment handle:
                    // only the exact image's current registry mapping may authorize release.
                    cleanup.attachment.take();
                    cleanup.release_attachment()
                })();
                report(Progress::Step("cleanup"));
                finish_real_image_test(result, cleanup);
                report(Progress::Done);
            });
            let mut open = "spawn";
            loop {
                match steps.recv_timeout(DISK_CHILD_DEADLINE) {
                    Ok(Progress::Step(step)) => open = step,
                    Ok(Progress::Done) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                        "image B's `{open}` is still open after {DISK_CHILD_DEADLINE:?} \
                             while only A's lease is held"
                    ),
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        panic!("image B's lifecycle stopped inside `{open}`")
                    }
                }
            }
            {
                let mut order = order.lock().expect("order");
                order.push("B finished");
                order.push("A released");
            }
            drop(held);
            detach_a.join().expect("A's detach thread")
        })?;

        assert_eq!(
            *order.lock().expect("order"),
            ["B finished", "A released", "A detached"]
        );
        assert!(
            backend.recovered_image_attachment(&image_a)?.is_none(),
            "A's detach released its own device"
        );
        assert_eq!(backend.image_capacity(&image_a)?, capacity("1g"));
        backend.resize_image(&image_a, capacity("2g"))?;
        assert_eq!(backend.image_capacity(&image_a)?, capacity("2g"));
        Ok(())
    })();
    finish_real_image_test(result, cleanup_a);
}

/// How [`real_apfs_fixture_run_ended_by_its_parent`] ends: `panic`, `kill` or `sweep`.
#[cfg(target_os = "macos")]
const FIXTURE_ENDING: &str = "COWSHED_TEST_FIXTURE_ENDING";
/// The line a fixture run prints once each of its images is attached.
#[cfg(target_os = "macos")]
const FIXTURE_IMAGE: &str = "cowshed-test-fixture-image=";
/// The line a fixture run prints once a process it started works in its root: that pid and
/// the root, space-separated.
#[cfg(target_os = "macos")]
const FIXTURE_WORKER: &str = "cowshed-test-fixture-worker=";

/// A real-image fixture leaves nothing attached and nothing running however its run ends. A
/// panic unwinds through the scratch root's own teardown. A kill — the runner's no-progress
/// bound, a host-wide stop — runs no teardown at all, so its parent reclaims the
/// exact root after reaping the child. The same cleanup ends the dead owner's processes and
/// releases its images without sweeping other host runs.
/// Fixtures staged under `$TMPDIR` stayed attached after timed-out runs, and the stock Nx daemon a
/// timed-out test started kept running in its deleted root. The run's worker stands in for
/// that daemon: a process the run started that outlives it, selected, as the daemon is, by its
/// working directory alone. The run attaches two RAW images, as a failed re-adoption leaves its
/// replacement and a retained input, so teardown and sweep must release both. The first is
/// mounted, so teardown also unmounts; the retained input is attached read-only and unmounted,
/// all its release needs, so it spends no mount.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end() {
    use cowshed_os::fork_lock::Spawn;
    use std::io::{BufRead, BufReader};
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Child, Command, Stdio};

    // Helpers retain the suite's run identity and own only the roots their parent recovers.
    // Terse child output keeps libtest's test-name progress off the first image record.
    // Forward stderr line by line so a hung helper still shows its open step, and report
    // whether it printed a panic: its output is this test's, so a panic would read as this
    // test's assertion failure behind any timeout.
    let run = |ending: &str| -> (Child, std::thread::JoinHandle<bool>) {
        let mut child = Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "tests::real_apfs_fixture_run_ended_by_its_parent",
                "--ignored",
                "--nocapture",
                "--quiet",
            ])
            .env(FIXTURE_ENDING, ending)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked()
            .expect("spawn a fixture run");
        let stderr = BufReader::new(child.stderr.take().expect("run stderr"));
        let forwarded = std::thread::spawn(move || {
            stderr.lines().fold(false, |panicked, line| {
                let line = line.expect("run stderr");
                cowshed_os::eprintln!("{line}");
                panicked || line.contains("panicked at")
            })
        });
        (child, forwarded)
    };
    let backend = MacOsApfsBackend::new(runner(TestNamespace::mint().expect("a test namespace")));
    for ending in ["panic", "kill"] {
        let (mut child, forwarded) = run(ending);
        let mut lines = BufReader::new(child.stdout.take().expect("run stdout"))
            .lines()
            .map(|line| line.expect("run stdout"));
        let images: Vec<PathBuf> = lines
            .by_ref()
            .filter_map(|line| line.strip_prefix(FIXTURE_IMAGE).map(PathBuf::from))
            .take(2)
            .collect();
        // A red run must not leak the images it proves leaked.
        let _leaks: Vec<_> = images
            .iter()
            .map(|image| RealImageCleanup::new(&backend, image.clone()))
            .collect();
        assert_eq!(images.len(), 2, "the {ending} run attached {images:?}");
        let (worker, root) = lines
            .find_map(|line| {
                let (pid, root) = line.strip_prefix(FIXTURE_WORKER)?.split_once(' ')?;
                Some((pid.parse::<libc::pid_t>().ok()?, PathBuf::from(root)))
            })
            .unwrap_or_else(|| panic!("the {ending} run never started its worker"));
        if ending == "kill" {
            child.kill().expect("kill the run mid-fixture");
        }
        lines.for_each(drop);
        let status = child.wait_bounded().expect("reap the run");
        assert!(
            !forwarded.join().expect("forward the run's stderr"),
            "the {ending} run's deliberate failure printed a panic"
        );
        match ending {
            "panic" => assert_eq!(status.code(), Some(101), "the run failed"),
            _ => {
                assert_eq!(status.signal(), Some(libc::SIGKILL), "the run was killed");
                crate::scratch_namespace::reclaim_dead_root(&root)
                    .expect("reclaim exactly the killed fixture's root");
            }
        }
        for image in &images {
            assert!(
                backend
                    .attached_whole_devices(image)
                    .expect("attachment inventory")
                    .is_empty(),
                "{ending}: {} is still attached",
                image.display()
            );
            assert!(!image.exists(), "{ending}: {} remains", image.display());
        }
        let left = crate::scratch_namespace::processes::processes_in(|cwd| cwd.starts_with(&root))
            .expect("list the processes working under the run's root");
        assert_eq!(left, [], "{ending}: worker {worker} outlived its run");
        assert!(!root.exists(), "{ending}: {} remains", root.display());
    }
}
/// Block until `pid`, which is not this process's child, exits, on the kernel's exit event. A
/// process already gone, or already exiting, has exited. The device tests reach no cowshed-core
/// internals, so this is the test's own copy of the supervisor manager's waiter.
#[cfg(target_os = "macos")]
fn wait_for_exit(pid: libc::pid_t) -> io::Result<()> {
    let ident = usize::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: kqueue returns a new descriptor this function owns and closes.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kevent structs are plain data that outlive the call; `queue` is live until
    // the close below.
    let result = unsafe {
        let change = libc::kevent {
            ident,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let mut event = std::mem::zeroed::<libc::kevent>();
        let returned = libc::kevent(queue, &change, 1, &mut event, 1, std::ptr::null());
        let outcome = match returned {
            ..0 => Err(io::Error::last_os_error()),
            // A registration that failed comes back as an event carrying the error.
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(()),
        };
        libc::close(queue);
        outcome
    };
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        other => other,
    }
}

/// The fixture root the killed-caller attach pair shares through the environment.
#[cfg(target_os = "macos")]
const FIXTURE_KILL_ROOT: &str = "COWSHED_TEST_FIXTURE_KILL_ROOT";

/// An in-kernel attach outlives its caller: the caller is ended the way an agent tool kills a
/// tree — SIGTERM to every process working in its root, the attaching `hdiutil` among them,
/// then SIGKILL to the caller itself — mid-attach. The protected disk-image child (`hdiutil`
/// starts with those signals ignored and blocked) finishes regardless, so the next invocation
/// (this parent, after the kill) finds the image attached and releases it cleanly: no whole
/// device of the image remains, no attachment still backs the file, nothing keeps running in
/// its root, and no diskimagesiod ever served the image. The host's departed-drive
/// count is the lane's check (specs/cowshed/08_testing.md, "Host limit"), not one test's on a
/// shared host.
#[cfg(target_os = "macos")]
#[test]
fn real_apfs_a_disk_image_attach_survives_its_callers_kill() {
    use std::io::{BufRead, BufReader};
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    let root = ScratchRoot::new_owned("fixture-kill").expect("owned scratch root");
    let system = runner(root.namespace());
    let backend = MacOsApfsBackend::new(system);
    let image = root.path().join("attach.dmg");
    // Releases and deletes the image if the test fails before it does.
    let _cleanup = RealImageCleanup::new(&backend, image.clone());
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "tests::real_apfs_disk_image_attach_outliving_a_killed_caller",
            "--ignored",
            "--nocapture",
        ])
        .env(FIXTURE_KILL_ROOT, root.path())
        .process_group(0)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Its step log reaches this test's output; an unread pipe would fill and stop it.
        .stderr(Stdio::inherit())
        .spawn_locked()
        .expect("spawn the attaching caller");
    let caller = libc::pid_t::try_from(child.id()).expect("caller pid");
    let in_root = |cwd: &Path| cwd.starts_with(root.path());
    let mut lines = BufReader::new(child.stdout.take().expect("caller stdout")).lines();
    // Each attach's hdiutil is announced once it runs. Stop the caller there, so it cannot
    // move past the attach, and strike only while that hdiutil still runs: an announcement
    // read after its attach had already finished is passed over for the next one.
    let attach = loop {
        let line = lines
            .next()
            .expect("the caller's attach never came in flight")
            .expect("caller line");
        let Some(announced) = line.strip_prefix("inflight ") else {
            continue;
        };
        let announced: libc::pid_t = announced.parse().expect("announced hdiutil pid");
        // SAFETY: kill only names the caller, this test's unreaped child.
        assert_eq!(
            unsafe { libc::kill(caller, libc::SIGSTOP) },
            0,
            "stop the caller"
        );
        // WUNTRACED returns once the stop has taken effect.
        let (stopped, status) = cowshed_os::test_backstop::waitpid(caller, libc::WUNTRACED)
            .expect("wait for the caller's stop");
        assert_eq!(stopped, caller);
        assert!(libc::WIFSTOPPED(status), "the caller ended before its stop");
        let tree = crate::scratch_namespace::processes::processes_in(in_root)
            .expect("list the caller's tree");
        if tree.iter().any(|&(pid, _)| pid == announced) {
            break announced;
        }
        // SAFETY: as above.
        assert_eq!(unsafe { libc::kill(caller, libc::SIGCONT) }, 0, "resume");
    };
    // The tree-killer's two strokes: SIGTERM to every process working in the root
    // (the disk-image child among them, which holds it blocked), SIGKILL to the caller.
    for (pid, _) in
        crate::scratch_namespace::processes::processes_in(in_root).expect("list the caller's tree")
    {
        // SAFETY: kill only names a process; a pid gone since the listing reports ESRCH.
        unsafe { libc::kill(pid, libc::SIGTERM) };
    }
    child.kill().expect("kill the caller mid-attach");
    let _ = child.wait_bounded();
    lines.for_each(drop);
    // The detached attach, and anything else still working in the root, ends on its own:
    // wait for each one's exit event, then reconcile as the next invocation does.
    loop {
        let left =
            crate::scratch_namespace::processes::processes_in(in_root).expect("list the root");
        if left.is_empty() {
            break;
        }
        for (pid, _) in left {
            wait_for_exit(pid).expect("watch a detached disk child's exit");
        }
    }
    assert!(
        !backend
            .attached_whole_devices(&image)
            .expect("attachment inventory")
            .is_empty(),
        "the killed caller's attach (hdiutil {attach}) did not complete"
    );
    let image_helpers = crate::disk_image_helpers::image_helper_pids(&image)
        .expect("join this image to its helper PIDs");
    assert!(
        image_helpers.is_empty(),
        "an in-kernel attach named diskimagesiod helpers {image_helpers:?}"
    );
    backend
        .release_image(&image, DetachIntent::Release)
        .expect("release the attach the dead caller finished");
    assert!(
        backend
            .attached_whole_devices(&image)
            .expect("attachment inventory")
            .is_empty(),
        "the released image is still attached"
    );
    // The image is free to attach again: detached growth refuses a file any registered
    // attachment backs (01a_apfs.md, "Images") or a helper locks, and it succeeds, so no image
    // driver still holds it — proved without another attach. The released image is then
    // deleted, so a passing run retains no fixture.
    system
        .grow_image(&image, capacity("2g"))
        .expect("no image driver still holds the released image");
    backend
        .delete_image(&image)
        .expect("delete the released image");
    assert_eq!(
        crate::scratch_namespace::processes::processes_in(in_root).expect("list the root"),
        [],
        "a process outlived the released image in its root"
    );
}

/// The caller [`real_apfs_a_disk_image_attach_survives_its_callers_kill`] kills: it
/// attaches its image, detaches it, and begins again, so the kill lands mid-attach.
#[cfg(target_os = "macos")]
#[test]
#[ignore = "spawned by real_apfs_a_disk_image_attach_survives_its_callers_kill"]
fn real_apfs_disk_image_attach_outliving_a_killed_caller() {
    let Some(root) = std::env::var_os(FIXTURE_KILL_ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    std::env::set_current_dir(&root).expect("work in the fixture root");
    let namespace = TestNamespace::mint().expect("a test namespace");
    let backend = MacOsApfsBackend::new(runner(namespace));
    let image = root.join("attach.dmg");
    crate::blank_image::blank_image(namespace, &image);
    // Announce only the attaching hdiutil while attach is in flight, so the parent kills
    // mid-attach rather than during inventory reads or the exact-device release afterward.
    // Release starts only after attach returned and the attaching flag has been cleared.
    let watched = root.clone();
    let attaching = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watching = std::sync::Arc::clone(&attaching);
    std::thread::spawn(move || {
        let mut announced = std::collections::BTreeSet::new();
        loop {
            let found =
                crate::scratch_namespace::processes::processes_in(|cwd| cwd.starts_with(&watched))
                    .unwrap_or_default();
            for (pid, _) in found {
                let mut path = [0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
                // SAFETY: `path` is writable for its whole length, the size passed.
                let length =
                    unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
                let hdiutil = usize::try_from(length).is_ok_and(|length| {
                    length > 0 && String::from_utf8_lossy(&path[..length]).ends_with("/hdiutil")
                });
                if hdiutil
                    && watching.load(std::sync::atomic::Ordering::SeqCst)
                    && announced.insert(pid)
                {
                    println!("inflight {pid}");
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    loop {
        println!("attaching");
        attaching.store(true, std::sync::atomic::Ordering::SeqCst);
        let attachment = backend
            .attach_verified(&image)
            .expect("attach a fixture image");
        attaching.store(false, std::sync::atomic::Ordering::SeqCst);
        backend
            .detach(&attachment, DetachIntent::Release)
            .expect("detach a fixture image");
    }
}

/// One real-image fixture run, ended as [`FIXTURE_ENDING`] says: it fails or waits to be
/// killed once its images are attached and its worker started. The parent owns recovery of
/// this exact root after a kill; no child starts a host-wide sweep of unrelated runs.
#[cfg(target_os = "macos")]
#[test]
#[ignore = "spawned by real_apfs_fixture_runs_leave_nothing_attached_or_running_however_they_end"]
fn real_apfs_fixture_run_ended_by_its_parent() {
    let Some(ending) = std::env::var_os(FIXTURE_ENDING) else {
        return;
    };
    let root = ScratchRoot::new_owned("fixture-run").expect("owned scratch root");
    let backend = MacOsApfsBackend::new(runner(root.namespace()));
    for name in ["first", "retained/second"] {
        let image = root.path().join(name).with_extension(IMAGE_EXTENSION);
        crate::blank_image::blank_image(root.namespace(), &image);
        if name == "first" {
            let mount = root.path().join(format!("{name}-volume"));
            fs::create_dir(&mount).expect("a fixture mountpoint");
            backend
                .attach_mounted(&image, &mount, MountAccess::ReadWrite, false)
                .expect("attach and mount a RAW fixture");
        } else {
            // Attached only: its release is the claim, and a mount would add none.
            let record = FixtureRetainedInput(&image);
            let input = RetainedInput::named_by(&record, &image)
                .expect("the fixture records only its own retained input");
            backend
                .attach_retained_verified(&input)
                .expect("attach the recorded retained input read-only");
        }
        println!("{FIXTURE_IMAGE}{}", image.display());
    }
    // Bounded, so a worker no teardown or sweep ends still ends; a passing run ends it within
    // seconds.
    #[expect(
        clippy::zombie_processes,
        reason = "the worker must outlive this run; the scratch root or the next sweep ends it"
    )]
    let worker = cowshed_os::fork_lock::Spawn::spawn_locked(
        std::process::Command::new("/bin/sleep")
            .arg("600")
            .current_dir(root.path())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
    )
    .expect("start a worker in the root");
    println!("{FIXTURE_WORKER}{} {}", worker.id(), root.path().display());
    if ending == "panic" {
        // Unwinds through the scratch root's teardown as a failed assertion does, without the
        // panic hook: its `panicked at` would read as the parent's own assertion failure.
        std::panic::resume_unwind(Box::new("the fixture's run fails with its images attached"));
    }
    // Killed here; the parent's stdin closes only if it failed first.
    let _ = io::stdin().read_line(&mut String::new());
}
