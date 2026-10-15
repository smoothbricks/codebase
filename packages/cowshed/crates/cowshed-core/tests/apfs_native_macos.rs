#[cfg(target_os = "macos")]
#[path = "apfs_native_macos/macos.rs"]
mod macos;

#[cfg(target_os = "macos")]
mod kernel_unmount {
    use std::fs::File;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use cowshed_apfs::image::ImageCapacity;
    use cowshed_apfs::mounts::{
        FilesystemId, HeldMount, NativeUnmountError, PostUnmountFd, UnmountEffect, UnmountMode,
        hold_mount,
    };
    use cowshed_apfs::{
        ApfsBackend, ApfsError, AttachedDiskImage, CommandOutput, CommandRequest, CommandRunError,
        CommandRunner, DetachIntent, MacOsApfsBackend, MountAccess, OwnedVolumeRoot,
        SystemCommandRunner,
    };
    use cowshed_os::fork_lock::Fenced;

    use super::macos::{RealFixture, attached, kernel_mount_at};

    #[derive(Debug)]
    struct ObservedUnmount {
        mode: UnmountMode,
        filesystem: Option<FilesystemId>,
        retired_errno: Option<i32>,
    }

    /// All effects remain production effects. Only the held-root answer can be redirected to
    /// another disposable image, to prove the owner refuses it before any native unmount.
    struct WitnessedNativeUnmount {
        foreign_root: PathBuf,
        substitute_root: AtomicBool,
        disk_arbitration_unmounts: AtomicUsize,
        observed: Mutex<Vec<ObservedUnmount>>,
    }

    impl WitnessedNativeUnmount {
        fn observe_command(&self, request: &CommandRequest) {
            if request.program == Path::new("/usr/sbin/diskutil")
                && request
                    .args
                    .first()
                    .is_some_and(|argument| argument == "unmount")
            {
                self.disk_arbitration_unmounts
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl CommandRunner for WitnessedNativeUnmount {
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            self.observe_command(request);
            SystemCommandRunner.run(request)
        }

        fn run_guarded<E>(
            &self,
            request: &CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<CommandOutput>, E>
        where
            E: From<CommandRunError> + std::fmt::Display,
        {
            self.observe_command(request);
            SystemCommandRunner.run_guarded(request, check)
        }

        fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<File>>> {
            SystemCommandRunner.image_lease(identity)
        }

        fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<File>>> {
            SystemCommandRunner.pin_raw_device(device)
        }

        fn hold_mount(&self, mount_point: &Path) -> io::Result<HeldMount> {
            let root = if self.substitute_root.load(Ordering::SeqCst) {
                self.foreign_root.as_path()
            } else {
                mount_point
            };
            SystemCommandRunner.hold_mount(root)
        }

        fn unmount_guarded<E>(
            &self,
            mode: UnmountMode,
            prove: impl FnOnce() -> Result<Option<OwnedVolumeRoot>, E>,
        ) -> Result<UnmountEffect, E>
        where
            E: From<NativeUnmountError> + std::fmt::Display,
        {
            let mut filesystem = None;
            let effect = SystemCommandRunner.unmount_guarded::<E>(mode, || {
                let root = prove()?;
                filesystem = root.as_ref().map(OwnedVolumeRoot::filesystem_id);
                Ok(root)
            })?;
            let retired_errno = match &effect {
                UnmountEffect::NoEffect => None,
                UnmountEffect::Unmounted {
                    post_fd: PostUnmountFd::Dead(source),
                } => source.raw_os_error(),
            };
            self.observed
                .lock()
                .expect("native observations")
                .push(ObservedUnmount {
                    mode,
                    filesystem,
                    retired_errno,
                });
            Ok(effect)
        }

        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            SystemCommandRunner.attached_disk_images()
        }

        fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
            SystemCommandRunner.grow_image(image, capacity)
        }
    }

    /// A fresh mount_apfs kernel mount is immediately released by its real owner, without
    /// waiting for Disk Arbitration to learn it. The independent foreign root is throwaway,
    /// never the host volume beneath a mountpoint (01a_apfs, Mounts and owned-root identity).
    #[test]
    fn real_apfs_kernel_mount_is_immediately_unmounted_by_held_fsid_without_touching_a_foreign_root()
     {
        let fixture = RealFixture::new("held-fsid-unmount");
        let image = fixture.root().join("owned.dmg");
        let foreign_image = fixture.root().join("foreign.dmg");
        let mount = fixture.root().join("owned-mount");
        let foreign_mount = fixture.root().join("foreign-mount");
        fixture.blank_image(&image);
        fixture.blank_image(&foreign_image);
        let system = MacOsApfsBackend::new(SystemCommandRunner);
        let foreign = system
            .attach_mounted(
                &foreign_image,
                &foreign_mount,
                MountAccess::ReadWrite,
                false,
            )
            .expect("mount disposable foreign image");
        let foreign_root = hold_mount(&foreign_mount).expect("hold foreign fixture root");
        let foreign_id = foreign_root.filesystem_id();
        let foreign_name = cowshed_apfs::volume_name(&foreign_mount).expect("foreign name");
        let backend = MacOsApfsBackend::new(WitnessedNativeUnmount {
            foreign_root: foreign_mount.clone(),
            substitute_root: AtomicBool::new(false),
            disk_arbitration_unmounts: AtomicUsize::new(0),
            observed: Mutex::new(Vec::new()),
        });
        let owned = backend
            .attach_verified(&image)
            .expect("attach verified owned image");
        fixture.remount(owned.volume_device(), &mount, "nobrowse,owners,noatime");
        let owned_root = hold_mount(&mount).expect("hold fresh kernel mount root");
        let owned_id = owned_root.filesystem_id();
        assert_ne!(owned_id, foreign_id);

        backend
            .runner()
            .substitute_root
            .store(true, Ordering::SeqCst);
        let refusal = backend
            .unmount_verified(&owned, DetachIntent::WhenIdle)
            .expect_err("a foreign held root cannot authorize unmount");
        backend
            .runner()
            .substitute_root
            .store(false, Ordering::SeqCst);
        assert!(
            matches!(
                &refusal,
                ApfsError::UnmountTargetNotMounted { volume_device, found, is_root: true, .. }
                    if volume_device == owned.volume_device() && found == foreign.volume_device()
            ),
            "{refusal}"
        );
        assert_eq!(
            kernel_mount_at(&mount)
                .expect("owned still mounted after refusal")
                .source_device,
            owned.volume_device()
        );
        assert!(
            backend
                .runner()
                .observed
                .lock()
                .expect("observations")
                .is_empty()
        );

        backend
            .rename_volume(&owned, &mount, "[cowshed] kernel-unmount fixture")
            .expect("event-only held root retains descriptor-native naming");
        assert_eq!(
            cowshed_apfs::volume_name(&mount)
                .expect("owned name readback")
                .to_string_lossy(),
            "[cowshed] kernel-unmount fixture"
        );
        assert_eq!(
            hold_mount(&mount)
                .expect("root after naming")
                .filesystem_id(),
            owned_id
        );
        for _ in 0..2 {
            backend
                .unmount_verified(&owned, DetachIntent::WhenIdle)
                .expect("fresh kernel namespace unmount and repeated no-effect unmount");
            assert!(kernel_mount_at(&mount).is_none());
            let retained = backend
                .existing_attachment(&image)
                .expect("current exact-image mapping")
                .expect("namespace-only unmount retains the image");
            assert_eq!(retained.volume_device(), owned.volume_device());
            assert_eq!(retained.whole_device(), owned.whole_device());
        }
        {
            let observed = backend
                .runner()
                .observed
                .lock()
                .expect("native observations");
            assert_eq!(
                observed.len(),
                2,
                "one native effect and one positive no-effect result"
            );
            assert!(
                observed
                    .iter()
                    .all(|entry| entry.mode == UnmountMode::Normal)
            );
            assert_eq!(observed[0].filesystem, Some(owned_id));
            assert_eq!(observed[0].retired_errno, Some(libc::ENOTSUP));
            assert_eq!(observed[1].filesystem, None);
            assert_eq!(observed[1].retired_errno, None);
        }
        assert_eq!(
            backend
                .runner()
                .disk_arbitration_unmounts
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            hold_mount(&foreign_mount)
                .expect("foreign root unchanged")
                .filesystem_id(),
            foreign_id
        );
        assert_eq!(
            cowshed_apfs::volume_name(&foreign_mount).expect("foreign name unchanged"),
            foreign_name
        );
        assert_eq!(
            kernel_mount_at(&foreign_mount)
                .expect("foreign remains mounted")
                .source_device,
            foreign.volume_device()
        );
        drop(owned_root);
        drop(foreign_root);
        backend
            .detach(&owned, DetachIntent::WhenIdle)
            .expect("normal exact owned-image cleanup");
        system
            .detach(&foreign, DetachIntent::WhenIdle)
            .expect("normal exact foreign-fixture cleanup");
        assert!(kernel_mount_at(&mount).is_none());
        assert!(kernel_mount_at(&foreign_mount).is_none());
        assert!(!attached(&image));
        assert!(!attached(&foreign_image));
    }
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn native_apfs_bootstrap_refuses_unsupported_platform() {
    use cowshed_core::storage::bootstrap::native::{
        NativeBootstrapError, NativeBootstrapMode, bootstrap_system_storage,
    };
    use std::path::Path;

    let result = bootstrap_system_storage(
        Path::new("/unused"),
        Path::new("/tmp"),
        NativeBootstrapMode::ExistingOnly,
    )
    .await;
    assert!(
        matches!(result, Err(NativeBootstrapError::UnsupportedPlatform(_))),
        "a host without APFS must not claim a successful native bootstrap"
    );
}
