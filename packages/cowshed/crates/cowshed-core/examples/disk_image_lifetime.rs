//! Compare kernel object lifetimes for production detach, never-mounted detach, DA eject, and commands alone.
//! Run on a quiet macOS host: disk_image_lifetime <scratch-parent> <production|unmounted|eject|commands> [cycles].
//! Counts are host-wide, so concurrent attachments must be reported rather than attributed here.

#[cfg(target_os = "macos")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use cowshed_apfs::image::ImageCapacity;
    use cowshed_apfs::{
        ApfsBackend, CommandRequest, CommandRunner, CreateImageRequest, DetachIntent,
        DiskImageSource, MountAccess, SystemCommandRunner,
    };
    use cowshed_core::storage::apfs::{ApfsSubstrateConfig, native::MacOsApfsExecutionHost};
    use cowshed_os::namespace::{HostNamespace, TestNamespace};
    use std::{fs, path::PathBuf, time::Duration};

    let mut args = std::env::args_os().skip(1);
    let parent = fs::canonicalize(PathBuf::from(args.next().ok_or("missing scratch-parent")?))?;
    let mode = args
        .next()
        .ok_or("missing production|unmounted|eject|commands mode")?;
    if !["production", "unmounted", "eject", "commands"]
        .iter()
        .any(|valid| mode == *valid)
    {
        return Err("mode must be production, unmounted, eject or commands".into());
    }
    let cycles = args
        .next()
        .map(|value| value.to_string_lossy().parse::<usize>())
        .transpose()?
        .unwrap_or(10);
    if cycles == 0 || args.next().is_some() {
        return Err("expected a positive cycle count and no extra arguments".into());
    }
    let root = parent.join(format!(
        "disk-image-lifetime-{}",
        uuid::Uuid::new_v4().simple()
    ));
    fs::create_dir(&root)?;
    let mount = root.join("mount");
    fs::create_dir(&mount)?;
    // The volumes this run mints are its own, never the production volumes.
    let namespace = TestNamespace::mint()?;
    let runner = SystemCommandRunner::new(HostNamespace::Test(namespace));
    let host = MacOsApfsExecutionHost::new(
        runner,
        ApfsSubstrateConfig::new(HostNamespace::Test(namespace), &root, &mount),
    )?;
    let count = |cycle: usize, phase: &str| -> Result<(), Box<dyn std::error::Error>> {
        let output = runner.run(&CommandRequest::new(
            "/usr/sbin/ioclasscount",
            [
                "AppleDiskImageDevice",
                "DIDeviceIOUserClient",
                "AppleAPFSVolume",
                "IOMedia",
            ],
        ))?;
        if !output.succeeded() {
            return Err(
                format!("ioclasscount: {}", String::from_utf8_lossy(&output.stderr)).into(),
            );
        }
        println!(
            "mode={} cycle={cycle} phase={phase} {}",
            mode.to_string_lossy(),
            String::from_utf8_lossy(&output.stdout).trim()
        );
        Ok(())
    };
    count(0, "before-create")?;
    let request = CreateImageRequest {
        staged_stem: root.join("image"),
        capacity: ImageCapacity::from_gibibytes(1),
        volume_name: "cowshed-lifetime".into(),
        // SAFETY: credential queries take no arguments and cannot fail.
        owner_uid: unsafe { libc::getuid() },
        owner_gid: unsafe { libc::getgid() },
    };
    let image = host.backend().create_staged_image(&request)?;
    let outcome = (|| -> Result<(), Box<dyn std::error::Error>> {
        std::thread::sleep(Duration::from_secs(1));
        count(0, "baseline")?;
        for cycle in 1..=cycles {
            count(cycle, "before")?;
            if mode == "commands" {
                // No I/O Registry walk, raw-device pin, fsck or mount while attached.
                let output = runner.run(&CommandRequest::new(
                    "/usr/sbin/diskutil",
                    [
                        "image".as_ref(),
                        "attach".as_ref(),
                        "--nobrowse".as_ref(),
                        "--noMount".as_ref(),
                        "--plist".as_ref(),
                        image.as_os_str(),
                    ],
                ))?;
                if !output.succeeded() {
                    return Err(
                        format!("attach: {}", String::from_utf8_lossy(&output.stderr)).into(),
                    );
                }
                let plist = plist::Value::from_reader(std::io::Cursor::new(output.stdout))?;
                let device = plist
                    .as_dictionary()
                    .and_then(|root| root.get("system-entities"))
                    .and_then(plist::Value::as_array)
                    .and_then(|entities| entities.first())
                    .and_then(plist::Value::as_dictionary)
                    .and_then(|entity| entity.get("dev-entry"))
                    .and_then(plist::Value::as_string)
                    .ok_or("attach has no whole-device entry")?;
                let output =
                    runner.run(&CommandRequest::new("/usr/bin/hdiutil", ["detach", device]))?;
                if !output.succeeded() {
                    return Err(format!(
                        "detach {device}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )
                    .into());
                }
            } else {
                let attachment = if mode == "unmounted" {
                    host.backend().attach_verified(&image)?
                } else {
                    host.backend()
                        .attach_mounted(&image, &mount, MountAccess::ReadWrite, false)?
                };
                let device = attachment.whole_device().to_owned();
                // Release the raw pin before the deliberately independent eject comparison.
                drop(attachment);
                if mode == "eject" {
                    let output = runner.run(&CommandRequest::new(
                        "/usr/sbin/diskutil",
                        ["eject", &device],
                    ))?;
                    if !output.succeeded() {
                        return Err(format!(
                            "eject {device}: {}",
                            String::from_utf8_lossy(&output.stderr)
                        )
                        .into());
                    }
                } else {
                    host.detach_existing_image(&image, DetachIntent::Release)?;
                }
            }
            count(cycle, "after")?;
            std::thread::sleep(Duration::from_secs(1));
            count(cycle, "settled")?;
            if runner.attached_disk_images()?.iter().any(|attached| matches!(&attached.source, DiskImageSource::File(path) if path == &image)) {
                return Err(format!("{} remains attached after cycle {cycle}", image.display()).into());
            }
        }
        Ok(())
    })();
    if let Err(error) = host.detach_existing_image(&image, DetachIntent::Release) {
        cowshed_os::eprintln!("cleanup failed; retaining {}: {error}", root.display());
        if let Err(primary) = outcome {
            cowshed_os::eprintln!("measurement failed: {primary}");
        }
        return Err(error.into());
    }
    host.backend().delete_image(&image)?;
    fs::remove_dir_all(&root)?;
    outcome
}

#[cfg(not(target_os = "macos"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("disk-image lifetime measurements require macOS".into())
}
