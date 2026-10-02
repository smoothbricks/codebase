#[cfg(target_os = "macos")]
#[path = "support/scratch_apfs.rs"]
mod scratch_apfs;

#[cfg(target_os = "macos")]
#[tokio::test]
async fn real_apfs_inventory_survives_concurrent_image_teardown() {
    use cowshed_core::apfs::{
        ApfsBackend, CreateImageRequest, MacOsApfsBackend, SystemCommandRunner,
    };
    use cowshed_core::metadata::ImageCapacity;
    use cowshed_core::storage::bootstrap::native::validate_existing_host_storage;
    use std::process::Command;

    let root = scratch_apfs::ScratchRoot::new("inventory-teardown").expect("scratch root");
    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    let image = backend
        .create_staged_image(&CreateImageRequest {
            staged_stem: root.path().join("transient"),
            capacity: ImageCapacity::from_gibibytes(1),
            volume_name: "cowshed-inventory-transient".to_owned(),
            // SAFETY: getuid/getgid only read this process's credentials.
            owner_uid: unsafe { libc::getuid() },
            owner_gid: unsafe { libc::getgid() },
        })
        .expect("create a real case-sensitive ASIF image");

    // Validation crosses SystemEvidenceSource, not a canned command runner. A live image's
    // attachment changes diskutil's global inventory while the home container stays put.
    let home = std::env::home_dir().expect("home directory");
    for _ in 0..8 {
        let attached = Command::new("/usr/sbin/diskutil")
            .args(["image", "attach", "--noMount", "--plist"])
            .arg(&image)
            .output()
            .expect("attach real image");
        assert!(
            attached.status.success(),
            "diskutil image attach: {}",
            String::from_utf8_lossy(&attached.stderr)
        );
        let device = plist::Value::from_reader(std::io::Cursor::new(&attached.stdout))
            .expect("attach plist")
            .as_dictionary()
            .and_then(|dict| dict.get("system-entities"))
            .and_then(plist::Value::as_array)
            .and_then(|entities| {
                entities
                    .iter()
                    .filter_map(|entity| entity.as_dictionary()?.get("dev-entry")?.as_string())
                    .find(|entry| {
                        entry
                            .strip_prefix("/dev/")
                            .unwrap_or(entry)
                            .strip_prefix("disk")
                            .is_some_and(|digits| {
                                !digits.is_empty()
                                    && digits.bytes().all(|byte| byte.is_ascii_digit())
                            })
                    })
            })
            .expect("whole attached disk")
            .to_owned();
        let detach = std::thread::spawn(move || {
            let output = Command::new("/usr/sbin/diskutil")
                .args(["eject", &device])
                .output()
                .expect("eject real image");
            assert!(
                output.status.success(),
                "diskutil eject: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        });
        let validation = validate_existing_host_storage(&home).await;
        detach.join().expect("detach worker");
        validation.expect("existing home storage must survive unrelated APFS teardown");
    }
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn real_apfs_inventory_refuses_an_unsupported_host() {
    use cowshed_core::ErrorCode;
    use cowshed_core::storage::bootstrap::native::validate_existing_host_storage;

    let error = validate_existing_host_storage(std::path::Path::new("/"))
        .await
        .expect_err("non-macOS host cannot claim an APFS inventory");
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert!(error.message.contains("unsupported"));
}
