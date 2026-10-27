#[cfg(target_os = "macos")]
#[path = "apfs_integration/macos.rs"]
mod macos;

#[cfg(not(target_os = "macos"))]
#[test]
fn native_apfs_clone_refuses_a_host_without_clonefile() {
    use cowshed_apfs::{CloneFileError, MacOsApfsBackend, SystemCommandRunner};
    use cowshed_os::namespace::{HostNamespace, TestNamespace};
    use std::path::Path;

    let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
    let backend = MacOsApfsBackend::new(SystemCommandRunner::new(namespace));
    assert!(matches!(
        backend
            .prepare_clone(
                Path::new("/tmp/source.dmg"),
                Path::new("/tmp/clone.dmg"),
                cowshed_apfs::ImageCapture::Detached,
            )
            .and_then(|admitted| admitted.clone_image()),
        Err(CloneFileError::UnsupportedPlatform)
    ));
}
