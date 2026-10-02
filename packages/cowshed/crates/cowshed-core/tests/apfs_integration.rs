#[cfg(target_os = "macos")]
#[path = "apfs_integration/macos.rs"]
mod macos;

#[cfg(not(target_os = "macos"))]
#[test]
fn native_apfs_clone_refuses_a_host_without_clonefile() {
    use cowshed_core::apfs::{ApfsBackend, CloneFileError, MacOsApfsBackend, SystemCommandRunner};
    use std::path::Path;

    let backend = MacOsApfsBackend::new(SystemCommandRunner);
    assert!(matches!(
        backend.clone_image(Path::new("/tmp/source.asif"), Path::new("/tmp/clone.asif")),
        Err(CloneFileError::UnsupportedPlatform)
    ));
}
