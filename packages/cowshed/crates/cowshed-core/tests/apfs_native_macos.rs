#[cfg(target_os = "macos")]
#[path = "apfs_native_macos/macos.rs"]
mod macos;

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
