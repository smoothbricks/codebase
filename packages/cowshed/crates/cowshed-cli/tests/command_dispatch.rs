#[cfg(target_os = "macos")]
#[path = "command_dispatch/macos.rs"]
mod macos;

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn native_dispatch_refuses_unsupported_host_before_opening_storage() {
    use cowshed_core::ErrorCode;
    use cowshed_core::runtime::ProjectRuntime;
    use cowshed_core::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
    use std::path::PathBuf;

    let storage = ValidatedHostStorage::new(
        PathBuf::from("/tmp"),
        CanonicalRoots::at(PathBuf::from("/tmp/store"), PathBuf::from("/tmp/caches")),
    );
    match ProjectRuntime::open_for_adopt_at("/unused", None, storage).await {
        Err(error) => assert_eq!(error.code, ErrorCode::EnvironmentMissing),
        Ok(runtime) => {
            runtime.shutdown().await.expect("stop unexpected runtime");
            panic!("native dispatch must refuse a host without macOS APFS");
        }
    }
}
