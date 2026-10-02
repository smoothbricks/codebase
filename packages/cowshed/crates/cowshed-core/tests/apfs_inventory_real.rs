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
