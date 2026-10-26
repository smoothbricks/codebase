//! UUID-keyed credentials for macOS's independent APFSUserAgent discovery path.
use super::{
    APFS_USER_AGENT, BootstrapHost, CS_USER_AGENT, HostCommand, HostError, SECURITY,
    SYSTEM_KEYCHAIN,
};
use cowshed_os::process::ProcessStatus;
use zeroize::Zeroizing;

pub(super) fn ensure_uuid_volume_password(
    host: &dyn BootstrapHost,
    label: &str,
    uuid: &str,
    password: &str,
) -> Result<(), HostError> {
    ensure_uuid_volume_password_in(host, label, uuid, password, SYSTEM_KEYCHAIN)
}

pub(super) fn uuid_volume_password_matches(
    host: &dyn BootstrapHost,
    label: &str,
    uuid: &str,
    password: &str,
) -> Result<bool, HostError> {
    Ok(
        lookup_uuid_volume_password_in(host, label, uuid, SYSTEM_KEYCHAIN)?
            .is_some_and(|stored| stored.as_slice() == password.as_bytes()),
    )
}

fn lookup_uuid_volume_password_in(
    host: &dyn BootstrapHost,
    label: &str,
    uuid: &str,
    keychain: &str,
) -> Result<Option<Zeroizing<Vec<u8>>>, HostError> {
    let output = host.run_command(&HostCommand::new(
        SECURITY,
        [
            "find-generic-password",
            "-a",
            uuid,
            "-s",
            uuid,
            "-w",
            keychain,
        ],
    ))?;
    let mut bytes = Zeroizing::new(output.stdout);
    match output.status {
        ProcessStatus::Exit(44) => Ok(None),
        ProcessStatus::Exit(0) => {
            if bytes.last() == Some(&b'\n') {
                bytes.pop();
                if bytes.last() == Some(&b'\r') {
                    bytes.pop();
                }
            }
            Ok(Some(bytes))
        }
        status => Err(HostError::new(format!(
            "cannot read APFS discovery password for {label} ({uuid}) in {keychain}: {status}"
        ))),
    }
}

pub(super) fn delete_uuid_volume_password(
    host: &dyn BootstrapHost,
    uuid: &str,
) -> Result<(), HostError> {
    delete_uuid_volume_password_in(host, uuid, SYSTEM_KEYCHAIN)
}

fn delete_uuid_volume_password_in(
    host: &dyn BootstrapHost,
    uuid: &str,
    keychain: &str,
) -> Result<(), HostError> {
    let output = host.run_command(&HostCommand::new(
        SECURITY,
        ["delete-generic-password", "-a", uuid, "-s", uuid, keychain],
    ))?;
    match output.status {
        ProcessStatus::Exit(0 | 44) => Ok(()),
        status => Err(HostError::new(format!(
            "cannot remove APFS discovery password for {uuid} in {keychain}: {status}"
        ))),
    }
}

fn ensure_uuid_volume_password_in(
    host: &dyn BootstrapHost,
    label: &str,
    uuid: &str,
    password: &str,
    keychain: &str,
) -> Result<(), HostError> {
    let lookup = || lookup_uuid_volume_password_in(host, label, uuid, keychain);
    if let Some(existing) = lookup()? {
        return if existing.as_slice() == password.as_bytes() {
            Ok(())
        } else {
            Err(HostError::new(format!(
                "APFS discovery password for {label} ({uuid}) in {keychain} differs from the verified boot password; refusing to replace it"
            )))
        };
    }
    let output = host
        .run_command(&HostCommand::new(
            SECURITY,
            [
                "add-generic-password",
                "-a",
                uuid,
                "-s",
                uuid,
                "-l",
                label,
                "-D",
                "Encrypted Volume Password",
                "-w",
                password,
                "-T",
                SECURITY,
                "-T",
                APFS_USER_AGENT,
                "-T",
                CS_USER_AGENT,
                keychain,
            ],
        ))
        .map_err(|mut error| {
            // The host may report an invalid argv. Never include the credential in that error.
            error.message = error.message.replace(password, "<redacted>");
            error
        })?;
    if !output.succeeded() {
        return Err(HostError::new(format!(
            "cannot persist APFS discovery password for {label} ({uuid}) in {keychain}: {}",
            output.status
        )));
    }
    match lookup()? {
        Some(stored) if stored.as_slice() == password.as_bytes() => Ok(()),
        _ => Err(HostError::new(format!(
            "APFS discovery password readback for {label} ({uuid}) in {keychain} did not match; setup is incomplete"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::test_backstop::CommandBackstop as _;
    use security_framework::item::{ItemClass, ItemSearchOptions};
    use security_framework::os::macos::keychain::{CreateOptions, SecKeychain};
    use std::fs;
    use std::process::Command;

    struct FixtureRoot(std::path::PathBuf);

    impl Drop for FixtureRoot {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                cowshed_os::eprintln!(
                    "cannot clean private keychain fixture {}: {error}",
                    self.0.display()
                );
                assert!(
                    std::thread::panicking(),
                    "private keychain fixture cleanup failed"
                );
            }
        }
    }

    #[test]
    fn uuid_volume_password_matches_native_discovery_without_login_item() {
        const CHILD: &str = "COWSHED_UUID_KEYCHAIN_FIXTURE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::bootstrap::native::macos::keychain::tests::uuid_volume_password_matches_native_discovery_without_login_item",
                    "--nocapture", "--test-threads=1",
                ])
                .env(CHILD, "1")
                .output_bounded()
                .unwrap();
            assert!(
                output.status.success(),
                "isolated keychain fixture failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("UUID keychain fixture: absent control, native match, idempotence, mismatch refusal passed"));
            return;
        }
        // Security's StorageManager::shouldAddToSearchList excludes private keychains.
        // Neither path is the actual System keychain or contains /login.keychain.
        let _interaction = SecKeychain::disable_user_interaction().unwrap();
        let root =
            std::env::temp_dir().join(format!("cowshed-uuid-keychain-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let _cleanup = FixtureRoot(root.clone());
        let system_path = root.join("system.keychain");
        let login_path = root.join("empty-user-fixture.keychain");
        let system = CreateOptions::new()
            .password("fixture-keychain-password")
            .create(&system_path)
            .unwrap();
        let login = CreateOptions::new()
            .password("fixture-keychain-password")
            .create(&login_path)
            .unwrap();
        let keychains = [system, login];
        let label = "cowshed.store";
        let uuid = "FEC35F46-22C8-40BC-943A-ADC4BD39CAE5";
        let password = "synthetic-volume-password";
        let host = super::super::SystemBootstrapHost;
        let keychain_path = system_path.to_str().unwrap();
        // The label-keyed boot credential is present, but no remembered-volume password exists.
        let output = host
            .run_command(&HostCommand::new(
                SECURITY,
                [
                    "add-generic-password",
                    "-a",
                    label,
                    "-s",
                    label,
                    "-w",
                    password,
                    "-T",
                    SECURITY,
                    keychain_path,
                ],
            ))
            .unwrap();
        assert!(
            output.succeeded(),
            "cannot persist fixture boot credential: {}",
            output.status
        );
        let query = || {
            ItemSearchOptions::new()
                .keychains(&keychains)
                .class(ItemClass::generic_password())
                .service(uuid)
                .load_attributes(true)
                .search()
        };
        assert_eq!(
            query().unwrap_err().code(),
            security_framework_sys::base::errSecItemNotFound
        );
        ensure_uuid_volume_password_in(&host, label, uuid, password, keychain_path).unwrap();
        assert_eq!(query().unwrap().len(), 1);
        assert_eq!(
            ItemSearchOptions::new()
                .keychains(&keychains)
                .class(ItemClass::generic_password())
                .account(uuid)
                .load_attributes(true)
                .search()
                .unwrap()
                .len(),
            1
        );
        // The login fixture remains empty: only the System-shaped fixture supplies the UUID match.
        assert_eq!(
            ItemSearchOptions::new()
                .keychains(&keychains[1..])
                .class(ItemClass::generic_password())
                .service(uuid)
                .load_attributes(true)
                .search()
                .unwrap_err()
                .code(),
            security_framework_sys::base::errSecItemNotFound
        );
        ensure_uuid_volume_password_in(&host, label, uuid, password, keychain_path).unwrap();
        let error = ensure_uuid_volume_password_in(
            &host,
            label,
            uuid,
            "different-synthetic-password",
            keychain_path,
        )
        .unwrap_err();
        assert!(error.to_string().contains("refusing to replace"));
        assert!(!error.to_string().contains(password));
        assert!(!error.to_string().contains("different-synthetic-password"));
        let metadata = Command::new(SECURITY)
            .args(["dump-keychain", "-a", keychain_path])
            .output_bounded()
            .unwrap();
        assert!(metadata.status.success());
        let metadata = String::from_utf8(metadata.stdout).unwrap();
        assert!(metadata.contains("Encrypted Volume Password"));
        assert!(metadata.contains(APFS_USER_AGENT));
        assert!(metadata.contains(CS_USER_AGENT));
        delete_uuid_volume_password_in(&host, uuid, keychain_path).unwrap();
        assert_eq!(
            query().unwrap_err().code(),
            security_framework_sys::base::errSecItemNotFound
        );
        delete_uuid_volume_password_in(&host, uuid, keychain_path).unwrap();
        drop(keychains);
        println!(
            "UUID keychain fixture: absent control, native match, idempotence, mismatch refusal passed"
        );
    }
}
