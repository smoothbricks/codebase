//! What `diskutil` says of a volume it just created: its identifier, and the exact attestation
//! of its `info` before provisioning touches it.

use plist::Value;

use super::BootstrapExecutionError;

pub(crate) fn parse_created_apfs_identifier(
    stdout: &[u8],
) -> Result<String, BootstrapExecutionError> {
    let stdout = std::str::from_utf8(stdout).map_err(|_| {
        BootstrapExecutionError::CreatedVolumeOutput(
            "diskutil addVolume output is not UTF-8".to_owned(),
        )
    })?;
    let prefix = "Created new APFS Volume ";
    let identifiers: Vec<_> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .collect();
    if identifiers.len() != 1 {
        return Err(BootstrapExecutionError::CreatedVolumeOutput(format!(
            "expected one created DeviceIdentifier, found {}",
            identifiers.len()
        )));
    }
    let identifier = identifiers[0];
    // `device` owns the `diskN[sM…]` grammar, and the inventory parser this identifier is later
    // compared against uses it. Depth one is an APFS volume: `disk3` is a container and
    // `disk3s1s4` is a snapshot of a slice, neither of which addVolume returns.
    if cowshed_apfs::device::identifier_depth(identifier) != Some(1) {
        return Err(BootstrapExecutionError::CreatedVolumeOutput(format!(
            "malformed created DeviceIdentifier {identifier:?}"
        )));
    }
    Ok(identifier.to_owned())
}

/// Whether a newly created volume is still detached, or was mounted at the
/// default `/Volumes/<name>` by the system before the attestation ran.
///
/// The caller unmounts the latter before mounting the volume at its private
/// mountpoint, so provisioning converges to the same state on either host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreatedMountState {
    Unmounted,
    AutoMounted,
}

pub(crate) fn attest_created_apfs_info(
    bytes: &[u8],
    expected_identifier: &str,
    expected_container: &str,
    expected_name: &str,
) -> Result<CreatedMountState, BootstrapExecutionError> {
    let value = Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| BootstrapExecutionError::CreatedVolumeAttestation(error.to_string()))?;
    let dictionary = value.as_dictionary().ok_or_else(|| {
        BootstrapExecutionError::CreatedVolumeAttestation(
            "diskutil info plist root is not a dictionary".to_owned(),
        )
    })?;
    for (key, expected) in [
        ("DeviceIdentifier", expected_identifier),
        ("APFSContainerReference", expected_container),
        ("VolumeName", expected_name),
        ("FilesystemType", "apfs"),
    ] {
        let actual = dictionary.get(key).and_then(Value::as_string);
        if actual != Some(expected) {
            return Err(BootstrapExecutionError::CreatedVolumeAttestation(format!(
                "{key} is {actual:?}, expected {expected:?}"
            )));
        }
    }
    let mount_state = match dictionary.get("MountPoint") {
        None => CreatedMountState::Unmounted,
        Some(Value::String(value)) if value.is_empty() => CreatedMountState::Unmounted,
        // Some macOS releases mount a freshly created APFS volume at its default
        // location regardless of `-nomount`; both cowshed volumes on a macOS 26
        // host are observed there immediately after `diskutil apfs addVolume`.
        // Only that exact default is tolerated — any other mountpoint means the
        // volume is not the pristine object this attestation is vouching for.
        Some(Value::String(value)) if *value == format!("/Volumes/{expected_name}") => {
            CreatedMountState::AutoMounted
        }
        Some(other) => {
            return Err(BootstrapExecutionError::CreatedVolumeAttestation(format!(
                "new APFS volume is mounted at an unexpected location: {other:?}"
            )));
        }
    };
    if !matches!(dictionary.get("APFSSnapshot"), Some(Value::Boolean(false))) {
        return Err(BootstrapExecutionError::CreatedVolumeAttestation(
            "new APFS object is not an ordinary volume".to_owned(),
        ));
    }
    for role_key in ["APFSVolumeRole", "APFSVolumeRoles", "Roles"] {
        match dictionary.get(role_key) {
            None => {}
            Some(Value::Array(values)) if values.is_empty() => {}
            Some(Value::String(value)) if value.is_empty() => {}
            Some(_) => {
                return Err(BootstrapExecutionError::CreatedVolumeAttestation(format!(
                    "new APFS volume unexpectedly has {role_key} metadata"
                )));
            }
        }
    }
    Ok(mount_state)
}

#[cfg(test)]
mod tests {
    /// `device::identifier_depth` is the one device grammar, and it rejects leading zeros because
    /// the kernel never prints them and these identifiers gate mount and detach decisions by
    /// textual comparison. A volume attested as `disk01s1` would be recorded under a spelling the
    /// inventory parser refuses, so provisioning could never match it to the device again.
    #[test]
    fn created_volume_identifier_shares_the_device_grammar() {
        use super::parse_created_apfs_identifier;

        assert_eq!(
            parse_created_apfs_identifier(b"Created new APFS Volume disk3s5\n").unwrap(),
            "disk3s5"
        );
        for rejected in [
            &b"Created new APFS Volume disk01s1\n"[..],
            b"Created new APFS Volume disk3s1s4\n",
            b"Created new APFS Volume disk3\n",
            b"Created new APFS Volume disk3sx\n",
        ] {
            assert!(
                parse_created_apfs_identifier(rejected).is_err(),
                "{}",
                String::from_utf8_lossy(rejected)
            );
        }
    }
}
