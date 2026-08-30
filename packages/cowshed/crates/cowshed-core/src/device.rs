//! macOS disk-device identifier grammar — the one place `diskN`/`diskNsM…` strings are parsed.
//!
//! `diskutil` and `hdiutil` name devices `disk<unit>[s<slice>]…` with decimal components the
//! kernel prints without zero-padding, so a leading zero (`disk01`) never names a real device and
//! is rejected everywhere: these identifiers are compared textually before they gate mount and
//! detach decisions, and admitting a second spelling of the same device would let those
//! comparisons disagree about identity.

/// The number of slice components in a bare identifier — `disk3` → 0, `disk3s1` → 1,
/// `disk3s1s4` → 2 (an APFS snapshot of slice 1) — or `None` when the string is not a
/// well-formed identifier. No `/dev/` prefix.
pub(crate) fn identifier_depth(value: &str) -> Option<usize> {
    let tail = value.strip_prefix("disk")?;
    let mut components = tail.split('s');
    let mut depth = 0;
    let unit = components.next()?;
    if !valid_component(unit) {
        return None;
    }
    for slice in components {
        if !valid_component(slice) {
            return None;
        }
        depth += 1;
    }
    Some(depth)
}

fn valid_component(component: &str) -> bool {
    !component.is_empty()
        && component.bytes().all(|byte| byte.is_ascii_digit())
        && (component == "0" || !component.starts_with('0'))
}

/// The `diskN` container prefix of a valid identifier of any depth, or `None` when the string is
/// not a well-formed identifier.
pub(crate) fn container_of(identifier: &str) -> Option<&str> {
    identifier_depth(identifier)?;
    let digits = identifier["disk".len()..]
        .bytes()
        .take_while(u8::is_ascii_digit)
        .count();
    Some(&identifier[.."disk".len() + digits])
}

pub(crate) const DISKUTIL: &str = "/usr/sbin/diskutil";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_counts_slices_and_rejects_malformed_identifiers() {
        assert_eq!(identifier_depth("disk0"), Some(0));
        assert_eq!(identifier_depth("disk12"), Some(0));
        assert_eq!(identifier_depth("disk12s3"), Some(1));
        assert_eq!(identifier_depth("disk3s1s4"), Some(2));
        assert_eq!(identifier_depth("disk0s0"), Some(1));
        for invalid in [
            "",
            "disk",
            "disks1",
            "disk12s",
            "disk12sx",
            "disk01",
            "disk1s01",
            "Disk1",
            "/dev/disk1",
            "disk1 ",
            "rdisk1",
        ] {
            assert_eq!(identifier_depth(invalid), None, "{invalid:?}");
        }
    }

    #[test]
    fn container_is_the_unit_prefix_at_every_depth_and_only_for_valid_identifiers() {
        assert_eq!(container_of("disk3"), Some("disk3"));
        assert_eq!(container_of("disk3s5"), Some("disk3"));
        assert_eq!(container_of("disk13s1"), Some("disk13"));
        assert_eq!(container_of("disk3s1s1"), Some("disk3"));
        for invalid in ["disk", "disk01s1", "disk3sx", "not-a-disk"] {
            assert_eq!(container_of(invalid), None, "{invalid:?}");
        }
    }
}
