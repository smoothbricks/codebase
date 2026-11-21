//! Linux: the kernel mount table is `/proc/self/mountinfo`.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;

/// NixOS's system profile, then the FHS administrator directories.
pub const TRUSTED_PATH: &str = "/run/current-system/sw/bin:/usr/sbin:/sbin:/usr/bin:/bin";

/// The administrator's own program directory, which no package manager writes.
pub const SYSTEM_DIRECTORY: &str = "/usr/local/libexec/cowshed";

/// The source of the ZFS mount whose device is `directory`'s and whose root is that
/// filesystem's own root.
pub fn zfs_mount_source(directory: &File) -> io::Result<Option<String>> {
    let device = directory.metadata()?.dev();
    let device = format!("{}:{}", libc::major(device), libc::minor(device));
    let table = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(table.lines().find_map(|line| source_of(line, &device)))
}

/// One `mountinfo` line: `<id> <parent> <major:minor> <root> <mountpoint> <options> <optional…>
/// - <fstype> <source> <super options>`.
fn source_of(line: &str, device: &str) -> Option<String> {
    let (mount, filesystem) = line.split_once(" - ")?;
    let mut mount = mount.split(' ');
    let (line_device, root) = (mount.nth(2)?, mount.next()?);
    let mut filesystem = filesystem.split(' ');
    let (fstype, source) = (filesystem.next()?, filesystem.next()?);
    (line_device == device && root == "/" && fstype == "zfs").then(|| source.to_owned())
}

#[cfg(test)]
mod tests {
    use super::source_of;

    #[test]
    fn a_mountinfo_line_names_its_dataset_only_for_a_zfs_root_on_that_device() {
        let zfs = "771 542 0:83 / /home/u/mnt/a rw,noatime shared:1 - zfs z/dev/cowshed/ws/a rw,xattr,noacl";
        assert_eq!(
            source_of(zfs, "0:83").as_deref(),
            Some("z/dev/cowshed/ws/a")
        );
        assert_eq!(source_of(zfs, "0:84"), None);
        let bind = "553 542 0:41 /store /nix/store ro - zfs z/nixos/nix rw";
        assert_eq!(source_of(bind, "0:41"), None);
        let tmpfs = "30 1 0:83 / /tmp rw - tmpfs tmpfs rw";
        assert_eq!(source_of(tmpfs, "0:83"), None);
        assert_eq!(source_of("garbage", "0:83"), None);
    }
}
