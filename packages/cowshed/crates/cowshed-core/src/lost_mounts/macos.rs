//! macOS's mount table and its change events: `getfsstat`, and the kqueue `EVFILT_FS` filter
//! every unmount signals (`cowshed-apfs`'s `mounts`).

pub(super) use cowshed_apfs::mounts::{MountTableEvents, kernel_mounts, vanished};
