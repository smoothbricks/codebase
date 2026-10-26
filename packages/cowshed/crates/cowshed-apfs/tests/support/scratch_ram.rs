//! Exact RAM ownership retained by scratch roots (01_storage.md, "Host namespace";
//! 05_gateway.md, "RAM disks"). The gateway's pending lease records cover interrupted
//! attaches; a receipt published before a fixture keeps a disk covers ownership after keep.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;

use cowshed_apfs::ram_disk::SystemRamDisks;
use cowshed_gateway_types::{RamDiskAttached, RamDiskDevice, RamDiskEntry, RamDisks};
use cowshed_os::namespace::{HostNamespace, TestNamespace};

const RECEIPTS: &str = "fixture-ram-disks";

/// Publish exact ownership before a successful attach can be handed to its fixture.
pub(crate) fn record(namespace: TestNamespace, disk: &RamDiskAttached) -> io::Result<()> {
    // SAFETY: geteuid has no preconditions.
    let root = HostNamespace::Test(namespace).macos_user_root(unsafe { libc::geteuid() });
    let directory = root.join(RECEIPTS);
    super::verify_no_symlinks(&root, &directory).map_err(io::Error::other)?;
    match fs::DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let bytes = serde_json::to_vec(disk).map_err(io::Error::other)?;
    let staged = directory.join(format!(".{}.new", disk.registry_entry));
    fs::write(&staged, bytes)?;
    File::open(&staged)?.sync_all()?;
    fs::rename(&staged, directory.join(disk.registry_entry.to_string()))?;
    File::open(&directory)?.sync_all()
}

/// Settle retained exact ownership and recorded gateway leases. A pending intent
/// without an exact identity preserves the root: size alone cannot authorize a dead
/// namespace sweeper to detach a live sibling's disk.
pub(crate) fn release(namespace: TestNamespace, root: &Path) -> io::Result<()> {
    let disks = SystemRamDisks::new(HostNamespace::Test(namespace));
    let mut owned = BTreeMap::new();
    let mut pending = Vec::new();
    for (name, receipts) in [(RECEIPTS, true), ("ram-leases", false)] {
        let directory = root.join(name);
        super::verify_no_symlinks(root, &directory).map_err(io::Error::other)?;
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                io::Error::other(format!(
                    "RAM ownership path is not UTF-8: {}",
                    path.display()
                ))
            })?;
            // Published lease intents precede gateway staging writes. A retained
            // fixture receipt staging file can itself still prove the exact identity.
            let staged = name.starts_with('.') && name.ends_with(".new");
            if staged && !receipts {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(io::Error::other(format!(
                    "RAM ownership must be a regular file: {}",
                    path.display()
                )));
            }
            let body = fs::read(&path)?;
            if receipts {
                let disk: RamDiskAttached = serde_json::from_slice(&body)
                    .map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?;
                let recorded_name = if staged {
                    name.strip_prefix('.')
                        .and_then(|name| name.strip_suffix(".new"))
                        .filter(|name| !name.is_empty())
                        .ok_or_else(|| {
                            io::Error::other(format!(
                                "malformed RAM receipt staging name: {}",
                                path.display()
                            ))
                        })?
                } else {
                    name
                };
                if recorded_name != disk.registry_entry.to_string() {
                    return Err(io::Error::other(format!(
                        "RAM receipt name disagrees with its entry: {}",
                        path.display()
                    )));
                }
                owned.insert(disk.registry_entry.get(), disk);
            } else {
                let body = std::str::from_utf8(&body).map_err(io::Error::other)?;
                if name.ends_with(".intent") {
                    pending.push(path);
                } else {
                    let disk = RamDiskAttached {
                        device: RamDiskDevice::parse(body).map_err(io::Error::other)?,
                        registry_entry: RamDiskEntry::parse(name).map_err(io::Error::other)?,
                    };
                    owned.insert(disk.registry_entry.get(), disk);
                }
            }
        }
    }
    for disk in owned.into_values() {
        if !disks.registered(disk.registry_entry)? {
            continue;
        }
        let outcome = disks.detach(&disk.device, disk.registry_entry, 64 * 1024);
        cowshed_os::eprintln!(
            "scratch RAM {} entry={}: {outcome}",
            disk.device,
            disk.registry_entry
        );
        if disks.registered(disk.registry_entry)? {
            return Err(io::Error::other(format!(
                "scratch root {} retains RAM {} entry={} after {outcome}",
                root.display(),
                disk.device,
                disk.registry_entry
            )));
        }
    }
    if pending.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "scratch root {} retains RAM intents without exact registry identities: {pending:?}",
            root.display()
        )))
    }
}
