//! An image file's physical layout: how many extents its data occupies, and a rewrite that lays
//! the same bytes out again contiguously.
//!
//! Extents are the whole cost of cloning an image (01_storage.md, "Clone cost follows extents,
//! not size"): `clonefile` shares the source's extent map and the first write to either file
//! copies it. A main image fragments with every write it takes while clones share its blocks, so
//! the one remedy is to write its data again into fresh, contiguous space.
//!
//! The rewrite is a plain `pread`/`pwrite` copy. `clonefile`, `copyfile(3)`, and
//! `std::fs::copy` all clone on APFS, and a clone is exactly the shared extent map this exists to
//! get rid of.

use std::fs::{self, File, OpenOptions, Permissions};
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::ApfsStorageError;
use crate::apfs::SECTOR_BYTES;
use crate::storage::lifecycle::ExtentCount;

/// The rewrite's read/write unit: large enough that the allocator lays each write out as one run,
/// small enough to stay one reusable buffer.
const COPY_CHUNK_BYTES: u64 = 8 << 20;

/// Count the physically contiguous runs the file's data occupies, one `F_LOG2PHYS_EXT` query per
/// run, skipping holes.
pub fn count_extents(path: &Path) -> Result<ExtentCount, ApfsStorageError> {
    let file =
        File::open(path).map_err(|error| io_error("open image to count extents", path, error))?;
    let length = file
        .metadata()
        .map_err(|error| io_error("inspect image to count extents", path, error))?
        .len();
    let mut extents = 0_u64;
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(&file, offset, length)
        .map_err(|error| io_error("find image data to count extents", path, error))?
    {
        let mut position = start;
        while position < end {
            position += physical_run(&file, position, end - position)
                .map_err(|error| io_error("map image extents", path, error))?;
            extents += 1;
        }
        offset = end;
    }
    Ok(ExtentCount::new(extents))
}

/// Where a rewrite stages its copy: beside the image, so publishing it is a same-volume rename,
/// and named `<image>.defrag` so no enumeration of images or sidecars ever reads it as either.
pub fn rewrite_sibling(image: &Path) -> PathBuf {
    let mut name = image.as_os_str().to_owned();
    name.push(".defrag");
    PathBuf::from(name)
}

/// Refuse a rewrite the image's volume has no room for, before anything is detached.
///
/// The copy needs as many free bytes as the image has allocated, and keeps them: clones and
/// checkpoints still share the old blocks, so the space is not given back when the copy replaces
/// the image.
pub fn require_room_for_rewrite(image: &Path) -> Result<u64, ApfsStorageError> {
    let needed = allocated_bytes(image)?;
    let parent = image
        .parent()
        .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
    let available = available_bytes(parent)?;
    if needed > available {
        return Err(ApfsStorageError::InsufficientSpace {
            path: image.to_owned(),
            needed,
            available,
        });
    }
    Ok(needed)
}

/// Copy the image's data into a fresh sibling, holes preserved, and rename it over the image.
///
/// The caller holds the workspace's lifecycle lock and has detached the image. Returns the data
/// bytes copied. A failure before the rename leaves the image untouched and removes the copy.
pub fn rewrite_contiguously(image: &Path) -> Result<u64, ApfsStorageError> {
    let source =
        File::open(image).map_err(|error| io_error("open image to rewrite", image, error))?;
    let metadata = source
        .metadata()
        .map_err(|error| io_error("inspect image to rewrite", image, error))?;
    let sibling = rewrite_sibling(image);
    // Only an interrupted rewrite leaves a sibling behind, and nothing else can be writing one
    // while the caller holds the lock: it is garbage, and the new copy takes its name.
    remove_if_present(&sibling)?;
    let destination = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&sibling)
        .map_err(|error| io_error("create the rewrite copy", &sibling, error))?;
    let copied =
        copy_data(&source, &destination, metadata.len(), image, &sibling).and_then(|bytes| {
            destination
                .set_permissions(Permissions::from_mode(metadata.mode() & 0o7777))
                .map_err(|error| {
                    io_error("copy the image's mode onto the rewrite", &sibling, error)
                })?;
            destination
                .sync_all()
                .map_err(|error| io_error("flush the rewrite copy", &sibling, error))?;
            Ok(bytes)
        });
    drop(destination);
    let bytes = match copied {
        Ok(bytes) => bytes,
        Err(primary) => {
            return Err(match fs::remove_file(&sibling) {
                Ok(()) => primary,
                Err(cleanup) => ApfsStorageError::Cleanup {
                    operation: "remove the copy a failed rewrite left behind",
                    primary: Box::new(primary),
                    cleanup: Box::new(io_error("remove the rewrite copy", &sibling, cleanup)),
                },
            });
        }
    };
    fs::rename(&sibling, image)
        .map_err(|error| io_error("publish the rewritten image", image, error))?;
    let parent = image
        .parent()
        .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync the rewritten image's directory", parent, error))?;
    Ok(bytes)
}

fn copy_data(
    source: &File,
    destination: &File,
    length: u64,
    image: &Path,
    sibling: &Path,
) -> Result<u64, ApfsStorageError> {
    // The copy passes each byte through once; caching it would only evict what the rest of the
    // machine is using, hundreds of gigabytes' worth for a large main.
    bypass_cache(source).map_err(|error| io_error("bypass the cache reading", image, error))?;
    bypass_cache(destination)
        .map_err(|error| io_error("bypass the cache writing", sibling, error))?;
    let mut buffer = vec![0_u8; buffer_len(COPY_CHUNK_BYTES)?];
    let mut copied = 0_u64;
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(source, offset, length)
        .map_err(|error| io_error("find image data to rewrite", image, error))?
    {
        let mut position = start;
        while position < end {
            let step = (end - position).min(COPY_CHUNK_BYTES);
            let chunk = &mut buffer[..buffer_len(step)?];
            source
                .read_exact_at(chunk, position)
                .map_err(|error| io_error("read image data", image, error))?;
            destination
                .write_all_at(chunk, position)
                .map_err(|error| io_error("write the rewrite copy", sibling, error))?;
            position += step;
        }
        // APFS can zero-fill the hole a write skips over (the boot Data volume does), which would
        // turn every hole the image had into allocated zeros: punch it back.
        if start > offset {
            punch_hole(destination, offset, start - offset).map_err(|error| {
                io_error("keep the image's hole in the rewrite copy", sibling, error)
            })?;
        }
        copied += end - start;
        offset = end;
    }
    // Extending the length past the last data region leaves the tail a hole.
    destination
        .set_len(length)
        .map_err(|error| io_error("size the rewrite copy", sibling, error))?;
    Ok(copied)
}

fn buffer_len(bytes: u64) -> Result<usize, ApfsStorageError> {
    usize::try_from(bytes)
        .map_err(|_| ApfsStorageError::InvalidPlan("rewrite chunk exceeds the address space"))
}

fn remove_if_present(path: &Path) -> Result<(), ApfsStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(
            "remove an interrupted rewrite's copy",
            path,
            error,
        )),
    }
}

fn allocated_bytes(image: &Path) -> Result<u64, ApfsStorageError> {
    fs::metadata(image)
        .map(|metadata| metadata.blocks().saturating_mul(SECTOR_BYTES))
        .map_err(|error| io_error("inspect image allocation", image, error))
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> ApfsStorageError {
    ApfsStorageError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

/// The next `[start, end)` run of data at or after `offset`, or `None` when only a hole remains.
#[cfg(target_os = "macos")]
fn next_data_region(file: &File, offset: u64, length: u64) -> io::Result<Option<(u64, u64)>> {
    use std::os::fd::AsRawFd;

    if offset >= length {
        return Ok(None);
    }
    let seek = |from: u64, whence| -> io::Result<Option<u64>> {
        let from = libc::off_t::try_from(from).map_err(io::Error::other)?;
        // SAFETY: the descriptor is live for the borrow of `file`; `lseek` only moves its offset,
        // which nothing here reads (every transfer is positional).
        let found = unsafe { libc::lseek(file.as_raw_fd(), from, whence) };
        if found < 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ENXIO) => Ok(None),
                _ => Err(error),
            };
        }
        u64::try_from(found).map(Some).map_err(io::Error::other)
    };
    let Some(start) = seek(offset, libc::SEEK_DATA)? else {
        return Ok(None);
    };
    let end = seek(start, libc::SEEK_HOLE)?.unwrap_or(length);
    Ok(Some((start, end.min(length))))
}

/// How many bytes from `offset` on sit in one physically contiguous run, at most `length`.
#[cfg(target_os = "macos")]
fn physical_run(file: &File, offset: u64, length: u64) -> io::Result<u64> {
    use std::os::fd::AsRawFd;

    let mut query = libc::log2phys {
        l2p_flags: 0,
        l2p_contigbytes: libc::off_t::try_from(length).map_err(io::Error::other)?,
        l2p_devoffset: libc::off_t::try_from(offset).map_err(io::Error::other)?,
    };
    // SAFETY: `query` is a live, writable `log2phys` for the duration of the call, which is the
    // argument `F_LOG2PHYS_EXT` reads the file range from and writes the device run into.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_LOG2PHYS_EXT, &raw mut query) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let contiguous = query.l2p_contigbytes;
    match u64::try_from(contiguous) {
        Ok(0) | Err(_) => Err(io::Error::other(format!(
            "F_LOG2PHYS_EXT reported {contiguous} contiguous bytes at offset {offset}"
        ))),
        Ok(contiguous) => Ok(contiguous.min(length)),
    }
}

#[cfg(target_os = "macos")]
fn bypass_cache(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `F_NOCACHE` takes an int flag and only changes the descriptor's caching policy.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Deallocate `[offset, offset + length)`, leaving a hole that reads as zeros.
#[cfg(target_os = "macos")]
fn punch_hole(file: &File, offset: u64, length: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let mut hole = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: libc::off_t::try_from(offset).map_err(io::Error::other)?,
        fp_length: libc::off_t::try_from(length).map_err(io::Error::other)?,
    };
    // SAFETY: `hole` is a live `fpunchhole_t` for the duration of the call; `F_PUNCHHOLE` reads
    // the range from it and changes nothing but the file's allocation.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &raw mut hole) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn available_bytes(directory: &Path) -> Result<u64, ApfsStorageError> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
        ApfsStorageError::Host(format!("path contains NUL: {}", directory.display()))
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` is writable storage `statfs` fills on success.
    if unsafe { libc::statfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io_error(
            "read free space",
            directory,
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: `statfs` returned 0, so it initialized every field.
    let stats = unsafe { stats.assume_init() };
    Ok(stats.f_bavail.saturating_mul(u64::from(stats.f_bsize)))
}

#[cfg(not(target_os = "macos"))]
fn next_data_region(_: &File, _: u64, _: u64) -> io::Result<Option<(u64, u64)>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image extents are read with macOS F_LOG2PHYS_EXT",
    ))
}

#[cfg(not(target_os = "macos"))]
fn physical_run(_: &File, _: u64, _: u64) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image extents are read with macOS F_LOG2PHYS_EXT",
    ))
}

#[cfg(not(target_os = "macos"))]
fn bypass_cache(_: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image rewrites bypass the cache with macOS F_NOCACHE",
    ))
}

#[cfg(not(target_os = "macos"))]
fn punch_hole(_: &File, _: u64, _: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image holes are punched with macOS F_PUNCHHOLE",
    ))
}

#[cfg(not(target_os = "macos"))]
fn available_bytes(directory: &Path) -> Result<u64, ApfsStorageError> {
    Err(io_error(
        "read free space",
        directory,
        io::Error::new(
            io::ErrorKind::Unsupported,
            "free space is read with macOS statfs",
        ),
    ))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use std::fs::{self, File, OpenOptions, Permissions};
    use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    use super::{
        count_extents, next_data_region, punch_hole, rewrite_contiguously, rewrite_sibling,
    };

    const MIB: u64 = 1024 * 1024;
    const PAGE: u64 = 16 * 1024;

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn scratch(test: &str) -> Scratch {
        let root =
            std::env::temp_dir().join(format!("cowshed-extents-{test}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("scratch root");
        Scratch(root)
    }

    /// The bytes at `offset` in the fixture: every page distinct, so a misplaced page is caught.
    fn pattern(offset: u64, length: u64) -> Vec<u8> {
        (offset..offset + length)
            .map(|position| u8::try_from((position / PAGE + position) % 251).expect("byte"))
            .collect()
    }

    fn data_regions(path: &Path) -> Vec<(u64, u64)> {
        let file = File::open(path).expect("open");
        let length = file.metadata().expect("stat").len();
        let mut regions = Vec::new();
        let mut offset = 0;
        while let Some((start, end)) = next_data_region(&file, offset, length).expect("regions") {
            regions.push((start, end));
            offset = end;
        }
        regions
    }

    /// Two data regions around a hole and a trailing hole — the shape of a sparse disk image —
    /// with every other page of the data rewritten while a clone shares it.
    fn fragmented_sparse_file(root: &Path) -> (PathBuf, Vec<(u64, u64)>) {
        let path = root.join("image.asif");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .expect("create");
        file.write_all_at(&pattern(0, 20 * MIB), 0)
            .expect("write data");
        // Holes punched, not skipped over: APFS may zero-fill a hole a write skips.
        punch_hole(&file, 4 * MIB, 8 * MIB).expect("interior hole");
        punch_hole(&file, 16 * MIB, 4 * MIB).expect("trailing hole");
        file.sync_all().expect("flush");
        let regions = [(0, 4 * MIB), (12 * MIB, 16 * MIB)];
        let holder = root.join("holder");
        assert!(
            std::process::Command::new("/bin/cp")
                .arg("-c")
                .arg(&path)
                .arg(&holder)
                .status()
                .expect("cp -c")
                .success()
        );
        let mut page = vec![0_u8; 16 * 1024];
        for (start, end) in regions {
            let mut offset = start;
            while offset + PAGE <= end {
                file.read_exact_at(&mut page, offset).expect("read page");
                file.write_all_at(&page, offset).expect("rewrite page");
                offset += 2 * PAGE;
            }
        }
        file.sync_all().expect("flush fragments");
        fs::remove_file(holder).expect("drop the clone");
        fs::set_permissions(&path, Permissions::from_mode(0o640)).expect("mode");
        (path, regions.to_vec())
    }

    #[test]
    fn rewrite_keeps_every_byte_hole_and_mode_and_collapses_the_extents() {
        let root = scratch("rewrite");
        let (image, regions) = fragmented_sparse_file(&root.0);
        let fragmented = count_extents(&image).expect("count fragmented");
        // 256 pages rewritten apart from their neighbours: each rewritten page and each page it
        // left behind is its own run.
        assert!(
            fragmented.get() >= 256,
            "the fixture did not fragment: {fragmented} extents"
        );
        let regions_before = data_regions(&image);
        assert_eq!(regions_before, regions);
        // A copy an interrupted rewrite left behind is garbage the next rewrite replaces.
        fs::write(rewrite_sibling(&image), b"stale").expect("stale copy");

        let copied = rewrite_contiguously(&image).expect("rewrite");

        assert_eq!(copied, 8 * MIB, "only the data is copied, never the holes");
        let rewritten = count_extents(&image).expect("count rewritten");
        assert!(
            rewritten.get() * 10 <= fragmented.get(),
            "{fragmented} extents only fell to {rewritten}"
        );
        assert_eq!(data_regions(&image), regions_before, "holes stay holes");
        let metadata = fs::metadata(&image).expect("stat");
        assert_eq!(metadata.len(), 20 * MIB);
        assert_eq!(metadata.mode() & 0o7777, 0o640);
        let file = File::open(&image).expect("open");
        for (start, end) in regions {
            let mut data = vec![0_u8; usize::try_from(end - start).expect("length")];
            file.read_exact_at(&mut data, start).expect("read");
            assert!(
                data == pattern(start, end - start),
                "data at {start} changed"
            );
        }
        assert!(
            !rewrite_sibling(&image).exists(),
            "the copy became the image"
        );
    }

    #[test]
    fn count_skips_holes_and_a_file_of_holes_has_no_extents() {
        let root = scratch("holes");
        let path = root.0.join("holes");
        let file = File::create(&path).expect("create");
        file.set_len(8 * MIB).expect("hole");
        assert_eq!(count_extents(&path).expect("count").get(), 0);
        file.write_all_at(&pattern(4 * MIB, MIB), 4 * MIB)
            .expect("write");
        file.sync_all().expect("flush");
        assert!(count_extents(&path).expect("count").get() >= 1);
    }
}
