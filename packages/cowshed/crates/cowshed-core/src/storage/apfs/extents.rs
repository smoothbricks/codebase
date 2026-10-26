//! An image file's physical layout: how many extents its data occupies, and a rewrite that lays
//! the same bytes out again contiguously.
//!
//! Extents govern the first-write map-copy cost (01a_apfs.md, "Clone cost follows extents,
//! not size"): `clonefile` shares the source's extent map and the first write to either file
//! copies it. Attach, mount, identity work and other images have separate costs.
//!
//! A main image fragments with every write it takes while clones share its blocks, so
//! the one remedy is to write its data again into fresh, contiguous space.
//!
//! The rewrite is a plain `pread`/`pwrite` copy. `clonefile`, `copyfile(3)`, and
//! `std::fs::copy` all clone on APFS, and a clone is exactly the shared extent map this exists to
//! get rid of.

use std::fs::{self, File, OpenOptions, Permissions};
use std::io;
use std::ops::ControlFlow;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::ApfsStorageError;
#[cfg(target_os = "macos")]
use crate::copy::native::bypass_cache;
use crate::copy::native::next_data_region;
use crate::storage::lifecycle::ExtentCount;
use cowshed_apfs::SECTOR_BYTES;

/// The rewrite's read/write unit: large enough that the allocator lays each write out as one run,
/// small enough to stay one reusable buffer.
const COPY_CHUNK_BYTES: u64 = 8 << 20;

/// Count the physically contiguous runs the file's data occupies, one `F_LOG2PHYS_EXT` query per
/// run, skipping holes.
pub fn count_extents(path: &Path) -> Result<ExtentCount, ApfsStorageError> {
    Ok(measure(path, None)?.extents)
}

/// Count the file's runs up to `stop_at`, never past it: whether an image is past a bound is
/// decided without walking a map of millions.
pub fn count_extents_to(path: &Path, stop_at: u64) -> Result<ExtentCount, ApfsStorageError> {
    Ok(measure(path, Some(stop_at))?.extents)
}

/// A disposable witness of this file's physical write generation, never a Git tree or HEAD.
/// No second registry is kept: callers retain this value only across the operation it proves.
#[derive(Clone, Debug)]
pub struct ExtentProof {
    generation: ImageGeneration,
    extents: ExtentCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ImageGeneration {
    device: u64,
    inode: u64,
    length: u64,
    blocks: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

impl ImageGeneration {
    fn of(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
            blocks: metadata.blocks(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

impl ExtentProof {
    /// The runs the proof read, at most where its walk stopped.
    pub fn extents(&self) -> ExtentCount {
        self.extents
    }

    pub fn revalidate(&self, image: &Path) -> Result<(), ApfsStorageError> {
        let metadata = fs::metadata(image)
            .map_err(|error| io_error("revalidate image extent generation", image, error))?;
        if self.generation != ImageGeneration::of(&metadata) {
            return Err(ApfsStorageError::Host(format!(
                "image {} changed physical generation during extent proof; fork readiness is unknown, stop its writers and retry",
                image.display()
            )));
        }
        Ok(())
    }
}

/// Refuse unknown or changing physical generations before publishing a forkable image, and warn
/// about an oversized one: past the standalone first-write sensitivity bound the fork still
/// proceeds, paying its first write, and the warning names that cost and the defrag remedy. The
/// walk stops at 7141, not at the end of an arbitrarily fragmented image, so the count and cost
/// it reports are lower bounds.
pub fn prove_forkable(image: &Path) -> Result<ExtentProof, ApfsStorageError> {
    let proof = measure(image, Some(ExtentCount::FORKABLE_MAX.get() + 1))?;
    if !proof.extents.is_forkable() {
        cowshed_os::eprintln!(
            "cowshed: warning: image {} has at least {} extents, above the {} standalone first-write sensitivity bound; its first write costs at least {:?} at the observed 140.046964 us/extent (cause unattributed); cowshed defrag <workspace> restores fast forks",
            image.display(),
            proof.extents,
            ExtentCount::FORKABLE_MAX,
            proof.extents.observed_first_write_cost()
        );
    }
    Ok(proof)
}

fn measure(path: &Path, stop_at: Option<u64>) -> Result<ExtentProof, ApfsStorageError> {
    let file =
        File::open(path).map_err(|error| io_error("open image to count extents", path, error))?;
    // Bytes written to the file but not yet placed have no device offset (delayed allocation),
    // which F_LOG2PHYS_EXT reports as -1. fsync(2) places them without F_FULLFSYNC's drive-cache
    // flush. It runs before the generation is read, since placing data changes the block count.
    // SAFETY: fsync takes an open descriptor, which `file` owns for the duration of the call.
    if unsafe { libc::fsync(std::os::fd::AsRawFd::as_raw_fd(&file)) } == -1 {
        return Err(io_error(
            "place written image data before counting extents",
            path,
            io::Error::last_os_error(),
        ));
    }
    let generation = ImageGeneration::of(
        &file
            .metadata()
            .map_err(|error| io_error("inspect image to count extents", path, error))?,
    );
    let mut extents = 0_u64;
    for_each_run(&file, generation.length, |_| {
        extents += 1;
        if stop_at.is_some_and(|limit| extents >= limit) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .map_err(|error| io_error("map image extents", path, error))?;
    let proof = ExtentProof {
        generation,
        extents: ExtentCount::new(extents),
    };
    proof.revalidate(path)?;
    Ok(proof)
}

/// How many of `clone`'s data bytes sit on the same device blocks as the same offsets of `source`:
/// the extents a `clonefile` of `source` still shares, which a copy never does (01a_apfs.md,
/// "Clone cost follows extents, not size"). A structural witness of a clone, read from the kernel,
/// so it holds however long the clone took on a loaded host.
pub fn shared_bytes(source: &Path, clone: &Path) -> Result<u64, ApfsStorageError> {
    let open = |path: &Path| {
        let file = File::open(path)
            .map_err(|error| io_error("open image to compare extents", path, error))?;
        let length = file
            .metadata()
            .map_err(|error| io_error("inspect image to compare extents", path, error))?
            .len();
        Ok::<_, ApfsStorageError>((file, length))
    };
    let (source_file, source_length) = open(source)?;
    let (clone_file, clone_length) = open(clone)?;
    let length = source_length.min(clone_length);
    let mut shared = 0_u64;
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(&clone_file, offset, length)
        .map_err(|error| io_error("find clone data to compare extents", clone, error))?
    {
        let mut position = start;
        while position < end {
            let theirs = physical_run(&clone_file, position, end - position)
                .map_err(|error| io_error("map clone extents", clone, error))?;
            let Some((source_start, source_end)) = next_data_region(&source_file, position, length)
                .map_err(|error| io_error("find source data to compare extents", source, error))?
            else {
                return Ok(shared);
            };
            let step = if source_start > position {
                // A hole in the source here: nothing it could share.
                theirs.bytes.min(source_start - position)
            } else {
                let ours = physical_run(
                    &source_file,
                    position,
                    theirs.bytes.min(source_end - position),
                )
                .map_err(|error| io_error("map source extents", source, error))?;
                if ours.device == theirs.device {
                    shared += ours.bytes;
                }
                ours.bytes
            };
            position += step;
        }
        offset = end;
    }
    Ok(shared)
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

/// What a copy that may be stopped did ([`copy_contiguously`]).
#[derive(Debug, Eq, PartialEq)]
pub enum Copied<Stop> {
    /// Every data byte is in the destination, its mode copied and flushed.
    Complete { bytes: u64 },
    /// `keep_going` broke with `why` once `bytes` were copied; the destination is removed.
    Stopped { bytes: u64, why: Stop },
}

/// Copy the image's data into a fresh sibling, holes preserved, and rename it over the image.
///
/// The caller holds the workspace's lifecycle lock and has detached the image. Returns the data
/// bytes copied. A failure before the rename leaves the image untouched and removes the copy.
pub fn rewrite_contiguously(image: &Path) -> Result<u64, ApfsStorageError> {
    let sibling = rewrite_sibling(image);
    // Only an interrupted rewrite leaves a sibling behind, and nothing else can be writing one
    // while the caller holds the lock: it is garbage, and the new copy takes its name.
    remove_if_present(&sibling)?;
    let bytes = match copy_contiguously(image, &sibling, |_| {
        ControlFlow::<std::convert::Infallible>::Continue(())
    })? {
        Copied::Complete { bytes } => bytes,
        Copied::Stopped { why, .. } => match why {},
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

/// Copy `source`'s data into a new file at `destination` with plain `pread`/`pwrite`, holes
/// preserved, its mode copied, flushed: fresh, contiguous space that shares no extent with
/// `source`. `keep_going` is asked after every chunk with the bytes copied so far; a break stops
/// the copy. A stop or a failure removes the destination, so it exists only complete.
pub fn copy_contiguously<Stop>(
    source: &Path,
    destination: &Path,
    keep_going: impl FnMut(u64) -> ControlFlow<Stop>,
) -> Result<Copied<Stop>, ApfsStorageError> {
    let reader =
        File::open(source).map_err(|error| io_error("open the image to copy", source, error))?;
    let metadata = reader
        .metadata()
        .map_err(|error| io_error("inspect the image to copy", source, error))?;
    let writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(destination)
        .map_err(|error| io_error("create the contiguous copy", destination, error))?;
    let copied = copy_data(
        &reader,
        &writer,
        metadata.len(),
        source,
        destination,
        keep_going,
    )
    .and_then(|copied| {
        if let Copied::Complete { .. } = copied {
            writer
                .set_permissions(Permissions::from_mode(metadata.mode() & 0o7777))
                .map_err(|error| {
                    io_error("copy the image's mode onto the copy", destination, error)
                })?;
            writer
                .sync_all()
                .map_err(|error| io_error("flush the contiguous copy", destination, error))?;
        }
        Ok(copied)
    });
    drop(writer);
    match copied {
        Ok(complete @ Copied::Complete { .. }) => Ok(complete),
        Ok(stopped @ Copied::Stopped { .. }) => fs::remove_file(destination)
            .map(|()| stopped)
            .map_err(|error| io_error("remove the copy a stop left behind", destination, error)),
        Err(primary) => Err(match fs::remove_file(destination) {
            Ok(()) => primary,
            Err(cleanup) => ApfsStorageError::Cleanup {
                operation: "remove the copy a failed copy left behind",
                primary: Box::new(primary),
                cleanup: Box::new(io_error("remove the contiguous copy", destination, cleanup)),
            },
        }),
    }
}

fn copy_data<Stop>(
    source: &File,
    destination: &File,
    length: u64,
    image: &Path,
    copy: &Path,
    mut keep_going: impl FnMut(u64) -> ControlFlow<Stop>,
) -> Result<Copied<Stop>, ApfsStorageError> {
    // The copy passes each byte through once; caching it would only evict what the rest of the
    // machine is using, hundreds of gigabytes' worth for a large main.
    bypass_cache(source).map_err(|error| io_error("bypass the cache reading", image, error))?;
    bypass_cache(destination).map_err(|error| io_error("bypass the cache writing", copy, error))?;
    let mut buffer = vec![0_u8; buffer_len(COPY_CHUNK_BYTES)?];
    let mut copied = 0_u64;
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(source, offset, length)
        .map_err(|error| io_error("find image data to copy", image, error))?
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
                .map_err(|error| io_error("write the contiguous copy", copy, error))?;
            position += step;
            if let ControlFlow::Break(why) = keep_going(copied + (position - start)) {
                return Ok(Copied::Stopped {
                    bytes: copied + (position - start),
                    why,
                });
            }
        }
        // APFS can zero-fill the hole a write skips over (the boot Data volume does), which would
        // turn every hole the image had into allocated zeros: punch it back.
        if start > offset {
            punch_hole(destination, offset, start - offset).map_err(|error| {
                io_error("keep the image's hole in the contiguous copy", copy, error)
            })?;
        }
        copied += end - start;
        offset = end;
    }
    // Sizing an APFS file can materialize zero-filled blocks after its last data run. The
    // interior holes were punched after the following write; the trailing one has no following
    // write, so punch it after the final size is set.
    destination
        .set_len(length)
        .map_err(|error| io_error("size the contiguous copy", copy, error))?;
    if offset < length {
        punch_hole(destination, offset, length - offset).map_err(|error| {
            io_error(
                "keep the image's trailing hole in the contiguous copy",
                copy,
                error,
            )
        })?;
    }
    Ok(Copied::Complete { bytes: copied })
}

/// What catching a copy up to a newer image did ([`catch_up`]).
#[derive(Debug, Eq, PartialEq)]
pub enum CaughtUp<Stop> {
    /// The copy holds `next`'s bytes and holes: `written` data bytes were written where `next`
    /// does not share `base`'s blocks, and `punched` bytes the copy held where `next` has a hole
    /// were punched.
    Complete { written: u64, punched: u64 },
    /// `keep_going` broke with `why` once `bytes` were written or punched; the copy holds part of
    /// each.
    Stopped { bytes: u64, why: Stop },
}

/// Make `copy`, which holds `base`'s bytes and holes, hold `next`'s, writing only where they
/// differ (16_build_volumes.md, "Seed compaction"). `base` and `next` are never-written clones,
/// so where `next`'s data sits on the device blocks `base` has at the same offset
/// (`F_LOG2PHYS_EXT`: a clone shares its source's blocks until either is written), it holds
/// `base`'s bytes and the copy is left as it is. Every other data range of `next` is written into
/// `copy` in place with `pread`/`pwrite` past the cache: an overwrite of a file's own unshared
/// blocks keeps its runs (measured 2026-10-26 on the store volume: sixteen 1 MiB overwrites of a
/// contiguous 64 MiB file left its 8 runs, the same writes into a clone of it made 87), so the
/// copy stays contiguous. Then whatever data the copy holds where `next` has a hole is punched:
/// `base`'s data there, and any hole APFS filled with zeros beside a write. The copy takes
/// `next`'s length. `keep_going` is asked after every chunk written and before every punch of at
/// most a chunk, with the bytes written and punched so far; a break leaves the copy holding part
/// of each, for the caller to discard.
pub fn catch_up<Stop>(
    base: &Path,
    next: &Path,
    copy: &Path,
    mut keep_going: impl FnMut(u64) -> ControlFlow<Stop>,
) -> Result<CaughtUp<Stop>, ApfsStorageError> {
    let open = |path: &Path| {
        let file = File::open(path)
            .map_err(|error| io_error("open a seed to catch up from", path, error))?;
        let length = file
            .metadata()
            .map_err(|error| io_error("inspect a seed to catch up from", path, error))?
            .len();
        Ok::<_, ApfsStorageError>((file, length))
    };
    let (base_file, base_length) = open(base)?;
    let (next_file, length) = open(next)?;
    let writer = OpenOptions::new()
        .write(true)
        .open(copy)
        .map_err(|error| io_error("open the copy to catch up", copy, error))?;
    bypass_cache(&next_file).map_err(|error| io_error("bypass the cache reading", next, error))?;
    bypass_cache(&writer).map_err(|error| io_error("bypass the cache writing", copy, error))?;
    writer
        .set_len(length)
        .map_err(|error| io_error("size the copy to its new seed", copy, error))?;
    let mut buffer = vec![0_u8; buffer_len(COPY_CHUNK_BYTES)?];
    let mut written = 0_u64;
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(&next_file, offset, length)
        .map_err(|error| io_error("find the new seed's data", next, error))?
    {
        let mut position = start;
        while position < end {
            let theirs = physical_run(&next_file, position, end - position)
                .map_err(|error| io_error("map the new seed's extents", next, error))?;
            let (step, shared) = match next_data_region(&base_file, position, base_length)
                .map_err(|error| io_error("find the old seed's data", base, error))?
            {
                Some((base_start, base_end)) if base_start <= position => {
                    let ours =
                        physical_run(&base_file, position, theirs.bytes.min(base_end - position))
                            .map_err(|error| io_error("map the old seed's extents", base, error))?;
                    (ours.bytes, ours.device == theirs.device)
                }
                // A hole in the old seed here, up to its next data: nothing it could share.
                Some((base_start, _)) => (theirs.bytes.min(base_start - position), false),
                None => (theirs.bytes, false),
            };
            if !shared {
                let until = position + step;
                let mut at = position;
                while at < until {
                    let bytes = (until - at).min(COPY_CHUNK_BYTES);
                    let chunk = &mut buffer[..buffer_len(bytes)?];
                    next_file
                        .read_exact_at(chunk, at)
                        .map_err(|error| io_error("read the new seed's data", next, error))?;
                    writer
                        .write_all_at(chunk, at)
                        .map_err(|error| io_error("write the copy's catch-up", copy, error))?;
                    at += bytes;
                    written += bytes;
                    if let ControlFlow::Break(why) = keep_going(written) {
                        return Ok(CaughtUp::Stopped {
                            bytes: written,
                            why,
                        });
                    }
                }
            }
            position += step;
        }
        offset = end;
    }
    // A write still in the kernel's cache has no blocks yet: place them before the copy's
    // allocation is read.
    writer
        .sync_all()
        .map_err(|error| io_error("flush the copy's catch-up", copy, error))?;
    let mut punched = 0_u64;
    let mut hole = 0;
    while hole < length {
        let (hole_end, data_end) = next_data_region(&next_file, hole, length)
            .map_err(|error| io_error("find the new seed's holes", next, error))?
            .unwrap_or((length, length));
        let mut at = hole;
        while let Some((start, end)) = next_data_region(&writer, at, hole_end)
            .map_err(|error| io_error("find the copy's data", copy, error))?
        {
            if start >= hole_end {
                break;
            }
            // Bounded punches, each admitted as a write is: a punch frees the copy's blocks,
            // which is disk work a gate or a land waits on as much as a write.
            let mut from = start;
            while from < end {
                if let ControlFlow::Break(why) = keep_going(written + punched) {
                    return Ok(CaughtUp::Stopped {
                        bytes: written + punched,
                        why,
                    });
                }
                let bytes = (end - from).min(COPY_CHUNK_BYTES);
                punch_hole(&writer, from, bytes).map_err(|error| {
                    io_error("punch the new seed's hole in the copy", copy, error)
                })?;
                punched += bytes;
                from += bytes;
            }
            at = end;
        }
        hole = data_end;
    }
    writer
        .sync_all()
        .map_err(|error| io_error("flush the caught-up copy", copy, error))?;
    Ok(CaughtUp::Complete { written, punched })
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

pub(super) fn allocated_bytes(image: &Path) -> Result<u64, ApfsStorageError> {
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

/// Call `run` with the start of every physically contiguous run of the file's data, ascending,
/// skipping holes, until `run` breaks: one `F_LOG2PHYS_EXT` query per run. Elsewhere than macOS
/// nothing maps a file to the device and no RAW image lives there, so each data region counts as
/// one run.
pub(super) fn for_each_run(
    file: &File,
    length: u64,
    mut run: impl FnMut(u64) -> ControlFlow<()>,
) -> io::Result<()> {
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(file, offset, length)? {
        let mut position = start;
        while position < end {
            if run(position).is_break() {
                return Ok(());
            }
            position += run_bytes(file, position, end - position)?;
        }
        offset = end;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn run_bytes(file: &File, offset: u64, length: u64) -> io::Result<u64> {
    physical_run(file, offset, length).map(|run| run.bytes)
}

#[cfg(not(target_os = "macos"))]
fn run_bytes(_: &File, _: u64, length: u64) -> io::Result<u64> {
    Ok(length)
}

/// One physically contiguous run of a file: where it starts on the device and how many bytes long.
struct PhysicalRun {
    device: u64,
    bytes: u64,
}

/// The physically contiguous run from `offset` on, at most `length` bytes.
#[cfg(target_os = "macos")]
fn physical_run(file: &File, offset: u64, length: u64) -> io::Result<PhysicalRun> {
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
    let device = query.l2p_devoffset;
    let device = u64::try_from(device).map_err(|_| {
        io::Error::other(format!(
            "F_LOG2PHYS_EXT reported device offset {device} at offset {offset}"
        ))
    })?;
    match u64::try_from(contiguous) {
        Ok(0) | Err(_) => Err(io::Error::other(format!(
            "F_LOG2PHYS_EXT reported {contiguous} contiguous bytes at offset {offset}"
        ))),
        Ok(contiguous) => Ok(PhysicalRun {
            device,
            bytes: contiguous.min(length),
        }),
    }
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
fn physical_run(_: &File, _: u64, _: u64) -> io::Result<PhysicalRun> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image extents are read with macOS F_LOG2PHYS_EXT",
    ))
}

#[cfg(not(target_os = "macos"))]
fn punch_hole(_: &File, _: u64, _: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image holes are punched with macOS F_PUNCHHOLE",
    ))
}

/// A rewrite copies a whole image, and the recovery copy's Linux pass-through would put every byte
/// of it in the page cache: refuse, as every other rewrite primitive here does off macOS.
#[cfg(not(target_os = "macos"))]
fn bypass_cache(_: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "image rewrites bypass the cache with macOS F_NOCACHE",
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
pub(crate) mod tests {
    use cowshed_os::test_backstop::CommandBackstop as _;
    use std::fs::{self, File, OpenOptions, Permissions};
    use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    use super::{
        count_extents, next_data_region, punch_hole, rewrite_contiguously, rewrite_sibling,
    };

    const MIB: u64 = 1024 * 1024;
    const PAGE: u64 = 16 * 1024;

    pub(in crate::storage::apfs) struct Scratch(pub(in crate::storage::apfs) PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    pub(in crate::storage::apfs) fn scratch(test: &str) -> Scratch {
        let root =
            std::env::temp_dir().join(format!("cowshed-extents-{test}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("scratch root");
        Scratch(root)
    }

    #[test]
    fn extent_proof_rejects_writes_replacements_and_missing_images() {
        let root = scratch("generation");
        let image = root.0.join("image.dmg");
        fs::write(&image, vec![7_u8; 8192]).expect("image");
        let proof = super::prove_forkable(&image).expect("small image generation");
        proof.revalidate(&image).expect("unchanged generation");
        let original = fs::metadata(&image)
            .expect("metadata")
            .modified()
            .expect("mtime");
        let file = OpenOptions::new()
            .write(true)
            .open(&image)
            .expect("open writer");
        file.write_all_at(&[9_u8; 4096], 0)
            .expect("same-size write");
        file.set_modified(original)
            .expect("restore mtime, not ctime");
        assert!(
            proof.revalidate(&image).is_err(),
            "mtime alone is not generation proof"
        );
        drop(file);

        let proof = super::prove_forkable(&image).expect("new write generation");
        let replacement = root.0.join("replacement");
        fs::write(&replacement, fs::read(&image).expect("read image")).expect("replacement");
        File::options()
            .write(true)
            .open(&replacement)
            .expect("replacement file")
            .set_modified(original)
            .expect("same mtime");
        fs::rename(&replacement, &image).expect("replace inode");
        assert!(
            proof.revalidate(&image).is_err(),
            "identical bytes are not the same generation"
        );
        let proof = super::prove_forkable(&image).expect("replacement generation");
        fs::remove_file(&image).expect("remove");
        assert!(
            proof.revalidate(&image).is_err(),
            "missing is unknown, not ready"
        );
        assert!(super::prove_forkable(&image).is_err());
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
    pub(crate) fn fragmented_sparse_file(root: &Path) -> (PathBuf, Vec<(u64, u64)>) {
        let path = root.join("image.dmg");
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
                .status_bounded()
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
        let bounded = super::measure(&image, Some(1)).expect("bounded extent walk");
        assert_eq!(
            bounded.extents.get(),
            1,
            "stop without walking the fragmented map"
        );
        bounded
            .revalidate(&image)
            .expect("same physical generation");
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
        // A write past a hole lets APFS zero-fill the hole (the store volume does, under load):
        // the count would then walk allocated zeros whose blocks may not have device offsets yet.
        // Holes are punched, as in [`fragmented_sparse_file`], and proven before the count.
        drop(file);
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open for writing");
        file.write_all_at(&pattern(0, 8 * MIB), 0).expect("write");
        file.sync_all().expect("flush the data");
        punch_hole(&file, 0, 4 * MIB).expect("leading hole");
        punch_hole(&file, 5 * MIB, 3 * MIB).expect("trailing hole");
        file.sync_all().expect("flush the holes");
        assert_eq!(
            data_regions(&path),
            [(4 * MIB, 5 * MIB)],
            "the holes are holes"
        );
        assert_eq!(
            fs::metadata(&path).expect("stat").blocks() * 512,
            MIB,
            "only the data is allocated"
        );
        // One 8 MiB write is laid out as one run (`COPY_CHUNK_BYTES`), so the MiB left between the
        // holes is one run too; a hole counted as a run would add to it.
        assert_eq!(
            count_extents(&path).expect("count").get(),
            1,
            "the data region alone is counted"
        );
    }

    #[test]
    fn a_stopped_copy_removes_its_copy_and_leaves_the_source_as_it_was() {
        let root = scratch("stopped");
        let (image, regions) = fragmented_sparse_file(&root.0);
        let extents = count_extents(&image).expect("count");
        let copy = root.0.join("copy.dmg");
        let mut asked = 0;
        let copied = super::copy_contiguously(&image, &copy, |bytes| {
            asked += 1;
            if bytes >= MIB {
                std::ops::ControlFlow::Break("a gate started")
            } else {
                std::ops::ControlFlow::Continue(())
            }
        })
        .expect("copy");
        assert_eq!(
            copied,
            super::Copied::Stopped {
                bytes: 4 * MIB,
                why: "a gate started"
            }
        );
        assert_eq!(
            asked, 1,
            "asked after every chunk, stopped at the first break"
        );
        assert!(!copy.exists(), "a stopped copy is removed");
        assert_eq!(data_regions(&image), regions);
        assert_eq!(count_extents(&image).expect("count"), extents);
        assert_eq!(
            super::count_extents_to(&image, 3).expect("bounded").get(),
            3,
            "a bounded count stops at its bound"
        );
    }

    /// A catch-up (16_build_volumes.md, "Seed compaction") writes only what the next seed does
    /// not share with the copied one: nothing for a plain clone of it; for a clone with a range
    /// rewritten, data written into a hole and a data range punched, exactly its unshared data,
    /// after which the copy holds the next seed's bytes and holes and keeps its few extents.
    #[test]
    fn compaction_catch_up_writes_only_what_the_next_seed_changed() {
        use std::ops::ControlFlow;
        let root = scratch("catch-up");
        let (base, _) = fragmented_sparse_file(&root.0);
        let copy = root.0.join("copy.dmg");
        assert_eq!(
            super::copy_contiguously(&base, &copy, |_| ControlFlow::<()>::Continue(()))
                .expect("copy"),
            super::Copied::Complete { bytes: 8 * MIB }
        );
        let compact = count_extents(&copy).expect("count the copy");
        let clone = |name: &str| {
            let path = root.0.join(name);
            assert!(
                std::process::Command::new("/bin/cp")
                    .arg("-c")
                    .arg(&base)
                    .arg(&path)
                    .status_locked()
                    .expect("cp -c")
                    .success()
            );
            path
        };
        let unchanged = clone("unchanged.dmg");
        assert_eq!(
            super::catch_up(
                &base,
                &unchanged,
                &copy,
                |_| ControlFlow::<()>::Continue(())
            )
            .expect("catch up to a plain clone"),
            super::CaughtUp::Complete {
                written: 0,
                punched: 0
            },
            "a clone shares every block"
        );

        let next = clone("next.dmg");
        let file = OpenOptions::new()
            .write(true)
            .open(&next)
            .expect("open the next seed");
        let mib = usize::try_from(MIB).expect("MiB");
        file.write_all_at(&vec![0xa5; mib], 2 * MIB)
            .expect("rewrite a data range");
        file.write_all_at(&vec![0x5a; mib], 6 * MIB)
            .expect("write into a hole");
        punch_hole(&file, 13 * MIB, MIB).expect("punch a data range");
        file.sync_all().expect("flush the next seed");
        drop(file);
        let data: u64 = data_regions(&next)
            .iter()
            .map(|(start, end)| end - start)
            .sum();
        let shared = super::shared_bytes(&base, &next).expect("shared bytes");
        let mut asked = Vec::new();
        let caught = super::catch_up(&base, &next, &copy, |bytes| {
            asked.push(bytes);
            ControlFlow::<()>::Continue(())
        })
        .expect("catch up");
        let super::CaughtUp::Complete { written, punched } = caught else {
            panic!("nothing stops the catch-up: {caught:?}");
        };
        assert_eq!(written, data - shared, "only the unshared data is written");
        assert!(
            written >= 2 * MIB && written * 2 < data,
            "the shared ranges are skipped: {written} of {data} bytes written"
        );
        assert!(punched >= MIB, "the punched range is punched: {punched}");
        assert!(
            asked.iter().filter(|bytes| **bytes >= written).count()
                > usize::try_from(punched.div_ceil(8 * MIB)).expect("punches"),
            "the last write and every punch of at most a chunk are admitted: {asked:?}"
        );
        assert_eq!(
            data_regions(&copy),
            data_regions(&next),
            "holes as the next seed's"
        );
        assert!(
            fs::read(&copy).expect("read the copy") == fs::read(&next).expect("read the next seed"),
            "the copy holds the next seed's bytes"
        );
        let after = count_extents(&copy).expect("count the caught-up copy");
        assert!(
            after.get() <= compact.get() + 4,
            "in place, the copy keeps its runs: {compact} extents became {after}"
        );
    }
}
