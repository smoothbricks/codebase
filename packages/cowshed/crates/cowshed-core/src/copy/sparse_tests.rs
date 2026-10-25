//! Behavioural proof that a held-file copy and its verification touch only the bytes a file holds
//! as data (02_workspaces.md, "Recovering a recovery image": a cross-filesystem copy is sparse, and
//! its `published` state records the SHA-256 of bytes that were compared). Fixtures are real sparse
//! files whose data regions sit at known offsets; every expected digest is built from those regions
//! and the zero runs between them, never by reading a file back.

use std::fs;
use std::io;
use std::os::unix::fs::{FileExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

use crate::api::Sha256Digest;
use crate::storage::directory::tests::Scratch;

use super::native::next_data_region;
use super::{Content, Run, Runs, ZEROS, compare_file_bytes, copy_held_file, verify_held_file};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// A gibibyte of logical length with a few MiB of data: the shape of a recovery image, whose guest
/// wrote a little of a large sparse capacity. Hashing every logical byte is the digest's price, so
/// the length is one an unoptimized test build hashes in seconds.
const LARGE_LENGTH: u64 = 1 << 30;
/// Data at the head, two interior regions and a trailing hole. Every boundary is a multiple of
/// 64 KiB, so each filesystem's SEEK_DATA granularity reports it exactly, and every gap is at least
/// 16 MiB: APFS zero-fills a shorter gap when a write lands past it (an 8 MiB gap reads back as
/// data, a 16 MiB one as a hole), so a shorter gap would be no hole to walk.
const LARGE_REGIONS: [(u64, u64); 3] = [
    (0, 64 * KIB),
    (256 * MIB + 192 * KIB, 257 * MIB),
    (768 * MIB, 770 * MIB),
];

/// A small fixture whose gaps all stay holes on APFS (see [`LARGE_REGIONS`]).
const SMALL_LENGTH: u64 = 128 * MIB;
const SMALL_REGIONS: [(u64, u64); 1] = [(32 * MIB, 33 * MIB)];
/// A hole of the small source, where a destination may hold what the source does not.
const SMALL_HOLE: (u64, u64) = (64 * MIB, 65 * MIB);

/// The shared RAII scratch root, under the runtime directory where one is offered.
fn scratch(label: &str) -> Scratch {
    let parent = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    Scratch::new_in(&parent, &format!("copy-sparse-{label}"))
}

/// The bytes at `offset`: nonzero everywhere, so no data byte reads as a hole's zero, and distinct
/// per page, so a dropped or misplaced page is caught.
fn pattern(offset: u64, length: u64) -> Vec<u8> {
    (offset..offset + length)
        .map(|position| u8::try_from((position / (16 * KIB) + position) % 251 + 1).expect("byte"))
        .collect()
}

/// A sparse file of `length` holding `pattern` over exactly `regions`. It is sized first, so every
/// positional write lands inside the file and none extends it across a gap.
fn sparse_file(path: &Path, length: u64, regions: &[(u64, u64)]) -> fs::File {
    let file = held_file(path);
    file.set_len(length).expect("size the sparse file");
    for &(start, end) in regions {
        file.write_all_at(&pattern(start, end - start), start)
            .expect("write a data region");
    }
    file.sync_all().expect("place the data regions");
    file
}

/// A newly created, held regular file, as the recovery copy creates its partial.
fn held_file(path: &Path) -> fs::File {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .expect("create a held file")
}

fn data_regions(file: &fs::File, length: u64) -> Vec<(u64, u64)> {
    let mut regions = Vec::new();
    let mut offset = 0;
    while let Some((start, end)) = next_data_region(file, offset, length).expect("data region") {
        regions.push((start, end));
        offset = end;
    }
    regions
}

/// The runs a comparison walks between two files holding data over the same `regions`: each
/// region read from both, each gap a hole in both and read from neither.
fn same_region_runs(length: u64, regions: &[(u64, u64)]) -> Vec<Run> {
    let hole = |start, end| Run {
        start,
        end,
        source: Content::Hole,
        destination: Content::Hole,
    };
    let mut runs = Vec::new();
    let mut offset = 0;
    for &(start, end) in regions {
        if start > offset {
            runs.push(hole(offset, start));
        }
        runs.push(Run {
            start,
            end,
            source: Content::Data,
            destination: Content::Data,
        });
        offset = end;
    }
    if offset < length {
        runs.push(hole(offset, length));
    }
    runs
}

fn walked_runs(source: &fs::File, destination: &fs::File, length: u64) -> Vec<Run> {
    Runs::new(source, destination, length)
        .expect("plan the comparison")
        .collect::<io::Result<_>>()
        .expect("walk the comparison")
}

/// SHA-256 of `length` logical bytes that are `pattern` over `regions` and zero everywhere else.
fn expected_digest(length: u64, regions: &[(u64, u64)]) -> Sha256Digest {
    let hash_zeros = |digest: &mut Sha256, mut bytes: u64| {
        while bytes > 0 {
            let count = usize::try_from(bytes).map_or(ZEROS.len(), |rest| rest.min(ZEROS.len()));
            digest.update(&ZEROS[..count]);
            bytes -= u64::try_from(count).expect("chunk");
        }
    };
    let mut digest = Sha256::new();
    let mut offset = 0;
    for &(start, end) in regions {
        hash_zeros(&mut digest, start - offset);
        digest.update(pattern(start, end - start));
        offset = end;
    }
    hash_zeros(&mut digest, length - offset);
    Sha256Digest::from_bytes(digest.finalize().into())
}

/// A large sparse recovery image copies and verifies through its data regions alone: the walk the
/// comparison consumes reads exactly the source's data, every gap is a hole in both files and is
/// read from neither, and the recorded digest is still SHA-256 over every logical byte. The
/// byte-at-a-time comparison this replaced read all of `LARGE_LENGTH` from both files, holes
/// included, and had no walk to assert.
#[test]
fn a_large_sparse_copy_verifies_reading_only_its_data_regions() {
    let scratch = scratch("large");
    let source = sparse_file(&scratch.path().join("source"), LARGE_LENGTH, &LARGE_REGIONS);
    assert_eq!(
        data_regions(&source, LARGE_LENGTH),
        LARGE_REGIONS,
        "the fixture is sparse"
    );
    let destination = held_file(&scratch.path().join("destination"));

    copy_held_file(&source, &destination).expect("copy the sparse file");

    assert_eq!(
        data_regions(&destination, LARGE_LENGTH),
        LARGE_REGIONS,
        "the copy kept every hole"
    );
    assert_eq!(
        walked_runs(&source, &destination, LARGE_LENGTH),
        same_region_runs(LARGE_LENGTH, &LARGE_REGIONS),
        "the comparison reads the data regions and nothing else"
    );
    let digest = verify_held_file(&source, &destination).expect("the sparse copy verifies");
    assert_eq!(
        digest,
        expected_digest(LARGE_LENGTH, &LARGE_REGIONS),
        "the digest covers every logical byte, holes included"
    );
}

/// The walker visits exactly the data regions: from the start, from a hole, from inside a region
/// (entered at the offset), within a range that ends inside a region, and past the last region,
/// where ENXIO means only a hole remains.
#[test]
fn the_walker_visits_exactly_the_data_regions() {
    let scratch = scratch("walker");
    let file = sparse_file(&scratch.path().join("file"), LARGE_LENGTH, &LARGE_REGIONS);

    assert_eq!(data_regions(&file, LARGE_LENGTH), LARGE_REGIONS);
    let [head, interior, last] = LARGE_REGIONS;
    assert_eq!(
        next_data_region(&file, head.1, LARGE_LENGTH).expect("from a hole"),
        Some(interior)
    );
    assert_eq!(
        next_data_region(&file, interior.0 + 64 * KIB, LARGE_LENGTH).expect("inside a region"),
        Some((interior.0 + 64 * KIB, interior.1))
    );
    assert_eq!(
        next_data_region(&file, 0, 32 * KIB).expect("a range ending inside a region"),
        Some((0, 32 * KIB))
    );
    assert_eq!(
        next_data_region(&file, last.1, LARGE_LENGTH).expect("the trailing hole"),
        None
    );
}

/// Data a destination holds inside a source hole differs from the source: the hole compares as
/// zeros without being read, so the stray bytes are refused.
#[test]
fn a_destination_with_data_in_a_source_hole_is_refused() {
    let scratch = scratch("stray");
    let source = sparse_file(&scratch.path().join("source"), SMALL_LENGTH, &SMALL_REGIONS);
    let stray = [SMALL_REGIONS[0], SMALL_HOLE];
    let destination = sparse_file(&scratch.path().join("destination"), SMALL_LENGTH, &stray);
    assert_eq!(data_regions(&destination, SMALL_LENGTH), stray);

    let error = compare_file_bytes(&source, &destination, SMALL_LENGTH)
        .expect_err("data inside a source hole must refuse");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert_eq!(error.to_string(), "file content differs");
}

/// Zeros a destination allocated where the source has a hole are the bytes the hole reads as: the
/// comparison accepts them either way round, and the digest is the source's logical content.
/// (Verification's allocation contract separately refuses a copy that materialized its holes.)
#[test]
fn a_destination_with_allocated_zeros_in_a_source_hole_is_accepted() {
    let scratch = scratch("zeros");
    let source = sparse_file(&scratch.path().join("source"), SMALL_LENGTH, &SMALL_REGIONS);
    let destination = sparse_file(
        &scratch.path().join("destination"),
        SMALL_LENGTH,
        &SMALL_REGIONS,
    );
    let (start, end) = SMALL_HOLE;
    destination
        .write_all_at(
            &vec![0; usize::try_from(end - start).expect("hole length")],
            start,
        )
        .expect("allocate zeros");
    destination.sync_all().expect("place the zeros");
    assert_eq!(
        data_regions(&destination, SMALL_LENGTH),
        [SMALL_REGIONS[0], SMALL_HOLE],
        "the zeros are allocated data"
    );
    assert!(
        walked_runs(&source, &destination, SMALL_LENGTH).contains(&Run {
            start,
            end,
            source: Content::Hole,
            destination: Content::Data,
        }),
        "only the destination is read over the source's hole"
    );

    let digest = compare_file_bytes(&source, &destination, SMALL_LENGTH)
        .expect("allocated zeros equal a hole");
    let reverse = compare_file_bytes(&destination, &source, SMALL_LENGTH)
        .expect("a hole equals allocated zeros");

    assert_eq!(digest, expected_digest(SMALL_LENGTH, &SMALL_REGIONS));
    assert_eq!(reverse, digest);
}

/// A file of holes alone copies to holes alone and verifies reading neither file: its walk is one
/// run, a hole in both, and its digest is that of its length in zeros.
#[test]
fn a_file_of_holes_alone_copies_and_verifies_without_a_read() {
    let scratch = scratch("holes");
    let source = sparse_file(&scratch.path().join("source"), SMALL_LENGTH, &[]);
    assert!(
        data_regions(&source, SMALL_LENGTH).is_empty(),
        "the fixture is all hole"
    );
    let destination = held_file(&scratch.path().join("destination"));

    copy_held_file(&source, &destination).expect("copy a file of holes");

    assert!(
        data_regions(&destination, SMALL_LENGTH).is_empty(),
        "the copy kept the hole"
    );
    assert_eq!(
        walked_runs(&source, &destination, SMALL_LENGTH),
        same_region_runs(SMALL_LENGTH, &[]),
        "the comparison reads neither file"
    );
    assert_eq!(
        verify_held_file(&source, &destination).expect("a copy of holes verifies"),
        expected_digest(SMALL_LENGTH, &[])
    );
}
