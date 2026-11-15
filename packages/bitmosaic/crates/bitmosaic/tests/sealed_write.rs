//! The slice writer: `image_len` and `write_image_into` give the sealed
//! image of a strictly ascending slice without building the set.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bitmosaic::{Bitmosaic, BitmosaicView, PatchError, PatchScratch, image_len, write_image_into};

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

#[inline(always)]
fn bump() {
    let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
}

// SAFETY: every method forwards to `System` unchanged; the counter is a
// thread-local side effect that cannot affect the returned pointers.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations_during(body: impl FnOnce()) -> usize {
    let before = ALLOCS.with(|n| n.get());
    body();
    ALLOCS.with(|n| n.get()) - before
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// `k` distinct values below `below`, ascending.
fn draw(k: usize, below: u32, state: &mut u64) -> Vec<u32> {
    let mut values: Vec<u32> = (0..2 * k)
        .map(|_| (xorshift(state) % u64::from(below)) as u32)
        .collect();
    values.sort_unstable();
    values.dedup();
    values.truncate(k);
    values
}

/// Sets that reach every form the sealed writer has: each small form, a
/// directory of one chunk and of many, every container kind, and the
/// Elias-Fano root. The flag says the image holds no directory, so its
/// write may not allocate.
fn sets() -> Vec<(&'static str, Vec<u32>, bool)> {
    let mut state = 0x5EA1_ED00_0000_0001u64;
    let mut sets: Vec<(&'static str, Vec<u32>, bool)> = vec![
        ("empty", vec![], true),
        ("singleton", vec![7], true),
        ("four ordinals", vec![0, 1, 2, 10], true),
        ("four of 83", vec![7, 17, 40, 82], true),
        (
            "thirteen of 70",
            vec![1, 3, 9, 14, 20, 25, 33, 40, 47, 55, 60, 66, 69],
            true,
        ),
        ("one interval", (0..83).collect(), true),
        (
            "three intervals",
            (100..400).chain(1_000..1_200).chain(9_000..9_050).collect(),
            true,
        ),
        (
            "window at",
            (0..12).map(|i| 1_000 + 7 * i + i % 2).collect(),
            true,
        ),
        ("list16", (0..8).map(|i| 500 + 900 * i * i).collect(), true),
        (
            "window wide",
            (0..400)
                .filter(|v| v % 2 == 0 || v % 7 == 0)
                .map(|v| 3_000 + v)
                .collect(),
            true,
        ),
        ("a whole chunk", (0..65_536).collect(), true),
        (
            "a stride in chunk zero",
            (0..40).map(|i| 300 * i).collect(),
            false,
        ),
        (
            "a cone in chunk zero",
            (0..60).map(|i| 700 * i + i % 3).collect(),
            false,
        ),
        ("one member above chunk zero", vec![70_000], false),
        ("two chunks", vec![1, 70_000], false),
        ("words", (0..30_000).filter(|v| v % 3 != 0).collect(), false),
        (
            "runs",
            (0..20).flat_map(|i| 3_000 * i..3_000 * i + 1_000).collect(),
            false,
        ),
        (
            "many chunks",
            (0..200_000).filter(|v| v % 3 != 0).collect(),
            false,
        ),
        (
            "elias-fano",
            (0..10_000).map(|i| i * 429_497 + i % 7).collect(),
            true,
        ),
    ];
    for (k, below) in [
        (2, 83),
        (20, 83),
        (64, 256),
        (16, 4_096),
        (64, 4_096),
        (64, 65_536),
    ] {
        sets.push(("drawn", draw(k, below, &mut state), k < 64 || below <= 256));
    }
    sets
}

//#region smoo/bitmosaic!n/sealed-write
#[test]
fn the_slice_writer_writes_the_sealed_image_and_names_its_length() {
    let mut scratch = PatchScratch::try_new().expect("scratch");
    for (name, members, _) in sets() {
        let sealed = Bitmosaic::from_sorted(members.iter().copied()).to_bytes();
        assert_eq!(image_len(&members), sealed.len(), "{name}: image_len");
        let mut out = vec![0xEE; sealed.len() + 3];
        let written = write_image_into(&members, &mut out, &mut scratch);
        assert_eq!(written, Ok(sealed.len()), "{name}: written length");
        assert_eq!(out[..sealed.len()], sealed[..], "{name}: bytes");
        assert_eq!(
            out[sealed.len()..],
            [0xEE; 3],
            "{name}: bytes past the image"
        );
        let view = BitmosaicView::open_canonical(&out[..sealed.len()])
            .unwrap_or_else(|error| panic!("{name}: the written image is canonical: {error}"));
        assert!(view.range().eq(members.iter().copied()), "{name}: members");
    }
}

#[test]
fn a_short_slot_and_an_unsorted_slice_are_refused_with_the_slot_untouched() {
    let mut scratch = PatchScratch::try_new().expect("scratch");
    for (name, members, _) in sets() {
        let needed = image_len(&members);
        let mut out = vec![0xEE; needed - 1];
        assert_eq!(
            write_image_into(&members, &mut out, &mut scratch),
            Err(PatchError::Capacity { needed }),
            "{name}"
        );
        assert!(
            out.iter().all(|&byte| byte == 0xEE),
            "{name}: slot untouched"
        );
    }
    let mut out = [0xEE; 16];
    for unsorted in [&[3u32, 3][..], &[5, 4], &[1, 2, 2, 9]] {
        assert_eq!(
            write_image_into(unsorted, &mut out, &mut scratch),
            Err(PatchError::Unsorted)
        );
        assert_eq!(out, [0xEE; 16]);
    }
}

#[test]
fn an_image_without_a_directory_is_written_and_measured_without_allocating() {
    // Instrument check first: a counter that cannot report a violation
    // cannot certify its absence.
    assert!(allocations_during(|| drop(std::hint::black_box(vec![0u8; 64]))) > 0);
    let mut scratch = PatchScratch::try_new().expect("scratch");
    let mut out = vec![0u8; 1 << 16];
    for (name, members, no_directory) in sets() {
        if !no_directory {
            continue;
        }
        let allocations = allocations_during(|| {
            let needed = image_len(&members);
            let written = write_image_into(&members, &mut out, &mut scratch);
            assert_eq!(written, Ok(needed));
        });
        assert_eq!(allocations, 0, "{name}");
    }
}
//#endregion smoo/bitmosaic!n/sealed-write

//#region smoo/bitmosaic!n/small-choice-floor
/// The writers that choose a small form without the ladder (below a floor
/// under every directory of the set) and the writer that always runs it
/// give the same bytes, for every nonempty subset of thirteen positions
/// under embeddings that make them ordinals, a wide span, a window above
/// zero and a spread over the chunk.
#[test]
fn every_small_subset_has_one_image_whichever_writer_chooses_its_form() {
    let embeddings: [fn(u32) -> u32; 4] = [
        |position| position,
        |position| 37 * position,
        |position| 1_000 + 3 * position,
        |position| 5_000 * position + position % 3,
    ];
    let mut scratch = PatchScratch::try_new().expect("scratch");
    let (mut out, mut slot) = ([0u8; 64], [0u8; 64]);
    for embed in embeddings {
        for subset in 1u32..1 << 13 {
            let members: Vec<u32> = (0..13)
                .filter(|position| subset >> position & 1 != 0)
                .map(embed)
                .collect();
            let owned = Bitmosaic::from_sorted(members.iter().copied());
            let (sealed, forest) = (owned.to_bytes(), owned.to_forest_bytes());
            assert_eq!(image_len(&members), sealed.len(), "{members:?}");
            assert_eq!(
                write_image_into(&members, &mut out, &mut scratch),
                Ok(sealed.len()),
                "{members:?}"
            );
            assert_eq!(
                out[..sealed.len()],
                sealed[..],
                "{members:?}: the slice writer"
            );
            slot[0] = bitmosaic::EMPTY_U32_IMAGE[0];
            let report = bitmosaic::patch(&mut slot, &members, &[], &mut scratch)
                .unwrap_or_else(|error| panic!("{members:?}: {error}"));
            assert_eq!(
                slot[..report.serialized_len],
                forest[..],
                "{members:?}: a patch of the empty slot"
            );
            assert!(
                BitmosaicView::open_canonical(&sealed).is_ok(),
                "{members:?}: canonical"
            );
            assert!(
                BitmosaicView::open_verified(&forest).is_ok(),
                "{members:?}: the slot image verifies"
            );
        }
    }
}
//#endregion smoo/bitmosaic!n/small-choice-floor
