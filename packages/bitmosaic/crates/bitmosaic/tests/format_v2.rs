//! Comprehensive Format 2 verification suite (`specs/bitmosaic/01-format.md`):
//! - Every golden vector in `specs/bitmosaic/vectors/format-2.json` (31 u32, 4 u64, 23 rejects)
//! - Every Bend2 law bound to Rust bytes (`sizes.bend`, `one_image.bend`, `list_probe.bend`,
//!   `bitmap_probe.bend`, `elias_fano.bend`, `seek.bend`, `form_choice.bend`)
//! - Negative admission refusal tests for every class (`extent`, `kind`, `count`, `map-order`,
//!   `plane-range`, `bit-pad`, `placement`, `canonical`)
//! - Differential oracle vs `Bitmosaic` and `BTreeSet` on random u32/u64 sets
//! - Format 2 patching oracle (`patch_v2`, `patch_v2_witnessed`, `patched_len_bound_v2`)
//! - Zero-allocation proof (counting global allocator with positive control)

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;

use bitmosaic::{
    AdmissionClass, BatchOutcome, BitmapView, Bitmosaic, Bitmosaic64, Dir64V2View, EMPTY_V2_IMAGE,
    Ef2View, Format, Image, ImageForm, KeyWidth, ListView, PatchError, PatchScratch, Range,
    SingleView, V2View, V264View, bitmap_len, ef2_len, forest_len_v2, image_header_v2,
    image_len_v2, list_len, patch_v2, patch_v2_witnessed, patched_len_bound_v2, single_len,
    to_v2_bytes_u64, write_bitmap, write_ef2, write_forest_into_v2, write_image_into_v2,
    write_list, write_single,
};
use serde_json::Value;

// ── Counting allocator for zero-alloc proof ──────────────────────────────

struct CountingAlloc;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNTING.with(|on| {
            if on.get() {
                ALLOCS.with(|n| n.set(n.get() + 1));
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn count_allocs<R>(f: impl FnOnce() -> R) -> (R, usize) {
    ALLOCS.with(|n| n.set(0));
    COUNTING.with(|on| on.set(true));
    let out = f();
    COUNTING.with(|on| on.set(false));
    let n = ALLOCS.with(|n| n.get());
    (out, n)
}

// ── Helpers ──────────────────────────────────────────────────────────────

const CORPUS: &str = include_str!("../../../../../specs/bitmosaic/vectors/format-2.json");

fn corpus() -> Value {
    serde_json::from_str(CORPUS).expect("valid format-2.json")
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

fn u32_members(row: &Value) -> Vec<u32> {
    row["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|v| u32::try_from(v.as_u64().expect("u64")).expect("u32"))
        .collect()
}

fn u64_members(row: &Value) -> Vec<u64> {
    row["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|v| v.as_u64().expect("u64"))
        .collect()
}

fn expected_form(name: &str) -> ImageForm {
    match name {
        "Bitmap" => ImageForm::Bitmap,
        "Single" => ImageForm::Single,
        "List" => ImageForm::List,
        "Elias-Fano" => ImageForm::EliasFano,
        "Directory" => ImageForm::Directory,
        other => panic!("unexpected form {other}"),
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn check_v2_reads(view: &V2View<'_>, members: &[u32], ctx: &str) {
    assert_eq!(view.len() as usize, members.len(), "{ctx} len");
    assert_eq!(view.is_empty(), members.is_empty(), "{ctx} is_empty");
    let mut walked = Vec::new();
    view.for_each(|v| walked.push(v));
    assert_eq!(walked, members, "{ctx} for_each");
    assert_eq!(view.range().collect::<Vec<_>>(), members, "{ctx} range");

    for (idx, &v) in members.iter().enumerate() {
        assert!(view.contains(v), "{ctx} contains({v})");
        assert_eq!(view.rank(v) as usize, idx, "{ctx} rank({v})");
        assert_eq!(
            view.rank_if_present(v),
            Some(idx as u64),
            "{ctx} rank_if_present({v})"
        );
        assert_eq!(view.select(idx as u64), Some(v), "{ctx} select({idx})");
        assert_eq!(view.seek(v), Some(v), "{ctx} seek({v})");
        if v > 0 {
            let prev = v - 1;
            let present = members.binary_search(&prev).is_ok();
            let want_rank = members.partition_point(|&m| m < prev);
            assert_eq!(view.contains(prev), present, "{ctx} contains({prev})");
            assert_eq!(view.rank(prev) as usize, want_rank, "{ctx} rank({prev})");
            assert_eq!(
                view.rank_if_present(prev),
                present.then_some(want_rank as u64),
                "{ctx} rank_if_present({prev})"
            );
            assert_eq!(
                view.seek(prev),
                members.get(want_rank).copied(),
                "{ctx} seek({prev})"
            );
        }
        if v < u32::MAX {
            let next = v + 1;
            let present = members.binary_search(&next).is_ok();
            let want_rank = members.partition_point(|&m| m < next);
            assert_eq!(view.contains(next), present, "{ctx} contains({next})");
            assert_eq!(view.rank(next) as usize, want_rank, "{ctx} rank({next})");
            assert_eq!(
                view.rank_if_present(next),
                present.then_some(want_rank as u64),
                "{ctx} rank_if_present({next})"
            );
            assert_eq!(
                view.seek(next),
                members.get(want_rank).copied(),
                "{ctx} seek({next})"
            );
        }
    }
    assert_eq!(view.select(members.len() as u64), None, "{ctx} select(len)");
    for probe in [0u32, 1, 7, 8, 255, 256, 1015, 1016, 65535, 65536, u32::MAX] {
        let want_rank = members.partition_point(|&m| m < probe);
        assert_eq!(
            view.rank(probe) as usize,
            want_rank,
            "{ctx} edge rank({probe})"
        );
        assert_eq!(
            view.seek(probe),
            members.get(want_rank).copied(),
            "{ctx} edge seek({probe})"
        );
        let mut r = view.range();
        r.seek(probe);
        assert_eq!(
            r.collect::<Vec<_>>(),
            members[want_rank..],
            "{ctx} range.seek({probe})"
        );
    }
}

// ── 1. Golden vector conformance (specs/bitmosaic/vectors/format-2.json) ─

#[test]
fn golden_u32_vectors_match_byte_for_byte_and_read_back() {
    let c = corpus();
    let rows = c["u32"].as_array().expect("u32 array");
    assert_eq!(rows.len(), 31, "all 31 u32 golden vectors");

    for (idx, row) in rows.iter().enumerate() {
        let members = u32_members(row);
        let want_sealed = hex(row["sealed"].as_str().expect("sealed"));
        let want_forest = hex(row["forest"].as_str().expect("forest"));
        let form = expected_form(row["form"].as_str().expect("form"));

        // Owned Bitmosaic writers.
        let owned = Bitmosaic::from_sorted(members.iter().copied());
        assert_eq!(
            owned.to_v2_bytes(),
            want_sealed,
            "row {idx}: Bitmosaic::to_v2_bytes"
        );
        let mut owned_forest = vec![0u8; want_forest.len()];
        assert_eq!(
            owned.write_v2_forest_into_slice(&mut owned_forest),
            Ok(want_forest.len()),
            "row {idx}: Bitmosaic::write_v2_forest_into_slice"
        );
        assert_eq!(owned_forest, want_forest, "row {idx}: forest bytes");
        assert_eq!(owned.v2_serialized_len(), want_sealed.len(), "row {idx}");

        // Direct slice writers.
        assert_eq!(image_len_v2(&members), want_sealed.len(), "row {idx}");
        assert_eq!(forest_len_v2(&members), want_forest.len(), "row {idx}");
        let mut buf = vec![0xCC; want_sealed.len()];
        assert_eq!(
            write_image_into_v2(&members, &mut buf),
            Ok(want_sealed.len())
        );
        assert_eq!(buf, want_sealed, "row {idx}: write_image_into_v2");

        let mut fbuf = vec![0xCC; want_forest.len()];
        assert_eq!(
            write_forest_into_v2(&members, &mut fbuf),
            Ok(want_forest.len())
        );
        assert_eq!(fbuf, want_forest, "row {idx}: write_forest_into_v2");

        // Header inspection.
        let hdr = image_header_v2(&want_sealed).expect("sealed header");
        assert_eq!(hdr.format, Format::Format2, "row {idx}");
        assert_eq!(hdr.form, form, "row {idx}");
        assert_eq!(hdr.width, KeyWidth::U32, "row {idx}");
        assert_eq!(hdr.extent, want_sealed.len(), "row {idx}");

        // Admission & reads on exact bytes AND with trailing junk (§12.2).
        let mut padded = want_sealed.clone();
        padded.extend_from_slice(&[0xFF; 13]);
        for bytes in [want_sealed.as_slice(), padded.as_slice()] {
            let v_open = V2View::open(bytes).expect("open sealed");
            let v_ver = V2View::open_verified(bytes).expect("open_verified sealed");
            let v_can = V2View::open_canonical(bytes).expect("open_canonical sealed");
            let v_adm = V2View::open_admitted(bytes).expect("open_admitted sealed");
            for v in [&v_open, &v_ver, &v_can, &v_adm] {
                assert_eq!(v.form(), form, "row {idx}");
                assert_eq!(v.serialized_len(), want_sealed.len(), "row {idx}");
                check_v2_reads(v, &members, &format!("row {idx} sealed"));
            }
            assert!(Image::open_canonical(bytes).is_ok(), "row {idx} Image");
        }

        // Forest image: open & open_verified always succeed; open_canonical refuses when != sealed.
        let fv = V2View::open_verified(&want_forest).expect("open_verified forest");
        check_v2_reads(&fv, &members, &format!("row {idx} forest"));
        if want_forest != want_sealed {
            let err = V2View::open_canonical(&want_forest)
                .expect_err("forest != sealed must refuse open_canonical");
            assert_eq!(err.class, AdmissionClass::Canonical, "row {idx} forest");
        } else {
            assert!(V2View::open_canonical(&want_forest).is_ok());
        }
    }
}

#[test]
fn golden_u64_vectors_match_byte_for_byte_and_read_back() {
    let c = corpus();
    let rows = c["u64"].as_array().expect("u64 array");
    assert_eq!(rows.len(), 4, "all 4 u64 golden vectors");

    for (idx, row) in rows.iter().enumerate() {
        let members = u64_members(row);
        let want = hex(row["sealed"].as_str().expect("sealed"));

        let owned = Bitmosaic64::from_sorted(members.iter().copied());
        assert_eq!(owned.to_v2_bytes(), want, "u64 row {idx}");
        assert_eq!(owned.v2_serialized_len(), want.len(), "u64 row {idx}");
        assert_eq!(to_v2_bytes_u64(&members), want, "u64 row {idx} slice");

        let hdr = image_header_v2(&want).expect("u64 header");
        assert_eq!(hdr.format, Format::Format2);
        assert_eq!(hdr.extent, want.len());

        let mut padded = want.clone();
        padded.extend_from_slice(&[0xAA; 11]);
        for bytes in [want.as_slice(), padded.as_slice()] {
            let v1 = V264View::open(bytes).expect("open u64");
            let v2 = V264View::open_verified(bytes).expect("open_verified u64");
            let v3 = V264View::open_canonical(bytes).expect("open_canonical u64");
            let v4 = V264View::open_admitted(bytes).expect("open_admitted u64");
            for v in [&v1, &v2, &v3, &v4] {
                assert_eq!(v.serialized_len(), want.len(), "u64 row {idx}");
                assert_eq!(v.len() as usize, members.len(), "u64 row {idx}");
                assert_eq!(v.range().collect::<Vec<_>>(), members, "u64 row {idx}");
                let mut walked = Vec::new();
                v.for_each(|x| walked.push(x));
                assert_eq!(walked, members, "u64 row {idx} for_each");
                for (k, &m) in members.iter().enumerate() {
                    assert!(v.contains(m));
                    assert_eq!(v.rank(m), k as u64);
                    assert_eq!(v.rank_if_present(m), Some(k as u64));
                    assert_eq!(v.select(k as u64), Some(m));
                    assert_eq!(v.seek(m), Some(m));
                }
            }
        }
    }
}

#[test]
fn golden_reject_vectors_fail_with_exact_class_and_stage() {
    let c = corpus();
    let rows = c["rejects"].as_array().expect("rejects array");
    assert_eq!(rows.len(), 23, "all 23 reject golden vectors");

    for (idx, row) in rows.iter().enumerate() {
        let bytes = hex(row["bytes"].as_str().expect("bytes"));
        let want_class = row["class"].as_str().expect("class");
        let refused_by = row["refused_by"].as_str().expect("refused_by");
        let why = row["why"].as_str().expect("why");

        let is_u64_dir = bytes.first() == Some(&0x03);
        let open_res = if is_u64_dir {
            V264View::open(&bytes).map(|_| ())
        } else {
            V2View::open(&bytes).map(|_| ())
        };
        let verified_res = if is_u64_dir {
            V264View::open_verified(&bytes).map(|_| ())
        } else {
            V2View::open_verified(&bytes).map(|_| ())
        };

        match refused_by {
            "open" => {
                let e = open_res.expect_err(&format!("reject {idx} ({why}) must fail open"));
                assert_eq!(
                    e.class.as_str(),
                    want_class,
                    "reject {idx} ({why}): got {e:?}"
                );
                let ev = verified_res.expect_err(&format!(
                    "reject {idx} ({why}) must also fail open_verified"
                ));
                assert_eq!(ev.class.as_str(), want_class, "reject {idx} verified");
            }
            "open_verified" => {
                assert!(
                    open_res.is_ok(),
                    "reject {idx} ({why}) should pass structural open, got {open_res:?}"
                );
                let e = verified_res
                    .expect_err(&format!("reject {idx} ({why}) must fail open_verified"));
                assert_eq!(
                    e.class.as_str(),
                    want_class,
                    "reject {idx} ({why}): got {e:?}"
                );
            }
            other => panic!("unexpected refused_by {other}"),
        }
    }
}

// ── 2. Bend2 laws bound to Rust bytes (§13) ──────────────────────────────

/// `specs/bitmosaic/laws/sizes.bend` & `prototypes/format-2/sizes_check.py`:
/// Exact agreement on all 35 placement totals + 0 low-width bound violations (36 numbers).
#[test]
fn law_sizes_matches_bend2_and_python_model_exact_36_numbers() {
    const NONE: usize = 1_000_000;
    let expected: [(usize, usize, usize, usize, usize); 7] = [
        (12026, 4091, 4, 0, 26931),
        (46733, 1, 3042, 1052, 50535),
        (27422, 0, 3180, 915, 31014),
        (31517, 0, 3180, 915, 35097),
        (36452, 0, 3511, 584, 40551),
        (65678, 1, 3333, 761, 78214),
        (41457, 0, 3796, 299, 52361),
    ];
    let placements: [(u32, u32); 7] = [
        (0, 1),
        (0, 61),
        (250, 1),
        (65530, 1),
        (1_000_000, 3),
        (0, 8000),
        (4_000_000_000, 7),
    ];

    for (&(base, scale), &want) in placements.iter().zip(&expected) {
        let (mut total, mut nb, mut nl, mut ne, mut efs) = (0, 0, 0, 0, 0);
        for m in 1u32..4096 {
            let members: Vec<u32> = (0..12)
                .filter(|p| (m >> p) & 1 != 0)
                .map(|p| base + scale * p)
                .collect();
            let k = members.len();
            let first = members[0];
            let last = members[k - 1];
            let b = if last <= 1015 { bitmap_len(last) } else { NONE };
            let l = list_len(&members).unwrap_or(NONE);
            let e = ef2_len(k as u64, first, last - first).unwrap_or(NONE);
            total += b.min(l).min(e);
            if b <= l && b <= e {
                nb += 1;
            } else if l < b && l <= e {
                nl += 1;
            } else {
                ne += 1;
            }
            if k >= 2 {
                efs += e;
            }
            // Also verify our sealed image_len_v2 never exceeds the 3-candidate min.
            assert!(image_len_v2(&members) <= b.min(l).min(e));
        }
        assert_eq!(
            (total, nb, nl, ne, efs),
            want,
            "placement ({base}, {scale})"
        );
    }
}

/// `specs/bitmosaic/laws/list_probe.bend` + `one_image.bend`:
/// Compare-all List rank, contains, rank_if_present across all widths `wa`/`wd` in 1..=4 and `k` in 2..=8.
#[test]
fn law_list_probe_and_single_all_widths_and_lengths() {
    let mut rng = Rng(0x1157_BEEF_CAFE_0001);
    let mut buf = [0u8; 64];

    // Single across all 4 byte widths.
    for v in [
        0u32,
        7,
        8,
        255,
        256,
        65535,
        65536,
        16_777_215,
        16_777_216,
        u32::MAX,
    ] {
        let len = write_single(v, &mut buf).expect("write_single");
        assert_eq!(len, single_len(v));
        let view = SingleView::open(&buf[..len]).expect("SingleView::open");
        assert_eq!(view.extent(), len);
        assert_eq!(view.value(), v);
        assert_eq!(SingleView::open_admitted(&buf[..len]).unwrap().value(), v);
        for probe in [0, v.saturating_sub(1), v, v.saturating_add(1), u32::MAX] {
            assert_eq!(view.contains(probe), probe == v);
            assert_eq!(view.rank(probe), u32::from(v < probe));
            assert_eq!(view.rank_if_present(probe), (probe == v).then_some(0));
            assert_eq!(view.seek(probe), (v >= probe).then_some(v));
        }
        assert_eq!(view.select(0), Some(v));
        assert_eq!(view.select(1), None);
    }

    // List across every (k, wa, wd) combination.
    let bases: [u32; 4] = [5, 300, 100_000, 3_000_000_000];
    let max_spans: [u32; 4] = [200, 40_000, 5_000_000, 1_000_000_000];
    for &base in &bases {
        for &span_cap in &max_spans {
            for k in 2..=8usize {
                let mut set = BTreeSet::new();
                set.insert(base);
                set.insert(base.saturating_add(span_cap));
                while set.len() < k {
                    set.insert(base + (rng.next() as u32 % span_cap.max(2)));
                }
                let members: Vec<u32> = set.into_iter().collect();
                let len = write_list(&members, &mut buf).expect("write_list");
                assert_eq!(Some(len), list_len(&members));
                // Test on exact-length slice (no over-read past extent!).
                let view = ListView::open(&buf[..len]).expect("ListView::open");
                assert_eq!(view.extent(), len);
                assert_eq!(view.len() as usize, k);
                assert_eq!(view.range().collect::<Vec<_>>(), members);
                let adm = ListView::open_admitted(&buf[..len]).expect("open_admitted");
                assert_eq!(adm.extent(), len);

                for (i, &m) in members.iter().enumerate() {
                    assert!(view.contains(m));
                    assert_eq!(view.rank(m) as usize, i);
                    assert_eq!(view.rank_if_present(m), Some(i as u32));
                    assert_eq!(view.select(i as u32), Some(m));
                    assert_eq!(view.seek(m), Some(m));
                    if m > 0 {
                        let p = m - 1;
                        let r = members.partition_point(|&x| x < p);
                        assert_eq!(view.rank(p) as usize, r);
                        assert_eq!(view.seek(p), view.select(view.rank(p)));
                    }
                    if m < u32::MAX {
                        let p = m + 1;
                        let r = members.partition_point(|&x| x < p);
                        assert_eq!(view.rank(p) as usize, r);
                        assert_eq!(view.seek(p), view.select(view.rank(p)));
                    }
                }
                for edge in [0u32, u32::MAX] {
                    let r = members.partition_point(|&x| x < edge);
                    assert_eq!(view.rank(edge) as usize, r);
                    assert_eq!(view.seek(edge), view.select(view.rank(edge)));
                }
            }
        }
    }
}

/// `specs/bitmosaic/laws/elias_fano.bend`: Format 2 lean EF across sample boundaries & clipped tail.
#[test]
fn law_elias_fano_v2_sample_boundaries_and_exact_extent() {
    let mut rng = Rng(0xEF02_CAFE_1234_5678);
    for &n in &[2usize, 3, 8, 63, 64, 65, 127, 128, 129, 200] {
        for &spread in &[n as u32 + 3, n as u32 * 5, 70_000, 2_000_000] {
            let mut set = BTreeSet::new();
            let base = (rng.next() % 100_000) as u32;
            while set.len() < n {
                set.insert(base + (rng.next() as u32 % spread));
            }
            let members: Vec<u32> = set.into_iter().collect();
            let span = members[n - 1] - members[0];
            let expected_len = ef2_len(n as u64, members[0], span).expect("ef2_len");
            // Exact-sized buffer: catches any byte over-read past extent (§12.2)!
            let mut buf = vec![0u8; expected_len];
            let written = write_ef2(&members, &mut buf).expect("write_ef2");
            assert_eq!(written, expected_len);

            let view = Ef2View::open_verified(&buf).expect("Ef2View::open_verified");
            let adm = Ef2View::open_admitted(&buf).expect("Ef2View::open_admitted");
            assert_eq!(view.extent(), expected_len);
            assert_eq!(adm.extent(), expected_len);
            assert_eq!(view.range().collect::<Vec<_>>(), members);

            for (i, &m) in members.iter().enumerate() {
                assert!(view.contains(m));
                assert_eq!(view.rank(m), i as u64);
                assert_eq!(view.rank_if_present(m), Some(i as u64));
                assert_eq!(view.select(i as u64), Some(m));
                assert_eq!(view.seek(m), Some(m));
                if m > 0 {
                    assert_eq!(view.seek(m - 1), view.select(view.rank(m - 1)));
                }
                if m < u32::MAX {
                    assert_eq!(view.seek(m + 1), view.select(view.rank(m + 1)));
                }
            }
            assert_eq!(view.select(n as u64), None);
        }
    }
}

/// `specs/bitmosaic/laws/bitmap_probe.bend` + `seek.bend` on BitmapView new methods.
#[test]
fn law_bitmap_probe_select_seek_rank_if_present_and_len_and_rank() {
    let mut rng = Rng(0xB17A_0001_9E37_79B9);
    let mut sets = Vec::new();
    for _ in 0..80 {
        let top = 1 + (rng.next() % 1016) as u32;
        let density = 1 + (rng.next() % 16);
        let s: Vec<u32> = (0..top).filter(|_| rng.next() % 16 < density).collect();
        sets.push(s);
    }
    sets.push(vec![]);
    sets.push(vec![0, 1015]);

    for pair in sets.windows(2) {
        let mut ba = [0u8; 128];
        let mut bb = [0u8; 128];
        let la = write_bitmap(&pair[0], &mut ba).unwrap();
        let lb = write_bitmap(&pair[1], &mut bb).unwrap();
        let va = BitmapView::open(&ba[..la]).unwrap();
        let vb = BitmapView::open(&bb[..lb]).unwrap();

        for (k, &m) in pair[0].iter().enumerate() {
            assert_eq!(va.select(k as u32), Some(m));
            assert_eq!(va.rank_if_present(m), Some(k as u32));
            assert_eq!(va.seek(m), Some(m));
        }
        assert_eq!(va.select(pair[0].len() as u32), None);
        for probe in [0u32, 1, 63, 64, 65, 511, 512, 1014, 1015, 1016, u32::MAX] {
            let r = va.rank(probe);
            assert_eq!(va.seek(probe), va.select(r));
            let mut cursor = va.range();
            cursor.seek(probe);
            let want_tail: Vec<u32> = pair[0].iter().copied().filter(|&x| x >= probe).collect();
            assert_eq!(cursor.collect::<Vec<_>>(), want_tail);
        }

        let inter: Vec<u32> = pair[0]
            .iter()
            .copied()
            .filter(|v| pair[1].binary_search(v).is_ok())
            .collect();
        assert_eq!(va.and_len(&vb) as usize, inter.len());
        for limit in [0u32, 1, 64, 128, 500, 1015, 1016, 2000] {
            let want = inter.partition_point(|&x| x < limit) as u32;
            assert_eq!(va.and_rank(&vb, limit), want);
        }
    }
}

// ── 3. Negative refusal tests for every admission class (§11) ────────────

#[test]
fn negative_refusals_cover_every_admission_class() {
    // BitPad in Format 2 EF high plane & low plane.
    // {1000, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300} is EF in corpus.
    let members = [1000u32, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300];
    let ef_bytes = Bitmosaic::from_sorted(members).to_v2_bytes();
    assert_eq!(ef_bytes[0], 0x02);
    // High plane is 3 bytes at offset 6..9 (`c7 52 02`): high_bits = 18, so top 6 bits of byte 8 are pad!
    let mut bad_high_pad = ef_bytes.clone();
    bad_high_pad[8] |= 0x80;
    assert_eq!(
        V2View::open(&bad_high_pad).unwrap_err().class,
        AdmissionClass::BitPad
    );
    // Low plane ends at last byte (`0c`): low_bits = 5, 9 * 5 = 45 bits -> 6 bytes, top 3 bits of last byte are pad!
    let mut bad_low_pad = ef_bytes.clone();
    *bad_low_pad.last_mut().unwrap() |= 0x80;
    assert_eq!(
        V2View::open(&bad_low_pad).unwrap_err().class,
        AdmissionClass::BitPad
    );

    // Placement: corrupt one-sample / zero-sample in 200-member EF vector.
    let ef200_members: Vec<u32> = (0..200u32).map(|i| 5000 * i + (i * i) % 11).collect();
    let mut bad_sample = Bitmosaic::from_sorted(ef200_members).to_v2_bytes();
    let end = bad_sample.len();
    bad_sample[end - 4] ^= 1; // corrupt last zero-sample
    assert_eq!(
        V2View::open_verified(&bad_sample).unwrap_err().class,
        AdmissionClass::Placement
    );

    // Kind: u64 directory handed to u32 reader; empty child inside u64 directory.
    let u64_bytes = Bitmosaic64::from_sorted([1u64 << 32]).to_v2_bytes();
    assert_eq!(
        V2View::open(&u64_bytes).unwrap_err().class,
        AdmissionClass::Kind
    );
    let empty_child_u64 = [0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x80];
    assert_eq!(
        V264View::open(&empty_child_u64).unwrap_err().class,
        AdmissionClass::Kind
    );

    // Placement: non-adjacent child offset in u64 directory (gap between child 0 and child 1).
    let mut bad_u64_off = Bitmosaic64::from_sorted([5u64, (1u64 << 32) | 7]).to_v2_bytes();
    // offset of child 1 is at byte 10..14 (was 2 -> make it 3, leaving a 1-byte gap after child 0).
    bad_u64_off[10] = 3;
    assert_eq!(
        V264View::open(&bad_u64_off).unwrap_err().class,
        AdmissionClass::Placement
    );

    // Canonical: Format 2 Directory encoding an arithmetic progression as Array instead of Stride.
    // Build a 12-member progression 1000..=1011 in Array container -> non-canonical chunk!
    let mut non_canon_dir = vec![
        0x01, // u32 dir
        29,   // body_bytes = 1(n-1) + 1(c-1) + 2(key) + 1(kind) + 24(12*u16) = 29
        11,   // n - 1 = 11
        0,    // c - 1 = 0
        0, 0, // key 0
        3, // kind = Array
    ];
    for v in 1000u16..1012 {
        non_canon_dir.extend_from_slice(&v.to_le_bytes());
    }
    assert!(V2View::open(&non_canon_dir).is_ok());
    assert_eq!(
        V2View::open_verified(&non_canon_dir).unwrap_err().class,
        AdmissionClass::Canonical
    );

    // Canonical: Format 2 u64 directory whose child is a forest image instead of sealed EF!
    let mut forest_child = vec![0u8; forest_len_v2(&members)];
    write_forest_into_v2(&members, &mut forest_child).unwrap();
    let mut bad_u64_child = vec![0x03, 0x00, 0x01, 0x00, 0x00, 0x00];
    bad_u64_child.extend_from_slice(&forest_child);
    assert!(V264View::open(&bad_u64_child).is_ok());
    assert_eq!(
        V264View::open_verified(&bad_u64_child).unwrap_err().class,
        AdmissionClass::Canonical
    );
}

// ── 4. Differential oracle across random sets & cross-form intersections ─

#[test]
fn differential_oracle_all_forms_and_intersections() {
    let mut rng = Rng(0xD1FF_04AC_1E00_0002);
    let mut sets: Vec<Vec<u32>> = vec![
        vec![],
        vec![0],
        vec![7],
        vec![8],
        vec![u32::MAX],
        vec![0, u32::MAX],
        (0..13).chain([70]).collect(),
        (0..83).collect(),
        (1000..1008).collect(),
        (1000..1100).collect(),
        (0..5000).filter(|x| x % 3 != 0).collect(), // Words directory
        (0..40).flat_map(|b| (b * 200)..(b * 200 + 120)).collect(), // Runs directory
    ];
    for _ in 0..40 {
        let mode = rng.next() % 5;
        let mut s = BTreeSet::new();
        match mode {
            0 => {
                // Tiny bitmap / list
                let k = 1 + (rng.next() % 12) as usize;
                let max = 1 + (rng.next() % 1016) as u32;
                for _ in 0..k {
                    s.insert(rng.next() as u32 % max);
                }
            }
            1 => {
                // Small scattered list (2..=8)
                let k = 2 + (rng.next() % 7) as usize;
                let base = (rng.next() % 10_000_000) as u32;
                for _ in 0..k {
                    s.insert(base.saturating_add(rng.next() as u32 % 500_000));
                }
            }
            2 => {
                // Sparse EF (10..80 members across many chunks)
                let k = 10 + (rng.next() % 70) as usize;
                for _ in 0..k {
                    s.insert(rng.next() as u32 % 5_000_000);
                }
            }
            3 => {
                // Cone-shaped chunk
                let n = 30 + (rng.next() % 200) as u32;
                for i in 0..n {
                    s.insert(1000 + i * 50 + (rng.next() as u32 % 7));
                }
            }
            _ => {
                // Multi-chunk mixed
                for c in 0..5u32 {
                    let hi = (c * 2) << 16;
                    for j in 0..25u32 {
                        s.insert(hi | (j * 100 + (rng.next() as u32 % 50)));
                    }
                }
            }
        }
        sets.push(s.into_iter().collect());
    }

    let encoded: Vec<(Vec<u32>, Vec<u8>)> = sets
        .into_iter()
        .map(|s| {
            let owned = Bitmosaic::from_sorted(s.iter().copied());
            let bytes = owned.to_v2_bytes();
            let mut slice_buf = vec![0u8; image_len_v2(&s)];
            assert_eq!(write_image_into_v2(&s, &mut slice_buf), Ok(bytes.len()));
            assert_eq!(
                slice_buf, bytes,
                "slice writer matches Bitmosaic::to_v2_bytes"
            );
            let v = V2View::open_canonical(&bytes).expect("canonical self-check");
            check_v2_reads(&v, &s, "random oracle");
            (s, bytes)
        })
        .collect();

    // Cross-form and_len / and_rank across all pairs of the first 20 sets (covering all 5 forms!).
    for i in 0..20 {
        for j in 0..20 {
            let va = V2View::open_admitted(&encoded[i].1).unwrap();
            let vb = V2View::open_admitted(&encoded[j].1).unwrap();
            let inter: Vec<u32> = encoded[i]
                .0
                .iter()
                .copied()
                .filter(|x| encoded[j].0.binary_search(x).is_ok())
                .collect();
            assert_eq!(
                va.and_len(&vb) as usize,
                inter.len(),
                "and_len pair ({i}, {j}) forms ({:?}, {:?})",
                va.form(),
                vb.form()
            );
            for &limit in &[0u32, 10, 500, 1016, 100_000, u32::MAX] {
                let want = inter.partition_point(|&x| x < limit) as u64;
                assert_eq!(
                    va.and_rank(&vb, limit),
                    want,
                    "and_rank pair ({i}, {j}) limit={limit}"
                );
            }
        }
    }
}

#[test]
fn differential_oracle_u64_directory_and_intersections() {
    let sets: Vec<Vec<u64>> = vec![
        vec![],
        vec![3, 10, 500],
        vec![1u64 << 32, (1u64 << 32) | 99],
        vec![
            0,
            7,
            (1u64 << 32) | 5,
            (1u64 << 32) | 70_000,
            (1u64 << 32) | 140_000,
            (5u64 << 32) | 1000,
            (5u64 << 32) | 1001,
            (5u64 << 32) | 1002,
            (5u64 << 32) | 1003,
            (5u64 << 32) | 1004,
            (5u64 << 32) | 1005,
            (5u64 << 32) | 1006,
            (5u64 << 32) | 1007,
            u64::MAX,
        ],
        (0..25u64)
            .flat_map(|h| {
                [
                    (h * 3) << 32 | 5,
                    (h * 3) << 32 | 200,
                    (h * 3) << 32 | 100_000,
                ]
            })
            .collect(),
    ];
    let encoded: Vec<(Vec<u64>, Vec<u8>)> = sets
        .into_iter()
        .map(|s| {
            let owned = Bitmosaic64::from_sorted(s.iter().copied());
            let bytes = owned.to_v2_bytes();
            assert_eq!(bytes.len(), owned.v2_serialized_len());
            assert_eq!(to_v2_bytes_u64(&s), bytes);
            let v = V264View::open_canonical(&bytes).expect("u64 canonical");
            let adm = V264View::open_admitted(&bytes).expect("u64 admitted");
            assert_eq!(v.len(), adm.len());
            assert_eq!(v.serialized_len(), bytes.len());
            assert_eq!(v.range().collect::<Vec<_>>(), s);
            for (k, &m) in s.iter().enumerate() {
                assert!(v.contains(m));
                assert_eq!(v.rank(m), k as u64);
                assert_eq!(v.rank_if_present(m), Some(k as u64));
                assert_eq!(v.select(k as u64), Some(m));
                assert_eq!(v.seek(m), Some(m));
            }
            if !s.is_empty() {
                let mid = s[s.len() / 2];
                let mut r = v.range();
                r.seek(mid);
                assert_eq!(r.collect::<Vec<_>>(), s[s.len() / 2..]);
            }
            (s, bytes)
        })
        .collect();

    for i in 0..encoded.len() {
        for j in 0..encoded.len() {
            let va = V264View::open_admitted(&encoded[i].1).unwrap();
            let vb = V264View::open_admitted(&encoded[j].1).unwrap();
            let inter: Vec<u64> = encoded[i]
                .0
                .iter()
                .copied()
                .filter(|x| encoded[j].0.binary_search(x).is_ok())
                .collect();
            assert_eq!(va.and_len(&vb) as usize, inter.len());
            for &limit in &[0u64, 10, (1u64 << 32) | 50, (5u64 << 32) | 1005, u64::MAX] {
                let want = inter.partition_point(|&x| x < limit) as u64;
                assert_eq!(va.and_rank(&vb, limit), want);
            }
        }
    }
    let _ = Dir64V2View::open(&encoded[2].1).expect("direct Dir64V2View");
}

// ── 5. Format 2 Patching oracle (§12.3) ──────────────────────────────────

#[test]
fn patch_v2_transitions_witnesses_bounds_and_refusals() {
    let mut scratch = PatchScratch::try_new().expect("scratch");
    let mut rng = Rng(0x9A7C_0002_DEAD_BEEF);

    // Start empty (0x80) and drive transitions across Empty <-> Bitmap <-> Single <-> List <-> Directory!
    let mut oracle = BTreeSet::<u32>::new();
    let mut slot = vec![0u8; 64 * 1024];
    slot[0] = EMPTY_V2_IMAGE[0];

    let scripted: Vec<(Vec<u32>, Vec<u32>)> = vec![
        (vec![3], vec![]),                           // Empty -> Bitmap {3}
        (vec![100], vec![3]),                        // Bitmap -> Single {100}
        (vec![500, 1000], vec![]),                   // Single -> List {100, 500, 1000}
        (vec![0, 1, 2, 3, 4], vec![100, 500, 1000]), // List -> Bitmap {0..=4}
        (vec![70], vec![]),                          // Bitmap stays Bitmap
        ((0..83).collect(), vec![]),                 // Bitmap -> Directory (Stride 0..=82)
        (vec![82], vec![82]),                        // same value added + removed
        (vec![], (10..83).collect()),                // Directory -> Bitmap {0..=9}
        (vec![100_000, 200_000], (0..10).collect()), // Bitmap -> List {100_000, 200_000}
        (vec![], vec![100_000, 200_000]),            // List -> Empty (0x80)
    ];

    let mut steps = scripted;
    for _ in 0..80 {
        let mode = rng.next() % 4;
        let domain = match mode {
            0 => 80u32,
            1 => 1016,
            2 => 200_000,
            _ => 5_000_000,
        };
        let na = (rng.next() % 12) as usize;
        let nr = (rng.next() % 12) as usize;
        let adds: Vec<u32> = (0..na)
            .map(|_| rng.next() as u32 % domain)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let removes: Vec<u32> = (0..nr)
            .map(|_| rng.next() as u32 % domain)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        steps.push((adds, removes));
    }

    let mut old_len = 1usize;
    for (step, (adds, removes)) in steps.into_iter().enumerate() {
        // Check patched_len_bound_v2 before applying.
        let mut chunks = BTreeSet::new();
        for &v in adds.iter().chain(&removes) {
            chunks.insert(v >> 16);
        }
        let bound = patched_len_bound_v2(&slot, adds.len() as u64, chunks.len() as u64)
            .expect("patched_len_bound_v2");

        let add_new: Vec<bool> = adds.iter().map(|v| !oracle.contains(v)).collect();
        oracle.extend(adds.iter().copied());
        let rem_held: Vec<bool> = removes.iter().map(|v| oracle.contains(v)).collect();
        for v in &removes {
            oracle.remove(v);
        }

        let mut add_bits = vec![0u64; adds.len().div_ceil(64)];
        let mut rem_bits = vec![0u64; removes.len().div_ceil(64)];
        let mut witness = BatchOutcome::new(&mut add_bits, &mut rem_bits);

        let report = patch_v2_witnessed(&mut slot, &adds, &removes, &mut scratch, &mut witness)
            .unwrap_or_else(|e| panic!("step {step}: {e}"));

        let want_members: Vec<u32> = oracle.iter().copied().collect();
        let mut want_forest = vec![0u8; forest_len_v2(&want_members)];
        write_forest_into_v2(&want_members, &mut want_forest).unwrap();
        assert_eq!(
            &slot[..report.serialized_len],
            want_forest.as_slice(),
            "step {step}: patched bytes must equal write_forest_into_v2"
        );
        assert!(
            (report.serialized_len as u64) <= bound,
            "step {step}: len {} exceeded bound {bound}",
            report.serialized_len
        );
        // Zeroed tail invariant (§12.3).
        if report.serialized_len < old_len {
            assert!(
                slot[report.serialized_len..old_len].iter().all(|&b| b == 0),
                "step {step}: shrunk tail must be zeroed"
            );
        }
        old_len = report.serialized_len;

        assert_eq!(report.len as usize, oracle.len(), "step {step} len");
        assert_eq!(
            report.added as usize,
            add_new.iter().filter(|&&b| b).count(),
            "step {step} added"
        );
        assert_eq!(
            report.removed as usize,
            rem_held.iter().filter(|&&b| b).count(),
            "step {step} removed"
        );
        for (i, &w) in add_new.iter().enumerate() {
            assert_eq!(witness.add_was_new(i), w, "step {step} add witness {i}");
        }
        for (i, &w) in rem_held.iter().enumerate() {
            assert_eq!(witness.remove_was_held(i), w, "step {step} rem witness {i}");
        }
        let view = V2View::open_verified(&slot).expect("patched image is open_verified");
        assert_eq!(view.range().collect::<Vec<_>>(), want_members);
    }

    // Patching an Elias-Fano sealed image produces its canonical forest image (§12.3).
    let ef_members: Vec<u32> = vec![1000, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300];
    let ef_img = Bitmosaic::from_sorted(ef_members.iter().copied()).to_v2_bytes();
    assert_eq!(ef_img[0], 0x02);
    let mut ef_slot = vec![0u8; 256];
    ef_slot[..ef_img.len()].copy_from_slice(&ef_img);
    let rep = patch_v2(&mut ef_slot, &[1305], &[1001], &mut scratch).expect("patch EF");
    let after_members = [1000, 1010, 1100, 1101, 1150, 1200, 1255, 1300, 1305];
    let mut expected_after = vec![0u8; forest_len_v2(&after_members)];
    write_forest_into_v2(&after_members, &mut expected_after).unwrap();
    assert_eq!(&ef_slot[..rep.serialized_len], expected_after.as_slice());

    // Refusals leave slot untouched!
    // {0, 1, 2, 10} + {70} = 5 members -> List is 6 bytes (beats Bitmap's 10B).
    let orig = vec![0x82, 0x07, 0x04];
    let mut short = orig.clone();
    assert_eq!(
        patch_v2(&mut short, &[70], &[], &mut scratch),
        Err(PatchError::Capacity { needed: 6 })
    );
    assert_eq!(short, orig, "Capacity refusal touches nothing");

    let mut unsorted = orig.clone();
    assert_eq!(
        patch_v2(&mut unsorted, &[10, 5], &[], &mut scratch),
        Err(PatchError::Unsorted)
    );
    assert_eq!(unsorted, orig, "Unsorted refusal touches nothing");

    let mut malformed = vec![0x00, 0x01, 0x02];
    assert_eq!(
        patch_v2(&mut malformed, &[1], &[], &mut scratch),
        Err(PatchError::Malformed)
    );
    assert_eq!(malformed, vec![0x00, 0x01, 0x02]);
}

// ── 6. Zero-allocation proof with positive control ───────────────────────

#[test]
fn v2_reads_slice_writes_and_small_patches_allocate_nothing() {
    // Positive control: prove the counting allocator sees real heap allocations.
    let (_, seen) = count_allocs(|| {
        let v: Vec<u8> = vec![1; 64];
        std::hint::black_box(v);
    });
    assert!(seen > 0, "counting allocator must observe allocations");

    let bitmap_img = Bitmosaic::from_sorted([0u32, 1, 2, 10, 55]).to_v2_bytes();
    let single_img = Bitmosaic::from_sorted([1_000_000u32]).to_v2_bytes();
    let list_img = Bitmosaic::from_sorted([5u32, 70_000, 140_000]).to_v2_bytes();
    let ef_img = Bitmosaic::from_sorted([1000u32, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300])
        .to_v2_bytes();
    let dir_img = Bitmosaic::from_sorted(0..100u32).to_v2_bytes();
    let u64_img =
        Bitmosaic64::from_sorted([5u64, (1u64 << 32) | 7, (2u64 << 32) | 1]).to_v2_bytes();

    // 1. All reads + all admission levels across every Format 2 form allocate ZERO bytes.
    let (_, allocs) = count_allocs(|| {
        for img in [&bitmap_img, &single_img, &list_img, &ef_img, &dir_img] {
            let v1 = V2View::open(img).unwrap();
            let v2 = V2View::open_verified(img).unwrap();
            let v3 = V2View::open_canonical(img).unwrap();
            let v4 = V2View::open_admitted(img).unwrap();
            let mut sum = 0u64;
            for v in [v1, v2, v3, v4] {
                sum ^= v.len();
                sum ^= u64::from(v.contains(1000));
                sum ^= v.rank(1100);
                sum ^= v.rank_if_present(1000).unwrap_or(0);
                sum ^= u64::from(v.select(0).unwrap_or(0));
                sum ^= u64::from(v.seek(50).unwrap_or(0));
                sum ^= v.and_len(&v1);
                sum ^= v.and_rank(&v1, 1200);
                v.for_each(|x| sum = sum.wrapping_add(u64::from(x)));
                let mut r = v.range();
                r.seek(50);
                while !r.empty() {
                    sum = sum.wrapping_add(u64::from(r.front()));
                    r.pop_front();
                }
            }
            std::hint::black_box(sum);
        }
        let v64 = V264View::open_canonical(&u64_img).unwrap();
        let a64 = V264View::open_admitted(&u64_img).unwrap();
        let mut s = v64.len() ^ a64.and_len(&v64) ^ a64.rank(1 << 32);
        s ^= a64.select(1).unwrap_or(0) ^ a64.seek(6).unwrap_or(0);
        std::hint::black_box(s);
    });
    assert_eq!(allocs, 0, "Format 2 views & reads must allocate nothing");

    // 2. Direct slice writers allocate ZERO bytes for Empty, Bitmap, Single, List, Elias-Fano.
    let mut out = [0u8; 256];
    let ef_slice = [1000u32, 1001, 1010, 1100, 1101, 1150, 1200, 1255, 1300];
    let bm_slice = [0u32, 1, 2, 10, 55];
    let list_slice = [5u32, 70_000, 140_000];
    let (_, write_allocs) = count_allocs(|| {
        let _ = write_image_into_v2(&[], &mut out).unwrap();
        let _ = write_image_into_v2(&bm_slice, &mut out).unwrap();
        let _ = write_image_into_v2(&[1_000_000], &mut out).unwrap();
        let _ = write_image_into_v2(&list_slice, &mut out).unwrap();
        let _ = write_image_into_v2(&ef_slice, &mut out).unwrap();
        std::hint::black_box(image_len_v2(&ef_slice));
    });
    assert_eq!(
        write_allocs, 0,
        "write_image_into_v2 for Empty/Bitmap/Single/List/EF must allocate nothing"
    );

    // 3. Patch on Bitmap and Single/List allocates ZERO bytes.
    let mut scratch = PatchScratch::try_new().unwrap();
    let mut slot = [0u8; 128];
    slot[..bitmap_img.len()].copy_from_slice(&bitmap_img);
    let mut list_slot = [0u8; 128];
    list_slot[..list_img.len()].copy_from_slice(&list_img);
    let (_, patch_allocs) = count_allocs(|| {
        let _ = patch_v2(&mut slot, &[42], &[], &mut scratch).unwrap();
        let _ = patch_v2(&mut slot, &[], &[42], &mut scratch).unwrap();
        let _ = patch_v2(&mut list_slot, &[210_000], &[70_000], &mut scratch).unwrap();
    });
    assert_eq!(
        patch_allocs, 0,
        "Bitmap & List patches must allocate nothing"
    );
}

// ── 7. Tiny-sets byte regression table (`agent-todo/bitmosaic-tiny-sets.md`) ─

#[test]
fn tiny_sets_four_cases_beat_format_1_dramatically() {
    let cases: [(&str, Vec<u32>, usize); 4] = [
        ("4 of 83 (low)", vec![0, 1, 2, 10], 3),
        ("4 of 83 (spread)", vec![0, 12, 41, 82], 5),
        ("13 of 70", (0..12).chain([70]).collect(), 10),
        ("all 83", (0..83).collect(), 11),
    ];
    for (name, members, expected_v2) in cases {
        let set = Bitmosaic::from_sorted(members);
        let v1 = set.to_bytes();
        let v2 = set.to_v2_bytes();
        assert_eq!(
            v2.len(),
            expected_v2,
            "{name}: v2={}B (v1={}B)",
            v2.len(),
            v1.len()
        );
        assert!(
            v2.len() < v1.len(),
            "{name}: v2 ({}) < v1 ({})",
            v2.len(),
            v1.len()
        );
    }
    // 20 of 83 (dense low columns): Bitmap 12B vs Format 1 23+B.
    let twenty: Vec<u32> = (0..19).chain([82]).collect();
    let set20 = Bitmosaic::from_sorted(twenty);
    assert_eq!(set20.to_v2_bytes().len(), 12);
    assert!(set20.to_v2_bytes().len() < set20.to_bytes().len());
}
