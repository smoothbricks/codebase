use core::ops::ControlFlow;
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bitmosaic::{AdmissionClass, EMPTY_MAPLET_IMAGE, Maplet, MapletView};

struct CountingAlloc;

static TRACKING: AtomicBool = AtomicBool::new(false);
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_LOCK: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn count_allocs<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let _guard = ALLOC_LOCK.lock().unwrap();
    ALLOC_COUNT.store(0, Ordering::SeqCst);
    TRACKING.store(true, Ordering::SeqCst);
    let out = f();
    TRACKING.store(false, Ordering::SeqCst);
    let n = ALLOC_COUNT.load(Ordering::SeqCst);
    (out, n)
}

fn collect_live(m: &Maplet, key: u32) -> Vec<u16> {
    let mut out = Vec::new();
    let _ = m.for_each_match::<()>(key, |v| {
        out.push(v);
        ControlFlow::Continue(())
    });
    out
}

fn collect_view(v: &MapletView<'_>, key: u32) -> Vec<u16> {
    let mut out = Vec::new();
    let _ = v.for_each_match::<()>(key, |val| {
        out.push(val);
        ControlFlow::Continue(())
    });
    out
}

#[test]
fn empty_maplet_roundtrip_and_two_byte_image() {
    let m = Maplet::new();
    assert!(m.is_empty());
    assert_eq!(m.len(), 0);
    assert_eq!(m.get(0), None);
    assert_eq!(m.get(42), None);

    let sealed = m.seal();
    assert_eq!(sealed.as_slice(), &EMPTY_MAPLET_IMAGE);
    assert_eq!(sealed.len(), 2);

    let view = MapletView::open(&sealed).expect("valid empty maplet");
    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
    assert_eq!(view.key_count(), 0);
    assert!(!view.is_multi_value());
    assert_eq!(view.extent(), 2);
    assert_eq!(view.get(0), None);
    assert!(collect_view(&view, 99).is_empty());

    let admitted = MapletView::open_admitted(&sealed).expect("admitted empty maplet");
    assert_eq!(admitted.extent(), 2);
    assert!(admitted.is_empty());

    let promoted = view.promote();
    assert!(promoted.is_empty());
    assert_eq!(promoted.seal(), sealed);
}

#[test]
fn tiny_and_singleton_byte_sizes_and_roundtrip() {
    // 1 entry with small key (u8 val): Bitmap(2B) + 2B hdr + 1B uleb + 1B val = 6B!
    let mut m1 = Maplet::new();
    m1.insert(3, 7);
    let s1 = m1.seal();
    assert!(
        s1.len() <= 7,
        "tiny singleton should be <= 7B, got {}",
        s1.len()
    );
    let v1 = MapletView::open(&s1).unwrap();
    assert_eq!(v1.get(3), Some(7));
    assert_eq!(v1.get(4), None);
    assert_eq!(v1.promote().seal(), s1);

    // 1 entry with 32-bit fingerprint key (u8 val): Single(5B) + 2B hdr + 1B uleb + 1B val = 9B
    let mut m1_wide = Maplet::new();
    m1_wide.insert(0xDEAD_BEEF, 42);
    let s1w = m1_wide.seal();
    assert_eq!(s1w.len(), 9);
    let v1w = MapletView::open(&s1w).unwrap();
    assert_eq!(v1w.get(0xDEAD_BEEF), Some(42));
    assert_eq!(v1w.promote().seal(), s1w);

    // 4 small reducer-slot entries (u8 vals)
    let mut m4 = Maplet::new();
    for (k, v) in [(10u32, 1u16), (25, 2), (40, 3), (82, 4)] {
        m4.insert(k, v);
    }
    let s4 = m4.seal();
    assert!(
        s4.len() <= 16,
        "4-entry maplet should be <= 16B, got {}",
        s4.len()
    );
    let v4 = MapletView::open(&s4).unwrap();
    assert_eq!(v4.len(), 4);
    assert_eq!(v4.key_count(), 4);
    assert!(!v4.is_multi_value());
    for (k, v) in [(10u32, 1u16), (25, 2), (40, 3), (82, 4)] {
        assert_eq!(v4.get(k), Some(v));
    }
    assert_eq!(v4.promote().seal(), s4);
}

#[test]
fn multi_value_newest_first_single_and_multi_key_and_u16_max() {
    // Case A: single key with multiple values (0 offset bytes).
    let mut single_k = Maplet::new();
    single_k.insert(777, 10);
    single_k.insert(777, 20);
    single_k.insert(777, 30);
    single_k.insert(777, 10); // exact duplicate -> deduplicated
    assert_eq!(single_k.len(), 3);
    assert_eq!(single_k.get(777), Some(30));
    assert_eq!(collect_live(&single_k, 777), vec![30, 20, 10]);

    let s = single_k.seal();
    let v = MapletView::open(&s).unwrap();
    assert!(v.is_multi_value());
    assert_eq!(v.key_count(), 1);
    assert_eq!(v.len(), 3);
    assert_eq!(v.get(777), Some(30));
    assert_eq!(collect_view(&v, 777), vec![30, 20, 10]);

    // Early break stops on newest value.
    let mut seen = Vec::new();
    let brk = v.for_each_match(777, |val| {
        seen.push(val);
        ControlFlow::Break(val)
    });
    assert_eq!(brk, ControlFlow::Break(30));
    assert_eq!(seen, vec![30]);

    let promoted = v.promote();
    assert_eq!(collect_live(&promoted, 777), vec![30, 20, 10]);
    assert_eq!(promoted.seal(), s);

    // Case B: multi-key with u16::MAX (spills in live Maplet, exact in MapletView).
    let mut multi = Maplet::new();
    multi.insert(100, 5);
    multi.insert(100, u16::MAX);
    multi.insert(200, 300);
    multi.insert(300, 1);
    multi.insert(300, 2);
    multi.insert(300, 4000);
    assert_eq!(multi.len(), 6);
    assert_eq!(multi.get(100), Some(u16::MAX));
    assert_eq!(collect_live(&multi, 100), vec![u16::MAX, 5]);
    assert_eq!(collect_live(&multi, 300), vec![4000, 2, 1]);

    let sm = multi.seal();
    let vm = MapletView::open(&sm).unwrap();
    let vm_adm = MapletView::open_admitted(&sm).unwrap();
    assert!(vm.is_multi_value());
    assert_eq!(vm.key_count(), 3);
    assert_eq!(vm.len(), 6);
    assert_eq!(vm.extent(), sm.len());
    assert_eq!(vm_adm.extent(), sm.len());
    assert_eq!(collect_view(&vm, 100), vec![u16::MAX, 5]);
    assert_eq!(collect_view(&vm_adm, 100), vec![u16::MAX, 5]);
    assert_eq!(collect_view(&vm, 200), vec![300]);
    assert_eq!(collect_view(&vm, 300), vec![4000, 2, 1]);
    assert_eq!(vm.get(999), None);

    let pm = vm.promote();
    assert_eq!(collect_live(&pm, 100), vec![u16::MAX, 5]);
    assert_eq!(collect_live(&pm, 200), vec![300]);
    assert_eq!(collect_live(&pm, 300), vec![4000, 2, 1]);
    assert_eq!(pm.seal(), sm);

    // Clear reuses capacity and resets cleanly.
    let mut cleared = pm;
    cleared.clear();
    assert!(cleared.is_empty());
    assert_eq!(cleared.get(100), None);
    assert_eq!(cleared.seal(), EMPTY_MAPLET_IMAGE);
}

#[test]
fn differential_oracle_unique_and_multi_value_across_scales() {
    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    let mut rng = 0x51A0_CAFE_BABE_1337u64;
    let configs: &[(usize, u32, u16)] = &[
        (1, 100, 10),
        (8, 1_000, 200),
        (64, 50, 500),             // heavy multi-value + u16 vals
        (400, 300, 255),           // multi-value crossing N > 255 offset width boundary + u8 vals
        (2_048, u32::MAX, 200),    // sparse 32-bit fingerprints, u8 vals -> Ef2View
        (4_096, u32::MAX, 60_000), // sparse 32-bit fingerprints, u16 vals -> Ef2View
    ];

    for &(count, key_mod, val_max) in configs {
        let mut live = Maplet::new();
        let mut oracle: BTreeMap<u32, Vec<u16>> = BTreeMap::new();

        for _ in 0..count {
            let raw_k = (xorshift(&mut rng) >> 16) as u32;
            let k = if key_mod == u32::MAX {
                raw_k
            } else {
                raw_k % key_mod
            };
            let v = (xorshift(&mut rng) % (u64::from(val_max) + 1)) as u16;
            live.insert(k, v);
            let slot = oracle.entry(k).or_default();
            if !slot.contains(&v) {
                slot.push(v);
            }
        }

        let total_expected: usize = oracle.values().map(Vec::len).sum();
        assert_eq!(live.len(), total_expected);

        // Live Maplet: zero false negatives for every inserted (k, v).
        for (&k, vals) in &oracle {
            let got = collect_live(&live, k);
            for &v in vals {
                assert!(
                    got.contains(&v),
                    "live Maplet false negative for key={k}, val={v}"
                );
            }
        }

        // Sealed MapletView: exact match on every present and absent key.
        let mut buf = Vec::new();
        let written = live.seal_into(&mut buf);
        assert_eq!(written, buf.len());

        let view = MapletView::open(&buf).expect("canonical sealed image must admit");
        let admitted = MapletView::open_admitted(&buf).expect("open_admitted must succeed");
        assert_eq!(view.extent(), buf.len());
        assert_eq!(admitted.extent(), buf.len());
        assert_eq!(view.len(), total_expected);
        assert_eq!(view.key_count(), oracle.len());

        for (&k, vals) in &oracle {
            let expected_newest_first: Vec<u16> = vals.iter().copied().rev().collect();
            assert_eq!(view.get(k), expected_newest_first.first().copied());
            assert_eq!(admitted.get(k), expected_newest_first.first().copied());
            assert_eq!(collect_view(&view, k), expected_newest_first);
            assert_eq!(collect_view(&admitted, k), expected_newest_first);
        }

        // Probe 128 absent keys.
        for _ in 0..128 {
            let probe = (xorshift(&mut rng) >> 16) as u32;
            if !oracle.contains_key(&probe) {
                assert_eq!(view.get(probe), None);
                assert!(collect_view(&view, probe).is_empty());
            }
        }

        // Promote -> re-seal must produce byte-for-byte identical canonical bytes.
        let promoted = view.promote();
        assert_eq!(promoted.len(), total_expected);
        assert_eq!(promoted.seal(), buf);
    }
}

#[test]
fn corrupt_sealed_bytes_return_err_and_never_panic() {
    // Explicit error-class checks.
    assert_eq!(
        MapletView::open(&[]).unwrap_err().class,
        AdmissionClass::Extent
    );
    assert_eq!(
        MapletView::open(&[0x4D]).unwrap_err().class,
        AdmissionClass::Extent
    );
    assert_eq!(
        MapletView::open(&[0x42, 0x14]).unwrap_err().class,
        AdmissionClass::Magic
    );
    assert_eq!(
        MapletView::open(&[0x4D, 0x24]).unwrap_err().class,
        AdmissionClass::Kind
    );
    assert_eq!(
        MapletView::open(&[0x4D, 0x18]).unwrap_err().class,
        AdmissionClass::Kind
    );
    for bad_empty in [0x15u8, 0x16, 0x17] {
        assert_eq!(
            MapletView::open(&[0x4D, bad_empty]).unwrap_err().class,
            AdmissionClass::Canonical
        );
    }
    // Zero v2_len
    assert_eq!(
        MapletView::open(&[0x4D, 0x10, 0x00]).unwrap_err().class,
        AdmissionClass::Extent
    );
    // Non-empty header wrapping EMPTY_V2_IMAGE (0x80)
    assert_eq!(
        MapletView::open(&[0x4D, 0x10, 0x01, 0x80])
            .unwrap_err()
            .class,
        AdmissionClass::Canonical
    );
    // Non-canonical u16 flag when all values <= 255
    let mut m_u8 = Maplet::new();
    m_u8.insert(100, 42);
    let mut bad_u16 = m_u8.seal();
    bad_u16[1] |= 0x01; // set FLAG_VAL_U16
    bad_u16.push(0x00); // widen value 42 -> 42u16 LE
    assert_eq!(
        MapletView::open(&bad_u16).unwrap_err().class,
        AdmissionClass::Canonical
    );

    // Trailing bytes past extent are ignored cleanly (embedded framing).
    let mut embedded = m_u8.seal();
    let exact_len = embedded.len();
    embedded.extend_from_slice(&[0xFF, 0xAA, 0x55, 0x00]);
    let view = MapletView::open(&embedded).unwrap();
    assert_eq!(view.extent(), exact_len);
    assert_eq!(view.get(100), Some(42));

    // Multi-value corruption: bad total_values <= key_count, bad offset order, out-of-range offset.
    let mut mv = Maplet::new();
    mv.insert(10, 1);
    mv.insert(10, 2);
    mv.insert(20, 3);
    mv.insert(30, 4);
    let valid_mv = mv.seal();
    // valid_mv layout: [0x4D, 0x12, v2_len(1B), total_values=4(1B), v2(v2_len), off0=2, off1=3, vals(4B)]
    let v2_len = valid_mv[2] as usize;
    let off_at = 4 + v2_len;

    let mut bad_tv = valid_mv.clone();
    bad_tv[3] = 3; // total_values == key_count == 3 -> non-canonical
    assert_eq!(
        MapletView::open(&bad_tv).unwrap_err().class,
        AdmissionClass::Canonical
    );

    let mut bad_order = valid_mv.clone();
    bad_order[off_at] = 2;
    bad_order[off_at + 1] = 2; // not strictly ascending
    assert_eq!(
        MapletView::open(&bad_order).unwrap_err().class,
        AdmissionClass::MapOrder
    );

    let mut bad_range = valid_mv.clone();
    bad_range[off_at + 1] = 4; // >= total_values
    assert_eq!(
        MapletView::open(&bad_range).unwrap_err().class,
        AdmissionClass::PlaneRange
    );

    // Exhaustive truncation + systematic mutation across all four modes: never panics!
    let mut m_u16 = Maplet::new();
    for i in 0..32u32 {
        m_u16.insert(i * 1009, (i * 100 + 300) as u16);
    }
    let mut mv_u16 = Maplet::new();
    for i in 0..20u32 {
        mv_u16.insert(i * 777, (i * 50) as u16);
        mv_u16.insert(i * 777, (i * 50 + 500) as u16);
    }

    for sample in [m_u8.seal(), m_u16.seal(), valid_mv, mv_u16.seal()] {
        for cut in 0..sample.len() {
            assert!(
                MapletView::open(&sample[..cut]).is_err(),
                "truncated image at {cut}/{} must fail",
                sample.len()
            );
        }
        for pos in 0..sample.len() {
            for delta in [0x01u8, 0x80, 0xFF] {
                let mut mutated = sample.clone();
                mutated[pos] ^= delta;
                // Must NEVER panic; may return Ok (if mutated value/key happened to stay valid) or Err.
                if let Ok(v) = MapletView::open(&mutated) {
                    let _ = v.get(10);
                    let _ = v.for_each_match::<()>(10, |_| ControlFlow::Continue(()));
                }
            }
        }
    }
}

#[test]
fn zero_allocation_on_live_and_sealed_lookup_and_open() {
    let mut live = Maplet::with_capacity(1024);
    for i in 0..1024u32 {
        live.insert(i.wrapping_mul(0x9E37_79B1), (i & 0xFF) as u16);
    }
    live.insert(0x9E37_79B1, 99); // multi-value entry
    let sealed = live.seal();

    let probe_present = 0x9E37_79B1u32;
    let probe_absent = 0x1234_5678u32;

    let (hit, allocs_live_get) = count_allocs(|| live.get(probe_present));
    assert_eq!(hit, Some(99));
    assert_eq!(allocs_live_get, 0, "Maplet::get allocated");

    let (count, allocs_live_iter) = count_allocs(|| {
        let mut n = 0usize;
        let _ = live.for_each_match::<()>(probe_present, |_| {
            n += 1;
            ControlFlow::Continue(())
        });
        n
    });
    assert!(count >= 2);
    assert_eq!(allocs_live_iter, 0, "Maplet::for_each_match allocated");

    let (view_res, allocs_open) = count_allocs(|| MapletView::open(&sealed));
    let view = view_res.expect("valid sealed maplet");
    assert_eq!(allocs_open, 0, "MapletView::open allocated");

    let (adm_opt, allocs_open_adm) = count_allocs(|| MapletView::open_admitted(&sealed));
    let adm = adm_opt.expect("valid admitted maplet");
    assert_eq!(allocs_open_adm, 0, "MapletView::open_admitted allocated");

    let ((g1, g2), allocs_view_get) =
        count_allocs(|| (view.get(probe_present), adm.get(probe_absent)));
    assert_eq!(g1, Some(99));
    assert_eq!(g2, None);
    assert_eq!(allocs_view_get, 0, "MapletView::get allocated");

    let (vcount, allocs_view_iter) = count_allocs(|| {
        let mut n = 0usize;
        let _ = view.for_each_match::<()>(probe_present, |_| {
            n += 1;
            ControlFlow::Continue(())
        });
        n
    });
    assert_eq!(vcount, 2);
    assert_eq!(allocs_view_iter, 0, "MapletView::for_each_match allocated");
}
