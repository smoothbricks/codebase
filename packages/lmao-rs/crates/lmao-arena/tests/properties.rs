//! Property tests for the arena — the acceptance criteria for the allocator.zig port.
//!
//! Semantics note (discovered while porting): buddy merges are ADDRESS-based
//! (right buddy = offset + block_size). Bump allocation aligns to 8 bytes, so two
//! consecutively bump-allocated blocks are adjacent ONLY when the block size is a
//! multiple of 8 (span_system always is; column blocks usually are not because of
//! the null-bitmap prefix). Merges are therefore guaranteed within SPLIT FAMILIES
//! (children of one buddy split are exactly adjacent) — the conservation property
//! is stated over split families, matching the Zig behavior, not over arbitrary
//! bump-allocated neighbors.

use lmao_arena::raw::{self};
use lmao_arena::{Arena, SizeClass, block_size};
use proptest::prelude::*;

fn size_class_strategy() -> impl Strategy<Value = SizeClass> {
    prop_oneof![
        Just(SizeClass::SpanSystem),
        Just(SizeClass::Col1B),
        Just(SizeClass::Col4B),
        Just(SizeClass::Col8B),
    ]
}

fn capacity_strategy() -> impl Strategy<Value = u32> {
    (0u32..7).prop_map(|t| 8u32 << t)
}

proptest! {
    /// Allocated blocks never overlap and never overlap the header.
    #[test]
    fn allocated_blocks_do_not_overlap(
        allocs in prop::collection::vec((size_class_strategy(), capacity_strategy()), 1..200),
    ) {
        let mut arena = Arena::new(1 << 20);
        let mut live: Vec<(u32, u32)> = Vec::new(); // (offset, size)
        for (sc, cap) in allocs {
            let off = arena.alloc(sc, cap);
            prop_assert!(off != 0, "OOM not expected at this scale");
            prop_assert!(off >= lmao_arena::HEADER_SIZE as u32);
            let size = block_size(sc, cap);
            for &(o, s) in &live {
                prop_assert!(off + size <= o || off >= o + s, "blocks overlap");
            }
            live.push((off, size));
        }
    }

    /// Alloc → free → alloc of the same (class, capacity) reuses the freed block
    /// (freelist pop), and free-then-realloc conserves the bump pointer (no leak
    /// growth for a balanced workload).
    #[test]
    fn free_then_alloc_reuses_block(
        sc in size_class_strategy(),
        cap in capacity_strategy(),
        rounds in 1usize..50,
    ) {
        let mut arena = Arena::new(1 << 20);
        let first = arena.alloc(sc, cap);
        arena.free(first, sc, cap);
        let bump_after_first = arena.bump_ptr();
        for _ in 0..rounds {
            let off = arena.alloc(sc, cap);
            prop_assert_eq!(off, first, "freelist must reuse the freed block");
            arena.free(off, sc, cap);
        }
        prop_assert_eq!(arena.bump_ptr(), bump_after_first, "balanced alloc/free must not grow bump");
    }

    /// Buddy conservation over a split family: allocating two tier-N blocks out of
    /// a freed tier-N+1 parent (buddy split), then freeing both, must merge back so
    /// a subsequent tier-N+1 alloc reuses the parent region without bumping.
    #[test]
    fn buddy_merge_conserves_split_family(
        sc in size_class_strategy(),
        tier in 0usize..6, // leave room for tier+1
    ) {
        let cap = 8u32 << tier;
        // Skip combos collapsed by the small-block tier clamp (col_1b cap 8/16
        // share one effective tier — no split relationship to test).
        prop_assume!(raw::effective_block_size(sc, cap * 2) == 2 * raw::effective_block_size(sc, cap));
        let child_size = raw::effective_block_size(sc, cap);
        let mut arena = Arena::new(1 << 20);
        // Seed a tier+1 block and free it, so the next two tier-N allocs split it.
        let parent = arena.alloc(sc, cap * 2);
        arena.free(parent, sc, cap * 2);
        let bump_after_parent = arena.bump_ptr();

        let a = arena.alloc(sc, cap);
        let b = arena.alloc(sc, cap);
        prop_assert_eq!(a, parent, "first child must come from the split parent");
        prop_assert_eq!(b, parent + child_size, "split children are exactly adjacent");
        prop_assert_eq!(arena.bump_ptr(), bump_after_parent, "split must not bump");

        // Free both children (either order triggers the address-based merge).
        arena.free(a, sc, cap);
        arena.free(b, sc, cap);

        let merged = arena.alloc(sc, cap * 2);
        prop_assert_eq!(merged, parent, "merged parent must be reused");
        prop_assert_eq!(arena.bump_ptr(), bump_after_parent, "no bump growth for a balanced split/merge cycle");
    }

    /// Freelist stats are cascading and model-checked at the TOP tier (cap 512:
    /// no tier above → frees never merge, allocs never split — a pure LIFO
    /// freelist): after N frees head.freelist_len == N; popping is LIFO and the
    /// surviving head carries reuse_count.
    #[test]
    fn cascading_stats_match_model(
        sc in size_class_strategy(),
        n in 2usize..20,
    ) {
        let cap = 512u32; // top tier
        let mut arena = Arena::new(1 << 22);
        let blocks: Vec<u32> = (0..n).map(|_| arena.alloc(sc, cap)).collect();
        for &b in &blocks {
            arena.free(b, sc, cap);
        }
        prop_assert_eq!(arena.freelist_len(sc, cap), n as u32, "cascaded freelist_len");

        // Pop one: LIFO head; new head must carry reuse_count = 1 and len = n-1.
        let popped = arena.alloc(sc, cap);
        prop_assert_eq!(popped, *blocks.last().unwrap(), "LIFO pop");
        prop_assert_eq!(arena.freelist_len(sc, cap), n as u32 - 1);
        prop_assert_eq!(raw::freelist_reuse_count(arena.mem(), sc, cap), 1);
    }

    /// find_and_remove semantics via the free path: a fresh arena's first tier-N
    /// alloc splits a top-tier bump block all the way down, parking one sibling on
    /// every tier between. Freeing that alloc must find each sibling (right-neighbor
    /// scan), remove it, and cascade the merge to FULL reconstitution: every split
    /// tier empty, the top tier holding the one reconstituted block.
    #[test]
    fn merge_removes_sibling_from_freelist(
        sc in size_class_strategy(),
        tier in 0usize..6,
    ) {
        let cap = 8u32 << tier;
        prop_assume!(raw::effective_block_size(sc, cap) < raw::effective_block_size(sc, 512));
        let mut arena = Arena::new(1 << 20);
        let a = arena.alloc(sc, cap); // bump top tier, split chain down
        prop_assert_eq!(arena.freelist_len(sc, cap), 1, "own-tier sibling parked on freelist");
        arena.free(a, sc, cap); // right-sibling merges cascade to the top
        prop_assert_eq!(arena.freelist_len(sc, cap), 0, "sibling removed by merge");
        prop_assert_eq!(arena.freelist_len(sc, 512), 1, "top tier reconstituted");
        prop_assert!(raw::freelist_merge_count(arena.mem(), sc, 512) >= 1, "merge counted");
        // And the reconstituted block is reused, not re-bumped.
        let bump = arena.bump_ptr();
        let again = arena.alloc(sc, cap);
        prop_assert_eq!(again, a);
        prop_assert_eq!(arena.bump_ptr(), bump);
    }

    /// Identity blocks: LIFO reuse through their dedicated freelist; span ids are
    /// a strictly increasing global counter.
    #[test]
    fn identity_blocks_reuse_and_span_ids_increase(rounds in 1usize..30) {
        let mut arena = Arena::new(1 << 20);
        let first = arena.alloc_identity();
        prop_assert!(first != 0);
        let mut last_span_id = raw::read_identity_span_id(arena.mem(), first);
        arena.free_identity(first);
        for _ in 0..rounds {
            let id = arena.alloc_identity();
            prop_assert_eq!(id, first, "identity freelist must reuse LIFO");
            let span_id = raw::read_identity_span_id(arena.mem(), id);
            prop_assert!(span_id > last_span_id, "span_id_counter strictly increases");
            last_span_id = span_id;
            arena.free_identity(id);
        }
    }

    /// Memory growth: a minimal arena grows on demand (native Vec backend) instead
    /// of returning the OOM sentinel; all offsets stay valid post-growth.
    #[test]
    fn arena_grows_on_demand(count in 1usize..64) {
        let mut arena = Arena::new(lmao_arena::HEADER_SIZE); // header only, zero slack
        for _ in 0..count {
            let off = arena.alloc(SizeClass::Col8B, 512);
            prop_assert!(off != 0, "Vec backend must grow, not OOM");
            let end = off + block_size(SizeClass::Col8B, 512);
            prop_assert!((end as usize) <= arena.len());
        }
    }

    /// Span lifecycle over arena blocks: row 0 span-start, row 1 pre-armed as
    /// span-exception then resolved by span_end; log entries append from row 2 and
    /// timestamps are monotone under a monotone clock.
    #[test]
    fn span_lifecycle_rows(cap in capacity_strategy(), logs in 0u32..6) {
        let mut arena = Arena::new(1 << 20);
        let system = arena.alloc(SizeClass::SpanSystem, cap);
        let identity = arena.alloc_identity();
        let root = arena.alloc(SizeClass::Col8B, 8); // any 16+ byte scratch block for TraceRoot
        let m = arena.mem_mut();
        raw::init_trace_root(m, root, 1_000.0, 10.0);

        raw::span_start(m, system, identity, root, cap, 11.0);
        prop_assert_eq!(raw::read_entry_type(m, system, 0, cap), raw::ENTRY_TYPE_SPAN_START);
        prop_assert_eq!(raw::read_entry_type(m, system, 1, cap), raw::ENTRY_TYPE_SPAN_EXCEPTION);
        prop_assert_eq!(raw::read_write_index(m, identity), 2);

        let mut prev_ts = raw::read_timestamp(m, system, 0);
        for i in 0..logs.min(cap.saturating_sub(2)) {
            let idx = raw::write_log_entry(m, system, identity, root, 5, cap, 12.0 + i as f64);
            prop_assert_eq!(idx, 2 + i);
            let ts = raw::read_timestamp(m, system, idx);
            prop_assert!(ts >= prev_ts, "monotone clock → monotone timestamps");
            prev_ts = ts;
        }

        raw::span_end(m, system, root, cap, raw::ENTRY_TYPE_SPAN_OK, 99.0);
        prop_assert_eq!(raw::read_entry_type(m, system, 1, cap), raw::ENTRY_TYPE_SPAN_OK);
        prop_assert!(raw::read_timestamp(m, system, 1) > prev_ts);
    }

    /// Column writes: value readback + null-bitmap validity bits, with lazy
    /// allocation on first write (col_offset == 0).
    #[test]
    fn column_write_read_and_validity(
        cap in capacity_strategy(),
        rows in prop::collection::btree_set(0u32..8, 1..8),
        value in prop::num::f64::NORMAL,
    ) {
        let mut arena = Arena::new(1 << 20);
        let mut col = 0u32; // lazy: first write allocates
        for &row in &rows {
            let row = row.min(cap - 1);
            col = raw::write_col_f64(arena.mem_mut(), col, row, value, cap);
            prop_assert!(col != 0);
        }
        for row in 0..cap.min(8) {
            let expect_valid = rows.iter().any(|&r| r.min(cap - 1) == row);
            prop_assert_eq!(
                raw::read_col_is_valid(arena.mem(), col, row) == 1,
                expect_valid,
                "validity bit for row {}", row
            );
            if expect_valid {
                prop_assert_eq!(raw::read_col_f64(arena.mem(), col, row, cap), value);
            }
        }
    }

    /// Exact slabs honor every advertised alignment and keep all live logical
    /// extents disjoint. A released descriptor is reusable exactly once, while
    /// a repeated release is ignored rather than duplicating ownership.
    #[test]
    fn exact_slabs_are_aligned_disjoint_and_single_owner(
        requests in prop::collection::vec((1u32..2048, 0u32..8), 1..100),
    ) {
        let mut arena = Arena::new(lmao_arena::HEADER_SIZE);
        let mut live: Vec<(u32, u32, u32)> = Vec::with_capacity(requests.len());

        for (byte_len, alignment_power) in requests {
            let alignment = 1u32 << alignment_power;
            let offset = raw::alloc_exact(arena.mem_mut(), byte_len, alignment);
            prop_assert_ne!(offset, 0);
            prop_assert_eq!(offset % alignment, 0);
            for &(other_offset, other_len, _) in &live {
                prop_assert!(
                    offset + byte_len <= other_offset || offset >= other_offset + other_len,
                    "live exact slabs overlap"
                );
            }
            live.push((offset, byte_len, alignment));
        }

        let (released, byte_len, alignment) = *live.last().unwrap();
        raw::free_exact(arena.mem_mut(), released, byte_len, alignment);
        let free_count = arena.free_count();
        raw::free_exact(arena.mem_mut(), released, byte_len, alignment);
        prop_assert_eq!(arena.free_count(), free_count, "repeat free must be idempotent");

        let recycled = raw::alloc_exact(arena.mem_mut(), byte_len, alignment);
        prop_assert_eq!(recycled, released, "matching exact descriptor must reuse its address");
        let second_owner = raw::alloc_exact(arena.mem_mut(), byte_len, alignment);
        prop_assert_ne!(second_owner, recycled, "two live exact slabs must not alias");
    }
}

#[test]
fn zero_offset_frees_are_no_ops_for_every_size_class_and_identity() {
    let mut arena = Arena::new(1 << 20);
    let free_count = arena.free_count();
    let bump_ptr = arena.bump_ptr();

    for sc in [
        SizeClass::SpanSystem,
        SizeClass::Col1B,
        SizeClass::Col4B,
        SizeClass::Col8B,
    ] {
        arena.free(0, sc, 64);
    }
    arena.free_identity(0);

    assert_eq!(arena.free_count(), free_count);
    assert_eq!(arena.bump_ptr(), bump_ptr);
}

#[test]
fn repeated_free_after_buddy_merge_does_not_duplicate_ownership() {
    for sc in [
        SizeClass::SpanSystem,
        SizeClass::Col1B,
        SizeClass::Col4B,
        SizeClass::Col8B,
    ] {
        let mut arena = Arena::new(1 << 20);
        let offset = arena.alloc(sc, 8);
        arena.free(offset, sc, 8);
        let free_count = arena.free_count();

        arena.free(offset, sc, 8);

        assert_eq!(
            arena.free_count(),
            free_count,
            "second free for {sc:?} must be ignored"
        );
        let first_owner = arena.alloc(sc, 8);
        let second_owner = arena.alloc(sc, 8);
        assert_ne!(
            first_owner, second_owner,
            "two live {sc:?} owners must not alias"
        );
    }
}

#[test]
fn repeated_identity_free_does_not_duplicate_ownership() {
    let mut arena = Arena::new(1 << 20);
    let offset = arena.alloc_identity();
    arena.free_identity(offset);
    let free_count = arena.free_count();

    arena.free_identity(offset);

    assert_eq!(arena.free_count(), free_count);
    let first_owner = arena.alloc_identity();
    let second_owner = arena.alloc_identity();
    assert_ne!(first_owner, second_owner);
}

#[test]
fn recycled_column_clears_validity_and_value_bytes() {
    let mut arena = Arena::new(1 << 20);
    let capacity = 64;
    let row = 5;
    let column = raw::write_col_f64(arena.mem_mut(), 0, row, 91.25, capacity);
    assert_eq!(raw::read_col_is_valid(arena.mem(), column, row), 1);

    arena.free(column, SizeClass::Col8B, capacity);
    let recycled = arena.alloc(SizeClass::Col8B, capacity);

    assert_eq!(recycled, column, "test must exercise the recycled block");
    assert_eq!(raw::read_col_is_valid(arena.mem(), recycled, row), 0);
    assert_eq!(raw::read_col_f64(arena.mem(), recycled, row, capacity), 0.0);
}
