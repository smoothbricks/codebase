//! Batch capacity admission: a batch that can run out of slot capacity
//! answers `NEEDS_GROWTH` before its first write, so a host without a journal
//! grows and replays over the exact image it had. Tier 1 clears batches from
//! the row count, tier 2 from the rows the slot lacks, tier 3 by a dry run.
//! Debug builds also assert inside the VM that a cleared batch never runs out.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use bitmosaic::{BitmosaicView, EMPTY_U32_IMAGE, patched_len_bound};
use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    ErrorCode, Opcode, SLOT_META_SIZE, STATE_FORMAT_VERSION, STATE_HEADER_SIZE, STATE_MAGIC,
    SlotMetaOffset, SlotType, StateHeaderOffset, StructFieldType,
};
use columine_vm::bitmap_ops::{
    BitmapEnv, batch_bitmap_add, batch_bitmap_remove, get_bitmap_storage,
};
use columine_vm::capacity::CapacityCounts;
use columine_vm::hooks::NoVm;
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_grown_state_size, calculate_state_size, init_state,
    needs_growth_slot,
};
use columine_vm::vm::{CheckedMap, Vm, f64s_as_bytes, u32s_as_bytes};
use proptest::prelude::*;

const OK: u32 = ErrorCode::Ok as u32;
const NEEDS_GROWTH: u32 = ErrorCode::NeedsGrowth as u32;
const TYPE: u32 = 7;

// ---------------------------------------------------------------------------
// An allocator that refuses this thread's allocations on demand, forwarding
// every other call to the system allocator unchanged.
// ---------------------------------------------------------------------------

struct RefusingAllocator;

thread_local! {
    static ALLOCATION_REFUSED: Cell<bool> = const { Cell::new(false) };
}

fn allocation_refused() -> bool {
    ALLOCATION_REFUSED.try_with(Cell::get).unwrap_or(false)
}

// SAFETY: every call forwards the original pointer and layout to System; a
// refused allocation returns null, which GlobalAlloc permits for any call.
unsafe impl GlobalAlloc for RefusingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if allocation_refused() {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if allocation_refused() {
            return core::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if allocation_refused() {
            return core::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: RefusingAllocator = RefusingAllocator;

struct RefusalScope;

impl Drop for RefusalScope {
    fn drop(&mut self) {
        ALLOCATION_REFUSED.with(|refused| refused.set(false));
    }
}

/// Run `run` while every allocation on this thread fails.
fn without_memory<T>(run: impl FnOnce() -> T) -> T {
    ALLOCATION_REFUSED.with(|refused| refused.set(true));
    let scope = RefusalScope;
    let result = run();
    drop(scope);
    result
}

fn program(num_slots: u8, num_inputs: u8, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(Opcode::Halt as u8);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(u32::from(num_slots).to_le_bytes());
    prog.extend(u32::from(num_inputs).to_le_bytes());
    prog.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    prog.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    prog.extend(init);
    prog.extend_from_slice(reduce);
    prog
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

fn for_each(body: &[u8]) -> Vec<u8> {
    let mut out = vec![Opcode::ForEach as u8, 2, 1];
    out.extend(TYPE.to_le_bytes());
    out.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn grow(vm: &mut Vm, state: &[u8], slot: u32) -> Vec<u8> {
    let size = calculate_grown_state_size(state, slot).expect("grown size");
    let mut next = vec![0u8; size as usize];
    vm.grow_state(state, &mut next, slot).expect("grow");
    next
}

/// Run one batch the way a host without a journal does: on `NEEDS_GROWTH`
/// the image must be exactly as it was, then grow and replay.
fn run_growing(vm: &mut Vm, state: &mut Vec<u8>, prog: &[u8], cols: &[&[u8]], rows: u32) {
    for _ in 0..32 {
        let before = state.clone();
        match vm.execute_batch(state, prog, cols, rows) {
            OK => return,
            NEEDS_GROWTH => {
                assert_eq!(&before, state, "a refused batch wrote state");
                *state = grow(vm, state, needs_growth_slot());
            }
            other => panic!("batch refused with {other}"),
        }
    }
    panic!("growth did not converge");
}

// ---------------------------------------------------------------------------
// Tier accounting on one hash map.
// ---------------------------------------------------------------------------

/// One HashMap of declared capacity 16: physical 32, admits 22 keys.
fn map_program() -> Vec<u8> {
    program(
        1,
        2,
        &[Opcode::SlotDef as u8, 0, SlotType::HashMap as u8, 16, 0],
        &[Opcode::BatchMapUpsertLast as u8, 0, 0, 1],
    )
}

fn upsert(vm: &mut Vm, state: &mut [u8], prog: &[u8], keys: &[u32]) -> u32 {
    let vals: Vec<u32> = keys.iter().map(|k| k * 10).collect();
    vm.execute_batch(
        state,
        prog,
        &[u32s_as_bytes(keys), u32s_as_bytes(&vals)],
        keys.len() as u32,
    )
}

#[test]
fn each_tier_answers_only_what_the_tier_before_cannot() {
    let prog = map_program();
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let counts = |vm: &Vm| vm.capacity_counts();

    // Room for every row: the row count clears it.
    assert_eq!(
        OK,
        upsert(&mut vm, &mut state, &prog, &(1..=20).collect::<Vec<_>>())
    );
    assert_eq!(
        CapacityCounts {
            bound: 1,
            ..CapacityCounts::default()
        },
        counts(&vm)
    );

    // 20 live keys, room for 2 more: ten updates pass the bound only by
    // reading the rows, which name no absent key.
    let updates: Vec<u32> = (1..=10).collect();
    assert_eq!(OK, upsert(&mut vm, &mut state, &prog, &updates));
    assert_eq!(1, counts(&vm).probed);

    // Two new keys named five times each: ten absent rows exceed the room,
    // so only a dry run can tell the two keys fit. Without memory for the
    // dry run's image the batch is refused by name with nothing written, and
    // with memory back the same batch runs.
    let repeated: Vec<u32> = [100, 101].repeat(5);
    let before = state.clone();
    let values: Vec<u32> = repeated.iter().map(|k| k * 10).collect();
    let cols = [u32s_as_bytes(&repeated), u32s_as_bytes(&values)];
    assert_eq!(
        ErrorCode::OutOfMemory as u32,
        without_memory(|| vm.execute_batch(&mut state, &prog, &cols, 10))
    );
    assert_eq!(before, state);
    assert_eq!(OK, upsert(&mut vm, &mut state, &prog, &repeated));
    assert_eq!(1, counts(&vm).dry_run_fit);
    let map = CheckedMap::bind(&state, 0).unwrap();
    assert_eq!(Some(1000), map.get(100));
    assert_eq!(Some(1010), map.get(101));

    // Full: one more key refuses before any write, journal or not.
    let before = state.clone();
    assert_eq!(
        NEEDS_GROWTH,
        upsert(&mut vm, &mut state, &prog, &[1, 2, 200])
    );
    assert_eq!(before, state);
    assert_eq!(0, needs_growth_slot());
    assert_eq!(1, counts(&vm).refused);

    vm.undo_enable(&state).expect("window");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        NEEDS_GROWTH,
        upsert(&mut vm, &mut state, &prog, &[1, 2, 200])
    );
    assert_eq!(before, state);
    assert_eq!(
        checkpoint,
        vm.undo_checkpoint(),
        "a refused batch journals nothing"
    );
    vm.undo_commit();

    let mut grown = grow(&mut vm, &state, 0);
    assert_eq!(OK, upsert(&mut vm, &mut grown, &prog, &[1, 2, 200]));
    assert_eq!(Some(2000), CheckedMap::bind(&grown, 0).unwrap().get(200));

    // An erase needs no working memory of its own: a VM that has never
    // erased removes several keys, whatever their probe clusters, while every
    // allocation fails, so a removal batch cannot stop after its first key.
    let mut fresh = Vm::default();
    let removed: Vec<u32> = (1..=20).chain([200]).collect();
    assert_eq!(
        ErrorCode::Ok,
        without_memory(|| fresh.map_remove(false, &mut grown, 0, &removed))
    );
    let map = CheckedMap::bind(&grown, 0).unwrap();
    for key in removed {
        assert_eq!(None, map.get(key), "key {key}");
    }
    assert_eq!(Some(1000), map.get(100));

    // Bitmap patches reserve their working memory per batch too. A batch
    // adds one value to bitmap 0, then 300 to bitmap 1; on a VM whose patch
    // scratch only served one-value patches, every allocation refused, the
    // batch is refused as a whole with nothing written rather than after its
    // first patch. A VM that never ran a batch, adding, removing or running
    // algebra, is refused the same way.
    let bitmaps = |reduce: &[u8]| {
        program(
            2,
            2,
            &[
                vec![Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 0, 2],
                vec![Opcode::SlotDef as u8, 1, SlotType::Bitmap as u8, 0, 2],
            ]
            .concat(),
            reduce,
        )
    };
    let adds = bitmaps(&[
        Opcode::BatchBitmapAdd as u8,
        0,
        0,
        Opcode::BatchBitmapAdd as u8,
        1,
        1,
    ]);
    let mut state = init(&adds);
    let mut vm = Vm::default();
    let one = [7u32];
    assert_eq!(
        OK,
        vm.execute_batch(
            &mut state,
            &adds,
            &[u32s_as_bytes(&one), u32s_as_bytes(&one)],
            1
        )
    );
    let small = vec![9u32; 300];
    let large: Vec<u32> = (0..300).map(|i| i * 977).collect();
    let cols = [u32s_as_bytes(&small), u32s_as_bytes(&large)];
    let before = state.clone();
    assert_eq!(
        ErrorCode::OutOfMemory as u32,
        without_memory(|| vm.execute_batch(&mut state, &adds, &cols, 300))
    );
    assert_eq!(before, state, "a refused batch writes nothing");
    assert_eq!(OK, vm.execute_batch(&mut state, &adds, &cols, 300));
    for reduce in [
        vec![Opcode::BatchBitmapAdd as u8, 0, 0],
        vec![Opcode::BatchBitmapRemove as u8, 0, 0],
        vec![Opcode::BatchBitmapOr as u8, 0, 1],
    ] {
        let prog = bitmaps(&reduce);
        let mut fresh = Vm::default();
        let before = state.clone();
        assert_eq!(
            ErrorCode::OutOfMemory as u32,
            without_memory(|| fresh.execute_batch(&mut state, &prog, &cols, 300)),
            "{reduce:?}"
        );
        assert_eq!(before, state);
    }

    // A FOR_EACH body's per-element form is drawn from the reduce section
    // and backed before the batch writes. A VM that has admitted this
    // program, but never widened its body because no row matched, widens it
    // while every allocation fails: the batch runs whole, its upsert and
    // every body upsert, rather than stopping after the first.
    let body = [Opcode::BatchMapUpsertLast as u8, 1, 0, 1].repeat(400);
    let prog = program(
        2,
        3,
        &[
            vec![Opcode::SlotDef as u8, 0, SlotType::HashMap as u8, 16, 0],
            // Room for every body row, so tier 1 clears the batch without a
            // dry run, whose image copy would need memory of its own.
            vec![Opcode::SlotDef as u8, 1, SlotType::HashMap as u8, 0, 4],
        ]
        .concat(),
        &[
            vec![Opcode::BatchMapUpsertLast as u8, 0, 0, 1],
            for_each(&body),
        ]
        .concat(),
    );
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let (keys, values) = ([3u32], [30u32]);
    let unmatched = [TYPE + 1];
    assert_eq!(
        OK,
        vm.execute_batch(
            &mut state,
            &prog,
            &[
                u32s_as_bytes(&keys),
                u32s_as_bytes(&values),
                u32s_as_bytes(&unmatched)
            ],
            1
        )
    );
    let matched = [TYPE];
    let cols = [
        u32s_as_bytes(&keys),
        u32s_as_bytes(&values),
        u32s_as_bytes(&matched),
    ];
    assert_eq!(
        OK,
        without_memory(|| vm.execute_batch(&mut state, &prog, &cols, 1))
    );
    assert_eq!(Some(30), CheckedMap::bind(&state, 1).unwrap().get(3));
}

// ---------------------------------------------------------------------------
// Every family, through grow-and-replay with no journal.
// ---------------------------------------------------------------------------

/// Slot 0 HashMap, 1 Bitmap, 2 OrderedList (u32), 3 Nested (outer map of
/// inner sets), 4 StructMap (u32 + u32 array). Inputs: 0 key, 1 value,
/// 2 type, 3 array offsets, 4 array values.
fn mixed_program() -> Vec<u8> {
    let init = [
        vec![Opcode::SlotDef as u8, 0, SlotType::HashMap as u8, 4, 0],
        vec![Opcode::SlotDef as u8, 1, SlotType::Bitmap as u8, 4, 0],
        vec![
            Opcode::SlotOrderedList as u8,
            2,
            SlotType::OrderedList as u8,
            4,
            0,
            0,
        ],
        vec![
            Opcode::SlotNested as u8,
            3,
            SlotType::Nested as u8,
            4,
            0,
            SlotType::HashSet as u8,
            2,
            0,
            0,
        ],
        vec![
            Opcode::SlotStructMap as u8,
            4,
            SlotType::StructMap as u8,
            4,
            0,
            2,
            StructFieldType::UInt32 as u8,
            StructFieldType::ArrayU32 as u8,
        ],
    ]
    .concat();
    let body = [
        vec![Opcode::ListAppend as u8, 2, 1],
        vec![Opcode::NestedSetInsert as u8, 3, 0, 1],
    ]
    .concat();
    let reduce = [
        vec![Opcode::BatchMapUpsertLast as u8, 0, 0, 1],
        vec![Opcode::BatchBitmapAdd as u8, 1, 1],
        // slot 4, key 0, one (val 1 → field 0), one array (3, 4 → field 1).
        vec![
            Opcode::BatchStructMapUpsertLast as u8,
            4,
            0,
            1,
            1,
            0,
            1,
            3,
            4,
            1,
        ],
        for_each(&body),
    ]
    .concat();
    program(5, 5, &init, &reduce)
}

#[derive(Debug, Clone)]
struct Row {
    key: u32,
    value: u32,
    typed: bool,
    array: Vec<u32>,
}

fn row() -> impl Strategy<Value = Row> {
    (
        0u32..40,
        0u32..4000,
        any::<bool>(),
        prop::collection::vec(0u32..9, 0..5),
    )
        .prop_map(|(key, value, typed, array)| Row {
            key,
            // Spread over chunks, so bitmap images pay per-chunk bytes.
            value: ((value % 997) << 16) | (value * 7 + 1),
            typed,
            array,
        })
}

/// The batch's columns, owned.
struct Batch {
    keys: Vec<u32>,
    values: Vec<u32>,
    types: Vec<u32>,
    offsets: Vec<u32>,
    array: Vec<u32>,
    reals: Vec<f64>,
}

impl Batch {
    fn of(rows: &[Row]) -> Self {
        let mut offsets = vec![0];
        let mut array = Vec::new();
        for row in rows {
            array.extend_from_slice(&row.array);
            offsets.push(array.len() as u32);
        }
        Self {
            keys: rows.iter().map(|r| r.key).collect(),
            values: rows.iter().map(|r| r.value).collect(),
            types: rows
                .iter()
                .map(|r| if r.typed { TYPE } else { 0 })
                .collect(),
            offsets,
            array,
            reals: rows.iter().map(|r| f64::from(r.value)).collect(),
        }
    }

    fn cols(&self) -> [&[u8]; 6] {
        [
            u32s_as_bytes(&self.keys),
            u32s_as_bytes(&self.values),
            u32s_as_bytes(&self.types),
            u32s_as_bytes(&self.offsets),
            u32s_as_bytes(&self.array),
            f64s_as_bytes(&self.reals),
        ]
    }
}

fn map_contents(state: &[u8]) -> BTreeMap<u32, u32> {
    let map = CheckedMap::bind(state, 0).unwrap();
    (0..40).filter_map(|k| map.get(k).map(|v| (k, v))).collect()
}

fn bitmap_contents(state: &[u8]) -> BTreeSet<u32> {
    let storage = get_bitmap_storage(&SlotMetaView::read(state, 1));
    match storage.view(state).unwrap() {
        Some(view) => view.range().collect(),
        None => BTreeSet::new(),
    }
}

fn list_len(state: &[u8]) -> u32 {
    SlotMetaView::read(state, 2).size(state)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Growth and replay converge, a refusal leaves the image byte-equal,
    /// and the result matches the same batches on a VM that never grows.
    #[test]
    fn grow_and_replay_never_runs_over_a_partial_write(
        batches in prop::collection::vec(prop::collection::vec(row(), 1..24), 1..10),
    ) {
        let prog = mixed_program();
        let mut state = init(&prog);
        let mut vm = Vm::default();
        let mut model_map = BTreeMap::new();
        let mut model_bits = BTreeSet::new();
        let mut model_list = 0u32;
        for rows in &batches {
            let batch = Batch::of(rows);
            run_growing(&mut vm, &mut state, &prog, &batch.cols(), rows.len() as u32);
            for row in rows {
                model_map.insert(row.key, row.value);
                model_bits.insert(row.value);
                model_list += u32::from(row.typed);
            }
        }
        prop_assert_eq!(map_contents(&state), model_map);
        prop_assert_eq!(bitmap_contents(&state), model_bits);
        prop_assert_eq!(list_len(&state), model_list);
    }
}

fn nested(inner: SlotType, inner_cap: u8, inner_agg: u8) -> Vec<u8> {
    vec![
        Opcode::SlotNested as u8,
        0,
        SlotType::Nested as u8,
        8,
        0,
        inner as u8,
        inner_cap,
        0,
        inner_agg,
    ]
}

/// Single-family programs. A tier that clears a batch which then runs out
/// trips the VM's debug assertion; a refusal that wrote trips
/// [`run_growing`].
fn family_programs() -> Vec<(&'static str, Vec<u8>, u8)> {
    let map2 = [
        Opcode::SlotStructMap2 as u8,
        0,
        SlotType::StructMap2 as u8,
        4,
        0,
        1,
        StructFieldType::UInt32 as u8,
    ];
    vec![
        ("map", map_program(), 2),
        (
            "set",
            program(
                1,
                2,
                &[Opcode::SlotDef as u8, 0, SlotType::HashSet as u8, 4, 0],
                &[Opcode::BatchSetInsert as u8, 0, 1],
            ),
            2,
        ),
        (
            "bitmap",
            program(
                1,
                2,
                &[Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 64, 0],
                &[
                    Opcode::BatchBitmapAdd as u8,
                    0,
                    1,
                    Opcode::BatchBitmapRemove as u8,
                    0,
                    0,
                ],
            ),
            2,
        ),
        (
            "bitmap algebra",
            program(
                2,
                2,
                &[
                    Opcode::SlotDef as u8,
                    0,
                    SlotType::Bitmap as u8,
                    64,
                    0,
                    Opcode::SlotDef as u8,
                    1,
                    SlotType::Bitmap as u8,
                    64,
                    0,
                ],
                &[
                    Opcode::BatchBitmapAdd as u8,
                    1,
                    1,
                    Opcode::BatchBitmapOr as u8,
                    0,
                    1,
                    Opcode::BatchBitmapAdd as u8,
                    0,
                    0,
                    Opcode::BatchBitmapXor as u8,
                    1,
                    0,
                ],
            ),
            2,
        ),
        (
            "nested sets",
            program(
                1,
                3,
                &nested(SlotType::HashSet, 1, 0),
                // One outer key (the type) gathers every value: its inner set
                // rehashes repeatedly and the arena, not the outer table, binds.
                &for_each(&[Opcode::NestedSetInsert as u8, 0, 2, 1]),
            ),
            3,
        ),
        (
            "nested sets by key",
            program(
                1,
                3,
                &nested(SlotType::HashSet, 1, 0),
                // Many outer keys, each opening a container in an arena
                // that inner rehashes have already spent.
                &for_each(&[Opcode::NestedSetInsert as u8, 0, 0, 1]),
            ),
            3,
        ),
        (
            "nested maps",
            program(
                1,
                3,
                &nested(SlotType::HashMap, 2, 0),
                &for_each(&[Opcode::NestedMapUpsertLast as u8, 0, 2, 1, 0]),
            ),
            3,
        ),
        (
            "nested aggregates",
            program(
                1,
                6,
                &nested(SlotType::Aggregate, 0, 1),
                &for_each(&[Opcode::NestedAggUpdate as u8, 0, 0, 5]),
            ),
            6,
        ),
        (
            "bitmap in a body",
            program(
                1,
                3,
                &[Opcode::SlotDef as u8, 0, SlotType::Bitmap as u8, 4, 0],
                &for_each(&[Opcode::BatchBitmapAdd as u8, 0, 1]),
            ),
            3,
        ),
        (
            "list",
            program(
                1,
                3,
                &[
                    Opcode::SlotOrderedList as u8,
                    0,
                    SlotType::OrderedList as u8,
                    4,
                    0,
                    0,
                ],
                &for_each(&[Opcode::ListAppend as u8, 0, 1]),
            ),
            3,
        ),
        (
            "struct-map arena",
            program(
                1,
                5,
                &[
                    Opcode::SlotStructMap as u8,
                    0,
                    SlotType::StructMap as u8,
                    4,
                    0,
                    2,
                    StructFieldType::UInt32 as u8,
                    StructFieldType::ArrayU32 as u8,
                ],
                &[
                    Opcode::BatchStructMapUpsertLast as u8,
                    0,
                    0,
                    1,
                    1,
                    0,
                    1,
                    3,
                    4,
                    1,
                ],
            ),
            5,
        ),
        (
            "pairs",
            program(
                1,
                3,
                &map2,
                &[Opcode::BatchStructMap2UpsertLast as u8, 0, 0, 1, 1, 2, 0],
            ),
            3,
        ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn row_count_and_row_tiers_never_clear_a_batch_that_runs_out(
        batches in prop::collection::vec(prop::collection::vec(row(), 1..40), 1..12),
    ) {
        for (name, prog, inputs) in family_programs() {
            let mut state = init(&prog);
            let mut vm = Vm::default();
            for rows in &batches {
                let batch = Batch::of(rows);
                let cols = batch.cols();
                run_growing(&mut vm, &mut state, &prog, &cols[..usize::from(inputs)], rows.len() as u32);
            }
            let counts = vm.capacity_counts();
            prop_assert!(counts.bound + counts.probed + counts.dry_run_fit > 0, "{}: no batch ran", name);
        }
    }
}

/// A new outer key needs its initial container in an arena that inner
/// rehashes may already have spent down to a few bytes: interleaving the two
/// walks the arena through every remainder.
#[test]
fn nested_initial_containers_are_admitted_against_the_spent_arena() {
    let prog = program(
        1,
        3,
        &[
            Opcode::SlotNested as u8,
            0,
            SlotType::Nested as u8,
            4,
            0,
            SlotType::HashSet as u8,
            1,
            0,
            0,
        ],
        &for_each(&[Opcode::NestedSetInsert as u8, 0, 0, 1]),
    );
    let mut state = init(&prog);
    let mut vm = Vm::default();
    let types = [TYPE];
    for i in 0..600u32 {
        for (outer, inner) in [(1, i), (10_000 + i, 0)] {
            let (keys, values) = ([outer], [inner]);
            let cols = [
                u32s_as_bytes(&keys),
                u32s_as_bytes(&values),
                u32s_as_bytes(&types),
            ];
            run_growing(&mut vm, &mut state, &prog, &cols, 1);
        }
    }

    // A sentinel outer or inner key is no key growth could place: refused
    // by name with nothing written, never a request to grow.
    for (outer, inner) in [
        (u32::MAX, 3),
        (u32::MAX - 1, 3),
        (2, u32::MAX),
        (2, u32::MAX - 1),
    ] {
        let (keys, values) = ([outer], [inner]);
        let cols = [
            u32s_as_bytes(&keys),
            u32s_as_bytes(&values),
            u32s_as_bytes(&types),
        ];
        let before = state.clone();
        assert_eq!(
            ErrorCode::InvalidKey as u32,
            vm.execute_batch(&mut state, &prog, &cols, 1),
            "outer {outer:#x}, inner {inner:#x}"
        );
        assert_eq!(before, state);
    }

    // One batch grows outer key 1's inner set past its load factor partway:
    // the rows before the rehash insert plainly, the rehash needs a plan as
    // large as the set. A VM whose scratch only ever planned small rehashes
    // is refused as a whole while every allocation fails, with nothing
    // written, rather than after its plain rows. The arena is grown first so
    // that only memory, not capacity, can refuse the batch.
    let keys = [1u32; 160];
    let values: Vec<u32> = (1000..1160).collect();
    let row_types = [TYPE; 160];
    let cols = [
        u32s_as_bytes(&keys),
        u32s_as_bytes(&values),
        u32s_as_bytes(&row_types),
    ];
    loop {
        let mut trial = state.clone();
        match Vm::default().execute_batch(&mut trial, &prog, &cols, 160) {
            OK => break,
            NEEDS_GROWTH => state = grow(&mut vm, &state, needs_growth_slot()),
            other => panic!("trial batch refused with {other}"),
        }
    }
    // A VM that has never run a batch cannot back its declaration check,
    // and one that has only admitted a one-row batch cannot back admission's
    // per-row storage for 160 rows, while every allocation fails: both are
    // refused by name with nothing written, never aborted.
    let mut fresh = Vm::default();
    let before = state.clone();
    assert_eq!(
        ErrorCode::OutOfMemory as u32,
        without_memory(|| fresh.execute_batch(&mut state, &prog, &cols, 160))
    );
    assert_eq!(before, state, "a refused batch writes nothing");
    let mut one_row_state = state.clone();
    run_growing(
        &mut fresh,
        &mut one_row_state,
        &prog,
        &[
            u32s_as_bytes(&[3u32]),
            u32s_as_bytes(&[5u32]),
            u32s_as_bytes(&types),
        ],
        1,
    );
    assert_eq!(
        ErrorCode::OutOfMemory as u32,
        without_memory(|| fresh.execute_batch(&mut state, &prog, &cols, 160))
    );
    assert_eq!(before, state, "admission refuses before any write");

    // Warm a fresh VM on a copy: 160 rows of new outer keys, so admission
    // and plain nested plans have run at this batch size, and only tiny
    // inner tables have rehashed.
    let mut cold = Vm::default();
    let warm_keys: Vec<u32> = (50_000..50_160).collect();
    let warm_values = [0u32; 160];
    let warm_cols = [
        u32s_as_bytes(&warm_keys),
        u32s_as_bytes(&warm_values),
        u32s_as_bytes(&row_types),
    ];
    let mut warm_state = state.clone();
    run_growing(&mut cold, &mut warm_state, &prog, &warm_cols, 160);
    let before = state.clone();
    assert_eq!(
        ErrorCode::OutOfMemory as u32,
        without_memory(|| cold.execute_batch(&mut state, &prog, &cols, 160))
    );
    assert_eq!(before, state, "a refused batch writes nothing");
    assert_eq!(OK, cold.execute_batch(&mut state, &prog, &cols, 160));
}

// ---------------------------------------------------------------------------
// The bitmap image bound against real slot patches, in sequence.
// ---------------------------------------------------------------------------

fn bitmap_slot(capacity: u32) -> (Vec<u8>, SlotMetaView) {
    let data = STATE_HEADER_SIZE + SLOT_META_SIZE;
    let mut state = vec![0u8; (data + capacity * 4 + 4) as usize];
    state[..4].copy_from_slice(&STATE_MAGIC.to_le_bytes());
    state[StateHeaderOffset::FORMAT_VERSION as usize] = STATE_FORMAT_VERSION;
    state[StateHeaderOffset::NUM_SLOTS as usize] = 1;
    let base = STATE_HEADER_SIZE;
    state[(base + SlotMetaOffset::OFFSET) as usize..][..4].copy_from_slice(&data.to_le_bytes());
    state[(base + SlotMetaOffset::CAPACITY) as usize..][..4]
        .copy_from_slice(&capacity.to_le_bytes());
    state[(base + SlotMetaOffset::TYPE_FLAGS) as usize] = SlotType::Bitmap as u8;
    state[data as usize..][..4].copy_from_slice(&EMPTY_U32_IMAGE);
    let meta = SlotMetaView::read(&state, 0);
    (state, meta)
}

/// Members shaped to stress the arms: strides, runs, dense chunks, isolated
/// values one per chunk, scattered values.
fn shaped() -> impl Strategy<Value = Vec<u32>> {
    let chunk = 0u32..12;
    prop::collection::vec(
        prop_oneof![
            (chunk.clone(), 0u32..60_000, 1u32..64, 1u32..400).prop_map(|(c, s, step, n)| {
                (0..n)
                    .map(|i| (c << 16) | ((s + i * step) & 0xffff))
                    .collect::<Vec<_>>()
            }),
            (chunk.clone(), 0u32..60_000, 1u32..5000).prop_map(|(c, s, n)| {
                (0..n)
                    .map(|i| (c << 16) | ((s + i) & 0xffff))
                    .collect::<Vec<_>>()
            }),
            (chunk, 0u32..65_536).prop_map(|(c, v)| vec![(c << 16) | v]),
            (0u32..65_536, 0u32..65_536).prop_map(|(a, b)| vec![a << 16 | b]),
            // Scattered members, most of them alone in their chunk.
            prop::collection::vec(any::<u32>(), 1..300),
        ],
        0..8,
    )
    .prop_map(|parts| parts.concat())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every image reachable during a sequence of patches stays within the
    /// bound computed from the starting image and the members the whole
    /// sequence adds or touches.
    #[test]
    fn bitmap_image_never_exceeds_its_admission_bound(
        initial in shaped(),
        ops in prop::collection::vec((any::<bool>(), shaped()), 1..5),
    ) {
        let (mut state, meta) = bitmap_slot(1 << 17);
        let storage = get_bitmap_storage(&meta);
        let mut env = BitmapEnv::default();
        let mut hooks = NoVm::default();
        let mut sorted = initial.clone();
        sorted.sort_unstable();
        sorted.dedup();
        prop_assert_eq!(ErrorCode::Ok, batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &sorted, None));
        let start = state.clone();
        let mut set: BTreeSet<u32> = sorted.iter().copied().collect();
        let (mut adds, mut touches) = (0u64, 0u64);
        for (add, values) in ops {
            for &v in &values {
                if add && !set.contains(&v) { adds += 1; touches += 1; }
                if !add && set.contains(&v) { touches += 1; }
            }
            let code = if add {
                batch_bitmap_add(&mut env, &mut hooks, false, &mut state, &meta, &values, None)
            } else {
                batch_bitmap_remove(&mut env, &mut hooks, false, &mut state, &meta, &values)
            };
            prop_assert_eq!(ErrorCode::Ok, code);
            for &v in &values {
                if add { set.insert(v); } else { set.remove(&v); }
            }
            let now = u64::from(storage.serialized_len(&state).unwrap());
            let bound = patched_len_bound(storage.serialized_data(&start).unwrap(), adds, touches).unwrap();
            prop_assert!(now <= bound, "image {} > bound {} (adds {}, touches {})", now, bound, adds, touches);
        }
        let view = BitmosaicView::open(storage.serialized_data(&state).unwrap()).unwrap();
        prop_assert_eq!(view.len(), set.len() as u64);
    }
}
