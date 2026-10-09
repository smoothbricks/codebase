//! Journal profile oracles through the public, read-only status surface.
//!
//! A window is either production (undo-only) or scenario (paired undo+redo),
//! and one window may mix both. These tests drive real state through
//! `init_state`, `map_upsert`, and `capture_mutation`, then read only
//! `undo_status`, `undo_redo_lane_status`, the exported segments, and the state
//! image. The journal's privacy is enforced by the compile-fail doctests on
//! `Vm::undo_status` in `src/vm.rs`, not by a field-access test here.
//!
//! Physical writes through `capture_mutation` honour its contract: the callback
//! receives the whole image but writes only its declared range (vm.rs
//! `capture_mutation`, external physical writers). Every capture here
//! declares exactly the 8-byte cell it changes.
//!
//! Allocation claims use a thread-local scoped counter over the system
//! allocator, the same forwarding pattern as `containers.rs`.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{AggType, ErrorCode, Opcode, SlotType};
use columine_vm::bytes;
use columine_vm::hashmap_ops::{CmpType, Strategy, bind_slot_map};
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state};
use columine_vm::vm::{RedoLaneStatus, UNDO_CAPACITY, UndoStatus, Vm};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// Assemble a program: 32-byte hash prefix, content header, init, reduce.
/// Mirrors the fixtures in `dispatch.rs`; an init-only program carries a lone
/// HALT in its reduce section.
fn program(num_slots: u32, num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(0);
    let mut reduce = reduce.to_vec();
    reduce.push(0);
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(num_slots.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend((init.len() as u32).to_le_bytes());
    prog.extend((reduce.len() as u32).to_le_bytes());
    prog.extend(init);
    prog.extend(reduce);
    prog
}

/// One HASHMAP slot of the requested capacity, built by `init_state`.
fn map_program(capacity: u16) -> Vec<u8> {
    let [lo, hi] = capacity.to_le_bytes();
    program(
        1,
        0,
        &[Opcode::SlotDef as u8, 0, SlotType::HashMap as u8, lo, hi],
        &[],
    )
}

/// One COUNT aggregate slot, built by `init_state`. Its 8-byte count cell sits
/// at the slot's data offset and starts at zero.
fn count_program() -> Vec<u8> {
    program(
        1,
        0,
        &[
            Opcode::SlotDef as u8,
            0,
            SlotType::Aggregate as u8,
            AggType::Count as u8,
            0,
        ],
        &[],
    )
}

fn init(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS)
        .expect("valid program admission");
    assert!(size > 0, "program must be accepted");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state");
    state
}

fn upsert(vm: &mut Vm, state: &mut [u8], key: u32, value: u32, delta: bool) -> ErrorCode {
    vm.map_upsert(
        Strategy::Last,
        delta,
        state,
        0,
        &[key],
        &[value],
        None,
        CmpType::F64,
    )
}

fn lookup(state: &[u8], key: u32) -> Option<u32> {
    bind_slot_map(&SlotMetaView::read(state, 0)).get_u32(state, key)
}

/// Data offset of the COUNT cell, read from the slot's metadata.
fn count_cell(state: &[u8]) -> u32 {
    SlotMetaView::read(state, 0).offset
}

fn count_value(state: &[u8], cell: u32) -> u64 {
    bytes::read_u64(state, cell)
}

/// Write the count cell through its declared 8-byte range. The callback writes
/// only that range; the rest of the image it is handed is never touched.
fn set_count(vm: &mut Vm, state: &mut [u8], cell: u32, value: u64) -> Result<(), ErrorCode> {
    vm.capture_mutation(false, state, &[(cell, 8)], |image| {
        bytes::write_u64(image, cell, value)
    })
}

fn image_len(state: &[u8]) -> u32 {
    u32::try_from(state.len()).expect("image fits u32")
}

// ---------------------------------------------------------------------------
// Scoped allocation counting (same forwarding pattern as containers.rs).
// ---------------------------------------------------------------------------

struct CountingAllocator;

thread_local! {
    // Thread-local accounting excludes allocations made by concurrent tests.
    static ALLOCATION_COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn count_allocation() {
    let _ = ALLOCATION_COUNT.try_with(|count| {
        if let Some(current) = count.get() {
            count.set(Some(current + 1));
        }
    });
}

// SAFETY: every allocation and deallocation forwards the original pointer and
// layout to System; the counter observes calls and never changes the contract.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct AllocationScope;

impl Drop for AllocationScope {
    fn drop(&mut self) {
        ALLOCATION_COUNT.with(|count| count.set(None));
    }
}

/// Run `run` and return its result with the allocations this thread made.
fn allocations_during<T>(run: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATION_COUNT.with(|count| {
        assert!(count.get().is_none(), "allocation scopes cannot nest");
        count.set(Some(0));
    });
    let scope = AllocationScope;
    let result = run();
    let count = ALLOCATION_COUNT.with(|count| count.get().expect("active allocation scope"));
    drop(scope);
    (result, count)
}

// ---------------------------------------------------------------------------
// Profile oracles.
// ---------------------------------------------------------------------------

/// A fresh VM reports no journal and no redo lane. A pure window then records
/// real map writes as undo-only: the paired prefix stays at zero, the redo lane
/// is never admitted (inactive, no entries, no capacity), and a paired export of
/// the window is refused because there is no redo lane to export.
#[test]
fn pure_window_writes_undo_only_and_never_admits_the_redo_lane() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let before = state.clone();
    let mut vm = Vm::default();

    assert_eq!(
        UndoStatus {
            enabled: false,
            overflow: false,
            checkpoint: 0,
            delta_count: 0
        },
        vm.undo_status()
    );
    assert_eq!(
        RedoLaneStatus {
            active: false,
            entries: 0,
            capacity: 0
        },
        vm.undo_redo_lane_status()
    );

    vm.undo_enable(&state).expect("undo window opens");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: 0,
            delta_count: 0
        },
        vm.undo_status()
    );

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, false));
    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 43, 200, false));
    let end = vm.undo_checkpoint();
    assert!(end > 0, "real map writes journal undo entries");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: end,
            delta_count: 0
        },
        vm.undo_status(),
        "no paired write ever advances the paired prefix"
    );
    assert_eq!(
        RedoLaneStatus {
            active: false,
            entries: 0,
            capacity: 0
        },
        vm.undo_redo_lane_status(),
        "a pure window never reserves or writes the redo lane"
    );
    assert_eq!(
        Err(ErrorCode::DeltaLaneMismatch),
        vm.delta_export_segment(0, end),
        "a pure window has no redo lane to export"
    );
    assert_eq!(Some(100), lookup(&state, 42));
    assert_eq!(Some(200), lookup(&state, 43));

    assert_eq!(Ok(image_len(&state)), vm.undo_rollback(&mut state, 0));
    assert_eq!(
        before, state,
        "production rollback restores the image byte for byte"
    );
}

/// A pure physical write after a warm-up allocates nothing. `undo_enable`
/// reserves the journal's storage, so a measured capture on a fresh VM touches
/// only reserved memory, journals exactly its one changed cell, and never
/// admits the redo lane. The warm write stays in the image and rollback
/// restores the complete image.
#[test]
fn warm_pure_capture_allocates_nothing_and_admits_no_redo() {
    let prog = count_program();
    let mut state = init(&prog);
    let original = state.clone();
    let cell = count_cell(&state);
    assert_eq!(0, count_value(&state, cell), "the count starts at zero");
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");

    set_count(&mut vm, &mut state, cell, 1).expect("warm write");
    assert_eq!(
        1,
        vm.undo_checkpoint(),
        "the warm write changes the count cell: one entry"
    );

    let (result, allocations) = allocations_during(|| set_count(&mut vm, &mut state, cell, 2));
    assert_eq!(Ok(()), result);
    assert_eq!(
        0, allocations,
        "the reserved journal absorbs a pure write without allocating"
    );
    assert_eq!(
        2,
        vm.undo_checkpoint(),
        "the measured write is a real journaled change"
    );
    assert_eq!(2, count_value(&state, cell));
    assert_eq!(
        RedoLaneStatus {
            active: false,
            entries: 0,
            capacity: 0
        },
        vm.undo_redo_lane_status(),
        "no redo lane on a pure VM, before or after the measured write"
    );
    assert_eq!(0, vm.undo_status().delta_count);

    assert_eq!(Ok(image_len(&state)), vm.undo_rollback(&mut state, 0));
    assert_eq!(
        original, state,
        "rollback restores the complete image, warm write included"
    );
}

/// A paired write admits the redo lane before its first write. Every journaled
/// entry then has a redo partner, the paired prefix covers the whole window,
/// and the exported undo and redo segments replay byte-exactly in both
/// directions on a fresh VM.
#[test]
fn paired_window_admits_the_redo_lane_and_exports_a_replayable_range() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, true));
    let end = vm.undo_checkpoint();
    assert!(end > 0, "the paired write journals");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: end,
            delta_count: end
        },
        vm.undo_status(),
        "the paired prefix covers the whole paired window"
    );
    let lane = vm.undo_redo_lane_status();
    assert!(lane.active, "the paired writer activates the lane");
    assert_eq!(
        end as usize, lane.entries,
        "every undo entry has a redo partner"
    );
    assert!(
        lane.capacity >= UNDO_CAPACITY as usize,
        "the lane reserves its scenario capacity before the first write"
    );

    assert_eq!(Ok(end), vm.delta_export_segment(0, end));
    let undo = vm.delta_export_undo_bytes();
    let redo_bytes = vm.delta_export_redo_bytes();
    let after = state.clone();
    let entry = Vm::delta_export_entry_size();

    let mut replay = Vm::default();
    let mut rolled_back = after.clone();
    assert_eq!(
        Ok(image_len(&after)),
        replay.delta_apply_rollback_segment(&mut rolled_back, &undo, entry)
    );
    assert_eq!(
        before, rolled_back,
        "the exported undo restores the window's start"
    );
    let mut rolled_forward = before.clone();
    assert_eq!(
        Ok(image_len(&after)),
        replay.delta_apply_rollforward_segment(&mut rolled_forward, &redo_bytes, entry)
    );
    assert_eq!(
        after, rolled_forward,
        "the exported redo restores the window's end"
    );

    assert_eq!(Ok(image_len(&after)), vm.undo_rollback(&mut state, 0));
    assert_eq!(before, state);
}

/// A window may hold both profiles. A range containing any undo-only record
/// returns `DeltaLaneMismatch`, and the refusal leaves the previously exported
/// selection in place: its undo bytes, redo bytes, length, and overflow flag.
/// The earlier paired prefix stays exportable.
#[test]
fn mixed_window_refuses_a_range_with_an_undo_only_record_and_keeps_the_last_selection() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, true));
    let paired_end = vm.undo_checkpoint();
    assert_eq!(Ok(paired_end), vm.delta_export_segment(0, paired_end));
    let previous_undo = vm.delta_export_undo_bytes();
    let previous_redo = vm.delta_export_redo_bytes();
    let previous_len = vm.delta_export_len_bytes();
    let previous_overflow = vm.delta_export_overflow();

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 43, 200, false));
    let mixed_end = vm.undo_checkpoint();
    assert!(
        mixed_end > paired_end,
        "the ordinary writer journals undo-only entries"
    );
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: mixed_end,
            delta_count: paired_end
        },
        vm.undo_status(),
        "undo-only entries never advance the paired prefix"
    );
    let lane = vm.undo_redo_lane_status();
    assert!(lane.active);
    assert_eq!(
        mixed_end as usize, lane.entries,
        "an undo-only entry occupies a lane slot with no redo payload"
    );

    assert_eq!(
        Err(ErrorCode::DeltaLaneMismatch),
        vm.delta_export_segment(0, mixed_end)
    );
    assert_eq!(
        previous_undo,
        vm.delta_export_undo_bytes(),
        "refusal keeps the previous undo bytes"
    );
    assert_eq!(
        previous_redo,
        vm.delta_export_redo_bytes(),
        "refusal keeps the previous redo bytes"
    );
    assert_eq!(previous_len, vm.delta_export_len_bytes());
    assert_eq!(previous_overflow, vm.delta_export_overflow());

    assert_eq!(
        Ok(paired_end),
        vm.delta_export_segment(0, paired_end),
        "the paired prefix stays exportable"
    );
    assert_eq!(previous_undo, vm.delta_export_undo_bytes());
    assert_eq!(previous_redo, vm.delta_export_redo_bytes());
}

/// Activation after undo-only writes pads the redo lane with the earlier
/// records. The paired suffix exports on its own range, while every range
/// reaching back into the undo-only prefix is refused. The suffix replays
/// exactly to the prefix checkpoint.
#[test]
fn paired_suffix_after_an_undo_only_prefix_exports_only_its_own_range() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, false));
    let prefix_end = vm.undo_checkpoint();
    assert!(prefix_end > 0, "the undo-only prefix journals");
    assert!(
        !vm.undo_redo_lane_status().active,
        "the lane waits for its first paired writer"
    );
    let before_suffix = state.clone();

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 43, 200, true));
    let end = vm.undo_checkpoint();
    assert!(end > prefix_end, "the paired suffix journals");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: end,
            delta_count: end
        },
        vm.undo_status()
    );
    assert_eq!(
        Err(ErrorCode::DeltaLaneMismatch),
        vm.delta_export_segment(0, end),
        "the undo-only prefix cannot be part of a paired export"
    );
    assert_eq!(
        Ok(end - prefix_end),
        vm.delta_export_segment(prefix_end, end)
    );
    let undo = vm.delta_export_undo_bytes();
    let redo_bytes = vm.delta_export_redo_bytes();
    let entry = Vm::delta_export_entry_size();
    let after = state.clone();

    let mut replay = Vm::default();
    let mut rolled_back = after.clone();
    assert_eq!(
        Ok(image_len(&after)),
        replay.delta_apply_rollback_segment(&mut rolled_back, &undo, entry)
    );
    assert_eq!(
        before_suffix, rolled_back,
        "the suffix undoes exactly to the prefix image"
    );
    let mut rolled_forward = before_suffix.clone();
    assert_eq!(
        Ok(image_len(&after)),
        replay.delta_apply_rollforward_segment(&mut rolled_forward, &redo_bytes, entry)
    );
    assert_eq!(after, rolled_forward);

    assert_eq!(
        Ok(image_len(&after)),
        vm.undo_rollback(&mut state, prefix_end)
    );
    assert_eq!(
        before_suffix, state,
        "production rollback to the prefix checkpoint restores its image"
    );
}

/// Ending a paired window disables undo and keeps the lane's reusable storage.
/// A reopened pure window reports an inactive lane with no entries, however
/// much capacity it retains, and its range is still refused for paired export.
#[test]
fn ended_paired_window_retains_lane_storage_but_a_reopened_pure_window_stays_inactive() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, true));
    let retained = vm.undo_redo_lane_status().capacity;
    assert!(
        retained >= UNDO_CAPACITY as usize,
        "the paired window reserved the lane"
    );

    vm.undo_commit();
    assert_eq!(
        UndoStatus {
            enabled: false,
            overflow: false,
            checkpoint: 0,
            delta_count: 0
        },
        vm.undo_status()
    );
    assert_eq!(
        RedoLaneStatus {
            active: false,
            entries: 0,
            capacity: retained
        },
        vm.undo_redo_lane_status(),
        "end disables the lane and keeps its reusable storage"
    );

    vm.undo_enable(&state).expect("undo window reopens");
    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 43, 200, false));
    let end = vm.undo_checkpoint();
    assert!(end > 0, "the reopened pure window journals");
    assert_eq!(
        RedoLaneStatus {
            active: false,
            entries: 0,
            capacity: retained
        },
        vm.undo_redo_lane_status(),
        "a reopened pure window never admits the lane, however much storage it retains"
    );
    assert_eq!(0, vm.undo_status().delta_count);
    assert_eq!(
        Err(ErrorCode::DeltaLaneMismatch),
        vm.delta_export_segment(0, end)
    );
    vm.undo_commit();
}

/// Beginning while a window is active is refused before any reset or state
/// access. The refusal leaves the window's status, checkpoint, image and last
/// selected export exactly as they were, and the window keeps accepting writes.
#[test]
fn enable_on_an_active_window_is_refused_and_preserves_status_checkpoint_and_selection() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, true));
    let end = vm.undo_checkpoint();
    assert_eq!(Ok(end), vm.delta_export_segment(0, end));

    let status = vm.undo_status();
    let lane = vm.undo_redo_lane_status();
    let selected_undo = vm.delta_export_undo_bytes();
    let selected_redo = vm.delta_export_redo_bytes();
    let selected_len = vm.delta_export_len_bytes();
    let selected_overflow = vm.delta_export_overflow();
    let image = state.clone();

    assert_eq!(
        Err(ErrorCode::SpeculationActive),
        vm.undo_enable(&state),
        "beginning an active window is refused"
    );
    assert_eq!(status, vm.undo_status(), "the refusal keeps the status");
    assert_eq!(
        lane,
        vm.undo_redo_lane_status(),
        "the refusal keeps the lane"
    );
    assert_eq!(
        end,
        vm.undo_checkpoint(),
        "the refusal keeps the checkpoint"
    );
    assert_eq!(
        selected_undo,
        vm.delta_export_undo_bytes(),
        "the refusal keeps the selected undo bytes"
    );
    assert_eq!(
        selected_redo,
        vm.delta_export_redo_bytes(),
        "the refusal keeps the selected redo bytes"
    );
    assert_eq!(selected_len, vm.delta_export_len_bytes());
    assert_eq!(selected_overflow, vm.delta_export_overflow());
    assert_eq!(image, state, "the refusal touches no state");

    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 43, 200, true));
    assert!(
        vm.undo_checkpoint() > end,
        "the window still journals after the refusal"
    );
    assert_eq!(
        selected_undo,
        vm.delta_export_undo_bytes(),
        "a write does not move the selected export"
    );

    assert_eq!(Ok(image_len(&state)), vm.undo_rollback(&mut state, end));
    assert_eq!(
        image, state,
        "rollback to the refused-begin checkpoint restores its image"
    );
    assert_eq!(
        end,
        vm.undo_checkpoint(),
        "the rollback lands on the checkpoint that was refused"
    );
    vm.undo_commit();
}

/// The journal refusal is sticky. A capture whose reservation exceeds the
/// remaining budget returns `JournalOverflow` before its writer runs. Rolling
/// back and freeing every entry does not reopen the window; only `undo_commit`
/// followed by `undo_enable` does.
///
/// The budget is reached by real writes: each one advances the COUNT aggregate,
/// so each changes its cell and journals exactly one entry. No padding or
/// oversized state is used to create journal pressure.
#[test]
fn journal_budget_refusal_is_sticky_until_the_window_ends_and_reopens() {
    let prog = count_program();
    let mut state = init(&prog);
    let cell = count_cell(&state);
    let original = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");

    for value in 1..=u64::from(UNDO_CAPACITY) {
        set_count(&mut vm, &mut state, cell, value).expect("write within the budget");
    }
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: UNDO_CAPACITY,
            delta_count: 0
        },
        vm.undo_status()
    );
    assert_eq!(u64::from(UNDO_CAPACITY), count_value(&state, cell));
    let full = state.clone();

    // The next write needs one entry the journal no longer has.
    let mut writer_ran = false;
    let refused = vm.capture_mutation(false, &mut state, &[(cell, 8)], |image| {
        writer_ran = true;
        bytes::write_u64(image, cell, u64::from(UNDO_CAPACITY) + 1)
    });
    assert_eq!(Err(ErrorCode::JournalOverflow), refused);
    assert!(
        !writer_ran,
        "a refused reservation never reaches the writer"
    );
    assert_eq!(full, state, "the refused writer changed nothing");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: true,
            checkpoint: UNDO_CAPACITY,
            delta_count: 0
        },
        vm.undo_status()
    );

    // Refused again for any capture, however small: the diagnostic is sticky.
    assert_eq!(
        Err(ErrorCode::JournalOverflow),
        set_count(&mut vm, &mut state, cell, u64::MAX)
    );

    // Rolling back frees every entry, yet the window stays refused.
    assert_eq!(Ok(image_len(&state)), vm.undo_rollback(&mut state, 0));
    assert_eq!(original, state, "rollback restores the pre-window image");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: true,
            checkpoint: 0,
            delta_count: 0
        },
        vm.undo_status(),
        "freed entries do not reopen a refused window"
    );
    assert_eq!(
        Err(ErrorCode::JournalOverflow),
        set_count(&mut vm, &mut state, cell, u64::MAX)
    );

    vm.undo_commit();
    assert_eq!(
        UndoStatus {
            enabled: false,
            overflow: false,
            checkpoint: 0,
            delta_count: 0
        },
        vm.undo_status(),
        "end clears the refusal and the journal"
    );

    vm.undo_enable(&state).expect("a committed window reopens");
    assert_eq!(
        UndoStatus {
            enabled: true,
            overflow: false,
            checkpoint: 0,
            delta_count: 0
        },
        vm.undo_status()
    );
    set_count(&mut vm, &mut state, cell, 1).expect("the reopened window accepts writes");
    assert_eq!(1, vm.undo_checkpoint());
}

/// `undo_status` returns a value copy. Edits to a copy change nothing in the
/// VM, and the snapshot agrees with the live accessors when it was taken.
#[test]
fn status_snapshot_is_a_value_copy_that_cannot_steer_the_journal() {
    let prog = map_program(16);
    let mut state = init(&prog);
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("undo window opens");
    assert_eq!(ErrorCode::Ok, upsert(&mut vm, &mut state, 42, 100, false));

    let observed = vm.undo_status();
    assert_eq!(vm.undo_checkpoint(), observed.checkpoint);
    let mut copy = observed;
    copy.enabled = false;
    copy.overflow = true;
    copy.checkpoint = 0;
    copy.delta_count = 99;
    assert!(
        !copy.enabled && copy.overflow && copy.checkpoint == 0 && copy.delta_count == 99,
        "the copy took the edits"
    );
    assert_eq!(
        observed,
        vm.undo_status(),
        "the VM's status is unaffected by edits to a copy"
    );

    assert_eq!(
        ErrorCode::Ok,
        upsert(&mut vm, &mut state, 43, 200, false),
        "the window still accepts writes"
    );
    assert!(vm.undo_status().enabled);
    assert!(!vm.undo_status().overflow);
    assert_eq!(vm.undo_checkpoint(), vm.undo_status().checkpoint);
    vm.undo_commit();
}
