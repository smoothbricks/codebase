//! Row partition equivalence at the VM owner, including physical continuation.
mod common;
use columine_types::{
    PROGRAM_MAGIC,
    types::{ErrorCode, SlotMetaOffset},
};
use columine_vm::{
    key_index::NoStrings,
    meta::SlotMetaView,
    state_init::{DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state},
    temporal::TEMPORAL_MAGIC,
    undo_log::FLAT_UNDO_ENTRY_SIZE,
    vm::{CheckedStructMap, Vm, i64s_as_bytes, u32s_as_bytes},
};
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
struct Row {
    kind: u32,
    key: u32,
    deadline: i64,
    now: i64,
    guard: u32,
}
fn program() -> Vec<u8> {
    program_fields(true)
}
fn program_fields(stored: bool) -> Vec<u8> {
    let mut init = vec![0x18, 0, 6, 16, 0, 1, 1]; // one i64 field
    init.extend([0x10, 1, 0x90, 32, 0]);
    init.extend([0; 8]);
    init.extend([3, 0, 0]); // zero TTL/grace, clock column, no start-of, HALT
    let body = if stored {
        vec![0x80, 0, 1, 1, 2, 0, 0]
    } else {
        vec![0x80, 0, 1, 0, 0]
    };
    let mut reduce = vec![0xe0, 0, 1];
    reduce.extend(1u32.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    reduce.extend([0x8a, 0, 4, 0]);
    let mut program = vec![0u8; 32];
    program.extend(PROGRAM_MAGIC.to_le_bytes());
    program.extend([2, 0, 0, 0]);
    program.extend(2u32.to_le_bytes());
    program.extend(5u32.to_le_bytes());
    program.extend((init.len() as u32).to_le_bytes());
    program.extend((reduce.len() as u32).to_le_bytes());
    program.extend(init);
    program.extend(reduce);
    program.extend(TEMPORAL_MAGIC.to_le_bytes());
    program.extend(32u32.to_le_bytes());
    program.extend(3u32.to_le_bytes());
    program.extend(1u32.to_le_bytes());
    program.extend(0u32.to_le_bytes());
    program.extend(4u32.to_le_bytes());
    program.extend(1u32.to_le_bytes());
    program.extend([0, 1, 0, 0]);
    program.extend(0i64.to_le_bytes());
    program
}
fn image(program: &[u8]) -> Vec<u8> {
    let len =
        calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("program sizes");
    let mut image = vec![0; len as usize];
    init_state(&mut image, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("program initializes");
    image
}
fn run(vm: &mut Vm, state: &mut [u8], program: &[u8], rows: &[Row]) -> u32 {
    let kinds: Vec<u32> = rows.iter().map(|r| r.kind).collect();
    let keys: Vec<u32> = rows.iter().map(|r| r.key).collect();
    let values: Vec<i64> = rows.iter().map(|r| r.deadline).collect();
    let clocks: Vec<i64> = rows.iter().map(|r| r.now).collect();
    let guards: Vec<u32> = rows.iter().map(|r| r.guard).collect();
    vm.execute_batch_delta(
        state,
        program,
        &[
            u32s_as_bytes(&kinds),
            u32s_as_bytes(&keys),
            i64s_as_bytes(&values),
            i64s_as_bytes(&clocks),
            u32s_as_bytes(&guards),
        ],
        rows.len() as u32,
    )
}
fn projection(state: &[u8]) -> Vec<u32> {
    let map = CheckedStructMap::bind(state, 0).expect("valid map");
    let mut position = map.iter_start();
    let mut keys = Vec::new();
    while position < map.capacity() {
        keys.push(map.iter_key(position).expect("live key"));
        position = map.iter_next(position).expect("valid cursor");
    }
    keys.sort_unstable();
    keys
}
fn settled(vm: &mut Vm, mut state: Vec<u8>) -> Vec<u8> {
    vm.seal_state(&mut state, &NoStrings).expect("seal image");
    for slot in 0..2 {
        let meta = SlotMetaView::read(&state, slot);
        state[(meta.meta_base + SlotMetaOffset::CHANGE_FLAGS) as usize] = 0;
    }
    state
}
fn check(rows: &[Row]) {
    let p = program();
    let mut whole = image(&p);
    let mut single = whole.clone();
    let mut vm = Vm::default();
    let mut other = Vm::default();
    assert_eq!(run(&mut vm, &mut whole, &p, rows), 0, "whole {rows:?}");
    let mut expected = BTreeMap::new();
    for row in rows {
        if row.kind == 1 {
            expected.insert(row.key, (row.deadline, row.guard));
        }
        expected.retain(|_, value| value.1 == 0 || value.0 > row.now);
        assert_eq!(
            run(&mut other, &mut single, &p, std::slice::from_ref(row)),
            0,
            "singleton {row:?}"
        );
        assert_eq!(
            projection(&single),
            expected.keys().copied().collect::<Vec<_>>()
        );
    }
    assert_eq!(
        projection(&whole),
        expected.keys().copied().collect::<Vec<_>>()
    );
    assert_eq!(
        settled(&mut vm, whole),
        settled(&mut other, single),
        "whole image vs singleton image {rows:?}"
    );
}
#[test]
fn high_time_erasure_survives_earlier_irrelevant_or_inactive_rows() {
    for kind in [0, 2] {
        check(&[
            Row {
                kind: 1,
                key: 1,
                deadline: 50,
                now: 0,
                guard: 1,
            },
            Row {
                kind,
                key: 2,
                deadline: 1000,
                now: 100,
                guard: 1,
            },
            Row {
                kind: 2,
                key: 2,
                deadline: 0,
                now: 1,
                guard: 1,
            },
        ]);
    }
}
#[test]
fn adjacent_micros_past_f64_precision_expire_at_their_own_inclusive_boundaries() {
    let base = (1i64 << 53) + 1;
    check(&[
        Row {
            kind: 1,
            key: 1,
            deadline: base,
            now: base - 1,
            guard: 1,
        },
        Row {
            kind: 1,
            key: 2,
            deadline: base + 1,
            now: base,
            guard: 1,
        },
        Row {
            kind: 2,
            key: 3,
            deadline: 0,
            now: base - 20,
            guard: 1,
        },
    ]);
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn whole_equals_singletons_with_nonmonotone_clocks_and_keep_last(
        rows in prop::collection::vec((0u32..3,1u32..5,-20i64..100,-20i64..100,0u32..2),0..40)) {
        let rows: Vec<Row> = rows.into_iter().map(|(kind,key,deadline,now,guard)| Row {kind,key,deadline,now,guard}).collect();
        check(&rows);
    }
}
#[test]
fn idle_expiry_does_not_evaluate_stored_rows_or_record_any_mutation() {
    use columine_types::types::CmpType;
    use columine_vm::row_exprs::{
        BatchView, Binding, RowCell, RowColumns, RowExpression, bind_row_columns,
    };
    struct Guard {
        calls: usize,
    }
    impl RowExpression for Guard {
        fn admit(&mut self, _: &[u8], _: CmpType, _: &BatchView<'_>) -> Result<Binding, ErrorCode> {
            Ok(Binding::Live)
        }
        fn eval(
            &mut self,
            _: &[u8],
            _: CmpType,
            batch: &BatchView<'_>,
            _: u32,
            _: &mut dyn columine_vm::logical_export::Interner,
        ) -> Result<RowCell, ErrorCode> {
            assert!(batch.stored_row().is_some());
            self.calls += 1;
            Ok(RowCell::U32(1))
        }
    }
    let mut p = program();
    let table_at = p.len() - 40;
    let temporal = p.split_off(table_at);
    p.extend(columine_vm::row_exprs::ROW_EXPRESSIONS_MAGIC.to_le_bytes());
    p.extend([1, 0, 0, 4, 3]); // one predicate, type col0, target4, three match ids
    for kind in [0u32, 1, 2] {
        p.extend(kind.to_le_bytes());
    }
    p.extend([1, 0, 0]); // one-byte evaluator payload
    p.extend(temporal);
    let mut state = image(&p);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &p,
            &[Row {
                kind: 1,
                key: 1,
                deadline: 10000,
                now: 0,
                guard: 1
            }]
        ),
        0
    );
    vm.undo_enable(&state).expect("journal admits image");
    let at = vm.undo_checkpoint();
    let before = state.clone();
    let types = vec![2; 999];
    let keys = vec![2; 999];
    let values = vec![0i64; 999];
    let clocks: Vec<i64> = (1..1000).collect();
    let mut eval = Guard { calls: 0 };
    let mut rows = RowColumns::new();
    let mut cols = vec![
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        i64s_as_bytes(&values),
        i64s_as_bytes(&clocks),
        &[],
    ];
    let mut live = bind_row_columns(
        &p,
        &state,
        &mut cols,
        999,
        &mut eval,
        &mut NoStrings,
        &mut rows,
    )
    .expect("bind live guard");
    assert_eq!(
        vm.execute_batch_delta_live(&mut state, &p, &cols, 999, &mut live),
        0
    );
    common::memory::with_allocations(0, || {
        assert_eq!(
            vm.execute_batch_delta_live(&mut state, &p, &cols, 999, &mut live),
            0
        );
    });
    assert_eq!(
        eval.calls, 0,
        "irrelevant rows never visit a stored predicate"
    );
    assert_eq!(
        vm.undo_checkpoint(),
        at,
        "no index mutation or flag write on the idle path"
    );
    assert_eq!(state, before);
}
#[test]
fn exact_undo_redo_and_continuation_include_the_schedule() {
    let p = program();
    let mut state = image(&p);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &p,
            &[Row {
                kind: 1,
                key: 1,
                deadline: 50,
                now: 0,
                guard: 1
            }]
        ),
        0
    );
    let before = state.clone();
    vm.undo_enable(&state).expect("journal");
    let at = vm.undo_checkpoint();
    let rows = [
        Row {
            kind: 2,
            key: 2,
            deadline: 0,
            now: 100,
            guard: 1,
        },
        Row {
            kind: 1,
            key: 1,
            deadline: 500,
            now: 1,
            guard: 1,
        },
    ];
    assert_eq!(run(&mut vm, &mut state, &p, &rows), 0);
    let after = state.clone();
    let end = vm.undo_checkpoint();
    vm.delta_export_segment(at, end).expect("paired export");
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    let mut restored = before.clone();
    let mut receiver = Vm::default();
    receiver
        .delta_apply_rollforward_segment(&mut restored, &redo, FLAT_UNDO_ENTRY_SIZE)
        .expect("redo");
    assert_eq!(restored, after);
    let continuation = [Row {
        kind: 2,
        key: 2,
        deadline: 0,
        now: 500,
        guard: 1,
    }];
    assert_eq!(run(&mut receiver, &mut restored, &p, &continuation), 0);
    assert_eq!(run(&mut vm, &mut state, &p, &continuation), 0);
    assert_eq!(restored, state);
    let mut rollback = after;
    receiver
        .delta_apply_rollback_segment(&mut rollback, &undo, FLAT_UNDO_ENTRY_SIZE)
        .expect("undo");
    assert_eq!(rollback, before);
    vm.undo_rollback(&mut state, at)
        .expect("rollback whole window");
    assert_eq!(state, before);
}
#[test]
fn missing_clock_binding_refuses_ttl_before_any_write() {
    let mut p = program();
    p.truncate(p.len() - 40);
    let mut state = image(&p);
    let before = state.clone();
    assert_eq!(
        run(
            &mut Vm::default(),
            &mut state,
            &p,
            &[Row {
                kind: 1,
                key: 1,
                deadline: 5,
                now: 0,
                guard: 1
            }]
        ),
        ErrorCode::InvalidProgram as u32
    );
    assert_eq!(state, before);
}

#[test]
fn explicit_mutation_program_writes_without_advancing_due_deadlines() {
    let mut event = program();
    let init_len = u32::from_le_bytes(event[48..52].try_into().unwrap()) as usize;
    let insert = 56 + init_len - 1;
    event.splice(insert..insert, [0x10, 2, 0x40, 16, 0]);
    event[40..44].copy_from_slice(&3u32.to_le_bytes());
    event[48..52].copy_from_slice(&((init_len + 5) as u32).to_le_bytes());
    let mut state = image(&event);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &event,
            &[Row {
                kind: 1,
                key: 1,
                deadline: 5,
                now: 0,
                guard: 1
            }]
        ),
        0
    );
    let mut mutation = event.clone();
    let reduce = 56 + init_len + 5;
    mutation.truncate(reduce);
    mutation.extend([0x22, 2, 0, 1, 0]);
    mutation[52..56].copy_from_slice(&5u32.to_le_bytes());
    assert_eq!(
        vm.execute_mutations_in(
            &mut state,
            &mutation,
            &[u32s_as_bytes(&[9]), u32s_as_bytes(&[99])],
            1,
            &NoStrings
        ),
        0
    );
    assert_eq!(
        SlotMetaView::read(&state, 2).size(&state),
        1,
        "explicit mutation really wrote"
    );
    assert_eq!(
        projection(&state),
        [1],
        "no synthetic clock evicted the due row"
    );
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &event,
            &[Row {
                kind: 2,
                key: 2,
                deadline: 0,
                now: 10,
                guard: 1
            }]
        ),
        0
    );
    assert!(
        projection(&state).is_empty(),
        "a later real event advances expiry"
    );
    assert_eq!(SlotMetaView::read(&state, 2).size(&state), 1);
}

#[test]
fn absent_deadline_keeps_the_row_without_scheduling_it() {
    let p = program_fields(false);
    let mut state = image(&p);
    let mut vm = Vm::default();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &p,
            &[Row {
                kind: 1,
                key: 1,
                deadline: 1,
                now: 100,
                guard: 1
            }]
        ),
        0
    );
    assert_eq!(projection(&state), [1]);
    assert_eq!(SlotMetaView::read(&state, 1).eviction_index_size(&state), 0);
}

#[test]
fn overflowing_deadline_is_a_typed_refusal_and_exactly_rolls_back() {
    let mut p = program();
    let at = p.len() - 8;
    p[at..].copy_from_slice(&i64::MAX.to_le_bytes());
    let mut state = image(&p);
    let before = state.clone();
    let mut vm = Vm::default();
    vm.undo_enable(&state).expect("journal");
    let checkpoint = vm.undo_checkpoint();
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &p,
            &[Row {
                kind: 1,
                key: 1,
                deadline: 1,
                now: 0,
                guard: 1
            }]
        ),
        ErrorCode::InvalidCellValue as u32
    );
    vm.undo_rollback(&mut state, checkpoint)
        .expect("rollback refusing row");
    assert_eq!(state, before);
}

fn legacy_ttl_program(trigger: bool) -> Vec<u8> {
    use columine_types::types::{PROGRAM_FORMAT_VERSION, ProgramHeader};
    let mut init = vec![0x10, 0, if trigger { 0x30 } else { 0x10 }, 16, 0];
    init.extend(10f32.to_le_bytes());
    init.extend(0f32.to_le_bytes());
    init.extend([2, 0, 0]);
    let body = [0x22, 0, 1, 3];
    let mut reduce = vec![0xe0, 0, 1];
    reduce.extend(1u32.to_le_bytes());
    reduce.extend((body.len() as u16).to_le_bytes());
    reduce.extend(body);
    reduce.push(0);
    let mut p = vec![0; 32];
    p.extend(
        ProgramHeader {
            magic: PROGRAM_MAGIC,
            version: PROGRAM_FORMAT_VERSION,
            num_callbacks: 0,
            flags: 0,
            num_slots: 1,
            num_inputs: 5,
            init_code_len: init.len() as u32,
            reduce_code_len: reduce.len() as u32,
        }
        .to_wire_bytes(),
    );
    p.extend(init);
    p.extend(reduce);
    p.extend(TEMPORAL_MAGIC.to_le_bytes());
    p.extend(8u32.to_le_bytes());
    p.extend(4u32.to_le_bytes());
    p.extend(0u32.to_le_bytes());
    p
}
fn run_legacy(vm: &mut Vm, state: &mut [u8], p: &[u8], rows: &[Row]) -> u32 {
    let kinds: Vec<u32> = rows.iter().map(|r| r.kind).collect();
    let keys: Vec<u32> = rows.iter().map(|r| r.key).collect();
    let ages: Vec<f64> = rows.iter().map(|r| r.deadline as f64).collect();
    let values: Vec<u32> = rows.iter().map(|r| r.guard).collect();
    let clocks: Vec<i64> = rows.iter().map(|r| r.now * 1_000_000).collect();
    vm.execute_batch_delta(
        state,
        p,
        &[
            u32s_as_bytes(&kinds),
            u32s_as_bytes(&keys),
            columine_vm::vm::f64s_as_bytes(&ages),
            u32s_as_bytes(&values),
            i64s_as_bytes(&clocks),
        ],
        rows.len() as u32,
    )
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn ttl_before_each_row_uses_event_clock_not_custom_age_column(
        input in prop::collection::vec((0u32..3,1u32..5,-20i64..100,-20i64..100,0u32..2),0..40)) {
        let rows:Vec<Row>=input.into_iter().map(|(kind,key,deadline,now,guard)|Row{kind,key,deadline,now,guard}).collect();
        let p=legacy_ttl_program(false); let mut whole=image(&p); let mut single=whole.clone();
        let mut vm=Vm::default(); let mut other=Vm::default(); let mut expected=BTreeMap::new();
        assert_eq!(run_legacy(&mut vm,&mut whole,&p,&rows),0);
        for row in &rows {
            expected.retain(|_, &mut (_,age): &mut (u32,i64)| age >= row.now-10);
            if row.kind==1 { expected.insert(row.key,(row.guard,row.deadline)); }
            assert_eq!(run_legacy(&mut other,&mut single,&p,std::slice::from_ref(row)),0);
            let map=columine_vm::vm::CheckedMap::bind(&single,0).expect("map");
            for key in 1..5 { assert_eq!(map.get(key),expected.get(&key).map(|entry|entry.0)); }
        }
        assert_eq!(whole,single,"same physical TTL continuation for {rows:?}");
    }
}
#[test]
fn trigger_buffer_overflow_refuses_the_whole_batch_without_dropping_observations() {
    let p = legacy_ttl_program(true);
    let mut state = image(&p);
    let before = state.clone();
    let rows: Vec<Row> = (0..columine_vm::state_init::EVICTED_BUFFER_CAP + 2)
        .map(|now| {
            let now = i64::from(now);
            Row {
                kind: 1,
                key: 1,
                deadline: now - 11,
                now,
                guard: 1,
            }
        })
        .collect();
    let result = run_legacy(&mut Vm::default(), &mut state, &p, &rows);
    assert_eq!(result, ErrorCode::CapacityExceeded as u32);
    assert_eq!(
        state, before,
        "capacity admission refuses before the first event write"
    );
}
