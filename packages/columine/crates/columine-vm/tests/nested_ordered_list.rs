//! Behavioral tests for nested slots with inner `ORDERED_LIST` struct rows
//! (`SLOT_NESTED` 0x1a with `inner_type = OrderedList` and `NESTED_LIST_APPEND_STRUCT` 0x93).

use std::cell::Cell;
use std::collections::BTreeMap;

use columine_types::types::{
    ChangeFlag, EMPTY_KEY, ErrorCode, STATE_HEADER_SIZE, SlotMetaOffset, SlotType, StructFieldType,
    TOMBSTONE, align8, struct_field_offset,
};
use columine_types::vo::{UnavailableVoAdmission, VoProgramMetadata};
use columine_types::{InternedStr, PROGRAM_MAGIC};
use columine_vm::bytes;
use columine_vm::key_index::NoStrings;
use columine_vm::logical_export::{
    InternLookup, Interner, LogicalSlot, SlotEncoding, SlotTransfer, export_slot, import_slot,
    rewrite_slot_set,
};
use columine_vm::meta::SlotMetaView;
use columine_vm::nested::{NESTED_PREFIX_SIZE, NestedListView, nested_list_lookup};
use columine_vm::program_admission::{AdmittedProgram, ProgramAdmissionError};
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, ProgramInvalidCause, SizingRefusal,
    calculate_grown_state_size, calculate_state_size, compute_struct_row_layout_padded, init_state,
    validate_state,
};
use columine_vm::struct_map::Validity;
use columine_vm::vm::{Vm, f64s_as_bytes, i64s_as_bytes, u32s_as_bytes};
use proptest::prelude::*;
use proptest::test_runner::{TestCaseError, TestRunner};

const HALT: u8 = 0x00;
const SLOT_NESTED: u8 = 0x1a;
const NESTED_LIST_APPEND_STRUCT: u8 = 0x93;
const FOR_EACH: u8 = 0xe0;
const TYPE_EVENT: u32 = 7;

const UINT32: u8 = StructFieldType::UInt32 as u8;
const INT64: u8 = StructFieldType::Int64 as u8;
const FLOAT64: u8 = StructFieldType::Float64 as u8;
const BOOL: u8 = StructFieldType::Bool as u8;
const STRING: u8 = StructFieldType::String as u8;
const ARRAY_U32: u8 = StructFieldType::ArrayU32 as u8;
const ARRAY_BOOL: u8 = StructFieldType::ArrayBool as u8;
const VO: u8 = StructFieldType::Vo as u8;

#[derive(Default)]
struct TestInterner {
    to_id: BTreeMap<String, u32>,
    to_text: Vec<String>,
}

impl InternLookup for TestInterner {
    fn text(&self, id: u32) -> Option<&str> {
        self.to_text.get(id as usize).map(String::as_str)
    }
}

impl Interner for TestInterner {
    fn intern(&mut self, text: InternedStr<'_>) -> Result<u32, ErrorCode> {
        let s = text.as_str();
        if let Some(&id) = self.to_id.get(s) {
            return Ok(id);
        }
        let id = u32::try_from(self.to_text.len()).expect("intern id fits u32");
        self.to_text.push(s.to_owned());
        self.to_id.insert(s.to_owned(), id);
        Ok(id)
    }
}

fn len_u32(len: usize) -> u32 {
    u32::try_from(len).expect("length fits u32")
}

fn program(num_slots: u32, num_inputs: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; 32];
    prog.extend(PROGRAM_MAGIC.to_le_bytes());
    prog.extend([2, 0, 0, 0]);
    prog.extend(num_slots.to_le_bytes());
    prog.extend(num_inputs.to_le_bytes());
    prog.extend(len_u32(init.len() + 1).to_le_bytes());
    prog.extend(len_u32(reduce.len()).to_le_bytes());
    prog.extend_from_slice(init);
    prog.push(HALT);
    prog.extend_from_slice(reduce);
    prog
}

fn nested_list_init(
    slot: u8,
    outer_cap: u16,
    inner_cap: u16,
    agg_byte: u8,
    fields: &[u8],
) -> Vec<u8> {
    let mut init = vec![SLOT_NESTED, slot, SlotType::Nested as u8];
    init.extend(outer_cap.to_le_bytes());
    init.push(SlotType::OrderedList as u8);
    init.extend(inner_cap.to_le_bytes());
    init.push(agg_byte);
    init.push(u8::try_from(fields.len()).expect("field count fits u8"));
    init.extend_from_slice(fields);
    init
}

fn for_each(type_col: u8, body: &[u8]) -> Vec<u8> {
    let mut reduce = vec![FOR_EACH, type_col, 1];
    reduce.extend(TYPE_EVENT.to_le_bytes());
    reduce.extend(
        u16::try_from(body.len())
            .expect("body fits u16")
            .to_le_bytes(),
    );
    reduce.extend_from_slice(body);
    reduce
}

fn append_body(slot: u8, outer_key_col: u8, pairs: &[(u8, u8)]) -> Vec<u8> {
    let mut body = vec![
        NESTED_LIST_APPEND_STRUCT,
        slot,
        outer_key_col,
        u8::try_from(pairs.len()).expect("pair count fits u8"),
    ];
    for &(val_col, field_idx) in pairs {
        body.push(val_col);
        body.push(field_idx);
    }
    body
}

fn init_fresh(prog: &[u8]) -> Vec<u8> {
    let size =
        calculate_state_size(prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("program must admit");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init_state must succeed");
    validate_state(&state).expect("fresh state must validate");
    state
}

#[derive(Clone, Debug, PartialEq)]
enum FieldVal {
    U32(u32),
    I64(i64),
    F64(f64),
    Bool(bool),
}

/// Independent byte-level reader matching the documented nested ordered-list image layout:
/// prefix (8 bytes) -> descriptor (align8(num_fields)) -> outer keys -> outer ptrs -> arena hdr -> arena data;
/// inner list at ptr: [count:u32][capacity:u32][rows: row_size * capacity].
fn read_list_raw(
    state: &[u8],
    slot: u32,
    outer_key: u32,
) -> Option<(u32, Vec<Vec<Option<FieldVal>>>)> {
    let meta = SlotMetaView::read(state, slot);
    assert_eq!(meta.slot_type(), SlotType::Nested);
    let base = meta.offset;
    let inner_type = SlotType::from_u8(state[base as usize] & 0x0f)?;
    assert_eq!(inner_type, SlotType::OrderedList);
    let num_fields = state[(base + 5) as usize];
    let stored_row_size = u32::from(u16::from_le_bytes([
        state[(base + 6) as usize],
        state[(base + 7) as usize],
    ]));
    let desc_off = base + NESTED_PREFIX_SIZE;
    let desc = &state[desc_off as usize..(desc_off + u32::from(num_fields)) as usize];
    let layout = compute_struct_row_layout_padded(num_fields, desc);
    assert_eq!(stored_row_size, layout.row_size);
    let outer_keys = desc_off + align8(u32::from(num_fields));
    let outer_ptrs = outer_keys + meta.capacity * 4;

    let mut ptr = 0u32;
    let mut pos = columine_types::types::hash_key(outer_key, meta.capacity);
    for _ in 0..meta.capacity {
        let k = bytes::read_u32(state, outer_keys + pos * 4);
        if k == outer_key {
            ptr = bytes::read_u32(state, outer_ptrs + pos * 4);
            break;
        }
        if k == EMPTY_KEY {
            return None;
        }
        pos = (pos + 1) & (meta.capacity - 1);
    }
    if ptr == 0 {
        return None;
    }

    let count = bytes::read_u32(state, ptr);
    let capacity = bytes::read_u32(state, ptr + 4);
    assert!(count <= capacity);
    let rows_base = ptr + 8;
    let mut rows = Vec::with_capacity(count as usize);
    for idx in 0..count {
        let row_off = rows_base + idx * stored_row_size;
        let mut fields = Vec::with_capacity(usize::from(num_fields));
        for fi in 0..num_fields {
            let present = state[(row_off + u32::from(fi) / 8) as usize] & (1u8 << (fi % 8)) != 0;
            if !present {
                fields.push(None);
                continue;
            }
            let f_off = row_off + struct_field_offset(num_fields, desc, fi);
            let val = match StructFieldType::from_u8(desc[usize::from(fi)]).expect("valid field") {
                StructFieldType::UInt32 | StructFieldType::String => {
                    FieldVal::U32(bytes::read_u32(state, f_off))
                }
                StructFieldType::Int64 => FieldVal::I64(bytes::read_i64(state, f_off)),
                StructFieldType::Float64 => FieldVal::F64(bytes::read_f64(state, f_off)),
                StructFieldType::Bool => FieldVal::Bool(state[f_off as usize] != 0),
                other => panic!("unexpected field type {other:?}"),
            };
            fields.push(Some(val));
        }
        rows.push(fields);
    }
    Some((capacity, rows))
}

#[test]
fn rows_appended_under_three_keys_read_back_per_key_in_element_order_with_provided_fields_only() {
    // 4-field struct: [UInt32, Int64, Float64, Bool]; only fields 0, 1, 3 are provided in this op.
    let init = nested_list_init(0, 16, 4, 0, &[UINT32, INT64, FLOAT64, BOOL]);
    let body = append_body(0, 1, &[(2, 0), (3, 1), (4, 3)]);
    let prog = program(1, 5, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_EVENT; 6];
    let keys = [10u32, 20, 30, 10, 30, 20];
    let f0 = [100u32, 200, 300, 101, 301, 201];
    let f1 = [-10i64, 2_000_000_000_000, 30, -11, 31, 2_000_000_000_001];
    let f3 = [1u32, 0, 1, 0, 0, 1];

    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&f0),
            i64s_as_bytes(&f1),
            u32s_as_bytes(&f3),
        ],
        6,
    );
    assert_eq!(status, ErrorCode::Ok as u32);
    validate_state(&state).expect("state validates after batch");

    assert_eq!(read_list_raw(&state, 0, 999), None);
    assert_eq!(nested_list_lookup(&state, 0, 999), Ok(None));

    let view10: NestedListView = nested_list_lookup(&state, 0, 10)
        .expect("lookup key 10 must succeed")
        .expect("key 10 must be present");
    assert_eq!(view10.count, 2);
    assert_eq!(view10.capacity, 4);
    assert_eq!(view10.num_fields, 4);
    assert_eq!(view10.bitset_bytes, 1);
    assert_eq!(
        view10.row_size,
        compute_struct_row_layout_padded(4, &[UINT32, INT64, FLOAT64, BOOL]).row_size
    );
    assert_eq!(
        &state[view10.descriptor_offset as usize..view10.descriptor_offset as usize + 4],
        &[UINT32, INT64, FLOAT64, BOOL]
    );
    assert_eq!(
        bytes::read_u32(
            &state,
            view10.rows_offset + struct_field_offset(4, &[UINT32, INT64, FLOAT64, BOOL], 0)
        ),
        100
    );
    assert_eq!(
        bytes::read_i64(
            &state,
            view10.rows_offset
                + view10.row_size
                + struct_field_offset(4, &[UINT32, INT64, FLOAT64, BOOL], 1)
        ),
        -11
    );

    let (cap10, rows10) = read_list_raw(&state, 0, 10).expect("key 10 present");
    assert_eq!(cap10, 4);
    assert_eq!(
        rows10,
        vec![
            vec![
                Some(FieldVal::U32(100)),
                Some(FieldVal::I64(-10)),
                None,
                Some(FieldVal::Bool(true)),
            ],
            vec![
                Some(FieldVal::U32(101)),
                Some(FieldVal::I64(-11)),
                None,
                Some(FieldVal::Bool(false)),
            ],
        ]
    );

    let (cap20, rows20) = read_list_raw(&state, 0, 20).expect("key 20 present");
    assert_eq!(cap20, 4);
    assert_eq!(
        rows20,
        vec![
            vec![
                Some(FieldVal::U32(200)),
                Some(FieldVal::I64(2_000_000_000_000)),
                None,
                Some(FieldVal::Bool(false)),
            ],
            vec![
                Some(FieldVal::U32(201)),
                Some(FieldVal::I64(2_000_000_000_001)),
                None,
                Some(FieldVal::Bool(true)),
            ],
        ]
    );

    let (cap30, rows30) = read_list_raw(&state, 0, 30).expect("key 30 present");
    assert_eq!(cap30, 4);
    assert_eq!(
        rows30,
        vec![
            vec![
                Some(FieldVal::U32(300)),
                Some(FieldVal::I64(30)),
                None,
                Some(FieldVal::Bool(true)),
            ],
            vec![
                Some(FieldVal::U32(301)),
                Some(FieldVal::I64(31)),
                None,
                Some(FieldVal::Bool(false)),
            ],
        ]
    );
}

#[test]
fn same_key_elements_in_one_batch_append_in_element_order() {
    let init = nested_list_init(0, 8, 8, 0, &[UINT32, FLOAT64]);
    let body = append_body(0, 1, &[(2, 0), (3, 1)]);
    let prog = program(1, 4, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();

    let types = [TYPE_EVENT; 5];
    let keys = [42u32; 5];
    let seq = [10u32, 20, 30, 40, 50];
    let scores = [1.5f64, 2.25, -3.75, 4.0, 5.125];

    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&seq),
            f64s_as_bytes(&scores),
        ],
        5,
    );
    assert_eq!(status, ErrorCode::Ok as u32);

    let (_, rows) = read_list_raw(&state, 0, 42).expect("key 42 present");
    assert_eq!(rows.len(), 5);
    for i in 0..5 {
        assert_eq!(
            rows[i],
            vec![Some(FieldVal::U32(seq[i])), Some(FieldVal::F64(scores[i]))]
        );
    }
}

#[test]
fn appends_past_inner_initial_cap_grow_list_and_preserve_every_row_and_order() {
    // Small initial inner capacity (2) forces repeated inner-list doubling (2 -> 4 -> 8 -> 16 -> 32)
    // and also exhausts the small slot arena, triggering outer slot growth via Vm::grow_state.
    let init = nested_list_init(0, 4, 2, 0, &[UINT32, INT64]);
    let body = append_body(0, 1, &[(2, 0), (3, 1)]);
    let prog = program(1, 4, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();

    let mut grew_slot = false;
    let total = 25u32;
    for i in 0..total {
        let types = [TYPE_EVENT];
        let keys = [7u32];
        let v0 = [i * 10 + 1];
        let v1 = [i64::from(i) * -1000];
        let cols = [
            u32s_as_bytes(&types),
            u32s_as_bytes(&keys),
            u32s_as_bytes(&v0),
            i64s_as_bytes(&v1),
        ];
        loop {
            let before = state.clone();
            let status = vm.execute_batch(&mut state, &prog, &cols, 1);
            if status == ErrorCode::Ok as u32 {
                break;
            }
            assert_eq!(status, ErrorCode::NeedsGrowth as u32);
            // Refused batch leaves image completely unchanged.
            assert_eq!(
                state, before,
                "NeedsGrowth refusal must leave image unchanged"
            );
            let room = vm.needs_growth_room().expect("growth room recorded");
            let grown_size = calculate_grown_state_size(&state, 0, room).expect("grown size");
            let mut next = vec![0u8; grown_size as usize];
            vm.grow_state(&state, &mut next, 0, room)
                .expect("grow_state must succeed");
            state = next;
            grew_slot = true;
        }
    }

    assert!(grew_slot, "arena exhaustion must trigger slot growth");
    validate_state(&state).expect("grown state validates");

    let (cap, rows) = read_list_raw(&state, 0, 7).expect("key 7 present");
    assert_eq!(cap, 32, "capacity doubled 2 -> 4 -> 8 -> 16 -> 32");
    assert_eq!(rows.len(), total as usize);
    for i in 0..total {
        assert_eq!(
            rows[i as usize],
            vec![
                Some(FieldVal::U32(i * 10 + 1)),
                Some(FieldVal::I64(i64::from(i) * -1000)),
            ]
        );
    }
}

#[derive(Clone, Debug)]
struct SplitRow {
    key: u32,
    f0: Option<u32>,
    f1: Option<i64>,
    f2: Option<bool>,
}

fn split_row_strategy() -> impl Strategy<Value = SplitRow> {
    (
        1u32..=4,
        prop::option::of(any::<u32>()),
        prop::option::of(any::<i64>()),
        prop::option::of(any::<bool>()),
    )
        .prop_map(|(key, f0, f1, f2)| SplitRow { key, f0, f1, f2 })
}

fn rows_and_cuts_strategy() -> impl Strategy<Value = (Vec<SplitRow>, Vec<usize>)> {
    prop::collection::vec(split_row_strategy(), 1..=64).prop_flat_map(|rows| {
        let len = rows.len();
        let cuts = if len <= 1 {
            Just(Vec::new()).boxed()
        } else {
            let max_coarse = (len - 1).min(4);
            prop_oneof![
                prop::collection::vec(1..len, 0..=max_coarse),
                prop::collection::vec(1..len, 0..len),
            ]
            .boxed()
        };
        (Just(rows), cuts)
    })
}

fn validity_bitmap(valid: impl ExactSizeIterator<Item = bool>) -> Vec<u8> {
    let mut bits = vec![0u8; valid.len().div_ceil(8)];
    for (idx, is_valid) in valid.enumerate() {
        if is_valid {
            bits[idx / 8] |= 1u8 << (idx % 8);
        }
    }
    bits
}

fn execute_rows_with_growth(
    vm: &mut Vm,
    state: &mut Vec<u8>,
    prog: &[u8],
    rows: &[SplitRow],
) -> Result<(), String> {
    let types = vec![TYPE_EVENT; rows.len()];
    let keys: Vec<u32> = rows.iter().map(|r| r.key).collect();
    // Non-zero dummy payloads for absent cells ensure a missed validity check is observable.
    let f0: Vec<u32> = rows.iter().map(|r| r.f0.unwrap_or(0xDEAD_BEEF)).collect();
    let f1: Vec<i64> = rows.iter().map(|r| r.f1.unwrap_or(-123_456_789)).collect();
    let f2: Vec<u32> = rows
        .iter()
        .map(|r| u32::from(r.f2.unwrap_or(true)))
        .collect();
    let f0_valid = validity_bitmap(rows.iter().map(|r| r.f0.is_some()));
    let f1_valid = validity_bitmap(rows.iter().map(|r| r.f1.is_some()));
    let f2_valid = validity_bitmap(rows.iter().map(|r| r.f2.is_some()));

    let cols = [
        u32s_as_bytes(&types),
        u32s_as_bytes(&keys),
        u32s_as_bytes(&f0),
        i64s_as_bytes(&f1),
        u32s_as_bytes(&f2),
    ];
    let validity_cols = [
        None,
        None,
        Some(f0_valid.as_slice()),
        Some(f1_valid.as_slice()),
        Some(f2_valid.as_slice()),
    ];
    let validity = Validity(&validity_cols);

    loop {
        let status = vm.execute_batch_in(
            state,
            prog,
            &cols,
            validity,
            len_u32(rows.len()),
            &NoStrings,
        );
        if status == ErrorCode::Ok as u32 {
            return Ok(());
        }
        if status != ErrorCode::NeedsGrowth as u32 {
            return Err(format!("unexpected execute_batch_in status: {status}"));
        }
        let room = vm
            .needs_growth_room()
            .ok_or_else(|| "NeedsGrowth without recorded growth room".to_owned())?;
        let grown_size = calculate_grown_state_size(state, 0, room)
            .map_err(|e| format!("calculate_grown_state_size failed: {e:?}"))?;
        let mut next = vec![0u8; grown_size as usize];
        vm.grow_state(state, &mut next, 0, room)
            .map_err(|e| format!("grow_state failed: {e:?}"))?;
        *state = next;
    }
}

#[test]
fn split_invariance_any_batch_partition_matches_the_reference_list_per_key() {
    const INITIAL_INNER_CAP: u16 = 2;
    const CASES: u32 = 256;

    let init = nested_list_init(0, 16, INITIAL_INNER_CAP, 0, &[UINT32, INT64, BOOL]);
    let body = append_body(0, 1, &[(2, 0), (3, 1), (4, 2)]);
    let prog = program(1, 5, &init, &for_each(0, &body));

    let grew_inner_keys = Cell::new(0u32);
    let multi_same_key_batches = Cell::new(0u32);
    let mut runner = TestRunner::new(ProptestConfig::with_cases(CASES));
    let verdict = runner.run(&rows_and_cuts_strategy(), |(rows, mut cuts)| {
        let mut whole_state = init_fresh(&prog);
        let mut vm_whole = Vm::default();
        execute_rows_with_growth(&mut vm_whole, &mut whole_state, &prog, &rows)
            .map_err(TestCaseError::fail)?;
        validate_state(&whole_state)
            .map_err(|e| TestCaseError::fail(format!("whole state invalid: {e:?}")))?;

        cuts.sort_unstable();
        cuts.dedup();
        cuts.push(rows.len());

        let mut split_state = init_fresh(&prog);
        let mut vm_split = Vm::default();
        let mut start = 0usize;
        for end in cuts {
            let batch = &rows[start..end];
            let mut batch_counts = [0u32; 5];
            for row in batch {
                batch_counts[row.key as usize] += 1;
            }
            if batch_counts.iter().any(|&c| c > 1) {
                multi_same_key_batches.set(multi_same_key_batches.get() + 1);
            }
            execute_rows_with_growth(&mut vm_split, &mut split_state, &prog, batch)
                .map_err(TestCaseError::fail)?;
            start = end;
        }
        validate_state(&split_state)
            .map_err(|e| TestCaseError::fail(format!("split state invalid: {e:?}")))?;

        let mut expected: BTreeMap<u32, Vec<Vec<Option<FieldVal>>>> = BTreeMap::new();
        for row in &rows {
            expected.entry(row.key).or_default().push(vec![
                row.f0.map(FieldVal::U32),
                row.f1.map(FieldVal::I64),
                row.f2.map(FieldVal::Bool),
            ]);
        }

        for key in 1u32..=5 {
            let whole = read_list_raw(&whole_state, 0, key);
            let split = read_list_raw(&split_state, 0, key);
            prop_assert_eq!(
                &whole,
                &split,
                "key {} differs between whole-batch and partitioned execution",
                key
            );
            match expected.get(&key) {
                Some(expected_rows) => {
                    let Some((split_cap, split_rows)) = split else {
                        return Err(TestCaseError::fail(format!(
                            "key {key} present in input was absent from folded state"
                        )));
                    };
                    prop_assert_eq!(
                        &split_rows,
                        expected_rows,
                        "key {} list differs from input-order reference model",
                        key
                    );
                    if expected_rows.len() > usize::from(INITIAL_INNER_CAP) {
                        prop_assert!(
                            split_cap > u32::from(INITIAL_INNER_CAP),
                            "key {} with {} rows must grow past initial inner cap {}, got {}",
                            key,
                            expected_rows.len(),
                            INITIAL_INNER_CAP,
                            split_cap
                        );
                        grew_inner_keys.set(grew_inner_keys.get() + 1);
                    }
                }
                None => {
                    prop_assert_eq!(
                        split,
                        None,
                        "key {} absent from input must be absent from folded state",
                        key
                    );
                }
            }
        }
        Ok(())
    });

    if let Err(failure) = verdict {
        panic!("{failure}");
    }
    assert!(
        grew_inner_keys.get() > 0,
        "batch-by-batch fold must reach inner capacity growth past {INITIAL_INNER_CAP}"
    );
    assert!(
        multi_same_key_batches.get() > 0,
        "partitioned batches must exercise multiple same-key rows in a single batch"
    );
}

#[test]
fn speculative_rollback_restores_image_including_arena_header_and_outer_table() {
    let init = nested_list_init(0, 16, 2, 0, &[UINT32, FLOAT64]);
    let body = append_body(0, 1, &[(2, 0), (3, 1)]);
    let prog = program(1, 4, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();

    // Commit one initial row under key 1 so both existing-key growth and new-key creation happen inside speculation.
    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT]),
            u32s_as_bytes(&[1u32]),
            u32s_as_bytes(&[11u32]),
            f64s_as_bytes(&[1.25]),
        ],
        1,
    );
    assert_eq!(status, ErrorCode::Ok as u32);

    let committed = state.clone();

    vm.undo_enable(&state).expect("undo_enable");
    let mark = vm.undo_checkpoint();

    // Appends 3 more rows under key 1 (grows 2 -> 4) and 2 rows under new key 2.
    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT; 5]),
            u32s_as_bytes(&[1u32, 1, 2, 1, 2]),
            u32s_as_bytes(&[12u32, 13, 21, 14, 22]),
            f64s_as_bytes(&[2.0, 3.0, 4.0, 5.0, 6.0]),
        ],
        5,
    );
    assert_eq!(status, ErrorCode::Ok as u32);
    assert_ne!(state, committed);
    assert_eq!(read_list_raw(&state, 0, 1).unwrap().1.len(), 4);
    assert_eq!(read_list_raw(&state, 0, 2).unwrap().1.len(), 2);

    vm.undo_rollback(&mut state, mark).expect("undo_rollback");
    vm.undo_commit();

    assert_eq!(
        state, committed,
        "rollback must restore every byte including arena header, outer table, and metadata"
    );
    validate_state(&state).expect("rolled-back state validates");
}

#[test]
fn logical_export_then_import_reproduces_every_list() {
    let init = nested_list_init(0, 8, 2, 0, &[STRING, INT64, BOOL]);
    let body = append_body(0, 1, &[(2, 0), (3, 1), (4, 2)]);
    let prog = program(1, 5, &init, &for_each(0, &body));

    let mut interner = TestInterner::default();
    let alpha = interner.intern(InternedStr::new("alpha").unwrap()).unwrap();
    let beta = interner.intern(InternedStr::new("beta").unwrap()).unwrap();
    let gamma = interner.intern(InternedStr::new("gamma").unwrap()).unwrap();

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();
    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT; 5]),
            u32s_as_bytes(&[10u32, 20, 10, 10, 20]),
            u32s_as_bytes(&[alpha, beta, gamma, alpha, beta]),
            i64s_as_bytes(&[100, 200, 300, 400, 500]),
            u32s_as_bytes(&[1u32, 0, 1, 0, 1]),
        ],
        5,
    );
    assert_eq!(status, ErrorCode::Ok as u32);

    let exported: LogicalSlot<'static> =
        export_slot(&state, 0, &SlotEncoding::RAW, &interner).expect("export nested list");
    assert_eq!(exported.kind, SlotType::Nested);
    assert_eq!(exported.nested.len(), 2);

    // Import into a freshly initialized image with a fresh interner (different string ids).
    let mut fresh_interner = TestInterner::default();
    let _dummy = fresh_interner
        .intern(InternedStr::new("padding-id-0").unwrap())
        .unwrap();
    let mut imported_image = init_fresh(&prog);
    import_slot(
        &mut imported_image,
        &exported,
        &SlotEncoding::RAW,
        &mut fresh_interner,
    )
    .expect("import nested list");
    validate_state(&imported_image).expect("imported image validates");

    let reexported = export_slot(&imported_image, 0, &SlotEncoding::RAW, &fresh_interner)
        .expect("re-export after import");
    assert_eq!(reexported, exported);

    // Also test rewrite_slot_set into a program with different outer/inner capacities.
    let target_init = nested_list_init(0, 16, 4, 0, &[STRING, INT64, BOOL]);
    let target_prog = program(1, 5, &target_init, &for_each(0, &body));
    let rewritten = rewrite_slot_set(
        &state,
        &target_prog,
        DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        &[SlotTransfer {
            source: 0,
            target: 0,
            encoding: SlotEncoding::RAW,
            transform: None,
        }],
        &mut interner,
        None,
    )
    .expect("rewrite_slot_set must transfer nested ordered list");
    let rewritten_export =
        export_slot(&rewritten, 0, &SlotEncoding::RAW, &interner).expect("export rewritten");
    assert_eq!(rewritten_export, exported);
}

#[test]
fn program_admission_refuses_invalid_nested_ordered_list_declarations() {
    let cases: [(&str, Vec<u8>); 6] = [
        (
            "inner OrderedList with zero fields",
            nested_list_init(0, 8, 4, 0, &[]),
        ),
        (
            "inner OrderedList with unknown field type 11",
            nested_list_init(0, 8, 4, 0, &[UINT32, 11]),
        ),
        (
            "inner OrderedList with VO field type",
            nested_list_init(0, 8, 4, 0, &[UINT32, VO]),
        ),
        (
            "inner OrderedList with ArrayU32 field type",
            nested_list_init(0, 8, 4, 0, &[ARRAY_U32]),
        ),
        (
            "inner OrderedList with ArrayBool field type",
            nested_list_init(0, 8, 4, 0, &[UINT32, ARRAY_BOOL]),
        ),
        (
            "inner OrderedList with nonzero agg byte",
            nested_list_init(0, 8, 4, 1, &[UINT32]),
        ),
    ];

    for (name, init_code) in cases {
        let prog = program(1, 0, &init_code, &[]);
        assert_eq!(
            calculate_state_size(&prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
            Err(SizingRefusal::Invalid(
                ProgramInvalidCause::MalformedProgram
            )),
            "calculate_state_size must refuse: {name}"
        );
        let mut buf = vec![0u8; 4096];
        assert_eq!(
            init_state(&mut buf, &prog, DEFAULT_ACCEPTED_PROGRAM_MAGICS),
            Err(ErrorCode::InvalidProgram),
            "init_state must refuse: {name}"
        );
        let admitted = AdmittedProgram::load(
            prog.into_boxed_slice(),
            VoProgramMetadata::default(),
            UnavailableVoAdmission,
            DEFAULT_ACCEPTED_PROGRAM_MAGICS,
        );
        assert!(
            matches!(
                admitted,
                Err(ProgramAdmissionError::InvalidProgram(
                    ProgramInvalidCause::MalformedProgram
                ))
            ),
            "AdmittedProgram::load must refuse: {name}"
        );
    }
}

#[test]
fn change_feed_and_watch_log_record_whole_slot_change_on_nested_list_append() {
    let init = nested_list_init(0, 8, 4, 0, &[UINT32]);
    let body = append_body(0, 1, &[(2, 0)]);
    let prog = program(1, 3, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();
    let coverage = vm.watch(&state, 0, 16, 256).expect("watch slot 0");
    vm.feed_bind(&[1u64 << 1], 0, 16).expect("feed_bind slot 0");
    vm.feed_begin_pass(&state);

    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT]),
            u32s_as_bytes(&[5u32]),
            u32s_as_bytes(&[99u32]),
        ],
        1,
    );
    assert_eq!(status, ErrorCode::Ok as u32);
    assert_ne!(
        state[(STATE_HEADER_SIZE + SlotMetaOffset::CHANGE_FLAGS) as usize] & ChangeFlag::INSERTED,
        0
    );
    assert!(
        vm.feed()
            .overflowed(columine_vm::change_feed::Source::Slot(0))
    );

    let mut out = columine_vm::watch_log::NetChanges::default();
    let answer = vm
        .net_changes(
            &state,
            0,
            coverage,
            &columine_vm::key_index::NoStrings,
            &mut out,
        )
        .expect("net_changes");
    assert_eq!(answer, columine_vm::watch_log::NetAnswer::Overflowed);
    vm.feed_end_pass();
}

#[test]
fn refused_plan_leaves_image_unchanged() {
    let init = nested_list_init(0, 8, 4, 0, &[UINT32]);
    let body = append_body(0, 1, &[(2, 0)]);
    let prog = program(1, 3, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();
    let before = state.clone();

    // Sentinel outer key (TOMBSTONE) refuses with InvalidKey and leaves the image unchanged.
    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT]),
            u32s_as_bytes(&[TOMBSTONE]),
            u32s_as_bytes(&[123u32]),
        ],
        1,
    );
    assert_eq!(status, ErrorCode::InvalidKey as u32);
    assert_eq!(state, before, "refused plan must leave image unchanged");

    // Out-of-range field_idx refuses with InvalidProgram and leaves the image unchanged.
    let bad_body = append_body(0, 1, &[(2, 1)]);
    let bad_prog = program(1, 3, &init, &for_each(0, &bad_body));
    let status = vm.execute_batch(
        &mut state,
        &bad_prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT]),
            u32s_as_bytes(&[1u32]),
            u32s_as_bytes(&[123u32]),
        ],
        1,
    );
    assert_eq!(status, ErrorCode::InvalidProgram as u32);
    assert_eq!(
        state, before,
        "refused field_idx must leave image unchanged"
    );
}

#[test]
fn a_malformed_nested_list_image_is_an_error_not_absence() {
    let init = nested_list_init(0, 8, 4, 0, &[UINT32, INT64]);
    let body = append_body(0, 1, &[(2, 0), (3, 1)]);
    let prog = program(1, 4, &init, &for_each(0, &body));

    let mut state = init_fresh(&prog);
    let mut vm = Vm::default();
    let status = vm.execute_batch(
        &mut state,
        &prog,
        &[
            u32s_as_bytes(&[TYPE_EVENT]),
            u32s_as_bytes(&[10u32]),
            u32s_as_bytes(&[77u32]),
            i64s_as_bytes(&[999]),
        ],
        1,
    );
    assert_eq!(status, ErrorCode::Ok as u32);

    // Absent key on a well-formed image is Ok(None), distinct from corruption.
    assert_eq!(nested_list_lookup(&state, 0, 999), Ok(None));
    assert!(matches!(nested_list_lookup(&state, 0, 10), Ok(Some(_))));

    // Out-of-range slot is InvalidSlot.
    assert_eq!(
        nested_list_lookup(&state, 1, 10),
        Err(ErrorCode::InvalidSlot)
    );

    // Corrupting the outer pointer past arena_used returns Err(InvalidState).
    let meta = SlotMetaView::read(&state, 0);
    let prefix = columine_vm::nested::read_nested_prefix(&state, meta.offset);
    let desc_size = prefix.descriptor_size();
    let outer_keys = columine_vm::nested::outer_keys_offset(meta.offset, desc_size);
    let outer_ptrs = columine_vm::nested::outer_ptrs_offset(meta.offset, desc_size, meta.capacity);
    let pos = (0..meta.capacity)
        .find(|&p| bytes::read_u32(&state, outer_keys + p * 4) == 10)
        .expect("outer key 10 is present");

    let mut bad_ptr = state.clone();
    let bad_offset = len_u32(bad_ptr.len()) - 4;
    bytes::write_u32(&mut bad_ptr, outer_ptrs + pos * 4, bad_offset);
    assert_eq!(
        nested_list_lookup(&bad_ptr, 0, 10),
        Err(ErrorCode::InvalidState)
    );

    // Corrupting the inner-kind byte to another valid kind returns Err(InvalidProgram).
    let mut wrong_inner = state.clone();
    wrong_inner[meta.offset as usize] = SlotType::HashSet as u8;
    assert_eq!(
        nested_list_lookup(&wrong_inner, 0, 10),
        Err(ErrorCode::InvalidProgram)
    );

    // Corrupting the inner-kind byte to an unassigned nibble returns Err(InvalidState).
    let mut bogus_inner = state.clone();
    bogus_inner[meta.offset as usize] = 0x0f;
    assert_eq!(
        nested_list_lookup(&bogus_inner, 0, 10),
        Err(ErrorCode::InvalidState)
    );

    // Corrupting a descriptor field byte to an unknown or arena kind returns Err(InvalidState).
    let desc_off = columine_vm::nested::NestedPrefix::descriptor_offset(meta.offset) as usize;
    let mut unknown_desc = state.clone();
    unknown_desc[desc_off] = 11;
    assert_eq!(
        nested_list_lookup(&unknown_desc, 0, 10),
        Err(ErrorCode::InvalidState)
    );

    let mut arena_desc = state.clone();
    arena_desc[desc_off] = StructFieldType::Vo as u8;
    assert_eq!(
        nested_list_lookup(&arena_desc, 0, 10),
        Err(ErrorCode::InvalidState)
    );

    // Corrupting prefix row_size so it disagrees with the descriptor returns Err(InvalidState).
    let mut bad_row_size = state.clone();
    bytes::write_u16(&mut bad_row_size, meta.offset + 6, 1);
    assert_eq!(
        nested_list_lookup(&bad_row_size, 0, 10),
        Err(ErrorCode::InvalidState)
    );

    // Zeroing inner_initial_cap violates prefix geometry and returns Err(InvalidState).
    let mut zero_inner_cap = state.clone();
    bytes::write_u16(&mut zero_inner_cap, meta.offset + 1, 0);
    assert_eq!(
        nested_list_lookup(&zero_inner_cap, 0, 10),
        Err(ErrorCode::InvalidState)
    );
}
