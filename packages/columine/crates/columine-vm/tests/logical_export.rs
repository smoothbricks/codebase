//! Acceptance coverage for logical slot export/import: per-kind
//! round trips (export of import is the identity), fold continuation
//! after import (the same next batch lands identically on the original
//! and on the rebuilt image), the eager slot-set rewrite with field
//! transform plans, and typed refusals. Generated cases come from a deterministic
//! LCG so every run exercises the same states.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    AggType, DERIVED_FACT_TOMBSTONE_IDENTITY, ErrorCode, Opcode, PROGRAM_FORMAT_VERSION,
    PROGRAM_HASH_PREFIX, ProgramHeader, SlotType, StructFieldType,
};
use columine_vm::hashmap_ops::{CmpType, Strategy, batch_map_upsert};
use columine_vm::hooks::NoVm;
use columine_vm::logical_export::{
    CellEncoding, ExprEvaluator, FieldTransform, InternLookup, Interner, LogicalAggregate,
    LogicalCondition, LogicalConditionCell, LogicalEvictionEntry, LogicalExportCause, LogicalInner,
    LogicalNestedEntry, LogicalRow, LogicalScalar, LogicalSlot, LogicalValue, SlotEncoding,
    SlotTransfer, TransformPlan, TransformRowView, apply_transform, export_slot, import_slot,
    rewrite_slot_set,
};
use columine_vm::meta::SlotMetaView;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, calculate_state_size, init_state, validate_state,
};
use std::borrow::Cow;
use std::collections::HashMap;

#[path = "common/memory.rs"]
mod memory;

const MAGICS: &[u32] = DEFAULT_ACCEPTED_PROGRAM_MAGICS;

fn program(num_slots: u32, init: &[u8]) -> Vec<u8> {
    let mut prog = vec![0u8; PROGRAM_HASH_PREFIX as usize];
    prog.extend(
        ProgramHeader {
            magic: PROGRAM_MAGIC,
            version: PROGRAM_FORMAT_VERSION,
            num_callbacks: 0,
            flags: 0,
            num_slots,
            num_inputs: 0,
            init_code_len: init.len() as u32,
            reduce_code_len: 0,
        }
        .to_wire_bytes(),
    );
    prog.extend_from_slice(init);
    prog
}

fn slot_def(slot: u8, flags: u8, cap: u16) -> Vec<u8> {
    let mut init = vec![Opcode::SlotDef as u8, slot, flags];
    init.extend(cap.to_le_bytes());
    init
}

fn slot_def_ttl(slot: u8, flags: u8, cap: u16) -> Vec<u8> {
    let mut init = slot_def(slot, flags | 0x10, cap);
    init.extend(60.0f32.to_le_bytes());
    init.extend(5.0f32.to_le_bytes());
    init.push(1);
    init.push(0);
    init
}

fn struct_map_def(
    slot: u8,
    opcode: Opcode,
    kind: SlotType,
    cap: u16,
    fields: &[StructFieldType],
) -> Vec<u8> {
    let mut init = vec![opcode as u8, slot, kind as u8];
    init.extend(cap.to_le_bytes());
    init.push(fields.len() as u8);
    init.extend(fields.iter().map(|f| *f as u8));
    init
}

fn halt(mut init: Vec<u8>) -> Vec<u8> {
    init.push(Opcode::Halt as u8);
    init
}

fn fresh_image(prog: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(prog, MAGICS).expect("program admitted");
    let mut image = vec![0u8; size as usize];
    init_state(&mut image, prog, MAGICS).expect("init");
    image
}

/// Build a fresh image from a program and import every slot into it.
fn import_image(
    prog: &[u8],
    slots: &[LogicalSlot],
    enc: &SlotEncoding,
    interner: &mut TestInterner,
) -> Vec<u8> {
    let mut image = fresh_image(prog);
    for logical in slots {
        import_slot(&mut image, logical, enc, interner).expect("import slot");
    }
    validate_state(&image).expect("assembled image validates");
    image
}

/// An interner owned by the test: ids are stable indices into its table,
/// so export through it and import through it must keep every id.
#[derive(Default)]
struct TestInterner {
    texts: Vec<String>,
    ids: HashMap<String, u32>,
}

impl InternLookup for TestInterner {
    fn text(&self, id: u32) -> Option<&str> {
        self.texts.get(id as usize).map(String::as_str)
    }
}

impl Interner for TestInterner {
    fn intern(&mut self, text: &str) -> Result<u32, ErrorCode> {
        if let Some(&id) = self.ids.get(text) {
            return Ok(id);
        }
        let id = self.texts.len() as u32;
        self.texts.push(text.to_owned());
        self.ids.insert(text.to_owned(), id);
        Ok(id)
    }
}

fn base_slot<'text>(slot: u32, kind: SlotType) -> LogicalSlot<'text> {
    LogicalSlot {
        slot,
        kind,
        agg_type: 0,
        field_types: Vec::new(),
        rows: Vec::new(),
        aggregate: None,
        scalar: None,
        nested: Vec::new(),
        eviction: Vec::new(),
        evicted: Vec::new(),
        condition: None,
    }
}

fn row<'text>(
    key: LogicalValue<'text>,
    fields: Vec<Option<LogicalValue<'text>>>,
    cmp_ts: Option<f64>,
) -> LogicalRow<'text> {
    LogicalRow {
        key,
        key2: None,
        fields,
        cmp_ts,
    }
}

fn round_trip(
    prog: &[u8],
    logical: &LogicalSlot,
    enc: &SlotEncoding,
    interner: &mut TestInterner,
) -> Vec<u8> {
    let image = import_image(prog, std::slice::from_ref(logical), enc, interner);
    validate_state(&image).expect("imported image validates");
    let back = export_slot(&image, logical.slot, enc, interner).expect("export");
    assert_eq!(&back, logical, "export of import is the identity");
    image
}

#[test]
fn borrowed_text_import_matches_owned_without_a_text_allocation() {
    let key = String::from("ключ/borrowed");
    let value = String::from("text/borrowed/🦀");
    let enc = SlotEncoding {
        key: CellEncoding::Interned,
        value: CellEncoding::Interned,
        ..SlotEncoding::RAW
    };
    let prog = program(1, &halt(slot_def(0, SlotType::HashMap as u8, 8)));
    let mut interner = TestInterner::default();
    interner.intern(&key).expect("key admitted before import");
    interner
        .intern(&value)
        .expect("value admitted before import");
    let mut borrowed = base_slot(0, SlotType::HashMap);
    borrowed.rows.push(row(
        LogicalValue::Text(Cow::Borrowed(&key)),
        vec![Some(LogicalValue::Text(Cow::Borrowed(&value)))],
        None,
    ));
    let mut owned = base_slot(0, SlotType::HashMap);
    owned.rows.push(row(
        LogicalValue::Text(Cow::Owned(key.clone())),
        vec![Some(LogicalValue::Text(Cow::Owned(value.clone())))],
        None,
    ));
    let mut borrowed_image = fresh_image(&prog);
    let mut owned_image = fresh_image(&prog);
    memory::with_allocations(0, || {
        for _ in 0..1024 {
            let copy = borrowed.rows[0].key.clone();
            let LogicalValue::Text(Cow::Borrowed(text)) = copy else {
                panic!("a borrowed logical cell acquired a second owner");
            };
            assert_eq!(text.as_ptr(), key.as_ptr());
            std::hint::black_box(copy);
        }
        import_slot(&mut borrowed_image, &borrowed, &enc, &mut interner).expect("borrowed import");
    });
    import_slot(&mut owned_image, &owned, &enc, &mut interner).expect("owned import");
    assert_eq!(borrowed_image, owned_image);
    assert_eq!(
        export_slot(&borrowed_image, 0, &enc, &interner).expect("export"),
        owned
    );
    let (_, owned_peak) = memory::peak_bytes(|| std::hint::black_box(key.clone()));
    let (_, borrowed_peak) =
        memory::peak_bytes(|| std::hint::black_box(borrowed.rows[0].key.clone()));
    assert!(owned_peak >= key.len());
    assert_eq!(borrowed_peak, 0);
}

#[test]
fn transforms_preserve_borrowed_text_sources() {
    struct ReadField;
    impl ExprEvaluator for ReadField {
        fn eval<'text>(
            &mut self,
            _bytecode: &[u8],
            _constants: &[u8],
            row: &TransformRowView<'_, 'text>,
        ) -> Result<Option<LogicalValue<'text>>, ErrorCode> {
            Ok(row.fields[0].clone())
        }
    }
    let text = String::from("shared/🦀");
    let mut old = base_slot(0, SlotType::StructMap);
    old.field_types = vec![StructFieldType::String];
    old.rows.push(row(
        LogicalValue::U32(1),
        vec![Some(LogicalValue::Text(Cow::Borrowed(&text)))],
        None,
    ));
    let plan = TransformPlan {
        fields: vec![
            FieldTransform::Copy(0),
            FieldTransform::Expr {
                bytecode: vec![],
                constants: vec![],
            },
        ],
    };
    let output = apply_transform(
        &old,
        &[StructFieldType::String, StructFieldType::String],
        &plan,
        std::slice::from_ref(&old),
        &mut ReadField,
    )
    .expect("copy and expression transform");
    assert_eq!(output.rows[0].fields.len(), plan.fields.len());
    for field in &output.rows[0].fields {
        let Some(LogicalValue::Text(Cow::Borrowed(borrowed))) = field else {
            panic!("a transform copied borrowed text into another owner");
        };
        assert_eq!(borrowed.as_ptr(), text.as_ptr());
        assert_eq!(*borrowed, text);
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
    fn below(&mut self, n: u32) -> u32 {
        (self.next() % u64::from(n)) as u32
    }
}

#[test]
fn hashmap_round_trip_with_timestamps_ttl_and_encodings() {
    let prog = program(1, &halt(slot_def_ttl(0, SlotType::HashMap as u8 | 0x20, 8)));
    let enc = SlotEncoding {
        key: CellEncoding::Interned,
        value: CellEncoding::Boolean,
        ..SlotEncoding::RAW
    };
    let mut interner = TestInterner::default();
    let mut logical = base_slot(0, SlotType::HashMap);
    logical.rows = vec![
        row(
            LogicalValue::Text("alpha".into()),
            vec![Some(LogicalValue::Bool(true))],
            Some(102.5),
        ),
        row(
            LogicalValue::Text("beta".into()),
            vec![Some(LogicalValue::Bool(false))],
            Some(99.25),
        ),
    ];
    logical.eviction = vec![LogicalEvictionEntry {
        timestamp: 102.5,
        key: LogicalValue::Text("alpha".into()),
        value: LogicalValue::Bool(true),
    }];
    logical.evicted = vec![LogicalEvictionEntry {
        timestamp: 10.0,
        key: LogicalValue::Text("gone".into()),
        value: LogicalValue::Bool(false),
    }];
    round_trip(&prog, &logical, &enc, &mut interner);
}

#[test]
fn hashset_bitmap_and_array_round_trip() {
    let mut interner = TestInterner::default();
    let set_prog = program(1, &halt(slot_def(0, SlotType::HashSet as u8, 16)));
    let mut set = base_slot(0, SlotType::HashSet);
    set.rows = [3u32, 17, 42, 1000]
        .into_iter()
        .map(|k| row(LogicalValue::U32(k), Vec::new(), None))
        .collect();
    round_trip(&set_prog, &set, &SlotEncoding::RAW, &mut interner);

    let bitmap_prog = program(1, &halt(slot_def(0, SlotType::Bitmap as u8, 16)));
    let mut bitmap = base_slot(0, SlotType::Bitmap);
    bitmap.rows = [0u32, 63, 64, 65_537, 1_000_003]
        .into_iter()
        .map(|k| row(LogicalValue::U32(k), Vec::new(), None))
        .collect();
    round_trip(&bitmap_prog, &bitmap, &SlotEncoding::RAW, &mut interner);

    let mut array_init = vec![Opcode::SlotArray as u8, 0];
    array_init.extend(8u16.to_le_bytes());
    array_init.push(0);
    let array_prog = program(1, &halt(array_init));
    let mut array = base_slot(0, SlotType::Array);
    array.rows = vec![
        row(
            LogicalValue::U32(2),
            vec![Some(LogicalValue::U32(77))],
            Some(5.5),
        ),
        row(
            LogicalValue::U32(9),
            vec![Some(LogicalValue::U32(78))],
            Some(6.5),
        ),
    ];
    round_trip(&array_prog, &array, &SlotEncoding::RAW, &mut interner);
}

#[test]
fn aggregate_and_scalar_round_trip_every_subtype() {
    let cases: &[(AggType, LogicalValue, u64)] = &[
        (AggType::Sum, LogicalValue::F64(123.5), 7),
        (AggType::Min, LogicalValue::F64(-4.25), 3),
        (AggType::Max, LogicalValue::F64(9.75), 3),
        (AggType::Avg, LogicalValue::F64(55.0), 11),
        (AggType::Count, LogicalValue::U64(1 << 40), 1 << 40),
        (AggType::SumI64, LogicalValue::I64(-(1 << 41)), 5),
        (AggType::MinI64, LogicalValue::I64(-99), 2),
        (AggType::MaxI64, LogicalValue::I64(99), 2),
    ];
    for &(agg, ref value, count) in cases {
        let prog = program(
            1,
            &halt(slot_def(0, SlotType::Aggregate as u8, u16::from(agg as u8))),
        );
        let mut logical = base_slot(0, SlotType::Aggregate);
        logical.agg_type = agg as u8;
        logical.aggregate = Some(LogicalAggregate {
            value: value.clone(),
            count,
        });
        let mut interner = TestInterner::default();
        round_trip(&prog, &logical, &SlotEncoding::RAW, &mut interner);
    }

    let scalars: &[(AggType, LogicalValue)] = &[
        (AggType::ScalarU32, LogicalValue::U32(u32::MAX - 5)),
        (AggType::ScalarF64, LogicalValue::F64(-0.0)),
        (AggType::ScalarI64, LogicalValue::I64(i64::MIN + 3)),
    ];
    for &(agg, ref value) in scalars {
        let prog = program(
            1,
            &halt(slot_def(0, SlotType::Scalar as u8, u16::from(agg as u8))),
        );
        let mut logical = base_slot(0, SlotType::Scalar);
        logical.agg_type = agg as u8;
        logical.scalar = Some(LogicalScalar {
            value: value.clone(),
            cmp_ts: 4242.5,
        });
        let mut interner = TestInterner::default();
        round_trip(&prog, &logical, &SlotEncoding::RAW, &mut interner);
    }
}

#[test]
fn struct_maps_round_trip_all_field_types() {
    let fields = vec![
        StructFieldType::UInt32,
        StructFieldType::Int64,
        StructFieldType::Float64,
        StructFieldType::Bool,
        StructFieldType::String,
        StructFieldType::ArrayU32,
        StructFieldType::ArrayI64,
        StructFieldType::ArrayF64,
        StructFieldType::ArrayString,
        StructFieldType::ArrayBool,
        StructFieldType::Vo,
    ];
    let full_row = vec![
        Some(LogicalValue::U32(7)),
        Some(LogicalValue::I64(-8)),
        Some(LogicalValue::F64(2.5)),
        Some(LogicalValue::Bool(true)),
        Some(LogicalValue::Text("tier".into())),
        Some(LogicalValue::Array(vec![
            LogicalValue::U32(1),
            LogicalValue::U32(2),
        ])),
        Some(LogicalValue::Array(vec![LogicalValue::I64(-3)])),
        Some(LogicalValue::Array(vec![LogicalValue::F64(0.5)])),
        Some(LogicalValue::Array(vec![
            LogicalValue::Text("a".into()),
            LogicalValue::Text("b".into()),
        ])),
        Some(LogicalValue::Array(vec![
            LogicalValue::Bool(true),
            LogicalValue::Bool(false),
        ])),
        Some(LogicalValue::Bytes(vec![9, 8, 7, 6, 5])),
    ];
    let sparse_row: Vec<Option<LogicalValue>> = vec![
        Some(LogicalValue::U32(1)),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ];

    let prog = program(
        1,
        &halt(struct_map_def(
            0,
            Opcode::SlotStructMap,
            SlotType::StructMap,
            8,
            &fields,
        )),
    );
    let mut logical = base_slot(0, SlotType::StructMap);
    logical.field_types = fields.clone();
    logical.rows = vec![
        row(LogicalValue::U32(10), full_row.clone(), None),
        row(LogicalValue::U32(20), sparse_row.clone(), None),
    ];
    let mut interner = TestInterner::default();
    round_trip(&prog, &logical, &SlotEncoding::RAW, &mut interner);

    // StructMap2 has no VO field form (checked_struct_layout admits Vo
    // only on StructMap), so its case drops the Vo field.
    let fields2: Vec<StructFieldType> = fields[..fields.len() - 1].to_vec();
    let full_row2: Vec<Option<LogicalValue>> = full_row[..full_row.len() - 1].to_vec();
    let sparse_row2: Vec<Option<LogicalValue>> = sparse_row[..sparse_row.len() - 1].to_vec();
    let prog2 = program(
        1,
        &halt(struct_map_def(
            0,
            Opcode::SlotStructMap2,
            SlotType::StructMap2,
            8,
            &fields2,
        )),
    );
    let mut logical2 = base_slot(0, SlotType::StructMap2);
    logical2.field_types = fields2;
    logical2.rows = vec![
        LogicalRow {
            key: LogicalValue::U32(10),
            key2: Some(LogicalValue::U32(3)),
            fields: sparse_row2,
            cmp_ts: None,
        },
        LogicalRow {
            key: LogicalValue::U32(10),
            key2: Some(LogicalValue::U32(u32::MAX)),
            fields: full_row2,
            cmp_ts: None,
        },
    ];
    round_trip(&prog2, &logical2, &SlotEncoding::RAW, &mut interner);
}

#[test]
fn ordered_lists_and_nested_round_trip() {
    let mut interner = TestInterner::default();
    let scalar_init = {
        let mut init = vec![
            Opcode::SlotOrderedList as u8,
            0,
            SlotType::OrderedList as u8,
        ];
        init.extend(8u16.to_le_bytes());
        init.push(StructFieldType::String as u8);
        halt(init)
    };
    let prog = program(1, &scalar_init);
    let mut list = base_slot(0, SlotType::OrderedList);
    list.field_types = vec![StructFieldType::String];
    list.rows = ["x", "y", "x"]
        .iter()
        .enumerate()
        .map(|(i, t)| {
            row(
                LogicalValue::U32(i as u32),
                vec![Some(LogicalValue::Text((*t).into()))],
                None,
            )
        })
        .collect();
    round_trip(&prog, &list, &SlotEncoding::RAW, &mut interner);

    let struct_init = {
        let mut init = vec![
            Opcode::SlotOrderedList as u8,
            0,
            SlotType::OrderedList as u8,
        ];
        init.extend(8u16.to_le_bytes());
        init.push(0xff);
        init.push(2);
        init.push(StructFieldType::UInt32 as u8);
        init.push(StructFieldType::Bool as u8);
        halt(init)
    };
    let prog = program(1, &struct_init);
    let mut list = base_slot(0, SlotType::OrderedList);
    list.field_types = vec![StructFieldType::UInt32, StructFieldType::Bool];
    list.rows = vec![
        row(
            LogicalValue::U32(0),
            vec![Some(LogicalValue::U32(4)), Some(LogicalValue::Bool(true))],
            None,
        ),
        row(
            LogicalValue::U32(1),
            vec![None, Some(LogicalValue::Bool(false))],
            None,
        ),
    ];
    round_trip(&prog, &list, &SlotEncoding::RAW, &mut interner);

    let nested_prog = |inner: SlotType, agg: AggType| {
        let mut init = vec![Opcode::SlotNested as u8, 0, SlotType::Nested as u8];
        init.extend(8u16.to_le_bytes());
        init.push(inner as u8);
        init.extend(16u16.to_le_bytes());
        init.push(agg as u8);
        program(1, &halt(init))
    };

    let mut nested = base_slot(0, SlotType::Nested);
    nested.agg_type = AggType::Sum as u8;
    nested.nested = vec![LogicalNestedEntry {
        outer_key: LogicalValue::U32(5),
        inner: LogicalInner {
            kind: SlotType::HashMap,
            agg_type: AggType::Sum as u8,
            rows: vec![
                row(
                    LogicalValue::U32(1),
                    vec![Some(LogicalValue::U32(100))],
                    None,
                ),
                row(
                    LogicalValue::U32(2),
                    vec![Some(LogicalValue::U32(200))],
                    None,
                ),
            ],
            aggregate: None,
        },
    }];
    round_trip(
        &nested_prog(SlotType::HashMap, AggType::Sum),
        &nested,
        &SlotEncoding::RAW,
        &mut interner,
    );

    let mut nested = base_slot(0, SlotType::Nested);
    nested.agg_type = AggType::Sum as u8;
    nested.nested = vec![LogicalNestedEntry {
        outer_key: LogicalValue::U32(5),
        inner: LogicalInner {
            kind: SlotType::HashSet,
            agg_type: AggType::Sum as u8,
            rows: vec![
                row(LogicalValue::U32(9), Vec::new(), None),
                row(LogicalValue::U32(11), Vec::new(), None),
            ],
            aggregate: None,
        },
    }];
    round_trip(
        &nested_prog(SlotType::HashSet, AggType::Sum),
        &nested,
        &SlotEncoding::RAW,
        &mut interner,
    );

    let mut nested = base_slot(0, SlotType::Nested);
    nested.agg_type = AggType::Sum as u8;
    nested.nested = vec![LogicalNestedEntry {
        outer_key: LogicalValue::U32(5),
        inner: LogicalInner {
            kind: SlotType::Aggregate,
            agg_type: AggType::Sum as u8,
            rows: Vec::new(),
            aggregate: Some(LogicalAggregate {
                value: LogicalValue::F64(17.5),
                count: 4,
            }),
        },
    }];
    round_trip(
        &nested_prog(SlotType::Aggregate, AggType::Sum),
        &nested,
        &SlotEncoding::RAW,
        &mut interner,
    );
}

#[test]
fn condition_tree_round_trip_carries_derived_cells() {
    let prog = program(1, &halt(slot_def(0, SlotType::ConditionTree as u8, 16)));
    let mut logical = base_slot(0, SlotType::ConditionTree);
    logical.condition = Some(LogicalCondition {
        lifecycle_generation: 9,
        last_removed_key: 77,
        cells: vec![
            LogicalConditionCell {
                pos: 3,
                identity: 0x0002_0000_0000_002a,
                value: 0x1122_3344_5566_7788,
            },
            LogicalConditionCell {
                pos: 4,
                identity: DERIVED_FACT_TOMBSTONE_IDENTITY,
                value: 0,
            },
        ],
    });
    let mut interner = TestInterner::default();
    round_trip(&prog, &logical, &SlotEncoding::RAW, &mut interner);
}

#[test]
fn generated_hashmap_states_round_trip_and_continue_identically() {
    // Deterministic generated states: many small maps with latest-wins
    // timestamps, imported under a larger layout, then the same next
    // batch folded into both images.
    let mut lcg = Lcg(0x5eed_2026_1024);
    let prog_small = program(1, &halt(slot_def(0, SlotType::HashMap as u8, 8)));
    let prog_big = program(1, &halt(slot_def(0, SlotType::HashMap as u8, 64)));
    for _case in 0..48 {
        let mut original = fresh_image(&prog_small);
        let meta = SlotMetaView::read(&original, 0);
        let mut hooks = NoVm::default();
        let rows = 1 + lcg.below(6);
        let mut keys: Vec<u32> = Vec::new();
        let mut vals: Vec<u32> = Vec::new();
        let mut cmps: Vec<u8> = Vec::new();
        for _ in 0..rows {
            keys.push(1 + lcg.below(50));
            vals.push(lcg.below(10_000));
            cmps.extend_from_slice(&(f64::from(lcg.below(1000)) / 8.0).to_le_bytes());
        }
        assert_eq!(
            batch_map_upsert(
                Strategy::Latest,
                false,
                &mut original,
                &meta,
                0,
                &keys,
                &vals,
                Some(&cmps),
                CmpType::F64,
                &mut hooks,
            ),
            ErrorCode::Ok
        );
        let exported = export_slot(&original, 0, &SlotEncoding::RAW, &TestInterner::default())
            .expect("export");
        let mut migrated = import_image(
            &prog_big,
            std::slice::from_ref(&exported),
            &SlotEncoding::RAW,
            &mut TestInterner::default(),
        );
        assert_eq!(
            export_slot(&migrated, 0, &SlotEncoding::RAW, &TestInterner::default())
                .expect("re-export"),
            exported
        );

        // The continuation batch mixes stale and fresh timestamps: a
        // migrated slot that lost its comparison lane lets the stale
        // row win and the exports diverge here.
        let mut next_keys: Vec<u32> = Vec::new();
        let mut next_vals: Vec<u32> = Vec::new();
        let mut next_cmps: Vec<u8> = Vec::new();
        for _ in 0..4 {
            next_keys.push(1 + lcg.below(50));
            next_vals.push(lcg.below(10_000));
            next_cmps.extend_from_slice(&(f64::from(lcg.below(1000)) / 8.0).to_le_bytes());
        }
        for image in [&mut original, &mut migrated] {
            let meta = SlotMetaView::read(image, 0);
            let mut hooks = NoVm::default();
            assert_eq!(
                batch_map_upsert(
                    Strategy::Latest,
                    false,
                    image,
                    &meta,
                    0,
                    &next_keys,
                    &next_vals,
                    Some(&next_cmps),
                    CmpType::F64,
                    &mut hooks,
                ),
                ErrorCode::Ok
            );
        }
        assert_eq!(
            export_slot(&original, 0, &SlotEncoding::RAW, &TestInterner::default())
                .expect("export original"),
            export_slot(&migrated, 0, &SlotEncoding::RAW, &TestInterner::default())
                .expect("export migrated"),
            "continuing the fold after import matches continuing the original"
        );
    }
}

/// A test evaluator: bytecode is one byte naming the old field index to
/// read as a number; project reverses the VO bytes so the test can see
/// the projection ran.
struct FieldNumberEval;

impl ExprEvaluator for FieldNumberEval {
    fn eval<'text>(
        &mut self,
        bytecode: &[u8],
        _constants: &[u8],
        row: &TransformRowView<'_, 'text>,
    ) -> Result<Option<LogicalValue<'text>>, ErrorCode> {
        let index = usize::from(*bytecode.first().ok_or(ErrorCode::InvalidProgram)?);
        match row.fields.get(index) {
            Some(Some(LogicalValue::U32(v))) => Ok(Some(LogicalValue::F64(f64::from(*v) + 0.5))),
            Some(Some(LogicalValue::I64(v))) => Ok(Some(LogicalValue::F64(*v as f64))),
            Some(Some(LogicalValue::F64(v))) => Ok(Some(LogicalValue::F64(*v + 20.0))),
            Some(None) | None => Ok(None),
            _ => Err(ErrorCode::InvalidCellValue),
        }
    }

    fn project<'text>(
        &mut self,
        bytes: &[u8],
        _reader: &[u8; 32],
        _row: &TransformRowView<'_, 'text>,
    ) -> Result<Option<LogicalValue<'text>>, ErrorCode> {
        Ok(Some(LogicalValue::Bytes(
            bytes.iter().rev().copied().collect(),
        )))
    }
}

#[test]
fn eager_rewrite_copies_unchanged_and_applies_transform_plan() {
    // Slot 0: hash map, geometry identical in both programs (region copy).
    // Slot 1: struct map old [U32, String, Vo] -> new [String, F64, Vo]
    // by Copy, Expr, Project.
    let mut old_init = slot_def(0, SlotType::HashMap as u8, 8);
    old_init.extend(struct_map_def(
        1,
        Opcode::SlotStructMap,
        SlotType::StructMap,
        8,
        &[
            StructFieldType::UInt32,
            StructFieldType::String,
            StructFieldType::Vo,
        ],
    ));
    let old_prog = program(2, &halt(old_init));
    let mut new_init = slot_def(0, SlotType::HashMap as u8, 8);
    new_init.extend(struct_map_def(
        1,
        Opcode::SlotStructMap,
        SlotType::StructMap,
        32,
        &[
            StructFieldType::String,
            StructFieldType::Float64,
            StructFieldType::Vo,
        ],
    ));
    let new_prog = program(2, &halt(new_init));

    let mut interner = TestInterner::default();
    let mut old_map = base_slot(0, SlotType::HashMap);
    old_map.rows = vec![row(
        LogicalValue::U32(3),
        vec![Some(LogicalValue::U32(30))],
        Some(12.5),
    )];
    let mut old_rows = base_slot(1, SlotType::StructMap);
    old_rows.field_types = vec![
        StructFieldType::UInt32,
        StructFieldType::String,
        StructFieldType::Vo,
    ];
    old_rows.rows = vec![row(
        LogicalValue::U32(7),
        vec![
            Some(LogicalValue::U32(41)),
            Some(LogicalValue::Text("gold".into())),
            Some(LogicalValue::Bytes(vec![1, 2, 3])),
        ],
        None,
    )];
    let old_image = import_image(
        &old_prog,
        &[old_map.clone(), old_rows],
        &SlotEncoding::RAW,
        &mut interner,
    );

    let plan = vec![
        SlotTransfer {
            source: 0,
            target: 0,
            encoding: SlotEncoding::RAW,
            transform: None,
        },
        SlotTransfer {
            source: 1,
            target: 1,
            encoding: SlotEncoding::RAW,
            transform: Some(TransformPlan {
                fields: vec![
                    FieldTransform::Copy(1),
                    FieldTransform::Expr {
                        bytecode: vec![0],
                        constants: Vec::new(),
                    },
                    FieldTransform::Project {
                        from: 2,
                        reader: [7u8; 32],
                    },
                ],
            }),
        },
    ];
    let mut eval = FieldNumberEval;
    let new_image = rewrite_slot_set(
        &old_image,
        &new_prog,
        MAGICS,
        &plan,
        &mut interner,
        Some(&mut eval),
    )
    .expect("rewrite");
    validate_state(&new_image).expect("rewritten image validates");
    assert_eq!(
        export_slot(&new_image, 0, &SlotEncoding::RAW, &interner).expect("export copied map"),
        old_map,
        "the geometry-identical slot moves whole, comparison timestamp included"
    );
    let migrated =
        export_slot(&new_image, 1, &SlotEncoding::RAW, &interner).expect("export transformed");
    assert_eq!(
        migrated.rows,
        vec![row(
            LogicalValue::U32(7),
            vec![
                Some(LogicalValue::Text("gold".into())),
                Some(LogicalValue::F64(41.5)),
                Some(LogicalValue::Bytes(vec![3, 2, 1])),
            ],
            None,
        )]
    );
}

#[test]
fn apply_transform_preserves_aggregate_count_and_scalar_timestamp() {
    let mut eval = FieldNumberEval;
    let mut agg = base_slot(0, SlotType::Aggregate);
    agg.agg_type = AggType::Avg as u8;
    agg.aggregate = Some(LogicalAggregate {
        value: LogicalValue::F64(80.0),
        count: 4,
    });
    let plan = TransformPlan {
        fields: vec![FieldTransform::Expr {
            bytecode: vec![0],
            constants: Vec::new(),
        }],
    };
    let out = apply_transform(&agg, &[], &plan, std::slice::from_ref(&agg), &mut eval)
        .expect("transform aggregate");
    assert_eq!(
        out.aggregate,
        Some(LogicalAggregate {
            value: LogicalValue::F64(100.0),
            count: 4
        })
    );

    let mut scalar = base_slot(0, SlotType::Scalar);
    scalar.agg_type = AggType::ScalarU32 as u8;
    scalar.scalar = Some(LogicalScalar {
        value: LogicalValue::U32(6),
        cmp_ts: 33.25,
    });
    let plan = TransformPlan {
        fields: vec![FieldTransform::Expr {
            bytecode: vec![0],
            constants: Vec::new(),
        }],
    };
    let out = apply_transform(
        &scalar,
        &[StructFieldType::Float64],
        &plan,
        std::slice::from_ref(&scalar),
        &mut eval,
    )
    .expect("transform scalar");
    assert_eq!(
        out.scalar,
        Some(LogicalScalar {
            value: LogicalValue::F64(6.5),
            cmp_ts: 33.25
        })
    );
}

#[test]
fn refusals_are_typed() {
    let prog = program(1, &halt(slot_def(0, SlotType::HashMap as u8, 8)));
    let mut image = fresh_image(&prog);
    // A truncated image refuses as InvalidState, not a panic.
    let error = export_slot(
        &image[..40],
        0,
        &SlotEncoding::RAW,
        &TestInterner::default(),
    )
    .expect_err("truncated image refuses");
    assert_eq!(error.cause, LogicalExportCause::Vm(ErrorCode::InvalidState));
    assert_eq!(error.op, "export_slot");

    // Kind mismatch on import.
    let wrong = base_slot(0, SlotType::HashSet);
    let error = import_slot(
        &mut image,
        &wrong,
        &SlotEncoding::RAW,
        &mut TestInterner::default(),
    )
    .expect_err("kind mismatch refuses");
    assert_eq!(
        error.cause,
        LogicalExportCause::KindMismatch {
            expected: SlotType::HashSet,
            found: SlotType::HashMap
        }
    );

    // Duplicate keys refuse.
    let mut dup = base_slot(0, SlotType::HashMap);
    dup.rows = vec![
        row(
            LogicalValue::U32(1),
            vec![Some(LogicalValue::U32(1))],
            Some(1.0),
        ),
        row(
            LogicalValue::U32(1),
            vec![Some(LogicalValue::U32(2))],
            Some(1.0),
        ),
    ];
    let error = import_slot(
        &mut image,
        &dup,
        &SlotEncoding::RAW,
        &mut TestInterner::default(),
    )
    .expect_err("duplicate key refuses");
    assert_eq!(error.cause, LogicalExportCause::DuplicateKey);

    // An interned id the resolver does not know refuses by name.
    let enc = SlotEncoding {
        key: CellEncoding::Interned,
        ..SlotEncoding::RAW
    };
    let mut one = base_slot(0, SlotType::HashMap);
    one.rows = vec![row(
        LogicalValue::Text("k".into()),
        vec![Some(LogicalValue::U32(1))],
        Some(1.0),
    )];
    let mut interner = TestInterner::default();
    let mut image = fresh_image(&prog);
    import_slot(&mut image, &one, &enc, &mut interner).expect("import interned");
    let error = export_slot(&image, 0, &enc, &TestInterner::default())
        .expect_err("unknown intern id refuses");
    assert_eq!(error.cause, LogicalExportCause::UnknownInternId(0));
}

#[test]
fn cmp_lane_mismatch_and_nested_subtype_refuse() {
    // A row with no timestamp into a map that stores one refuses: the
    // alternative is inventing a value that loses every comparison.
    let ts_prog = program(1, &halt(slot_def(0, SlotType::HashMap as u8, 8)));
    let mut no_ts = base_slot(0, SlotType::HashMap);
    no_ts.rows = vec![row(
        LogicalValue::U32(1),
        vec![Some(LogicalValue::U32(1))],
        None,
    )];
    let mut image = fresh_image(&ts_prog);
    let error = import_slot(
        &mut image,
        &no_ts,
        &SlotEncoding::RAW,
        &mut TestInterner::default(),
    )
    .expect_err("missing comparison timestamp refuses");
    assert!(matches!(error.cause, LogicalExportCause::InvalidRow(_)));

    // And the mirror image: a timestamp aimed at a map with no lane
    // (the 0x40 no-timestamps flag) is never silently dropped.
    let bare_prog = program(1, &halt(slot_def(0, SlotType::HashMap as u8 | 0x40, 8)));
    let mut with_ts = base_slot(0, SlotType::HashMap);
    with_ts.rows = vec![row(
        LogicalValue::U32(1),
        vec![Some(LogicalValue::U32(1))],
        Some(2.0),
    )];
    let mut image = fresh_image(&bare_prog);
    let error = import_slot(
        &mut image,
        &with_ts,
        &SlotEncoding::RAW,
        &mut TestInterner::default(),
    )
    .expect_err("unstorable comparison timestamp refuses");
    assert!(matches!(error.cause, LogicalExportCause::InvalidRow(_)));
    // The same rows without the timestamp round-trip through the
    // no-lane layout, so the refusal is about the lane, not the map.
    let mut interner = TestInterner::default();
    round_trip(&bare_prog, &no_ts, &SlotEncoding::RAW, &mut interner);

    // A nested Sum does not import into a nested Min: both store an
    // f64 value and a count, so only the subtype check separates them.
    let mut init = vec![Opcode::SlotNested as u8, 0, SlotType::Nested as u8];
    init.extend(8u16.to_le_bytes());
    init.push(SlotType::Aggregate as u8);
    init.extend(16u16.to_le_bytes());
    init.push(AggType::Min as u8);
    let min_prog = program(1, &halt(init));
    let mut nested = base_slot(0, SlotType::Nested);
    nested.agg_type = AggType::Min as u8;
    nested.nested = vec![LogicalNestedEntry {
        outer_key: LogicalValue::U32(5),
        inner: LogicalInner {
            kind: SlotType::Aggregate,
            agg_type: AggType::Sum as u8,
            rows: Vec::new(),
            aggregate: Some(LogicalAggregate {
                value: LogicalValue::F64(17.5),
                count: 4,
            }),
        },
    }];
    let mut image = fresh_image(&min_prog);
    let error = import_slot(
        &mut image,
        &nested,
        &SlotEncoding::RAW,
        &mut TestInterner::default(),
    )
    .expect_err("nested subtype mismatch refuses");
    assert!(matches!(
        error.cause,
        LogicalExportCause::InvalidRow("nested aggregate subtype mismatch")
    ));
}
