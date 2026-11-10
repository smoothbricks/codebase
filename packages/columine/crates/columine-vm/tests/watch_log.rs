//! The watch log (`specs/columine/07-change-feed.md` §7, acceptance §8):
//! each write of a watched slot advances its sequence by one and logs its
//! key, kind and prior; a write that changes nothing logs and advances
//! nothing; a rolled-back attempt leaves nothing; row images decode after
//! growth and an interner release; `net_changes` nets inserts, updates and
//! deletes, drops a key written back, and answers `Overflowed` below
//! coverage, across a keyless write and past ring eviction; a refold of a
//! rewound state records nothing new; `log_settle` drops a divergent
//! future; a cloned log answers like its source; and a watched slot records
//! warm passes with zero allocations.

mod common;

use columine_types::InternedStr;
use columine_types::PROGRAM_MAGIC;
use columine_types::types::{
    AggType, ChangeFlag, ErrorCode, NET_CHANGE_ALIGN, NET_CHANGE_HEADER_SIZE, NetChangeKind,
    NetChangeOffset, Opcode, SlotMetaOffset, SlotType, StructFieldType, WATCH_LOG_ENTRY_SIZE,
    WATCH_LOG_HEADER_SIZE, WatchLogEntryOffset, WatchLogOffset,
};
use columine_vm::bitmap_ops::{BitmapAlgebraOp, BitmapSource};
use columine_vm::hashmap_ops::{CmpType, Strategy};
use columine_vm::key_index::{InternBytes, NoStrings};
use columine_vm::logical_export::{
    InternLookup, Interner, LogicalRow, LogicalSlot, LogicalValue, SlotEncoding, import_slot,
};
use columine_vm::meta::slot_meta_base;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, GrowthRoom, calculate_grown_state_size, calculate_state_size,
    init_state,
};
use columine_vm::vm::{Vm, f64s_as_bytes, u32s_as_bytes};
use columine_vm::watch_log::{NetAnswer, NetChanges};
use common::memory::with_allocations;

const OK: u32 = ErrorCode::Ok as u32;
const ADD: u32 = 0xc0_0001;

/// Batch columns.
const TYPE: u8 = 0;
const KEY: u8 = 1;
const KEY2: u8 = 2;
const VALUE: u8 = 3;
const VALUE2: u8 = 4;
const STAMP: u8 = 5;
const COLUMNS: u32 = 6;

fn program(slots: u32, init: &[u8], reduce: &[u8]) -> Vec<u8> {
    let mut init = init.to_vec();
    init.push(Opcode::Halt as u8);
    let mut out = vec![0u8; 32];
    out.extend(PROGRAM_MAGIC.to_le_bytes());
    out.extend([2, 0, 0, 0]); // version 2, no callbacks, no flags
    out.extend(slots.to_le_bytes());
    out.extend(COLUMNS.to_le_bytes());
    out.extend(u32::try_from(init.len()).unwrap().to_le_bytes());
    out.extend(u32::try_from(reduce.len()).unwrap().to_le_bytes());
    out.extend_from_slice(&init);
    out.extend_from_slice(reduce);
    out
}

fn image(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("admitted");
    let mut state = vec![0u8; size as usize];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

fn slot_def(slot: u8, kind: SlotType) -> Vec<u8> {
    vec![Opcode::SlotDef as u8, slot, kind as u8, 16, 0]
}

/// A TTL slot of `kind`: one second of life, stamped by [`STAMP`].
fn ttl_slot_def(slot: u8, kind: SlotType) -> Vec<u8> {
    let mut def = vec![Opcode::SlotDef as u8, slot, kind as u8 | 0x10, 16, 0];
    def.extend(1.0f32.to_le_bytes());
    def.extend(0.0f32.to_le_bytes());
    def.extend([STAMP, 0]);
    def
}

fn struct_map_def(slot: u8, opcode: Opcode, kind: SlotType, fields: &[StructFieldType]) -> Vec<u8> {
    let mut def = vec![
        opcode as u8,
        slot,
        kind as u8,
        16,
        0,
        u8::try_from(fields.len()).unwrap(),
    ];
    def.extend(fields.iter().map(|&field| field as u8));
    def
}

fn for_each(body: &[u8]) -> Vec<u8> {
    let mut code = vec![Opcode::ForEach as u8, TYPE, 1];
    code.extend(ADD.to_le_bytes());
    code.extend(u16::try_from(body.len()).unwrap().to_le_bytes());
    code.extend_from_slice(body);
    code
}

/// One log row: its keys and its two value cells, stamped `stamp`.
#[derive(Clone, Copy)]
struct Row {
    key: u32,
    key2: u32,
    value: u32,
    value2: u32,
    stamp: f64,
}

const fn row(key: u32, key2: u32, value: u32) -> Row {
    Row {
        key,
        key2,
        value,
        value2: value + 1,
        stamp: 1.0,
    }
}

struct Batch {
    cols: Vec<Vec<u8>>,
    rows: u32,
}

impl Batch {
    fn slices(&self) -> Vec<&[u8]> {
        self.cols.iter().map(Vec::as_slice).collect()
    }
}

fn batch(rows: &[Row]) -> Batch {
    let lane = |cell: &dyn Fn(&Row) -> u32| -> Vec<u8> {
        let cells: Vec<u32> = rows.iter().map(cell).collect();
        u32s_as_bytes(&cells).to_vec()
    };
    let stamps: Vec<f64> = rows.iter().map(|row| row.stamp).collect();
    let mut cols = vec![Vec::new(); COLUMNS as usize];
    cols[usize::from(TYPE)] = lane(&|_| ADD);
    cols[usize::from(KEY)] = lane(&|row| row.key);
    cols[usize::from(KEY2)] = lane(&|row| row.key2);
    cols[usize::from(VALUE)] = lane(&|row| row.value);
    cols[usize::from(VALUE2)] = lane(&|row| row.value2);
    cols[usize::from(STAMP)] = f64s_as_bytes(&stamps).to_vec();
    Batch {
        cols,
        rows: u32::try_from(rows.len()).unwrap(),
    }
}

fn run(vm: &mut Vm, state: &mut [u8], program: &[u8], rows: &[Row]) -> u32 {
    let batch = batch(rows);
    vm.execute_batch(state, program, &batch.slices(), batch.rows)
}

fn seq(state: &[u8], slot: u32) -> u64 {
    let at = (slot_meta_base(slot) + SlotMetaOffset::WRITE_SEQ) as usize;
    u64::from_le_bytes(state[at..at + 8].try_into().unwrap())
}

/// One decoded net change: key lanes, kind and prior.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Net {
    key1: u32,
    key2: u32,
    kind: u8,
    prior: Option<Vec<u8>>,
}

const fn insert(key1: u32, key2: u32) -> Net {
    Net {
        key1,
        key2,
        kind: NetChangeKind::INSERT,
        prior: None,
    }
}

fn update(key1: u32, key2: u32, prior: &[u8]) -> Net {
    Net {
        key1,
        key2,
        kind: NetChangeKind::UPDATE,
        prior: Some(prior.to_vec()),
    }
}

fn delete(key1: u32, key2: u32, prior: Option<&[u8]>) -> Net {
    Net {
        key1,
        key2,
        kind: NetChangeKind::DELETE,
        prior: prior.map(<[u8]>::to_vec),
    }
}

/// Decode `out`'s records by the layout `columine_types` names.
fn decode(out: &NetChanges) -> Vec<Net> {
    let bytes = out.bytes();
    let mut at = 0usize;
    let mut changes = Vec::new();
    while at < bytes.len() {
        assert_eq!(at % NET_CHANGE_ALIGN as usize, 0, "records start aligned");
        let header = &bytes[at..at + NET_CHANGE_HEADER_SIZE as usize];
        let u32_at =
            |offset: u32| u32::from_le_bytes(header[offset as usize..][..4].try_into().unwrap());
        let prior_len = u32_at(NetChangeOffset::PRIOR_LEN) as usize;
        let body = at + NET_CHANGE_HEADER_SIZE as usize;
        changes.push(Net {
            key1: u32_at(NetChangeOffset::KEY1),
            key2: u32_at(NetChangeOffset::KEY2),
            kind: header[NetChangeOffset::KIND as usize],
            prior: (header[NetChangeOffset::HAS_PRIOR as usize] == 1)
                .then(|| bytes[body..body + prior_len].to_vec()),
        });
        at = (body + prior_len).next_multiple_of(NET_CHANGE_ALIGN as usize);
    }
    assert_eq!(changes.len(), out.len() as usize);
    changes
}

/// `slot`'s net change from `since`, or `None` when it overflowed.
fn net(vm: &Vm, state: &[u8], slot: u32, since: u64) -> Option<Vec<Net>> {
    let mut out = NetChanges::default();
    match vm.net_changes(state, slot, since, &NoStrings, &mut out) {
        Ok(NetAnswer::Changes) => Some(decode(&out)),
        Ok(NetAnswer::Overflowed) => None,
        Err(error) => panic!("net_changes refused: {error:?}"),
    }
}

/// Run `write`, require it advanced `slot`'s sequence by exactly one, and
/// answer that one write's net change.
fn one_write(
    vm: &mut Vm,
    state: &mut Vec<u8>,
    slot: u32,
    write: impl FnOnce(&mut Vm, &mut Vec<u8>),
) -> Vec<Net> {
    let before = seq(state, slot);
    write(vm, state);
    assert_eq!(
        seq(state, slot),
        before + 1,
        "one write call, one sequence step"
    );
    net(vm, state, slot, before).expect("a keyed write answers its change")
}

fn upsert(vm: &mut Vm, state: &mut [u8], slot: u32, key: u32, value: u32) {
    let result = vm.map_upsert(
        Strategy::Last,
        false,
        state,
        slot,
        &[key],
        &[value],
        None,
        CmpType::U32,
    );
    assert_eq!(result, ErrorCode::Ok);
}

fn le(value: u32) -> [u8; 4] {
    value.to_le_bytes()
}

#[test]
fn container_writes_advance_by_one_and_log_key_kind_and_prior() {
    let init = [
        slot_def(0, SlotType::HashMap),
        slot_def(1, SlotType::HashSet),
        slot_def(2, SlotType::Bitmap),
        ttl_slot_def(3, SlotType::HashMap),
    ]
    .concat();
    let code = program(4, &init, &[]);
    let state = image(&code);
    let mut vm = Vm::default();
    for slot in 0..4 {
        assert_eq!(vm.watch(&state, slot, 64, 1024), Ok(0));
    }

    let mut s = state;
    assert_eq!(
        one_write(&mut vm, &mut s, 0, |vm, s| upsert(vm, s, 0, 5, 10)),
        [insert(5, 0)]
    );
    assert_eq!(
        one_write(&mut vm, &mut s, 0, |vm, s| upsert(vm, s, 0, 5, 11)),
        [update(5, 0, &le(10))]
    );
    assert_eq!(
        one_write(&mut vm, &mut s, 0, |vm, s| {
            assert_eq!(vm.map_remove(false, s, 0, &[5]), ErrorCode::Ok);
        }),
        [delete(5, 0, Some(&le(11)))]
    );

    assert_eq!(
        one_write(&mut vm, &mut s, 1, |vm, s| {
            assert_eq!(vm.set_insert(false, s, 1, &[3], None), ErrorCode::Ok);
        }),
        [insert(3, 0)]
    );
    assert_eq!(
        one_write(&mut vm, &mut s, 1, |vm, s| {
            assert_eq!(vm.set_remove(false, s, 1, &[3]), ErrorCode::Ok);
        }),
        [delete(3, 0, None)]
    );

    // A bitmap patch is one write; it logs the members it changed.
    assert_eq!(
        one_write(&mut vm, &mut s, 2, |vm, s| {
            assert_eq!(vm.set_insert(false, s, 2, &[7, 9], None), ErrorCode::Ok);
        }),
        [insert(7, 0), insert(9, 0)]
    );
    assert_eq!(
        one_write(&mut vm, &mut s, 2, |vm, s| {
            assert_eq!(vm.set_insert(false, s, 2, &[9, 4], None), ErrorCode::Ok);
        }),
        [insert(4, 0)]
    );
    assert_eq!(
        one_write(&mut vm, &mut s, 2, |vm, s| {
            assert_eq!(vm.set_remove(false, s, 2, &[7, 8]), ErrorCode::Ok);
        }),
        [delete(7, 0, None)]
    );

    // A TTL eviction removes its key with its value.
    let stamps = f64s_as_bytes(&[1.0]).to_vec();
    assert_eq!(
        vm.map_upsert(
            Strategy::Last,
            false,
            &mut s,
            3,
            &[6],
            &[60],
            Some(stamps.as_slice()),
            CmpType::F64
        ),
        ErrorCode::Ok
    );
    assert_eq!(
        one_write(&mut vm, &mut s, 3, |vm, s| {
            assert_eq!(vm.evict_all_expired(false, s, 1_000.0), Ok(1));
        }),
        [delete(6, 0, Some(&le(60)))]
    );
}

/// Read a struct-map row image's fields back: the row bytes, then each
/// length-prefixed payload.
fn image_payloads(image: &[u8], row_size: usize) -> Vec<&[u8]> {
    let mut payloads = Vec::new();
    let mut at = row_size;
    while at < image.len() {
        let len = u32::from_le_bytes(image[at..at + 4].try_into().unwrap()) as usize;
        payloads.push(&image[at + 4..at + 4 + len]);
        at += 4 + len;
    }
    payloads
}

#[test]
fn struct_and_two_key_row_writes_log_their_row_images() {
    use StructFieldType::UInt32;
    let init = [
        struct_map_def(0, Opcode::SlotStructMap, SlotType::StructMap, &[UInt32]),
        struct_map_def(
            1,
            Opcode::SlotStructMap2,
            SlotType::StructMap2,
            &[UInt32, UInt32],
        ),
    ]
    .concat();
    let upsert = program(
        2,
        &init,
        &[
            Opcode::BatchStructMapUpsertLast as u8,
            0,
            KEY,
            1,
            VALUE,
            0,
            0,
            Opcode::BatchStructMap2UpsertLast as u8,
            1,
            KEY,
            KEY2,
            2,
            VALUE,
            0,
            VALUE2,
            1,
        ],
    );
    let remove = program(
        2,
        &init,
        &[
            Opcode::BatchStructMapRemove as u8,
            0,
            KEY,
            Opcode::BatchStructMap2Remove as u8,
            1,
            KEY,
            KEY2,
        ],
    );
    let prefix = program(
        2,
        &init,
        &[Opcode::BatchStructMap2RemovePrefix as u8, 1, KEY],
    );
    let mut state = image(&upsert);
    let mut vm = Vm::default();
    assert_eq!(vm.watch(&state, 0, 64, 1024), Ok(0));
    assert_eq!(vm.watch(&state, 1, 64, 1024), Ok(0));

    assert_eq!(OK, run(&mut vm, &mut state, &upsert, &[row(5, 1, 10)]));
    assert_eq!((seq(&state, 0), seq(&state, 1)), (1, 1));
    let first_one = net(&vm, &state, 0, 0).unwrap();
    assert_eq!(first_one, [insert(5, 0)]);
    assert_eq!(net(&vm, &state, 1, 0).unwrap(), [insert(5, 1)]);

    let row_one = |state: &[u8]| {
        let map = columine_vm::vm::CheckedStructMap::bind(state, 0).unwrap();
        let at = map.row(5).unwrap() as usize;
        let size = columine_vm::struct_map::StructMapSlot::bind(state, 0).row_size as usize;
        state[at..at + size].to_vec()
    };
    let stored_one = row_one(&state);
    assert_eq!(OK, run(&mut vm, &mut state, &upsert, &[row(5, 1, 11)]));
    assert_eq!((seq(&state, 0), seq(&state, 1)), (2, 2));
    // A one-key row of one u32 field: bitset byte, then the cell; no
    // payloads follow.
    assert_eq!(net(&vm, &state, 0, 1).unwrap(), [update(5, 0, &stored_one)]);
    let two = net(&vm, &state, 1, 1).unwrap();
    assert_eq!(two.len(), 1);
    assert_eq!(
        (two[0].key1, two[0].key2, two[0].kind),
        (5, 1, NetChangeKind::UPDATE)
    );
    let prior = two[0].prior.as_ref().unwrap();
    assert_eq!(&prior[1..9], [le(10), le(11)].concat().as_slice());

    assert_eq!(OK, run(&mut vm, &mut state, &remove, &[row(5, 1, 0)]));
    assert_eq!((seq(&state, 0), seq(&state, 1)), (3, 3));
    let removed = net(&vm, &state, 0, 2).unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(
        (removed[0].key1, removed[0].kind),
        (5, NetChangeKind::DELETE)
    );
    assert_eq!(
        net(&vm, &state, 1, 2).unwrap()[0].kind,
        NetChangeKind::DELETE
    );

    // A prefix removal removes each row in one write of its own.
    assert_eq!(
        OK,
        run(&mut vm, &mut state, &upsert, &[row(8, 1, 1), row(8, 2, 2)])
    );
    let before = seq(&state, 1);
    assert_eq!(OK, run(&mut vm, &mut state, &prefix, &[row(8, 0, 0)]));
    assert_eq!(seq(&state, 1), before + 2);
    let gone = net(&vm, &state, 1, before).unwrap();
    let mut gone: Vec<_> = gone
        .iter()
        .map(|net| (net.key1, net.key2, net.kind))
        .collect();
    gone.sort_unstable();
    assert_eq!(
        gone,
        [(8, 1, NetChangeKind::DELETE), (8, 2, NetChangeKind::DELETE)]
    );
}

#[test]
fn aggregate_scalar_and_list_writes_log_one_entry_each() {
    let init = [
        vec![
            Opcode::SlotDef as u8,
            0,
            SlotType::Aggregate as u8,
            AggType::Count as u8,
            0,
        ],
        vec![
            Opcode::SlotDef as u8,
            1,
            SlotType::Scalar as u8,
            AggType::ScalarU32 as u8,
            0,
        ],
        vec![
            Opcode::SlotOrderedList as u8,
            2,
            SlotType::OrderedList as u8,
            16,
            0,
            0,
        ],
    ]
    .concat();
    let code = program(
        3,
        &init,
        &[
            vec![Opcode::BatchAggCount as u8, 0],
            for_each(&[
                Opcode::BatchScalarLatest as u8,
                1,
                VALUE,
                STAMP,
                Opcode::ListAppend as u8,
                2,
                VALUE,
            ]),
        ]
        .concat(),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    for slot in 0..3 {
        assert_eq!(vm.watch(&state, slot, 64, 1024), Ok(0));
    }
    assert_eq!(OK, run(&mut vm, &mut state, &code, &[row(1, 0, 40)]));
    assert_eq!([seq(&state, 0), seq(&state, 1), seq(&state, 2)], [1, 1, 1]);
    // An aggregate is always present: its first write updates its 0.
    assert_eq!(net(&vm, &state, 0, 0).unwrap(), [update(0, 0, &[0; 8])]);
    // An unset scalar decodes to nothing: its first write inserts it.
    assert_eq!(net(&vm, &state, 1, 0).unwrap(), [insert(0, 0)]);
    // A list member is keyed by its position.
    assert_eq!(net(&vm, &state, 2, 0).unwrap(), [insert(0, 0)]);

    let mut later = row(1, 0, 41);
    later.stamp = 2.0;
    assert_eq!(OK, run(&mut vm, &mut state, &code, &[later]));
    assert_eq!([seq(&state, 0), seq(&state, 1), seq(&state, 2)], [2, 2, 2]);
    assert_eq!(
        net(&vm, &state, 0, 1).unwrap(),
        [update(0, 0, &1u64.to_le_bytes())]
    );
    assert_eq!(
        net(&vm, &state, 1, 1).unwrap(),
        [update(0, 0, &u64::from(40u32).to_le_bytes())]
    );
    assert_eq!(net(&vm, &state, 2, 1).unwrap(), [insert(1, 0)]);
}

#[test]
fn an_equal_value_a_ttl_refresh_and_a_seal_advance_nothing() {
    let init = [
        slot_def(0, SlotType::HashMap),
        ttl_slot_def(1, SlotType::HashSet),
    ]
    .concat();
    let code = program(2, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 1024).unwrap();
    vm.watch(&state, 1, 64, 1024).unwrap();
    upsert(&mut vm, &mut state, 0, 5, 10);
    upsert(&mut vm, &mut state, 0, 5, 10);
    assert_eq!(seq(&state, 0), 1);
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[3], Some(&[1.0])),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[3], Some(&[2.0])),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, 1), 1, "a TTL-only refresh advances nothing");
    vm.seal_state(&mut state, &NoStrings).expect("seal");
    assert_eq!((seq(&state, 0), seq(&state, 1)), (1, 1));
    assert_eq!(net(&vm, &state, 0, 0).unwrap(), [insert(5, 0)]);
    assert_eq!(net(&vm, &state, 1, 0).unwrap(), [insert(3, 0)]);
}

#[test]
fn a_rolled_back_attempt_restores_the_sequence_and_leaves_no_entries() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 1024).unwrap();
    upsert(&mut vm, &mut state, 0, 5, 10);
    vm.undo_enable(&state).unwrap();
    let checkpoint = vm.undo_checkpoint();
    upsert(&mut vm, &mut state, 0, 5, 11);
    upsert(&mut vm, &mut state, 0, 6, 12);
    assert_eq!(seq(&state, 0), 3);
    vm.undo_rollback(&mut state, checkpoint).unwrap();
    vm.undo_commit();
    assert_eq!(seq(&state, 0), 1);
    assert_eq!(net(&vm, &state, 0, 0).unwrap(), [insert(5, 0)]);
    // The retry records at the restored stamps.
    upsert(&mut vm, &mut state, 0, 5, 13);
    assert_eq!(net(&vm, &state, 0, 1).unwrap(), [update(5, 0, &le(10))]);
}

/// A resolver whose ids can be released.
#[derive(Default)]
struct Strings {
    texts: Vec<Option<String>>,
}

impl InternLookup for Strings {
    fn text(&self, id: u32) -> Option<&str> {
        self.texts.get(id as usize)?.as_deref()
    }
}

impl Interner for Strings {
    fn intern(&mut self, text: InternedStr<'_>) -> Result<u32, ErrorCode> {
        let text = text.as_str();
        if let Some(id) = self
            .texts
            .iter()
            .position(|held| held.as_deref() == Some(text))
        {
            return Ok(id as u32);
        }
        self.texts.push(Some(text.to_owned()));
        Ok(self.texts.len() as u32 - 1)
    }
}

impl InternBytes for Strings {
    fn bytes(&self, id: u32) -> Option<&[u8]> {
        self.text(id).map(str::as_bytes)
    }
}

#[test]
fn a_row_image_decodes_after_growth_and_an_interner_release() {
    use StructFieldType::{ArrayString, ArrayU32, String as Text, UInt32, Vo};
    let fields = [UInt32, Text, ArrayU32, ArrayString, Vo];
    let init = struct_map_def(0, Opcode::SlotStructMap, SlotType::StructMap, &fields);
    let remove = program(1, &init, &[Opcode::BatchStructMapRemove as u8, 0, KEY]);
    let mut state = image(&remove);
    let mut strings = Strings::default();
    let logical = LogicalSlot {
        slot: 0,
        kind: SlotType::StructMap,
        agg_type: 0,
        field_types: fields.to_vec(),
        rows: vec![LogicalRow {
            key: LogicalValue::U32(10),
            key2: None,
            fields: vec![
                Some(LogicalValue::U32(7)),
                Some(LogicalValue::Text("tier".into())),
                Some(LogicalValue::Array(vec![
                    LogicalValue::U32(1),
                    LogicalValue::U32(2),
                ])),
                Some(LogicalValue::Array(vec![
                    LogicalValue::Text("a".into()),
                    LogicalValue::Text("bc".into()),
                ])),
                Some(LogicalValue::Bytes(vec![9, 8, 7])),
            ],
            cmp_ts: None,
        }],
        aggregate: None,
        scalar: None,
        nested: Vec::new(),
        eviction: Vec::new(),
        evicted: Vec::new(),
        condition: None,
    };
    import_slot(&mut state, &logical, &SlotEncoding::RAW, &mut strings).expect("import");
    assert_eq!(seq(&state, 0), 0, "a logical import starts the slot at 0");
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 4096).unwrap();
    let rows = batch(&[row(10, 0, 0)]);
    assert_eq!(
        OK,
        vm.execute_batch_in(&mut state, &remove, &rows.slices(), rows.rows, &strings)
    );
    let map = columine_vm::struct_map::StructMapSlot::bind(&state, 0);
    let row_size = map.row_size as usize;

    let size = calculate_grown_state_size(&state, 0, GrowthRoom::Rows).unwrap();
    let mut grown = vec![0u8; size as usize];
    vm.grow_state(&state, &mut grown, 0, GrowthRoom::Rows)
        .unwrap();
    strings.texts.iter_mut().for_each(|text| *text = None);

    let mut out = NetChanges::default();
    assert_eq!(
        vm.net_changes(&grown, 0, 0, &strings, &mut out),
        Ok(NetAnswer::Changes)
    );
    let changes = decode(&out);
    assert_eq!(changes.len(), 1);
    assert_eq!(
        (changes[0].key1, changes[0].kind),
        (10, NetChangeKind::DELETE)
    );
    let image = changes[0].prior.as_ref().unwrap();
    let mut array_text = Vec::new();
    for text in ["a", "bc"] {
        array_text.extend((text.len() as u32).to_le_bytes());
        array_text.extend(text.as_bytes());
    }
    let numbers = [le(1), le(2)].concat();
    assert_eq!(
        image_payloads(image, row_size),
        [b"tier".as_slice(), &numbers, &array_text, &[9, 8, 7]]
    );
}

#[test]
fn net_changes_nets_inserts_updates_and_deletes_and_drops_a_key_written_back() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    upsert(&mut vm, &mut state, 0, 2, 20);
    upsert(&mut vm, &mut state, 0, 3, 30);
    upsert(&mut vm, &mut state, 0, 4, 40);
    let coverage = vm.watch(&state, 0, 64, 1024).unwrap();
    assert_eq!(coverage, 3);
    upsert(&mut vm, &mut state, 0, 1, 10);
    upsert(&mut vm, &mut state, 0, 2, 21);
    assert_eq!(vm.map_remove(false, &mut state, 0, &[3]), ErrorCode::Ok);
    upsert(&mut vm, &mut state, 0, 4, 41);
    upsert(&mut vm, &mut state, 0, 4, 40);
    upsert(&mut vm, &mut state, 0, 5, 50);
    assert_eq!(vm.map_remove(false, &mut state, 0, &[5]), ErrorCode::Ok);
    assert_eq!(
        net(&vm, &state, 0, coverage).unwrap(),
        [
            insert(1, 0),
            update(2, 0, &le(20)),
            delete(3, 0, Some(&le(30)))
        ]
    );
    // Watching again keeps the log and answers its coverage.
    assert_eq!(vm.watch(&state, 0, 1, 0), Ok(coverage));
}

#[test]
fn net_changes_overflows_below_coverage_across_a_keyless_write_and_past_eviction() {
    let init = [
        slot_def(0, SlotType::HashMap),
        slot_def(1, SlotType::Bitmap),
        slot_def(2, SlotType::Bitmap),
    ]
    .concat();
    let code = program(3, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    assert_eq!(net(&vm, &state, 0, 0), None, "an unwatched slot overflows");
    upsert(&mut vm, &mut state, 0, 1, 10);
    // A ring of two entries.
    assert_eq!(vm.watch(&state, 0, 2, 64), Ok(1));
    assert_eq!(net(&vm, &state, 0, 0), None, "since below coverage");
    upsert(&mut vm, &mut state, 0, 2, 20);
    upsert(&mut vm, &mut state, 0, 3, 30);
    assert_eq!(net(&vm, &state, 0, 1).unwrap().len(), 2);
    upsert(&mut vm, &mut state, 0, 4, 40);
    // The ring evicted the entry stamped 2: a since below it overflows.
    assert_eq!(vm.coverage(0), Some(2));
    assert_eq!(net(&vm, &state, 0, 1), None);
    assert_eq!(
        net(&vm, &state, 0, 2).unwrap(),
        [insert(3, 0), insert(4, 0)]
    );

    vm.watch(&state, 1, 64, 64).unwrap();
    assert_eq!(
        vm.set_insert(false, &mut state, 2, &[5], None),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[1], None),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.bitmap_algebra(
            false,
            BitmapAlgebraOp::Or,
            &mut state,
            1,
            BitmapSource::Slot(2)
        ),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[6], None),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, 1), 3);
    assert_eq!(
        net(&vm, &state, 1, 0),
        None,
        "a whole-slot entry in the window"
    );
    assert_eq!(net(&vm, &state, 1, 1), None);
    assert_eq!(net(&vm, &state, 1, 2).unwrap(), [insert(6, 0)]);
}

#[test]
fn a_refold_records_nothing_new_and_later_entries_are_ignored() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 1024).unwrap();
    let basis = state.clone();
    upsert(&mut vm, &mut state, 0, 1, 10);
    let one = state.clone();
    upsert(&mut vm, &mut state, 0, 2, 20);
    // The state one write in: the entry stamped 2 belongs to a later state.
    assert_eq!(net(&vm, &one, 0, 0).unwrap(), [insert(1, 0)]);
    // Rewind and refold the same writes: the log already holds them.
    let mut again = basis;
    upsert(&mut vm, &mut again, 0, 1, 10);
    upsert(&mut vm, &mut again, 0, 2, 20);
    assert_eq!(again, state);
    assert_eq!(
        net(&vm, &again, 0, 0).unwrap(),
        [insert(1, 0), insert(2, 0)]
    );
}

#[test]
fn log_settle_drops_entries_above_a_restored_sequence() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 1024).unwrap();
    upsert(&mut vm, &mut state, 0, 1, 10);
    let one = state.clone();
    upsert(&mut vm, &mut state, 0, 2, 20);
    // Abandon that future: restore the one-write state and write another.
    let mut state = one;
    vm.log_settle(&state);
    upsert(&mut vm, &mut state, 0, 3, 30);
    assert_eq!(
        net(&vm, &state, 0, 0).unwrap(),
        [insert(1, 0), insert(3, 0)]
    );
}

#[test]
fn a_delta_rollforward_overflows_and_its_rollback_settles() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 1024).unwrap();
    vm.undo_enable(&state).unwrap();
    let start = vm.undo_checkpoint();
    let result = vm.map_upsert(
        Strategy::Last,
        true,
        &mut state,
        0,
        &[1],
        &[10],
        None,
        CmpType::U32,
    );
    assert_eq!(result, ErrorCode::Ok);
    let end = vm.undo_checkpoint();
    vm.delta_export_segment(start, end).unwrap();
    let undo = vm.delta_export_undo().to_vec();
    let redo = vm.delta_export_redo().to_vec();
    vm.undo_commit();
    let entry = Vm::delta_export_entry_size();
    vm.delta_apply_rollback_segment(&mut state, &undo, entry)
        .unwrap();
    assert_eq!(seq(&state, 0), 0);
    assert_eq!(net(&vm, &state, 0, 0).unwrap(), []);
    vm.delta_apply_rollforward_segment(&mut state, &redo, entry)
        .unwrap();
    assert_eq!(seq(&state, 0), 1);
    assert_eq!(net(&vm, &state, 0, 0), None, "a write no site logged");
}

#[test]
fn a_cloned_log_answers_like_its_source_and_diverges_independently() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut source = Vm::default();
    source.watch(&state, 0, 64, 1024).unwrap();
    upsert(&mut source, &mut state, 0, 1, 10);
    let mut fork = Vm::default();
    let mut forked = state.clone();
    fork.log_clone_from(&source).expect("clone");
    assert_eq!(net(&fork, &forked, 0, 0), net(&source, &state, 0, 0));
    upsert(&mut source, &mut state, 0, 2, 20);
    upsert(&mut fork, &mut forked, 0, 3, 30);
    assert_eq!(
        net(&source, &state, 0, 0).unwrap(),
        [insert(1, 0), insert(2, 0)]
    );
    assert_eq!(
        net(&fork, &forked, 0, 0).unwrap(),
        [insert(1, 0), insert(3, 0)]
    );
}

#[test]
fn a_watched_slot_records_warm_passes_with_zero_allocations() {
    use StructFieldType::UInt32;
    let init = [
        slot_def(0, SlotType::HashMap),
        struct_map_def(1, Opcode::SlotStructMap, SlotType::StructMap, &[UInt32]),
    ]
    .concat();
    let code = program(
        2,
        &init,
        &[
            Opcode::BatchMapUpsertLast as u8,
            0,
            KEY,
            VALUE,
            Opcode::BatchStructMapUpsertLast as u8,
            1,
            KEY,
            1,
            VALUE,
            0,
            0,
        ],
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 32, 256).unwrap();
    vm.watch(&state, 1, 32, 1024).unwrap();
    let passes: [Batch; 2] = [1, 2].map(|value| {
        let rows: Vec<Row> = (0..8).map(|key| row(10 + key, 0, value)).collect();
        batch(&rows)
    });
    let cols: Vec<Vec<&[u8]>> = passes.iter().map(Batch::slices).collect();
    for pass in &cols {
        assert_eq!(OK, vm.execute_batch(&mut state, &code, pass, 8));
    }
    let mut results = [0u32; 16];
    with_allocations(0, || {
        for (index, result) in results.iter_mut().enumerate() {
            *result = vm.execute_batch(&mut state, &code, &cols[index % 2], 8);
        }
    });
    assert!(results.iter().all(|&result| result == OK), "{results:?}");
    // Every pass rewrote every row: the rings evicted, and the last pass's
    // eight changes are still answered.
    let since = seq(&state, 0) - 8;
    assert_eq!(net(&vm, &state, 0, since).unwrap().len(), 8);
    assert_eq!(net(&vm, &state, 1, seq(&state, 1) - 8).unwrap().len(), 8);
}

#[test]
fn a_constant_cardinality_xor_marks_and_logs_its_whole_slot() {
    let init = [slot_def(0, SlotType::Bitmap), slot_def(1, SlotType::Bitmap)].concat();
    let code = program(2, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    assert_eq!(
        vm.set_insert(false, &mut state, 0, &[1, 2], None),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[2, 3], None),
        ErrorCode::Ok
    );
    assert_eq!(vm.watch(&state, 0, 64, 64), Ok(1));
    // {1, 2} xor {2, 3} = {1, 3}: same size, different members.
    assert_eq!(
        vm.bitmap_algebra(
            false,
            BitmapAlgebraOp::Xor,
            &mut state,
            0,
            BitmapSource::Slot(1)
        ),
        ErrorCode::Ok
    );
    assert_eq!(seq(&state, 0), 2, "a membership change is a write");
    assert_eq!(
        net(&vm, &state, 0, 1),
        None,
        "the keyless write logs a whole-slot entry"
    );
}

#[test]
fn an_eviction_call_advances_once_and_logs_every_removed_key() {
    let code = program(1, &ttl_slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    let stamps = f64s_as_bytes(&[1.0, 1.0]).to_vec();
    assert_eq!(
        vm.map_upsert(
            Strategy::Last,
            false,
            &mut state,
            0,
            &[6, 7],
            &[60, 70],
            Some(stamps.as_slice()),
            CmpType::F64
        ),
        ErrorCode::Ok
    );
    let before = seq(&state, 0);
    vm.watch(&state, 0, 64, 1024).unwrap();
    assert_eq!(vm.evict_all_expired(false, &mut state, 1_000.0), Ok(2));
    assert_eq!(seq(&state, 0), before + 1, "one eviction call, one write");
    let mut gone = net(&vm, &state, 0, before).unwrap();
    gone.sort_unstable_by_key(|net| net.key1);
    assert_eq!(
        gone,
        [delete(6, 0, Some(&le(60))), delete(7, 0, Some(&le(70)))]
    );
}

#[test]
fn an_equal_nested_value_marks_and_logs_nothing() {
    let init = [
        Opcode::SlotNested as u8,
        0,
        SlotType::Nested as u8,
        8,
        0,
        SlotType::HashMap as u8,
        4,
        0,
        0,
    ];
    let code = program(
        1,
        &init,
        &for_each(&[Opcode::NestedMapUpsertLast as u8, 0, KEY, KEY2, VALUE]),
    );
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 64, 64).unwrap();
    assert_eq!(OK, run(&mut vm, &mut state, &code, &[row(1, 2, 5)]));
    let written = seq(&state, 0);
    assert_eq!(written, 1);
    let flags = (slot_meta_base(0) + SlotMetaOffset::CHANGE_FLAGS) as usize;
    state[flags] = 0;
    assert_eq!(OK, run(&mut vm, &mut state, &code, &[row(1, 2, 5)]));
    assert_eq!(state[flags], 0, "an equal value sets no flag");
    assert_eq!(seq(&state, 0), written, "an equal value advances nothing");
    assert_eq!(
        net(&vm, &state, 0, written).unwrap(),
        [],
        "and logs nothing"
    );
}

#[test]
fn a_log_clone_refuses_out_of_memory_and_keeps_its_log() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut source = Vm::default();
    source.watch(&state, 0, 64, 1024).unwrap();
    upsert(&mut source, &mut state, 0, 1, 10);
    let mut fork = Vm::default();
    assert_eq!(
        with_allocations(0, || fork.log_clone_from(&source)),
        Err(ErrorCode::OutOfMemory)
    );
    assert_eq!(
        net(&fork, &state, 0, 0),
        None,
        "the refused clone changed nothing"
    );
    assert_eq!(fork.log_clone_from(&source), Ok(()));
    assert_eq!(net(&fork, &state, 0, 0).unwrap(), [insert(1, 0)]);
}

/// `slot`'s net-change answer and records from `since`, as bytes.
fn net_bytes(
    vm: &Vm,
    state: &[u8],
    slot: u32,
    since: u64,
    strings: &(impl InternBytes + ?Sized),
) -> (NetAnswer, Vec<u8>) {
    let mut out = NetChanges::default();
    let answer = vm
        .net_changes(state, slot, since, strings, &mut out)
        .expect("net_changes");
    (answer, out.bytes().to_vec())
}

fn put_u32(record: &mut [u8], at: u32, value: u32) {
    record[at as usize..at as usize + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(record: &mut [u8], at: u32, value: u64) {
    record[at as usize..at as usize + 8].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn an_exported_log_imports_over_its_image_and_answers_alike() {
    let init = [
        slot_def(0, SlotType::HashMap),
        slot_def(1, SlotType::Bitmap),
    ]
    .concat();
    let code = program(2, &init, &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    upsert(&mut vm, &mut state, 0, 9, 90);
    assert_eq!(vm.watch(&state, 0, 16, 256), Ok(1));
    upsert(&mut vm, &mut state, 0, 1, 10);
    upsert(&mut vm, &mut state, 0, 9, 91);
    upsert(&mut vm, &mut state, 0, 2, 20);
    assert_eq!(vm.map_remove(false, &mut state, 0, &[1]), ErrorCode::Ok);
    upsert(&mut vm, &mut state, 0, 3, 30);
    assert_eq!(seq(&state, 0), 6);
    // A bitmap slot with a keyless write in its log.
    vm.watch(&state, 1, 16, 64).unwrap();
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[4, 5], None),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.bitmap_algebra(
            false,
            BitmapAlgebraOp::Or,
            &mut state,
            1,
            BitmapSource::Slot(1)
        ),
        ErrorCode::Ok
    );
    assert_eq!(
        vm.set_insert(false, &mut state, 1, &[6], None),
        ErrorCode::Ok
    );

    for above in [0, 3] {
        let mut record = Vec::new();
        vm.log_export(&state, 0, above, &mut record).unwrap();
        vm.log_export(&state, 1, above.min(1), &mut record).unwrap();
        let first = vm_record_len(&record);
        let mut fresh = Vm::default();
        let coverage = fresh.log_import(&state, 0, &record[..first]).unwrap();
        assert_eq!(coverage, above.max(1));
        assert_eq!(fresh.coverage(0), Some(coverage));
        fresh.log_import(&state, 1, &record[first..]).unwrap();
        for since in above.max(1)..=seq(&state, 0) {
            assert_eq!(
                net_bytes(&fresh, &state, 0, since, &NoStrings),
                net_bytes(&vm, &state, 0, since, &NoStrings),
                "since {since}, exported above {above}"
            );
        }
        for since in 1..=seq(&state, 1) {
            assert_eq!(
                net_bytes(&fresh, &state, 1, since, &NoStrings),
                net_bytes(&vm, &state, 1, since, &NoStrings),
                "bitmap since {since}"
            );
        }
        // The imported log keeps recording at the slot's next stamps.
        let mut next = state.clone();
        let mut warm = Vm::default();
        warm.log_import(&next, 0, &record[..first]).unwrap();
        upsert(&mut warm, &mut next, 0, 4, 40);
        assert_eq!(
            net_bytes(&warm, &next, 0, 6, &NoStrings).1.len(),
            NET_CHANGE_HEADER_SIZE as usize
        );
    }
}

/// The length of the first slot's record in `record`: its header and
/// entries.
fn vm_record_len(record: &[u8]) -> usize {
    let at = |offset: u32| u32::from_le_bytes(record[offset as usize..][..4].try_into().unwrap());
    let mut end = WATCH_LOG_HEADER_SIZE as usize;
    for _ in 0..at(WatchLogOffset::COUNT) {
        let prior = at(end as u32 + WatchLogEntryOffset::PRIOR_LEN) as usize;
        end = (end + WATCH_LOG_ENTRY_SIZE as usize + prior).next_multiple_of(8);
    }
    end
}

#[test]
fn an_export_with_an_older_image_holds_only_that_image_s_history() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    vm.watch(&state, 0, 16, 256).unwrap();
    upsert(&mut vm, &mut state, 0, 1, 10);
    let basis = state.clone();
    // The step's speculative write: past the basis image.
    upsert(&mut vm, &mut state, 0, 2, 20);
    let mut record = Vec::new();
    vm.log_export(&basis, 0, 0, &mut record).unwrap();
    let mut restored = Vm::default();
    assert_eq!(restored.log_import(&basis, 0, &record), Ok(0));
    assert_eq!(
        net_bytes(&restored, &basis, 0, 0, &NoStrings),
        net_bytes(&vm, &basis, 0, 0, &NoStrings)
    );
    // The full record names a stamp past the basis: refused.
    let mut ahead = Vec::new();
    vm.log_export(&state, 0, 0, &mut ahead).unwrap();
    assert_eq!(
        restored.log_import(&basis, 0, &ahead),
        Err(ErrorCode::InvalidState)
    );
    assert_eq!(
        net_bytes(&restored, &basis, 0, 0, &NoStrings),
        net_bytes(&vm, &basis, 0, 0, &NoStrings),
        "a refused import leaves the log as it was"
    );
}

#[test]
fn malformed_and_oversized_records_refuse() {
    let code = program(1, &slot_def(0, SlotType::HashMap), &[]);
    let mut state = image(&code);
    let mut vm = Vm::default();
    let mut none = Vec::new();
    assert_eq!(
        vm.log_export(&state, 0, 0, &mut none),
        Err(ErrorCode::InvalidSlot),
        "an unwatched slot has no log to export"
    );
    vm.watch(&state, 0, 4, 64).unwrap();
    upsert(&mut vm, &mut state, 0, 1, 10);
    upsert(&mut vm, &mut state, 0, 1, 11);
    let mut record = Vec::new();
    vm.log_export(&state, 0, 0, &mut record).unwrap();
    // An insert with no prior, then an update with its 4-byte value padded
    // to 8.
    let first = WATCH_LOG_HEADER_SIZE;
    let second = first + WATCH_LOG_ENTRY_SIZE;
    assert_eq!(record.len(), (second + WATCH_LOG_ENTRY_SIZE + 8) as usize);
    let refuse = |name: &str, edit: &dyn Fn(&mut Vec<u8>), code: ErrorCode| {
        let mut bad = record.clone();
        edit(&mut bad);
        let mut fresh = Vm::default();
        assert_eq!(fresh.log_import(&state, 0, &bad), Err(code), "{name}");
        assert!(
            !fresh.watched(0),
            "{name}: a refused import watches nothing"
        );
    };
    use ErrorCode::{InvalidSlot, InvalidState, OutOfMemory};
    refuse("truncated header", &|r| r.truncate(10), InvalidState);
    refuse(
        "truncated entry",
        &|r| r.truncate(r.len() - 1),
        InvalidState,
    );
    refuse("trailing byte", &|r| r.push(0), InvalidState);
    refuse(
        "no ring entries",
        &|r| put_u32(r, WatchLogOffset::ENTRIES, 0),
        InvalidState,
    );
    refuse(
        "count past the ring",
        &|r| put_u32(r, WatchLogOffset::ENTRIES, 1),
        InvalidState,
    );
    refuse(
        "priors past the images",
        &|r| put_u32(r, WatchLogOffset::IMAGE_BYTES, 3),
        InvalidState,
    );
    refuse("header padding", &|r| r[20] = 1, InvalidState);
    refuse(
        "entry padding",
        &|r| r[(first + 18) as usize] = 1,
        InvalidState,
    );
    refuse(
        "prior padding",
        &|r| r[(second + WATCH_LOG_ENTRY_SIZE + 5) as usize] = 1,
        InvalidState,
    );
    refuse(
        "coverage past the state",
        &|r| put_u64(r, WatchLogOffset::COVERAGE, 9),
        InvalidState,
    );
    refuse(
        "stamp past the state",
        &|r| put_u64(r, second + WatchLogEntryOffset::STAMP, 3),
        InvalidState,
    );
    refuse(
        "stamps out of order",
        &|r| {
            put_u64(r, first + WatchLogEntryOffset::STAMP, 2);
            put_u64(r, second + WatchLogEntryOffset::STAMP, 1);
        },
        InvalidState,
    );
    refuse(
        "stamp at coverage",
        &|r| put_u64(r, WatchLogOffset::COVERAGE, 1),
        InvalidState,
    );
    refuse(
        "unknown kind",
        &|r| r[(first + WatchLogEntryOffset::KIND) as usize] = 0x40,
        InvalidState,
    );
    refuse(
        "an update with no held key",
        &|r| r[(second + WatchLogEntryOffset::HELD) as usize] = 0,
        InvalidState,
    );
    refuse(
        "an insert with a prior",
        &|r| r[(second + WatchLogEntryOffset::KIND) as usize] = ChangeFlag::INSERTED,
        InvalidState,
    );
    refuse(
        "prior length past the record",
        &|r| put_u32(r, second + WatchLogEntryOffset::PRIOR_LEN, 64),
        InvalidState,
    );
    let mut fresh = Vm::default();
    assert_eq!(fresh.log_import(&state, 3, &record), Err(InvalidSlot));
    let mut huge = record.clone();
    put_u32(&mut huge, WatchLogOffset::ENTRIES, u32::MAX);
    assert_eq!(
        with_allocations(0, || fresh.log_import(&state, 0, &huge)),
        Err(OutOfMemory),
        "a ring the memory cannot back refuses before anything changes"
    );
    assert!(!fresh.watched(0));
    // An import over a watched slot replaces its log.
    fresh.watch(&state, 0, 1, 0).unwrap();
    assert_eq!(fresh.log_import(&state, 0, &record), Ok(0));
    assert_eq!(
        net_bytes(&fresh, &state, 0, 0, &NoStrings),
        net_bytes(&vm, &state, 0, 0, &NoStrings)
    );
    let mut empty = Vec::new();
    assert_eq!(
        with_allocations(0, || vm.log_export(&state, 0, 0, &mut empty)),
        Err(OutOfMemory)
    );
}

#[test]
fn a_struct_row_image_with_text_array_and_value_object_round_trips() {
    use StructFieldType::{ArrayString, ArrayU32, String as Text, UInt32, Vo};
    let fields = [UInt32, Text, ArrayU32, ArrayString, Vo];
    let init = struct_map_def(0, Opcode::SlotStructMap, SlotType::StructMap, &fields);
    let remove = program(1, &init, &[Opcode::BatchStructMapRemove as u8, 0, KEY]);
    let mut state = image(&remove);
    let mut strings = Strings::default();
    let logical = LogicalSlot {
        slot: 0,
        kind: SlotType::StructMap,
        agg_type: 0,
        field_types: fields.to_vec(),
        rows: vec![LogicalRow {
            key: LogicalValue::U32(10),
            key2: None,
            fields: vec![
                Some(LogicalValue::U32(7)),
                Some(LogicalValue::Text("tier".into())),
                Some(LogicalValue::Array(vec![
                    LogicalValue::U32(1),
                    LogicalValue::U32(2),
                ])),
                Some(LogicalValue::Array(vec![
                    LogicalValue::Text("a".into()),
                    LogicalValue::Text("bc".into()),
                ])),
                Some(LogicalValue::Bytes(vec![9, 8, 7])),
            ],
            cmp_ts: None,
        }],
        aggregate: None,
        scalar: None,
        nested: Vec::new(),
        eviction: Vec::new(),
        evicted: Vec::new(),
        condition: None,
    };
    import_slot(&mut state, &logical, &SlotEncoding::RAW, &mut strings).expect("import");
    let mut vm = Vm::default();
    vm.watch(&state, 0, 8, 4096).unwrap();
    let rows = batch(&[row(10, 0, 0)]);
    assert_eq!(
        OK,
        vm.execute_batch_in(&mut state, &remove, &rows.slices(), rows.rows, &strings)
    );
    let mut record = Vec::new();
    vm.log_export(&state, 0, 0, &mut record).unwrap();
    // The interner releases every id: the imported image still decodes.
    strings.texts.iter_mut().for_each(|text| *text = None);
    let mut restored = Vm::default();
    restored.log_import(&state, 0, &record).unwrap();
    let (answer, bytes) = net_bytes(&restored, &state, 0, 0, &strings);
    assert_eq!(answer, NetAnswer::Changes);
    assert_eq!(bytes, net_bytes(&vm, &state, 0, 0, &strings).1);
    let mut out = NetChanges::default();
    restored
        .net_changes(&state, 0, 0, &strings, &mut out)
        .unwrap();
    let changes = decode(&out);
    assert_eq!(
        (changes[0].key1, changes[0].kind),
        (10, NetChangeKind::DELETE)
    );
    let map = columine_vm::struct_map::StructMapSlot::bind(&state, 0);
    let mut array_text = Vec::new();
    for text in ["a", "bc"] {
        array_text.extend((text.len() as u32).to_le_bytes());
        array_text.extend(text.as_bytes());
    }
    assert_eq!(
        image_payloads(changes[0].prior.as_ref().unwrap(), map.row_size as usize),
        [
            b"tier".as_slice(),
            &[le(1), le(2)].concat(),
            &array_text,
            &[9, 8, 7]
        ]
    );
}
