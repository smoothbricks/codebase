//! A struct-map row's field is present only when its source cell is valid in
//! the column's Arrow validity bitmap: a column's null is a clear validity
//! bit, which the producer writes and every reader checks, and a field whose
//! presence bit is clear is absent. The batch runs through the VM's execute
//! entry; the row is read through its presence bits and cells.

use columine_types::PROGRAM_MAGIC;
use columine_types::types::ErrorCode;
use columine_vm::bytes;
use columine_vm::key_index::NoStrings;
use columine_vm::state_init::{
    DEFAULT_ACCEPTED_PROGRAM_MAGICS, bind_struct_map, calculate_state_size, init_state,
};
use columine_vm::struct_map::{StructMapSlot, Validity};
use columine_vm::vm::{CheckedStructMap, Vm, f64s_as_bytes, i64s_as_bytes, u32s_as_bytes};

const OK: u32 = ErrorCode::Ok as u32;
const COLUMN_UNDERRUN: u32 = ErrorCode::ColumnUnderrun as u32;
const UINT32: u8 = 0;
const INT64: u8 = 1;
const FLOAT64: u8 = 2;
const BOOL: u8 = 3;
/// The record's fields by row position, each filled from column position + 1
/// (column 0 is the key).
const FIELDS: [u8; 4] = [FLOAT64, INT64, BOOL, UINT32];
const KEY: u32 = 7;

/// One struct map of `FIELDS` under a u32 key, and one 0x80 upsert of every
/// field from its own column.
fn program() -> Vec<u8> {
    let mut init = vec![0x18, 0, 6, 16, 0, 4];
    init.extend_from_slice(&FIELDS);
    init.push(0);
    let reduce = vec![0x80, 0, 0, 4, 1, 0, 2, 1, 3, 2, 4, 3, 0];
    let mut program = vec![0u8; 32];
    program.extend(PROGRAM_MAGIC.to_le_bytes());
    program.extend([2, 0, 0, 0]);
    program.extend(1u32.to_le_bytes());
    program.extend(5u32.to_le_bytes());
    program.extend(
        u32::try_from(init.len())
            .expect("init fits u32")
            .to_le_bytes(),
    );
    program.extend(
        u32::try_from(reduce.len())
            .expect("reduce fits u32")
            .to_le_bytes(),
    );
    program.extend(init);
    program.extend(reduce);
    program
}

fn state(program: &[u8]) -> Vec<u8> {
    let size = calculate_state_size(program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("admitted");
    let mut state = vec![0u8; usize::try_from(size).expect("size fits usize")];
    init_state(&mut state, program, DEFAULT_ACCEPTED_PROGRAM_MAGICS).expect("init");
    state
}

/// The present fields of `KEY`'s row, each as its stored bits, `None` where
/// its presence bit is clear; with whether an absent field's cell is zero.
fn stored(state: &[u8]) -> ([Option<u64>; 4], bool) {
    let row = CheckedStructMap::bind(state, 0)
        .expect("struct geometry")
        .row(KEY)
        .expect("the key holds a row");
    let map = bind_struct_map(state, 0).expect("struct map slot");
    let mut fields = [None; 4];
    let mut absent_cells_zero = true;
    for (field, kind) in FIELDS.iter().enumerate() {
        let idx = u8::try_from(field).expect("field fits u8");
        let at = (row + map.field_offset(state, idx)) as usize;
        let cell = match *kind {
            FLOAT64 | INT64 => bytes::read_u64(state, at as u32),
            BOOL => u64::from(state[at]),
            _ => u64::from(bytes::read_u32(state, at as u32)),
        };
        if StructMapSlot::is_field_set(state, row, idx) {
            fields[field] = Some(cell);
        } else {
            absent_cells_zero &= cell == 0;
        }
    }
    (fields, absent_cells_zero)
}

/// Run one row for `KEY` holding `(f64, i64, bool, u32)` under `validity`.
fn run(
    vm: &mut Vm,
    state: &mut [u8],
    program: &[u8],
    row: (f64, i64, u32, u32),
    validity: Validity<'_>,
) -> u32 {
    let (keys, floats, ints, flags, counts) = ([KEY], [row.0], [row.1], [row.2], [row.3]);
    let cols = [
        u32s_as_bytes(&keys),
        f64s_as_bytes(&floats),
        i64s_as_bytes(&ints),
        u32s_as_bytes(&flags),
        u32s_as_bytes(&counts),
    ];
    vm.execute_batch_in(state, program, &cols, validity, 1, &NoStrings)
}

const NULL: &[u8] = &[0];

#[test]
fn a_null_source_cell_leaves_its_field_absent() {
    let program = program();
    let mut state = state(&program);
    let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    // The f64, i64 and bool columns are null on the row; the u32 column has no
    // bitmap, so its cell is valid.
    let validity = [None, Some(NULL), Some(NULL), Some(NULL)];
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            (0.0, 0, 0, 5),
            Validity(&validity)
        ),
        OK
    );
    assert_eq!(stored(&state), ([None, None, None, Some(5)], true));
}

#[test]
fn a_later_null_row_clears_the_field_a_present_row_set() {
    let program = program();
    let mut state = state(&program);
    let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            (2.5, -3, 0, 5),
            Validity::ALL_VALID
        ),
        OK
    );
    assert_eq!(
        stored(&state),
        (
            [
                Some(2.5f64.to_bits()),
                Some((-3i64).cast_unsigned()),
                Some(0),
                Some(5)
            ],
            true
        )
    );
    let validity = [None, Some(NULL), Some(NULL), Some(NULL)];
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            (9.0, 9, 1, 6),
            Validity(&validity)
        ),
        OK
    );
    assert_eq!(stored(&state), ([None, None, None, Some(6)], true));
}

#[test]
fn a_valid_cell_under_a_bitmap_is_present() {
    let program = program();
    let mut state = state(&program);
    let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    let valid: &[u8] = &[1];
    let validity = [None, Some(valid), Some(NULL), Some(valid)];
    assert_eq!(
        run(
            &mut vm,
            &mut state,
            &program,
            (1.5, 4, 1, 5),
            Validity(&validity)
        ),
        OK
    );
    assert_eq!(
        stored(&state),
        ([Some(1.5f64.to_bits()), None, Some(1), Some(5)], true)
    );
}

#[test]
fn a_bitmap_short_of_the_cell_is_a_column_underrun() {
    let program = program();
    let mut state = state(&program);
    let mut vm = Vm::new(DEFAULT_ACCEPTED_PROGRAM_MAGICS);
    let keys: Vec<u32> = (0..9).collect();
    let floats = [1.0f64; 9];
    let ints = [1i64; 9];
    let flags = [1u32; 9];
    let cols = [
        u32s_as_bytes(&keys),
        f64s_as_bytes(&floats),
        i64s_as_bytes(&ints),
        u32s_as_bytes(&flags),
        u32s_as_bytes(&flags),
    ];
    // Nine cells need two bitmap bytes; one reaches only the first eight.
    let short: &[u8] = &[0xff];
    let validity = [None, Some(short)];
    assert_eq!(
        vm.execute_batch_in(
            &mut state,
            &program,
            &cols,
            Validity(&validity),
            9,
            &NoStrings
        ),
        COLUMN_UNDERRUN
    );
}
