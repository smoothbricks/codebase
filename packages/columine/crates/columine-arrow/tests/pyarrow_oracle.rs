//! Cross-implementation check: pyarrow must agree with us about the planes
//! whose values a width check cannot verify.
//!
//! Adapted from the `lmao-arrow` pyarrow oracle. Reading our stream back with
//! `arrow-ipc` proves we are self-consistent; it does not prove we are right,
//! because both sides are the same Rust implementation of the format. An
//! unsigned plane that emitted its bytes as signed would round-trip perfectly
//! through `arrow-ipc` and still be wrong, so the values that distinguish the
//! new planes are checked against an implementation that shares no code with
//! ours.
//!
//! The development shell provisions pyarrow for exactly this, so a missing
//! interpreter is a broken test environment rather than a reason to skip.

use std::io::Write;
use std::process::Command;

use arrow_schema::{DataType, Field};
use columine_arrow::{
    ArrowType, DynamicColumns, DynamicSchemaConfig, MetadataLimits, MetadataStorage,
    SignalSchemaField, logical_schema_ipc_bytes, write_arrow_ipc_from_dynamic_columns,
};

fn require_pyarrow() {
    let probe = Command::new("python3")
        .args(["-c", "import pyarrow"])
        .output()
        .expect("python3 must be available for the mandatory pyarrow oracle");
    assert!(
        probe.status.success(),
        "pyarrow must be installed for the mandatory IPC oracle: {}",
        String::from_utf8_lossy(&probe.stderr)
    );
}

/// Emit an Arrow IPC stream for `fields`/`logical`, write it to a temp file,
/// and return the path.
fn write_stream(
    name: &str,
    fields: &[SignalSchemaField],
    logical: Vec<Field>,
    rows: u32,
    write: impl FnOnce(&mut DynamicColumns),
) -> std::path::PathBuf {
    let schema_bytes = logical_schema_ipc_bytes(logical).expect("encode schema message");
    let config = DynamicSchemaConfig::new(&schema_bytes, fields).expect("validate schema");
    let mut columns = DynamicColumns::new(fields, rows);
    write(&mut columns);
    let mut metadata =
        MetadataStorage::for_fields(fields, MetadataLimits::default()).expect("size metadata");
    let mut output = vec![0u8; 1 << 16];
    let written =
        write_arrow_ipc_from_dynamic_columns(&columns, &config, &mut output, &mut metadata)
            .expect("emit arrow ipc");

    let dir = std::env::temp_dir().join("columine-pyarrow-oracle");
    std::fs::create_dir_all(&dir).expect("create oracle directory");
    let path = dir.join(format!("{name}.arrow"));
    let mut file = std::fs::File::create(&path).expect("create stream file");
    file.write_all(&output[..written])
        .expect("write stream bytes");
    path
}

/// Run `script` with the stream path bound to `PATH`, and return its stdout.
fn pyarrow_says(path: &std::path::Path, script: &str) -> String {
    let program = format!(
        "import pyarrow.ipc as ipc\ntable = ipc.open_stream('{}').read_all()\n{script}",
        path.display()
    );
    let out = Command::new("python3")
        .args(["-c", &program])
        .output()
        .expect("run pyarrow");
    assert!(
        out.status.success(),
        "pyarrow failed to read our stream: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .expect("pyarrow stdout is utf8")
        .trim()
        .to_owned()
}

#[test]
fn pyarrow_agrees_on_the_unsigned_planes() {
    require_pyarrow();
    let fields = [
        SignalSchemaField::new(ArrowType::UInt8, true),
        SignalSchemaField::new(ArrowType::UInt16, true),
        SignalSchemaField::new(ArrowType::UInt32, true),
        SignalSchemaField::new(ArrowType::UInt64, true),
        // Alongside them, the signed plane of the same width reading the same
        // bit pattern: this is the pair that a width check cannot tell apart.
        SignalSchemaField::new(ArrowType::Int32, true),
    ];
    let logical = vec![
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("i32", DataType::Int32, true),
    ];
    let path = write_stream("unsigned", &fields, logical, 1, |columns| {
        assert!(columns.begin_row());
        columns.append_uint(0, 255).unwrap();
        columns.append_uint(1, 65_535).unwrap();
        columns.append_uint(2, 0xFFFF_FFFF).unwrap();
        columns.append_uint(3, u64::MAX).unwrap();
        columns.append_int(4, -1).unwrap();
        columns.end_row();
    });

    // Schema first: pyarrow must see the logical types we declared, not just
    // buffers of the right size.
    assert_eq!(
        pyarrow_says(&path, "print(','.join(str(f.type) for f in table.schema))"),
        "uint8,uint16,uint32,uint64,int32"
    );
    // Then the values. 0xFFFFFFFF is 4294967295 on the unsigned plane and -1
    // on the signed one, from the identical four bytes.
    assert_eq!(
        pyarrow_says(
            &path,
            "print(','.join(str(table.column(i)[0].as_py()) for i in range(table.num_columns)))"
        ),
        "255,65535,4294967295,18446744073709551615,-1"
    );
}

#[test]
fn pyarrow_agrees_on_the_decimal_planes() {
    require_pyarrow();
    let fields = [
        SignalSchemaField::new(ArrowType::Decimal128, true),
        SignalSchemaField::new(ArrowType::Decimal256, true),
    ];
    // Scale 2, so a wrong byte order or a mis-signed high half shows up as a
    // visibly different decimal rather than as a plausible one.
    let logical = vec![
        Field::new("d128", DataType::Decimal128(38, 2), true),
        Field::new("d256", DataType::Decimal256(76, 2), true),
    ];

    // 12345 at scale 2 is 123.45; the negative row exercises two's-complement
    // sign extension across both widths.
    let mut negative_256 = [0xFFu8; 32];
    negative_256[..16].copy_from_slice(&(-6789i128).to_le_bytes());
    let path = write_stream("decimal", &fields, logical, 2, |columns| {
        assert!(columns.begin_row());
        columns
            .append_fixed_bytes(0, &12_345i128.to_le_bytes())
            .unwrap();
        columns
            .append_fixed_bytes(1, &{
                let mut wide = [0u8; 32];
                wide[..16].copy_from_slice(&12_345i128.to_le_bytes());
                wide
            })
            .unwrap();
        columns.end_row();

        assert!(columns.begin_row());
        columns
            .append_fixed_bytes(0, &(-6789i128).to_le_bytes())
            .unwrap();
        columns.append_fixed_bytes(1, &negative_256).unwrap();
        columns.end_row();
    });

    assert_eq!(
        pyarrow_says(&path, "print(','.join(str(f.type) for f in table.schema))"),
        "decimal128(38, 2),decimal256(76, 2)"
    );
    assert_eq!(
        pyarrow_says(
            &path,
            "print(';'.join(','.join(str(table.column(i)[r].as_py()) for i in range(table.num_columns)) for r in range(table.num_rows)))"
        ),
        "123.45,123.45;-67.89,-67.89"
    );
}

#[test]
fn pyarrow_agrees_on_the_remaining_new_planes() {
    require_pyarrow();
    let fields = [
        SignalSchemaField::new(ArrowType::Int8, true),
        SignalSchemaField::new(ArrowType::Int16, true),
        SignalSchemaField::new(ArrowType::Float16, true),
        SignalSchemaField::new(ArrowType::Float32, true),
        SignalSchemaField::new(ArrowType::LargeUtf8, true),
        SignalSchemaField::new(ArrowType::LargeBinary, true),
        SignalSchemaField::new_fixed_size_binary(4, true),
        SignalSchemaField::new(ArrowType::IntervalYearMonth, true),
    ];
    let logical = vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("f16", DataType::Float16, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("large_utf8", DataType::LargeUtf8, true),
        Field::new("large_binary", DataType::LargeBinary, true),
        Field::new("fsb", DataType::FixedSizeBinary(4), true),
        Field::new(
            "ym",
            DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
            true,
        ),
    ];
    let path = write_stream("remaining", &fields, logical, 1, |columns| {
        assert!(columns.begin_row());
        columns.append_int(0, -128).unwrap();
        columns.append_int(1, -32_768).unwrap();
        columns.append_float(2, 1.5).unwrap();
        columns.append_float(3, -2.25).unwrap();
        columns.append_variable(4, "αβ".as_bytes()).unwrap();
        columns.append_variable(5, &[0x00, 0xFF]).unwrap();
        columns
            .append_fixed_bytes(6, &[0xDE, 0xAD, 0xBE, 0xEF])
            .unwrap();
        columns.append_int(7, -13).unwrap();
        columns.end_row();
    });

    assert_eq!(
        pyarrow_says(&path, "print(','.join(str(f.type) for f in table.schema))"),
        "int8,int16,halffloat,float,large_string,large_binary,fixed_size_binary[4],month_interval"
    );
    // The first seven planes convert straight to Python values.
    assert_eq!(
        pyarrow_says(
            &path,
            "print(','.join(repr(table.column(i)[0].as_py()) for i in range(7)))"
        ),
        "-128,-32768,1.5,-2.25,'αβ',b'\\x00\\xff',b'\\xde\\xad\\xbe\\xef'"
    );
    // pyarrow 24 has no Python binding for `month_interval` values at all:
    // wrapping the array raises `KeyError: 21`, wrapping a scalar raises
    // `NotImplementedError`, and casting to int32 is unimplemented. What it
    // DOES do is decode the type and validate every buffer length against
    // every field node while reading the table, which is asserted above and
    // by the row count here. The interval VALUES are asserted in
    // `plane_round_trip::interval_planes_round_trip_each_unit`; no
    // independent reader available in this repo can read them.
    assert_eq!(pyarrow_says(&path, "print(table.num_rows)"), "1");
}
