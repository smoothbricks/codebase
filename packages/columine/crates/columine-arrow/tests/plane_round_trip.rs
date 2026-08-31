//! Value-level round trips for every Arrow plane.
//!
//! Each test declares a schema, writes cells through the real typed appends,
//! emits Arrow IPC, and reads it back with `arrow-ipc`'s own StreamReader. The
//! assertions are on VALUES, not on shapes: a plane that emits a well-formed
//! stream carrying the wrong number has not round-tripped.
//!
//! The values chosen are the boundaries that distinguish planes of equal
//! width, because equal width is exactly where a physical-tag mistake hides.

use std::io::Cursor;

use arrow_array::{
    Array, Date32Array, Date64Array, Decimal128Array, Decimal256Array, DurationNanosecondArray,
    FixedSizeBinaryArray, Float16Array, Float32Array, Int8Array, Int16Array, Int32Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeStringArray, RecordBatch, Time32MillisecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use columine_arrow::{
    ArrowType, DynamicColumns, DynamicSchemaConfig, MetadataLimits, MetadataStorage,
    SignalSchemaField, logical_schema_ipc_bytes, write_arrow_ipc_from_dynamic_columns,
};

/// Write `rows` rows through the typed append surface, emit Arrow IPC, and
/// hand back the batch an independent Arrow reader decoded from those bytes.
fn round_trip(
    fields: &[SignalSchemaField],
    logical: Vec<Field>,
    rows: u32,
    write: impl FnOnce(&mut DynamicColumns),
) -> RecordBatch {
    let schema_bytes = logical_schema_ipc_bytes(logical).expect("encode schema message");
    let config = DynamicSchemaConfig::new(&schema_bytes, fields).expect("validate schema");

    let mut columns = DynamicColumns::new(fields, rows);
    write(&mut columns);
    assert_eq!(columns.count, rows, "producer wrote the wrong row count");

    let mut metadata =
        MetadataStorage::for_fields(fields, MetadataLimits::default()).expect("size metadata");
    let mut output = vec![0u8; 1 << 16];
    let written =
        write_arrow_ipc_from_dynamic_columns(&columns, &config, &mut output, &mut metadata)
            .expect("emit arrow ipc");

    let mut reader =
        StreamReader::try_new(Cursor::new(&output[..written]), None).expect("read arrow ipc");
    let batch = reader
        .next()
        .expect("one record batch")
        .expect("valid batch");
    assert_eq!(batch.num_rows(), rows as usize);
    batch
}

/// One column of `field`, named `name` with logical type `logical`.
fn single(
    field: SignalSchemaField,
    name: &str,
    logical: DataType,
    rows: u32,
    write: impl FnOnce(&mut DynamicColumns),
) -> RecordBatch {
    round_trip(
        &[field],
        vec![Field::new(name, logical, field.is_nullable())],
        rows,
        write,
    )
}

fn plane(arrow_type: ArrowType) -> SignalSchemaField {
    SignalSchemaField::new(arrow_type, true)
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> &T {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| {
            panic!(
                "column {index} is {:?}, not the expected array type",
                batch.schema().field(index).data_type()
            )
        })
}

/// Append one value per row through `append`, ending each row.
fn rows_of<T: Copy>(
    columns: &mut DynamicColumns,
    values: &[T],
    mut append: impl FnMut(&mut DynamicColumns, T),
) {
    for value in values {
        assert!(columns.begin_row());
        append(columns, *value);
        columns.end_row();
    }
}

#[test]
fn signed_integer_planes_round_trip_their_boundaries() {
    let values = [i64::from(i8::MIN), -1, 0, i64::from(i8::MAX)];
    let batch = single(plane(ArrowType::Int8), "i8", DataType::Int8, 4, |columns| {
        rows_of(columns, &values, |c, v| c.append_int(0, v).unwrap());
    });
    assert_eq!(
        column::<Int8Array>(&batch, 0).values(),
        &[i8::MIN, -1, 0, i8::MAX]
    );

    let values = [i64::from(i16::MIN), -1, i64::from(i16::MAX)];
    let batch = single(
        plane(ArrowType::Int16),
        "i16",
        DataType::Int16,
        3,
        |columns| {
            rows_of(columns, &values, |c, v| c.append_int(0, v).unwrap());
        },
    );
    assert_eq!(
        column::<Int16Array>(&batch, 0).values(),
        &[i16::MIN, -1, i16::MAX]
    );

    // The four-byte SIGNED plane must still read -1 as -1. The identical bit
    // pattern on the unsigned plane below reads 4294967295, and that pair is
    // the whole reason the two planes are not one.
    let values = [-1i64, i64::from(i32::MIN), i64::from(i32::MAX)];
    let batch = single(
        plane(ArrowType::Int32),
        "i32",
        DataType::Int32,
        3,
        |columns| {
            rows_of(columns, &values, |c, v| c.append_int(0, v).unwrap());
        },
    );
    assert_eq!(
        column::<Int32Array>(&batch, 0).values(),
        &[-1, i32::MIN, i32::MAX]
    );
}

#[test]
fn unsigned_integer_planes_round_trip_the_upper_half() {
    let batch = single(
        plane(ArrowType::UInt8),
        "u8",
        DataType::UInt8,
        2,
        |columns| {
            rows_of(columns, &[0u64, 255], |c, v| c.append_uint(0, v).unwrap());
        },
    );
    assert_eq!(column::<UInt8Array>(&batch, 0).values(), &[0, 255]);

    let batch = single(
        plane(ArrowType::UInt16),
        "u16",
        DataType::UInt16,
        2,
        |columns| {
            rows_of(columns, &[0u64, 65_535], |c, v| {
                c.append_uint(0, v).unwrap()
            });
        },
    );
    assert_eq!(column::<UInt16Array>(&batch, 0).values(), &[0, 65_535]);

    // 0xFFFF_FFFF is 4294967295 here and -1 on the signed plane. This is the
    // assertion the old single four-byte plane could not make.
    let batch = single(
        plane(ArrowType::UInt32),
        "u32",
        DataType::UInt32,
        2,
        |columns| {
            rows_of(columns, &[0u64, 0xFFFF_FFFF], |c, v| {
                c.append_uint(0, v).unwrap()
            });
        },
    );
    let u32s = column::<UInt32Array>(&batch, 0);
    assert_eq!(u32s.values(), &[0, 0xFFFF_FFFF]);
    assert_eq!(u32s.value(1), 4_294_967_295);

    let batch = single(
        plane(ArrowType::UInt64),
        "u64",
        DataType::UInt64,
        2,
        |columns| {
            rows_of(columns, &[0u64, u64::MAX], |c, v| {
                c.append_uint(0, v).unwrap()
            });
        },
    );
    let u64s = column::<UInt64Array>(&batch, 0);
    assert_eq!(u64s.values(), &[0, u64::MAX]);
    assert_eq!(u64s.value(1), 18_446_744_073_709_551_615);
}

#[test]
fn unsigned_planes_reject_a_value_wider_than_the_plane() {
    let fields = [plane(ArrowType::UInt8)];
    let mut columns = DynamicColumns::new(&fields, 1);
    assert!(columns.begin_row());
    // 256 does not fit a byte. Storing it as 0 would be a silently wrong
    // number, so the append fails instead.
    assert!(columns.append_uint(0, 256).is_err());
    // A negative value cannot reach an unsigned plane at all: the carrier is
    // `u64`, and the signed append rejects the plane.
    assert!(columns.append_int(0, -1).is_err());
}

#[test]
fn float_planes_round_trip_at_their_own_precision() {
    let batch = single(
        plane(ArrowType::Float32),
        "f32",
        DataType::Float32,
        3,
        |columns| {
            rows_of(
                columns,
                &[1.5f64, f64::from(f32::MIN), f64::INFINITY],
                |c, v| c.append_float(0, v).unwrap(),
            );
        },
    );
    let f32s = column::<Float32Array>(&batch, 0);
    assert_eq!(f32s.value(0), 1.5);
    assert_eq!(f32s.value(1), f32::MIN);
    assert!(f32s.value(2).is_infinite());

    // `binary16` holds 1.5 and -0.0009765625 (2^-10) exactly; 65520.0 rounds
    // to infinity because it is past the largest half, and 1e-8 rounds to zero
    // because it is below the smallest subnormal.
    let batch = single(
        plane(ArrowType::Float16),
        "f16",
        DataType::Float16,
        5,
        |columns| {
            rows_of(
                columns,
                &[1.5f64, -0.0009765625, 65520.0, 1e-8, 2048.0],
                |c, v| c.append_float(0, v).unwrap(),
            );
        },
    );
    let f16s = column::<Float16Array>(&batch, 0);
    assert_eq!(f16s.value(0).to_f32(), 1.5);
    assert_eq!(f16s.value(1).to_f32(), -0.0009765625);
    assert!(f16s.value(2).to_f32().is_infinite());
    assert_eq!(f16s.value(3).to_f32(), 0.0);
    assert_eq!(f16s.value(4).to_f32(), 2048.0);
}

#[test]
fn decimal_planes_round_trip_full_width_values() {
    // Scale 0 so the read-back value is the plain integer, and the widest
    // precision each plane admits.
    let values = [
        99_999_999_999_999_999_999_999_999_999_999_999_999i128,
        -1,
        0,
    ];
    let batch = single(
        plane(ArrowType::Decimal128),
        "d128",
        DataType::Decimal128(38, 0),
        3,
        |columns| {
            for value in values {
                assert!(columns.begin_row());
                columns
                    .append_fixed_bytes(0, &value.to_le_bytes())
                    .expect("sixteen little-endian bytes");
                columns.end_row();
            }
        },
    );
    let decimals = column::<Decimal128Array>(&batch, 0);
    assert_eq!(decimals.value(0), values[0]);
    assert_eq!(decimals.value(1), -1);
    assert_eq!(decimals.value(2), 0);

    // Decimal256 is thirty-two little-endian bytes; sign-extend the high half
    // by hand so the negative case is a real 256-bit negative and not a
    // positive number with zeroed padding.
    let mut wide = [0u8; 32];
    wide[..16].copy_from_slice(&123_456_789_i128.to_le_bytes());
    let mut negative = [0xFFu8; 32];
    negative[..16].copy_from_slice(&(-42i128).to_le_bytes());
    let batch = single(
        plane(ArrowType::Decimal256),
        "d256",
        DataType::Decimal256(76, 0),
        2,
        |columns| {
            for value in [wide, negative] {
                assert!(columns.begin_row());
                columns
                    .append_fixed_bytes(0, &value)
                    .expect("thirty-two little-endian bytes");
                columns.end_row();
            }
        },
    );
    let decimals = column::<Decimal256Array>(&batch, 0);
    assert_eq!(decimals.value_as_string(0), "123456789");
    assert_eq!(decimals.value_as_string(1), "-42");
}

#[test]
fn large_offset_planes_round_trip_text_and_bytes() {
    let batch = single(
        plane(ArrowType::LargeUtf8),
        "large_utf8",
        DataType::LargeUtf8,
        3,
        |columns| {
            for text in ["α", "", "second value"] {
                assert!(columns.begin_row());
                columns.append_variable(0, text.as_bytes()).unwrap();
                columns.end_row();
            }
        },
    );
    let strings = column::<LargeStringArray>(&batch, 0);
    assert_eq!(strings.value(0), "α");
    assert_eq!(strings.value(1), "");
    assert_eq!(strings.value(2), "second value");
    // 64-bit offsets: n+1 entries of eight bytes each.
    assert_eq!(strings.value_offsets(), &[0i64, 2, 2, 14]);

    let batch = single(
        plane(ArrowType::LargeBinary),
        "large_binary",
        DataType::LargeBinary,
        2,
        |columns| {
            for bytes in [&[0u8, 0xFF][..], &[][..]] {
                assert!(columns.begin_row());
                columns.append_variable(0, bytes).unwrap();
                columns.end_row();
            }
        },
    );
    let binary = column::<LargeBinaryArray>(&batch, 0);
    assert_eq!(binary.value(0), &[0, 0xFF]);
    assert_eq!(binary.value(1), &[] as &[u8]);
}

#[test]
fn fixed_size_binary_round_trips_at_its_declared_width() {
    let field = SignalSchemaField::new_fixed_size_binary(16, true);
    let uuid = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];
    let batch = single(field, "fsb", DataType::FixedSizeBinary(16), 2, |columns| {
        for value in [uuid, [0u8; 16]] {
            assert!(columns.begin_row());
            columns.append_fixed_bytes(0, &value).unwrap();
            columns.end_row();
        }
    });
    let fixed = column::<FixedSizeBinaryArray>(&batch, 0);
    assert_eq!(fixed.value(0), &uuid);
    assert_eq!(fixed.value(1), &[0u8; 16]);

    // A short or long cell is a different value, not a padded one.
    let mut columns = DynamicColumns::new(&[field], 1);
    assert!(columns.begin_row());
    assert!(columns.append_fixed_bytes(0, &[0u8; 15]).is_err());
    assert!(columns.append_fixed_bytes(0, &[0u8; 17]).is_err());
}

#[test]
fn fixed_size_binary_width_must_agree_with_the_logical_type() {
    let field = SignalSchemaField::new_fixed_size_binary(16, true);
    let bytes =
        logical_schema_ipc_bytes(vec![Field::new("fsb", DataType::FixedSizeBinary(32), true)])
            .unwrap();
    // A 32-byte logical type over a 16-byte plane would read past every row.
    assert!(DynamicSchemaConfig::new(&bytes, &[field]).is_err());
}

#[test]
fn fixed_size_binary_width_is_bounded_by_the_variable_width_budget() {
    let ok =
        SignalSchemaField::new_fixed_size_binary(columine_arrow::MAX_FIXED_SIZE_BINARY_WIDTH, true);
    let bytes = logical_schema_ipc_bytes(vec![Field::new(
        "fsb",
        DataType::FixedSizeBinary(i32::from(columine_arrow::MAX_FIXED_SIZE_BINARY_WIDTH)),
        true,
    )])
    .unwrap();
    assert!(DynamicSchemaConfig::new(&bytes, &[ok]).is_ok());

    for width in [0, columine_arrow::MAX_FIXED_SIZE_BINARY_WIDTH + 1] {
        let field = SignalSchemaField::new_fixed_size_binary(width, true);
        let bytes = logical_schema_ipc_bytes(vec![Field::new(
            "fsb",
            DataType::FixedSizeBinary(i32::from(width)),
            true,
        )])
        .unwrap();
        assert!(
            DynamicSchemaConfig::new(&bytes, &[field]).is_err(),
            "width {width} must be rejected"
        );
    }
}

#[test]
fn interval_planes_round_trip_each_unit() {
    let batch = single(
        plane(ArrowType::IntervalYearMonth),
        "ym",
        DataType::Interval(IntervalUnit::YearMonth),
        3,
        |columns| {
            rows_of(columns, &[-13i64, 0, 25], |c, v| {
                c.append_int(0, v).unwrap()
            });
        },
    );
    let months = column::<IntervalYearMonthArray>(&batch, 0);
    assert_eq!(months.values(), &[-13, 0, 25]);

    // DayTime is two i32s in one eight-byte cell: days then milliseconds.
    let batch = single(
        plane(ArrowType::IntervalDayTime),
        "dt",
        DataType::Interval(IntervalUnit::DayTime),
        2,
        |columns| {
            for (days, millis) in [(5i32, 1_500i32), (-1, -2)] {
                let mut cell = [0u8; 8];
                cell[..4].copy_from_slice(&days.to_le_bytes());
                cell[4..].copy_from_slice(&millis.to_le_bytes());
                assert!(columns.begin_row());
                columns.append_fixed_bytes(0, &cell).unwrap();
                columns.end_row();
            }
        },
    );
    let day_times = column::<IntervalDayTimeArray>(&batch, 0);
    assert_eq!(day_times.value(0).days, 5);
    assert_eq!(day_times.value(0).milliseconds, 1_500);
    assert_eq!(day_times.value(1).days, -1);
    assert_eq!(day_times.value(1).milliseconds, -2);

    // MonthDayNano is months, days, then nanoseconds in sixteen bytes.
    let batch = single(
        plane(ArrowType::IntervalMonthDayNano),
        "mdn",
        DataType::Interval(IntervalUnit::MonthDayNano),
        1,
        |columns| {
            let mut cell = [0u8; 16];
            cell[..4].copy_from_slice(&(-2i32).to_le_bytes());
            cell[4..8].copy_from_slice(&3i32.to_le_bytes());
            cell[8..].copy_from_slice(&123_456_789_i64.to_le_bytes());
            assert!(columns.begin_row());
            columns.append_fixed_bytes(0, &cell).unwrap();
            columns.end_row();
        },
    );
    let month_day_nanos = column::<IntervalMonthDayNanoArray>(&batch, 0);
    assert_eq!(month_day_nanos.value(0).months, -2);
    assert_eq!(month_day_nanos.value(0).days, 3);
    assert_eq!(month_day_nanos.value(0).nanoseconds, 123_456_789);
}

#[test]
fn temporal_logical_types_round_trip_on_the_shared_integer_planes() {
    // Date32 and Time32 ride the four-byte signed plane; the Schema message
    // carries the logical type, so a reader gets a date and not an int.
    let batch = round_trip(
        &[plane(ArrowType::Int32), plane(ArrowType::Int32)],
        vec![
            Field::new("date32", DataType::Date32, true),
            Field::new("time32", DataType::Time32(TimeUnit::Millisecond), true),
        ],
        2,
        |columns| {
            for (days, millis) in [(19_000i64, 3_600_000i64), (-1, 0)] {
                assert!(columns.begin_row());
                columns.append_int(0, days).unwrap();
                columns.append_int(1, millis).unwrap();
                columns.end_row();
            }
        },
    );
    assert_eq!(column::<Date32Array>(&batch, 0).values(), &[19_000, -1]);
    assert_eq!(
        column::<Time32MillisecondArray>(&batch, 1).values(),
        &[3_600_000, 0]
    );

    // Date64, Time64, Timestamp and Duration ride the eight-byte signed plane.
    let batch = round_trip(
        &[
            plane(ArrowType::Int64),
            plane(ArrowType::Int64),
            plane(ArrowType::Int64),
            plane(ArrowType::Int64),
        ],
        vec![
            Field::new("date64", DataType::Date64, true),
            Field::new("time64", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("duration", DataType::Duration(TimeUnit::Nanosecond), true),
        ],
        1,
        |columns| {
            assert!(columns.begin_row());
            columns.append_int(0, 1_700_000_000_000).unwrap();
            columns.append_int(1, 86_399_999_999_999).unwrap();
            columns.append_int(2, 1_700_000_000_000_000).unwrap();
            columns.append_int(3, -1).unwrap();
            columns.end_row();
        },
    );
    assert_eq!(column::<Date64Array>(&batch, 0).value(0), 1_700_000_000_000);
    assert_eq!(
        column::<Time64NanosecondArray>(&batch, 1).value(0),
        86_399_999_999_999
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(&batch, 2).value(0),
        1_700_000_000_000_000
    );
    assert_eq!(column::<DurationNanosecondArray>(&batch, 3).value(0), -1);
}

#[test]
fn shared_planes_reject_logical_types_of_the_same_width() {
    // Each pair is (physical plane, a logical type of the same byte width that
    // belongs to a DIFFERENT plane). Width agreeing is not enough.
    let rejected = [
        (ArrowType::Int32, DataType::UInt32),
        (ArrowType::Int32, DataType::Float32),
        (
            ArrowType::Int32,
            DataType::Interval(IntervalUnit::YearMonth),
        ),
        // Arrow allows Time32 only in seconds or milliseconds.
        (ArrowType::Int32, DataType::Time32(TimeUnit::Microsecond)),
        (ArrowType::Int64, DataType::UInt64),
        (ArrowType::Int64, DataType::Float64),
        (ArrowType::Int64, DataType::Interval(IntervalUnit::DayTime)),
        // Arrow allows Time64 only in microseconds or nanoseconds.
        (ArrowType::Int64, DataType::Time64(TimeUnit::Second)),
        (ArrowType::UInt32, DataType::Int32),
        (ArrowType::Float64, DataType::Int64),
        (ArrowType::Float16, DataType::UInt16),
        (ArrowType::UInt16, DataType::Float16),
        (ArrowType::Binary, DataType::LargeBinary),
        (ArrowType::LargeUtf8, DataType::Utf8),
        // A Decimal128 that does not fit 128 bits.
        (ArrowType::Decimal128, DataType::Decimal128(50, 0)),
        (ArrowType::Decimal256, DataType::Decimal256(90, 0)),
        (ArrowType::Decimal128, DataType::Decimal256(38, 0)),
    ];
    for (physical, logical) in rejected {
        let field = plane(physical);
        let bytes = logical_schema_ipc_bytes(vec![Field::new("f", logical.clone(), true)]).unwrap();
        assert!(
            DynamicSchemaConfig::new(&bytes, &[field]).is_err(),
            "{physical:?} must reject {logical:?}"
        );
    }
}

#[test]
fn every_plane_emits_a_readable_stream_in_one_schema() {
    // One schema over the whole table: proves the buffer vector and the field
    // node vector stay in agreement across all twenty-three planes at once,
    // which per-plane tests cannot show.
    let fields: Vec<SignalSchemaField> = ArrowType::ALL
        .iter()
        .map(|arrow_type| match arrow_type {
            ArrowType::FixedSizeBinary => SignalSchemaField::new_fixed_size_binary(4, true),
            other => SignalSchemaField::new(*other, true),
        })
        .collect();
    let logical: Vec<Field> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| Field::new(format!("p{index}"), field.to_data_type(), true))
        .collect();

    let batch = round_trip(&fields, logical, 2, |columns| {
        for row in 0..2u32 {
            assert!(columns.begin_row());
            for (index, field) in ArrowType::ALL.iter().enumerate() {
                let index = index as u32;
                let value = i64::from(row) + 1;
                match field {
                    // Null takes no value at all.
                    ArrowType::Null => columns.append_null(index).unwrap(),
                    ArrowType::Bool => columns.append_bool(index, row == 0).unwrap(),
                    ArrowType::Int8
                    | ArrowType::Int16
                    | ArrowType::Int32
                    | ArrowType::Int64
                    | ArrowType::IntervalYearMonth => columns.append_int(index, value).unwrap(),
                    ArrowType::UInt8
                    | ArrowType::UInt16
                    | ArrowType::UInt32
                    | ArrowType::UInt64 => {
                        columns.append_uint(index, value as u64).unwrap();
                    }
                    ArrowType::Float16 | ArrowType::Float32 | ArrowType::Float64 => {
                        columns.append_float(index, 0.5).unwrap();
                    }
                    ArrowType::Decimal128 | ArrowType::IntervalMonthDayNano => {
                        columns.append_fixed_bytes(index, &[row as u8; 16]).unwrap();
                    }
                    ArrowType::Decimal256 => {
                        columns.append_fixed_bytes(index, &[0u8; 32]).unwrap();
                    }
                    ArrowType::IntervalDayTime => {
                        columns.append_fixed_bytes(index, &[row as u8; 8]).unwrap();
                    }
                    ArrowType::FixedSizeBinary => {
                        columns.append_fixed_bytes(index, &[row as u8; 4]).unwrap();
                    }
                    ArrowType::Binary
                    | ArrowType::LargeBinary
                    | ArrowType::Utf8
                    | ArrowType::LargeUtf8 => {
                        columns.append_variable(index, b"ab").unwrap();
                    }
                }
            }
            columns.end_row();
        }
    });

    assert_eq!(batch.num_columns(), ArrowType::ALL.len());
    for (index, arrow_type) in ArrowType::ALL.iter().enumerate() {
        let array = batch.column(index);
        assert_eq!(array.len(), 2, "{arrow_type:?} lost its rows");
        // Every plane but Null wrote both cells, so nothing is null. The Null
        // plane has no validity buffer at all, so its nullness is logical:
        // `null_count` reads the buffer and sees none, `logical_null_count`
        // reads the type and sees every row.
        let expected_nulls = usize::from(*arrow_type == ArrowType::Null) * 2;
        assert_eq!(array.logical_null_count(), expected_nulls, "{arrow_type:?}");
    }
}
