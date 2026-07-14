//! Replaces `packages/columine/src/parsing/msgpack_extractor.zig`.

use crate::msgpack_scanner::Reader;
use crate::{
    ArrowType, ColumnValue, DynamicColumns, ExtractionConfig, ParseError,
    json_extractor::ExtractionError, json_scanner::parse_iso8601_to_micros,
};

/// Extracts either a concatenated MessagePack map stream or an array of maps.
pub fn extract_msgpack_events(
    input: &[u8],
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    stream: bool,
) -> Result<usize, ExtractionError> {
    if input.is_empty() {
        return Ok(0);
    }
    let mut reader = Reader::new(input);
    let mut count = 0;
    // ZIG-PARITY: an exactly-capacity batch is rejected (`count >= capacity` runs after every event); intended fix: accept full batches, resolved at the event_processor level.
    // Zig checks `count >= capacity` after EVERY event, so an exactly-capacity
    // batch is rejected too — mirrored bug-for-bug (flagged for the stage-4B
    // event-processor audit, same as the JSON extractor).
    if stream {
        while !reader.at_end() {
            extract_msgpack_event(&mut reader, config, columns, work_buffer)?;
            count += 1;
            if count >= columns.capacity as usize {
                return Err(ExtractionError::TooManyEvents);
            }
        }
    } else {
        let size = reader
            .read_array_header()
            .ok_or(ExtractionError::InvalidJson)?;
        for _ in 0..size {
            extract_msgpack_event(&mut reader, config, columns, work_buffer)?;
            count += 1;
            if count >= columns.capacity as usize {
                return Err(ExtractionError::TooManyEvents);
            }
        }
    }
    Ok(count)
}

fn extract_msgpack_event(
    reader: &mut Reader<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
) -> Result<(), ExtractionError> {
    if config.field_entries.len() > 64 {
        return Err(ExtractionError::OutOfMemory);
    }
    // Zig order: map header parses BEFORE beginRow, so an invalid header
    // never opens a row.
    let fields = reader
        .read_map_header()
        .ok_or(ExtractionError::InvalidJson)?;
    if !columns.begin_row() {
        return Err(ExtractionError::TooManyEvents);
    }
    let result = extract_msgpack_fields(reader, fields, config, columns, work_buffer);
    if result.is_err() {
        // Zig never reaches endRow on the error path; abandon, don't commit.
        columns.abandon_row();
    }
    result
}

fn extract_msgpack_fields(
    reader: &mut Reader<'_>,
    fields: u32,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
) -> Result<(), ExtractionError> {
    // Stack-allocated like Zig's `[64]bool` — per-event ingest hot path.
    let mut set = [false; 64];
    let mut extra_count: u32 = 0;
    let mut extra_end: usize = 0;
    // ZIG-PARITY: when the $extra work buffer is under 5 bytes, undeclared fields are silently dropped instead of erroring; intended fix: refuse loudly (Err) on an unusable buffer.
    // Zig: `extra_active = fallback set AND work_buffer.len >= 5`; when the
    // buffer is too small for even the header, undeclared fields are silently
    // SKIPPED and the fallback column nulls — not an error.
    let extra_active = config.fallback_column.is_some() && work_buffer.len() >= 5;
    for _ in 0..fields {
        let key_start = reader.position();
        let key = reader.read_string().ok_or(ExtractionError::InvalidJson)?;
        let key_end = reader.position();
        // Zig looks the raw key bytes up in a byte-keyed map; a non-UTF-8 key
        // is simply never declared, it is NOT a parse error.
        let lookup = std::str::from_utf8(key)
            .ok()
            .and_then(|name| config.field_map.get(name));
        if let Some(lookup) = lookup {
            extract_typed_value(reader, lookup.arrow_type, columns, lookup.column)?;
            set[lookup.column] = true;
        } else if extra_active {
            let value_start = reader.position();
            reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
            let value_end = reader.position();
            if extra_count == 0 {
                extra_end = 5;
            }
            let raw = &reader.input()[key_start..key_end];
            let raw_value = &reader.input()[value_start..value_end];
            let end = extra_end
                .checked_add(raw.len() + raw_value.len())
                .ok_or(ExtractionError::BufferOverflow)?;
            if end > work_buffer.len() {
                return Err(ExtractionError::BufferOverflow);
            }
            work_buffer[extra_end..extra_end + raw.len()].copy_from_slice(raw);
            work_buffer[extra_end + raw.len()..end].copy_from_slice(raw_value);
            extra_end = end;
            extra_count += 1;
        } else {
            reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
        }
    }
    for (column, _, _) in &config.field_entries {
        if !set[*column] {
            append(columns, *column, None)?;
        }
    }
    if let Some(column) = config.fallback_column {
        if extra_count == 0 {
            append(columns, column, None)?;
        } else {
            work_buffer[0] = 0xdf;
            work_buffer[1..5].copy_from_slice(&extra_count.to_be_bytes());
            append(
                columns,
                column,
                Some(ColumnValue::Binary(work_buffer[..extra_end].to_vec())),
            )?;
        }
    }
    columns.end_row();
    Ok(())
}

fn extract_typed_value(
    reader: &mut Reader<'_>,
    kind: ArrowType,
    columns: &mut DynamicColumns,
    column: usize,
) -> Result<(), ExtractionError> {
    let first = *reader
        .input()
        .get(reader.position())
        .ok_or(ExtractionError::InvalidJson)?;
    let value = match kind {
        ArrowType::Utf8 => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else {
                Some(ColumnValue::Utf8(
                    std::str::from_utf8(
                        reader
                            .read_string()
                            .ok_or(ExtractionError::InvalidFieldType)?,
                    )
                    .map_err(|_| ExtractionError::InvalidFieldType)?
                    .to_owned(),
                ))
            }
        }
        ArrowType::Int32 | ArrowType::Int64 => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if is_integer(first) {
                Some(ColumnValue::Int64(
                    reader
                        .read_integer()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                ))
            } else if matches!(first, 0xca | 0xcb) {
                Some(ColumnValue::Int64(
                    reader
                        .read_float()
                        .ok_or(ExtractionError::InvalidFieldType)? as i64,
                ))
            } else if is_string(first) {
                let value = std::str::from_utf8(
                    reader
                        .read_string()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                )
                .map_err(|_| ExtractionError::InvalidFieldType)?;
                Some(ColumnValue::Int64(
                    parse_iso8601_to_micros(value)
                        .map_err(|_| ExtractionError::InvalidFieldType)?,
                ))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        ArrowType::Float64 => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if matches!(first, 0xca | 0xcb) {
                Some(ColumnValue::Float64(
                    reader
                        .read_float()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                ))
            } else if is_integer(first) {
                Some(ColumnValue::Float64(
                    reader
                        .read_integer()
                        .ok_or(ExtractionError::InvalidFieldType)? as f64,
                ))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        ArrowType::Bool => match first {
            0xc0 => {
                reader.skip_value();
                None
            }
            0xc2 => {
                reader.skip_value();
                Some(ColumnValue::Bool(false))
            }
            0xc3 => {
                reader.skip_value();
                Some(ColumnValue::Bool(true))
            }
            _ => return Err(ExtractionError::InvalidFieldType),
        },
        ArrowType::Binary => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if matches!(first, 0xc4..=0xc6) {
                // msgpack_extractor.zig `.Binary`: internal typed persistence
                // pre-encodes Binary/S.unknown values as an opaque canonical
                // MessagePack document carried by a standard bin value. Store
                // that bin PAYLOAD directly; wrapping the bin token itself
                // would require a second decode after Arrow materialization
                // and corrupt raw bytes.
                let payload = reader.read_bin().ok_or(ExtractionError::InvalidJson)?;
                Some(ColumnValue::Binary(payload.to_vec()))
            } else {
                // External standard MessagePack may provide a structured value
                // directly. Preserve its exact document bytes for normal
                // Binary materialization.
                let start = reader.position();
                reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
                Some(ColumnValue::Binary(
                    reader.input()[start..reader.position()].to_vec(),
                ))
            }
        }
        ArrowType::Null => {
            reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
            None
        }
    };
    append(columns, column, value)
}
fn append(
    columns: &mut DynamicColumns,
    column: usize,
    value: Option<ColumnValue>,
) -> Result<(), ExtractionError> {
    crate::append_cell(columns, column, value).map_err(|error| match error {
        ParseError::BufferOverflow => ExtractionError::BufferOverflow,
        _ => ExtractionError::InvalidFieldType,
    })
}
fn is_integer(byte: u8) -> bool {
    byte & 0x80 == 0 || byte & 0xe0 == 0xe0 || (0xcc..=0xd3).contains(&byte)
}
fn is_string(byte: u8) -> bool {
    byte & 0xe0 == 0xa0 || matches!(byte, 0xd9..=0xdb)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CellExt, SignalSchemaField, build_extraction_config};
    fn field(arrow_type: ArrowType) -> SignalSchemaField {
        SignalSchemaField::new(arrow_type, true)
    }
    fn str_(out: &mut Vec<u8>, value: &str) {
        out.push(0xa0 | value.len() as u8);
        out.extend(value.as_bytes())
    }
    fn config(fields: &[SignalSchemaField], names: &[&str]) -> ExtractionConfig {
        build_extraction_config(fields, names).unwrap()
    }
    #[test]
    fn extract_msgpack_events_stream_format_with_typed_extraction() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Int32),
            field(ArrowType::Int32),
        ];
        let mut input = vec![0x84];
        for (key, value) in [
            ("id", Some("ev-1")),
            ("type", Some("order")),
            ("timestamp", None),
            ("value.qty", None),
        ] {
            str_(&mut input, key);
            if let Some(v) = value {
                str_(&mut input, v)
            } else if key == "timestamp" {
                input.push(0xd3);
                input.extend(1000_i64.to_be_bytes())
            } else {
                input.push(5)
            }
        }
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 1024];
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config(&fields, &["id", "type", "timestamp", "value.qty"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert_eq!(columns.cell(0, 0), Some(ColumnValue::Utf8("ev-1".into())));
        assert_eq!(columns.cell(1, 0), Some(ColumnValue::Utf8("order".into())));
    }
    #[test]
    fn extract_msgpack_events_rejects_exactly_capacity_batch() {
        // Zig's post-event `count >= capacity` check fires even when the
        // stream ends exactly at capacity.
        let fields = [field(ArrowType::Utf8)];
        let mut input = vec![0x81];
        str_(&mut input, "id");
        str_(&mut input, "x");
        let mut columns = DynamicColumns::new(&fields, 1);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config(&fields, &["id"]),
                &mut columns,
                &mut work,
                true
            ),
            Err(ExtractionError::TooManyEvents)
        );
    }
    #[test]
    fn extract_msgpack_events_tiny_work_buffer_skips_undeclared() {
        // Zig: fallback configured but work_buffer < 5 bytes → extra_active
        // is false; undeclared fields are skipped and $extra nulls, no error.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        str_(&mut input, "undeclared");
        input.push(7);
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 4];
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config(&fields, &["id", "value.$extra"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert!(columns.is_null(1, 0));
    }
    #[test]
    fn extract_msgpack_events_extra_copies_raw_key_value_bytes() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        str_(&mut input, "qty");
        input.push(42);
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 128];
        extract_msgpack_events(
            &input,
            &config(&fields, &["id", "value.$extra"]),
            &mut columns,
            &mut work,
            true,
        )
        .unwrap();
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(vec![
                0xdf, 0, 0, 0, 1, 0xa3, b'q', b't', b'y', 42
            ]))
        );
    }
    #[test]
    fn extract_msgpack_events_non_utf8_key_goes_to_extra() {
        // Zig keys a byte map; a non-UTF-8 key is undeclared, not an error.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        input.push(0xa2);
        input.extend([0xff, 0xfe]);
        input.push(1);
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config(&fields, &["id", "value.$extra"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert!(!columns.is_null(1, 0));
    }
    #[test]
    fn extract_msgpack_events_empty_stream() {
        let fields = [field(ArrowType::Utf8)];
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
                &[],
                &config(&fields, &["id"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            0
        );
    }
    #[test]
    fn extract_msgpack_events_empty_array() {
        let fields = [field(ArrowType::Utf8)];
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
                &[0x90],
                &config(&fields, &["id"]),
                &mut columns,
                &mut work,
                false
            )
            .unwrap(),
            0
        );
    }
}

#[cfg(test)]
mod bin_unwrap_pin {
    //! f33e06007: a declared-Binary value carried as a standard msgpack bin
    //! (0xc4/c5/c6) stores the bin PAYLOAD, not the wrapped token — wrapping
    //! would double-encode internal typed persistence and corrupt raw bytes.
    use crate::msgpack_scanner::Reader;

    #[test]
    fn read_bin_unwraps_all_three_headers() {
        let mut input = vec![0xc4, 3, 1, 2, 3];
        assert_eq!(Reader::new(&input).read_bin(), Some(&[1u8, 2, 3][..]));
        input = vec![0xc5, 0, 3, 4, 5, 6];
        assert_eq!(Reader::new(&input).read_bin(), Some(&[4u8, 5, 6][..]));
        input = vec![0xc6, 0, 0, 0, 2, 7, 8];
        assert_eq!(Reader::new(&input).read_bin(), Some(&[7u8, 8][..]));
        assert_eq!(Reader::new(&[0xa1, b'x']).read_bin(), None);
        assert_eq!(Reader::new(&[0xc4, 5, 1]).read_bin(), None);
    }
}
