//! Per-batch cost of `EventProcessor::create_log_entry` by input format and
//! batch size, on the base event-log scanner and on the named extraction path.
//!
//! Every event carries an id, a type, a timestamp and a `value` object with
//! one integer and one string member. The base handle stores `value` as raw
//! bytes; the named handle extracts its members into typed columns. The same
//! input is processed for every timed batch into a reused output buffer.
//!
//! Run: `cargo bench -p columine-event-processor --bench batch_scale`.

use std::time::Instant;

use columine_arrow::{ArrowType, DynamicSchemaConfig, SignalSchemaField};
use columine_event_processor::{EventProcessor, InputFormat, ParseOptions, ResultCode};

const WARMUP: u32 = 20;

fn options() -> ParseOptions {
    ParseOptions {
        base_path: true,
        msgpack_growth: true,
        diagnostics: false,
    }
}

fn base_handle(rows: u32) -> EventProcessor {
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Int64, false),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let config = DynamicSchemaConfig::from_physical_fields_with_names(
        &fields,
        b"id\0type\0timestamp\0value\0",
    )
    .expect("base schema");
    EventProcessor::new(options(), rows, config).expect("base handle")
}

fn named_handle(rows: u32) -> EventProcessor {
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Utf8, false),
        SignalSchemaField::new(ArrowType::Int64, false),
        SignalSchemaField::new(ArrowType::Int64, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
    ];
    let config = DynamicSchemaConfig::from_physical_fields_with_names(
        &fields,
        b"id\0type\0timestamp\0value.amount\0value.label\0",
    )
    .expect("named schema");
    EventProcessor::new(options(), rows, config).expect("named handle")
}

fn json_batch(rows: u32) -> Vec<u8> {
    let mut out = b"[".to_vec();
    for row in 0..rows {
        if row != 0 {
            out.push(b',');
        }
        out.extend(
            format!(
                r#"{{"id":"event-{row:08}","type":"purchase","timestamp":{},"value":{{"amount":{},"label":"item label {row}"}}}}"#,
                1_700_000_000_000_000u64 + u64::from(row),
                u64::from(row) * 7,
            )
            .bytes(),
        );
    }
    out.push(b']');
    out
}

fn msgpack_str(out: &mut Vec<u8>, text: &str) {
    out.push(0xd9);
    out.push(u8::try_from(text.len()).expect("short string"));
    out.extend(text.bytes());
}

fn msgpack_i64(out: &mut Vec<u8>, value: i64) {
    out.push(0xd3);
    out.extend(value.to_be_bytes());
}

fn msgpack_batch(rows: u32) -> Vec<u8> {
    let mut out = vec![0xdd];
    out.extend(rows.to_be_bytes());
    for row in 0..rows {
        out.push(0x84);
        msgpack_str(&mut out, "id");
        msgpack_str(&mut out, &format!("event-{row:08}"));
        msgpack_str(&mut out, "type");
        msgpack_str(&mut out, "purchase");
        msgpack_str(&mut out, "timestamp");
        msgpack_i64(&mut out, 1_700_000_000_000_000 + i64::from(row));
        msgpack_str(&mut out, "value");
        out.push(0x82);
        msgpack_str(&mut out, "amount");
        msgpack_i64(&mut out, i64::from(row) * 7);
        msgpack_str(&mut out, "label");
        msgpack_str(&mut out, &format!("item label {row}"));
    }
    out
}

fn time(handle: &mut EventProcessor, input: &[u8], format: InputFormat, iters: u32) -> f64 {
    let mut output = vec![0u8; input.len() * 4 + (1 << 20)];
    let mut total = 0u128;
    for i in 0..iters + WARMUP {
        let start = Instant::now();
        let code = handle.create_log_entry(input, format, &mut output);
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(code, ResultCode::Ok, "{format:?}");
        if i >= WARMUP {
            total += elapsed;
        }
    }
    total as f64 / f64::from(iters) / 1000.0
}

fn main() {
    println!("| path | format | rows | per batch |");
    println!("|---|---|---:|---:|");
    for rows in [1u32, 100, 1000] {
        let iters = if rows == 1000 { 500 } else { 5000 };
        let json = json_batch(rows);
        let msgpack = msgpack_batch(rows);
        for (path, mut handle) in [("base", base_handle(rows)), ("named", named_handle(rows))] {
            for (format, input) in [(InputFormat::Json, &json), (InputFormat::Msgpack, &msgpack)] {
                let us = time(&mut handle, input, format, iters);
                println!("| {path} | {format:?} | {rows} | {us:.2} us |");
            }
        }
    }
}
