//! A layout whose routing rows name no subtype (specs/columine/10-physical-vocabulary.md §7) stores, on both lanes,
//! exactly the bytes it stored before a routing row could name one: the log entry's result header and Arrow IPC stream,
//! byte for byte, recorded from the processor as it was.

use std::collections::HashMap;

use arrow_schema::Field;
use columine_arrow::schema::PayloadDiscriminator;
use columine_arrow::{
    ArrowType, CELL_ENCODING_KEY, DynamicSchemaConfig, RecordMember, SignalSchemaField,
    logical_schema_ipc_bytes,
};
use columine_event_processor::{
    CellRefusal, CellTarget, EventProcessor, InputFormat, ParseOptions, RecordRow, ResultCode,
    TEXT_HASH_BYTES, TextPlace, ValueCells,
};
use columine_parsing::validate::{
    NoAddressGrammar, NumberValue, UnavailableVoAdmission, ValueKind, ValueView,
};

//#region smoo/columine!n/vocab-value-cells
const ENCODING: &str = "test-cells";

/// The record and the carrier as the text of the numbers and strings each was handed.
#[derive(Default)]
struct TextCells {
    record: Vec<String>,
    carrier: Vec<String>,
}

fn render(value: &dyn ValueView) -> String {
    match (value.kind(), value.as_number(), value.as_str()) {
        (ValueKind::Number, Some(NumberValue::Signed(number)), _) => number.to_string(),
        (ValueKind::Number, Some(NumberValue::Unsigned(number)), _) => number.to_string(),
        (ValueKind::String, _, Some(text)) => format!("{text:?}"),
        (kind, _, _) => kind.as_str().to_owned(),
    }
}

fn finish(parts: &[String], out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
    if parts.is_empty() {
        return Ok(false);
    }
    out.extend(parts.join(";").bytes());
    Ok(true)
}

impl ValueCells for TextCells {
    fn encoding(&self) -> &'static str {
        ENCODING
    }
    fn prepare(&mut self, _target: CellTarget<'_>) -> Result<(), CellRefusal> {
        Ok(())
    }
    fn member(
        &mut self,
        column: u32,
        value: &dyn ValueView,
        out: &mut Vec<u8>,
    ) -> Result<(), CellRefusal> {
        out.extend(format!("member {column} {}", render(value)).bytes());
        Ok(())
    }
    fn begin_record(&mut self) {
        self.record.clear();
    }
    fn put(&mut self, row: &RecordRow<'_>, value: &dyn ValueView) -> Result<(), CellRefusal> {
        self.record.push(format!(
            "{:?}@{}={}",
            row.path,
            row.descriptor_position,
            render(value)
        ));
        Ok(())
    }
    fn finish_record(&mut self, out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        finish(&self.record, out)
    }
    fn begin_undeclared(&mut self) {
        self.carrier.clear();
    }
    fn put_undeclared(&mut self, path: &str, value: &dyn ValueView) -> Result<(), CellRefusal> {
        self.carrier.push(format!("{path}={}", render(value)));
        Ok(())
    }
    fn finish_undeclared(&mut self, out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        finish(&self.carrier, out)
    }
    fn text(
        &mut self,
        _hash_column: u32,
        _value: &str,
        _hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
    fn text_frame(
        &mut self,
        _hash_column: u32,
        _bytes: &[u8],
        _hash: &mut [u8; TEXT_HASH_BYTES],
        _inline: &mut Vec<u8>,
    ) -> Result<TextPlace, CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        _hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
}

/// A routing row of no subtype: `member` of `order` at field `field`, descriptor `position`.
fn plain(member: &str, field: u16, position: u32) -> RecordMember {
    RecordMember {
        discriminant: "order".into(),
        subtype: None,
        member: member.into(),
        path: vec![field],
        descriptor_position: position,
    }
}

/// `type`, `value.name`, the declared record and the carrier, with `qty` and `note` routed to the record.
fn old_form() -> DynamicSchemaConfig {
    let names = ["type", "value.name", "value.$declared", "value.$undeclared"];
    let fields = [
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new(ArrowType::Binary, true),
        SignalSchemaField::new(ArrowType::Binary, true),
    ];
    let logical = names
        .iter()
        .zip(&fields)
        .map(|(name, field)| {
            let column = Field::new(*name, field.to_data_type(), true);
            if field.arrow_type == ArrowType::Binary {
                column.with_metadata(HashMap::from([(
                    CELL_ENCODING_KEY.to_owned(),
                    ENCODING.to_owned(),
                )]))
            } else {
                column
            }
        })
        .collect();
    let message = logical_schema_ipc_bytes(logical).unwrap();
    let joined: Vec<u8> = names
        .iter()
        .flat_map(|name| name.bytes().chain([0]))
        .collect();
    DynamicSchemaConfig {
        semantic_schema: Some(
            br#"{"order":{"kind":"object","fields":{"name":{"kind":"string"},"qty":{"kind":"optional","value":{"kind":"number"}},"note":{"kind":"optional","value":{"kind":"string"}}}}}"#
                .to_vec(),
        ),
        payload_discriminator: Some(PayloadDiscriminator {
            key: "type".into(),
members: vec![columine_arrow::schema::DiscriminatedMember { discriminant: "order".into(), member: "name".into(), path: Vec::new(), column: 1 },],
        }),
        record_members: vec![plain("qty", 3, 7), plain("note", 4, 8)],
        ..DynamicSchemaConfig::with_field_names(&message, &fields, &joined).unwrap()
    }
}

/// The log entry an old-form processor writes from `input`: its result header and Arrow IPC stream.
fn entry(input: &[u8], format: InputFormat) -> Vec<u8> {
    let options = ParseOptions {
        base_path: false,
        msgpack_growth: true,
        diagnostics: true,
    };
    let mut ep =
        EventProcessor::<UnavailableVoAdmission, NoAddressGrammar, TextCells>::new_with_cells(
            options,
            8,
            old_form(),
            UnavailableVoAdmission,
            TextCells::default(),
        )
        .unwrap();
    let mut output = vec![0u8; 1 << 16];
    let code = ep.create_log_entry(input, format, &mut output, &[]);
    assert_eq!(
        code,
        ResultCode::Ok,
        "{format:?}: header {:?}",
        &output[..32]
    );
    let length = u32::from_le_bytes(output[8..12].try_into().unwrap()) as usize;
    let offset = u32::from_le_bytes(output[4..8].try_into().unwrap()) as usize;
    output.truncate(offset + length);
    output
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Two orders, each with a routed member.
const JSON: &[u8] = br#"[{"type":"order","value":{"name":"a","qty":5,"note":"n"}},{"type":"order","value":{"name":"b","qty":6}}]"#;

/// [`JSON`] as MessagePack.
const MSGPACK: &[u8] = &[
    0x92, // two events
    0x82, 0xa4, b't', b'y', b'p', b'e', 0xa5, b'o', b'r', b'd', b'e',
    b'r', // {"type":"order",
    0xa5, b'v', b'a', b'l', b'u', b'e', 0x83, //  "value":{
    0xa4, b'n', b'a', b'm', b'e', 0xa1, b'a', //   "name":"a",
    0xa3, b'q', b't', b'y', 0x05, //   "qty":5,
    0xa4, b'n', b'o', b't', b'e', 0xa1, b'n', //   "note":"n"}}
    0x82, 0xa4, b't', b'y', b'p', b'e', 0xa5, b'o', b'r', b'd', b'e',
    b'r', // {"type":"order",
    0xa5, b'v', b'a', b'l', b'u', b'e', 0x82, //  "value":{
    0xa4, b'n', b'a', b'm', b'e', 0xa1, b'b', //   "name":"b",
    0xa3, b'q', b't', b'y', 0x06, //   "qty":6}}
];

/// The entry both lanes stored for the orders before a routing row could name a subtype: the same rows, the same bytes.
const STORED: &str = "0000000020000000b80300000200000000000000000000000000000000000000ffffffffb80100001000000000000a000c000a00090004000a0000001000000000010400080008000000040008000000040000000400000034010000f4000000840000000400000096ffffff34000000140000000c000000000001040c00000000000000e0feffff1100000076616c75652e24756e6465636c61726564000000010000000400000088ffffff08000000140000000a000000746573742d63656c6c7300000700000073622e63656c6c00000012001800140012001300080000000c0004001200000030000000140000000c000000000001040c000000000000005cffffff0f00000076616c75652e246465636c6172656400010000000c00000008000c00080004000800000008000000140000000a000000746573742d63656c6c7300000700000073622e63656c6c00d4ffffff140000000c000000000001050c00000000000000c4ffffff0a00000076616c75652e6e616d6500001000140010000e000f0004000000080010000000180000000c000000000001051000000000000000040004000400000004000000747970650000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffffffff5801000014000000000000000c001600140013000c0004000c0000009000000000000000140000000000000304000a0018000c00080004000a00000014000000d40000000200000000000000000000000c0000000000000000000000010000000000000008000000000000000c0000000000000018000000000000000a000000000000002800000000000000010000000000000030000000000000000c00000000000000400000000000000002000000000000004800000000000000010000000000000050000000000000000c00000000000000600000000000000018000000000000007800000000000000010000000000000080000000000000000c0000000000000090000000000000000000000000000000040000000200000000000000000000000000000002000000000000000000000000000000020000000000000000000000000000000200000000000000020000000000000000000000030000000000000000000000050000000a000000000000006f726465726f7264657200000000000003000000000000000000000001000000020000000000000061620000000000000300000000000000000000001100000018000000000000005b335d40373d353b5b345d40383d226e225b335d40373d36000000000000000000000000000000000000000000000000ffffffff00000000";

/// The bytes the processor stored for [`JSON`] and [`MSGPACK`] before a routing row could name a subtype.
const RECORDED: [(InputFormat, &str); 2] =
    [(InputFormat::Json, STORED), (InputFormat::Msgpack, STORED)];

#[test]
fn an_old_form_layout_stores_the_bytes_it_stored_before_subtype_rows() {
    let stored = [
        hex(&entry(JSON, InputFormat::Json)),
        hex(&entry(MSGPACK, InputFormat::Msgpack)),
    ];
    assert_eq!(
        stored,
        RECORDED.map(|(_, recorded)| recorded.to_owned()),
        "[json, msgpack]"
    );
}
//#endregion smoo/columine!n/vocab-value-cells
