//! The embedder's reserved opaque extension
//! (specs/columine/10-physical-vocabulary.md §7): a value of that MessagePack
//! extension type in a dynamic cell (a dynamic column, or a dynamic member
//! the declared record holds) reaches the embedder unread, as its payload
//! bytes, in both payload spellings. Anywhere else it is refused by name with
//! the cell's path, and a processor that reserves no type reads it as before.

use columine_arrow::{DynamicMember, RecordMember};
use columine_parsing::json_extractor::ExtractionError;
use columine_parsing::msgpack_extractor::extract_msgpack_events_with_projections;
use columine_parsing::validate::{NumberValue, ValueKind, ValueView};
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, ConfigError, DynamicColumns, ExtractionConfig,
    FrameProjections, PayloadDiscriminator, SignalSchemaField,
    build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;
use columine_types::cells::{
    CellRefusal, CellTarget, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
};

//#region smoo/columine!n/vocab-value-cells
const ENCODING: &str = "test-cells";
/// The extension type the test embedder reserves.
const OPAQUE: i8 = 7;

/// A value as text: an opaque value as its payload in hex, strings quoted,
/// numbers as written, objects by their members, any other value by its kind.
fn render(value: &dyn ValueView) -> String {
    if let Some(payload) = value.as_opaque() {
        let hex: String = payload.iter().map(|byte| format!("{byte:02x}")).collect();
        return format!("opaque {hex}");
    }
    match value.kind() {
        ValueKind::String => format!("{:?}", value.as_str().unwrap_or_default()),
        ValueKind::Number => match value.as_number() {
            Some(NumberValue::Signed(number)) => number.to_string(),
            Some(NumberValue::Unsigned(number)) => number.to_string(),
            Some(NumberValue::Float(number)) => number.to_string(),
            None => "number".to_owned(),
        },
        ValueKind::Object => {
            let mut members = Vec::new();
            value.visit_object(&mut |name, member| {
                members.push(format!("{name}={}", render(member)));
            });
            format!("{{{}}}", members.join(","))
        }
        kind => kind.as_str().to_owned(),
    }
}

/// The test embedder: each cell is the text of what it was handed.
#[derive(Default)]
struct TextCells {
    record: Vec<String>,
    carrier: Vec<String>,
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

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        _hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }
}

const NAMES: [&str; 6] = [
    "type",
    "value.data",
    "value.name",
    "value.meta",
    "value.$declared",
    "value.$undeclared",
];

fn utf8() -> SignalSchemaField {
    SignalSchemaField::new(ArrowType::Utf8, true)
}

fn binary() -> SignalSchemaField {
    SignalSchemaField::new(ArrowType::Binary, true)
}

/// `type`; `value.data`, a dynamic column; `value.name`, a typed column;
/// `value.meta`, a member cell that is not dynamic; the declared record,
/// holding the dynamic member `order.extra` at `[5]`, descriptor 9, and the
/// member `order.note` at `[6]`, descriptor 10; and the carrier. Every Binary
/// cell is the embedder's.
fn config(opaque_ext: Option<i8>) -> ([SignalSchemaField; 6], ExtractionConfig) {
    let fields = [utf8(), binary(), utf8(), binary(), binary(), binary()];
    let cell = Some(ENCODING.to_owned());
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &NAMES,
        &[],
        None,
        Some(&PayloadDiscriminator {
            key: "type".into(),
            members: Vec::new(),
        }),
        CellLayout {
            cell_metadata: &[None, cell.clone(), None, cell.clone(), cell.clone(), cell],
            record_members: &[
                RecordMember {
                    discriminant: "order".into(),
                    member: "extra".into(),
                    path: vec![5],
                    descriptor_position: 9,
                },
                RecordMember {
                    discriminant: "order".into(),
                    member: "note".into(),
                    path: vec![6],
                    descriptor_position: 10,
                },
            ],
            dynamic_members: &[DynamicMember {
                event_type: "order".into(),
                member: "extra".into(),
            }],
            dynamic_columns: &[1],
            opaque_ext,
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .unwrap();
    (fields, config)
}

/// A MessagePack value of the test events.
enum Mp<'a> {
    Str(&'a str),
    Nil,
    /// An extension value: its type and payload.
    Ext(i8, &'a [u8]),
    Map(Vec<(&'a str, Mp<'a>)>),
}

fn msgpack(out: &mut Vec<u8>, value: &Mp<'_>) {
    match value {
        Mp::Str(text) => {
            out.push(0xa0 | u8::try_from(text.len()).unwrap());
            out.extend(text.as_bytes());
        }
        Mp::Nil => out.push(0xc0),
        Mp::Ext(kind, payload) => {
            out.extend([
                0xc7,
                u8::try_from(payload.len()).unwrap(),
                kind.to_be_bytes()[0],
            ]);
            out.extend(*payload);
        }
        Mp::Map(entries) => {
            out.push(0x80 | u8::try_from(entries.len()).unwrap());
            for (key, value) in entries {
                msgpack(out, &Mp::Str(key));
                msgpack(out, value);
            }
        }
    }
}

/// Extract `events` and answer columns `value.data` and `value.$declared`
/// of every row, as text: `-` is the empty cell.
fn stored(opaque_ext: Option<i8>, events: &[Mp<'_>]) -> Result<Vec<[String; 2]>, ExtractionError> {
    let (fields, config) = config(opaque_ext);
    let mut input = vec![0x90 | u8::try_from(events.len()).unwrap()];
    for event in events {
        msgpack(&mut input, event);
    }
    let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
    let mut embedder = TextCells::default();
    let rows = extract_msgpack_events_with_projections(
        &input,
        &config,
        &mut columns,
        &mut [0; 256],
        false,
        FrameProjections::default(),
        &mut NoCategories,
        &mut CellWriter::Embedder(&mut embedder),
        &mut Vec::new(),
    )?;
    Ok((0..rows)
        .map(|row| {
            [1, 4].map(|column| match read_cell(&columns, column, row) {
                None => "-".to_owned(),
                Some(ColumnValue::Binary(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
                Some(other) => format!("{other:?}"),
            })
        })
        .collect())
}

fn order<'a>(payload: Vec<(&'a str, Mp<'a>)>) -> Mp<'a> {
    Mp::Map(vec![
        ("type", Mp::Str("order")),
        ("value", Mp::Map(payload)),
    ])
}

/// The accepted events: nested with `type` first, flat with `type` last,
/// explicit nils, and an empty payload.
fn accepted() -> Vec<Mp<'static>> {
    vec![
        order(vec![
            ("data", Mp::Ext(OPAQUE, &[1, 2, 3])),
            ("extra", Mp::Ext(OPAQUE, &[4, 5])),
        ]),
        Mp::Map(vec![
            ("value.data", Mp::Ext(OPAQUE, &[0x0a, 0x0b])),
            ("value.extra", Mp::Ext(OPAQUE, &[0x0c])),
            ("type", Mp::Str("order")),
        ]),
        order(vec![("data", Mp::Nil), ("extra", Mp::Nil)]),
        order(Vec::new()),
    ]
}

#[test]
fn an_opaque_value_reaches_a_dynamic_cell_unread_in_both_spellings() {
    let row = |cells: [&str; 2]| cells.map(str::to_owned);
    assert_eq!(
        stored(Some(OPAQUE), &accepted()),
        Ok(vec![
            row(["member 1 opaque 010203", "[5]@9=opaque 0405"]),
            row(["member 1 opaque 0a0b", "[5]@9=opaque 0c"]),
            // An explicit nil in a dynamic cell stays a present null.
            row(["member 1 null", "[5]@9=null"]),
            row(["-", "-"]),
        ])
    );
}

#[test]
fn an_opaque_value_outside_a_dynamic_cell_is_refused_by_name_with_the_cells_path() {
    let opaque = || Mp::Ext(OPAQUE, &[1]);
    let refusals = [
        // A typed column, nested and flat.
        (order(vec![("name", opaque())]), "value.name"),
        (
            Mp::Map(vec![("type", Mp::Str("order")), ("value.name", opaque())]),
            "value.name",
        ),
        // A member cell that is not dynamic.
        (order(vec![("meta", opaque())]), "value.meta"),
        // A record member that is not dynamic.
        (order(vec![("note", opaque())]), "value.note"),
        // The carrier of undeclared members.
        (order(vec![("loose", opaque())]), "value.loose"),
        // Inside a value the lane parses, even in a dynamic cell.
        (
            order(vec![("data", Mp::Map(vec![("inner", opaque())]))]),
            "value.data",
        ),
    ];
    for (event, path) in refusals {
        assert_eq!(
            stored(Some(OPAQUE), &[event]),
            Err(ExtractionError::OpaqueValue { path: path.into() }),
            "{path}"
        );
    }
}

#[test]
fn an_extension_is_read_as_before_where_no_type_is_reserved() {
    let row = |cells: [&str; 2]| cells.map(str::to_owned);
    assert_eq!(
        stored(None, &accepted()),
        Ok(vec![
            row(["member 1 unknown", "[5]@9=unknown"]),
            row(["member 1 unknown", "[5]@9=unknown"]),
            row(["member 1 null", "[5]@9=null"]),
            row(["-", "-"]),
        ])
    );
    assert_eq!(
        stored(None, &[order(vec![("name", Mp::Ext(OPAQUE, &[1]))])]),
        Err(ExtractionError::InvalidFieldType)
    );
}

#[test]
fn creation_refuses_the_bigint_extension_and_a_cell_the_embedder_does_not_write() {
    let refused = |fields: &[SignalSchemaField],
                   names: &[&str],
                   cell_metadata: &[Option<String>],
                   opaque_ext: i8,
                   encoding: Option<&str>| {
        build_extraction_config_with_semantic_schemas(
            fields,
            names,
            &[],
            None,
            None,
            CellLayout {
                cell_metadata,
                dynamic_columns: &[1],
                opaque_ext: Some(opaque_ext),
                encoding,
                ..CellLayout::default()
            },
        )
        .err()
    };
    let cell = Some(ENCODING.to_owned());
    let invalid = Some(ConfigError::InvalidOpaqueExt);
    // The bigint extension keeps its meaning.
    assert_eq!(
        refused(
            &[utf8(), binary()],
            &["type", "value.data"],
            &[None, cell.clone()],
            20,
            Some(ENCODING)
        ),
        invalid
    );
    // No embedder takes an opaque value.
    assert_eq!(
        refused(
            &[utf8(), binary()],
            &["type", "value.data"],
            &[],
            OPAQUE,
            None
        ),
        invalid
    );
    // A carrier columine writes itself (here a Utf8 one, beside an
    // embedder's cells) would capture it unread.
    assert_eq!(
        refused(
            &[utf8(), binary(), utf8()],
            &["type", "value.data", "value.$undeclared"],
            &[None, cell, None],
            OPAQUE,
            Some(ENCODING)
        ),
        invalid
    );
}
//#endregion smoo/columine!n/vocab-value-cells
