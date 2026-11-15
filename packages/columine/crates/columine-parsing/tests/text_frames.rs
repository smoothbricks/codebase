//! A text member that arrives as bytes on the MessagePack lane
//! (specs/columine/10-physical-vocabulary.md §7 "With an embedder's
//! `ValueCells`"): 32 bytes are its hash, and any other bytes are handed to
//! the embedder, which alone decides whether they are a frame it admits.

use columine_arrow::TextColumn;
use columine_parsing::json_extractor::ExtractionError;
use columine_parsing::msgpack_extractor::extract_msgpack_events_with_projections;
use columine_parsing::validate::ValueView;
use columine_parsing::{
    ArrowType, CellLayout, CellWriter, ColumnValue, DynamicColumns, FrameProjections,
    SignalSchemaField, build_extraction_config_with_semantic_schemas, read_cell,
};
use columine_types::category::NoCategories;
use columine_types::cells::{
    CellRefusal, CellTarget, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
};

//#region smoo/columine!n/vocab-value-cells
const ENCODING: &str = "test-cells";

/// The test embedder's frame of a text: `F:` and the text. The text's hash is
/// its length in every byte, and `keep` is kept inline.
#[derive(Default)]
struct FrameCells {
    /// Every hash `text_by_hash` was asked for.
    by_hash: Vec<[u8; TEXT_HASH_BYTES]>,
}

fn place(text: &[u8], hash: &mut [u8; TEXT_HASH_BYTES]) -> TextPlace {
    *hash = [u8::try_from(text.len()).unwrap(); TEXT_HASH_BYTES];
    if text == b"keep" {
        TextPlace::Inline
    } else {
        TextPlace::HashOnly
    }
}

impl ValueCells for FrameCells {
    fn encoding(&self) -> &'static str {
        ENCODING
    }

    fn prepare(&mut self, _target: CellTarget<'_>) -> Result<(), CellRefusal> {
        Ok(())
    }

    fn member(
        &mut self,
        _column: u32,
        _value: &dyn ValueView,
        _out: &mut Vec<u8>,
    ) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn begin_record(&mut self) {}

    fn put(&mut self, _row: &RecordRow<'_>, _value: &dyn ValueView) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn finish_record(&mut self, _out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        Ok(false)
    }

    fn begin_undeclared(&mut self) {}

    fn put_undeclared(&mut self, _path: &str, _value: &dyn ValueView) -> Result<(), CellRefusal> {
        Err(CellRefusal::Unsupported)
    }

    fn finish_undeclared(&mut self, _out: &mut Vec<u8>) -> Result<bool, CellRefusal> {
        Ok(false)
    }

    fn text(
        &mut self,
        _hash_column: u32,
        value: &str,
        hash: &mut [u8; TEXT_HASH_BYTES],
    ) -> Result<TextPlace, CellRefusal> {
        Ok(place(value.as_bytes(), hash))
    }

    fn text_by_hash(
        &mut self,
        _hash_column: u32,
        hash: &[u8; TEXT_HASH_BYTES],
    ) -> Result<(), CellRefusal> {
        self.by_hash.push(*hash);
        Ok(())
    }

    fn text_frame(
        &mut self,
        _hash_column: u32,
        bytes: &[u8],
        hash: &mut [u8; TEXT_HASH_BYTES],
        inline: &mut Vec<u8>,
    ) -> Result<TextPlace, CellRefusal> {
        let text = bytes.strip_prefix(b"F:").ok_or(CellRefusal::Refused {
            code: "not-a-text-frame",
        })?;
        let placed = place(text, hash);
        if placed == TextPlace::Inline {
            inline.extend(text);
        }
        Ok(placed)
    }
}

const HASH: usize = 1;
const INLINE: usize = 2;

fn fields() -> [SignalSchemaField; 3] {
    [
        SignalSchemaField::new(ArrowType::Utf8, true),
        SignalSchemaField::new_fixed_size_binary(32, true),
        SignalSchemaField::new(ArrowType::Utf8, true),
    ]
}

/// One event per `note`, each a MessagePack `bin` or `str` as tagged.
enum Note<'a> {
    Bin(&'a [u8]),
    Str(&'a str),
}

fn events(notes: &[Note<'_>]) -> Vec<u8> {
    let short = |len: usize| u8::try_from(len).unwrap();
    let mut out = vec![0x90 | short(notes.len())];
    for note in notes {
        // {"type": "order", "value": {"note": <note>}}
        out.extend([0x82, 0xa4]);
        out.extend(b"type");
        out.push(0xa5);
        out.extend(b"order");
        out.push(0xa5);
        out.extend(b"value");
        out.extend([0x81, 0xa4]);
        out.extend(b"note");
        match note {
            Note::Bin(bytes) => {
                out.extend([0xc4, short(bytes.len())]);
                out.extend(*bytes);
            }
            Note::Str(text) => {
                out.push(0xa0 | short(text.len()));
                out.extend(text.as_bytes());
            }
        }
    }
    out
}

/// The hash and inline cells of every row the lane stores from `notes`, and
/// the hashes the embedder was asked for.
type Stored = (
    Vec<[Option<ColumnValue<'static>>; 2]>,
    Vec<[u8; TEXT_HASH_BYTES]>,
);

fn store(notes: &[Note<'_>]) -> Result<Stored, ExtractionError> {
    let fields = fields();
    let config = build_extraction_config_with_semantic_schemas(
        &fields,
        &["type", "value.note", "note_inline"],
        &[],
        None,
        None,
        CellLayout {
            text_columns: &[TextColumn {
                hash: HASH,
                inline: Some(INLINE),
            }],
            encoding: Some(ENCODING),
            ..CellLayout::default()
        },
    )
    .expect("a text's hash and inline columns");
    let mut columns = DynamicColumns::new(&fields, 8).expect("column plane fits");
    let mut embedder = FrameCells::default();
    let rows = extract_msgpack_events_with_projections(
        &events(notes),
        &config,
        &mut columns,
        &mut [0; 64],
        false,
        FrameProjections::default(),
        &mut NoCategories,
        &mut CellWriter::Embedder(&mut embedder),
        &mut Vec::new(),
    )?;
    Ok((
        (0..rows)
            .map(|row| [HASH, INLINE].map(|column| read_cell(&columns, column, row)))
            .collect(),
        embedder.by_hash,
    ))
}

fn hash(byte: u8) -> Option<ColumnValue<'static>> {
    Some(ColumnValue::FixedBytes(vec![byte; TEXT_HASH_BYTES].into()))
}

fn text(text: &str) -> Option<ColumnValue<'static>> {
    Some(ColumnValue::Utf8(text.to_owned().into()))
}

#[test]
fn bytes_that_are_no_hash_are_handed_to_the_embedder_as_the_texts_frame() {
    // 32 bytes are a hash even when they spell a frame.
    let mut frame_shaped_hash = [b'x'; TEXT_HASH_BYTES];
    frame_shaped_hash[..2].copy_from_slice(b"F:");
    assert_eq!(
        store(&[
            Note::Bin(b"F:keep"),
            Note::Bin(b"F:dropped"),
            Note::Bin(b"F:"),
            Note::Bin(&frame_shaped_hash),
            Note::Str("keep"),
        ]),
        Ok((
            vec![
                [hash(4), text("keep")],
                [hash(7), None],
                [hash(0), None],
                [
                    Some(ColumnValue::FixedBytes(frame_shaped_hash.to_vec().into())),
                    None
                ],
                [hash(4), text("keep")],
            ],
            vec![frame_shaped_hash],
        )),
        "a frame kept inline, a frame kept by hash, an empty text's frame, a hash, a text"
    );
}

#[test]
fn bytes_the_embedder_does_not_admit_are_refused_by_its_name() {
    for bytes in [&b"keep"[..], b"", &[7; TEXT_HASH_BYTES + 1]] {
        assert_eq!(
            store(&[Note::Bin(bytes)]),
            Err(ExtractionError::Cell(CellRefusal::Refused {
                code: "not-a-text-frame"
            })),
            "{bytes:?}"
        );
    }
}
//#endregion smoo/columine!n/vocab-value-cells
