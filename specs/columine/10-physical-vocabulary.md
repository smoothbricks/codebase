# The physical vocabulary

## 1. Scope

The parse plane turns a batch of events into Arrow IPC messages: `columine-arrow` holds the columns and writes the
messages, `columine-parsing` fills the columns from JSON and MessagePack, `columine-event-processor` composes them. This
spec states which Arrow types a column may have, how a schema describes them, which messages a stream holds, and the
three seams an embedder fills. A column's type is a function of its field's declaration alone: never of a batch's
values, and never of which rows were written.

## 2. Planes

A plane is one value layout: a width, a signedness and a buffer set. The plane table is declared once, in
`columine-arrow/src/schema.rs` (`ArrowType`), and its tags are frozen: a new plane appends.

| tags          | planes                                                   | Arrow types                                                                                                        |
| ------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| 0             | Null                                                     | Null                                                                                                               |
| 5             | Bool                                                     | Boolean                                                                                                            |
| 7, 8, 1, 6    | Int8, Int16, Int32, Int64                                | the integer of that width; the four-byte plane also Date32 and Time32, the eight-byte plane also Date64 and Time64 |
| 9, 10, 11, 12 | UInt8, UInt16, UInt32, UInt64                            | the unsigned integer of that width                                                                                 |
| 13, 14, 2     | Float16, Float32, Float64                                | the float of that width                                                                                            |
| 15, 16        | Decimal128, Decimal256                                   | a decimal that fits                                                                                                |
| 3, 17         | Binary, LargeBinary                                      | opaque bytes                                                                                                       |
| 4, 18         | Utf8, LargeUtf8                                          | text                                                                                                               |
| 19            | FixedSizeBinary                                          | `type_param` bytes per row                                                                                         |
| 20, 21, 22    | IntervalYearMonth, IntervalDayTime, IntervalMonthDayNano | the interval of that unit                                                                                          |
| 23            | Timestamp                                                | `Timestamp(unit, "UTC")`, `type_param` the unit                                                                    |
| 24            | Duration                                                 | `Duration(unit)`, `type_param` the unit                                                                            |

A unit is 0 seconds, 1 milliseconds, 2 microseconds, 3 nanoseconds. Every logical Arrow type belongs to exactly one
plane: a Timestamp or a Duration is never admitted on the Int64 plane, because its unit decides what a producer's value
means and so belongs in the descriptor. A timestamp without a zone, or with a zone other than `UTC`, has no plane.
Nested types have no plane.

## 3. The field descriptor

A schema is one Arrow IPC Schema message plus one descriptor per field. A descriptor is eight bytes on the wire:

```
[plane u8][nullable u8][type_param u16 LE][dictionary u16 LE][index_plane u8][0]
```

- `plane` is the plane of the field's values.
- `type_param` is the FixedSizeBinary width or a temporal plane's unit, and zero for every other plane.
- `dictionary` is 0 for a plain column. A nonzero value makes the column dictionary-encoded, as an Arrow field is: its
  values are of `plane` (Utf8 only) and its own buffers are indexes of `index_plane`, which is UInt8, UInt16 or UInt32
  (tag 9, 10 or 11) and zero for a plain column. Slot 1 is the schema's stream dictionary (§5); slot `2 + k` is schema
  dictionary `k` of the creation call's dictionary table (§4).

Creation checks each descriptor against its field of the Schema message (type, unit, zone, index type, nullability) and
refuses a schema where they disagree, so the descriptors retained for writing cannot differ from the schema copied to
every output.

A slot is not an Arrow dictionary id. The Schema message's writer numbers its dictionaries, and creation reads each
encoded field's id from the message. Fields of one slot may share one id or have one each; an id used under two slots is
refused. A dictionary's message is written once per id of its slot, so a Schema message that shares the id pays for one.

**The buffer budget.** A field contributes one field node and the buffers its own layout declares (none for Null,
validity and data for a fixed-width or encoded column, validity, offsets and data for a variable-width one). Retained
metadata and the caller-built batch validator are sized from the sum over the schema's fields. A dictionary's values are
not a field's buffers: they travel in their own message of one node and three buffers.

## 4. Schema dictionaries

A schema dictionary has fixed values, supplied at creation in a table: per dictionary its values in index order, where
an entry may be null for an index the declaration skips. Creation refuses a table entry no field names, a field naming
an entry the table lacks, and an index plane too narrow for the table's values.

The stream preamble is the Schema message followed by one DictionaryBatch message per schema dictionary. It is built
once at creation and copied to the head of every stream, and it is what a store keeps as the schema. A parse lane
resolves a value of an encoded column by the dictionary: the text of a value the dictionary holds stores its index, and
any other text is refused naming the column.

## 5. The stream dictionary

The stream dictionary's values come from the data and belong to whoever owns the stream: an ordered list of distinct
texts in which a value's ordinal is its position and never changes. The owner hands it to a parse call as a
`CategoryImage` (`columine-types`):

- `ordinal(value)` answers the value's ordinal, appending the value when the image does not hold it; a value it holds
  changes nothing. It refuses with `Full { limit }` or `TooLong { bytes, max }`.
- `len()` is how many values the image holds; `value(ordinal)` is the text at an ordinal.

A lane stores the ordinal in the column's index plane, so the column store holds exactly the Arrow buffer. A parsed
batch records the image's length when it began (its mark). Encoding writes, before the record batch, one DictionaryBatch
of the values from the mark on, and none when the batch added none. The message is a delta exactly when the mark is
above zero: a dictionary's first values define it, so the messages of a stream from its first batch on are one valid
Arrow stream, and a reader that starts later is first given the image as one DictionaryBatch. Undoing a refused batch,
by dropping the ordinals from its mark on, is the owner's.

A caller-built batch (§8) stores ordinals its caller obtained; it names its mark, and its ordinals are checked against
the image's length.

## 6. Field identity

An Arrow field of the Schema message may carry the metadata key `sb.fields`: a JSON array with one object per declared
field the column holds, `{ "lib": text, "n": integer, "name": text, "type": text }`: the declaring library, the field's
number in it, its local name and the digest of its type. The Schema message is copied to output unchanged. Creation
decodes the key and refuses a value that is not such an array, a column whose entries carry two type digests, one
`(lib, n)` twice in a column, and one `(lib, n)` on two columns. A name is display: nothing resolves a field by it.

## 7. Value cells <a id="smoo/columine!n/vocab-value-cells"></a>

Four cells of a row hold bytes the plane does not interpret: a structured member's own Binary column, the record of the
row's declared members that have no column (`value.$declared`), the carrier of its undeclared members
(`value.$undeclared`), and a text member's content hash. Who computes them is the embedder's choice, made when the
processor is created.

**With an embedder's `ValueCells`** (`columine-types`), a lane hands it each value as a `ValueView`, after the semantic
schema has judged that value, and stores the bytes it writes:

- `member(column, value, out)` writes a structured member's cell;
- `begin_record`, `put(row, value)` per declared member without a column, `finish_record(out)` write the row's record,
  null when no member was put. `row` is the routing row creation received for `(event type, member)`: the member's
  field-number path and its descriptor position;
- `begin_undeclared`, `put_undeclared(path, value)`, `finish_undeclared(out)` write the row's carrier;
- `text(hash_column, text, hash)` computes a text's 32-byte content hash and answers whether its bytes are also stored
  inline, in the text's Utf8 column; `text_by_hash(hash_column, hash)` answers for a text that arrived as its hash
  alone. Which columns are a text's is a creation fact, rows of `(hash column, inline column?)`; nothing reads it from a
  column's name.

`ValueCells::encoding()` names the encoding of the Binary cells, and every Binary field the embedder writes says the
same name in its `sb.cell` field metadata. A Binary field without `sb.cell` holds raw bytes. A reader learns a cell's
encoding from the schema and never from its bytes; creation refuses a Binary field whose `sb.cell` names an encoding the
processor does not write. `prepare(target)` is called once per target at creation, so a schema the embedder cannot serve
yields no processor.

**With no embedder**, columine's native path writes MessagePack into a structured member's column and the carrier
(`sb.cell` absent or `msgpack`). This is columine's own standalone form, not any embedder's stored form. It has no
record and no text: a schema with a routing row or a text column is refused at creation.

**Placement is total.** Where a schema gives payload members columns of their own (a `value.<member>` column exists),
every top-level member a semantic schema declares has a column or a routing row, or creation refuses the schema. A
declared member is never dropped and never falls to the carrier. A schema that stores the whole payload in one `value`
column places every member there. A routing row's discriminant is the event's `type`, and a schema with routing rows has
a `value.$declared` column the embedder writes.

An embedder processor takes the extraction path only. A value-object frame certified by the admission capability is
written to a raw Binary column as its certified bytes; a frame nested inside a member the embedder writes has no cell of
its own and refuses the row.

## 8. Coercion <a id="smoo/columine!n/vocab-coercion"></a>

One table for both lanes; a value of another kind is refused naming the column.

| column                   | takes                                                                                          |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| integer plane            | an integral number in range; at eight bytes also a decimal string                              |
| float plane              | a number                                                                                       |
| Bool                     | a boolean                                                                                      |
| Timestamp                | an integer of the field's unit, or the text of an RFC 3339 UTC instant no finer than the unit  |
| Duration                 | an integer of the field's unit                                                                 |
| schema dictionary        | the text of one of its values                                                                  |
| stream dictionary        | a text, stored as the ordinal the image answers                                                |
| Utf8                     | a text                                                                                         |
| fixed-size planes        | bytes of exactly the width; on the JSON wire their hex                                         |
| Binary without `sb.cell` | raw bytes, or a certified frame for a value object; with no embedder, any value as MessagePack |
| Binary with `sb.cell`    | any value, as the bytes that encoding's writer produces                                        |
| text hash column         | a text, stored as the hash the embedder computes; on a wire with bytes also a 32-byte hash     |

**A bounded string.** <a id="smoo/columine!n/vocab-bounded-string"></a> A semantic `string` node may bound a text's
UTF-8 length: `{"kind":"string","maxBytes":N}`, `N` a `u32`, absent = unbounded. A longer text is a payload violation
judged before extraction on every lane, expected `string of at most N bytes`, observed `string of M bytes`; a member the
node does not define refuses the schema. The Utf8 plane itself has no bound. **The instant grammar.**
<a id="smoo/columine!n/vocab-instant-grammar"></a> One reader, `columine_parsing::instant`, reads an instant's text for
every lane and for any host that names it: RFC 3339 with an uppercase `T`, a four-digit year 0000 to 9999, a real
calendar date, hours 00 to 23, minutes and seconds 00 to 59 (no leap second), an optional fraction of one or more
digits, and `Z` or a `+hh:mm` / `-hh:mm` offset with hours to 23 and minutes to 59. Nothing else: no lowercase letters,
no space, no basic format, no missing offset, no trailing text. Its value is the exact count of nanoseconds since the
Unix epoch; a fraction digit past the nanosecond must be zero. Into a column of a unit, a non-zero digit finer than the
unit is refused, never truncated, and so is an instant the unit's `i64` cannot hold. The vectors every reader of the
grammar shares are `columine-parsing/test-vectors/instant-grammar.json`. A caller-built batch carries each column in its
storage plane (an encoded column as its index plane) and is checked against the same schema: lengths, offsets, text, and
every index against its dictionary's length.

## 9. The generated host table

`packages/columine/src/event-processor-abi.generated.ts` is written from the Rust definitions (the plane table with each
plane's carriers, the descriptor layout, limits, status and creation-failure codes, the caller-built batch header, the
unit codes, the metadata keys) by `columine-event-processor/tests/host_abi.rs` under `UPDATE_GENERATED=1`, and that test
fails when the committed file differs. No host number is typed by hand.

## 10. Behavioral acceptance

`columine-arrow/tests/vocabulary.rs`, `columine-event-processor/tests/vocabulary.rs`,
`columine-event-processor/tests/value_cells.rs` and `columine-event-processor/tests/host_abi.rs` cover, each group of
tests fenced with the id of its line:

- <a id="smoo/columine!n/vocab-temporal-planes"></a> a Timestamp or Duration field refused under the Int64 tag and
  created under its own, a zone-less timestamp refused;
- <a id="smoo/columine!n/vocab-temporal-instant"></a> an RFC 3339 instant stored in the column's unit, refused when
  finer than the unit, refused by an Int64 column;
- <a id="smoo/columine!n/vocab-enum-dictionary"></a> an encoded column read back by `arrow-ipc` as a dictionary array of
  the declared values, an unknown value refused;
- <a id="smoo/columine!n/vocab-stream-dictionary"></a> three batches through one image: the first stream defines the
  dictionary, the second carries only what it added as a delta, the third carries no dictionary message, and a reader
  given the image reads each batch's texts;
- <a id="smoo/columine!n/vocab-image-limit"></a> a value past the image's limit refusing the batch;
- <a id="smoo/columine!n/vocab-buffer-budget"></a> a schema of 512 encoded columns over one dictionary created and
  encoded;
- <a id="smoo/columine!n/vocab-compact-index"></a> a caller-built batch whose index is past its dictionary refused;
- <a id="smoo/columine!n/vocab-field-identity"></a> a column with two type digests, and one identity named twice, in one
  column or on two, refused at creation;
- <a id="smoo/columine!n/vocab-value-cells"></a> a declared member with no column written to the record through the
  seam, creation refused when no embedder is injected, creation refused for a declared member with neither a column nor
  a routing row, and a text stored as the embedder's hash with its bytes inline when the embedder says so;
- <a id="smoo/columine!n/vocab-lanes-agree"></a> the JSON and MessagePack lanes producing the same stream for one
  logical batch over every new plane;
- <a id="smoo/columine!n/vocab-host-table"></a> the generated host table equal to what the Rust definitions render.
