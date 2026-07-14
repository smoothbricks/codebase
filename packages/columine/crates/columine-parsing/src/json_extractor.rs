//! Replaces `packages/columine/src/parsing/json_extractor.zig`.

use crate::{
    ArrowType, ColumnValue, DynamicColumns, ExtractionConfig, ParseError,
    json_parser::{JsonParser, Token},
};
use columine_arrow::VariableValueReservation;

pub use crate::{build_extraction_config, free_extraction_config};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtractionError {
    InvalidJson,
    InvalidFieldType,
    BufferOverflow,
    MsgpackError,
    TooManyEvents,
    OutOfMemory,
}

/// `json_extractor.zig NO_FIELD`.
pub const NO_FIELD: u16 = 0xFFFF;

/// Diagnostic stage bytes (`ExtractionDiagnostic.Stage` — order is ABI,
/// decoded by lib.ts `DIAGNOSTIC_STAGES`).
pub mod diagnostic_stage {
    pub const NONE: u8 = 0;
    pub const JSON: u8 = 1;
    pub const VALUE: u8 = 2;
    pub const MSGPACK: u8 = 3;
    pub const COLUMN: u8 = 4;
    pub const SCHEMA: u8 = 5;
}

/// Diagnostic detail bytes (`ExtractionDiagnostic.Detail` — order is ABI,
/// decoded by lib.ts `DIAGNOSTIC_DETAILS`).
pub mod diagnostic_detail {
    pub const NONE: u8 = 0;
    pub const INVALID_JSON: u8 = 1;
    pub const TYPE_MISMATCH: u8 = 2;
    pub const INVALID_NUMBER: u8 = 3;
    pub const BUFFER_OVERFLOW: u8 = 4;
    pub const TOO_MANY_FIELDS: u8 = 5;
    pub const TOO_MANY_EVENTS: u8 = 6;
    pub const OUT_OF_MEMORY: u8 = 7;
}

/// JSON value-kind bytes (`JsonValueType` — order is ABI, decoded by lib.ts
/// `JSON_VALUE_TYPES`).
pub mod json_value_type {
    pub const UNKNOWN: u8 = 0;
    pub const NULL: u8 = 1;
    pub const STRING: u8 = 2;
    pub const NUMBER: u8 = 3;
    pub const BOOL: u8 = 4;
    pub const OBJECT: u8 = 5;
    pub const ARRAY: u8 = 6;
}

/// Per-field extraction diagnostic (`json_extractor.zig ExtractionDiagnostic`)
/// — populated at the failure site so the EP header can report the exact
/// stage/field/type/row instead of a coarse derivation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtractionDiagnostic {
    pub stage: u8,
    pub detail: u8,
    pub field_index: u16,
    pub expected_type: u8,
    pub actual_type: u8,
    pub row_index: u16,
}

impl Default for ExtractionDiagnostic {
    fn default() -> Self {
        Self {
            stage: diagnostic_stage::NONE,
            detail: diagnostic_detail::NONE,
            field_index: NO_FIELD,
            expected_type: 0,
            actual_type: json_value_type::UNKNOWN,
            row_index: 0,
        }
    }
}

impl ExtractionDiagnostic {
    #[allow(clippy::too_many_arguments)]
    fn set(&mut self, stage: u8, detail: u8, field_index: u16, expected: u8, actual: u8, row: u32) {
        *self = Self {
            stage,
            detail,
            field_index,
            expected_type: expected,
            actual_type: actual,
            row_index: row.min(u16::MAX as u32) as u16,
        };
    }

    /// Zig `completeDiagnostic`: derive stage/detail from the error only if
    /// no failure site filled them in.
    pub fn complete(&mut self, failure: &ExtractionError) {
        if self.stage != diagnostic_stage::NONE {
            return;
        }
        self.stage = match failure {
            ExtractionError::InvalidJson => diagnostic_stage::JSON,
            ExtractionError::InvalidFieldType => diagnostic_stage::VALUE,
            ExtractionError::MsgpackError => diagnostic_stage::MSGPACK,
            ExtractionError::BufferOverflow
            | ExtractionError::TooManyEvents
            | ExtractionError::OutOfMemory => diagnostic_stage::COLUMN,
        };
        self.detail = match failure {
            ExtractionError::InvalidJson => diagnostic_detail::INVALID_JSON,
            ExtractionError::InvalidFieldType => diagnostic_detail::TYPE_MISMATCH,
            ExtractionError::BufferOverflow => diagnostic_detail::BUFFER_OVERFLOW,
            ExtractionError::MsgpackError => diagnostic_detail::INVALID_JSON,
            ExtractionError::TooManyEvents => diagnostic_detail::TOO_MANY_EVENTS,
            ExtractionError::OutOfMemory => diagnostic_detail::OUT_OF_MEMORY,
        };
    }
}

/// Zig `jsonValueType`.
fn json_value_type_of(token: &Token) -> u8 {
    match token {
        Token::Null => json_value_type::NULL,
        Token::String(_) => json_value_type::STRING,
        Token::Number(_) => json_value_type::NUMBER,
        Token::True | Token::False => json_value_type::BOOL,
        Token::ObjectBegin => json_value_type::OBJECT,
        Token::ArrayBegin => json_value_type::ARRAY,
        _ => json_value_type::UNKNOWN,
    }
}

/// Zig `columnFailure`: column-stage diagnostic with detail from the
/// storage error.
fn column_failure(
    diagnostic: &mut ExtractionDiagnostic,
    col_idx: u32,
    arrow_type: ArrowType,
    actual: u8,
    row: u32,
    failure: ParseError,
) -> ExtractionError {
    let (detail, error) = match failure {
        ParseError::BufferOverflow => (
            diagnostic_detail::BUFFER_OVERFLOW,
            ExtractionError::BufferOverflow,
        ),
        ParseError::OutOfMemory => (
            diagnostic_detail::OUT_OF_MEMORY,
            ExtractionError::OutOfMemory,
        ),
        _ => (
            diagnostic_detail::TYPE_MISMATCH,
            ExtractionError::InvalidFieldType,
        ),
    };
    diagnostic.set(
        diagnostic_stage::COLUMN,
        detail,
        col_idx as u16,
        arrow_type as u8,
        actual,
        row,
    );
    error
}

/// Zig `variableValueFailure` (reservation/commit errors on a declared
/// column).
fn variable_value_failure(
    diagnostic: &mut ExtractionDiagnostic,
    col_idx: u32,
    arrow_type: ArrowType,
    actual: u8,
    row: u32,
    failure: columine_arrow::VariableValueError,
) -> ExtractionError {
    let parse_error = match failure {
        columine_arrow::VariableValueError::InvalidFieldType => ParseError::InvalidFieldType,
        columine_arrow::VariableValueError::BufferOverflow => ParseError::BufferOverflow,
        columine_arrow::VariableValueError::OutOfMemory => ParseError::OutOfMemory,
    };
    column_failure(diagnostic, col_idx, arrow_type, actual, row, parse_error)
}

/// Zig `msgpackFailure` ($extra workspace writer errors — msgpack stage, so
/// the EP growth loop can key on exactly this).
fn msgpack_failure(
    diagnostic: &mut ExtractionDiagnostic,
    col_idx: u32,
    actual: u8,
    row: u32,
    failure: ExtractionError,
) -> ExtractionError {
    let (detail, error) = match failure {
        ExtractionError::BufferOverflow => (
            diagnostic_detail::BUFFER_OVERFLOW,
            ExtractionError::BufferOverflow,
        ),
        ExtractionError::MsgpackError => (
            diagnostic_detail::INVALID_NUMBER,
            ExtractionError::InvalidFieldType,
        ),
        ExtractionError::OutOfMemory => (
            diagnostic_detail::OUT_OF_MEMORY,
            ExtractionError::OutOfMemory,
        ),
        _ => (
            diagnostic_detail::INVALID_JSON,
            ExtractionError::InvalidJson,
        ),
    };
    diagnostic.set(
        diagnostic_stage::MSGPACK,
        detail,
        col_idx as u16,
        ArrowType::Binary as u8,
        actual,
        row,
    );
    error
}

/// Zig `directMsgpackFailure` (declared-Binary writer errors: capacity
/// failures are COLUMN failures — the sink is column storage — while
/// malformed input stays msgpack/json).
fn direct_msgpack_failure(
    diagnostic: &mut ExtractionDiagnostic,
    col_idx: u32,
    actual: u8,
    row: u32,
    failure: ExtractionError,
) -> ExtractionError {
    match failure {
        ExtractionError::BufferOverflow => column_failure(
            diagnostic,
            col_idx,
            ArrowType::Binary,
            actual,
            row,
            ParseError::BufferOverflow,
        ),
        ExtractionError::OutOfMemory => column_failure(
            diagnostic,
            col_idx,
            ArrowType::Binary,
            actual,
            row,
            ParseError::OutOfMemory,
        ),
        other => msgpack_failure(diagnostic, col_idx, actual, row, other),
    }
}

pub fn extract_json_events(
    input: &[u8],
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    diagnostic: &mut ExtractionDiagnostic,
) -> Result<usize, ExtractionError> {
    *diagnostic = ExtractionDiagnostic::default();
    let mut parser = JsonParser::new(input);
    if parser.expect_array_begin().is_err() {
        diagnostic.set(
            diagnostic_stage::JSON,
            diagnostic_detail::INVALID_JSON,
            NO_FIELD,
            0,
            json_value_type::UNKNOWN,
            0,
        );
        return Err(ExtractionError::InvalidJson);
    }
    let mut count = 0;
    while !parser.is_array_end() {
        if let Err(err) = extract_json_event(&mut parser, config, columns, work_buffer, diagnostic)
        {
            diagnostic.row_index = count.min(u16::MAX as usize) as u16;
            diagnostic.complete(&err);
            return Err(err);
        }
        count += 1;
        // ZIG-PARITY: an exactly-capacity batch is rejected (`count >= capacity` runs after every event); intended fix: accept full batches, resolved at the event_processor level.
        // Zig checks `count >= capacity` unconditionally after each event, so
        // an exactly-capacity batch is rejected too. Mirrored bug-for-bug —
        // the off-by-one is flagged for the stage-4B/event-processor audit.
        if count >= columns.capacity as usize {
            return Err(ExtractionError::TooManyEvents);
        }
    }
    parser
        .next_token()
        .map_err(|_| ExtractionError::InvalidJson)?;
    Ok(count)
}

pub fn extract_json_event(
    parser: &mut JsonParser<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    diagnostic: &mut ExtractionDiagnostic,
) -> Result<(), ExtractionError> {
    if !columns.begin_row() {
        diagnostic.set(
            diagnostic_stage::COLUMN,
            diagnostic_detail::TOO_MANY_EVENTS,
            NO_FIELD,
            0,
            json_value_type::UNKNOWN,
            columns.count,
        );
        return Err(ExtractionError::TooManyEvents);
    }
    let result = extract_json_event_open(parser, config, columns, work_buffer, diagnostic);
    if result.is_err() {
        // Zig abandons the half-built row on error (endRow is never reached);
        // committing a partial row here would corrupt the batch.
        columns.abandon_row();
    }
    result
}
fn extract_json_event_open(
    parser: &mut JsonParser<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    diagnostic: &mut ExtractionDiagnostic,
) -> Result<(), ExtractionError> {
    if parser.expect_object_begin().is_err() {
        diagnostic.set(
            diagnostic_stage::JSON,
            diagnostic_detail::INVALID_JSON,
            NO_FIELD,
            0,
            json_value_type::UNKNOWN,
            columns.count,
        );
        return Err(ExtractionError::InvalidJson);
    }
    // Schema-sized presence tracking retained by DynamicColumns
    // (json_extractor.zig `columns_seen` — a fixed 64-column array here once
    // failed every schema wider than 64 fields, found by the TS
    // buffer-lifecycle suite).
    columns.columns_seen.fill(false);
    // The writer wraps the buffer up front, but a too-small buffer only
    // becomes an error on the first undeclared field, exactly like Zig's
    // lazy `MsgpackValueWriter.init(...) orelse return error.BufferOverflow`.
    let mut extra = MsgpackValueWriter::new(work_buffer);
    let mut extra_count = 0;
    while !parser.is_object_end() {
        let name = parser
            .expect_field_name()
            .map_err(|_| ExtractionError::InvalidJson)?;
        if let Some(lookup) = config.field_map.get(&name) {
            if let Err(err) = extract_typed_value(
                parser,
                lookup.arrow_type,
                columns,
                lookup.column,
                diagnostic,
            ) {
                // Zig backfills field/expected/row on any typed-value error
                // whose site did not set them.
                if diagnostic.field_index == NO_FIELD {
                    diagnostic.field_index = lookup.column as u16;
                }
                if diagnostic.expected_type == 0 {
                    diagnostic.expected_type = lookup.arrow_type as u8;
                }
                diagnostic.row_index = columns.count.min(u16::MAX as u32) as u16;
                diagnostic.complete(&err);
                return Err(err);
            }
            columns.columns_seen[lookup.column] = true;
        } else if let Some(fallback) = config.fallback_column {
            let row = columns.count;
            let Some(writer) = extra.as_mut() else {
                diagnostic.set(
                    diagnostic_stage::MSGPACK,
                    diagnostic_detail::BUFFER_OVERFLOW,
                    fallback as u16,
                    ArrowType::Binary as u8,
                    json_value_type::UNKNOWN,
                    row,
                );
                return Err(ExtractionError::BufferOverflow);
            };
            if extra_count == 0 {
                writer.reserve_map32().map_err(|err| {
                    msgpack_failure(
                        diagnostic,
                        fallback as u32,
                        json_value_type::UNKNOWN,
                        row,
                        err,
                    )
                })?;
            }
            writer.write_string(&name).map_err(|err| {
                msgpack_failure(
                    diagnostic,
                    fallback as u32,
                    json_value_type::STRING,
                    row,
                    err,
                )
            })?;
            let Ok(token) = parser.next_token() else {
                diagnostic.set(
                    diagnostic_stage::JSON,
                    diagnostic_detail::INVALID_JSON,
                    fallback as u16,
                    ArrowType::Binary as u8,
                    json_value_type::UNKNOWN,
                    row,
                );
                return Err(ExtractionError::InvalidJson);
            };
            let actual = json_value_type_of(&token);
            writer
                .write_value(parser, token)
                .map_err(|err| msgpack_failure(diagnostic, fallback as u32, actual, row, err))?;
            extra_count += 1;
        } else {
            parser
                .skip_value()
                .map_err(|_| ExtractionError::InvalidJson)?;
        }
    }
    parser
        .next_token()
        .map_err(|_| ExtractionError::InvalidJson)?;
    for (column, _, _) in &config.field_entries {
        if !columns.columns_seen[*column] {
            append(columns, *column, None)?;
        }
    }
    if let Some(column) = config.fallback_column {
        match extra.as_mut() {
            Some(writer) if extra_count > 0 => {
                let row = columns.count;
                let bytes = writer.finish_map32(extra_count)?;
                // Zig appends straight from the workspace slice
                // (`appendBinary(fallback_idx, msgpack_bytes)`) — no
                // intermediate materialization.
                if let Err(err) = columns.append_binary(column as u32, bytes) {
                    return Err(column_failure(
                        diagnostic,
                        column as u32,
                        ArrowType::Binary,
                        json_value_type::OBJECT,
                        row,
                        err,
                    ));
                }
            }
            _ => append(columns, column, None)?,
        }
    }
    columns.end_row();
    Ok(())
}

pub(crate) fn extract_typed_value(
    parser: &mut JsonParser<'_>,
    arrow_type: ArrowType,
    columns: &mut DynamicColumns,
    column: usize,
    diagnostic: &mut ExtractionDiagnostic,
) -> Result<(), ExtractionError> {
    let token = parser
        .next_token()
        .map_err(|_| ExtractionError::InvalidJson)?;
    let actual = json_value_type_of(&token);
    let row = columns.count;
    let value = match arrow_type {
        ArrowType::Utf8 => match token {
            Token::String(value) => Some(ColumnValue::Utf8(value)),
            Token::Null => None,
            _ => {
                return Err(invalid_field_type(
                    diagnostic,
                    column as u32,
                    arrow_type,
                    actual,
                    row,
                ));
            }
        },
        ArrowType::Int32 | ArrowType::Int64 => match token {
            Token::Number(value) => Some(ColumnValue::Int64(
                value
                    .parse::<i64>()
                    .or_else(|_| value.parse::<f64>().map(|v| v as i64))
                    .map_err(|_| {
                        invalid_number(diagnostic, column as u32, arrow_type, actual, row)
                    })?,
            )),
            Token::String(value) => Some(ColumnValue::Int64(
                value
                    .parse()
                    .or_else(|_| parse_timestamp_to_micros(&value).ok_or(()))
                    .map_err(|_| {
                        invalid_number(diagnostic, column as u32, arrow_type, actual, row)
                    })?,
            )),
            Token::Null => None,
            _ => {
                return Err(invalid_field_type(
                    diagnostic,
                    column as u32,
                    arrow_type,
                    actual,
                    row,
                ));
            }
        },
        ArrowType::Float64 => match token {
            Token::Number(value) => Some(ColumnValue::Float64(value.parse().map_err(|_| {
                invalid_number(diagnostic, column as u32, arrow_type, actual, row)
            })?)),
            Token::Null => None,
            _ => {
                return Err(invalid_field_type(
                    diagnostic,
                    column as u32,
                    arrow_type,
                    actual,
                    row,
                ));
            }
        },
        ArrowType::Bool => match token {
            Token::True => Some(ColumnValue::Bool(true)),
            Token::False => Some(ColumnValue::Bool(false)),
            Token::Null => None,
            _ => {
                return Err(invalid_field_type(
                    diagnostic,
                    column as u32,
                    arrow_type,
                    actual,
                    row,
                ));
            }
        },
        ArrowType::Binary => match token {
            Token::Null => None,
            token @ (Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::ObjectBegin
            | Token::ArrayBegin) => {
                // json_extractor.zig `.Binary`: msgpack-encode DIRECTLY into
                // the column's data buffer via a reservation (grows in place
                // up to the column byte limit; a 4 KiB scratch here once made
                // every larger declared-Binary value fail — found by the TS
                // buffer-lifecycle suite). Commit publishes offset/validity,
                // so a parse failure leaves the column logically unchanged.
                let res = columns.reserve_binary_value(column as u32).map_err(|err| {
                    variable_value_failure(diagnostic, column as u32, arrow_type, actual, row, err)
                })?;
                let mut writer = MsgpackValueWriter::new_column(columns, res).map_err(|err| {
                    direct_msgpack_failure(diagnostic, column as u32, actual, row, err)
                })?;
                writer.write_value(parser, token).map_err(|err| {
                    direct_msgpack_failure(diagnostic, column as u32, actual, row, err)
                })?;
                let written = writer.offset();
                res.commit(columns, written).map_err(|err| {
                    variable_value_failure(diagnostic, column as u32, arrow_type, actual, row, err)
                })?;
                return Ok(());
            }
            _ => {
                return Err(invalid_field_type(
                    diagnostic,
                    column as u32,
                    arrow_type,
                    actual,
                    row,
                ));
            }
        },
        ArrowType::Null => {
            // DELIBERATE DIVERGENCE from json_extractor.zig: the Zig code
            // appends null WITHOUT consuming a container value, leaving the
            // token stream desynchronized mid-object (a latent bug — its own
            // msgpack extractor DOES skip here). We skip the container so the
            // stream stays coherent; flagged in the README for the stage-4B
            // unification audit.
            if matches!(token, Token::ObjectBegin | Token::ArrayBegin) {
                parser
                    .skip_open_container(token)
                    .map_err(|_| ExtractionError::InvalidJson)?;
            }
            None
        }
    };
    append(columns, column, value).map_err(|err| {
        // Zig columnFailure on the append result — but only when a failure
        // site has not already claimed the diagnostic.
        if diagnostic.stage == diagnostic_stage::NONE {
            let parse_error = match err {
                ExtractionError::BufferOverflow => ParseError::BufferOverflow,
                ExtractionError::OutOfMemory => ParseError::OutOfMemory,
                _ => ParseError::InvalidFieldType,
            };
            return column_failure(
                diagnostic,
                column as u32,
                arrow_type,
                actual,
                row,
                parse_error,
            );
        }
        err
    })
}

/// Zig `invalidFieldType`.
fn invalid_field_type(
    diagnostic: &mut ExtractionDiagnostic,
    col_idx: u32,
    arrow_type: ArrowType,
    actual: u8,
    row: u32,
) -> ExtractionError {
    diagnostic.set(
        diagnostic_stage::VALUE,
        diagnostic_detail::TYPE_MISMATCH,
        col_idx as u16,
        arrow_type as u8,
        actual,
        row,
    );
    ExtractionError::InvalidFieldType
}

/// Zig `invalidNumber`.
fn invalid_number(
    diagnostic: &mut ExtractionDiagnostic,
    col_idx: u32,
    arrow_type: ArrowType,
    actual: u8,
    row: u32,
) -> ExtractionError {
    diagnostic.set(
        diagnostic_stage::VALUE,
        diagnostic_detail::INVALID_NUMBER,
        col_idx as u16,
        arrow_type as u8,
        actual,
        row,
    );
    ExtractionError::InvalidFieldType
}
/// Zig `initColumn`'s error mapping: reservation failures keep BufferOverflow
/// and OutOfMemory distinct; InvalidFieldType surfaces as InvalidJson.
fn variable_write_error(error: columine_arrow::VariableValueError) -> ExtractionError {
    match error {
        columine_arrow::VariableValueError::BufferOverflow => ExtractionError::BufferOverflow,
        columine_arrow::VariableValueError::OutOfMemory => ExtractionError::OutOfMemory,
        columine_arrow::VariableValueError::InvalidFieldType => ExtractionError::InvalidJson,
    }
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

/// Where the msgpack bytes land: the reusable `$extra` workspace (a plain
/// slice, EP-grown between retries) or a column-data reservation that grows
/// in place up to the column's byte limit (json_extractor.zig `initColumn` —
/// declared Binary values encode DIRECTLY into column storage, never through
/// a bounded scratch).
pub(crate) enum MsgpackSink<'a> {
    Slice(&'a mut [u8]),
    Column {
        cols: &'a mut DynamicColumns,
        res: VariableValueReservation,
    },
}

pub(crate) struct MsgpackValueWriter<'a> {
    sink: MsgpackSink<'a>,
    offset: usize,
}
impl<'a> MsgpackValueWriter<'a> {
    pub(crate) fn new(buffer: &'a mut [u8]) -> Option<Self> {
        (buffer.len() >= 5).then_some(Self {
            sink: MsgpackSink::Slice(buffer),
            offset: 0,
        })
    }
    /// Zig `MsgpackValueWriter.initColumn`: pre-grow 5 bytes, preserve none.
    pub(crate) fn new_column(
        cols: &'a mut DynamicColumns,
        res: VariableValueReservation,
    ) -> Result<Self, ExtractionError> {
        res.ensure_capacity_preserving(cols, 5, 0)
            .map_err(variable_write_error)?;
        Ok(Self {
            sink: MsgpackSink::Column { cols, res },
            offset: 0,
        })
    }
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }
    fn reserve(&mut self, bytes: usize) -> Result<(), ExtractionError> {
        let end = self
            .offset
            .checked_add(bytes)
            .ok_or(ExtractionError::BufferOverflow)?;
        match &mut self.sink {
            MsgpackSink::Slice(buffer) => {
                if end > buffer.len() {
                    return Err(ExtractionError::BufferOverflow);
                }
            }
            MsgpackSink::Column { cols, res } => {
                // Zig `ensureBytes`: grow to offset+n, preserving the bytes
                // written so far (growth reallocates; the tail is scratch).
                res.ensure_capacity_preserving(cols, end, self.offset)
                    .map_err(variable_write_error)?;
            }
        }
        Ok(())
    }
    fn buf(&mut self) -> &mut [u8] {
        match &mut self.sink {
            MsgpackSink::Slice(buffer) => buffer,
            MsgpackSink::Column { cols, res } => res.buffer(cols),
        }
    }
    pub(crate) fn written(&mut self) -> &[u8] {
        let offset = self.offset;
        &self.buf()[..offset]
    }
    pub(crate) fn reserve_map32(&mut self) -> Result<(), ExtractionError> {
        self.reserve(5)?;
        let offset = self.offset;
        self.buf()[offset] = 0xdf;
        self.offset += 5;
        Ok(())
    }
    pub(crate) fn finish_map32(&mut self, count: u32) -> Result<&[u8], ExtractionError> {
        if self.offset < 5 {
            return Err(ExtractionError::MsgpackError);
        }
        self.buf()[1..5].copy_from_slice(&count.to_be_bytes());
        Ok(self.written())
    }
    pub(crate) fn write_value(
        &mut self,
        parser: &mut JsonParser<'_>,
        token: Token,
    ) -> Result<(), ExtractionError> {
        match token {
            Token::String(value) => self.write_string(&value),
            Token::Number(value) => self.write_number(&value),
            Token::True => self.write_byte(0xc3),
            Token::False => self.write_byte(0xc2),
            Token::Null => self.write_byte(0xc0),
            Token::ObjectBegin => self.write_object(parser),
            Token::ArrayBegin => self.write_array(parser),
            Token::ObjectEnd | Token::ArrayEnd => Err(ExtractionError::InvalidJson),
        }
    }
    fn write_object(&mut self, parser: &mut JsonParser<'_>) -> Result<(), ExtractionError> {
        let header = self.offset;
        self.reserve_map32()?;
        let mut count: u32 = 0;
        while !parser.is_object_end() {
            self.write_string(
                &parser
                    .expect_field_name()
                    .map_err(|_| ExtractionError::InvalidJson)?,
            )?;
            let value = parser
                .next_token()
                .map_err(|_| ExtractionError::InvalidJson)?;
            self.write_value(parser, value)?;
            count += 1;
        }
        parser
            .next_token()
            .map_err(|_| ExtractionError::InvalidJson)?;
        self.buf()[header + 1..header + 5].copy_from_slice(&count.to_be_bytes());
        Ok(())
    }
    fn write_array(&mut self, parser: &mut JsonParser<'_>) -> Result<(), ExtractionError> {
        self.reserve(5)?;
        let header = self.offset;
        self.buf()[header] = 0xdd;
        self.offset += 5;
        let mut count: u32 = 0;
        while !parser.is_array_end() {
            let value = parser
                .next_token()
                .map_err(|_| ExtractionError::InvalidJson)?;
            self.write_value(parser, value)?;
            count += 1;
        }
        parser
            .next_token()
            .map_err(|_| ExtractionError::InvalidJson)?;
        self.buf()[header + 1..header + 5].copy_from_slice(&count.to_be_bytes());
        Ok(())
    }
    fn write_byte(&mut self, value: u8) -> Result<(), ExtractionError> {
        self.reserve(1)?;
        let offset = self.offset;
        self.buf()[offset] = value;
        self.offset += 1;
        Ok(())
    }
    fn write_number(&mut self, text: &str) -> Result<(), ExtractionError> {
        match text.parse::<i64>() {
            Ok(value) => self.write_integer(value),
            Err(_) => self.write_float(text.parse().map_err(|_| ExtractionError::MsgpackError)?),
        }
    }
    fn write_integer(&mut self, value: i64) -> Result<(), ExtractionError> {
        if value >= 0 {
            self.write_unsigned(value as u64)
        } else if value >= -32 {
            self.write_byte(value as i8 as u8)
        } else if value >= i64::from(i8::MIN) {
            self.write_byte(0xd0)?;
            self.write_byte(value as i8 as u8)
        } else if value >= i64::from(i16::MIN) {
            self.write_byte(0xd1)?;
            self.write_bytes(&(value as i16).to_be_bytes())
        } else if value >= i64::from(i32::MIN) {
            self.write_byte(0xd2)?;
            self.write_bytes(&(value as i32).to_be_bytes())
        } else {
            self.write_byte(0xd3)?;
            self.write_bytes(&value.to_be_bytes())
        }
    }
    fn write_unsigned(&mut self, value: u64) -> Result<(), ExtractionError> {
        if value <= 127 {
            self.write_byte(value as u8)
        } else if u8::try_from(value).is_ok() {
            self.write_byte(0xcc)?;
            self.write_byte(value as u8)
        } else if u16::try_from(value).is_ok() {
            self.write_byte(0xcd)?;
            self.write_bytes(&(value as u16).to_be_bytes())
        } else if u32::try_from(value).is_ok() {
            self.write_byte(0xce)?;
            self.write_bytes(&(value as u32).to_be_bytes())
        } else {
            self.write_byte(0xcf)?;
            self.write_bytes(&value.to_be_bytes())
        }
    }
    fn write_float(&mut self, value: f64) -> Result<(), ExtractionError> {
        self.write_byte(0xcb)?;
        self.write_bytes(&value.to_bits().to_be_bytes())
    }
    pub(crate) fn write_string(&mut self, value: &str) -> Result<(), ExtractionError> {
        let len = value.len();
        if len <= 31 {
            self.write_byte(0xa0 | len as u8)?;
        } else if u8::try_from(len).is_ok() {
            self.write_byte(0xd9)?;
            self.write_byte(len as u8)?;
        } else if u16::try_from(len).is_ok() {
            self.write_byte(0xda)?;
            self.write_bytes(&(len as u16).to_be_bytes())?;
        } else if u32::try_from(len).is_ok() {
            self.write_byte(0xdb)?;
            self.write_bytes(&(len as u32).to_be_bytes())?;
        } else {
            return Err(ExtractionError::BufferOverflow);
        }
        self.write_bytes(value.as_bytes())
    }
    fn write_bytes(&mut self, values: &[u8]) -> Result<(), ExtractionError> {
        self.reserve(values.len())?;
        let offset = self.offset;
        self.buf()[offset..offset + values.len()].copy_from_slice(values);
        self.offset += values.len();
        Ok(())
    }
}

/// Scalar ISO timestamp parser used for string-to-i64 extraction.
///
/// json_extractor.zig carries its OWN `parseTimestampToMicros`, which drifts
/// from json_scanner.zig's `parseIso8601ToMicros` in three observable ways,
/// all mirrored here:
/// - no year range check (year 0001 is accepted; the scanner rejects <1970),
/// - a fraction longer than 3 digits yields 0 ms (the scanner takes the
///   first three),
/// - a non-digit fraction of length 1..=3 rejects the whole timestamp (the
///   scanner's `catch 0` accepts it with 0 ms),
/// - fields are strict digits (the scanner's parseInt accepts a leading '+').
// ZIG-PARITY: this is one of two drifted timestamp parsers (extractor variant vs json_scanner::parse_iso8601_to_micros); intended fix: unify on one parser with deliberate fraction/sign semantics.
pub fn parse_timestamp_to_micros(value: &str) -> Option<i64> {
    let s = value.as_bytes();
    if s.len() < 20 || s[s.len() - 1] != b'Z' {
        return None;
    }
    let year = parse_digits(&s[0..4])?;
    if s[4] != b'-' {
        return None;
    }
    let month = parse_digits(&s[5..7])?;
    if s[7] != b'-' {
        return None;
    }
    let day = parse_digits(&s[8..10])?;
    if s[10] != b'T' {
        return None;
    }
    let hour = parse_digits(&s[11..13])?;
    if s[13] != b':' {
        return None;
    }
    let minute = parse_digits(&s[14..16])?;
    if s[16] != b':' {
        return None;
    }
    let second = parse_digits(&s[17..19])?;
    let mut millis: i64 = 0;
    if s.len() > 20 && s[19] == b'.' {
        let fraction = &s[20..s.len() - 1];
        if (1..=3).contains(&fraction.len()) {
            millis = parse_digits(fraction)?;
            for _ in fraction.len()..3 {
                millis *= 10;
            }
        }
        // Zig: fraction length 0 or >3 leaves millis at 0 without rejecting.
    }
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2).div_euclid(5) + day - 1;
    let doe = yoe * 365 + yoe.div_euclid(4) - yoe.div_euclid(100) + doy;
    let epoch_days = era * 146_097 + doe - 719_468;
    let total_seconds = epoch_days * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(total_seconds * 1_000_000 + millis * 1_000)
}

fn parse_digits(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(
        bytes
            .iter()
            .fold(0_i64, |total, digit| total * 10 + i64::from(digit - b'0')),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CellExt, SignalSchemaField, build_extraction_config};
    fn config(fields: &[SignalSchemaField], names: &[&str]) -> ExtractionConfig {
        build_extraction_config(fields, names).unwrap()
    }
    fn field(kind: ArrowType) -> SignalSchemaField {
        SignalSchemaField::new(kind, true)
    }
    fn one_value(json: &[u8], kind: ArrowType) -> DynamicColumns {
        let mut c = DynamicColumns::new(&[field(kind)], 1);
        c.begin_row();
        let mut p = JsonParser::new(json);
        extract_typed_value(
            &mut p,
            kind,
            &mut c,
            0,
            &mut ExtractionDiagnostic::default(),
        )
        .unwrap();
        c.end_row();
        c
    }
    #[test]
    fn extract_typed_value_string() {
        assert_eq!(
            one_value(br#""hello""#, ArrowType::Utf8).cell(0, 0),
            Some(ColumnValue::Utf8("hello".into()))
        );
    }
    #[test]
    fn extract_typed_value_number_to_int64() {
        assert_eq!(
            one_value(b"42", ArrowType::Int32).cell(0, 0),
            Some(ColumnValue::Int64(42))
        );
    }
    #[test]
    fn extract_typed_value_number_to_float64() {
        assert_eq!(
            one_value(b"99.99", ArrowType::Float64).cell(0, 0),
            Some(ColumnValue::Float64(99.99))
        );
    }
    #[test]
    fn extract_typed_value_boolean() {
        assert_eq!(
            one_value(b"true", ArrowType::Bool).cell(0, 0),
            Some(ColumnValue::Bool(true))
        );
    }
    #[test]
    fn extract_typed_value_null() {
        assert!(one_value(b"null", ArrowType::Utf8).is_null(0, 0));
    }
    #[test]
    fn msgpack_value_writer_typed_number_serialization() {
        for (json, expected) in [
            (b"42".as_slice(), 42),
            (b"true".as_slice(), 0xc3),
            (b"null".as_slice(), 0xc0),
        ] {
            let mut buffer = [0; 256];
            let mut w = MsgpackValueWriter::new(&mut buffer).unwrap();
            let mut p = JsonParser::new(json);
            let token = p.next_token().unwrap();
            w.write_value(&mut p, token).unwrap();
            assert_eq!(w.written()[0], expected);
        }
    }
    #[test]
    fn msgpack_value_writer_nested_object_serialization() {
        let mut b = [0; 256];
        let mut w = MsgpackValueWriter::new(&mut b).unwrap();
        let mut p = JsonParser::new(br#"{"a":1,"b":true}"#);
        let token = p.next_token().unwrap();
        w.write_value(&mut p, token).unwrap();
        assert_eq!(
            w.written(),
            &[0xdf, 0, 0, 0, 2, 0xa1, b'a', 1, 0xa1, b'b', 0xc3]
        );
    }
    #[test]
    fn msgpack_value_writer_array_serialization() {
        let mut b = [0; 256];
        let mut w = MsgpackValueWriter::new(&mut b).unwrap();
        let mut p = JsonParser::new(b"[1,2,3]");
        let token = p.next_token().unwrap();
        w.write_value(&mut p, token).unwrap();
        assert_eq!(w.written(), &[0xdd, 0, 0, 0, 3, 1, 2, 3]);
    }
    #[test]
    fn extra_produces_typed_msgpack() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut c = DynamicColumns::new(&fields, 10);
        let mut work = [0; 1024];
        extract_json_events(
            br#"[{"id":"1","count":42}]"#,
            &config(&fields, &["id", "value.$extra"]),
            &mut c,
            &mut work,
            &mut ExtractionDiagnostic::default(),
        )
        .unwrap();
        assert_eq!(
            c.cell(1, 0),
            Some(ColumnValue::Binary(vec![
                0xdf, 0, 0, 0, 1, 0xa5, b'c', b'o', b'u', b'n', b't', 42
            ]))
        );
    }
    #[test]
    fn extract_json_events_does_not_silently_drop_undeclared_fields() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Int64),
            field(ArrowType::Binary),
        ];
        let mut c = DynamicColumns::new(&fields, 10);
        let mut work = [0; 4096];
        let extra_fields = (1..=40)
            .map(|index| format!(r#""k{index:02}":"v""#))
            .collect::<Vec<_>>()
            .join(",");
        let json = format!(r#"[{{"id":"1","type":"order","timestamp":1000,{extra_fields}}}]"#);
        extract_json_events(
            json.as_bytes(),
            &config(&fields, &["id", "type", "timestamp", "value.$extra"]),
            &mut c,
            &mut work,
            &mut ExtractionDiagnostic::default(),
        )
        .unwrap();
        assert!(!c.is_null(3, 0));
    }
    #[test]
    fn extract_json_events_returns_buffer_overflow_when_workspace_too_small() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Int64),
            field(ArrowType::Binary),
        ];
        let mut c = DynamicColumns::new(&fields, 1);
        let mut work = [0; 16];
        assert_eq!(
            extract_json_events(
                br#"[{"id":"1","type":"order","timestamp":1000,"extra":"abcdefghijklmnopqrstuvwxyz"}]"#,
                &config(&fields, &["id", "type", "timestamp", "value.$extra"]),
                &mut c,
                &mut work,
                &mut ExtractionDiagnostic::default()
            ),
            Err(ExtractionError::BufferOverflow)
        );
    }
    #[test]
    fn extract_json_events_multiple_events() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Int64),
            field(ArrowType::Utf8),
            field(ArrowType::Binary),
        ];
        let mut c = DynamicColumns::new(&fields, 10);
        let mut work = [0; 1024];
        assert_eq!(extract_json_events(br#"[{"id":"1","type":"order","timestamp":1000,"orderId":"A"},{"id":"2","type":"order","timestamp":2000,"orderId":"B"}]"#,&config(&fields,&["id","type","timestamp","orderId","value.$extra"]),&mut c,&mut work,&mut ExtractionDiagnostic::default()).unwrap(),2);
    }
    #[test]
    fn extract_json_events_with_undeclared_fields() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Int64),
            field(ArrowType::Utf8),
            field(ArrowType::Binary),
        ];
        let mut c = DynamicColumns::new(&fields, 10);
        let mut work = [0; 1024];
        assert_eq!(
            extract_json_events(
                br#"[{"id":"1","type":"order","timestamp":1000,"orderId":"A","extra":"ignored"},{"id":"2","type":"order","timestamp":2000,"orderId":"B"}]"#,
                &config(&fields, &["id", "type", "timestamp", "orderId", "value.$extra"]),
                &mut c,
                &mut work,
                &mut ExtractionDiagnostic::default()
            )
            .unwrap(),
            2
        );
    }
    #[test]
    fn extract_json_events_supports_schemas_wider_than_64_fields() {
        // json_extractor.zig tracks presence in the schema-sized
        // `columns_seen` workspace — no 64-column ceiling (a fixed array +
        // rejection here once failed every wide schema; the TS
        // dynamic-schema-extraction suite pins >64-field processing).
        let fields = vec![field(ArrowType::Utf8); 65];
        let names: Vec<String> = (0..65).map(|i| format!("f{i:02}")).collect();
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut c = DynamicColumns::new(&fields, 2);
        let mut work = [0; 256];
        assert_eq!(
            extract_json_events(
                br#"[{"f00":"v","f64":"w"}]"#,
                &config(&fields, &borrowed),
                &mut c,
                &mut work,
                &mut ExtractionDiagnostic::default()
            )
            .unwrap(),
            1
        );
        assert_eq!(c.columns[0].read_variable(0), Some(b"v".as_slice()));
        assert_eq!(c.columns[64].read_variable(0), Some(b"w".as_slice()));
    }
    #[test]
    fn binary_column_serializes_object_to_typed_msgpack() {
        let c = one_value(br#"{"nested":true}"#, ArrowType::Binary);
        assert_eq!(
            c.cell(0, 0),
            Some(ColumnValue::Binary(vec![
                0xdf, 0, 0, 0, 1, 0xa6, b'n', b'e', b's', b't', b'e', b'd', 0xc3
            ]))
        );
    }
    #[test]
    fn parse_timestamp_to_micros_test() {
        assert!(parse_timestamp_to_micros("2024-01-15T10:30:00.123Z").is_some());
        assert!(parse_timestamp_to_micros("2024-01-15T10:30:00Z").is_some());
        assert_eq!(
            parse_timestamp_to_micros("2024-01-01T00:00:00Z"),
            Some(1_704_067_200_000_000)
        );
        assert_eq!(parse_timestamp_to_micros("not a date"), None);
        assert_eq!(parse_timestamp_to_micros(""), None);
    }
    /// Pins the drift between json_extractor.zig's own timestamp parser and
    /// json_scanner.zig's (see parse_timestamp_to_micros doc).
    #[test]
    fn parse_timestamp_to_micros_extractor_variant_drift() {
        use crate::json_scanner::parse_iso8601_to_micros;
        // No year-range check here; the scanner rejects years before 1970.
        assert!(parse_timestamp_to_micros("1900-01-15T10:30:00Z").is_some());
        assert!(parse_iso8601_to_micros("1900-01-15T10:30:00Z").is_err());
        // Fraction longer than 3 digits: extractor drops it to 0 ms, the
        // scanner keeps the first three digits.
        let base = parse_timestamp_to_micros("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(
            parse_timestamp_to_micros("2024-01-15T10:30:00.1234Z"),
            Some(base)
        );
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.1234Z").unwrap(),
            base + 123_000
        );
        // Non-digit short fraction: extractor rejects, scanner accepts as 0.
        assert_eq!(parse_timestamp_to_micros("2024-01-15T10:30:00.abZ"), None);
        assert!(parse_iso8601_to_micros("2024-01-15T10:30:00.abZ").is_ok());
    }
    /// Zig rejects an exactly-capacity batch (`count >= capacity` runs after
    /// every event, including the last) — mirrored bug-for-bug.
    #[test]
    fn extract_json_events_rejects_exactly_capacity_batch() {
        let fields = [field(ArrowType::Utf8)];
        let mut c = DynamicColumns::new(&fields, 1);
        let mut work = [0; 64];
        assert_eq!(
            extract_json_events(
                br#"[{"id":"1"}]"#,
                &config(&fields, &["id"]),
                &mut c,
                &mut work,
                &mut ExtractionDiagnostic::default()
            ),
            Err(ExtractionError::TooManyEvents)
        );
    }
    /// The $extra writer is lazy: a tiny work buffer is fine as long as every
    /// field is declared.
    #[test]
    fn extract_json_events_tiny_work_buffer_ok_without_undeclared_fields() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut c = DynamicColumns::new(&fields, 2);
        let mut work = [0; 2];
        assert_eq!(
            extract_json_events(
                br#"[{"id":"1"}]"#,
                &config(&fields, &["id", "value.$extra"]),
                &mut c,
                &mut work,
                &mut ExtractionDiagnostic::default()
            )
            .unwrap(),
            1
        );
        assert!(c.is_null(1, 0));
    }
    /// On a mid-event error the pending row is abandoned, not committed.
    #[test]
    fn extract_json_events_error_abandons_partial_row() {
        let fields = [field(ArrowType::Int64)];
        let mut c = DynamicColumns::new(&fields, 2);
        let mut work = [0; 64];
        assert_eq!(
            extract_json_events(
                br#"[{"n":"not a number or timestamp"}]"#,
                &config(&fields, &["n"]),
                &mut c,
                &mut work,
                &mut ExtractionDiagnostic::default()
            ),
            Err(ExtractionError::InvalidFieldType)
        );
        assert_eq!(c.count(), 0);
    }
}
