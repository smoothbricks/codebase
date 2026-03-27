//! JSON Field Extraction to Typed Columns
//!
//! Extracts JSON event fields directly to Arrow columns based on schema.
//! - Declared fields (in schema) -> typed columns
//! - Undeclared fields -> $extra msgpack Binary column
//!
//! $extra column: Contains extra data beyond what schema specifies.
//! When JSON has fields not declared in schema, they are serialized
//! to typed msgpack and stored in the value.$extra Binary column.
//! This preserves all data without losing undeclared fields.
//!
//! Type coercion:
//! - JSON string -> Utf8
//! - JSON number -> Int64 or Float64 (schema determines)
//! - JSON boolean -> Bool
//! - JSON object/array -> Binary (typed msgpack)
//! - JSON null -> null in column
//!
//! Binary columns store typed msgpack — preserving JSON type fidelity
//! (numbers as int/float, bools as bool, nested structures as map/array).
//!
//! Architecture:
//! This module bridges the gap between JSON parsing and columnar storage.
//! It uses JsonParser for tokenization and DynamicColumns for output,
//! with MsgpackValueWriter for serializing complex/undeclared fields to
//! properly typed msgpack.

const std = @import("std");
// can share this file while pointing at their own columns/dynamic_schema.
const json_parser = @import("json_parser");
const columns = @import("columns");
const dynamic_schema = @import("dynamic_schema");

/// Extraction errors
pub const ExtractionError = error{
    InvalidJson,
    InvalidFieldType,
    BufferOverflow,
    MsgpackError,
    TooManyEvents,
    OutOfMemory,
};

/// Field lookup entry (used for iteration, e.g. null-fill for unset columns)
const FieldEntry = struct {
    col_idx: u32,
    arrow_type: dynamic_schema.ArrowType,
    name: []const u8,
};

/// O(1) lookup value stored in the field hash map
const FieldLookup = struct {
    col_idx: u32,
    arrow_type: dynamic_schema.ArrowType,
};

/// Configuration for extraction
pub const ExtractionConfig = struct {
    /// Field entries for iteration (null-fill loop needs all columns)
    field_entries: []const FieldEntry,
    /// O(1) field name -> column index + type lookup
    field_map: std.StringHashMapUnmanaged(FieldLookup),
    /// Index of $extra column (null if no extra column)
    fallback_col_idx: ?u32,
    /// Total number of columns
    num_columns: u32,
};

/// Build extraction config from schema field metadata
/// Caller must provide field names separately (not stored in SignalSchemaField)
pub fn buildExtractionConfig(
    allocator: std.mem.Allocator,
    field_metadata: []const dynamic_schema.SignalSchemaField,
    field_names: []const []const u8,
) !ExtractionConfig {
    std.debug.assert(field_metadata.len == field_names.len);

    var entries = try allocator.alloc(FieldEntry, field_names.len);
    errdefer allocator.free(entries);

    // Build hash map for O(1) field lookup by name.
    // Keys point into field_names storage owned by DynamicSchemaConfig,
    // which outlives this config.
    var field_map = std.StringHashMapUnmanaged(FieldLookup){};
    errdefer field_map.deinit(allocator);
    try field_map.ensureTotalCapacity(allocator, @intCast(field_names.len));

    var fallback_idx: ?u32 = null;

    for (field_metadata, field_names, 0..) |meta, name, i| {
        const col_idx: u32 = @intCast(i);
        entries[i] = .{
            .col_idx = col_idx,
            .arrow_type = meta.arrow_type,
            .name = name,
        };

        field_map.putAssumeCapacityNoClobber(name, .{
            .col_idx = col_idx,
            .arrow_type = meta.arrow_type,
        });

        // Check if this is the $extra column
        if (std.mem.eql(u8, name, "value.$extra")) {
            fallback_idx = col_idx;
        }
    }

    return .{
        .field_entries = entries,
        .field_map = field_map,
        .fallback_col_idx = fallback_idx,
        .num_columns = @intCast(field_metadata.len),
    };
}

/// Free extraction config
pub fn freeExtractionConfig(allocator: std.mem.Allocator, config: *ExtractionConfig) void {
    config.field_map.deinit(allocator);
    allocator.free(config.field_entries);
}

/// Extract a single JSON event to typed columns
///
/// Parses a JSON object and appends field values to the appropriate columns.
/// - System fields (id, type, timestamp) go to fixed positions
/// - Value fields are matched by name and type-coerced
/// - Undeclared fields are serialized inline to typed msgpack in $extra
///
/// @param parser - JSON parser positioned at object start
/// @param config - Extraction configuration with field mappings
/// @param dynamic_cols - Output columns
/// @param work_buffer - Temporary buffer for $extra msgpack serialization
pub fn extractJsonEvent(
    parser: *json_parser.JsonParser,
    config: *const ExtractionConfig,
    dynamic_cols: *columns.DynamicColumns,
    work_buffer: []u8,
) ExtractionError!void {
    if (config.num_columns > 64) {
        return error.OutOfMemory;
    }

    // Start a new row
    if (!dynamic_cols.beginRow()) {
        return error.TooManyEvents;
    }

    // Track which columns have been set (for null detection)
    var columns_set: [64]bool = [_]bool{false} ** 64;

    // Streaming typed msgpack writer for undeclared fields ($extra column).
    // Created lazily only when we encounter the first undeclared field.
    // Uses work_buffer for msgpack output.
    var extra_writer: ?MsgpackValueWriter = null;
    var extra_count: u32 = 0;

    // Stack-allocated scratch buffer for individual Binary column values
    var binary_scratch: [4096]u8 = undefined;

    // Expect object begin (should be at this position already)
    parser.expectObjectBegin() catch return error.InvalidJson;

    // Parse all fields
    while (true) {
        // Check for object end
        if (parser.isObjectEnd()) {
            _ = parser.nextToken() catch return error.InvalidJson; // consume object_end
            break;
        }

        // Get field name
        const field_name = parser.expectFieldName() catch |err| {
            return switch (err) {
                error.EndOfInput => break,
                else => error.InvalidJson,
            };
        };

        // Look up field in config via O(1) hash map
        const lookup = config.field_map.get(field_name);

        if (lookup) |e| {
            // Declared field - extract value based on type
            try extractTypedValue(parser, e.arrow_type, dynamic_cols, e.col_idx, &binary_scratch);
            columns_set[e.col_idx] = true;
        } else {
            // Undeclared field - serialize to typed msgpack in $extra if configured
            if (config.fallback_col_idx != null) {
                // Lazy-init $extra writer on first undeclared field
                if (extra_writer == null) {
                    extra_writer = MsgpackValueWriter.init(work_buffer) orelse return error.BufferOverflow;
                    // Reserve map32 header (patched in finish)
                    extra_writer.?.reserveMap32Header() catch return error.BufferOverflow;
                }

                // Write field key as msgpack string
                extra_writer.?.writeStr(field_name) catch return error.BufferOverflow;

                // Write field value as typed msgpack (walks token stream)
                const value_token = parser.nextToken() catch return error.InvalidJson;
                extra_writer.?.writeValue(parser, value_token) catch return error.BufferOverflow;

                extra_count += 1;
            } else {
                parser.skipValue() catch return error.InvalidJson;
            }
        }
    }

    // Append nulls for columns not set
    for (config.field_entries) |e| {
        if (!columns_set[e.col_idx]) {
            _ = dynamic_cols.appendNull(e.col_idx);
        }
    }

    // Finalize $extra column if present
    if (config.fallback_col_idx) |fallback_idx| {
        if (extra_writer) |*writer| {
            const msgpack_bytes = writer.finishMap32(extra_count);

            const result = dynamic_cols.appendBinary(fallback_idx, msgpack_bytes);
            if (result != .OK) return error.BufferOverflow;
        } else {
            _ = dynamic_cols.appendNull(fallback_idx);
        }
    }

    dynamic_cols.endRow();
}

/// Extract multiple JSON events from array to DynamicColumns
///
/// Parses JSON array of event objects and extracts each to columns.
/// Returns count of events extracted.
///
/// @param json_input - JSON array string: [{"id":"1",...}, {"id":"2",...}]
/// @param config - Extraction configuration with field mappings
/// @param dynamic_cols - Output columns (must be reset before calling)
/// @param work_buffer - Temporary buffer for msgpack serialization
/// @return Count of events extracted, or error
pub fn extractJsonEvents(
    json_input: []const u8,
    config: *const ExtractionConfig,
    dynamic_cols: *columns.DynamicColumns,
    work_buffer: []u8,
) ExtractionError!u32 {
    var parser = json_parser.JsonParser.init(json_input);
    defer parser.deinit();

    // Expect array begin
    parser.expectArrayBegin() catch return error.InvalidJson;

    var count: u32 = 0;
    while (!parser.isArrayEnd()) {
        try extractJsonEvent(&parser, config, dynamic_cols, work_buffer);
        count += 1;

        if (count >= dynamic_cols.capacity) {
            return error.TooManyEvents;
        }
    }

    // Consume array end
    _ = parser.nextToken() catch return error.InvalidJson;

    return count;
}

/// Extract a value from parser and append to column based on Arrow type
fn extractTypedValue(
    parser: *json_parser.JsonParser,
    arrow_type: dynamic_schema.ArrowType,
    dynamic_cols: *columns.DynamicColumns,
    col_idx: u32,
    binary_scratch: *[4096]u8,
) ExtractionError!void {
    const token = parser.nextToken() catch return error.InvalidJson;

    switch (arrow_type) {
        .Utf8 => {
            // Expect string
            switch (token) {
                .string => |s| {
                    const result = dynamic_cols.appendUtf8(col_idx, s);
                    if (result != .OK) return error.BufferOverflow;
                },
                .null_ => _ = dynamic_cols.appendNull(col_idx),
                else => return error.InvalidFieldType,
            }
        },
        .Int32, .Int64 => {
            // Expect number or string, coerce to i64.
            // Try direct integer parse first (lossless for all i64 values),
            // fall back to float parse for decimal JSON numbers.
            switch (token) {
                .number => |n| {
                    if (std.fmt.parseInt(i64, n, 10)) |i| {
                        const result = dynamic_cols.appendInt64(col_idx, i);
                        if (result != .OK) return error.BufferOverflow;
                    } else |_| {
                        // Decimal number — parse as float, truncate to i64
                        const f = std.fmt.parseFloat(f64, n) catch return error.InvalidFieldType;
                        const i: i64 = @intFromFloat(f);
                        const result = dynamic_cols.appendInt64(col_idx, i);
                        if (result != .OK) return error.BufferOverflow;
                    }
                },
                .string => |s| {
                    // Try direct integer parse first (supports bigint-as-string)
                    if (std.fmt.parseInt(i64, s, 10)) |i| {
                        const result = dynamic_cols.appendInt64(col_idx, i);
                        if (result != .OK) return error.BufferOverflow;
                    } else |_| {
                        // Fall back to ISO-8601 timestamp string → microseconds
                        const micros = parseTimestampToMicros(s) orelse return error.InvalidFieldType;
                        const result = dynamic_cols.appendInt64(col_idx, micros);
                        if (result != .OK) return error.BufferOverflow;
                    }
                },
                .null_ => _ = dynamic_cols.appendNull(col_idx),
                else => return error.InvalidFieldType,
            }
        },
        .Float64 => {
            // Expect number, keep as f64
            switch (token) {
                .number => |n| {
                    const f = std.fmt.parseFloat(f64, n) catch return error.InvalidFieldType;
                    const result = dynamic_cols.appendFloat64(col_idx, f);
                    if (result != .OK) return error.BufferOverflow;
                },
                .null_ => _ = dynamic_cols.appendNull(col_idx),
                else => return error.InvalidFieldType,
            }
        },
        .Bool => {
            switch (token) {
                .true_ => {
                    const result = dynamic_cols.appendBool(col_idx, true);
                    if (result != .OK) return error.BufferOverflow;
                },
                .false_ => {
                    const result = dynamic_cols.appendBool(col_idx, false);
                    if (result != .OK) return error.BufferOverflow;
                },
                .null_ => _ = dynamic_cols.appendNull(col_idx),
                else => return error.InvalidFieldType,
            }
        },
        .Binary => {
            // Serialize to typed msgpack — preserving JSON type fidelity
            switch (token) {
                .string => |s| {
                    var mp = MsgpackValueWriter.init(binary_scratch) orelse return error.BufferOverflow;
                    mp.writeStr(s) catch return error.BufferOverflow;
                    const result = dynamic_cols.appendBinary(col_idx, mp.getWritten());
                    if (result != .OK) return error.BufferOverflow;
                },
                .object_begin, .array_begin, .number, .true_, .false_ => {
                    var mp = MsgpackValueWriter.init(binary_scratch) orelse return error.BufferOverflow;
                    mp.writeValue(parser, token) catch return error.BufferOverflow;
                    const result = dynamic_cols.appendBinary(col_idx, mp.getWritten());
                    if (result != .OK) return error.BufferOverflow;
                },
                .null_ => _ = dynamic_cols.appendNull(col_idx),
                else => return error.InvalidFieldType,
            }
        },
        .Null => {
            // Null type column - always null
            _ = dynamic_cols.appendNull(col_idx);
        },
    }
}

// =============================================================================
// MsgpackValueWriter — typed msgpack serialization from JSON token stream
// =============================================================================

/// Writes typed msgpack bytes to a fixed buffer.
/// Handles all JSON value types with proper type fidelity:
/// - string → msgpack str
/// - number → msgpack int (if integer) or float64 (if fractional/exponent)
/// - true/false → msgpack bool
/// - null → msgpack nil
/// - object → msgpack map32 (deferred count, recursive)
/// - array → msgpack array32 (deferred count, recursive)
const MsgpackValueWriter = struct {
    buffer: []u8,
    offset: usize,

    // Explicit error set required because writeValue is recursive
    // (writeValue → writeObject/writeArray → writeValue)
    const WriteError = error{ BufferOverflow, InvalidJson, InvalidNumber };

    fn init(buffer: []u8) ?MsgpackValueWriter {
        if (buffer.len < 5) return null;
        return .{ .buffer = buffer, .offset = 0 };
    }

    /// Reserve 5 bytes for a map32 header at the current position.
    /// Call finishMap32() to patch the count after writing all pairs.
    fn reserveMap32Header(self: *MsgpackValueWriter) WriteError!void {
        try self.ensureBytes(5);
        // Write map32 tag, count patched later
        self.buffer[self.offset] = 0xdf;
        self.offset += 5;
    }

    /// Patch a previously reserved map32 header with the actual count.
    /// Returns the written bytes from buffer start.
    fn finishMap32(self: *MsgpackValueWriter, count: u32) []const u8 {
        // Patch count in bytes 1-4 (byte 0 is 0xdf tag)
        self.buffer[1] = @intCast((count >> 24) & 0xff);
        self.buffer[2] = @intCast((count >> 16) & 0xff);
        self.buffer[3] = @intCast((count >> 8) & 0xff);
        self.buffer[4] = @intCast(count & 0xff);
        return self.buffer[0..self.offset];
    }

    fn getWritten(self: *const MsgpackValueWriter) []const u8 {
        return self.buffer[0..self.offset];
    }

    /// Serialize a JSON token (and any nested structure) to typed msgpack.
    fn writeValue(self: *MsgpackValueWriter, parser: *json_parser.JsonParser, token: json_parser.Token) WriteError!void {
        switch (token) {
            .string => |s| try self.writeStr(s),
            .number => |n| try self.writeNumber(n),
            .true_ => try self.writeBool(true),
            .false_ => try self.writeBool(false),
            .null_ => try self.writeNil(),
            .object_begin => try self.writeObject(parser),
            .array_begin => try self.writeArray(parser),
            else => return error.InvalidJson,
        }
    }

    fn writeObject(self: *MsgpackValueWriter, parser: *json_parser.JsonParser) WriteError!void {
        // Reserve map32 header (5 bytes), patch count after writing pairs
        try self.ensureBytes(5);
        const header_pos = self.offset;
        self.buffer[self.offset] = 0xdf;
        self.offset += 5;

        var count: u32 = 0;
        while (!parser.isObjectEnd()) {
            const key = parser.expectFieldName() catch return error.InvalidJson;
            try self.writeStr(key);

            const val_token = parser.nextToken() catch return error.InvalidJson;
            try self.writeValue(parser, val_token);

            count += 1;
        }
        // Consume object_end
        _ = parser.nextToken() catch return error.InvalidJson;

        // Patch map32 count
        self.buffer[header_pos + 1] = @intCast((count >> 24) & 0xff);
        self.buffer[header_pos + 2] = @intCast((count >> 16) & 0xff);
        self.buffer[header_pos + 3] = @intCast((count >> 8) & 0xff);
        self.buffer[header_pos + 4] = @intCast(count & 0xff);
    }

    fn writeArray(self: *MsgpackValueWriter, parser: *json_parser.JsonParser) WriteError!void {
        // Reserve array32 header (5 bytes), patch count after writing elements
        try self.ensureBytes(5);
        const header_pos = self.offset;
        self.buffer[self.offset] = 0xdd;
        self.offset += 5;

        var count: u32 = 0;
        while (!parser.isArrayEnd()) {
            const elem_token = parser.nextToken() catch return error.InvalidJson;
            try self.writeValue(parser, elem_token);
            count += 1;
        }
        // Consume array_end
        _ = parser.nextToken() catch return error.InvalidJson;

        // Patch array32 count
        self.buffer[header_pos + 1] = @intCast((count >> 24) & 0xff);
        self.buffer[header_pos + 2] = @intCast((count >> 16) & 0xff);
        self.buffer[header_pos + 3] = @intCast((count >> 8) & 0xff);
        self.buffer[header_pos + 4] = @intCast(count & 0xff);
    }

    fn writeNumber(self: *MsgpackValueWriter, text: []const u8) WriteError!void {
        // Try integer first (preserves JSON integer type fidelity)
        if (std.fmt.parseInt(i64, text, 10)) |val| {
            try self.writeInt(val);
        } else |_| {
            // Fractional or exponent — encode as float64
            const f = std.fmt.parseFloat(f64, text) catch return error.InvalidNumber;
            try self.writeFloat64(f);
        }
    }

    fn writeInt(self: *MsgpackValueWriter, val: i64) WriteError!void {
        if (val >= 0) {
            try self.writeUint(@intCast(val));
        } else if (val >= -32) {
            // Negative fixint: single byte, 0xe0..0xff
            try self.ensureBytes(1);
            self.buffer[self.offset] = @bitCast(@as(i8, @intCast(val)));
            self.offset += 1;
        } else if (val >= std.math.minInt(i8)) {
            try self.ensureBytes(2);
            self.buffer[self.offset] = 0xd0; // int 8
            self.buffer[self.offset + 1] = @bitCast(@as(i8, @intCast(val)));
            self.offset += 2;
        } else if (val >= std.math.minInt(i16)) {
            try self.ensureBytes(3);
            self.buffer[self.offset] = 0xd1; // int 16
            const u: u16 = @bitCast(@as(i16, @intCast(val)));
            self.buffer[self.offset + 1] = @intCast((u >> 8) & 0xff);
            self.buffer[self.offset + 2] = @intCast(u & 0xff);
            self.offset += 3;
        } else if (val >= std.math.minInt(i32)) {
            try self.ensureBytes(5);
            self.buffer[self.offset] = 0xd2; // int 32
            const u: u32 = @bitCast(@as(i32, @intCast(val)));
            self.buffer[self.offset + 1] = @intCast((u >> 24) & 0xff);
            self.buffer[self.offset + 2] = @intCast((u >> 16) & 0xff);
            self.buffer[self.offset + 3] = @intCast((u >> 8) & 0xff);
            self.buffer[self.offset + 4] = @intCast(u & 0xff);
            self.offset += 5;
        } else {
            try self.ensureBytes(9);
            self.buffer[self.offset] = 0xd3; // int 64
            const u: u64 = @bitCast(val);
            inline for (0..8) |i| {
                self.buffer[self.offset + 1 + i] = @intCast((u >> @intCast(56 - i * 8)) & 0xff);
            }
            self.offset += 9;
        }
    }

    fn writeUint(self: *MsgpackValueWriter, val: u64) WriteError!void {
        if (val <= 127) {
            // Positive fixint: single byte
            try self.ensureBytes(1);
            self.buffer[self.offset] = @intCast(val);
            self.offset += 1;
        } else if (val <= std.math.maxInt(u8)) {
            try self.ensureBytes(2);
            self.buffer[self.offset] = 0xcc; // uint 8
            self.buffer[self.offset + 1] = @intCast(val);
            self.offset += 2;
        } else if (val <= std.math.maxInt(u16)) {
            try self.ensureBytes(3);
            self.buffer[self.offset] = 0xcd; // uint 16
            self.buffer[self.offset + 1] = @intCast((val >> 8) & 0xff);
            self.buffer[self.offset + 2] = @intCast(val & 0xff);
            self.offset += 3;
        } else if (val <= std.math.maxInt(u32)) {
            try self.ensureBytes(5);
            self.buffer[self.offset] = 0xce; // uint 32
            self.buffer[self.offset + 1] = @intCast((val >> 24) & 0xff);
            self.buffer[self.offset + 2] = @intCast((val >> 16) & 0xff);
            self.buffer[self.offset + 3] = @intCast((val >> 8) & 0xff);
            self.buffer[self.offset + 4] = @intCast(val & 0xff);
            self.offset += 5;
        } else {
            try self.ensureBytes(9);
            self.buffer[self.offset] = 0xcf; // uint 64
            inline for (0..8) |i| {
                self.buffer[self.offset + 1 + i] = @intCast((val >> @intCast(56 - i * 8)) & 0xff);
            }
            self.offset += 9;
        }
    }

    fn writeFloat64(self: *MsgpackValueWriter, val: f64) WriteError!void {
        try self.ensureBytes(9);
        self.buffer[self.offset] = 0xcb; // float 64
        const bits: u64 = @bitCast(val);
        inline for (0..8) |i| {
            self.buffer[self.offset + 1 + i] = @intCast((bits >> @intCast(56 - i * 8)) & 0xff);
        }
        self.offset += 9;
    }

    fn writeStr(self: *MsgpackValueWriter, s: []const u8) WriteError!void {
        // String header
        if (s.len <= 31) {
            try self.ensureBytes(1);
            self.buffer[self.offset] = @as(u8, 0xa0) | @as(u8, @intCast(s.len));
            self.offset += 1;
        } else if (s.len <= std.math.maxInt(u8)) {
            try self.ensureBytes(2);
            self.buffer[self.offset] = 0xd9; // str 8
            self.buffer[self.offset + 1] = @intCast(s.len);
            self.offset += 2;
        } else if (s.len <= std.math.maxInt(u16)) {
            try self.ensureBytes(3);
            self.buffer[self.offset] = 0xda; // str 16
            self.buffer[self.offset + 1] = @intCast((s.len >> 8) & 0xff);
            self.buffer[self.offset + 2] = @intCast(s.len & 0xff);
            self.offset += 3;
        } else if (s.len <= std.math.maxInt(u32)) {
            try self.ensureBytes(5);
            self.buffer[self.offset] = 0xdb; // str 32
            self.buffer[self.offset + 1] = @intCast((s.len >> 24) & 0xff);
            self.buffer[self.offset + 2] = @intCast((s.len >> 16) & 0xff);
            self.buffer[self.offset + 3] = @intCast((s.len >> 8) & 0xff);
            self.buffer[self.offset + 4] = @intCast(s.len & 0xff);
            self.offset += 5;
        } else {
            // String exceeds msgpack str32 max — can't fit in fixed buffer
            return error.BufferOverflow;
        }
        // String body
        if (self.offset + s.len > self.buffer.len) return error.BufferOverflow;
        @memcpy(self.buffer[self.offset..][0..s.len], s);
        self.offset += s.len;
    }

    fn writeBool(self: *MsgpackValueWriter, val: bool) WriteError!void {
        try self.ensureBytes(1);
        self.buffer[self.offset] = if (val) 0xc3 else 0xc2;
        self.offset += 1;
    }

    fn writeNil(self: *MsgpackValueWriter) WriteError!void {
        try self.ensureBytes(1);
        self.buffer[self.offset] = 0xc0;
        self.offset += 1;
    }

    fn ensureBytes(self: *MsgpackValueWriter, n: usize) WriteError!void {
        if (self.offset + n > self.buffer.len) return error.BufferOverflow;
    }
};

// =============================================================================
// ISO-8601 timestamp parsing (ported from json_scanner.zig)
// =============================================================================

/// Parse ISO-8601 timestamp to microseconds since epoch
///
/// Supported formats:
/// - "2024-01-15T10:30:00.123Z" (with milliseconds)
/// - "2024-01-15T10:30:00Z" (without milliseconds)
///
/// Limitations:
/// - Only UTC (Z suffix) supported
/// - Millisecond precision max (not microsecond from string)
/// - Year range: 1970-2099
pub fn parseTimestampToMicros(s: []const u8) ?i64 {
    // Minimum: "YYYY-MM-DDTHH:MM:SSZ" = 20 chars
    if (s.len < 20) return null;
    if (s[s.len - 1] != 'Z') return null;

    // Parse date components
    const year = parseDigits4(s[0..4]) orelse return null;
    if (s[4] != '-') return null;
    const month = parseDigits2(s[5..7]) orelse return null;
    if (s[7] != '-') return null;
    const day = parseDigits2(s[8..10]) orelse return null;
    if (s[10] != 'T') return null;

    // Parse time components
    const hour = parseDigits2(s[11..13]) orelse return null;
    if (s[13] != ':') return null;
    const minute = parseDigits2(s[14..16]) orelse return null;
    if (s[16] != ':') return null;
    const second = parseDigits2(s[17..19]) orelse return null;

    // Parse optional fractional seconds
    var millis: i64 = 0;
    if (s.len > 20 and s[19] == '.') {
        const frac_end = s.len - 1; // Before 'Z'
        const frac_str = s[20..frac_end];
        if (frac_str.len >= 1 and frac_str.len <= 3) {
            millis = parseDigitsN(frac_str) orelse return null;
            // Pad to 3 digits: ".1" -> 100, ".12" -> 120
            var padded = millis;
            var i: usize = frac_str.len;
            while (i < 3) : (i += 1) {
                padded *= 10;
            }
            millis = padded;
        }
    }

    // Validate ranges
    if (month < 1 or month > 12) return null;
    if (day < 1 or day > 31) return null;
    if (hour > 23) return null;
    if (minute > 59) return null;
    if (second > 59) return null;

    // Convert to epoch days using civil_from_days algorithm
    const y: i64 = @as(i64, year) - @as(i64, if (month <= 2) @as(u32, 1) else 0);
    const era: i64 = @divFloor(y, 400);
    const yoe: i64 = y - era * 400;
    const m: i64 = @intCast(month);
    const doy: i64 = @divFloor((153 * (m + (if (month > 2) @as(i64, -3) else 9)) + 2), 5) + @as(i64, day) - 1;
    const doe: i64 = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const epoch_days: i64 = era * 146097 + doe - 719468;

    // Convert to microseconds
    const day_seconds: i64 = epoch_days * 86400;
    const total_seconds: i64 = day_seconds + @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    return total_seconds * 1_000_000 + millis * 1_000;
}

fn parseDigits2(s: *const [2]u8) ?u32 {
    const d0 = s[0] -% '0';
    const d1 = s[1] -% '0';
    if (d0 > 9 or d1 > 9) return null;
    return @as(u32, d0) * 10 + d1;
}

fn parseDigits4(s: *const [4]u8) ?u32 {
    const d0 = s[0] -% '0';
    const d1 = s[1] -% '0';
    const d2 = s[2] -% '0';
    const d3 = s[3] -% '0';
    if (d0 > 9 or d1 > 9 or d2 > 9 or d3 > 9) return null;
    return @as(u32, d0) * 1000 + @as(u32, d1) * 100 + @as(u32, d2) * 10 + d3;
}

fn parseDigitsN(s: []const u8) ?i64 {
    var result: i64 = 0;
    for (s) |c| {
        const d = c -% '0';
        if (d > 9) return null;
        result = result * 10 + d;
    }
    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "extractTypedValue - string" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json =
        \\"hello"
    ;
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    var scratch: [4096]u8 = undefined;
    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Utf8, &cols, 0, &scratch);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(!cols.isNull(0, 0));
}

test "extractTypedValue - number to int64" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Int32, .nullable = 0 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json = "42";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    var scratch: [4096]u8 = undefined;
    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Int32, &cols, 0, &scratch);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(!cols.isNull(0, 0));
}

test "extractTypedValue - number to float64" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Float64, .nullable = 0 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json = "99.99";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    var scratch: [4096]u8 = undefined;
    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Float64, &cols, 0, &scratch);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(!cols.isNull(0, 0));
}

test "extractTypedValue - boolean" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Bool, .nullable = 0 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json = "true";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    var scratch: [4096]u8 = undefined;
    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Bool, &cols, 0, &scratch);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(!cols.isNull(0, 0));
}

test "extractTypedValue - null" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 1 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json = "null";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    var scratch: [4096]u8 = undefined;
    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Utf8, &cols, 0, &scratch);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(cols.isNull(0, 0));
}

test "MsgpackValueWriter - typed number serialization" {
    var buffer: [256]u8 = undefined;

    // Integer 42 → msgpack positive fixint (single byte: 0x2a)
    var w1 = MsgpackValueWriter.init(&buffer) orelse unreachable;
    var p1 = json_parser.JsonParser.init("42");
    defer p1.deinit();
    const t1 = try p1.nextToken();
    try w1.writeValue(&p1, t1);
    const r1 = w1.getWritten();
    try std.testing.expectEqual(@as(u8, 42), r1[0]);
    try std.testing.expectEqual(@as(usize, 1), r1.len);

    // Boolean true → 0xc3
    var w2 = MsgpackValueWriter.init(&buffer) orelse unreachable;
    var p2 = json_parser.JsonParser.init("true");
    defer p2.deinit();
    const t2 = try p2.nextToken();
    try w2.writeValue(&p2, t2);
    const r2 = w2.getWritten();
    try std.testing.expectEqual(@as(u8, 0xc3), r2[0]);

    // Null → 0xc0
    var w3 = MsgpackValueWriter.init(&buffer) orelse unreachable;
    var p3 = json_parser.JsonParser.init("null");
    defer p3.deinit();
    const t3 = try p3.nextToken();
    try w3.writeValue(&p3, t3);
    const r3 = w3.getWritten();
    try std.testing.expectEqual(@as(u8, 0xc0), r3[0]);
}

test "MsgpackValueWriter - nested object serialization" {
    var buffer: [256]u8 = undefined;
    var w = MsgpackValueWriter.init(&buffer) orelse unreachable;

    // {"a":1,"b":true}
    const json =
        \\{"a":1,"b":true}
    ;
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    const t = try parser.nextToken(); // object_begin
    try w.writeValue(&parser, t);

    const result = w.getWritten();
    // map32 header: 0xdf + 4-byte count (2)
    try std.testing.expectEqual(@as(u8, 0xdf), result[0]);
    try std.testing.expectEqual(@as(u8, 0x00), result[1]);
    try std.testing.expectEqual(@as(u8, 0x00), result[2]);
    try std.testing.expectEqual(@as(u8, 0x00), result[3]);
    try std.testing.expectEqual(@as(u8, 0x02), result[4]);
    // Key "a": fixstr(1) + 'a'
    try std.testing.expectEqual(@as(u8, 0xa1), result[5]);
    try std.testing.expectEqual(@as(u8, 'a'), result[6]);
    // Value 1: positive fixint
    try std.testing.expectEqual(@as(u8, 1), result[7]);
    // Key "b": fixstr(1) + 'b'
    try std.testing.expectEqual(@as(u8, 0xa1), result[8]);
    try std.testing.expectEqual(@as(u8, 'b'), result[9]);
    // Value true: 0xc3
    try std.testing.expectEqual(@as(u8, 0xc3), result[10]);
}

test "MsgpackValueWriter - array serialization" {
    var buffer: [256]u8 = undefined;
    var w = MsgpackValueWriter.init(&buffer) orelse unreachable;

    const json = "[1,2,3]";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    const t = try parser.nextToken(); // array_begin
    try w.writeValue(&parser, t);

    const result = w.getWritten();
    // array32 header: 0xdd + 4-byte count (3)
    try std.testing.expectEqual(@as(u8, 0xdd), result[0]);
    try std.testing.expectEqual(@as(u8, 0x00), result[1]);
    try std.testing.expectEqual(@as(u8, 0x00), result[2]);
    try std.testing.expectEqual(@as(u8, 0x00), result[3]);
    try std.testing.expectEqual(@as(u8, 0x03), result[4]);
    // Elements 1, 2, 3 as positive fixints
    try std.testing.expectEqual(@as(u8, 1), result[5]);
    try std.testing.expectEqual(@as(u8, 2), result[6]);
    try std.testing.expectEqual(@as(u8, 3), result[7]);
}

test "$extra produces typed msgpack" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Binary, .nullable = 1 }, // value.$extra
    };
    const names = [_][]const u8{ "id", "value.$extra" };

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 10);
    defer cols.deinit();

    // Undeclared field "count" with integer value 42
    const json =
        \\[{"id":"1","count":42}]
    ;

    var work_buffer: [1024]u8 = undefined;
    const count = try extractJsonEvents(json, &config, &cols, &work_buffer);

    try testing.expectEqual(@as(u32, 1), count);
    try testing.expect(!cols.isNull(1, 0)); // $extra not null

    // Verify the msgpack content has typed integer, not string
    const extra_col = cols.getColumn(1).?;
    const extra_start = extra_col.offsets.?[0];
    const extra_end = extra_col.offsets.?[1];
    const extra_data = extra_col.data.?[extra_start..extra_end];
    // map32 header
    try testing.expectEqual(@as(u8, 0xdf), extra_data[0]);
    // count = 1
    try testing.expectEqual(@as(u8, 0x01), extra_data[4]);
    // Key "count": fixstr(5) + "count"
    try testing.expectEqual(@as(u8, 0xa5), extra_data[5]);
    try testing.expectEqualStrings("count", extra_data[6..11]);
    // Value 42: positive fixint (NOT a string!)
    try testing.expectEqual(@as(u8, 42), extra_data[11]);
}

test "extractJsonEvents does not silently drop undeclared fields" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int64, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value.$extra
    };
    const names = [_][]const u8{ "id", "type", "timestamp", "value.$extra" };

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 10);
    defer cols.deinit();

    const json =
        \\[{"id":"1","type":"order","timestamp":1000,"k01":"v","k02":"v","k03":"v","k04":"v","k05":"v","k06":"v","k07":"v","k08":"v","k09":"v","k10":"v","k11":"v","k12":"v","k13":"v","k14":"v","k15":"v","k16":"v","k17":"v","k18":"v","k19":"v","k20":"v","k21":"v","k22":"v","k23":"v","k24":"v","k25":"v","k26":"v","k27":"v","k28":"v","k29":"v","k30":"v","k31":"v","k32":"v","k33":"v","k34":"v","k35":"v","k36":"v","k37":"v","k38":"v","k39":"v","k40":"v"}]
    ;

    var work_buffer: [4096]u8 = undefined;
    const count = try extractJsonEvents(json, &config, &cols, &work_buffer);

    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(@as(u32, 1), cols.count);
    try testing.expect(!cols.isNull(3, 0));
}

test "extractJsonEvents returns BufferOverflow when workspace is too small" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int64, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value.$extra
    };
    const names = [_][]const u8{ "id", "type", "timestamp", "value.$extra" };

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 1);
    defer cols.deinit();

    const json =
        \\[{"id":"1","type":"order","timestamp":1000,"extra":"abcdefghijklmnopqrstuvwxyz"}]
    ;

    var tiny_work_buffer: [16]u8 = undefined;
    try testing.expectError(error.BufferOverflow, extractJsonEvents(json, &config, &cols, &tiny_work_buffer));
}

test "extractJsonEvents - multiple events" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int64, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Utf8, .nullable = 1 }, // value.orderId
        .{ .arrow_type = .Binary, .nullable = 1 }, // value.$extra
    };
    const names = [_][]const u8{ "id", "type", "timestamp", "orderId", "value.$extra" };

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 10);
    defer cols.deinit();

    const json =
        \\[{"id":"1","type":"order","timestamp":1000,"orderId":"A"},
        \\ {"id":"2","type":"order","timestamp":2000,"orderId":"B"}]
    ;

    var work_buffer: [1024]u8 = undefined;
    const count = try extractJsonEvents(json, &config, &cols, &work_buffer);

    try testing.expectEqual(@as(u32, 2), count);
    try testing.expectEqual(@as(u32, 2), cols.count);
}

test "extractJsonEvents - with undeclared fields" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int64, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Utf8, .nullable = 1 }, // orderId
        .{ .arrow_type = .Binary, .nullable = 1 }, // value.$extra
    };
    const names = [_][]const u8{ "id", "type", "timestamp", "orderId", "value.$extra" };

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 10);
    defer cols.deinit();

    const json =
        \\[{"id":"1","type":"order","timestamp":1000,"orderId":"A","extra":"ignored"},
        \\ {"id":"2","type":"order","timestamp":2000,"orderId":"B"}]
    ;

    var work_buffer: [1024]u8 = undefined;
    const count = try extractJsonEvents(json, &config, &cols, &work_buffer);

    try testing.expectEqual(@as(u32, 2), count);
    try testing.expectEqual(@as(u32, 2), cols.count);
}

test "extractJsonEvents rejects schemas wider than tracking array" {
    const testing = std.testing;

    // 65 unique field names to exceed the 64-column columns_set limit
    const name_data = "f00\x00f01\x00f02\x00f03\x00f04\x00f05\x00f06\x00f07\x00f08\x00f09\x00" ++
        "f10\x00f11\x00f12\x00f13\x00f14\x00f15\x00f16\x00f17\x00f18\x00f19\x00" ++
        "f20\x00f21\x00f22\x00f23\x00f24\x00f25\x00f26\x00f27\x00f28\x00f29\x00" ++
        "f30\x00f31\x00f32\x00f33\x00f34\x00f35\x00f36\x00f37\x00f38\x00f39\x00" ++
        "f40\x00f41\x00f42\x00f43\x00f44\x00f45\x00f46\x00f47\x00f48\x00f49\x00" ++
        "f50\x00f51\x00f52\x00f53\x00f54\x00f55\x00f56\x00f57\x00f58\x00f59\x00" ++
        "f60\x00f61\x00f62\x00f63\x00f64\x00";
    _ = name_data;

    var fields: [65]dynamic_schema.SignalSchemaField = undefined;
    var names: [65][]const u8 = undefined;
    // Generate unique names f00..f64
    var name_bufs: [65][3]u8 = undefined;
    for (0..65) |i| {
        name_bufs[i][0] = 'f';
        name_bufs[i][1] = '0' + @as(u8, @intCast(i / 10));
        name_bufs[i][2] = '0' + @as(u8, @intCast(i % 10));
        names[i] = &name_bufs[i];
        fields[i] = .{ .arrow_type = .Utf8, .nullable = 1 };
    }

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 1);
    defer cols.deinit();

    const json =
        \\[{"f00":"v"}]
    ;
    var work_buffer: [256]u8 = undefined;

    try testing.expectError(error.OutOfMemory, extractJsonEvents(json, &config, &cols, &work_buffer));
}

test "Binary column serializes object to typed msgpack" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Binary, .nullable = 1 },
    };

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 10);
    defer cols.deinit();

    // Parse an object value into a Binary column
    const json =
        \\{"nested":true}
    ;
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    var scratch: [4096]u8 = undefined;
    try testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Binary, &cols, 0, &scratch);
    cols.endRow();

    try testing.expectEqual(@as(u32, 1), cols.count);
    try testing.expect(!cols.isNull(0, 0));

    const bin_col = cols.getColumn(0).?;
    const bin_start = bin_col.offsets.?[0];
    const bin_end = bin_col.offsets.?[1];
    const data = bin_col.data.?[bin_start..bin_end];
    // map32 header: 0xdf, count=1
    try testing.expectEqual(@as(u8, 0xdf), data[0]);
    try testing.expectEqual(@as(u8, 0x01), data[4]);
}

test "parseTimestampToMicros" {
    const testing = std.testing;

    // With milliseconds
    const ts = parseTimestampToMicros("2024-01-15T10:30:00.123Z");
    try testing.expect(ts != null);

    // Without milliseconds
    const ts2 = parseTimestampToMicros("2024-01-15T10:30:00Z");
    try testing.expect(ts2 != null);

    // Known value: 2024-01-01T00:00:00Z = 1704067200 seconds
    const expected_micros: i64 = 1704067200 * 1_000_000;
    const result = parseTimestampToMicros("2024-01-01T00:00:00Z");
    try testing.expectEqual(expected_micros, result.?);

    // Invalid
    try testing.expect(parseTimestampToMicros("not a date") == null);
    try testing.expect(parseTimestampToMicros("") == null);
}
