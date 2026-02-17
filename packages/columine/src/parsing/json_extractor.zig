//! JSON Field Extraction to Typed Columns
//!
//! Extracts JSON event fields directly to Arrow columns based on schema.
//! - Declared fields (in schema) -> typed columns
//! - Undeclared fields -> $extra msgpack Binary column
//!
//! $extra column: Contains extra data beyond what schema specifies.
//! When JSON has fields not declared in schema, they are serialized
//! to msgpack and stored in the value.$extra Binary column.
//! This preserves all data without losing undeclared fields.
//!
//! Type coercion:
//! - JSON string -> Utf8
//! - JSON number -> Int64 or Float64 (schema determines)
//! - JSON boolean -> Bool
//! - JSON object/array -> Binary (msgpack serialized)
//! - JSON null -> null in column
//!
//! Architecture:
//! This module bridges the gap between JSON parsing and columnar storage.
//! It uses JsonParser for tokenization and DynamicColumns for output,
//! with msgpack for serializing complex/undeclared fields.

const std = @import("std");
const msgpack = @import("msgpack");
const json_parser = @import("json_parser.zig");
const columns = @import("columns.zig");
const dynamic_schema = @import("../arrow/dynamic_schema.zig");

/// Extraction errors
pub const ExtractionError = error{
    InvalidJson,
    InvalidFieldType,
    BufferOverflow,
    MsgpackError,
    TooManyEvents,
    OutOfMemory,
};

/// Field lookup entry
const FieldEntry = struct {
    col_idx: u32,
    arrow_type: dynamic_schema.ArrowType,
    name: []const u8,
};

/// Configuration for extraction
pub const ExtractionConfig = struct {
    /// Field name to column index mapping
    field_entries: []const FieldEntry,
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

    var fallback_idx: ?u32 = null;

    for (field_metadata, field_names, 0..) |meta, name, i| {
        entries[i] = .{
            .col_idx = @intCast(i),
            .arrow_type = meta.arrow_type,
            .name = name,
        };

        // Check if this is the $extra column
        if (std.mem.eql(u8, name, "value.$extra")) {
            fallback_idx = @intCast(i);
        }
    }

    return .{
        .field_entries = entries,
        .fallback_col_idx = fallback_idx,
        .num_columns = @intCast(field_metadata.len),
    };
}

/// Free extraction config
pub fn freeExtractionConfig(allocator: std.mem.Allocator, config: *ExtractionConfig) void {
    allocator.free(config.field_entries);
}

/// Extract a single JSON event to typed columns
///
/// Parses a JSON object and appends field values to the appropriate columns.
/// - System fields (id, type, timestamp) go to fixed positions
/// - Value fields are matched by name and type-coerced
/// - Undeclared fields are collected and serialized to $extra
///
/// @param parser - JSON parser positioned at object start
/// @param config - Extraction configuration with field mappings
/// @param dynamic_cols - Output columns
/// @param work_buffer - Temporary buffer for msgpack serialization
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

    // Buffer for collecting undeclared fields (for $extra column)
    var undeclared_count: u32 = 0;
    var undeclared_names: [32][]const u8 = undefined;
    var undeclared_values: [32][]const u8 = undefined;

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

        // Look up field in config
        const entry = findFieldEntry(config.field_entries, field_name);

        if (entry) |e| {
            // Declared field - extract value based on type
            try extractTypedValue(parser, e.arrow_type, dynamic_cols, e.col_idx);
            if (e.col_idx >= columns_set.len) return error.OutOfMemory;
            columns_set[e.col_idx] = true;
        } else {
            // Undeclared field - capture for $extra column
            if (undeclared_count < 32) {
                undeclared_names[undeclared_count] = field_name;
                // Capture the raw JSON value
                const raw_value = parser.captureRawValue() catch return error.InvalidJson;
                undeclared_values[undeclared_count] = raw_value;
                undeclared_count += 1;
            } else {
                // Too many undeclared fields, just skip
                _ = parser.skipValue() catch return error.InvalidJson;
            }
        }
    }

    // Append nulls for columns not set
    for (config.field_entries) |e| {
        if (!columns_set[e.col_idx]) {
            _ = dynamic_cols.appendNull(e.col_idx);
        }
    }

    // Serialize undeclared fields to $extra column if present
    if (config.fallback_col_idx) |fallback_idx| {
        if (undeclared_count > 0) {
            const msgpack_bytes = serializeUndeclaredFields(
                undeclared_names[0..undeclared_count],
                undeclared_values[0..undeclared_count],
                work_buffer,
            ) catch return error.MsgpackError;

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

/// Find a field entry by name
fn findFieldEntry(entries: []const FieldEntry, name: []const u8) ?FieldEntry {
    for (entries) |e| {
        if (std.mem.eql(u8, e.name, name)) {
            return e;
        }
    }
    return null;
}

/// Extract a value from parser and append to column based on Arrow type
fn extractTypedValue(
    parser: *json_parser.JsonParser,
    arrow_type: dynamic_schema.ArrowType,
    dynamic_cols: *columns.DynamicColumns,
    col_idx: u32,
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
        .Int => {
            // Expect number, coerce to i64
            switch (token) {
                .number => |n| {
                    // Parse as float first (JSON numbers can have decimals)
                    const f = std.fmt.parseFloat(f64, n) catch return error.InvalidFieldType;
                    const i: i64 = @intFromFloat(f);
                    const result = dynamic_cols.appendInt64(col_idx, i);
                    if (result != .OK) return error.BufferOverflow;
                },
                .null_ => _ = dynamic_cols.appendNull(col_idx),
                else => return error.InvalidFieldType,
            }
        },
        .FloatingPoint => {
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
            // Complex types or explicit binary - capture raw JSON
            // For now, store the raw JSON string
            switch (token) {
                .string => |s| {
                    const result = dynamic_cols.appendBinary(col_idx, s);
                    if (result != .OK) return error.BufferOverflow;
                },
                .object_begin, .array_begin => {
                    // Need to go back and capture the whole value
                    // For now, we'll just skip and store empty
                    // TODO: Proper msgpack serialization of complex values
                    skipRemainingValue(parser, token) catch return error.InvalidJson;
                    const result = dynamic_cols.appendBinary(col_idx, "{}");
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

/// Skip remaining tokens for a value that's already been started
fn skipRemainingValue(parser: *json_parser.JsonParser, start_token: json_parser.Token) !void {
    switch (start_token) {
        .object_begin => {
            var depth: u32 = 1;
            while (depth > 0) {
                const t = try parser.nextToken();
                switch (t) {
                    .object_begin => depth += 1,
                    .object_end => depth -= 1,
                    else => {},
                }
            }
        },
        .array_begin => {
            var depth: u32 = 1;
            while (depth > 0) {
                const t = try parser.nextToken();
                switch (t) {
                    .array_begin => depth += 1,
                    .array_end => depth -= 1,
                    else => {},
                }
            }
        },
        else => {}, // Primitive already consumed
    }
}

/// Serialize undeclared fields to msgpack map format
fn serializeUndeclaredFields(
    names: []const []const u8,
    values: []const []const u8,
    buffer: []u8,
) ![]const u8 {
    // Simple msgpack map format
    // For now, we'll create a minimal representation
    // Format: fixmap header + (key,value) pairs

    if (names.len == 0) return buffer[0..0];
    if (names.len > 15) return error.OutOfMemory; // Max fixmap size

    var offset: usize = 0;

    // Write fixmap header (0x80 + count)
    buffer[offset] = @as(u8, 0x80) | @as(u8, @intCast(names.len));
    offset += 1;

    for (names, values) |name, value| {
        // Write key as fixstr
        if (name.len > 31) return error.OutOfMemory;
        buffer[offset] = @as(u8, 0xa0) | @as(u8, @intCast(name.len));
        offset += 1;
        if (offset + name.len > buffer.len) return error.OutOfMemory;
        @memcpy(buffer[offset..][0..name.len], name);
        offset += name.len;

        // Write value as fixstr (raw JSON for now)
        if (value.len <= 31) {
            buffer[offset] = @as(u8, 0xa0) | @as(u8, @intCast(value.len));
            offset += 1;
        } else if (value.len <= 255) {
            buffer[offset] = 0xd9; // str 8
            offset += 1;
            buffer[offset] = @intCast(value.len);
            offset += 1;
        } else {
            return error.OutOfMemory;
        }
        if (offset + value.len > buffer.len) return error.OutOfMemory;
        @memcpy(buffer[offset..][0..value.len], value);
        offset += value.len;
    }

    return buffer[0..offset];
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

    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Utf8, &cols, 0);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(!cols.isNull(0, 0));
}

test "extractTypedValue - number to int64" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Int, .nullable = 0 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json = "42";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Int, &cols, 0);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(!cols.isNull(0, 0));
}

test "extractTypedValue - number to float64" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .FloatingPoint, .nullable = 0 },
    };

    var cols = try columns.DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    const json = "99.99";
    var parser = json_parser.JsonParser.init(json);
    defer parser.deinit();

    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .FloatingPoint, &cols, 0);
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

    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Bool, &cols, 0);
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

    try std.testing.expect(cols.beginRow());
    try extractTypedValue(&parser, .Utf8, &cols, 0);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expect(cols.isNull(0, 0));
}

test "serializeUndeclaredFields" {
    var buffer: [256]u8 = undefined;

    const names = [_][]const u8{ "foo", "bar" };
    const values = [_][]const u8{ "123", "true" };

    const result = try serializeUndeclaredFields(&names, &values, &buffer);

    // Verify fixmap header (0x82 = fixmap with 2 elements)
    try std.testing.expectEqual(@as(u8, 0x82), result[0]);

    // Verify first key "foo" (0xa3 = fixstr with 3 chars)
    try std.testing.expectEqual(@as(u8, 0xa3), result[1]);
    try std.testing.expectEqualStrings("foo", result[2..5]);
}

test "findFieldEntry" {
    const entries = [_]FieldEntry{
        .{ .col_idx = 0, .arrow_type = .Utf8, .name = "id" },
        .{ .col_idx = 1, .arrow_type = .Utf8, .name = "type" },
        .{ .col_idx = 2, .arrow_type = .Int, .name = "timestamp" },
    };

    const found = findFieldEntry(&entries, "type");
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u32, 1), found.?.col_idx);

    const not_found = findFieldEntry(&entries, "missing");
    try std.testing.expect(not_found == null);
}

test "extractJsonEvents - multiple events" {
    const testing = std.testing;

    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
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
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
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

test "extractJsonEvents rejects schemas wider than tracking bitmap" {
    const testing = std.testing;

    var fields: [65]dynamic_schema.SignalSchemaField = undefined;
    var names: [65][]const u8 = undefined;
    for (0..65) |i| {
        fields[i] = .{ .arrow_type = .Utf8, .nullable = 1 };
        names[i] = "f";
    }

    var config = try buildExtractionConfig(testing.allocator, &fields, &names);
    defer freeExtractionConfig(testing.allocator, &config);

    var cols = try columns.DynamicColumns.init(testing.allocator, &fields, 1);
    defer cols.deinit();

    const json =
        \\[{"f":"v"}]
    ;
    var work_buffer: [256]u8 = undefined;

    try testing.expectError(error.OutOfMemory, extractJsonEvents(json, &config, &cols, &work_buffer));
}
