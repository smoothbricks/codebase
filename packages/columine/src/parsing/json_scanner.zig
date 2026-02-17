//! JSON event array parser
//!
//! Uses std.json.Scanner for streaming zero-copy parsing.
//!
//! Timestamp format support:
//! - ISO-8601 with Z suffix: "2024-01-15T10:30:00.000Z" (with milliseconds)
//! - ISO-8601 with Z suffix: "2024-01-15T10:30:00Z" (without milliseconds)
//! - Numeric milliseconds: 1705315800000 (JS Date.now() style)
//!
//! Limitations (documented):
//! - Only UTC (Z suffix) supported, no timezone offsets like +05:30
//! - Millisecond precision max (not microsecond from string)
//! - Year range: 1970-2099 (no ancient dates or far future)

const std = @import("std");
const columns = @import("columns");
const EventColumns = columns.EventColumns;
const ParseError = columns.ParseError;

/// Parse JSON array of events into columnar format
///
/// Expected format: [{"id": "...", "type": "...", "timestamp": "...", "value": ...}, ...]
///
/// Returns ParseError.OK on success, error code otherwise.
pub fn parseJsonEvents(
    input: []const u8,
    output: *EventColumns,
    allocator: std.mem.Allocator,
) ParseError {
    _ = allocator; // For future use if we need temporary buffers

    var scanner = std.json.Scanner.initCompleteInput(std.heap.page_allocator, input);
    defer scanner.deinit();

    // Expect array start
    const first = scanner.next() catch return .INVALID_JSON;
    if (first != .array_begin) return .INVALID_JSON;

    // Parse each event object
    while (true) {
        const token = scanner.next() catch return .INVALID_JSON;
        switch (token) {
            .array_end => break,
            .object_begin => {
                const err = parseEventObject(&scanner, input, output);
                if (err != .OK) return err;
            },
            else => return .INVALID_JSON,
        }
    }

    return .OK;
}

/// Parse a single event object
fn parseEventObject(
    scanner: *std.json.Scanner,
    input: []const u8,
    output: *EventColumns,
) ParseError {
    var id: ?[]const u8 = null;
    var event_type: ?[]const u8 = null;
    var timestamp_us: ?i64 = null;
    var value_start: usize = 0;
    var value_end: usize = 0;
    var has_value = false;

    // Parse fields
    while (true) {
        const field_token = scanner.next() catch return .INVALID_JSON;
        switch (field_token) {
            .object_end => break,
            .string => |field_name| {
                if (std.mem.eql(u8, field_name, "id")) {
                    const val = scanner.next() catch return .INVALID_JSON;
                    switch (val) {
                        .string => |s| id = s,
                        else => return .INVALID_FIELD_TYPE,
                    }
                } else if (std.mem.eql(u8, field_name, "type")) {
                    const val = scanner.next() catch return .INVALID_JSON;
                    switch (val) {
                        .string => |s| event_type = s,
                        else => return .INVALID_FIELD_TYPE,
                    }
                } else if (std.mem.eql(u8, field_name, "timestamp")) {
                    const val = scanner.next() catch return .INVALID_JSON;
                    switch (val) {
                        .string => |ts_str| {
                            timestamp_us = parseIso8601ToMicros(ts_str) catch return .INVALID_FIELD_TYPE;
                        },
                        .number => |num_str| {
                            // Numeric timestamp (assume milliseconds from JS Date.now())
                            const ms = std.fmt.parseInt(i64, num_str, 10) catch return .INVALID_FIELD_TYPE;
                            timestamp_us = ms * 1000;
                        },
                        else => return .INVALID_FIELD_TYPE,
                    }
                } else if (std.mem.eql(u8, field_name, "value")) {
                    // After reading field name, cursor is at ':'. Skip ':' and whitespace to find value start.
                    value_start = scanner.cursor;
                    while (value_start < input.len) {
                        const c = input[value_start];
                        if (c != ':' and c != ' ' and c != '\t' and c != '\n' and c != '\r') break;
                        value_start += 1;
                    }
                    skipJsonValue(scanner) catch return .INVALID_JSON;
                    value_end = scanner.cursor;
                    has_value = true;
                } else {
                    // Unknown field - skip
                    skipJsonValue(scanner) catch return .INVALID_JSON;
                }
            },
            else => return .INVALID_JSON,
        }
    }

    // Validate required fields
    const id_val = id orelse return .MISSING_FIELD;
    const type_val = event_type orelse return .MISSING_FIELD;
    const ts_val = timestamp_us orelse return .MISSING_FIELD;

    // Extract value bytes if present (raw JSON substring from input)
    const value_bytes: ?[]const u8 = if (has_value)
        input[value_start..value_end]
    else
        null;

    return output.addEvent(id_val, type_val, ts_val, value_bytes);
}

/// Skip a JSON value (object, array, or primitive)
fn skipJsonValue(scanner: *std.json.Scanner) !void {
    const token = try scanner.next();
    switch (token) {
        .object_begin => {
            var depth: u32 = 1;
            while (depth > 0) {
                const t = try scanner.next();
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
                const t = try scanner.next();
                switch (t) {
                    .array_begin => depth += 1,
                    .array_end => depth -= 1,
                    else => {},
                }
            }
        },
        // Primitives: string, number, true, false, null - already consumed
        else => {},
    }
}

/// Parse ISO-8601 timestamp to microseconds since epoch
///
/// Supported formats:
/// - "2024-01-15T10:30:00.123Z" (with milliseconds)
/// - "2024-01-15T10:30:00Z" (without milliseconds)
///
/// Limitations:
/// - Only UTC (Z suffix) supported, no timezone offsets
/// - Millisecond precision max
/// - Year range: 1970-2099
pub fn parseIso8601ToMicros(s: []const u8) !i64 {
    // Minimum format: YYYY-MM-DDTHH:MM:SSZ = 20 chars
    if (s.len < 20) return error.InvalidFormat;

    // Must end with Z (UTC only)
    if (s[s.len - 1] != 'Z') return error.InvalidFormat;

    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return error.InvalidFormat;
    if (s[4] != '-') return error.InvalidFormat;
    const month = std.fmt.parseInt(u32, s[5..7], 10) catch return error.InvalidFormat;
    if (s[7] != '-') return error.InvalidFormat;
    const day = std.fmt.parseInt(u32, s[8..10], 10) catch return error.InvalidFormat;
    if (s[10] != 'T') return error.InvalidFormat;
    const hour = std.fmt.parseInt(u32, s[11..13], 10) catch return error.InvalidFormat;
    if (s[13] != ':') return error.InvalidFormat;
    const minute = std.fmt.parseInt(u32, s[14..16], 10) catch return error.InvalidFormat;
    if (s[16] != ':') return error.InvalidFormat;
    const second = std.fmt.parseInt(u32, s[17..19], 10) catch return error.InvalidFormat;

    // Validate ranges
    if (year < 1970 or year > 2099) return error.InvalidFormat;
    if (month < 1 or month > 12) return error.InvalidFormat;
    if (day < 1 or day > 31) return error.InvalidFormat;
    if (hour > 23) return error.InvalidFormat;
    if (minute > 59) return error.InvalidFormat;
    if (second > 59) return error.InvalidFormat;

    // Parse optional milliseconds
    var millis: i64 = 0;
    if (s.len > 20 and s[19] == '.') {
        // Find the end (Z)
        const frac_end = s.len - 1; // Before Z
        const frac_str = s[20..frac_end];
        if (frac_str.len >= 3) {
            millis = std.fmt.parseInt(i64, frac_str[0..3], 10) catch 0;
        } else if (frac_str.len > 0) {
            // Pad with zeros: ".1" -> 100ms, ".12" -> 120ms
            var padded: [3]u8 = [_]u8{ '0', '0', '0' };
            @memcpy(padded[0..frac_str.len], frac_str);
            millis = std.fmt.parseInt(i64, &padded, 10) catch 0;
        }
    }

    // Convert to epoch days then to microseconds
    const epoch_days = epochDays(year, month, day);
    const day_seconds = @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    const total_seconds = epoch_days * 86400 + day_seconds;
    return total_seconds * 1_000_000 + millis * 1_000;
}

/// Days since Unix epoch (1970-01-01)
/// Uses the civil calendar algorithm from Howard Hinnant
fn epochDays(year: i32, month: u32, day: u32) i64 {
    var y = year;
    var m = month;
    if (m <= 2) {
        y -= 1;
        m += 12;
    }
    const era: i64 = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe: u32 = @intCast(y - @as(i32, @intCast(era)) * 400);
    const doy: u32 = (153 * (m - 3) + 2) / 5 + day - 1;
    const doe: u32 = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + @as(i64, doe) - 719468;
}

// =============================================================================
// Tests
// =============================================================================

test "parseIso8601ToMicros - full format with millis" {
    const ts = try parseIso8601ToMicros("2024-01-15T10:30:00.123Z");
    // Expected: 2024-01-15T10:30:00.123Z
    // 2024-01-15 = 19737 days since epoch
    // 10:30:00 = 37800 seconds
    // 123ms = 123000 microseconds
    const expected = 19737 * 86400 * 1_000_000 + 37800 * 1_000_000 + 123_000;
    try std.testing.expectEqual(expected, ts);
}

test "parseIso8601ToMicros - no milliseconds" {
    const ts = try parseIso8601ToMicros("2024-01-15T10:30:00Z");
    const expected = 19737 * 86400 * 1_000_000 + 37800 * 1_000_000;
    try std.testing.expectEqual(expected, ts);
}

test "parseIso8601ToMicros - epoch" {
    const ts = try parseIso8601ToMicros("1970-01-01T00:00:00Z");
    try std.testing.expectEqual(@as(i64, 0), ts);
}

test "parseIso8601ToMicros - rejects no Z suffix" {
    const result = parseIso8601ToMicros("2024-01-15T10:30:00");
    try std.testing.expectError(error.InvalidFormat, result);
}

test "parseIso8601ToMicros - rejects invalid year" {
    const result = parseIso8601ToMicros("1900-01-15T10:30:00Z");
    try std.testing.expectError(error.InvalidFormat, result);
}

test "parseIso8601ToMicros - single digit millis" {
    // ".1" should be interpreted as 100ms
    const ts = try parseIso8601ToMicros("2024-01-15T10:30:00.1Z");
    const expected = 19737 * 86400 * 1_000_000 + 37800 * 1_000_000 + 100_000;
    try std.testing.expectEqual(expected, ts);
}

test "parseIso8601ToMicros - two digit millis" {
    // ".12" should be interpreted as 120ms
    const ts = try parseIso8601ToMicros("2024-01-15T10:30:00.12Z");
    const expected = 19737 * 86400 * 1_000_000 + 37800 * 1_000_000 + 120_000;
    try std.testing.expectEqual(expected, ts);
}

test "parseJsonEvents - single event with ISO timestamp" {
    const json =
        \\[{"id":"abc-123","type":"orderPlaced","timestamp":"2024-01-15T10:30:00.000Z","value":{"qty":5}}]
    ;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 1), cols.count);

    try std.testing.expectEqualStrings("abc-123", cols.getId(0).?);
    try std.testing.expectEqualStrings("orderPlaced", cols.getType(0).?);
    try std.testing.expect(cols.hasValue(0));
}

test "parseJsonEvents - numeric timestamp" {
    const json =
        \\[{"id":"id-1","type":"test","timestamp":1705315800000}]
    ;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.OK, result);

    // 1705315800000ms * 1000 = microseconds
    const expected_us: i64 = 1705315800000 * 1000;
    try std.testing.expectEqual(expected_us, cols.getTimestamp(0).?);
}

test "parseJsonEvents - event without value" {
    const json =
        \\[{"id":"id-1","type":"test","timestamp":"1970-01-01T00:00:00Z"}]
    ;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expect(!cols.hasValue(0));
}

test "parseJsonEvents - multiple events" {
    const json =
        \\[
        \\  {"id":"id-1","type":"a","timestamp":"1970-01-01T00:00:00Z","value":1},
        \\  {"id":"id-2","type":"b","timestamp":"1970-01-01T00:00:01Z"},
        \\  {"id":"id-3","type":"c","timestamp":"1970-01-01T00:00:02Z","value":"str"}
        \\]
    ;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 3), cols.count);

    try std.testing.expectEqualStrings("id-1", cols.getId(0).?);
    try std.testing.expectEqualStrings("id-2", cols.getId(1).?);
    try std.testing.expectEqualStrings("id-3", cols.getId(2).?);

    try std.testing.expect(cols.hasValue(0));
    try std.testing.expect(!cols.hasValue(1));
    try std.testing.expect(cols.hasValue(2));
}

test "parseJsonEvents - invalid JSON returns error" {
    const json = "{not valid json";

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.INVALID_JSON, result);
}

test "parseJsonEvents - missing required field returns error" {
    // Missing 'type' field
    const json =
        \\[{"id":"id-1","timestamp":"1970-01-01T00:00:00Z"}]
    ;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.MISSING_FIELD, result);
}

test "parseJsonEvents - nested value preserved as raw JSON" {
    const json =
        \\[{"id":"id-1","type":"test","timestamp":"1970-01-01T00:00:00Z","value":{"nested":{"deep":true}}}]
    ;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expect(cols.hasValue(0));

    // Value should be the raw JSON substring
    const value = cols.getValue(0).?;
    try std.testing.expectEqualStrings("{\"nested\":{\"deep\":true}}", value);
}

test "parseJsonEvents - empty array" {
    const json = "[]";

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseJsonEvents(json, &cols, std.testing.allocator);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 0), cols.count);
}
