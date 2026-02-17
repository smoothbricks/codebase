//! Msgpack event parser (base path)
//!
//! Parses msgpack event maps into columnar EventColumns format.
//! Hand-rolled byte-level reader for zero-copy parsing — no Payload allocation.
//!
//! Supports two input formats:
//! - MSGPACK (array): standard msgpack array of map objects `[{...}, {...}]`
//! - MSGPACK_STREAM: concatenated msgpack maps `{...}{...}{...}` (zero-parse SQS concat)
//!
//! Timestamp support:
//! - Integer milliseconds (JS Date.getTime() style) → microseconds
//! - ISO-8601 string with Z suffix (reuses json_scanner logic)

const std = @import("std");
const columns = @import("columns");
const EventColumns = columns.EventColumns;
const ParseError = columns.ParseError;

// =============================================================================
// Public API
// =============================================================================

/// Parse standard msgpack array of event maps
///
/// Expected format: array header followed by N map objects.
/// Each map has keys: "id", "type", "timestamp", "value" (optional).
pub fn parseMsgpackEvents(input: []const u8, output: *EventColumns) ParseError {
    if (input.len == 0) return .OK;

    var pos: usize = 0;
    const n = readArrayHeader(input, &pos) orelse return .INVALID_MSGPACK;

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const err = parseEventMap(input, &pos, output);
        if (err != .OK) return err;
    }

    return .OK;
}

/// Parse concatenated msgpack maps (stream format)
///
/// Expected format: consecutive map objects with no array wrapper.
/// Reads maps until end of input.
pub fn parseMsgpackStream(input: []const u8, output: *EventColumns) ParseError {
    if (input.len == 0) return .OK;

    var pos: usize = 0;
    while (pos < input.len) {
        const err = parseEventMap(input, &pos, output);
        if (err != .OK) return err;
    }

    return .OK;
}

// =============================================================================
// Event Map Parsing
// =============================================================================

/// Parse a single msgpack map into an event row
fn parseEventMap(input: []const u8, pos: *usize, output: *EventColumns) ParseError {
    const n_fields = readMapHeader(input, pos) orelse return .INVALID_MSGPACK;

    var id: ?[]const u8 = null;
    var event_type: ?[]const u8 = null;
    var timestamp_us: ?i64 = null;
    var value_start: usize = 0;
    var value_end: usize = 0;
    var has_value = false;

    var i: u32 = 0;
    while (i < n_fields) : (i += 1) {
        // Read field name (must be string)
        const key = readString(input, pos) orelse return .INVALID_MSGPACK;

        if (std.mem.eql(u8, key, "id")) {
            id = readString(input, pos) orelse return .INVALID_FIELD_TYPE;
        } else if (std.mem.eql(u8, key, "type")) {
            event_type = readString(input, pos) orelse return .INVALID_FIELD_TYPE;
        } else if (std.mem.eql(u8, key, "timestamp")) {
            timestamp_us = readTimestamp(input, pos) orelse return .INVALID_FIELD_TYPE;
        } else if (std.mem.eql(u8, key, "value")) {
            // Record raw msgpack byte range for value (zero-copy)
            value_start = pos.*;
            skipValue(input, pos) orelse return .INVALID_MSGPACK;
            value_end = pos.*;
            has_value = true;
        } else {
            // Unknown field — skip
            skipValue(input, pos) orelse return .INVALID_MSGPACK;
        }
    }

    // Validate required fields
    const id_val = id orelse return .MISSING_FIELD;
    const type_val = event_type orelse return .MISSING_FIELD;
    const ts_val = timestamp_us orelse return .MISSING_FIELD;

    // Value stored as raw msgpack bytes (nil value excluded)
    const value_bytes: ?[]const u8 = if (has_value and value_start < value_end)
        // Check if value is msgpack nil (0xc0) — treat as null
        if (value_end - value_start == 1 and input[value_start] == 0xc0)
            null
        else
            input[value_start..value_end]
    else
        null;

    return output.addEvent(id_val, type_val, ts_val, value_bytes);
}

// =============================================================================
// Msgpack Readers — zero-copy byte-level primitives
// =============================================================================

/// Read map header, return field count. Returns null on invalid format or EOF.
fn readMapHeader(input: []const u8, pos: *usize) ?u32 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    if (b & 0xf0 == 0x80) {
        // fixmap: lower 4 bits = count
        pos.* += 1;
        return @intCast(b & 0x0f);
    } else if (b == 0xde) {
        // map16
        if (pos.* + 3 > input.len) return null;
        const n = readU16(input, pos.* + 1);
        pos.* += 3;
        return @intCast(n);
    } else if (b == 0xdf) {
        // map32
        if (pos.* + 5 > input.len) return null;
        const n = readU32(input, pos.* + 1);
        pos.* += 5;
        return n;
    }
    return null;
}

/// Read array header, return element count.
fn readArrayHeader(input: []const u8, pos: *usize) ?u32 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    if (b & 0xf0 == 0x90) {
        // fixarray: lower 4 bits = count
        pos.* += 1;
        return @intCast(b & 0x0f);
    } else if (b == 0xdc) {
        // array16
        if (pos.* + 3 > input.len) return null;
        const n = readU16(input, pos.* + 1);
        pos.* += 3;
        return @intCast(n);
    } else if (b == 0xdd) {
        // array32
        if (pos.* + 5 > input.len) return null;
        const n = readU32(input, pos.* + 1);
        pos.* += 5;
        return n;
    }
    return null;
}

/// Read a string value. Returns slice into input (zero-copy). Null on non-string or EOF.
fn readString(input: []const u8, pos: *usize) ?[]const u8 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    var len: u32 = undefined;
    var header_size: usize = undefined;

    if (b & 0xe0 == 0xa0) {
        // fixstr: lower 5 bits = length
        len = @intCast(b & 0x1f);
        header_size = 1;
    } else if (b == 0xd9) {
        // str8
        if (pos.* + 2 > input.len) return null;
        len = @intCast(input[pos.* + 1]);
        header_size = 2;
    } else if (b == 0xda) {
        // str16
        if (pos.* + 3 > input.len) return null;
        len = @intCast(readU16(input, pos.* + 1));
        header_size = 3;
    } else if (b == 0xdb) {
        // str32
        if (pos.* + 5 > input.len) return null;
        len = readU32(input, pos.* + 1);
        header_size = 5;
    } else {
        return null;
    }

    const data_start = pos.* + header_size;
    const data_end = data_start + len;
    if (data_end > input.len) return null;

    pos.* = data_end;
    return input[data_start..data_end];
}

/// Read an integer (signed or unsigned) and return as i64.
fn readInteger(input: []const u8, pos: *usize) ?i64 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    // positive fixint: 0x00-0x7f
    if (b & 0x80 == 0) {
        pos.* += 1;
        return @intCast(b);
    }

    // negative fixint: 0xe0-0xff
    if (b & 0xe0 == 0xe0) {
        pos.* += 1;
        return @as(i64, @as(i8, @bitCast(b)));
    }

    switch (b) {
        0xcc => {
            // uint8
            if (pos.* + 2 > input.len) return null;
            pos.* += 2;
            return @intCast(input[pos.* - 1]);
        },
        0xcd => {
            // uint16
            if (pos.* + 3 > input.len) return null;
            const v = readU16(input, pos.* + 1);
            pos.* += 3;
            return @intCast(v);
        },
        0xce => {
            // uint32
            if (pos.* + 5 > input.len) return null;
            const v = readU32(input, pos.* + 1);
            pos.* += 5;
            return @intCast(v);
        },
        0xcf => {
            // uint64
            if (pos.* + 9 > input.len) return null;
            const v = readU64(input, pos.* + 1);
            pos.* += 9;
            // u64 may not fit in i64 — timestamps always fit
            return @intCast(v);
        },
        0xd0 => {
            // int8
            if (pos.* + 2 > input.len) return null;
            pos.* += 2;
            return @as(i64, @as(i8, @bitCast(input[pos.* - 1])));
        },
        0xd1 => {
            // int16
            if (pos.* + 3 > input.len) return null;
            const v: i16 = @bitCast(readU16(input, pos.* + 1));
            pos.* += 3;
            return @intCast(v);
        },
        0xd2 => {
            // int32
            if (pos.* + 5 > input.len) return null;
            const v: i32 = @bitCast(readU32(input, pos.* + 1));
            pos.* += 5;
            return @intCast(v);
        },
        0xd3 => {
            // int64
            if (pos.* + 9 > input.len) return null;
            const v: i64 = @bitCast(readU64(input, pos.* + 1));
            pos.* += 9;
            return v;
        },
        else => return null,
    }
}

/// Read a float value and return as f64.
fn readFloat(input: []const u8, pos: *usize) ?f64 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    if (b == 0xca) {
        // float32
        if (pos.* + 5 > input.len) return null;
        const bits = readU32(input, pos.* + 1);
        pos.* += 5;
        return @floatCast(@as(f32, @bitCast(bits)));
    } else if (b == 0xcb) {
        // float64
        if (pos.* + 9 > input.len) return null;
        const bits = readU64(input, pos.* + 1);
        pos.* += 9;
        return @bitCast(bits);
    }
    return null;
}

/// Read timestamp: integer (milliseconds) or string (ISO-8601) → microseconds.
fn readTimestamp(input: []const u8, pos: *usize) ?i64 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    // Try integer first (most common for SQS — JS Date.getTime())
    if (b & 0x80 == 0 or b & 0xe0 == 0xe0 or (b >= 0xcc and b <= 0xd3)) {
        const ms = readInteger(input, pos) orelse return null;
        return ms * 1000; // ms → μs
    }

    // Try float (some encoders use float for timestamps)
    if (b == 0xca or b == 0xcb) {
        const f = readFloat(input, pos) orelse return null;
        const ms: i64 = @intFromFloat(f);
        return ms * 1000; // ms → μs
    }

    // Try string (ISO-8601)
    if (b & 0xe0 == 0xa0 or b == 0xd9 or b == 0xda or b == 0xdb) {
        const s = readString(input, pos) orelse return null;
        return parseIso8601ToMicros(s) catch return null;
    }

    return null;
}

/// Skip any msgpack value (recursively for containers). Returns null on invalid/truncated input.
fn skipValue(input: []const u8, pos: *usize) ?void {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    // positive fixint
    if (b & 0x80 == 0) {
        pos.* += 1;
        return;
    }

    // fixmap
    if (b & 0xf0 == 0x80) {
        const n: u32 = @intCast(b & 0x0f);
        pos.* += 1;
        var i: u32 = 0;
        while (i < n * 2) : (i += 1) {
            skipValue(input, pos) orelse return null;
        }
        return;
    }

    // fixarray
    if (b & 0xf0 == 0x90) {
        const n: u32 = @intCast(b & 0x0f);
        pos.* += 1;
        var i: u32 = 0;
        while (i < n) : (i += 1) {
            skipValue(input, pos) orelse return null;
        }
        return;
    }

    // fixstr
    if (b & 0xe0 == 0xa0) {
        const len: u32 = @intCast(b & 0x1f);
        pos.* = pos.* + 1 + len;
        if (pos.* > input.len) return null;
        return;
    }

    // negative fixint
    if (b & 0xe0 == 0xe0) {
        pos.* += 1;
        return;
    }

    switch (b) {
        // nil, false, true
        0xc0, 0xc2, 0xc3 => {
            pos.* += 1;
            return;
        },

        // bin8
        0xc4 => {
            if (pos.* + 2 > input.len) return null;
            const len: u32 = @intCast(input[pos.* + 1]);
            pos.* = pos.* + 2 + len;
            if (pos.* > input.len) return null;
            return;
        },
        // bin16
        0xc5 => {
            if (pos.* + 3 > input.len) return null;
            const len: u32 = @intCast(readU16(input, pos.* + 1));
            pos.* = pos.* + 3 + len;
            if (pos.* > input.len) return null;
            return;
        },
        // bin32
        0xc6 => {
            if (pos.* + 5 > input.len) return null;
            const len = readU32(input, pos.* + 1);
            pos.* = pos.* + 5 + len;
            if (pos.* > input.len) return null;
            return;
        },

        // ext8
        0xc7 => {
            if (pos.* + 2 > input.len) return null;
            const len: u32 = @intCast(input[pos.* + 1]);
            pos.* = pos.* + 3 + len; // 1 header + 1 len + 1 type + data
            if (pos.* > input.len) return null;
            return;
        },
        // ext16
        0xc8 => {
            if (pos.* + 3 > input.len) return null;
            const len: u32 = @intCast(readU16(input, pos.* + 1));
            pos.* = pos.* + 4 + len;
            if (pos.* > input.len) return null;
            return;
        },
        // ext32
        0xc9 => {
            if (pos.* + 5 > input.len) return null;
            const len = readU32(input, pos.* + 1);
            pos.* = pos.* + 6 + len;
            if (pos.* > input.len) return null;
            return;
        },

        // float32
        0xca => {
            pos.* += 5;
            if (pos.* > input.len) return null;
            return;
        },
        // float64
        0xcb => {
            pos.* += 9;
            if (pos.* > input.len) return null;
            return;
        },

        // uint8, int8
        0xcc, 0xd0 => {
            pos.* += 2;
            if (pos.* > input.len) return null;
            return;
        },
        // uint16, int16
        0xcd, 0xd1 => {
            pos.* += 3;
            if (pos.* > input.len) return null;
            return;
        },
        // uint32, int32
        0xce, 0xd2 => {
            pos.* += 5;
            if (pos.* > input.len) return null;
            return;
        },
        // uint64, int64
        0xcf, 0xd3 => {
            pos.* += 9;
            if (pos.* > input.len) return null;
            return;
        },

        // fixext1
        0xd4 => {
            pos.* += 3;
            if (pos.* > input.len) return null;
            return;
        },
        // fixext2
        0xd5 => {
            pos.* += 4;
            if (pos.* > input.len) return null;
            return;
        },
        // fixext4
        0xd6 => {
            pos.* += 6;
            if (pos.* > input.len) return null;
            return;
        },
        // fixext8
        0xd7 => {
            pos.* += 10;
            if (pos.* > input.len) return null;
            return;
        },
        // fixext16
        0xd8 => {
            pos.* += 18;
            if (pos.* > input.len) return null;
            return;
        },

        // str8
        0xd9 => {
            if (pos.* + 2 > input.len) return null;
            const len: u32 = @intCast(input[pos.* + 1]);
            pos.* = pos.* + 2 + len;
            if (pos.* > input.len) return null;
            return;
        },
        // str16
        0xda => {
            if (pos.* + 3 > input.len) return null;
            const len: u32 = @intCast(readU16(input, pos.* + 1));
            pos.* = pos.* + 3 + len;
            if (pos.* > input.len) return null;
            return;
        },
        // str32
        0xdb => {
            if (pos.* + 5 > input.len) return null;
            const len = readU32(input, pos.* + 1);
            pos.* = pos.* + 5 + len;
            if (pos.* > input.len) return null;
            return;
        },

        // array16
        0xdc => {
            if (pos.* + 3 > input.len) return null;
            const n: u32 = @intCast(readU16(input, pos.* + 1));
            pos.* += 3;
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                skipValue(input, pos) orelse return null;
            }
            return;
        },
        // array32
        0xdd => {
            if (pos.* + 5 > input.len) return null;
            const n = readU32(input, pos.* + 1);
            pos.* += 5;
            var i: u32 = 0;
            while (i < n) : (i += 1) {
                skipValue(input, pos) orelse return null;
            }
            return;
        },

        // map16
        0xde => {
            if (pos.* + 3 > input.len) return null;
            const n: u32 = @intCast(readU16(input, pos.* + 1));
            pos.* += 3;
            var i: u32 = 0;
            while (i < n * 2) : (i += 1) {
                skipValue(input, pos) orelse return null;
            }
            return;
        },
        // map32
        0xdf => {
            if (pos.* + 5 > input.len) return null;
            const n = readU32(input, pos.* + 1);
            pos.* += 5;
            var i: u32 = 0;
            while (i < n * 2) : (i += 1) {
                skipValue(input, pos) orelse return null;
            }
            return;
        },

        // 0xc1 is never used (reserved)
        else => return null,
    }
}

// =============================================================================
// Big-endian byte readers
// =============================================================================

inline fn readU16(input: []const u8, offset: usize) u16 {
    return std.mem.readInt(u16, input[offset..][0..2], .big);
}

inline fn readU32(input: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, input[offset..][0..4], .big);
}

inline fn readU64(input: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, input[offset..][0..8], .big);
}

// =============================================================================
// ISO-8601 timestamp parsing (duplicated from json_scanner — pure arithmetic)
// =============================================================================

/// Parse ISO-8601 timestamp to microseconds since epoch
///
/// Supported: "2024-01-15T10:30:00.123Z", "2024-01-15T10:30:00Z"
/// Only UTC (Z suffix), millisecond precision max, year range 1970-2099.
pub fn parseIso8601ToMicros(s: []const u8) !i64 {
    if (s.len < 20) return error.InvalidFormat;
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

    if (year < 1970 or year > 2099) return error.InvalidFormat;
    if (month < 1 or month > 12) return error.InvalidFormat;
    if (day < 1 or day > 31) return error.InvalidFormat;
    if (hour > 23) return error.InvalidFormat;
    if (minute > 59) return error.InvalidFormat;
    if (second > 59) return error.InvalidFormat;

    var millis: i64 = 0;
    if (s.len > 20 and s[19] == '.') {
        const frac_end = s.len - 1;
        const frac_str = s[20..frac_end];
        if (frac_str.len >= 3) {
            millis = std.fmt.parseInt(i64, frac_str[0..3], 10) catch 0;
        } else if (frac_str.len > 0) {
            var padded: [3]u8 = [_]u8{ '0', '0', '0' };
            @memcpy(padded[0..frac_str.len], frac_str);
            millis = std.fmt.parseInt(i64, &padded, 10) catch 0;
        }
    }

    const epoch_days = epochDays(year, month, day);
    const day_seconds = @as(i64, hour) * 3600 + @as(i64, minute) * 60 + @as(i64, second);
    const total_seconds = epoch_days * 86400 + day_seconds;
    return total_seconds * 1_000_000 + millis * 1_000;
}

/// Days since Unix epoch (Howard Hinnant civil calendar algorithm)
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

/// Helper: build a simple msgpack map with id, type, timestamp (int ms), value
fn buildTestEvent(
    buf: []u8,
    id: []const u8,
    event_type: []const u8,
    timestamp_ms: i64,
    value: ?[]const u8,
) usize {
    var pos: usize = 0;
    const n_fields: u8 = if (value != null) 4 else 3;

    // fixmap header
    buf[pos] = 0x80 | n_fields;
    pos += 1;

    // "id" key + string value
    pos += writeTestStr(buf[pos..], "id");
    pos += writeTestStr(buf[pos..], id);

    // "type" key + string value
    pos += writeTestStr(buf[pos..], "type");
    pos += writeTestStr(buf[pos..], event_type);

    // "timestamp" key + int64 value
    pos += writeTestStr(buf[pos..], "timestamp");
    buf[pos] = 0xd3; // int64
    pos += 1;
    std.mem.writeInt(i64, buf[pos..][0..8], timestamp_ms, .big);
    pos += 8;

    // "value" key + raw msgpack (or omit)
    if (value) |v| {
        pos += writeTestStr(buf[pos..], "value");
        @memcpy(buf[pos..][0..v.len], v);
        pos += v.len;
    }

    return pos;
}

fn writeTestStr(buf: []u8, s: []const u8) usize {
    std.debug.assert(s.len < 32); // fixstr only
    buf[0] = 0xa0 | @as(u8, @intCast(s.len));
    @memcpy(buf[1..][0..s.len], s);
    return 1 + s.len;
}

test "parseMsgpackStream - single event with int timestamp" {
    var input: [256]u8 = undefined;
    // value = msgpack fixmap {"qty": 5}
    const correct_value = [_]u8{
        0x81, // fixmap(1)
        0xa3, 'q', 't', 'y', // fixstr(3) "qty"
        0x05, // positive fixint 5
    };
    const event_len = buildTestEvent(&input, "abc-123", "orderPlaced", 1705315800000, &correct_value);

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..event_len], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expectEqualStrings("abc-123", cols.getId(0).?);
    try std.testing.expectEqualStrings("orderPlaced", cols.getType(0).?);
    // 1705315800000ms * 1000 = μs
    try std.testing.expectEqual(@as(i64, 1705315800000 * 1000), cols.getTimestamp(0).?);
    try std.testing.expect(cols.hasValue(0));
}

test "parseMsgpackStream - multiple events concatenated" {
    var input: [512]u8 = undefined;
    const len1 = buildTestEvent(&input, "id-1", "a", 1000, null);
    const len2 = buildTestEvent(input[len1..], "id-2", "b", 2000, null);
    const total = len1 + len2;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..total], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 2), cols.count);
    try std.testing.expectEqualStrings("id-1", cols.getId(0).?);
    try std.testing.expectEqualStrings("id-2", cols.getId(1).?);
    try std.testing.expectEqual(@as(i64, 1000 * 1000), cols.getTimestamp(0).?);
    try std.testing.expectEqual(@as(i64, 2000 * 1000), cols.getTimestamp(1).?);
}

test "parseMsgpackEvents - array format" {
    var input: [512]u8 = undefined;
    // Build array header for 2 events
    input[0] = 0x92; // fixarray(2)
    const len1 = buildTestEvent(input[1..], "id-1", "a", 1000, null);
    const len2 = buildTestEvent(input[1 + len1 ..], "id-2", "b", 2000, null);
    const total = 1 + len1 + len2;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackEvents(input[0..total], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 2), cols.count);
    try std.testing.expectEqualStrings("id-1", cols.getId(0).?);
    try std.testing.expectEqualStrings("id-2", cols.getId(1).?);
}

test "parseMsgpackStream - string timestamp (ISO-8601)" {
    var input: [256]u8 = undefined;
    var pos: usize = 0;

    // Build manually: fixmap(3), id, type, timestamp as string
    input[pos] = 0x83; // fixmap(3)
    pos += 1;
    pos += writeTestStr(input[pos..], "id");
    pos += writeTestStr(input[pos..], "id-1");
    pos += writeTestStr(input[pos..], "type");
    pos += writeTestStr(input[pos..], "test");
    pos += writeTestStr(input[pos..], "timestamp");
    // ISO-8601 string "1970-01-01T00:00:00Z" (20 chars)
    const ts = "1970-01-01T00:00:00Z";
    input[pos] = 0xa0 | @as(u8, @intCast(ts.len));
    pos += 1;
    @memcpy(input[pos..][0..ts.len], ts);
    pos += ts.len;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..pos], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(i64, 0), cols.getTimestamp(0).?);
}

test "parseMsgpackStream - missing required field" {
    var input: [256]u8 = undefined;
    var pos: usize = 0;

    // fixmap(2): only id and timestamp, missing type
    input[pos] = 0x82;
    pos += 1;
    pos += writeTestStr(input[pos..], "id");
    pos += writeTestStr(input[pos..], "id-1");
    pos += writeTestStr(input[pos..], "timestamp");
    input[pos] = 0xd3; // int64
    pos += 1;
    std.mem.writeInt(i64, input[pos..][0..8], 1000, .big);
    pos += 8;

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..pos], &cols);
    try std.testing.expectEqual(ParseError.MISSING_FIELD, result);
}

test "parseMsgpackStream - empty input" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(&[_]u8{}, &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 0), cols.count);
}

test "parseMsgpackEvents - empty array" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    // fixarray(0) = 0x90
    const result = parseMsgpackEvents(&[_]u8{0x90}, &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 0), cols.count);
}

test "parseMsgpackStream - value preserved as raw msgpack" {
    var input: [256]u8 = undefined;
    // Value is msgpack string "hello"
    const value_bytes = [_]u8{ 0xa5, 'h', 'e', 'l', 'l', 'o' };
    const event_len = buildTestEvent(&input, "id-1", "test", 1000, &value_bytes);

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..event_len], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expect(cols.hasValue(0));

    // Raw msgpack bytes should be preserved
    const raw = cols.getValue(0).?;
    try std.testing.expectEqualSlices(u8, &value_bytes, raw);
}

test "parseMsgpackStream - nil value treated as null" {
    var input: [256]u8 = undefined;
    // Value is msgpack nil (0xc0)
    const nil_bytes = [_]u8{0xc0};
    const event_len = buildTestEvent(&input, "id-1", "test", 1000, &nil_bytes);

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..event_len], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    // nil value should be treated as absent
    try std.testing.expect(!cols.hasValue(0));
}

test "parseMsgpackStream - event without value field" {
    var input: [256]u8 = undefined;
    const event_len = buildTestEvent(&input, "id-1", "test", 1000, null);

    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = parseMsgpackStream(input[0..event_len], &cols);
    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expect(!cols.hasValue(0));
}

test "parseMsgpackStream - invalid input (not a map)" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    // 0xa5 = fixstr(5), not a map
    const result = parseMsgpackStream(&[_]u8{ 0xa5, 'h', 'e', 'l', 'l', 'o' }, &cols);
    try std.testing.expectEqual(ParseError.INVALID_MSGPACK, result);
}
