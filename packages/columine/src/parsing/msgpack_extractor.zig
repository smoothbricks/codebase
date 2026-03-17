//! Msgpack Field Extraction to Typed Columns
//!
//! Extracts msgpack event fields directly to Arrow columns based on schema.
//! - Declared fields (in schema) → typed columns
//! - Undeclared fields → $extra msgpack Binary column (zero-copy from input)
//!
//! Key advantage over JSON extraction: undeclared fields are already msgpack,
//! so $extra column is built by memcpy, not by JSON→msgpack conversion.
//! Binary-typed declared fields are zero-copy slices of the input buffer.

const std = @import("std");
const columns = @import("columns");
const dynamic_schema = @import("dynamic_schema");
const json_extractor = @import("json_extractor");

// Reuse types from json_extractor — single source of truth for extraction config
const ExtractionConfig = json_extractor.ExtractionConfig;
const ExtractionError = json_extractor.ExtractionError;
const ArrowType = dynamic_schema.ArrowType;
const DynamicColumns = columns.DynamicColumns;

// Import scanner helpers for low-level msgpack reading
const scanner = @import("msgpack_scanner");

// =============================================================================
// Public API
// =============================================================================

/// Extract msgpack events to typed DynamicColumns
///
/// @param input - msgpack bytes (array or stream format)
/// @param config - extraction config with field mappings
/// @param dynamic_cols - output columns (must be reset before calling)
/// @param work_buffer - temporary buffer for $extra map assembly
/// @param is_stream - true for MSGPACK_STREAM (concatenated maps), false for MSGPACK (array)
/// @return count of events extracted
pub fn extractMsgpackEvents(
    input: []const u8,
    config: *const ExtractionConfig,
    dynamic_cols: *DynamicColumns,
    work_buffer: []u8,
    is_stream: bool,
) ExtractionError!u32 {
    if (input.len == 0) return 0;

    var pos: usize = 0;
    var count: u32 = 0;

    if (!is_stream) {
        // MSGPACK: standard array header
        const n = readArrayHeader(input, &pos) orelse return error.InvalidJson;
        var i: u32 = 0;
        while (i < n) : (i += 1) {
            try extractMsgpackEvent(input, &pos, config, dynamic_cols, work_buffer);
            count += 1;
            if (count >= dynamic_cols.capacity) return error.TooManyEvents;
        }
    } else {
        // MSGPACK_STREAM: concatenated maps until EOF
        while (pos < input.len) {
            try extractMsgpackEvent(input, &pos, config, dynamic_cols, work_buffer);
            count += 1;
            if (count >= dynamic_cols.capacity) return error.TooManyEvents;
        }
    }

    return count;
}

// =============================================================================
// Per-Event Extraction
// =============================================================================

/// Extract a single msgpack event map to typed columns
fn extractMsgpackEvent(
    input: []const u8,
    pos: *usize,
    config: *const ExtractionConfig,
    dynamic_cols: *DynamicColumns,
    work_buffer: []u8,
) ExtractionError!void {
    if (config.num_columns > 64) return error.OutOfMemory;

    const n_fields = readMapHeader(input, pos) orelse return error.InvalidJson;

    if (!dynamic_cols.beginRow()) return error.TooManyEvents;

    // Track which columns have been set (for null-fill)
    var columns_set: [64]bool = [_]bool{false} ** 64;

    // $extra column: accumulate undeclared field bytes directly from input
    var extra_buf_pos: usize = 5; // reserve 5 bytes for map32 header
    var extra_count: u32 = 0;
    const extra_active = config.fallback_col_idx != null and work_buffer.len >= 5;

    var i: u32 = 0;
    while (i < n_fields) : (i += 1) {
        // Read field key (must be string)
        const key_start = pos.*;
        const key = readString(input, pos) orelse return error.InvalidJson;
        const key_end = pos.*;

        // Look up in extraction config (O(1) hash)
        const lookup = config.field_map.get(key);

        if (lookup) |e| {
            // Declared field — extract typed value
            try extractTypedValue(input, pos, e.arrow_type, dynamic_cols, e.col_idx);
            columns_set[e.col_idx] = true;
        } else {
            // Undeclared field — copy raw msgpack key+value bytes to work_buffer
            if (extra_active) {
                const value_start = pos.*;
                skipValue(input, pos) orelse return error.InvalidJson;
                const value_end = pos.*;

                // Key bytes (from input) + value bytes (from input)
                const key_bytes = input[key_start..key_end];
                const value_bytes = input[value_start..value_end];
                const needed = key_bytes.len + value_bytes.len;

                if (extra_buf_pos + needed > work_buffer.len) {
                    return error.BufferOverflow;
                }

                @memcpy(work_buffer[extra_buf_pos..][0..key_bytes.len], key_bytes);
                extra_buf_pos += key_bytes.len;
                @memcpy(work_buffer[extra_buf_pos..][0..value_bytes.len], value_bytes);
                extra_buf_pos += value_bytes.len;
                extra_count += 1;
            } else {
                skipValue(input, pos) orelse return error.InvalidJson;
            }
        }
    }

    // Null-fill unset declared columns
    for (config.field_entries) |e| {
        if (!columns_set[e.col_idx]) {
            _ = dynamic_cols.appendNull(e.col_idx);
        }
    }

    // Finalize $extra column
    if (config.fallback_col_idx) |fallback_idx| {
        if (extra_count > 0) {
            // Patch map32 header: 0xdf + big-endian u32 count
            work_buffer[0] = 0xdf;
            std.mem.writeInt(u32, work_buffer[1..5], extra_count, .big);

            const result = dynamic_cols.appendBinary(fallback_idx, work_buffer[0..extra_buf_pos]);
            if (result != .OK) return error.BufferOverflow;
        } else {
            _ = dynamic_cols.appendNull(fallback_idx);
        }
    }

    dynamic_cols.endRow();
}

/// Extract a typed value from msgpack input and append to the appropriate column
fn extractTypedValue(
    input: []const u8,
    pos: *usize,
    arrow_type: ArrowType,
    dynamic_cols: *DynamicColumns,
    col_idx: u32,
) ExtractionError!void {
    if (pos.* >= input.len) return error.InvalidJson;
    const b = input[pos.*];

    switch (arrow_type) {
        .Utf8 => {
            // Expect string or nil
            if (b == 0xc0) {
                pos.* += 1;
                _ = dynamic_cols.appendNull(col_idx);
                return;
            }
            const s = readString(input, pos) orelse return error.InvalidFieldType;
            const result = dynamic_cols.appendUtf8(col_idx, s);
            if (result != .OK) return error.BufferOverflow;
        },
        .Int, .Int64 => {
            // Integer, float (truncated), or string (ISO-8601 timestamp)
            if (b == 0xc0) {
                pos.* += 1;
                _ = dynamic_cols.appendNull(col_idx);
                return;
            }
            // Try integer first
            if (b & 0x80 == 0 or b & 0xe0 == 0xe0 or (b >= 0xcc and b <= 0xd3)) {
                const v = readInteger(input, pos) orelse return error.InvalidFieldType;
                const result = dynamic_cols.appendInt64(col_idx, v);
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            // Try float → truncate to int
            if (b == 0xca or b == 0xcb) {
                const f = readFloat(input, pos) orelse return error.InvalidFieldType;
                const v: i64 = @intFromFloat(f);
                const result = dynamic_cols.appendInt64(col_idx, v);
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            // Try string (ISO-8601 timestamp → microseconds)
            if (b & 0xe0 == 0xa0 or b == 0xd9 or b == 0xda or b == 0xdb) {
                const s = readString(input, pos) orelse return error.InvalidFieldType;
                const micros = scanner.parseIso8601ToMicros(s) catch return error.InvalidFieldType;
                const result = dynamic_cols.appendInt64(col_idx, micros);
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            return error.InvalidFieldType;
        },
        .FloatingPoint => {
            if (b == 0xc0) {
                pos.* += 1;
                _ = dynamic_cols.appendNull(col_idx);
                return;
            }
            // Try float
            if (b == 0xca or b == 0xcb) {
                const f = readFloat(input, pos) orelse return error.InvalidFieldType;
                const result = dynamic_cols.appendFloat64(col_idx, f);
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            // Integer → float promotion
            if (b & 0x80 == 0 or b & 0xe0 == 0xe0 or (b >= 0xcc and b <= 0xd3)) {
                const v = readInteger(input, pos) orelse return error.InvalidFieldType;
                const result = dynamic_cols.appendFloat64(col_idx, @floatFromInt(v));
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            return error.InvalidFieldType;
        },
        .Bool => {
            if (b == 0xc0) {
                pos.* += 1;
                _ = dynamic_cols.appendNull(col_idx);
                return;
            }
            if (b == 0xc3) {
                pos.* += 1;
                const result = dynamic_cols.appendBool(col_idx, true);
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            if (b == 0xc2) {
                pos.* += 1;
                const result = dynamic_cols.appendBool(col_idx, false);
                if (result != .OK) return error.BufferOverflow;
                return;
            }
            return error.InvalidFieldType;
        },
        .Binary => {
            // Store raw msgpack bytes of the value (zero-copy slice)
            if (b == 0xc0) {
                pos.* += 1;
                _ = dynamic_cols.appendNull(col_idx);
                return;
            }
            const value_start = pos.*;
            skipValue(input, pos) orelse return error.InvalidJson;
            const raw = input[value_start..pos.*];
            const result = dynamic_cols.appendBinary(col_idx, raw);
            if (result != .OK) return error.BufferOverflow;
        },
        .Null => {
            // Skip the value
            skipValue(input, pos) orelse return error.InvalidJson;
            _ = dynamic_cols.appendNull(col_idx);
        },
    }
}

// =============================================================================
// Msgpack Reader Wrappers
//
// These delegate to the scanner's low-level readers. We re-expose them here
// because the scanner module's helpers are private (not pub). Instead of
// making them pub (would pollute the scanner's API), we duplicate the minimal
// inline readers needed by the extractor.
// =============================================================================

fn readMapHeader(input: []const u8, pos: *usize) ?u32 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];
    if (b & 0xf0 == 0x80) {
        pos.* += 1;
        return @intCast(b & 0x0f);
    } else if (b == 0xde) {
        if (pos.* + 3 > input.len) return null;
        const n = std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big);
        pos.* += 3;
        return @intCast(n);
    } else if (b == 0xdf) {
        if (pos.* + 5 > input.len) return null;
        const n = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big);
        pos.* += 5;
        return n;
    }
    return null;
}

fn readArrayHeader(input: []const u8, pos: *usize) ?u32 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];
    if (b & 0xf0 == 0x90) {
        pos.* += 1;
        return @intCast(b & 0x0f);
    } else if (b == 0xdc) {
        if (pos.* + 3 > input.len) return null;
        const n = std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big);
        pos.* += 3;
        return @intCast(n);
    } else if (b == 0xdd) {
        if (pos.* + 5 > input.len) return null;
        const n = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big);
        pos.* += 5;
        return n;
    }
    return null;
}

fn readString(input: []const u8, pos: *usize) ?[]const u8 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];
    var len: u32 = undefined;
    var header_size: usize = undefined;
    if (b & 0xe0 == 0xa0) {
        len = @intCast(b & 0x1f);
        header_size = 1;
    } else if (b == 0xd9) {
        if (pos.* + 2 > input.len) return null;
        len = @intCast(input[pos.* + 1]);
        header_size = 2;
    } else if (b == 0xda) {
        if (pos.* + 3 > input.len) return null;
        len = @intCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big));
        header_size = 3;
    } else if (b == 0xdb) {
        if (pos.* + 5 > input.len) return null;
        len = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big);
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

fn readInteger(input: []const u8, pos: *usize) ?i64 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];
    if (b & 0x80 == 0) { pos.* += 1; return @intCast(b); }
    if (b & 0xe0 == 0xe0) { pos.* += 1; return @as(i64, @as(i8, @bitCast(b))); }
    switch (b) {
        0xcc => { if (pos.* + 2 > input.len) return null; pos.* += 2; return @intCast(input[pos.* - 1]); },
        0xcd => { if (pos.* + 3 > input.len) return null; const v = std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big); pos.* += 3; return @intCast(v); },
        0xce => { if (pos.* + 5 > input.len) return null; const v = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big); pos.* += 5; return @intCast(v); },
        0xcf => { if (pos.* + 9 > input.len) return null; const v = std.mem.readInt(u64, input[pos.* + 1 ..][0..8], .big); pos.* += 9; return @intCast(v); },
        0xd0 => { if (pos.* + 2 > input.len) return null; pos.* += 2; return @as(i64, @as(i8, @bitCast(input[pos.* - 1]))); },
        0xd1 => { if (pos.* + 3 > input.len) return null; const v: i16 = @bitCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big)); pos.* += 3; return @intCast(v); },
        0xd2 => { if (pos.* + 5 > input.len) return null; const v: i32 = @bitCast(std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big)); pos.* += 5; return @intCast(v); },
        0xd3 => { if (pos.* + 9 > input.len) return null; const v: i64 = @bitCast(std.mem.readInt(u64, input[pos.* + 1 ..][0..8], .big)); pos.* += 9; return v; },
        else => return null,
    }
}

fn readFloat(input: []const u8, pos: *usize) ?f64 {
    if (pos.* >= input.len) return null;
    const b = input[pos.*];
    if (b == 0xca) {
        if (pos.* + 5 > input.len) return null;
        const bits = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big);
        pos.* += 5;
        return @floatCast(@as(f32, @bitCast(bits)));
    } else if (b == 0xcb) {
        if (pos.* + 9 > input.len) return null;
        const bits = std.mem.readInt(u64, input[pos.* + 1 ..][0..8], .big);
        pos.* += 9;
        return @bitCast(bits);
    }
    return null;
}

fn skipValue(input: []const u8, pos: *usize) ?void {
    // Delegate to the scanner's skip logic (re-implemented here since scanner's is private)
    if (pos.* >= input.len) return null;
    const b = input[pos.*];

    if (b & 0x80 == 0) { pos.* += 1; return; }
    if (b & 0xf0 == 0x80) { const n: u32 = @intCast(b & 0x0f); pos.* += 1; var j: u32 = 0; while (j < n * 2) : (j += 1) { skipValue(input, pos) orelse return null; } return; }
    if (b & 0xf0 == 0x90) { const n: u32 = @intCast(b & 0x0f); pos.* += 1; var j: u32 = 0; while (j < n) : (j += 1) { skipValue(input, pos) orelse return null; } return; }
    if (b & 0xe0 == 0xa0) { const l: u32 = @intCast(b & 0x1f); pos.* = pos.* + 1 + l; if (pos.* > input.len) return null; return; }
    if (b & 0xe0 == 0xe0) { pos.* += 1; return; }

    switch (b) {
        0xc0, 0xc2, 0xc3 => { pos.* += 1; return; },
        0xc4 => { if (pos.* + 2 > input.len) return null; const l: u32 = @intCast(input[pos.* + 1]); pos.* = pos.* + 2 + l; if (pos.* > input.len) return null; return; },
        0xc5 => { if (pos.* + 3 > input.len) return null; const l: u32 = @intCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big)); pos.* = pos.* + 3 + l; if (pos.* > input.len) return null; return; },
        0xc6 => { if (pos.* + 5 > input.len) return null; const l = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big); pos.* = pos.* + 5 + l; if (pos.* > input.len) return null; return; },
        0xc7 => { if (pos.* + 2 > input.len) return null; const l: u32 = @intCast(input[pos.* + 1]); pos.* = pos.* + 3 + l; if (pos.* > input.len) return null; return; },
        0xc8 => { if (pos.* + 3 > input.len) return null; const l: u32 = @intCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big)); pos.* = pos.* + 4 + l; if (pos.* > input.len) return null; return; },
        0xc9 => { if (pos.* + 5 > input.len) return null; const l = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big); pos.* = pos.* + 6 + l; if (pos.* > input.len) return null; return; },
        0xca => { pos.* += 5; if (pos.* > input.len) return null; return; },
        0xcb => { pos.* += 9; if (pos.* > input.len) return null; return; },
        0xcc, 0xd0 => { pos.* += 2; if (pos.* > input.len) return null; return; },
        0xcd, 0xd1 => { pos.* += 3; if (pos.* > input.len) return null; return; },
        0xce, 0xd2 => { pos.* += 5; if (pos.* > input.len) return null; return; },
        0xcf, 0xd3 => { pos.* += 9; if (pos.* > input.len) return null; return; },
        0xd4 => { pos.* += 3; if (pos.* > input.len) return null; return; },
        0xd5 => { pos.* += 4; if (pos.* > input.len) return null; return; },
        0xd6 => { pos.* += 6; if (pos.* > input.len) return null; return; },
        0xd7 => { pos.* += 10; if (pos.* > input.len) return null; return; },
        0xd8 => { pos.* += 18; if (pos.* > input.len) return null; return; },
        0xd9 => { if (pos.* + 2 > input.len) return null; const l: u32 = @intCast(input[pos.* + 1]); pos.* = pos.* + 2 + l; if (pos.* > input.len) return null; return; },
        0xda => { if (pos.* + 3 > input.len) return null; const l: u32 = @intCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big)); pos.* = pos.* + 3 + l; if (pos.* > input.len) return null; return; },
        0xdb => { if (pos.* + 5 > input.len) return null; const l = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big); pos.* = pos.* + 5 + l; if (pos.* > input.len) return null; return; },
        0xdc => { if (pos.* + 3 > input.len) return null; const n: u32 = @intCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big)); pos.* += 3; var j: u32 = 0; while (j < n) : (j += 1) { skipValue(input, pos) orelse return null; } return; },
        0xdd => { if (pos.* + 5 > input.len) return null; const n = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big); pos.* += 5; var j: u32 = 0; while (j < n) : (j += 1) { skipValue(input, pos) orelse return null; } return; },
        0xde => { if (pos.* + 3 > input.len) return null; const n: u32 = @intCast(std.mem.readInt(u16, input[pos.* + 1 ..][0..2], .big)); pos.* += 3; var j: u32 = 0; while (j < n * 2) : (j += 1) { skipValue(input, pos) orelse return null; } return; },
        0xdf => { if (pos.* + 5 > input.len) return null; const n = std.mem.readInt(u32, input[pos.* + 1 ..][0..4], .big); pos.* += 5; var j: u32 = 0; while (j < n * 2) : (j += 1) { skipValue(input, pos) orelse return null; } return; },
        else => return null,
    }
}

// =============================================================================
// Tests
// =============================================================================

fn writeTestStr(buf: []u8, s: []const u8) usize {
    std.debug.assert(s.len < 32);
    buf[0] = 0xa0 | @as(u8, @intCast(s.len));
    @memcpy(buf[1..][0..s.len], s);
    return 1 + s.len;
}

fn writeTestInt64(buf: []u8, val: i64) usize {
    buf[0] = 0xd3;
    std.mem.writeInt(i64, buf[1..][0..8], val, .big);
    return 9;
}

test "extractMsgpackEvents - stream format with typed extraction" {
    // Set up a simple schema: id (Utf8), type (Utf8), timestamp (Int), value.qty (Int)
    const field_meta = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0, .reserved = [_]u8{ 0, 0 } },
        .{ .arrow_type = .Utf8, .nullable = 0, .reserved = [_]u8{ 0, 0 } },
        .{ .arrow_type = .Int, .nullable = 0, .reserved = [_]u8{ 0, 0 } },
        .{ .arrow_type = .Int, .nullable = 1, .reserved = [_]u8{ 0, 0 } },
    };
    const field_names = [_][]const u8{ "id", "type", "timestamp", "value.qty" };

    var config = try json_extractor.buildExtractionConfig(
        std.testing.allocator,
        &field_meta,
        &field_names,
    );
    defer json_extractor.freeExtractionConfig(std.testing.allocator, &config);

    var dyn_cols = try DynamicColumns.init(std.testing.allocator, 4, 10, 4096);
    defer dyn_cols.deinit();

    // Build msgpack event: {id: "ev-1", type: "order", timestamp: 1000, "value.qty": 5}
    // Note: field names in extraction config use dotted paths for value subfields,
    // but msgpack maps use the full path as the key name.
    // Actually — the extraction config field names correspond to how the event map keys
    // are named. For the base 4 fields they're "id", "type", "timestamp".
    // For value subfields, the EP flattens them to "value.qty" during extraction.
    // But for msgpack input, the map may have nested "value" object...
    // For this test, use the simple flat map case.
    var input: [128]u8 = undefined;
    var pos: usize = 0;
    input[pos] = 0x84; // fixmap(4)
    pos += 1;
    pos += writeTestStr(input[pos..], "id");
    pos += writeTestStr(input[pos..], "ev-1");
    pos += writeTestStr(input[pos..], "type");
    pos += writeTestStr(input[pos..], "order");
    pos += writeTestStr(input[pos..], "timestamp");
    pos += writeTestInt64(input[pos..], 1000);
    pos += writeTestStr(input[pos..], "value.qty");
    input[pos] = 0x05; // positive fixint 5
    pos += 1;

    var work_buf: [1024]u8 = undefined;
    const count = try extractMsgpackEvents(input[0..pos], &config, &dyn_cols, &work_buf, true);

    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(u32, 1), dyn_cols.count);

    // Check id column (col 0)
    const id_start = dyn_cols.columns[0].offsets.?[0];
    const id_end = dyn_cols.columns[0].offsets.?[1];
    try std.testing.expectEqualStrings("ev-1", dyn_cols.columns[0].data.?[id_start..id_end]);

    // Check type column (col 1)
    const type_start = dyn_cols.columns[1].offsets.?[0];
    const type_end = dyn_cols.columns[1].offsets.?[1];
    try std.testing.expectEqualStrings("order", dyn_cols.columns[1].data.?[type_start..type_end]);
}

test "extractMsgpackEvents - empty stream" {
    const field_meta = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0, .reserved = [_]u8{ 0, 0 } },
    };
    const field_names = [_][]const u8{"id"};

    var config = try json_extractor.buildExtractionConfig(
        std.testing.allocator,
        &field_meta,
        &field_names,
    );
    defer json_extractor.freeExtractionConfig(std.testing.allocator, &config);

    var dyn_cols = try DynamicColumns.init(std.testing.allocator, 1, 10, 4096);
    defer dyn_cols.deinit();

    var work_buf: [64]u8 = undefined;
    const count = try extractMsgpackEvents(&[_]u8{}, &config, &dyn_cols, &work_buf, true);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "extractMsgpackEvents - empty array" {
    const field_meta = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0, .reserved = [_]u8{ 0, 0 } },
    };
    const field_names = [_][]const u8{"id"};

    var config = try json_extractor.buildExtractionConfig(
        std.testing.allocator,
        &field_meta,
        &field_names,
    );
    defer json_extractor.freeExtractionConfig(std.testing.allocator, &config);

    var dyn_cols = try DynamicColumns.init(std.testing.allocator, 1, 10, 4096);
    defer dyn_cols.deinit();

    var work_buf: [64]u8 = undefined;
    // fixarray(0) = 0x90
    const count = try extractMsgpackEvents(&[_]u8{0x90}, &config, &dyn_cols, &work_buf, false);
    try std.testing.expectEqual(@as(u32, 0), count);
}
