//! JSON Parser abstraction for field extraction
//!
//! Provides a unified Token-streaming interface consumed by json_extractor.zig.
//! Backend selection is compile-time:
//! - Native (x86_64, aarch64): simdjzon SIMD DOM parser
//! - WASM / other targets: std.json.Scanner
//!
//! The extractor code is completely backend-agnostic -- it only calls
//! nextToken(), expectFieldName(), skipValue(), etc.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

/// Whether to use simdjzon SIMD backend (set by build.zig per target)
const use_simdjzon = build_options.use_simdjzon;

/// Conditional import: only resolved when use_simdjzon is true at comptime
const simdjzon = if (use_simdjzon) @import("simdjzon") else undefined;

/// Parser error types
pub const ParserError = error{
    InvalidJson,
    UnexpectedToken,
    EndOfInput,
    InvalidNumber,
    OutOfMemory,
};

/// JSON token types for field extraction
pub const Token = union(enum) {
    object_begin,
    object_end,
    array_begin,
    array_end,
    string: []const u8,
    number: []const u8, // Raw number string for precise conversion
    true_,
    false_,
    null_,
};

/// Target architecture info for debugging
pub const target_info = struct {
    pub const arch = @tagName(builtin.cpu.arch);
    pub const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;
};

/// Report which JSON parser backend is used
pub fn backendName() []const u8 {
    return if (use_simdjzon) "simdjzon" else "std.json.Scanner";
}

// ============================================================================
// Scanner compatibility struct for simdjzon backend
// ============================================================================
// Provides parser.scanner.cursor for any extractor code that accesses
// cursor position directly (e.g. raw JSON capture for Binary columns).
const ScannerCompat = struct {
    cursor: usize,
};

/// JSON parser for field extraction
///
/// Wraps either simdjzon DOM parser or std.json.Scanner (selected at comptime)
/// to provide object field iteration. Both backends produce identical Token
/// values. The scanner returns slices into the original input for strings,
/// enabling zero-copy parsing.
pub const JsonParser = struct {
    // Backend-specific scanner state.
    // For Scanner: the actual std.json.Scanner.
    // For simdjzon: a compat struct with cursor field.
    scanner: if (use_simdjzon) ScannerCompat else std.json.Scanner,
    input: []const u8,

    // simdjzon-specific fields (void when not using simdjzon)
    simdjzon_parser: if (use_simdjzon) simdjzon.dom.Parser else void,
    tape_idx: if (use_simdjzon) usize else void,
    // Buffer for formatting numbers back to text (simdjzon stores typed values)
    num_buf: if (use_simdjzon) [64]u8 else void,
    num_slice: if (use_simdjzon) []const u8 else void,

    const Self = @This();

    /// Initialize parser with input JSON bytes
    pub fn init(input: []const u8) Self {
        if (use_simdjzon) {
            return initSimdjzon(input);
        } else {
            return .{
                .scanner = std.json.Scanner.initCompleteInput(std.heap.page_allocator, input),
                .input = input,
                .simdjzon_parser = {},
                .tape_idx = {},
                .num_buf = {},
                .num_slice = {},
            };
        }
    }

    fn initSimdjzon(input: []const u8) Self {
        var parser = simdjzon.dom.Parser.initFixedBuffer(std.heap.page_allocator, input, .{}) catch {
            // If initialization fails, return a parser in error state
            // nextToken() will return InvalidJson
            return .{
                .scanner = .{ .cursor = 0 },
                .input = input,
                .simdjzon_parser = undefined,
                .tape_idx = 0,
                .num_buf = undefined,
                .num_slice = &.{},
            };
        };
        parser.parse() catch {
            parser.deinit();
            return .{
                .scanner = .{ .cursor = 0 },
                .input = input,
                .simdjzon_parser = undefined,
                .tape_idx = 0,
                .num_buf = undefined,
                .num_slice = &.{},
            };
        };
        return .{
            .scanner = .{ .cursor = 0 },
            .input = input,
            .simdjzon_parser = parser,
            // Start at tape index 1 (skip ROOT entry)
            .tape_idx = 1,
            .num_buf = undefined,
            .num_slice = &.{},
        };
    }

    /// Release parser resources
    pub fn deinit(self: *Self) void {
        if (use_simdjzon) {
            self.simdjzon_parser.deinit();
        } else {
            self.scanner.deinit();
        }
    }

    /// Get next token from the JSON stream
    pub fn nextToken(self: *Self) ParserError!Token {
        if (use_simdjzon) {
            return self.nextTokenSimdjzon();
        } else {
            return self.nextTokenScanner();
        }
    }

    fn nextTokenScanner(self: *Self) ParserError!Token {
        const token = self.scanner.next() catch return error.InvalidJson;
        return switch (token) {
            .object_begin => .object_begin,
            .object_end => .object_end,
            .array_begin => .array_begin,
            .array_end => .array_end,
            .string => |s| Token{ .string = s },
            .number => |n| Token{ .number = n },
            .true => .true_,
            .false => .false_,
            .null => .null_,
            .end_of_document => error.EndOfInput,
            else => error.UnexpectedToken,
        };
    }

    fn nextTokenSimdjzon(self: *Self) ParserError!Token {
        const tape = self.simdjzon_parser.doc.tape.items;
        if (self.tape_idx >= tape.len) return error.EndOfInput;

        const entry = tape[self.tape_idx];
        const tape_type = simdjzon.dom.TapeType.from_u64(entry);

        // Advance cursor through input to match current token position.
        self.advanceCursorToNextToken(tape_type);

        switch (tape_type) {
            .START_OBJECT => {
                self.tape_idx += 1;
                return .object_begin;
            },
            .END_OBJECT => {
                self.tape_idx += 1;
                return .object_end;
            },
            .START_ARRAY => {
                self.tape_idx += 1;
                return .array_begin;
            },
            .END_ARRAY => {
                self.tape_idx += 1;
                return .array_end;
            },
            .STRING => {
                // Read string from simdjzon's string_buf via tape payload
                const str_buf_offset = simdjzon.dom.TapeType.extract_value(entry);
                const str_len = std.mem.readInt(u32, (self.simdjzon_parser.doc.string_buf.items.ptr + str_buf_offset)[0..@sizeOf(u32)], .little);
                const str_ptr: [*]const u8 = @ptrCast(self.simdjzon_parser.doc.string_buf.items.ptr + str_buf_offset + @sizeOf(u32));
                const s = str_ptr[0..str_len];
                self.tape_idx += 1;
                self.advanceCursorPastString();
                return Token{ .string = s };
            },
            .INT64 => {
                const val: i64 = @bitCast(tape[self.tape_idx + 1]);
                const slice = std.fmt.bufPrint(&self.num_buf, "{d}", .{val}) catch return error.InvalidNumber;
                self.num_slice = slice;
                self.tape_idx += 2;
                self.advanceCursorPastNumber();
                return Token{ .number = self.num_slice };
            },
            .UINT64 => {
                const val: u64 = tape[self.tape_idx + 1];
                const slice = std.fmt.bufPrint(&self.num_buf, "{d}", .{val}) catch return error.InvalidNumber;
                self.num_slice = slice;
                self.tape_idx += 2;
                self.advanceCursorPastNumber();
                return Token{ .number = self.num_slice };
            },
            .DOUBLE => {
                // Use raw number bytes from input for precision preservation
                const num_start = self.scanner.cursor;
                self.tape_idx += 2;
                self.advanceCursorPastNumber();
                const num_end = self.scanner.cursor;
                if (num_start < num_end and num_end <= self.input.len) {
                    const raw = std.mem.trimRight(u8, self.input[num_start..num_end], &[_]u8{ ' ', '\t', '\n', '\r', ',', '}', ']', ':' });
                    if (raw.len > 0) {
                        return Token{ .number = raw };
                    }
                }
                // Fallback: format the double value
                const val: f64 = @bitCast(tape[self.tape_idx - 1]);
                const slice = std.fmt.bufPrint(&self.num_buf, "{d}", .{val}) catch return error.InvalidNumber;
                self.num_slice = slice;
                return Token{ .number = self.num_slice };
            },
            .TRUE => {
                self.tape_idx += 1;
                self.advanceCursorPastKeyword(4);
                return .true_;
            },
            .FALSE => {
                self.tape_idx += 1;
                self.advanceCursorPastKeyword(5);
                return .false_;
            },
            .NULL => {
                self.tape_idx += 1;
                self.advanceCursorPastKeyword(4);
                return .null_;
            },
            .ROOT => {
                self.tape_idx += 1;
                return self.nextTokenSimdjzon();
            },
            else => return error.UnexpectedToken,
        }
    }

    // ========================================================================
    // Cursor tracking for simdjzon backend
    // ========================================================================

    fn advanceCursorToNextToken(self: *Self, tape_type: simdjzon.dom.TapeType) void {
        while (self.scanner.cursor < self.input.len) {
            const c = self.input[self.scanner.cursor];
            switch (c) {
                ' ', '\t', '\n', '\r', ',', ':' => self.scanner.cursor += 1,
                else => break,
            }
        }
        switch (tape_type) {
            .START_OBJECT, .END_OBJECT, .START_ARRAY, .END_ARRAY => {
                if (self.scanner.cursor < self.input.len) {
                    self.scanner.cursor += 1;
                }
            },
            else => {},
        }
    }

    fn advanceCursorPastString(self: *Self) void {
        if (self.scanner.cursor < self.input.len and self.input[self.scanner.cursor] == '"') {
            self.scanner.cursor += 1;
        }
        while (self.scanner.cursor < self.input.len) {
            const c = self.input[self.scanner.cursor];
            self.scanner.cursor += 1;
            if (c == '"') break;
            if (c == '\\' and self.scanner.cursor < self.input.len) {
                self.scanner.cursor += 1;
            }
        }
    }

    fn advanceCursorPastNumber(self: *Self) void {
        while (self.scanner.cursor < self.input.len) {
            const c = self.input[self.scanner.cursor];
            switch (c) {
                '0'...'9', '.', '-', '+', 'e', 'E' => self.scanner.cursor += 1,
                else => break,
            }
        }
    }

    fn advanceCursorPastKeyword(self: *Self, len: usize) void {
        self.scanner.cursor += @min(len, self.input.len - self.scanner.cursor);
    }

    /// Peek at the next token without consuming it
    pub fn peekToken(self: *Self) ParserError!Token {
        if (use_simdjzon) {
            const saved_tape_idx = self.tape_idx;
            const saved_cursor = self.scanner.cursor;
            const token = self.nextToken() catch |e| return e;
            self.tape_idx = saved_tape_idx;
            self.scanner.cursor = saved_cursor;
            return token;
        } else {
            const state = self.scanner;
            const token = self.nextToken() catch |e| return e;
            self.scanner = state;
            return token;
        }
    }

    /// Expect a specific token type, error if not matched
    pub fn expectObjectBegin(self: *Self) ParserError!void {
        const token = try self.nextToken();
        if (token != .object_begin) return error.UnexpectedToken;
    }

    /// Expect array begin
    pub fn expectArrayBegin(self: *Self) ParserError!void {
        const token = try self.nextToken();
        if (token != .array_begin) return error.UnexpectedToken;
    }

    /// Get field name (expects string token)
    pub fn expectFieldName(self: *Self) ParserError![]const u8 {
        const token = try self.nextToken();
        return switch (token) {
            .string => |s| s,
            .object_end => error.EndOfInput, // No more fields
            else => error.UnexpectedToken,
        };
    }

    /// Get string value
    pub fn expectString(self: *Self) ParserError![]const u8 {
        const token = try self.nextToken();
        return switch (token) {
            .string => |s| s,
            else => error.UnexpectedToken,
        };
    }

    /// Get number as i64
    pub fn expectInt64(self: *Self) ParserError!i64 {
        const token = try self.nextToken();
        return switch (token) {
            .number => |n| std.fmt.parseInt(i64, n, 10) catch return error.InvalidNumber,
            else => error.UnexpectedToken,
        };
    }

    /// Get number as f64
    pub fn expectFloat64(self: *Self) ParserError!f64 {
        const token = try self.nextToken();
        return switch (token) {
            .number => |n| std.fmt.parseFloat(f64, n) catch return error.InvalidNumber,
            else => error.UnexpectedToken,
        };
    }

    /// Get boolean value
    pub fn expectBool(self: *Self) ParserError!bool {
        const token = try self.nextToken();
        return switch (token) {
            .true_ => true,
            .false_ => false,
            else => error.UnexpectedToken,
        };
    }

    /// Skip current value (object, array, or primitive)
    /// Returns the raw bytes of the skipped value in the input
    pub fn skipValue(self: *Self) ParserError![]const u8 {
        const start_cursor = self.scanner.cursor;
        try self.skipValueInternal();
        const end_cursor = self.scanner.cursor;
        return self.input[start_cursor..end_cursor];
    }

    /// Skip a value without returning the bytes
    fn skipValueInternal(self: *Self) ParserError!void {
        const token = try self.nextToken();
        switch (token) {
            .object_begin => {
                var depth: u32 = 1;
                while (depth > 0) {
                    const t = try self.nextToken();
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
                    const t = try self.nextToken();
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

    /// Get raw value bytes from current position
    /// Captures the JSON substring for complex values
    pub fn captureRawValue(self: *Self) ParserError![]const u8 {
        return self.skipValue();
    }

    /// Check if at end of current object
    pub fn isObjectEnd(self: *Self) bool {
        if (use_simdjzon) {
            const saved_tape_idx = self.tape_idx;
            const saved_cursor = self.scanner.cursor;
            const token = self.nextToken() catch return true;
            self.tape_idx = saved_tape_idx;
            self.scanner.cursor = saved_cursor;
            return token == .object_end;
        } else {
            const state = self.scanner;
            const token = self.nextToken() catch return true;
            self.scanner = state;
            return token == .object_end;
        }
    }

    /// Check if at end of current array
    pub fn isArrayEnd(self: *Self) bool {
        if (use_simdjzon) {
            const saved_tape_idx = self.tape_idx;
            const saved_cursor = self.scanner.cursor;
            const token = self.nextToken() catch return true;
            self.tape_idx = saved_tape_idx;
            self.scanner.cursor = saved_cursor;
            return token == .array_end;
        } else {
            const state = self.scanner;
            const token = self.nextToken() catch return true;
            self.scanner = state;
            return token == .array_end;
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "backendName reflects target" {
    const name = backendName();
    if (use_simdjzon) {
        try std.testing.expectEqualStrings("simdjzon", name);
    } else {
        try std.testing.expectEqualStrings("std.json.Scanner", name);
    }
}

test "target_info reports architecture" {
    // Just verify it doesn't crash
    _ = target_info.arch;
    _ = target_info.is_wasm;
}

test "JsonParser basic object parsing" {
    const json =
        \\{"id":"test-123","count":42}
    ;

    var parser = JsonParser.init(json);
    defer parser.deinit();

    try parser.expectObjectBegin();

    // Field 1: id
    const field1 = try parser.expectFieldName();
    try std.testing.expectEqualStrings("id", field1);
    const val1 = try parser.expectString();
    try std.testing.expectEqualStrings("test-123", val1);

    // Field 2: count
    const field2 = try parser.expectFieldName();
    try std.testing.expectEqualStrings("count", field2);
    const val2 = try parser.expectInt64();
    try std.testing.expectEqual(@as(i64, 42), val2);

    // End of object
    const token = try parser.nextToken();
    try std.testing.expect(token == .object_end);
}

test "JsonParser skipValue for object" {
    const json =
        \\{"simple":"value","complex":{"nested":true,"array":[1,2,3]},"after":"ok"}
    ;

    var parser = JsonParser.init(json);
    defer parser.deinit();

    try parser.expectObjectBegin();

    // Field 1: simple
    _ = try parser.expectFieldName();
    _ = try parser.expectString();

    // Field 2: complex - skip it
    const field2 = try parser.expectFieldName();
    try std.testing.expectEqualStrings("complex", field2);
    _ = try parser.skipValue();

    // Field 3: after - should be accessible after skip
    const field3 = try parser.expectFieldName();
    try std.testing.expectEqualStrings("after", field3);
    const val3 = try parser.expectString();
    try std.testing.expectEqualStrings("ok", val3);
}

test "JsonParser array parsing" {
    const json =
        \\[{"id":"a"},{"id":"b"}]
    ;

    var parser = JsonParser.init(json);
    defer parser.deinit();

    try parser.expectArrayBegin();

    // Element 1
    try parser.expectObjectBegin();
    _ = try parser.expectFieldName();
    const val1 = try parser.expectString();
    try std.testing.expectEqualStrings("a", val1);
    const end1 = try parser.nextToken();
    try std.testing.expect(end1 == .object_end);

    // Element 2
    try parser.expectObjectBegin();
    _ = try parser.expectFieldName();
    const val2 = try parser.expectString();
    try std.testing.expectEqualStrings("b", val2);
    const end2 = try parser.nextToken();
    try std.testing.expect(end2 == .object_end);

    // End array
    const end_array = try parser.nextToken();
    try std.testing.expect(end_array == .array_end);
}

test "JsonParser float parsing" {
    const json =
        \\{"amount":99.99}
    ;

    var parser = JsonParser.init(json);
    defer parser.deinit();

    try parser.expectObjectBegin();
    _ = try parser.expectFieldName();
    const amount = try parser.expectFloat64();
    try std.testing.expectApproxEqAbs(@as(f64, 99.99), amount, 0.001);
}

test "JsonParser boolean and null" {
    const json =
        \\{"active":true,"deleted":false,"data":null}
    ;

    var parser = JsonParser.init(json);
    defer parser.deinit();

    try parser.expectObjectBegin();

    // active: true
    _ = try parser.expectFieldName();
    const active = try parser.expectBool();
    try std.testing.expect(active);

    // deleted: false
    _ = try parser.expectFieldName();
    const deleted = try parser.expectBool();
    try std.testing.expect(!deleted);

    // data: null
    _ = try parser.expectFieldName();
    const token = try parser.nextToken();
    try std.testing.expect(token == .null_);
}

test "JsonParser isObjectEnd" {
    const json =
        \\{"a":1}
    ;

    var parser = JsonParser.init(json);
    defer parser.deinit();

    try parser.expectObjectBegin();
    try std.testing.expect(!parser.isObjectEnd());

    _ = try parser.expectFieldName();
    _ = try parser.expectInt64();

    try std.testing.expect(parser.isObjectEnd());
}
