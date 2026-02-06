//! JSON Parser abstraction for field extraction
//!
//! Provides a unified interface for parsing JSON objects and extracting
//! field values to typed columns. Uses std.json.Scanner internally.
//!
//! Design decision (ZIG-04): zimdjson (SIMD JSON parser) was evaluated but
//! has Zig 0.14 API incompatibility with Zig 0.15.2. The std.json.Scanner
//! approach is proven to work in Phase 7.2 and works on all targets including
//! WASM without platform-specific concerns.
//!
//! This module provides:
//! - Token-based streaming parser
//! - Object field iteration
//! - Type-safe value extraction
//! - Skip functionality for undeclared fields

const std = @import("std");
const builtin = @import("builtin");

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
    return "std.json.Scanner";
}

/// JSON parser for field extraction
///
/// Wraps std.json.Scanner to provide object field iteration.
/// The scanner returns slices into the original input for strings,
/// enabling zero-copy parsing.
pub const JsonParser = struct {
    scanner: std.json.Scanner,
    input: []const u8,

    const Self = @This();

    /// Initialize parser with input JSON bytes
    pub fn init(input: []const u8) Self {
        return .{
            .scanner = std.json.Scanner.initCompleteInput(std.heap.page_allocator, input),
            .input = input,
        };
    }

    /// Release parser resources
    pub fn deinit(self: *Self) void {
        self.scanner.deinit();
    }

    /// Get next token from the JSON stream
    pub fn nextToken(self: *Self) ParserError!Token {
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

    /// Peek at the next token without consuming it
    pub fn peekToken(self: *Self) ParserError!Token {
        const state = self.scanner;
        const token = self.nextToken() catch |e| return e;
        self.scanner = state;
        return token;
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
        const state = self.scanner;
        const token = self.nextToken() catch return true;
        self.scanner = state;
        return token == .object_end;
    }

    /// Check if at end of current array
    pub fn isArrayEnd(self: *Self) bool {
        const state = self.scanner;
        const token = self.nextToken() catch return true;
        self.scanner = state;
        return token == .array_end;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "backendName returns std.json.Scanner" {
    try std.testing.expectEqualStrings("std.json.Scanner", backendName());
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
