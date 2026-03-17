//! Columnar buffers for parsed events
//!
//! Events are parsed from JSON/msgpack into columnar format for Arrow IPC encoding.
//! Each column is a contiguous buffer that grows as events are parsed.
//!
//! Two column storage types:
//! - EventColumns: Fixed 4-column storage (id, type, timestamp, value) for base schema
//! - DynamicColumns: N-column storage for schemas with value.* field extraction
//!
//! Arrow compatibility:
//! - String columns use offset/length encoding (Arrow binary format)
//! - offsets[i] = start offset of string i in data buffer
//! - offsets[count] = total data length (Arrow requires n+1 offsets)
//! - Null bitmap uses Arrow's LSB-first bit packing

const std = @import("std");
// can provide their own dynamic_schema (they differ slightly).
const dynamic_schema = @import("dynamic_schema");

/// Maximum events per batch (prevents unbounded growth)
pub const MAX_EVENTS_PER_BATCH: u32 = 65536;

/// Maximum bytes for a single string column (id, type)
pub const MAX_STRING_BYTES: u32 = 1024 * 1024; // 1MB

/// Maximum bytes for value column (serialized JSON/msgpack)
pub const MAX_VALUE_BYTES: u32 = 16 * 1024 * 1024; // 16MB

/// Error codes for parsing operations
/// Values match TypeScript EventLogError codes for JS interop
pub const ParseError = enum(u32) {
    OK = 0,
    INVALID_JSON = 1,
    INVALID_MSGPACK = 2,
    MISSING_FIELD = 3,
    INVALID_FIELD_TYPE = 4,
    TOO_MANY_EVENTS = 5,
    BUFFER_OVERFLOW = 6,
    OUT_OF_MEMORY = 7,
};

/// Columnar buffers for event fields
///
/// String columns use offset/length encoding (Arrow binary format):
/// - offsets[i] = start offset of string i in data buffer
/// - offsets[count] = total data length
/// - data = concatenated string bytes
///
/// Memory layout is designed for zero-copy Arrow IPC encoding.
pub const EventColumns = struct {
    /// Number of events
    count: u32,

    // ID column (string - UUID format, typically 36 chars)
    id_offsets: []u32,
    id_data: []u8,
    id_data_len: u32,

    // Type column (string - event type identifier)
    type_offsets: []u32,
    type_data: []u8,
    type_data_len: u32,

    // Timestamp column (i64 microseconds since Unix epoch)
    timestamps: []i64,

    // Value column (binary - serialized JSON/msgpack, nullable)
    // Null bitmap indicates which events have values
    value_offsets: []u32,
    value_data: []u8,
    value_data_len: u32,
    value_nulls: []u8, // Bit-packed: bit i = 1 if event i has value (Arrow LSB-first)

    allocator: std.mem.Allocator,

    /// Initialize columnar buffers with given capacity
    ///
    /// Capacity is clamped to MAX_EVENTS_PER_BATCH.
    /// Initial buffer sizes are estimates based on typical event sizes.
    pub fn init(allocator: std.mem.Allocator, capacity: u32) !EventColumns {
        const cap = @min(capacity, MAX_EVENTS_PER_BATCH);

        // Initialize offset arrays with 0 for first element (Arrow requirement)
        var id_offsets = try allocator.alloc(u32, cap + 1);
        id_offsets[0] = 0;

        var type_offsets = try allocator.alloc(u32, cap + 1);
        type_offsets[0] = 0;

        var value_offsets = try allocator.alloc(u32, cap + 1);
        value_offsets[0] = 0;

        const value_nulls = try allocator.alloc(u8, (cap + 7) / 8);
        @memset(value_nulls, 0);

        return EventColumns{
            .count = 0,
            .id_offsets = id_offsets,
            .id_data = try allocator.alloc(u8, cap * 36), // UUID = 36 chars
            .id_data_len = 0,
            .type_offsets = type_offsets,
            .type_data = try allocator.alloc(u8, cap * 64), // avg type name estimate
            .type_data_len = 0,
            .timestamps = try allocator.alloc(i64, cap),
            .value_offsets = value_offsets,
            .value_data = try allocator.alloc(u8, cap * 256), // avg value size estimate
            .value_data_len = 0,
            .value_nulls = value_nulls,
            .allocator = allocator,
        };
    }

    /// Free all allocated memory
    pub fn deinit(self: *EventColumns) void {
        self.allocator.free(self.id_offsets);
        self.allocator.free(self.id_data);
        self.allocator.free(self.type_offsets);
        self.allocator.free(self.type_data);
        self.allocator.free(self.timestamps);
        self.allocator.free(self.value_offsets);
        self.allocator.free(self.value_data);
        self.allocator.free(self.value_nulls);
    }

    /// Reset buffers for reuse without reallocating
    pub fn reset(self: *EventColumns) void {
        self.count = 0;
        self.id_data_len = 0;
        self.type_data_len = 0;
        self.value_data_len = 0;
        @memset(self.value_nulls, 0);
    }

    /// Add an event to the columnar buffers
    ///
    /// Returns ParseError.OK on success, appropriate error code on failure.
    /// Strings are copied into the internal data buffers.
    pub fn addEvent(
        self: *EventColumns,
        id: []const u8,
        event_type: []const u8,
        timestamp_us: i64,
        value: ?[]const u8,
    ) ParseError {
        if (self.count >= self.timestamps.len) return .TOO_MANY_EVENTS;

        const idx = self.count;

        // ID column
        self.id_offsets[idx] = self.id_data_len;
        if (self.id_data_len + id.len > self.id_data.len) return .BUFFER_OVERFLOW;
        @memcpy(self.id_data[self.id_data_len..][0..id.len], id);
        self.id_data_len += @intCast(id.len);

        // Type column
        self.type_offsets[idx] = self.type_data_len;
        if (self.type_data_len + event_type.len > self.type_data.len) return .BUFFER_OVERFLOW;
        @memcpy(self.type_data[self.type_data_len..][0..event_type.len], event_type);
        self.type_data_len += @intCast(event_type.len);

        // Timestamp column
        self.timestamps[idx] = timestamp_us;

        // Value column (nullable)
        self.value_offsets[idx] = self.value_data_len;
        if (value) |v| {
            if (self.value_data_len + v.len > self.value_data.len) return .BUFFER_OVERFLOW;
            @memcpy(self.value_data[self.value_data_len..][0..v.len], v);
            self.value_data_len += @intCast(v.len);
            // Set validity bit (1 = valid, Arrow LSB-first)
            self.value_nulls[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);
        }
        // else: leave null bit as 0 (null)

        self.count += 1;

        // Set final offset for offset arrays (Arrow requires n+1 offsets)
        self.id_offsets[self.count] = self.id_data_len;
        self.type_offsets[self.count] = self.type_data_len;
        self.value_offsets[self.count] = self.value_data_len;

        return .OK;
    }

    /// Get ID string for event at index
    pub fn getId(self: *const EventColumns, idx: u32) ?[]const u8 {
        if (idx >= self.count) return null;
        const start = self.id_offsets[idx];
        const end = self.id_offsets[idx + 1];
        return self.id_data[start..end];
    }

    /// Get type string for event at index
    pub fn getType(self: *const EventColumns, idx: u32) ?[]const u8 {
        if (idx >= self.count) return null;
        const start = self.type_offsets[idx];
        const end = self.type_offsets[idx + 1];
        return self.type_data[start..end];
    }

    /// Get timestamp for event at index
    pub fn getTimestamp(self: *const EventColumns, idx: u32) ?i64 {
        if (idx >= self.count) return null;
        return self.timestamps[idx];
    }

    /// Check if event at index has a value (not null)
    pub fn hasValue(self: *const EventColumns, idx: u32) bool {
        if (idx >= self.count) return false;
        return (self.value_nulls[idx / 8] & (@as(u8, 1) << @intCast(idx % 8))) != 0;
    }

    /// Get value bytes for event at index (returns null if event has no value)
    pub fn getValue(self: *const EventColumns, idx: u32) ?[]const u8 {
        if (idx >= self.count) return null;
        if (!self.hasValue(idx)) return null;
        const start = self.value_offsets[idx];
        const end = self.value_offsets[idx + 1];
        return self.value_data[start..end];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "EventColumns - init and deinit" {
    var cols = try EventColumns.init(std.testing.allocator, 100);
    defer cols.deinit();

    try std.testing.expectEqual(@as(u32, 0), cols.count);
}

test "EventColumns - add single event" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = cols.addEvent(
        "550e8400-e29b-41d4-a716-446655440000",
        "orderPlaced",
        1705315800000000, // 2024-01-15T10:30:00Z in microseconds
        "{\"orderId\":\"123\"}",
    );

    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expectEqual(@as(u32, 1), cols.count);

    // Verify ID
    const id = cols.getId(0).?;
    try std.testing.expectEqualStrings("550e8400-e29b-41d4-a716-446655440000", id);

    // Verify type
    const event_type = cols.getType(0).?;
    try std.testing.expectEqualStrings("orderPlaced", event_type);

    // Verify timestamp
    const ts = cols.getTimestamp(0).?;
    try std.testing.expectEqual(@as(i64, 1705315800000000), ts);

    // Verify value
    try std.testing.expect(cols.hasValue(0));
    const value = cols.getValue(0).?;
    try std.testing.expectEqualStrings("{\"orderId\":\"123\"}", value);
}

test "EventColumns - null value" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    const result = cols.addEvent(
        "test-id",
        "testEvent",
        0,
        null,
    );

    try std.testing.expectEqual(ParseError.OK, result);
    try std.testing.expect(!cols.hasValue(0));
    try std.testing.expectEqual(@as(?[]const u8, null), cols.getValue(0));
}

test "EventColumns - multiple events" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    _ = cols.addEvent("id-1", "type-a", 100, "value1");
    _ = cols.addEvent("id-2", "type-b", 200, null);
    _ = cols.addEvent("id-3", "type-a", 300, "value3");

    try std.testing.expectEqual(@as(u32, 3), cols.count);

    try std.testing.expectEqualStrings("id-2", cols.getId(1).?);
    try std.testing.expectEqualStrings("type-b", cols.getType(1).?);
    try std.testing.expectEqual(@as(i64, 200), cols.getTimestamp(1).?);
    try std.testing.expect(!cols.hasValue(1));

    try std.testing.expect(cols.hasValue(0));
    try std.testing.expect(cols.hasValue(2));
}

test "EventColumns - reset for reuse" {
    var cols = try EventColumns.init(std.testing.allocator, 10);
    defer cols.deinit();

    _ = cols.addEvent("id-1", "type-a", 100, "value1");
    try std.testing.expectEqual(@as(u32, 1), cols.count);

    cols.reset();
    try std.testing.expectEqual(@as(u32, 0), cols.count);

    _ = cols.addEvent("id-2", "type-b", 200, null);
    try std.testing.expectEqual(@as(u32, 1), cols.count);
    try std.testing.expectEqualStrings("id-2", cols.getId(0).?);
}

test "EventColumns - too many events" {
    var cols = try EventColumns.init(std.testing.allocator, 2);
    defer cols.deinit();

    _ = cols.addEvent("id-1", "t", 0, null);
    _ = cols.addEvent("id-2", "t", 0, null);
    const result = cols.addEvent("id-3", "t", 0, null);

    try std.testing.expectEqual(ParseError.TOO_MANY_EVENTS, result);
}

// =============================================================================
// DynamicColumns - N-column storage for field extraction
// =============================================================================

/// Column type determines storage layout
pub const ColumnType = enum {
    utf8, // Variable-length string: offsets + data
    int64, // Fixed 8 bytes per value
    float64, // Fixed 8 bytes per value
    bool_, // Bit-packed (1 bit per value)
    binary, // Variable-length binary: offsets + data
};

/// Storage for a single column of typed data
pub const ColumnStorage = struct {
    col_type: ColumnType,
    allocator: std.mem.Allocator,

    // Validity bitmap (Arrow LSB-first) - all columns are nullable
    validity: []u8,

    // Type-specific data
    // For utf8/binary: offsets and data
    offsets: ?[]u32,
    data: ?[]u8,
    data_len: u32,

    // For int64/float64: fixed-width array
    fixed_i64: ?[]i64,
    fixed_f64: ?[]f64,

    // For bool: bit-packed data
    bool_data: ?[]u8,

    /// Initialize column storage for given type and capacity
    pub fn init(allocator: std.mem.Allocator, col_type: ColumnType, capacity: u32) !ColumnStorage {
        const cap = @min(capacity, MAX_EVENTS_PER_BATCH);

        // Validity bitmap always present
        const validity = try allocator.alloc(u8, (cap + 7) / 8);
        @memset(validity, 0);

        var self = ColumnStorage{
            .col_type = col_type,
            .allocator = allocator,
            .validity = validity,
            .offsets = null,
            .data = null,
            .data_len = 0,
            .fixed_i64 = null,
            .fixed_f64 = null,
            .bool_data = null,
        };

        switch (col_type) {
            .utf8, .binary => {
                var offs = try allocator.alloc(u32, cap + 1);
                offs[0] = 0;
                self.offsets = offs;
                self.data = try allocator.alloc(u8, cap * 128); // Estimate 128 bytes avg
            },
            .int64 => {
                self.fixed_i64 = try allocator.alloc(i64, cap);
            },
            .float64 => {
                self.fixed_f64 = try allocator.alloc(f64, cap);
            },
            .bool_ => {
                self.bool_data = try allocator.alloc(u8, (cap + 7) / 8);
                @memset(self.bool_data.?, 0);
            },
        }

        return self;
    }

    /// Free all allocated memory
    pub fn deinit(self: *ColumnStorage) void {
        self.allocator.free(self.validity);
        if (self.offsets) |o| self.allocator.free(o);
        if (self.data) |d| self.allocator.free(d);
        if (self.fixed_i64) |f| self.allocator.free(f);
        if (self.fixed_f64) |f| self.allocator.free(f);
        if (self.bool_data) |b| self.allocator.free(b);
    }

    /// Reset column for reuse
    pub fn reset(self: *ColumnStorage) void {
        @memset(self.validity, 0);
        self.data_len = 0;
        if (self.bool_data) |b| @memset(b, 0);
    }
};

/// Dynamic columnar buffers for N-column extraction
///
/// Unlike EventColumns (fixed 4 columns), DynamicColumns supports
/// arbitrary column count based on schema. Used when schema has
/// value.* fields that need extraction.
///
/// All value.* columns are nullable because events may not contain
/// all declared fields (sparse data).
pub const DynamicColumns = struct {
    allocator: std.mem.Allocator,
    count: u32, // Number of rows

    /// Per-column storage
    columns: []ColumnStorage,

    /// Field count (matches schema)
    field_count: u32,

    /// Column capacity (max rows)
    capacity: u32,

    const Self = @This();

    /// Initialize with field metadata from schema
    pub fn init(
        allocator: std.mem.Allocator,
        field_metadata: []const dynamic_schema.SignalSchemaField,
        capacity: u32,
    ) !Self {
        const cap = @min(capacity, MAX_EVENTS_PER_BATCH);
        const num_fields: u32 = @intCast(field_metadata.len);

        var columns_storage = try allocator.alloc(ColumnStorage, num_fields);
        errdefer allocator.free(columns_storage);

        // Initialize each column based on its Arrow type
        var initialized: u32 = 0;
        errdefer {
            for (columns_storage[0..initialized]) |*col| {
                col.deinit();
            }
        }

        for (field_metadata, 0..) |field, i| {
            const col_type: ColumnType = switch (field.arrow_type) {
                .Utf8 => .utf8,
                .Binary => .binary,
                .Int, .Int64 => .int64,
                .FloatingPoint => .float64,
                .Bool => .bool_,
                .Null => .utf8, // Treat null type as empty strings
            };
            columns_storage[i] = try ColumnStorage.init(allocator, col_type, cap);
            initialized += 1;
        }

        return Self{
            .allocator = allocator,
            .count = 0,
            .columns = columns_storage,
            .field_count = num_fields,
            .capacity = cap,
        };
    }

    /// Free all allocated memory
    pub fn deinit(self: *Self) void {
        for (self.columns) |*col| {
            col.deinit();
        }
        self.allocator.free(self.columns);
    }

    /// Reset all columns for reuse
    pub fn reset(self: *Self) void {
        self.count = 0;
        for (self.columns) |*col| {
            col.reset();
        }
    }

    /// Begin a new row - call before appending values
    /// Returns false if at capacity
    pub fn beginRow(self: *Self) bool {
        return self.count < self.capacity;
    }

    /// Complete the current row
    pub fn endRow(self: *Self) void {
        const row_idx = self.count;
        self.count += 1;

        // Update offsets for variable-length columns
        for (self.columns) |*col| {
            if (col.offsets) |offs| {
                offs[self.count] = col.data_len;
            }
        }

        // For any column without explicit value, null is already set (validity bit 0)
        _ = row_idx;
    }

    /// Append a UTF-8 string value to a column
    pub fn appendUtf8(self: *Self, col_idx: u32, value: []const u8) ParseError {
        if (col_idx >= self.field_count) return .INVALID_FIELD_TYPE;

        var col = &self.columns[col_idx];
        if (col.col_type != .utf8 and col.col_type != .binary) return .INVALID_FIELD_TYPE;

        const row_idx = self.count;
        const offs = col.offsets.?;
        const data = col.data.?;

        // Store offset
        offs[row_idx] = col.data_len;

        // Copy data
        if (col.data_len + value.len > data.len) return .BUFFER_OVERFLOW;
        @memcpy(data[col.data_len..][0..value.len], value);
        col.data_len += @intCast(value.len);

        // Set validity bit
        col.validity[row_idx / 8] |= @as(u8, 1) << @intCast(row_idx % 8);

        return .OK;
    }

    /// Append an Int64 value to a column
    pub fn appendInt64(self: *Self, col_idx: u32, value: i64) ParseError {
        if (col_idx >= self.field_count) return .INVALID_FIELD_TYPE;

        var col = &self.columns[col_idx];
        if (col.col_type != .int64) return .INVALID_FIELD_TYPE;

        const row_idx = self.count;
        col.fixed_i64.?[row_idx] = value;

        // Set validity bit
        col.validity[row_idx / 8] |= @as(u8, 1) << @intCast(row_idx % 8);

        return .OK;
    }

    /// Append a Float64 value to a column
    pub fn appendFloat64(self: *Self, col_idx: u32, value: f64) ParseError {
        if (col_idx >= self.field_count) return .INVALID_FIELD_TYPE;

        var col = &self.columns[col_idx];
        if (col.col_type != .float64) return .INVALID_FIELD_TYPE;

        const row_idx = self.count;
        col.fixed_f64.?[row_idx] = value;

        // Set validity bit
        col.validity[row_idx / 8] |= @as(u8, 1) << @intCast(row_idx % 8);

        return .OK;
    }

    /// Append a boolean value to a column
    pub fn appendBool(self: *Self, col_idx: u32, value: bool) ParseError {
        if (col_idx >= self.field_count) return .INVALID_FIELD_TYPE;

        var col = &self.columns[col_idx];
        if (col.col_type != .bool_) return .INVALID_FIELD_TYPE;

        const row_idx = self.count;

        // Set value bit (Arrow LSB-first)
        if (value) {
            col.bool_data.?[row_idx / 8] |= @as(u8, 1) << @intCast(row_idx % 8);
        }

        // Set validity bit
        col.validity[row_idx / 8] |= @as(u8, 1) << @intCast(row_idx % 8);

        return .OK;
    }

    /// Append binary data to a column
    pub fn appendBinary(self: *Self, col_idx: u32, value: []const u8) ParseError {
        // Binary uses same storage as UTF-8
        return self.appendUtf8(col_idx, value);
    }

    /// Append null to a column (no-op since null is default)
    pub fn appendNull(self: *Self, col_idx: u32) ParseError {
        if (col_idx >= self.field_count) return .INVALID_FIELD_TYPE;
        // Validity bit is already 0 (null) from init/reset
        return .OK;
    }

    /// Check if a value is null at given row/column
    pub fn isNull(self: *const Self, col_idx: u32, row_idx: u32) bool {
        if (col_idx >= self.field_count or row_idx >= self.count) return true;
        const col = &self.columns[col_idx];
        return (col.validity[row_idx / 8] & (@as(u8, 1) << @intCast(row_idx % 8))) == 0;
    }

    /// Get column storage for direct access (for Arrow encoding)
    pub fn getColumn(self: *const Self, col_idx: u32) ?*const ColumnStorage {
        if (col_idx >= self.field_count) return null;
        return &self.columns[col_idx];
    }
};

// =============================================================================
// DynamicColumns Tests
// =============================================================================

test "DynamicColumns - init and deinit" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value
    };

    var cols = try DynamicColumns.init(std.testing.allocator, &fields, 100);
    defer cols.deinit();

    try std.testing.expectEqual(@as(u32, 0), cols.count);
    try std.testing.expectEqual(@as(u32, 4), cols.field_count);
}

test "DynamicColumns - append values" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Int, .nullable = 0 }, // count
        .{ .arrow_type = .FloatingPoint, .nullable = 1 }, // amount
        .{ .arrow_type = .Bool, .nullable = 1 }, // active
    };

    var cols = try DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    // Row 1
    try std.testing.expect(cols.beginRow());
    try std.testing.expectEqual(ParseError.OK, cols.appendUtf8(0, "id-001"));
    try std.testing.expectEqual(ParseError.OK, cols.appendInt64(1, 42));
    try std.testing.expectEqual(ParseError.OK, cols.appendFloat64(2, 99.99));
    try std.testing.expectEqual(ParseError.OK, cols.appendBool(3, true));
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 1), cols.count);

    // Verify values are not null
    try std.testing.expect(!cols.isNull(0, 0));
    try std.testing.expect(!cols.isNull(1, 0));
    try std.testing.expect(!cols.isNull(2, 0));
    try std.testing.expect(!cols.isNull(3, 0));
}

test "DynamicColumns - null values" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .FloatingPoint, .nullable = 1 }, // optional_amount
    };

    var cols = try DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    // Row 1 - no optional_amount
    try std.testing.expect(cols.beginRow());
    try std.testing.expectEqual(ParseError.OK, cols.appendUtf8(0, "id-001"));
    try std.testing.expectEqual(ParseError.OK, cols.appendNull(1));
    cols.endRow();

    // Verify null detection
    try std.testing.expect(!cols.isNull(0, 0));
    try std.testing.expect(cols.isNull(1, 0));
}

test "DynamicColumns - multiple rows" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // name
        .{ .arrow_type = .Int, .nullable = 0 }, // count
    };

    var cols = try DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    // Add 3 rows
    try std.testing.expect(cols.beginRow());
    _ = cols.appendUtf8(0, "alice");
    _ = cols.appendInt64(1, 10);
    cols.endRow();

    try std.testing.expect(cols.beginRow());
    _ = cols.appendUtf8(0, "bob");
    _ = cols.appendInt64(1, 20);
    cols.endRow();

    try std.testing.expect(cols.beginRow());
    _ = cols.appendUtf8(0, "charlie");
    _ = cols.appendInt64(1, 30);
    cols.endRow();

    try std.testing.expectEqual(@as(u32, 3), cols.count);
}

test "DynamicColumns - reset for reuse" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 },
    };

    var cols = try DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    // Add a row
    try std.testing.expect(cols.beginRow());
    _ = cols.appendUtf8(0, "test");
    cols.endRow();
    try std.testing.expectEqual(@as(u32, 1), cols.count);

    // Reset
    cols.reset();
    try std.testing.expectEqual(@as(u32, 0), cols.count);

    // Add another row
    try std.testing.expect(cols.beginRow());
    _ = cols.appendUtf8(0, "new");
    cols.endRow();
    try std.testing.expectEqual(@as(u32, 1), cols.count);
}

test "DynamicColumns - invalid column type error" {
    const fields = [_]dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Int, .nullable = 0 }, // int column
    };

    var cols = try DynamicColumns.init(std.testing.allocator, &fields, 10);
    defer cols.deinit();

    try std.testing.expect(cols.beginRow());

    // Try to append string to int column
    const result = cols.appendUtf8(0, "not an int");
    try std.testing.expectEqual(ParseError.INVALID_FIELD_TYPE, result);
}
