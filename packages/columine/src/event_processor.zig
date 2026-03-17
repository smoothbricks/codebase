//! Columine EventProcessor - JSON to Arrow IPC
//!
//! Entry point for the Parse + Compact pipeline.
//! Provides createLogEntry for JSON-to-Arrow-IPC conversion.
//!
//! This is the generic columine version WITHOUT dedup (bloom filter, checkpoint).
//!
const std = @import("std");
const builtin = @import("builtin");

// =============================================================================
// Module imports
// =============================================================================

// Parsing modules - JSON and msgpack to columnar format.
// Named modules (wired in build.zig) so each file belongs to exactly
pub const parsing = struct {
    pub const columns = @import("columns");
    pub const json_scanner = @import("json_scanner");
    pub const json_parser = @import("json_parser");
    pub const json_extractor = @import("json_extractor");
    pub const msgpack_scanner = @import("msgpack_scanner");
    pub const msgpack_extractor = @import("msgpack_extractor");
};

// Arrow module - IPC encoding for output
pub const arrow = struct {
    pub const ipc_writer = @import("ipc_writer");
    pub const dynamic_schema = @import("dynamic_schema");
    pub const dynamic_record_batch = @import("dynamic_record_batch");
};

// =============================================================================
// Re-export commonly used types
// =============================================================================

// Parsing types
pub const EventColumns = parsing.columns.EventColumns;
pub const DynamicColumns = parsing.columns.DynamicColumns;
pub const ColumnStorage = parsing.columns.ColumnStorage;
pub const ColumnType = parsing.columns.ColumnType;
pub const ParseError = parsing.columns.ParseError;
pub const parseJsonEvents = parsing.json_scanner.parseJsonEvents;

// JSON extraction types
pub const JsonParser = parsing.json_parser.JsonParser;
pub const extractJsonEvent = parsing.json_extractor.extractJsonEvent;
pub const ExtractionConfig = parsing.json_extractor.ExtractionConfig;
pub const ExtractionError = parsing.json_extractor.ExtractionError;

// Arrow types
pub const IpcWriter = arrow.ipc_writer.IpcWriter;
pub const BodyBuilder = arrow.ipc_writer.BodyBuilder;
pub const BufferDesc = arrow.dynamic_record_batch.BufferDesc;
pub const FieldNode = arrow.dynamic_record_batch.FieldNode;

// Dynamic schema types
pub const DynamicSchemaConfig = arrow.dynamic_schema.DynamicSchemaConfig;
pub const SignalSchemaField = arrow.dynamic_schema.SignalSchemaField;
pub const ArrowType = arrow.dynamic_schema.ArrowType;

// =============================================================================
// Constants
// =============================================================================

pub const VERSION: u32 = 1;

/// Input format enum for createLogEntry
pub const InputFormat = enum(u8) {
    JSON = 0,
    MSGPACK = 1, // standard msgpack: array of map objects
    ARROW_PASSTHROUGH = 2,
    MSGPACK_STREAM = 3, // concatenated msgpack maps (no array wrapper)
};

/// Result codes for WASM exports
pub const ResultCode = enum(u32) {
    OK = 0,
    INVALID_HANDLE = 1,
    PARSE_ERROR = 2,
    ENCODE_ERROR = 3,
    OUT_OF_MEMORY = 4,
    INVALID_FORMAT = 5,
    TOO_MANY_EVENTS = 6,
    BUFFER_OVERFLOW = 7,
};

/// Result header written to output buffer (32 bytes)
/// Layout:
///   0-3:   code (u32)
///   4-7:   arrow_ipc_offset (u32)
///   8-11:  arrow_ipc_len (u32)
///  12-15:  events_processed (u32)
///  16-19:  duplicates_filtered (u32)
///  20-31:  reserved (12 bytes)
pub const ResultHeader = extern struct {
    code: u32,
    arrow_ipc_offset: u32,
    arrow_ipc_len: u32,
    events_processed: u32,
    duplicates_filtered: u32,
    reserved: [12]u8 = [_]u8{0} ** 12,
};

comptime {
    std.debug.assert(@sizeOf(ResultHeader) == 32);
}

// =============================================================================
// EventProcessor instance (Parse + Compact only, no dedup)
// =============================================================================

const EventProcessor = struct {
    event_columns: EventColumns,
    allocator: std.mem.Allocator,
    /// Schema configuration (required for Arrow encoding)
    schema_config: DynamicSchemaConfig,
    /// Dynamic columns for extraction path (null if using base path)
    dynamic_columns: ?DynamicColumns,
    /// Extraction configuration (null if using base path)
    extraction_config: ?parsing.json_extractor.ExtractionConfig,
    /// Work buffer for msgpack serialization (empty if using base path)
    work_buffer: []u8,
    /// Estimated total bytes allocated by this processor.
    working_set_estimate: usize,

    const NATIVE_DEFAULT_WORK_BUFFER_BYTES: usize = 4 * 1024;
    const WASM_WORKING_SET_CAP_BYTES: usize = 64 * 1024 * 1024;
    const WASM_MAX_WORK_BUFFER_BYTES: usize = 16 * 1024 * 1024;

    /// Initialize EventProcessor with schema from TypeScript.
    /// Schema is required for Arrow encoding.
    fn init(
        alloc: std.mem.Allocator,
        capacity: u32,
        schema_bytes: []const u8,
        field_meta_ptr: [*]const SignalSchemaField,
        field_count: u32,
    ) !*EventProcessor {
        const self = try alloc.create(EventProcessor);
        errdefer alloc.destroy(self);

        const event_cap = @min(capacity, parsing.columns.MAX_EVENTS_PER_BATCH);

        // Initialize schema config
        const schema_config = try DynamicSchemaConfig.init(
            alloc,
            schema_bytes,
            field_meta_ptr,
            field_count,
        );
        errdefer {
            var config = schema_config;
            config.deinit();
        }

        const estimated = estimateWorkingSetBytes(event_cap, schema_config.field_metadata, false, 0) catch {
            return error.OutOfMemory;
        };
        if (builtin.cpu.arch == .wasm32 and estimated > WASM_WORKING_SET_CAP_BYTES) {
            return error.OutOfMemory;
        }

        var cols = try EventColumns.init(alloc, event_cap);
        errdefer cols.deinit();

        self.* = EventProcessor{
            .event_columns = cols,
            .allocator = alloc,
            .schema_config = schema_config,
            .dynamic_columns = null,
            .extraction_config = null,
            .work_buffer = &.{},
            .working_set_estimate = estimated,
        };

        return self;
    }

    /// Initialize EventProcessor with field names for extraction path.
    fn initWithFieldNames(
        alloc: std.mem.Allocator,
        capacity: u32,
        schema_bytes: []const u8,
        field_meta_ptr: [*]const SignalSchemaField,
        field_count: u32,
        field_names_raw: []const u8,
    ) !*EventProcessor {
        const self = try alloc.create(EventProcessor);
        errdefer alloc.destroy(self);

        const event_cap = @min(capacity, parsing.columns.MAX_EVENTS_PER_BATCH);

        // Initialize schema config with field names
        const schema_config = try DynamicSchemaConfig.initWithFieldNames(
            alloc,
            schema_bytes,
            field_meta_ptr,
            field_count,
            field_names_raw,
        );
        errdefer {
            var config = schema_config;
            config.deinit();
        }

        // Initialize extraction structures if schema has extraction fields
        var dyn_cols: ?DynamicColumns = null;
        var extract_config: ?parsing.json_extractor.ExtractionConfig = null;
        var work_buf: []u8 = &.{};
        var work_buf_size: usize = 0;

        if (schema_config.has_extraction_fields) {
            work_buf_size = if (builtin.cpu.arch == .wasm32) 64 * 1024 else NATIVE_DEFAULT_WORK_BUFFER_BYTES;
        }

        const estimated = estimateWorkingSetBytes(
            event_cap,
            schema_config.field_metadata,
            schema_config.has_extraction_fields,
            work_buf_size,
        ) catch {
            return error.OutOfMemory;
        };
        if (builtin.cpu.arch == .wasm32 and estimated > WASM_WORKING_SET_CAP_BYTES) {
            return error.OutOfMemory;
        }

        var cols = try EventColumns.init(alloc, event_cap);
        errdefer cols.deinit();

        if (schema_config.has_extraction_fields) {
            dyn_cols = try DynamicColumns.init(alloc, schema_config.field_metadata, event_cap);
            errdefer if (dyn_cols) |*dc| dc.deinit();

            extract_config = try parsing.json_extractor.buildExtractionConfig(
                alloc,
                schema_config.field_metadata,
                schema_config.field_names,
            );
            errdefer if (extract_config) |*ec| parsing.json_extractor.freeExtractionConfig(alloc, ec);

            work_buf = try alloc.alloc(u8, work_buf_size);
            errdefer alloc.free(work_buf);
        }

        self.* = EventProcessor{
            .event_columns = cols,
            .allocator = alloc,
            .schema_config = schema_config,
            .dynamic_columns = dyn_cols,
            .extraction_config = extract_config,
            .work_buffer = work_buf,
            .working_set_estimate = estimated,
        };

        return self;
    }

    fn deinit(self: *EventProcessor) void {
        self.event_columns.deinit();
        var config = self.schema_config;
        config.deinit();
        if (self.dynamic_columns) |*dc| dc.deinit();
        if (self.extraction_config) |*ec| parsing.json_extractor.freeExtractionConfig(self.allocator, ec);
        if (self.work_buffer.len > 0) self.allocator.free(self.work_buffer);
        self.allocator.destroy(self);
    }

    fn estimateWorkingSetBytes(
        capacity: u32,
        field_metadata: []const SignalSchemaField,
        include_dynamic: bool,
        work_buffer_len: usize,
    ) !usize {
        var total: usize = @sizeOf(EventProcessor);
        total = try addChecked(total, estimateEventColumnsBytes(capacity));
        if (include_dynamic) {
            total = try addChecked(total, estimateDynamicColumnsBytes(capacity, field_metadata));
            total = try addChecked(total, work_buffer_len);
        }
        return total;
    }

    fn estimateEventColumnsBytes(capacity: u32) usize {
        const cap: usize = @intCast(capacity);
        const offsets = (cap + 1) * @sizeOf(u32);
        return (offsets * 3) +
            (((cap + 7) / 8)) +
            (cap * 36) +
            (cap * 64) +
            (cap * @sizeOf(i64)) +
            (cap * 256);
    }

    fn estimateDynamicColumnsBytes(capacity: u32, field_metadata: []const SignalSchemaField) usize {
        const cap: usize = @intCast(capacity);
        var total: usize = @sizeOf(ColumnStorage) * field_metadata.len;

        for (field_metadata) |field| {
            total += (cap + 7) / 8; // validity bitmap
            switch (field.arrow_type) {
                .Utf8, .Binary => {
                    total += (cap + 1) * @sizeOf(u32);
                    total += cap * 128;
                },
                .Int, .Int64, .FloatingPoint => {
                    total += cap * 8;
                },
                .Bool => {
                    total += (cap + 7) / 8;
                },
                .Null => {
                    total += (cap + 1) * @sizeOf(u32);
                    total += cap * 128;
                },
            }
        }

        return total;
    }

    fn addChecked(a: usize, b: usize) !usize {
        const sum = @addWithOverflow(a, b);
        if (sum[1] != 0) return error.OutOfMemory;
        return sum[0];
    }

    fn ensureWorkBufferCapacity(self: *EventProcessor, required_len: usize) !void {
        if (self.work_buffer.len >= required_len) return;

        var target = if (self.work_buffer.len == 0) NATIVE_DEFAULT_WORK_BUFFER_BYTES else self.work_buffer.len;
        while (target < required_len) {
            const next = target * 2;
            if (next < target) return error.OutOfMemory;
            target = next;
        }

        if (builtin.cpu.arch == .wasm32 and target > WASM_MAX_WORK_BUFFER_BYTES) {
            return error.OutOfMemory;
        }

        const delta = target - self.work_buffer.len;
        if (builtin.cpu.arch == .wasm32 and self.working_set_estimate + delta > WASM_WORKING_SET_CAP_BYTES) {
            return error.OutOfMemory;
        }

        const next_buf = try self.allocator.alloc(u8, target);
        errdefer self.allocator.free(next_buf);
        if (self.work_buffer.len > 0) {
            @memcpy(next_buf[0..self.work_buffer.len], self.work_buffer);
            self.allocator.free(self.work_buffer);
        }
        self.work_buffer = next_buf;
        self.working_set_estimate += delta;
    }
};

// =============================================================================
// Handle table for WASM (simple array, limited handles)
// =============================================================================

var g_handles: [256]?*EventProcessor = [_]?*EventProcessor{null} ** 256;
var g_next_handle: u32 = 1;

fn allocHandle(ep: *EventProcessor) ?u32 {
    var i: u32 = 0;
    while (i < 256) : (i += 1) {
        const idx = (g_next_handle + i) % 256;
        if (g_handles[idx] == null) {
            g_handles[idx] = ep;
            g_next_handle = (idx + 1) % 256;
            return idx;
        }
    }
    return null;
}

fn getProcessor(handle: u32) ?*EventProcessor {
    if (handle >= 256) return null;
    return g_handles[handle];
}

fn freeHandle(handle: u32) void {
    if (handle < 256) {
        g_handles[handle] = null;
    }
}

// =============================================================================
// Allocator selection (WASM vs native)
// =============================================================================

// Use wasm_allocator on wasm targets so linear memory can grow with workload.
const allocator = if (builtin.cpu.arch == .wasm32)
    std.heap.wasm_allocator
else
    std.heap.page_allocator;

// =============================================================================
// WASM/FFI Exports
// =============================================================================

/// Get version for compatibility checks
pub export fn ep_version() u32 {
    return VERSION;
}

/// Create EventProcessor with schema (Parse + Compact only, no dedup).
///
/// Schema is required for Arrow encoding. TypeScript defines the schema
/// using Flechette, and Zig stores and uses those bytes for IPC output.
///
/// Parameters:
/// - capacity: expected number of events per batch
/// - schema_ptr: pointer to pre-computed Arrow schema bytes (from Flechette)
/// - schema_len: length of schema bytes
/// - field_meta_ptr: pointer to SignalSchemaField array (4 bytes per field)
/// - field_count: number of fields in schema
///
/// Returns: handle (0 = error)
pub export fn ep_create_with_schema(
    capacity: u32,
    schema_ptr: [*]const u8,
    schema_len: u32,
    field_meta_ptr: [*]const SignalSchemaField,
    field_count: u32,
) u32 {
    const schema_bytes = schema_ptr[0..schema_len];

    const ep = EventProcessor.init(
        allocator,
        capacity,
        schema_bytes,
        field_meta_ptr,
        field_count,
    ) catch return 0;

    return allocHandle(ep) orelse {
        ep.deinit();
        return 0;
    };
}

/// Create EventProcessor with schema AND field names for extraction.
///
/// This version accepts field names which enables the extraction path for
/// schemas with value.* fields. Without field names, Zig cannot match JSON
/// keys to columns.
///
/// Parameters:
/// - capacity: expected number of events per batch
/// - schema_ptr: pointer to pre-computed Arrow schema bytes (from Flechette)
/// - schema_len: length of schema bytes
/// - field_meta_ptr: pointer to SignalSchemaField array (4 bytes per field)
/// - field_count: number of fields in schema
/// - field_names_ptr: pointer to null-terminated field names (e.g., "id\0type\0timestamp\0value.orderId\0")
/// - field_names_len: total length of field_names_ptr including nulls
///
/// Returns: handle (0 = error)
pub export fn ep_create_with_schema_and_names(
    capacity: u32,
    schema_ptr: [*]const u8,
    schema_len: u32,
    field_meta_ptr: [*]const SignalSchemaField,
    field_count: u32,
    field_names_ptr: [*]const u8,
    field_names_len: u32,
) u32 {
    const schema_bytes = schema_ptr[0..schema_len];
    const field_names_raw = field_names_ptr[0..field_names_len];

    const ep = EventProcessor.initWithFieldNames(
        allocator,
        capacity,
        schema_bytes,
        field_meta_ptr,
        field_count,
        field_names_raw,
    ) catch return 0;

    return allocHandle(ep) orelse {
        ep.deinit();
        return 0;
    };
}

/// Destroy EventProcessor instance
pub export fn ep_destroy(handle: u32) void {
    if (getProcessor(handle)) |ep| {
        ep.deinit();
        freeHandle(handle);
    }
}

/// Create log entry from input (Parse + Compact only, no dedup)
///
/// handle: EventProcessor handle
/// input_ptr: pointer to input data
/// input_len: length of input data
/// format: InputFormat (0=JSON, 1=MSGPACK [deferred], 2=ARROW_PASSTHROUGH)
/// output_ptr: pointer to output buffer (caller-provided, min 32 bytes for header)
/// output_len: length of output buffer
///
/// Returns: ResultCode (0=OK)
/// On success, output_ptr contains ResultHeader followed by Arrow IPC bytes.
pub export fn ep_create_log_entry(
    handle: u32,
    input_ptr: [*]const u8,
    input_len: u32,
    format: u8,
    output_ptr: [*]u8,
    output_len: u32,
) u32 {
    const ep = getProcessor(handle) orelse return @intFromEnum(ResultCode.INVALID_HANDLE);

    const input = input_ptr[0..input_len];
    const output = output_ptr[0..output_len];

    if (output_len < @sizeOf(ResultHeader)) {
        return @intFromEnum(ResultCode.OUT_OF_MEMORY);
    }

    const input_format: InputFormat = @enumFromInt(format);
    const arrow_offset: u32 = @sizeOf(ResultHeader);

    // Branch based on extraction path
    if (ep.schema_config.has_extraction_fields) {
        // EXTRACTION PATH: Use DynamicColumns for schemas with value.* fields
        var dyn_cols = &ep.dynamic_columns.?;
        dyn_cols.reset();

        // Parse with extraction
        var extraction_error_code: ResultCode = .PARSE_ERROR;
        const extracted: u32 = switch (input_format) {
            .JSON => extractJsonEventsWithWorkspaceGrowth(ep, input, dyn_cols, &extraction_error_code) catch {
                writeResultHeader(output, extraction_error_code, 0, 0, 0, 0);
                return @intFromEnum(extraction_error_code);
            },
            .MSGPACK => extractMsgpackEventsWithWorkspaceGrowth(ep, input, dyn_cols, &extraction_error_code, false) catch {
                writeResultHeader(output, extraction_error_code, 0, 0, 0, 0);
                return @intFromEnum(extraction_error_code);
            },
            .MSGPACK_STREAM => extractMsgpackEventsWithWorkspaceGrowth(ep, input, dyn_cols, &extraction_error_code, true) catch {
                writeResultHeader(output, extraction_error_code, 0, 0, 0, 0);
                return @intFromEnum(extraction_error_code);
            },
            .ARROW_PASSTHROUGH => {
                writeResultHeader(output, .INVALID_FORMAT, 0, 0, 0, 0);
                return @intFromEnum(ResultCode.INVALID_FORMAT);
            },
        };

        // No dedup in columine - all events are processed
        const processed_count = extracted;
        const duplicates: u32 = 0;

        // Encode with DynamicColumns encoder
        const arrow_bytes = arrow.ipc_writer.writeArrowIpcFromDynamicColumns(
            dyn_cols,
            &ep.schema_config,
            output[arrow_offset..],
        ) catch |err| {
            const code = mapEncodeError(err);
            writeResultHeader(output, code, 0, 0, 0, 0);
            return @intFromEnum(code);
        };

        const arrow_len: u32 = @intCast(arrow_bytes.len);
        writeResultHeader(output, .OK, arrow_offset, arrow_len, processed_count, duplicates);
    } else {
        // BASE PATH: Use EventColumns for standard 4-column schema
        ep.event_columns.reset();

        // Parse with json_scanner
        const parse_result: ParseError = switch (input_format) {
            .JSON => parsing.json_scanner.parseJsonEvents(input, &ep.event_columns, allocator),
            .MSGPACK => parsing.msgpack_scanner.parseMsgpackEvents(input, &ep.event_columns),
            .MSGPACK_STREAM => parsing.msgpack_scanner.parseMsgpackStream(input, &ep.event_columns),
            .ARROW_PASSTHROUGH => .OK,
        };

        if (parse_result != .OK) {
            const code = mapParseError(parse_result);
            writeResultHeader(output, code, 0, 0, 0, 0);
            return @intFromEnum(code);
        }

        // No dedup in columine - all events are processed
        const processed_count: u32 = ep.event_columns.count;
        const duplicates: u32 = 0;

        // Encode with EventColumns encoder
        const arrow_bytes = arrow.ipc_writer.writeArrowIpcFromColumnsWithSchema(
            &ep.event_columns,
            &ep.schema_config,
            output[arrow_offset..],
        ) catch |err| {
            const code = mapEncodeError(err);
            writeResultHeader(output, code, 0, 0, 0, 0);
            return @intFromEnum(code);
        };

        const arrow_len: u32 = @intCast(arrow_bytes.len);
        writeResultHeader(output, .OK, arrow_offset, arrow_len, processed_count, duplicates);
    }

    return @intFromEnum(ResultCode.OK);
}

// =============================================================================
// Helpers
// =============================================================================

fn writeResultHeader(
    output: []u8,
    code: ResultCode,
    arrow_offset: u32,
    arrow_len: u32,
    events_processed: u32,
    duplicates_filtered: u32,
) void {
    const header = ResultHeader{
        .code = @intFromEnum(code),
        .arrow_ipc_offset = arrow_offset,
        .arrow_ipc_len = arrow_len,
        .events_processed = events_processed,
        .duplicates_filtered = duplicates_filtered,
    };
    @memcpy(output[0..@sizeOf(ResultHeader)], std.mem.asBytes(&header));
}

fn mapParseError(err: ParseError) ResultCode {
    return switch (err) {
        .TOO_MANY_EVENTS => .TOO_MANY_EVENTS,
        .BUFFER_OVERFLOW => .BUFFER_OVERFLOW,
        .OUT_OF_MEMORY => .OUT_OF_MEMORY,
        else => .PARSE_ERROR,
    };
}

fn mapExtractionError(err: ExtractionError) ResultCode {
    return switch (err) {
        error.TooManyEvents => .TOO_MANY_EVENTS,
        error.BufferOverflow => .BUFFER_OVERFLOW,
        error.OutOfMemory => .OUT_OF_MEMORY,
        else => .PARSE_ERROR,
    };
}

fn mapEncodeError(err: anyerror) ResultCode {
    return switch (err) {
        error.BufferOverflow => .BUFFER_OVERFLOW,
        error.OutOfMemory => .OUT_OF_MEMORY,
        else => .ENCODE_ERROR,
    };
}

fn extractJsonEventsWithWorkspaceGrowth(
    ep: *EventProcessor,
    input: []const u8,
    dyn_cols: *DynamicColumns,
    out_error_code: *ResultCode,
) error{ExtractionFailed}!u32 {
    const extraction_config = &ep.extraction_config.?;

    while (true) {
        const extracted = parsing.json_extractor.extractJsonEvents(
            input,
            extraction_config,
            dyn_cols,
            ep.work_buffer,
        ) catch |err| {
            if (err == error.BufferOverflow) {
                const min_needed = if (ep.work_buffer.len == 0)
                    @min(input.len + 64, EventProcessor.WASM_MAX_WORK_BUFFER_BYTES)
                else
                    @min(ep.work_buffer.len * 2, EventProcessor.WASM_MAX_WORK_BUFFER_BYTES);

                ep.ensureWorkBufferCapacity(min_needed) catch {
                    out_error_code.* = .OUT_OF_MEMORY;
                    return error.ExtractionFailed;
                };
                continue;
            }

            out_error_code.* = mapExtractionError(err);
            return error.ExtractionFailed;
        };

        return extracted;
    }
}

fn extractMsgpackEventsWithWorkspaceGrowth(
    ep: *EventProcessor,
    input: []const u8,
    dyn_cols: *DynamicColumns,
    out_error_code: *ResultCode,
    is_stream: bool,
) error{ExtractionFailed}!u32 {
    const extraction_config = &ep.extraction_config.?;

    while (true) {
        const extracted = parsing.msgpack_extractor.extractMsgpackEvents(
            input,
            extraction_config,
            dyn_cols,
            ep.work_buffer,
            is_stream,
        ) catch |err| {
            if (err == error.BufferOverflow) {
                const min_needed = if (ep.work_buffer.len == 0)
                    @min(input.len + 64, EventProcessor.WASM_MAX_WORK_BUFFER_BYTES)
                else
                    @min(ep.work_buffer.len * 2, EventProcessor.WASM_MAX_WORK_BUFFER_BYTES);

                ep.ensureWorkBufferCapacity(min_needed) catch {
                    out_error_code.* = .OUT_OF_MEMORY;
                    return error.ExtractionFailed;
                };
                continue;
            }

            out_error_code.* = mapExtractionError(err);
            return error.ExtractionFailed;
        };

        return extracted;
    }
}

// =============================================================================
// Tests
// =============================================================================

test {
    // Include all submodule tests
    _ = parsing.columns;
    _ = parsing.json_scanner;
    _ = parsing.json_parser;
    _ = parsing.json_extractor;
    _ = parsing.msgpack_scanner;
    _ = parsing.msgpack_extractor;
    _ = arrow.ipc_writer;
    _ = arrow.dynamic_schema;
    _ = arrow.dynamic_record_batch;
}

test "ep_create_log_entry with schema" {
    // Create processor with schema
    // Using a minimal valid schema message for testing
    const schema_bytes = [_]u8{
        0xFF, 0xFF, 0xFF, 0xFF, // continuation marker
        0x08, 0x00, 0x00, 0x00, // metadata size = 8
        0x00, 0x00, 0x00, 0x00, // placeholder FlatBuffer data
        0x00, 0x00, 0x00, 0x00,
    };
    const fields = [_]arrow.dynamic_schema.SignalSchemaField{
        .{ .arrow_type = .Utf8, .nullable = 0 }, // id
        .{ .arrow_type = .Utf8, .nullable = 0 }, // type
        .{ .arrow_type = .Int, .nullable = 0 }, // timestamp
        .{ .arrow_type = .Binary, .nullable = 1 }, // value
    };

    // No policy param - columine has no dedup
    const handle = ep_create_with_schema(100, &schema_bytes, 16, &fields, 4);
    defer ep_destroy(handle);
    try std.testing.expect(handle != 0);

    // Process JSON
    const input =
        \\[{"id":"test","type":"click","timestamp":1705315800000000}]
    ;
    var output: [8192]u8 = undefined;
    const result = ep_create_log_entry(handle, input.ptr, input.len, 0, &output, output.len);

    // Verify success
    try std.testing.expectEqual(@as(u32, 0), result); // ResultCode.OK

    // Verify result header shows arrow data
    const header = @as(*const ResultHeader, @ptrCast(@alignCast(&output)));
    try std.testing.expect(header.arrow_ipc_len > 0);
    try std.testing.expectEqual(@as(u32, 1), header.events_processed);
    // No dedup in columine
    try std.testing.expectEqual(@as(u32, 0), header.duplicates_filtered);
}

test "parse and extraction errors map to explicit result codes" {
    try std.testing.expectEqual(ResultCode.TOO_MANY_EVENTS, mapParseError(.TOO_MANY_EVENTS));
    try std.testing.expectEqual(ResultCode.BUFFER_OVERFLOW, mapParseError(.BUFFER_OVERFLOW));
    try std.testing.expectEqual(ResultCode.OUT_OF_MEMORY, mapParseError(.OUT_OF_MEMORY));

    try std.testing.expectEqual(ResultCode.TOO_MANY_EVENTS, mapExtractionError(error.TooManyEvents));
    try std.testing.expectEqual(ResultCode.BUFFER_OVERFLOW, mapExtractionError(error.BufferOverflow));
    try std.testing.expectEqual(ResultCode.OUT_OF_MEMORY, mapExtractionError(error.OutOfMemory));
}
