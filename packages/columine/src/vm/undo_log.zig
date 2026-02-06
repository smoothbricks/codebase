// =============================================================================
// Undo Log for Speculation Rollback
// =============================================================================
//
// Records every mutation for rollback during speculative execution.
//
// When speculation is rejected, we replay the undo log in reverse order
// to restore state to pre-speculation state.
//
// Undo entries are stored in pre-allocated TypedArrays for efficiency.

const std = @import("std");
const state = @import("state.zig");

// =============================================================================
// Undo Operation Types
// =============================================================================

pub const UndoOp = enum(u8) {
    // HashMap operations
    MAP_INSERT = 1, // Was insert -> rollback: delete(key)
    MAP_UPDATE = 2, // Was update -> rollback: set(key, prev_value, prev_ts)
    MAP_DELETE = 3, // Was delete -> rollback: restore(key, prev_value, prev_ts)

    // HashSet operations
    SET_INSERT = 4, // Was insert -> rollback: delete(elem)
    SET_DELETE = 5, // Was delete -> rollback: insert(elem)

    // Aggregate operations
    AGG_UPDATE = 6, // -> rollback: restore prev value and count

    // SortedArray operations
    SORTED_INSERT = 7, // -> rollback: remove value (TODO: need index tracking)

    // Nested map operations (includes key path)
    NESTED_MAP_INSERT = 8, // -> rollback: delete at key path
    NESTED_MAP_UPDATE = 9, // -> rollback: restore at key path
    NESTED_MAP_DELETE = 10, // -> rollback: restore at key path
    NESTED_SET_INSERT = 11, // -> rollback: delete at key path
    NESTED_SET_DELETE = 12, // -> rollback: insert at key path
    NESTED_AGG_UPDATE = 13, // -> rollback: restore at key path
    NESTED_SLOT_CREATE = 14, // -> rollback: remove slot at key path
};

// =============================================================================
// Undo Log Entry (fixed size for array storage)
// =============================================================================

pub const UndoEntry = extern struct {
    op: UndoOp,
    slot: u8,
    depth: u8, // Key path depth (0 = flat, >0 = nested)
    _pad: u8,
    key: u32, // Primary key (or last key in path)
    prev_value: u32, // Previous value (for updates/deletes)
    prev_timestamp_or_key1: u64, // Previous timestamp OR first key in path
    // For deep nesting, additional keys stored in aux array
};

// Maximum nesting depth supported
pub const MAX_DEPTH = 8;

// =============================================================================
// Undo Log
// =============================================================================

pub const UndoLog = struct {
    // Main entry storage
    entries: []UndoEntry,
    count: u32,
    capacity: u32,

    // Auxiliary storage for deep key paths (depth > 2)
    // Format: [key2, key3, key4, ...] for each entry needing it
    aux_keys: []u32,
    aux_offset: u32,
    aux_capacity: u32,

    // For AVG aggregates, we need to store prev_count too
    aux_counts: []u64,
    aux_count_offset: u32,

    pub fn init(allocator: *state.Allocator, capacity: u32) UndoLog {
        return .{
            .entries = allocator.alloc(UndoEntry, capacity),
            .count = 0,
            .capacity = capacity,
            .aux_keys = allocator.alloc(u32, capacity * MAX_DEPTH),
            .aux_offset = 0,
            .aux_capacity = capacity * MAX_DEPTH,
            .aux_counts = allocator.alloc(u64, capacity / 4), // Only ~25% are aggregates
            .aux_count_offset = 0,
        };
    }

    pub fn clear(self: *UndoLog) void {
        self.count = 0;
        self.aux_offset = 0;
        self.aux_count_offset = 0;
    }

    // ═══════════════════════════════════════════════════════════════════
    // Flat operations (depth = 0)
    // ═══════════════════════════════════════════════════════════════════

    pub fn logMapInsert(self: *UndoLog, slot: u8, key: u32) void {
        self.append(.{
            .op = .MAP_INSERT,
            .slot = slot,
            .depth = 0,
            ._pad = 0,
            .key = key,
            .prev_value = 0,
            .prev_timestamp_or_key1 = 0,
        });
    }

    pub fn logMapUpdate(self: *UndoLog, slot: u8, key: u32, prev_value: u32, prev_timestamp: f64) void {
        self.append(.{
            .op = .MAP_UPDATE,
            .slot = slot,
            .depth = 0,
            ._pad = 0,
            .key = key,
            .prev_value = prev_value,
            .prev_timestamp_or_key1 = @bitCast(prev_timestamp),
        });
    }

    pub fn logMapDelete(self: *UndoLog, slot: u8, key: u32, prev_value: u32, prev_timestamp: f64) void {
        self.append(.{
            .op = .MAP_DELETE,
            .slot = slot,
            .depth = 0,
            ._pad = 0,
            .key = key,
            .prev_value = prev_value,
            .prev_timestamp_or_key1 = @bitCast(prev_timestamp),
        });
    }

    pub fn logSetInsert(self: *UndoLog, slot: u8, elem: u32) void {
        self.append(.{
            .op = .SET_INSERT,
            .slot = slot,
            .depth = 0,
            ._pad = 0,
            .key = elem,
            .prev_value = 0,
            .prev_timestamp_or_key1 = 0,
        });
    }

    pub fn logSetDelete(self: *UndoLog, slot: u8, elem: u32) void {
        self.append(.{
            .op = .SET_DELETE,
            .slot = slot,
            .depth = 0,
            ._pad = 0,
            .key = elem,
            .prev_value = 0,
            .prev_timestamp_or_key1 = 0,
        });
    }

    pub fn logAggUpdate(self: *UndoLog, slot: u8, prev_value: f64, prev_count: u64) void {
        const entry_idx = self.count;
        self.append(.{
            .op = .AGG_UPDATE,
            .slot = slot,
            .depth = 0,
            ._pad = 0,
            .key = 0,
            .prev_value = @bitCast(@as(u32, @truncate(entry_idx))), // Index to aux_counts
            .prev_timestamp_or_key1 = @bitCast(prev_value),
        });
        // Store count in aux array
        if (self.aux_count_offset < self.aux_counts.len) {
            self.aux_counts[self.aux_count_offset] = prev_count;
            self.aux_count_offset += 1;
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Nested operations (depth > 0)
    // ═══════════════════════════════════════════════════════════════════

    pub fn logNestedMapInsert(self: *UndoLog, slot: u8, key_path: []const u32) void {
        const depth: u8 = @intCast(key_path.len);
        const entry = UndoEntry{
            .op = .NESTED_MAP_INSERT,
            .slot = slot,
            .depth = depth,
            ._pad = 0,
            .key = key_path[key_path.len - 1], // Last key
            .prev_value = 0,
            .prev_timestamp_or_key1 = if (depth >= 1) key_path[0] else 0,
        };
        self.append(entry);

        // Store additional keys in aux if depth > 2
        if (depth > 2) {
            self.storeAuxKeys(key_path[1 .. key_path.len - 1]);
        }
    }

    pub fn logNestedMapUpdate(self: *UndoLog, slot: u8, key_path: []const u32, prev_value: u32, prev_timestamp: f64) void {
        const depth: u8 = @intCast(key_path.len);
        // For updates, we need both prev_value and prev_timestamp
        // Store key1 in aux_keys, use prev_timestamp_or_key1 for actual timestamp
        const entry = UndoEntry{
            .op = .NESTED_MAP_UPDATE,
            .slot = slot,
            .depth = depth,
            ._pad = 0,
            .key = key_path[key_path.len - 1],
            .prev_value = prev_value,
            .prev_timestamp_or_key1 = @bitCast(prev_timestamp),
        };
        self.append(entry);

        // Store all keys except last in aux
        if (depth > 1) {
            self.storeAuxKeys(key_path[0 .. key_path.len - 1]);
        }
    }

    pub fn logNestedSetInsert(self: *UndoLog, slot: u8, key_path: []const u32, elem: u32) void {
        const depth: u8 = @intCast(key_path.len);
        const entry = UndoEntry{
            .op = .NESTED_SET_INSERT,
            .slot = slot,
            .depth = depth,
            ._pad = 0,
            .key = elem,
            .prev_value = 0,
            .prev_timestamp_or_key1 = if (depth >= 1) key_path[0] else 0,
        };
        self.append(entry);

        if (depth > 1) {
            self.storeAuxKeys(key_path[1..]);
        }
    }

    pub fn logNestedSetDelete(self: *UndoLog, slot: u8, key_path: []const u32, elem: u32) void {
        const depth: u8 = @intCast(key_path.len);
        const entry = UndoEntry{
            .op = .NESTED_SET_DELETE,
            .slot = slot,
            .depth = depth,
            ._pad = 0,
            .key = elem,
            .prev_value = 0,
            .prev_timestamp_or_key1 = if (depth >= 1) key_path[0] else 0,
        };
        self.append(entry);

        if (depth > 1) {
            self.storeAuxKeys(key_path[1..]);
        }
    }

    pub fn logNestedAggUpdate(self: *UndoLog, slot: u8, key_path: []const u32, prev_value: f64, prev_count: u64) void {
        const depth: u8 = @intCast(key_path.len);
        self.append(.{
            .op = .NESTED_AGG_UPDATE,
            .slot = slot,
            .depth = depth,
            ._pad = 0,
            .key = 0,
            .prev_value = @as(u32, @truncate(self.aux_count_offset)),
            .prev_timestamp_or_key1 = @bitCast(prev_value),
        });

        // Store count
        if (self.aux_count_offset < self.aux_counts.len) {
            self.aux_counts[self.aux_count_offset] = prev_count;
            self.aux_count_offset += 1;
        }

        // Store key path
        self.storeAuxKeys(key_path);
    }

    pub fn logNestedSlotCreate(self: *UndoLog, slot: u8, key_path: []const u32) void {
        const depth: u8 = @intCast(key_path.len);
        const entry = UndoEntry{
            .op = .NESTED_SLOT_CREATE,
            .slot = slot,
            .depth = depth,
            ._pad = 0,
            .key = key_path[key_path.len - 1],
            .prev_value = 0,
            .prev_timestamp_or_key1 = if (depth >= 1) key_path[0] else 0,
        };
        self.append(entry);

        if (depth > 2) {
            self.storeAuxKeys(key_path[1 .. key_path.len - 1]);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Rollback
    // ═══════════════════════════════════════════════════════════════════

    /// Rollback all changes in reverse order
    pub fn rollback(self: *UndoLog, vm_state: *state.State) void {
        var aux_key_cursor = self.aux_offset;
        var aux_count_cursor = self.aux_count_offset;

        // Process entries in reverse
        var i = self.count;
        while (i > 0) {
            i -= 1;
            const entry = self.entries[i];

            switch (entry.op) {
                // Flat operations
                .MAP_INSERT => {
                    const slot = vm_state.getSlot(entry.slot);
                    switch (slot.*) {
                        .HASHMAP => |map| _ = map.remove(entry.key),
                        else => {},
                    }
                },
                .MAP_UPDATE => {
                    const slot = vm_state.getSlot(entry.slot);
                    switch (slot.*) {
                        .HASHMAP => |map| {
                            const prev_ts: f64 = @bitCast(entry.prev_timestamp_or_key1);
                            map.restore(entry.key, entry.prev_value, prev_ts);
                        },
                        else => {},
                    }
                },
                .MAP_DELETE => {
                    const slot = vm_state.getSlot(entry.slot);
                    switch (slot.*) {
                        .HASHMAP => |map| {
                            const prev_ts: f64 = @bitCast(entry.prev_timestamp_or_key1);
                            map.restore(entry.key, entry.prev_value, prev_ts);
                        },
                        else => {},
                    }
                },
                .SET_INSERT => {
                    const slot = vm_state.getSlot(entry.slot);
                    switch (slot.*) {
                        .HASHSET => |set| _ = set.remove(entry.key),
                        else => {},
                    }
                },
                .SET_DELETE => {
                    const slot = vm_state.getSlot(entry.slot);
                    switch (slot.*) {
                        .HASHSET => |set| set.restore(entry.key),
                        else => {},
                    }
                },
                .AGG_UPDATE => {
                    const slot = vm_state.getSlot(entry.slot);
                    switch (slot.*) {
                        .AGGREGATE => |agg| {
                            aux_count_cursor -= 1;
                            const prev_count = self.aux_counts[aux_count_cursor];
                            const prev_value: f64 = @bitCast(entry.prev_timestamp_or_key1);
                            agg.restore(prev_value, prev_count);
                        },
                        else => {},
                    }
                },

                // Nested operations - reconstruct key path and apply
                .NESTED_MAP_INSERT, .NESTED_MAP_UPDATE, .NESTED_MAP_DELETE, .NESTED_SET_INSERT, .NESTED_SET_DELETE, .NESTED_AGG_UPDATE, .NESTED_SLOT_CREATE => {
                    // Reconstruct key path from entry and aux
                    var key_path: [MAX_DEPTH]u32 = undefined;
                    const depth = entry.depth;

                    if (depth >= 1) {
                        key_path[0] = @truncate(entry.prev_timestamp_or_key1);
                    }
                    if (depth > 2) {
                        // Read intermediate keys from aux (in reverse)
                        const aux_count = depth - 2;
                        aux_key_cursor -= aux_count;
                        for (0..aux_count) |j| {
                            key_path[j + 1] = self.aux_keys[aux_key_cursor + j];
                        }
                    }
                    key_path[depth - 1] = entry.key;

                    // Navigate to nested slot
                    const nested = vm_state.getNestedSlot(entry.slot, key_path[0 .. depth - 1]);

                    // Apply undo based on op type
                    switch (entry.op) {
                        .NESTED_MAP_INSERT => {
                            switch (nested.*) {
                                .HASHMAP => |map| _ = map.remove(entry.key),
                                else => {},
                            }
                        },
                        .NESTED_MAP_UPDATE => {
                            switch (nested.*) {
                                .HASHMAP => |map| {
                                    const prev_ts: f64 = @bitCast(entry.prev_timestamp_or_key1);
                                    map.restore(entry.key, entry.prev_value, prev_ts);
                                },
                                else => {},
                            }
                        },
                        .NESTED_SET_INSERT => {
                            switch (nested.*) {
                                .HASHSET => |set| _ = set.remove(entry.key),
                                else => {},
                            }
                        },
                        .NESTED_SET_DELETE => {
                            switch (nested.*) {
                                .HASHSET => |set| set.restore(entry.key),
                                else => {},
                            }
                        },
                        .NESTED_AGG_UPDATE => {
                            switch (nested.*) {
                                .AGGREGATE => |agg| {
                                    aux_count_cursor -= 1;
                                    const prev_count = self.aux_counts[aux_count_cursor];
                                    const prev_value: f64 = @bitCast(entry.prev_timestamp_or_key1);
                                    agg.restore(prev_value, prev_count);
                                },
                                else => {},
                            }
                        },
                        .NESTED_SLOT_CREATE => {
                            // TODO: Need to remove the created slot
                            // This is complex because MapOfSlots.remove needs the parent
                        },
                        else => {},
                    }
                },

                .SORTED_INSERT => {
                    // TODO: SortedArray rollback needs index tracking
                },
            }
        }

        self.clear();
    }

    // ═══════════════════════════════════════════════════════════════════
    // Internal helpers
    // ═══════════════════════════════════════════════════════════════════

    fn append(self: *UndoLog, entry: UndoEntry) void {
        if (self.count < self.capacity) {
            self.entries[self.count] = entry;
            self.count += 1;
        }
    }

    fn storeAuxKeys(self: *UndoLog, keys: []const u32) void {
        for (keys) |k| {
            if (self.aux_offset < self.aux_capacity) {
                self.aux_keys[self.aux_offset] = k;
                self.aux_offset += 1;
            }
        }
    }
};
