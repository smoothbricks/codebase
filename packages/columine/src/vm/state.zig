// =============================================================================
// =============================================================================
//
// Supports arbitrary nesting: Map<K1, Map<K2, Map<K3, ...V>>>
//
// Each slot can be:
//   - HashMap<K, V>         - flat key-value
//   - HashSet<V>            - unique values
//   - Aggregate             - sum/count/min/max/avg
//   - SortedArray<V>        - for percentiles
//   - NestedMap<K, Slot>    - map where values are other slots (recursive)
//
// Memory layout:
//   - All slots allocated from arena allocator
//   - Nested slots lazily created on first access to key path
//   - Undo log tracks all mutations for rollback

const std = @import("std");
const opcodes = @import("opcodes.zig");

// =============================================================================
// Slot Types
// =============================================================================

pub const SlotType = enum(u8) {
    EMPTY = 0,
    HASHMAP = 1,
    HASHSET = 2,
    AGGREGATE = 3,
    SORTED_ARRAY = 4,
    // Nested containers - value is another slot
    MAP_OF_SLOTS = 5, // Map<K, Slot> - for arbitrary nesting
};

pub const AggType = opcodes.AggType;
pub const ValueType = opcodes.ValueType;

// =============================================================================
// Forward declarations for recursive types
// =============================================================================

pub const Slot = union(SlotType) {
    EMPTY: void,
    HASHMAP: *HashMap,
    HASHSET: *HashSet,
    AGGREGATE: *Aggregate,
    SORTED_ARRAY: *SortedArray,
    MAP_OF_SLOTS: *MapOfSlots,
};

// =============================================================================
// HashMap - flat key-value storage
// =============================================================================

const EMPTY_KEY: u32 = 0xFFFFFFFF;
const TOMBSTONE: u32 = 0xFFFFFFFE;

pub const HashMap = struct {
    keys: []u32, // interned string indices
    values: []u32, // value indices or inline values
    timestamps: []f64, // for keepValue(latest/earliest)
    capacity: u32,
    size: u32,
    key_type: ValueType,
    val_type: ValueType,

    pub fn init(allocator: *Allocator, capacity_hint: u32, key_type: ValueType, val_type: ValueType) *HashMap {
        const cap = nextPowerOf2(capacity_hint * 2); // 50% load factor
        
        const self = allocator.create(HashMap);
        self.* = .{
            .keys = allocator.alloc(u32, cap),
            .values = allocator.alloc(u32, cap),
            .timestamps = allocator.alloc(f64, cap),
            .capacity = cap,
            .size = 0,
            .key_type = key_type,
            .val_type = val_type,
        };
        
        // Initialize keys to empty
        for (self.keys) |*k| k.* = EMPTY_KEY;
        for (self.timestamps) |*t| t.* = -std.math.inf(f64);
        
        return self;
    }

    fn hash(key: u32, cap: u32) u32 {
        var h: u64 = key;
        h ^= h >> 16;
        h *%= 0x85ebca6b;
        h ^= h >> 13;
        h *%= 0xc2b2ae35;
        h ^= h >> 16;
        return @intCast(h & (cap - 1));
    }

    pub fn upsertLatest(self: *HashMap, key: u32, value: u32, timestamp: f64) UpsertResult {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                // Insert new
                self.keys[slot] = key;
                self.values[slot] = value;
                self.timestamps[slot] = timestamp;
                self.size += 1;
                return .{ .was_insert = true, .prev_value = 0, .prev_timestamp = 0 };
            } else if (k == key) {
                // Update if newer
                if (timestamp > self.timestamps[slot]) {
                    const prev_val = self.values[slot];
                    const prev_ts = self.timestamps[slot];
                    self.values[slot] = value;
                    self.timestamps[slot] = timestamp;
                    return .{ .was_insert = false, .prev_value = prev_val, .prev_timestamp = prev_ts };
                }
                return .{ .was_insert = false, .prev_value = self.values[slot], .prev_timestamp = self.timestamps[slot] };
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn upsertEarliest(self: *HashMap, key: u32, value: u32, timestamp: f64) UpsertResult {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                self.keys[slot] = key;
                self.values[slot] = value;
                self.timestamps[slot] = timestamp;
                self.size += 1;
                return .{ .was_insert = true, .prev_value = 0, .prev_timestamp = 0 };
            } else if (k == key) {
                if (timestamp < self.timestamps[slot]) {
                    const prev_val = self.values[slot];
                    const prev_ts = self.timestamps[slot];
                    self.values[slot] = value;
                    self.timestamps[slot] = timestamp;
                    return .{ .was_insert = false, .prev_value = prev_val, .prev_timestamp = prev_ts };
                }
                return .{ .was_insert = false, .prev_value = self.values[slot], .prev_timestamp = self.timestamps[slot] };
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn upsertFirst(self: *HashMap, key: u32, value: u32) UpsertResult {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                self.keys[slot] = key;
                self.values[slot] = value;
                self.size += 1;
                return .{ .was_insert = true, .prev_value = 0, .prev_timestamp = 0 };
            } else if (k == key) {
                // First wins - don't update
                return .{ .was_insert = false, .prev_value = self.values[slot], .prev_timestamp = 0 };
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn upsertLast(self: *HashMap, key: u32, value: u32) UpsertResult {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                self.keys[slot] = key;
                self.values[slot] = value;
                self.size += 1;
                return .{ .was_insert = true, .prev_value = 0, .prev_timestamp = 0 };
            } else if (k == key) {
                // Last wins - always update
                const prev_val = self.values[slot];
                self.values[slot] = value;
                return .{ .was_insert = false, .prev_value = prev_val, .prev_timestamp = 0 };
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn get(self: *const HashMap, key: u32) ?u32 {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY) return null;
            if (k == key) return self.values[slot];
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn remove(self: *HashMap, key: u32) ?u32 {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY) return null;
            if (k == key) {
                const prev = self.values[slot];
                self.keys[slot] = TOMBSTONE;
                self.size -= 1;
                return prev;
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    // Restore a previously deleted key (for undo)
    pub fn restore(self: *HashMap, key: u32, value: u32, timestamp: f64) void {
        var slot = hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                self.keys[slot] = key;
                self.values[slot] = value;
                self.timestamps[slot] = timestamp;
                self.size += 1;
                return;
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }
};

pub const UpsertResult = struct {
    was_insert: bool,
    prev_value: u32,
    prev_timestamp: f64,
};

// =============================================================================
// HashSet - unique values
// =============================================================================

pub const HashSet = struct {
    keys: []u32,
    capacity: u32,
    size: u32,
    elem_type: ValueType,

    pub fn init(allocator: *Allocator, capacity_hint: u32, elem_type: ValueType) *HashSet {
        const cap = nextPowerOf2(capacity_hint * 2);
        
        const self = allocator.create(HashSet);
        self.* = .{
            .keys = allocator.alloc(u32, cap),
            .capacity = cap,
            .size = 0,
            .elem_type = elem_type,
        };
        
        for (self.keys) |*k| k.* = EMPTY_KEY;
        return self;
    }

    pub fn insert(self: *HashSet, key: u32) bool {
        var slot = HashMap.hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                self.keys[slot] = key;
                self.size += 1;
                return true; // inserted
            } else if (k == key) {
                return false; // already exists
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn contains(self: *const HashSet, key: u32) bool {
        var slot = HashMap.hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY) return false;
            if (k == key) return true;
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn remove(self: *HashSet, key: u32) bool {
        var slot = HashMap.hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY_KEY) return false;
            if (k == key) {
                self.keys[slot] = TOMBSTONE;
                self.size -= 1;
                return true;
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn restore(self: *HashSet, key: u32) void {
        _ = self.insert(key);
    }
};

// =============================================================================
// Aggregate - running sum/count/min/max/avg
// =============================================================================

pub const Aggregate = struct {
    agg_type: AggType,
    value: f64,
    count: u64, // for AVG
    
    pub fn init(allocator: *Allocator, agg_type: AggType) *Aggregate {
        const self = allocator.create(Aggregate);
        self.* = .{
            .agg_type = agg_type,
            .value = switch (agg_type) {
                .SUM, .COUNT, .AVG => 0.0,
                .MIN => std.math.inf(f64),
                .MAX => -std.math.inf(f64),
            },
            .count = 0,
        };
        return self;
    }

    pub fn update(self: *Aggregate, val: f64) f64 {
        const prev = self.value;
        switch (self.agg_type) {
            .SUM => self.value += val,
            .COUNT => self.value += 1,
            .MIN => self.value = @min(self.value, val),
            .MAX => self.value = @max(self.value, val),
            .AVG => {
                // Welford's online algorithm
                self.count += 1;
                self.value += (val - self.value) / @as(f64, @floatFromInt(self.count));
            },
        }
        return prev;
    }

    pub fn restore(self: *Aggregate, prev_value: f64, prev_count: u64) void {
        self.value = prev_value;
        self.count = prev_count;
    }
};

// =============================================================================
// SortedArray - for percentiles
// =============================================================================

pub const SortedArray = struct {
    data: []f64,
    size: u32,
    capacity: u32,
    elem_type: ValueType,

    pub fn init(allocator: *Allocator, capacity_hint: u32, elem_type: ValueType) *SortedArray {
        const self = allocator.create(SortedArray);
        self.* = .{
            .data = allocator.alloc(f64, capacity_hint),
            .size = 0,
            .capacity = capacity_hint,
            .elem_type = elem_type,
        };
        return self;
    }

    pub fn insert(self: *SortedArray, val: f64) void {
        if (self.size >= self.capacity) return; // TODO: grow or evict
        
        // Binary search for insertion point
        var left: u32 = 0;
        var right: u32 = self.size;
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (self.data[mid] < val) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        
        // Shift elements right
        var i = self.size;
        while (i > left) : (i -= 1) {
            self.data[i] = self.data[i - 1];
        }
        self.data[left] = val;
        self.size += 1;
    }

    pub fn percentile(self: *const SortedArray, p: f64) f64 {
        if (self.size == 0) return 0;
        const idx: u32 = @intFromFloat(@as(f64, @floatFromInt(self.size - 1)) * p / 100.0);
        return self.data[idx];
    }
};

// =============================================================================
// MapOfSlots - nested map where values are other slots
// =============================================================================

pub const MapOfSlots = struct {
    keys: []u32,
    slots: []Slot, // Each value is a Slot (can be another MapOfSlots for deeper nesting)
    capacity: u32,
    size: u32,
    key_type: ValueType,
    // Template for creating new nested slots
    nested_template: SlotTemplate,
    allocator: *Allocator,

    pub fn init(allocator: *Allocator, capacity_hint: u32, key_type: ValueType, template: SlotTemplate) *MapOfSlots {
        const cap = nextPowerOf2(capacity_hint * 2);
        
        const self = allocator.create(MapOfSlots);
        self.* = .{
            .keys = allocator.alloc(u32, cap),
            .slots = allocator.alloc(Slot, cap),
            .capacity = cap,
            .size = 0,
            .key_type = key_type,
            .nested_template = template,
            .allocator = allocator,
        };
        
        for (self.keys) |*k| k.* = EMPTY_KEY;
        for (self.slots) |*s| s.* = Slot{ .EMPTY = {} };
        
        return self;
    }

    /// Get or create nested slot at key
    pub fn getOrCreate(self: *MapOfSlots, key: u32) *Slot {
        var slot_idx = HashMap.hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot_idx];
            if (k == EMPTY_KEY or k == TOMBSTONE) {
                // Create new nested slot from template
                self.keys[slot_idx] = key;
                self.slots[slot_idx] = self.nested_template.instantiate(self.allocator);
                self.size += 1;
                return &self.slots[slot_idx];
            } else if (k == key) {
                return &self.slots[slot_idx];
            }
            slot_idx = (slot_idx + 1) & (self.capacity - 1);
        }
    }

    pub fn get(self: *const MapOfSlots, key: u32) ?*Slot {
        var slot_idx = HashMap.hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot_idx];
            if (k == EMPTY_KEY) return null;
            if (k == key) {
                // Need to cast away const for the pointer
                const mutable_self: *MapOfSlots = @constCast(self);
                return &mutable_self.slots[slot_idx];
            }
            slot_idx = (slot_idx + 1) & (self.capacity - 1);
        }
    }

    pub fn remove(self: *MapOfSlots, key: u32) ?Slot {
        var slot_idx = HashMap.hash(key, self.capacity);
        
        while (true) {
            const k = self.keys[slot_idx];
            if (k == EMPTY_KEY) return null;
            if (k == key) {
                const prev = self.slots[slot_idx];
                self.keys[slot_idx] = TOMBSTONE;
                self.slots[slot_idx] = .EMPTY;
                self.size -= 1;
                return prev;
            }
            slot_idx = (slot_idx + 1) & (self.capacity - 1);
        }
    }
};

// =============================================================================
// Slot Template - describes how to create nested slots
// =============================================================================

pub const SlotTemplate = struct {
    slot_type: SlotType,
    capacity_hint: u32,
    key_type: ValueType,
    val_type: ValueType,
    agg_type: AggType,
    // For deeper nesting
    nested: ?*const SlotTemplate,

    pub fn instantiate(self: SlotTemplate, allocator: *Allocator) Slot {
        return switch (self.slot_type) {
            .EMPTY => .EMPTY,
            .HASHMAP => .{ .HASHMAP = HashMap.init(allocator, self.capacity_hint, self.key_type, self.val_type) },
            .HASHSET => .{ .HASHSET = HashSet.init(allocator, self.capacity_hint, self.val_type) },
            .AGGREGATE => .{ .AGGREGATE = Aggregate.init(allocator, self.agg_type) },
            .SORTED_ARRAY => .{ .SORTED_ARRAY = SortedArray.init(allocator, self.capacity_hint, self.val_type) },
            .MAP_OF_SLOTS => .{ .MAP_OF_SLOTS = MapOfSlots.init(
                allocator,
                self.capacity_hint,
                self.key_type,
                if (self.nested) |n| n.* else SlotTemplate.empty(),
            ) },
        };
    }

    pub fn empty() SlotTemplate {
        return .{
            .slot_type = .EMPTY,
            .capacity_hint = 0,
            .key_type = .UINT32,
            .val_type = .UINT32,
            .agg_type = .SUM,
            .nested = null,
        };
    }
};

// =============================================================================
// State - collection of slots
// =============================================================================

pub const MAX_SLOTS = 256;

pub const State = struct {
    slots: [MAX_SLOTS]Slot,
    templates: [MAX_SLOTS]SlotTemplate, // For recreating on rollback
    allocator: *Allocator,

    pub fn init(allocator: *Allocator) State {
        var state = State{
            .slots = undefined,
            .templates = undefined,
            .allocator = allocator,
        };
        for (&state.slots) |*s| s.* = Slot{ .EMPTY = {} };
        for (&state.templates) |*t| t.* = SlotTemplate.empty();
        return state;
    }

    pub fn createSlot(self: *State, slot_idx: u8, template: SlotTemplate) void {
        self.templates[slot_idx] = template;
        self.slots[slot_idx] = template.instantiate(self.allocator);
    }

    pub fn getSlot(self: *State, slot_idx: u8) *Slot {
        return &self.slots[slot_idx];
    }

    /// Navigate to nested slot via key path, creating intermediates as needed
    pub fn getNestedSlot(self: *State, slot_idx: u8, key_path: []const u32) *Slot {
        var current = &self.slots[slot_idx];
        
        for (key_path) |key| {
            switch (current.*) {
                .MAP_OF_SLOTS => |map| {
                    current = map.getOrCreate(key);
                },
                else => {
                    // Type error - trying to nest into non-map
                    return current;
                },
            }
        }
        
        return current;
    }
};

// =============================================================================
// Arena Allocator
// =============================================================================

pub const Allocator = struct {
    buffer: []u8,
    offset: usize,

    pub fn init(buffer: []u8) Allocator {
        return .{ .buffer = buffer, .offset = 0 };
    }

    pub fn create(self: *Allocator, comptime T: type) *T {
        const alignment: usize = @alignOf(T);
        const mask = alignment - 1;
        const aligned = (self.offset + mask) & ~mask;
        const size = @sizeOf(T);
        self.offset = aligned + size;
        
        const ptr: *T = @ptrCast(@alignCast(self.buffer[aligned..].ptr));
        return ptr;
    }

    pub fn alloc(self: *Allocator, comptime T: type, count: u32) []T {
        const alignment: usize = @alignOf(T);
        const mask = alignment - 1;
        const aligned = (self.offset + mask) & ~mask;
        const size = @sizeOf(T) * count;
        self.offset = aligned + size;
        
        const ptr: [*]T = @ptrCast(@alignCast(self.buffer[aligned..].ptr));
        return ptr[0..count];
    }

    pub fn reset(self: *Allocator) void {
        self.offset = 0;
    }
};

// =============================================================================
// Helpers
// =============================================================================

fn nextPowerOf2(n: u32) u32 {
    var v = n;
    if (v == 0) return 16;
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v += 1;
    return if (v < 16) 16 else v;
}
