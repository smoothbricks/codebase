const std = @import("std");
const builtin = @import("builtin");

// =============================================================================
// Memory Allocator - platform-specific bump allocator
// =============================================================================

const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

// For WASM: use linear memory starting after globals
// For native: use a static buffer
const HEAP_SIZE = 64 * 1024 * 1024; // 64MB heap

var static_heap: if (is_wasm) void else [HEAP_SIZE]u8 = if (is_wasm) {} else undefined;
var heap_offset: usize = if (is_wasm) 1024 * 1024 + 4096 else 0;

fn alloc(comptime T: type, count: usize) []T {
    const bytes = count * @sizeOf(T);
    const alignment: usize = @alignOf(T);
    const aligned = (heap_offset + alignment - 1) & ~(alignment - 1);
    heap_offset = aligned + bytes;

    if (is_wasm) {
        // WASM: use linear memory addresses directly
        const ptr: [*]T = @ptrFromInt(aligned);
        return ptr[0..count];
    } else {
        // Native: use static buffer
        const ptr: [*]T = @ptrCast(@alignCast(static_heap[aligned..].ptr));
        return ptr[0..count];
    }
}

fn reset_heap() void {
    heap_offset = if (is_wasm) 1024 * 1024 + 4096 else 0;
}

// =============================================================================
// Hash Map for keyBy operations
// Uses open addressing with linear probing
// =============================================================================

const EMPTY: u32 = 0xFFFFFFFF;
const TOMBSTONE: u32 = 0xFFFFFFFE;

pub const HashMap = struct {
    keys: []u32,
    values: []u32, // indices into original array
    timestamps: []f64, // for keepValue(latest)
    capacity: usize,
    size: usize,

    pub fn init(capacity: usize) HashMap {
        const cap = nextPowerOf2(capacity * 2); // Load factor 0.5
        const keys = alloc(u32, cap);
        const values = alloc(u32, cap);
        const timestamps = alloc(f64, cap);

        for (0..cap) |i| {
            keys[i] = EMPTY;
            timestamps[i] = -std.math.inf(f64);
        }

        return .{
            .keys = keys,
            .values = values,
            .timestamps = timestamps,
            .capacity = cap,
            .size = 0,
        };
    }

    fn nextPowerOf2(n: usize) usize {
        var v = n;
        v -= 1;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v += 1;
        return if (v < 16) 16 else v;
    }

    fn hash(key: u32, cap: usize) usize {
        // FNV-1a style mixing
        var h: u64 = key;
        h ^= h >> 16;
        h *%= 0x85ebca6b;
        h ^= h >> 13;
        h *%= 0xc2b2ae35;
        h ^= h >> 16;
        return @intCast(h & (cap - 1));
    }

    /// Insert or update - keeps latest by timestamp
    pub fn upsertLatest(self: *HashMap, key: u32, index: u32, timestamp: f64) void {
        var slot = hash(key, self.capacity);

        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY or k == TOMBSTONE) {
                // Insert new
                self.keys[slot] = key;
                self.values[slot] = index;
                self.timestamps[slot] = timestamp;
                self.size += 1;
                return;
            } else if (k == key) {
                // Update if newer
                if (timestamp > self.timestamps[slot]) {
                    self.values[slot] = index;
                    self.timestamps[slot] = timestamp;
                }
                return;
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    /// Remove a key
    pub fn remove(self: *HashMap, key: u32) bool {
        var slot = hash(key, self.capacity);

        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY) return false;
            if (k == key) {
                self.keys[slot] = TOMBSTONE;
                self.size -= 1;
                return true;
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    /// Get value index for a key
    pub fn get(self: *const HashMap, key: u32) ?u32 {
        var slot = hash(key, self.capacity);

        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY) return null;
            if (k == key) return self.values[slot];
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    /// Collect all value indices into output array
    pub fn collectValues(self: *const HashMap, out: [*]u32) usize {
        var count: usize = 0;
        for (0..self.capacity) |i| {
            const k = self.keys[i];
            if (k != EMPTY and k != TOMBSTONE) {
                out[count] = self.values[i];
                count += 1;
            }
        }
        return count;
    }
};

// =============================================================================
// Hash Set for unique() operations
// =============================================================================

pub const HashSet = struct {
    keys: []u32,
    capacity: usize,
    size: usize,

    pub fn init(capacity: usize) HashSet {
        const cap = HashMap.nextPowerOf2(capacity * 2);
        const keys = alloc(u32, cap);
        for (0..cap) |i| {
            keys[i] = EMPTY;
        }
        return .{ .keys = keys, .capacity = cap, .size = 0 };
    }

    pub fn insert(self: *HashSet, key: u32) bool {
        var slot = HashMap.hash(key, self.capacity);

        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY or k == TOMBSTONE) {
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
            if (k == EMPTY) return false;
            if (k == key) return true;
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    pub fn remove(self: *HashSet, key: u32) bool {
        var slot = HashMap.hash(key, self.capacity);

        while (true) {
            const k = self.keys[slot];
            if (k == EMPTY) return false;
            if (k == key) {
                self.keys[slot] = TOMBSTONE;
                self.size -= 1;
                return true;
            }
            slot = (slot + 1) & (self.capacity - 1);
        }
    }

    /// Union: add all elements from other set
    pub fn unionWith(self: *HashSet, other: *const HashSet) void {
        for (0..other.capacity) |i| {
            const k = other.keys[i];
            if (k != EMPTY and k != TOMBSTONE) {
                _ = self.insert(k);
            }
        }
    }

    /// Intersect in-place: remove elements not in other
    pub fn intersectWith(self: *HashSet, other: *const HashSet) void {
        for (0..self.capacity) |i| {
            const k = self.keys[i];
            if (k != EMPTY and k != TOMBSTONE) {
                if (!other.contains(k)) {
                    self.keys[i] = TOMBSTONE;
                    self.size -= 1;
                }
            }
        }
    }

    /// Except in-place: remove elements that are in other
    pub fn exceptWith(self: *HashSet, other: *const HashSet) void {
        for (0..self.capacity) |i| {
            const k = self.keys[i];
            if (k != EMPTY and k != TOMBSTONE) {
                if (other.contains(k)) {
                    self.keys[i] = TOMBSTONE;
                    self.size -= 1;
                }
            }
        }
    }

    /// Export to sorted array
    pub fn toSortedArray(self: *const HashSet, out: [*]u32) usize {
        var count: usize = 0;
        for (0..self.capacity) |i| {
            const k = self.keys[i];
            if (k != EMPTY and k != TOMBSTONE) {
                out[count] = k;
                count += 1;
            }
        }
        // Sort using insertion sort (good for small-medium sizes)
        const slice = out[0..count];
        for (1..count) |i| {
            const key = slice[i];
            var j: usize = i;
            while (j > 0 and slice[j - 1] > key) {
                slice[j] = slice[j - 1];
                j -= 1;
            }
            slice[j] = key;
        }
        return count;
    }
};

// =============================================================================
// Exported Functions - Reducer Operations
// =============================================================================

// --- HashMap operations ---

var global_map: ?HashMap = null;

export fn hashmap_create(capacity: usize) void {
    global_map = HashMap.init(capacity);
}

export fn hashmap_upsert_latest(key: u32, index: u32, timestamp: f64) void {
    if (global_map) |*map| {
        map.upsertLatest(key, index, timestamp);
    }
}

export fn hashmap_remove(key: u32) u32 {
    if (global_map) |*map| {
        return if (map.remove(key)) 1 else 0;
    }
    return 0;
}

export fn hashmap_get(key: u32) u32 {
    if (global_map) |*map| {
        return map.get(key) orelse EMPTY;
    }
    return EMPTY;
}

export fn hashmap_size() usize {
    if (global_map) |*map| {
        return map.size;
    }
    return 0;
}

export fn hashmap_collect(out_ptr: [*]u32) usize {
    if (global_map) |*map| {
        return map.collectValues(out_ptr);
    }
    return 0;
}

/// Batch upsert - process entire batch at once (the hot path)
export fn hashmap_batch_upsert_latest(
    keys_ptr: [*]const u32,
    indices_ptr: [*]const u32,
    timestamps_ptr: [*]const f64,
    len: usize,
) void {
    if (global_map) |*map| {
        for (0..len) |i| {
            map.upsertLatest(keys_ptr[i], indices_ptr[i], timestamps_ptr[i]);
        }
    }
}

/// Batch remove keys
export fn hashmap_batch_remove(keys_ptr: [*]const u32, len: usize) usize {
    if (global_map) |*map| {
        var removed: usize = 0;
        for (0..len) |i| {
            if (map.remove(keys_ptr[i])) removed += 1;
        }
        return removed;
    }
    return 0;
}

// --- HashSet operations ---

var global_set: ?HashSet = null;
var global_set_b: ?HashSet = null;

export fn hashset_create(capacity: usize) void {
    global_set = HashSet.init(capacity);
}

export fn hashset_create_b(capacity: usize) void {
    global_set_b = HashSet.init(capacity);
}

export fn hashset_insert(key: u32) u32 {
    if (global_set) |*set| {
        return if (set.insert(key)) 1 else 0;
    }
    return 0;
}

export fn hashset_insert_b(key: u32) u32 {
    if (global_set_b) |*set| {
        return if (set.insert(key)) 1 else 0;
    }
    return 0;
}

export fn hashset_batch_insert(keys_ptr: [*]const u32, len: usize) usize {
    if (global_set) |*set| {
        var inserted: usize = 0;
        for (0..len) |i| {
            if (set.insert(keys_ptr[i])) inserted += 1;
        }
        return inserted;
    }
    return 0;
}

export fn hashset_batch_insert_b(keys_ptr: [*]const u32, len: usize) usize {
    if (global_set_b) |*set| {
        var inserted: usize = 0;
        for (0..len) |i| {
            if (set.insert(keys_ptr[i])) inserted += 1;
        }
        return inserted;
    }
    return 0;
}

export fn hashset_contains(key: u32) u32 {
    if (global_set) |*set| {
        return if (set.contains(key)) 1 else 0;
    }
    return 0;
}

export fn hashset_size() usize {
    if (global_set) |*set| {
        return set.size;
    }
    return 0;
}

export fn hashset_union() void {
    if (global_set) |*a| {
        if (global_set_b) |*b| {
            a.unionWith(b);
        }
    }
}

export fn hashset_intersect() void {
    if (global_set) |*a| {
        if (global_set_b) |*b| {
            a.intersectWith(b);
        }
    }
}

export fn hashset_except() void {
    if (global_set) |*a| {
        if (global_set_b) |*b| {
            a.exceptWith(b);
        }
    }
}

export fn hashset_to_sorted(out_ptr: [*]u32) usize {
    if (global_set) |*set| {
        return set.toSortedArray(out_ptr);
    }
    return 0;
}

// --- Memory management ---

export fn heap_reset() void {
    reset_heap();
    global_map = null;
    global_set = null;
    global_set_b = null;
}

// =============================================================================
// SIMD Aggregations (keep these for columnar operations)
// =============================================================================

const V4f64 = @Vector(4, f64);

export fn sum_f64(arr_ptr: [*]const f64, len: usize) f64 {
    const arr = arr_ptr[0..len];
    var sum_vec: V4f64 = @splat(0.0);
    var i: usize = 0;

    while (i + 4 <= len) : (i += 4) {
        const v: V4f64 = arr[i..][0..4].*;
        sum_vec += v;
    }

    var result = @reduce(.Add, sum_vec);
    while (i < len) : (i += 1) {
        result += arr[i];
    }
    return result;
}

export fn min_f64(arr_ptr: [*]const f64, len: usize) f64 {
    if (len == 0) return std.math.inf(f64);

    const arr = arr_ptr[0..len];
    var min_vec: V4f64 = @splat(arr[0]);
    var i: usize = 0;

    while (i + 4 <= len) : (i += 4) {
        const v: V4f64 = arr[i..][0..4].*;
        min_vec = @min(min_vec, v);
    }

    var result = @reduce(.Min, min_vec);
    while (i < len) : (i += 1) {
        result = @min(result, arr[i]);
    }
    return result;
}

export fn max_f64(arr_ptr: [*]const f64, len: usize) f64 {
    if (len == 0) return -std.math.inf(f64);

    const arr = arr_ptr[0..len];
    var max_vec: V4f64 = @splat(arr[0]);
    var i: usize = 0;

    while (i + 4 <= len) : (i += 4) {
        const v: V4f64 = arr[i..][0..4].*;
        max_vec = @max(max_vec, v);
    }

    var result = @reduce(.Max, max_vec);
    while (i < len) : (i += 1) {
        result = @max(result, arr[i]);
    }
    return result;
}

export fn filter_gte_f64(arr_ptr: [*]const f64, len: usize, threshold: f64) usize {
    const arr = arr_ptr[0..len];
    const thresh_vec: V4f64 = @splat(threshold);

    var count: usize = 0;
    var i: usize = 0;

    while (i + 4 <= len) : (i += 4) {
        const v: V4f64 = arr[i..][0..4].*;
        const cmp = v >= thresh_vec;
        // Use @popCount on the mask for efficient counting
        const mask: u4 = @bitCast(cmp);
        count += @popCount(mask);
    }

    while (i < len) : (i += 1) {
        if (arr[i] >= threshold) count += 1;
    }

    return count;
}

export fn filter_lt_f64(arr_ptr: [*]const f64, len: usize, threshold: f64) usize {
    const arr = arr_ptr[0..len];
    const thresh_vec: V4f64 = @splat(threshold);

    var count: usize = 0;
    var i: usize = 0;

    while (i + 4 <= len) : (i += 4) {
        const v: V4f64 = arr[i..][0..4].*;
        const cmp = v < thresh_vec;
        const mask: u4 = @bitCast(cmp);
        count += @popCount(mask);
    }

    while (i < len) : (i += 1) {
        if (arr[i] < threshold) count += 1;
    }

    return count;
}

// =============================================================================
// Sorted Array Operations (for when data is already sorted)
// =============================================================================

export fn sorted_union(a_ptr: [*]const u32, a_len: usize, b_ptr: [*]const u32, b_len: usize, out_ptr: [*]u32) usize {
    const a = a_ptr[0..a_len];
    const b = b_ptr[0..b_len];

    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;

    while (i < a.len and j < b.len) {
        const va = a[i];
        const vb = b[j];
        if (va < vb) {
            out_ptr[k] = va;
            k += 1;
            i += 1;
        } else if (vb < va) {
            out_ptr[k] = vb;
            k += 1;
            j += 1;
        } else {
            out_ptr[k] = va;
            k += 1;
            i += 1;
            j += 1;
        }
    }

    while (i < a.len) : (i += 1) {
        out_ptr[k] = a[i];
        k += 1;
    }
    while (j < b.len) : (j += 1) {
        out_ptr[k] = b[j];
        k += 1;
    }

    return k;
}

export fn sorted_intersect(a_ptr: [*]const u32, a_len: usize, b_ptr: [*]const u32, b_len: usize, out_ptr: [*]u32) usize {
    const a = a_ptr[0..a_len];
    const b = b_ptr[0..b_len];

    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;

    while (i < a.len and j < b.len) {
        const va = a[i];
        const vb = b[j];
        if (va < vb) {
            i += 1;
        } else if (vb < va) {
            j += 1;
        } else {
            out_ptr[k] = va;
            k += 1;
            i += 1;
            j += 1;
        }
    }

    return k;
}

export fn sorted_except(a_ptr: [*]const u32, a_len: usize, b_ptr: [*]const u32, b_len: usize, out_ptr: [*]u32) usize {
    const a = a_ptr[0..a_len];
    const b = b_ptr[0..b_len];

    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;

    while (i < a.len and j < b.len) {
        const va = a[i];
        const vb = b[j];
        if (va < vb) {
            out_ptr[k] = va;
            k += 1;
            i += 1;
        } else if (vb < va) {
            j += 1;
        } else {
            i += 1;
            j += 1;
        }
    }

    while (i < a.len) : (i += 1) {
        out_ptr[k] = a[i];
        k += 1;
    }

    return k;
}

export fn sorted_has(arr_ptr: [*]const u32, len: usize, target: u32) u32 {
    const arr = arr_ptr[0..len];

    var left: usize = 0;
    var right: usize = len;

    while (left < right) {
        const mid = left + (right - left) / 2;
        const val = arr[mid];
        if (val < target) {
            left = mid + 1;
        } else if (val > target) {
            right = mid;
        } else {
            return 1;
        }
    }

    return 0;
}
