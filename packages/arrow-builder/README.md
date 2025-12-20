# @smoothbricks/arrow-builder

A low-level, high-performance columnar buffer engine for building Apache Arrow tables with explicit memory management
and zero-copy data structures.

## Overview

Arrow-builder is a lightweight alternative to `apache-arrow-js` builders, designed for use cases that require:

- **Explicit allocations**: No hidden resizes or memory surprises
- **Zero-copy construction**: Direct TypedArray access with no intermediate copies
- **Cache-aligned buffers**: 64-byte aligned TypedArrays optimized for CPU cache
- **V8-optimized codegen**: Runtime class generation for monomorphic property access
- **Predictable performance**: Hot-path operations with minimal overhead

Unlike the official `apache-arrow-js` builders which automatically resize and copy data, arrow-builder gives you
complete control over memory allocation and layout.

## Use Cases

Arrow-builder is a **generic columnar buffer engine** suitable for any tabular data collection scenario:

- **Time-series data collection**: High-frequency sensor readings, market data ticks
- **Metrics aggregation**: System metrics, application performance monitoring
- **Event sourcing buffers**: Event streams with structured attributes
- **Database result caching**: Efficient in-memory columnar storage
- **Analytics pipelines**: Fast data transformation and aggregation
- **Streaming data processing**: Low-latency event processing

## Key Features

### 1. Explicit Memory Management

```typescript
import { createColumnBuffer } from '@smoothbricks/arrow-builder';

// Create buffer with explicit capacity
const buffer = createColumnBuffer(schema, 1000);

// Write data with manual bounds checking
buffer.timestamp[buffer.writeIndex] = timestamp;
buffer.entry_type[buffer.writeIndex] = opType;
buffer.writeIndex++;

// Chain to next buffer when full
if (buffer.writeIndex >= buffer.capacity) {
  buffer.next = createColumnBuffer(schema, 1000);
}
```

### 2. Zero-Copy Arrow Tables

Arrow-builder constructs Arrow tables directly from your TypedArrays with no copies:

```typescript
// Your columnar data (already in memory)
const buffer = createColumnBuffer(schema, capacity);

// Zero-copy conversion to Arrow Table
const table = convertToArrowTable(buffer);

// TypedArrays are reused, not copied
```

### 3. Cache-Aligned TypedArrays

All buffers are automatically aligned to 64-byte cache line boundaries for optimal CPU performance:

```typescript
// Internally allocates cache-aligned ArrayBuffers
const buffer = createColumnBuffer(schema, 64);

// All TypedArrays are 64-byte aligned
buffer.timestamp; // BigInt64Array (aligned)
buffer.entry_type; // Uint8Array (aligned)
```

### 4. V8-Optimized Runtime Codegen

Arrow-builder generates optimized classes at runtime to maximize V8 performance:

```typescript
// Generated class with direct properties (not lazy getters)
class GeneratedColumnBuffer {
  timestamps: Float64Array; // Direct property
  operations: Uint8Array; // Direct property
  attr_userId_values: Uint32Array; // Direct property
  attr_userId_nulls: Uint8Array; // Direct property
  // ...
}

// V8 optimizations:
// - Hidden class stability
// - Monomorphic inline caching
// - Predictable memory layout
```

## Architecture

### Column Layout

Each attribute column consists of **two arrays sharing one ArrayBuffer**:

```
[null bitmap bytes | padding | value bytes]
         ↓                          ↓
   attr_X_nulls              attr_X_values
```

This design:

- Maintains cache locality (related data in same buffer)
- Ensures proper alignment (padding to bytesPerElement boundaries)
- Minimizes memory allocations (one buffer per column)

### Schema System

Arrow-builder uses Sury schemas with metadata to determine TypedArray types:

```typescript
import * as s from '@sury/sury';

const schema = {
  userId: s.number, // → Float64Array
  status: s.enum, // → Uint8/16/32Array (based on enum size)
  category: s.string, // → Uint32Array (string interning)
  isActive: s.boolean, // → Uint8Array
};

// Attach metadata
schema.userId.__schema_type = 'number';
schema.status.__schema_type = 'enum';
schema.status.__enum_values = ['pending', 'active', 'completed'];
```

### TypedArray Mapping

| Schema Type            | TypedArray   | Bytes | Use Case                 |
| ---------------------- | ------------ | ----- | ------------------------ |
| `number`               | Float64Array | 8     | Full-precision numbers   |
| `boolean`              | Uint8Array   | 1     | Boolean flags (0/1)      |
| `enum` (≤256 values)   | Uint8Array   | 1     | Small enums              |
| `enum` (≤65536 values) | Uint16Array  | 2     | Medium enums             |
| `enum` (>65536 values) | Uint32Array  | 4     | Large enums              |
| `category`             | Uint32Array  | 4     | String interning indices |
| `text`                 | Uint32Array  | 4     | Raw string indices       |

## Installation

```bash
npm install @smoothbricks/arrow-builder
# or
bun add @smoothbricks/arrow-builder
```

## API Reference

### Core Functions

#### `createColumnBuffer(schema, capacity?)`

Create a columnar buffer with the specified schema and capacity.

```typescript
import { createColumnBuffer } from '@smoothbricks/arrow-builder';
import type { SchemaFields } from '@smoothbricks/arrow-builder';

const schema: SchemaFields = {
  userId: userIdSchema,
  timestamp: timestampSchema,
};

const buffer = createColumnBuffer(schema, 1000);
```

**Parameters:**

- `schema`: Schema defining column types
- `capacity`: Buffer capacity (default: 64)

**Returns:** `ColumnBuffer` with direct TypedArray properties

#### `createAttributeColumns(schema, capacity?)`

Create attribute columns as a record of TypedArrays.

```typescript
import { createAttributeColumns } from '@smoothbricks/arrow-builder';

const columns = createAttributeColumns(schema, 1000);
// Returns: { attr_userId: Float64Array, attr_timestamp: Float64Array, ... }
```

### Type Utilities

#### `Microseconds`

Branded type for microsecond-precision timestamps.

```typescript
import { Microseconds } from '@smoothbricks/arrow-builder';

// Convert from milliseconds
const timestamp = Microseconds.fromMillis(Date.now());

// Convert from nanoseconds (Node.js)
const precise = Microseconds.fromNanos(process.hrtime.bigint());

// Use in buffer
buffer.timestamp[idx] = timestamp;
```

**Benefits:**

- Type-safe time unit handling
- Prevents mixing milliseconds and microseconds
- Zero runtime overhead (compile-time only)

## Performance Characteristics

### Hot Path Operations

Arrow-builder is optimized for the **hot path** (writing data):

```typescript
// Hot path: Direct property access, no function calls
buffer.timestamp[idx] = timestamp; // ~1-2 CPU cycles
buffer.entry_type[idx] = opType; // ~1-2 CPU cycles
buffer.attr_userId_values[idx] = userId; // ~1-2 CPU cycles
buffer.writeIndex++; // ~1 CPU cycle
```

### Cold Path Operations

Arrow conversion happens in the **cold path** (background processing):

```typescript
// Cold path: Run in background, no hot-path impact
const table = convertToArrowTable(buffer);
```

### Memory Layout

- System columns (timestamps, operations): **Eagerly allocated**
- Attribute columns: **Lazily allocated** on first access
- All allocations: **64-byte aligned** for cache efficiency

## Comparison with apache-arrow-js

| Feature         | arrow-builder        | apache-arrow-js  |
| --------------- | -------------------- | ---------------- |
| Allocations     | Explicit             | Hidden/automatic |
| Resizing        | Manual chaining      | Automatic grow   |
| Memory control  | Full control         | Opaque           |
| Cache alignment | 64-byte aligned      | Not guaranteed   |
| V8 optimization | Runtime codegen      | Generic builders |
| Use case        | Performance-critical | General purpose  |

## Examples

### Basic Time-Series Data Collection

```typescript
import { createColumnBuffer, Microseconds } from '@smoothbricks/arrow-builder';

// Define schema
const schema = {
  metric: metricSchema, // enum: 'cpu', 'memory', 'disk'
  value: valueSchema, // number
};

// Create buffer
const buffer = createColumnBuffer(schema, 1000);

// Write data
function recordMetric(metric: number, value: number) {
  const idx = buffer.writeIndex;

  buffer.timestamp[idx] = Microseconds.fromMillis(Date.now());
  buffer.entry_type[idx] = 1; // METRIC_SAMPLE operation
  buffer.attr_metric_values[idx] = metric;
  buffer.attr_value_values[idx] = value;

  buffer.writeIndex++;
}

// Use it
recordMetric(0, 45.2); // CPU: 45.2%
recordMetric(1, 8192); // Memory: 8192 MB
```

### Event Sourcing Buffer

```typescript
import { createColumnBuffer, Microseconds } from '@smoothbricks/arrow-builder';

const schema = {
  eventType: eventTypeSchema, // enum: 'created', 'updated', 'deleted'
  entityId: entityIdSchema, // category (string interning)
  payload: payloadSchema, // text (raw strings)
};

const buffer = createColumnBuffer(schema, 500);

function appendEvent(eventType: number, entityId: number, payload: number) {
  const idx = buffer.writeIndex;

  buffer.timestamp[idx] = Microseconds.fromNanos(process.hrtime.bigint());
  buffer.entry_type[idx] = eventType;
  buffer.attr_entityId_values[idx] = entityId;
  buffer.attr_payload_values[idx] = payload;

  buffer.writeIndex++;

  // Chain when full
  if (buffer.writeIndex >= buffer.capacity) {
    buffer.next = createColumnBuffer(schema, 500);
  }
}
```

### Database Result Caching

```typescript
import { createColumnBuffer } from '@smoothbricks/arrow-builder';

const schema = {
  id: idSchema,
  name: nameSchema,
  age: ageSchema,
  active: activeSchema,
};

const buffer = createColumnBuffer(schema, 10000);

// Cache query results
function cacheResults(rows: Array<{ id: number; name: string; age: number; active: boolean }>) {
  for (const row of rows) {
    const idx = buffer.writeIndex;

    buffer.timestamp[idx] = Microseconds.fromMillis(Date.now());
    buffer.entry_type[idx] = 0; // ROW operation
    buffer.attr_id_values[idx] = row.id;
    buffer.attr_name_values[idx] = internString(row.name);
    buffer.attr_age_values[idx] = row.age;
    buffer.attr_active_values[idx] = row.active ? 1 : 0;

    buffer.writeIndex++;
  }
}
```

## Advanced Topics

### Buffer Chaining

When a buffer reaches capacity, chain to the next buffer:

```typescript
let headBuffer = createColumnBuffer(schema, 1000);
let currentBuffer = headBuffer;

function writeEntry(data: Entry) {
  if (currentBuffer.writeIndex >= currentBuffer.capacity) {
    currentBuffer.next = createColumnBuffer(schema, 1000);
    currentBuffer = currentBuffer.next;
  }

  // Write to current buffer
  const idx = currentBuffer.writeIndex;
  // ... write data ...
  currentBuffer.writeIndex++;
}
```

### Null Handling

Each attribute has a null bitmap (Arrow format: 1=valid, 0=null):

```typescript
// Write valid value
buffer.attr_userId_values[idx] = 12345;
buffer.attr_userId_nulls[idx] = 1; // Mark as valid

// Write null value
buffer.attr_userId_values[idx] = 0; // Value doesn't matter
buffer.attr_userId_nulls[idx] = 0; // Mark as null
```

### Custom Schemas

Extend the schema system for domain-specific types:

```typescript
import * as s from '@sury/sury';

function createEnumSchema<T extends string>(values: readonly T[]) {
  const schema = s.enum(values);
  schema.__schema_type = 'enum';
  schema.__enum_values = values;
  return schema;
}

const statusSchema = createEnumSchema(['pending', 'active', 'completed']);
```

## License

MIT

## Building

Run `nx build arrow-builder` to build the library.

## Testing

Run `bun test` to execute tests.
