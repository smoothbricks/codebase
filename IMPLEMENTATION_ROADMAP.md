# Implementation Roadmap: Trace Logging System

Complete step-by-step implementation guide for the trace logging library.

## Phase 1: Schema Definition System (START HERE)

### Step 1.1: Create Schema Type Definitions

**File**: `packages/lmao/src/lib/schema/types.ts`

**Task**: Define core TypeScript types for schema definitions.

**Implementation**:
```typescript
// Schema type definitions
export type SchemaType = 'string' | 'number' | 'boolean' | 'union';

export interface BaseSchemaField<T extends SchemaType> {
  type: T;
}

export interface StringSchemaField extends BaseSchemaField<'string'> {
  mask?: 'hash' | 'url' | 'sql' | 'email';
}

export interface NumberSchemaField extends BaseSchemaField<'number'> {}

export interface BooleanSchemaField extends BaseSchemaField<'boolean'> {}

export interface UnionSchemaField extends BaseSchemaField<'union'> {
  values: readonly string[];
}

export type SchemaField = 
  | StringSchemaField 
  | NumberSchemaField 
  | BooleanSchemaField 
  | UnionSchemaField;

export interface OptionalSchemaField {
  optional: true;
  field: SchemaField;
}

export type TagAttributeSchema = Record<string, SchemaField | OptionalSchemaField>;

// Schema builder object
export interface SchemaBuilder {
  string: () => StringSchemaField;
  number: () => NumberSchemaField;
  boolean: () => BooleanSchemaField;
  union: <T extends readonly string[]>(values: T) => UnionSchemaField;
  optional: <T extends SchemaField>(field: T) => OptionalSchemaField;
}
```

**Acceptance Criteria**:
- [ ] All types compile without errors
- [ ] Types support the examples in `01a_trace_schema_system.md` lines 26-52
- [ ] Optional fields are properly typed

---

### Step 1.2: Implement Schema Builder (S object)

**File**: `packages/lmao/src/lib/schema/builder.ts`

**Task**: Create the `S` builder object that provides fluent schema definition.

**Implementation**:
```typescript
import type { SchemaBuilder, SchemaField, StringSchemaField, OptionalSchemaField } from './types';

class StringSchemaBuilder {
  private field: StringSchemaField = { type: 'string' };

  with(mask: 'hash' | 'url' | 'sql' | 'email'): StringSchemaField {
    return { ...this.field, mask };
  }

  build(): StringSchemaField {
    return this.field;
  }
}

export const S: SchemaBuilder = {
  string: () => new StringSchemaBuilder().build(),
  
  number: () => ({ type: 'number' }),
  
  boolean: () => ({ type: 'boolean' }),
  
  union: <T extends readonly string[]>(values: T) => ({
    type: 'union',
    values,
  }),
  
  optional: <T extends SchemaField>(field: T): OptionalSchemaField => ({
    optional: true,
    field,
  }),
};

// Extend StringSchemaField to support .with() method
declare module './types' {
  interface StringSchemaField {
    with(mask: 'hash' | 'url' | 'sql' | 'email'): StringSchemaField;
  }
}
```

**Acceptance Criteria**:
- [ ] `S.string()` returns a `StringSchemaField`
- [ ] `S.string().with('hash')` returns a field with mask
- [ ] `S.optional(S.string())` returns an optional field
- [ ] `S.union(['SELECT', 'INSERT'])` returns a union field
- [ ] All examples from spec lines 26-52 work

---

### Step 1.3: Implement `defineTagAttributes` Function

**File**: `packages/lmao/src/lib/schema/defineTagAttributes.ts`

**Task**: Create the main function that defines tag attribute schemas.

**Implementation**:
```typescript
import type { TagAttributeSchema } from './types';

export function defineTagAttributes<T extends TagAttributeSchema>(
  schema: T
): T {
  // Validate schema structure
  for (const [key, field] of Object.entries(schema)) {
    if (!isValidField(field)) {
      throw new Error(`Invalid schema field for '${key}': ${JSON.stringify(field)}`);
    }
  }
  
  // Return validated schema
  return schema;
}

function isValidField(field: unknown): boolean {
  if (typeof field !== 'object' || field === null) return false;
  
  // Check if it's an optional field
  if ('optional' in field && field.optional === true) {
    return 'field' in field && isValidField(field.field);
  }
  
  // Check if it's a base field with type
  return 'type' in field && 
         typeof field.type === 'string' &&
         ['string', 'number', 'boolean', 'union'].includes(field.type);
}
```

**Acceptance Criteria**:
- [ ] Function accepts schema objects matching the spec
- [ ] Returns the same schema (for now, validation only)
- [ ] Throws errors for invalid schemas
- [ ] Works with examples from spec lines 26-52

---

### Step 1.4: Implement Schema Extension (`.extend()`)

**File**: `packages/lmao/src/lib/schema/extend.ts`

**Task**: Allow schemas to be extended with additional fields.

**Implementation**:
```typescript
import type { TagAttributeSchema } from './types';

export function extendSchema<T extends TagAttributeSchema, U extends TagAttributeSchema>(
  base: T,
  extension: U
): T & U {
  // Check for conflicts
  for (const key of Object.keys(extension)) {
    if (key in base) {
      throw new Error(`Schema conflict: field '${key}' already exists in base schema`);
    }
  }
  
  return { ...base, ...extension };
}

// Add extend method to schema result
export interface ExtendedSchema<T extends TagAttributeSchema> {
  extend<U extends TagAttributeSchema>(extension: U): ExtendedSchema<T & U>;
}

export function createExtendedSchema<T extends TagAttributeSchema>(
  schema: T
): ExtendedSchema<T> {
  return {
    extend: <U extends TagAttributeSchema>(extension: U) => {
      const merged = extendSchema(schema, extension);
      return createExtendedSchema(merged);
    },
  };
}
```

**Acceptance Criteria**:
- [ ] `baseAttributes.extend({...})` works as shown in spec
- [ ] Throws error on field name conflicts
- [ ] Returns properly typed merged schema
- [ ] Can chain multiple `.extend()` calls

---

### Step 1.5: Add Tests for Schema System

**File**: `packages/lmao/src/lib/schema/__tests__/defineTagAttributes.test.ts`

**Task**: Write comprehensive tests for schema definition.

**Test Cases**:
```typescript
import { defineTagAttributes, S } from '../defineTagAttributes';

describe('defineTagAttributes', () => {
  it('defines base attributes', () => {
    const base = defineTagAttributes({
      requestId: S.string(),
      userId: S.optional(S.string().with('hash')),
      timestamp: S.number(),
    });
    
    expect(base).toBeDefined();
  });
  
  it('supports schema extension', () => {
    const base = defineTagAttributes({
      requestId: S.string(),
    });
    
    const extended = base.extend({
      duration: S.number(),
    });
    
    expect(extended).toHaveProperty('requestId');
    expect(extended).toHaveProperty('duration');
  });
  
  it('validates union types', () => {
    const schema = defineTagAttributes({
      operation: S.union(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    });
    
    expect(schema).toBeDefined();
  });
});
```

**Acceptance Criteria**:
- [ ] All tests pass
- [ ] Tests cover all schema types from spec
- [ ] Tests verify type safety

---

## Phase 2: Columnar Buffer Foundation

### Step 2.1: Create Base SpanBuffer Interface

**File**: `packages/lmao/src/lib/buffer/types.ts`

**Task**: Define the core SpanBuffer interface structure.

**Implementation**:
```typescript
export interface SpanBuffer {
  // Core columns - always present
  timestamps: Float64Array;
  operations: Uint8Array;
  nullBitmap: Uint8Array | Uint16Array | Uint32Array;
  
  // Tree structure
  children: SpanBuffer[];
  parent?: SpanBuffer;
  
  // Buffer management
  writeIndex: number;
  capacity: number;
  next?: SpanBuffer;
  
  spanId: number;
}
```

**Acceptance Criteria**:
- [ ] Interface matches spec from `01b_columnar_buffer_architecture.md` lines 70-88
- [ ] All required fields are present

---

### Step 2.2: Implement Cache-Aligned Capacity Calculation

**File**: `packages/lmao/src/lib/buffer/capacity.ts`

**Task**: Implement cache line alignment utility.

**Implementation**:
```typescript
const CACHE_LINE_SIZE = 64; // bytes

export function getCacheAlignedCapacity(
  elementCount: number,
  bytesPerElement: number
): number {
  const totalBytes = elementCount * bytesPerElement;
  const alignedBytes = Math.ceil(totalBytes / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
  return Math.ceil(alignedBytes / bytesPerElement);
}
```

**Acceptance Criteria**:
- [ ] Returns capacity aligned to 64-byte boundaries
- [ ] Handles different element sizes correctly
- [ ] Matches examples from spec lines 193-198

---

### Step 2.3: Create Empty SpanBuffer Factory

**File**: `packages/lmao/src/lib/buffer/createSpanBuffer.ts`

**Task**: Create function to allocate empty SpanBuffer with core columns.

**Implementation**:
```typescript
import type { SpanBuffer } from './types';
import { getCacheAlignedCapacity } from './capacity';

let nextGlobalSpanId = 1;

export function createEmptySpanBuffer(
  spanId: number,
  requestedCapacity: number,
  attributeCount: number
): SpanBuffer {
  // Choose bitmap type based on attribute count
  let BitmapType: Uint8ArrayConstructor | Uint16ArrayConstructor | Uint32ArrayConstructor;
  if (attributeCount <= 8) {
    BitmapType = Uint8Array;
  } else if (attributeCount <= 16) {
    BitmapType = Uint16Array;
  } else if (attributeCount <= 32) {
    BitmapType = Uint32Array;
  } else {
    throw new Error(`Too many attributes: ${attributeCount}. Maximum 32 supported.`);
  }
  
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity, 1);
  
  return {
    spanId,
    timestamps: new Float64Array(alignedCapacity),
    operations: new Uint8Array(alignedCapacity),
    nullBitmap: new BitmapType(alignedCapacity),
    children: [],
    writeIndex: 0,
    capacity: requestedCapacity,
  };
}
```

**Acceptance Criteria**:
- [ ] Creates buffer with correct capacity
- [ ] All arrays have equal length (alignedCapacity)
- [ ] Bitmap type chosen correctly based on attribute count
- [ ] Matches spec lines 174-302

---

## Phase 3: Schema-to-Buffer Integration

### Step 3.1: Generate Attribute Columns from Schema

**File**: `packages/lmao/src/lib/buffer/generateAttributeColumns.ts`

**Task**: Generate TypedArray columns based on tag attribute schema.

**Implementation**:
```typescript
import type { TagAttributeSchema, SchemaField } from '../schema/types';
import { getCacheAlignedCapacity } from './capacity';

export function generateAttributeColumns(
  schema: TagAttributeSchema,
  alignedCapacity: number
): Record<string, TypedArray> {
  const attributeColumns: Record<string, TypedArray> = {};
  
  for (const [fieldName, fieldConfig] of Object.entries(schema)) {
    const columnName = `attr_${fieldName}`;
    const actualField = 'optional' in fieldConfig ? fieldConfig.field : fieldConfig;
    
    let typedArray: TypedArray;
    switch (actualField.type) {
      case 'string':
      case 'union':
        typedArray = new Uint32Array(alignedCapacity); // String registry indices
        break;
      case 'number':
        typedArray = new Float64Array(alignedCapacity);
        break;
      case 'boolean':
        typedArray = new Uint8Array(alignedCapacity);
        break;
      default:
        typedArray = new Uint32Array(alignedCapacity);
    }
    
    attributeColumns[columnName] = typedArray;
  }
  
  return attributeColumns;
}
```

**Acceptance Criteria**:
- [ ] Generates columns with `attr_` prefix
- [ ] Uses correct TypedArray type for each field type
- [ ] All arrays have equal length (alignedCapacity)
- [ ] Matches spec lines 375-418

---

### Step 3.2: Create Schema-Aware SpanBuffer Factory

**File**: `packages/lmao/src/lib/buffer/createSchemaBuffer.ts`

**Task**: Combine schema and buffer creation.

**Implementation**:
```typescript
import type { TagAttributeSchema } from '../schema/types';
import { createEmptySpanBuffer } from './createSpanBuffer';
import { generateAttributeColumns } from './generateAttributeColumns';
import { getCacheAlignedCapacity } from './capacity';

let nextGlobalSpanId = 1;

export function createSchemaBuffer(
  schema: TagAttributeSchema,
  requestedCapacity: number = 64
): SpanBuffer & Record<string, TypedArray> {
  const attributeCount = Object.keys(schema).length;
  const alignedCapacity = getCacheAlignedCapacity(requestedCapacity, 1);
  
  const baseBuffer = createEmptySpanBuffer(
    nextGlobalSpanId++,
    requestedCapacity,
    attributeCount
  );
  
  const attributeColumns = generateAttributeColumns(schema, alignedCapacity);
  
  return {
    ...baseBuffer,
    ...attributeColumns,
  };
}
```

**Acceptance Criteria**:
- [ ] Creates buffer with schema-defined columns
- [ ] All columns have equal length
- [ ] Returns properly typed buffer

---

## Phase 4: Module Context System

### Step 4.1: Define ModuleContext Interface

**File**: `packages/lmao/src/lib/context/types.ts`

**Task**: Define module and task context types.

**Implementation**:
```typescript
export interface ModuleMetadata {
  gitSha: string;
  filePath: string;
  moduleName: string;
}

export interface ModuleContext {
  moduleId: number;
  gitSha: string;
  filePath: string;
  spanBufferCapacityStats: {
    currentCapacity: number;
    totalWrites: number;
    overflowWrites: number;
    totalBuffersCreated: number;
  };
}

export interface TaskContext {
  module: ModuleContext;
  spanNameId: number;
  lineNumber: number;
}
```

**Acceptance Criteria**:
- [ ] Types match spec from `01j_module_context_and_spanlogger_generation.md`
- [ ] All required fields present

---

### Step 4.2: Implement Module Registration

**File**: `packages/lmao/src/lib/context/moduleRegistry.ts`

**Task**: Track registered modules.

**Implementation**:
```typescript
import type { ModuleMetadata, ModuleContext } from './types';

let nextModuleId = 1;
const modules = new Map<number, ModuleContext>();

export function registerModule(metadata: ModuleMetadata): number {
  const moduleId = nextModuleId++;
  const moduleContext: ModuleContext = {
    moduleId,
    gitSha: metadata.gitSha,
    filePath: metadata.filePath,
    spanBufferCapacityStats: {
      currentCapacity: 64,
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };
  
  modules.set(moduleId, moduleContext);
  return moduleId;
}
```

**Acceptance Criteria**:
- [ ] Assigns unique module IDs
- [ ] Initializes capacity stats
- [ ] Stores module context

---

### Step 4.3: Implement `createModuleContext`

**File**: `packages/lmao/src/lib/context/createModuleContext.ts`

**Task**: Create module context with schema.

**Implementation**:
```typescript
import type { TagAttributeSchema, ModuleMetadata } from '../schema/types';
import { registerModule } from './moduleRegistry';
import type { ModuleContext } from './types';

export function createModuleContext(config: {
  moduleMetadata: ModuleMetadata;
  tagAttributes: TagAttributeSchema;
}) {
  const moduleContext: ModuleContext = {
    moduleId: registerModule(config.moduleMetadata),
    gitSha: config.moduleMetadata.gitSha,
    filePath: config.moduleMetadata.filePath,
    spanBufferCapacityStats: {
      currentCapacity: 64,
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };
  
  return {
    moduleContext,
    // task wrapper will be added in next phase
  };
}
```

**Acceptance Criteria**:
- [ ] Creates module context
- [ ] Registers module
- [ ] Returns context with schema

---

## Phase 5: Entry Types and Operations

### Step 5.1: Define Entry Type Enum

**File**: `packages/lmao/src/lib/entry/types.ts`

**Task**: Define all entry types from spec.

**Implementation**:
```typescript
export type EntryType =
  | 'span-start'
  | 'span-ok'
  | 'span-err'
  | 'span-exception'
  | 'tag'
  | 'info'
  | 'debug'
  | 'warn'
  | 'error'
  | 'ff-access'
  | 'ff-usage';

export const ENTRY_TYPE_CODES: Record<EntryType, number> = {
  'span-start': 1,
  'span-ok': 2,
  'span-err': 3,
  'span-exception': 4,
  'tag': 5,
  'info': 6,
  'debug': 7,
  'warn': 8,
  'error': 9,
  'ff-access': 10,
  'ff-usage': 11,
};
```

**Acceptance Criteria**:
- [ ] All entry types from `01h_entry_types_and_logging_primitives.md` are defined
- [ ] Enum codes are unique

---

## Implementation Order Summary

1. **Phase 1: Schema Definition** (Steps 1.1-1.5) - **START HERE**
2. **Phase 2: Buffer Foundation** (Steps 2.1-2.3)
3. **Phase 3: Schema-Buffer Integration** (Steps 3.1-3.2)
4. **Phase 4: Module Context** (Steps 4.1-4.3)
5. **Phase 5: Entry Types** (Step 5.1)

Each phase builds on the previous one. Complete Phase 1 before moving to Phase 2.

## Testing Strategy

- Unit tests for each function
- Type tests to verify TypeScript inference
- Integration tests for schema-to-buffer flow
- Performance benchmarks for buffer operations

## Next Steps After Phase 1

After completing `defineTagAttributes`:
1. Verify it works with all examples from the spec
2. Add TypeScript type inference tests
3. Document the API
4. Move to Phase 2 (buffer creation)

---

## Notes for Coding Agent

- Follow the exact file paths specified
- Implement acceptance criteria for each step
- Run tests after each step
- Maintain TypeScript strict mode
- Reference the spec documents for detailed requirements
- Start with Phase 1, Step 1.1 and proceed sequentially

