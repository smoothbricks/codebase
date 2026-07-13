import { getSchemaType } from '@smoothbricks/arrow-builder';
import type { EagerColumnDescriptor } from '../physicalLayoutPlan.js';
import type { MessageLayoutFamily, MessagePhysicalLayout } from '../runtimeHint.js';
import type { LogSchema } from '../schema/LogSchema.js';

export type WasmNumericFamily = 'u8' | 'u32' | 'f64';

const EMPTY_EAGER_COLUMNS: EagerColumnDescriptor = Object.freeze({
  names: Object.freeze([]),
  words: Object.freeze([]),
  key: '',
});
export type WasmArrayType = 'Uint8Array' | 'Uint32Array' | 'Float64Array' | 'BigUint64Array';

export interface WasmColumnLayoutTemplate {
  readonly name: string;
  readonly columnIndex: number;
  readonly family: WasmNumericFamily;
  readonly arrayType: WasmArrayType;
  readonly byteWidth: 1 | 4 | 8;
  readonly alignment: 1 | 4 | 8;
  readonly eager: boolean;
}

export interface WasmColumnLayoutDescriptor extends WasmColumnLayoutTemplate {
  readonly nullOffset: number;
  readonly nullByteLength: number;
  readonly valueOffset: number;
  readonly valueLength: number;
}

export interface WasmNumericSlabLayoutDescriptor {
  readonly family: WasmNumericFamily;
  readonly byteLength: number;
  readonly alignment: 1 | 4 | 8;
}

export interface WasmSystemSlabLayoutDescriptor {
  readonly family: 'system';
  readonly byteLength: number;
  readonly alignment: 8;
  readonly timestampOffset: 0;
  readonly entryTypeOffset: number | null;
  readonly messageIdOffset: number | null;
  readonly messageIdValidityOffset: number | null;
  readonly messageDenseIndexOffset: number | null;
  readonly messageValidityOffset: number | null;
  readonly rowHeaderOffset: number | null;
  /** Logical row offset into the JS raw-message sidecar; contributes no WASM bytes. */
  readonly messageValueOffset: 0 | null;
}

export type WasmSlabLayoutDescriptor = WasmSystemSlabLayoutDescriptor | WasmNumericSlabLayoutDescriptor;

export interface WasmPhysicalLayoutDescriptor {
  readonly capacity: number;
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly messagePhysicalLayout: MessagePhysicalLayout;
  readonly eagerColumns: EagerColumnDescriptor;
  readonly system: WasmSystemSlabLayoutDescriptor;
  readonly slabs: Readonly<Record<WasmNumericFamily, WasmNumericSlabLayoutDescriptor | null>>;
  readonly columns: readonly WasmColumnLayoutDescriptor[];
}

export interface WasmLayoutTemplate {
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly messagePhysicalLayout: MessagePhysicalLayout;
  readonly eagerColumns: EagerColumnDescriptor;
  readonly columns: readonly WasmColumnLayoutTemplate[];
  forCapacity(capacity: number): WasmPhysicalLayoutDescriptor;
}

function align(value: number, alignment: number): number {
  return (value + alignment - 1) & ~(alignment - 1);
}

function freezeExactLayout(
  capacity: number,
  messageLayoutFamily: MessageLayoutFamily,
  messagePhysicalLayout: MessagePhysicalLayout,
  templates: readonly WasmColumnLayoutTemplate[],
  eagerColumns: EagerColumnDescriptor,
): WasmPhysicalLayoutDescriptor {
  if (!Number.isSafeInteger(capacity) || capacity <= 0) {
    throw new RangeError(`WASM layout capacity must be a positive safe integer, received ${capacity}`);
  }

  const nullByteLength = Math.ceil(capacity / 8);
  const familyOffsets: Record<WasmNumericFamily, number> = { u8: 0, u32: 0, f64: 0 };
  const columns: WasmColumnLayoutDescriptor[] = [];

  for (const template of templates) {
    const nullOffset = familyOffsets[template.family];
    const valueOffset = align(nullOffset + nullByteLength, template.alignment);
    const valueLength = capacity * template.byteWidth;
    familyOffsets[template.family] = valueOffset + valueLength;
    columns.push(
      Object.freeze({
        ...template,
        nullOffset,
        nullByteLength,
        valueOffset,
        valueLength,
      }),
    );
  }

  const slab = (family: WasmNumericFamily, alignment: 1 | 4 | 8): WasmNumericSlabLayoutDescriptor | null => {
    const byteLength = familyOffsets[family];
    return byteLength === 0 ? null : Object.freeze({ family, byteLength, alignment });
  };

  const hasStaticMessages = messageLayoutFamily !== 'dynamic-only';
  const messageValueOffset = messageLayoutFamily === 'static-only' ? null : 0;
  let systemByteLength = capacity * 8;
  let entryTypeOffset: number | null = null;
  let messageIdOffset: number | null = null;
  let messageIdValidityOffset: number | null = null;
  let messageDenseIndexOffset: number | null = null;
  let messageValidityOffset: number | null = null;
  let rowHeaderOffset: number | null = null;

  if (messagePhysicalLayout === 'packed') {
    rowHeaderOffset = align(systemByteLength, 4);
    systemByteLength = rowHeaderOffset + capacity * 4;
  } else {
    entryTypeOffset = systemByteLength;
    systemByteLength = entryTypeOffset + capacity;
    if (hasStaticMessages) {
      if (messagePhysicalLayout === 'current') {
        messageIdOffset = align(systemByteLength, 2);
        systemByteLength = messageIdOffset + capacity * 2;
        messageIdValidityOffset = systemByteLength;
        systemByteLength = messageIdValidityOffset + nullByteLength;
      } else {
        messageDenseIndexOffset = align(systemByteLength, 4);
        systemByteLength = messageDenseIndexOffset + capacity * 4;
        messageValidityOffset = systemByteLength;
        systemByteLength = messageValidityOffset + nullByteLength;
      }
    } else {
      messageValidityOffset = systemByteLength;
      systemByteLength = messageValidityOffset + nullByteLength;
    }
  }

  const system: WasmSystemSlabLayoutDescriptor = Object.freeze({
    family: 'system',
    byteLength: align(systemByteLength, 8),
    alignment: 8,
    timestampOffset: 0,
    entryTypeOffset,
    messageIdOffset,
    messageIdValidityOffset,
    messageDenseIndexOffset,
    messageValidityOffset,
    rowHeaderOffset,
    messageValueOffset,
  });

  return Object.freeze({
    capacity,
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns,
    system,
    slabs: Object.freeze({
      u8: slab('u8', 1),
      u32: slab('u32', 4),
      f64: slab('f64', 8),
    }),
    columns: Object.freeze(columns),
  });
}

function buildWasmLayoutTemplate(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily,
  messagePhysicalLayout: MessagePhysicalLayout,
  eagerColumns: EagerColumnDescriptor,
): WasmLayoutTemplate {
  const columns: WasmColumnLayoutTemplate[] = [];
  const preallocatedColumns = new Set(eagerColumns.names);
  let columnIndex = 0;

  for (const name of schema._columnNames) {
    const field = schema.fields[name];
    const schemaType = getSchemaType(field);
    const eager =
      preallocatedColumns.has(name) ||
      (typeof field === 'object' && field !== null && Reflect.get(field, '__eager') === true);
    let storage: Pick<WasmColumnLayoutTemplate, 'family' | 'arrayType' | 'byteWidth' | 'alignment'> | null = null;

    switch (schemaType) {
      case 'enum':
      case 'boolean':
        storage = { family: 'u8', arrayType: 'Uint8Array', byteWidth: 1, alignment: 1 };
        break;
      case 'number':
        storage = { family: 'f64', arrayType: 'Float64Array', byteWidth: 8, alignment: 8 };
        break;
      case 'bigUint64':
        storage = { family: 'f64', arrayType: 'BigUint64Array', byteWidth: 8, alignment: 8 };
        break;
    }

    if (storage !== null && name !== 'message') {
      columns.push(Object.freeze({ name, columnIndex, eager, ...storage }));
    }
    columnIndex++;
  }

  const frozenColumns = Object.freeze(columns);
  const descriptors = new Map<number, WasmPhysicalLayoutDescriptor>();
  return Object.freeze({
    messageLayoutFamily,
    messagePhysicalLayout,
    eagerColumns,
    columns: frozenColumns,
    forCapacity(capacity: number): WasmPhysicalLayoutDescriptor {
      let descriptor = descriptors.get(capacity);
      if (descriptor === undefined) {
        descriptor = freezeExactLayout(capacity, messageLayoutFamily, messagePhysicalLayout, frozenColumns, eagerColumns);
        descriptors.set(capacity, descriptor);
      }
      return descriptor;
    },
  });
}

const templates = new WeakMap<LogSchema, Map<string, WasmLayoutTemplate>>();

export function createWasmLayoutTemplate(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  messagePhysicalLayout: MessagePhysicalLayout = 'current',
  eagerColumns: EagerColumnDescriptor = EMPTY_EAGER_COLUMNS,
): WasmLayoutTemplate {
  const cacheKey = `${messageLayoutFamily}:${messagePhysicalLayout}:${eagerColumns.key}`;
  let familyTemplates = templates.get(schema);
  let template = familyTemplates?.get(cacheKey);
  if (template === undefined) {
    template = buildWasmLayoutTemplate(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns);
    familyTemplates ??= new Map();
    familyTemplates.set(cacheKey, template);
    templates.set(schema, familyTemplates);
  }
  return template;
}

export function getWasmPhysicalLayout(
  schema: LogSchema,
  capacity: number,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
  messagePhysicalLayout: MessagePhysicalLayout = 'current',
  eagerColumns: EagerColumnDescriptor = EMPTY_EAGER_COLUMNS,
): WasmPhysicalLayoutDescriptor {
  return createWasmLayoutTemplate(schema, messageLayoutFamily, messagePhysicalLayout, eagerColumns).forCapacity(capacity);
}
