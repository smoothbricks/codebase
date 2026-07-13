import { getSchemaType } from '@smoothbricks/arrow-builder';
import type { MessageLayoutFamily } from '../runtimeHint.js';
import type { LogSchema } from '../schema/LogSchema.js';

export type WasmNumericFamily = 'u8' | 'u32' | 'f64';
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
  readonly entryTypeOffset: number;
  readonly logHeaderOffset?: number;
}

export type WasmSlabLayoutDescriptor = WasmSystemSlabLayoutDescriptor | WasmNumericSlabLayoutDescriptor;

export interface WasmPhysicalLayoutDescriptor {
  readonly capacity: number;
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly system: WasmSystemSlabLayoutDescriptor;
  readonly slabs: Readonly<Record<WasmNumericFamily, WasmNumericSlabLayoutDescriptor | null>>;
  readonly columns: readonly WasmColumnLayoutDescriptor[];
}

export interface WasmLayoutTemplate {
  readonly messageLayoutFamily: MessageLayoutFamily;
  readonly columns: readonly WasmColumnLayoutTemplate[];
  forCapacity(capacity: number): WasmPhysicalLayoutDescriptor;
}

function align(value: number, alignment: number): number {
  return (value + alignment - 1) & ~(alignment - 1);
}

function freezeExactLayout(
  capacity: number,
  messageLayoutFamily: MessageLayoutFamily,
  templates: readonly WasmColumnLayoutTemplate[],
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

  const entryTypeOffset = capacity * 8;
  const systemByteLength = capacity * 9;
  const system =
    messageLayoutFamily === 'dynamic-only'
      ? Object.freeze({
          family: 'system' as const,
          byteLength: systemByteLength,
          alignment: 8 as const,
          timestampOffset: 0 as const,
          entryTypeOffset,
        })
      : Object.freeze({
          family: 'system' as const,
          byteLength: align(systemByteLength, 4) + capacity * 4,
          alignment: 8 as const,
          timestampOffset: 0 as const,
          entryTypeOffset,
          logHeaderOffset: align(systemByteLength, 4),
        });

  return Object.freeze({
    capacity,
    messageLayoutFamily,
    system,
    slabs: Object.freeze({
      u8: slab('u8', 1),
      u32: slab('u32', 4),
      f64: slab('f64', 8),
    }),
    columns: Object.freeze(columns),
  });
}

function buildWasmLayoutTemplate(schema: LogSchema, messageLayoutFamily: MessageLayoutFamily): WasmLayoutTemplate {
  const columns: WasmColumnLayoutTemplate[] = [];
  let columnIndex = 0;

  for (const name of schema._columnNames) {
    const field = schema.fields[name];
    const schemaType = getSchemaType(field);
    const eager = typeof field === 'object' && field !== null && Reflect.get(field, '__eager') === true;
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
    columns: frozenColumns,
    forCapacity(capacity: number): WasmPhysicalLayoutDescriptor {
      let descriptor = descriptors.get(capacity);
      if (descriptor === undefined) {
        descriptor = freezeExactLayout(capacity, messageLayoutFamily, frozenColumns);
        descriptors.set(capacity, descriptor);
      }
      return descriptor;
    },
  });
}

const templates = new WeakMap<LogSchema, Map<MessageLayoutFamily, WasmLayoutTemplate>>();

export function createWasmLayoutTemplate(
  schema: LogSchema,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
): WasmLayoutTemplate {
  let familyTemplates = templates.get(schema);
  let template = familyTemplates?.get(messageLayoutFamily);
  if (template === undefined) {
    template = buildWasmLayoutTemplate(schema, messageLayoutFamily);
    familyTemplates ??= new Map();
    familyTemplates.set(messageLayoutFamily, template);
    templates.set(schema, familyTemplates);
  }
  return template;
}

export function getWasmPhysicalLayout(
  schema: LogSchema,
  capacity: number,
  messageLayoutFamily: MessageLayoutFamily = 'mixed',
): WasmPhysicalLayoutDescriptor {
  return createWasmLayoutTemplate(schema, messageLayoutFamily).forCapacity(capacity);
}
